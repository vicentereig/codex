use pretty_assertions::assert_eq;

use super::failure_injection_support::*;
use super::failure_injection_tests::receipt_params_for_matrix;
use super::failure_injection_tests::runtime_with_command_at;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn frozen_state_is_whole_record_and_ciphertext_safe() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = runtime_with_command_at(home.clone()).await?;
    assert!(matches!(
        runtime
            .persist_coordination_recipient_receipt(receipt_params_for_matrix())
            .await?,
        PersistRecipientReceiptOutcome::Applied(_)
    ));
    let before = frozen_state(
        &runtime,
        FrozenStateInputs {
            sqlite_home: home.as_path(),
            controlled_effect_count: 7,
        },
    )
    .await?;
    let same = frozen_state(
        &runtime,
        FrozenStateInputs {
            sqlite_home: home.as_path(),
            controlled_effect_count: 7,
        },
    )
    .await?;
    assert_eq!(before, same);
    assert_eq!(before.controlled_effect_count, 7);
    assert!(matches!(
        &before.marker,
        FrozenMarker::Entry {
            kind: FrozenEntryKind::File,
            contents: FrozenMarkerContents::Bounded(_),
        }
    ));
    assert!(
        before
            .directory_entries
            .windows(2)
            .all(|pair| pair[0] <= pair[1])
    );

    let ciphertexts: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT ciphertext FROM coordination_commands UNION ALL SELECT ciphertext FROM coordination_inbox",
    )
    .fetch_all(&*runtime.pool)
    .await?;
    assert_eq!(ciphertexts, vec![vec![0xA5; 384], vec![0xA5; 384]]);
    let rendered = format!("{before:?}");
    assert_eq!(
        rendered.matches("<redacted ciphertext: 384 bytes>").count(),
        2
    );
    assert!(!rendered.contains(&format!("{:?}", vec![0xA5; 384])));

    sqlx::query("DROP TRIGGER coordination_command_transition_guard")
        .execute(&*runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_commands SET ciphertext=zeroblob(encoded_payload_bytes)")
        .execute(&*runtime.pool)
        .await?;
    let changed = frozen_state(
        &runtime,
        FrozenStateInputs {
            sqlite_home: home.as_path(),
            controlled_effect_count: 7,
        },
    )
    .await?;
    assert_ne!(before, changed);
    Ok(())
}

#[tokio::test]
async fn frozen_marker_states_are_bounded_and_do_not_follow_links() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = runtime_with_command_at(home.clone()).await?;
    let marker = home.join(super::authority_marker::MARKER_FILE_NAME);
    tokio::fs::remove_file(&marker).await?;
    let missing = frozen_state(&runtime, FrozenStateInputs::new(&home)).await?;
    assert!(matches!(missing.marker, FrozenMarker::Missing));

    tokio::fs::write(
        &marker,
        vec![0; super::authority_marker::MAX_MARKER_BYTES as usize + 1],
    )
    .await?;
    let oversized = frozen_state(&runtime, FrozenStateInputs::new(&home)).await?;
    assert!(matches!(
        oversized.marker,
        FrozenMarker::Entry {
            kind: FrozenEntryKind::File,
            contents: FrozenMarkerContents::Oversized { .. },
        }
    ));

    #[cfg(unix)]
    {
        tokio::fs::remove_file(&marker).await?;
        let target = home.join("marker-target");
        tokio::fs::write(&target, b"marker-target-must-not-be-read").await?;
        std::os::unix::fs::symlink(&target, &marker)?;
        let linked = frozen_state(&runtime, FrozenStateInputs::new(&home)).await?;
        assert!(matches!(
            linked.marker,
            FrozenMarker::Entry {
                kind: FrozenEntryKind::Symlink,
                contents: FrozenMarkerContents::NotRegular,
            }
        ));
    }
    Ok(())
}

#[tokio::test]
async fn frozen_state_debug_redacts_every_variable_value_and_keeps_exact_equality()
-> anyhow::Result<()> {
    const TEXT_SENTINEL: &str = "private-text-sentinel-A";
    const OTHER_TEXT: &str = "private-text-sentinel-B";
    const BLOB_SENTINEL: &[u8] = b"private-blob-sentinel-A";
    const OTHER_BLOB: &[u8] = b"private-blob-sentinel-B";
    const MARKER_SENTINEL: &[u8] = b"private-marker-sentinel-A";
    const OTHER_MARKER: &[u8] = b"private-marker-sentinel-B";
    const NAME_SENTINEL: &str = "private-name-sentinel-A";
    const OTHER_NAME: &str = "private-name-sentinel-B";

    let home = unique_temp_dir();
    let runtime = runtime_with_command_at(home.clone()).await?;
    sqlx::query("UPDATE coordination_projection_outbox SET last_error=?")
        .bind(TEXT_SENTINEL)
        .execute(&*runtime.pool)
        .await?;
    sqlx::query("DROP TRIGGER coordination_events_immutable_update")
        .execute(&*runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_events SET idempotency_key_bytes=?")
        .bind(BLOB_SENTINEL)
        .execute(&*runtime.pool)
        .await?;
    let marker = home.join(super::authority_marker::MARKER_FILE_NAME);
    tokio::fs::write(&marker, MARKER_SENTINEL).await?;
    let sentinel_name = home.join(NAME_SENTINEL);
    tokio::fs::write(&sentinel_name, b"contents are not captured").await?;

    let seeded = frozen_state(&runtime, FrozenStateInputs::new(&home)).await?;
    let rendered = format!("{seeded:?}");
    assert!(rendered.contains("<redacted text:"));
    assert!(rendered.contains("<redacted bytes:"));
    assert!(rendered.contains("<redacted file name:"));
    assert!(!rendered.contains(TEXT_SENTINEL));
    assert!(!rendered.contains(NAME_SENTINEL));
    assert!(!rendered.contains(&format!("{BLOB_SENTINEL:?}")));
    assert!(!rendered.contains(&format!("{MARKER_SENTINEL:?}")));

    sqlx::query("UPDATE coordination_projection_outbox SET last_error=?")
        .bind(OTHER_TEXT)
        .execute(&*runtime.pool)
        .await?;
    let changed_text = frozen_state(&runtime, FrozenStateInputs::new(&home)).await?;
    assert_ne!(seeded, changed_text);

    sqlx::query("UPDATE coordination_projection_outbox SET last_error=?")
        .bind(TEXT_SENTINEL)
        .execute(&*runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_events SET idempotency_key_bytes=?")
        .bind(OTHER_BLOB)
        .execute(&*runtime.pool)
        .await?;
    let changed_blob = frozen_state(&runtime, FrozenStateInputs::new(&home)).await?;
    assert_ne!(seeded, changed_blob);

    sqlx::query("UPDATE coordination_events SET idempotency_key_bytes=?")
        .bind(BLOB_SENTINEL)
        .execute(&*runtime.pool)
        .await?;
    tokio::fs::write(&marker, OTHER_MARKER).await?;
    let changed_marker = frozen_state(&runtime, FrozenStateInputs::new(&home)).await?;
    assert_ne!(seeded, changed_marker);

    tokio::fs::write(&marker, MARKER_SENTINEL).await?;
    tokio::fs::rename(&sentinel_name, home.join(OTHER_NAME)).await?;
    let changed_name = frozen_state(&runtime, FrozenStateInputs::new(&home)).await?;
    assert_ne!(seeded, changed_name);
    Ok(())
}
