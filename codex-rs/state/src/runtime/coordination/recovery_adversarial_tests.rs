use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationSemanticSlot;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::inbox_test_support::persist_initial_receipt;
use super::inbox_test_support::runtime_with_assignment_command;
use super::legacy_links::record_legacy_link;
use super::recovery::RecoveryWriteError;
use super::recovery_test_support::*;
use crate::StateRuntime;
use crate::model::coordination_recovery::CheckedLegacyLink;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn same_source_with_different_reduction_fails_closed() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let event = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 31);
    let root = thread_id(super::aggregate_test_support::ROOT);
    let first = CheckedLegacyLink::new(root, epoch, &event)?;
    record_legacy_link(&runtime.pool, &first).await?;

    let mut changed = serde_json::to_value(event)?;
    changed["reportedSuccess"] = json!({"status":"unavailable","reason":"ambiguousSource"});
    let changed: CoordinationEvent = serde_json::from_value(changed)?;
    let changed = CheckedLegacyLink::new(root, epoch, &changed)?;
    assert!(matches!(
        record_legacy_link(&runtime.pool, &changed).await,
        Err(RecoveryWriteError::DivergentReduction)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_legacy_links")
            .fetch_one(&*runtime.pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn exact_fingerprint_collision_is_not_treated_as_duplicate() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let event = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 32);
    let root = thread_id(super::aggregate_test_support::ROOT);
    let link = CheckedLegacyLink::new(root, epoch, &event)?;
    sqlx::query(
        "INSERT INTO coordination_legacy_links \
         (compatibility_event_id,root_thread_id,state_epoch,source_shape,source_thread_id,\
          source_turn_id,source_item_id,source_ordinal,semantic_slot,source_identity_bytes,\
          source_identity_fingerprint,canonical_event_bytes,canonical_event_fingerprint,\
          adapter_version,sanitizer_version,after_revision,suppressed_by_native_event_id,\
          suppressed_at_ms,created_at_ms) \
         VALUES ('641753a2-b8b8-557b-afcf-1c3c17bbbc47',?,?,\
          'subAgentActivity',?,NULL,NULL,32,'legacyInteractionObserved',?,?,?, ?,1,1,1,NULL,NULL,1)",
    )
    .bind(root.to_string())
    .bind(epoch.to_string())
    .bind(root.to_string())
    .bind(b"different-exact-identity".as_slice())
    .bind(link.source_identity_bytes.fingerprint().as_slice())
    .bind(b"{}".as_slice())
    .bind([0_u8; 32].as_slice())
    .execute(&*runtime.pool)
    .await?;
    assert!(matches!(
        record_legacy_link(&runtime.pool, &link).await,
        Err(RecoveryWriteError::IdentityCollision)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_legacy_links")
            .fetch_one(&*runtime.pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn exact_bytes_with_divergent_structured_fields_are_corrupt() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let event = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 33);
    let root = thread_id(super::aggregate_test_support::ROOT);
    let link = CheckedLegacyLink::new(root, epoch, &event)?;
    sqlx::query(
        "INSERT INTO coordination_legacy_links \
         (compatibility_event_id,root_thread_id,state_epoch,source_shape,source_thread_id,\
          source_turn_id,source_item_id,source_ordinal,semantic_slot,source_identity_bytes,\
          source_identity_fingerprint,canonical_event_bytes,canonical_event_fingerprint,\
          adapter_version,sanitizer_version,after_revision,suppressed_by_native_event_id,\
          suppressed_at_ms,created_at_ms) \
         VALUES (?,?,?,'subAgentActivity',?,NULL,NULL,34,'legacyInteractionObserved',\
                 ?,?,?,?,1,1,1,NULL,NULL,1)",
    )
    .bind(link.compatibility_event_id.to_string())
    .bind(root.to_string())
    .bind(epoch.to_string())
    .bind(root.to_string())
    .bind(link.source_identity_bytes.as_slice())
    .bind(link.source_identity_bytes.fingerprint().as_slice())
    .bind(link.canonical_event_bytes.as_slice())
    .bind(link.canonical_event_bytes.fingerprint().as_slice())
    .execute(&*runtime.pool)
    .await?;
    assert!(matches!(
        record_legacy_link(&runtime.pool, &link).await,
        Err(RecoveryWriteError::CorruptState)
    ));
    Ok(())
}

#[tokio::test]
async fn quarantine_remains_read_only_for_recovery() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let receipt = persist_initial_receipt(&runtime).await?;
    sqlx::query(
        "UPDATE coordination_authority SET status='quarantined',quarantine_reason='test',\
         updated_at_ms=updated_at_ms+1 WHERE singleton_id=1",
    )
    .execute(&*runtime.pool)
    .await?;
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM coordination_degradation_records")
        .fetch_one(&*runtime.pool)
        .await?;
    assert!(matches!(
        super::recovery_batch::recover_coordination_batch(&runtime.pool, i64::MAX / 4, 100).await,
        Err(RecoveryWriteError::Quarantined)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
            .fetch_one(&*runtime.pool)
            .await?,
        before
    );
    for rejected in [
        sqlx::query(
            "UPDATE coordination_commands SET lifecycle='expired',version=version+1,\
             lease_expires_at_ms=NULL,ciphertext=NULL,purged_at_ms=?,updated_at_ms=? \
             WHERE operation_id=?",
        )
        .bind(receipt.expires_at_ms)
        .bind(receipt.expires_at_ms)
        .bind(receipt.command_operation_id.to_string())
        .execute(&*runtime.pool)
        .await,
        sqlx::query(
            "UPDATE coordination_inbox SET lifecycle='expired',version=version+1,\
             retry_after_ms=MAX(retry_after_ms,?),lease_expires_at_ms=NULL,\
             lease_claim_operation_id=NULL,ciphertext=NULL,\
             purged_at_ms=?,updated_at_ms=? WHERE receipt_id=?",
        )
        .bind(receipt.expires_at_ms)
        .bind(receipt.expires_at_ms)
        .bind(receipt.expires_at_ms)
        .bind(receipt.receipt_id.to_string())
        .execute(&*runtime.pool)
        .await,
    ] {
        let error = rejected.expect_err("quarantine must reject raw coordination writes");
        assert!(
            error
                .to_string()
                .contains("quarantined coordination authority is read-only"),
            "unexpected raw write rejection: {error}"
        );
    }
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT c.lifecycle,i.lifecycle FROM coordination_commands c \
             JOIN coordination_inbox i ON i.command_operation_id=c.operation_id \
             WHERE c.operation_id=? AND i.receipt_id=?",
        )
        .bind(receipt.command_operation_id.to_string())
        .bind(receipt.receipt_id.to_string())
        .fetch_one(&*runtime.pool)
        .await?,
        ("pending".to_string(), "received".to_string())
    );
    Ok(())
}

#[tokio::test]
async fn unavailable_pool_is_deferred_not_misclassified_as_quarantine() -> anyhow::Result<()> {
    let (runtime, _) = runtime_with_root().await?;
    runtime.pool.close().await;
    assert!(matches!(
        super::recovery_batch::recover_coordination_batch(&runtime.pool, 100, 1).await,
        Err(RecoveryWriteError::Deferred)
    ));
    Ok(())
}

#[tokio::test]
async fn marker_loss_is_durably_quarantined_before_recovery_mutates() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let runtime = StateRuntime::init(sqlite_home.clone(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(super::aggregate_test_support::reserve_params())
        .await?;
    let epoch = sqlx::query_scalar::<_, String>(
        "SELECT state_epoch FROM coordination_authority WHERE singleton_id=1",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    let epoch = codex_coordination::StateEpoch::parse(&epoch)?;
    tokio::fs::remove_file(sqlite_home.join(super::authority_marker::MARKER_FILE_NAME)).await?;
    let event = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 44);
    let link = CheckedLegacyLink::new(
        thread_id(super::aggregate_test_support::ROOT),
        epoch,
        &event,
    )?;
    assert!(matches!(
        record_legacy_link(&runtime.pool, &link).await,
        Err(RecoveryWriteError::Quarantined)
    ));
    assert_eq!(
        sqlx::query_as::<_, (String, i64)>(
            "SELECT status,(SELECT COUNT(*) FROM coordination_legacy_links) \
             FROM coordination_authority WHERE singleton_id=1",
        )
        .fetch_one(&*runtime.pool)
        .await?,
        ("quarantined".to_string(), 0)
    );
    Ok(())
}
