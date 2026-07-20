use std::collections::BTreeSet;
use std::ffi::OsString;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use pretty_assertions::assert_eq;

use super::aggregate_journal::AggregateStep;
use super::authority_marker::MARKER_FILE_NAME;
use super::degradation::record_exogenous_terminal_degradation;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashInjector;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_support::FrozenStateInputs;
use super::failure_injection_support::assert_integrity;
use super::failure_injection_support::frozen_state;
use super::failure_injection_tests::delivery_now;
use super::failure_injection_tests::observation;
use super::failure_injection_tests::receipt_params_for_matrix;
use super::failure_injection_tests::runtime_with_command_at;
use super::failure_injection_tests::runtime_with_root_at;
use crate::GOALS_DB_FILENAME;
use crate::LOGS_DB_FILENAME;
use crate::MEMORIES_DB_FILENAME;
use crate::STATE_DB_FILENAME;
use crate::StateRuntime;
use crate::THREAD_HISTORY_DB_FILENAME;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::model::coordination_recovery::RecordExogenousTerminalOutcome;
use crate::runtime::test_support::unique_temp_dir;

struct FakeControlledSink(AtomicUsize);

impl FakeControlledSink {
    fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    fn calls(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    fn invoke_after_verified_receipt(&self, durable_receipts: i64) {
        assert_eq!(durable_receipts, 1);
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn response_loss_cannot_authorize_a_controlled_effect_before_reopen_and_retry()
-> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = runtime_with_command_at(home.clone()).await?;
    let now_ms = delivery_now(&runtime).await?;
    let sink = FakeControlledSink::new();
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Aggregate(AggregateStep::AfterCommit),
            occurrence: 1,
        },
        now_ms,
    );
    assert!(
        runtime
            .persist_coordination_recipient_receipt_with(receipt_params_for_matrix(), &injector,)
            .await
            .is_err()
    );
    assert_eq!(sink.calls(), 0);
    drop(runtime);

    let reopened = StateRuntime::init(home, "test".to_string()).await?;
    let durable_receipts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM coordination_inbox WHERE receipt_id=?")
            .bind(super::inbox_test_support::RECEIPT_ONE)
            .fetch_one(&*reopened.pool)
            .await?;
    assert_eq!(durable_receipts, 1);
    assert_eq!(sink.calls(), 0);
    let before_effect = frozen_state(
        &reopened,
        FrozenStateInputs {
            sqlite_home: reopened.codex_home(),
            controlled_effect_count: sink.calls(),
        },
    )
    .await?;
    assert!(matches!(
        reopened
            .persist_coordination_recipient_receipt(receipt_params_for_matrix())
            .await?,
        PersistRecipientReceiptOutcome::Duplicate(_)
    ));
    sink.invoke_after_verified_receipt(durable_receipts);
    assert_eq!(sink.calls(), 1);
    let after_effect = frozen_state(
        &reopened,
        FrozenStateInputs {
            sqlite_home: reopened.codex_home(),
            controlled_effect_count: sink.calls(),
        },
    )
    .await?;
    assert_eq!(before_effect.controlled_effect_count, 0);
    assert_eq!(after_effect.controlled_effect_count, 1);
    assert_eq!(before_effect.tables, after_effect.tables);
    assert_integrity(&reopened).await?;
    Ok(())
}

#[tokio::test]
async fn pending_outboxes_remain_unpublished_across_restart_and_clock_advance() -> anyhow::Result<()>
{
    let home = unique_temp_dir();
    let (runtime, epoch) = runtime_with_root_at(home.clone()).await?;
    let sentinel_path = home.join("coordination-sidecar-sentinel");
    let sentinel = b"stage-2-must-not-touch-this-sidecar";
    tokio::fs::write(&sentinel_path, sentinel).await?;
    assert!(matches!(
        record_exogenous_terminal_degradation(&runtime.pool, observation(epoch)?).await?,
        RecordExogenousTerminalOutcome::Applied(_)
    ));
    let before = frozen_state(&runtime, FrozenStateInputs::new(runtime.codex_home())).await?;
    assert_only_allowed_directory_entries(&home).await?;
    assert_pending_and_unpublished(&runtime).await?;

    let clock = CrashInjector::recording(1_753_000_000_000);
    drop(runtime);
    for advance_ms in [1, 86_400_000, 7 * 86_400_000] {
        clock.advance(advance_ms);
        let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
        assert_eq!(
            frozen_state(&reopened, FrozenStateInputs::new(reopened.codex_home()),).await?,
            before
        );
        assert_pending_and_unpublished(&reopened).await?;
        assert_eq!(tokio::fs::read(&sentinel_path).await?, sentinel);
        assert_only_allowed_directory_entries(&home).await?;
        assert_integrity(&reopened).await?;
        drop(reopened);
    }
    Ok(())
}

async fn assert_pending_and_unpublished(runtime: &StateRuntime) -> anyhow::Result<()> {
    let counts: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM coordination_projection_outbox WHERE status='pending'),\
         (SELECT COUNT(*) FROM coordination_projection_outbox),\
         (SELECT COUNT(*) FROM coordination_degradation_publication_outbox WHERE status='pending'),\
         (SELECT COUNT(*) FROM coordination_degradation_publication_outbox),\
         (SELECT COUNT(*) FROM coordination_roots WHERE published_revision=0),\
         (SELECT COUNT(*) FROM coordination_roots)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert!(counts.1 > 0);
    assert!(counts.3 > 0);
    assert_eq!(
        (counts.0, counts.2, counts.4),
        (counts.1, counts.3, counts.5)
    );
    Ok(())
}

async fn assert_only_allowed_directory_entries(path: &std::path::Path) -> anyhow::Result<()> {
    let required_database_names = [
        STATE_DB_FILENAME,
        LOGS_DB_FILENAME,
        GOALS_DB_FILENAME,
        MEMORIES_DB_FILENAME,
    ];
    let allowed_database_names = [
        STATE_DB_FILENAME,
        LOGS_DB_FILENAME,
        GOALS_DB_FILENAME,
        MEMORIES_DB_FILENAME,
        THREAD_HISTORY_DB_FILENAME,
    ];
    let required = required_database_names
        .into_iter()
        .chain([MARKER_FILE_NAME, "coordination-sidecar-sentinel"])
        .map(OsString::from)
        .collect::<BTreeSet<_>>();
    let allowed = allowed_database_names
        .into_iter()
        .flat_map(|name| {
            [
                OsString::from(name),
                OsString::from(format!("{name}-wal")),
                OsString::from(format!("{name}-shm")),
            ]
        })
        .chain([
            OsString::from(MARKER_FILE_NAME),
            OsString::from("coordination-sidecar-sentinel"),
        ])
        .collect::<BTreeSet<_>>();
    let actual = directory_entries(path).await?;
    assert!(
        actual.is_subset(&allowed),
        "unexpected sqlite-home entries: {actual:?}"
    );
    assert!(
        required.is_subset(&actual),
        "missing required sqlite-home entries: {actual:?}"
    );
    Ok(())
}

async fn directory_entries(path: &std::path::Path) -> anyhow::Result<BTreeSet<OsString>> {
    let mut entries = tokio::fs::read_dir(path).await?;
    let mut names = BTreeSet::new();
    while let Some(entry) = entries.next_entry().await? {
        names.insert(entry.file_name());
    }
    Ok(names)
}
