use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use pretty_assertions::assert_eq;

use super::aggregate_journal::AggregateStep;
use super::degradation::record_exogenous_terminal_degradation;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashInjector;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_support::assert_integrity;
use super::failure_injection_support::frozen_state;
use super::failure_injection_tests::delivery_now;
use super::failure_injection_tests::observation;
use super::failure_injection_tests::receipt_params_for_matrix;
use super::failure_injection_tests::runtime_with_command_at;
use super::failure_injection_tests::runtime_with_root_at;
use crate::StateRuntime;
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
    assert!(matches!(
        reopened
            .persist_coordination_recipient_receipt(receipt_params_for_matrix())
            .await?,
        PersistRecipientReceiptOutcome::Duplicate(_)
    ));
    sink.invoke_after_verified_receipt(durable_receipts);
    assert_eq!(sink.calls(), 1);
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
    let before = frozen_state(&runtime).await?;
    let before_entries = directory_entries(&home).await?;
    assert_pending_and_unpublished(&runtime).await?;

    let clock = CrashInjector::recording(1_753_000_000_000);
    drop(runtime);
    for advance_ms in [1, 86_400_000, 7 * 86_400_000] {
        clock.advance(advance_ms);
        let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
        assert_eq!(frozen_state(&reopened).await?, before);
        assert_pending_and_unpublished(&reopened).await?;
        assert_eq!(tokio::fs::read(&sentinel_path).await?, sentinel);
        assert_eq!(directory_entries(&home).await?, before_entries);
        assert_integrity(&reopened).await?;
        drop(reopened);
    }
    Ok(())
}

async fn assert_pending_and_unpublished(runtime: &StateRuntime) -> anyhow::Result<()> {
    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM coordination_projection_outbox WHERE status='pending'),\
         (SELECT COUNT(*) FROM coordination_degradation_publication_outbox WHERE status='pending'),\
         (SELECT COUNT(*) FROM coordination_roots WHERE published_revision=0),\
         (SELECT COUNT(*) FROM coordination_roots)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert!(counts.0 > 0);
    assert!(counts.1 > 0);
    assert_eq!(counts.2, counts.3);
    Ok(())
}

async fn directory_entries(path: &std::path::Path) -> anyhow::Result<Vec<std::ffi::OsString>> {
    let mut entries = tokio::fs::read_dir(path).await?;
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        names.push(entry.file_name());
    }
    names.sort();
    Ok(names)
}
