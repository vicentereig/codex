use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use pretty_assertions::assert_eq;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::aggregate_test_support::OPERATION;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::inbox_test_support::*;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::InboxMaintenanceBatch;
use crate::model::coordination_inbox::InboxTransportResolution;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_inbox::RecordInboxSelectionOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcome;

struct FailOnce {
    inbox_step: Option<InboxStep>,
    aggregate_step: Option<AggregateStep>,
    failed: AtomicBool,
    now_ms: i64,
}

struct FailAggregateOccurrence {
    occurrence: usize,
    seen: AtomicUsize,
    now_ms: i64,
}

impl AggregateFailureInjector for FailAggregateOccurrence {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        if step == AggregateStep::AggregateMutation
            && self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.occurrence
        {
            anyhow::bail!("injected aggregate mutation occurrence {}", self.occurrence);
        }
        Ok(())
    }

    fn now_ms(&self) -> i64 {
        self.now_ms
    }
}

impl InboxFailureInjector for FailAggregateOccurrence {
    fn after_inbox_step(&self, _step: InboxStep) -> anyhow::Result<()> {
        Ok(())
    }
}

impl FailOnce {
    fn inbox(step: InboxStep) -> Self {
        Self {
            inbox_step: Some(step),
            aggregate_step: None,
            failed: AtomicBool::new(false),
            now_ms: chrono::Utc::now().timestamp_millis() + 1,
        }
    }

    fn aggregate(step: AggregateStep) -> Self {
        Self {
            inbox_step: None,
            aggregate_step: Some(step),
            failed: AtomicBool::new(false),
            now_ms: chrono::Utc::now().timestamp_millis() + 1,
        }
    }

    fn fail(&self) -> anyhow::Result<()> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            anyhow::bail!("injected inbox failure")
        }
        Ok(())
    }
}

impl AggregateFailureInjector for FailOnce {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        if self.aggregate_step == Some(step) {
            self.fail()?;
        }
        Ok(())
    }

    fn now_ms(&self) -> i64 {
        self.now_ms
    }
}

impl InboxFailureInjector for FailOnce {
    fn after_inbox_step(&self, step: InboxStep) -> anyhow::Result<()> {
        if self.inbox_step == Some(step) {
            self.fail()?;
        }
        Ok(())
    }
}

fn initial_params() -> crate::model::coordination_inbox::PersistRecipientReceipt {
    receipt_params(
        OPERATION,
        RECEIPT_ONE,
        "019f7c6c-1111-7000-8000-000000000702",
        1,
        0,
        Vec::new(),
    )
}

#[tokio::test]
async fn receive_crashes_roll_back_acceptance_receipt_event_and_revision() -> anyhow::Result<()> {
    for step in [
        InboxStep::CommandRead,
        InboxStep::TargetFence,
        InboxStep::ReceiptEvent,
        InboxStep::ReceiptInsert,
    ] {
        let runtime = runtime_with_assignment_command().await?;
        assert!(
            runtime
                .persist_coordination_recipient_receipt_with(
                    initial_params(),
                    &FailOnce::inbox(step)
                )
                .await
                .is_err()
        );
        let state: (i64, i64, i64, String) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM coordination_inbox),\
             (SELECT count(*) FROM coordination_turn_bindings),\
             (SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?),\
             (SELECT lifecycle FROM coordination_assignment_generations LIMIT 1)",
        )
        .bind(super::aggregate_test_support::ROOT)
        .fetch_one(&*runtime.pool)
        .await?;
        assert_eq!(state, (0, 0, 1, "reserved".to_string()), "step {step:?}");
    }
    Ok(())
}

#[tokio::test]
async fn aggregate_and_before_commit_crashes_leave_no_half_receipt() -> anyhow::Result<()> {
    for step in [
        AggregateStep::RevisionAllocation,
        AggregateStep::AggregateMutation,
        AggregateStep::EventInsert,
        AggregateStep::OutboxInsert,
        AggregateStep::BeforeCommit,
    ] {
        let runtime = runtime_with_assignment_command().await?;
        assert!(
            runtime
                .persist_coordination_recipient_receipt_with(
                    initial_params(),
                    &FailOnce::aggregate(step),
                )
                .await
                .is_err()
        );
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM coordination_inbox),\
             (SELECT count(*) FROM coordination_turn_bindings),\
             (SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?)",
        )
        .bind(super::aggregate_test_support::ROOT)
        .fetch_one(&*runtime.pool)
        .await?;
        assert_eq!(counts, (0, 0, 1), "step {step:?}");
    }
    Ok(())
}

#[tokio::test]
async fn every_repeated_acceptance_mutation_failure_rolls_back_the_receipt() -> anyhow::Result<()> {
    for occurrence in 1..=3 {
        let runtime = runtime_with_assignment_command().await?;
        let injector = FailAggregateOccurrence {
            occurrence,
            seen: AtomicUsize::new(0),
            now_ms: chrono::Utc::now().timestamp_millis() + 1,
        };
        assert!(
            runtime
                .persist_coordination_recipient_receipt_with(initial_params(), &injector)
                .await
                .is_err(),
            "aggregate mutation occurrence {occurrence} was not reached"
        );
        assert_eq!(injector.seen.load(Ordering::SeqCst), occurrence);
        let state: (i64, i64, String) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM coordination_inbox),\
             (SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?),\
             (SELECT lifecycle FROM coordination_assignment_generations LIMIT 1)",
        )
        .bind(super::aggregate_test_support::ROOT)
        .fetch_one(&*runtime.pool)
        .await?;
        assert_eq!(state, (0, 1, "reserved".to_string()));
    }
    Ok(())
}

#[tokio::test]
async fn response_loss_after_commit_recovers_as_exact_duplicate() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    assert!(
        runtime
            .persist_coordination_recipient_receipt_with(
                initial_params(),
                &FailOnce::aggregate(AggregateStep::AfterCommit),
            )
            .await
            .is_err()
    );
    let replay = runtime
        .persist_coordination_recipient_receipt(initial_params())
        .await?;
    assert!(matches!(
        replay,
        PersistRecipientReceiptOutcome::Duplicate(_)
    ));
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_inbox),(SELECT count(*) FROM coordination_turn_bindings)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (1, 1));
    Ok(())
}

#[tokio::test]
async fn claim_and_selection_crashes_leave_only_the_last_committed_boundary() -> anyhow::Result<()>
{
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now_ms = metadata.expires_at_ms - 10_000;
    let claim_params = ClaimInboxReceipt {
        receipt_id: metadata.receipt_id,
        claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
        expected_version: 0,
        expected_lease_epoch: 0,
        now_ms,
        lease_expires_at_ms: now_ms + 1_000,
    };
    assert!(
        runtime
            .claim_coordination_receipt_for_inclusion_with(
                claim_params.clone(),
                &FailOnce::inbox(InboxStep::ClaimUpdate),
            )
            .await
            .is_err()
    );
    let after_claim_crash: (String, i64, i64, i64) = sqlx::query_as(
        "SELECT lifecycle,version,claim_count,lease_epoch FROM coordination_inbox WHERE receipt_id=?",
    )
    .bind(RECEIPT_ONE)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(after_claim_crash, ("received".to_string(), 0, 0, 0));

    let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
        .claim_coordination_receipt_for_inclusion(claim_params)
        .await?
    else {
        anyhow::bail!("claim failed")
    };
    let selection = RecordInboxSelection {
        lease: claim.lease,
        inference_attempt_id: inference_attempt("attempt-crash"),
        event_context: None,
        selected_at_ms: now_ms + 1,
    };
    for step in [InboxStep::SelectionInsert, InboxStep::InboxUpdate] {
        assert!(
            runtime
                .record_coordination_inclusion_selection_with(
                    selection.clone(),
                    &FailOnce::inbox(step),
                )
                .await
                .is_err()
        );
        let state: (String, i64, i64) = sqlx::query_as(
            "SELECT lifecycle,version,(SELECT count(*) FROM coordination_inbox_inclusions WHERE receipt_id=?) FROM coordination_inbox WHERE receipt_id=?",
        )
        .bind(RECEIPT_ONE)
        .bind(RECEIPT_ONE)
        .fetch_one(&*runtime.pool)
        .await?;
        assert_eq!(state, ("leased".to_string(), 1, 0), "step {step:?}");
    }
    Ok(())
}

#[tokio::test]
async fn transport_outcome_crashes_preserve_the_committed_selection() -> anyhow::Result<()> {
    for step in [InboxStep::SelectionUpdate, InboxStep::InboxUpdate] {
        let runtime = runtime_with_assignment_command().await?;
        let metadata = persist_initial_receipt(&runtime).await?;
        let now_ms = metadata.expires_at_ms - 10_000;
        let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
            .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
                receipt_id: metadata.receipt_id,
                claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
                expected_version: 0,
                expected_lease_epoch: 0,
                now_ms,
                lease_expires_at_ms: now_ms + 1_000,
            })
            .await?
        else {
            anyhow::bail!("claim failed")
        };
        let RecordInboxSelectionOutcome::Applied(selection) = runtime
            .record_coordination_inclusion_selection(RecordInboxSelection {
                lease: claim.lease,
                inference_attempt_id: inference_attempt("attempt-outcome-crash"),
                event_context: None,
                selected_at_ms: now_ms + 1,
            })
            .await?
        else {
            anyhow::bail!("selection failed")
        };
        assert!(
            runtime
                .record_coordination_inbox_transport_outcome_with(
                    RecordInboxTransportOutcome {
                        selection: selection.token,
                        resolution: InboxTransportResolution::SendSucceeded,
                        completed_at_ms: now_ms + 2,
                    },
                    &FailOnce::inbox(step),
                )
                .await
                .is_err()
        );
        let state: (String, String, i64, i64) = sqlx::query_as(
            "SELECT i.lifecycle,x.transport_state,i.version,x.version FROM coordination_inbox i JOIN coordination_inbox_inclusions x USING(receipt_id) WHERE i.receipt_id=?",
        )
        .bind(RECEIPT_ONE)
        .fetch_one(&*runtime.pool)
        .await?;
        assert_eq!(
            state,
            ("selected".to_string(), "selected".to_string(), 2, 0),
            "step {step:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn post_commit_response_loss_replays_claim_selection_outcome_and_purge() -> anyhow::Result<()>
{
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now_ms = metadata.expires_at_ms - 100_000;
    let claim_params = ClaimInboxReceipt {
        receipt_id: metadata.receipt_id,
        claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
        expected_version: 0,
        expected_lease_epoch: 0,
        now_ms,
        lease_expires_at_ms: now_ms + 10_000,
    };
    assert!(
        runtime
            .claim_coordination_receipt_for_inclusion_with(
                claim_params.clone(),
                &FailOnce::aggregate(AggregateStep::AfterCommit),
            )
            .await
            .is_err()
    );
    let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
        .claim_coordination_receipt_for_inclusion(claim_params)
        .await?
    else {
        anyhow::bail!("committed claim did not replay")
    };

    let selection_params = RecordInboxSelection {
        lease: claim.lease,
        inference_attempt_id: inference_attempt("attempt-response-loss"),
        event_context: None,
        selected_at_ms: now_ms + 1,
    };
    assert!(
        runtime
            .record_coordination_inclusion_selection_with(
                selection_params.clone(),
                &FailOnce::aggregate(AggregateStep::AfterCommit),
            )
            .await
            .is_err()
    );
    let RecordInboxSelectionOutcome::Duplicate(selection) = runtime
        .record_coordination_inclusion_selection(selection_params)
        .await?
    else {
        anyhow::bail!("committed selection did not replay")
    };

    let outcome_params = RecordInboxTransportOutcome {
        selection: selection.token,
        resolution: InboxTransportResolution::SendSucceeded,
        completed_at_ms: now_ms + 2,
    };
    assert!(
        runtime
            .record_coordination_inbox_transport_outcome_with(
                outcome_params.clone(),
                &FailOnce::aggregate(AggregateStep::AfterCommit),
            )
            .await
            .is_err()
    );
    let duplicate = runtime
        .record_coordination_inbox_transport_outcome(outcome_params)
        .await?;
    let crate::model::coordination_inbox::RecordInboxTransportOutcomeResult::Duplicate(terminal) =
        duplicate
    else {
        anyhow::bail!("committed outcome did not replay")
    };

    let purge = InboxMaintenanceBatch {
        now_ms: terminal.expires_at_ms,
        limit: 16,
    };
    assert!(
        runtime
            .expire_coordination_inbox_payloads_with(
                purge.clone(),
                &FailOnce::aggregate(AggregateStep::AfterCommit),
            )
            .await
            .is_err()
    );
    assert!(
        runtime
            .expire_coordination_inbox_payloads(purge)
            .await?
            .changed_receipts
            .is_empty()
    );
    Ok(())
}
