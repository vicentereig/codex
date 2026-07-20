use codex_coordination::BoundedList;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::InterruptionReason;
use codex_coordination::TurnOutcome;
use pretty_assertions::assert_eq;

use super::aggregate_journal::CoordinationWriteError;
use super::aggregate_test_support::*;
use super::aggregates::AssignmentTransitionOutcome;
use super::inbox_test_support::*;
use crate::model::coordination::TerminalAssignment;
use crate::model::coordination::TerminalTurn;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_inbox::RecordInboxSelectionOutcome;

#[tokio::test]
async fn accepted_turn_binding_is_not_terminal_inclusion_evidence() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let terminal = TerminalAssignment {
        context: context(
            CoordinationSemanticSlot::TurnCompleted,
            "019f7c6c-1111-7000-8000-000000000730",
            "019f7c6c-1111-7000-8000-000000000130",
            true,
            2,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000731",
                "019f7c6c-1111-7000-8000-000000000131",
            )],
        ),
        terminal: TerminalTurn::Completed {
            target: target(1),
            target_turn_id: turn("turn-b"),
            outcome: TurnOutcome::Succeeded,
            included_generations: BoundedList::new(vec![generation(1)], /*omitted_count*/ 0)?,
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 1,
    };
    assert!(matches!(
        runtime
            .terminal_coordination_assignment(terminal.clone())
            .await,
        Err(CoordinationWriteError::GenerationFenced)
    ));

    let now = metadata.expires_at_ms - 100;
    let claimed = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: now,
            lease_expires_at_ms: metadata.expires_at_ms - 1,
        })
        .await?;
    let ClaimInboxReceiptOutcome::Claimed(claimed) = claimed else {
        anyhow::bail!("claim failed")
    };
    assert!(matches!(
        runtime
            .record_coordination_inclusion_selection(RecordInboxSelection {
                lease: claimed.lease,
                inference_attempt_id: inference_attempt("attempt-a"),
                event_context: None,
                selected_at_ms: now + 1,
            })
            .await?,
        RecordInboxSelectionOutcome::Applied(_)
    ));
    assert!(matches!(
        runtime
            .terminal_coordination_assignment(terminal.clone())
            .await?,
        AssignmentTransitionOutcome::Applied { ref events } if events.len() == 2
    ));
    assert!(matches!(
        runtime.terminal_coordination_assignment(terminal).await?,
        AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2
    ));
    Ok(())
}

#[tokio::test]
async fn requested_interrupt_terminal_causes_resolves_and_replays_receipt() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_inclusion().await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(interrupt_command(2))
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    let interrupt = runtime
        .persist_coordination_recipient_receipt(receipt_params(
            INTERRUPT_OPERATION,
            RECEIPT_TWO,
            "019f7c6c-1111-7000-8000-000000000706",
            3,
            1,
            Vec::new(),
        ))
        .await?;
    let crate::model::coordination_inbox::PersistRecipientReceiptOutcome::Applied(interrupt) =
        interrupt
    else {
        anyhow::bail!("interrupt receipt failed")
    };
    let terminal = TerminalAssignment {
        context: context(
            CoordinationSemanticSlot::TurnInterrupted,
            "019f7c6c-1111-7000-8000-000000000707",
            "019f7c6c-1111-7000-8000-000000000107",
            true,
            4,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000708",
                "019f7c6c-1111-7000-8000-000000000108",
            )],
        ),
        terminal: TerminalTurn::Interrupted {
            target: target(1),
            target_turn_id: turn("turn-b"),
            interruption_reason: InterruptionReason::Requested {
                operation_id: CoordinationOperationId::parse(INTERRUPT_OPERATION)?,
            },
            included_generations: BoundedList::new(vec![generation(1)], /*omitted_count*/ 0)?,
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 1,
    };
    let AssignmentTransitionOutcome::Applied { events } = runtime
        .terminal_coordination_assignment(terminal.clone())
        .await?
    else {
        anyhow::bail!("requested terminal was not applied")
    };
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].envelope().causes.items(),
        &[interrupt.receipt_event_id]
    );
    let row: (String, Option<String>, i64) = sqlx::query_as(
        "SELECT lifecycle,resolution_event_id,version FROM coordination_inbox WHERE receipt_id=?",
    )
    .bind(RECEIPT_TWO)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(
        row,
        (
            "processed".to_string(),
            Some(events[0].envelope().event_id.to_string()),
            1
        )
    );
    assert!(matches!(
        runtime.terminal_coordination_assignment(terminal).await?,
        AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2
    ));
    Ok(())
}

#[tokio::test]
async fn terminal_first_race_defers_late_interrupt_receipt_without_barrier() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_inclusion().await?;
    runtime
        .record_coordination_command_intent(interrupt_command(2))
        .await?;
    let terminal = TerminalAssignment {
        context: context(
            CoordinationSemanticSlot::TurnCompleted,
            "019f7c6c-1111-7000-8000-000000000707",
            "019f7c6c-1111-7000-8000-000000000107",
            true,
            3,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000708",
                "019f7c6c-1111-7000-8000-000000000108",
            )],
        ),
        terminal: TerminalTurn::Completed {
            target: target(1),
            target_turn_id: turn("turn-b"),
            outcome: TurnOutcome::Succeeded,
            included_generations: BoundedList::new(vec![generation(1)], /*omitted_count*/ 0)?,
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 1,
    };
    assert!(matches!(
        runtime.terminal_coordination_assignment(terminal).await?,
        AssignmentTransitionOutcome::Applied { .. }
    ));
    assert_eq!(
        runtime
            .persist_coordination_recipient_receipt(receipt_params(
                INTERRUPT_OPERATION,
                RECEIPT_TWO,
                "019f7c6c-1111-7000-8000-000000000706",
                5,
                1,
                Vec::new(),
            ))
            .await?,
        PersistRecipientReceiptOutcome::Deferred
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM coordination_inbox WHERE operation_kind='interrupt'",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(count, 0);
    Ok(())
}
