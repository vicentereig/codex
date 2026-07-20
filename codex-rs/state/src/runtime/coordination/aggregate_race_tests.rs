use codex_coordination::AssignmentId;
use codex_coordination::BoundedList;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::Evidence;
use codex_coordination::ReceiptId;
use codex_coordination::TurnOutcome;
use pretty_assertions::assert_eq;

use super::aggregate_test_support::*;
use super::aggregates::AssignmentTransitionOutcome;
use crate::StateRuntime;
use crate::model::coordination::AcceptAssignment;
use crate::model::coordination::AssignmentReservation;
use crate::model::coordination::TerminalAssignment;
use crate::model::coordination::TerminalTurn;
use crate::runtime::test_support::unique_temp_dir;

pub(super) async fn accepted_one_reserved_two()
-> anyhow::Result<(std::sync::Arc<StateRuntime>, AssignmentId, AcceptAssignment)> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let assignment_id = AssignmentId::parse(ASSIGNMENT)?;
    runtime
        .accept_coordination_assignment(AcceptAssignment {
            context: context(
                CoordinationSemanticSlot::AssignmentAccepted,
                "019f7c6c-1111-7000-8000-000000000720",
                OPERATION,
                true,
                1,
                Vec::new(),
            ),
            assignment_id,
            generation: generation(1),
            receipt_id: ReceiptId::parse("019f7c6c-1111-7000-8000-000000000220")?,
            bound_turn_id: Evidence::Known {
                value: turn("turn-b"),
            },
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version: 0,
        })
        .await?;
    let follow_operation = "019f7c6c-1111-7000-8000-000000000120";
    let mut followup = reserve_params();
    followup.context = context(
        CoordinationSemanticSlot::AssignmentRequested,
        "019f7c6c-1111-7000-8000-000000000721",
        follow_operation,
        false,
        2,
        Vec::new(),
    );
    followup.operation_id = CoordinationOperationId::parse(follow_operation)?;
    followup.reservation = AssignmentReservation::Followup {
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 1,
    };
    runtime.reserve_coordination_assignment(followup).await?;
    let accept_two = AcceptAssignment {
        context: context(
            CoordinationSemanticSlot::AssignmentAccepted,
            "019f7c6c-1111-7000-8000-000000000722",
            follow_operation,
            true,
            3,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000723",
                "019f7c6c-1111-7000-8000-000000000121",
            )],
        ),
        assignment_id,
        generation: generation(2),
        receipt_id: ReceiptId::parse("019f7c6c-1111-7000-8000-000000000221")?,
        bound_turn_id: Evidence::Known {
            value: turn("turn-b"),
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 2,
    };
    Ok((runtime, assignment_id, accept_two))
}

pub(super) fn terminal_for_generation_one(
    expected_root_revision: u64,
    expected_head_version: u64,
    with_close_identity: bool,
) -> anyhow::Result<TerminalAssignment> {
    let secondary = with_close_identity.then_some((
        CoordinationSemanticSlot::AssignmentGenerationClosed,
        "019f7c6c-1111-7000-8000-000000000725",
        "019f7c6c-1111-7000-8000-000000000123",
    ));
    Ok(TerminalAssignment {
        context: context(
            CoordinationSemanticSlot::TurnCompleted,
            "019f7c6c-1111-7000-8000-000000000724",
            "019f7c6c-1111-7000-8000-000000000122",
            true,
            expected_root_revision,
            secondary.into_iter().collect(),
        ),
        terminal: TerminalTurn::Completed {
            target: target(1),
            target_turn_id: turn("turn-b"),
            outcome: TurnOutcome::Succeeded,
            included_generations: BoundedList::new(
                vec![generation(1)],
                /*omitted_count*/ 0,
            )?,
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version,
    })
}

#[tokio::test]
async fn terminal_and_accept_orders_converge_without_double_closing() -> anyhow::Result<()> {
    let (terminal_first, assignment_id, mut accept_after) = accepted_one_reserved_two().await?;
    let terminal = terminal_for_generation_one(3, 2, true)?;
    assert!(matches!(
        terminal_first
            .terminal_coordination_assignment(terminal)
            .await?,
        AssignmentTransitionOutcome::Applied { ref events } if events.len() == 2
    ));
    accept_after.context.expected_root_revision = 5;
    accept_after.expected_head_version = 3;
    terminal_first
        .accept_coordination_assignment(accept_after.clone())
        .await?;
    assert!(matches!(
        terminal_first
            .accept_coordination_assignment(accept_after)
            .await?,
        AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 1
    ));
    let terminal_first_record = terminal_first
        .coordination_assignment_aggregate(assignment_id)
        .await?
        .expect("aggregate");

    let (accept_first, assignment_id, accept_two) = accepted_one_reserved_two().await?;
    accept_first
        .accept_coordination_assignment(accept_two)
        .await?;
    let terminal_after = terminal_for_generation_one(5, 3, true)?;
    assert!(matches!(
        accept_first
            .terminal_coordination_assignment(terminal_after.clone())
            .await?,
        AssignmentTransitionOutcome::Applied { ref events } if events.len() == 1
    ));
    assert!(matches!(
        accept_first
            .terminal_coordination_assignment(terminal_after)
            .await?,
        AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 1
    ));
    let accept_first_record = accept_first
        .coordination_assignment_aggregate(assignment_id)
        .await?
        .expect("aggregate");

    assert_eq!(
        terminal_first_record
            .generations
            .iter()
            .map(|generation| generation.lifecycle)
            .collect::<Vec<_>>(),
        accept_first_record
            .generations
            .iter()
            .map(|generation| generation.lifecycle)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        terminal_first_record.generations[0].lifecycle,
        crate::model::coordination::GenerationLifecycle::Superseded
    );
    assert_eq!(
        terminal_first_record.generations[1].lifecycle,
        crate::model::coordination::GenerationLifecycle::Accepted
    );
    let terminal_counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_turn_terminals),(SELECT count(*) FROM coordination_assignment_generations WHERE lifecycle='superseded')",
    )
    .fetch_one(&*terminal_first.pool)
    .await?;
    let accept_counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_turn_terminals),(SELECT count(*) FROM coordination_assignment_generations WHERE lifecycle='superseded')",
    )
    .fetch_one(&*accept_first.pool)
    .await?;
    assert_eq!(terminal_counts, (1, 1));
    assert_eq!(accept_counts, terminal_counts);
    Ok(())
}
