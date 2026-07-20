use codex_coordination::AssignmentId;
use codex_coordination::BoundedList;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::Evidence;
use codex_coordination::InterruptionReason;
use codex_coordination::ReceiptId;
use codex_coordination::TurnOutcome;
use codex_coordination::WaitOutcome;
use pretty_assertions::assert_eq;

use super::aggregate_failure_support::wait_params;
use super::aggregate_journal::CoordinationWriteError;
use super::aggregate_race_tests::accepted_one_reserved_two;
use super::aggregate_test_support::*;
use super::aggregates::AssignmentTransitionOutcome;
use super::aggregates::ReserveAssignmentOutcome;
use crate::StateRuntime;
use crate::model::coordination::AcceptAssignment;
use crate::model::coordination::AssignmentReservation;
use crate::model::coordination::TerminalAssignment;
use crate::model::coordination::TerminalTurn;
use crate::runtime::test_support::unique_temp_dir;

pub(super) async fn accepted_one() -> anyhow::Result<(std::sync::Arc<StateRuntime>, AssignmentId)> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let assignment_id = AssignmentId::parse(ASSIGNMENT)?;
    runtime
        .accept_coordination_assignment(AcceptAssignment {
            context: context(
                CoordinationSemanticSlot::AssignmentAccepted,
                "019f7c6c-1111-7000-8000-000000000740",
                OPERATION,
                true,
                1,
                Vec::new(),
            ),
            assignment_id,
            generation: generation(1),
            receipt_id: ReceiptId::parse("019f7c6c-1111-7000-8000-000000000240")?,
            bound_turn_id: Evidence::Known {
                value: turn("turn-b"),
            },
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version: 0,
        })
        .await?;
    Ok((runtime, assignment_id))
}

fn followup(
    event_id: &str,
    operation: &str,
    revision: u64,
    version: u64,
) -> anyhow::Result<crate::model::coordination::ReserveAssignment> {
    let mut params = reserve_params();
    params.context = context(
        CoordinationSemanticSlot::AssignmentRequested,
        event_id,
        operation,
        false,
        revision,
        Vec::new(),
    );
    params.operation_id = CoordinationOperationId::parse(operation)?;
    params.reservation = AssignmentReservation::Followup {
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: version,
    };
    Ok(params)
}

#[tokio::test]
async fn concurrent_exact_and_divergent_reserves_are_single_winner() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let params = reserve_params();
    let (left, right) = tokio::join!(
        runtime.reserve_coordination_assignment(params.clone()),
        runtime.reserve_coordination_assignment(params)
    );
    let outcomes = [left?, right?];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ReserveAssignmentOutcome::Reserved { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ReserveAssignmentOutcome::Duplicate { .. }))
            .count(),
        1
    );

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let first = reserve_params();
    let mut divergent = first.clone();
    divergent.assignment_id = AssignmentId::parse("019f7c6c-1111-7000-8000-000000000399")?;
    let (left, right) = tokio::join!(
        runtime.reserve_coordination_assignment(first),
        runtime.reserve_coordination_assignment(divergent)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let error = left.err().or_else(|| right.err()).expect("divergent loser");
    assert!(
        matches!(error, CoordinationWriteError::DivergentIntent),
        "unexpected divergent reserve error: {error:?}"
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM coordination_assignment_generations")
        .fetch_one(&*runtime.pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_followups_fence_loser_then_refreshed_retry_gets_next_generation()
-> anyhow::Result<()> {
    let (runtime, _) = accepted_one().await?;
    let left = followup(
        "019f7c6c-1111-7000-8000-000000000741",
        "019f7c6c-1111-7000-8000-000000000141",
        2,
        1,
    )?;
    let right = followup(
        "019f7c6c-1111-7000-8000-000000000742",
        "019f7c6c-1111-7000-8000-000000000142",
        2,
        1,
    )?;
    let (left_result, right_result) = tokio::join!(
        runtime.reserve_coordination_assignment(left.clone()),
        runtime.reserve_coordination_assignment(right.clone())
    );
    let (winner, loser) = match (left_result, right_result) {
        (
            Ok(ReserveAssignmentOutcome::Reserved {
                generation: winner, ..
            }),
            Err(err),
        ) => (winner, (right, err)),
        (
            Err(err),
            Ok(ReserveAssignmentOutcome::Reserved {
                generation: winner, ..
            }),
        ) => (winner, (left, err)),
        other => panic!("expected one follow-up winner and one fence, got {other:?}"),
    };
    assert_eq!(winner, generation(2));
    assert!(matches!(
        loser.1,
        CoordinationWriteError::VersionFenced | CoordinationWriteError::RevisionFenced
    ));
    let mut retry = loser.0;
    retry.context.expected_root_revision = 3;
    retry.reservation = AssignmentReservation::Followup {
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 2,
    };
    assert!(matches!(
        runtime.reserve_coordination_assignment(retry).await?,
        ReserveAssignmentOutcome::Reserved { generation: next, .. } if next == generation(3)
    ));
    Ok(())
}

#[tokio::test]
async fn accepting_newer_generation_fences_out_of_order_older_accept() -> anyhow::Result<()> {
    let (runtime, assignment_id, mut accept_two) = accepted_one_reserved_two().await?;
    let operation_three = "019f7c6c-1111-7000-8000-000000000143";
    runtime
        .reserve_coordination_assignment(followup(
            "019f7c6c-1111-7000-8000-000000000743",
            operation_three,
            3,
            2,
        )?)
        .await?;
    let accept_three = AcceptAssignment {
        context: context(
            CoordinationSemanticSlot::AssignmentAccepted,
            "019f7c6c-1111-7000-8000-000000000744",
            operation_three,
            true,
            4,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000745",
                "019f7c6c-1111-7000-8000-000000000145",
            )],
        ),
        assignment_id,
        generation: generation(3),
        receipt_id: ReceiptId::parse("019f7c6c-1111-7000-8000-000000000243")?,
        bound_turn_id: Evidence::Known {
            value: turn("turn-b"),
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 3,
    };
    runtime.accept_coordination_assignment(accept_three).await?;
    accept_two.context.expected_root_revision = 6;
    accept_two.expected_head_version = 4;
    assert!(matches!(
        runtime.accept_coordination_assignment(accept_two).await?,
        AssignmentTransitionOutcome::Fenced { current_generation } if current_generation == generation(3)
    ));
    Ok(())
}

pub(super) fn terminal(
    slot: CoordinationSemanticSlot,
    event: &str,
    operation: &str,
    interrupted: bool,
) -> anyhow::Result<TerminalAssignment> {
    let context = context(
        slot,
        event,
        operation,
        true,
        2,
        vec![(
            CoordinationSemanticSlot::AssignmentGenerationClosed,
            if interrupted {
                "019f7c6c-1111-7000-8000-000000000748"
            } else {
                "019f7c6c-1111-7000-8000-000000000747"
            },
            if interrupted {
                "019f7c6c-1111-7000-8000-000000000148"
            } else {
                "019f7c6c-1111-7000-8000-000000000147"
            },
        )],
    );
    let included = BoundedList::new(vec![generation(1)], /*omitted_count*/ 0)?;
    Ok(TerminalAssignment {
        context,
        terminal: if interrupted {
            TerminalTurn::Interrupted {
                target: target(1),
                target_turn_id: turn("turn-b"),
                interruption_reason: InterruptionReason::Shutdown,
                included_generations: included,
            }
        } else {
            TerminalTurn::Completed {
                target: target(1),
                target_turn_id: turn("turn-b"),
                outcome: TurnOutcome::Succeeded,
                included_generations: included,
            }
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 1,
    })
}

#[tokio::test]
async fn concurrent_complete_and_interrupt_are_first_wins() -> anyhow::Result<()> {
    let (runtime, _) = accepted_one().await?;
    let completed = terminal(
        CoordinationSemanticSlot::TurnCompleted,
        "019f7c6c-1111-7000-8000-000000000746",
        "019f7c6c-1111-7000-8000-000000000146",
        false,
    )?;
    let interrupted = terminal(
        CoordinationSemanticSlot::TurnInterrupted,
        "019f7c6c-1111-7000-8000-000000000749",
        "019f7c6c-1111-7000-8000-000000000149",
        true,
    )?;
    let (left, right) = tokio::join!(
        runtime.terminal_coordination_assignment(completed),
        runtime.terminal_coordination_assignment(interrupted)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(matches!(
        left.err().or_else(|| right.err()),
        Some(CoordinationWriteError::TerminalConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn concurrent_divergent_wait_ends_are_first_wins() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let (start, left) = wait_params(
        CoordinationSemanticSlot::WaitStarted,
        "019f7c6c-1111-7000-8000-000000000750",
        "019f7c6c-1111-7000-8000-000000000150",
        1,
    )?;
    runtime.start_coordination_wait(start).await?;
    let mut right = left.clone();
    right.context.primary.event_id =
        codex_coordination::CoordinationEventId::parse("019f7c6c-1111-7000-8000-000000000751")?;
    right.outcome = Evidence::Known {
        value: WaitOutcome::TimedOut,
    };
    let (left, right) = tokio::join!(
        runtime.end_coordination_wait(left),
        runtime.end_coordination_wait(right)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(matches!(
        left.err().or_else(|| right.err()),
        Some(CoordinationWriteError::DivergentIntent)
    ));
    Ok(())
}

#[tokio::test]
async fn quarantine_and_write_are_linearized_and_future_writes_fail() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let quarantine = sqlx::query("UPDATE coordination_authority SET status='quarantined',quarantine_reason='race test',updated_at_ms=4000000000000 WHERE singleton_id=1")
        .execute(&*runtime.pool);
    let write = runtime.reserve_coordination_assignment(reserve_params());
    let (quarantine, write) = tokio::join!(quarantine, write);
    quarantine?;
    assert!(matches!(
        write,
        Ok(ReserveAssignmentOutcome::Reserved { .. }) | Err(CoordinationWriteError::Quarantined)
    ));
    let mut later = reserve_params();
    later.context.primary.event_id =
        codex_coordination::CoordinationEventId::parse("019f7c6c-1111-7000-8000-000000000752")?;
    later.context.primary.operation_id =
        CoordinationOperationId::parse("019f7c6c-1111-7000-8000-000000000152")?;
    later.operation_id = later.context.primary.operation_id;
    assert!(matches!(
        runtime.reserve_coordination_assignment(later).await,
        Err(CoordinationWriteError::Quarantined)
    ));
    Ok(())
}
