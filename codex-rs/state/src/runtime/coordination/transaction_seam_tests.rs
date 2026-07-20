use codex_coordination::AssignmentId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::GenerationCloseReason;
use pretty_assertions::assert_eq;

use super::aggregate_failure_support::wait_params;
use super::aggregate_journal::AggregateStep;
use super::aggregate_race_tests::accepted_one_reserved_two;
use super::aggregate_race_tests::terminal_for_generation_one;
use super::aggregate_test_support::*;
use super::aggregates::AssignmentTransitionOutcome;
use super::aggregates::ReserveAssignmentOutcome;
use super::aggregates::WaitTransitionOutcome;
use super::commands::CommandStep;
use super::commands_tests::assignment_command;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashInjector;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_support::FrozenCoordinationState;
use super::failure_injection_support::FrozenStateInputs;
use super::failure_injection_support::assert_integrity;
use super::failure_injection_support::frozen_state;
use super::failure_injection_tests::delivery_now;
use super::failure_injection_tests::receipt_params_for_matrix;
use super::failure_injection_tests::runtime_with_command_at;
use super::inbox::InboxStep;
use crate::StateRuntime;
use crate::model::coordination::CloseReservedAssignment;
use crate::runtime::test_support::unique_temp_dir;

const NOW_MS: i64 = 4_000_000_000_000;

#[tokio::test]
async fn all_six_aggregate_entry_points_share_transaction_ownership() -> anyhow::Result<()> {
    let mut counts = Vec::new();

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let recorder = CrashInjector::recording(NOW_MS);
    assert!(matches!(
        runtime
            .reserve_coordination_assignment_with(reserve_params(), &recorder)
            .await?,
        ReserveAssignmentOutcome::Reserved { .. }
    ));
    counts.push(aggregate_trace_count(&recorder));

    let (runtime, _, accept) = accepted_one_reserved_two().await?;
    let recorder = CrashInjector::recording(NOW_MS);
    assert!(matches!(
        runtime
            .accept_coordination_assignment_with(accept, &recorder)
            .await?,
        AssignmentTransitionOutcome::Applied { .. }
    ));
    counts.push(aggregate_trace_count(&recorder));

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let recorder = CrashInjector::recording(NOW_MS);
    assert!(matches!(
        runtime
            .close_reserved_coordination_assignment_with(close_params()?, &recorder)
            .await?,
        AssignmentTransitionOutcome::Applied { .. }
    ));
    counts.push(aggregate_trace_count(&recorder));

    let (runtime, _, _) = accepted_one_reserved_two().await?;
    let recorder = CrashInjector::recording(NOW_MS);
    assert!(matches!(
        runtime
            .terminal_coordination_assignment_with(
                terminal_for_generation_one(3, 2, true)?,
                &recorder,
            )
            .await?,
        AssignmentTransitionOutcome::Applied { .. }
    ));
    counts.push(aggregate_trace_count(&recorder));

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let (start, _) = wait_params(
        CoordinationSemanticSlot::WaitStarted,
        "019f7c6c-1111-7000-8000-000000000734",
        "019f7c6c-1111-7000-8000-000000000134",
        1,
    )?;
    let recorder = CrashInjector::recording(NOW_MS);
    assert!(matches!(
        runtime
            .start_coordination_wait_with(start, &recorder)
            .await?,
        WaitTransitionOutcome::Applied { .. }
    ));
    counts.push(aggregate_trace_count(&recorder));

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let (start, end) = wait_params(
        CoordinationSemanticSlot::WaitStarted,
        "019f7c6c-1111-7000-8000-000000000734",
        "019f7c6c-1111-7000-8000-000000000134",
        1,
    )?;
    runtime.start_coordination_wait(start).await?;
    let recorder = CrashInjector::recording(NOW_MS);
    assert!(matches!(
        runtime.end_coordination_wait_with(end, &recorder).await?,
        WaitTransitionOutcome::Applied { .. }
    ));
    counts.push(aggregate_trace_count(&recorder));

    assert_eq!(counts, vec![15, 24, 16, 27, 13, 15]);
    Ok(())
}

#[tokio::test]
async fn begin_failures_execute_then_trace_subsystem_rollback() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
    let before = snapshot(&runtime).await?;
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Aggregate(AggregateStep::TransactionBegin),
            occurrence: 1,
        },
        NOW_MS,
    );
    assert!(
        runtime
            .reserve_coordination_assignment_with(reserve_params(), &injector)
            .await
            .is_err()
    );
    assert_eq!(
        boundaries(&injector),
        vec![
            Boundary::Aggregate(AggregateStep::TransactionBegin),
            Boundary::Aggregate(AggregateStep::Rollback),
        ]
    );
    drop(runtime);
    let runtime = StateRuntime::init(home, "test".to_string()).await?;
    assert_eq!(snapshot(&runtime).await?, before);
    assert!(matches!(
        runtime
            .reserve_coordination_assignment(reserve_params())
            .await?,
        ReserveAssignmentOutcome::Reserved { .. }
    ));
    assert_integrity(&runtime).await?;

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Command(CommandStep::TransactionBegin),
            occurrence: 1,
        },
        NOW_MS,
    );
    assert!(
        runtime
            .record_coordination_command_intent_with(assignment_command(), &injector)
            .await
            .is_err()
    );
    assert_eq!(
        boundaries(&injector),
        vec![
            Boundary::Command(CommandStep::TransactionBegin),
            Boundary::Command(CommandStep::Rollback),
        ]
    );

    let runtime = runtime_with_command_at(unique_temp_dir()).await?;
    let now_ms = delivery_now(&runtime).await?;
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Inbox(InboxStep::TransactionBegin),
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
    assert_eq!(
        boundaries(&injector),
        vec![
            Boundary::Inbox(InboxStep::TransactionBegin),
            Boundary::Inbox(InboxStep::Rollback),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn finish_failures_trace_owned_rollback_but_response_loss_does_not() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Aggregate(AggregateStep::BeforeCommit),
            occurrence: 1,
        },
        NOW_MS,
    );
    assert!(
        runtime
            .reserve_coordination_assignment_with(reserve_params(), &injector)
            .await
            .is_err()
    );
    assert_eq!(
        boundaries(&injector)
            .iter()
            .rev()
            .take(2)
            .copied()
            .collect::<Vec<_>>(),
        vec![
            Boundary::Aggregate(AggregateStep::Rollback),
            Boundary::Aggregate(AggregateStep::BeforeCommit),
        ]
    );

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Command(CommandStep::CommandInsert),
            occurrence: 1,
        },
        NOW_MS,
    );
    assert!(
        runtime
            .record_coordination_command_intent_with(assignment_command(), &injector)
            .await
            .is_err()
    );
    assert_eq!(
        boundaries(&injector)
            .iter()
            .rev()
            .take(2)
            .copied()
            .collect::<Vec<_>>(),
        vec![
            Boundary::Command(CommandStep::Rollback),
            Boundary::Command(CommandStep::CommandInsert),
        ]
    );

    let runtime = runtime_with_command_at(unique_temp_dir()).await?;
    let now_ms = delivery_now(&runtime).await?;
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Inbox(InboxStep::ReceiptInsert),
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
    assert_eq!(
        boundaries(&injector)
            .iter()
            .rev()
            .take(2)
            .copied()
            .collect::<Vec<_>>(),
        vec![
            Boundary::Inbox(InboxStep::Rollback),
            Boundary::Inbox(InboxStep::ReceiptInsert),
        ]
    );

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Aggregate(AggregateStep::AfterCommit),
            occurrence: 1,
        },
        NOW_MS,
    );
    assert!(
        runtime
            .reserve_coordination_assignment_with(reserve_params(), &injector)
            .await
            .is_err()
    );
    assert_no_rollback_after_commit(&injector, Boundary::Aggregate(AggregateStep::Rollback));

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let injector = CrashInjector::fail_at(
        CrashPoint {
            boundary: Boundary::Aggregate(AggregateStep::AfterCommit),
            occurrence: 1,
        },
        NOW_MS,
    );
    assert!(
        runtime
            .record_coordination_command_intent_with(assignment_command(), &injector)
            .await
            .is_err()
    );
    assert_no_rollback_after_commit(&injector, Boundary::Command(CommandStep::Rollback));

    let runtime = runtime_with_command_at(unique_temp_dir()).await?;
    let now_ms = delivery_now(&runtime).await?;
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
    assert_no_rollback_after_commit(&injector, Boundary::Inbox(InboxStep::Rollback));
    Ok(())
}

fn aggregate_trace_count(injector: &CrashInjector) -> usize {
    let trace = boundaries(injector);
    assert_eq!(
        trace.first(),
        Some(&Boundary::Aggregate(AggregateStep::TransactionBegin))
    );
    assert_eq!(
        trace.last(),
        Some(&Boundary::Aggregate(AggregateStep::AfterCommit))
    );
    assert_eq!(
        trace
            .iter()
            .filter(|step| **step == Boundary::Aggregate(AggregateStep::TransactionBegin))
            .count(),
        1
    );
    assert!(!trace.contains(&Boundary::Aggregate(AggregateStep::Rollback)));
    trace.len()
}

fn boundaries(injector: &CrashInjector) -> Vec<Boundary> {
    injector
        .trace()
        .into_iter()
        .map(|point| point.boundary)
        .collect()
}

async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}

fn assert_no_rollback_after_commit(injector: &CrashInjector, rollback: Boundary) {
    let trace = boundaries(injector);
    assert_eq!(
        trace.last(),
        Some(&Boundary::Aggregate(AggregateStep::AfterCommit))
    );
    assert!(!trace.contains(&rollback));
}

fn close_params() -> anyhow::Result<CloseReservedAssignment> {
    Ok(CloseReservedAssignment {
        context: context(
            CoordinationSemanticSlot::AssignmentGenerationClosed,
            "019f7c6c-1111-7000-8000-000000000731",
            "019f7c6c-1111-7000-8000-000000000131",
            false,
            1,
            Vec::new(),
        ),
        assignment_id: AssignmentId::parse(ASSIGNMENT)?,
        generation: generation(1),
        reason: GenerationCloseReason::AbandonedBeforeAcceptance,
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 0,
    })
}
