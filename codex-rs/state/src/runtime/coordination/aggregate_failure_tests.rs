use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;

use super::aggregate_failure_support::*;
use super::aggregate_journal::AggregateStep;
use super::aggregate_race_tests::accepted_one_reserved_two;
use super::aggregate_race_tests::terminal_for_generation_one;
use super::aggregate_test_support::*;
use super::aggregates::AssignmentTransitionOutcome;
use super::aggregates::ReserveAssignmentOutcome;
use super::aggregates::WaitTransitionOutcome;
use crate::StateRuntime;
use crate::model::coordination::CloseReservedAssignment;
use crate::runtime::test_support::unique_temp_dir;
use codex_coordination::AssignmentId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::GenerationCloseReason;

#[tokio::test]
async fn reserve_is_atomic_gap_free_and_exactly_idempotent() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let params = reserve_params();

    assert!(
        runtime
            .reserve_coordination_assignment_with(
                params.clone(),
                &FailAt(AggregateStep::OutboxInsert)
            )
            .await
            .is_err()
    );
    let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM coordination_assignment_heads),(SELECT count(*) FROM coordination_events),(SELECT count(*) FROM coordination_projection_outbox)")
        .fetch_one(&*runtime.pool).await?;
    assert_eq!(counts, (0, 0, 0));

    let applied = runtime
        .reserve_coordination_assignment(params.clone())
        .await?;
    let duplicate = runtime.reserve_coordination_assignment(params).await?;
    let (applied_generation, applied_event) = match applied {
        ReserveAssignmentOutcome::Reserved { generation, event } => (generation, event),
        other => panic!("expected reservation, got {other:?}"),
    };
    let (duplicate_generation, duplicate_event) = match duplicate {
        ReserveAssignmentOutcome::Duplicate { generation, event } => (generation, event),
        other => panic!("expected duplicate, got {other:?}"),
    };
    assert_eq!(
        (duplicate_generation, duplicate_event),
        (applied_generation, applied_event)
    );
    let record: (i64, Option<i64>, i64, i64, i64) = sqlx::query_as("SELECT h.next_generation,h.accepted_generation,r.committed_revision,(SELECT count(*) FROM coordination_events),(SELECT count(*) FROM coordination_projection_outbox) FROM coordination_assignment_heads h JOIN coordination_roots r USING(root_thread_id)")
        .fetch_one(&*runtime.pool).await?;
    assert_eq!(record, (2, None, 1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn every_reserve_boundary_rolls_back_and_post_commit_loss_replays() -> anyhow::Result<()> {
    for (step, occurrence) in [
        (AggregateStep::AuthorityRead, 1),
        (AggregateStep::IdempotencyRead, 1),
        (AggregateStep::IdempotencyRead, 2),
        (AggregateStep::AggregateRead, 1),
        (AggregateStep::RootCreate, 1),
        (AggregateStep::AggregateRead, 2),
        (AggregateStep::RevisionAllocation, 1),
        (AggregateStep::AggregateMutation, 1),
        (AggregateStep::AggregateMutation, 2),
        (AggregateStep::EventInsert, 1),
        (AggregateStep::OutboxInsert, 1),
        (AggregateStep::BeforeCommit, 1),
    ] {
        let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
        let injector = FailOccurrence {
            step,
            occurrence,
            seen: AtomicUsize::new(0),
        };
        assert!(
            runtime
                .reserve_coordination_assignment_with(reserve_params(), &injector)
                .await
                .is_err(),
            "{step:?} occurrence {occurrence}"
        );
        let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM coordination_assignment_heads),(SELECT count(*) FROM coordination_events),(SELECT count(*) FROM coordination_projection_outbox)")
            .fetch_one(&*runtime.pool).await?;
        assert_eq!(counts, (0, 0, 0), "{step:?} occurrence {occurrence}");
    }

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let lost = FailOccurrence {
        step: AggregateStep::AfterCommit,
        occurrence: 1,
        seen: AtomicUsize::new(0),
    };
    assert!(
        runtime
            .reserve_coordination_assignment_with(reserve_params(), &lost)
            .await
            .is_err()
    );
    assert!(matches!(
        runtime
            .reserve_coordination_assignment(reserve_params())
            .await?,
        ReserveAssignmentOutcome::Duplicate { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn every_accept_pair_boundary_rolls_back_and_post_commit_loss_replays() -> anyhow::Result<()>
{
    for nth in 1..=64 {
        let (runtime, _, accept) = accepted_one_reserved_two().await?;
        let before = durable_snapshot(&runtime).await?;
        let injector = FailNth::new(nth);
        let result = runtime
            .accept_coordination_assignment_with(accept.clone(), &injector)
            .await;
        let Some(step) = injector.failed_step() else {
            assert!(matches!(
                result?,
                AssignmentTransitionOutcome::Applied { .. }
            ));
            break;
        };
        assert!(result.is_err(), "accept boundary {nth} ({step:?})");
        if step == AggregateStep::AfterCommit {
            assert!(matches!(
                runtime.accept_coordination_assignment(accept).await?,
                AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2
            ));
            break;
        }
        assert_eq!(
            durable_snapshot(&runtime).await?,
            before,
            "accept boundary {nth} ({step:?})"
        );
    }
    let (runtime, _, accept) = accepted_one_reserved_two().await?;
    assert!(
        runtime
            .accept_coordination_assignment_with(
                accept.clone(),
                &FailAt(AggregateStep::AfterCommit)
            )
            .await
            .is_err()
    );
    assert!(
        matches!(runtime.accept_coordination_assignment(accept).await?, AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2)
    );
    Ok(())
}

#[tokio::test]
async fn every_close_boundary_rolls_back_and_post_commit_loss_replays() -> anyhow::Result<()> {
    for nth in 1..=64 {
        let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
        runtime
            .reserve_coordination_assignment(reserve_params())
            .await?;
        let close = CloseReservedAssignment {
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
        };
        let before = durable_snapshot(&runtime).await?;
        let injector = FailNth::new(nth);
        let result = runtime
            .close_reserved_coordination_assignment_with(close.clone(), &injector)
            .await;
        let Some(step) = injector.failed_step() else {
            assert!(matches!(
                result?,
                AssignmentTransitionOutcome::Applied { .. }
            ));
            break;
        };
        assert!(result.is_err(), "close boundary {nth} ({step:?})");
        if step == AggregateStep::AfterCommit {
            assert!(matches!(
                runtime.close_reserved_coordination_assignment(close).await?,
                AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 1
            ));
            break;
        }
        assert_eq!(
            durable_snapshot(&runtime).await?,
            before,
            "close boundary {nth} ({step:?})"
        );
    }
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let close = CloseReservedAssignment {
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
    };
    assert!(
        runtime
            .close_reserved_coordination_assignment_with(
                close.clone(),
                &FailAt(AggregateStep::AfterCommit)
            )
            .await
            .is_err()
    );
    assert!(matches!(
        runtime
            .close_reserved_coordination_assignment(close)
            .await?,
        AssignmentTransitionOutcome::Duplicate { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn every_wait_boundary_rolls_back_and_post_commit_loss_replays() -> anyhow::Result<()> {
    for nth in 1..=64 {
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
        let before = durable_snapshot(&runtime).await?;
        let injector = FailNth::new(nth);
        let result = runtime
            .start_coordination_wait_with(start.clone(), &injector)
            .await;
        let Some(step) = injector.failed_step() else {
            assert!(matches!(result?, WaitTransitionOutcome::Applied { .. }));
            break;
        };
        assert!(result.is_err(), "wait start boundary {nth} ({step:?})");
        if step == AggregateStep::AfterCommit {
            assert!(matches!(
                runtime.start_coordination_wait(start).await?,
                WaitTransitionOutcome::Duplicate { .. }
            ));
            break;
        }
        assert_eq!(durable_snapshot(&runtime).await?, before);
    }

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
    assert!(
        runtime
            .start_coordination_wait_with(start.clone(), &FailAt(AggregateStep::AfterCommit))
            .await
            .is_err()
    );
    assert!(matches!(
        runtime.start_coordination_wait(start).await?,
        WaitTransitionOutcome::Duplicate { .. }
    ));

    for nth in 1..=64 {
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
        let before = durable_snapshot(&runtime).await?;
        let injector = FailNth::new(nth);
        let result = runtime
            .end_coordination_wait_with(end.clone(), &injector)
            .await;
        let Some(step) = injector.failed_step() else {
            assert!(matches!(result?, WaitTransitionOutcome::Applied { .. }));
            break;
        };
        assert!(result.is_err(), "wait end boundary {nth} ({step:?})");
        if step == AggregateStep::AfterCommit {
            assert!(matches!(
                runtime.end_coordination_wait(end).await?,
                WaitTransitionOutcome::Duplicate { .. }
            ));
            break;
        }
        assert_eq!(durable_snapshot(&runtime).await?, before);
    }
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
    assert!(
        runtime
            .end_coordination_wait_with(end.clone(), &FailAt(AggregateStep::AfterCommit))
            .await
            .is_err()
    );
    assert!(matches!(
        runtime.end_coordination_wait(end).await?,
        WaitTransitionOutcome::Duplicate { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn every_variable_terminal_boundary_rolls_back_and_post_commit_loss_replays()
-> anyhow::Result<()> {
    for nth in 1..=96 {
        let (runtime, _, _) = accepted_one_reserved_two().await?;
        let terminal = terminal_for_generation_one(3, 2, true)?;
        let before = durable_snapshot(&runtime).await?;
        let injector = FailNth::new(nth);
        let result = runtime
            .terminal_coordination_assignment_with(terminal.clone(), &injector)
            .await;
        let Some(step) = injector.failed_step() else {
            assert!(matches!(
                result?,
                AssignmentTransitionOutcome::Applied { .. }
            ));
            break;
        };
        assert!(result.is_err(), "terminal boundary {nth} ({step:?})");
        if step == AggregateStep::AfterCommit {
            assert!(matches!(
                runtime.terminal_coordination_assignment(terminal).await?,
                AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2
            ));
            break;
        }
        assert_eq!(
            durable_snapshot(&runtime).await?,
            before,
            "terminal boundary {nth} ({step:?})"
        );
    }
    let (runtime, _, _) = accepted_one_reserved_two().await?;
    let terminal = terminal_for_generation_one(3, 2, true)?;
    assert!(
        runtime
            .terminal_coordination_assignment_with(
                terminal.clone(),
                &FailAt(AggregateStep::AfterCommit)
            )
            .await
            .is_err()
    );
    assert!(
        matches!(runtime.terminal_coordination_assignment(terminal).await?, AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2)
    );
    Ok(())
}
