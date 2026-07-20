use codex_coordination::AssignmentId;
use codex_coordination::BoundedList;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::Evidence;
use codex_coordination::InterruptionReason;
use codex_coordination::ReceiptId;
use codex_coordination::TurnOutcome;
use codex_coordination::WaitOutcome;
use codex_coordination::WaitTarget;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::aggregate_journal::CoordinationWriteError;
use super::aggregate_test_support::*;
use super::aggregates::AssignmentTransitionOutcome;
use super::aggregates::ReserveAssignmentOutcome;
use super::inbox_test_support::FOLLOWUP_OPERATION;
use super::inbox_test_support::RECEIPT_TWO;
use super::inbox_test_support::followup_command;
use super::inbox_test_support::persist_assignment_inclusion;
use super::inbox_test_support::persist_initial_assignment_inclusion;
use super::inbox_test_support::receipt_params;
use super::inbox_test_support::runtime_with_assignment_command;
use crate::StateRuntime;
use crate::model::coordination::AcceptAssignment;
use crate::model::coordination::AssignmentReservation;
use crate::model::coordination::EndCoordinationWait;
use crate::model::coordination::StartCoordinationWait;
use crate::model::coordination::TerminalAssignment;
use crate::model::coordination::TerminalTurn;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn terminal_rejects_reserved_unbound_target_generation() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let terminal = TerminalAssignment {
        context: context(
            CoordinationSemanticSlot::TurnCompleted,
            "019f7c6c-1111-7000-8000-000000000724",
            "019f7c6c-1111-7000-8000-000000000122",
            true,
            1,
            Vec::new(),
        ),
        terminal: TerminalTurn::Completed {
            target: target(1),
            target_turn_id: turn("turn-b"),
            outcome: TurnOutcome::Succeeded,
            included_generations: BoundedList::new(vec![generation(1)], /*omitted_count*/ 0)?,
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 0,
    };
    assert!(matches!(
        runtime.terminal_coordination_assignment(terminal).await,
        Err(CoordinationWriteError::GenerationFenced)
    ));
    Ok(())
}

#[tokio::test]
async fn interrupted_terminal_accepts_zero_cause_variants_but_requires_requested_receipt()
-> anyhow::Result<()> {
    let interrupted = |reason, event, operation| -> anyhow::Result<TerminalAssignment> {
        Ok(TerminalAssignment {
            context: context(
                CoordinationSemanticSlot::TurnInterrupted,
                event,
                operation,
                true,
                3,
                vec![(
                    CoordinationSemanticSlot::AssignmentGenerationClosed,
                    "019f7c6c-1111-7000-8000-000000000739",
                    "019f7c6c-1111-7000-8000-000000000139",
                )],
            ),
            terminal: TerminalTurn::Interrupted {
                target: target(1),
                target_turn_id: turn("turn-b"),
                interruption_reason: reason,
                included_generations: BoundedList::new(
                    vec![generation(1)],
                    /*omitted_count*/ 0,
                )?,
            },
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version: 2,
        })
    };

    let (runtime, _, _) = super::aggregate_race_tests::accepted_one_reserved_two().await?;
    assert!(matches!(
        runtime
            .terminal_coordination_assignment(interrupted(
                InterruptionReason::Shutdown,
                "019f7c6c-1111-7000-8000-000000000738",
                "019f7c6c-1111-7000-8000-000000000138",
            )?)
            .await?,
        AssignmentTransitionOutcome::Applied { ref events } if events.len() == 2
    ));

    let (runtime, _, _) = super::aggregate_race_tests::accepted_one_reserved_two().await?;
    assert!(matches!(
        runtime
            .terminal_coordination_assignment(interrupted(
                InterruptionReason::Requested {
                    operation_id: CoordinationOperationId::parse(
                        "019f7c6c-1111-7000-8000-000000000137",
                    )?,
                },
                "019f7c6c-1111-7000-8000-000000000738",
                "019f7c6c-1111-7000-8000-000000000138",
            )?)
            .await,
        Err(CoordinationWriteError::GenerationFenced)
    ));
    Ok(())
}

#[tokio::test]
async fn accepted_followup_wait_and_terminal_bundles_preserve_closed_generations()
-> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let assignment_id = AssignmentId::parse(ASSIGNMENT)?;
    let receipt_one = ReceiptId::parse("019f7c6c-1111-7000-8000-000000000201")?;
    let accept_one = AcceptAssignment {
        context: context(
            CoordinationSemanticSlot::AssignmentAccepted,
            "019f7c6c-1111-7000-8000-000000000702",
            OPERATION,
            true,
            1,
            Vec::new(),
        ),
        assignment_id,
        generation: generation(1),
        receipt_id: receipt_one,
        bound_turn_id: Evidence::Known {
            value: turn("turn-b"),
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 0,
    };
    persist_initial_assignment_inclusion(&runtime).await?;
    assert!(matches!(
        runtime
            .accept_coordination_assignment(accept_one.clone())
            .await?,
        AssignmentTransitionOutcome::Duplicate { .. }
    ));
    assert!(matches!(
        runtime.accept_coordination_assignment(accept_one).await?,
        AssignmentTransitionOutcome::Duplicate { .. }
    ));

    let follow_operation = FOLLOWUP_OPERATION;
    let generation_two = match runtime
        .record_coordination_command_intent(followup_command(2))
        .await?
    {
        crate::model::coordination_commands::RecordCoordinationCommandOutcome::Applied(
            metadata,
        ) => metadata.target.generation,
        other => panic!("expected follow-up command, got {other:?}"),
    };
    assert_eq!(generation_two, generation(2));
    assert_eq!(generation_two, generation(2));
    let accept_two = AcceptAssignment {
        context: context(
            CoordinationSemanticSlot::AssignmentAccepted,
            "019f7c6c-1111-7000-8000-000000000704",
            follow_operation,
            true,
            3,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000705",
                "019f7c6c-1111-7000-8000-000000000106",
            )],
        ),
        assignment_id,
        generation: generation_two,
        receipt_id: ReceiptId::parse("019f7c6c-1111-7000-8000-000000000202")?,
        bound_turn_id: Evidence::Known {
            value: turn("turn-b"),
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 2,
    };
    persist_assignment_inclusion(
        &runtime,
        receipt_params(
            FOLLOWUP_OPERATION,
            RECEIPT_TWO,
            "019f7c6c-1111-7000-8000-000000000704",
            3,
            2,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000705",
                "019f7c6c-1111-7000-8000-000000000106",
            )],
        ),
        "assignment-attempt-g2",
    )
    .await?;
    let accepted = runtime
        .accept_coordination_assignment(accept_two.clone())
        .await?;
    assert!(
        matches!(accepted, AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2)
    );
    let mut retry_without_close_identity = accept_two.clone();
    retry_without_close_identity.context.secondary =
        BoundedList::new(Vec::new(), /*omitted_count*/ 0)?;
    assert!(matches!(
        runtime
            .accept_coordination_assignment(retry_without_close_identity)
            .await?,
        AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2
    ));
    let mut retry_with_divergent_close_identity = accept_two;
    retry_with_divergent_close_identity.context.secondary = BoundedList::new(
        vec![crate::model::coordination::NativeEventIdentity {
            event_id: codex_coordination::CoordinationEventId::parse(
                "019f7c6c-1111-7000-8000-000000000715",
            )?,
            operation_id: CoordinationOperationId::parse("019f7c6c-1111-7000-8000-000000000116")?,
        }],
        /*omitted_count*/ 0,
    )?;
    assert!(matches!(
        runtime
            .accept_coordination_assignment(retry_with_divergent_close_identity)
            .await,
        Err(CoordinationWriteError::DivergentIntent)
    ));
    assert_eq!(
        runtime
            .coordination_bound_generations(thread(ROOT), &turn("turn-b"), assignment_id)
            .await?,
        vec![generation(1), generation(2)]
    );

    let wait_operation = "019f7c6c-1111-7000-8000-000000000103";
    let wait_target: WaitTarget = serde_json::from_value(json!({
        "target": target(2),
        "observedState": {"status":"known","value":"active"}
    }))?;
    let wait_targets = BoundedList::new(vec![wait_target], /*omitted_count*/ 0)?;
    let start = StartCoordinationWait {
        context: context(
            CoordinationSemanticSlot::WaitStarted,
            "019f7c6c-1111-7000-8000-000000000706",
            wait_operation,
            false,
            5,
            Vec::new(),
        ),
        operation_id: CoordinationOperationId::parse(wait_operation)?,
        targets: wait_targets.clone(),
        timeout_ms: 30_000,
    };
    runtime.start_coordination_wait(start).await?;
    let end = EndCoordinationWait {
        context: context(
            CoordinationSemanticSlot::WaitEnded,
            "019f7c6c-1111-7000-8000-000000000707",
            wait_operation,
            false,
            6,
            Vec::new(),
        ),
        operation_id: CoordinationOperationId::parse(wait_operation)?,
        targets: wait_targets,
        outcome: Evidence::Known {
            value: WaitOutcome::TargetTerminal,
        },
        failure: Evidence::NotApplicable,
        expected_wait_version: 0,
    };
    runtime.end_coordination_wait(end.clone()).await?;
    runtime.end_coordination_wait(end).await?;

    let terminal_operation = "019f7c6c-1111-7000-8000-000000000104";
    let terminal = TerminalAssignment {
        context: context(
            CoordinationSemanticSlot::TurnCompleted,
            "019f7c6c-1111-7000-8000-000000000708",
            terminal_operation,
            true,
            7,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000709",
                "019f7c6c-1111-7000-8000-000000000107",
            )],
        ),
        terminal: TerminalTurn::Completed {
            target: target(2),
            target_turn_id: turn("turn-b"),
            outcome: TurnOutcome::Succeeded,
            included_generations: BoundedList::new(
                vec![generation(1), generation(2)],
                /*omitted_count*/ 0,
            )?,
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 3,
    };
    let reversed_terminal = TerminalAssignment {
        context: terminal.context.clone(),
        terminal: TerminalTurn::Completed {
            target: target(2),
            target_turn_id: turn("turn-b"),
            outcome: TurnOutcome::Succeeded,
            included_generations: BoundedList::new(
                vec![generation(2), generation(1)],
                /*omitted_count*/ 0,
            )?,
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 3,
    };
    assert!(matches!(
        runtime
            .terminal_coordination_assignment(reversed_terminal)
            .await,
        Err(CoordinationWriteError::AssignmentConflict)
    ));
    let first_terminal = runtime
        .terminal_coordination_assignment(terminal.clone())
        .await?;
    assert!(
        matches!(first_terminal, AssignmentTransitionOutcome::Applied { ref events } if events.len() == 2)
    );
    assert!(matches!(
        runtime.terminal_coordination_assignment(terminal).await?,
        AssignmentTransitionOutcome::Duplicate { ref events } if events.len() == 2
    ));
    let competing = TerminalAssignment {
        context: context(
            CoordinationSemanticSlot::TurnCompleted,
            "019f7c6c-1111-7000-8000-000000000710",
            "019f7c6c-1111-7000-8000-000000000105",
            true,
            9,
            Vec::new(),
        ),
        terminal: TerminalTurn::Completed {
            target: target(2),
            target_turn_id: turn("turn-b"),
            outcome: TurnOutcome::Failed,
            included_generations: BoundedList::new(
                vec![generation(1), generation(2)],
                /*omitted_count*/ 0,
            )?,
        },
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 4,
    };
    assert!(matches!(
        runtime.terminal_coordination_assignment(competing).await,
        Err(CoordinationWriteError::TerminalConflict)
    ));

    let record = runtime
        .coordination_assignment_aggregate(assignment_id)
        .await?
        .expect("aggregate");
    assert_eq!(record.head.accepted_generation, None);
    assert_eq!(
        record.generations[0].lifecycle,
        crate::model::coordination::GenerationLifecycle::Superseded
    );
    assert_eq!(
        record.generations[1].lifecycle,
        crate::model::coordination::GenerationLifecycle::Terminal
    );

    let post_terminal_operation = "019f7c6c-1111-7000-8000-000000000108";
    let mut post_terminal_followup = reserve_params();
    post_terminal_followup.context = context(
        CoordinationSemanticSlot::AssignmentRequested,
        "019f7c6c-1111-7000-8000-000000000711",
        post_terminal_operation,
        false,
        9,
        Vec::new(),
    );
    post_terminal_followup.operation_id = CoordinationOperationId::parse(post_terminal_operation)?;
    post_terminal_followup.reservation = AssignmentReservation::Followup {
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 4,
    };
    assert!(matches!(
        runtime
            .reserve_coordination_assignment(post_terminal_followup)
            .await?,
        ReserveAssignmentOutcome::Reserved { generation: reserved, .. }
            if reserved == generation(3)
    ));
    Ok(())
}
