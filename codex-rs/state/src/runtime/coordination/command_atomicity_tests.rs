use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_coordination::ContentEvidence;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::EncodedPayloadBytes;
use codex_coordination::UnavailableReason;
use pretty_assertions::assert_eq;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::aggregate_test_support::*;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::commands_tests::accepted_runtime;
use super::commands_tests::assignment_command;
use crate::StateRuntime;
use crate::model::coordination_commands::*;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn message_and_interrupt_capture_the_accepted_generation() -> anyhow::Result<()> {
    let fixture = accepted_runtime().await?;
    let runtime = fixture.runtime;
    let message_operation = "019f7c6c-1111-7000-8000-000000000151";
    let message = RecordCoordinationCommand::new(
        CoordinationCommandIntent::Message {
            context: context(
                CoordinationSemanticSlot::MessageSubmissionRecorded,
                "019f7c6c-1111-7000-8000-000000000751",
                message_operation,
                false,
                2,
                Vec::new(),
            ),
            operation_id: CoordinationOperationId::parse(message_operation)?,
            target: target(1),
            content: ContentEvidence::Unavailable {
                reason: UnavailableReason::EncryptedPayload,
            },
            encoded_payload_bytes: EncodedPayloadBytes::new(4)?,
        },
        CommandCiphertext::new(vec![1, 2, 3, 4])?,
    )?;
    assert!(
        runtime
            .record_coordination_command_intent_with(
                message.clone(),
                &FailOnce::command(CommandStep::CommandInsert),
            )
            .await
            .is_err()
    );
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT committed_revision,\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_commands)\
         FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (2, 2, 1));
    let RecordCoordinationCommandOutcome::Applied(message) =
        runtime.record_coordination_command_intent(message).await?
    else {
        anyhow::bail!("message was not applied");
    };
    assert_eq!(message.target.captured_head_generation, Some(generation(1)));
    assert_eq!(message.target.turn_id, Some(turn("turn-b")));

    let interrupt_operation = "019f7c6c-1111-7000-8000-000000000152";
    let interrupt = RecordCoordinationCommand::new(
        CoordinationCommandIntent::Interrupt {
            context: context(
                CoordinationSemanticSlot::InterruptRequested,
                "019f7c6c-1111-7000-8000-000000000752",
                interrupt_operation,
                false,
                3,
                Vec::new(),
            ),
            operation_id: CoordinationOperationId::parse(interrupt_operation)?,
            target: target(1),
        },
        CommandCiphertext::new(Vec::new())?,
    )?;
    assert!(
        runtime
            .record_coordination_command_intent_with(
                interrupt.clone(),
                &FailOnce::command(CommandStep::CommandInsert),
            )
            .await
            .is_err()
    );
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT committed_revision,\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_commands)\
         FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (3, 3, 2));
    let RecordCoordinationCommandOutcome::Applied(interrupt) = runtime
        .record_coordination_command_intent(interrupt)
        .await?
    else {
        anyhow::bail!("interrupt was not applied");
    };
    assert_eq!(
        interrupt
            .target
            .captured_turn_set
            .expect("capture")
            .generations(),
        &[generation(1)]
    );
    Ok(())
}

struct FailOnce {
    aggregate: Option<AggregateStep>,
    command: Option<CommandStep>,
    failed: AtomicBool,
}

impl FailOnce {
    fn command(step: CommandStep) -> Self {
        Self {
            aggregate: None,
            command: Some(step),
            failed: AtomicBool::new(false),
        }
    }

    fn aggregate(step: AggregateStep) -> Self {
        Self {
            aggregate: Some(step),
            command: None,
            failed: AtomicBool::new(false),
        }
    }
}

impl AggregateFailureInjector for FailOnce {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        if self.aggregate == Some(step) && !self.failed.swap(true, Ordering::SeqCst) {
            anyhow::bail!("injected aggregate failure");
        }
        Ok(())
    }
}

impl CommandFailureInjector for FailOnce {
    fn after_command_step(&self, step: CommandStep) -> anyhow::Result<()> {
        if self.command == Some(step) && !self.failed.swap(true, Ordering::SeqCst) {
            anyhow::bail!("injected command failure");
        }
        Ok(())
    }
}

#[tokio::test]
async fn failure_after_command_insert_rolls_back_the_whole_bundle() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    assert!(
        runtime
            .record_coordination_command_intent_with(
                assignment_command(),
                &FailOnce::command(CommandStep::CommandInsert),
            )
            .await
            .is_err()
    );
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_commands),\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_assignment_generations)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (0, 0, 0));
    assert!(matches!(
        runtime
            .record_coordination_command_intent(assignment_command())
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    Ok(())
}

#[tokio::test]
async fn communication_intent_rolls_back_at_every_exposed_transaction_boundary()
-> anyhow::Result<()> {
    let communication_command = |interrupt: bool| -> anyhow::Result<_> {
        let (event_id, operation_id) = if interrupt {
            (
                "019f7c6c-1111-7000-8000-000000000759",
                "019f7c6c-1111-7000-8000-000000000159",
            )
        } else {
            (
                "019f7c6c-1111-7000-8000-000000000758",
                "019f7c6c-1111-7000-8000-000000000158",
            )
        };
        let context = context(
            if interrupt {
                CoordinationSemanticSlot::InterruptRequested
            } else {
                CoordinationSemanticSlot::MessageSubmissionRecorded
            },
            event_id,
            operation_id,
            false,
            2,
            Vec::new(),
        );
        let operation_id = CoordinationOperationId::parse(operation_id)?;
        if interrupt {
            return Ok(RecordCoordinationCommand::new(
                CoordinationCommandIntent::Interrupt {
                    context,
                    operation_id,
                    target: target(1),
                },
                CommandCiphertext::new(Vec::new())?,
            )?);
        }
        Ok(RecordCoordinationCommand::new(
            CoordinationCommandIntent::Message {
                context,
                operation_id,
                target: target(1),
                content: ContentEvidence::Unavailable {
                    reason: UnavailableReason::EncryptedPayload,
                },
                encoded_payload_bytes: EncodedPayloadBytes::new(4)?,
            },
            CommandCiphertext::new(vec![1, 2, 3, 4])?,
        )?)
    };
    let aggregate_steps = [
        AggregateStep::AuthorityRead,
        AggregateStep::IdempotencyRead,
        AggregateStep::AggregateRead,
        AggregateStep::RevisionAllocation,
        AggregateStep::EventInsert,
        AggregateStep::OutboxInsert,
        AggregateStep::BeforeCommit,
    ];
    let command_steps = [
        CommandStep::IdentityRead,
        CommandStep::TargetCapture,
        CommandStep::CommandInsert,
    ];
    for interrupt in [false, true] {
        for step in aggregate_steps {
            let fixture = accepted_runtime().await?;
            assert!(
                fixture
                    .runtime
                    .record_coordination_command_intent_with(
                        communication_command(interrupt)?,
                        &FailOnce::aggregate(step),
                    )
                    .await
                    .is_err(),
                "{step:?} must fail"
            );
            assert_accepted_baseline(&fixture.runtime).await?;
        }
        for step in command_steps {
            let fixture = accepted_runtime().await?;
            assert!(
                fixture
                    .runtime
                    .record_coordination_command_intent_with(
                        communication_command(interrupt)?,
                        &FailOnce::command(step),
                    )
                    .await
                    .is_err(),
                "{step:?} must fail"
            );
            assert_accepted_baseline(&fixture.runtime).await?;
        }
    }
    Ok(())
}

async fn assert_accepted_baseline(runtime: &StateRuntime) -> anyhow::Result<()> {
    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT committed_revision,\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_projection_outbox),\
         (SELECT count(*) FROM coordination_commands)\
         FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (2, 2, 2, 1));
    Ok(())
}

#[tokio::test]
async fn response_loss_after_commit_replays_as_duplicate() -> anyhow::Result<()> {
    let state_dir = unique_temp_dir();
    let runtime = StateRuntime::init(state_dir.clone(), "test".to_string()).await?;
    assert!(
        runtime
            .record_coordination_command_intent_with(
                assignment_command(),
                &FailOnce::aggregate(AggregateStep::AfterCommit),
            )
            .await
            .is_err()
    );
    drop(runtime);
    let runtime = StateRuntime::init(state_dir, "test".to_string()).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(assignment_command())
            .await?,
        RecordCoordinationCommandOutcome::Duplicate(_)
    ));
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_commands),\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_projection_outbox)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (1, 1, 1));
    Ok(())
}
