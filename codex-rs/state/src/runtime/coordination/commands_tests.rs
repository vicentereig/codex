use codex_coordination::ContentEvidence;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::EncodedPayloadBytes;
use codex_coordination::Evidence;
use codex_coordination::IdempotencyKey;
use codex_coordination::ReceiptId;
use codex_coordination::UnavailableReason;
use pretty_assertions::assert_eq;

use super::aggregate_test_support::*;
use super::command_identity::validate_stored_tuple;
use super::commands::CommandWriteError;
use crate::StateRuntime;
use crate::model::coordination::AcceptAssignment;
use crate::model::coordination_commands::*;
use crate::runtime::test_support::unique_temp_dir;

pub(super) fn assignment_command() -> RecordCoordinationCommand {
    RecordCoordinationCommand::new(
        CoordinationCommandIntent::Assignment {
            reservation: reserve_params(),
        },
        CommandCiphertext::new(vec![0xA5; 384]).expect("ciphertext"),
    )
    .expect("command")
}

pub(super) async fn accepted_runtime() -> anyhow::Result<StateRuntimeFixture> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let command = assignment_command();
    let assignment = match runtime.record_coordination_command_intent(command).await? {
        RecordCoordinationCommandOutcome::Applied(metadata) => metadata.target.assignment_id,
        RecordCoordinationCommandOutcome::Duplicate(_) => anyhow::bail!("unexpected duplicate"),
    };
    runtime
        .accept_coordination_assignment(AcceptAssignment {
            context: context(
                CoordinationSemanticSlot::AssignmentAccepted,
                "019f7c6c-1111-7000-8000-000000000702",
                OPERATION,
                true,
                1,
                Vec::new(),
            ),
            assignment_id: assignment,
            generation: generation(1),
            receipt_id: ReceiptId::parse("019f7c6c-1111-7000-8000-000000000201")?,
            bound_turn_id: Evidence::Known {
                value: turn("turn-b"),
            },
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version: 0,
        })
        .await?;
    Ok(StateRuntimeFixture { runtime })
}

pub(super) struct StateRuntimeFixture {
    pub(super) runtime: std::sync::Arc<StateRuntime>,
}

#[tokio::test]
async fn assignment_intent_is_one_atomic_idempotent_bundle() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let command = assignment_command();
    let applied = runtime
        .record_coordination_command_intent(command.clone())
        .await?;
    let duplicate = runtime
        .record_coordination_command_intent(command.clone())
        .await?;
    assert!(matches!(
        (&applied, &duplicate),
        (
            RecordCoordinationCommandOutcome::Applied(left),
            RecordCoordinationCommandOutcome::Duplicate(right)
        ) if left == right
    ));
    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_commands),\
         (SELECT count(*) FROM coordination_assignment_generations),\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_projection_outbox)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (1, 1, 1, 1));
    assert!(!format!("{command:?}").contains(&"a5".repeat(16)));
    Ok(())
}

#[tokio::test]
async fn concurrent_duplicate_records_once_and_returns_the_same_metadata() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let left = runtime.clone();
    let right = runtime.clone();
    let command = assignment_command();
    let (left, right) = tokio::join!(
        left.record_coordination_command_intent(command.clone()),
        right.record_coordination_command_intent(command),
    );
    let outcomes = [left?, right?];
    let applied = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            RecordCoordinationCommandOutcome::Applied(metadata) => Some(metadata),
            RecordCoordinationCommandOutcome::Duplicate(_) => None,
        })
        .expect("one applied outcome");
    let duplicate = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            RecordCoordinationCommandOutcome::Duplicate(metadata) => Some(metadata),
            RecordCoordinationCommandOutcome::Applied(_) => None,
        })
        .expect("one duplicate outcome");
    assert_eq!(applied, duplicate);
    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_commands),\
         (SELECT count(*) FROM coordination_assignment_generations),\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_projection_outbox)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (1, 1, 1, 1));
    Ok(())
}

#[tokio::test]
async fn divergent_payload_conflicts_without_consuming_a_revision() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .record_coordination_command_intent(assignment_command())
        .await?;
    let divergent = RecordCoordinationCommand::new(
        CoordinationCommandIntent::Assignment {
            reservation: reserve_params(),
        },
        CommandCiphertext::new(vec![0x5A; 384])?,
    )?;
    assert!(matches!(
        runtime.record_coordination_command_intent(divergent).await,
        Err(CommandWriteError::IdempotencyConflict)
    ));
    let revision: i64 = sqlx::query_scalar(
        "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(revision, 1);

    let mut divergent_event = assignment_command();
    let CoordinationCommandIntent::Assignment { reservation } = &mut divergent_event.intent else {
        unreachable!("assignment fixture")
    };
    reservation.context.primary.event_id =
        CoordinationEventId::parse("019f7c6c-1111-7000-8000-000000000799")?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(divergent_event)
            .await,
        Err(CommandWriteError::IdempotencyConflict)
    ));
    let revision: i64 = sqlx::query_scalar(
        "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(revision, 1);
    Ok(())
}

#[test]
fn idempotency_tuple_classifier_distinguishes_collision_from_corruption() -> anyhow::Result<()> {
    let command = assignment_command();
    let context = command.intent.context();
    let key = IdempotencyKey::new(
        context.root_thread_id,
        context.actor.thread_id,
        turn("turn-a"),
        command.intent.operation_id(),
        CoordinationSemanticSlot::AssignmentRequested,
    );
    assert!(matches!(
        validate_stored_tuple(b"different-tuple", key.fingerprint().as_slice(), &key),
        Err(CommandWriteError::IdempotencyCollision)
    ));
    assert!(matches!(
        validate_stored_tuple(key.tuple_bytes(), &[0; 32], &key),
        Err(CommandWriteError::CorruptStoredCommand)
    ));
    Ok(())
}

#[tokio::test]
async fn reused_operation_or_event_identity_conflicts_without_revision_change() -> anyhow::Result<()>
{
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .record_coordination_command_intent(assignment_command())
        .await?;
    let message = |event_id: &str, operation_id: &str| -> anyhow::Result<_> {
        RecordCoordinationCommand::new(
            CoordinationCommandIntent::Message {
                context: context(
                    CoordinationSemanticSlot::MessageSubmissionRecorded,
                    event_id,
                    operation_id,
                    false,
                    1,
                    Vec::new(),
                ),
                operation_id: CoordinationOperationId::parse(operation_id)?,
                target: target(1),
                content: ContentEvidence::Unavailable {
                    reason: UnavailableReason::EncryptedPayload,
                },
                encoded_payload_bytes: EncodedPayloadBytes::new(4)?,
            },
            CommandCiphertext::new(vec![1, 2, 3, 4])?,
        )
        .map_err(anyhow::Error::from)
    };
    assert!(matches!(
        runtime
            .record_coordination_command_intent(message(
                "019f7c6c-1111-7000-8000-000000000798",
                OPERATION,
            )?)
            .await,
        Err(CommandWriteError::IdentityConflict)
    ));
    assert!(matches!(
        runtime
            .record_coordination_command_intent(message(
                "019f7c6c-1111-7000-8000-000000000701",
                "019f7c6c-1111-7000-8000-000000000198",
            )?)
            .await,
        Err(CommandWriteError::IdentityConflict)
    ));
    let revision: i64 = sqlx::query_scalar(
        "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(revision, 1);
    Ok(())
}

#[tokio::test]
async fn corrupt_stored_tuple_command_or_event_fails_closed() -> anyhow::Result<()> {
    let tuple_runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    tuple_runtime
        .record_coordination_command_intent(assignment_command())
        .await?;
    sqlx::query("DROP TRIGGER coordination_command_identity_immutable")
        .execute(&*tuple_runtime.pool)
        .await?;
    sqlx::query("DROP TRIGGER coordination_command_transition_guard")
        .execute(&*tuple_runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_commands SET idempotency_tuple_bytes=x'01'")
        .execute(&*tuple_runtime.pool)
        .await?;
    assert!(matches!(
        tuple_runtime
            .record_coordination_command_intent(assignment_command())
            .await,
        Err(CommandWriteError::CorruptStoredCommand)
    ));

    let command_runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    command_runtime
        .record_coordination_command_intent(assignment_command())
        .await?;
    sqlx::query("DROP TRIGGER coordination_command_identity_immutable")
        .execute(&*command_runtime.pool)
        .await?;
    sqlx::query("DROP TRIGGER coordination_command_transition_guard")
        .execute(&*command_runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_commands SET command_fingerprint=zeroblob(32)")
        .execute(&*command_runtime.pool)
        .await?;
    assert!(matches!(
        command_runtime
            .record_coordination_command_intent(assignment_command())
            .await,
        Err(CommandWriteError::CorruptStoredCommand)
    ));

    let event_runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    event_runtime
        .record_coordination_command_intent(assignment_command())
        .await?;
    sqlx::query("DROP TRIGGER coordination_events_immutable_update")
        .execute(&*event_runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_events SET event_fingerprint=zeroblob(32)")
        .execute(&*event_runtime.pool)
        .await?;
    assert!(matches!(
        event_runtime
            .record_coordination_command_intent(assignment_command())
            .await,
        Err(CommandWriteError::CorruptStoredCommand)
    ));
    for runtime in [&tuple_runtime, &command_runtime, &event_runtime] {
        let revision: i64 = sqlx::query_scalar(
            "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
        )
        .bind(ROOT)
        .fetch_one(&*runtime.pool)
        .await?;
        assert_eq!(revision, 1);
    }
    Ok(())
}
