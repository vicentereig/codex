use codex_coordination::AssignmentId;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::Evidence;
use codex_coordination::ReceiptId;
use pretty_assertions::assert_eq;

use super::aggregate_test_support::*;
use super::command_lease_tests::pending_command;
use super::commands::CommandWriteError;
use super::commands_tests::accepted_runtime;
use super::commands_tests::assignment_command;
use crate::model::coordination::AcceptAssignment;
use crate::model::coordination::AssignmentReservation;
use crate::model::coordination_commands::*;

#[tokio::test]
async fn hard_expiry_clears_payload_and_quarantine_rejects_access() -> anyhow::Result<()> {
    let (runtime, pending) = pending_command().await?;
    sqlx::query(
        "UPDATE coordination_commands SET lifecycle='expired',version=version+1,\
         ciphertext=NULL,purged_at_ms=updated_at_ms+1,updated_at_ms=updated_at_ms+1 \
         WHERE operation_id=?",
    )
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("a live command cannot be expired before its hard deadline");
    assert_eq!(
        runtime
            .expire_coordination_command_payloads(pending.expires_at_ms, 1)
            .await?,
        1
    );
    assert!(matches!(
        runtime
            .claim_coordination_command(
                pending.operation_id,
                1,
                0,
                pending.expires_at_ms,
                pending.expires_at_ms + 1,
            )
            .await?,
        ClaimCoordinationCommandOutcome::Expired
    ));

    let (quarantined, metadata) = pending_command().await?;
    let ClaimCoordinationCommandOutcome::Claimed(claimed) = quarantined
        .claim_coordination_command(
            metadata.operation_id,
            0,
            0,
            metadata.retry_after_ms,
            metadata.retry_after_ms + 1_000,
        )
        .await?
    else {
        anyhow::bail!("not claimed");
    };
    let begun = quarantined
        .begin_coordination_command_attempt(claimed.lease, metadata.retry_after_ms + 1)
        .await?;
    sqlx::query(
        "UPDATE coordination_authority SET status='quarantined',quarantine_reason='test',\
         updated_at_ms=updated_at_ms+1 WHERE singleton_id=1",
    )
    .execute(&*quarantined.pool)
    .await?;
    assert!(matches!(
        quarantined
            .record_coordination_command_intent(assignment_command())
            .await,
        Err(CommandWriteError::Quarantined)
    ));
    assert!(matches!(
        quarantined
            .claim_coordination_command(
                metadata.operation_id,
                0,
                0,
                metadata.retry_after_ms,
                metadata.retry_after_ms + 1_000,
            )
            .await,
        Err(CommandWriteError::Quarantined)
    ));
    assert!(matches!(
        quarantined
            .begin_coordination_command_attempt(begun.lease.clone(), metadata.retry_after_ms + 2)
            .await,
        Err(CommandWriteError::Quarantined)
    ));
    assert!(matches!(
        quarantined
            .resolve_coordination_command_attempt(
                begun,
                CommandAttemptResolution::Succeeded {
                    ack: crate::model::coordination_inbox::CommittedReceiptAck {
                        receipt_id: ReceiptId::parse("019f7c6c-1111-7000-8000-000000000201",)?,
                        command_operation_id: metadata.operation_id,
                        receipt_event_id: codex_coordination::CoordinationEventId::parse(
                            "019f7c6c-1111-7000-8000-000000000702",
                        )?,
                        delivery_fingerprint: [0; 32],
                        encoded_payload_bytes: 0,
                        durable_received_at_ms: 0,
                        expires_at_ms: 0,
                    },
                },
                metadata.retry_after_ms + 2,
            )
            .await,
        Err(CommandWriteError::Quarantined)
    ));
    assert!(matches!(
        quarantined
            .reclaim_expired_coordination_command_leases(metadata.retry_after_ms + 2_000, 1)
            .await,
        Err(CommandWriteError::Quarantined)
    ));
    assert!(matches!(
        quarantined
            .expire_coordination_command_payloads(metadata.expires_at_ms, 1)
            .await,
        Err(CommandWriteError::Quarantined)
    ));
    let payload: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT ciphertext FROM coordination_commands WHERE operation_id=?")
            .bind(OPERATION)
            .fetch_one(&*quarantined.pool)
            .await?;
    assert!(payload.is_some(), "quarantine must remain read-only");
    Ok(())
}

#[tokio::test]
async fn newer_bound_generation_fences_an_interrupt_before_attempt() -> anyhow::Result<()> {
    let fixture = accepted_runtime().await?;
    let runtime = fixture.runtime;
    let interrupt_operation = "019f7c6c-1111-7000-8000-000000000160";
    let interrupt_command = RecordCoordinationCommand::new(
        CoordinationCommandIntent::Interrupt {
            context: context(
                CoordinationSemanticSlot::InterruptRequested,
                "019f7c6c-1111-7000-8000-000000000760",
                interrupt_operation,
                false,
                2,
                Vec::new(),
            ),
            operation_id: CoordinationOperationId::parse(interrupt_operation)?,
            target: target(1),
        },
        CommandCiphertext::new(Vec::new())?,
    )?;
    let RecordCoordinationCommandOutcome::Applied(interrupt) = runtime
        .record_coordination_command_intent(interrupt_command.clone())
        .await?
    else {
        anyhow::bail!("interrupt was not applied");
    };

    let followup_operation = "019f7c6c-1111-7000-8000-000000000161";
    let mut followup = reserve_params();
    followup.context = context(
        CoordinationSemanticSlot::AssignmentRequested,
        "019f7c6c-1111-7000-8000-000000000761",
        followup_operation,
        false,
        3,
        Vec::new(),
    );
    followup.operation_id = CoordinationOperationId::parse(followup_operation)?;
    followup.reservation = AssignmentReservation::Followup {
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 1,
    };
    runtime.reserve_coordination_assignment(followup).await?;
    runtime
        .accept_coordination_assignment(AcceptAssignment {
            context: context(
                CoordinationSemanticSlot::AssignmentAccepted,
                "019f7c6c-1111-7000-8000-000000000762",
                followup_operation,
                true,
                4,
                vec![(
                    CoordinationSemanticSlot::AssignmentGenerationClosed,
                    "019f7c6c-1111-7000-8000-000000000763",
                    "019f7c6c-1111-7000-8000-000000000162",
                )],
            ),
            assignment_id: interrupt.target.assignment_id,
            generation: generation(2),
            receipt_id: ReceiptId::parse("019f7c6c-1111-7000-8000-000000000260")?,
            bound_turn_id: Evidence::Known {
                value: turn("turn-b"),
            },
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version: 2,
        })
        .await?;
    assert!(matches!(
        runtime
            .claim_coordination_command(
                interrupt.operation_id,
                0,
                0,
                interrupt.retry_after_ms,
                interrupt.retry_after_ms + 1_000,
            )
            .await?,
        ClaimCoordinationCommandOutcome::Fenced
    ));
    assert!(matches!(
        runtime
            .record_coordination_command_intent(interrupt_command)
            .await?,
        RecordCoordinationCommandOutcome::Duplicate(metadata) if metadata == interrupt
    ));
    let stale_operation = "019f7c6c-1111-7000-8000-000000000163";
    assert!(matches!(
        runtime
            .record_coordination_command_intent(RecordCoordinationCommand::new(
                CoordinationCommandIntent::Interrupt {
                    context: context(
                        CoordinationSemanticSlot::InterruptRequested,
                        "019f7c6c-1111-7000-8000-000000000764",
                        stale_operation,
                        false,
                        6,
                        Vec::new(),
                    ),
                    operation_id: CoordinationOperationId::parse(stale_operation)?,
                    target: target(1),
                },
                CommandCiphertext::new(Vec::new())?,
            )?)
            .await,
        Err(CommandWriteError::GenerationFenced)
    ));
    let revision: i64 = sqlx::query_scalar(
        "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(revision, 6);
    Ok(())
}

#[tokio::test]
async fn interrupt_with_more_than_four_bound_generations_rolls_back_intent() -> anyhow::Result<()> {
    let fixture = accepted_runtime().await?;
    let runtime = fixture.runtime;
    let assignment_id = AssignmentId::parse(ASSIGNMENT)?;
    let mut root_revision = 2_u64;
    let mut head_version = 1_u64;

    for generation_number in 2..=5 {
        let request_operation =
            format!("019f7c6c-1111-7000-8000-{:012x}", 0x180 + generation_number);
        let request_event = format!("019f7c6c-1111-7000-8000-{:012x}", 0x780 + generation_number);
        let mut followup = reserve_params();
        followup.context = context(
            CoordinationSemanticSlot::AssignmentRequested,
            &request_event,
            &request_operation,
            false,
            root_revision,
            Vec::new(),
        );
        followup.operation_id = CoordinationOperationId::parse(&request_operation)?;
        followup.reservation = AssignmentReservation::Followup {
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version: head_version,
        };
        runtime.reserve_coordination_assignment(followup).await?;
        root_revision += 1;
        head_version += 1;

        let accept_event = format!("019f7c6c-1111-7000-8000-{:012x}", 0x880 + generation_number);
        let close_event = format!("019f7c6c-1111-7000-8000-{:012x}", 0x980 + generation_number);
        let close_operation = format!("019f7c6c-1111-7000-8000-{:012x}", 0x280 + generation_number);
        let receipt = format!("019f7c6c-1111-7000-8000-{:012x}", 0x380 + generation_number);
        runtime
            .accept_coordination_assignment(AcceptAssignment {
                context: context(
                    CoordinationSemanticSlot::AssignmentAccepted,
                    &accept_event,
                    &request_operation,
                    true,
                    root_revision,
                    vec![(
                        CoordinationSemanticSlot::AssignmentGenerationClosed,
                        &close_event,
                        &close_operation,
                    )],
                ),
                assignment_id,
                generation: generation(generation_number),
                receipt_id: ReceiptId::parse(&receipt)?,
                bound_turn_id: Evidence::Known {
                    value: turn("turn-b"),
                },
                expected_owner_thread_id: thread(ROOT),
                expected_owner_turn_id: turn("turn-a"),
                expected_head_version: head_version,
            })
            .await?;
        root_revision += 2;
        head_version += 1;
    }

    let interrupt_operation = "019f7c6c-1111-7000-8000-000000000190";
    let error = runtime
        .record_coordination_command_intent(RecordCoordinationCommand::new(
            CoordinationCommandIntent::Interrupt {
                context: context(
                    CoordinationSemanticSlot::InterruptRequested,
                    "019f7c6c-1111-7000-8000-000000000790",
                    interrupt_operation,
                    false,
                    root_revision,
                    Vec::new(),
                ),
                operation_id: CoordinationOperationId::parse(interrupt_operation)?,
                target: target(5),
            },
            CommandCiphertext::new(Vec::new())?,
        )?)
        .await
        .expect_err("the frozen event cannot represent five included generations");
    assert!(matches!(
        error,
        CommandWriteError::Input(CommandInputError::InvalidGenerationSet)
    ));
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT committed_revision,\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_commands)\
         FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (root_revision as i64, root_revision as i64, 1));
    Ok(())
}
