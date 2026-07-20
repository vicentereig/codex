use codex_coordination::CoordinationFailureCode;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::GenerationCloseReason;
use pretty_assertions::assert_eq;
use sqlx::Row;

use super::aggregate_test_support::*;
use super::commands::CommandWriteError;
use super::commands_tests::assignment_command;
use super::inbox_test_support::RECEIPT_ONE;
use super::inbox_test_support::persist_initial_receipt;
use crate::StateRuntime;
use crate::model::coordination::CloseReservedAssignment;
use crate::model::coordination_commands::*;
use crate::model::coordination_inbox::CommittedReceiptAck;
use crate::runtime::test_support::unique_temp_dir;

pub(super) async fn pending_command()
-> anyhow::Result<(std::sync::Arc<StateRuntime>, CoordinationCommandMetadata)> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let RecordCoordinationCommandOutcome::Applied(metadata) = runtime
        .record_coordination_command_intent(assignment_command())
        .await?
    else {
        anyhow::bail!("unexpected duplicate");
    };
    Ok((runtime, metadata))
}

async fn committed_ack(runtime: &StateRuntime) -> anyhow::Result<CommittedReceiptAck> {
    persist_initial_receipt(runtime).await?;
    Ok(runtime
        .coordination_durable_receipt_ack(codex_coordination::ReceiptId::parse(RECEIPT_ONE)?)
        .await?)
}

fn uncommitted_ack() -> CommittedReceiptAck {
    CommittedReceiptAck {
        receipt_id: codex_coordination::ReceiptId::parse(RECEIPT_ONE).expect("receipt"),
        command_operation_id: codex_coordination::CoordinationOperationId::parse(OPERATION)
            .expect("operation"),
        receipt_event_id: codex_coordination::CoordinationEventId::parse(
            "019f7c6c-1111-7000-8000-000000000702",
        )
        .expect("event"),
        delivery_fingerprint: [0; 32],
        encoded_payload_bytes: 0,
        durable_received_at_ms: 0,
        expires_at_ms: 0,
    }
}

#[tokio::test]
async fn claim_begin_reclaim_and_retry_use_independent_counters() -> anyhow::Result<()> {
    let (runtime, pending) = pending_command().await?;
    let now = pending.retry_after_ms;
    let ClaimCoordinationCommandOutcome::Claimed(first) = runtime
        .claim_coordination_command(pending.operation_id, 0, 0, now, now + 100)
        .await?
    else {
        anyhow::bail!("command was not claimed");
    };
    assert_eq!(
        (
            first.metadata.version,
            first.metadata.claim_count,
            first.metadata.attempt_count,
            first.metadata.lease_epoch,
        ),
        (1, 1, 0, 1)
    );
    assert_eq!(
        runtime
            .reclaim_expired_coordination_command_leases(now + 100, 1)
            .await?,
        1
    );
    let ClaimCoordinationCommandOutcome::Claimed(second) = runtime
        .claim_coordination_command(pending.operation_id, 2, 1, now + 100, now + 300)
        .await?
    else {
        anyhow::bail!("reclaimed command was not claimed");
    };
    assert!(matches!(
        runtime
            .begin_coordination_command_attempt(first.lease, now + 101)
            .await,
        Err(CommandWriteError::LeaseFenced)
    ));
    let begun = runtime
        .begin_coordination_command_attempt(second.lease, now + 101)
        .await?;
    assert_eq!(begun.attempt, 1);
    let ResolveCommandAttemptOutcome::Applied(retried) = runtime
        .resolve_coordination_command_attempt(
            begun,
            CommandAttemptResolution::RetryAt {
                retry_at_ms: now + 250,
                code: CoordinationFailureCode::StateUnavailable,
            },
            now + 102,
        )
        .await?
    else {
        anyhow::bail!("retry was not recorded");
    };
    assert_eq!(
        (
            retried.lifecycle,
            retried.claim_count,
            retried.attempt_count,
            retried.lease_epoch,
        ),
        (CommandLifecycle::Pending, 2, 1, 2)
    );
    assert!(matches!(
        runtime
            .claim_coordination_command(
                pending.operation_id,
                retried.version,
                retried.lease_epoch,
                now + 249,
                now + 400,
            )
            .await?,
        ClaimCoordinationCommandOutcome::NotReady
    ));
    sqlx::query(
        "UPDATE coordination_commands SET lifecycle='leased',version=version+1,\
         claim_count=claim_count+1,lease_epoch=lease_epoch+1,lease_expires_at_ms=?,\
         updated_at_ms=? WHERE operation_id=?",
    )
    .bind(now + 400)
    .bind(now + 249)
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("raw SQL cannot claim a retry before its deadline");
    assert!(matches!(
        runtime
            .claim_coordination_command(
                pending.operation_id,
                retried.version,
                retried.lease_epoch,
                now + 250,
                now + 400,
            )
            .await?,
        ClaimCoordinationCommandOutcome::Claimed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn begun_attempt_survives_reopen_and_recovery_poisons_unknown_delivery() -> anyhow::Result<()>
{
    let state_dir = unique_temp_dir();
    let runtime = StateRuntime::init(state_dir.clone(), "test".to_string()).await?;
    let RecordCoordinationCommandOutcome::Applied(pending) = runtime
        .record_coordination_command_intent(assignment_command())
        .await?
    else {
        anyhow::bail!("unexpected duplicate");
    };
    let now = pending.retry_after_ms;
    let ClaimCoordinationCommandOutcome::Claimed(claimed) = runtime
        .claim_coordination_command(pending.operation_id, 0, 0, now, now + 100)
        .await?
    else {
        anyhow::bail!("not claimed");
    };
    let stale_attempt = runtime
        .begin_coordination_command_attempt(claimed.lease, now + 1)
        .await?;
    drop(runtime);

    let runtime = StateRuntime::init(state_dir, "test".to_string()).await?;
    assert_eq!(
        runtime
            .reclaim_expired_coordination_command_leases(now + 100, 1)
            .await?,
        0
    );
    let recovered =
        super::recovery_batch::recover_coordination_batch(&runtime.pool, now + 100, 1).await?;
    assert_eq!(
        recovered.dispositions,
        vec![super::recovery::RecoveryDisposition::CommandPoisoned]
    );
    assert!(matches!(
        runtime
            .resolve_coordination_command_attempt(
                stale_attempt,
                CommandAttemptResolution::Succeeded {
                    ack: uncommitted_ack(),
                },
                now + 101,
            )
            .await?,
        ResolveCommandAttemptOutcome::Terminal(CommandLifecycle::Poisoned)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
            .fetch_one(&*runtime.pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn two_claimers_produce_one_lease() -> anyhow::Result<()> {
    let (runtime, pending) = pending_command().await?;
    let now = pending.retry_after_ms;
    let left = runtime.clone();
    let right = runtime.clone();
    let operation = pending.operation_id;
    let (left, right) = tokio::join!(
        left.claim_coordination_command(operation, 0, 0, now, now + 1_000),
        right.claim_coordination_command(operation, 0, 0, now, now + 1_000),
    );
    let outcomes = [left?, right?];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ClaimCoordinationCommandOutcome::Claimed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                ClaimCoordinationCommandOutcome::Fenced | ClaimCoordinationCommandOutcome::NotReady
            ))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn closed_assignment_is_fenced_before_payload_is_claimed() -> anyhow::Result<()> {
    let (runtime, pending) = pending_command().await?;
    runtime
        .close_reserved_coordination_assignment(CloseReservedAssignment {
            context: context(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000711",
                OPERATION,
                false,
                1,
                Vec::new(),
            ),
            assignment_id: pending.target.assignment_id,
            generation: generation(1),
            reason: GenerationCloseReason::AbandonedBeforeAcceptance,
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version: 0,
        })
        .await?;

    assert!(matches!(
        runtime
            .claim_coordination_command(
                pending.operation_id,
                0,
                0,
                pending.retry_after_ms,
                pending.retry_after_ms + 1_000,
            )
            .await?,
        ClaimCoordinationCommandOutcome::Fenced
    ));
    Ok(())
}

#[tokio::test]
async fn success_is_first_wins_and_shortens_payload_ttl() -> anyhow::Result<()> {
    let (runtime, pending) = pending_command().await?;
    let now = pending.retry_after_ms;
    let ClaimCoordinationCommandOutcome::Claimed(claimed) = runtime
        .claim_coordination_command(pending.operation_id, 0, 0, now, now + 1_000)
        .await?
    else {
        anyhow::bail!("not claimed");
    };
    assert!(matches!(
        runtime
            .resolve_coordination_command_attempt(
                BegunCommandAttempt {
                    lease: claimed.lease.clone(),
                    attempt: 0,
                },
                CommandAttemptResolution::Succeeded {
                    ack: uncommitted_ack(),
                },
                now + 1,
            )
            .await?,
        ResolveCommandAttemptOutcome::Fenced
    ));
    let begun = runtime
        .begin_coordination_command_attempt(claimed.lease, now + 1)
        .await?;
    assert!(matches!(
        runtime
            .begin_coordination_command_attempt(begun.lease.clone(), now + 1)
            .await,
        Err(CommandWriteError::LeaseFenced)
    ));
    let ack = committed_ack(&runtime).await?;
    let ResolveCommandAttemptOutcome::Applied(succeeded) = runtime
        .resolve_coordination_command_attempt(
            begun.clone(),
            CommandAttemptResolution::Succeeded { ack: ack.clone() },
            now + 2,
        )
        .await?
    else {
        anyhow::bail!("not succeeded");
    };
    assert_eq!(succeeded.lifecycle, CommandLifecycle::Succeeded);
    assert_eq!(succeeded.expires_at_ms, now + 2 + 86_400_000);
    assert!(matches!(
        runtime
            .resolve_coordination_command_attempt(
                begun.clone(),
                CommandAttemptResolution::Succeeded { ack: ack.clone() },
                now + 3,
            )
            .await?,
        ResolveCommandAttemptOutcome::Terminal(CommandLifecycle::Succeeded)
    ));
    let mut divergent_ack = ack;
    divergent_ack.delivery_fingerprint[0] ^= 0xff;
    assert!(matches!(
        runtime
            .resolve_coordination_command_attempt(
                begun.clone(),
                CommandAttemptResolution::Succeeded { ack: divergent_ack },
                now + 4,
            )
            .await,
        Err(CommandWriteError::IdempotencyConflict)
    ));
    assert!(matches!(
        runtime
            .resolve_coordination_command_attempt(
                begun,
                CommandAttemptResolution::Poisoned {
                    code: CoordinationFailureCode::Internal,
                },
                now + 5,
            )
            .await,
        Err(CommandWriteError::IdempotencyConflict)
    ));
    assert_eq!(
        runtime
            .expire_coordination_command_payloads(succeeded.expires_at_ms, 8)
            .await?,
        1
    );
    assert_eq!(
        runtime
            .expire_coordination_command_payloads(succeeded.expires_at_ms, 8)
            .await?,
        0
    );
    let row = sqlx::query(
        "SELECT lifecycle,ciphertext,purged_at_ms FROM coordination_commands WHERE operation_id=?",
    )
    .bind(OPERATION)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(row.get::<String, _>("lifecycle"), "succeeded");
    assert_eq!(row.get::<Option<Vec<u8>>, _>("ciphertext"), None);
    assert_eq!(
        row.get::<Option<i64>, _>("purged_at_ms"),
        Some(succeeded.expires_at_ms)
    );
    Ok(())
}

#[tokio::test]
async fn poison_is_first_wins_with_a_closed_failure_code() -> anyhow::Result<()> {
    let (runtime, pending) = pending_command().await?;
    let now = pending.retry_after_ms;
    let ClaimCoordinationCommandOutcome::Claimed(claimed) = runtime
        .claim_coordination_command(pending.operation_id, 0, 0, now, now + 1_000)
        .await?
    else {
        anyhow::bail!("not claimed");
    };
    let begun = runtime
        .begin_coordination_command_attempt(claimed.lease, now + 1)
        .await?;
    let ResolveCommandAttemptOutcome::Applied(poisoned) = runtime
        .resolve_coordination_command_attempt(
            begun.clone(),
            CommandAttemptResolution::Poisoned {
                code: CoordinationFailureCode::RetryExhausted,
            },
            now + 2,
        )
        .await?
    else {
        anyhow::bail!("not poisoned");
    };
    assert_eq!(poisoned.lifecycle, CommandLifecycle::Poisoned);
    assert_eq!(poisoned.expires_at_ms, now + 2 + 86_400_000);
    let failure_code: String =
        sqlx::query_scalar("SELECT failure_code FROM coordination_commands WHERE operation_id=?")
            .bind(OPERATION)
            .fetch_one(&*runtime.pool)
            .await?;
    assert_eq!(failure_code, "retryExhausted");
    assert!(matches!(
        runtime
            .resolve_coordination_command_attempt(
                begun,
                CommandAttemptResolution::Succeeded {
                    ack: uncommitted_ack(),
                },
                now + 3,
            )
            .await?,
        ResolveCommandAttemptOutcome::Terminal(CommandLifecycle::Poisoned)
    ));
    assert_eq!(
        runtime
            .expire_coordination_command_payloads(poisoned.expires_at_ms, 1)
            .await?,
        1
    );
    let payload: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT ciphertext FROM coordination_commands WHERE operation_id=?")
            .bind(OPERATION)
            .fetch_one(&*runtime.pool)
            .await?;
    assert_eq!(payload, None);
    Ok(())
}

#[tokio::test]
async fn lease_is_clipped_and_terminal_ttl_never_extends_initial_seven_days() -> anyhow::Result<()>
{
    let (runtime, pending) = pending_command().await?;
    assert_eq!(
        pending.expires_at_ms - pending.retry_after_ms,
        7 * 24 * 60 * 60 * 1_000
    );
    assert_eq!(
        runtime
            .expire_coordination_command_payloads(pending.expires_at_ms - 1, 1)
            .await?,
        0
    );
    let now = pending.expires_at_ms - 1_000;
    let ClaimCoordinationCommandOutcome::Claimed(claimed) = runtime
        .claim_coordination_command(
            pending.operation_id,
            0,
            0,
            now,
            pending.expires_at_ms + 10_000,
        )
        .await?
    else {
        anyhow::bail!("not claimed");
    };
    assert_eq!(claimed.lease.lease_expires_at_ms, pending.expires_at_ms);
    let begun = runtime
        .begin_coordination_command_attempt(claimed.lease, now + 1)
        .await?;
    let ack = committed_ack(&runtime).await?;
    let ResolveCommandAttemptOutcome::Applied(succeeded) = runtime
        .resolve_coordination_command_attempt(
            begun,
            CommandAttemptResolution::Succeeded { ack },
            now + 2,
        )
        .await?
    else {
        anyhow::bail!("not succeeded");
    };
    assert_eq!(succeeded.expires_at_ms, pending.expires_at_ms);
    Ok(())
}
