use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationOperationId;
use codex_coordination::EncodedPayloadBytes;
use pretty_assertions::assert_eq;

use super::aggregate_test_support::*;
use super::commands_tests::assignment_command;
use crate::StateRuntime;
use crate::model::coordination_commands::*;
use crate::runtime::test_support::unique_temp_dir;

#[test]
fn payload_bounds_and_debug_are_redacted() -> anyhow::Result<()> {
    let sentinel = b"plaintext-secret-sentinel";
    let mut bytes = sentinel.repeat(2_731);
    bytes.resize(65_536, 0xA5);
    let ciphertext = CommandCiphertext::new(bytes)?;
    assert_eq!(ciphertext.encoded_len(), 65_536);
    assert!(!format!("{ciphertext:?}").contains("plaintext-secret-sentinel"));
    assert!(matches!(
        CommandCiphertext::new(vec![0; 65_537]),
        Err(CommandInputError::PayloadOverLimit)
    ));
    Ok(())
}

#[tokio::test]
async fn maximum_payload_is_persisted_exactly_without_truncation() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    assert!(matches!(
        CommandCiphertext::new(vec![0; 65_537]),
        Err(CommandInputError::PayloadOverLimit)
    ));
    let untouched: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_roots),\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_commands)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(untouched, (0, 0, 0));
    let payload = vec![0xA5; 65_536];
    let mut reservation = reserve_params();
    reservation.encoded_payload_bytes = EncodedPayloadBytes::new(payload.len() as u32)?;
    let command = RecordCoordinationCommand::new(
        CoordinationCommandIntent::Assignment { reservation },
        CommandCiphertext::new(payload.clone())?,
    )?;
    assert!(matches!(
        runtime.record_coordination_command_intent(command).await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    let stored: (i64, Vec<u8>) = sqlx::query_as(
        "SELECT encoded_payload_bytes,ciphertext FROM coordination_commands WHERE operation_id=?",
    )
    .bind(OPERATION)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(stored, (65_536, payload));
    Ok(())
}

#[tokio::test]
async fn payload_is_absent_from_events_outbox_debug_and_errors() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let sentinel = "plaintext-secret-sentinel";
    let mut payload = sentinel.repeat(20).into_bytes();
    payload.truncate(384);
    let encoded = payload
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let command = RecordCoordinationCommand::new(
        CoordinationCommandIntent::Assignment {
            reservation: reserve_params(),
        },
        CommandCiphertext::new(payload.clone())?,
    )?;
    assert!(!format!("{command:?}").contains(sentinel));
    assert!(!format!("{command:?}").contains(&encoded));
    let applied = runtime
        .record_coordination_command_intent(command.clone())
        .await?;
    assert!(!format!("{applied:?}").contains(sentinel));
    assert!(!format!("{applied:?}").contains(&encoded));
    sqlx::query(
        "UPDATE coordination_commands SET idempotency_tuple_bytes=x'01' WHERE operation_id=?",
    )
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("immutable tuple");
    let stored_event: Vec<u8> = sqlx::query_scalar(
        "SELECT canonical_event_bytes FROM coordination_events WHERE event_id=?",
    )
    .bind(CoordinationEventId::parse("019f7c6c-1111-7000-8000-000000000701")?.to_string())
    .fetch_one(&*runtime.pool)
    .await?;
    let stored_event = String::from_utf8(stored_event)?;
    assert!(!stored_event.contains(sentinel));
    assert!(!stored_event.contains(&encoded));
    let outbox: (String, i64, i64, i64, Option<String>) = sqlx::query_as(
        "SELECT status,version,lease_epoch,retry_count,last_error \
         FROM coordination_projection_outbox WHERE event_id=?",
    )
    .bind("019f7c6c-1111-7000-8000-000000000701")
    .fetch_one(&*runtime.pool)
    .await?;
    assert!(!format!("{outbox:?}").contains(sentinel));
    assert!(!format!("{outbox:?}").contains(&encoded));
    let claimed = runtime
        .claim_coordination_command(
            CoordinationOperationId::parse(OPERATION)?,
            0,
            0,
            chrono::Utc::now().timestamp_millis(),
            chrono::Utc::now().timestamp_millis() + 1_000,
        )
        .await?;
    assert!(!format!("{claimed:?}").contains(sentinel));
    assert!(!format!("{claimed:?}").contains(&encoded));
    let mut divergent = command;
    divergent.ciphertext = CommandCiphertext::new(vec![0x5A; 384])?;
    let error = runtime
        .record_coordination_command_intent(divergent)
        .await
        .expect_err("divergent ciphertext must conflict");
    assert!(!format!("{error:?}").contains(sentinel));
    assert!(!format!("{error:?}").contains(&encoded));
    Ok(())
}

#[tokio::test]
async fn raw_sql_cannot_bypass_claim_or_counter_transitions() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let RecordCoordinationCommandOutcome::Applied(metadata) = runtime
        .record_coordination_command_intent(assignment_command())
        .await?
    else {
        anyhow::bail!("unexpected duplicate");
    };
    sqlx::query(
        "UPDATE coordination_commands SET lifecycle='leased',version=version+1,\
         lease_expires_at_ms=?,updated_at_ms=updated_at_ms+1 WHERE operation_id=?",
    )
    .bind(metadata.retry_after_ms + 100)
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("claim must increment claim count and lease epoch");
    let ClaimCoordinationCommandOutcome::Claimed(claimed) = runtime
        .claim_coordination_command(
            metadata.operation_id,
            0,
            0,
            metadata.retry_after_ms,
            metadata.retry_after_ms + 100,
        )
        .await?
    else {
        anyhow::bail!("command was not claimed");
    };
    sqlx::query(
        "UPDATE coordination_commands SET lifecycle='pending',version=version+1,\
         lease_expires_at_ms=NULL,updated_at_ms=updated_at_ms+1 WHERE operation_id=?",
    )
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("an active lease cannot be reclaimed before its deadline");
    sqlx::query(
        "UPDATE coordination_commands SET version=version+1,attempt_count=attempt_count+1,\
         attempted_lease_epoch=lease_epoch,updated_at_ms=? WHERE operation_id=?",
    )
    .bind(metadata.retry_after_ms + 100)
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("an attempt cannot begin at or after its lease deadline");
    sqlx::query(
        "UPDATE coordination_commands SET lifecycle='succeeded',version=version+1,\
         lease_expires_at_ms=NULL,terminal_at_ms=?,updated_at_ms=? WHERE operation_id=?",
    )
    .bind(metadata.retry_after_ms + 1)
    .bind(metadata.retry_after_ms + 1)
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("resolution requires a durably begun attempt");
    let begun = runtime
        .begin_coordination_command_attempt(claimed.lease, metadata.retry_after_ms + 1)
        .await?;
    sqlx::query(
        "UPDATE coordination_commands SET lifecycle='pending',version=version+1,\
         retry_after_ms=retry_after_ms+50,lease_expires_at_ms=NULL,\
         updated_at_ms=updated_at_ms+1 WHERE operation_id=?",
    )
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("a retry must preserve a bounded failure code");
    sqlx::query(
        "UPDATE coordination_commands SET lifecycle='succeeded',version=version+1,\
         lease_expires_at_ms=NULL,failure_code=NULL,terminal_at_ms=?,\
         expires_at_ms=expires_at_ms-1,updated_at_ms=? WHERE operation_id=?",
    )
    .bind(metadata.retry_after_ms + 2)
    .bind(metadata.retry_after_ms + 2)
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("terminal payload expiry must use the frozen 24-hour minimum");
    sqlx::query(
        "UPDATE coordination_commands SET version=version+1,lease_epoch=lease_epoch+1,\
         updated_at_ms=updated_at_ms+1 WHERE operation_id=?",
    )
    .bind(OPERATION)
    .execute(&*runtime.pool)
    .await
    .expect_err("only a pending-to-leased claim can advance lease epoch");
    assert_eq!(begun.attempt, 1);
    Ok(())
}
