use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::StateEpoch;
use pretty_assertions::assert_eq;
use sqlx::Row;

use super::aggregate_test_support::OPERATION;
use super::commands_tests::assignment_command;
use super::inbox_test_support::persist_initial_receipt;
use super::recovery::RecoveryDisposition;
use super::recovery::RecoveryWriteError;
use crate::StateRuntime;
use crate::model::coordination_commands::ClaimCoordinationCommandOutcome;
use crate::model::coordination_commands::CommandCiphertext;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery_maintenance::CheckedMaintenanceDegradation;
use crate::model::coordination_recovery_maintenance::RecoveryRecordKind;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn expired_command_and_inbox_payloads_degrade_once_without_payload_bytes()
-> anyhow::Result<()> {
    let sentinel = b"PRIVATE_COMMAND_/tmp/approval_tool_output";
    let mut ciphertext = vec![0xA5; 384];
    ciphertext[..sentinel.len()].copy_from_slice(sentinel);
    let mut command = assignment_command();
    command.ciphertext = CommandCiphertext::new(ciphertext)?;
    assert!(!format!("{command:?}").contains("PRIVATE_COMMAND"));

    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let RecordCoordinationCommandOutcome::Applied(command_metadata) =
        runtime.record_coordination_command_intent(command).await?
    else {
        anyhow::bail!("unexpected duplicate command");
    };
    let inbox_metadata = persist_initial_receipt(&runtime).await?;
    assert_eq!(command_metadata.expires_at_ms, inbox_metadata.expires_at_ms);

    let recovered = super::recovery_batch::recover_coordination_batch(
        &runtime.pool,
        command_metadata.expires_at_ms,
        100,
    )
    .await?;
    assert_eq!(
        recovered.dispositions,
        vec![
            RecoveryDisposition::CommandPayloadExpired,
            RecoveryDisposition::InboxPayloadExpired,
        ]
    );
    assert_eq!(
        sqlx::query_as::<_, (String, Option<Vec<u8>>)>(
            "SELECT lifecycle,ciphertext FROM coordination_commands WHERE operation_id=?",
        )
        .bind(OPERATION)
        .fetch_one(&*runtime.pool)
        .await?,
        ("expired".to_string(), None)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
            .fetch_one(&*runtime.pool)
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM coordination_degradation_publication_outbox",
        )
        .fetch_one(&*runtime.pool)
        .await?,
        2
    );
    let records = sqlx::query(
        "SELECT identity_bytes,canonical_record_bytes FROM coordination_degradation_records",
    )
    .fetch_all(&*runtime.pool)
    .await?;
    for record in records {
        for field in [
            record.get::<Vec<u8>, _>("identity_bytes"),
            record.get::<Vec<u8>, _>("canonical_record_bytes"),
        ] {
            assert!(!String::from_utf8_lossy(&field).contains("PRIVATE_COMMAND"));
        }
    }

    let replay = super::recovery_batch::recover_coordination_batch(
        &runtime.pool,
        command_metadata.expires_at_ms,
        100,
    )
    .await?;
    assert!(replay.dispositions.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
            .fetch_one(&*runtime.pool)
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn uncertain_attempt_is_poisoned_even_after_payload_ttl() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let RecordCoordinationCommandOutcome::Applied(metadata) = runtime
        .record_coordination_command_intent(assignment_command())
        .await?
    else {
        anyhow::bail!("unexpected duplicate command");
    };
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
    runtime
        .begin_coordination_command_attempt(claimed.lease, metadata.retry_after_ms + 1)
        .await?;

    let recovered = super::recovery_batch::recover_coordination_batch(
        &runtime.pool,
        metadata.expires_at_ms + 1,
        1,
    )
    .await?;
    assert_eq!(
        recovered.dispositions,
        vec![RecoveryDisposition::CommandPoisoned]
    );
    assert_eq!(
        sqlx::query_as::<_, (String, Option<Vec<u8>>)>(
            "SELECT lifecycle,ciphertext FROM coordination_commands WHERE operation_id=?",
        )
        .bind(OPERATION)
        .fetch_one(&*runtime.pool)
        .await?,
        ("poisoned".to_string(), None)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM coordination_degradation_records \
             WHERE reason='poisonedAttempt'",
        )
        .fetch_one(&*runtime.pool)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM coordination_degradation_records \
             WHERE reason='stateLossDegraded' AND recovery_record_kind='assignment'",
        )
        .fetch_one(&*runtime.pool)
        .await?,
        0
    );
    assert_eq!(
        super::recovery_batch::recover_coordination_batch(
            &runtime.pool,
            metadata.expires_at_ms + 1,
            1,
        )
        .await?
        .dispositions,
        vec![RecoveryDisposition::AssignmentStranded]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM coordination_degradation_records \
             WHERE reason='stateLossDegraded' AND recovery_record_kind='assignment'",
        )
        .fetch_one(&*runtime.pool)
        .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn matching_stranded_degradation_without_outbox_fails_closed() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let RecordCoordinationCommandOutcome::Applied(metadata) = runtime
        .record_coordination_command_intent(assignment_command())
        .await?
    else {
        anyhow::bail!("unexpected duplicate command");
    };
    assert_eq!(
        super::recovery_batch::recover_coordination_batch(
            &runtime.pool,
            metadata.expires_at_ms,
            1,
        )
        .await?
        .dispositions,
        vec![RecoveryDisposition::CommandPayloadExpired]
    );
    let epoch = StateEpoch::parse(
        &sqlx::query_scalar::<_, String>(
            "SELECT state_epoch FROM coordination_authority WHERE singleton_id=1",
        )
        .fetch_one(&*runtime.pool)
        .await?,
    )?;
    let record_id = BoundedId::<MAX_ID_BYTES>::new(format!(
        "{}:{}",
        metadata.target.assignment_id,
        metadata.target.generation.get()
    ))?;
    let degradation = CheckedMaintenanceDegradation::new(
        metadata.root_thread_id,
        epoch,
        RecoveryRecordKind::Assignment,
        record_id,
        CoordinationSemanticSlot::AssignmentRequested,
        DegradationReason::StateLossDegraded,
        metadata.expires_at_ms,
        1,
    )?;
    sqlx::query(
        "INSERT INTO coordination_degradation_records \
         (degradation_id,root_thread_id,state_epoch,source_kind,source_shape,source_thread_id,\
          source_turn_id,source_item_id,source_ordinal,recovery_record_kind,recovery_record_id,\
          semantic_slot,reason,target_thread_id,target_turn_id,terminal_kind,terminal_outcome,\
          included_generations_bytes,identity_bytes,identity_fingerprint,canonical_record_bytes,\
          canonical_record_fingerprint,adapter_version,sanitizer_version,observed_at,\
          after_revision,created_at_ms) \
         VALUES (?,?,?,'recovery',NULL,NULL,NULL,NULL,NULL,'assignment',?,\
                 'assignmentRequested','stateLossDegraded',NULL,NULL,NULL,NULL,NULL,\
                 ?,?,?,?,1,1,?,1,?)",
    )
    .bind(degradation.degradation_id.to_string())
    .bind(metadata.root_thread_id.to_string())
    .bind(epoch.to_string())
    .bind(degradation.record_id.as_str())
    .bind(degradation.identity_bytes.as_slice())
    .bind(degradation.identity_bytes.fingerprint().as_slice())
    .bind(degradation.canonical_record_bytes.as_slice())
    .bind(degradation.canonical_record_bytes.fingerprint().as_slice())
    .bind(metadata.expires_at_ms)
    .bind(metadata.expires_at_ms)
    .execute(&*runtime.pool)
    .await?;

    assert!(matches!(
        super::recovery_batch::recover_coordination_batch(
            &runtime.pool,
            metadata.expires_at_ms,
            100,
        )
        .await,
        Err(RecoveryWriteError::CorruptState)
    ));
    assert_eq!(
        (
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
                .fetch_one(&*runtime.pool)
                .await?,
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM coordination_degradation_publication_outbox",
            )
            .fetch_one(&*runtime.pool)
            .await?,
        ),
        (2, 1)
    );
    Ok(())
}
