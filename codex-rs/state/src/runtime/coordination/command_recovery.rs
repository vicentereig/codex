use sqlx::Row;
use sqlx::SqliteConnection;

use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;

use super::maintenance_degradation::record_maintenance_degradation_in;
use super::recovery::RecoveryWriteError;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery_maintenance::CheckedMaintenanceDegradation;
use crate::model::coordination_recovery_maintenance::RecoveryRecordKind;

const TERMINAL_PAYLOAD_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

pub(super) async fn expire_payloads(
    connection: &mut SqliteConnection,
    state_epoch: StateEpoch,
    now_ms: i64,
    limit: u32,
) -> Result<u64, RecoveryWriteError> {
    let candidates = sqlx::query(
        "SELECT c.operation_id,c.root_thread_id,c.operation_kind,c.lifecycle,c.version,\
         c.expires_at_ms,e.revision FROM coordination_commands c JOIN coordination_events e \
         ON e.event_id=c.intent_event_id WHERE c.ciphertext IS NOT NULL AND c.expires_at_ms<=? \
         ORDER BY c.expires_at_ms,c.operation_id LIMIT ?",
    )
    .bind(now_ms)
    .bind(i64::from(limit))
    .fetch_all(&mut *connection)
    .await
    .map_err(internal)?;
    let mut changed = 0;
    for candidate in candidates {
        let lifecycle: String = candidate.get("lifecycle");
        let terminal = matches!(lifecycle.as_str(), "succeeded" | "poisoned");
        let result = sqlx::query(
            "UPDATE coordination_commands SET lifecycle=CASE WHEN lifecycle IN \
             ('succeeded','poisoned') THEN lifecycle ELSE 'expired' END,version=version+1,\
             lease_expires_at_ms=NULL,ciphertext=NULL,purged_at_ms=MAX(intent_at_ms,?),\
             updated_at_ms=MAX(updated_at_ms,?) WHERE operation_id=? AND version=? \
             AND lifecycle=? AND ciphertext IS NOT NULL AND expires_at_ms<=?",
        )
        .bind(now_ms.max(0))
        .bind(now_ms.max(0))
        .bind(candidate.get::<String, _>("operation_id"))
        .bind(candidate.get::<i64, _>("version"))
        .bind(&lifecycle)
        .bind(now_ms)
        .execute(&mut *connection)
        .await
        .map_err(internal)?;
        if result.rows_affected() != 1 {
            return Err(RecoveryWriteError::Deferred);
        }
        if !terminal {
            let degradation = maintenance_degradation(
                &candidate,
                state_epoch,
                DegradationReason::ExpiredPayload,
                candidate.get("expires_at_ms"),
            )?;
            record_maintenance_degradation_in(connection, &degradation, now_ms).await?;
        }
        changed += 1;
    }
    Ok(changed)
}

pub(super) async fn poison_uncertain_attempts(
    connection: &mut SqliteConnection,
    state_epoch: StateEpoch,
    now_ms: i64,
    limit: u32,
) -> Result<u64, RecoveryWriteError> {
    let candidates = sqlx::query(
        "SELECT c.operation_id,c.root_thread_id,c.operation_kind,c.version,c.lease_epoch,\
         c.lease_expires_at_ms,e.revision FROM coordination_commands c \
         JOIN coordination_events e ON e.event_id=c.intent_event_id \
         WHERE lifecycle='leased' AND attempted_lease_epoch=lease_epoch \
         AND lease_expires_at_ms<=? \
         ORDER BY lease_expires_at_ms,operation_id LIMIT ?",
    )
    .bind(now_ms)
    .bind(i64::from(limit))
    .fetch_all(&mut *connection)
    .await
    .map_err(internal)?;
    let mut changed = 0;
    for candidate in candidates {
        let result = sqlx::query(
            "UPDATE coordination_commands SET lifecycle='poisoned',version=version+1,\
             lease_expires_at_ms=NULL,failure_code='internal',terminal_at_ms=?,\
             expires_at_ms=MIN(expires_at_ms,? + ?),\
             ciphertext=CASE WHEN expires_at_ms<=? THEN NULL ELSE ciphertext END,\
             purged_at_ms=CASE WHEN expires_at_ms<=? THEN ? ELSE purged_at_ms END,updated_at_ms=? \
             WHERE operation_id=? AND lifecycle='leased' AND version=? AND lease_epoch=? \
             AND attempted_lease_epoch=lease_epoch AND lease_expires_at_ms<=?",
        )
        .bind(now_ms.max(0))
        .bind(now_ms.max(0))
        .bind(TERMINAL_PAYLOAD_TTL_MS)
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms.max(0))
        .bind(now_ms.max(0))
        .bind(candidate.get::<String, _>("operation_id"))
        .bind(candidate.get::<i64, _>("version"))
        .bind(candidate.get::<i64, _>("lease_epoch"))
        .bind(now_ms)
        .execute(&mut *connection)
        .await
        .map_err(internal)?;
        if result.rows_affected() != 1 {
            return Err(RecoveryWriteError::Deferred);
        }
        let degradation = maintenance_degradation(
            &candidate,
            state_epoch,
            DegradationReason::PoisonedAttempt,
            candidate.get("lease_expires_at_ms"),
        )?;
        record_maintenance_degradation_in(connection, &degradation, now_ms).await?;
        changed += 1;
    }
    Ok(changed)
}

fn maintenance_degradation(
    row: &sqlx::sqlite::SqliteRow,
    state_epoch: StateEpoch,
    reason: DegradationReason,
    observed_at: i64,
) -> Result<CheckedMaintenanceDegradation, RecoveryWriteError> {
    CheckedMaintenanceDegradation::new(
        ThreadId::try_from(row.get::<String, _>("root_thread_id"))
            .map_err(|_| RecoveryWriteError::CorruptState)?,
        state_epoch,
        RecoveryRecordKind::Command,
        BoundedId::<MAX_ID_BYTES>::new(row.get::<String, _>("operation_id"))
            .map_err(|_| RecoveryWriteError::CorruptState)?,
        command_slot(&row.get::<String, _>("operation_kind"))?,
        reason,
        observed_at,
        row.get::<i64, _>("revision")
            .try_into()
            .map_err(|_| RecoveryWriteError::CorruptState)?,
    )
    .map_err(RecoveryWriteError::from)
}

fn command_slot(value: &str) -> Result<CoordinationSemanticSlot, RecoveryWriteError> {
    match value {
        "assignmentSpawn" | "assignmentFollowup" => {
            Ok(CoordinationSemanticSlot::AssignmentRequested)
        }
        "message" => Ok(CoordinationSemanticSlot::MessageSubmissionRecorded),
        "interrupt" => Ok(CoordinationSemanticSlot::InterruptRequested),
        _ => Err(RecoveryWriteError::CorruptState),
    }
}

pub(super) async fn reclaim_safe_leases(
    connection: &mut SqliteConnection,
    now_ms: i64,
    limit: u32,
) -> Result<u64, RecoveryWriteError> {
    let result = sqlx::query(
        "UPDATE coordination_commands SET lifecycle='pending',version=version+1,\
         lease_expires_at_ms=NULL,updated_at_ms=MAX(updated_at_ms,?) \
         WHERE operation_id IN (SELECT operation_id FROM coordination_commands \
         WHERE lifecycle='leased' AND lease_expires_at_ms<=? AND expires_at_ms>? \
         AND (attempted_lease_epoch IS NULL OR attempted_lease_epoch<lease_epoch) \
         ORDER BY lease_expires_at_ms,operation_id LIMIT ?)",
    )
    .bind(now_ms.max(0))
    .bind(now_ms)
    .bind(now_ms)
    .bind(i64::from(limit))
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    Ok(result.rows_affected())
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
