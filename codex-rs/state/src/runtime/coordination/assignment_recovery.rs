use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::maintenance_degradation::record_maintenance_degradation_in_with;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery_maintenance::CheckedMaintenanceDegradation;
use crate::model::coordination_recovery_maintenance::RecoveryRecordKind;

pub(super) async fn classify_stranded_assignments(
    connection: &mut SqliteConnection,
    state_epoch: StateEpoch,
    now_ms: i64,
    limit: u32,
    injector: &dyn RecoveryFailureInjector,
) -> Result<u64, RecoveryWriteError> {
    let candidates = sqlx::query(
        "SELECT g.assignment_id,g.generation,g.created_revision,h.root_thread_id,\
         COALESCE(c.terminal_at_ms,c.purged_at_ms,c.expires_at_ms) observed_at,\
         d.degradation_id existing_degradation_id \
         FROM coordination_assignment_generations g \
         JOIN coordination_assignment_heads h ON h.assignment_id=g.assignment_id \
         JOIN coordination_commands c ON c.operation_id=g.operation_id \
         LEFT JOIN coordination_degradation_records d \
           ON d.root_thread_id=h.root_thread_id AND d.source_kind='recovery' \
             AND d.recovery_record_kind='assignment' \
             AND d.recovery_record_id=g.assignment_id||':'||g.generation \
             AND d.reason='stateLossDegraded' \
         WHERE g.lifecycle='reserved' AND c.lifecycle IN ('poisoned','expired') \
         ORDER BY d.degradation_id IS NOT NULL,g.created_revision,g.assignment_id,g.generation \
         LIMIT ?",
    )
    .bind(i64::from(limit))
    .fetch_all(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_recovery_step(RecoveryStep::RecoveryRead)
        .map_err(RecoveryWriteError::Internal)?;
    let mut changed = 0;
    for candidate in candidates {
        let degradation = CheckedMaintenanceDegradation::new(
            ThreadId::try_from(candidate.get::<String, _>("root_thread_id"))
                .map_err(|_| RecoveryWriteError::CorruptState)?,
            state_epoch,
            RecoveryRecordKind::Assignment,
            BoundedId::<MAX_ID_BYTES>::new(format!(
                "{}:{}",
                candidate.get::<String, _>("assignment_id"),
                candidate.get::<i64, _>("generation")
            ))
            .map_err(|_| RecoveryWriteError::CorruptState)?,
            CoordinationSemanticSlot::AssignmentRequested,
            DegradationReason::StateLossDegraded,
            candidate.get("observed_at"),
            candidate
                .get::<i64, _>("created_revision")
                .try_into()
                .map_err(|_| RecoveryWriteError::CorruptState)?,
        )?;
        if candidate
            .get::<Option<String>, _>("existing_degradation_id")
            .is_some_and(|id| id != degradation.degradation_id.to_string())
        {
            return Err(RecoveryWriteError::CorruptState);
        }
        if record_maintenance_degradation_in_with(connection, &degradation, now_ms, injector)
            .await?
        {
            changed += 1;
        }
    }
    Ok(changed)
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
