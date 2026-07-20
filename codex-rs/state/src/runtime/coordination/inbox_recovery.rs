use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::ReceiptId;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::maintenance_degradation::record_maintenance_degradation_in;
use super::recovery::RecoveryWriteError;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery_maintenance::CheckedMaintenanceDegradation;
use crate::model::coordination_recovery_maintenance::RecoveryRecordKind;

pub(super) async fn record_expired_payload_degradations(
    connection: &mut SqliteConnection,
    state_epoch: StateEpoch,
    receipts: &[ReceiptId],
    created_at_ms: i64,
) -> Result<(), RecoveryWriteError> {
    for receipt in receipts {
        let row = sqlx::query(
            "SELECT i.root_thread_id,i.operation_kind,i.lifecycle,i.expires_at_ms,e.revision \
             FROM coordination_inbox i JOIN coordination_events e \
             ON e.event_id=i.receipt_event_id WHERE i.receipt_id=?",
        )
        .bind(receipt.to_string())
        .fetch_optional(&mut *connection)
        .await
        .map_err(internal)?
        .ok_or(RecoveryWriteError::CorruptState)?;
        if row.get::<String, _>("lifecycle") != "expired" {
            continue;
        }
        let degradation = CheckedMaintenanceDegradation::new(
            ThreadId::try_from(row.get::<String, _>("root_thread_id"))
                .map_err(|_| RecoveryWriteError::CorruptState)?,
            state_epoch,
            RecoveryRecordKind::Inbox,
            BoundedId::<MAX_ID_BYTES>::new(receipt.to_string())
                .map_err(|_| RecoveryWriteError::CorruptState)?,
            inbox_slot(&row.get::<String, _>("operation_kind"))?,
            DegradationReason::ExpiredPayload,
            row.get("expires_at_ms"),
            row.get::<i64, _>("revision")
                .try_into()
                .map_err(|_| RecoveryWriteError::CorruptState)?,
        )
        .map_err(RecoveryWriteError::from)?;
        record_maintenance_degradation_in(connection, &degradation, created_at_ms).await?;
    }
    Ok(())
}

fn inbox_slot(value: &str) -> Result<CoordinationSemanticSlot, RecoveryWriteError> {
    match value {
        "assignmentSpawn" | "assignmentFollowup" => {
            Ok(CoordinationSemanticSlot::AssignmentAccepted)
        }
        "message" => Ok(CoordinationSemanticSlot::MessageDurablyReceived),
        "interrupt" => Ok(CoordinationSemanticSlot::InterruptDurablyReceived),
        _ => Err(RecoveryWriteError::CorruptState),
    }
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
