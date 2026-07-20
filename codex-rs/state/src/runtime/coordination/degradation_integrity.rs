use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::recovery::RecoveryWriteError;
use crate::model::coordination_recovery::DegradationId;

pub(super) async fn validate_degradation_outbox_in(
    connection: &mut SqliteConnection,
    degradation_id: DegradationId,
    root_thread_id: ThreadId,
    after_revision: u64,
    source_ordinal: u64,
) -> Result<(), RecoveryWriteError> {
    let row = sqlx::query(
        "SELECT root_thread_id,after_revision,source_ordinal,stable_record_id,status,version,\
         lease_epoch,retry_count,retry_after_ms,lease_expires_at_ms,failure_code,created_at_ms,\
         updated_at_ms FROM coordination_degradation_publication_outbox WHERE degradation_id=?",
    )
    .bind(degradation_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?
    .ok_or(RecoveryWriteError::CorruptState)?;
    let status: String = row.get("status");
    let lease_expires_at_ms = row.get::<Option<i64>, _>("lease_expires_at_ms");
    let failure_code = row.get::<Option<String>, _>("failure_code");
    let created_at_ms = row.get::<i64, _>("created_at_ms");
    if row.get::<String, _>("root_thread_id") != root_thread_id.to_string()
        || unsigned(row.get("after_revision"))? != after_revision
        || unsigned(row.get("source_ordinal"))? != source_ordinal
        || DegradationId::parse(&row.get::<String, _>("stable_record_id"))? != degradation_id
        || !matches!(
            status.as_str(),
            "pending" | "leased" | "materialized" | "poisoned"
        )
        || unsigned(row.get("version")).is_err()
        || unsigned(row.get("lease_epoch")).is_err()
        || unsigned(row.get::<i64, _>("retry_count"))? > 8
        || row.get::<i64, _>("retry_after_ms") < 0
        || created_at_ms < 0
        || row.get::<i64, _>("updated_at_ms") < created_at_ms
        || (status == "leased") != lease_expires_at_ms.is_some()
        || lease_expires_at_ms.is_some_and(|deadline| deadline < 0)
        || (status == "poisoned" && failure_code.is_none())
        || (matches!(status.as_str(), "leased" | "materialized") && failure_code.is_some())
    {
        return Err(RecoveryWriteError::CorruptState);
    }
    Ok(())
}

fn unsigned(value: i64) -> Result<u64, RecoveryWriteError> {
    value
        .try_into()
        .map_err(|_| RecoveryWriteError::CorruptState)
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
