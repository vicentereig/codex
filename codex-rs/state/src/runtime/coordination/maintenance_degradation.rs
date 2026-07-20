use sqlx::Row;
use sqlx::SqliteConnection;

use super::degradation_integrity::validate_degradation_outbox_in;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery::semantic_slot_sql;
use crate::model::coordination_recovery_maintenance::CheckedMaintenanceDegradation;
use crate::model::coordination_recovery_maintenance::RecoveryRecordKind;
use crate::model::coordination_recovery_maintenance::maintenance_fingerprint;

pub(super) async fn record_maintenance_degradation_in(
    connection: &mut SqliteConnection,
    degradation: &CheckedMaintenanceDegradation,
    created_at_ms: i64,
) -> Result<bool, RecoveryWriteError> {
    recovery_guard::validate_anchor(
        connection,
        &degradation.root_thread_id,
        degradation.after_revision,
    )
    .await?;
    let mut rows = sqlx::query(
        "SELECT degradation_id,root_thread_id,state_epoch,source_kind,source_shape,\
         source_thread_id,source_turn_id,source_item_id,source_ordinal,recovery_record_kind,\
         recovery_record_id,semantic_slot,reason,target_thread_id,target_turn_id,terminal_kind,\
         terminal_outcome,included_generations_bytes,identity_bytes,identity_fingerprint,\
         canonical_record_bytes,canonical_record_fingerprint,adapter_version,sanitizer_version,\
         observed_at,after_revision FROM coordination_degradation_records \
         WHERE degradation_id=? OR (root_thread_id=? AND identity_fingerprint=?)",
    )
    .bind(degradation.degradation_id.to_string())
    .bind(degradation.root_thread_id.to_string())
    .bind(degradation.identity_bytes.fingerprint().as_slice())
    .fetch_all(&mut *connection)
    .await
    .map_err(internal)?;
    if rows.len() > 1 {
        return Err(RecoveryWriteError::IdentityCollision);
    }
    if let Some(row) = rows.pop() {
        compare_existing(degradation, &row)?;
        validate_degradation_outbox_in(
            connection,
            degradation.degradation_id,
            degradation.root_thread_id,
            degradation.after_revision,
            0,
        )
        .await?;
        return Ok(false);
    }
    sqlx::query(
        "INSERT INTO coordination_degradation_records \
         (degradation_id,root_thread_id,state_epoch,source_kind,source_shape,source_thread_id,\
          source_turn_id,source_item_id,source_ordinal,recovery_record_kind,recovery_record_id,\
          semantic_slot,reason,target_thread_id,target_turn_id,terminal_kind,terminal_outcome,\
          included_generations_bytes,identity_bytes,identity_fingerprint,canonical_record_bytes,\
          canonical_record_fingerprint,adapter_version,sanitizer_version,observed_at,\
          after_revision,created_at_ms) \
         VALUES (?,?,?,'recovery',NULL,NULL,NULL,NULL,NULL,?,?,?, ?,NULL,NULL,NULL,NULL,NULL,\
                 ?,?,?,?,1,1,?,?,?)",
    )
    .bind(degradation.degradation_id.to_string())
    .bind(degradation.root_thread_id.to_string())
    .bind(degradation.state_epoch.to_string())
    .bind(record_kind_sql(degradation.record_kind))
    .bind(degradation.record_id.as_str())
    .bind(semantic_slot_sql(degradation.semantic_slot))
    .bind(reason_sql(degradation.reason))
    .bind(degradation.identity_bytes.as_slice())
    .bind(degradation.identity_bytes.fingerprint().as_slice())
    .bind(degradation.canonical_record_bytes.as_slice())
    .bind(degradation.canonical_record_bytes.fingerprint().as_slice())
    .bind(degradation.observed_at)
    .bind(degradation.after_revision as i64)
    .bind(created_at_ms.max(0))
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    sqlx::query(
        "INSERT INTO coordination_degradation_publication_outbox \
         (degradation_id,root_thread_id,after_revision,source_ordinal,stable_record_id,status,\
          version,lease_epoch,retry_count,retry_after_ms,lease_expires_at_ms,failure_code,\
          created_at_ms,updated_at_ms) VALUES (?,?,?,0,?,'pending',0,0,0,0,NULL,NULL,?,?)",
    )
    .bind(degradation.degradation_id.to_string())
    .bind(degradation.root_thread_id.to_string())
    .bind(degradation.after_revision as i64)
    .bind(degradation.degradation_id.to_string())
    .bind(created_at_ms.max(0))
    .bind(created_at_ms.max(0))
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    Ok(true)
}

fn compare_existing(
    degradation: &CheckedMaintenanceDegradation,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<(), RecoveryWriteError> {
    let identity: Vec<u8> = row.get("identity_bytes");
    let identity_fingerprint: Vec<u8> = row.get("identity_fingerprint");
    let canonical: Vec<u8> = row.get("canonical_record_bytes");
    let canonical_fingerprint: Vec<u8> = row.get("canonical_record_fingerprint");
    if identity_fingerprint.as_slice() != maintenance_fingerprint(&identity)
        || canonical_fingerprint.as_slice() != maintenance_fingerprint(&canonical)
    {
        return Err(RecoveryWriteError::CorruptState);
    }
    if identity != degradation.identity_bytes.as_slice()
        || row.get::<String, _>("degradation_id") != degradation.degradation_id.to_string()
        || row.get::<String, _>("root_thread_id") != degradation.root_thread_id.to_string()
        || row.get::<Option<String>, _>("state_epoch") != Some(degradation.state_epoch.to_string())
    {
        return Err(RecoveryWriteError::IdentityCollision);
    }
    if canonical != degradation.canonical_record_bytes.as_slice() {
        return Err(RecoveryWriteError::DivergentObservation);
    }
    if row.get::<String, _>("source_kind") != "recovery"
        || row.get::<Option<String>, _>("source_shape").is_some()
        || row.get::<Option<String>, _>("source_thread_id").is_some()
        || row.get::<Option<String>, _>("source_turn_id").is_some()
        || row.get::<Option<String>, _>("source_item_id").is_some()
        || row.get::<Option<i64>, _>("source_ordinal").is_some()
        || row.get::<String, _>("recovery_record_kind") != record_kind_sql(degradation.record_kind)
        || row.get::<String, _>("recovery_record_id") != degradation.record_id.as_str()
        || row.get::<String, _>("semantic_slot") != semantic_slot_sql(degradation.semantic_slot)
        || row.get::<String, _>("reason") != reason_sql(degradation.reason)
        || row.get::<Option<String>, _>("target_thread_id").is_some()
        || row.get::<Option<String>, _>("target_turn_id").is_some()
        || row.get::<Option<String>, _>("terminal_kind").is_some()
        || row.get::<Option<String>, _>("terminal_outcome").is_some()
        || row
            .get::<Option<Vec<u8>>, _>("included_generations_bytes")
            .is_some()
        || row.get::<i64, _>("adapter_version") != 1
        || row.get::<i64, _>("sanitizer_version") != 1
        || row.get::<i64, _>("observed_at") != degradation.observed_at
        || row.get::<i64, _>("after_revision") != degradation.after_revision as i64
    {
        return Err(RecoveryWriteError::CorruptState);
    }
    Ok(())
}

fn record_kind_sql(kind: RecoveryRecordKind) -> &'static str {
    match kind {
        RecoveryRecordKind::Assignment => "assignment",
        RecoveryRecordKind::Command => "command",
        RecoveryRecordKind::Inbox => "inbox",
    }
}

fn reason_sql(reason: DegradationReason) -> &'static str {
    match reason {
        DegradationReason::CoordinationTemporarilyUnavailable => {
            "coordinationTemporarilyUnavailable"
        }
        DegradationReason::MissingProvenance => "missingProvenance",
        DegradationReason::AmbiguousSource => "ambiguousSource",
        DegradationReason::OverLimit => "overLimit",
        DegradationReason::InvalidLegacyValue => "invalidLegacyValue",
        DegradationReason::CorruptSource => "corruptSource",
        DegradationReason::PoisonedAttempt => "poisonedAttempt",
        DegradationReason::ExpiredPayload => "expiredPayload",
        DegradationReason::StateLossDegraded => "stateLossDegraded",
    }
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
