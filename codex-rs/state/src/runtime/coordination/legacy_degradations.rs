use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::degradation_integrity::validate_degradation_outbox_in;
use super::recovery::NoRecoveryFailure;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_legacy_degradation::CheckedLegacyReductionDegradation;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery::semantic_slot_sql;
use crate::model::coordination_recovery::source_shape_sql;

pub(super) async fn record_legacy_degradation_in(
    connection: &mut SqliteConnection,
    degradation: &CheckedLegacyReductionDegradation,
    created_at_ms: i64,
) -> Result<bool, RecoveryWriteError> {
    record_legacy_degradation_in_with(connection, degradation, created_at_ms, &NoRecoveryFailure)
        .await
}

pub(super) async fn record_legacy_degradation_in_with(
    connection: &mut SqliteConnection,
    degradation: &CheckedLegacyReductionDegradation,
    created_at_ms: i64,
    injector: &dyn RecoveryFailureInjector,
) -> Result<bool, RecoveryWriteError> {
    recovery_guard::validate_anchor_with(
        connection,
        &degradation.root_thread_id,
        degradation.after_revision,
        injector,
    )
    .await?;
    let existing = existing(connection, degradation).await?;
    injector
        .after_recovery_step(RecoveryStep::LegacyRead)
        .map_err(RecoveryWriteError::Internal)?;
    if let Some(row) = existing {
        compare_existing(degradation, &row)?;
        validate_degradation_outbox_in(
            connection,
            degradation.degradation_id,
            degradation.root_thread_id,
            degradation.after_revision,
            degradation.source.source_ordinal,
        )
        .await?;
        injector
            .after_recovery_step(RecoveryStep::PublicationRead)
            .map_err(RecoveryWriteError::Internal)?;
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
         VALUES (?,?,?,'legacyReduction',?,?,?,?,?,NULL,NULL,?,?,NULL,NULL,NULL,NULL,NULL,\
                 ?,?,?,?,1,1,?,?,?)",
    )
    .bind(degradation.degradation_id.to_string())
    .bind(degradation.root_thread_id.to_string())
    .bind(degradation.state_epoch.to_string())
    .bind(source_shape_sql(degradation.source.shape))
    .bind(degradation.source.source_thread_id.map(|id| id.to_string()))
    .bind(
        degradation
            .source
            .source_turn_id
            .as_ref()
            .map(codex_coordination::BoundedId::as_str),
    )
    .bind(
        degradation
            .source
            .source_item_id
            .as_ref()
            .map(codex_coordination::BoundedId::as_str),
    )
    .bind(degradation.source.source_ordinal as i64)
    .bind(semantic_slot_sql(degradation.source.semantic_slot))
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
    injector
        .after_recovery_step(RecoveryStep::DegradationInsert)
        .map_err(RecoveryWriteError::Internal)?;
    sqlx::query(
        "INSERT INTO coordination_degradation_publication_outbox \
         (degradation_id,root_thread_id,after_revision,source_ordinal,stable_record_id,status,\
          version,lease_epoch,retry_count,retry_after_ms,lease_expires_at_ms,failure_code,\
          created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,'pending',0,0,0,0,NULL,NULL,?,?)",
    )
    .bind(degradation.degradation_id.to_string())
    .bind(degradation.root_thread_id.to_string())
    .bind(degradation.after_revision as i64)
    .bind(degradation.source.source_ordinal as i64)
    .bind(degradation.degradation_id.to_string())
    .bind(created_at_ms.max(0))
    .bind(created_at_ms.max(0))
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_recovery_step(RecoveryStep::DegradationOutboxInsert)
        .map_err(RecoveryWriteError::Internal)?;
    Ok(true)
}

pub(super) async fn validate_existing_legacy_degradation_in(
    connection: &mut SqliteConnection,
    degradation: &CheckedLegacyReductionDegradation,
    injector: &dyn RecoveryFailureInjector,
) -> Result<(), RecoveryWriteError> {
    let row = existing(connection, degradation)
        .await?
        .ok_or(RecoveryWriteError::CorruptState)?;
    injector
        .after_recovery_step(RecoveryStep::LegacyRead)
        .map_err(RecoveryWriteError::Internal)?;
    compare_existing(degradation, &row)?;
    validate_degradation_outbox_in(
        connection,
        degradation.degradation_id,
        degradation.root_thread_id,
        degradation.after_revision,
        degradation.source.source_ordinal,
    )
    .await?;
    injector
        .after_recovery_step(RecoveryStep::PublicationRead)
        .map_err(RecoveryWriteError::Internal)
}

async fn existing(
    connection: &mut SqliteConnection,
    degradation: &CheckedLegacyReductionDegradation,
) -> Result<Option<sqlx::sqlite::SqliteRow>, RecoveryWriteError> {
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
    Ok(rows.pop())
}

fn compare_existing(
    degradation: &CheckedLegacyReductionDegradation,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<(), RecoveryWriteError> {
    let identity: Vec<u8> = row.get("identity_bytes");
    let identity_fingerprint: Vec<u8> = row.get("identity_fingerprint");
    let canonical: Vec<u8> = row.get("canonical_record_bytes");
    let canonical_fingerprint: Vec<u8> = row.get("canonical_record_fingerprint");
    if identity_fingerprint.as_slice() != sha256(&identity)
        || canonical_fingerprint.as_slice() != sha256(&canonical)
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
        return Err(RecoveryWriteError::DivergentReduction);
    }
    if row.get::<String, _>("source_kind") != "legacyReduction"
        || row.get::<String, _>("source_shape") != source_shape_sql(degradation.source.shape)
        || row.get::<Option<String>, _>("source_thread_id")
            != degradation.source.source_thread_id.map(|id| id.to_string())
        || row.get::<Option<String>, _>("source_turn_id")
            != degradation
                .source
                .source_turn_id
                .as_ref()
                .map(|id| id.as_str().to_owned())
        || row.get::<Option<String>, _>("source_item_id")
            != degradation
                .source
                .source_item_id
                .as_ref()
                .map(|id| id.as_str().to_owned())
        || row.get::<i64, _>("source_ordinal") != degradation.source.source_ordinal as i64
        || row
            .get::<Option<String>, _>("recovery_record_kind")
            .is_some()
        || row.get::<Option<String>, _>("recovery_record_id").is_some()
        || row.get::<String, _>("semantic_slot")
            != semantic_slot_sql(degradation.source.semantic_slot)
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

fn reason_sql(reason: DegradationReason) -> &'static str {
    match reason {
        DegradationReason::AmbiguousSource => "ambiguousSource",
        DegradationReason::OverLimit => "overLimit",
        DegradationReason::InvalidLegacyValue => "invalidLegacyValue",
        DegradationReason::CorruptSource => "corruptSource",
        DegradationReason::StateLossDegraded => "stateLossDegraded",
        DegradationReason::CoordinationTemporarilyUnavailable
        | DegradationReason::MissingProvenance
        | DegradationReason::PoisonedAttempt
        | DegradationReason::ExpiredPayload => unreachable!("checked legacy reason"),
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
