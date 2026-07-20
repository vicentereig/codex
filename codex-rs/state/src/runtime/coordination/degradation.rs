use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;
use sqlx::SqliteConnection;
use sqlx::SqlitePool;

use super::degradation_integrity::validate_degradation_outbox_in;
use super::recovery::NoRecoveryFailure;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_recovery::CheckedExogenousTerminalObservation;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery::DegradationRecord;
use crate::model::coordination_recovery::DegradationSourceKind;
use crate::model::coordination_recovery::ExogenousTerminalObservation;
use crate::model::coordination_recovery::RecordExogenousTerminalOutcome;
use crate::model::coordination_recovery::TerminalEvidenceKind;
use crate::model::coordination_recovery::TerminalEvidenceOutcome;
use crate::model::coordination_recovery::semantic_slot_sql;
use crate::model::coordination_recovery::source_shape_sql;

pub(crate) async fn record_exogenous_terminal_degradation(
    pool: &SqlitePool,
    observation: ExogenousTerminalObservation,
) -> Result<RecordExogenousTerminalOutcome, RecoveryWriteError> {
    record_exogenous_terminal_degradation_with(pool, observation, &NoRecoveryFailure).await
}

pub(super) async fn record_exogenous_terminal_degradation_with(
    pool: &SqlitePool,
    observation: ExogenousTerminalObservation,
    injector: &dyn RecoveryFailureInjector,
) -> Result<RecordExogenousTerminalOutcome, RecoveryWriteError> {
    let Some(observation) = observation.check()? else {
        return Ok(RecordExogenousTerminalOutcome::UnknownProvenance);
    };
    let mut connection = recovery_guard::begin_with(pool, injector).await?;
    let result = record_checked(&mut connection, &observation, injector).await;
    recovery_guard::finish_with(connection, result, injector).await
}

pub(super) async fn record_checked(
    connection: &mut SqliteConnection,
    observation: &CheckedExogenousTerminalObservation,
    injector: &dyn RecoveryFailureInjector,
) -> Result<RecordExogenousTerminalOutcome, RecoveryWriteError> {
    recovery_guard::active_authority_with(
        connection,
        &observation.root_thread_id,
        observation.captured_state_epoch,
        injector,
    )
    .await?;
    recovery_guard::validate_anchor_with(
        connection,
        &observation.root_thread_id,
        observation.after_revision,
        injector,
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
    .bind(observation.degradation_id.to_string())
    .bind(observation.root_thread_id.to_string())
    .bind(observation.identity_bytes.fingerprint().as_slice())
    .fetch_all(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_recovery_step(RecoveryStep::LegacyRead)
        .map_err(RecoveryWriteError::Internal)?;
    if rows.len() > 1 {
        return Err(RecoveryWriteError::IdentityCollision);
    }
    if let Some(row) = rows.pop() {
        compare_existing(observation, &row)?;
        validate_degradation_outbox_in(
            connection,
            observation.degradation_id,
            observation.root_thread_id,
            observation.after_revision,
            observation.source.source_ordinal,
        )
        .await?;
        injector
            .after_recovery_step(RecoveryStep::PublicationRead)
            .map_err(RecoveryWriteError::Internal)?;
        return Ok(RecordExogenousTerminalOutcome::Duplicate(record(
            observation,
        )));
    }
    let now = chrono::Utc::now().timestamp_millis().max(0);
    let included = serde_json::to_vec(&observation.included_generations)
        .map_err(|_| RecoveryWriteError::CorruptState)?;
    sqlx::query(
        "INSERT INTO coordination_degradation_records \
         (degradation_id,root_thread_id,state_epoch,source_kind,source_shape,source_thread_id,\
          source_turn_id,source_item_id,source_ordinal,semantic_slot,reason,target_thread_id,\
          target_turn_id,terminal_kind,terminal_outcome,included_generations_bytes,identity_bytes,\
          identity_fingerprint,canonical_record_bytes,canonical_record_fingerprint,adapter_version,\
          sanitizer_version,observed_at,after_revision,created_at_ms) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,1,?,?,?)",
    )
    .bind(observation.degradation_id.to_string())
    .bind(observation.root_thread_id.to_string())
    .bind(
        observation
            .captured_state_epoch
            .map(|epoch| epoch.to_string()),
    )
    .bind(source_kind_sql(DegradationSourceKind::ExogenousTerminal))
    .bind(source_shape_sql(observation.source.shape))
    .bind(observation.source.source_thread_id.map(|id| id.to_string()))
    .bind(
        observation
            .source
            .source_turn_id
            .as_ref()
            .map(codex_coordination::BoundedId::as_str),
    )
    .bind(
        observation
            .source
            .source_item_id
            .as_ref()
            .map(codex_coordination::BoundedId::as_str),
    )
    .bind(observation.source.source_ordinal as i64)
    .bind(semantic_slot_sql(observation.source.semantic_slot))
    .bind(reason_sql(
        DegradationReason::CoordinationTemporarilyUnavailable,
    ))
    .bind(observation.target_thread_id.to_string())
    .bind(observation.target_turn_id.as_str())
    .bind(terminal_kind_sql(observation.terminal_kind))
    .bind(terminal_outcome_sql(observation.terminal_outcome))
    .bind(included)
    .bind(observation.identity_bytes.as_slice())
    .bind(observation.identity_bytes.fingerprint().as_slice())
    .bind(observation.canonical_record_bytes.as_slice())
    .bind(observation.canonical_record_bytes.fingerprint().as_slice())
    .bind(observation.observed_at)
    .bind(observation.after_revision as i64)
    .bind(now)
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
    .bind(observation.degradation_id.to_string())
    .bind(observation.root_thread_id.to_string())
    .bind(observation.after_revision as i64)
    .bind(observation.source.source_ordinal as i64)
    .bind(observation.degradation_id.to_string())
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_recovery_step(RecoveryStep::DegradationOutboxInsert)
        .map_err(RecoveryWriteError::Internal)?;
    Ok(RecordExogenousTerminalOutcome::Applied(record(observation)))
}

fn compare_existing(
    observation: &CheckedExogenousTerminalObservation,
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
    if identity_fingerprint.as_slice() != observation.identity_bytes.fingerprint()
        || identity != observation.identity_bytes.as_slice()
        || row.get::<String, _>("degradation_id") != observation.degradation_id.to_string()
        || row.get::<String, _>("root_thread_id") != observation.root_thread_id.to_string()
        || row.get::<Option<String>, _>("state_epoch")
            != observation
                .captured_state_epoch
                .map(|epoch| epoch.to_string())
    {
        return Err(RecoveryWriteError::IdentityCollision);
    }
    if canonical_fingerprint.as_slice() != observation.canonical_record_bytes.fingerprint()
        || canonical != observation.canonical_record_bytes.as_slice()
    {
        return Err(RecoveryWriteError::DivergentObservation);
    }
    let included = serde_json::to_vec(&observation.included_generations)
        .map_err(|_| RecoveryWriteError::CorruptState)?;
    if row.get::<String, _>("source_kind") != "exogenousTerminal"
        || row.get::<String, _>("source_shape") != source_shape_sql(observation.source.shape)
        || row.get::<Option<String>, _>("source_thread_id")
            != observation.source.source_thread_id.map(|id| id.to_string())
        || row.get::<Option<String>, _>("source_turn_id")
            != observation
                .source
                .source_turn_id
                .as_ref()
                .map(|id| id.as_str().to_owned())
        || row.get::<Option<String>, _>("source_item_id")
            != observation
                .source
                .source_item_id
                .as_ref()
                .map(|id| id.as_str().to_owned())
        || row.get::<i64, _>("source_ordinal") != observation.source.source_ordinal as i64
        || row
            .get::<Option<String>, _>("recovery_record_kind")
            .is_some()
        || row.get::<Option<String>, _>("recovery_record_id").is_some()
        || row.get::<String, _>("semantic_slot")
            != semantic_slot_sql(observation.source.semantic_slot)
        || row.get::<String, _>("reason") != "coordinationTemporarilyUnavailable"
        || row.get::<String, _>("target_thread_id") != observation.target_thread_id.to_string()
        || row.get::<String, _>("target_turn_id") != observation.target_turn_id.as_str()
        || row.get::<String, _>("terminal_kind") != terminal_kind_sql(observation.terminal_kind)
        || row.get::<String, _>("terminal_outcome")
            != terminal_outcome_sql(observation.terminal_outcome)
        || row.get::<Vec<u8>, _>("included_generations_bytes") != included
        || row.get::<i64, _>("adapter_version") != 1
        || row.get::<i64, _>("sanitizer_version") != 1
        || row.get::<i64, _>("observed_at") != observation.observed_at
        || row.get::<i64, _>("after_revision") != observation.after_revision as i64
    {
        return Err(RecoveryWriteError::CorruptState);
    }
    Ok(())
}

fn record(observation: &CheckedExogenousTerminalObservation) -> DegradationRecord {
    DegradationRecord {
        degradation_id: observation.degradation_id,
        root_thread_id: observation.root_thread_id,
        state_epoch: observation.captured_state_epoch,
        source_kind: DegradationSourceKind::ExogenousTerminal,
        source: observation.source.clone(),
        reason: DegradationReason::CoordinationTemporarilyUnavailable,
        after_revision: observation.after_revision,
    }
}

fn source_kind_sql(kind: DegradationSourceKind) -> &'static str {
    match kind {
        DegradationSourceKind::ExogenousTerminal => "exogenousTerminal",
        DegradationSourceKind::LegacyReduction => "legacyReduction",
        DegradationSourceKind::Recovery => "recovery",
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

fn terminal_kind_sql(kind: TerminalEvidenceKind) -> &'static str {
    match kind {
        TerminalEvidenceKind::Completed => "completed",
        TerminalEvidenceKind::Interrupted => "interrupted",
    }
}

fn terminal_outcome_sql(outcome: TerminalEvidenceOutcome) -> &'static str {
    match outcome {
        TerminalEvidenceOutcome::Succeeded => "succeeded",
        TerminalEvidenceOutcome::Failed => "failed",
        TerminalEvidenceOutcome::Cancelled => "cancelled",
        TerminalEvidenceOutcome::Interrupted => "interrupted",
        TerminalEvidenceOutcome::Unknown => "unknown",
    }
}

fn sha256(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
