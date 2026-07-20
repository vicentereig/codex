use codex_coordination::CoordinationEventId;
use sqlx::Row;
use sqlx::SqliteConnection;
use sqlx::SqlitePool;

use super::recovery::NoRecoveryFailure;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_recovery::CheckedLegacyLink;
use crate::model::coordination_recovery::LegacyLinkRecord;
use crate::model::coordination_recovery::RecordLegacyLinkOutcome;
use crate::model::coordination_recovery::semantic_slot_sql;
use crate::model::coordination_recovery::source_shape_sql;

pub(crate) async fn record_legacy_link(
    pool: &SqlitePool,
    link: &CheckedLegacyLink,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    record_legacy_link_with(pool, link, &NoRecoveryFailure).await
}

pub(super) async fn record_legacy_link_with(
    pool: &SqlitePool,
    link: &CheckedLegacyLink,
    injector: &dyn RecoveryFailureInjector,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    let mut connection = recovery_guard::begin_with(pool, injector).await?;
    let result = record_legacy_link_in_with(&mut connection, link, injector).await;
    recovery_guard::finish_with(connection, result, injector).await
}

pub(super) async fn record_legacy_link_in(
    connection: &mut SqliteConnection,
    link: &CheckedLegacyLink,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    record_legacy_link_in_with(connection, link, &NoRecoveryFailure).await
}

pub(super) async fn record_legacy_link_in_with(
    connection: &mut SqliteConnection,
    link: &CheckedLegacyLink,
    injector: &dyn RecoveryFailureInjector,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    recovery_guard::active_authority_with(
        connection,
        &link.root_thread_id,
        Some(link.expected_state_epoch),
        injector,
    )
    .await?;
    recovery_guard::validate_anchor_with(
        connection,
        &link.root_thread_id,
        link.after_revision,
        injector,
    )
    .await?;
    let mut rows = sqlx::query(
        "SELECT compatibility_event_id,root_thread_id,state_epoch,source_shape,source_thread_id,\
         source_turn_id,source_item_id,source_ordinal,semantic_slot,source_identity_bytes,\
         source_identity_fingerprint,canonical_event_bytes,canonical_event_fingerprint,\
         adapter_version,sanitizer_version,after_revision,suppressed_by_native_event_id \
         FROM coordination_legacy_links \
         WHERE compatibility_event_id=? OR (root_thread_id=? AND source_identity_fingerprint=?)",
    )
    .bind(link.compatibility_event_id.to_string())
    .bind(link.root_thread_id.to_string())
    .bind(link.source_identity_bytes.fingerprint().as_slice())
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
        let existing = compare_existing(link, &row)?;
        return apply_requested_suppression(connection, link, existing, injector).await;
    }
    validate_native_suppression(connection, link, injector).await?;
    let now = injector.now_ms();
    let (suppression_id, suppressed_at_ms) = link
        .native_suppression
        .map(|suppression| {
            (
                Some(suppression.event_id.to_string()),
                Some(suppression.suppressed_at_ms),
            )
        })
        .unwrap_or((None, None));
    sqlx::query(
        "INSERT INTO coordination_legacy_links \
         (compatibility_event_id,root_thread_id,state_epoch,source_shape,source_thread_id,\
          source_turn_id,source_item_id,source_ordinal,semantic_slot,source_identity_bytes,\
          source_identity_fingerprint,canonical_event_bytes,canonical_event_fingerprint,\
          adapter_version,sanitizer_version,after_revision,suppressed_by_native_event_id,\
          suppressed_at_ms,created_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,1,?,?,?,?)",
    )
    .bind(link.compatibility_event_id.to_string())
    .bind(link.root_thread_id.to_string())
    .bind(link.expected_state_epoch.to_string())
    .bind(source_shape_sql(link.source.shape))
    .bind(link.source.source_thread_id.map(|id| id.to_string()))
    .bind(
        link.source
            .source_turn_id
            .as_ref()
            .map(codex_coordination::BoundedId::as_str),
    )
    .bind(
        link.source
            .source_item_id
            .as_ref()
            .map(codex_coordination::BoundedId::as_str),
    )
    .bind(link.source.source_ordinal as i64)
    .bind(semantic_slot_sql(link.source.semantic_slot))
    .bind(link.source_identity_bytes.as_slice())
    .bind(link.source_identity_bytes.fingerprint().as_slice())
    .bind(link.canonical_event_bytes.as_slice())
    .bind(link.canonical_event_bytes.fingerprint().as_slice())
    .bind(link.after_revision as i64)
    .bind(suppression_id)
    .bind(suppressed_at_ms)
    .bind(now)
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_recovery_step(RecoveryStep::LegacyInsert)
        .map_err(RecoveryWriteError::Internal)?;
    Ok(match link.native_suppression {
        Some(suppression) => RecordLegacyLinkOutcome::Suppressed(
            record(link, Some(suppression.event_id)),
            suppression.event_id,
        ),
        None => RecordLegacyLinkOutcome::Linked(record(link, None)),
    })
}

pub(super) async fn validate_existing_legacy_link_in(
    connection: &mut SqliteConnection,
    link: &CheckedLegacyLink,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    let row = sqlx::query(
        "SELECT compatibility_event_id,root_thread_id,state_epoch,source_shape,source_thread_id,\
         source_turn_id,source_item_id,source_ordinal,semantic_slot,source_identity_bytes,\
         source_identity_fingerprint,canonical_event_bytes,canonical_event_fingerprint,\
         adapter_version,sanitizer_version,after_revision,suppressed_by_native_event_id \
         FROM coordination_legacy_links \
         WHERE compatibility_event_id=? AND root_thread_id=? AND source_identity_fingerprint=?",
    )
    .bind(link.compatibility_event_id.to_string())
    .bind(link.root_thread_id.to_string())
    .bind(link.source_identity_bytes.fingerprint().as_slice())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?
    .ok_or(RecoveryWriteError::CorruptState)?;
    compare_existing(link, &row)
}

pub(crate) async fn correlate_legacy_link_with_native(
    pool: &SqlitePool,
    link: &CheckedLegacyLink,
    native_event_id: CoordinationEventId,
    suppressed_at_ms: i64,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    correlate_legacy_link_with_native_with(
        pool,
        link,
        native_event_id,
        suppressed_at_ms,
        &NoRecoveryFailure,
    )
    .await
}

pub(super) async fn correlate_legacy_link_with_native_with(
    pool: &SqlitePool,
    link: &CheckedLegacyLink,
    native_event_id: CoordinationEventId,
    suppressed_at_ms: i64,
    injector: &dyn RecoveryFailureInjector,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    let requested = link
        .clone()
        .with_native_suppression(native_event_id, suppressed_at_ms)?;
    record_legacy_link_with(pool, &requested, injector).await
}

async fn apply_requested_suppression(
    connection: &mut SqliteConnection,
    link: &CheckedLegacyLink,
    existing: RecordLegacyLinkOutcome,
    injector: &dyn RecoveryFailureInjector,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    let Some(requested) = link.native_suppression else {
        return Ok(existing);
    };
    match existing {
        RecordLegacyLinkOutcome::Suppressed(record, event_id) => {
            if event_id == requested.event_id {
                Ok(RecordLegacyLinkOutcome::Suppressed(record, event_id))
            } else {
                Err(RecoveryWriteError::NativeCorrelationConflict)
            }
        }
        RecordLegacyLinkOutcome::Duplicate(_) | RecordLegacyLinkOutcome::Linked(_) => {
            validate_native_suppression(connection, link, injector).await?;
            let updated = sqlx::query(
                "UPDATE coordination_legacy_links SET suppressed_by_native_event_id=?,\
                 suppressed_at_ms=? WHERE compatibility_event_id=?\
                 AND suppressed_by_native_event_id IS NULL",
            )
            .bind(requested.event_id.to_string())
            .bind(requested.suppressed_at_ms)
            .bind(link.compatibility_event_id.to_string())
            .execute(&mut *connection)
            .await
            .map_err(internal)?;
            if updated.rows_affected() != 1 {
                return Err(RecoveryWriteError::NativeCorrelationConflict);
            }
            injector
                .after_recovery_step(RecoveryStep::LegacyUpdate)
                .map_err(RecoveryWriteError::Internal)?;
            Ok(RecordLegacyLinkOutcome::Suppressed(
                record(link, Some(requested.event_id)),
                requested.event_id,
            ))
        }
    }
}

async fn validate_native_suppression(
    connection: &mut SqliteConnection,
    link: &CheckedLegacyLink,
    injector: &dyn RecoveryFailureInjector,
) -> Result<(), RecoveryWriteError> {
    let Some(suppression) = link.native_suppression else {
        return Ok(());
    };
    let source_item_id = link
        .source
        .source_item_id
        .as_ref()
        .map(codex_coordination::BoundedId::as_str);
    let valid = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM coordination_events e,\
         json_each(CAST(e.canonical_event_bytes AS TEXT),'$.source.suppressionKeys.items') key \
         WHERE e.event_id=? AND e.root_thread_id=? \
         AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.source.source')='native' \
         AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind')=? \
         AND json_extract(key.value,'$.shape')=? \
         AND json_extract(key.value,'$.sourceOrdinal')=? \
         AND ((? IS NULL AND json_extract(key.value,'$.sourceItemId.status')!='known') \
           OR (? IS NOT NULL AND json_extract(key.value,'$.sourceItemId.status')='known' \
             AND json_extract(key.value,'$.sourceItemId.value')=?))",
    )
    .bind(suppression.event_id.to_string())
    .bind(link.root_thread_id.to_string())
    .bind(semantic_slot_sql(link.source.semantic_slot))
    .bind(source_shape_sql(link.source.shape))
    .bind(link.source.source_ordinal as i64)
    .bind(source_item_id)
    .bind(source_item_id)
    .bind(source_item_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?
    .is_some();
    injector
        .after_recovery_step(RecoveryStep::LegacyRead)
        .map_err(RecoveryWriteError::Internal)?;
    if !valid {
        return Err(RecoveryWriteError::NativeCorrelationConflict);
    }
    Ok(())
}

fn compare_existing(
    link: &CheckedLegacyLink,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<RecordLegacyLinkOutcome, RecoveryWriteError> {
    let stored_identity: Vec<u8> = row.get("source_identity_bytes");
    let stored_identity_fingerprint: Vec<u8> = row.get("source_identity_fingerprint");
    let stored_canonical: Vec<u8> = row.get("canonical_event_bytes");
    let stored_canonical_fingerprint: Vec<u8> = row.get("canonical_event_fingerprint");
    if stored_identity_fingerprint.as_slice() != link.source_identity_bytes.fingerprint()
        || stored_identity != link.source_identity_bytes.as_slice()
        || row.get::<String, _>("compatibility_event_id") != link.compatibility_event_id.to_string()
        || row.get::<String, _>("root_thread_id") != link.root_thread_id.to_string()
        || row.get::<String, _>("state_epoch") != link.expected_state_epoch.to_string()
    {
        return Err(RecoveryWriteError::IdentityCollision);
    }
    if row.get::<String, _>("source_shape") != source_shape_sql(link.source.shape)
        || row.get::<Option<String>, _>("source_thread_id")
            != link.source.source_thread_id.map(|id| id.to_string())
        || row.get::<Option<String>, _>("source_turn_id")
            != link
                .source
                .source_turn_id
                .as_ref()
                .map(|id| id.as_str().to_owned())
        || row.get::<Option<String>, _>("source_item_id")
            != link
                .source
                .source_item_id
                .as_ref()
                .map(|id| id.as_str().to_owned())
        || row.get::<i64, _>("source_ordinal") != link.source.source_ordinal as i64
        || row.get::<String, _>("semantic_slot") != semantic_slot_sql(link.source.semantic_slot)
        || row.get::<i64, _>("adapter_version") != 1
        || row.get::<i64, _>("sanitizer_version") != 1
        || row.get::<i64, _>("after_revision") != link.after_revision as i64
    {
        return Err(RecoveryWriteError::CorruptState);
    }
    if stored_canonical_fingerprint.as_slice() != link.canonical_event_bytes.fingerprint()
        || stored_canonical != link.canonical_event_bytes.as_slice()
    {
        return Err(RecoveryWriteError::DivergentReduction);
    }
    let suppressed = row
        .get::<Option<String>, _>("suppressed_by_native_event_id")
        .map(|id| CoordinationEventId::parse(&id))
        .transpose()
        .map_err(|_| RecoveryWriteError::CorruptState)?;
    let record = record(link, suppressed);
    Ok(match suppressed {
        Some(event_id) => RecordLegacyLinkOutcome::Suppressed(record, event_id),
        None => RecordLegacyLinkOutcome::Duplicate(record),
    })
}

fn record(
    link: &CheckedLegacyLink,
    suppressed_by_native_event_id: Option<CoordinationEventId>,
) -> LegacyLinkRecord {
    LegacyLinkRecord {
        compatibility_event_id: link.compatibility_event_id,
        root_thread_id: link.root_thread_id,
        state_epoch: link.expected_state_epoch,
        source: link.source.clone(),
        after_revision: link.after_revision,
        suppressed_by_native_event_id,
    }
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
