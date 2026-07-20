use codex_coordination::CoordinationEventId;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqliteConnection;
use sqlx::SqlitePool;

use super::legacy_degradations::record_legacy_degradation_in;
use super::legacy_degradations::validate_existing_legacy_degradation_in;
use super::legacy_links::record_legacy_link_in;
use super::legacy_links::validate_existing_legacy_link_in;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_recovery_state::AdvanceLegacyScanOutcome;
use crate::model::coordination_recovery_state::LegacyScanCheckpoint;
use crate::model::coordination_recovery_state::LegacyScanPage;

pub(crate) async fn advance_legacy_scan_checkpoint(
    pool: &SqlitePool,
    page: &LegacyScanPage,
) -> Result<AdvanceLegacyScanOutcome, RecoveryWriteError> {
    page.validate()?;
    let mut connection = recovery_guard::begin(pool).await?;
    let result = advance_in(&mut connection, page).await;
    recovery_guard::finish(&mut connection, result).await
}

async fn advance_in(
    connection: &mut SqliteConnection,
    page: &LegacyScanPage,
) -> Result<AdvanceLegacyScanOutcome, RecoveryWriteError> {
    recovery_guard::active_authority(
        connection,
        &page.root_thread_id,
        Some(page.expected_state_epoch),
    )
    .await?;
    let existing =
        load_checkpoint(connection, &page.root_thread_id, &page.source_thread_id).await?;
    let expected_last_order = page
        .links
        .iter()
        .map(|link| (link.source.source_ordinal, link.compatibility_event_id))
        .chain(
            existing
                .as_ref()
                .and_then(|checkpoint| checkpoint.last_order),
        )
        .max();
    if let Some(existing) = &existing {
        if exact_target(existing, page) {
            for link in &page.links {
                validate_existing_legacy_link_in(connection, link).await?;
            }
            for degradation in &page.degradations {
                validate_existing_legacy_degradation_in(connection, degradation).await?;
            }
            return Ok(AdvanceLegacyScanOutcome::Duplicate(existing.clone()));
        }
        if existing.version != page.expected_version {
            return Ok(AdvanceLegacyScanOutcome::Fenced(existing.clone()));
        }
        if page.last_order != expected_last_order
            || page.expected_prefix_fingerprint != Some(existing.scanned_prefix_fingerprint)
            || page.next_physical_ordinal < existing.next_physical_ordinal
            || (page.next_physical_ordinal == existing.next_physical_ordinal
                && page.scanned_prefix_fingerprint != existing.scanned_prefix_fingerprint)
            || (existing.complete && !page.complete)
        {
            record_source_changed_degradations(connection, page).await?;
            return Ok(AdvanceLegacyScanOutcome::SourceChanged(existing.clone()));
        }
    } else {
        if page.last_order != expected_last_order {
            return Err(
                crate::model::coordination_recovery::RecoveryInputError::InvalidCheckpointOrder
                    .into(),
            );
        }
        if page.expected_version != 0 || page.expected_prefix_fingerprint.is_some() {
            return Err(RecoveryWriteError::EpochMismatch);
        }
    }
    for link in &page.links {
        record_legacy_link_in(connection, link).await?;
    }
    for degradation in &page.degradations {
        record_legacy_degradation_in(connection, degradation, page.now_ms).await?;
    }
    let next_version = existing.as_ref().map_or(Ok(0), |checkpoint| {
        checkpoint
            .version
            .checked_add(1)
            .filter(|version| *version <= i64::MAX as u64)
            .ok_or(RecoveryWriteError::CorruptState)
    })?;
    let (last_ordinal, last_event_id) = page
        .last_order
        .map(|(ordinal, id)| (Some(ordinal as i64), Some(id.to_string())))
        .unwrap_or((None, None));
    match existing {
        Some(_) => {
            let expected_prefix = page
                .expected_prefix_fingerprint
                .ok_or(RecoveryWriteError::CorruptState)?;
            let updated = sqlx::query(
                "UPDATE coordination_legacy_scan_checkpoints SET next_physical_ordinal=?,\
                 scanned_prefix_fingerprint=?,last_source_ordinal=?,last_compatibility_event_id=?,\
                 complete=?,version=?,updated_at_ms=? WHERE root_thread_id=?\
                 AND source_thread_id=? AND adapter_version=1 AND version=?\
                 AND scanned_prefix_fingerprint=?",
            )
            .bind(page.next_physical_ordinal as i64)
            .bind(page.scanned_prefix_fingerprint.as_slice())
            .bind(last_ordinal)
            .bind(last_event_id)
            .bind(i64::from(page.complete))
            .bind(next_version as i64)
            .bind(page.now_ms)
            .bind(page.root_thread_id.to_string())
            .bind(page.source_thread_id.to_string())
            .bind(page.expected_version as i64)
            .bind(expected_prefix.as_slice())
            .execute(&mut *connection)
            .await
            .map_err(internal)?;
            if updated.rows_affected() != 1 {
                return Err(RecoveryWriteError::Deferred);
            }
        }
        None => {
            sqlx::query(
                "INSERT INTO coordination_legacy_scan_checkpoints \
                 (root_thread_id,state_epoch,source_thread_id,adapter_version,next_physical_ordinal,\
                  scanned_prefix_fingerprint,last_source_ordinal,last_compatibility_event_id,\
                  complete,version,created_at_ms,updated_at_ms) VALUES (?,?,?,1,?,?,?,?,?,0,?,?)",
            )
            .bind(page.root_thread_id.to_string())
            .bind(page.expected_state_epoch.to_string())
            .bind(page.source_thread_id.to_string())
            .bind(page.next_physical_ordinal as i64)
            .bind(page.scanned_prefix_fingerprint.as_slice())
            .bind(last_ordinal)
            .bind(last_event_id)
            .bind(i64::from(page.complete))
            .bind(page.now_ms)
            .bind(page.now_ms)
            .execute(&mut *connection)
            .await
            .map_err(internal)?;
        }
    }
    Ok(AdvanceLegacyScanOutcome::Advanced(LegacyScanCheckpoint {
        root_thread_id: page.root_thread_id,
        state_epoch: page.expected_state_epoch,
        source_thread_id: page.source_thread_id,
        next_physical_ordinal: page.next_physical_ordinal,
        scanned_prefix_fingerprint: page.scanned_prefix_fingerprint,
        last_order: page.last_order,
        complete: page.complete,
        version: next_version,
    }))
}

async fn record_source_changed_degradations(
    connection: &mut SqliteConnection,
    page: &LegacyScanPage,
) -> Result<(), RecoveryWriteError> {
    for degradation in &page.degradations {
        record_legacy_degradation_in(connection, degradation, page.now_ms).await?;
    }
    Ok(())
}

async fn load_checkpoint(
    connection: &mut SqliteConnection,
    root_thread_id: &ThreadId,
    source_thread_id: &ThreadId,
) -> Result<Option<LegacyScanCheckpoint>, RecoveryWriteError> {
    let row = sqlx::query(
        "SELECT * FROM coordination_legacy_scan_checkpoints WHERE root_thread_id=?\
         AND source_thread_id=? AND adapter_version=1",
    )
    .bind(root_thread_id.to_string())
    .bind(source_thread_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    row.map(checkpoint_from_row).transpose()
}

fn checkpoint_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<LegacyScanCheckpoint, RecoveryWriteError> {
    let last_ordinal = row.get::<Option<i64>, _>("last_source_ordinal");
    let last_event_id = row.get::<Option<String>, _>("last_compatibility_event_id");
    let last_order = match (last_ordinal, last_event_id) {
        (None, None) => None,
        (Some(ordinal), Some(event_id)) => Some((
            unsigned(ordinal)?,
            CoordinationEventId::parse(&event_id).map_err(|_| RecoveryWriteError::CorruptState)?,
        )),
        _ => return Err(RecoveryWriteError::CorruptState),
    };
    Ok(LegacyScanCheckpoint {
        root_thread_id: ThreadId::try_from(row.get::<String, _>("root_thread_id"))
            .map_err(|_| RecoveryWriteError::CorruptState)?,
        state_epoch: StateEpoch::parse(&row.get::<String, _>("state_epoch"))
            .map_err(|_| RecoveryWriteError::CorruptState)?,
        source_thread_id: ThreadId::try_from(row.get::<String, _>("source_thread_id"))
            .map_err(|_| RecoveryWriteError::CorruptState)?,
        next_physical_ordinal: unsigned(row.get("next_physical_ordinal"))?,
        scanned_prefix_fingerprint: row
            .get::<Vec<u8>, _>("scanned_prefix_fingerprint")
            .try_into()
            .map_err(|_| RecoveryWriteError::CorruptState)?,
        last_order,
        complete: row.get::<i64, _>("complete") == 1,
        version: unsigned(row.get("version"))?,
    })
}

fn exact_target(checkpoint: &LegacyScanCheckpoint, page: &LegacyScanPage) -> bool {
    checkpoint.root_thread_id == page.root_thread_id
        && checkpoint.state_epoch == page.expected_state_epoch
        && checkpoint.source_thread_id == page.source_thread_id
        && checkpoint.next_physical_ordinal == page.next_physical_ordinal
        && checkpoint.scanned_prefix_fingerprint == page.scanned_prefix_fingerprint
        && checkpoint.last_order == page.last_order
        && checkpoint.complete == page.complete
}

fn unsigned(value: i64) -> Result<u64, RecoveryWriteError> {
    value
        .try_into()
        .map_err(|_| RecoveryWriteError::CorruptState)
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
