use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqliteConnection;
use std::path::Path;

use super::authority_marker::MARKER_FILE_NAME;
use super::authority_marker::MarkerDisposition;
use super::authority_marker::MarkerRead;
use super::authority_marker::read_marker;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;

pub(super) async fn active_authority(
    connection: &mut SqliteConnection,
    root_thread_id: &ThreadId,
    expected_epoch: Option<StateEpoch>,
) -> Result<StateEpoch, RecoveryWriteError> {
    let epoch = active_epoch(connection).await?;
    if expected_epoch.is_some_and(|expected| expected != epoch) {
        return Err(RecoveryWriteError::EpochMismatch);
    }
    let root_epoch = sqlx::query_scalar::<_, String>(
        "SELECT state_epoch FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(root_thread_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?
    .ok_or(RecoveryWriteError::EpochMismatch)?;
    if root_epoch != epoch.to_string() {
        return Err(RecoveryWriteError::EpochMismatch);
    }
    Ok(epoch)
}

pub(super) async fn active_epoch(
    connection: &mut SqliteConnection,
) -> Result<StateEpoch, RecoveryWriteError> {
    let authority =
        sqlx::query("SELECT state_epoch,status FROM coordination_authority WHERE singleton_id=1")
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?
            .ok_or(RecoveryWriteError::EpochMismatch)?;
    if authority.get::<String, _>("status") != "active" {
        return Err(RecoveryWriteError::Quarantined);
    }
    let epoch = StateEpoch::parse(&authority.get::<String, _>("state_epoch"))
        .map_err(|_| RecoveryWriteError::CorruptState)?;
    Ok(epoch)
}

pub(super) async fn validate_anchor(
    connection: &mut SqliteConnection,
    root_thread_id: &ThreadId,
    after_revision: u64,
) -> Result<(), RecoveryWriteError> {
    let committed_revision = sqlx::query_scalar::<_, i64>(
        "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(root_thread_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?
    .ok_or(RecoveryWriteError::EpochMismatch)?;
    if u64::try_from(committed_revision).map_err(|_| RecoveryWriteError::CorruptState)?
        < after_revision
    {
        return Err(RecoveryWriteError::AnchorAheadOfRoot);
    }
    Ok(())
}

pub(super) async fn begin(
    pool: &sqlx::SqlitePool,
) -> Result<sqlx::pool::PoolConnection<sqlx::Sqlite>, RecoveryWriteError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|_| RecoveryWriteError::Deferred)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(|_| RecoveryWriteError::Deferred)?;
    if let Err(error) = verify_marker(&mut connection).await {
        // Marker divergence commits its quarantine inside `verify_marker`; every
        // other failed preflight still owns the immediate transaction. A
        // best-effort rollback is therefore required even for `Quarantined`.
        let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
        return Err(error);
    }
    Ok(connection)
}

async fn verify_marker(connection: &mut SqliteConnection) -> Result<(), RecoveryWriteError> {
    let authority =
        sqlx::query("SELECT state_epoch,status FROM coordination_authority WHERE singleton_id=1")
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?
            .ok_or(RecoveryWriteError::EpochMismatch)?;
    if authority.get::<String, _>("status") != "active" {
        return Err(RecoveryWriteError::Quarantined);
    }
    let epoch = StateEpoch::parse(&authority.get::<String, _>("state_epoch"))
        .map_err(|_| RecoveryWriteError::CorruptState)?;
    let database_path =
        sqlx::query_scalar::<_, String>("SELECT file FROM pragma_database_list WHERE name='main'")
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?
            .filter(|path| !path.is_empty())
            .ok_or(RecoveryWriteError::CorruptState)?;
    let marker_path = Path::new(&database_path)
        .parent()
        .ok_or(RecoveryWriteError::CorruptState)?
        .join(MARKER_FILE_NAME);
    let marker = read_marker(marker_path.as_path())
        .await
        .map_err(|_| RecoveryWriteError::Deferred)?;
    if matches!(
        marker,
        MarkerRead::Valid {
            state_epoch,
            disposition: MarkerDisposition::Ordinary,
        } if state_epoch == epoch
    ) {
        return Ok(());
    }
    sqlx::query(
        "UPDATE coordination_authority SET status='quarantined',\
         quarantine_reason='coordination authority marker changed during recovery',\
         updated_at_ms=MAX(updated_at_ms,?) WHERE singleton_id=1 AND status='active'",
    )
    .bind(chrono::Utc::now().timestamp_millis().max(0))
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    sqlx::query("COMMIT")
        .execute(&mut *connection)
        .await
        .map_err(|_| RecoveryWriteError::Deferred)?;
    Err(RecoveryWriteError::Quarantined)
}

pub(super) async fn finish<T>(
    connection: &mut SqliteConnection,
    result: Result<T, RecoveryWriteError>,
) -> Result<T, RecoveryWriteError> {
    match result {
        Ok(value) => {
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(|_| RecoveryWriteError::Deferred)?;
            Ok(value)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

pub(super) async fn finish_with<T>(
    connection: &mut SqliteConnection,
    result: Result<T, RecoveryWriteError>,
    injector: &dyn RecoveryFailureInjector,
) -> Result<T, RecoveryWriteError> {
    match result {
        Ok(value) => {
            if let Err(error) = injector.after_recovery_step(RecoveryStep::BeforeCommit) {
                let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
                return Err(RecoveryWriteError::Internal(error));
            }
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(|_| RecoveryWriteError::Deferred)?;
            injector
                .after_recovery_step(RecoveryStep::AfterCommit)
                .map_err(RecoveryWriteError::Internal)?;
            Ok(value)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
