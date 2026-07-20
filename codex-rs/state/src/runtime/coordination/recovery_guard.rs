use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::Sqlite;
use sqlx::SqliteConnection;
use sqlx::SqlitePool;
use sqlx::Transaction;
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

pub(super) async fn active_authority_with(
    connection: &mut SqliteConnection,
    root_thread_id: &ThreadId,
    expected_epoch: Option<StateEpoch>,
    injector: &dyn RecoveryFailureInjector,
) -> Result<StateEpoch, RecoveryWriteError> {
    let epoch = active_epoch_with(connection, injector).await?;
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
    injector
        .after_recovery_step(RecoveryStep::AuthorityRead)
        .map_err(RecoveryWriteError::Internal)?;
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

pub(super) async fn active_epoch_with(
    connection: &mut SqliteConnection,
    injector: &dyn RecoveryFailureInjector,
) -> Result<StateEpoch, RecoveryWriteError> {
    let epoch = active_epoch(connection).await?;
    injector
        .after_recovery_step(RecoveryStep::AuthorityRead)
        .map_err(RecoveryWriteError::Internal)?;
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

pub(super) async fn validate_anchor_with(
    connection: &mut SqliteConnection,
    root_thread_id: &ThreadId,
    after_revision: u64,
    injector: &dyn RecoveryFailureInjector,
) -> Result<(), RecoveryWriteError> {
    validate_anchor(connection, root_thread_id, after_revision).await?;
    injector
        .after_recovery_step(RecoveryStep::AnchorRead)
        .map_err(RecoveryWriteError::Internal)
}

async fn begin(
    pool: &sqlx::SqlitePool,
    injector: &dyn RecoveryFailureInjector,
) -> Result<Transaction<'static, Sqlite>, RecoveryWriteError> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|_| RecoveryWriteError::Deferred)?;
    if let Err(error) = injector.after_recovery_step(RecoveryStep::TransactionBegin) {
        return rollback_after_begin(transaction, injector, RecoveryWriteError::Internal(error))
            .await;
    }
    match verify_marker(&mut transaction, injector).await {
        Ok(MarkerPreflight::Verified) => Ok(transaction),
        Ok(MarkerPreflight::NeedsQuarantineCommit) => {
            transaction
                .commit()
                .await
                .map_err(|_| RecoveryWriteError::Deferred)?;
            match injector.after_recovery_step(RecoveryStep::MarkerCommit) {
                Ok(()) => Err(RecoveryWriteError::Quarantined),
                Err(error) => Err(RecoveryWriteError::Internal(error)),
            }
        }
        Err(error) => rollback_after_begin(transaction, injector, error).await,
    }
}

pub(super) async fn begin_with(
    pool: &SqlitePool,
    injector: &dyn RecoveryFailureInjector,
) -> Result<Transaction<'static, Sqlite>, RecoveryWriteError> {
    begin(pool, injector).await
}

async fn rollback_after_begin<T>(
    transaction: Transaction<'static, Sqlite>,
    injector: &dyn RecoveryFailureInjector,
    error: RecoveryWriteError,
) -> Result<T, RecoveryWriteError> {
    transaction
        .rollback()
        .await
        .map_err(|_| RecoveryWriteError::Deferred)?;
    injector
        .after_recovery_step(RecoveryStep::Rollback)
        .map_err(RecoveryWriteError::Internal)?;
    Err(error)
}

async fn verify_marker(
    connection: &mut SqliteConnection,
    injector: &dyn RecoveryFailureInjector,
) -> Result<MarkerPreflight, RecoveryWriteError> {
    let authority =
        sqlx::query("SELECT state_epoch,status FROM coordination_authority WHERE singleton_id=1")
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?
            .ok_or(RecoveryWriteError::EpochMismatch)?;
    injector
        .after_recovery_step(RecoveryStep::MarkerRead)
        .map_err(RecoveryWriteError::Internal)?;
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
    injector
        .after_recovery_step(RecoveryStep::MarkerRead)
        .map_err(RecoveryWriteError::Internal)?;
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
        return Ok(MarkerPreflight::Verified);
    }
    sqlx::query(
        "UPDATE coordination_authority SET status='quarantined',\
         quarantine_reason='coordination authority marker changed during recovery',\
         updated_at_ms=MAX(updated_at_ms,?) WHERE singleton_id=1 AND status='active'",
    )
    .bind(injector.now_ms())
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_recovery_step(RecoveryStep::MarkerUpdate)
        .map_err(RecoveryWriteError::Internal)?;
    Ok(MarkerPreflight::NeedsQuarantineCommit)
}

enum MarkerPreflight {
    Verified,
    NeedsQuarantineCommit,
}

pub(super) async fn finish<T>(
    transaction: Transaction<'static, Sqlite>,
    result: Result<T, RecoveryWriteError>,
) -> Result<T, RecoveryWriteError> {
    match result {
        Ok(value) => {
            transaction
                .commit()
                .await
                .map_err(|_| RecoveryWriteError::Deferred)?;
            Ok(value)
        }
        Err(error) => {
            transaction
                .rollback()
                .await
                .map_err(|_| RecoveryWriteError::Deferred)?;
            Err(error)
        }
    }
}

pub(super) async fn finish_with<T>(
    transaction: Transaction<'static, Sqlite>,
    result: Result<T, RecoveryWriteError>,
    injector: &dyn RecoveryFailureInjector,
) -> Result<T, RecoveryWriteError> {
    match result {
        Ok(value) => {
            if let Err(error) = injector.after_recovery_step(RecoveryStep::BeforeCommit) {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| RecoveryWriteError::Deferred)?;
                injector
                    .after_recovery_step(RecoveryStep::Rollback)
                    .map_err(RecoveryWriteError::Internal)?;
                return Err(RecoveryWriteError::Internal(error));
            }
            transaction
                .commit()
                .await
                .map_err(|_| RecoveryWriteError::Deferred)?;
            injector
                .after_recovery_step(RecoveryStep::AfterCommit)
                .map_err(RecoveryWriteError::Internal)?;
            Ok(value)
        }
        Err(error) => {
            transaction
                .rollback()
                .await
                .map_err(|_| RecoveryWriteError::Deferred)?;
            injector
                .after_recovery_step(RecoveryStep::Rollback)
                .map_err(RecoveryWriteError::Internal)?;
            Err(error)
        }
    }
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
