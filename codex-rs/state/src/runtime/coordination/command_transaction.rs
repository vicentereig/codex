use sqlx::SqliteConnection;

use super::aggregate_journal::AggregateStep;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::commands::CommandWriteError;
use crate::StateRuntime;

pub(super) async fn begin_command(
    runtime: &StateRuntime,
    injector: &dyn CommandFailureInjector,
) -> Result<sqlx::pool::PoolConnection<sqlx::Sqlite>, CommandWriteError> {
    let mut connection = runtime.pool.acquire().await.map_err(internal)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(internal)?;
    if let Err(error) = injector.after_command_step(CommandStep::TransactionBegin) {
        rollback_command(&mut connection, injector).await?;
        return Err(internal(error));
    }
    Ok(connection)
}

pub(super) async fn finish_command<T>(
    connection: &mut SqliteConnection,
    result: Result<T, CommandWriteError>,
    injector: &dyn CommandFailureInjector,
) -> Result<T, CommandWriteError> {
    match result {
        Ok(value) => {
            if let Err(error) = injector.after_step(AggregateStep::BeforeCommit) {
                rollback_command(connection, injector).await?;
                return Err(internal(error));
            }
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(internal)?;
            injector
                .after_step(AggregateStep::AfterCommit)
                .map_err(internal)?;
            Ok(value)
        }
        Err(error) => {
            rollback_command(connection, injector).await?;
            Err(error)
        }
    }
}

async fn rollback_command(
    connection: &mut SqliteConnection,
    injector: &dyn CommandFailureInjector,
) -> Result<(), CommandWriteError> {
    sqlx::query("ROLLBACK")
        .execute(&mut *connection)
        .await
        .map_err(internal)?;
    injector
        .after_command_step(CommandStep::Rollback)
        .map_err(internal)
}

fn internal(error: impl Into<anyhow::Error>) -> CommandWriteError {
    CommandWriteError::Internal(error.into())
}
