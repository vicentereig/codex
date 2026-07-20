use sqlx::Sqlite;
use sqlx::Transaction;

use super::aggregate_journal::AggregateStep;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::commands::CommandWriteError;
use crate::StateRuntime;

pub(super) async fn begin_command(
    runtime: &StateRuntime,
    injector: &dyn CommandFailureInjector,
) -> Result<Transaction<'static, Sqlite>, CommandWriteError> {
    let transaction = runtime
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(internal)?;
    if let Err(error) = injector.after_command_step(CommandStep::TransactionBegin) {
        rollback_command(transaction, injector).await?;
        return Err(internal(error));
    }
    Ok(transaction)
}

pub(super) async fn finish_command<T>(
    transaction: Transaction<'static, Sqlite>,
    result: Result<T, CommandWriteError>,
    injector: &dyn CommandFailureInjector,
) -> Result<T, CommandWriteError> {
    match result {
        Ok(value) => {
            if let Err(error) = injector.after_step(AggregateStep::BeforeCommit) {
                rollback_command(transaction, injector).await?;
                return Err(internal(error));
            }
            transaction.commit().await.map_err(internal)?;
            injector
                .after_step(AggregateStep::AfterCommit)
                .map_err(internal)?;
            Ok(value)
        }
        Err(error) => {
            rollback_command(transaction, injector).await?;
            Err(error)
        }
    }
}

async fn rollback_command(
    transaction: Transaction<'static, Sqlite>,
    injector: &dyn CommandFailureInjector,
) -> Result<(), CommandWriteError> {
    transaction.rollback().await.map_err(internal)?;
    injector
        .after_command_step(CommandStep::Rollback)
        .map_err(internal)
}

fn internal(error: impl Into<anyhow::Error>) -> CommandWriteError {
    CommandWriteError::Internal(error.into())
}
