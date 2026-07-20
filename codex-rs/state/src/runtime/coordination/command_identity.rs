use codex_coordination::IdempotencyKey;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::command_rows::CommandPayloadAccess;
use super::command_rows::load_command_by_operation;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::commands::CommandWriteError;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_commands::RecordCoordinationCommand;

pub(super) async fn preflight_identity(
    connection: &mut SqliteConnection,
    params: &RecordCoordinationCommand,
    key: &IdempotencyKey,
    injector: &dyn CommandFailureInjector,
) -> Result<(), CommandWriteError> {
    if let Some(existing) = load_command_by_operation(
        connection,
        params.intent.operation_id(),
        CommandPayloadAccess::MetadataOnly,
    )
    .await?
        && (existing.metadata.root_thread_id != params.intent.context().root_thread_id
            || existing.tuple_fingerprint.as_slice() != key.fingerprint())
    {
        return Err(CommandWriteError::IdentityConflict);
    }
    let event_key = sqlx::query(
        "SELECT root_thread_id,idempotency_key_fingerprint FROM coordination_events WHERE event_id=?",
    )
    .bind(params.intent.context().primary.event_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_command_step(CommandStep::IdentityRead)
        .map_err(internal)?;
    if let Some(row) = event_key
        && (row.get::<String, _>("root_thread_id")
            != params.intent.context().root_thread_id.to_string()
            || row
                .get::<Vec<u8>, _>("idempotency_key_fingerprint")
                .as_slice()
                != key.fingerprint())
    {
        return Err(CommandWriteError::IdentityConflict);
    }
    let semantic_identity = sqlx::query(
        "SELECT event_id,root_thread_id,idempotency_key_fingerprint FROM coordination_events \
         WHERE json_extract(CAST(canonical_event_bytes AS TEXT),'$.kind')=? \
         AND json_extract(CAST(canonical_event_bytes AS TEXT),'$.operationId')=? LIMIT 1",
    )
    .bind(CommandKind::from_intent(&params.intent).event_kind_sql())
    .bind(params.intent.operation_id().to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    if let Some(row) = semantic_identity
        && (row.get::<String, _>("event_id")
            != params.intent.context().primary.event_id.to_string()
            || row.get::<String, _>("root_thread_id")
                != params.intent.context().root_thread_id.to_string()
            || row
                .get::<Vec<u8>, _>("idempotency_key_fingerprint")
                .as_slice()
                != key.fingerprint())
    {
        return Err(CommandWriteError::IdentityConflict);
    }
    Ok(())
}

pub(super) fn validate_stored_tuple(
    stored_bytes: &[u8],
    stored_fingerprint: &[u8],
    key: &IdempotencyKey,
) -> Result<(), CommandWriteError> {
    if stored_fingerprint != key.fingerprint() {
        return Err(CommandWriteError::CorruptStoredCommand);
    }
    if stored_bytes != key.tuple_bytes() {
        return Err(CommandWriteError::IdempotencyCollision);
    }
    Ok(())
}

fn internal(error: impl Into<anyhow::Error>) -> CommandWriteError {
    CommandWriteError::Internal(error.into())
}
