use codex_coordination::BoundedId;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationOrder;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::IdempotencyKey;
use codex_coordination::MAX_ID_BYTES;
use sqlx::SqliteConnection;

use super::accept_transitions::accept;
use super::aggregate_journal::compare_event;
use super::aggregate_journal::known_turn;
use super::aggregates::AssignmentTransitionOutcome;
use super::command_rows::CommandPayloadAccess;
use super::command_rows::StoredCommand;
use super::command_rows::load_command_by_operation;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxWriteError;
use super::inbox_receipt::accept_params;
use super::inbox_rows::InboxPayloadAccess;
use super::inbox_rows::StoredInbox;
use super::inbox_rows::load_inbox_by_receipt;
use super::inbox_rows::load_inbox_by_tuple;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_inbox::PersistRecipientReceipt;

pub(super) async fn validate_duplicate(
    connection: &mut SqliteConnection,
    params: &PersistRecipientReceipt,
    stored: &StoredInbox,
    injector: &dyn InboxFailureInjector,
) -> Result<(), InboxWriteError> {
    let turn = known_turn(&params.context.actor)?;
    let key = receipt_key(params, stored.metadata.kind, turn);
    if stored.tuple_fingerprint != key.fingerprint() || stored.tuple_bytes != key.tuple_bytes() {
        return Err(InboxWriteError::IdempotencyCollision);
    }
    if stored.metadata.receipt_id != params.receipt_id
        || stored.metadata.root_thread_id != params.context.root_thread_id
        || stored.metadata.receipt_event_id != params.context.primary.event_id
        || stored.metadata.recipient_thread_id != params.context.actor.thread_id
        || &stored.metadata.recipient_turn_id != turn
    {
        return Err(InboxWriteError::IdempotencyConflict);
    }
    let command = load_command_by_operation(
        connection,
        params.command_operation_id,
        CommandPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(InboxWriteError::CorruptStoredInbox)?;
    if command.command_fingerprint != stored.command_fingerprint
        || command.metadata.root_thread_id != stored.metadata.root_thread_id
        || command.metadata.intent_event_id != stored.metadata.intent_event_id
        || command.metadata.target.assignment_id != stored.metadata.target_assignment_id
        || command.metadata.target.generation != stored.metadata.target_generation
    {
        return Err(InboxWriteError::IdempotencyConflict);
    }
    match stored.metadata.kind {
        CommandKind::AssignmentSpawn | CommandKind::AssignmentFollowup => {
            let outcome = accept(
                connection,
                accept_params(params, &command, turn.clone()),
                injector,
            )
            .await?;
            if !matches!(outcome, AssignmentTransitionOutcome::Duplicate { .. }) {
                return Err(InboxWriteError::CorruptStoredInbox);
            }
        }
        CommandKind::Message | CommandKind::Interrupt => {
            if !params.context.secondary.items().is_empty() {
                return Err(InboxWriteError::IdempotencyConflict);
            }
            let CoordinationOrder::Native {
                state_epoch,
                revision,
            } = stored.receipt_event.envelope().order
            else {
                return Err(InboxWriteError::CorruptStoredInbox);
            };
            compare_event(
                &params.context,
                &params.context.primary,
                state_epoch,
                revision,
                receipt_kind(&command, params.receipt_id, turn.clone())?,
                &[&command.event],
                &stored.receipt_event,
            )?;
        }
    }
    Ok(())
}

pub(super) async fn check_unclaimed_identities(
    connection: &mut SqliteConnection,
    params: &PersistRecipientReceipt,
    key: &IdempotencyKey,
    kind: CommandKind,
    _injector: &dyn InboxFailureInjector,
) -> Result<(), InboxWriteError> {
    if load_inbox_by_receipt(
        connection,
        params.receipt_id,
        InboxPayloadAccess::MetadataOnly,
    )
    .await?
    .is_some()
    {
        return Err(InboxWriteError::IdentityConflict);
    }
    if let Some(stored) = load_inbox_by_tuple(
        connection,
        &params.context.root_thread_id,
        &params.context.actor.thread_id,
        &key.fingerprint(),
    )
    .await?
    {
        return if stored.tuple_bytes == key.tuple_bytes() {
            Err(InboxWriteError::IdempotencyConflict)
        } else {
            Err(InboxWriteError::IdempotencyCollision)
        };
    }
    let event_kind = match kind {
        CommandKind::AssignmentSpawn | CommandKind::AssignmentFollowup => "assignmentAccepted",
        CommandKind::Message => "messageDurablyReceived",
        CommandKind::Interrupt => "interruptDurablyReceived",
    };
    // At this point no inbox row owns the identity. Any journal event with the
    // proposed event id or semantic operation therefore represents a competing
    // claim, not storage corruption. Detect it before INSERT so callers receive
    // the same closed identity error as command-intent preflight rather than a
    // backend-specific UNIQUE/trigger failure.
    let claimed = sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(SELECT 1 FROM coordination_events e WHERE e.event_id=? OR (\
         e.root_thread_id=? AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind')=? AND \
         json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.operationId')=?))",
    )
    .bind(params.context.primary.event_id.to_string())
    .bind(params.context.root_thread_id.to_string())
    .bind(event_kind)
    .bind(params.command_operation_id.to_string())
    .fetch_one(&mut *connection)
    .await
    .map_err(super::inbox::internal)?;
    if claimed != 0 {
        return Err(InboxWriteError::IdentityConflict);
    }
    Ok(())
}

pub(super) fn receipt_kind(
    command: &StoredCommand,
    receipt_id: codex_coordination::ReceiptId,
    bound_turn: BoundedId<MAX_ID_BYTES>,
) -> Result<CoordinationEventKind, InboxWriteError> {
    match command.event.kind() {
        CoordinationEventKind::AssignmentRequested {
            operation_id,
            mode,
            target,
            ..
        } => Ok(CoordinationEventKind::AssignmentAccepted {
            operation_id: *operation_id,
            mode: *mode,
            target: target.clone(),
            receipt_id,
            bound_turn_id: codex_coordination::Evidence::Known { value: bound_turn },
        }),
        CoordinationEventKind::MessageSubmissionRecorded {
            operation_id,
            target,
            ..
        } => Ok(CoordinationEventKind::MessageDurablyReceived {
            operation_id: *operation_id,
            target: target.clone(),
            receipt_id,
        }),
        CoordinationEventKind::InterruptRequested {
            operation_id,
            target,
        } => Ok(CoordinationEventKind::InterruptDurablyReceived {
            operation_id: *operation_id,
            target: target.clone(),
            receipt_id,
        }),
        _ => Err(InboxWriteError::CorruptStoredInbox),
    }
}

pub(super) fn receipt_key(
    params: &PersistRecipientReceipt,
    kind: CommandKind,
    turn: &BoundedId<MAX_ID_BYTES>,
) -> IdempotencyKey {
    IdempotencyKey::new(
        params.context.root_thread_id,
        params.context.actor.thread_id,
        turn.clone(),
        params.command_operation_id,
        receipt_slot(kind),
    )
}

pub(super) fn receipt_slot(kind: CommandKind) -> CoordinationSemanticSlot {
    match kind {
        CommandKind::AssignmentSpawn | CommandKind::AssignmentFollowup => {
            CoordinationSemanticSlot::AssignmentAccepted
        }
        CommandKind::Message => CoordinationSemanticSlot::MessageDurablyReceived,
        CommandKind::Interrupt => CoordinationSemanticSlot::InterruptDurablyReceived,
    }
}
