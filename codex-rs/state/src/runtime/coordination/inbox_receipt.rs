use codex_coordination::BoundedId;
use codex_coordination::CoordinationEvent;
use codex_coordination::Evidence;
use codex_coordination::IdempotencyKey;
use codex_coordination::MAX_ID_BYTES;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::accept_transitions::accept;
use super::aggregate_journal::*;
use super::aggregates::AssignmentTransitionOutcome;
use super::command_rows::CommandPayloadAccess;
use super::command_rows::StoredCommand;
use super::command_rows::ciphertext as command_ciphertext;
use super::command_rows::load_command_by_operation;
use super::command_rows::target_is_current;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::inbox::internal as inbox_internal;
use super::inbox_receipt_identity::*;
use super::inbox_rows::*;
use crate::model::coordination::AcceptAssignment;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_commands::CommandLifecycle;
use crate::model::coordination_inbox::*;

pub(super) async fn persist_receipt(
    connection: &mut SqliteConnection,
    params: PersistRecipientReceipt,
    injector: &dyn InboxFailureInjector,
) -> Result<PersistRecipientReceiptOutcome, InboxWriteError> {
    validate_identities(&params.context)?;
    if params.context.primary.operation_id != params.command_operation_id {
        return Err(InboxWriteError::IdempotencyConflict);
    }
    // Mutating APIs remain closed while authority is quarantined, including exact
    // duplicate replays. Check before consulting durable idempotency state so a
    // duplicate cannot accidentally become a write-side health probe.
    authority(connection, injector).await?;
    if let Some(stored) = load_inbox_by_command(
        connection,
        params.command_operation_id,
        InboxPayloadAccess::MetadataOnly,
    )
    .await?
    {
        injector
            .after_inbox_step(InboxStep::DuplicateRead)
            .map_err(inbox_internal)?;
        validate_duplicate(connection, &params, &stored, injector).await?;
        return Ok(PersistRecipientReceiptOutcome::Duplicate(stored.metadata));
    }
    let command = load_command_by_operation(
        connection,
        params.command_operation_id,
        CommandPayloadAccess::Claim,
    )
    .await?
    .ok_or(InboxWriteError::IdentityConflict)?;
    injector
        .after_inbox_step(InboxStep::CommandRead)
        .map_err(inbox_internal)?;
    validate_command_context(&params, &command)?;
    let recipient_turn = known_turn(&params.context.actor)?.clone();
    let key = receipt_key(&params, command.metadata.kind, &recipient_turn);
    check_unclaimed_identities(connection, &params, &key, command.metadata.kind, injector).await?;
    let now = injector.now_ms().max(0);
    let (sender_intent_at_ms, encoded_payload_bytes) =
        command_delivery_fields(connection, &params).await?;
    let payload = command_ciphertext(&command)?;
    if now < sender_intent_at_ms || now >= command.metadata.expires_at_ms {
        return Err(InboxWriteError::Expired);
    }
    if !matches!(
        command.metadata.lifecycle,
        CommandLifecycle::Pending | CommandLifecycle::Leased
    ) {
        return Err(InboxWriteError::Expired);
    }
    if !target_is_current(connection, &command.metadata).await? {
        return Ok(PersistRecipientReceiptOutcome::Deferred);
    }
    fence_target(connection, &params, &command).await?;
    injector
        .after_inbox_step(InboxStep::TargetFence)
        .map_err(inbox_internal)?;
    let receipt_event = write_receipt_event(connection, &params, &command, injector).await?;
    injector
        .after_inbox_step(InboxStep::ReceiptEvent)
        .map_err(inbox_internal)?;
    let metadata = initial_metadata(
        &params,
        &command,
        &receipt_event,
        recipient_turn,
        encoded_payload_bytes,
    )?;
    let captured_set = command
        .metadata
        .target
        .captured_turn_set
        .as_ref()
        .map(crate::model::coordination_commands::CapturedGenerationSet::canonical_bytes);
    let command_fingerprint: [u8; 32] = command
        .command_fingerprint
        .as_slice()
        .try_into()
        .map_err(|_| InboxWriteError::CorruptStoredInbox)?;
    let ciphertext_fingerprint: [u8; 32] = command
        .ciphertext_fingerprint
        .as_slice()
        .try_into()
        .map_err(|_| InboxWriteError::CorruptStoredInbox)?;
    let delivery_fingerprint = delivery_fingerprint_from_parts(
        key.tuple_bytes(),
        &metadata,
        &command_fingerprint,
        &ciphertext_fingerprint,
        command.metadata.target.captured_head_generation,
        captured_set,
        sender_intent_at_ms,
        metadata.expires_at_ms,
    );
    insert_receipt(
        connection,
        &metadata,
        &key,
        &command,
        sender_intent_at_ms,
        now,
        delivery_fingerprint,
        payload.as_bytes(),
    )
    .await?;
    injector
        .after_inbox_step(InboxStep::ReceiptInsert)
        .map_err(inbox_internal)?;
    let stored = load_inbox_by_receipt(
        connection,
        params.receipt_id,
        InboxPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(InboxWriteError::CorruptStoredInbox)?;
    Ok(PersistRecipientReceiptOutcome::Applied(stored.metadata))
}

pub(super) fn validate_command_context(
    params: &PersistRecipientReceipt,
    command: &StoredCommand,
) -> Result<(), InboxWriteError> {
    let turn = known_turn(&params.context.actor)?;
    let target = &command.metadata.target;
    if params.context.root_thread_id != command.metadata.root_thread_id
        || params.context.primary.operation_id != command.metadata.operation_id
        || params.context.actor.thread_id != target.target_thread_id
        || target.turn_id.as_ref().is_some_and(|value| value != turn)
    {
        return Err(InboxWriteError::TurnFenced);
    }
    if !matches!(
        (&params.context.responsibility_owner, &target.turn_id),
        (Evidence::Known { value: owner }, _)
            if owner.thread_id == params.target.expected_owner_thread_id
                && matches!(&owner.turn_id, Evidence::Known { value } if value == &params.target.expected_owner_turn_id)
    ) {
        return Err(InboxWriteError::TurnFenced);
    }
    Ok(())
}

async fn fence_target(
    connection: &mut SqliteConnection,
    params: &PersistRecipientReceipt,
    command: &StoredCommand,
) -> Result<(), InboxWriteError> {
    let row = sqlx::query("SELECT root_thread_id,child_thread_id,owner_thread_id,owner_turn_id,version FROM coordination_assignment_heads WHERE assignment_id=?")
        .bind(command.metadata.target.assignment_id.to_string())
        .fetch_optional(&mut *connection).await.map_err(inbox_internal)?
        .ok_or(InboxWriteError::GenerationFenced)?;
    let expected_version = i64::try_from(params.target.expected_head_version)
        .map_err(|_| InboxWriteError::GenerationFenced)?;
    if row.get::<String, _>("root_thread_id") != params.context.root_thread_id.to_string()
        || row.get::<String, _>("child_thread_id")
            != command.metadata.target.target_thread_id.to_string()
        || row.get::<String, _>("owner_thread_id")
            != params.target.expected_owner_thread_id.to_string()
        || row.get::<String, _>("owner_turn_id") != params.target.expected_owner_turn_id.as_str()
        || row.get::<i64, _>("version") != expected_version
    {
        return Err(InboxWriteError::GenerationFenced);
    }
    Ok(())
}

async fn write_receipt_event(
    connection: &mut SqliteConnection,
    params: &PersistRecipientReceipt,
    command: &StoredCommand,
    injector: &dyn InboxFailureInjector,
) -> Result<CoordinationEvent, InboxWriteError> {
    let turn = known_turn(&params.context.actor)?.clone();
    match command.metadata.kind {
        CommandKind::AssignmentSpawn | CommandKind::AssignmentFollowup => {
            match accept(connection, accept_params(params, command, turn), injector).await? {
                AssignmentTransitionOutcome::Applied { events } => events
                    .into_iter()
                    .next()
                    .ok_or(InboxWriteError::CorruptStoredInbox),
                AssignmentTransitionOutcome::Duplicate { .. } => {
                    Err(InboxWriteError::CorruptStoredInbox)
                }
                AssignmentTransitionOutcome::Fenced { .. } => {
                    Err(InboxWriteError::GenerationFenced)
                }
            }
        }
        CommandKind::Message | CommandKind::Interrupt => {
            if !params.context.secondary.items().is_empty() {
                return Err(InboxWriteError::IdempotencyConflict);
            }
            let epoch = authority(connection, injector).await?;
            ensure_root(
                connection,
                &params.context.root_thread_id,
                epoch,
                /*create*/ false,
                injector,
            )
            .await?;
            fence_root_revision(connection, &params.context, injector).await?;
            let revision =
                allocate(connection, &params.context.root_thread_id, 1, injector).await?[0];
            let event = make_event(
                &params.context,
                &params.context.primary,
                epoch,
                revision,
                receipt_kind(command, params.receipt_id, turn)?,
                &[&command.event],
            )?;
            journal(
                connection,
                &params.context,
                std::slice::from_ref(&event),
                injector,
            )
            .await?;
            Ok(event)
        }
    }
}

pub(super) fn accept_params(
    params: &PersistRecipientReceipt,
    command: &StoredCommand,
    turn: BoundedId<MAX_ID_BYTES>,
) -> AcceptAssignment {
    AcceptAssignment {
        context: params.context.clone(),
        assignment_id: command.metadata.target.assignment_id,
        generation: command.metadata.target.generation,
        receipt_id: params.receipt_id,
        bound_turn_id: Evidence::Known { value: turn },
        expected_owner_thread_id: params.target.expected_owner_thread_id,
        expected_owner_turn_id: params.target.expected_owner_turn_id.clone(),
        expected_head_version: params.target.expected_head_version,
    }
}

async fn command_delivery_fields(
    connection: &mut SqliteConnection,
    params: &PersistRecipientReceipt,
) -> Result<(i64, u32), InboxWriteError> {
    let row = sqlx::query(
        "SELECT intent_at_ms,encoded_payload_bytes FROM coordination_commands WHERE operation_id=?",
    )
    .bind(params.command_operation_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(inbox_internal)?
    .ok_or(InboxWriteError::IdentityConflict)?;
    let encoded = row
        .get::<i64, _>("encoded_payload_bytes")
        .try_into()
        .map_err(|_| InboxWriteError::CorruptStoredInbox)?;
    Ok((row.get("intent_at_ms"), encoded))
}

fn initial_metadata(
    params: &PersistRecipientReceipt,
    command: &StoredCommand,
    event: &CoordinationEvent,
    recipient_turn_id: BoundedId<MAX_ID_BYTES>,
    encoded_payload_bytes: u32,
) -> Result<InboxReceiptMetadata, InboxWriteError> {
    let sender = &command.event.envelope().actor;
    Ok(InboxReceiptMetadata {
        receipt_id: params.receipt_id,
        command_operation_id: params.command_operation_id,
        root_thread_id: params.context.root_thread_id,
        intent_event_id: command.metadata.intent_event_id,
        receipt_event_id: event.envelope().event_id,
        sender_thread_id: sender.thread_id,
        sender_turn_id: known_turn(sender)?.clone(),
        recipient_thread_id: params.context.actor.thread_id,
        recipient_turn_id,
        kind: command.metadata.kind,
        target_assignment_id: command.metadata.target.assignment_id,
        target_generation: command.metadata.target.generation,
        lifecycle: InboxLifecycle::Received,
        version: 0,
        claim_count: 0,
        retry_count: 0,
        lease_epoch: 0,
        retry_after_ms: 0,
        expires_at_ms: command.metadata.expires_at_ms,
        encoded_payload_bytes,
        delivery_fingerprint: [0; 32],
    })
}

async fn insert_receipt(
    connection: &mut SqliteConnection,
    metadata: &InboxReceiptMetadata,
    key: &IdempotencyKey,
    command: &StoredCommand,
    sender_intent_at_ms: i64,
    now: i64,
    delivery_fingerprint: [u8; 32],
    ciphertext: &[u8],
) -> Result<(), InboxWriteError> {
    let captured_set = command.metadata.target.captured_turn_set.as_ref();
    sqlx::query("INSERT INTO coordination_inbox (receipt_id,command_operation_id,root_thread_id,intent_event_id,receipt_event_id,sender_thread_id,sender_turn_id,recipient_thread_id,recipient_turn_id,operation_kind,target_assignment_id,target_generation,captured_head_generation,captured_turn_set_bytes,captured_turn_set_fingerprint,receipt_tuple_bytes,receipt_tuple_fingerprint,delivery_fingerprint,sender_command_fingerprint,encoded_payload_bytes,ciphertext,ciphertext_fingerprint,lifecycle,version,claim_count,retry_count,lease_epoch,retry_after_ms,lease_expires_at_ms,failure_code,sender_intent_at_ms,durable_received_at_ms,terminal_at_ms,absolute_expires_at_ms,expires_at_ms,purged_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'received',0,0,0,0,0,NULL,NULL,?,?,NULL,?,?,NULL,?)")
        .bind(metadata.receipt_id.to_string())
        .bind(metadata.command_operation_id.to_string())
        .bind(metadata.root_thread_id.to_string())
        .bind(metadata.intent_event_id.to_string())
        .bind(metadata.receipt_event_id.to_string())
        .bind(metadata.sender_thread_id.to_string())
        .bind(metadata.sender_turn_id.as_str())
        .bind(metadata.recipient_thread_id.to_string())
        .bind(metadata.recipient_turn_id.as_str())
        .bind(metadata.kind.as_sql())
        .bind(metadata.target_assignment_id.to_string())
        .bind(metadata.target_generation.get() as i64)
        .bind(command.metadata.target.captured_head_generation.map(|value| value.get() as i64))
        .bind(captured_set.map(crate::model::coordination_commands::CapturedGenerationSet::canonical_bytes))
        .bind(captured_set.map(|set| sha256(set.canonical_bytes()).to_vec()))
        .bind(key.tuple_bytes())
        .bind(key.fingerprint().as_slice())
        .bind(delivery_fingerprint.as_slice())
        .bind(&command.command_fingerprint)
        .bind(metadata.encoded_payload_bytes as i64)
        .bind(ciphertext)
        .bind(&command.ciphertext_fingerprint)
        .bind(sender_intent_at_ms)
        .bind(now)
        .bind(metadata.expires_at_ms)
        .bind(metadata.expires_at_ms)
        .bind(now)
        .execute(&mut *connection)
        .await
        .map_err(inbox_internal)?;
    Ok(())
}
