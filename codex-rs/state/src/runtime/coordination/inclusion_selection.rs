use codex_coordination::BoundedId;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationOrder;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use sqlx::SqliteConnection;

use super::aggregate_journal::*;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::inbox::internal;
use super::inbox_claim::unresolved_interrupt_blocks;
use super::inbox_rows::InboxPayloadAccess;
use super::inbox_rows::StoredInbox;
use super::inbox_rows::load_inbox_by_receipt;
use super::inclusion_rows::TransportState;
use super::inclusion_rows::committed_selection;
use super::inclusion_rows::latest_selection;
use super::inclusion_rows::load_selection;
use crate::model::coordination::NativeEventContext;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_inbox::RecordInboxSelectionOutcome;

pub(super) async fn record_selection(
    connection: &mut SqliteConnection,
    params: RecordInboxSelection,
    injector: &dyn InboxFailureInjector,
) -> Result<RecordInboxSelectionOutcome, InboxWriteError> {
    authority(connection, injector).await?;
    let inbox = load_inbox_by_receipt(
        connection,
        params.lease.receipt_id,
        InboxPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(InboxWriteError::IdentityConflict)?;
    if let Some(existing) = load_selection(
        connection,
        params.lease.receipt_id,
        &params.inference_attempt_id,
    )
    .await?
    {
        validate_duplicate_selection(connection, &params, &inbox, &existing, injector).await?;
        return Ok(RecordInboxSelectionOutcome::Duplicate(committed_selection(
            &existing,
        )));
    }
    if params.selected_at_ms < 0
        || params.selected_at_ms >= inbox.metadata.expires_at_ms
        || params.selected_at_ms >= params.lease.lease_expires_at_ms
    {
        return Ok(RecordInboxSelectionOutcome::Expired);
    }
    if inbox.metadata.lifecycle != crate::model::coordination_inbox::InboxLifecycle::Leased
        || inbox.metadata.version != params.lease.version
        || inbox.metadata.lease_epoch != params.lease.lease_epoch
        || inbox.metadata.recipient_turn_id != params.lease.target_turn_id
        || inbox.metadata.delivery_fingerprint != params.lease.delivery_fingerprint
        || inbox.lease_expires_at_ms != Some(params.lease.lease_expires_at_ms)
        || inbox.lease_claim_operation_id != Some(params.lease.claim_operation_id)
    {
        return Ok(RecordInboxSelectionOutcome::Fenced);
    }
    if unresolved_interrupt_blocks(connection, &inbox).await? {
        return Ok(RecordInboxSelectionOutcome::NotReady);
    }
    let prior = latest_selection(connection, inbox.metadata.receipt_id).await?;
    let semantic_claim = prior.is_none();
    if prior.as_ref().is_some_and(|selection| {
        !matches!(
            selection.transport_state,
            TransportState::SendFailed | TransportState::SendUnknown
        )
    }) {
        return Ok(RecordInboxSelectionOutcome::NotReady);
    }
    let semantic_event_id =
        inclusion_event(connection, &params, &inbox, semantic_claim, injector).await?;
    let inbox_version = params
        .lease
        .version
        .checked_add(1)
        .ok_or(InboxWriteError::LeaseFenced)?;
    sqlx::query("INSERT INTO coordination_inbox_inclusions (receipt_id,inference_attempt_id,root_thread_id,target_turn_id,delivery_fingerprint,selected_at_ms,lease_expires_at_ms,semantic_claim,semantic_event_id,inbox_version,lease_epoch,claim_operation_id,transport_state,transport_completed_at_ms,retry_after_ms,version,failure_code) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'selected',NULL,NULL,0,NULL)")
        .bind(inbox.metadata.receipt_id.to_string())
        .bind(params.inference_attempt_id.as_str())
        .bind(inbox.metadata.root_thread_id.to_string())
        .bind(inbox.metadata.recipient_turn_id.as_str())
        .bind(inbox.metadata.delivery_fingerprint.as_slice())
        .bind(params.selected_at_ms)
        .bind(params.lease.lease_expires_at_ms)
        .bind(i64::from(semantic_claim))
        .bind(semantic_event_id.map(|event_id| event_id.to_string()))
        .bind(i64::try_from(inbox_version).map_err(|_| InboxWriteError::LeaseFenced)?)
        .bind(i64::try_from(params.lease.lease_epoch).map_err(|_| InboxWriteError::LeaseFenced)?)
        .bind(params.lease.claim_operation_id.to_string())
        .execute(&mut *connection).await.map_err(internal)?;
    injector
        .after_inbox_step(InboxStep::SelectionInsert)
        .map_err(internal)?;
    let changed = sqlx::query("UPDATE coordination_inbox SET lifecycle='selected',version=version+1,updated_at_ms=? WHERE receipt_id=? AND lifecycle='leased' AND version=? AND lease_epoch=? AND lease_claim_operation_id=? AND lease_expires_at_ms>?")
        .bind(params.selected_at_ms)
        .bind(inbox.metadata.receipt_id.to_string())
        .bind(i64::try_from(params.lease.version).map_err(|_| InboxWriteError::LeaseFenced)?)
        .bind(i64::try_from(params.lease.lease_epoch).map_err(|_| InboxWriteError::LeaseFenced)?)
        .bind(params.lease.claim_operation_id.to_string())
        .bind(params.selected_at_ms)
        .execute(&mut *connection).await.map_err(internal)?.rows_affected();
    if changed != 1 {
        return Ok(RecordInboxSelectionOutcome::Fenced);
    }
    injector
        .after_inbox_step(InboxStep::InboxUpdate)
        .map_err(internal)?;
    let stored = load_selection(
        connection,
        inbox.metadata.receipt_id,
        &params.inference_attempt_id,
    )
    .await?
    .ok_or(InboxWriteError::CorruptStoredInbox)?;
    Ok(RecordInboxSelectionOutcome::Applied(committed_selection(
        &stored,
    )))
}

async fn inclusion_event(
    connection: &mut SqliteConnection,
    params: &RecordInboxSelection,
    inbox: &StoredInbox,
    semantic_claim: bool,
    injector: &dyn InboxFailureInjector,
) -> Result<Option<CoordinationEventId>, InboxWriteError> {
    match (inbox.metadata.kind, semantic_claim, &params.event_context) {
        (CommandKind::AssignmentSpawn | CommandKind::AssignmentFollowup, _, None) => Ok(None),
        (CommandKind::Message, false, None) => Ok(None),
        (CommandKind::Message, true, Some(context)) => {
            validate_inclusion_context(context, inbox)?;
            if load_idempotent(
                connection,
                context,
                &context.primary,
                CoordinationSemanticSlot::MessageIncludedInModelInput,
                injector,
            )
            .await?
            .is_some()
            {
                return Err(InboxWriteError::CorruptStoredInbox);
            }
            let epoch = authority(connection, injector).await?;
            fence_root_revision(connection, context, injector).await?;
            let revision = allocate(connection, &context.root_thread_id, 1, injector).await?[0];
            let kind = message_inclusion_kind(inbox, params.inference_attempt_id.clone())?;
            let event = make_event(
                context,
                &context.primary,
                epoch,
                revision,
                kind,
                &[&inbox.receipt_event],
            )?;
            journal(connection, context, std::slice::from_ref(&event), injector).await?;
            Ok(Some(event.envelope().event_id))
        }
        (CommandKind::Interrupt, _, _) => Err(InboxWriteError::NotReady),
        _ => Err(InboxWriteError::IdempotencyConflict),
    }
}

fn validate_inclusion_context(
    context: &NativeEventContext,
    inbox: &StoredInbox,
) -> Result<(), InboxWriteError> {
    validate_identities(context)?;
    if !context.secondary.items().is_empty()
        || context.root_thread_id != inbox.metadata.root_thread_id
        || context.primary.operation_id != inbox.metadata.command_operation_id
        || context.actor.thread_id != inbox.metadata.recipient_thread_id
        || known_turn(&context.actor)? != &inbox.metadata.recipient_turn_id
    {
        return Err(InboxWriteError::IdempotencyConflict);
    }
    Ok(())
}

fn message_inclusion_kind(
    inbox: &StoredInbox,
    inference_attempt_id: BoundedId<MAX_ID_BYTES>,
) -> Result<CoordinationEventKind, InboxWriteError> {
    match inbox.receipt_event.kind() {
        CoordinationEventKind::MessageDurablyReceived {
            operation_id,
            target,
            receipt_id,
        } => Ok(CoordinationEventKind::MessageIncludedInModelInput {
            operation_id: *operation_id,
            target: target.clone(),
            receipt_id: *receipt_id,
            inference_attempt_id,
        }),
        _ => Err(InboxWriteError::CorruptStoredInbox),
    }
}

async fn validate_duplicate_selection(
    connection: &mut SqliteConnection,
    params: &RecordInboxSelection,
    inbox: &StoredInbox,
    stored: &super::inclusion_rows::StoredInclusion,
    injector: &dyn InboxFailureInjector,
) -> Result<(), InboxWriteError> {
    let expected_inbox_version = params
        .lease
        .version
        .checked_add(1)
        .ok_or(InboxWriteError::LeaseFenced)?;
    if stored.target_turn_id != params.lease.target_turn_id
        || stored.delivery_fingerprint != params.lease.delivery_fingerprint
        || stored.selected_at_ms != params.selected_at_ms
        || stored.lease_expires_at_ms != params.lease.lease_expires_at_ms
        || stored.inbox_version != expected_inbox_version
        || stored.lease_epoch != params.lease.lease_epoch
        || stored.claim_operation_id != params.lease.claim_operation_id
    {
        return Err(InboxWriteError::IdempotencyConflict);
    }
    match (stored.semantic_event_id, &params.event_context) {
        (None, None) => Ok(()),
        (Some(event_id), Some(context)) => {
            let stored_event = load_event_id(connection, &event_id.to_string(), injector).await?;
            let CoordinationOrder::Native {
                state_epoch,
                revision,
            } = stored_event.event.envelope().order
            else {
                return Err(InboxWriteError::CorruptStoredInbox);
            };
            compare_event(
                context,
                &context.primary,
                state_epoch,
                revision,
                message_inclusion_kind(inbox, params.inference_attempt_id.clone())?,
                &[&inbox.receipt_event],
                &stored_event.event,
            )?;
            Ok(())
        }
        _ => Err(InboxWriteError::IdempotencyConflict),
    }
}
