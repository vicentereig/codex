use codex_coordination::AssignmentEvidence;
use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEventKind;
use codex_coordination::InterruptionReason;
use codex_coordination::MAX_ID_BYTES;
use codex_protocol::ThreadId;
use sqlx::SqliteConnection;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::aggregate_journal::CoordinationWriteError;
use super::aggregate_journal::load_event_id;
use super::inbox::InboxWriteError;
use super::inbox::internal;
use super::inbox_rows::InboxPayloadAccess;
use super::inbox_rows::TERMINAL_INBOX_TTL_MS;
use super::inbox_rows::load_inbox_by_receipt;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_inbox::InboxLifecycle;
use crate::model::coordination_inbox::ResolveInterruptReceipt;

/// Requires durable local model-input inclusion for every terminal generation.
/// A prospective turn binding is deliberately insufficient evidence.
pub(super) async fn require_terminal_inclusions(
    connection: &mut SqliteConnection,
    root_thread_id: ThreadId,
    target_thread_id: ThreadId,
    target_turn_id: &BoundedId<MAX_ID_BYTES>,
    assignment_id: AssignmentId,
    generations: &[AssignmentGeneration],
) -> Result<(), CoordinationWriteError> {
    for generation in generations {
        let included: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM coordination_inbox_inclusions x JOIN coordination_inbox i USING (receipt_id) WHERE i.root_thread_id=? AND i.recipient_thread_id=? AND i.recipient_turn_id=? AND i.target_assignment_id=? AND i.target_generation=? AND i.operation_kind IN ('assignmentSpawn','assignmentFollowup') AND x.semantic_claim=1 LIMIT 1",
        )
        .bind(root_thread_id.to_string())
        .bind(target_thread_id.to_string())
        .bind(target_turn_id.as_str())
        .bind(assignment_id.to_string())
        .bind(generation.get() as i64)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| CoordinationWriteError::Internal(error.into()))?;
        if included != Some(1) {
            return Err(CoordinationWriteError::GenerationFenced);
        }
    }
    Ok(())
}

/// Resolves an interrupt barrier in the same transaction that journals its
/// causally-linked requested `TurnInterrupted` event.
pub(super) async fn resolve_interrupt_receipt(
    connection: &mut SqliteConnection,
    params: ResolveInterruptReceipt,
    injector: &dyn AggregateFailureInjector,
) -> Result<(), InboxWriteError> {
    let stored = load_inbox_by_receipt(
        connection,
        params.receipt_id,
        InboxPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(InboxWriteError::IdentityConflict)?;
    if stored.metadata.kind != CommandKind::Interrupt
        || stored.metadata.lifecycle != InboxLifecycle::Received
        || stored.metadata.version != params.expected_version
    {
        return Err(InboxWriteError::GenerationFenced);
    }
    let terminal =
        load_event_id(connection, &params.terminal_event_id.to_string(), injector).await?;
    match terminal.event.kind() {
        CoordinationEventKind::TurnInterrupted {
            target,
            target_turn_id,
            interruption_reason: InterruptionReason::Requested { operation_id },
            ..
        } if *operation_id == stored.metadata.command_operation_id
            && target_turn_id == &stored.metadata.recipient_turn_id
            && target.principal.thread_id == stored.metadata.recipient_thread_id
            && matches!(&target.assignment, AssignmentEvidence::Known { assignment_id, generation }
                if *assignment_id == stored.metadata.target_assignment_id
                    && *generation == stored.metadata.target_generation)
            && terminal
                .event
                .envelope()
                .causes
                .items()
                .contains(&stored.metadata.receipt_event_id) => {}
        _ => return Err(InboxWriteError::TerminalConflict),
    }
    let expires_at_ms = stored
        .metadata
        .expires_at_ms
        .min(params.resolved_at_ms.saturating_add(TERMINAL_INBOX_TTL_MS));
    let changed = sqlx::query("UPDATE coordination_inbox SET lifecycle='processed',version=version+1,resolution_event_id=?,terminal_at_ms=?,expires_at_ms=?,updated_at_ms=? WHERE receipt_id=? AND operation_kind='interrupt' AND lifecycle='received' AND version=?")
        .bind(params.terminal_event_id.to_string()).bind(params.resolved_at_ms).bind(expires_at_ms).bind(params.resolved_at_ms)
        .bind(params.receipt_id.to_string()).bind(i64::try_from(params.expected_version).map_err(|_| InboxWriteError::GenerationFenced)?)
        .execute(&mut *connection).await.map_err(internal)?.rows_affected();
    if changed != 1 {
        return Err(InboxWriteError::GenerationFenced);
    }
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    Ok(())
}
