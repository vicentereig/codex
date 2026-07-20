use codex_coordination::AssignmentEvidence;
use codex_coordination::AssignmentGeneration;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::GenerationCloseReason;
use codex_coordination::InterruptionReason;
use codex_coordination::MAX_ID_BYTES;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::accept_transitions::fence_assignment_owner;
use super::accept_transitions::head_row;
use super::aggregate_journal::*;
use super::aggregates::AssignmentTransitionOutcome;
use super::inbox::InboxWriteError;
use super::inbox_rows::InboxPayloadAccess;
use super::inbox_rows::StoredInbox;
use super::inbox_rows::load_inbox_by_command;
use super::inclusion_gate::require_terminal_inclusions;
use super::inclusion_gate::resolve_interrupt_receipt;
use super::terminal_facts::terminal_fields;
use crate::model::coordination::*;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_inbox::InboxLifecycle;
use crate::model::coordination_inbox::ResolveInterruptReceipt;

struct ClosePlan {
    generation: AssignmentGeneration,
    reason: GenerationCloseReason,
    cause: Option<CoordinationEvent>,
}

pub(super) async fn terminal(
    connection: &mut SqliteConnection,
    params: TerminalAssignment,
    injector: &dyn AggregateFailureInjector,
) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
    validate_identities(&params.context)?;
    let epoch = authority(connection, injector).await?;
    ensure_root(
        connection,
        &params.context.root_thread_id,
        epoch,
        /*create*/ false,
        injector,
    )
    .await?;
    let (
        kind,
        assignment_id,
        target_generation,
        target_principal,
        target_thread_id,
        target_turn_id,
        included,
        close_reason,
        terminal_kind,
    ) = terminal_fields(&params)?;
    let head = head_row(connection, assignment_id, injector)
        .await?
        .ok_or(CoordinationWriteError::AssignmentConflict)?;
    if head.root != params.context.root_thread_id.to_string()
        || head.child != target_thread_id.to_string()
    {
        return Err(CoordinationWriteError::RootMismatch);
    }
    if params.context.actor != target_principal
        || known_turn(&params.context.actor)? != &target_turn_id
    {
        return Err(CoordinationWriteError::GenerationFenced);
    }
    if included.is_empty()
        || !included
            .windows(2)
            .all(|pair| pair[0].get() < pair[1].get())
    {
        return Err(CoordinationWriteError::AssignmentConflict);
    }
    if included.binary_search(&target_generation).is_err() {
        return Err(CoordinationWriteError::GenerationFenced);
    }
    let interrupt_receipt = requested_interrupt_receipt(
        connection,
        &params,
        assignment_id,
        target_generation,
        target_thread_id,
        &target_turn_id,
    )
    .await?;
    let terminal_causes = interrupt_receipt
        .as_ref()
        .map(|receipt| vec![&receipt.receipt_event])
        .unwrap_or_default();
    let included_json =
        serde_json::to_string(&included.iter().map(|value| value.get()).collect::<Vec<_>>())
            .map_err(internal)?;
    if let Some(stored) = load_idempotent(
        connection,
        &params.context,
        &params.context.primary,
        kind.semantic_slot(),
        injector,
    )
    .await?
    {
        compare_event(
            &params.context,
            &params.context.primary,
            epoch,
            stored.revision,
            kind,
            &terminal_causes,
            &stored.event,
        )?;
        if interrupt_receipt.as_ref().is_some_and(|receipt| {
            receipt.metadata.lifecycle != InboxLifecycle::Processed
                || receipt.resolution_event_id != Some(stored.event.envelope().event_id)
        }) {
            return Err(CoordinationWriteError::TerminalConflict);
        }
        let row = sqlx::query("SELECT terminal_event_id,terminal_kind,included_generations_json FROM coordination_turn_terminals WHERE root_thread_id=? AND target_thread_id=? AND target_turn_id=?")
            .bind(params.context.root_thread_id.to_string()).bind(target_thread_id.to_string()).bind(target_turn_id.as_str())
            .fetch_optional(&mut *connection).await.map_err(internal)?.ok_or(CoordinationWriteError::CorruptStoredEvent)?;
        injector
            .after_step(AggregateStep::AggregateRead)
            .map_err(internal)?;
        if row.get::<String, _>("terminal_event_id") != stored.event.envelope().event_id.to_string()
            || row.get::<String, _>("terminal_kind") != terminal_kind
            || row.get::<String, _>("included_generations_json") != included_json
        {
            return Err(CoordinationWriteError::TerminalConflict);
        }
        let close_ids = sqlx::query("SELECT generation,close_event_id FROM coordination_turn_terminal_generations WHERE root_thread_id=? AND target_thread_id=? AND target_turn_id=? AND close_event_id IS NOT NULL ORDER BY generation")
            .bind(params.context.root_thread_id.to_string()).bind(target_thread_id.to_string()).bind(target_turn_id.as_str()).fetch_all(&mut *connection).await.map_err(internal)?;
        injector
            .after_step(AggregateStep::AggregateRead)
            .map_err(internal)?;
        if close_ids.len() > params.context.secondary.items().len() {
            return Err(CoordinationWriteError::IdempotencyConflict);
        }
        let mut events = vec![stored.event];
        for (index, row) in close_ids.into_iter().enumerate() {
            let generation = generation(row.get::<i64, _>("generation"))?;
            let closed = load_idempotent(
                connection,
                &params.context,
                &params.context.secondary.items()[index],
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                injector,
            )
            .await?
            .ok_or(CoordinationWriteError::CorruptStoredEvent)?;
            if closed.event.envelope().event_id.to_string()
                != row.get::<String, _>("close_event_id")
            {
                return Err(CoordinationWriteError::CorruptStoredEvent);
            }
            let valid_kind = matches!(
                closed.event.kind(),
                CoordinationEventKind::AssignmentGenerationClosed {
                    assignment: AssignmentEvidence::Known {
                        assignment_id: stored_assignment,
                        generation: stored_generation,
                    },
                    close_reason: stored_reason,
                } if *stored_assignment == assignment_id
                    && *stored_generation == generation
                    && (stored_reason == &close_reason
                        || matches!(stored_reason, GenerationCloseReason::Superseded { by_generation } if *by_generation > generation))
            );
            if !valid_kind || closed.event.envelope().causes.items().len() != 1 {
                return Err(CoordinationWriteError::CorruptStoredEvent);
            }
            let cause = load_event_id(
                connection,
                &closed.event.envelope().causes.items()[0].to_string(),
                injector,
            )
            .await?;
            compare_event(
                &params.context,
                &params.context.secondary.items()[index],
                epoch,
                closed.revision,
                closed.event.kind().clone(),
                &[&cause.event],
                &closed.event,
            )?;
            events.push(closed.event);
        }
        for identity in &params.context.secondary.items()[events.len() - 1..] {
            if load_idempotent(
                connection,
                &params.context,
                identity,
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                injector,
            )
            .await?
            .is_some()
            {
                return Err(CoordinationWriteError::IdentityCollision);
            }
        }
        return Ok(AssignmentTransitionOutcome::Duplicate { events });
    }
    let existing_terminal = sqlx::query_scalar::<_,i64>("SELECT 1 FROM coordination_turn_terminals WHERE root_thread_id=? AND target_thread_id=? AND target_turn_id=?")
        .bind(params.context.root_thread_id.to_string()).bind(target_thread_id.to_string()).bind(target_turn_id.as_str()).fetch_optional(&mut *connection).await.map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    if existing_terminal.is_some() {
        return Err(CoordinationWriteError::TerminalConflict);
    }
    if interrupt_receipt.as_ref().is_some_and(|receipt| {
        receipt.metadata.lifecycle != InboxLifecycle::Received
            || receipt.resolution_event_id.is_some()
    }) {
        return Err(CoordinationWriteError::TerminalConflict);
    }
    require_terminal_inclusions(
        connection,
        params.context.root_thread_id,
        target_thread_id,
        &target_turn_id,
        assignment_id,
        &included,
    )
    .await?;
    fence_assignment_owner(
        &params.context,
        &head,
        &params.expected_owner_thread_id,
        &params.expected_owner_turn_id,
    )?;
    if head.version != i64::try_from(params.expected_head_version).map_err(internal)? {
        return Err(CoordinationWriteError::VersionFenced);
    }
    fence_root_revision(connection, &params.context, injector).await?;
    let mut plans = Vec::new();
    for included_generation in &included {
        let state = sqlx::query("SELECT lifecycle,request_event_id,terminal_event_id FROM coordination_assignment_generations WHERE assignment_id=? AND generation=?")
            .bind(assignment_id.to_string()).bind(included_generation.get() as i64)
            .fetch_optional(&mut *connection).await.map_err(internal)?
            .ok_or(CoordinationWriteError::GenerationFenced)?;
        injector
            .after_step(AggregateStep::AggregateRead)
            .map_err(internal)?;
        let lifecycle: String = state.get("lifecycle");
        if state
            .get::<Option<String>, _>("terminal_event_id")
            .is_some()
        {
            return Err(CoordinationWriteError::TerminalConflict);
        }
        let bound: Option<i64> = sqlx::query_scalar("SELECT 1 FROM coordination_turn_bindings WHERE assignment_id=? AND generation=? AND root_thread_id=? AND turn_id=?")
            .bind(assignment_id.to_string()).bind(included_generation.get() as i64)
            .bind(params.context.root_thread_id.to_string()).bind(target_turn_id.as_str())
            .fetch_optional(&mut *connection).await.map_err(internal)?;
        injector
            .after_step(AggregateStep::AggregateRead)
            .map_err(internal)?;
        if bound.is_none() {
            return Err(CoordinationWriteError::GenerationFenced);
        }
        match lifecycle.as_str() {
            "accepted" => {
                let newer = sqlx::query("SELECT generation,request_event_id FROM coordination_assignment_generations WHERE assignment_id=? AND generation>? AND lifecycle IN ('reserved','abandoned') ORDER BY generation DESC LIMIT 1")
                    .bind(assignment_id.to_string()).bind(included_generation.get() as i64)
                    .fetch_optional(&mut *connection).await.map_err(internal)?;
                injector
                    .after_step(AggregateStep::AggregateRead)
                    .map_err(internal)?;
                if let Some(newer) = newer {
                    let by_generation = generation(newer.get::<i64, _>("generation"))?;
                    let cause = load_event_id(
                        connection,
                        &newer.get::<String, _>("request_event_id"),
                        injector,
                    )
                    .await?;
                    plans.push(ClosePlan {
                        generation: *included_generation,
                        reason: GenerationCloseReason::Superseded { by_generation },
                        cause: Some(cause.event),
                    });
                } else {
                    plans.push(ClosePlan {
                        generation: *included_generation,
                        reason: close_reason.clone(),
                        cause: None,
                    });
                }
            }
            "superseded" => {}
            "reserved" | "abandoned" => return Err(CoordinationWriteError::GenerationFenced),
            "terminal" => return Err(CoordinationWriteError::TerminalConflict),
            _ => return Err(CoordinationWriteError::CorruptStoredEvent),
        }
    }
    if params.context.secondary.items().len() < plans.len() {
        return Err(CoordinationWriteError::IdempotencyConflict);
    }
    for identity in params.context.secondary.items() {
        if load_idempotent(
            connection,
            &params.context,
            identity,
            CoordinationSemanticSlot::AssignmentGenerationClosed,
            injector,
        )
        .await?
        .is_some()
        {
            return Err(CoordinationWriteError::CorruptStoredEvent);
        }
    }
    let revisions = allocate(
        connection,
        &params.context.root_thread_id,
        1 + plans.len(),
        injector,
    )
    .await?;
    let terminal = make_event(
        &params.context,
        &params.context.primary,
        epoch,
        revisions[0],
        kind,
        &terminal_causes,
    )?;
    let mut events = vec![terminal.clone()];
    for (index, plan) in plans.iter().enumerate() {
        let causes = match &plan.cause {
            Some(cause) => vec![cause],
            None => vec![&terminal],
        };
        events.push(make_event(
            &params.context,
            &params.context.secondary.items()[index],
            epoch,
            revisions[index + 1],
            CoordinationEventKind::AssignmentGenerationClosed {
                assignment: AssignmentEvidence::Known {
                    assignment_id,
                    generation: plan.generation,
                },
                close_reason: plan.reason.clone(),
            },
            &causes,
        )?);
    }
    let now = now_ms(injector);
    sqlx::query("INSERT INTO coordination_turn_terminals (root_thread_id,target_thread_id,target_turn_id,terminal_kind,terminal_event_id,included_generations_json,revision,created_at_ms) VALUES (?,?,?,?,?,?,?,?)")
        .bind(params.context.root_thread_id.to_string()).bind(target_thread_id.to_string()).bind(target_turn_id.as_str()).bind(terminal_kind)
        .bind(terminal.envelope().event_id.to_string()).bind(&included_json).bind(revisions[0].get() as i64).bind(now).execute(&mut *connection).await.map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    for included_generation in &included {
        let close_index = plans
            .iter()
            .position(|plan| plan.generation == *included_generation);
        let close_id = close_index.map(|index| events[index + 1].envelope().event_id.to_string());
        sqlx::query("INSERT INTO coordination_turn_terminal_generations (root_thread_id,target_thread_id,target_turn_id,assignment_id,generation,close_event_id) VALUES (?,?,?,?,?,?)")
            .bind(params.context.root_thread_id.to_string()).bind(target_thread_id.to_string()).bind(target_turn_id.as_str()).bind(assignment_id.to_string())
            .bind(included_generation.get() as i64).bind(close_id).execute(&mut *connection).await.map_err(internal)?;
        injector
            .after_step(AggregateStep::AggregateMutation)
            .map_err(internal)?;
        if let Some(index) = close_index {
            let plan = &plans[index];
            let reason_json = serde_json::to_string(&plan.reason).map_err(internal)?;
            let changed = if matches!(plan.reason, GenerationCloseReason::Superseded { .. }) {
                sqlx::query("UPDATE coordination_assignment_generations SET lifecycle='superseded',superseded_event_id=?,close_reason_json=?,last_revision=?,updated_at_ms=? WHERE assignment_id=? AND generation=? AND lifecycle='accepted'")
                    .bind(events[index + 1].envelope().event_id.to_string()).bind(&reason_json)
                    .bind(revisions[index + 1].get() as i64).bind(now).bind(assignment_id.to_string()).bind(included_generation.get() as i64)
                    .execute(&mut *connection).await.map_err(internal)?.rows_affected()
            } else {
                sqlx::query("UPDATE coordination_assignment_generations SET lifecycle='terminal',terminal_event_id=?,close_event_id=?,terminal_kind=?,terminal_reason_json=?,close_reason_json=?,last_revision=?,updated_at_ms=? WHERE assignment_id=? AND generation=? AND lifecycle='accepted'")
                    .bind(terminal.envelope().event_id.to_string()).bind(events[index + 1].envelope().event_id.to_string()).bind(terminal_kind).bind(&reason_json).bind(&reason_json)
                    .bind(revisions[index + 1].get() as i64).bind(now).bind(assignment_id.to_string()).bind(included_generation.get() as i64)
                    .execute(&mut *connection).await.map_err(internal)?.rows_affected()
            };
            if changed != 1 {
                return Err(CoordinationWriteError::AssignmentConflict);
            }
            injector
                .after_step(AggregateStep::AggregateMutation)
                .map_err(internal)?;
        }
    }
    if !plans.is_empty() {
        let last_revision = revisions[plans.len()];
        let expected_head_version = i64::try_from(params.expected_head_version)
            .map_err(|_| CoordinationWriteError::VersionFenced)?;
        let changed = sqlx::query("UPDATE coordination_assignment_heads SET accepted_generation=NULL,version=version+1,last_revision=?,updated_at_ms=? WHERE assignment_id=? AND version=? AND accepted_generation IN (SELECT generation FROM coordination_turn_terminal_generations WHERE root_thread_id=? AND target_thread_id=? AND target_turn_id=? AND close_event_id IS NOT NULL)")
            .bind(last_revision.get() as i64).bind(now).bind(assignment_id.to_string()).bind(expected_head_version).bind(params.context.root_thread_id.to_string()).bind(target_thread_id.to_string()).bind(target_turn_id.as_str())
            .execute(&mut *connection).await.map_err(internal)?.rows_affected();
        if changed != 1 {
            return Err(CoordinationWriteError::AssignmentConflict);
        }
        injector
            .after_step(AggregateStep::AggregateMutation)
            .map_err(internal)?;
    }
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    journal(connection, &params.context, &events, injector).await?;
    if let Some(receipt) = interrupt_receipt {
        resolve_interrupt_receipt(
            connection,
            ResolveInterruptReceipt {
                receipt_id: receipt.metadata.receipt_id,
                expected_version: receipt.metadata.version,
                terminal_event_id: terminal.envelope().event_id,
                resolved_at_ms: now,
            },
            injector,
        )
        .await
        .map_err(coordination_from_inbox)?;
    }
    Ok(AssignmentTransitionOutcome::Applied { events })
}

async fn requested_interrupt_receipt(
    connection: &mut SqliteConnection,
    params: &TerminalAssignment,
    assignment_id: codex_coordination::AssignmentId,
    generation: AssignmentGeneration,
    target_thread_id: codex_protocol::ThreadId,
    target_turn_id: &BoundedId<MAX_ID_BYTES>,
) -> Result<Option<StoredInbox>, CoordinationWriteError> {
    let TerminalTurn::Interrupted {
        interruption_reason: InterruptionReason::Requested { operation_id },
        ..
    } = &params.terminal
    else {
        return Ok(None);
    };
    let receipt =
        load_inbox_by_command(connection, *operation_id, InboxPayloadAccess::MetadataOnly)
            .await
            .map_err(coordination_from_inbox)?
            .ok_or(CoordinationWriteError::GenerationFenced)?;
    if receipt.metadata.kind != CommandKind::Interrupt
        || receipt.metadata.root_thread_id != params.context.root_thread_id
        || receipt.metadata.recipient_thread_id != target_thread_id
        || receipt.metadata.recipient_turn_id != *target_turn_id
        || receipt.metadata.target_assignment_id != assignment_id
        || receipt.metadata.target_generation != generation
    {
        return Err(CoordinationWriteError::GenerationFenced);
    }
    Ok(Some(receipt))
}

fn coordination_from_inbox(error: InboxWriteError) -> CoordinationWriteError {
    match error {
        InboxWriteError::Quarantined => CoordinationWriteError::Quarantined,
        InboxWriteError::GenerationFenced
        | InboxWriteError::TurnFenced
        | InboxWriteError::LeaseFenced
        | InboxWriteError::NotReady
        | InboxWriteError::Expired => CoordinationWriteError::GenerationFenced,
        InboxWriteError::IdempotencyCollision | InboxWriteError::IdentityConflict => {
            CoordinationWriteError::IdentityCollision
        }
        InboxWriteError::IdempotencyConflict => CoordinationWriteError::IdempotencyConflict,
        InboxWriteError::TerminalConflict => CoordinationWriteError::TerminalConflict,
        InboxWriteError::CorruptStoredInbox => CoordinationWriteError::CorruptStoredEvent,
        InboxWriteError::RootMissing => CoordinationWriteError::RootMismatch,
        InboxWriteError::Input(error) => CoordinationWriteError::Internal(error.into()),
        InboxWriteError::Internal(error) => CoordinationWriteError::Internal(error),
    }
}
