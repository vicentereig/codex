use codex_coordination::AssignmentEvidence;
use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentMode;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::CoordinationTarget;
use codex_coordination::Evidence;
use codex_coordination::GenerationCloseReason;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::accept_duplicate::duplicate_accept_bundle;
use super::aggregate_journal::*;
use super::aggregates::AssignmentTransitionOutcome;
use crate::model::coordination::*;

pub(super) async fn accept(
    connection: &mut SqliteConnection,
    params: AcceptAssignment,
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
    let head = head_row(connection, params.assignment_id, injector)
        .await?
        .ok_or(CoordinationWriteError::AssignmentConflict)?;
    if head.root != params.context.root_thread_id.to_string() {
        return Err(CoordinationWriteError::RootMismatch);
    }
    let generation_state = generation_row(
        connection,
        params.assignment_id,
        params.generation,
        injector,
    )
    .await?
    .ok_or(CoordinationWriteError::AssignmentConflict)?;
    let request = load_event_id(connection, &generation_state.request_event, injector).await?;
    let (operation_id, mode, target) = requested_fields(&request.event)?;
    if params.context.primary.operation_id != operation_id {
        return Err(CoordinationWriteError::DivergentIntent);
    }
    let Evidence::Known { value: bound_turn } = &params.bound_turn_id else {
        return Err(CoordinationWriteError::AssignmentConflict);
    };
    let AssignmentEvidence::Known {
        assignment_id: target_assignment,
        generation: target_generation,
    } = &target.assignment
    else {
        return Err(CoordinationWriteError::CorruptStoredEvent);
    };
    if *target_assignment != params.assignment_id
        || *target_generation != params.generation
        || target.principal.thread_id.to_string() != head.child
        || params.context.actor.thread_id != target.principal.thread_id
        || known_turn(&params.context.actor)? != bound_turn
        || matches!(&target.principal.turn_id, Evidence::Known { value } if value != bound_turn)
    {
        return Err(CoordinationWriteError::GenerationFenced);
    }
    let kind = CoordinationEventKind::AssignmentAccepted {
        operation_id,
        mode,
        target,
        receipt_id: params.receipt_id,
        bound_turn_id: params.bound_turn_id.clone(),
    };
    if let Some(stored) = load_idempotent(
        connection,
        &params.context,
        &params.context.primary,
        CoordinationSemanticSlot::AssignmentAccepted,
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
            &[&request.event],
            &stored.event,
        )?;
        return duplicate_accept_bundle(
            connection,
            &params.context,
            epoch,
            stored,
            params.assignment_id,
            params.generation,
            injector,
        )
        .await;
    }
    let head = head_row(connection, params.assignment_id, injector)
        .await?
        .ok_or(CoordinationWriteError::AssignmentConflict)?;
    if head.root != params.context.root_thread_id.to_string() {
        return Err(CoordinationWriteError::RootMismatch);
    }
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
    if let Some(current) = head.accepted
        && params.generation.get() < current as u32
    {
        return Ok(AssignmentTransitionOutcome::Fenced {
            current_generation: generation(current)?,
        });
    }
    if generation_state.lifecycle != "reserved" {
        return Err(CoordinationWriteError::AssignmentConflict);
    }
    let previous = head
        .accepted
        .filter(|value| *value != params.generation.get() as i64);
    let count = if previous.is_some() { 2 } else { 1 };
    if previous.is_some() && params.context.secondary.items().len() != 1 {
        return Err(CoordinationWriteError::IdempotencyConflict);
    }
    if params.context.secondary.items().len() > 1 {
        return Err(CoordinationWriteError::IdempotencyConflict);
    }
    if let Some(identity) = params.context.secondary.items().first()
        && load_idempotent(
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
    let revisions = allocate(connection, &params.context.root_thread_id, count, injector).await?;
    let accepted = make_event(
        &params.context,
        &params.context.primary,
        epoch,
        revisions[0],
        kind,
        &[&request.event],
    )?;
    let mut events = vec![accepted.clone()];
    if let Some(previous) = previous {
        if params.context.secondary.items().len() != 1 {
            return Err(CoordinationWriteError::IdempotencyConflict);
        }
        let identity = &params.context.secondary.items()[0];
        let close = make_event(
            &params.context,
            identity,
            epoch,
            revisions[1],
            CoordinationEventKind::AssignmentGenerationClosed {
                assignment: AssignmentEvidence::Known {
                    assignment_id: params.assignment_id,
                    generation: generation(previous)?,
                },
                close_reason: GenerationCloseReason::Superseded {
                    by_generation: params.generation,
                },
            },
            &[&accepted],
        )?;
        events.push(close);
    }
    let now = now_ms(injector);
    let changed = sqlx::query("UPDATE coordination_assignment_generations SET lifecycle='accepted',accepted_event_id=?,accepted_receipt_id=?,last_revision=?,updated_at_ms=? WHERE assignment_id=? AND generation=? AND lifecycle='reserved'")
        .bind(accepted.envelope().event_id.to_string()).bind(params.receipt_id.to_string()).bind(revisions[0].get() as i64).bind(now)
        .bind(params.assignment_id.to_string()).bind(params.generation.get() as i64).execute(&mut *connection).await.map_err(internal)?.rows_affected();
    if changed != 1 {
        return Err(CoordinationWriteError::AssignmentConflict);
    }
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    sqlx::query("INSERT INTO coordination_turn_bindings (assignment_id,generation,root_thread_id,turn_id,accepted_event_id,created_at_ms) VALUES (?,?,?,?,?,?)")
        .bind(params.assignment_id.to_string()).bind(params.generation.get() as i64).bind(params.context.root_thread_id.to_string()).bind(bound_turn.as_str()).bind(accepted.envelope().event_id.to_string()).bind(now)
        .execute(&mut *connection).await.map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    if let Some(previous) = previous {
        let close_reason = serde_json::to_string(&GenerationCloseReason::Superseded {
            by_generation: params.generation,
        })
        .map_err(internal)?;
        let changed = sqlx::query("UPDATE coordination_assignment_generations SET lifecycle='superseded',superseded_event_id=?,close_reason_json=?,last_revision=?,updated_at_ms=? WHERE assignment_id=? AND generation=? AND lifecycle='accepted'")
            .bind(events[1].envelope().event_id.to_string()).bind(close_reason).bind(revisions[1].get() as i64).bind(now).bind(params.assignment_id.to_string()).bind(previous)
            .execute(&mut *connection).await.map_err(internal)?.rows_affected();
        if changed != 1 {
            return Err(CoordinationWriteError::AssignmentConflict);
        }
        injector
            .after_step(AggregateStep::AggregateMutation)
            .map_err(internal)?;
    }
    let last_revision = revisions[count - 1];
    let expected_head_version = i64::try_from(params.expected_head_version)
        .map_err(|_| CoordinationWriteError::VersionFenced)?;
    let changed = sqlx::query("UPDATE coordination_assignment_heads SET accepted_generation=?,version=version+1,last_revision=?,updated_at_ms=? WHERE assignment_id=? AND version=?")
        .bind(params.generation.get() as i64).bind(last_revision.get() as i64).bind(now).bind(params.assignment_id.to_string()).bind(expected_head_version)
        .execute(&mut *connection).await.map_err(internal)?.rows_affected();
    if changed != 1 {
        return Err(CoordinationWriteError::VersionFenced);
    }
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    journal(connection, &params.context, &events, injector).await?;
    Ok(AssignmentTransitionOutcome::Applied { events })
}

pub(super) async fn close_reserved(
    connection: &mut SqliteConnection,
    params: CloseReservedAssignment,
    injector: &dyn AggregateFailureInjector,
) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
    validate_identities(&params.context)?;
    if !params.context.secondary.items().is_empty() {
        return Err(CoordinationWriteError::IdempotencyConflict);
    }
    if !matches!(
        params.reason,
        GenerationCloseReason::DeliveryFailed { .. }
            | GenerationCloseReason::AbandonedBeforeAcceptance
    ) {
        return Err(CoordinationWriteError::AssignmentConflict);
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
    let head = head_row(connection, params.assignment_id, injector)
        .await?
        .ok_or(CoordinationWriteError::AssignmentConflict)?;
    if head.root != params.context.root_thread_id.to_string() {
        return Err(CoordinationWriteError::RootMismatch);
    }
    let generation_state = generation_row(
        connection,
        params.assignment_id,
        params.generation,
        injector,
    )
    .await?
    .ok_or(CoordinationWriteError::AssignmentConflict)?;
    let request = load_event_id(connection, &generation_state.request_event, injector).await?;
    let kind = CoordinationEventKind::AssignmentGenerationClosed {
        assignment: AssignmentEvidence::Known {
            assignment_id: params.assignment_id,
            generation: params.generation,
        },
        close_reason: params.reason.clone(),
    };
    if let Some(stored) = load_idempotent(
        connection,
        &params.context,
        &params.context.primary,
        CoordinationSemanticSlot::AssignmentGenerationClosed,
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
            &[&request.event],
            &stored.event,
        )?;
        return Ok(AssignmentTransitionOutcome::Duplicate {
            events: vec![stored.event],
        });
    }
    if params.context.actor.thread_id != params.expected_owner_thread_id
        || known_turn(&params.context.actor)? != &params.expected_owner_turn_id
    {
        return Err(CoordinationWriteError::OwnerFenced);
    }
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
    if generation_state.lifecycle != "reserved" {
        return Err(CoordinationWriteError::AssignmentConflict);
    }
    let revisions = allocate(connection, &params.context.root_thread_id, 1, injector).await?;
    let event = make_event(
        &params.context,
        &params.context.primary,
        epoch,
        revisions[0],
        kind,
        &[&request.event],
    )?;
    let close_reason = serde_json::to_string(&params.reason).map_err(internal)?;
    let changed = sqlx::query("UPDATE coordination_assignment_generations SET lifecycle=?,close_event_id=?,close_reason_json=?,last_revision=?,updated_at_ms=? WHERE assignment_id=? AND generation=? AND lifecycle='reserved'")
        .bind("abandoned").bind(event.envelope().event_id.to_string()).bind(close_reason).bind(revisions[0].get() as i64).bind(now_ms(injector)).bind(params.assignment_id.to_string()).bind(params.generation.get() as i64)
        .execute(&mut *connection).await.map_err(internal)?.rows_affected();
    if changed != 1 {
        return Err(CoordinationWriteError::AssignmentConflict);
    }
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    journal(
        connection,
        &params.context,
        std::slice::from_ref(&event),
        injector,
    )
    .await?;
    Ok(AssignmentTransitionOutcome::Applied {
        events: vec![event],
    })
}

pub(super) struct HeadRow {
    pub(super) root: String,
    pub(super) child: String,
    pub(super) accepted: Option<i64>,
    pub(super) next: i64,
    pub(super) owner_thread: String,
    pub(super) owner_turn: String,
    pub(super) version: i64,
}

pub(super) fn fence_assignment_owner(
    context: &NativeEventContext,
    head: &HeadRow,
    expected_thread_id: &codex_protocol::ThreadId,
    expected_turn_id: &codex_coordination::BoundedId<{ codex_coordination::MAX_ID_BYTES }>,
) -> Result<(), CoordinationWriteError> {
    if head.owner_thread != expected_thread_id.to_string()
        || head.owner_turn != expected_turn_id.as_str()
        || !matches!(
            &context.responsibility_owner,
            Evidence::Known { value }
                if value.thread_id == *expected_thread_id
                    && matches!(&value.turn_id, Evidence::Known { value } if value == expected_turn_id)
        )
    {
        return Err(CoordinationWriteError::OwnerFenced);
    }
    Ok(())
}
pub(super) struct GenerationRow {
    pub(super) lifecycle: String,
    pub(super) request_event: String,
}

pub(super) async fn head_row(
    connection: &mut SqliteConnection,
    assignment_id: codex_coordination::AssignmentId,
    injector: &dyn AggregateFailureInjector,
) -> Result<Option<HeadRow>, CoordinationWriteError> {
    let row = sqlx::query("SELECT root_thread_id,child_thread_id,accepted_generation,next_generation,owner_thread_id,owner_turn_id,version FROM coordination_assignment_heads WHERE assignment_id=?")
        .bind(assignment_id.to_string()).fetch_optional(&mut *connection).await.map_err(internal)?.map(|row| HeadRow {
            root: row.get("root_thread_id"), child: row.get("child_thread_id"), accepted: row.get("accepted_generation"), next: row.get("next_generation"),
            owner_thread: row.get("owner_thread_id"), owner_turn: row.get("owner_turn_id"), version: row.get("version"),
        });
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    Ok(row)
}

pub(super) async fn generation_row(
    connection: &mut SqliteConnection,
    assignment_id: codex_coordination::AssignmentId,
    generation: AssignmentGeneration,
    injector: &dyn AggregateFailureInjector,
) -> Result<Option<GenerationRow>, CoordinationWriteError> {
    let row = sqlx::query("SELECT lifecycle,request_event_id FROM coordination_assignment_generations WHERE assignment_id=? AND generation=?")
        .bind(assignment_id.to_string()).bind(generation.get() as i64).fetch_optional(&mut *connection).await.map_err(internal)?.map(|row| GenerationRow {
            lifecycle: row.get("lifecycle"), request_event: row.get("request_event_id"),
        });
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    Ok(row)
}

fn requested_fields(
    event: &CoordinationEvent,
) -> Result<
    (
        codex_coordination::CoordinationOperationId,
        AssignmentMode,
        CoordinationTarget,
    ),
    CoordinationWriteError,
> {
    match event.kind() {
        CoordinationEventKind::AssignmentRequested {
            operation_id,
            mode,
            target,
            ..
        } => Ok((*operation_id, *mode, target.clone())),
        _ => Err(CoordinationWriteError::CorruptStoredEvent),
    }
}
