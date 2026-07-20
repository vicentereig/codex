use codex_coordination::AssignmentEvidence;
use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentMode;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::CoordinationTarget;
use sqlx::SqliteConnection;

use super::accept_transitions::head_row;
use super::aggregate_journal::*;
use super::aggregates::ReserveAssignmentOutcome;
use crate::model::coordination::*;

pub(super) async fn reserve(
    connection: &mut SqliteConnection,
    params: ReserveAssignment,
    injector: &dyn AggregateFailureInjector,
) -> Result<ReserveAssignmentOutcome, CoordinationWriteError> {
    validate_identities(&params.context)?;
    if !params.context.secondary.items().is_empty() {
        return Err(CoordinationWriteError::IdempotencyConflict);
    }
    if params.context.primary.operation_id != params.operation_id
        || params.child_thread_id != params.target_principal.thread_id
    {
        return Err(CoordinationWriteError::DivergentIntent);
    }
    let epoch = authority(connection, injector).await?;
    if let Some(stored) = load_idempotent(
        connection,
        &params.context,
        &params.context.primary,
        CoordinationSemanticSlot::AssignmentRequested,
        injector,
    )
    .await?
    {
        let (generation, kind) = request_kind_from_duplicate(&params, stored.event.kind())?;
        compare_event(
            &params.context,
            &params.context.primary,
            epoch,
            stored.revision,
            kind,
            &[],
            &stored.event,
        )?;
        return Ok(ReserveAssignmentOutcome::Duplicate {
            generation,
            event: stored.event,
        });
    }
    let (generation, mode) = match &params.reservation {
        AssignmentReservation::Spawn => {
            if !matches!(
                &params.context.responsibility_owner,
                codex_coordination::Evidence::Known { value } if value == &params.context.actor
            ) {
                return Err(CoordinationWriteError::OwnerFenced);
            }
            ensure_root(
                connection,
                &params.context.root_thread_id,
                epoch,
                /*create*/ true,
                injector,
            )
            .await?;
            fence_root_revision(connection, &params.context, injector).await?;
            if head_row(connection, params.assignment_id, injector)
                .await?
                .is_some()
            {
                return Err(CoordinationWriteError::AssignmentConflict);
            }
            (
                AssignmentGeneration::new(1).map_err(internal)?,
                AssignmentMode::Spawn,
            )
        }
        AssignmentReservation::Followup {
            expected_owner_thread_id,
            expected_owner_turn_id,
            expected_head_version,
        } => {
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
            if head.root != params.context.root_thread_id.to_string()
                || head.child != params.child_thread_id.to_string()
            {
                return Err(CoordinationWriteError::RootMismatch);
            }
            if head.owner_thread != expected_owner_thread_id.to_string()
                || head.owner_turn != expected_owner_turn_id.as_str()
                || params.context.actor.thread_id != *expected_owner_thread_id
                || known_turn(&params.context.actor)? != expected_owner_turn_id
                || !matches!(
                    &params.context.responsibility_owner,
                    codex_coordination::Evidence::Known { value } if value == &params.context.actor
                )
            {
                return Err(CoordinationWriteError::OwnerFenced);
            }
            if head.version != i64::try_from(*expected_head_version).map_err(internal)? {
                return Err(CoordinationWriteError::VersionFenced);
            }
            fence_root_revision(connection, &params.context, injector).await?;
            let accepted_history: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM coordination_assignment_generations WHERE assignment_id=? AND accepted_event_id IS NOT NULL",
            )
            .bind(params.assignment_id.to_string())
            .fetch_one(&mut *connection)
            .await
            .map_err(internal)?;
            injector
                .after_step(AggregateStep::AggregateRead)
                .map_err(internal)?;
            if accepted_history == 0 {
                return Err(CoordinationWriteError::GenerationFenced);
            }
            (generation(head.next)?, AssignmentMode::Followup)
        }
    };
    let kind = request_kind(&params, generation, mode);
    let revisions = allocate(connection, &params.context.root_thread_id, 1, injector).await?;
    let event = make_event(
        &params.context,
        &params.context.primary,
        epoch,
        revisions[0],
        kind,
        &[],
    )?;
    let now = now_ms(injector);
    match params.reservation {
        AssignmentReservation::Spawn => {
            let actor_turn = known_turn(&params.context.actor)?;
            sqlx::query("INSERT INTO coordination_assignment_heads (assignment_id,root_thread_id,child_thread_id,accepted_generation,next_generation,owner_thread_id,owner_turn_id,version,last_revision,created_at_ms,updated_at_ms) VALUES (?,?,?,NULL,2,?,?,0,?,?,?)")
                .bind(params.assignment_id.to_string()).bind(params.context.root_thread_id.to_string()).bind(params.child_thread_id.to_string())
                .bind(params.context.actor.thread_id.to_string()).bind(actor_turn.as_str()).bind(revisions[0].get() as i64).bind(now).bind(now)
                .execute(&mut *connection).await.map_err(internal)?;
            injector
                .after_step(AggregateStep::AggregateMutation)
                .map_err(internal)?;
        }
        AssignmentReservation::Followup {
            expected_head_version,
            ..
        } => {
            let expected_head_version = i64::try_from(expected_head_version)
                .map_err(|_| CoordinationWriteError::VersionFenced)?;
            let changed = sqlx::query("UPDATE coordination_assignment_heads SET next_generation=next_generation+1,version=version+1,last_revision=?,updated_at_ms=? WHERE assignment_id=? AND version=?")
                .bind(revisions[0].get() as i64).bind(now).bind(params.assignment_id.to_string()).bind(expected_head_version)
                .execute(&mut *connection).await.map_err(internal)?.rows_affected();
            if changed != 1 {
                return Err(CoordinationWriteError::VersionFenced);
            }
            injector
                .after_step(AggregateStep::AggregateMutation)
                .map_err(internal)?;
        }
    }
    sqlx::query("INSERT INTO coordination_assignment_generations (assignment_id,generation,operation_id,mode,lifecycle,request_event_id,created_revision,last_revision,created_at_ms,updated_at_ms) VALUES (?,?,?,?,'reserved',?,?,?,?,?)")
        .bind(params.assignment_id.to_string()).bind(generation.get() as i64).bind(params.operation_id.to_string()).bind(mode_sql(mode)).bind(event.envelope().event_id.to_string())
        .bind(revisions[0].get() as i64).bind(revisions[0].get() as i64).bind(now).bind(now)
        .execute(&mut *connection).await.map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    journal(connection, &params.context, &[event.clone()], injector).await?;
    Ok(ReserveAssignmentOutcome::Reserved { generation, event })
}

fn request_kind(
    params: &ReserveAssignment,
    generation: AssignmentGeneration,
    mode: AssignmentMode,
) -> CoordinationEventKind {
    CoordinationEventKind::AssignmentRequested {
        operation_id: params.operation_id,
        mode,
        target: CoordinationTarget {
            principal: params.target_principal.clone(),
            assignment: AssignmentEvidence::Known {
                assignment_id: params.assignment_id,
                generation,
            },
        },
        objective: params.objective.clone(),
        encoded_payload_bytes: params.encoded_payload_bytes,
        requested_runtime: params.requested_runtime.clone(),
    }
}

fn request_kind_from_duplicate(
    params: &ReserveAssignment,
    stored: &CoordinationEventKind,
) -> Result<(AssignmentGeneration, CoordinationEventKind), CoordinationWriteError> {
    let CoordinationEventKind::AssignmentRequested { target, .. } = stored else {
        return Err(CoordinationWriteError::IdempotencyConflict);
    };
    let AssignmentEvidence::Known {
        assignment_id,
        generation,
    } = target.assignment
    else {
        return Err(CoordinationWriteError::CorruptStoredEvent);
    };
    if assignment_id != params.assignment_id {
        return Err(CoordinationWriteError::DivergentIntent);
    }
    let mode = match params.reservation {
        AssignmentReservation::Spawn => AssignmentMode::Spawn,
        AssignmentReservation::Followup { .. } => AssignmentMode::Followup,
    };
    Ok((generation, request_kind(params, generation, mode)))
}
