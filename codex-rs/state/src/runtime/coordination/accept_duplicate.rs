use codex_coordination::AssignmentEvidence;
use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::GenerationCloseReason;
use codex_coordination::StateEpoch;
use sqlx::SqliteConnection;

use super::aggregate_journal::*;
use super::aggregates::AssignmentTransitionOutcome;
use crate::model::coordination::NativeEventContext;

pub(super) async fn duplicate_accept_bundle(
    connection: &mut SqliteConnection,
    context: &NativeEventContext,
    epoch: StateEpoch,
    first: StoredEvent,
    assignment_id: AssignmentId,
    by_generation: AssignmentGeneration,
    injector: &dyn AggregateFailureInjector,
) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT superseded_event_id FROM coordination_assignment_generations WHERE assignment_id=? AND superseded_event_id IS NOT NULL",
    )
    .bind(assignment_id.to_string())
    .fetch_all(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    let mut matched = None;
    for event_id in rows {
        let candidate = load_event_id(connection, &event_id, injector).await?;
        let is_match = matches!(
            candidate.event.kind(),
            CoordinationEventKind::AssignmentGenerationClosed {
                assignment:
                    AssignmentEvidence::Known {
                        assignment_id: stored_assignment,
                        generation,
                    },
                close_reason:
                    GenerationCloseReason::Superseded {
                        by_generation: stored_by,
                    },
            } if *stored_assignment == assignment_id
                && *stored_by == by_generation
                && *generation != by_generation
        ) && candidate.event.envelope().causes.items()
            == [first.event.envelope().event_id];
        if is_match && matched.replace(candidate).is_some() {
            return Err(CoordinationWriteError::CorruptStoredEvent);
        }
    }
    let Some(second) = matched else {
        for identity in context.secondary.items() {
            if load_idempotent(
                connection,
                context,
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
        return Ok(AssignmentTransitionOutcome::Duplicate {
            events: vec![first.event],
        });
    };
    if second.revision.get() != first.revision.get() + 1 {
        return Err(CoordinationWriteError::CorruptStoredEvent);
    }
    if context.secondary.items().is_empty() {
        return Ok(AssignmentTransitionOutcome::Duplicate {
            events: vec![first.event, second.event],
        });
    }
    if context.secondary.items().len() != 1 {
        return Err(CoordinationWriteError::IdentityCollision);
    }
    let identity = &context.secondary.items()[0];
    let supplied = load_idempotent(
        connection,
        context,
        identity,
        CoordinationSemanticSlot::AssignmentGenerationClosed,
        injector,
    )
    .await?
    .ok_or(CoordinationWriteError::DivergentIntent)?;
    if supplied.event != second.event {
        return Err(CoordinationWriteError::DivergentIntent);
    }
    compare_event(
        context,
        identity,
        epoch,
        second.revision,
        second.event.kind().clone(),
        &[&first.event],
        &second.event,
    )?;
    second
        .event
        .validate_resolved_causes(&[&first.event])
        .map_err(|_| CoordinationWriteError::CorruptStoredEvent)?;
    Ok(AssignmentTransitionOutcome::Duplicate {
        events: vec![first.event, second.event],
    })
}
