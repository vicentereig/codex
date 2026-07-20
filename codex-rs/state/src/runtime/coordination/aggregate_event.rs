use codex_coordination::BoundedList;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationEventEnvelope;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationOrder;
use codex_coordination::CoordinationRevision;
use codex_coordination::ProjectionAttribution;
use codex_coordination::StateEpoch;

use super::aggregate_journal::CoordinationWriteError;
use super::aggregate_journal::internal;
use super::aggregate_journal::known_turn;
use crate::model::coordination::NativeEventContext;
use crate::model::coordination::NativeEventIdentity;

pub(super) fn make_event(
    context: &NativeEventContext,
    identity: &NativeEventIdentity,
    epoch: StateEpoch,
    revision: CoordinationRevision,
    kind: CoordinationEventKind,
    causes: &[&CoordinationEvent],
) -> Result<CoordinationEvent, CoordinationWriteError> {
    let cause_ids = BoundedList::new(
        causes
            .iter()
            .map(|event| event.envelope().event_id)
            .collect(),
        /*omitted_count*/ 0,
    )
    .map_err(internal)?;
    let projection = match &kind {
        CoordinationEventKind::TurnCompleted {
            target,
            target_turn_id,
            ..
        }
        | CoordinationEventKind::TurnInterrupted {
            target,
            target_turn_id,
            ..
        }
        | CoordinationEventKind::TerminalResultObserved {
            target,
            target_turn_id,
            ..
        } => ProjectionAttribution {
            thread_id: target.principal.thread_id,
            turn_id: target_turn_id.clone(),
        },
        _ => ProjectionAttribution {
            thread_id: context.actor.thread_id,
            turn_id: known_turn(&context.actor)?.clone(),
        },
    };
    let event = CoordinationEvent::try_new(
        CoordinationEventEnvelope {
            event_id: identity.event_id,
            root_thread_id: context.root_thread_id,
            order: CoordinationOrder::Native {
                state_epoch: epoch,
                revision,
            },
            occurred_at: context.occurred_at,
            actor: context.actor.clone(),
            responsibility_owner: context.responsibility_owner.clone(),
            projection,
            causes: cause_ids,
            source: context.source.clone(),
        },
        kind,
    )
    .map_err(internal)?;
    event.validate_resolved_causes(causes).map_err(internal)?;
    Ok(event)
}

pub(super) fn compare_event(
    context: &NativeEventContext,
    identity: &NativeEventIdentity,
    epoch: StateEpoch,
    revision: CoordinationRevision,
    kind: CoordinationEventKind,
    causes: &[&CoordinationEvent],
    stored: &CoordinationEvent,
) -> Result<(), CoordinationWriteError> {
    let expected = make_event(context, identity, epoch, revision, kind, causes)?;
    if expected.envelope().event_id != stored.envelope().event_id
        || expected.canonical_bytes() != stored.canonical_bytes()
    {
        return Err(CoordinationWriteError::DivergentIntent);
    }
    Ok(())
}
