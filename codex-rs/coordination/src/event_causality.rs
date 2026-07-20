use crate::AssignmentEvidence;
use crate::CoordinationError;
use crate::CoordinationEvent;
use crate::CoordinationEventKind;
use crate::CoordinationOrder;
use crate::GenerationCloseReason;
use crate::InterruptionReason;

/// Validates the causal predecessors named by a checked coordination event.
///
/// Callers must supply exactly the events named by `event.envelope().causes` in
/// the same order. This keeps lookup and retention policy outside the pure
/// validator while preventing a caller from validating against an unrelated
/// predecessor.
impl CoordinationEvent {
    pub fn validate_resolved_causes(
        &self,
        causes: &[&CoordinationEvent],
    ) -> Result<(), CoordinationError> {
        validate_causal_history(self, causes)
    }
}

fn validate_causal_history(
    event: &CoordinationEvent,
    causes: &[&CoordinationEvent],
) -> Result<(), CoordinationError> {
    let expected = event.envelope().causes.items();
    if expected.len() != causes.len()
        || !expected
            .iter()
            .zip(causes)
            .all(|(event_id, cause)| event_id == &cause.envelope().event_id)
    {
        return invariant("causal history must resolve every named cause in order");
    }
    if matches!(
        event.envelope().order,
        CoordinationOrder::Compatibility { .. }
    ) {
        return if causes.is_empty() {
            Ok(())
        } else {
            invariant("compatibility events cannot resolve native causes")
        };
    }
    let (state_epoch, revision) = native_order(event)?;
    for cause in causes {
        if cause.envelope().root_thread_id != event.envelope().root_thread_id {
            return invariant("causes must belong to the same root stream");
        }
        let (cause_epoch, cause_revision) = native_order(cause)?;
        if cause_epoch != state_epoch {
            return invariant("causes must belong to the same state epoch");
        }
        if cause_revision >= revision {
            return invariant("causes must have lower native revisions");
        }
    }
    validate_relationship(event.kind(), causes)
}

fn validate_relationship(
    kind: &CoordinationEventKind,
    causes: &[&CoordinationEvent],
) -> Result<(), CoordinationError> {
    let Some(cause) = causes.first() else {
        return Ok(());
    };
    match (kind, cause.kind()) {
        (
            CoordinationEventKind::AssignmentAccepted {
                operation_id,
                mode,
                target,
                ..
            },
            CoordinationEventKind::AssignmentRequested {
                operation_id: requested_operation,
                mode: requested_mode,
                target: requested_target,
                ..
            },
        ) if (operation_id, mode, target)
            == (requested_operation, requested_mode, requested_target) => {}
        (
            CoordinationEventKind::AssignmentGenerationClosed {
                assignment,
                close_reason: GenerationCloseReason::Superseded { by_generation },
            },
            CoordinationEventKind::AssignmentRequested {
                mode: crate::AssignmentMode::Followup,
                target,
                ..
            }
            | CoordinationEventKind::AssignmentAccepted {
                mode: crate::AssignmentMode::Followup,
                target,
                ..
            },
        ) if supersedes(assignment, &target.assignment, *by_generation) => {}
        (
            CoordinationEventKind::AssignmentGenerationClosed {
                assignment,
                close_reason: GenerationCloseReason::TurnCompleted { turn_id },
            },
            CoordinationEventKind::TurnCompleted {
                target,
                target_turn_id,
                included_generations,
                ..
            },
        ) if turn_id == target_turn_id
            && terminal_covers(assignment, &target.assignment, included_generations.items()) => {}
        (
            CoordinationEventKind::AssignmentGenerationClosed {
                assignment,
                close_reason: GenerationCloseReason::TurnInterrupted { turn_id },
            },
            CoordinationEventKind::TurnInterrupted {
                target,
                target_turn_id,
                included_generations,
                ..
            },
        ) if turn_id == target_turn_id
            && terminal_covers(assignment, &target.assignment, included_generations.items()) => {}
        (
            CoordinationEventKind::AssignmentGenerationClosed {
                assignment,
                close_reason:
                    GenerationCloseReason::DeliveryFailed { .. }
                    | GenerationCloseReason::AbandonedBeforeAcceptance,
            },
            CoordinationEventKind::AssignmentRequested { target, .. },
        ) if assignment == &target.assignment => {}
        (
            CoordinationEventKind::MessageDurablyReceived {
                operation_id,
                target,
                ..
            },
            CoordinationEventKind::MessageSubmissionRecorded {
                operation_id: submitted_operation,
                target: submitted_target,
                ..
            },
        ) if (operation_id, target) == (submitted_operation, submitted_target) => {}
        (
            CoordinationEventKind::MessageIncludedInModelInput {
                operation_id,
                target,
                receipt_id,
                ..
            },
            CoordinationEventKind::MessageDurablyReceived {
                operation_id: received_operation,
                target: received_target,
                receipt_id: received_receipt,
            },
        ) if (operation_id, target, receipt_id)
            == (received_operation, received_target, received_receipt) => {}
        (
            CoordinationEventKind::WaitEnded {
                operation_id,
                targets,
                ..
            },
            CoordinationEventKind::WaitStarted {
                operation_id: started_operation,
                targets: started_targets,
                ..
            },
        ) if operation_id == started_operation
            && targets.items().len() == started_targets.items().len()
            && targets
                .items()
                .iter()
                .zip(started_targets.items())
                .all(|(ended, started)| ended.target == started.target) => {}
        (
            CoordinationEventKind::InterruptDurablyReceived {
                operation_id,
                target,
                ..
            },
            CoordinationEventKind::InterruptRequested {
                operation_id: requested_operation,
                target: requested_target,
            },
        ) if (operation_id, target) == (requested_operation, requested_target) => {}
        (
            CoordinationEventKind::TurnInterrupted {
                target,
                interruption_reason: InterruptionReason::Requested { operation_id },
                ..
            },
            CoordinationEventKind::InterruptDurablyReceived {
                operation_id: received_operation,
                target: received_target,
                ..
            },
        ) if (operation_id, target) == (received_operation, received_target) => {}
        (
            CoordinationEventKind::TerminalResultObserved {
                target,
                target_turn_id,
                ..
            },
            CoordinationEventKind::TurnCompleted {
                target: terminal_target,
                target_turn_id: terminal_turn,
                ..
            }
            | CoordinationEventKind::TurnInterrupted {
                target: terminal_target,
                target_turn_id: terminal_turn,
                ..
            },
        ) if (target, target_turn_id) == (terminal_target, terminal_turn) => {}
        (
            CoordinationEventKind::HandoffDeliveryAttempted {
                result_id, from, ..
            },
            CoordinationEventKind::TerminalResultObserved {
                result_id: observed_result,
                target,
                ..
            },
        ) if result_id == observed_result && from == target => {}
        (
            CoordinationEventKind::HandoffDurablyReceived {
                handoff_id,
                result_id,
                attempt,
                from,
                to,
                ..
            }
            | CoordinationEventKind::HandoffDeliveryFailed {
                handoff_id,
                result_id,
                attempt,
                from,
                to,
                ..
            },
            CoordinationEventKind::HandoffDeliveryAttempted {
                handoff_id: attempted_handoff,
                result_id: attempted_result,
                attempt: attempted_generation,
                from: attempted_from,
                to: attempted_to,
            },
        ) if (handoff_id, result_id, attempt, from, to)
            == (
                attempted_handoff,
                attempted_result,
                attempted_generation,
                attempted_from,
                attempted_to,
            ) => {}
        (
            CoordinationEventKind::HandoffIncludedInModelInput {
                handoff_id,
                result_id,
                attempt,
                receipt_id,
                to,
                ..
            },
            CoordinationEventKind::HandoffDurablyReceived {
                handoff_id: received_handoff,
                result_id: received_result,
                attempt: received_attempt,
                receipt_id: received_receipt,
                to: received_target,
                ..
            },
        ) if (handoff_id, result_id, attempt, receipt_id, to)
            == (
                received_handoff,
                received_result,
                received_attempt,
                received_receipt,
                received_target,
            ) => {}
        (
            CoordinationEventKind::TurnInterrupted {
                interruption_reason:
                    InterruptionReason::UserInput
                    | InterruptionReason::Shutdown
                    | InterruptionReason::ExecutorLost
                    | InterruptionReason::LegacyUnavailable,
                ..
            },
            _,
        ) => {}
        _ => return invariant("cause kind or correlated fields do not match consequence"),
    }
    Ok(())
}

fn native_order(event: &CoordinationEvent) -> Result<(crate::StateEpoch, u64), CoordinationError> {
    match event.envelope().order {
        CoordinationOrder::Native {
            state_epoch,
            revision,
        } => Ok((state_epoch, revision.get())),
        CoordinationOrder::Compatibility { .. } => {
            invariant("native events cannot cite compatibility causes")
        }
    }
}

fn supersedes(
    closed: &AssignmentEvidence,
    later: &AssignmentEvidence,
    by_generation: crate::AssignmentGeneration,
) -> bool {
    matches!(
        (closed, later),
        (
            AssignmentEvidence::Known {
                assignment_id: closed_id,
                generation: closed_generation,
            },
            AssignmentEvidence::Known {
                assignment_id: later_id,
                generation: later_generation,
            }
        ) if closed_id == later_id
            && later_generation == &by_generation
            && later_generation.get() > closed_generation.get()
    )
}

fn terminal_covers(
    closed: &AssignmentEvidence,
    terminal: &AssignmentEvidence,
    included: &[crate::AssignmentGeneration],
) -> bool {
    let AssignmentEvidence::Known {
        assignment_id,
        generation,
    } = closed
    else {
        return false;
    };
    included.contains(generation)
        && match terminal {
            AssignmentEvidence::Known {
                assignment_id: terminal_id,
                ..
            } => terminal_id == assignment_id,
            AssignmentEvidence::Unavailable { .. } => true,
            AssignmentEvidence::NotApplicable => false,
        }
}

fn invariant<T>(message: &'static str) -> Result<T, CoordinationError> {
    Err(CoordinationError::Invariant(message))
}
