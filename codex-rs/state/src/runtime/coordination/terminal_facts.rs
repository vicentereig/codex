use codex_coordination::AssignmentEvidence;
use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationPrincipal;
use codex_coordination::GenerationCloseReason;
use codex_coordination::InterruptionReason;
use codex_coordination::MAX_ID_BYTES;
use codex_protocol::ThreadId;

use super::aggregate_journal::CoordinationWriteError;
use crate::model::coordination::TerminalAssignment;
use crate::model::coordination::TerminalTurn;

pub(super) type TerminalFields = (
    CoordinationEventKind,
    AssignmentId,
    AssignmentGeneration,
    CoordinationPrincipal,
    ThreadId,
    BoundedId<MAX_ID_BYTES>,
    Vec<AssignmentGeneration>,
    GenerationCloseReason,
    &'static str,
);

pub(super) fn terminal_fields(
    params: &TerminalAssignment,
) -> Result<TerminalFields, CoordinationWriteError> {
    match &params.terminal {
        TerminalTurn::Completed {
            target,
            target_turn_id,
            outcome,
            included_generations,
        } => Ok((
            CoordinationEventKind::TurnCompleted {
                target: target.clone(),
                target_turn_id: target_turn_id.clone(),
                outcome: *outcome,
                included_generations: included_generations.clone(),
            },
            known_assignment(&target.assignment)?.0,
            known_assignment(&target.assignment)?.1,
            target.principal.clone(),
            target.principal.thread_id,
            target_turn_id.clone(),
            included_generations.items().to_vec(),
            GenerationCloseReason::TurnCompleted {
                turn_id: target_turn_id.clone(),
            },
            "completed",
        )),
        TerminalTurn::Interrupted {
            target,
            target_turn_id,
            interruption_reason,
            included_generations,
        } => {
            // Requested interruptions require an InterruptDurablyReceived cause. That receipt
            // transition is introduced by the later command-intent staging boundary; accepting
            // it here would fabricate a causally invalid zero-cause terminal event.
            if matches!(interruption_reason, InterruptionReason::Requested { .. }) {
                return Err(CoordinationWriteError::AssignmentConflict);
            }
            Ok((
                CoordinationEventKind::TurnInterrupted {
                    target: target.clone(),
                    target_turn_id: target_turn_id.clone(),
                    interruption_reason: interruption_reason.clone(),
                    included_generations: included_generations.clone(),
                },
                known_assignment(&target.assignment)?.0,
                known_assignment(&target.assignment)?.1,
                target.principal.clone(),
                target.principal.thread_id,
                target_turn_id.clone(),
                included_generations.items().to_vec(),
                GenerationCloseReason::TurnInterrupted {
                    turn_id: target_turn_id.clone(),
                },
                "interrupted",
            ))
        }
    }
}

fn known_assignment(
    evidence: &AssignmentEvidence,
) -> Result<(AssignmentId, AssignmentGeneration), CoordinationWriteError> {
    match evidence {
        AssignmentEvidence::Known {
            assignment_id,
            generation,
        } => Ok((*assignment_id, *generation)),
        AssignmentEvidence::Unavailable { .. } | AssignmentEvidence::NotApplicable => {
            Err(CoordinationWriteError::AssignmentConflict)
        }
    }
}
