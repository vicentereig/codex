use codex_coordination::AssignmentEvidence;
use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentMode;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationTarget;

use crate::model::coordination::AssignmentReservation;
use crate::model::coordination_commands::CoordinationCommandIntent;

pub(super) fn expected_event_kind(
    intent: &CoordinationCommandIntent,
    generation: AssignmentGeneration,
) -> CoordinationEventKind {
    match intent {
        CoordinationCommandIntent::Assignment { reservation } => {
            let mode = match &reservation.reservation {
                AssignmentReservation::Spawn => AssignmentMode::Spawn,
                AssignmentReservation::Followup { .. } => AssignmentMode::Followup,
            };
            CoordinationEventKind::AssignmentRequested {
                operation_id: reservation.operation_id,
                mode,
                target: CoordinationTarget {
                    principal: reservation.target_principal.clone(),
                    assignment: AssignmentEvidence::Known {
                        assignment_id: reservation.assignment_id,
                        generation,
                    },
                },
                objective: reservation.objective.clone(),
                encoded_payload_bytes: reservation.encoded_payload_bytes,
                requested_runtime: reservation.requested_runtime.clone(),
            }
        }
        CoordinationCommandIntent::Message {
            operation_id,
            target,
            content,
            encoded_payload_bytes,
            ..
        } => CoordinationEventKind::MessageSubmissionRecorded {
            operation_id: *operation_id,
            target: target.clone(),
            content: content.clone(),
            encoded_payload_bytes: *encoded_payload_bytes,
        },
        CoordinationCommandIntent::Interrupt {
            operation_id,
            target,
            ..
        } => CoordinationEventKind::InterruptRequested {
            operation_id: *operation_id,
            target: target.clone(),
        },
    }
}
