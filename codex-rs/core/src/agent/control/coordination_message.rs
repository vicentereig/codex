//! Enabled-coordination message/follow-up delivery orchestration (Stage 3 contract freeze,
//! Decision 9; `codex-9u5.2.3.4`).
//!
//! This is the "small branch at the existing seam" pattern `coordination_spawn.rs` established
//! for spawn, applied to `send_message`/`followup_task`. It wraps -- rather than modifies --
//! `AgentControl::send_inter_agent_communication`: the disabled/default path
//! (`send_inter_agent_communication`, `submit_inter_agent_communication`) never calls anything in
//! this file, so its behavior (including its call to the full-payload
//! `agent_communication::emit_agent_communication_send`) is untouched by this module's existence.
//!
//! `deliver_message_coordinated` reads `AgentControl`'s crate-private `coordination` field and
//! returns an error immediately, before any controlled side effect, unless that field holds
//! `CoordinationControl::Enabled`. In this stage only test code can produce `Enabled`. There is no
//! production caller.
//!
//! Frozen enabled order (Decision 9 + the message/follow-up section of the Stage 3 preflight):
//! resolve durable live-operation key (Decision 5) -> commit the sender-side intent (resolving the
//! operation id itself *is* the durable, idempotent sender intent here -- unlike spawn, there is
//! no separate g1 reservation to make before the target-side transaction, since a message/
//! follow-up names no new thread/turn identity ahead of time) -> commit the target-side receipt
//! (and, for a follow-up, the next sequential generation + turn binding) in one atomic transaction
//! -> mark the receipt durably enqueued (the controlled queue side effect) -> emit metadata-only
//! telemetry -> acknowledge the caller. Root usability (state absent/quarantined/poisoned) is
//! checked at every boundary, not once upfront, mirroring spawn.
//!
//! Content never reaches this orchestration: it takes an already-constructed
//! `InterAgentCommunication`/`AgentCommunicationContext` purely so it can emit metadata-only
//! telemetry with the caller's kind/trigger_turn, and deliberately never reads `content` or
//! `encrypted_content` from it (see `agent_communication::emit_agent_communication_send_metadata_only`).
//! The ordinary communication payload continues to flow through the existing, untouched
//! `Op::InterAgentCommunication` path; this module owns only the durable receipt/generation/
//! materialization bookkeeping Decision 9 requires.

use super::*;
use crate::agent_communication::AgentCommunicationContext;
use crate::coordination::CoordinationControl;
use crate::coordination::MessageFailurePoint;
use crate::coordination::OperationIdentityKey;
use crate::coordination::SemanticSlot;
use codex_protocol::protocol::InterAgentCommunication;
use codex_state::CaptureReceiptOutcome;

fn coordination_gate_err(err: impl std::fmt::Display) -> CodexErr {
    CodexErr::Fatal(format!("coordination gate: {err}"))
}

/// Outcome of a coordinated message/follow-up delivery: the durable receipt identity plus
/// whatever generation/turn it ended up forever bound to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CoordinatedMessageReceipt {
    pub(crate) receipt_id: uuid::Uuid,
    pub(crate) operation_id: uuid::Uuid,
    pub(crate) captured_generation: Option<u32>,
    pub(crate) bound_turn_id: Option<String>,
}

impl AgentControl {
    /// Deliver a message (`key.semantic_slot == SemanticSlot::Message`) or follow-up
    /// (`SemanticSlot::Followup`) receipt under the enabled coordination path.
    ///
    /// `bound_turn_id_for_followup` is required (and only meaningful) for `Followup`: the caller
    /// decides whether the new generation binds a fresh turn id or the target's existing active
    /// turn id ("follow-up generations accept sequentially and may bind same active turn").
    ///
    /// A duplicate call for the identical `key` (and therefore the identical resolved operation
    /// id) never mutates the target generation counter twice: it resumes from the already-
    /// committed receipt row and re-marks it enqueued (a no-op if already enqueued).
    pub(crate) async fn deliver_message_coordinated(
        &self,
        key: OperationIdentityKey,
        sender_turn_id: String,
        target_thread_id: ThreadId,
        bound_turn_id_for_followup: Option<String>,
        communication: &InterAgentCommunication,
        context: AgentCommunicationContext,
        now_ms: i64,
    ) -> CodexResult<CoordinatedMessageReceipt> {
        let coordination = match &self.coordination {
            CoordinationControl::Disabled => {
                return Err(CodexErr::UnsupportedOperation(
                    "coordination is disabled".to_string(),
                ));
            }
            CoordinationControl::Enabled(state) => Arc::clone(state),
        };
        let root_thread_id = key.root_thread_id;
        let sender_thread_id = key.actor_thread_id;

        // Boundary: before the sender-side operation identity is resolved.
        coordination
            .ensure_root_usable(root_thread_id)
            .map_err(coordination_gate_err)?;
        coordination
            .check_message_failure_injection(MessageFailurePoint::BeforeIntent)
            .map_err(coordination_gate_err)?;

        let semantic_slot = key.semantic_slot;
        let operation_id = coordination.operation_identity.resolve(key);
        let receipt_id = uuid::Uuid::now_v7();

        // Boundary: after the (idempotent) sender intent is resolved, before the target-side
        // receipt/generation-acceptance transaction.
        coordination
            .ensure_root_usable(root_thread_id)
            .map_err(coordination_gate_err)?;
        coordination
            .check_message_failure_injection(MessageFailurePoint::AfterIntent)
            .map_err(coordination_gate_err)?;

        let outcome = match semantic_slot {
            SemanticSlot::Message => coordination
                .capture_queue_message_receipt(
                    root_thread_id,
                    codex_state::CaptureQueueMessageReceipt {
                        receipt_id,
                        operation_id: operation_id_uuid(operation_id),
                        sender_thread_id,
                        sender_turn_id,
                        target_thread_id,
                        now_ms,
                    },
                )
                .await
                .map_err(coordination_gate_err)?,
            SemanticSlot::Followup => {
                let bound_turn_id = bound_turn_id_for_followup.ok_or_else(|| {
                    CodexErr::Fatal(
                        "coordination gate: follow-up delivery requires bound_turn_id_for_followup"
                            .to_string(),
                    )
                })?;
                coordination
                    .accept_followup_generation(
                        root_thread_id,
                        codex_state::AcceptFollowupGeneration {
                            receipt_id,
                            operation_id: operation_id_uuid(operation_id),
                            sender_thread_id,
                            sender_turn_id,
                            target_thread_id,
                            bound_turn_id,
                            now_ms,
                        },
                    )
                    .await
                    .map_err(coordination_gate_err)?
            }
            SemanticSlot::Spawn => {
                return Err(CodexErr::Fatal(
                    "coordination gate: spawn semantic slot is not valid for message delivery"
                        .to_string(),
                ));
            }
        };
        let receipt = match outcome {
            CaptureReceiptOutcome::Captured(receipt) => receipt,
            CaptureReceiptOutcome::Duplicate(receipt) => receipt,
        };

        // Boundary: after the receipt (and, for follow-up, the generation) is durably committed,
        // before marking it enqueued.
        coordination
            .ensure_root_usable(root_thread_id)
            .map_err(coordination_gate_err)?;
        coordination
            .check_message_failure_injection(MessageFailurePoint::AfterReceipt)
            .map_err(coordination_gate_err)?;
        coordination
            .check_message_failure_injection(MessageFailurePoint::BeforeEnqueue)
            .map_err(coordination_gate_err)?;

        coordination
            .mark_receipt_enqueued(receipt.receipt_id, now_ms)
            .await
            .map_err(coordination_gate_err)?;

        // Boundary: after the controlled side effect (receipt marked enqueued), before
        // acknowledging the caller.
        coordination
            .ensure_root_usable(root_thread_id)
            .map_err(coordination_gate_err)?;
        coordination
            .check_message_failure_injection(MessageFailurePoint::AfterEnqueueBeforeAck)
            .map_err(coordination_gate_err)?;

        // Never `agent_communication::emit_agent_communication_send`: that logs the full
        // content/encrypted_content payload. The enabled path routes through the metadata-only
        // sibling instead (Decision 9).
        crate::agent_communication::emit_agent_communication_send_metadata_only(
            &receipt.receipt_id.to_string(),
            &receipt.operation_id.to_string(),
            &context,
            communication,
            target_thread_id,
            receipt.captured_generation,
        );

        Ok(CoordinatedMessageReceipt {
            receipt_id: receipt.receipt_id,
            operation_id: receipt.operation_id,
            captured_generation: receipt.captured_generation,
            bound_turn_id: receipt.bound_turn_id,
        })
    }
}

/// `CoordinationOperationId` has no public accessor for its inner `Uuid` outside `#[cfg(test)]`
/// (see `operation_identity.rs::CoordinationOperationId::as_uuid`), and the durable state layer
/// needs a real `Uuid` to key `coordination_message_receipts.operation_id`. Production code (this
/// module) parses the id's stable `Display` string representation instead of depending on the
/// test-only accessor -- the id's `Display` impl is exactly its UUID string, so this is lossless.
fn operation_id_uuid(operation_id: impl std::fmt::Display) -> uuid::Uuid {
    uuid::Uuid::parse_str(&operation_id.to_string())
        .expect("CoordinationOperationId always displays as a UUID")
}

#[cfg(test)]
#[path = "coordination_message_tests.rs"]
mod tests;
