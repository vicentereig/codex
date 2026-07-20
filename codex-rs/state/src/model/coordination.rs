use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::AssignmentMode;
use codex_coordination::BoundedId;
use codex_coordination::BoundedList;
use codex_coordination::ContentEvidence;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationFailureCode;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationPrincipal;
use codex_coordination::CoordinationSource;
use codex_coordination::CoordinationTarget;
use codex_coordination::EncodedPayloadBytes;
use codex_coordination::Evidence;
use codex_coordination::GenerationCloseReason;
use codex_coordination::InterruptionReason;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::ReceiptId;
use codex_coordination::RequestedRuntime;
use codex_coordination::TurnOutcome;
use codex_coordination::WaitOutcome;
use codex_coordination::WaitTarget;
use codex_protocol::ThreadId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeEventIdentity {
    pub event_id: CoordinationEventId,
    pub operation_id: CoordinationOperationId,
}

/// Native envelope facts supplied by the integration boundary. Revision, causes,
/// semantic kind, assignment generation, and lifecycle are owned by durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeEventContext {
    pub root_thread_id: ThreadId,
    pub expected_root_revision: u64,
    pub occurred_at: i64,
    pub actor: CoordinationPrincipal,
    pub responsibility_owner: Evidence<CoordinationPrincipal>,
    pub source: CoordinationSource,
    pub primary: NativeEventIdentity,
    pub secondary: BoundedList<NativeEventIdentity, 4>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AssignmentReservation {
    Spawn,
    Followup {
        expected_owner_thread_id: ThreadId,
        expected_owner_turn_id: BoundedId<MAX_ID_BYTES>,
        expected_head_version: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReserveAssignment {
    pub context: NativeEventContext,
    pub assignment_id: AssignmentId,
    pub child_thread_id: ThreadId,
    pub reservation: AssignmentReservation,
    pub operation_id: CoordinationOperationId,
    pub target_principal: CoordinationPrincipal,
    pub objective: ContentEvidence,
    pub encoded_payload_bytes: EncodedPayloadBytes,
    pub requested_runtime: RequestedRuntime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AcceptAssignment {
    pub context: NativeEventContext,
    pub assignment_id: AssignmentId,
    pub generation: AssignmentGeneration,
    pub receipt_id: ReceiptId,
    pub bound_turn_id: Evidence<BoundedId<MAX_ID_BYTES>>,
    pub expected_owner_thread_id: ThreadId,
    pub expected_owner_turn_id: BoundedId<MAX_ID_BYTES>,
    pub expected_head_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CloseReservedAssignment {
    pub context: NativeEventContext,
    pub assignment_id: AssignmentId,
    pub generation: AssignmentGeneration,
    pub reason: GenerationCloseReason,
    pub expected_owner_thread_id: ThreadId,
    pub expected_owner_turn_id: BoundedId<MAX_ID_BYTES>,
    pub expected_head_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TerminalTurn {
    Completed {
        target: CoordinationTarget,
        target_turn_id: BoundedId<MAX_ID_BYTES>,
        outcome: TurnOutcome,
        included_generations: BoundedList<AssignmentGeneration, 4>,
    },
    Interrupted {
        target: CoordinationTarget,
        target_turn_id: BoundedId<MAX_ID_BYTES>,
        interruption_reason: InterruptionReason,
        included_generations: BoundedList<AssignmentGeneration, 4>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Trusted state-internal terminal facts. Callers that consume external model
/// input must first durably prove every included generation; a turn binding by
/// itself is not that proof.
pub(crate) struct TerminalAssignment {
    pub context: NativeEventContext,
    pub terminal: TerminalTurn,
    pub expected_owner_thread_id: ThreadId,
    pub expected_owner_turn_id: BoundedId<MAX_ID_BYTES>,
    pub expected_head_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StartCoordinationWait {
    pub context: NativeEventContext,
    pub operation_id: CoordinationOperationId,
    pub targets: BoundedList<WaitTarget, 8>,
    pub timeout_ms: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EndCoordinationWait {
    pub context: NativeEventContext,
    pub operation_id: CoordinationOperationId,
    pub targets: BoundedList<WaitTarget, 8>,
    pub outcome: Evidence<WaitOutcome>,
    pub failure: Evidence<CoordinationFailureCode>,
    pub expected_wait_version: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GenerationLifecycle {
    Reserved,
    Accepted,
    Abandoned,
    Superseded,
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AssignmentHeadRecord {
    pub assignment_id: AssignmentId,
    pub root_thread_id: ThreadId,
    pub child_thread_id: ThreadId,
    pub accepted_generation: Option<AssignmentGeneration>,
    pub next_generation: AssignmentGeneration,
    pub owner_thread_id: ThreadId,
    pub owner_turn_id: BoundedId<MAX_ID_BYTES>,
    pub version: u64,
    pub last_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AssignmentGenerationRecord {
    pub assignment_id: AssignmentId,
    pub generation: AssignmentGeneration,
    pub mode: AssignmentMode,
    pub lifecycle: GenerationLifecycle,
    pub request_event_id: CoordinationEventId,
    pub accepted_event_id: Option<CoordinationEventId>,
    pub superseded_event_id: Option<CoordinationEventId>,
    pub terminal_event_id: Option<CoordinationEventId>,
    pub close_event_id: Option<CoordinationEventId>,
    pub accepted_receipt_id: Option<ReceiptId>,
    pub terminal_reason: Option<GenerationCloseReason>,
    pub last_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AssignmentAggregateRecord {
    pub head: AssignmentHeadRecord,
    pub generations: Vec<AssignmentGenerationRecord>,
}
