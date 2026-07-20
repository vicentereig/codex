use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;

use crate::AssignmentGeneration;
use crate::AssignmentId;
use crate::BoundedId;
use crate::BoundedList;
use crate::CompatibilityAdapterVersion;
use crate::CompatibilityOrdinal;
use crate::ContentEvidence;
use crate::CoordinationAgentPath;
use crate::CoordinationEventId;
use crate::CoordinationOperationId;
use crate::CoordinationRevision;
use crate::CoordinationSchemaVersion;
use crate::EncodedPayloadBytes;
use crate::Evidence;
use crate::HandoffId;
use crate::MAX_ID_BYTES;
use crate::ReceiptId;
use crate::RequestedRuntime;
use crate::ResultId;
use crate::SanitizerVersion;
use crate::SourceShape;
use crate::StateEpoch;
use crate::UnavailableReason;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum AssignmentEvidence {
    Known {
        assignment_id: AssignmentId,
        generation: AssignmentGeneration,
    },
    Unavailable {
        reason: UnavailableReason,
    },
    NotApplicable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationPrincipal {
    pub thread_id: ThreadId,
    pub turn_id: Evidence<BoundedId<MAX_ID_BYTES>>,
    pub agent_path: Evidence<CoordinationAgentPath>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinationTarget {
    pub principal: CoordinationPrincipal,
    pub assignment: AssignmentEvidence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ObservedState {
    Active,
    Idle,
    Completed,
    Failed,
    Interrupted,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WaitTarget {
    pub target: CoordinationTarget,
    pub observed_state: Evidence<ObservedState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CoordinationFailureCode {
    Unauthorized,
    StateUnavailable,
    StateQuarantined,
    InvalidPayload,
    PayloadOverLimit,
    TargetUnavailable,
    GenerationFenced,
    TerminalConflict,
    OwnershipConflict,
    IdempotencyConflict,
    RetryExhausted,
    CorruptEvidence,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceKey {
    pub shape: SourceShape,
    pub source_item_id: Evidence<BoundedId<MAX_ID_BYTES>>,
    pub source_ordinal: CompatibilityOrdinal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "source",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CoordinationSource {
    Native {
        schema_version: CoordinationSchemaVersion,
        sanitizer_version: SanitizerVersion,
        suppression_keys: BoundedList<SourceKey, 4>,
    },
    Compatibility {
        adapter_version: CompatibilityAdapterVersion,
        sanitizer_version: SanitizerVersion,
        key: SourceKey,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "mode",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CoordinationOrder {
    Native {
        state_epoch: StateEpoch,
        revision: CoordinationRevision,
    },
    Compatibility {
        after_revision: CompatibilityOrdinal,
        source_ordinal: CompatibilityOrdinal,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionAttribution {
    pub thread_id: ThreadId,
    pub turn_id: BoundedId<MAX_ID_BYTES>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AssignmentMode {
    Spawn,
    Followup,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "reason",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum GenerationCloseReason {
    Superseded { by_generation: AssignmentGeneration },
    TurnCompleted { turn_id: BoundedId<MAX_ID_BYTES> },
    TurnInterrupted { turn_id: BoundedId<MAX_ID_BYTES> },
    DeliveryFailed { code: CoordinationFailureCode },
    AbandonedBeforeAcceptance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "reason",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum InterruptionReason {
    Requested {
        operation_id: CoordinationOperationId,
    },
    UserInput,
    Shutdown,
    ExecutorLost,
    LegacyUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TurnOutcome {
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OwnershipChangeMode {
    ExplicitTransfer,
    LaterTurnRebind,
    FollowupClaim,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WaitOutcome {
    TargetTerminal,
    MailboxActivity,
    TimedOut,
    InterruptedByInput,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LegacyObservation {
    SpawnToolReported,
    MessageToolReported,
    ResumeToolReported,
    CloseToolReported,
    AgentStartedMarker,
    InteractionMarker,
    InterruptedMarker,
    CommunicationPersisted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CoordinationEventKind {
    AssignmentRequested {
        operation_id: CoordinationOperationId,
        mode: AssignmentMode,
        target: CoordinationTarget,
        objective: ContentEvidence,
        encoded_payload_bytes: EncodedPayloadBytes,
        requested_runtime: RequestedRuntime,
    },
    AssignmentAccepted {
        operation_id: CoordinationOperationId,
        mode: AssignmentMode,
        target: CoordinationTarget,
        receipt_id: ReceiptId,
        bound_turn_id: Evidence<BoundedId<MAX_ID_BYTES>>,
    },
    AssignmentGenerationClosed {
        assignment: AssignmentEvidence,
        close_reason: GenerationCloseReason,
    },
    MessageSubmissionRecorded {
        operation_id: CoordinationOperationId,
        target: CoordinationTarget,
        content: ContentEvidence,
        encoded_payload_bytes: EncodedPayloadBytes,
    },
    MessageDurablyReceived {
        operation_id: CoordinationOperationId,
        target: CoordinationTarget,
        receipt_id: ReceiptId,
    },
    MessageIncludedInModelInput {
        operation_id: CoordinationOperationId,
        target: CoordinationTarget,
        receipt_id: ReceiptId,
        inference_attempt_id: BoundedId<MAX_ID_BYTES>,
    },
    WaitStarted {
        operation_id: CoordinationOperationId,
        targets: BoundedList<WaitTarget, 8>,
        timeout_ms: u32,
    },
    WaitEnded {
        operation_id: CoordinationOperationId,
        targets: BoundedList<WaitTarget, 8>,
        outcome: Evidence<WaitOutcome>,
        failure: Evidence<CoordinationFailureCode>,
    },
    InterruptRequested {
        operation_id: CoordinationOperationId,
        target: CoordinationTarget,
    },
    InterruptDurablyReceived {
        operation_id: CoordinationOperationId,
        target: CoordinationTarget,
        receipt_id: ReceiptId,
    },
    TurnInterrupted {
        target: CoordinationTarget,
        target_turn_id: BoundedId<MAX_ID_BYTES>,
        interruption_reason: InterruptionReason,
        included_generations: BoundedList<AssignmentGeneration, 4>,
    },
    Detached {
        target: CoordinationTarget,
        previous_owner: Evidence<CoordinationPrincipal>,
    },
    DependencyDeclared {
        operation_id: CoordinationOperationId,
        dependent: CoordinationTarget,
        prerequisite: CoordinationTarget,
    },
    OwnershipChanged {
        operation_id: CoordinationOperationId,
        target: CoordinationTarget,
        previous_owner: Evidence<CoordinationPrincipal>,
        new_owner: Evidence<CoordinationPrincipal>,
        change_mode: OwnershipChangeMode,
    },
    TurnCompleted {
        target: CoordinationTarget,
        target_turn_id: BoundedId<MAX_ID_BYTES>,
        outcome: TurnOutcome,
        included_generations: BoundedList<AssignmentGeneration, 4>,
    },
    TerminalResultObserved {
        result_id: ResultId,
        target: CoordinationTarget,
        target_turn_id: BoundedId<MAX_ID_BYTES>,
        summary: ContentEvidence,
    },
    HandoffDeliveryAttempted {
        handoff_id: HandoffId,
        result_id: ResultId,
        attempt: AssignmentGeneration,
        from: CoordinationTarget,
        to: CoordinationTarget,
    },
    HandoffDurablyReceived {
        handoff_id: HandoffId,
        result_id: ResultId,
        attempt: AssignmentGeneration,
        receipt_id: ReceiptId,
        from: CoordinationTarget,
        to: CoordinationTarget,
    },
    HandoffIncludedInModelInput {
        handoff_id: HandoffId,
        result_id: ResultId,
        attempt: AssignmentGeneration,
        receipt_id: ReceiptId,
        to: CoordinationTarget,
        inference_attempt_id: BoundedId<MAX_ID_BYTES>,
    },
    HandoffDeliveryFailed {
        handoff_id: HandoffId,
        result_id: ResultId,
        attempt: AssignmentGeneration,
        from: CoordinationTarget,
        to: CoordinationTarget,
        code: CoordinationFailureCode,
        summary: ContentEvidence,
        retryable: bool,
    },
    LegacyInteractionObserved {
        observation: LegacyObservation,
        target: Evidence<CoordinationTarget>,
        content: ContentEvidence,
        reported_success: Evidence<bool>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationEventEnvelope {
    pub event_id: CoordinationEventId,
    pub root_thread_id: ThreadId,
    pub order: CoordinationOrder,
    pub occurred_at: i64,
    pub actor: CoordinationPrincipal,
    pub responsibility_owner: Evidence<CoordinationPrincipal>,
    pub projection: ProjectionAttribution,
    pub causes: BoundedList<CoordinationEventId, 4>,
    pub source: CoordinationSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EventData {
    #[serde(flatten)]
    pub(crate) envelope: CoordinationEventEnvelope,
    #[serde(flatten)]
    pub(crate) kind: CoordinationEventKind,
}
