use std::fmt;

use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationFailureCode;
use codex_coordination::CoordinationOperationId;
use codex_coordination::MAX_CIPHERTEXT_BYTES;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::ReceiptId;
use codex_protocol::ThreadId;

use super::coordination::NativeEventContext;
use super::coordination_commands::CommandKind;

pub(crate) const MAX_INBOX_MAINTENANCE_BATCH: u32 = 256;

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct InboxCiphertext(Vec<u8>);

impl InboxCiphertext {
    pub(crate) fn from_stored(bytes: Vec<u8>) -> Result<Self, InboxInputError> {
        if bytes.len() > MAX_CIPHERTEXT_BYTES as usize {
            return Err(InboxInputError::PayloadOverLimit);
        }
        Ok(Self(bytes))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for InboxCiphertext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboxCiphertext")
            .field("encoded_bytes", &self.0.len())
            .field("contents", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub(crate) enum InboxInputError {
    #[error("encoded inbox payload exceeds 65536 bytes")]
    PayloadOverLimit,
    #[error("lease deadline must be after now")]
    InvalidLeaseDeadline,
    #[error("maintenance batch limit must be between one and 256")]
    InvalidBatchLimit,
    #[error("retry deadline must not precede the transport outcome")]
    InvalidRetryDeadline,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReceiptTargetFence {
    pub expected_owner_thread_id: ThreadId,
    pub expected_owner_turn_id: BoundedId<MAX_ID_BYTES>,
    pub expected_head_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistRecipientReceipt {
    pub context: NativeEventContext,
    pub receipt_id: ReceiptId,
    pub command_operation_id: CoordinationOperationId,
    pub target: ReceiptTargetFence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboxLifecycle {
    Received,
    Leased,
    Selected,
    Processed,
    Poisoned,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InboxReceiptMetadata {
    pub receipt_id: ReceiptId,
    pub command_operation_id: CoordinationOperationId,
    pub root_thread_id: ThreadId,
    pub intent_event_id: CoordinationEventId,
    pub receipt_event_id: CoordinationEventId,
    pub sender_thread_id: ThreadId,
    pub sender_turn_id: BoundedId<MAX_ID_BYTES>,
    pub recipient_thread_id: ThreadId,
    pub recipient_turn_id: BoundedId<MAX_ID_BYTES>,
    pub kind: CommandKind,
    pub target_assignment_id: AssignmentId,
    pub target_generation: AssignmentGeneration,
    pub lifecycle: InboxLifecycle,
    pub version: u64,
    pub claim_count: u64,
    pub retry_count: u64,
    pub lease_epoch: u64,
    pub retry_after_ms: i64,
    pub expires_at_ms: i64,
    pub encoded_payload_bytes: u32,
    pub delivery_fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PersistRecipientReceiptOutcome {
    Applied(InboxReceiptMetadata),
    Duplicate(InboxReceiptMetadata),
    Deferred,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedReceiptAck {
    pub receipt_id: ReceiptId,
    pub command_operation_id: CoordinationOperationId,
    pub receipt_event_id: CoordinationEventId,
    pub delivery_fingerprint: [u8; 32],
    pub encoded_payload_bytes: u32,
    pub durable_received_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClaimInboxReceipt {
    pub receipt_id: ReceiptId,
    pub claim_operation_id: CoordinationOperationId,
    pub expected_version: u64,
    pub expected_lease_epoch: u64,
    pub now_ms: i64,
    pub lease_expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InboxLeaseToken {
    pub receipt_id: ReceiptId,
    pub claim_operation_id: CoordinationOperationId,
    pub version: u64,
    pub lease_epoch: u64,
    pub lease_expires_at_ms: i64,
    pub target_turn_id: BoundedId<MAX_ID_BYTES>,
    pub delivery_fingerprint: [u8; 32],
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ClaimedInboxReceipt {
    pub metadata: InboxReceiptMetadata,
    pub lease: InboxLeaseToken,
    pub ciphertext: InboxCiphertext,
}

impl fmt::Debug for ClaimedInboxReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaimedInboxReceipt")
            .field("metadata", &self.metadata)
            .field("lease", &self.lease)
            .field("ciphertext", &self.ciphertext)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClaimInboxReceiptOutcome {
    Claimed(ClaimedInboxReceipt),
    NotReady,
    Fenced,
    Terminal(InboxLifecycle),
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordInboxSelection {
    pub lease: InboxLeaseToken,
    pub inference_attempt_id: BoundedId<MAX_ID_BYTES>,
    pub event_context: Option<NativeEventContext>,
    pub selected_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InboxSelectionToken {
    pub receipt_id: ReceiptId,
    pub claim_operation_id: CoordinationOperationId,
    pub inference_attempt_id: BoundedId<MAX_ID_BYTES>,
    pub inbox_version: u64,
    pub inclusion_version: u64,
    pub lease_epoch: u64,
    pub target_turn_id: BoundedId<MAX_ID_BYTES>,
    pub delivery_fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedInboxSelection {
    pub token: InboxSelectionToken,
    pub semantic_claim: bool,
    pub semantic_event_id: Option<CoordinationEventId>,
    pub selected_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecordInboxSelectionOutcome {
    Applied(CommittedInboxSelection),
    Duplicate(CommittedInboxSelection),
    Fenced,
    NotReady,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InboxTransportResolution {
    SendSucceeded,
    SendFailed {
        code: CoordinationFailureCode,
        retry_at_ms: i64,
    },
    SendUnknown {
        retry_at_ms: i64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordInboxTransportOutcome {
    pub selection: InboxSelectionToken,
    pub resolution: InboxTransportResolution,
    pub completed_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecordInboxTransportOutcomeResult {
    Applied(InboxReceiptMetadata),
    Duplicate(InboxReceiptMetadata),
    Fenced,
    Expired,
    Terminal(InboxLifecycle),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InboxMaintenanceBatch {
    pub now_ms: i64,
    pub limit: u32,
}

impl InboxMaintenanceBatch {
    pub(crate) fn validate(&self) -> Result<(), InboxInputError> {
        if self.limit == 0 || self.limit > MAX_INBOX_MAINTENANCE_BATCH {
            return Err(InboxInputError::InvalidBatchLimit);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolveInterruptReceipt {
    pub receipt_id: ReceiptId,
    pub expected_version: u64,
    pub terminal_event_id: CoordinationEventId,
    pub resolved_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InboxMaintenanceOutcome {
    pub changed_receipts: Vec<ReceiptId>,
}
