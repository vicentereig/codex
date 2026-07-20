use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use uuid::Uuid;

use crate::BoundedId;
use crate::CoordinationError;
use crate::CoordinationEventId;
use crate::CoordinationOperationId;
use crate::MAX_ID_BYTES;

const COMPATIBILITY_NAMESPACE: Uuid = Uuid::from_u128(0x6f4a7f9e_2f75_5bf0_9af2_1a6d7c3e9b42);
const COMPATIBILITY_PREFIX: &[u8] = b"codex-coordination-compat\0";
const IDEMPOTENCY_PREFIX: &[u8] = b"codex-coordination-idempotency\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceShape {
    CollabToolItem,
    DerivedCollabEvent,
    SubAgentActivity,
    InterAgentCommunication,
    TurnComplete,
    TurnAborted,
}

impl SourceShape {
    fn as_str(self) -> &'static str {
        match self {
            Self::CollabToolItem => "collabToolItem",
            Self::DerivedCollabEvent => "derivedCollabEvent",
            Self::SubAgentActivity => "subAgentActivity",
            Self::InterAgentCommunication => "interAgentCommunication",
            Self::TurnComplete => "turnComplete",
            Self::TurnAborted => "turnAborted",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinationSemanticSlot {
    AssignmentRequested,
    AssignmentAccepted,
    AssignmentGenerationClosed,
    MessageSubmissionRecorded,
    MessageDurablyReceived,
    MessageIncludedInModelInput,
    WaitStarted,
    WaitEnded,
    InterruptRequested,
    InterruptDurablyReceived,
    TurnInterrupted,
    Detached,
    DependencyDeclared,
    OwnershipChanged,
    TurnCompleted,
    TerminalResultObserved,
    HandoffDeliveryAttempted,
    HandoffDurablyReceived,
    HandoffIncludedInModelInput,
    HandoffDeliveryFailed,
    LegacyInteractionObserved,
}

impl CoordinationSemanticSlot {
    fn as_str(self) -> &'static str {
        match self {
            Self::AssignmentRequested => "assignmentRequested",
            Self::AssignmentAccepted => "assignmentAccepted",
            Self::AssignmentGenerationClosed => "assignmentGenerationClosed",
            Self::MessageSubmissionRecorded => "messageSubmissionRecorded",
            Self::MessageDurablyReceived => "messageDurablyReceived",
            Self::MessageIncludedInModelInput => "messageIncludedInModelInput",
            Self::WaitStarted => "waitStarted",
            Self::WaitEnded => "waitEnded",
            Self::InterruptRequested => "interruptRequested",
            Self::InterruptDurablyReceived => "interruptDurablyReceived",
            Self::TurnInterrupted => "turnInterrupted",
            Self::Detached => "detached",
            Self::DependencyDeclared => "dependencyDeclared",
            Self::OwnershipChanged => "ownershipChanged",
            Self::TurnCompleted => "turnCompleted",
            Self::TerminalResultObserved => "terminalResultObserved",
            Self::HandoffDeliveryAttempted => "handoffDeliveryAttempted",
            Self::HandoffDurablyReceived => "handoffDurablyReceived",
            Self::HandoffIncludedInModelInput => "handoffIncludedInModelInput",
            Self::HandoffDeliveryFailed => "handoffDeliveryFailed",
            Self::LegacyInteractionObserved => "legacyInteractionObserved",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompatibilitySourceIdentity {
    shape: SourceShape,
    source_thread_id: Option<ThreadId>,
    source_turn_id: Option<BoundedId<MAX_ID_BYTES>>,
    source_item_id: Option<BoundedId<MAX_ID_BYTES>>,
    source_ordinal: u64,
    semantic_slot: CoordinationSemanticSlot,
}

impl CompatibilitySourceIdentity {
    pub fn new(
        shape: SourceShape,
        source_thread_id: Option<ThreadId>,
        source_turn_id: Option<BoundedId<MAX_ID_BYTES>>,
        source_item_id: Option<BoundedId<MAX_ID_BYTES>>,
        source_ordinal: u64,
        semantic_slot: CoordinationSemanticSlot,
    ) -> Result<Self, CoordinationError> {
        if source_ordinal > i64::MAX as u64 {
            return Err(CoordinationError::Invalid {
                field: "sourceOrdinal",
                reason: "must be in 0..=i64::MAX",
            });
        }
        Ok(Self {
            shape,
            source_thread_id,
            source_turn_id,
            source_item_id,
            source_ordinal,
            semantic_slot,
        })
    }

    pub fn event_id(&self) -> CoordinationEventId {
        let mut name = Vec::with_capacity(256);
        name.extend_from_slice(COMPATIBILITY_PREFIX);
        name.extend_from_slice(&1_u16.to_be_bytes());
        name.extend_from_slice(&1_u16.to_be_bytes());
        encode_present(&mut name, self.shape.as_str());
        encode_optional(
            &mut name,
            self.source_thread_id.map(|id| id.to_string()).as_deref(),
        );
        encode_optional(
            &mut name,
            self.source_turn_id.as_ref().map(BoundedId::as_str),
        );
        encode_optional(
            &mut name,
            self.source_item_id.as_ref().map(BoundedId::as_str),
        );
        name.extend_from_slice(&self.source_ordinal.to_be_bytes());
        encode_present(&mut name, self.semantic_slot.as_str());
        CoordinationEventId::new_compatibility_v5(&COMPATIBILITY_NAMESPACE, &name)
    }
}

fn encode_optional(target: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => encode_present(target, value),
        None => target.extend_from_slice(&u32::MAX.to_be_bytes()),
    }
}

fn encode_present(target: &mut Vec<u8>, value: &str) {
    target.extend_from_slice(&(value.len() as u32).to_be_bytes());
    target.extend_from_slice(value.as_bytes());
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotencyKey {
    tuple_bytes: Vec<u8>,
    fingerprint: [u8; 32],
}

impl IdempotencyKey {
    pub fn new(
        root_thread_id: ThreadId,
        actor_thread_id: ThreadId,
        actor_turn_id: BoundedId<MAX_ID_BYTES>,
        operation_id: CoordinationOperationId,
        semantic_slot: CoordinationSemanticSlot,
    ) -> Self {
        let mut tuple_bytes = Vec::with_capacity(128);
        tuple_bytes.extend_from_slice(IDEMPOTENCY_PREFIX);
        tuple_bytes.extend_from_slice(&1_u16.to_be_bytes());
        encode_present(&mut tuple_bytes, &root_thread_id.to_string());
        encode_present(&mut tuple_bytes, &actor_thread_id.to_string());
        encode_present(&mut tuple_bytes, actor_turn_id.as_str());
        tuple_bytes.extend_from_slice(operation_id.as_uuid().as_bytes());
        encode_present(&mut tuple_bytes, semantic_slot.as_str());
        let fingerprint = sha256(&tuple_bytes);
        Self {
            tuple_bytes,
            fingerprint,
        }
    }

    pub fn tuple_bytes(&self) -> &[u8] {
        &self.tuple_bytes
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotencyRecord {
    key: IdempotencyKey,
    content_fingerprint: [u8; 32],
}

impl IdempotencyRecord {
    pub fn from_event(key: IdempotencyKey, event: &crate::CoordinationEvent) -> Self {
        Self {
            key,
            content_fingerprint: event.fingerprint(),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_serializable<T: Serialize>(
        key: IdempotencyKey,
        content: &T,
    ) -> Result<Self, CoordinationError> {
        let content_fingerprint = sha256(&canonical_json_bytes(content)?);
        Ok(Self {
            key,
            content_fingerprint,
        })
    }

    pub fn key(&self) -> &IdempotencyKey {
        &self.key
    }

    pub fn content_fingerprint(&self) -> [u8; 32] {
        self.content_fingerprint
    }

    pub fn compare(&self, incoming: &Self) -> Result<IdempotencyMatch, IdempotencyConflict> {
        if self.key.fingerprint != incoming.key.fingerprint {
            return Ok(IdempotencyMatch::DistinctKey);
        }
        if self.key.tuple_bytes != incoming.key.tuple_bytes {
            return Err(IdempotencyConflict::KeyFingerprintCollision {
                key_fingerprint: self.key.fingerprint,
            });
        }
        if self.content_fingerprint != incoming.content_fingerprint {
            return Err(IdempotencyConflict::DivergentContent {
                key_fingerprint: self.key.fingerprint,
                existing_content_fingerprint: self.content_fingerprint,
                incoming_content_fingerprint: incoming.content_fingerprint,
            });
        }
        Ok(IdempotencyMatch::Duplicate)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdempotencyMatch {
    DistinctKey,
    Duplicate,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum IdempotencyConflict {
    #[error("idempotency key fingerprint collision")]
    KeyFingerprintCollision { key_fingerprint: [u8; 32] },
    #[error("same idempotency key was reused with divergent content")]
    DivergentContent {
        key_fingerprint: [u8; 32],
        existing_content_fingerprint: [u8; 32],
        incoming_content_fingerprint: [u8; 32],
    },
}

pub(crate) fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, CoordinationError> {
    Ok(serde_json::to_vec(&canonical_json(serde_json::to_value(
        value,
    )?))?)
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonical_json).collect()),
        Value::Object(entries) => {
            let mut entries = entries.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
