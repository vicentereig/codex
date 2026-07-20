use std::fmt;

use codex_coordination::AssignmentGeneration;
use codex_coordination::BoundedId;
use codex_coordination::CompatibilityOrdinal;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationOrder;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::CoordinationSource;
use codex_coordination::Evidence;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::SourceShape;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use uuid::Uuid;

const DEGRADATION_NAMESPACE: Uuid = Uuid::from_u128(0x2bb63723_d9ca_5896_8af5_2cb56f5e7024);
const DEGRADATION_ID_PREFIX: &[u8] = b"codex-coordination-degradation\0\x00\x01";

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CheckedBytes<const MAX: usize>(Vec<u8>);

impl<const MAX: usize> CheckedBytes<MAX> {
    pub(crate) fn new(bytes: Vec<u8>) -> Result<Self, RecoveryInputError> {
        if bytes.is_empty() || bytes.len() > MAX {
            return Err(RecoveryInputError::InvalidCanonicalBytes);
        }
        Ok(Self(bytes))
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn fingerprint(&self) -> [u8; 32] {
        Sha256::digest(&self.0).into()
    }
}

impl<const MAX: usize> fmt::Debug for CheckedBytes<MAX> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckedBytes")
            .field("encoded_bytes", &self.0.len())
            .field("contents", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub(crate) enum RecoveryInputError {
    #[error("canonical recovery evidence must be nonempty and within its fixed cap")]
    InvalidCanonicalBytes,
    #[error("the event is not a checked compatibility event for this root")]
    NotCompatibilityEvent,
    #[error("terminal degradation requires a terminal semantic slot")]
    InvalidTerminalSlot,
    #[error("included generations must be sorted, unique, and contain at most four entries")]
    InvalidGenerationEvidence,
    #[error("recovery batch limit must be between one and 100")]
    InvalidBatchLimit,
    #[error("publication lease deadline must be after now")]
    InvalidLeaseDeadline,
    #[error("publication retry deadline must be monotonic and after now")]
    InvalidRetryDeadline,
    #[error("legacy degradation reason is not a closed legacy-reduction reason")]
    InvalidLegacyDegradationReason,
    #[error("recovery timestamps must be nonnegative")]
    InvalidTimestamp,
    #[error("legacy checkpoint order does not match its checked page records")]
    InvalidCheckpointOrder,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LegacySourceIdentity {
    pub shape: SourceShape,
    pub source_thread_id: Option<ThreadId>,
    pub source_turn_id: Option<BoundedId<MAX_ID_BYTES>>,
    pub source_item_id: Option<BoundedId<MAX_ID_BYTES>>,
    pub source_ordinal: u64,
    #[serde(serialize_with = "serialize_semantic_slot")]
    pub semantic_slot: CoordinationSemanticSlot,
}

impl LegacySourceIdentity {
    pub(crate) fn from_event(event: &CoordinationEvent) -> Result<Self, RecoveryInputError> {
        let (key, source_ordinal) = match (&event.envelope().source, &event.envelope().order) {
            (
                CoordinationSource::Compatibility { key, .. },
                CoordinationOrder::Compatibility { source_ordinal, .. },
            ) if key.source_ordinal == *source_ordinal => (key, source_ordinal.get()),
            _ => return Err(RecoveryInputError::NotCompatibilityEvent),
        };
        let source_turn_id = match &event.envelope().actor.turn_id {
            Evidence::Known { value } => Some(value.clone()),
            Evidence::Unavailable { .. } | Evidence::NotApplicable => None,
        };
        let source_item_id = match &key.source_item_id {
            Evidence::Known { value } => Some(value.clone()),
            Evidence::Unavailable { .. } | Evidence::NotApplicable => None,
        };
        Ok(Self {
            shape: key.shape,
            source_thread_id: Some(event.envelope().actor.thread_id),
            source_turn_id,
            source_item_id,
            source_ordinal,
            semantic_slot: event.kind().semantic_slot(),
        })
    }

    pub(crate) fn canonical_bytes(&self) -> Result<CheckedBytes<1024>, RecoveryInputError> {
        CheckedBytes::new(
            serde_json::to_vec(self).map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckedLegacyLink {
    pub compatibility_event_id: CoordinationEventId,
    pub root_thread_id: ThreadId,
    pub expected_state_epoch: StateEpoch,
    pub source: LegacySourceIdentity,
    pub source_identity_bytes: CheckedBytes<1024>,
    pub canonical_event_bytes: CheckedBytes<8192>,
    pub after_revision: u64,
    pub native_suppression: Option<NativeSuppression>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeSuppression {
    pub event_id: CoordinationEventId,
    pub suppressed_at_ms: i64,
}

impl CheckedLegacyLink {
    pub(crate) fn new(
        root_thread_id: ThreadId,
        expected_state_epoch: StateEpoch,
        event: &CoordinationEvent,
    ) -> Result<Self, RecoveryInputError> {
        if event.envelope().root_thread_id != root_thread_id {
            return Err(RecoveryInputError::NotCompatibilityEvent);
        }
        let after_revision = match event.envelope().order {
            CoordinationOrder::Compatibility { after_revision, .. } => after_revision.get(),
            CoordinationOrder::Native { .. } => {
                return Err(RecoveryInputError::NotCompatibilityEvent);
            }
        };
        let source = LegacySourceIdentity::from_event(event)?;
        Ok(Self {
            compatibility_event_id: event.envelope().event_id,
            root_thread_id,
            expected_state_epoch,
            source_identity_bytes: source.canonical_bytes()?,
            source,
            canonical_event_bytes: CheckedBytes::new(event.canonical_bytes().to_vec())?,
            after_revision,
            native_suppression: None,
        })
    }

    pub(crate) fn with_native_suppression(
        mut self,
        event_id: CoordinationEventId,
        suppressed_at_ms: i64,
    ) -> Result<Self, RecoveryInputError> {
        if suppressed_at_ms < 0 {
            return Err(RecoveryInputError::InvalidTimestamp);
        }
        self.native_suppression = Some(NativeSuppression {
            event_id,
            suppressed_at_ms,
        });
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LegacyLinkRecord {
    pub compatibility_event_id: CoordinationEventId,
    pub root_thread_id: ThreadId,
    pub state_epoch: StateEpoch,
    pub source: LegacySourceIdentity,
    pub after_revision: u64,
    pub suppressed_by_native_event_id: Option<CoordinationEventId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecordLegacyLinkOutcome {
    Linked(LegacyLinkRecord),
    Duplicate(LegacyLinkRecord),
    Suppressed(LegacyLinkRecord, CoordinationEventId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum DegradationSourceKind {
    ExogenousTerminal,
    LegacyReduction,
    Recovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum DegradationReason {
    CoordinationTemporarilyUnavailable,
    MissingProvenance,
    AmbiguousSource,
    OverLimit,
    InvalidLegacyValue,
    CorruptSource,
    PoisonedAttempt,
    ExpiredPayload,
    StateLossDegraded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum TerminalEvidenceKind {
    Completed,
    Interrupted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum TerminalEvidenceOutcome {
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct DegradationId(Uuid);

impl DegradationId {
    pub(crate) fn from_uuid(id: Uuid) -> Result<Self, RecoveryInputError> {
        if id.is_nil() || id.get_version() != Some(uuid::Version::Sha1) {
            return Err(RecoveryInputError::InvalidCanonicalBytes);
        }
        Ok(Self(id))
    }

    pub(crate) fn parse(value: &str) -> Result<Self, RecoveryInputError> {
        let id = Uuid::parse_str(value).map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?;
        Self::from_uuid(id)
    }
}

impl fmt::Display for DegradationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TerminalProvenance {
    Known(LegacySourceIdentity),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExogenousTerminalObservation {
    pub root_thread_id: ThreadId,
    pub captured_state_epoch: Option<StateEpoch>,
    pub provenance: TerminalProvenance,
    pub target_thread_id: ThreadId,
    pub target_turn_id: BoundedId<MAX_ID_BYTES>,
    pub terminal_kind: TerminalEvidenceKind,
    pub terminal_outcome: TerminalEvidenceOutcome,
    pub included_generations: Evidence<Vec<AssignmentGeneration>>,
    pub observed_at: i64,
    pub after_revision: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DegradationIdentity<'a> {
    version: u16,
    root_thread_id: ThreadId,
    captured_state_epoch: Option<StateEpoch>,
    source: &'a LegacySourceIdentity,
    reason: DegradationReason,
    #[serde(serialize_with = "serialize_semantic_slot")]
    semantic_slot: CoordinationSemanticSlot,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalTerminalRecord<'a> {
    identity: &'a DegradationIdentity<'a>,
    source_kind: DegradationSourceKind,
    target_thread_id: ThreadId,
    target_turn_id: &'a BoundedId<MAX_ID_BYTES>,
    terminal_kind: TerminalEvidenceKind,
    terminal_outcome: TerminalEvidenceOutcome,
    included_generations: &'a Evidence<Vec<AssignmentGeneration>>,
    observed_at: i64,
    after_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckedExogenousTerminalObservation {
    pub degradation_id: DegradationId,
    pub root_thread_id: ThreadId,
    pub captured_state_epoch: Option<StateEpoch>,
    pub source: LegacySourceIdentity,
    pub target_thread_id: ThreadId,
    pub target_turn_id: BoundedId<MAX_ID_BYTES>,
    pub terminal_kind: TerminalEvidenceKind,
    pub terminal_outcome: TerminalEvidenceOutcome,
    pub included_generations: Evidence<Vec<AssignmentGeneration>>,
    pub identity_bytes: CheckedBytes<1024>,
    pub canonical_record_bytes: CheckedBytes<4096>,
    pub observed_at: i64,
    pub after_revision: u64,
}

impl ExogenousTerminalObservation {
    pub(crate) fn check(
        self,
    ) -> Result<Option<CheckedExogenousTerminalObservation>, RecoveryInputError> {
        let TerminalProvenance::Known(source) = self.provenance else {
            return Ok(None);
        };
        if self.observed_at < 0 {
            return Err(RecoveryInputError::InvalidTimestamp);
        }
        if !matches!(
            source.semantic_slot,
            CoordinationSemanticSlot::TurnCompleted
                | CoordinationSemanticSlot::TurnInterrupted
                | CoordinationSemanticSlot::TerminalResultObserved
        ) {
            return Err(RecoveryInputError::InvalidTerminalSlot);
        }
        if let Evidence::Known { value } = &self.included_generations
            && (value.len() > 4 || value.windows(2).any(|pair| pair[0] >= pair[1]))
        {
            return Err(RecoveryInputError::InvalidGenerationEvidence);
        }
        CompatibilityOrdinal::new(self.after_revision)
            .map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?;
        let identity = DegradationIdentity {
            version: 1,
            root_thread_id: self.root_thread_id,
            captured_state_epoch: self.captured_state_epoch,
            source: &source,
            reason: DegradationReason::CoordinationTemporarilyUnavailable,
            semantic_slot: source.semantic_slot,
        };
        let identity_bytes = CheckedBytes::new(
            serde_json::to_vec(&identity).map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?,
        )?;
        let degradation_id = deterministic_degradation_id(identity_bytes.as_slice())?;
        let canonical_record_bytes = CheckedBytes::new(
            serde_json::to_vec(&CanonicalTerminalRecord {
                identity: &identity,
                source_kind: DegradationSourceKind::ExogenousTerminal,
                target_thread_id: self.target_thread_id,
                target_turn_id: &self.target_turn_id,
                terminal_kind: self.terminal_kind,
                terminal_outcome: self.terminal_outcome,
                included_generations: &self.included_generations,
                observed_at: self.observed_at,
                after_revision: self.after_revision,
            })
            .map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?,
        )?;
        Ok(Some(CheckedExogenousTerminalObservation {
            degradation_id,
            root_thread_id: self.root_thread_id,
            captured_state_epoch: self.captured_state_epoch,
            source,
            target_thread_id: self.target_thread_id,
            target_turn_id: self.target_turn_id,
            terminal_kind: self.terminal_kind,
            terminal_outcome: self.terminal_outcome,
            included_generations: self.included_generations,
            identity_bytes,
            canonical_record_bytes,
            observed_at: self.observed_at,
            after_revision: self.after_revision,
        }))
    }
}

pub(crate) fn deterministic_degradation_id(
    identity_bytes: &[u8],
) -> Result<DegradationId, RecoveryInputError> {
    DegradationId::from_uuid(Uuid::new_v5(
        &DEGRADATION_NAMESPACE,
        &[DEGRADATION_ID_PREFIX, identity_bytes].concat(),
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DegradationRecord {
    pub degradation_id: DegradationId,
    pub root_thread_id: ThreadId,
    pub state_epoch: Option<StateEpoch>,
    pub source_kind: DegradationSourceKind,
    pub source: LegacySourceIdentity,
    pub reason: DegradationReason,
    pub after_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecordExogenousTerminalOutcome {
    Applied(DegradationRecord),
    Duplicate(DegradationRecord),
    UnknownProvenance,
}

fn serialize_semantic_slot<S>(
    slot: &CoordinationSemanticSlot,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(semantic_slot_sql(*slot))
}

pub(crate) fn source_shape_sql(shape: SourceShape) -> &'static str {
    match shape {
        SourceShape::CollabToolItem => "collabToolItem",
        SourceShape::DerivedCollabEvent => "derivedCollabEvent",
        SourceShape::SubAgentActivity => "subAgentActivity",
        SourceShape::InterAgentCommunication => "interAgentCommunication",
        SourceShape::TurnComplete => "turnComplete",
        SourceShape::TurnAborted => "turnAborted",
    }
}

pub(crate) fn semantic_slot_sql(slot: CoordinationSemanticSlot) -> &'static str {
    match slot {
        CoordinationSemanticSlot::AssignmentRequested => "assignmentRequested",
        CoordinationSemanticSlot::AssignmentAccepted => "assignmentAccepted",
        CoordinationSemanticSlot::AssignmentGenerationClosed => "assignmentGenerationClosed",
        CoordinationSemanticSlot::MessageSubmissionRecorded => "messageSubmissionRecorded",
        CoordinationSemanticSlot::MessageDurablyReceived => "messageDurablyReceived",
        CoordinationSemanticSlot::MessageIncludedInModelInput => "messageIncludedInModelInput",
        CoordinationSemanticSlot::WaitStarted => "waitStarted",
        CoordinationSemanticSlot::WaitEnded => "waitEnded",
        CoordinationSemanticSlot::InterruptRequested => "interruptRequested",
        CoordinationSemanticSlot::InterruptDurablyReceived => "interruptDurablyReceived",
        CoordinationSemanticSlot::TurnInterrupted => "turnInterrupted",
        CoordinationSemanticSlot::Detached => "detached",
        CoordinationSemanticSlot::DependencyDeclared => "dependencyDeclared",
        CoordinationSemanticSlot::OwnershipChanged => "ownershipChanged",
        CoordinationSemanticSlot::TurnCompleted => "turnCompleted",
        CoordinationSemanticSlot::TerminalResultObserved => "terminalResultObserved",
        CoordinationSemanticSlot::HandoffDeliveryAttempted => "handoffDeliveryAttempted",
        CoordinationSemanticSlot::HandoffDurablyReceived => "handoffDurablyReceived",
        CoordinationSemanticSlot::HandoffIncludedInModelInput => "handoffIncludedInModelInput",
        CoordinationSemanticSlot::HandoffDeliveryFailed => "handoffDeliveryFailed",
        CoordinationSemanticSlot::LegacyInteractionObserved => "legacyInteractionObserved",
    }
}
