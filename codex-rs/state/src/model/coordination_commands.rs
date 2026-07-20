use std::fmt;

use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::BoundedId;
use codex_coordination::ContentEvidence;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationFailureCode;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationTarget;
use codex_coordination::EncodedPayloadBytes;
use codex_coordination::MAX_CIPHERTEXT_BYTES;
use codex_coordination::MAX_ID_BYTES;
use codex_protocol::ThreadId;

use super::coordination::NativeEventContext;
use super::coordination::ReserveAssignment;

const MAX_CAPTURED_GENERATIONS: usize = 4;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CommandCiphertext(Vec<u8>);

impl CommandCiphertext {
    pub(crate) fn new(bytes: Vec<u8>) -> Result<Self, CommandInputError> {
        if bytes.len() > MAX_CIPHERTEXT_BYTES as usize {
            return Err(CommandInputError::PayloadOverLimit);
        }
        Ok(Self(bytes))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn encoded_len(&self) -> u32 {
        self.0.len() as u32
    }
}

impl fmt::Debug for CommandCiphertext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandCiphertext")
            .field("encoded_bytes", &self.0.len())
            .field("contents", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub(crate) enum CommandInputError {
    #[error("encoded command payload exceeds 65536 bytes")]
    PayloadOverLimit,
    #[error("encoded payload length does not match ciphertext length")]
    EncodedLengthMismatch,
    #[error("captured generation set must contain one to four sorted unique generations")]
    InvalidGenerationSet,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CoordinationCommandIntent {
    Assignment {
        reservation: ReserveAssignment,
    },
    Message {
        context: NativeEventContext,
        operation_id: CoordinationOperationId,
        target: CoordinationTarget,
        content: ContentEvidence,
        encoded_payload_bytes: EncodedPayloadBytes,
    },
    Interrupt {
        context: NativeEventContext,
        operation_id: CoordinationOperationId,
        target: CoordinationTarget,
    },
}

impl CoordinationCommandIntent {
    pub(crate) fn context(&self) -> &NativeEventContext {
        match self {
            Self::Assignment { reservation } => &reservation.context,
            Self::Message { context, .. } | Self::Interrupt { context, .. } => context,
        }
    }

    pub(crate) fn operation_id(&self) -> CoordinationOperationId {
        match self {
            Self::Assignment { reservation } => reservation.operation_id,
            Self::Message { operation_id, .. } | Self::Interrupt { operation_id, .. } => {
                *operation_id
            }
        }
    }

    pub(crate) fn encoded_payload_bytes(&self) -> u32 {
        match self {
            Self::Assignment { reservation } => reservation.encoded_payload_bytes.get(),
            Self::Message {
                encoded_payload_bytes,
                ..
            } => encoded_payload_bytes.get(),
            Self::Interrupt { .. } => 0,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct RecordCoordinationCommand {
    pub intent: CoordinationCommandIntent,
    pub ciphertext: CommandCiphertext,
}

impl RecordCoordinationCommand {
    pub(crate) fn new(
        intent: CoordinationCommandIntent,
        ciphertext: CommandCiphertext,
    ) -> Result<Self, CommandInputError> {
        if intent.encoded_payload_bytes() != ciphertext.encoded_len() {
            return Err(CommandInputError::EncodedLengthMismatch);
        }
        Ok(Self { intent, ciphertext })
    }
}

impl fmt::Debug for RecordCoordinationCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordCoordinationCommand")
            .field("operation_id", &self.intent.operation_id())
            .field("kind", &CommandKind::from_intent(&self.intent))
            .field("encoded_payload_bytes", &self.ciphertext.encoded_len())
            .field("ciphertext", &self.ciphertext)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandKind {
    AssignmentSpawn,
    AssignmentFollowup,
    Message,
    Interrupt,
}

impl CommandKind {
    pub(crate) fn from_intent(intent: &CoordinationCommandIntent) -> Self {
        match intent {
            CoordinationCommandIntent::Assignment { reservation } => {
                match &reservation.reservation {
                    super::coordination::AssignmentReservation::Spawn => Self::AssignmentSpawn,
                    super::coordination::AssignmentReservation::Followup { .. } => {
                        Self::AssignmentFollowup
                    }
                }
            }
            CoordinationCommandIntent::Message { .. } => Self::Message,
            CoordinationCommandIntent::Interrupt { .. } => Self::Interrupt,
        }
    }

    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            Self::AssignmentSpawn => "assignmentSpawn",
            Self::AssignmentFollowup => "assignmentFollowup",
            Self::Message => "message",
            Self::Interrupt => "interrupt",
        }
    }

    pub(crate) fn event_kind_sql(self) -> &'static str {
        match self {
            Self::AssignmentSpawn | Self::AssignmentFollowup => "assignmentRequested",
            Self::Message => "messageSubmissionRecorded",
            Self::Interrupt => "interruptRequested",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedCommandTarget {
    pub target_thread_id: ThreadId,
    pub assignment_id: AssignmentId,
    pub generation: AssignmentGeneration,
    pub turn_id: Option<BoundedId<MAX_ID_BYTES>>,
    pub captured_head_generation: Option<AssignmentGeneration>,
    pub captured_turn_set: Option<CapturedGenerationSet>,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CapturedGenerationSet {
    generations: Vec<AssignmentGeneration>,
    canonical_bytes: Vec<u8>,
}

impl CapturedGenerationSet {
    pub(crate) fn new(
        mut generations: Vec<AssignmentGeneration>,
    ) -> Result<Self, CommandInputError> {
        generations.sort_unstable();
        generations.dedup();
        if generations.is_empty() || generations.len() > MAX_CAPTURED_GENERATIONS {
            return Err(CommandInputError::InvalidGenerationSet);
        }
        let mut canonical_bytes = Vec::with_capacity(1 + generations.len() * 4);
        canonical_bytes.push(generations.len() as u8);
        for generation in &generations {
            canonical_bytes.extend_from_slice(&generation.get().to_be_bytes());
        }
        Ok(Self {
            generations,
            canonical_bytes,
        })
    }

    pub(crate) fn generations(&self) -> &[AssignmentGeneration] {
        &self.generations
    }

    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

impl fmt::Debug for CapturedGenerationSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapturedGenerationSet")
            .field("generations", &self.generations)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandLifecycle {
    Pending,
    Leased,
    Succeeded,
    Poisoned,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CoordinationCommandMetadata {
    pub operation_id: CoordinationOperationId,
    pub root_thread_id: ThreadId,
    pub intent_event_id: CoordinationEventId,
    pub kind: CommandKind,
    pub target: CapturedCommandTarget,
    pub lifecycle: CommandLifecycle,
    pub version: u64,
    pub claim_count: u64,
    pub attempt_count: u64,
    pub attempted_lease_epoch: Option<u64>,
    pub lease_epoch: u64,
    pub retry_after_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecordCoordinationCommandOutcome {
    Applied(CoordinationCommandMetadata),
    Duplicate(CoordinationCommandMetadata),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandLeaseToken {
    pub operation_id: CoordinationOperationId,
    pub version: u64,
    pub lease_epoch: u64,
    pub lease_expires_at_ms: i64,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ClaimedCoordinationCommand {
    pub metadata: CoordinationCommandMetadata,
    pub lease: CommandLeaseToken,
    pub ciphertext: CommandCiphertext,
}

impl fmt::Debug for ClaimedCoordinationCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaimedCoordinationCommand")
            .field("metadata", &self.metadata)
            .field("lease", &self.lease)
            .field("ciphertext", &self.ciphertext)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClaimCoordinationCommandOutcome {
    Claimed(ClaimedCoordinationCommand),
    NotReady,
    Fenced,
    Terminal(CommandLifecycle),
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BegunCommandAttempt {
    pub lease: CommandLeaseToken,
    pub attempt: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommandAttemptResolution {
    RetryAt {
        retry_at_ms: i64,
        code: CoordinationFailureCode,
    },
    Succeeded,
    Poisoned {
        code: CoordinationFailureCode,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResolveCommandAttemptOutcome {
    Applied(CoordinationCommandMetadata),
    Fenced,
    Expired,
    Terminal(CommandLifecycle),
}
