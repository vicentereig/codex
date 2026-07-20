use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use sha2::Digest;
use sha2::Sha256;
use uuid::Version;

use crate::AssignmentEvidence;
use crate::BoundedId;
use crate::CompatibilitySourceIdentity;
use crate::ContentEvidence;
use crate::CoordinationError;
use crate::CoordinationEventEnvelope;
use crate::CoordinationEventKind;
use crate::CoordinationOrder;
use crate::CoordinationPrincipal;
use crate::CoordinationSemanticSlot;
use crate::CoordinationSource;
use crate::CoordinationTarget;
use crate::EventData;
use crate::Evidence;
use crate::InterruptionReason;
use crate::MAX_EVENT_BYTES;
use crate::MAX_ID_BYTES;
use crate::OwnershipChangeMode;
use crate::UnavailableReason;
use crate::WaitOutcome;
use crate::canonical_json_bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinationEvent {
    data: EventData,
    canonical_bytes: Vec<u8>,
    fingerprint: [u8; 32],
}

impl CoordinationEvent {
    pub fn try_new(
        envelope: CoordinationEventEnvelope,
        kind: CoordinationEventKind,
    ) -> Result<Self, CoordinationError> {
        validate_local(&envelope, &kind)?;
        let data = EventData { envelope, kind };
        let bytes = canonical_json_bytes(&data)?;
        if bytes.len() > MAX_EVENT_BYTES {
            return Err(CoordinationError::TooLong {
                field: "coordinationEvent",
                limit: MAX_EVENT_BYTES,
            });
        }
        let fingerprint = Sha256::digest(&bytes).into();
        Ok(Self {
            data,
            canonical_bytes: bytes,
            fingerprint,
        })
    }

    pub fn envelope(&self) -> &CoordinationEventEnvelope {
        &self.data.envelope
    }

    pub fn kind(&self) -> &CoordinationEventKind {
        &self.data.kind
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}

impl Serialize for CoordinationEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CoordinationEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = EventData::deserialize(deserializer)?;
        Self::try_new(data.envelope, data.kind).map_err(serde::de::Error::custom)
    }
}

fn validate_local(
    envelope: &CoordinationEventEnvelope,
    kind: &CoordinationEventKind,
) -> Result<(), CoordinationError> {
    let native = match (&envelope.order, &envelope.source) {
        (
            CoordinationOrder::Native { .. },
            CoordinationSource::Native {
                suppression_keys, ..
            },
        ) if suppression_keys.omitted_count() == 0 => true,
        (
            CoordinationOrder::Compatibility { source_ordinal, .. },
            CoordinationSource::Compatibility { key, .. },
        ) if key.source_ordinal == *source_ordinal => {
            validate_compatibility_id(envelope, kind, key)?;
            false
        }
        _ => {
            return Err(CoordinationError::Invariant(
                "coordination source and order modes must agree",
            ));
        }
    };
    let expected_version = if native {
        Version::SortRand
    } else {
        Version::Sha1
    };
    if envelope.event_id.as_uuid().get_version() != Some(expected_version) {
        return Err(CoordinationError::Invariant(
            "native event IDs must be UUIDv7 and compatibility IDs UUIDv5",
        ));
    }
    if native {
        let Evidence::Known { value: actor_turn } = &envelope.actor.turn_id else {
            return Err(CoordinationError::Invariant(
                "native actor turn must be known",
            ));
        };
        let principal = terminal_projection(kind).unwrap_or(&envelope.actor);
        let turn = terminal_turn(kind).unwrap_or(actor_turn);
        if terminal_turn(kind).is_some()
            && !matches!(&principal.turn_id, Evidence::Known { value } if value == turn)
        {
            return Err(CoordinationError::Invariant(
                "terminal target turn evidence must match the terminal turn",
            ));
        }
        if envelope.projection.thread_id != principal.thread_id
            || envelope.projection.turn_id != *turn
        {
            return Err(CoordinationError::Invariant(
                "native projection attribution must be exact",
            ));
        }
    } else {
        if !envelope.causes.items().is_empty() {
            return Err(CoordinationError::Invariant(
                "compatibility events cannot claim native causes",
            ));
        }
        match &envelope.actor.turn_id {
            Evidence::Known { value } => {
                if envelope.projection.thread_id != envelope.actor.thread_id
                    || envelope.projection.turn_id != *value
                {
                    return Err(CoordinationError::Invariant(
                        "compatibility projection must use its source turn",
                    ));
                }
            }
            Evidence::Unavailable { .. } | Evidence::NotApplicable => {
                if envelope.projection.thread_id != envelope.root_thread_id {
                    return Err(CoordinationError::Invariant(
                        "compatibility fallback projection must use the root",
                    ));
                }
            }
        }
    }
    validate_kind(envelope, kind, native)
}

fn validate_kind(
    envelope: &CoordinationEventEnvelope,
    kind: &CoordinationEventKind,
    native: bool,
) -> Result<(), CoordinationError> {
    let known_target = |target: &CoordinationTarget| {
        if native && !matches!(target.assignment, AssignmentEvidence::Known { .. }) {
            Err(CoordinationError::Invariant(
                "native target assignment must be known",
            ))
        } else {
            Ok(())
        }
    };
    let known_owner = || {
        if native && !matches!(envelope.responsibility_owner, Evidence::Known { .. }) {
            Err(CoordinationError::Invariant(
                "native responsibility owner must be known",
            ))
        } else {
            Ok(())
        }
    };
    let expected_causes = match kind {
        CoordinationEventKind::AssignmentRequested {
            target, objective, ..
        }
        | CoordinationEventKind::MessageSubmissionRecorded {
            target,
            content: objective,
            ..
        } => {
            known_target(target)?;
            require_encrypted(native, objective)?;
            0
        }
        CoordinationEventKind::AssignmentAccepted { target, .. } => {
            known_target(target)?;
            1
        }
        CoordinationEventKind::AssignmentGenerationClosed { assignment, .. } => {
            if native && !matches!(assignment, AssignmentEvidence::Known { .. }) {
                return Err(CoordinationError::Invariant(
                    "native generation close assignment must be known",
                ));
            }
            1
        }
        CoordinationEventKind::MessageDurablyReceived { target, .. }
        | CoordinationEventKind::MessageIncludedInModelInput { target, .. }
        | CoordinationEventKind::InterruptDurablyReceived { target, .. } => {
            known_target(target)?;
            1
        }
        CoordinationEventKind::WaitStarted {
            targets,
            timeout_ms,
            ..
        } => {
            if *timeout_ms > 3_600_000 {
                return Err(CoordinationError::Invariant(
                    "wait timeout exceeds one hour",
                ));
            }
            if native {
                for target in targets.items() {
                    known_target(&target.target)?;
                }
            }
            0
        }
        CoordinationEventKind::WaitEnded {
            targets,
            outcome,
            failure,
            ..
        } => {
            if native {
                for target in targets.items() {
                    known_target(&target.target)?;
                }
            }
            let consistent = match (outcome, failure) {
                (
                    Evidence::Known {
                        value: WaitOutcome::Failed,
                    },
                    Evidence::Known { .. },
                ) => true,
                (Evidence::Known { value }, Evidence::NotApplicable) => {
                    *value != WaitOutcome::Failed
                }
                (Evidence::Unavailable { .. }, Evidence::Unavailable { .. }) => !native,
                _ => false,
            };
            if !consistent {
                return Err(CoordinationError::Invariant(
                    "wait outcome and failure evidence contradict",
                ));
            }
            1
        }
        CoordinationEventKind::InterruptRequested { target, .. } => {
            known_target(target)?;
            0
        }
        CoordinationEventKind::TurnInterrupted {
            interruption_reason,
            ..
        } => usize::from(matches!(
            interruption_reason,
            InterruptionReason::Requested { .. }
        )),
        CoordinationEventKind::Detached {
            target,
            previous_owner,
        } => {
            known_target(target)?;
            if native
                && (!matches!(previous_owner, Evidence::Known { .. })
                    || !matches!(envelope.responsibility_owner, Evidence::NotApplicable))
            {
                return Err(CoordinationError::Invariant(
                    "native detach requires a previous owner and clears responsibility",
                ));
            }
            0
        }
        CoordinationEventKind::DependencyDeclared {
            dependent,
            prerequisite,
            ..
        } => {
            known_target(dependent)?;
            known_target(prerequisite)?;
            known_owner()?;
            0
        }
        CoordinationEventKind::OwnershipChanged {
            target,
            previous_owner,
            new_owner,
            change_mode,
            ..
        } => {
            known_target(target)?;
            let previous_owner_valid = matches!(previous_owner, Evidence::Known { .. })
                || (*change_mode == OwnershipChangeMode::ExplicitTransfer
                    && matches!(previous_owner, Evidence::NotApplicable));
            if native
                && (!previous_owner_valid
                    || !matches!(new_owner, Evidence::Known { .. })
                    || envelope.responsibility_owner != *new_owner)
            {
                return Err(CoordinationError::Invariant(
                    "native ownership change requires exact known owners",
                ));
            }
            0
        }
        CoordinationEventKind::TerminalResultObserved { summary, .. } => {
            require_encrypted(native, summary)?;
            1
        }
        CoordinationEventKind::HandoffDeliveryAttempted { from, to, .. }
        | CoordinationEventKind::HandoffDurablyReceived { from, to, .. }
        | CoordinationEventKind::HandoffDeliveryFailed { from, to, .. } => {
            known_target(from)?;
            known_target(to)?;
            known_owner()?;
            1
        }
        CoordinationEventKind::HandoffIncludedInModelInput { to, .. } => {
            known_target(to)?;
            known_owner()?;
            1
        }
        CoordinationEventKind::LegacyInteractionObserved { .. } => {
            if native {
                return Err(CoordinationError::Invariant(
                    "legacy observations require compatibility order",
                ));
            }
            0
        }
        CoordinationEventKind::TurnCompleted { .. } => 0,
    };
    let expected_causes = if native { expected_causes } else { 0 };
    let omitted_native_facts = native
        && (envelope.causes.omitted_count() > 0
            || match kind {
                CoordinationEventKind::WaitStarted { targets, .. }
                | CoordinationEventKind::WaitEnded { targets, .. } => targets.omitted_count() > 0,
                CoordinationEventKind::TurnInterrupted {
                    included_generations,
                    ..
                }
                | CoordinationEventKind::TurnCompleted {
                    included_generations,
                    ..
                } => included_generations.omitted_count() > 0,
                _ => false,
            });
    if omitted_native_facts {
        return Err(CoordinationError::Invariant(
            "native event facts cannot be omitted",
        ));
    }
    if envelope.causes.items().len() != expected_causes {
        return Err(CoordinationError::Invariant(
            "event has the wrong cause cardinality",
        ));
    }
    Ok(())
}

fn require_encrypted(native: bool, content: &ContentEvidence) -> Result<(), CoordinationError> {
    if native
        && !matches!(
            content,
            ContentEvidence::Unavailable {
                reason: UnavailableReason::EncryptedPayload
            }
        )
    {
        return Err(CoordinationError::Invariant(
            "native encrypted content must remain unavailable",
        ));
    }
    Ok(())
}

fn terminal_projection(kind: &CoordinationEventKind) -> Option<&CoordinationPrincipal> {
    match kind {
        CoordinationEventKind::TurnInterrupted { target, .. }
        | CoordinationEventKind::TurnCompleted { target, .. }
        | CoordinationEventKind::TerminalResultObserved { target, .. } => Some(&target.principal),
        _ => None,
    }
}

fn terminal_turn(kind: &CoordinationEventKind) -> Option<&BoundedId<MAX_ID_BYTES>> {
    match kind {
        CoordinationEventKind::TurnInterrupted { target_turn_id, .. }
        | CoordinationEventKind::TurnCompleted { target_turn_id, .. }
        | CoordinationEventKind::TerminalResultObserved { target_turn_id, .. } => {
            Some(target_turn_id)
        }
        _ => None,
    }
}

fn validate_compatibility_id(
    envelope: &CoordinationEventEnvelope,
    kind: &CoordinationEventKind,
    key: &crate::SourceKey,
) -> Result<(), CoordinationError> {
    let turn_id = match &envelope.actor.turn_id {
        Evidence::Known { value } => Some(value.clone()),
        Evidence::Unavailable { .. } | Evidence::NotApplicable => None,
    };
    let item_id = match &key.source_item_id {
        Evidence::Known { value } => Some(value.clone()),
        Evidence::Unavailable { .. } | Evidence::NotApplicable => None,
    };
    let expected = CompatibilitySourceIdentity::new(
        key.shape,
        Some(envelope.actor.thread_id),
        turn_id,
        item_id,
        key.source_ordinal.get(),
        semantic_slot(kind),
    )?;
    if envelope.event_id != expected.event_id() {
        return Err(CoordinationError::Invariant(
            "compatibility event ID does not match its source identity",
        ));
    }
    Ok(())
}

fn semantic_slot(kind: &CoordinationEventKind) -> CoordinationSemanticSlot {
    use CoordinationEventKind as Kind;
    use CoordinationSemanticSlot as Slot;
    match kind {
        Kind::AssignmentRequested { .. } => Slot::AssignmentRequested,
        Kind::AssignmentAccepted { .. } => Slot::AssignmentAccepted,
        Kind::AssignmentGenerationClosed { .. } => Slot::AssignmentGenerationClosed,
        Kind::MessageSubmissionRecorded { .. } => Slot::MessageSubmissionRecorded,
        Kind::MessageDurablyReceived { .. } => Slot::MessageDurablyReceived,
        Kind::MessageIncludedInModelInput { .. } => Slot::MessageIncludedInModelInput,
        Kind::WaitStarted { .. } => Slot::WaitStarted,
        Kind::WaitEnded { .. } => Slot::WaitEnded,
        Kind::InterruptRequested { .. } => Slot::InterruptRequested,
        Kind::InterruptDurablyReceived { .. } => Slot::InterruptDurablyReceived,
        Kind::TurnInterrupted { .. } => Slot::TurnInterrupted,
        Kind::Detached { .. } => Slot::Detached,
        Kind::DependencyDeclared { .. } => Slot::DependencyDeclared,
        Kind::OwnershipChanged { .. } => Slot::OwnershipChanged,
        Kind::TurnCompleted { .. } => Slot::TurnCompleted,
        Kind::TerminalResultObserved { .. } => Slot::TerminalResultObserved,
        Kind::HandoffDeliveryAttempted { .. } => Slot::HandoffDeliveryAttempted,
        Kind::HandoffDurablyReceived { .. } => Slot::HandoffDurablyReceived,
        Kind::HandoffIncludedInModelInput { .. } => Slot::HandoffIncludedInModelInput,
        Kind::HandoffDeliveryFailed { .. } => Slot::HandoffDeliveryFailed,
        Kind::LegacyInteractionObserved { .. } => Slot::LegacyInteractionObserved,
    }
}
