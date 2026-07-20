use codex_coordination::CoordinationEventId;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;

use super::coordination_legacy_degradation::CheckedLegacyReductionDegradation;
use super::coordination_recovery::CheckedLegacyLink;
use super::coordination_recovery::DegradationId;
use super::coordination_recovery::RecoveryInputError;

pub(crate) const MAX_RECOVERY_BATCH: u32 = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LegacyScanCheckpoint {
    pub root_thread_id: ThreadId,
    pub state_epoch: StateEpoch,
    pub source_thread_id: ThreadId,
    pub next_physical_ordinal: u64,
    pub scanned_prefix_fingerprint: [u8; 32],
    pub last_order: Option<(u64, CoordinationEventId)>,
    pub complete: bool,
    pub version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LegacyScanPage {
    pub root_thread_id: ThreadId,
    pub expected_state_epoch: StateEpoch,
    pub source_thread_id: ThreadId,
    pub expected_version: u64,
    pub expected_prefix_fingerprint: Option<[u8; 32]>,
    pub next_physical_ordinal: u64,
    pub scanned_prefix_fingerprint: [u8; 32],
    pub last_order: Option<(u64, CoordinationEventId)>,
    pub complete: bool,
    pub links: Vec<CheckedLegacyLink>,
    pub degradations: Vec<CheckedLegacyReductionDegradation>,
    pub now_ms: i64,
}

impl LegacyScanPage {
    pub(crate) fn validate(&self) -> Result<(), RecoveryInputError> {
        if self.links.len() + self.degradations.len() > MAX_RECOVERY_BATCH as usize
            || self.now_ms < 0
            || self.next_physical_ordinal > i64::MAX as u64
            || self.expected_version > i64::MAX as u64
            || self.links.iter().any(|link| {
                link.root_thread_id != self.root_thread_id
                    || link.expected_state_epoch != self.expected_state_epoch
                    || link.source.source_thread_id != Some(self.source_thread_id)
            })
            || self.degradations.iter().any(|degradation| {
                degradation.root_thread_id != self.root_thread_id
                    || degradation.state_epoch != self.expected_state_epoch
                    || degradation.source.source_thread_id != Some(self.source_thread_id)
            })
        {
            return Err(RecoveryInputError::InvalidBatchLimit);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AdvanceLegacyScanOutcome {
    Advanced(LegacyScanCheckpoint),
    Duplicate(LegacyScanCheckpoint),
    Fenced(LegacyScanCheckpoint),
    SourceChanged(LegacyScanCheckpoint),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DegradationPublicationStatus {
    Pending,
    Leased,
    Materialized,
    Poisoned,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct DegradationPublicationLease {
    pub degradation_id: DegradationId,
    pub root_thread_id: ThreadId,
    pub after_revision: u64,
    pub source_ordinal: u64,
    pub stable_record_id: DegradationId,
    pub version: u64,
    pub lease_epoch: u64,
    pub lease_expires_at_ms: i64,
}

impl std::fmt::Debug for DegradationPublicationLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DegradationPublicationLease")
            .field("token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClaimDegradationPublicationsOutcome {
    Claimed(Vec<DegradationPublicationLease>),
    Deferred,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClaimDegradationPublications {
    pub root_thread_id: ThreadId,
    pub expected_state_epoch: StateEpoch,
    pub now_ms: i64,
    pub lease_expires_at_ms: i64,
    pub limit: u32,
}

impl ClaimDegradationPublications {
    pub(crate) fn validate(&self) -> Result<(), RecoveryInputError> {
        if self.limit == 0 || self.limit > MAX_RECOVERY_BATCH {
            return Err(RecoveryInputError::InvalidBatchLimit);
        }
        if self.now_ms < 0 || self.lease_expires_at_ms <= self.now_ms {
            return Err(RecoveryInputError::InvalidLeaseDeadline);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DegradationPublicationResolution {
    Materialized,
    Retry { retry_after_ms: i64 },
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResolveDegradationPublicationOutcome {
    Applied(DegradationPublicationStatus),
    Fenced,
    Terminal(DegradationPublicationStatus),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolveDegradationPublication {
    pub lease: DegradationPublicationLease,
    pub expected_state_epoch: StateEpoch,
    pub resolution: DegradationPublicationResolution,
    pub now_ms: i64,
}

impl ResolveDegradationPublication {
    pub(crate) fn validate(&self) -> Result<(), RecoveryInputError> {
        if self.now_ms < 0 {
            return Err(RecoveryInputError::InvalidTimestamp);
        }
        Ok(())
    }
}
