use codex_coordination::CompatibilityOrdinal;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use serde::Serialize;

use super::coordination_recovery::CheckedBytes;
use super::coordination_recovery::DegradationId;
use super::coordination_recovery::DegradationReason;
use super::coordination_recovery::LegacySourceIdentity;
use super::coordination_recovery::RecoveryInputError;
use super::coordination_recovery::deterministic_degradation_id;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckedLegacyReductionDegradation {
    pub degradation_id: DegradationId,
    pub root_thread_id: ThreadId,
    pub state_epoch: StateEpoch,
    pub source: LegacySourceIdentity,
    pub reason: DegradationReason,
    pub identity_bytes: CheckedBytes<1024>,
    pub canonical_record_bytes: CheckedBytes<4096>,
    pub observed_at: i64,
    pub after_revision: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyDegradationIdentity<'a> {
    version: u16,
    root_thread_id: ThreadId,
    state_epoch: StateEpoch,
    source_kind: &'static str,
    source: &'a LegacySourceIdentity,
    reason: DegradationReason,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalLegacyDegradation<'a> {
    identity: &'a LegacyDegradationIdentity<'a>,
    observed_at: i64,
    after_revision: u64,
}

impl CheckedLegacyReductionDegradation {
    pub(crate) fn new(
        root_thread_id: ThreadId,
        state_epoch: StateEpoch,
        source: LegacySourceIdentity,
        reason: DegradationReason,
        observed_at: i64,
        after_revision: u64,
    ) -> Result<Self, RecoveryInputError> {
        if observed_at < 0 {
            return Err(RecoveryInputError::InvalidTimestamp);
        }
        CompatibilityOrdinal::new(after_revision)
            .map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?;
        if !matches!(
            reason,
            DegradationReason::AmbiguousSource
                | DegradationReason::OverLimit
                | DegradationReason::InvalidLegacyValue
                | DegradationReason::CorruptSource
                | DegradationReason::StateLossDegraded
        ) {
            return Err(RecoveryInputError::InvalidLegacyDegradationReason);
        }
        let identity = LegacyDegradationIdentity {
            version: 1,
            root_thread_id,
            state_epoch,
            source_kind: "legacyReduction",
            source: &source,
            reason,
        };
        let identity_bytes = CheckedBytes::new(
            serde_json::to_vec(&identity).map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?,
        )?;
        let degradation_id = deterministic_degradation_id(identity_bytes.as_slice())?;
        let canonical_record_bytes = CheckedBytes::new(
            serde_json::to_vec(&CanonicalLegacyDegradation {
                identity: &identity,
                observed_at,
                after_revision,
            })
            .map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?,
        )?;
        Ok(Self {
            degradation_id,
            root_thread_id,
            state_epoch,
            source,
            reason,
            identity_bytes,
            canonical_record_bytes,
            observed_at,
            after_revision,
        })
    }
}
