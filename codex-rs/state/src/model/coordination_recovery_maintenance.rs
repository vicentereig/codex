use codex_coordination::BoundedId;
use codex_coordination::CompatibilityOrdinal;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use super::coordination_recovery::CheckedBytes;
use super::coordination_recovery::DegradationId;
use super::coordination_recovery::DegradationReason;
use super::coordination_recovery::RecoveryInputError;
use super::coordination_recovery::deterministic_degradation_id;
use super::coordination_recovery::semantic_slot_sql;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum RecoveryRecordKind {
    Assignment,
    Command,
    Inbox,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckedMaintenanceDegradation {
    pub degradation_id: DegradationId,
    pub root_thread_id: ThreadId,
    pub state_epoch: StateEpoch,
    pub record_kind: RecoveryRecordKind,
    pub record_id: BoundedId<MAX_ID_BYTES>,
    pub semantic_slot: CoordinationSemanticSlot,
    pub reason: DegradationReason,
    pub identity_bytes: CheckedBytes<1024>,
    pub canonical_record_bytes: CheckedBytes<4096>,
    pub observed_at: i64,
    pub after_revision: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MaintenanceIdentity<'a> {
    version: u16,
    root_thread_id: ThreadId,
    state_epoch: StateEpoch,
    source_kind: &'static str,
    record_kind: RecoveryRecordKind,
    record_id: &'a BoundedId<MAX_ID_BYTES>,
    #[serde(serialize_with = "serialize_slot")]
    semantic_slot: CoordinationSemanticSlot,
    reason: DegradationReason,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalMaintenanceRecord<'a> {
    identity: &'a MaintenanceIdentity<'a>,
    observed_at: i64,
    after_revision: u64,
}

impl CheckedMaintenanceDegradation {
    pub(crate) fn new(
        root_thread_id: ThreadId,
        state_epoch: StateEpoch,
        record_kind: RecoveryRecordKind,
        record_id: BoundedId<MAX_ID_BYTES>,
        semantic_slot: CoordinationSemanticSlot,
        reason: DegradationReason,
        observed_at: i64,
        after_revision: u64,
    ) -> Result<Self, RecoveryInputError> {
        if observed_at < 0 {
            return Err(RecoveryInputError::InvalidTimestamp);
        }
        CompatibilityOrdinal::new(after_revision)
            .map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?;
        let identity = MaintenanceIdentity {
            version: 1,
            root_thread_id,
            state_epoch,
            source_kind: "recovery",
            record_kind,
            record_id: &record_id,
            semantic_slot,
            reason,
        };
        let identity_bytes = CheckedBytes::new(
            serde_json::to_vec(&identity).map_err(|_| RecoveryInputError::InvalidCanonicalBytes)?,
        )?;
        let degradation_id = deterministic_degradation_id(identity_bytes.as_slice())?;
        let canonical_record_bytes = CheckedBytes::new(
            serde_json::to_vec(&CanonicalMaintenanceRecord {
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
            record_kind,
            record_id,
            semantic_slot,
            reason,
            identity_bytes,
            canonical_record_bytes,
            observed_at,
            after_revision,
        })
    }
}

fn serialize_slot<S>(slot: &CoordinationSemanticSlot, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(semantic_slot_sql(*slot))
}

pub(crate) fn maintenance_fingerprint(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
