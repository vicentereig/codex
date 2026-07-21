//! Canonical, bounded JSONL record shape for the coordination sidecar file.
//!
//! # Append-order contract (Decision 4, Stage 3 freeze, 2026-07-21)
//!
//! Physical order in this file is **materialization/dispatch order only**
//! for [`SidecarRecord::Degradation`] records — it is explicitly *not*
//! guaranteed to equal the canonical `(afterRevision, sourceOrdinal,
//! stableRecordId)` order. A late-discovered degradation/compatibility
//! record (for example, one found by scanning an archived child rollout
//! that is only loaded much later) cannot be retroactively inserted before
//! an already-appended greater-keyed record in an append-only file. **Any
//! consumer of the degradation/compatibility portion of this file must
//! buffer and sort by `(after_revision, source_ordinal, stable_record_id)`
//! before treating the stream as causally ordered.** This is a first-class
//! contract, not an implementation detail — Stage 5's subscriber and any
//! offline tooling must apply the same sort before use.
//!
//! [`SidecarRecord::Native`] records need no such sort: physical append
//! order is strictly `published_revision + 1` order by construction of the
//! R+1 claim (`codex_state`'s `claim_native_publications`), so causal order
//! and append order are identical for that stream.
//!
//! # Metadata-only
//!
//! Every field below is coordination *metadata* (identities, revision
//! numbers, ordinals, timestamps) — never model/conversation content.
//! Coordination events carry structural facts (assignment, ownership,
//! interrupt, wait, terminal observation, ...); the ordinary encrypted
//! response item remains the sole carrier of model-visible content and is
//! never routed through this file. See the sentinel tests in
//! `dispatcher_tests.rs`.

use serde::Deserialize;
use serde::Serialize;

/// Serialized records are capped well below any reasonable JSONL line
/// length; every field here is a small identity/number, so this bound only
/// exists to fail closed on corruption or a future field misuse, not
/// because legitimate records approach it.
pub(crate) const MAX_RECORD_BYTES: usize = 4096;
const MAX_ID_LEN: usize = 128;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(crate) enum SidecarRecord {
    Native(NativeSidecarRecord),
    Degradation(DegradationSidecarRecord),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeSidecarRecord {
    pub event_id: String,
    pub root_thread_id: String,
    pub state_epoch: String,
    pub revision: u64,
    pub materialized_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DegradationSidecarRecord {
    pub degradation_id: String,
    pub root_thread_id: String,
    pub state_epoch: String,
    pub after_revision: u64,
    pub source_ordinal: u64,
    pub stable_record_id: String,
    pub materialized_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SidecarRecordError {
    #[error("sidecar record identity must be nonempty and at most {MAX_ID_LEN} bytes")]
    InvalidIdentity,
    #[error("sidecar record timestamp must not be negative")]
    InvalidTimestamp,
    #[error("sidecar record serializes over the {MAX_RECORD_BYTES}-byte bound")]
    TooLarge,
}

impl SidecarRecord {
    /// The stable identity this record dedupes on: `event_id` for native
    /// records, `degradation_id` for degradation records. Two records with
    /// the same identity but different byte-for-byte content are a
    /// divergent-identity condition and must never both appear in the file.
    pub(crate) fn identity(&self) -> &str {
        match self {
            Self::Native(record) => &record.event_id,
            Self::Degradation(record) => &record.degradation_id,
        }
    }

    /// Validate the bounded canonical shape *before* writing. Fails closed
    /// (returns an error, writes nothing) on any malformed identity, a
    /// negative timestamp, or a record too large for the bound above.
    pub(crate) fn validate(&self) -> Result<(), SidecarRecordError> {
        let (ids, materialized_at_ms): (Vec<&str>, i64) = match self {
            Self::Native(record) => (
                vec![
                    record.event_id.as_str(),
                    record.root_thread_id.as_str(),
                    record.state_epoch.as_str(),
                ],
                record.materialized_at_ms,
            ),
            Self::Degradation(record) => (
                vec![
                    record.degradation_id.as_str(),
                    record.root_thread_id.as_str(),
                    record.state_epoch.as_str(),
                    record.stable_record_id.as_str(),
                ],
                record.materialized_at_ms,
            ),
        };
        if ids.iter().any(|id| id.is_empty() || id.len() > MAX_ID_LEN) {
            return Err(SidecarRecordError::InvalidIdentity);
        }
        if materialized_at_ms < 0 {
            return Err(SidecarRecordError::InvalidTimestamp);
        }
        let encoded = serde_json::to_vec(self).map_err(|_| SidecarRecordError::TooLarge)?;
        if encoded.len() > MAX_RECORD_BYTES {
            return Err(SidecarRecordError::TooLarge);
        }
        Ok(())
    }

    /// Canonical single-line encoding (no trailing newline): the exact bytes
    /// physically written to the file for this record.
    pub(crate) fn canonical_line(&self) -> Result<Vec<u8>, SidecarRecordError> {
        serde_json::to_vec(self).map_err(|_| SidecarRecordError::TooLarge)
    }

    /// Encoding used for divergent-identity detection: every field except
    /// `materialized_at_ms`, which legitimately differs between a record's
    /// first successful append and a later retry of the exact same logical
    /// record (the retry recomputes "now" but refers to the same claimed
    /// revision/degradation). Two records sharing an identity must compare
    /// equal here to be treated as the same record; if they don't, that is
    /// a genuine divergent-identity condition and must quarantine, never
    /// silently overwrite or duplicate.
    pub(crate) fn structural_key(&self) -> Vec<u8> {
        let normalized = match self.clone() {
            Self::Native(record) => Self::Native(NativeSidecarRecord {
                materialized_at_ms: 0,
                ..record
            }),
            Self::Degradation(record) => Self::Degradation(DegradationSidecarRecord {
                materialized_at_ms: 0,
                ..record
            }),
        };
        serde_json::to_vec(&normalized).expect("sidecar record fields are always serializable")
    }
}
