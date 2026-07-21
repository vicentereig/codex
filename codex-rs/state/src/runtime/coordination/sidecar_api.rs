//! Crate-external facade for the Stage 3 coordination sidecar dispatcher
//! (`codex-9u5.2.3.2`, `rollout/src/coordination_sidecar/`).
//!
//! Every other item in `runtime::coordination` stays `pub(crate)`: the native
//! `(published_revision + 1)` claim/ack state machine
//! ([`super::projection_outbox`]) and the degradation claim/ack state machine
//! ([`super::degradation_outbox`]) were both frozen by `codex-9u5.2.3.1` as
//! crate-internal. The rollout crate's sidecar writer/dispatcher needs to
//! drive those exact state machines and to read/persist the per-root sidecar
//! path (Decision 2 of the 2026-07-21 Stage 3 contract freeze), so this
//! module is the one deliberate widening: a narrow, typed, crate-boundary
//! surface built only for that consumer.
//!
//! This is not a production API. Nothing calls it outside test-driven
//! invocation from `codex-rollout`'s dispatcher (no worker/task registers
//! it, no capability flag exposes it); it exists purely so `.2.3.2` can be
//! implemented and tested without collapsing the `coordination` module
//! boundary entirely.

use codex_coordination::CoordinationEventId;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use sqlx::Row;

use super::degradation_outbox;
use super::projection_outbox;
use super::recovery::RecoveryWriteError;
use crate::StateRuntime;
use crate::model::coordination_recovery::DegradationId;
use crate::model::coordination_recovery_state::ClaimDegradationPublications;
use crate::model::coordination_recovery_state::ClaimDegradationPublicationsOutcome;
use crate::model::coordination_recovery_state::ClaimProjectionPublications;
use crate::model::coordination_recovery_state::ClaimProjectionPublicationsOutcome;
use crate::model::coordination_recovery_state::DegradationPublicationLease;
use crate::model::coordination_recovery_state::DegradationPublicationResolution;
use crate::model::coordination_recovery_state::DegradationPublicationStatus;
use crate::model::coordination_recovery_state::ProjectionPublicationLease;
use crate::model::coordination_recovery_state::ProjectionPublicationResolution;
use crate::model::coordination_recovery_state::ProjectionPublicationStatus;
use crate::model::coordination_recovery_state::ResolveDegradationPublication;
use crate::model::coordination_recovery_state::ResolveDegradationPublicationOutcome;
use crate::model::coordination_recovery_state::ResolveProjectionPublication;
use crate::model::coordination_recovery_state::ResolveProjectionPublicationOutcome;

/// Public error surface for sidecar-facing coordination calls. Deliberately
/// narrower than [`RecoveryWriteError`]: several of that error's variants
/// (`IdentityCollision`, `DivergentReduction`, ...) belong to aggregate/inbox
/// recovery and cannot occur through this facade's calls, so they collapse
/// into [`SidecarStateError::Internal`] rather than being exposed one-for-one.
#[derive(Debug, thiserror::Error)]
pub enum SidecarStateError {
    #[error("coordination authority is quarantined")]
    Quarantined,
    #[error("coordination authority epoch or root does not match")]
    EpochMismatch,
    #[error("stored coordination recovery evidence is corrupt")]
    CorruptState,
    #[error("recovery work is temporarily unavailable and retains its identity")]
    Deferred,
    #[error("sidecar path must be nonempty and at most 1024 bytes")]
    InvalidSidecarPath,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<RecoveryWriteError> for SidecarStateError {
    fn from(error: RecoveryWriteError) -> Self {
        match error {
            RecoveryWriteError::Quarantined => Self::Quarantined,
            RecoveryWriteError::EpochMismatch => Self::EpochMismatch,
            RecoveryWriteError::CorruptState => Self::CorruptState,
            RecoveryWriteError::Deferred => Self::Deferred,
            RecoveryWriteError::Internal(err) => Self::Internal(err),
            other => Self::Internal(anyhow::anyhow!(other.to_string())),
        }
    }
}

fn internal(error: impl Into<anyhow::Error>) -> SidecarStateError {
    SidecarStateError::Internal(error.into())
}

/// Mirrors [`ProjectionPublicationStatus`]/[`DegradationPublicationStatus`]
/// (identical shapes) as a single crate-external status type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SidecarPublicationStatus {
    Pending,
    Leased,
    Materialized,
    Poisoned,
}

impl From<ProjectionPublicationStatus> for SidecarPublicationStatus {
    fn from(status: ProjectionPublicationStatus) -> Self {
        match status {
            ProjectionPublicationStatus::Pending => Self::Pending,
            ProjectionPublicationStatus::Leased => Self::Leased,
            ProjectionPublicationStatus::Materialized => Self::Materialized,
            ProjectionPublicationStatus::Poisoned => Self::Poisoned,
        }
    }
}

impl From<DegradationPublicationStatus> for SidecarPublicationStatus {
    fn from(status: DegradationPublicationStatus) -> Self {
        match status {
            DegradationPublicationStatus::Pending => Self::Pending,
            DegradationPublicationStatus::Leased => Self::Leased,
            DegradationPublicationStatus::Materialized => Self::Materialized,
            DegradationPublicationStatus::Poisoned => Self::Poisoned,
        }
    }
}

/// A leased native (`published_revision + 1`) projection publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SidecarProjectionLease {
    pub event_id: CoordinationEventId,
    pub root_thread_id: ThreadId,
    pub revision: u64,
    pub version: u64,
    pub lease_epoch: u64,
    pub lease_expires_at_ms: i64,
}

impl From<ProjectionPublicationLease> for SidecarProjectionLease {
    fn from(lease: ProjectionPublicationLease) -> Self {
        Self {
            event_id: lease.event_id,
            root_thread_id: lease.root_thread_id,
            revision: lease.revision,
            version: lease.version,
            lease_epoch: lease.lease_epoch,
            lease_expires_at_ms: lease.lease_expires_at_ms,
        }
    }
}

impl From<SidecarProjectionLease> for ProjectionPublicationLease {
    fn from(lease: SidecarProjectionLease) -> Self {
        Self {
            event_id: lease.event_id,
            root_thread_id: lease.root_thread_id,
            revision: lease.revision,
            version: lease.version,
            lease_epoch: lease.lease_epoch,
            lease_expires_at_ms: lease.lease_expires_at_ms,
        }
    }
}

/// A leased degradation/compatibility publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SidecarDegradationLease {
    pub degradation_id: String,
    pub root_thread_id: ThreadId,
    pub after_revision: u64,
    pub source_ordinal: u64,
    pub stable_record_id: String,
    pub version: u64,
    pub lease_epoch: u64,
    pub lease_expires_at_ms: i64,
}

impl From<DegradationPublicationLease> for SidecarDegradationLease {
    fn from(lease: DegradationPublicationLease) -> Self {
        Self {
            degradation_id: lease.degradation_id.to_string(),
            root_thread_id: lease.root_thread_id,
            after_revision: lease.after_revision,
            source_ordinal: lease.source_ordinal,
            stable_record_id: lease.stable_record_id.to_string(),
            version: lease.version,
            lease_epoch: lease.lease_epoch,
            lease_expires_at_ms: lease.lease_expires_at_ms,
        }
    }
}

impl TryFrom<SidecarDegradationLease> for DegradationPublicationLease {
    type Error = SidecarStateError;

    fn try_from(lease: SidecarDegradationLease) -> Result<Self, Self::Error> {
        Ok(Self {
            degradation_id: DegradationId::parse(&lease.degradation_id)
                .map_err(|_| SidecarStateError::CorruptState)?,
            root_thread_id: lease.root_thread_id,
            after_revision: lease.after_revision,
            source_ordinal: lease.source_ordinal,
            stable_record_id: DegradationId::parse(&lease.stable_record_id)
                .map_err(|_| SidecarStateError::CorruptState)?,
            version: lease.version,
            lease_epoch: lease.lease_epoch,
            lease_expires_at_ms: lease.lease_expires_at_ms,
        })
    }
}

/// How the dispatcher wants to resolve a leased publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SidecarResolution {
    Materialized,
    Retry { retry_after_ms: i64 },
    Poisoned,
}

impl From<SidecarResolution> for ProjectionPublicationResolution {
    fn from(resolution: SidecarResolution) -> Self {
        match resolution {
            SidecarResolution::Materialized => Self::Materialized,
            SidecarResolution::Retry { retry_after_ms } => Self::Retry { retry_after_ms },
            SidecarResolution::Poisoned => Self::Poisoned,
        }
    }
}

impl From<SidecarResolution> for DegradationPublicationResolution {
    fn from(resolution: SidecarResolution) -> Self {
        match resolution {
            SidecarResolution::Materialized => Self::Materialized,
            SidecarResolution::Retry { retry_after_ms } => Self::Retry { retry_after_ms },
            SidecarResolution::Poisoned => Self::Poisoned,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SidecarClaimOutcome<T> {
    Claimed(Vec<T>),
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SidecarResolveOutcome {
    Applied(SidecarPublicationStatus),
    Fenced,
    Terminal(SidecarPublicationStatus),
}

impl From<ResolveProjectionPublicationOutcome> for SidecarResolveOutcome {
    fn from(outcome: ResolveProjectionPublicationOutcome) -> Self {
        match outcome {
            ResolveProjectionPublicationOutcome::Applied(status) => Self::Applied(status.into()),
            ResolveProjectionPublicationOutcome::Fenced => Self::Fenced,
            ResolveProjectionPublicationOutcome::Terminal(status) => Self::Terminal(status.into()),
        }
    }
}

impl From<ResolveDegradationPublicationOutcome> for SidecarResolveOutcome {
    fn from(outcome: ResolveDegradationPublicationOutcome) -> Self {
        match outcome {
            ResolveDegradationPublicationOutcome::Applied(status) => Self::Applied(status.into()),
            ResolveDegradationPublicationOutcome::Fenced => Self::Fenced,
            ResolveDegradationPublicationOutcome::Terminal(status) => Self::Terminal(status.into()),
        }
    }
}

/// Read the active `coordination_authority` state epoch, failing closed if
/// quarantined or absent. Always a fresh read (never cached), because the
/// authority can be quarantined by an unrelated writer between dispatch
/// ticks.
pub async fn active_state_epoch(runtime: &StateRuntime) -> Result<StateEpoch, SidecarStateError> {
    let row =
        sqlx::query("SELECT state_epoch,status FROM coordination_authority WHERE singleton_id=1")
            .fetch_optional(&*runtime.pool)
            .await
            .map_err(internal)?
            .ok_or(SidecarStateError::EpochMismatch)?;
    if row.get::<String, _>("status") != "active" {
        return Err(SidecarStateError::Quarantined);
    }
    StateEpoch::parse(&row.get::<String, _>("state_epoch"))
        .map_err(|_| SidecarStateError::CorruptState)
}

/// Read the persisted per-root sidecar path (Decision 2). `Ok(None)` means
/// the root exists but no sidecar path has been computed/persisted yet;
/// callers must treat that as "compute and persist once", never as
/// "recompute on every access".
pub async fn root_sidecar_path(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
) -> Result<Option<String>, SidecarStateError> {
    let row = sqlx::query("SELECT sidecar_path FROM coordination_roots WHERE root_thread_id=?")
        .bind(root_thread_id.to_string())
        .fetch_optional(&*runtime.pool)
        .await
        .map_err(internal)?
        .ok_or(SidecarStateError::EpochMismatch)?;
    Ok(row.get::<Option<String>, _>("sidecar_path"))
}

/// Persist `candidate_path` as the root's sidecar path if and only if none
/// is persisted yet (first writer wins), then return whichever path is
/// canonical — the candidate if this call won, or the already-persisted
/// value if another writer won the race first. Callers must use the
/// returned value, not their own candidate, from this point on.
pub async fn persist_root_sidecar_path(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    expected_state_epoch: StateEpoch,
    candidate_path: &str,
) -> Result<String, SidecarStateError> {
    if candidate_path.is_empty() || candidate_path.len() > 1024 {
        return Err(SidecarStateError::InvalidSidecarPath);
    }
    let now_ms = chrono::Utc::now().timestamp_millis().max(0);
    let updated = sqlx::query(
        "UPDATE coordination_roots SET sidecar_path=?,updated_at_ms=MAX(updated_at_ms,?) \
         WHERE root_thread_id=? AND state_epoch=? AND sidecar_path IS NULL",
    )
    .bind(candidate_path)
    .bind(now_ms)
    .bind(root_thread_id.to_string())
    .bind(expected_state_epoch.to_string())
    .execute(&*runtime.pool)
    .await
    .map_err(internal)?;
    if updated.rows_affected() == 1 {
        return Ok(candidate_path.to_string());
    }
    let row = sqlx::query(
        "SELECT sidecar_path FROM coordination_roots WHERE root_thread_id=? AND state_epoch=?",
    )
    .bind(root_thread_id.to_string())
    .bind(expected_state_epoch.to_string())
    .fetch_optional(&*runtime.pool)
    .await
    .map_err(internal)?;
    match row.and_then(|row| row.get::<Option<String>, _>("sidecar_path")) {
        Some(path) => Ok(path),
        None => Err(SidecarStateError::EpochMismatch),
    }
}

/// Claim a root's next native revision for publication. See
/// [`projection_outbox::claim_projection_publications`].
pub async fn claim_native_publications(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    expected_state_epoch: StateEpoch,
    now_ms: i64,
    lease_expires_at_ms: i64,
    limit: u32,
) -> Result<SidecarClaimOutcome<SidecarProjectionLease>, SidecarStateError> {
    let params = ClaimProjectionPublications {
        root_thread_id,
        expected_state_epoch,
        now_ms,
        lease_expires_at_ms,
        limit,
    };
    let outcome = projection_outbox::claim_projection_publications(&runtime.pool, &params).await?;
    Ok(match outcome {
        ClaimProjectionPublicationsOutcome::Claimed(leases) => {
            SidecarClaimOutcome::Claimed(leases.into_iter().map(Into::into).collect())
        }
        ClaimProjectionPublicationsOutcome::Deferred => SidecarClaimOutcome::Deferred,
    })
}

/// Resolve a leased native projection publication. See
/// [`projection_outbox::resolve_projection_publication`].
pub async fn resolve_native_publication(
    runtime: &StateRuntime,
    lease: SidecarProjectionLease,
    expected_state_epoch: StateEpoch,
    resolution: SidecarResolution,
    now_ms: i64,
) -> Result<SidecarResolveOutcome, SidecarStateError> {
    let params = ResolveProjectionPublication {
        lease: lease.into(),
        expected_state_epoch,
        resolution: resolution.into(),
        now_ms,
    };
    let outcome = projection_outbox::resolve_projection_publication(&runtime.pool, &params).await?;
    Ok(outcome.into())
}

/// Claim a root's eligible degradation/compatibility publications. See
/// [`degradation_outbox::claim_degradation_publications`].
pub async fn claim_degradation_publications(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    expected_state_epoch: StateEpoch,
    now_ms: i64,
    lease_expires_at_ms: i64,
    limit: u32,
) -> Result<SidecarClaimOutcome<SidecarDegradationLease>, SidecarStateError> {
    let params = ClaimDegradationPublications {
        root_thread_id,
        expected_state_epoch,
        now_ms,
        lease_expires_at_ms,
        limit,
    };
    let outcome =
        degradation_outbox::claim_degradation_publications(&runtime.pool, &params).await?;
    Ok(match outcome {
        ClaimDegradationPublicationsOutcome::Claimed(leases) => {
            SidecarClaimOutcome::Claimed(leases.into_iter().map(Into::into).collect())
        }
        ClaimDegradationPublicationsOutcome::Deferred => SidecarClaimOutcome::Deferred,
    })
}

/// Resolve a leased degradation/compatibility publication. See
/// [`degradation_outbox::resolve_degradation_publication`].
pub async fn resolve_degradation_publication(
    runtime: &StateRuntime,
    lease: SidecarDegradationLease,
    expected_state_epoch: StateEpoch,
    resolution: SidecarResolution,
    now_ms: i64,
) -> Result<SidecarResolveOutcome, SidecarStateError> {
    let params = ResolveDegradationPublication {
        lease: lease.try_into()?,
        expected_state_epoch,
        resolution: resolution.into(),
        now_ms,
    };
    let outcome =
        degradation_outbox::resolve_degradation_publication(&runtime.pool, &params).await?;
    Ok(outcome.into())
}
