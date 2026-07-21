use codex_coordination::CoordinationEventId;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqlitePool;

use super::recovery::NoRecoveryFailure;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_recovery_state::ClaimProjectionPublications;
use crate::model::coordination_recovery_state::ClaimProjectionPublicationsOutcome;
use crate::model::coordination_recovery_state::ProjectionPublicationLease;
use crate::model::coordination_recovery_state::ProjectionPublicationResolution;
use crate::model::coordination_recovery_state::ProjectionPublicationStatus;
use crate::model::coordination_recovery_state::ResolveProjectionPublication;
use crate::model::coordination_recovery_state::ResolveProjectionPublicationOutcome;

/// Claim a root's next native revision (`published_revision + 1`) for
/// publication, leasing the single eligible `coordination_projection_outbox`
/// row. Independent roots progress independently; a given root never has two
/// revisions eligible at once, because only the immediate successor of its
/// published watermark is claimable and `(root, revision)` is unique.
pub(crate) async fn claim_projection_publications(
    pool: &SqlitePool,
    params: &ClaimProjectionPublications,
) -> Result<ClaimProjectionPublicationsOutcome, RecoveryWriteError> {
    claim_projection_publications_with(pool, params, &NoRecoveryFailure).await
}

pub(super) async fn claim_projection_publications_with(
    pool: &SqlitePool,
    params: &ClaimProjectionPublications,
    injector: &dyn RecoveryFailureInjector,
) -> Result<ClaimProjectionPublicationsOutcome, RecoveryWriteError> {
    params.validate()?;
    let mut connection = recovery_guard::begin_with(pool, injector).await?;
    let result = async {
        recovery_guard::active_authority_with(
            &mut connection,
            &params.root_thread_id,
            Some(params.expected_state_epoch),
            injector,
        )
        .await?;
        let rows = sqlx::query(
            "SELECT o.event_id AS event_id,o.version AS version,o.lease_epoch AS lease_epoch,\
             e.revision AS revision,e.root_thread_id AS root_thread_id \
             FROM coordination_projection_outbox o \
             JOIN coordination_events e ON e.event_id=o.event_id \
             JOIN coordination_roots r ON r.root_thread_id=e.root_thread_id \
             WHERE e.root_thread_id=? AND e.revision=r.published_revision+1 AND (\
               (o.status='pending' AND o.retry_after_ms<=?) OR\
               (o.status='leased' AND o.lease_expires_at_ms<=?)\
             ) AND o.updated_at_ms<=? \
             ORDER BY e.revision LIMIT ?",
        )
        .bind(params.root_thread_id.to_string())
        .bind(params.now_ms)
        .bind(params.now_ms)
        .bind(params.now_ms)
        .bind(params.limit as i64)
        .fetch_all(&mut *connection)
        .await
        .map_err(internal)?;
        injector
            .after_recovery_step(RecoveryStep::PublicationRead)
            .map_err(RecoveryWriteError::Internal)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let event_id = event_id(&row.get::<String, _>("event_id"))?;
            let root_thread_id = thread(&row.get::<String, _>("root_thread_id"))?;
            let revision = unsigned(row.get("revision"))?;
            let old_version = unsigned(row.get("version"))?;
            let old_lease_epoch = unsigned(row.get("lease_epoch"))?;
            let updated = sqlx::query(
                "UPDATE coordination_projection_outbox SET status='leased',\
                 version=version+1,lease_epoch=lease_epoch+1,lease_expires_at_ms=?,\
                 last_error=NULL,updated_at_ms=MAX(updated_at_ms,?) \
                 WHERE event_id=? AND version=? AND lease_epoch=? AND (\
                   (status='pending' AND retry_after_ms<=?) OR\
                   (status='leased' AND lease_expires_at_ms<=?)\
                 )",
            )
            .bind(params.lease_expires_at_ms)
            .bind(params.now_ms)
            .bind(event_id.to_string())
            .bind(old_version as i64)
            .bind(old_lease_epoch as i64)
            .bind(params.now_ms)
            .bind(params.now_ms)
            .execute(&mut *connection)
            .await
            .map_err(internal)?;
            if updated.rows_affected() != 1 {
                return Err(RecoveryWriteError::Deferred);
            }
            injector
                .after_recovery_step(RecoveryStep::PublicationUpdate)
                .map_err(RecoveryWriteError::Internal)?;
            claimed.push(ProjectionPublicationLease {
                event_id,
                root_thread_id,
                revision,
                version: old_version + 1,
                lease_epoch: old_lease_epoch + 1,
                lease_expires_at_ms: params.lease_expires_at_ms,
            });
        }
        Ok(ClaimProjectionPublicationsOutcome::Claimed(claimed))
    }
    .await;
    match recovery_guard::finish_with(connection, result, injector).await {
        Err(RecoveryWriteError::Deferred) => Ok(ClaimProjectionPublicationsOutcome::Deferred),
        result => result,
    }
}

/// Resolve a leased native projection publication. A successful materialization
/// atomically advances the root's `published_revision` from `revision - 1` to
/// `revision` — the native watermark advance that the degradation outbox never
/// performs. Retry follows the same eight-scheduled-retries-then-poison rule as
/// the degradation outbox: `retry_count < 8` is retryable, and the ninth failure
/// (observed when `retry_count` is already 8) poisons the root's revision.
/// Poison is terminal in the same transaction that detects it; because
/// `published_revision` never advances past a poisoned revision, all subsequent
/// mutation on that root is blocked by construction of the R+1 claim.
pub(crate) async fn resolve_projection_publication(
    pool: &SqlitePool,
    params: &ResolveProjectionPublication,
) -> Result<ResolveProjectionPublicationOutcome, RecoveryWriteError> {
    resolve_projection_publication_with(pool, params, &NoRecoveryFailure).await
}

pub(super) async fn resolve_projection_publication_with(
    pool: &SqlitePool,
    params: &ResolveProjectionPublication,
    injector: &dyn RecoveryFailureInjector,
) -> Result<ResolveProjectionPublicationOutcome, RecoveryWriteError> {
    params.validate()?;
    let mut connection = recovery_guard::begin_with(pool, injector).await?;
    let result = async {
        recovery_guard::active_authority_with(
            &mut connection,
            &params.lease.root_thread_id,
            Some(params.expected_state_epoch),
            injector,
        )
        .await?;
        let row = sqlx::query(
            "SELECT e.root_thread_id AS root_thread_id,e.revision AS revision,o.status AS status,\
             o.version AS version,o.lease_epoch AS lease_epoch,\
             o.lease_expires_at_ms AS lease_expires_at_ms,o.retry_count AS retry_count,\
             o.retry_after_ms AS retry_after_ms,o.updated_at_ms AS updated_at_ms,\
             r.published_revision AS published_revision,r.committed_revision AS committed_revision \
             FROM coordination_projection_outbox o \
             JOIN coordination_events e ON e.event_id=o.event_id \
             JOIN coordination_roots r ON r.root_thread_id=e.root_thread_id \
             WHERE o.event_id=?",
        )
        .bind(params.lease.event_id.to_string())
        .fetch_optional(&mut *connection)
        .await
        .map_err(internal)?
        .ok_or(RecoveryWriteError::CorruptState)?;
        injector
            .after_recovery_step(RecoveryStep::PublicationRead)
            .map_err(RecoveryWriteError::Internal)?;
        let current_status = status(&row.get::<String, _>("status"))?;
        if thread(&row.get::<String, _>("root_thread_id"))? != params.lease.root_thread_id
            || unsigned(row.get("revision"))? != params.lease.revision
        {
            return Ok(ResolveProjectionPublicationOutcome::Fenced);
        }
        if matches!(
            current_status,
            ProjectionPublicationStatus::Materialized | ProjectionPublicationStatus::Poisoned
        ) {
            return Ok(ResolveProjectionPublicationOutcome::Terminal(
                current_status,
            ));
        }
        if current_status != ProjectionPublicationStatus::Leased
            || unsigned(row.get("version"))? != params.lease.version
            || unsigned(row.get("lease_epoch"))? != params.lease.lease_epoch
            || row.get::<Option<i64>, _>("lease_expires_at_ms")
                != Some(params.lease.lease_expires_at_ms)
            || params.now_ms >= params.lease.lease_expires_at_ms
            || params.now_ms < row.get::<i64, _>("updated_at_ms")
        {
            return Ok(ResolveProjectionPublicationOutcome::Fenced);
        }
        let retry_count = unsigned(row.get("retry_count"))?;
        let old_retry_after_ms: i64 = row.get("retry_after_ms");
        let (next_status, retry_after_ms, next_retry_count, last_error) = match params.resolution {
            ProjectionPublicationResolution::Materialized => {
                ("materialized", old_retry_after_ms, retry_count, None)
            }
            ProjectionPublicationResolution::Retry { retry_after_ms }
                if retry_count < 8
                    && retry_after_ms > params.now_ms
                    && retry_after_ms >= old_retry_after_ms =>
            {
                (
                    "pending",
                    retry_after_ms,
                    retry_count + 1,
                    Some("stateUnavailable"),
                )
            }
            ProjectionPublicationResolution::Retry { .. } if retry_count >= 8 => (
                "poisoned",
                old_retry_after_ms,
                retry_count,
                Some("retryExhausted"),
            ),
            ProjectionPublicationResolution::Poisoned => (
                "poisoned",
                old_retry_after_ms,
                retry_count,
                Some("internal"),
            ),
            ProjectionPublicationResolution::Retry { .. } => {
                return Err(
                    crate::model::coordination_recovery::RecoveryInputError::InvalidRetryDeadline
                        .into(),
                );
            }
        };
        let updated = sqlx::query(
            "UPDATE coordination_projection_outbox SET status=?,version=version+1,\
             retry_count=?,retry_after_ms=?,lease_expires_at_ms=NULL,last_error=?,\
             updated_at_ms=MAX(updated_at_ms,?) WHERE event_id=? AND status='leased'\
             AND version=? AND lease_epoch=? AND lease_expires_at_ms=?",
        )
        .bind(next_status)
        .bind(next_retry_count as i64)
        .bind(retry_after_ms)
        .bind(last_error)
        .bind(params.now_ms)
        .bind(params.lease.event_id.to_string())
        .bind(params.lease.version as i64)
        .bind(params.lease.lease_epoch as i64)
        .bind(params.lease.lease_expires_at_ms)
        .execute(&mut *connection)
        .await
        .map_err(internal)?;
        if updated.rows_affected() != 1 {
            return Ok(ResolveProjectionPublicationOutcome::Fenced);
        }
        injector
            .after_recovery_step(RecoveryStep::PublicationUpdate)
            .map_err(RecoveryWriteError::Internal)?;
        if matches!(
            params.resolution,
            ProjectionPublicationResolution::Materialized
        ) {
            let revision = params.lease.revision;
            let previous = i64::try_from(revision.saturating_sub(1))
                .map_err(|_| RecoveryWriteError::CorruptState)?;
            let current = i64::try_from(revision).map_err(|_| RecoveryWriteError::CorruptState)?;
            let advanced = sqlx::query(
                "UPDATE coordination_roots SET published_revision=?,\
                 updated_at_ms=MAX(updated_at_ms,?) \
                 WHERE root_thread_id=? AND published_revision=? AND committed_revision>=?",
            )
            .bind(current)
            .bind(params.now_ms)
            .bind(params.lease.root_thread_id.to_string())
            .bind(previous)
            .bind(current)
            .execute(&mut *connection)
            .await
            .map_err(internal)?;
            if advanced.rows_affected() != 1 {
                // We hold the lease on this revision, so the watermark could only
                // fail to be at `revision - 1` if durable state was corrupted.
                return Err(RecoveryWriteError::CorruptState);
            }
            injector
                .after_recovery_step(RecoveryStep::PublicationUpdate)
                .map_err(RecoveryWriteError::Internal)?;
        }
        Ok(ResolveProjectionPublicationOutcome::Applied(status(
            next_status,
        )?))
    }
    .await;
    recovery_guard::finish_with(connection, result, injector).await
}

fn status(value: &str) -> Result<ProjectionPublicationStatus, RecoveryWriteError> {
    match value {
        "pending" => Ok(ProjectionPublicationStatus::Pending),
        "leased" => Ok(ProjectionPublicationStatus::Leased),
        "materialized" => Ok(ProjectionPublicationStatus::Materialized),
        "poisoned" => Ok(ProjectionPublicationStatus::Poisoned),
        _ => Err(RecoveryWriteError::CorruptState),
    }
}

fn event_id(value: &str) -> Result<CoordinationEventId, RecoveryWriteError> {
    CoordinationEventId::parse(value).map_err(|_| RecoveryWriteError::CorruptState)
}

fn thread(value: &str) -> Result<ThreadId, RecoveryWriteError> {
    ThreadId::try_from(value).map_err(|_| RecoveryWriteError::CorruptState)
}

fn unsigned(value: i64) -> Result<u64, RecoveryWriteError> {
    value
        .try_into()
        .map_err(|_| RecoveryWriteError::CorruptState)
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
