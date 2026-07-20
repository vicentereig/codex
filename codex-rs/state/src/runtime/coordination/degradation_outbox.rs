use sqlx::Row;
use sqlx::SqlitePool;

use super::recovery::NoRecoveryFailure;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_recovery::DegradationId;
use crate::model::coordination_recovery_state::ClaimDegradationPublications;
use crate::model::coordination_recovery_state::ClaimDegradationPublicationsOutcome;
use crate::model::coordination_recovery_state::DegradationPublicationLease;
use crate::model::coordination_recovery_state::DegradationPublicationResolution;
use crate::model::coordination_recovery_state::DegradationPublicationStatus;
use crate::model::coordination_recovery_state::ResolveDegradationPublication;
use crate::model::coordination_recovery_state::ResolveDegradationPublicationOutcome;

pub(crate) async fn claim_degradation_publications(
    pool: &SqlitePool,
    params: &ClaimDegradationPublications,
) -> Result<ClaimDegradationPublicationsOutcome, RecoveryWriteError> {
    claim_degradation_publications_with(pool, params, &NoRecoveryFailure).await
}

pub(super) async fn claim_degradation_publications_with(
    pool: &SqlitePool,
    params: &ClaimDegradationPublications,
    injector: &dyn RecoveryFailureInjector,
) -> Result<ClaimDegradationPublicationsOutcome, RecoveryWriteError> {
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
            "SELECT o.* FROM coordination_degradation_publication_outbox o \
             JOIN coordination_roots r ON r.root_thread_id=o.root_thread_id \
             WHERE o.root_thread_id=? AND o.after_revision<=r.published_revision AND (\
               (o.status='pending' AND o.retry_after_ms<=?) OR\
               (o.status='leased' AND o.lease_expires_at_ms<=?)\
             ) AND o.updated_at_ms<=? \
             ORDER BY o.after_revision,o.source_ordinal,o.stable_record_id LIMIT ?",
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
            let degradation_id = DegradationId::parse(&row.get::<String, _>("degradation_id"))?;
            let old_version = unsigned(row.get("version"))?;
            let old_lease_epoch = unsigned(row.get("lease_epoch"))?;
            let updated = sqlx::query(
                "UPDATE coordination_degradation_publication_outbox SET status='leased',\
                 version=version+1,lease_epoch=lease_epoch+1,lease_expires_at_ms=?,\
                 failure_code=NULL,updated_at_ms=MAX(updated_at_ms,?) \
                 WHERE degradation_id=? AND version=? AND lease_epoch=? AND (\
                   (status='pending' AND retry_after_ms<=?) OR\
                   (status='leased' AND lease_expires_at_ms<=?)\
                 )",
            )
            .bind(params.lease_expires_at_ms)
            .bind(params.now_ms)
            .bind(degradation_id.to_string())
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
            claimed.push(DegradationPublicationLease {
                degradation_id,
                root_thread_id: params.root_thread_id,
                after_revision: unsigned(row.get("after_revision"))?,
                source_ordinal: unsigned(row.get("source_ordinal"))?,
                stable_record_id: DegradationId::parse(&row.get::<String, _>("stable_record_id"))?,
                version: old_version + 1,
                lease_epoch: old_lease_epoch + 1,
                lease_expires_at_ms: params.lease_expires_at_ms,
            });
        }
        Ok(ClaimDegradationPublicationsOutcome::Claimed(claimed))
    }
    .await;
    match recovery_guard::finish_with(&mut connection, result, injector).await {
        Err(RecoveryWriteError::Deferred) => Ok(ClaimDegradationPublicationsOutcome::Deferred),
        result => result,
    }
}

pub(crate) async fn resolve_degradation_publication(
    pool: &SqlitePool,
    params: &ResolveDegradationPublication,
) -> Result<ResolveDegradationPublicationOutcome, RecoveryWriteError> {
    resolve_degradation_publication_with(pool, params, &NoRecoveryFailure).await
}

pub(super) async fn resolve_degradation_publication_with(
    pool: &SqlitePool,
    params: &ResolveDegradationPublication,
    injector: &dyn RecoveryFailureInjector,
) -> Result<ResolveDegradationPublicationOutcome, RecoveryWriteError> {
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
            "SELECT root_thread_id,after_revision,source_ordinal,stable_record_id,status,version,\
             lease_epoch,lease_expires_at_ms,retry_count,retry_after_ms,updated_at_ms FROM \
             coordination_degradation_publication_outbox WHERE degradation_id=?",
        )
        .bind(params.lease.degradation_id.to_string())
        .fetch_optional(&mut *connection)
        .await
        .map_err(internal)?
        .ok_or(RecoveryWriteError::CorruptState)?;
        injector
            .after_recovery_step(RecoveryStep::PublicationRead)
            .map_err(RecoveryWriteError::Internal)?;
        let current_status = status(&row.get::<String, _>("status"))?;
        if row.get::<String, _>("root_thread_id") != params.lease.root_thread_id.to_string()
            || unsigned(row.get("after_revision"))? != params.lease.after_revision
            || unsigned(row.get("source_ordinal"))? != params.lease.source_ordinal
            || DegradationId::parse(&row.get::<String, _>("stable_record_id"))?
                != params.lease.stable_record_id
        {
            return Ok(ResolveDegradationPublicationOutcome::Fenced);
        }
        if matches!(
            current_status,
            DegradationPublicationStatus::Materialized | DegradationPublicationStatus::Poisoned
        ) {
            return Ok(ResolveDegradationPublicationOutcome::Terminal(
                current_status,
            ));
        }
        if current_status != DegradationPublicationStatus::Leased
            || unsigned(row.get("version"))? != params.lease.version
            || unsigned(row.get("lease_epoch"))? != params.lease.lease_epoch
            || row.get::<Option<i64>, _>("lease_expires_at_ms")
                != Some(params.lease.lease_expires_at_ms)
            || params.now_ms >= params.lease.lease_expires_at_ms
            || params.now_ms < row.get::<i64, _>("updated_at_ms")
        {
            return Ok(ResolveDegradationPublicationOutcome::Fenced);
        }
        let retry_count = unsigned(row.get("retry_count"))?;
        let old_retry_after_ms: i64 = row.get("retry_after_ms");
        let (next_status, retry_after_ms, next_retry_count, failure_code) = match params.resolution
        {
            DegradationPublicationResolution::Materialized => {
                ("materialized", old_retry_after_ms, retry_count, None)
            }
            DegradationPublicationResolution::Retry { retry_after_ms }
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
            DegradationPublicationResolution::Retry { .. } if retry_count >= 8 => (
                "poisoned",
                old_retry_after_ms,
                retry_count,
                Some("retryExhausted"),
            ),
            DegradationPublicationResolution::Poisoned => (
                "poisoned",
                old_retry_after_ms,
                retry_count,
                Some("internal"),
            ),
            DegradationPublicationResolution::Retry { .. } => {
                return Err(
                    crate::model::coordination_recovery::RecoveryInputError::InvalidRetryDeadline
                        .into(),
                );
            }
        };
        let updated = sqlx::query(
            "UPDATE coordination_degradation_publication_outbox SET status=?,version=version+1,\
             retry_count=?,retry_after_ms=?,lease_expires_at_ms=NULL,failure_code=?,\
             updated_at_ms=MAX(updated_at_ms,?) WHERE degradation_id=? AND status='leased'\
             AND root_thread_id=? AND version=? AND lease_epoch=? AND lease_expires_at_ms=?",
        )
        .bind(next_status)
        .bind(next_retry_count as i64)
        .bind(retry_after_ms)
        .bind(failure_code)
        .bind(params.now_ms)
        .bind(params.lease.degradation_id.to_string())
        .bind(params.lease.root_thread_id.to_string())
        .bind(params.lease.version as i64)
        .bind(params.lease.lease_epoch as i64)
        .bind(params.lease.lease_expires_at_ms)
        .execute(&mut *connection)
        .await
        .map_err(internal)?;
        if updated.rows_affected() != 1 {
            return Ok(ResolveDegradationPublicationOutcome::Fenced);
        }
        injector
            .after_recovery_step(RecoveryStep::PublicationUpdate)
            .map_err(RecoveryWriteError::Internal)?;
        Ok(ResolveDegradationPublicationOutcome::Applied(status(
            next_status,
        )?))
    }
    .await;
    recovery_guard::finish_with(&mut connection, result, injector).await
}

fn status(value: &str) -> Result<DegradationPublicationStatus, RecoveryWriteError> {
    match value {
        "pending" => Ok(DegradationPublicationStatus::Pending),
        "leased" => Ok(DegradationPublicationStatus::Leased),
        "materialized" => Ok(DegradationPublicationStatus::Materialized),
        "poisoned" => Ok(DegradationPublicationStatus::Poisoned),
        _ => Err(RecoveryWriteError::CorruptState),
    }
}

fn unsigned(value: i64) -> Result<u64, RecoveryWriteError> {
    value
        .try_into()
        .map_err(|_| RecoveryWriteError::CorruptState)
}

fn internal(error: impl Into<anyhow::Error>) -> RecoveryWriteError {
    RecoveryWriteError::Internal(error.into())
}
