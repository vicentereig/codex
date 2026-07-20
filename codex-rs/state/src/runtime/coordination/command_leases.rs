use codex_coordination::CoordinationOperationId;
use sqlx::SqliteConnection;

use super::command_rows::*;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::commands::CommandWriteError;
use super::commands::NoCommandFailure;
use crate::StateRuntime;
use crate::model::coordination_commands::*;

const MAX_MAINTENANCE_BATCH: u32 = 256;

impl StateRuntime {
    pub(crate) async fn claim_coordination_command(
        &self,
        operation_id: CoordinationOperationId,
        expected_version: u64,
        expected_lease_epoch: u64,
        now_ms: i64,
        requested_lease_deadline_ms: i64,
    ) -> Result<ClaimCoordinationCommandOutcome, CommandWriteError> {
        self.claim_coordination_command_with(
            operation_id,
            expected_version,
            expected_lease_epoch,
            now_ms,
            requested_lease_deadline_ms,
            &NoCommandFailure,
        )
        .await
    }

    pub(super) async fn claim_coordination_command_with(
        &self,
        operation_id: CoordinationOperationId,
        expected_version: u64,
        expected_lease_epoch: u64,
        now_ms: i64,
        requested_lease_deadline_ms: i64,
        injector: &dyn CommandFailureInjector,
    ) -> Result<ClaimCoordinationCommandOutcome, CommandWriteError> {
        let mut connection = begin(self, injector).await?;
        let result = claim(
            &mut connection,
            operation_id,
            expected_version,
            expected_lease_epoch,
            now_ms,
            requested_lease_deadline_ms,
            injector,
        )
        .await;
        finish_command(&mut connection, result, injector).await
    }

    pub(crate) async fn begin_coordination_command_attempt(
        &self,
        lease: CommandLeaseToken,
        now_ms: i64,
    ) -> Result<BegunCommandAttempt, CommandWriteError> {
        self.begin_coordination_command_attempt_with(lease, now_ms, &NoCommandFailure)
            .await
    }

    pub(super) async fn begin_coordination_command_attempt_with(
        &self,
        lease: CommandLeaseToken,
        now_ms: i64,
        injector: &dyn CommandFailureInjector,
    ) -> Result<BegunCommandAttempt, CommandWriteError> {
        let mut connection = begin(self, injector).await?;
        let result = begin_attempt(&mut connection, lease, now_ms, injector).await;
        finish_command(&mut connection, result, injector).await
    }

    pub(crate) async fn resolve_coordination_command_attempt(
        &self,
        attempt: BegunCommandAttempt,
        resolution: CommandAttemptResolution,
        now_ms: i64,
    ) -> Result<ResolveCommandAttemptOutcome, CommandWriteError> {
        self.resolve_coordination_command_attempt_with(
            attempt,
            resolution,
            now_ms,
            &NoCommandFailure,
        )
        .await
    }

    pub(super) async fn resolve_coordination_command_attempt_with(
        &self,
        attempt: BegunCommandAttempt,
        resolution: CommandAttemptResolution,
        now_ms: i64,
        injector: &dyn CommandFailureInjector,
    ) -> Result<ResolveCommandAttemptOutcome, CommandWriteError> {
        let mut connection = begin(self, injector).await?;
        let result = resolve(&mut connection, attempt, resolution, now_ms, injector).await;
        finish_command(&mut connection, result, injector).await
    }

    pub(crate) async fn reclaim_expired_coordination_command_leases(
        &self,
        now_ms: i64,
        limit: u32,
    ) -> Result<u64, CommandWriteError> {
        self.reclaim_expired_coordination_command_leases_with(now_ms, limit, &NoCommandFailure)
            .await
    }

    pub(super) async fn reclaim_expired_coordination_command_leases_with(
        &self,
        now_ms: i64,
        limit: u32,
        injector: &dyn CommandFailureInjector,
    ) -> Result<u64, CommandWriteError> {
        maintenance_limit(limit)?;
        let mut connection = begin(self, injector).await?;
        let result = reclaim(&mut connection, now_ms, limit, injector).await;
        finish_command(&mut connection, result, injector).await
    }

    pub(crate) async fn expire_coordination_command_payloads(
        &self,
        now_ms: i64,
        limit: u32,
    ) -> Result<u64, CommandWriteError> {
        self.expire_coordination_command_payloads_with(now_ms, limit, &NoCommandFailure)
            .await
    }

    pub(super) async fn expire_coordination_command_payloads_with(
        &self,
        now_ms: i64,
        limit: u32,
        injector: &dyn CommandFailureInjector,
    ) -> Result<u64, CommandWriteError> {
        maintenance_limit(limit)?;
        let mut connection = begin(self, injector).await?;
        let result = expire(&mut connection, now_ms, limit, injector).await;
        finish_command(&mut connection, result, injector).await
    }
}

async fn claim(
    connection: &mut SqliteConnection,
    operation_id: CoordinationOperationId,
    expected_version: u64,
    expected_lease_epoch: u64,
    now_ms: i64,
    requested_lease_deadline_ms: i64,
    injector: &dyn CommandFailureInjector,
) -> Result<ClaimCoordinationCommandOutcome, CommandWriteError> {
    ensure_active(connection).await?;
    authority_boundary(injector)?;
    let stored =
        load_command_by_operation(connection, operation_id, CommandPayloadAccess::MetadataOnly)
            .await?
            .ok_or(CommandWriteError::NotReady)?;
    injector
        .after_command_step(CommandStep::LeaseRead)
        .map_err(internal)?;
    match stored.metadata.lifecycle {
        CommandLifecycle::Succeeded | CommandLifecycle::Poisoned => {
            return Ok(ClaimCoordinationCommandOutcome::Terminal(
                stored.metadata.lifecycle,
            ));
        }
        CommandLifecycle::Expired => return Ok(ClaimCoordinationCommandOutcome::Expired),
        CommandLifecycle::Leased => return Ok(ClaimCoordinationCommandOutcome::NotReady),
        CommandLifecycle::Pending => {}
    }
    if now_ms >= stored.metadata.expires_at_ms {
        expire_one(connection, operation_id, now_ms).await?;
        command_boundary(injector, CommandStep::PayloadPurgeUpdate)?;
        return Ok(ClaimCoordinationCommandOutcome::Expired);
    }
    if stored.metadata.version != expected_version
        || stored.metadata.lease_epoch != expected_lease_epoch
    {
        return Ok(ClaimCoordinationCommandOutcome::Fenced);
    }
    let target_is_current = target_is_current(connection, &stored.metadata).await?;
    command_boundary(injector, CommandStep::LeaseRead)?;
    if !target_is_current {
        return Ok(ClaimCoordinationCommandOutcome::Fenced);
    }
    if now_ms < stored.metadata.retry_after_ms || requested_lease_deadline_ms <= now_ms {
        return Ok(ClaimCoordinationCommandOutcome::NotReady);
    }
    let deadline = requested_lease_deadline_ms.min(stored.metadata.expires_at_ms);
    let changed = sqlx::query(
        "UPDATE coordination_commands SET lifecycle='leased',version=version+1,\
         claim_count=claim_count+1,lease_epoch=lease_epoch+1,lease_expires_at_ms=?,\
         updated_at_ms=MAX(updated_at_ms,?) WHERE operation_id=? AND lifecycle='pending' \
         AND version=? AND lease_epoch=? AND retry_after_ms<=? AND expires_at_ms>? \
         AND ciphertext IS NOT NULL",
    )
    .bind(deadline)
    .bind(now_ms.max(0))
    .bind(operation_id.to_string())
    .bind(i64::try_from(expected_version).map_err(internal)?)
    .bind(i64::try_from(expected_lease_epoch).map_err(internal)?)
    .bind(now_ms)
    .bind(now_ms)
    .execute(&mut *connection)
    .await
    .map_err(internal)?
    .rows_affected();
    if changed != 1 {
        return Ok(ClaimCoordinationCommandOutcome::Fenced);
    }
    injector
        .after_command_step(CommandStep::ClaimUpdate)
        .map_err(internal)?;
    let stored = load_command_by_operation(connection, operation_id, CommandPayloadAccess::Claim)
        .await?
        .ok_or(CommandWriteError::CorruptStoredCommand)?;
    command_boundary(injector, CommandStep::LeaseRead)?;
    let ciphertext = ciphertext(&stored)?;
    let lease = CommandLeaseToken {
        operation_id,
        version: stored.metadata.version,
        lease_epoch: stored.metadata.lease_epoch,
        lease_expires_at_ms: deadline,
    };
    Ok(ClaimCoordinationCommandOutcome::Claimed(
        ClaimedCoordinationCommand {
            metadata: stored.metadata,
            lease,
            ciphertext,
        },
    ))
}

async fn begin_attempt(
    connection: &mut SqliteConnection,
    lease: CommandLeaseToken,
    now_ms: i64,
    injector: &dyn CommandFailureInjector,
) -> Result<BegunCommandAttempt, CommandWriteError> {
    ensure_active(connection).await?;
    authority_boundary(injector)?;
    let stored = load_command_by_operation(
        connection,
        lease.operation_id,
        CommandPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(CommandWriteError::NotReady)?;
    injector
        .after_command_step(CommandStep::LeaseRead)
        .map_err(internal)?;
    if stored.metadata.lifecycle != CommandLifecycle::Leased
        || stored.metadata.version != lease.version
        || stored.metadata.lease_epoch != lease.lease_epoch
        || lease.lease_expires_at_ms <= now_ms
        || stored.metadata.expires_at_ms <= now_ms
    {
        return Err(CommandWriteError::LeaseFenced);
    }
    let target_is_current = target_is_current(connection, &stored.metadata).await?;
    command_boundary(injector, CommandStep::LeaseRead)?;
    if !target_is_current {
        return Err(CommandWriteError::GenerationFenced);
    }
    let changed = sqlx::query(
        "UPDATE coordination_commands SET version=version+1,attempt_count=attempt_count+1,\
         attempted_lease_epoch=lease_epoch,\
         updated_at_ms=MAX(updated_at_ms,?) WHERE operation_id=? AND lifecycle='leased' \
         AND version=? AND lease_epoch=? AND lease_expires_at_ms=? AND lease_expires_at_ms>? \
         AND expires_at_ms>? AND (attempted_lease_epoch IS NULL OR attempted_lease_epoch<lease_epoch)",
    )
    .bind(now_ms.max(0))
    .bind(lease.operation_id.to_string())
    .bind(i64::try_from(lease.version).map_err(internal)?)
    .bind(i64::try_from(lease.lease_epoch).map_err(internal)?)
    .bind(lease.lease_expires_at_ms)
    .bind(now_ms)
    .bind(now_ms)
    .execute(&mut *connection)
    .await
    .map_err(internal)?
    .rows_affected();
    if changed != 1 {
        return Err(CommandWriteError::LeaseFenced);
    }
    injector
        .after_command_step(CommandStep::AttemptUpdate)
        .map_err(internal)?;
    let stored = load_command_by_operation(
        connection,
        lease.operation_id,
        CommandPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(CommandWriteError::CorruptStoredCommand)?;
    command_boundary(injector, CommandStep::LeaseRead)?;
    Ok(BegunCommandAttempt {
        lease: CommandLeaseToken {
            version: stored.metadata.version,
            ..lease
        },
        attempt: stored.metadata.attempt_count,
    })
}

async fn resolve(
    connection: &mut SqliteConnection,
    attempt: BegunCommandAttempt,
    resolution: CommandAttemptResolution,
    now_ms: i64,
    injector: &dyn CommandFailureInjector,
) -> Result<ResolveCommandAttemptOutcome, CommandWriteError> {
    ensure_active(connection).await?;
    authority_boundary(injector)?;
    let lease = &attempt.lease;
    let stored = load_command_by_operation(
        connection,
        lease.operation_id,
        CommandPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(CommandWriteError::NotReady)?;
    injector
        .after_command_step(CommandStep::LeaseRead)
        .map_err(internal)?;
    match stored.metadata.lifecycle {
        CommandLifecycle::Succeeded => {
            let CommandAttemptResolution::Succeeded { ack } = &resolution else {
                return Err(CommandWriteError::IdempotencyConflict);
            };
            validate_success_ack(connection, lease.operation_id, ack).await?;
            command_boundary(injector, CommandStep::LeaseRead)?;
            if stored.metadata.terminal_receipt_id != Some(ack.receipt_id)
                || stored.metadata.terminal_receipt_fingerprint != Some(ack.delivery_fingerprint)
            {
                return Err(CommandWriteError::IdempotencyConflict);
            }
            return Ok(ResolveCommandAttemptOutcome::Terminal(
                stored.metadata.lifecycle,
            ));
        }
        CommandLifecycle::Poisoned => {
            return Ok(ResolveCommandAttemptOutcome::Terminal(
                stored.metadata.lifecycle,
            ));
        }
        CommandLifecycle::Expired => return Ok(ResolveCommandAttemptOutcome::Expired),
        CommandLifecycle::Pending | CommandLifecycle::Leased => {}
    }
    if stored.metadata.lifecycle != CommandLifecycle::Leased
        || stored.metadata.version != lease.version
        || stored.metadata.lease_epoch != lease.lease_epoch
        || stored.metadata.attempt_count != attempt.attempt
        || stored.metadata.attempted_lease_epoch != Some(lease.lease_epoch)
        || lease.lease_expires_at_ms <= now_ms
        || stored.metadata.expires_at_ms <= now_ms
    {
        return Ok(ResolveCommandAttemptOutcome::Fenced);
    }
    let (
        lifecycle,
        retry_after,
        failure_code,
        terminal_at,
        expires_at,
        terminal_receipt_id,
        terminal_receipt_fingerprint,
    ) = match resolution {
        CommandAttemptResolution::RetryAt { retry_at_ms, code } => {
            if retry_at_ms <= now_ms || retry_at_ms >= stored.metadata.expires_at_ms {
                return Err(CommandWriteError::NotReady);
            }
            (
                "pending",
                retry_at_ms,
                Some(failure_code_sql(code)),
                None,
                stored.metadata.expires_at_ms,
                None,
                None,
            )
        }
        CommandAttemptResolution::Succeeded { ack } => {
            validate_success_ack(connection, lease.operation_id, &ack).await?;
            command_boundary(injector, CommandStep::LeaseRead)?;
            (
                "succeeded",
                stored.metadata.retry_after_ms,
                None,
                Some(now_ms.max(0)),
                stored
                    .metadata
                    .expires_at_ms
                    .min(now_ms.saturating_add(TERMINAL_PAYLOAD_TTL_MS)),
                Some(ack.receipt_id.to_string()),
                Some(ack.delivery_fingerprint.to_vec()),
            )
        }
        CommandAttemptResolution::Poisoned { code } => (
            "poisoned",
            stored.metadata.retry_after_ms,
            Some(failure_code_sql(code)),
            Some(now_ms.max(0)),
            stored
                .metadata
                .expires_at_ms
                .min(now_ms.saturating_add(TERMINAL_PAYLOAD_TTL_MS)),
            None,
            None,
        ),
    };
    let changed = sqlx::query(
        "UPDATE coordination_commands SET lifecycle=?,version=version+1,retry_after_ms=?,\
         lease_expires_at_ms=NULL,failure_code=?,terminal_at_ms=?,expires_at_ms=?,\
         terminal_receipt_id=?,terminal_receipt_fingerprint=?,updated_at_ms=MAX(updated_at_ms,?) WHERE operation_id=? AND lifecycle='leased' \
         AND version=? AND lease_epoch=? AND lease_expires_at_ms=? AND lease_expires_at_ms>? \
         AND expires_at_ms>?",
    )
    .bind(lifecycle)
    .bind(retry_after)
    .bind(failure_code)
    .bind(terminal_at)
    .bind(expires_at)
    .bind(terminal_receipt_id)
    .bind(terminal_receipt_fingerprint)
    .bind(now_ms.max(0))
    .bind(lease.operation_id.to_string())
    .bind(i64::try_from(lease.version).map_err(internal)?)
    .bind(i64::try_from(lease.lease_epoch).map_err(internal)?)
    .bind(lease.lease_expires_at_ms)
    .bind(now_ms)
    .bind(now_ms)
    .execute(&mut *connection)
    .await
    .map_err(internal)?
    .rows_affected();
    if changed != 1 {
        return Ok(ResolveCommandAttemptOutcome::Fenced);
    }
    injector
        .after_command_step(CommandStep::ResolutionUpdate)
        .map_err(internal)?;
    let metadata = load_command_by_operation(
        connection,
        lease.operation_id,
        CommandPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(CommandWriteError::CorruptStoredCommand)?
    .metadata;
    command_boundary(injector, CommandStep::LeaseRead)?;
    Ok(ResolveCommandAttemptOutcome::Applied(metadata))
}

async fn validate_success_ack(
    connection: &mut SqliteConnection,
    operation_id: CoordinationOperationId,
    ack: &crate::model::coordination_inbox::CommittedReceiptAck,
) -> Result<(), CommandWriteError> {
    if ack.command_operation_id != operation_id {
        return Err(CommandWriteError::IdempotencyConflict);
    }
    let row: Option<(String, Vec<u8>, String, i64, i64, i64)> = sqlx::query_as(
        "SELECT command_operation_id,delivery_fingerprint,receipt_event_id,encoded_payload_bytes,durable_received_at_ms,absolute_expires_at_ms FROM coordination_inbox WHERE receipt_id=?",
    )
    .bind(ack.receipt_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    let Some((stored_operation, fingerprint, receipt_event, encoded, received_at, expires_at)) =
        row
    else {
        return Err(CommandWriteError::IdempotencyConflict);
    };
    if stored_operation != operation_id.to_string()
        || fingerprint.as_slice() != ack.delivery_fingerprint
        || receipt_event != ack.receipt_event_id.to_string()
        || encoded != i64::from(ack.encoded_payload_bytes)
        || received_at != ack.durable_received_at_ms
        || expires_at != ack.expires_at_ms
    {
        return Err(CommandWriteError::IdempotencyConflict);
    }
    Ok(())
}

async fn reclaim(
    connection: &mut SqliteConnection,
    now_ms: i64,
    limit: u32,
    injector: &dyn CommandFailureInjector,
) -> Result<u64, CommandWriteError> {
    ensure_active(connection).await?;
    authority_boundary(injector)?;
    let result = sqlx::query(
        "UPDATE coordination_commands SET lifecycle=CASE WHEN expires_at_ms<=? THEN 'expired' \
         ELSE 'pending' END,version=version+1,lease_expires_at_ms=NULL,\
         ciphertext=CASE WHEN expires_at_ms<=? THEN NULL ELSE ciphertext END,\
         purged_at_ms=CASE WHEN expires_at_ms<=? THEN MAX(intent_at_ms,?) ELSE purged_at_ms END,\
         updated_at_ms=MAX(updated_at_ms,?) WHERE operation_id IN (SELECT operation_id \
         FROM coordination_commands WHERE lifecycle='leased' AND lease_expires_at_ms<=? \
         AND (attempted_lease_epoch IS NULL OR attempted_lease_epoch<lease_epoch) \
         ORDER BY lease_expires_at_ms,operation_id LIMIT ?)",
    )
    .bind(now_ms)
    .bind(now_ms)
    .bind(now_ms)
    .bind(now_ms.max(0))
    .bind(now_ms.max(0))
    .bind(now_ms)
    .bind(limit as i64)
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_command_step(CommandStep::ReclaimUpdate)
        .map_err(internal)?;
    Ok(result.rows_affected())
}

async fn expire(
    connection: &mut SqliteConnection,
    now_ms: i64,
    limit: u32,
    injector: &dyn CommandFailureInjector,
) -> Result<u64, CommandWriteError> {
    ensure_active(connection).await?;
    authority_boundary(injector)?;
    let result = sqlx::query(
        "UPDATE coordination_commands SET lifecycle=CASE WHEN lifecycle IN ('succeeded','poisoned') \
         THEN lifecycle ELSE 'expired' END,version=version+1,lease_expires_at_ms=NULL,\
         ciphertext=NULL,purged_at_ms=MAX(intent_at_ms,?),updated_at_ms=MAX(updated_at_ms,?) \
         WHERE operation_id IN (SELECT operation_id FROM coordination_commands \
         WHERE ciphertext IS NOT NULL AND expires_at_ms<=? ORDER BY expires_at_ms,operation_id LIMIT ?)",
    )
    .bind(now_ms.max(0)).bind(now_ms.max(0)).bind(now_ms).bind(limit as i64)
    .execute(&mut *connection).await.map_err(internal)?;
    injector
        .after_command_step(CommandStep::PayloadPurgeUpdate)
        .map_err(internal)?;
    Ok(result.rows_affected())
}

async fn expire_one(
    connection: &mut SqliteConnection,
    operation_id: CoordinationOperationId,
    now_ms: i64,
) -> Result<(), CommandWriteError> {
    sqlx::query(
        "UPDATE coordination_commands SET lifecycle='expired',version=version+1,\
         lease_expires_at_ms=NULL,ciphertext=NULL,purged_at_ms=MAX(intent_at_ms,?),\
         updated_at_ms=MAX(updated_at_ms,?) WHERE operation_id=? \
         AND lifecycle IN ('pending','leased') AND expires_at_ms<=?",
    )
    .bind(now_ms.max(0))
    .bind(now_ms.max(0))
    .bind(operation_id.to_string())
    .bind(now_ms)
    .execute(&mut *connection)
    .await
    .map_err(internal)?;
    Ok(())
}

async fn begin(
    runtime: &StateRuntime,
    injector: &dyn CommandFailureInjector,
) -> Result<sqlx::pool::PoolConnection<sqlx::Sqlite>, CommandWriteError> {
    let mut connection = runtime.pool.acquire().await.map_err(internal)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(internal)?;
    if let Err(error) = injector.after_command_step(CommandStep::TransactionBegin) {
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .map_err(internal)?;
        injector
            .after_command_step(CommandStep::Rollback)
            .map_err(internal)?;
        return Err(internal(error));
    }
    Ok(connection)
}

async fn ensure_active(connection: &mut SqliteConnection) -> Result<(), CommandWriteError> {
    let status: String =
        sqlx::query_scalar("SELECT status FROM coordination_authority WHERE singleton_id=1")
            .fetch_one(&mut *connection)
            .await
            .map_err(internal)?;
    if status != "active" {
        return Err(CommandWriteError::Quarantined);
    }
    Ok(())
}

fn maintenance_limit(limit: u32) -> Result<(), CommandWriteError> {
    if limit == 0 || limit > MAX_MAINTENANCE_BATCH {
        return Err(CommandWriteError::NotReady);
    }
    Ok(())
}

fn command_boundary(
    injector: &dyn CommandFailureInjector,
    step: CommandStep,
) -> Result<(), CommandWriteError> {
    injector.after_command_step(step).map_err(internal)
}

fn authority_boundary(injector: &dyn CommandFailureInjector) -> Result<(), CommandWriteError> {
    injector
        .after_step(super::aggregate_journal::AggregateStep::AuthorityRead)
        .map_err(internal)
}

fn internal(error: impl Into<anyhow::Error>) -> CommandWriteError {
    CommandWriteError::Internal(error.into())
}
