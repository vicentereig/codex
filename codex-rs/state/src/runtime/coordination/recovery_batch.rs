use sqlx::SqlitePool;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::assignment_recovery;
use super::command_recovery;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::inbox_maintenance;
use super::inbox_recovery;
use super::recovery::NoRecoveryFailure;
use super::recovery::RecoveryBatch;
use super::recovery::RecoveryDisposition;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_inbox::InboxMaintenanceBatch;
use crate::model::coordination_recovery_state::MAX_RECOVERY_BATCH;

pub(crate) async fn recover_coordination_batch(
    pool: &SqlitePool,
    now_ms: i64,
    limit: u32,
) -> Result<RecoveryBatch, RecoveryWriteError> {
    recover_coordination_batch_with(pool, now_ms, limit, &NoRecoveryFailure).await
}

pub(super) async fn recover_coordination_batch_with(
    pool: &SqlitePool,
    now_ms: i64,
    limit: u32,
    injector: &dyn RecoveryFailureInjector,
) -> Result<RecoveryBatch, RecoveryWriteError> {
    if now_ms < 0 {
        return Err(
            crate::model::coordination_recovery::RecoveryInputError::InvalidTimestamp.into(),
        );
    }
    if limit == 0 || limit > MAX_RECOVERY_BATCH {
        return Err(
            crate::model::coordination_recovery::RecoveryInputError::InvalidBatchLimit.into(),
        );
    }
    let mut connection = recovery_guard::begin_with(pool, injector).await?;
    let result = async {
        let state_epoch = recovery_guard::active_epoch_with(&mut connection, injector).await?;
        let mut dispositions = Vec::with_capacity(limit as usize);

        let poisoned = command_recovery::poison_uncertain_attempts(
            &mut connection,
            state_epoch,
            now_ms,
            remaining(limit, dispositions.len()),
            injector,
        )
        .await?;
        extend(
            &mut dispositions,
            poisoned,
            RecoveryDisposition::CommandPoisoned,
        );
        if dispositions.len() < limit as usize {
            let expired_commands = command_recovery::expire_payloads(
                &mut connection,
                state_epoch,
                now_ms,
                remaining(limit, dispositions.len()),
                injector,
            )
            .await?;
            extend(
                &mut dispositions,
                expired_commands,
                RecoveryDisposition::CommandPayloadExpired,
            );
        }
        if dispositions.len() < limit as usize {
            let stranded = assignment_recovery::classify_stranded_assignments(
                &mut connection,
                state_epoch,
                now_ms,
                remaining(limit, dispositions.len()),
                injector,
            )
            .await?;
            extend(
                &mut dispositions,
                stranded,
                RecoveryDisposition::AssignmentStranded,
            );
        }
        if dispositions.len() < limit as usize {
            let reclaimed = command_recovery::reclaim_safe_leases(
                &mut connection,
                now_ms,
                remaining(limit, dispositions.len()),
                injector,
            )
            .await?;
            extend(
                &mut dispositions,
                reclaimed,
                RecoveryDisposition::CommandLeaseReclaimed,
            );
        }
        if dispositions.len() < limit as usize {
            let inbox_injector = RecoveryInboxFailure(injector);
            let expired = inbox_maintenance::expire_payloads(
                &mut connection,
                InboxMaintenanceBatch {
                    now_ms,
                    limit: remaining(limit, dispositions.len()),
                },
                &inbox_injector,
            )
            .await
            .map_err(inbox_error)?;
            inbox_recovery::record_expired_payload_degradations(
                &mut connection,
                state_epoch,
                &expired.changed_receipts,
                now_ms,
                injector,
            )
            .await?;
            extend(
                &mut dispositions,
                expired.changed_receipts.len() as u64,
                RecoveryDisposition::InboxPayloadExpired,
            );
        }
        if dispositions.len() < limit as usize {
            let inbox_injector = RecoveryInboxFailure(injector);
            let reclaimed = inbox_maintenance::reclaim_leases(
                &mut connection,
                InboxMaintenanceBatch {
                    now_ms,
                    limit: remaining(limit, dispositions.len()),
                },
                &inbox_injector,
            )
            .await
            .map_err(inbox_error)?;
            extend(
                &mut dispositions,
                reclaimed.changed_receipts.len() as u64,
                RecoveryDisposition::InboxLeaseReclaimed,
            );
        }
        Ok(RecoveryBatch { dispositions })
    }
    .await;
    recovery_guard::finish_with(&mut connection, result, injector).await
}

fn remaining(limit: u32, completed: usize) -> u32 {
    limit - completed as u32
}

struct RecoveryInboxFailure<'a>(&'a dyn RecoveryFailureInjector);

impl AggregateFailureInjector for RecoveryInboxFailure<'_> {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        if step == AggregateStep::AuthorityRead {
            self.0.after_recovery_step(RecoveryStep::AuthorityRead)?;
        }
        Ok(())
    }
}

impl InboxFailureInjector for RecoveryInboxFailure<'_> {
    fn after_inbox_step(&self, step: InboxStep) -> anyhow::Result<()> {
        let step = match step {
            InboxStep::MaintenanceRead => RecoveryStep::RecoveryRead,
            InboxStep::SelectionUpdate | InboxStep::MaintenanceUpdate => {
                RecoveryStep::RecoveryUpdate
            }
            InboxStep::DuplicateRead
            | InboxStep::TransactionBegin
            | InboxStep::Rollback
            | InboxStep::CommandRead
            | InboxStep::TargetFence
            | InboxStep::ReceiptEvent
            | InboxStep::ReceiptInsert
            | InboxStep::ClaimUpdate
            | InboxStep::SelectionInsert
            | InboxStep::InboxUpdate => return Ok(()),
        };
        self.0.after_recovery_step(step)
    }
}

fn extend(
    dispositions: &mut Vec<RecoveryDisposition>,
    count: u64,
    disposition: RecoveryDisposition,
) {
    dispositions.extend(std::iter::repeat_n(disposition, count as usize));
}

fn inbox_error(error: InboxWriteError) -> RecoveryWriteError {
    match error {
        InboxWriteError::Quarantined => RecoveryWriteError::Quarantined,
        InboxWriteError::RootMissing => RecoveryWriteError::EpochMismatch,
        InboxWriteError::CorruptStoredInbox => RecoveryWriteError::CorruptState,
        other => RecoveryWriteError::Internal(anyhow::Error::new(other)),
    }
}
