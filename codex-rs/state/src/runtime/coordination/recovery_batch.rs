use sqlx::SqlitePool;

use super::assignment_recovery;
use super::command_recovery;
use super::inbox::InboxWriteError;
use super::inbox::NoInboxFailure;
use super::inbox_maintenance;
use super::inbox_recovery;
use super::recovery::RecoveryBatch;
use super::recovery::RecoveryDisposition;
use super::recovery::RecoveryWriteError;
use super::recovery_guard;
use crate::model::coordination_inbox::InboxMaintenanceBatch;
use crate::model::coordination_recovery_state::MAX_RECOVERY_BATCH;

pub(crate) async fn recover_coordination_batch(
    pool: &SqlitePool,
    now_ms: i64,
    limit: u32,
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
    let mut connection = recovery_guard::begin(pool).await?;
    let result = async {
        let state_epoch = recovery_guard::active_epoch(&mut connection).await?;
        let mut dispositions = Vec::with_capacity(limit as usize);

        let poisoned = command_recovery::poison_uncertain_attempts(
            &mut connection,
            state_epoch,
            now_ms,
            remaining(limit, dispositions.len()),
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
            )
            .await?;
            extend(
                &mut dispositions,
                reclaimed,
                RecoveryDisposition::CommandLeaseReclaimed,
            );
        }
        if dispositions.len() < limit as usize {
            let expired = inbox_maintenance::expire_payloads(
                &mut connection,
                InboxMaintenanceBatch {
                    now_ms,
                    limit: remaining(limit, dispositions.len()),
                },
                &NoInboxFailure,
            )
            .await
            .map_err(inbox_error)?;
            inbox_recovery::record_expired_payload_degradations(
                &mut connection,
                state_epoch,
                &expired.changed_receipts,
                now_ms,
            )
            .await?;
            extend(
                &mut dispositions,
                expired.changed_receipts.len() as u64,
                RecoveryDisposition::InboxPayloadExpired,
            );
        }
        if dispositions.len() < limit as usize {
            let reclaimed = inbox_maintenance::reclaim_leases(
                &mut connection,
                InboxMaintenanceBatch {
                    now_ms,
                    limit: remaining(limit, dispositions.len()),
                },
                &NoInboxFailure,
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
    recovery_guard::finish(&mut connection, result).await
}

fn remaining(limit: u32, completed: usize) -> u32 {
    limit - completed as u32
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
