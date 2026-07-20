use sqlx::SqliteConnection;

use super::aggregate_journal::authority;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::inbox::internal;
use super::inbox_rows::InboxPayloadAccess;
use super::inbox_rows::inbox_ciphertext;
use super::inbox_rows::load_inbox_by_receipt;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::ClaimedInboxReceipt;
use crate::model::coordination_inbox::InboxInputError;
use crate::model::coordination_inbox::InboxLeaseToken;
use crate::model::coordination_inbox::InboxLifecycle;

pub(super) async fn claim_receipt(
    connection: &mut SqliteConnection,
    params: ClaimInboxReceipt,
    injector: &dyn InboxFailureInjector,
) -> Result<ClaimInboxReceiptOutcome, InboxWriteError> {
    authority(connection, injector).await?;
    if params.now_ms < 0 || params.lease_expires_at_ms <= params.now_ms {
        return Err(InboxInputError::InvalidLeaseDeadline.into());
    }
    let stored = load_inbox_by_receipt(connection, params.receipt_id, InboxPayloadAccess::Claim)
        .await?
        .ok_or(InboxWriteError::IdentityConflict)?;
    let replay_version = params.expected_version.checked_add(1);
    let replay_epoch = params.expected_lease_epoch.checked_add(1);
    if stored.metadata.lifecycle == InboxLifecycle::Leased
        && stored.lease_claim_operation_id == Some(params.claim_operation_id)
        && Some(stored.metadata.version) == replay_version
        && Some(stored.metadata.lease_epoch) == replay_epoch
        && stored.lease_expires_at_ms == Some(params.lease_expires_at_ms)
        && stored.updated_at_ms == params.now_ms
    {
        let lease = InboxLeaseToken {
            receipt_id: stored.metadata.receipt_id,
            claim_operation_id: params.claim_operation_id,
            version: stored.metadata.version,
            lease_epoch: stored.metadata.lease_epoch,
            lease_expires_at_ms: params.lease_expires_at_ms,
            target_turn_id: stored.metadata.recipient_turn_id.clone(),
            delivery_fingerprint: stored.metadata.delivery_fingerprint,
        };
        return Ok(ClaimInboxReceiptOutcome::Claimed(ClaimedInboxReceipt {
            metadata: stored.metadata.clone(),
            lease,
            ciphertext: inbox_ciphertext(&stored)?,
        }));
    }
    match stored.metadata.lifecycle {
        InboxLifecycle::Processed | InboxLifecycle::Poisoned => {
            return Ok(ClaimInboxReceiptOutcome::Terminal(
                stored.metadata.lifecycle,
            ));
        }
        InboxLifecycle::Expired => return Ok(ClaimInboxReceiptOutcome::Expired),
        InboxLifecycle::Leased | InboxLifecycle::Selected => {
            return Ok(ClaimInboxReceiptOutcome::NotReady);
        }
        InboxLifecycle::Received => {}
    }
    if stored.metadata.kind == CommandKind::Interrupt {
        return Ok(ClaimInboxReceiptOutcome::NotReady);
    }
    if stored.metadata.version != params.expected_version
        || stored.metadata.lease_epoch != params.expected_lease_epoch
    {
        return Ok(ClaimInboxReceiptOutcome::Fenced);
    }
    if params.now_ms < stored.metadata.retry_after_ms {
        return Ok(ClaimInboxReceiptOutcome::NotReady);
    }
    if params.now_ms >= stored.metadata.expires_at_ms
        || params.lease_expires_at_ms > stored.metadata.expires_at_ms
    {
        return Ok(ClaimInboxReceiptOutcome::Expired);
    }
    if turn_is_terminal(connection, &stored).await?
        || unresolved_interrupt_blocks(connection, &stored).await?
    {
        return Ok(ClaimInboxReceiptOutcome::NotReady);
    }
    let expected_version =
        i64::try_from(params.expected_version).map_err(|_| InboxWriteError::LeaseFenced)?;
    let expected_epoch =
        i64::try_from(params.expected_lease_epoch).map_err(|_| InboxWriteError::LeaseFenced)?;
    let changed = sqlx::query("UPDATE coordination_inbox SET lifecycle='leased',version=version+1,claim_count=claim_count+1,lease_epoch=lease_epoch+1,lease_expires_at_ms=?,lease_claim_operation_id=?,updated_at_ms=? WHERE receipt_id=? AND lifecycle='received' AND version=? AND lease_epoch=? AND retry_after_ms<=? AND expires_at_ms>?")
        .bind(params.lease_expires_at_ms)
        .bind(params.claim_operation_id.to_string())
        .bind(params.now_ms)
        .bind(params.receipt_id.to_string())
        .bind(expected_version)
        .bind(expected_epoch)
        .bind(params.now_ms)
        .bind(params.now_ms)
        .execute(&mut *connection).await.map_err(internal)?.rows_affected();
    if changed != 1 {
        return Ok(ClaimInboxReceiptOutcome::Fenced);
    }
    injector
        .after_inbox_step(InboxStep::ClaimUpdate)
        .map_err(internal)?;
    let claimed = load_inbox_by_receipt(connection, params.receipt_id, InboxPayloadAccess::Claim)
        .await?
        .ok_or(InboxWriteError::CorruptStoredInbox)?;
    let lease_expires_at_ms = claimed
        .lease_expires_at_ms
        .ok_or(InboxWriteError::CorruptStoredInbox)?;
    let lease = InboxLeaseToken {
        receipt_id: claimed.metadata.receipt_id,
        claim_operation_id: params.claim_operation_id,
        version: claimed.metadata.version,
        lease_epoch: claimed.metadata.lease_epoch,
        lease_expires_at_ms,
        target_turn_id: claimed.metadata.recipient_turn_id.clone(),
        delivery_fingerprint: claimed.metadata.delivery_fingerprint,
    };
    Ok(ClaimInboxReceiptOutcome::Claimed(ClaimedInboxReceipt {
        metadata: claimed.metadata.clone(),
        lease,
        ciphertext: inbox_ciphertext(&claimed)?,
    }))
}

pub(super) async fn unresolved_interrupt_blocks(
    connection: &mut SqliteConnection,
    stored: &super::inbox_rows::StoredInbox,
) -> Result<bool, InboxWriteError> {
    let sets: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT captured_turn_set_bytes FROM coordination_inbox WHERE root_thread_id=? AND recipient_turn_id=? AND operation_kind='interrupt' AND lifecycle IN ('received','leased','selected') ORDER BY receipt_id",
    )
    .bind(stored.metadata.root_thread_id.to_string())
    .bind(stored.metadata.recipient_turn_id.as_str())
    .fetch_all(&mut *connection)
    .await
    .map_err(internal)?;
    Ok(sets.iter().any(|set| {
        !canonical_generation_set_contains(set, stored.metadata.target_generation.get())
    }))
}

async fn turn_is_terminal(
    connection: &mut SqliteConnection,
    stored: &super::inbox_rows::StoredInbox,
) -> Result<bool, InboxWriteError> {
    let terminal: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM coordination_turn_terminals WHERE root_thread_id=? AND target_thread_id=? AND target_turn_id=?",
    )
    .bind(stored.metadata.root_thread_id.to_string())
    .bind(stored.metadata.recipient_thread_id.to_string())
    .bind(stored.metadata.recipient_turn_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    Ok(terminal == Some(1))
}

fn canonical_generation_set_contains(bytes: &[u8], generation: u32) -> bool {
    let Some((&count, body)) = bytes.split_first() else {
        return false;
    };
    if usize::from(count) * 4 != body.len() {
        return false;
    }
    body.chunks_exact(4).any(|chunk| {
        let mut value = [0_u8; 4];
        value.copy_from_slice(chunk);
        u32::from_be_bytes(value) == generation
    })
}
