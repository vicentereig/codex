use pretty_assertions::assert_eq;

use super::inbox::InboxWriteError;
use super::inbox_test_support::*;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::InboxMaintenanceBatch;
use crate::model::coordination_inbox::InboxTransportResolution;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_inbox::RecordInboxSelectionOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcomeResult;

#[tokio::test]
async fn inbox_payload_expires_at_exact_boundary_and_purge_is_idempotent() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let before = runtime
        .expire_coordination_inbox_payloads(InboxMaintenanceBatch {
            now_ms: metadata.expires_at_ms - 1,
            limit: 16,
        })
        .await?;
    assert!(before.changed_receipts.is_empty());
    let boundary = runtime
        .expire_coordination_inbox_payloads(InboxMaintenanceBatch {
            now_ms: metadata.expires_at_ms,
            limit: 16,
        })
        .await?;
    assert_eq!(boundary.changed_receipts, vec![metadata.receipt_id]);
    let replay = runtime
        .expire_coordination_inbox_payloads(InboxMaintenanceBatch {
            now_ms: metadata.expires_at_ms,
            limit: 16,
        })
        .await?;
    assert!(replay.changed_receipts.is_empty());
    let state: (String, Option<Vec<u8>>, i64) = sqlx::query_as(
        "SELECT lifecycle,ciphertext,version FROM coordination_inbox WHERE receipt_id=?",
    )
    .bind(RECEIPT_ONE)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(state, ("expired".to_string(), None, 1));
    Ok(())
}

#[tokio::test]
async fn quarantine_permanently_denies_payload_claim_and_maintenance() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    sqlx::query("UPDATE coordination_authority SET status='quarantined',quarantine_reason='test quarantine'")
        .execute(&*runtime.pool).await?;
    assert!(matches!(
        runtime
            .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
                receipt_id: metadata.receipt_id,
                claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
                expected_version: 0,
                expected_lease_epoch: 0,
                now_ms: metadata.expires_at_ms - 100,
                lease_expires_at_ms: metadata.expires_at_ms - 1,
            })
            .await,
        Err(InboxWriteError::Quarantined)
    ));
    assert!(matches!(
        runtime
            .expire_coordination_inbox_payloads(InboxMaintenanceBatch {
                now_ms: metadata.expires_at_ms,
                limit: 16,
            })
            .await,
        Err(InboxWriteError::Quarantined)
    ));
    let payload: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT ciphertext FROM coordination_inbox WHERE receipt_id=?")
            .bind(RECEIPT_ONE)
            .fetch_one(&*runtime.pool)
            .await?;
    assert_eq!(payload.map(|bytes| bytes.len()), Some(384));
    Ok(())
}

#[tokio::test]
async fn invalid_or_unbounded_maintenance_batch_is_rejected() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    for limit in [0, 257] {
        assert!(matches!(
            runtime
                .expire_coordination_inbox_payloads(InboxMaintenanceBatch { now_ms: 0, limit })
                .await,
            Err(InboxWriteError::Input(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn successful_transport_sets_exact_24h_ttl_bounded_by_absolute_expiry() -> anyhow::Result<()>
{
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now: i64 = sqlx::query_scalar(
        "SELECT durable_received_at_ms FROM coordination_inbox WHERE receipt_id=?",
    )
    .bind(RECEIPT_ONE)
    .fetch_one(&*runtime.pool)
    .await?;
    let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: now,
            lease_expires_at_ms: now + 1_000,
        })
        .await?
    else {
        anyhow::bail!("claim failed")
    };
    let RecordInboxSelectionOutcome::Applied(selection) = runtime
        .record_coordination_inclusion_selection(RecordInboxSelection {
            lease: claim.lease,
            inference_attempt_id: inference_attempt("ttl-attempt"),
            event_context: None,
            selected_at_ms: now + 1,
        })
        .await?
    else {
        anyhow::bail!("selection failed")
    };
    let completed_at_ms = now + 2;
    let RecordInboxTransportOutcomeResult::Applied(terminal) = runtime
        .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
            selection: selection.token,
            resolution: InboxTransportResolution::SendSucceeded,
            completed_at_ms,
        })
        .await?
    else {
        anyhow::bail!("outcome failed")
    };
    assert_eq!(terminal.expires_at_ms, completed_at_ms + 86_400_000);
    let absolute: i64 = sqlx::query_scalar(
        "SELECT absolute_expires_at_ms FROM coordination_inbox WHERE receipt_id=?",
    )
    .bind(RECEIPT_ONE)
    .fetch_one(&*runtime.pool)
    .await?;
    assert!(terminal.expires_at_ms < absolute);
    Ok(())
}
