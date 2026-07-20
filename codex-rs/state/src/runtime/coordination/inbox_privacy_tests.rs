use super::inbox_test_support::*;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;

#[tokio::test]
async fn ciphertext_never_reaches_debug_ack_event_or_outbox_surfaces() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let ack = runtime
        .coordination_durable_receipt_ack(metadata.receipt_id)
        .await?;
    let now = metadata.expires_at_ms - 100;
    let claimed = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: now,
            lease_expires_at_ms: metadata.expires_at_ms - 1,
        })
        .await?;
    let ClaimInboxReceiptOutcome::Claimed(claimed) = claimed else {
        anyhow::bail!("claim failed")
    };
    let sentinel = "a5".repeat(32);
    for rendered in [
        format!("{metadata:?}"),
        format!("{ack:?}"),
        format!("{claimed:?}"),
    ] {
        assert!(!rendered.to_lowercase().contains(&sentinel));
    }
    let immutable_bytes: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT canonical_event_bytes FROM coordination_events UNION ALL SELECT CAST(event_id AS BLOB) FROM coordination_projection_outbox",
    )
    .fetch_all(&*runtime.pool)
    .await?;
    for bytes in immutable_bytes {
        assert!(
            !String::from_utf8_lossy(&bytes)
                .to_lowercase()
                .contains(&sentinel)
        );
    }
    assert_eq!(claimed.ciphertext.as_bytes().len(), 384);
    Ok(())
}
