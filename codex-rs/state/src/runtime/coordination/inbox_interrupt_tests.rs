use codex_coordination::CoordinationSemanticSlot;
use pretty_assertions::assert_eq;

use super::inbox_test_support::*;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;

#[tokio::test]
async fn interrupt_receipt_is_causal_and_never_claimable_for_inclusion() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    persist_initial_receipt(&runtime).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(interrupt_command(2))
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    let outcome = runtime
        .persist_coordination_recipient_receipt(receipt_params(
            INTERRUPT_OPERATION,
            RECEIPT_TWO,
            "019f7c6c-1111-7000-8000-000000000706",
            3,
            1,
            Vec::new(),
        ))
        .await?;
    let PersistRecipientReceiptOutcome::Applied(metadata) = outcome else {
        anyhow::bail!("interrupt receipt was not applied")
    };
    let receipt_bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT canonical_event_bytes FROM coordination_events WHERE event_id=?",
    )
    .bind(metadata.receipt_event_id.to_string())
    .fetch_one(&*runtime.pool)
    .await?;
    let event: serde_json::Value = serde_json::from_slice(&receipt_bytes)?;
    assert_eq!(event["kind"], "interruptDurablyReceived");
    assert_eq!(
        event["causes"]["items"][0],
        "019f7c6c-1111-7000-8000-000000000705"
    );
    let claim = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: metadata.expires_at_ms - 100,
            lease_expires_at_ms: metadata.expires_at_ms - 1,
        })
        .await?;
    assert_eq!(claim, ClaimInboxReceiptOutcome::NotReady);
    Ok(())
}

#[tokio::test]
async fn unresolved_interrupt_blocks_new_generation_inclusion() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    persist_initial_receipt(&runtime).await?;
    runtime
        .record_coordination_command_intent(interrupt_command(2))
        .await?;
    runtime
        .persist_coordination_recipient_receipt(receipt_params(
            INTERRUPT_OPERATION,
            RECEIPT_TWO,
            "019f7c6c-1111-7000-8000-000000000706",
            3,
            1,
            Vec::new(),
        ))
        .await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(followup_command(4))
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    let followup_receipt = runtime
        .persist_coordination_recipient_receipt(receipt_params(
            FOLLOWUP_OPERATION,
            "019f7c6c-1111-7000-8000-000000000203",
            "019f7c6c-1111-7000-8000-000000000707",
            5,
            2,
            vec![(
                CoordinationSemanticSlot::AssignmentGenerationClosed,
                "019f7c6c-1111-7000-8000-000000000708",
                "019f7c6c-1111-7000-8000-000000000108",
            )],
        ))
        .await?;
    let PersistRecipientReceiptOutcome::Applied(metadata) = followup_receipt else {
        anyhow::bail!("followup receipt was not applied")
    };
    let claim = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: metadata.expires_at_ms - 100,
            lease_expires_at_ms: metadata.expires_at_ms - 1,
        })
        .await?;
    assert_eq!(claim, ClaimInboxReceiptOutcome::NotReady);
    let sets: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT captured_turn_set_bytes FROM coordination_inbox WHERE operation_kind='interrupt'",
    )
    .fetch_all(&*runtime.pool)
    .await?;
    assert_eq!(sets, vec![vec![1, 0, 0, 0, 1]]);
    Ok(())
}
