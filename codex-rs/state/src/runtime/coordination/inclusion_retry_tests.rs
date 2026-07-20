use codex_coordination::CoordinationFailureCode;
use codex_coordination::CoordinationSemanticSlot;
use pretty_assertions::assert_eq;

use super::aggregate_test_support::context;
use super::inbox_test_support::*;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::InboxMaintenanceBatch;
use crate::model::coordination_inbox::InboxTransportResolution;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_inbox::RecordInboxSelectionOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcomeResult;

#[tokio::test]
async fn send_unknown_retries_with_new_attempt_and_one_semantic_event() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    persist_initial_receipt(&runtime).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(message_command(2))
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    let receipt = runtime
        .persist_coordination_recipient_receipt(receipt_params(
            MESSAGE_OPERATION,
            RECEIPT_TWO,
            "019f7c6c-1111-7000-8000-000000000706",
            3,
            1,
            Vec::new(),
        ))
        .await?;
    let PersistRecipientReceiptOutcome::Applied(metadata) = receipt else {
        anyhow::bail!("message receipt was not applied")
    };
    let first_now = metadata.expires_at_ms - 10_000;
    let first_claim = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: first_now,
            lease_expires_at_ms: first_now + 2_000,
        })
        .await?;
    let ClaimInboxReceiptOutcome::Claimed(first_claim) = first_claim else {
        anyhow::bail!("first claim failed")
    };
    let first_selection = runtime
        .record_coordination_inclusion_selection(RecordInboxSelection {
            lease: first_claim.lease,
            inference_attempt_id: inference_attempt("attempt-a"),
            event_context: Some(context(
                CoordinationSemanticSlot::MessageIncludedInModelInput,
                "019f7c6c-1111-7000-8000-000000000707",
                MESSAGE_OPERATION,
                true,
                4,
                Vec::new(),
            )),
            selected_at_ms: first_now + 1,
        })
        .await?;
    let RecordInboxSelectionOutcome::Applied(first_selection) = first_selection else {
        anyhow::bail!("first selection failed")
    };
    let first_outcome = runtime
        .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
            selection: first_selection.token.clone(),
            resolution: InboxTransportResolution::SendUnknown {
                retry_at_ms: first_now + 3,
            },
            completed_at_ms: first_now + 2,
        })
        .await?;
    assert!(matches!(
        first_outcome,
        RecordInboxTransportOutcomeResult::Applied(_)
    ));
    let duplicate = runtime
        .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
            selection: first_selection.token,
            resolution: InboxTransportResolution::SendUnknown {
                retry_at_ms: first_now + 3,
            },
            completed_at_ms: first_now + 2,
        })
        .await?;
    assert!(matches!(
        duplicate,
        RecordInboxTransportOutcomeResult::Duplicate(_)
    ));

    let second_claim = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_TWO),
            expected_version: 3,
            expected_lease_epoch: 1,
            now_ms: first_now + 3,
            lease_expires_at_ms: first_now + 4_000,
        })
        .await?;
    let ClaimInboxReceiptOutcome::Claimed(second_claim) = second_claim else {
        anyhow::bail!("second claim failed")
    };
    let second_selection = runtime
        .record_coordination_inclusion_selection(RecordInboxSelection {
            lease: second_claim.lease,
            inference_attempt_id: inference_attempt("attempt-b"),
            event_context: None,
            selected_at_ms: first_now + 4,
        })
        .await?;
    let RecordInboxSelectionOutcome::Applied(second_selection) = second_selection else {
        anyhow::bail!("second selection failed")
    };
    assert!(!second_selection.semantic_claim);
    assert_eq!(second_selection.semantic_event_id, None);
    assert!(matches!(
        runtime
            .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
                selection: second_selection.token,
                resolution: InboxTransportResolution::SendSucceeded,
                completed_at_ms: first_now + 5,
            })
            .await?,
        RecordInboxTransportOutcomeResult::Applied(_)
    ));
    let cardinality: (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*),sum(semantic_claim),sum(semantic_event_id IS NOT NULL) FROM coordination_inbox_inclusions WHERE receipt_id=?",
    )
    .bind(RECEIPT_TWO)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(cardinality, (2, 1, 1));
    let event_count: i64 = sqlx::query_scalar("SELECT count(*) FROM coordination_events WHERE json_extract(CAST(canonical_event_bytes AS TEXT),'$.kind')='messageIncludedInModelInput'")
        .fetch_one(&*runtime.pool).await?;
    assert_eq!(event_count, 1);
    Ok(())
}

#[tokio::test]
async fn divergent_transport_outcome_is_first_wins() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now = metadata.expires_at_ms - 10_000;
    let claim = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: now,
            lease_expires_at_ms: now + 1_000,
        })
        .await?;
    let ClaimInboxReceiptOutcome::Claimed(claim) = claim else {
        anyhow::bail!("claim failed")
    };
    let selection = runtime
        .record_coordination_inclusion_selection(RecordInboxSelection {
            lease: claim.lease,
            inference_attempt_id: inference_attempt("attempt-a"),
            event_context: None,
            selected_at_ms: now + 1,
        })
        .await?;
    let RecordInboxSelectionOutcome::Applied(selection) = selection else {
        anyhow::bail!("selection failed")
    };
    runtime
        .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
            selection: selection.token.clone(),
            resolution: InboxTransportResolution::SendFailed {
                code: CoordinationFailureCode::TargetUnavailable,
                retry_at_ms: now + 3,
            },
            completed_at_ms: now + 2,
        })
        .await?;
    let divergent = runtime
        .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
            selection: selection.token,
            resolution: InboxTransportResolution::SendSucceeded,
            completed_at_ms: now + 2,
        })
        .await;
    assert!(matches!(
        divergent,
        Err(super::inbox::InboxWriteError::TerminalConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn transport_outcome_at_lease_deadline_is_rejected_without_mutation() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now = metadata.expires_at_ms - 10_000;
    let lease_deadline = now + 100;
    let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: now,
            lease_expires_at_ms: lease_deadline,
        })
        .await?
    else {
        anyhow::bail!("claim failed")
    };
    let selection_params = RecordInboxSelection {
        lease: claim.lease,
        inference_attempt_id: inference_attempt("deadline-attempt"),
        event_context: None,
        selected_at_ms: now + 1,
    };
    let RecordInboxSelectionOutcome::Applied(selection) = runtime
        .record_coordination_inclusion_selection(selection_params.clone())
        .await?
    else {
        anyhow::bail!("selection failed")
    };
    let mut stale_duplicate = selection_params;
    stale_duplicate.lease.version += 1;
    assert!(matches!(
        runtime
            .record_coordination_inclusion_selection(stale_duplicate)
            .await,
        Err(super::inbox::InboxWriteError::IdempotencyConflict)
    ));
    assert!(matches!(
        runtime
            .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
                selection: selection.token,
                resolution: InboxTransportResolution::SendSucceeded,
                completed_at_ms: lease_deadline,
            })
            .await?,
        RecordInboxTransportOutcomeResult::Expired
    ));
    let state: (String, String) = sqlx::query_as(
        "SELECT i.lifecycle,x.transport_state FROM coordination_inbox i \
         JOIN coordination_inbox_inclusions x USING(receipt_id) WHERE i.receipt_id=?",
    )
    .bind(RECEIPT_ONE)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(state, ("selected".to_string(), "selected".to_string()));
    Ok(())
}

#[tokio::test]
async fn reclaimed_claim_fences_stale_lease_and_advances_epoch_once() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now = metadata.expires_at_ms - 10_000;
    let first_params = ClaimInboxReceipt {
        receipt_id: metadata.receipt_id,
        claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
        expected_version: 0,
        expected_lease_epoch: 0,
        now_ms: now,
        lease_expires_at_ms: now + 100,
    };
    let ClaimInboxReceiptOutcome::Claimed(first) = runtime
        .claim_coordination_receipt_for_inclusion(first_params.clone())
        .await?
    else {
        anyhow::bail!("first claim failed")
    };
    assert_eq!(
        runtime
            .reclaim_expired_coordination_inbox_leases(InboxMaintenanceBatch {
                now_ms: now + 100,
                limit: 16,
            })
            .await?
            .changed_receipts,
        vec![metadata.receipt_id]
    );
    assert_eq!(
        runtime
            .claim_coordination_receipt_for_inclusion(first_params)
            .await?,
        ClaimInboxReceiptOutcome::Fenced
    );
    assert!(matches!(
        runtime
            .record_coordination_inclusion_selection(RecordInboxSelection {
                lease: first.lease,
                inference_attempt_id: inference_attempt("stale-lease"),
                event_context: None,
                selected_at_ms: now + 50,
            })
            .await?,
        RecordInboxSelectionOutcome::Fenced
    ));
    let ClaimInboxReceiptOutcome::Claimed(second) = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_TWO),
            expected_version: 2,
            expected_lease_epoch: 1,
            now_ms: now + 100,
            lease_expires_at_ms: now + 500,
        })
        .await?
    else {
        anyhow::bail!("second claim failed")
    };
    assert_eq!((second.lease.version, second.lease.lease_epoch), (3, 2));
    assert!(matches!(
        runtime
            .record_coordination_inclusion_selection(RecordInboxSelection {
                lease: second.lease,
                inference_attempt_id: inference_attempt("current-lease"),
                event_context: None,
                selected_at_ms: now + 101,
            })
            .await?,
        RecordInboxSelectionOutcome::Applied(_)
    ));
    Ok(())
}

#[tokio::test]
async fn quarantine_rejects_exact_selection_and_outcome_replays() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now = metadata.expires_at_ms - 10_000;
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
    let selection_params = RecordInboxSelection {
        lease: claim.lease,
        inference_attempt_id: inference_attempt("quarantine-replay"),
        event_context: None,
        selected_at_ms: now + 1,
    };
    let RecordInboxSelectionOutcome::Applied(selection) = runtime
        .record_coordination_inclusion_selection(selection_params.clone())
        .await?
    else {
        anyhow::bail!("selection failed")
    };
    sqlx::query(
        "UPDATE coordination_authority SET status='quarantined',quarantine_reason='test',\
         updated_at_ms=updated_at_ms+1 WHERE singleton_id=1",
    )
    .execute(&*runtime.pool)
    .await?;
    assert!(matches!(
        runtime
            .record_coordination_inclusion_selection(selection_params)
            .await,
        Err(super::inbox::InboxWriteError::Quarantined)
    ));
    assert!(matches!(
        runtime
            .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
                selection: selection.token,
                resolution: InboxTransportResolution::SendSucceeded,
                completed_at_ms: now + 2,
            })
            .await,
        Err(super::inbox::InboxWriteError::Quarantined)
    ));
    Ok(())
}

#[tokio::test]
async fn concurrent_claimants_authorize_one_operation_and_only_that_operation_replays_payload()
-> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now = metadata.expires_at_ms - 10_000;
    let first = ClaimInboxReceipt {
        receipt_id: metadata.receipt_id,
        claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
        expected_version: 0,
        expected_lease_epoch: 0,
        now_ms: now,
        lease_expires_at_ms: now + 1_000,
    };
    let second = ClaimInboxReceipt {
        claim_operation_id: claim_operation(CLAIM_OPERATION_TWO),
        ..first.clone()
    };
    let (first_result, second_result) = tokio::join!(
        runtime.claim_coordination_receipt_for_inclusion(first.clone()),
        runtime.claim_coordination_receipt_for_inclusion(second.clone()),
    );
    let outcomes = [(first, first_result?), (second, second_result?)];
    assert_eq!(
        outcomes
            .iter()
            .filter(|(_, outcome)| matches!(outcome, ClaimInboxReceiptOutcome::Claimed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|(_, outcome)| matches!(outcome, ClaimInboxReceiptOutcome::NotReady))
            .count(),
        1
    );
    let (winner_params, winner) = outcomes
        .iter()
        .find_map(|(params, outcome)| match outcome {
            ClaimInboxReceiptOutcome::Claimed(claimed) => Some((params, claimed)),
            _ => None,
        })
        .expect("one winner");
    assert_eq!(
        winner.lease.claim_operation_id,
        winner_params.claim_operation_id
    );
    assert_eq!(
        runtime
            .claim_coordination_receipt_for_inclusion(winner_params.clone())
            .await?,
        ClaimInboxReceiptOutcome::Claimed(winner.clone())
    );
    let loser_params = outcomes
        .iter()
        .find(|(params, _)| params.claim_operation_id != winner_params.claim_operation_id)
        .map(|(params, _)| params)
        .expect("one loser");
    assert_eq!(
        runtime
            .claim_coordination_receipt_for_inclusion(loser_params.clone())
            .await?,
        ClaimInboxReceiptOutcome::NotReady
    );
    let mut forged_lease = winner.lease.clone();
    forged_lease.claim_operation_id = loser_params.claim_operation_id;
    assert!(matches!(
        runtime
            .record_coordination_inclusion_selection(RecordInboxSelection {
                lease: forged_lease,
                inference_attempt_id: inference_attempt("forged-claim"),
                event_context: None,
                selected_at_ms: now + 1,
            })
            .await?,
        RecordInboxSelectionOutcome::Fenced
    ));
    let RecordInboxSelectionOutcome::Applied(selection) = runtime
        .record_coordination_inclusion_selection(RecordInboxSelection {
            lease: winner.lease.clone(),
            inference_attempt_id: inference_attempt("authorized-claim"),
            event_context: None,
            selected_at_ms: now + 1,
        })
        .await?
    else {
        anyhow::bail!("authorized selection failed")
    };
    assert_eq!(
        selection.token.claim_operation_id,
        winner_params.claim_operation_id
    );
    let stored_claim: String = sqlx::query_scalar(
        "SELECT lease_claim_operation_id FROM coordination_inbox WHERE receipt_id=?",
    )
    .bind(RECEIPT_ONE)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(stored_claim, winner_params.claim_operation_id.to_string());
    Ok(())
}
