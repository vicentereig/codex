use codex_coordination::CoordinationSemanticSlot;
use pretty_assertions::assert_eq;

use super::aggregate_test_support::context;
use super::inbox_test_support::*;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_inbox::RecordInboxSelectionOutcome;

async fn runtime_with_message_receipt() -> anyhow::Result<std::sync::Arc<crate::StateRuntime>> {
    let runtime = runtime_with_assignment_command().await?;
    persist_initial_receipt(&runtime).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(message_command(2))
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    assert!(matches!(
        runtime
            .persist_coordination_recipient_receipt(receipt_params(
                MESSAGE_OPERATION,
                RECEIPT_TWO,
                "019f7c6c-1111-7000-8000-000000000706",
                3,
                1,
                Vec::new(),
            ))
            .await?,
        PersistRecipientReceiptOutcome::Applied(_)
    ));
    Ok(runtime)
}

#[tokio::test]
async fn raw_sql_cannot_null_or_retarget_immutable_receipt_fields() -> anyhow::Result<()> {
    let runtime = runtime_with_message_receipt().await?;
    for statement in [
        "UPDATE coordination_inbox SET captured_head_generation=NULL,version=version+1 WHERE receipt_id=?",
        "UPDATE coordination_inbox SET receipt_tuple_bytes=x'00',version=version+1 WHERE receipt_id=?",
        "UPDATE coordination_inbox SET recipient_turn_id='other-turn',version=version+1 WHERE receipt_id=?",
        "UPDATE coordination_inbox SET delivery_fingerprint=NULL,version=version+1 WHERE receipt_id=?",
    ] {
        assert!(
            sqlx::query(statement)
                .bind(RECEIPT_TWO)
                .execute(&*runtime.pool)
                .await
                .is_err(),
            "statement unexpectedly bypassed schema guard: {statement}"
        );
    }
    assert!(
        sqlx::query("DELETE FROM coordination_inbox WHERE receipt_id=?")
            .bind(RECEIPT_TWO)
            .execute(&*runtime.pool)
            .await
            .is_err()
    );
    let row: (String, String, i64) = sqlx::query_as(
        "SELECT recipient_turn_id,lifecycle,version FROM coordination_inbox WHERE receipt_id=?",
    )
    .bind(RECEIPT_TWO)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(row, ("turn-b".to_string(), "received".to_string(), 0));
    Ok(())
}

#[tokio::test]
async fn raw_sql_cannot_select_before_a_committed_claim() -> anyhow::Result<()> {
    let runtime = runtime_with_message_receipt().await?;
    let result = sqlx::query("INSERT INTO coordination_inbox_inclusions (receipt_id,inference_attempt_id,root_thread_id,target_turn_id,delivery_fingerprint,selected_at_ms,semantic_claim,semantic_event_id,inbox_version,lease_epoch,transport_state,transport_completed_at_ms,retry_after_ms,version,failure_code) SELECT receipt_id,'attempt-a',root_thread_id,recipient_turn_id,delivery_fingerprint,durable_received_at_ms,1,NULL,1,1,'selected',NULL,NULL,0,NULL FROM coordination_inbox WHERE receipt_id=?")
        .bind(RECEIPT_TWO).execute(&*runtime.pool).await;
    assert!(result.is_err());
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM coordination_inbox_inclusions WHERE receipt_id=?")
            .bind(RECEIPT_TWO)
            .fetch_one(&*runtime.pool)
            .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn raw_sql_cannot_skip_or_reassign_the_first_semantic_claim() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now_ms = metadata.expires_at_ms - 10_000;
    let claim = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms,
            lease_expires_at_ms: now_ms + 1_000,
        })
        .await?;
    let ClaimInboxReceiptOutcome::Claimed(claim) = claim else {
        anyhow::bail!("claim failed")
    };
    let result = sqlx::query("INSERT INTO coordination_inbox_inclusions (receipt_id,inference_attempt_id,root_thread_id,target_turn_id,delivery_fingerprint,selected_at_ms,lease_expires_at_ms,semantic_claim,semantic_event_id,inbox_version,lease_epoch,claim_operation_id,transport_state,transport_completed_at_ms,retry_after_ms,version,failure_code) VALUES (?,?,?,?,?,?,?,0,NULL,?,?,?,'selected',NULL,NULL,0,NULL)")
        .bind(RECEIPT_ONE)
        .bind("attempt-without-semantic-claim")
        .bind(super::aggregate_test_support::ROOT)
        .bind("turn-b")
        .bind(claim.lease.delivery_fingerprint.as_slice())
        .bind(now_ms + 1)
        .bind(claim.lease.lease_expires_at_ms)
        .bind(i64::try_from(claim.lease.version + 1)?)
        .bind(i64::try_from(claim.lease.lease_epoch)?)
        .bind(claim.lease.claim_operation_id.to_string())
        .execute(&*runtime.pool)
        .await;
    assert!(result.is_err());
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM coordination_inbox_inclusions WHERE receipt_id=?")
            .bind(RECEIPT_ONE)
            .fetch_one(&*runtime.pool)
            .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn raw_sql_cannot_create_two_live_selections_or_rewrite_outcome_fences() -> anyhow::Result<()>
{
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    let now_ms = metadata.expires_at_ms - 10_000;
    let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms,
            lease_expires_at_ms: now_ms + 1_000,
        })
        .await?
    else {
        anyhow::bail!("claim failed")
    };
    sqlx::query("UPDATE coordination_inbox SET lease_claim_operation_id=? WHERE receipt_id=?")
        .bind(CLAIM_OPERATION_TWO)
        .bind(RECEIPT_ONE)
        .execute(&*runtime.pool)
        .await
        .expect_err("active claim identity cannot be rewritten");
    sqlx::query("INSERT INTO coordination_inbox_inclusions (receipt_id,inference_attempt_id,root_thread_id,target_turn_id,delivery_fingerprint,selected_at_ms,lease_expires_at_ms,semantic_claim,semantic_event_id,inbox_version,lease_epoch,claim_operation_id,transport_state,transport_completed_at_ms,retry_after_ms,version,failure_code) VALUES (?,?,?,?,?,?,?,1,NULL,?,?,?,'selected',NULL,NULL,0,NULL)")
        .bind(RECEIPT_ONE)
        .bind("mismatched-claim")
        .bind(super::aggregate_test_support::ROOT)
        .bind("turn-b")
        .bind(claim.lease.delivery_fingerprint.as_slice())
        .bind(now_ms + 1)
        .bind(claim.lease.lease_expires_at_ms)
        .bind(i64::try_from(claim.lease.version + 1)?)
        .bind(i64::try_from(claim.lease.lease_epoch)?)
        .bind(CLAIM_OPERATION_TWO)
        .execute(&*runtime.pool)
        .await
        .expect_err("inclusion cannot substitute another claim identity");
    let RecordInboxSelectionOutcome::Applied(selection) = runtime
        .record_coordination_inclusion_selection(RecordInboxSelection {
            lease: claim.lease,
            inference_attempt_id: inference_attempt("attempt-first"),
            event_context: None,
            selected_at_ms: now_ms + 1,
        })
        .await?
    else {
        anyhow::bail!("selection failed")
    };
    let second = sqlx::query("INSERT INTO coordination_inbox_inclusions (receipt_id,inference_attempt_id,root_thread_id,target_turn_id,delivery_fingerprint,selected_at_ms,lease_expires_at_ms,semantic_claim,semantic_event_id,inbox_version,lease_epoch,claim_operation_id,transport_state,transport_completed_at_ms,retry_after_ms,version,failure_code) VALUES (?,?,?,?,?,?,?,0,NULL,?,?,?,'selected',NULL,NULL,0,NULL)")
        .bind(RECEIPT_ONE)
        .bind("attempt-second")
        .bind(super::aggregate_test_support::ROOT)
        .bind("turn-b")
        .bind(selection.token.delivery_fingerprint.as_slice())
        .bind(now_ms + 2)
        .bind(now_ms + 1_000)
        .bind(i64::try_from(selection.token.inbox_version)?)
        .bind(i64::try_from(selection.token.lease_epoch)?)
        .bind(selection.token.claim_operation_id.to_string())
        .execute(&*runtime.pool)
        .await;
    assert!(second.is_err());
    let rewritten = sqlx::query("UPDATE coordination_inbox_inclusions SET inbox_version=inbox_version+1,transport_state='sendSucceeded',transport_completed_at_ms=?,version=version+1 WHERE receipt_id=? AND inference_attempt_id=?")
        .bind(now_ms + 2)
        .bind(RECEIPT_ONE)
        .bind("attempt-first")
        .execute(&*runtime.pool)
        .await;
    assert!(rewritten.is_err());
    let state: (i64, String, i64, i64) = sqlx::query_as(
        "SELECT count(*),min(transport_state),min(inbox_version),min(lease_epoch) FROM coordination_inbox_inclusions WHERE receipt_id=?",
    )
    .bind(RECEIPT_ONE)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(
        state,
        (
            1,
            "selected".to_string(),
            i64::try_from(selection.token.inbox_version)?,
            i64::try_from(selection.token.lease_epoch)?
        )
    );
    Ok(())
}

#[tokio::test]
async fn raw_sql_cannot_attach_an_unrelated_interrupt_resolution() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    persist_initial_receipt(&runtime).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(interrupt_command(2))
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    assert!(matches!(
        runtime
            .persist_coordination_recipient_receipt(receipt_params(
                INTERRUPT_OPERATION,
                RECEIPT_TWO,
                "019f7c6c-1111-7000-8000-000000000706",
                3,
                1,
                Vec::new(),
            ))
            .await?,
        PersistRecipientReceiptOutcome::Applied(_)
    ));
    let result = sqlx::query("UPDATE coordination_inbox SET lifecycle='processed',version=version+1,resolution_event_id=?,terminal_at_ms=durable_received_at_ms,updated_at_ms=durable_received_at_ms WHERE receipt_id=?")
        .bind("019f7c6c-1111-7000-8000-000000000702").bind(RECEIPT_TWO)
        .execute(&*runtime.pool).await;
    assert!(result.is_err());
    let state: String =
        sqlx::query_scalar("SELECT lifecycle FROM coordination_inbox WHERE receipt_id=?")
            .bind(RECEIPT_TWO)
            .fetch_one(&*runtime.pool)
            .await?;
    assert_eq!(state, "received");
    Ok(())
}

#[tokio::test]
async fn raw_sql_receipt_requires_exact_intent_cause_and_projection_outbox() -> anyhow::Result<()> {
    for missing_outbox in [false, true] {
        let runtime = runtime_with_assignment_command().await?;
        persist_initial_receipt(&runtime).await?;
        sqlx::query(
            "CREATE TEMP TABLE saved_receipt AS SELECT * FROM coordination_inbox \
             WHERE receipt_id=?",
        )
        .bind(RECEIPT_ONE)
        .execute(&*runtime.pool)
        .await?;
        sqlx::query("DROP TRIGGER coordination_inbox_no_delete")
            .execute(&*runtime.pool)
            .await?;
        sqlx::query("DELETE FROM coordination_inbox WHERE receipt_id=?")
            .bind(RECEIPT_ONE)
            .execute(&*runtime.pool)
            .await?;
        if missing_outbox {
            sqlx::query("DELETE FROM coordination_projection_outbox WHERE event_id=?")
                .bind("019f7c6c-1111-7000-8000-000000000702")
                .execute(&*runtime.pool)
                .await?;
        } else {
            sqlx::query("DROP TRIGGER coordination_events_immutable_update")
                .execute(&*runtime.pool)
                .await?;
            sqlx::query(
                "UPDATE coordination_events SET canonical_event_bytes=CAST(\
                 json_set(CAST(canonical_event_bytes AS TEXT),'$.causes.items',\
                 json_array('019f7c6c-1111-7000-8000-000000000799')) AS BLOB) \
                 WHERE event_id=?",
            )
            .bind("019f7c6c-1111-7000-8000-000000000702")
            .execute(&*runtime.pool)
            .await?;
        }
        let reinsert = sqlx::query("INSERT INTO coordination_inbox SELECT * FROM saved_receipt")
            .execute(&*runtime.pool)
            .await;
        assert!(
            reinsert.is_err(),
            "receipt event guard accepted missing_outbox={missing_outbox}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn raw_sql_inclusion_requires_exact_receipt_cause_and_projection_outbox() -> anyhow::Result<()>
{
    for missing_outbox in [false, true] {
        let runtime = runtime_with_message_receipt().await?;
        let metadata_expires: i64 =
            sqlx::query_scalar("SELECT expires_at_ms FROM coordination_inbox WHERE receipt_id=?")
                .bind(RECEIPT_TWO)
                .fetch_one(&*runtime.pool)
                .await?;
        let now_ms = metadata_expires - 10_000;
        let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
            .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
                receipt_id: codex_coordination::ReceiptId::parse(RECEIPT_TWO)?,
                claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
                expected_version: 0,
                expected_lease_epoch: 0,
                now_ms,
                lease_expires_at_ms: now_ms + 1_000,
            })
            .await?
        else {
            anyhow::bail!("claim failed")
        };
        assert!(matches!(
            runtime
                .record_coordination_inclusion_selection(RecordInboxSelection {
                    lease: claim.lease,
                    inference_attempt_id: inference_attempt("raw-event-guard"),
                    event_context: Some(context(
                        CoordinationSemanticSlot::MessageIncludedInModelInput,
                        "019f7c6c-1111-7000-8000-000000000707",
                        MESSAGE_OPERATION,
                        true,
                        4,
                        Vec::new(),
                    )),
                    selected_at_ms: now_ms + 1,
                })
                .await?,
            RecordInboxSelectionOutcome::Applied(_)
        ));
        sqlx::query(
            "CREATE TEMP TABLE saved_inclusion AS SELECT * FROM coordination_inbox_inclusions \
             WHERE receipt_id=?",
        )
        .bind(RECEIPT_TWO)
        .execute(&*runtime.pool)
        .await?;
        sqlx::query("DROP TRIGGER coordination_inclusion_no_delete")
            .execute(&*runtime.pool)
            .await?;
        sqlx::query("DELETE FROM coordination_inbox_inclusions WHERE receipt_id=?")
            .bind(RECEIPT_TWO)
            .execute(&*runtime.pool)
            .await?;
        sqlx::query("DROP TRIGGER coordination_inbox_transition_guard")
            .execute(&*runtime.pool)
            .await?;
        sqlx::query(
            "UPDATE coordination_inbox SET lifecycle='leased',version=1 WHERE receipt_id=?",
        )
        .bind(RECEIPT_TWO)
        .execute(&*runtime.pool)
        .await?;
        if missing_outbox {
            sqlx::query("DELETE FROM coordination_projection_outbox WHERE event_id=?")
                .bind("019f7c6c-1111-7000-8000-000000000707")
                .execute(&*runtime.pool)
                .await?;
        } else {
            sqlx::query("DROP TRIGGER coordination_events_immutable_update")
                .execute(&*runtime.pool)
                .await?;
            sqlx::query(
                "UPDATE coordination_events SET canonical_event_bytes=CAST(\
                 json_set(CAST(canonical_event_bytes AS TEXT),'$.causes.items',\
                 json_array('019f7c6c-1111-7000-8000-000000000799')) AS BLOB) \
                 WHERE event_id=?",
            )
            .bind("019f7c6c-1111-7000-8000-000000000707")
            .execute(&*runtime.pool)
            .await?;
        }
        let reinsert =
            sqlx::query("INSERT INTO coordination_inbox_inclusions SELECT * FROM saved_inclusion")
                .execute(&*runtime.pool)
                .await;
        assert!(
            reinsert.is_err(),
            "inclusion event guard accepted missing_outbox={missing_outbox}"
        );
    }
    Ok(())
}
