use codex_coordination::CoordinationEventId;
use pretty_assertions::assert_eq;

use super::aggregate_test_support::OPERATION;
use super::inbox::InboxWriteError;
use super::inbox_test_support::*;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;

#[tokio::test]
async fn assignment_receipt_acceptance_is_one_atomic_replayable_bundle() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let params = receipt_params(
        OPERATION,
        RECEIPT_ONE,
        "019f7c6c-1111-7000-8000-000000000702",
        1,
        0,
        Vec::new(),
    );
    let applied = runtime
        .persist_coordination_recipient_receipt(params.clone())
        .await?;
    let duplicate = runtime
        .persist_coordination_recipient_receipt(params)
        .await?;
    assert!(matches!(
        (&applied, &duplicate),
        (
            PersistRecipientReceiptOutcome::Applied(left),
            PersistRecipientReceiptOutcome::Duplicate(right)
        ) if left == right
    ));
    let counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coordination_inbox),\
         (SELECT count(*) FROM coordination_turn_bindings),\
         (SELECT count(*) FROM coordination_events),\
         (SELECT count(*) FROM coordination_projection_outbox),\
         (SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?)",
    )
    .bind(super::aggregate_test_support::ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(counts, (1, 1, 2, 2, 2));
    let ack = runtime
        .coordination_durable_receipt_ack(codex_coordination::ReceiptId::parse(RECEIPT_ONE)?)
        .await?;
    assert_eq!(
        ack.receipt_event_id,
        event_id("019f7c6c-1111-7000-8000-000000000702")
    );
    assert!(!format!("{ack:?}").contains(&"a5".repeat(16)));
    Ok(())
}

#[tokio::test]
async fn concurrent_duplicate_receipt_commits_once() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let params = receipt_params(
        OPERATION,
        RECEIPT_ONE,
        "019f7c6c-1111-7000-8000-000000000702",
        1,
        0,
        Vec::new(),
    );
    let (left, right) = tokio::join!(
        runtime.persist_coordination_recipient_receipt(params.clone()),
        runtime.persist_coordination_recipient_receipt(params),
    );
    let outcomes = [left?, right?];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PersistRecipientReceiptOutcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PersistRecipientReceiptOutcome::Duplicate(_)))
            .count(),
        1
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM coordination_inbox")
        .fetch_one(&*runtime.pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn divergent_receipt_identity_consumes_no_revision() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    persist_initial_receipt(&runtime).await?;
    let mut divergent = receipt_params(
        OPERATION,
        RECEIPT_ONE,
        "019f7c6c-1111-7000-8000-000000000702",
        2,
        0,
        Vec::new(),
    );
    divergent.context.primary.event_id =
        CoordinationEventId::parse("019f7c6c-1111-7000-8000-000000000799")?;
    assert!(matches!(
        runtime
            .persist_coordination_recipient_receipt(divergent)
            .await,
        Err(InboxWriteError::IdempotencyConflict)
    ));
    let revision: i64 = sqlx::query_scalar(
        "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(super::aggregate_test_support::ROOT)
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(revision, 2);
    Ok(())
}

#[tokio::test]
async fn purged_receipt_still_returns_metadata_only_ack_and_duplicate() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let metadata = persist_initial_receipt(&runtime).await?;
    sqlx::query("UPDATE coordination_inbox SET lifecycle='expired',version=version+1,ciphertext=NULL,purged_at_ms=?,updated_at_ms=? WHERE receipt_id=?")
        .bind(metadata.expires_at_ms).bind(metadata.expires_at_ms).bind(RECEIPT_ONE)
        .execute(&*runtime.pool).await?;
    let duplicate = runtime
        .persist_coordination_recipient_receipt(receipt_params(
            OPERATION,
            RECEIPT_ONE,
            "019f7c6c-1111-7000-8000-000000000702",
            2,
            0,
            Vec::new(),
        ))
        .await?;
    assert!(matches!(
        duplicate,
        PersistRecipientReceiptOutcome::Duplicate(_)
    ));
    let ack = runtime
        .coordination_durable_receipt_ack(codex_coordination::ReceiptId::parse(RECEIPT_ONE)?)
        .await?;
    assert_eq!(ack.encoded_payload_bytes, 384);
    Ok(())
}

#[tokio::test]
async fn quarantined_exact_duplicate_is_rejected_before_duplicate_lookup() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    persist_initial_receipt(&runtime).await?;
    sqlx::query(
        "UPDATE coordination_authority SET status='quarantined',\
         quarantine_reason='test',updated_at_ms=updated_at_ms+1 WHERE singleton_id=1",
    )
    .execute(&*runtime.pool)
    .await?;
    assert!(matches!(
        runtime
            .persist_coordination_recipient_receipt(receipt_params(
                OPERATION,
                RECEIPT_ONE,
                "019f7c6c-1111-7000-8000-000000000702",
                2,
                0,
                Vec::new(),
            ))
            .await,
        Err(InboxWriteError::Quarantined)
    ));
    Ok(())
}

#[tokio::test]
async fn reused_journal_event_id_is_an_identity_conflict_not_a_sql_error() -> anyhow::Result<()> {
    let runtime = runtime_with_assignment_command().await?;
    let params = receipt_params(
        OPERATION,
        RECEIPT_ONE,
        "019f7c6c-1111-7000-8000-000000000701",
        1,
        0,
        Vec::new(),
    );
    assert!(matches!(
        runtime.persist_coordination_recipient_receipt(params).await,
        Err(InboxWriteError::IdentityConflict)
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM coordination_inbox")
        .fetch_one(&*runtime.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}
