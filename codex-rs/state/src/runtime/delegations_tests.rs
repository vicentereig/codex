use super::StateRuntime;
use crate::DurableDelegationStatus;
use codex_protocol::ThreadId;

#[tokio::test]
async fn delegation_bind_requires_exact_version_and_reserved_state() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "test-provider".to_string())
        .await
        .expect("state runtime");
    let parent = ThreadId::new();
    let child = ThreadId::new();
    runtime
        .reserve_delegation("delegation-1", "run-1", parent, "/root/worker", 1)
        .await
        .expect("reservation");

    assert!(
        !runtime
            .bind_delegation("delegation-1", child, 1, 2)
            .await
            .expect("stale CAS")
    );
    assert!(
        runtime
            .bind_delegation("delegation-1", child, 0, 2)
            .await
            .expect("bind")
    );
    assert!(
        !runtime
            .bind_delegation("delegation-1", child, 0, 3)
            .await
            .expect("duplicate bind")
    );
}

#[tokio::test]
async fn terminal_transition_cannot_be_overwritten() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "test-provider".to_string())
        .await
        .expect("state runtime");
    runtime
        .reserve_delegation("delegation-2", "run-2", ThreadId::new(), "/root/worker", 1)
        .await
        .expect("reservation");
    assert!(
        runtime
            .transition_delegation(
                "delegation-2",
                0,
                DurableDelegationStatus::Reserved,
                DurableDelegationStatus::Failed,
                2,
            )
            .await
            .expect("terminal transition")
    );
    assert!(
        !runtime
            .transition_delegation(
                "delegation-2",
                1,
                DurableDelegationStatus::Failed,
                DurableDelegationStatus::Completed,
                3,
            )
            .await
            .expect("terminal CAS")
    );
}

#[tokio::test]
async fn retry_claim_advances_attempt_and_lease_epoch_with_cas() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home, "test-provider".to_string())
        .await
        .expect("state runtime");
    runtime
        .reserve_delegation("delegation-3", "run-3", ThreadId::new(), "/root/worker", 1)
        .await
        .expect("reservation");
    assert!(
        runtime
            .transition_delegation(
                "delegation-3",
                0,
                DurableDelegationStatus::Reserved,
                DurableDelegationStatus::Retryable,
                2,
            )
            .await
            .expect("retryable transition")
    );
    assert!(
        runtime
            .claim_delegation_attempt("delegation-3", 1, 0, 3)
            .await
            .expect("claim")
    );
    assert!(
        !runtime
            .claim_delegation_attempt("delegation-3", 1, 0, 4)
            .await
            .expect("stale lease claim")
    );
}

#[tokio::test]
async fn delivery_intent_and_receipt_are_versioned_and_idempotent() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home, "test-provider".to_string())
        .await
        .expect("state runtime");
    let parent = ThreadId::new();
    let child = ThreadId::new();
    runtime
        .reserve_delegation("delegation-4", "run-4", parent, "/root/worker", 1)
        .await
        .expect("reservation");
    assert!(
        runtime
            .bind_delegation("delegation-4", child, 0, 2)
            .await
            .expect("bind")
    );
    assert!(
        runtime
            .record_delegation_delivery_intent("delegation-4", 1, 1, 3)
            .await
            .expect("intent")
    );
    assert!(
        !runtime
            .record_delegation_delivery_intent("delegation-4", 1, 1, 4)
            .await
            .expect("duplicate intent")
    );
    assert!(
        runtime
            .record_delegation_delivery_receipt("delegation-4", 2, 1, "run-4:1", 5)
            .await
            .expect("receipt")
    );
}
