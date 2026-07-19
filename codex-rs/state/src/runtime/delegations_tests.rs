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

#[test]
fn delegation_retry_backoff_is_bounded() {
    assert_eq!(StateRuntime::delegation_retry_delay_ms(0), 100);
    assert_eq!(StateRuntime::delegation_retry_delay_ms(9), 51_200);
    assert_eq!(StateRuntime::delegation_retry_delay_ms(10), 60_000);
    assert_eq!(StateRuntime::delegation_retry_delay_ms(-1), 100);
}

#[tokio::test]
async fn cancellation_requires_request_then_authoritative_confirmation() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home, "test-provider".to_string())
        .await
        .expect("state runtime");
    let child = ThreadId::new();
    runtime
        .reserve_delegation("delegation-5", "run-5", ThreadId::new(), "/root/worker", 1)
        .await
        .expect("reservation");
    assert!(
        runtime
            .bind_delegation("delegation-5", child, 0, 2)
            .await
            .expect("bind")
    );
    assert!(
        runtime
            .request_delegation_cancel("delegation-5", 1, 3)
            .await
            .expect("cancel request")
    );
    assert!(
        runtime
            .confirm_delegation_cancel("delegation-5", 2, 0, 4)
            .await
            .expect("cancel confirmation")
    );
    assert!(
        !runtime
            .request_delegation_cancel("delegation-5", 3, 5)
            .await
            .expect("stale cancellation")
    );
}

#[tokio::test]
async fn resumed_parent_reconstructs_obligations_and_retains_terminal_records() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home, "test-provider".to_string())
        .await
        .expect("state runtime");
    let parent = ThreadId::new();
    runtime
        .reserve_delegation("delegation-6", "run-6", parent, "/root/failed", 1)
        .await
        .expect("reservation");
    assert_eq!(
        runtime
            .delegation_finalization_for_parent(parent)
            .await
            .expect("blocked status"),
        crate::DelegationFinalization::Blocked
    );
    assert!(
        runtime
            .transition_delegation(
                "delegation-6",
                0,
                crate::DurableDelegationStatus::Reserved,
                crate::DurableDelegationStatus::Failed,
                2,
            )
            .await
            .expect("failure")
    );
    assert_eq!(
        runtime
            .delegation_finalization_for_parent(parent)
            .await
            .expect("partial status"),
        crate::DelegationFinalization::Partial
    );
    assert_eq!(
        runtime
            .list_delegations_for_parent(parent)
            .await
            .expect("records")
            .len(),
        1
    );
}

#[tokio::test]
async fn detached_delegation_does_not_block_resumed_parent() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home, "test-provider".to_string())
        .await
        .expect("state runtime");
    let parent = ThreadId::new();
    runtime
        .reserve_delegation("delegation-7", "run-7", parent, "/root/detached", 1)
        .await
        .expect("reservation");
    assert!(
        runtime
            .transition_delegation(
                "delegation-7",
                0,
                crate::DurableDelegationStatus::Reserved,
                crate::DurableDelegationStatus::Detached,
                2,
            )
            .await
            .expect("detach")
    );
    assert_eq!(
        runtime
            .delegation_finalization_for_parent(parent)
            .await
            .expect("ready status"),
        crate::DelegationFinalization::Ready
    );
}
