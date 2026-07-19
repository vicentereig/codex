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
        .reserve_delegation("delegation-1", "run-1", parent, "turn-1", "/root/worker", 1)
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
        .reserve_delegation(
            "delegation-2",
            "run-2",
            ThreadId::new(),
            "turn-1",
            "/root/worker",
            1,
        )
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
        .reserve_delegation(
            "delegation-3",
            "run-3",
            ThreadId::new(),
            "turn-1",
            "/root/worker",
            1,
        )
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
        .reserve_delegation("delegation-4", "run-4", parent, "turn-1", "/root/worker", 1)
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
    assert_eq!(
        runtime
            .reconcile_delegation_deliveries_once(3, 10, 2, 10)
            .await
            .expect("bounded reconciliation"),
        1
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
        .reserve_delegation(
            "delegation-5",
            "run-5",
            ThreadId::new(),
            "turn-1",
            "/root/worker",
            1,
        )
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
            .request_delegation_cancel("delegation-5", 1, 0, 3)
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
            .confirm_delegation_cancel("delegation-5", 2, 0, 5)
            .await
            .expect("duplicate cancellation confirmation")
    );
    assert!(
        !runtime
            .request_delegation_cancel("delegation-5", 3, 0, 5)
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
        .reserve_delegation("delegation-6", "run-6", parent, "turn-1", "/root/failed", 1)
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
    assert_eq!(
        runtime
            .list_delegations_for_parent(parent)
            .await
            .expect("record")
            .first()
            .expect("delegation")
            .parent_turn_id,
        "turn-1"
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
        .reserve_delegation(
            "delegation-7",
            "run-7",
            parent,
            "turn-1",
            "/root/detached",
            1,
        )
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

#[tokio::test]
async fn missing_child_recovery_never_invents_completion() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home, "test-provider".to_string())
        .await
        .expect("state runtime");
    let parent = ThreadId::new();
    runtime
        .reserve_delegation(
            "delegation-8",
            "run-8",
            parent,
            "turn-1",
            "/root/missing",
            1,
        )
        .await
        .expect("reservation");
    assert!(
        runtime
            .transition_delegation(
                "delegation-8",
                0,
                crate::DurableDelegationStatus::Reserved,
                crate::DurableDelegationStatus::Retryable,
                2,
            )
            .await
            .expect("retryable transition")
    );
    assert_eq!(
        runtime
            .delegation_finalization_for_parent(parent)
            .await
            .expect("retryable status"),
        crate::DelegationFinalization::Blocked
    );
    assert!(
        runtime
            .reconcile_missing_delegation("delegation-8", 1, 1, 1, 3)
            .await
            .expect("missing child reconciliation")
    );
    assert_eq!(
        runtime
            .delegation_finalization_for_parent(parent)
            .await
            .expect("unknown status"),
        crate::DelegationFinalization::Blocked
    );
}

#[tokio::test]
async fn terminal_acknowledgement_retains_then_collects_record() {
    let home = super::test_support::unique_temp_dir();
    let runtime = StateRuntime::init(home, "test-provider".to_string())
        .await
        .expect("state runtime");
    let parent = ThreadId::new();
    runtime
        .reserve_delegation("delegation-9", "run-9", parent, "turn-9", "/root/worker", 1)
        .await
        .expect("reservation");
    assert!(
        runtime
            .transition_delegation(
                "delegation-9",
                0,
                crate::DurableDelegationStatus::Reserved,
                crate::DurableDelegationStatus::Completed,
                2,
            )
            .await
            .expect("completion")
    );
    assert!(
        runtime
            .acknowledge_delegation("delegation-9", 1, 3, 10)
            .await
            .expect("acknowledgement")
    );
    assert_eq!(
        runtime
            .list_delegations_for_parent(parent)
            .await
            .expect("retained record")
            .len(),
        1
    );
    assert_eq!(
        runtime
            .gc_acknowledged_delegations(13, 10)
            .await
            .expect("gc"),
        1
    );
}
