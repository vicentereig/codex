use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn cancellation_before_bind_prevents_initial_delivery() {
    let ledger = DelegationLedger::new();
    let path = AgentPath::try_from("/root/worker").expect("path");
    let reservation = ledger.reserve(path.clone()).await;

    assert_eq!(ledger.cancel_pending().await, Vec::<ThreadId>::new());
    assert_eq!(
        ledger.bind(reservation, ThreadId::new()).await,
        DelegationBinding::Cancelled
    );
    assert_eq!(
        ledger.record(reservation).await,
        Some((path, None, DelegationState::Cancelled))
    );
}

#[tokio::test]
async fn bound_child_is_returned_for_cancellation_outside_ledger_lock() {
    let ledger = DelegationLedger::new();
    let reservation = ledger
        .reserve(AgentPath::try_from("/root/worker").expect("path"))
        .await;
    let child_thread_id = ThreadId::new();

    assert_eq!(
        ledger.bind(reservation, child_thread_id).await,
        DelegationBinding::Active
    );
    assert_eq!(ledger.cancel_pending().await, vec![child_thread_id]);
    assert_eq!(ledger.cancel_pending().await, Vec::<ThreadId>::new());
}

#[tokio::test]
async fn failed_reservation_remains_terminal_when_bound_late() {
    let ledger = DelegationLedger::new();
    let reservation = ledger
        .reserve(AgentPath::try_from("/root/worker").expect("path"))
        .await;
    ledger.fail(reservation).await;

    assert_eq!(
        ledger.bind(reservation, ThreadId::new()).await,
        DelegationBinding::Cancelled
    );
}

#[tokio::test]
async fn completion_wins_over_later_cancellation() {
    let ledger = DelegationLedger::new();
    let reservation = ledger
        .reserve(AgentPath::try_from("/root/worker").expect("path"))
        .await;
    let child_thread_id = ThreadId::new();
    let _ = ledger.bind(reservation, child_thread_id).await;

    ledger
        .settle(child_thread_id, DelegationState::Completed)
        .await;
    assert_eq!(ledger.cancel_pending().await, Vec::<ThreadId>::new());
    assert_eq!(
        ledger.record(reservation).await.map(|record| record.2),
        Some(DelegationState::Completed)
    );
}

#[tokio::test]
async fn detached_child_no_longer_blocks_required_outcome() {
    let ledger = DelegationLedger::new();
    let reservation = ledger
        .reserve(AgentPath::try_from("/root/worker").expect("path"))
        .await;
    let child_thread_id = ThreadId::new();
    let _ = ledger.bind(reservation, child_thread_id).await;

    assert!(ledger.detach(child_thread_id).await);
    assert_eq!(
        ledger.wait_for_required_outcome().await,
        Vec::<AgentPath>::new()
    );
}
