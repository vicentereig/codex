use super::*;

fn thread_id(value: u128) -> codex_protocol::ThreadId {
    codex_protocol::ThreadId::from_string(&uuid::Uuid::from_u128(value).to_string())
        .expect("valid uuid string parses into ThreadId")
}

fn identity(value: u128) -> PreallocatedThreadIdentity {
    PreallocatedThreadIdentity {
        thread_id: thread_id(value),
        turn_id: format!("turn-{value}"),
    }
}

fn operation_id() -> CoordinationOperationId {
    use super::super::operation_identity::OperationIdentityKey;
    use super::super::operation_identity::OperationIdentityMap;
    use super::super::operation_identity::SemanticSlot;
    OperationIdentityMap::new().resolve(OperationIdentityKey {
        root_thread_id: thread_id(1),
        actor_thread_id: thread_id(2),
        actor_turn_id: "turn".to_string(),
        call_id: "call".to_string(),
        semantic_slot: SemanticSlot::Spawn,
    })
}

#[test]
fn reserve_intent_is_idempotent_for_the_same_operation_id() {
    let ledger = SpawnReservationLedger::new();
    let operation_id = operation_id();

    let first = ledger.reserve_intent(operation_id, || identity(1));
    let second = ledger.reserve_intent(operation_id, || identity(2));

    assert_eq!(
        first, second,
        "duplicate reservation must reuse the identity"
    );
    assert_eq!(
        ledger.len(),
        1,
        "duplicate reservation must not create a second entry"
    );
}

#[test]
fn advance_never_regresses_stage() {
    let ledger = SpawnReservationLedger::new();
    let operation_id = operation_id();
    ledger.reserve_intent(operation_id, || identity(1));

    ledger.advance(operation_id, SpawnReservationStage::Acknowledged);
    ledger.advance(operation_id, SpawnReservationStage::IntentReserved);

    assert_eq!(
        ledger.stage(operation_id),
        Some(SpawnReservationStage::Acknowledged)
    );
}

#[test]
fn advance_on_unreserved_operation_is_a_no_op() {
    let ledger = SpawnReservationLedger::new();
    let operation_id = operation_id();

    ledger.advance(operation_id, SpawnReservationStage::Acknowledged);

    assert_eq!(ledger.stage(operation_id), None);
}

#[test]
fn stage_progression_matches_the_frozen_ordering() {
    let ledger = SpawnReservationLedger::new();
    let operation_id = operation_id();
    ledger.reserve_intent(operation_id, || identity(1));
    assert_eq!(
        ledger.stage(operation_id),
        Some(SpawnReservationStage::IntentReserved)
    );

    ledger.advance(operation_id, SpawnReservationStage::ReceiptAccepted);
    assert_eq!(
        ledger.stage(operation_id),
        Some(SpawnReservationStage::ReceiptAccepted)
    );

    ledger.advance(operation_id, SpawnReservationStage::ChildCreated);
    assert_eq!(
        ledger.stage(operation_id),
        Some(SpawnReservationStage::ChildCreated)
    );

    ledger.advance(operation_id, SpawnReservationStage::Acknowledged);
    assert_eq!(
        ledger.stage(operation_id),
        Some(SpawnReservationStage::Acknowledged)
    );
}

#[test]
fn failure_injector_fires_only_at_its_configured_point() {
    let injector = SpawnFailureInjector::fail_at(SpawnFailurePoint::AfterReceipt);

    assert!(injector.check(SpawnFailurePoint::BeforeIntent).is_ok());
    assert!(injector.check(SpawnFailurePoint::AfterIntent).is_ok());
    assert!(injector.check(SpawnFailurePoint::AfterReceipt).is_err());
    assert!(
        injector
            .check(SpawnFailurePoint::BeforeChildCreation)
            .is_ok()
    );
    assert!(
        injector
            .check(SpawnFailurePoint::AfterSideEffectBeforeAck)
            .is_ok()
    );
}

#[test]
fn no_op_failure_injector_never_fires() {
    let injector = SpawnFailureInjector::none();
    for point in [
        SpawnFailurePoint::BeforeIntent,
        SpawnFailurePoint::AfterIntent,
        SpawnFailurePoint::AfterReceipt,
        SpawnFailurePoint::BeforeChildCreation,
        SpawnFailurePoint::AfterSideEffectBeforeAck,
    ] {
        assert!(injector.check(point).is_ok());
    }
}
