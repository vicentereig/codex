use super::*;

fn thread_id_from_u128(value: u128) -> ThreadId {
    ThreadId::from_string(&uuid::Uuid::from_u128(value).to_string())
        .expect("valid uuid string parses into ThreadId")
}

fn key(
    root: u128,
    actor: u128,
    actor_turn_id: &str,
    call_id: &str,
    semantic_slot: SemanticSlot,
) -> OperationIdentityKey {
    OperationIdentityKey {
        root_thread_id: thread_id_from_u128(root),
        actor_thread_id: thread_id_from_u128(actor),
        actor_turn_id: actor_turn_id.to_string(),
        call_id: call_id.to_string(),
        semantic_slot,
    }
}

#[test]
fn identical_key_resolves_to_identical_operation_id_on_retry() {
    let map = OperationIdentityMap::new();
    let first = map.resolve(key(1, 2, "turn-1", "call-1", SemanticSlot::Spawn));
    let second = map.resolve(key(1, 2, "turn-1", "call-1", SemanticSlot::Spawn));
    assert_eq!(first, second);
    assert_eq!(map.len(), 1);
}

#[test]
fn key_differing_in_root_thread_is_a_distinct_operation() {
    let map = OperationIdentityMap::new();
    let first = map.resolve(key(1, 2, "turn-1", "call-1", SemanticSlot::Spawn));
    let second = map.resolve(key(9, 2, "turn-1", "call-1", SemanticSlot::Spawn));
    assert_ne!(first, second);
    assert_eq!(map.len(), 2);
}

#[test]
fn key_differing_in_actor_thread_is_a_distinct_operation() {
    let map = OperationIdentityMap::new();
    let first = map.resolve(key(1, 2, "turn-1", "call-1", SemanticSlot::Spawn));
    let second = map.resolve(key(1, 9, "turn-1", "call-1", SemanticSlot::Spawn));
    assert_ne!(first, second);
}

#[test]
fn key_differing_in_actor_turn_id_is_a_distinct_operation() {
    let map = OperationIdentityMap::new();
    let first = map.resolve(key(1, 2, "turn-1", "call-1", SemanticSlot::Spawn));
    let second = map.resolve(key(1, 2, "turn-2", "call-1", SemanticSlot::Spawn));
    assert_ne!(first, second);
}

#[test]
fn key_differing_in_call_id_is_a_distinct_operation() {
    let map = OperationIdentityMap::new();
    let first = map.resolve(key(1, 2, "turn-1", "call-1", SemanticSlot::Spawn));
    let second = map.resolve(key(1, 2, "turn-1", "call-2", SemanticSlot::Spawn));
    assert_ne!(first, second);
}

#[test]
fn operation_ids_are_uuidv7() {
    let map = OperationIdentityMap::new();
    let id = map.resolve(key(1, 2, "turn-1", "call-1", SemanticSlot::Spawn));
    assert_eq!(id.as_uuid().get_version_num(), 7);
}
