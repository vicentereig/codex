use std::collections::BTreeMap;

use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use serde::Serialize;

use super::*;

const SOURCE_THREAD: &str = "019f7c6c-1111-7000-8000-000000000601";
const ACTOR_THREAD: &str = "019f7c6c-1111-7000-8000-000000000602";
const OPERATION_ID: &str = "019f7c6c-1111-7000-8000-000000000101";

fn bounded(value: &str) -> BoundedId<MAX_ID_BYTES> {
    BoundedId::new(value).expect("bounded fixture")
}

fn source_thread() -> ThreadId {
    ThreadId::try_from(SOURCE_THREAD).expect("thread ID")
}

fn key(slot: CoordinationSemanticSlot) -> IdempotencyKey {
    IdempotencyKey::new(
        source_thread(),
        ThreadId::try_from(ACTOR_THREAD).expect("actor thread"),
        bounded("turn-a"),
        CoordinationOperationId::parse(OPERATION_ID).expect("operation ID"),
        slot,
    )
}

#[test]
fn compatibility_uuid_matches_interaction_marker_vector() {
    let identity = CompatibilitySourceIdentity::new(
        CompatibilitySourceShape::SubAgentActivity,
        Some(source_thread()),
        Some(bounded("turn-a")),
        Some(bounded("item-1")),
        0,
        CoordinationSemanticSlot::LegacyInteractionObserved,
    )
    .expect("identity");

    assert_eq!(
        identity.event_id().to_string(),
        "641753a2-b8b8-557b-afcf-1c3c17bbbc46"
    );
}

#[test]
fn compatibility_uuid_matches_absent_item_vector() {
    let identity = CompatibilitySourceIdentity::new(
        CompatibilitySourceShape::TurnComplete,
        Some(source_thread()),
        Some(bounded("turn-a")),
        None,
        12,
        CoordinationSemanticSlot::TurnCompleted,
    )
    .expect("identity");

    assert_eq!(
        identity.event_id().to_string(),
        "a52bf714-d9ad-57f1-bad8-5f98037694ea"
    );
}

#[test]
fn compatibility_ordinal_is_sqlite_safe() {
    assert!(
        CompatibilitySourceIdentity::new(
            CompatibilitySourceShape::TurnComplete,
            None,
            None,
            None,
            i64::MAX as u64 + 1,
            CoordinationSemanticSlot::TurnCompleted,
        )
        .is_err()
    );
}

#[derive(Serialize)]
struct ReverseFieldOrder {
    beta: u8,
    alpha: BTreeMap<&'static str, u8>,
}

#[derive(Serialize)]
struct ForwardFieldOrder {
    alpha: BTreeMap<&'static str, u8>,
    beta: u8,
}

#[test]
fn content_fingerprint_is_independent_of_json_object_order() {
    let alpha = BTreeMap::from([("first", 1), ("second", 2)]);
    let reverse = ReverseFieldOrder {
        beta: 3,
        alpha: alpha.clone(),
    };
    let forward = ForwardFieldOrder { alpha, beta: 3 };
    assert_ne!(
        serde_json::to_vec(&reverse).expect("reverse JSON"),
        serde_json::to_vec(&forward).expect("forward JSON")
    );

    let first = IdempotencyRecord::from_serializable(
        key(CoordinationSemanticSlot::AssignmentRequested),
        &reverse,
    )
    .expect("record");
    let duplicate = IdempotencyRecord::from_serializable(
        key(CoordinationSemanticSlot::AssignmentRequested),
        &forward,
    )
    .expect("record");

    assert_eq!(first.compare(&duplicate), Ok(IdempotencyMatch::Duplicate));
    assert_eq!(first.content_fingerprint(), duplicate.content_fingerprint());
}

#[test]
fn same_key_with_divergent_content_is_an_explicit_conflict() {
    let existing = IdempotencyRecord::from_serializable(
        key(CoordinationSemanticSlot::AssignmentRequested),
        &serde_json::json!({"mode": "spawn"}),
    )
    .expect("record");
    let incoming = IdempotencyRecord::from_serializable(
        key(CoordinationSemanticSlot::AssignmentRequested),
        &serde_json::json!({"mode": "followup"}),
    )
    .expect("record");

    assert!(matches!(
        existing.compare(&incoming),
        Err(IdempotencyConflict::DivergentContent { .. })
    ));
}

#[test]
fn a_different_semantic_slot_is_a_distinct_idempotency_key() {
    let requested = IdempotencyRecord::from_serializable(
        key(CoordinationSemanticSlot::AssignmentRequested),
        &serde_json::json!({"mode": "spawn"}),
    )
    .expect("record");
    let accepted = IdempotencyRecord::from_serializable(
        key(CoordinationSemanticSlot::AssignmentAccepted),
        &serde_json::json!({"mode": "spawn"}),
    )
    .expect("record");

    assert_eq!(
        requested.compare(&accepted),
        Ok(IdempotencyMatch::DistinctKey)
    );
    assert_ne!(requested.key().fingerprint(), accepted.key().fingerprint());
    assert!(!requested.key().tuple_bytes().starts_with(b"{"));
}
