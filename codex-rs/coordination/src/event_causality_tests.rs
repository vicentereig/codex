use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

use crate::BoundedId;
use crate::CoordinationEvent;
use crate::CoordinationOperationId;
use crate::CoordinationSemanticSlot;
use crate::IdempotencyConflict;
use crate::IdempotencyKey;
use crate::IdempotencyMatch;
use crate::IdempotencyRecord;
use crate::MAX_ID_BYTES;
use crate::event_fixture_tests::base_event;
use crate::event_fixture_tests::kind_fixtures;
use crate::event_fixture_tests::merge_kind;

const ROOT_THREAD: &str = "019f7c6c-1111-7000-8000-000000000601";
const ACTOR_THREAD: &str = "019f7c6c-1111-7000-8000-000000000602";
const OPERATION_ID: &str = "019f7c6c-1111-7000-8000-000000000101";
const CAUSE_EVENT_ID: &str = "019f7c6c-1111-7000-8000-000000000711";
const CONSEQUENCE_EVENT_ID: &str = "019f7c6c-1111-7000-8000-000000000712";

fn event(kind_index: usize, revision: u64, event_id: &str, cause_id: Option<&str>) -> Value {
    let mut value = merge_kind(base_event(), kind_fixtures().remove(kind_index));
    value["eventId"] = json!(event_id);
    value["order"]["revision"] = json!(revision);
    value["causes"] = match cause_id {
        Some(cause_id) => json!({"items": [cause_id], "omittedCount": 0}),
        None => json!({"items": [], "omittedCount": 0}),
    };
    value
}

fn checked(value: Value) -> CoordinationEvent {
    serde_json::from_value(value).expect("checked event")
}

fn pair(cause_kind: usize, consequence_kind: usize) -> (CoordinationEvent, CoordinationEvent) {
    (
        checked(event(
            cause_kind,
            1,
            CAUSE_EVENT_ID,
            causal_dummy(cause_kind),
        )),
        checked(event(
            consequence_kind,
            2,
            CONSEQUENCE_EVENT_ID,
            Some(CAUSE_EVENT_ID),
        )),
    )
}

fn causal_dummy(kind_index: usize) -> Option<&'static str> {
    matches!(
        kind_index,
        1 | 2 | 4 | 5 | 7 | 9 | 10 | 11 | 12 | 13 | 15..=19
    )
    .then_some("019f7c6c-1111-7000-8000-000000000710")
}

#[test]
fn validates_every_representable_exact_causal_edge() {
    for (cause_kind, consequence_kind) in [
        (0, 1),
        (3, 4),
        (4, 5),
        (6, 7),
        (8, 9),
        (9, 10),
        (14, 15),
        (15, 16),
        (16, 17),
        (17, 18),
        (16, 19),
    ] {
        let (cause, consequence) = pair(cause_kind, consequence_kind);
        consequence
            .validate_resolved_causes(&[&cause])
            .expect("valid causal edge");
    }
}

#[test]
fn validates_superseding_request_as_generation_close_cause() {
    for cause_kind in [0, 1] {
        let mut followup = event(cause_kind, 2, CAUSE_EVENT_ID, causal_dummy(cause_kind));
        followup["mode"] = json!("followup");
        followup["target"]["assignment"]["generation"] = json!(2);
        let mut close = event(2, 3, CONSEQUENCE_EVENT_ID, Some(CAUSE_EVENT_ID));
        close["assignment"]["generation"] = json!(1);
        close["closeReason"] = json!({"reason": "superseded", "byGeneration": 2});
        checked(close)
            .validate_resolved_causes(&[&checked(followup)])
            .expect("later generation request or acceptance supersedes the closed generation");
    }
}

#[test]
fn validates_reason_directed_generation_close_causes() {
    for (cause_kind, close_reason) in [
        (14, json!({"reason": "turnCompleted", "turnId": "turn-b"})),
        (10, json!({"reason": "turnInterrupted", "turnId": "turn-b"})),
        (
            0,
            json!({"reason": "deliveryFailed", "code": "targetUnavailable"}),
        ),
        (0, json!({"reason": "abandonedBeforeAcceptance"})),
    ] {
        let cause = checked(event(
            cause_kind,
            1,
            CAUSE_EVENT_ID,
            causal_dummy(cause_kind),
        ));
        let mut close = event(2, 2, CONSEQUENCE_EVENT_ID, Some(CAUSE_EVENT_ID));
        close["closeReason"] = close_reason;
        checked(close)
            .validate_resolved_causes(&[&cause])
            .expect("reason-directed generation close");
    }
}

#[test]
fn validates_terminal_result_from_completed_or_interrupted_turn() {
    for terminal_kind in [10, 14] {
        let terminal = checked(event(
            terminal_kind,
            1,
            CAUSE_EVENT_ID,
            causal_dummy(terminal_kind),
        ));
        checked(event(15, 2, CONSEQUENCE_EVENT_ID, Some(CAUSE_EVENT_ID)))
            .validate_resolved_causes(&[&terminal])
            .expect("terminal observation is caused by either terminal turn fact");
    }
}

#[test]
fn rejects_unresolved_cross_root_non_prior_and_compatibility_causes() {
    let (cause, consequence) = pair(0, 1);
    assert!(consequence.validate_resolved_causes(&[]).is_err());

    let wrong_id = checked(event(0, 1, "019f7c6c-1111-7000-8000-000000000799", None));
    assert!(consequence.validate_resolved_causes(&[&wrong_id]).is_err());

    let mut cross_root = event(0, 1, CAUSE_EVENT_ID, None);
    cross_root["rootThreadId"] = json!("019f7c6c-1111-7000-8000-000000000699");
    assert!(
        consequence
            .validate_resolved_causes(&[&checked(cross_root)])
            .is_err()
    );

    let non_prior = checked(event(0, 2, CAUSE_EVENT_ID, None));
    assert!(consequence.validate_resolved_causes(&[&non_prior]).is_err());

    let compatibility_id = "641753a2-b8b8-557b-afcf-1c3c17bbbc46";
    let compatibility = checked(merge_kind(
        crate::event_fixture_tests::compatibility_event(),
        kind_fixtures().remove(20),
    ));
    let native_with_compatibility_cause =
        checked(event(1, 2, CONSEQUENCE_EVENT_ID, Some(compatibility_id)));
    assert!(
        native_with_compatibility_cause
            .validate_resolved_causes(&[&compatibility])
            .is_err()
    );

    let mut other_epoch = event(0, 1, CAUSE_EVENT_ID, None);
    other_epoch["order"]["stateEpoch"] = json!("019f7c6c-1111-7000-8000-000000000899");
    assert!(
        consequence
            .validate_resolved_causes(&[&checked(other_epoch)])
            .is_err()
    );
    assert_eq!(
        cause.envelope().root_thread_id,
        consequence.envelope().root_thread_id
    );
}

#[test]
fn rejects_mismatched_correlated_fields_and_cause_kinds() {
    let (cause, consequence) = pair(0, 1);
    let mut wrong_operation = serde_json::to_value(cause).expect("serialize cause");
    wrong_operation["operationId"] = json!("019f7c6c-1111-7000-8000-000000000199");
    assert!(
        consequence
            .validate_resolved_causes(&[&checked(wrong_operation)])
            .is_err()
    );

    let unrelated = checked(event(3, 1, CAUSE_EVENT_ID, None));
    assert!(consequence.validate_resolved_causes(&[&unrelated]).is_err());
}

#[test]
fn rejects_the_full_correlated_field_contradiction_matrix() {
    let other_operation = json!("019f7c6c-1111-7000-8000-000000000199");
    let other_receipt = json!("019f7c6c-1111-7000-8000-000000000299");
    let other_result = json!("019f7c6c-1111-7000-8000-000000000499");
    let other_handoff = json!("019f7c6c-1111-7000-8000-000000000599");
    let generation = json!(2);
    for (cause_kind, consequence_kind, path, value) in [
        (0, 1, "/mode", json!("followup")),
        (0, 1, "/target/assignment/generation", generation.clone()),
        (3, 4, "/operationId", other_operation.clone()),
        (3, 4, "/target/assignment/generation", generation.clone()),
        (4, 5, "/operationId", other_operation.clone()),
        (4, 5, "/target/assignment/generation", generation.clone()),
        (4, 5, "/receiptId", other_receipt.clone()),
        (6, 7, "/operationId", other_operation.clone()),
        (
            6,
            7,
            "/targets/items/0/target/assignment/generation",
            generation.clone(),
        ),
        (8, 9, "/operationId", other_operation.clone()),
        (8, 9, "/target/assignment/generation", generation.clone()),
        (9, 10, "/operationId", other_operation),
        (9, 10, "/target/assignment/generation", generation.clone()),
        (14, 15, "/target/assignment/generation", generation.clone()),
        (15, 16, "/resultId", other_result.clone()),
        (15, 16, "/target/assignment/generation", generation.clone()),
        (16, 17, "/handoffId", other_handoff.clone()),
        (16, 17, "/resultId", other_result.clone()),
        (16, 17, "/attempt", generation.clone()),
        (16, 17, "/from/assignment/generation", generation.clone()),
        (16, 17, "/to/assignment/generation", generation.clone()),
        (17, 18, "/handoffId", other_handoff.clone()),
        (17, 18, "/resultId", other_result.clone()),
        (17, 18, "/attempt", generation.clone()),
        (17, 18, "/receiptId", other_receipt),
        (17, 18, "/to/assignment/generation", generation.clone()),
        (16, 19, "/handoffId", other_handoff),
        (16, 19, "/resultId", other_result),
        (16, 19, "/attempt", generation.clone()),
        (16, 19, "/from/assignment/generation", generation.clone()),
        (16, 19, "/to/assignment/generation", generation),
    ] {
        let mut cause = event(cause_kind, 1, CAUSE_EVENT_ID, causal_dummy(cause_kind));
        *cause.pointer_mut(path).expect("fixture path") = value;
        let consequence = checked(event(
            consequence_kind,
            2,
            CONSEQUENCE_EVENT_ID,
            Some(CAUSE_EVENT_ID),
        ));
        assert!(
            consequence
                .validate_resolved_causes(&[&checked(cause)])
                .is_err(),
            "accepted contradictory {cause_kind}->{consequence_kind} at {path}"
        );
    }
}

#[test]
fn rejects_generation_close_reason_contradictions() {
    let mut superseding = event(0, 1, CAUSE_EVENT_ID, None);
    superseding["mode"] = json!("followup");
    superseding["target"]["assignment"]["generation"] = json!(2);
    for (path, value) in [
        ("/mode", json!("spawn")),
        ("/target/assignment/generation", json!(1)),
        (
            "/target/assignment/assignmentId",
            json!("019f7c6c-1111-7000-8000-000000000399"),
        ),
    ] {
        let mut cause = superseding.clone();
        *cause.pointer_mut(path).expect("fixture path") = value;
        let mut close = event(2, 2, CONSEQUENCE_EVENT_ID, Some(CAUSE_EVENT_ID));
        close["closeReason"] = json!({"reason": "superseded", "byGeneration": 2});
        assert!(
            checked(close)
                .validate_resolved_causes(&[&checked(cause)])
                .is_err()
        );
    }

    let mut not_later = superseding.clone();
    not_later["target"]["assignment"]["generation"] = json!(1);
    let mut invalid_close = event(2, 2, CONSEQUENCE_EVENT_ID, Some(CAUSE_EVENT_ID));
    invalid_close["closeReason"] = json!({"reason": "superseded", "byGeneration": 1});
    assert!(
        checked(invalid_close)
            .validate_resolved_causes(&[&checked(not_later)])
            .is_err()
    );

    for (cause_kind, reason, mutation_path, mutation) in [
        (
            14,
            json!({"reason": "turnCompleted", "turnId": "turn-z"}),
            "/includedGenerations/items",
            json!([1]),
        ),
        (
            14,
            json!({"reason": "turnCompleted", "turnId": "turn-b"}),
            "/includedGenerations/items",
            json!([]),
        ),
        (
            14,
            json!({"reason": "turnCompleted", "turnId": "turn-b"}),
            "/target/assignment/assignmentId",
            json!("019f7c6c-1111-7000-8000-000000000399"),
        ),
        (
            10,
            json!({"reason": "turnInterrupted", "turnId": "turn-b"}),
            "/includedGenerations/items",
            json!([]),
        ),
        (
            10,
            json!({"reason": "turnInterrupted", "turnId": "turn-z"}),
            "/includedGenerations/items",
            json!([1]),
        ),
        (
            10,
            json!({"reason": "turnInterrupted", "turnId": "turn-b"}),
            "/target/assignment/assignmentId",
            json!("019f7c6c-1111-7000-8000-000000000399"),
        ),
        (
            0,
            json!({"reason": "abandonedBeforeAcceptance"}),
            "/target/assignment/generation",
            json!(2),
        ),
        (
            0,
            json!({"reason": "deliveryFailed", "code": "targetUnavailable"}),
            "/target/assignment/assignmentId",
            json!("019f7c6c-1111-7000-8000-000000000399"),
        ),
    ] {
        let mut cause = event(cause_kind, 1, CAUSE_EVENT_ID, causal_dummy(cause_kind));
        *cause.pointer_mut(mutation_path).expect("fixture path") = mutation;
        let mut close = event(2, 2, CONSEQUENCE_EVENT_ID, Some(CAUSE_EVENT_ID));
        close["closeReason"] = reason;
        assert!(
            checked(close)
                .validate_resolved_causes(&[&checked(cause)])
                .is_err()
        );
    }
}

#[test]
fn rejects_terminal_result_turn_mismatch() {
    let mut terminal = event(14, 1, CAUSE_EVENT_ID, None);
    terminal["targetTurnId"] = json!("turn-c");
    terminal["target"]["principal"]["turnId"] = json!({"status": "known", "value": "turn-c"});
    terminal["projection"]["turnId"] = json!("turn-c");
    let result = checked(event(15, 2, CONSEQUENCE_EVENT_ID, Some(CAUSE_EVENT_ID)));
    assert!(
        result
            .validate_resolved_causes(&[&checked(terminal)])
            .is_err()
    );
}

#[test]
fn event_fingerprint_drives_duplicate_and_divergent_idempotency() {
    let first = checked(event(0, 1, CAUSE_EVENT_ID, None));
    let key = idempotency_key();
    let existing = IdempotencyRecord::from_event(key.clone(), &first);
    let duplicate = IdempotencyRecord::from_event(key.clone(), &first);
    assert_eq!(
        existing.compare(&duplicate),
        Ok(IdempotencyMatch::Duplicate)
    );

    let mut divergent = event(0, 1, CAUSE_EVENT_ID, None);
    divergent["occurredAt"] = json!(1_753_000_001_i64);
    let divergent = IdempotencyRecord::from_event(key, &checked(divergent));
    assert!(matches!(
        existing.compare(&divergent),
        Err(IdempotencyConflict::DivergentContent { .. })
    ));
}

fn idempotency_key() -> IdempotencyKey {
    IdempotencyKey::new(
        ThreadId::from_string(ROOT_THREAD).expect("root thread"),
        ThreadId::from_string(ACTOR_THREAD).expect("actor thread"),
        BoundedId::<MAX_ID_BYTES>::new("turn-a").expect("turn ID"),
        CoordinationOperationId::parse(OPERATION_ID).expect("operation ID"),
        CoordinationSemanticSlot::AssignmentRequested,
    )
}
