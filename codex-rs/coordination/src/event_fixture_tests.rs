use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

use crate::BoundedId;
use crate::CompatibilitySourceIdentity;
use crate::CoordinationEvent;
use crate::CoordinationSemanticSlot;
use crate::SourceShape;

const EVENT_ID: &str = "019f7c6c-1111-7000-8000-000000000701";
const ROOT_THREAD: &str = "019f7c6c-1111-7000-8000-000000000601";
const ACTOR_THREAD: &str = "019f7c6c-1111-7000-8000-000000000602";
const TARGET_THREAD: &str = "019f7c6c-1111-7000-8000-000000000603";
const ASSIGNMENT_ID: &str = "019f7c6c-1111-7000-8000-000000000301";
const OPERATION_ID: &str = "019f7c6c-1111-7000-8000-000000000101";
const RECEIPT_ID: &str = "019f7c6c-1111-7000-8000-000000000201";
const RESULT_ID: &str = "019f7c6c-1111-7000-8000-000000000401";
const HANDOFF_ID: &str = "019f7c6c-1111-7000-8000-000000000501";
const STATE_EPOCH: &str = "019f7c6c-1111-7000-8000-000000000801";
const CAUSE_ID: &str = "019f7c6c-1111-7000-8000-000000000700";

fn unavailable(reason: &str) -> Value {
    json!({"status": "unavailable", "reason": reason})
}

fn principal(thread_id: &str, turn_id: &str, path: &str) -> Value {
    json!({
        "threadId": thread_id,
        "turnId": {"status": "known", "value": turn_id},
        "agentPath": {"status": "known", "value": path}
    })
}

fn assignment() -> Value {
    json!({"status": "known", "assignmentId": ASSIGNMENT_ID, "generation": 1})
}

fn target() -> Value {
    json!({
        "principal": principal(TARGET_THREAD, "turn-b", "/root/worker"),
        "assignment": assignment()
    })
}

pub(crate) fn base_event() -> Value {
    json!({
        "eventId": EVENT_ID,
        "rootThreadId": ROOT_THREAD,
        "order": {"mode": "native", "stateEpoch": STATE_EPOCH, "revision": 1},
        "occurredAt": 1_753_000_000,
        "actor": principal(ACTOR_THREAD, "turn-a", "/root"),
        "responsibilityOwner": {
            "status": "known",
            "value": principal(ACTOR_THREAD, "turn-a", "/root")
        },
        "projection": {"threadId": ACTOR_THREAD, "turnId": "turn-a"},
        "causes": {"items": [], "omittedCount": 0},
        "source": {
            "source": "native",
            "schemaVersion": 1,
            "sanitizerVersion": 1,
            "suppressionKeys": {"items": [], "omittedCount": 0}
        }
    })
}

pub(crate) fn compatibility_event() -> Value {
    let mut event = base_event();
    event["eventId"] = json!("641753a2-b8b8-557b-afcf-1c3c17bbbc46");
    event["order"] = json!({"mode": "compatibility", "afterRevision": 1, "sourceOrdinal": 0});
    event["actor"] = principal(ROOT_THREAD, "turn-a", "/root");
    event["projection"] = json!({"threadId": ROOT_THREAD, "turnId": "turn-a"});
    event["source"] = json!({
        "source": "compatibility",
        "adapterVersion": 1,
        "sanitizerVersion": 1,
        "key": {
            "shape": "subAgentActivity",
            "sourceItemId": {"status": "known", "value": "item-1"},
            "sourceOrdinal": 0
        }
    });
    event
}

pub(crate) fn kind_fixtures() -> Vec<Value> {
    let target = target();
    let encrypted = unavailable("encryptedPayload");
    let no_failure = json!({"status": "notApplicable"});
    let wait_target = json!({
        "target": target,
        "observedState": {"status": "known", "value": "active"}
    });
    vec![
        json!({
            "kind": "assignmentRequested", "operationId": OPERATION_ID, "mode": "spawn",
            "target": target, "objective": encrypted, "encodedPayloadBytes": 384,
            "requestedRuntime": {
                "model": {"source": "explicit", "value": "gpt-5.6-sol"},
                "reasoningEffort": {"source": "explicit", "value": "high"}
            }
        }),
        json!({
            "kind": "assignmentAccepted", "operationId": OPERATION_ID, "mode": "spawn",
            "target": target, "receiptId": RECEIPT_ID,
            "boundTurnId": {"status": "known", "value": "turn-b"}
        }),
        json!({
            "kind": "assignmentGenerationClosed", "assignment": assignment(),
            "closeReason": {"reason": "superseded", "byGeneration": 2}
        }),
        json!({
            "kind": "messageSubmissionRecorded", "operationId": OPERATION_ID,
            "target": target, "content": encrypted, "encodedPayloadBytes": 128
        }),
        json!({
            "kind": "messageDurablyReceived", "operationId": OPERATION_ID,
            "target": target, "receiptId": RECEIPT_ID
        }),
        json!({
            "kind": "messageIncludedInModelInput", "operationId": OPERATION_ID,
            "target": target, "receiptId": RECEIPT_ID, "inferenceAttemptId": "inference-1"
        }),
        json!({
            "kind": "waitStarted", "operationId": OPERATION_ID,
            "targets": {"items": [wait_target], "omittedCount": 0}, "timeoutMs": 30_000
        }),
        json!({
            "kind": "waitEnded", "operationId": OPERATION_ID,
            "targets": {"items": [wait_target], "omittedCount": 0},
            "outcome": {"status": "known", "value": "targetTerminal"}, "failure": no_failure
        }),
        json!({"kind": "interruptRequested", "operationId": OPERATION_ID, "target": target}),
        json!({
            "kind": "interruptDurablyReceived", "operationId": OPERATION_ID,
            "target": target, "receiptId": RECEIPT_ID
        }),
        json!({
            "kind": "turnInterrupted", "target": target, "targetTurnId": "turn-b",
            "interruptionReason": {"reason": "requested", "operationId": OPERATION_ID},
            "includedGenerations": {"items": [1], "omittedCount": 0}
        }),
        json!({
            "kind": "detached", "target": target,
            "previousOwner": {"status": "known", "value": principal(ACTOR_THREAD, "turn-a", "/root")}
        }),
        json!({
            "kind": "dependencyDeclared", "operationId": OPERATION_ID,
            "dependent": target, "prerequisite": target
        }),
        json!({
            "kind": "ownershipChanged", "operationId": OPERATION_ID, "target": target,
            "previousOwner": {"status": "known", "value": principal(ACTOR_THREAD, "turn-a", "/root")},
            "newOwner": {"status": "known", "value": principal(TARGET_THREAD, "turn-b", "/root/worker")},
            "changeMode": "explicitTransfer"
        }),
        json!({
            "kind": "turnCompleted", "target": target, "targetTurnId": "turn-b",
            "outcome": "succeeded", "includedGenerations": {"items": [1], "omittedCount": 0}
        }),
        json!({
            "kind": "terminalResultObserved", "resultId": RESULT_ID, "target": target,
            "targetTurnId": "turn-b", "summary": encrypted
        }),
        json!({
            "kind": "handoffDeliveryAttempted", "handoffId": HANDOFF_ID,
            "resultId": RESULT_ID, "attempt": 1, "from": target, "to": target
        }),
        json!({
            "kind": "handoffDurablyReceived", "handoffId": HANDOFF_ID,
            "resultId": RESULT_ID, "attempt": 1, "receiptId": RECEIPT_ID,
            "from": target, "to": target
        }),
        json!({
            "kind": "handoffIncludedInModelInput", "handoffId": HANDOFF_ID,
            "resultId": RESULT_ID, "attempt": 1, "receiptId": RECEIPT_ID,
            "to": target, "inferenceAttemptId": "inference-2"
        }),
        json!({
            "kind": "handoffDeliveryFailed", "handoffId": HANDOFF_ID,
            "resultId": RESULT_ID, "attempt": 1, "from": target, "to": target,
            "code": "targetUnavailable",
            "summary": {"status": "known", "value": "recipient unavailable", "source": "internalError"},
            "retryable": true
        }),
        json!({
            "kind": "legacyInteractionObserved", "observation": "interactionMarker",
            "target": {"status": "known", "value": target},
            "content": unavailable("sourceDoesNotEncode"),
            "reportedSuccess": unavailable("sourceDoesNotEncode")
        }),
    ]
}

pub(crate) fn merge_kind(mut event: Value, kind: Value) -> Value {
    let kind_name = kind["kind"].as_str().expect("kind name");
    if matches!(
        kind_name,
        "assignmentAccepted"
            | "assignmentGenerationClosed"
            | "messageDurablyReceived"
            | "messageIncludedInModelInput"
            | "waitEnded"
            | "interruptDurablyReceived"
            | "turnInterrupted"
            | "terminalResultObserved"
            | "handoffDeliveryAttempted"
            | "handoffDurablyReceived"
            | "handoffIncludedInModelInput"
            | "handoffDeliveryFailed"
    ) {
        event["causes"] = json!({"items": [CAUSE_ID], "omittedCount": 0});
    }
    if matches!(
        kind_name,
        "turnInterrupted" | "turnCompleted" | "terminalResultObserved"
    ) {
        event["projection"] = json!({"threadId": TARGET_THREAD, "turnId": "turn-b"});
    }
    if kind_name == "detached" {
        event["responsibilityOwner"] = json!({"status": "notApplicable"});
    } else if kind_name == "ownershipChanged" {
        event["responsibilityOwner"] = kind["newOwner"].clone();
    }
    event
        .as_object_mut()
        .expect("event object")
        .extend(kind.as_object().expect("kind object").clone());
    event
}

#[test]
fn every_schema_v1_kind_round_trips_literal_valid_json() {
    let fixtures = kind_fixtures();
    assert_eq!(fixtures.len(), 21);
    for kind in fixtures {
        let base = if kind["kind"] == "legacyInteractionObserved" {
            compatibility_event()
        } else {
            base_event()
        };
        let expected = merge_kind(base, kind);
        let event: CoordinationEvent =
            serde_json::from_value(expected.clone()).expect("valid schema-v1 event");
        assert_eq!(
            serde_json::to_value(event).expect("serialize event"),
            expected
        );
    }
}

#[test]
fn event_deserialization_rejects_invalid_checked_scalars_and_lists() {
    let kind = kind_fixtures().remove(0);
    let mut invalid_uuid = merge_kind(base_event(), kind.clone());
    invalid_uuid["eventId"] = json!("44f5a6d2-fbe8-4b3d-a242-0680e3385250");
    assert!(serde_json::from_value::<CoordinationEvent>(invalid_uuid).is_err());

    let mut invalid_turn = merge_kind(base_event(), kind.clone());
    invalid_turn["projection"]["turnId"] = json!("x".repeat(129));
    assert!(serde_json::from_value::<CoordinationEvent>(invalid_turn).is_err());

    let mut too_many_causes = merge_kind(base_event(), kind);
    too_many_causes["causes"] = json!({"items": vec![EVENT_ID; 5], "omittedCount": 0});
    assert!(serde_json::from_value::<CoordinationEvent>(too_many_causes).is_err());

    for path in [("source", "schemaVersion"), ("source", "sanitizerVersion")] {
        let mut invalid = merge_kind(base_event(), kind_fixtures().remove(0));
        invalid[path.0][path.1] = json!(2);
        assert!(serde_json::from_value::<CoordinationEvent>(invalid).is_err());
    }

    for (path, value) in [
        (("source", "adapterVersion"), json!(2)),
        (("source", "sanitizerVersion"), json!(2)),
        (("order", "afterRevision"), json!(i64::MAX as u64 + 1)),
        (("order", "sourceOrdinal"), json!(i64::MAX as u64 + 1)),
        (
            ("source", "key"),
            json!({
                "shape": "subAgentActivity",
                "sourceItemId": {"status": "known", "value": "item-1"},
                "sourceOrdinal": i64::MAX as u64 + 1
            }),
        ),
    ] {
        let mut invalid = merge_kind(compatibility_event(), kind_fixtures().remove(20));
        invalid[path.0][path.1] = value;
        assert!(serde_json::from_value::<CoordinationEvent>(invalid).is_err());
    }

    let mut oversized_payload = merge_kind(base_event(), kind_fixtures().remove(0));
    oversized_payload["encodedPayloadBytes"] = json!(65_537);
    assert!(serde_json::from_value::<CoordinationEvent>(oversized_payload).is_err());
}

fn native_event(kind_index: usize) -> Value {
    merge_kind(base_event(), kind_fixtures().remove(kind_index))
}

fn rejects(event: Value) {
    assert!(
        serde_json::from_value::<CoordinationEvent>(event.clone()).is_err(),
        "accepted contradictory event: {event}"
    );
}

#[test]
fn checked_event_rejects_local_semantic_contradictions() {
    let mut wrong_mode = native_event(0);
    wrong_mode["source"] = compatibility_event()["source"].clone();
    rejects(wrong_mode);

    let mut wrong_projection = native_event(0);
    wrong_projection["projection"]["threadId"] = json!(ROOT_THREAD);
    rejects(wrong_projection);

    let mut exposed_content = native_event(0);
    exposed_content["objective"] =
        json!({"status": "known", "value": "raw prompt", "source": "legacyV1"});
    rejects(exposed_content);

    let mut unknown_assignment = native_event(0);
    unknown_assignment["target"]["assignment"] = unavailable("missingLegacyField");
    rejects(unknown_assignment);

    let mut timed_out_too_late = native_event(6);
    timed_out_too_late["timeoutMs"] = json!(3_600_001);
    rejects(timed_out_too_late);

    let mut contradictory_wait = native_event(7);
    contradictory_wait["outcome"] = json!({"status": "known", "value": "failed"});
    contradictory_wait["failure"] = json!({"status": "notApplicable"});
    rejects(contradictory_wait);

    let mut unavailable_native_wait = native_event(7);
    unavailable_native_wait["outcome"] = unavailable("sourceDoesNotEncode");
    unavailable_native_wait["failure"] = unavailable("sourceDoesNotEncode");
    rejects(unavailable_native_wait);

    let mut detach_keeps_owner = native_event(11);
    detach_keeps_owner["responsibilityOwner"] = json!({
        "status": "known",
        "value": principal(ACTOR_THREAD, "turn-a", "/root")
    });
    rejects(detach_keeps_owner);

    let mut ownership_mismatch = native_event(13);
    ownership_mismatch["responsibilityOwner"] = json!({
        "status": "known",
        "value": principal(ACTOR_THREAD, "turn-a", "/root")
    });
    rejects(ownership_mismatch);

    let mut absent_owner_transfer = native_event(13);
    absent_owner_transfer["previousOwner"] = json!({"status": "notApplicable"});
    serde_json::from_value::<CoordinationEvent>(absent_owner_transfer.clone())
        .expect("explicit transfer may start without an owner");
    absent_owner_transfer["changeMode"] = json!("laterTurnRebind");
    rejects(absent_owner_transfer);

    let mut exposed_result = native_event(15);
    exposed_result["summary"] =
        json!({"status": "known", "value": "raw result", "source": "legacyV1"});
    rejects(exposed_result);

    let mut contradictory_terminal_turn = native_event(14);
    contradictory_terminal_turn["target"]["principal"]["turnId"]["value"] = json!("turn-c");
    rejects(contradictory_terminal_turn);

    let mut wrong_cause_count = native_event(1);
    wrong_cause_count["causes"] = json!({"items": [], "omittedCount": 0});
    rejects(wrong_cause_count);

    let mut omitted_cause = native_event(1);
    omitted_cause["causes"]["omittedCount"] = json!(1);
    rejects(omitted_cause);

    let mut omitted_wait_target = native_event(6);
    omitted_wait_target["targets"]["omittedCount"] = json!(1);
    rejects(omitted_wait_target);

    let mut omitted_generation = native_event(14);
    omitted_generation["includedGenerations"]["omittedCount"] = json!(1);
    rejects(omitted_generation);

    let mut wrong_compatibility_id = merge_kind(compatibility_event(), kind_fixtures().remove(20));
    wrong_compatibility_id["source"]["key"]["sourceItemId"]["value"] = json!("item-2");
    rejects(wrong_compatibility_id);
}

#[test]
fn compatibility_wait_end_never_invents_a_native_cause() {
    let identity = CompatibilitySourceIdentity::new(
        SourceShape::SubAgentActivity,
        Some(ThreadId::try_from(ROOT_THREAD).expect("source thread")),
        Some(BoundedId::new("turn-a").expect("source turn")),
        Some(BoundedId::new("item-1").expect("source item")),
        0,
        CoordinationSemanticSlot::WaitEnded,
    )
    .expect("compatibility identity");
    let mut event = merge_kind(compatibility_event(), kind_fixtures().remove(7));
    event["eventId"] = json!(identity.event_id().to_string());
    event["causes"] = json!({"items": [], "omittedCount": 0});
    event["outcome"] = unavailable("sourceDoesNotEncode");
    event["failure"] = unavailable("sourceDoesNotEncode");

    serde_json::from_value::<CoordinationEvent>(event).expect("checked compatibility wait end");
}

#[test]
fn checked_event_fingerprint_is_stable_and_escaped_size_is_bounded() {
    let json = native_event(0);
    let event: CoordinationEvent = serde_json::from_value(json.clone()).expect("checked event");
    let duplicate: CoordinationEvent = serde_json::from_value(json).expect("duplicate event");
    assert_eq!(event.canonical_bytes(), duplicate.canonical_bytes());
    assert_eq!(event.fingerprint(), duplicate.fingerprint());

    let escaped_turn = "\\".repeat(128);
    let escaped_path = format!("/root/{}", "a".repeat(250));
    let large_wait_target = json!({
        "target": {
            "principal": principal(TARGET_THREAD, &escaped_turn, &escaped_path),
            "assignment": assignment()
        },
        "observedState": {"status": "known", "value": "active"}
    });
    let mut oversized = native_event(6);
    oversized["targets"] = json!({"items": vec![large_wait_target; 8], "omittedCount": 0});
    oversized["source"]["suppressionKeys"] = json!({
        "items": (0..4).map(|ordinal| json!({
            "shape": "subAgentActivity",
            "sourceItemId": {"status": "known", "value": escaped_turn},
            "sourceOrdinal": ordinal
        })).collect::<Vec<_>>(),
        "omittedCount": 0
    });
    let error = serde_json::from_value::<CoordinationEvent>(oversized).expect_err("over 8 KiB");
    assert!(
        error.to_string().contains("coordinationEvent exceeds"),
        "unexpected error: {error}"
    );
}
