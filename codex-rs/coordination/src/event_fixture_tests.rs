use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

use super::event::CoordinationEvent;

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

fn base_event() -> Value {
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
        "projection": {"threadId": ROOT_THREAD, "turnId": "turn-a"},
        "causes": {"items": [], "omittedCount": 0},
        "source": {
            "source": "native",
            "schemaVersion": 1,
            "sanitizerVersion": 1,
            "suppressionKeys": {"items": [], "omittedCount": 0}
        }
    })
}

fn compatibility_event() -> Value {
    let mut event = base_event();
    event["eventId"] = json!("641753a2-b8b8-557b-afcf-1c3c17bbbc46");
    event["order"] = json!({"mode": "compatibility", "afterRevision": 1, "sourceOrdinal": 0});
    event["actor"] = principal(ROOT_THREAD, "turn-a", "/root");
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

fn kind_fixtures() -> Vec<Value> {
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

fn merge_kind(mut event: Value, kind: Value) -> Value {
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
