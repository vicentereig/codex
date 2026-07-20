use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::BoundedId;
use codex_coordination::BoundedList;
use codex_coordination::ContentEvidence;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationPrincipal;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::CoordinationSource;
use codex_coordination::CoordinationTarget;
use codex_coordination::EncodedPayloadBytes;
use codex_coordination::Evidence;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::RequestedRuntime;
use codex_protocol::ThreadId;
use serde_json::json;

use crate::model::coordination::AssignmentReservation;
use crate::model::coordination::NativeEventContext;
use crate::model::coordination::NativeEventIdentity;
use crate::model::coordination::ReserveAssignment;

pub(super) const ROOT: &str = "019f7c6c-1111-7000-8000-000000000601";
pub(super) const ASSIGNMENT: &str = "019f7c6c-1111-7000-8000-000000000301";
pub(super) const OPERATION: &str = "019f7c6c-1111-7000-8000-000000000101";

pub(super) fn thread(value: &str) -> ThreadId {
    ThreadId::try_from(value).expect("thread")
}

pub(super) fn turn(value: &str) -> BoundedId<MAX_ID_BYTES> {
    BoundedId::new(value).expect("turn")
}

pub(super) fn reserve_params() -> ReserveAssignment {
    const CHILD: &str = "019f7c6c-1111-7000-8000-000000000603";
    const EVENT: &str = "019f7c6c-1111-7000-8000-000000000701";
    let root = thread(ROOT);
    let actor: CoordinationPrincipal = serde_json::from_value(json!({
        "threadId": ROOT,
        "turnId": {"status":"known","value":"turn-a"},
        "agentPath": {"status":"known","value":"/root"}
    }))
    .expect("actor");
    let target_principal: CoordinationPrincipal = serde_json::from_value(json!({
        "threadId": CHILD,
        "turnId": {"status":"known","value":"turn-b"},
        "agentPath": {"status":"known","value":"/root/worker"}
    }))
    .expect("target");
    let operation_id = CoordinationOperationId::parse(OPERATION).expect("operation");
    ReserveAssignment {
        context: NativeEventContext {
            root_thread_id: root,
            expected_root_revision: 0,
            occurred_at: 1_753_000_000,
            actor: actor.clone(),
            responsibility_owner: Evidence::Known { value: actor },
            source: CoordinationSource::Native {
                schema_version: codex_coordination::CoordinationSchemaVersion::current(),
                sanitizer_version: codex_coordination::SanitizerVersion::current(),
                suppression_keys: BoundedList::new(Vec::new(), /*omitted_count*/ 0).expect("keys"),
            },
            primary: NativeEventIdentity {
                event_id: CoordinationEventId::parse(EVENT).expect("event"),
                operation_id,
            },
            secondary: BoundedList::new(Vec::new(), /*omitted_count*/ 0).expect("secondary"),
        },
        assignment_id: AssignmentId::parse(ASSIGNMENT).expect("assignment"),
        child_thread_id: thread(CHILD),
        reservation: AssignmentReservation::Spawn,
        operation_id,
        target_principal,
        objective: serde_json::from_value::<ContentEvidence>(
            json!({"status":"unavailable","reason":"encryptedPayload"}),
        )
        .expect("objective"),
        encoded_payload_bytes: EncodedPayloadBytes::new(384).expect("bytes"),
        requested_runtime: serde_json::from_value::<RequestedRuntime>(json!({
            "model":{"source":"explicit","value":"gpt-5.6-sol"},
            "reasoningEffort":{"source":"explicit","value":"high"}
        }))
        .expect("runtime"),
    }
}

pub(super) fn generation(value: u32) -> AssignmentGeneration {
    AssignmentGeneration::new(value).expect("generation")
}

pub(super) fn target(value: u32) -> CoordinationTarget {
    const CHILD: &str = "019f7c6c-1111-7000-8000-000000000603";
    serde_json::from_value(json!({
        "principal": {
            "threadId": CHILD,
            "turnId": {"status":"known","value":"turn-b"},
            "agentPath": {"status":"known","value":"/root/worker"}
        },
        "assignment": {"status":"known","assignmentId":ASSIGNMENT,"generation":value}
    }))
    .expect("target")
}

pub(super) fn context(
    _slot: CoordinationSemanticSlot,
    event_id: &str,
    operation_id: &str,
    target_actor: bool,
    expected_root_revision: u64,
    secondary: Vec<(CoordinationSemanticSlot, &str, &str)>,
) -> NativeEventContext {
    const CHILD: &str = "019f7c6c-1111-7000-8000-000000000603";
    let root = thread(ROOT);
    let root_actor: CoordinationPrincipal = serde_json::from_value(json!({
        "threadId": ROOT,
        "turnId": {"status":"known","value":"turn-a"},
        "agentPath": {"status":"known","value":"/root"}
    }))
    .expect("actor");
    let child_actor: CoordinationPrincipal = serde_json::from_value(json!({
        "threadId": CHILD,
        "turnId": {"status":"known","value":"turn-b"},
        "agentPath": {"status":"known","value":"/root/worker"}
    }))
    .expect("actor");
    let actor = if target_actor {
        child_actor
    } else {
        root_actor.clone()
    };
    let identity = |event_id: &str, operation_id: &str| NativeEventIdentity {
        event_id: CoordinationEventId::parse(event_id).expect("event"),
        operation_id: CoordinationOperationId::parse(operation_id).expect("operation"),
    };
    NativeEventContext {
        root_thread_id: root,
        expected_root_revision,
        occurred_at: 1_753_000_000,
        actor,
        responsibility_owner: Evidence::Known { value: root_actor },
        source: CoordinationSource::Native {
            schema_version: codex_coordination::CoordinationSchemaVersion::current(),
            sanitizer_version: codex_coordination::SanitizerVersion::current(),
            suppression_keys: BoundedList::new(Vec::new(), /*omitted_count*/ 0).expect("keys"),
        },
        primary: identity(event_id, operation_id),
        secondary: BoundedList::new(
            secondary
                .into_iter()
                .map(|(_, event, operation)| identity(event, operation))
                .collect(),
            /*omitted_count*/ 0,
        )
        .expect("secondary"),
    }
}
