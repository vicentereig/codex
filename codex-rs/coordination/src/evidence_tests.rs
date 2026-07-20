use pretty_assertions::assert_eq;

use super::*;

fn bounded(value: &str) -> BoundedId<MAX_ID_BYTES> {
    BoundedId::new(value).expect("bounded fixture")
}

#[test]
fn evidence_uses_the_closed_wire_shape() {
    let known = Evidence::Known {
        value: bounded("turn-a"),
    };
    let unavailable = Evidence::<BoundedId<MAX_ID_BYTES>>::Unavailable {
        reason: UnavailableReason::SourceDoesNotEncode,
    };
    let not_applicable = Evidence::<BoundedId<MAX_ID_BYTES>>::NotApplicable;

    assert_eq!(
        serde_json::to_value(known).expect("known evidence"),
        serde_json::json!({"status": "known", "value": "turn-a"})
    );
    assert_eq!(
        serde_json::to_value(unavailable).expect("unavailable evidence"),
        serde_json::json!({
            "status": "unavailable",
            "reason": "sourceDoesNotEncode"
        })
    );
    assert_eq!(
        serde_json::to_value(not_applicable).expect("not-applicable evidence"),
        serde_json::json!({"status": "notApplicable"})
    );
}

#[test]
fn requested_runtime_records_request_semantics_only() {
    let runtime = RequestedRuntime::new(
        Requested::Explicit {
            value: bounded("gpt-5.6-sol"),
        },
        Requested::Inherited,
    );

    let json = serde_json::to_value(&runtime).expect("runtime JSON");
    assert_eq!(
        json,
        serde_json::json!({
            "model": {"source": "explicit", "value": "gpt-5.6-sol"},
            "reasoningEffort": {"source": "inherited"}
        })
    );
    assert!(json.get("effectiveModel").is_none());
    assert!(json.get("effectiveReasoningEffort").is_none());
    assert_eq!(
        serde_json::from_value::<RequestedRuntime>(json).expect("runtime round trip"),
        runtime
    );
}

#[test]
fn unavailable_requested_runtime_retains_why() {
    let runtime = RequestedRuntime::new(
        Requested::Unavailable {
            reason: UnavailableReason::MissingLegacyField,
        },
        Requested::Explicit {
            value: ReasoningEffort::Xhigh,
        },
    );

    assert_eq!(
        serde_json::to_value(runtime).expect("runtime JSON"),
        serde_json::json!({
            "model": {"source": "unavailable", "reason": "missingLegacyField"},
            "reasoningEffort": {"source": "explicit", "value": "xhigh"}
        })
    );
}
