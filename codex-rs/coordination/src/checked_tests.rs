use codex_protocol::AgentPath;
use pretty_assertions::assert_eq;

use super::*;

#[test]
fn coordination_ids_are_non_nil_uuid_strings() {
    let literal = "019f7c6c-1111-7000-8000-000000000101";
    let id = CoordinationOperationId::parse(literal).expect("valid operation ID");

    assert_eq!(id.to_string(), literal);
    assert_eq!(
        serde_json::to_string(&id).expect("serialize"),
        format!("\"{literal}\"")
    );
    assert_eq!(
        serde_json::from_str::<CoordinationOperationId>(&format!("\"{literal}\""))
            .expect("deserialize"),
        id
    );
    assert!(CoordinationOperationId::parse("not-a-uuid").is_err());
    assert!(CoordinationOperationId::parse("00000000-0000-0000-0000-000000000000").is_err());
    assert!(CoordinationOperationId::parse("44f5a6d2-fbe8-4b3d-a242-0680e3385250").is_err());
    assert!(CoordinationOperationId::parse("993ecc79-57df-5f58-91fe-29b7fd3184d7").is_err());
    assert!(CoordinationEventId::parse("993ecc79-57df-5f58-91fe-29b7fd3184d7").is_ok());
    assert!(
        serde_json::from_str::<CoordinationOperationId>("\"44f5a6d2-fbe8-4b3d-a242-0680e3385250\"")
            .is_err()
    );
    assert!(
        serde_json::from_str::<CoordinationOperationId>("\"993ecc79-57df-5f58-91fe-29b7fd3184d7\"")
            .is_err()
    );
    assert!(
        serde_json::from_str::<CoordinationEventId>("\"993ecc79-57df-5f58-91fe-29b7fd3184d7\"")
            .is_ok()
    );
    assert_eq!(
        CoordinationOperationId::new_v7().as_uuid().get_version(),
        Some(uuid::Version::SortRand)
    );
}

#[test]
fn bounded_ids_enforce_utf8_bytes_and_controls() {
    let exact = "a".repeat(MAX_ID_BYTES);
    let bounded = BoundedId::<MAX_ID_BYTES>::new(&exact).expect("exact cap");
    assert_eq!(bounded.as_str(), exact);

    assert!(BoundedId::<MAX_ID_BYTES>::new("").is_err());
    assert!(BoundedId::<MAX_ID_BYTES>::new("turn\n1").is_err());
    assert!(BoundedId::<MAX_ID_BYTES>::new("é".repeat(65)).is_err());
    assert!(serde_json::from_str::<BoundedId<4>>("\"12345\"").is_err());
    assert_eq!(
        serde_json::from_str::<BoundedId<MAX_ID_BYTES>>(
            &serde_json::to_string(&bounded).expect("serialize")
        )
        .expect("deserialize"),
        bounded
    );
}

#[test]
fn coordination_path_adds_its_own_byte_cap() {
    let exact = AgentPath::try_from(format!("/root/{}", "a".repeat(250))).expect("protocol path");
    let over = AgentPath::try_from(format!("/root/{}", "a".repeat(251))).expect("protocol path");

    assert_eq!(exact.as_str().len(), MAX_AGENT_PATH_BYTES);
    let exact = CoordinationAgentPath::new(exact).expect("exact cap");
    assert_eq!(
        serde_json::from_str::<CoordinationAgentPath>(
            &serde_json::to_string(&exact).expect("serialize")
        )
        .expect("deserialize"),
        exact
    );
    assert!(CoordinationAgentPath::new(over).is_err());
}

#[test]
fn sanitized_text_redacts_normalizes_and_truncates_at_a_scalar_boundary() {
    let secret = "sk-abcdefghijklmnopqrstuvwxyz123456";
    let bearer = "Bearer abcdefghijklmnopqrstuvwxyz.123456";
    let aws_key = "AKIA1234567890ABCDEF";
    let assignment = "password=supersecretvalue";
    let text = SanitizedText::from_untrusted(&format!(
        "  {secret}  hello\r\n\0  {bearer} {aws_key} {assignment}  {}",
        "é".repeat(300),
    ));
    let boundary_text = SanitizedText::from_untrusted(&format!("{} {secret}", "x".repeat(500)));

    assert!(!text.as_str().contains(secret));
    assert!(!text.as_str().contains("abcdefghijklmnopqrstuvwxyz.123456"));
    assert!(!text.as_str().contains(aws_key));
    assert!(!text.as_str().contains("supersecretvalue"));
    assert!(text.as_str().contains("[REDACTED_SECRET]"));
    assert!(!boundary_text.as_str().contains("sk-abcdef"));
    assert!(!text.as_str().contains('\0'));
    assert!(!text.as_str().contains("  "));
    assert!(text.as_str().len() <= MAX_TEXT_BYTES);
    assert!(text.as_str().ends_with('…'));
    assert!(serde_json::from_str::<SanitizedText>(&format!("\"{secret}\"")).is_err());
    assert_eq!(
        serde_json::from_str::<SanitizedText>(&serde_json::to_string(&text).expect("serialize"))
            .expect("canonical sanitized text"),
        text
    );
}

#[test]
fn generation_and_revision_use_sqlite_safe_positive_ranges() {
    let generation = AssignmentGeneration::new(i32::MAX as u32).expect("max");
    assert_eq!(generation.get(), i32::MAX as u32);
    assert!(AssignmentGeneration::new(0).is_err());
    assert!(AssignmentGeneration::new(i32::MAX as u32 + 1).is_err());
    let revision = CoordinationRevision::new(i64::MAX as u64).expect("max");
    assert_eq!(revision.get(), i64::MAX as u64);
    assert!(CoordinationRevision::new(0).is_err());
    assert!(CoordinationRevision::new(i64::MAX as u64 + 1).is_err());
    assert_eq!(
        serde_json::from_str::<AssignmentGeneration>(
            &serde_json::to_string(&generation).expect("serialize")
        )
        .expect("deserialize"),
        generation
    );
    assert_eq!(
        serde_json::from_str::<CoordinationRevision>(
            &serde_json::to_string(&revision).expect("serialize")
        )
        .expect("deserialize"),
        revision
    );
}

#[test]
fn event_numeric_scalars_enforce_frozen_wire_bounds() {
    let version = CoordinationSchemaVersion::current();
    assert_eq!(version.get(), 1);
    assert_eq!(
        serde_json::from_str::<CoordinationSchemaVersion>("1").expect("version 1"),
        version
    );
    assert!(serde_json::from_str::<CoordinationSchemaVersion>("2").is_err());

    let ordinal = CompatibilityOrdinal::new(i64::MAX as u64).expect("max ordinal");
    assert_eq!(ordinal.get(), i64::MAX as u64);
    assert_eq!(
        serde_json::from_str::<CompatibilityOrdinal>(&ordinal.get().to_string())
            .expect("ordinal round trip"),
        ordinal
    );
    assert!(CompatibilityOrdinal::new(i64::MAX as u64 + 1).is_err());

    let bytes = EncodedPayloadBytes::new(MAX_CIPHERTEXT_BYTES).expect("ciphertext cap");
    assert_eq!(bytes.get(), MAX_CIPHERTEXT_BYTES);
    assert_eq!(
        serde_json::from_str::<EncodedPayloadBytes>(&bytes.get().to_string())
            .expect("payload round trip"),
        bytes
    );
    assert!(EncodedPayloadBytes::new(MAX_CIPHERTEXT_BYTES + 1).is_err());
}

#[test]
fn bounded_lists_reject_overflow_and_inconsistent_omissions() {
    let list = BoundedList::<u8, 2>::new(vec![1, 2], 3).expect("truncated list");
    assert_eq!(
        serde_json::to_value(&list).expect("serialize"),
        serde_json::json!({"items": [1, 2], "omittedCount": 3})
    );
    assert_eq!(
        serde_json::from_value::<BoundedList<u8, 2>>(
            serde_json::to_value(&list).expect("serialize")
        )
        .expect("deserialize"),
        list
    );
    assert!(BoundedList::<u8, 2>::complete(vec![1, 2, 3]).is_err());
    assert!(BoundedList::<u8, 2>::new(vec![1], 1).is_err());
    assert!(
        serde_json::from_value::<BoundedList<u8, 2>>(serde_json::json!({
            "items": [1, 2, 3], "omittedCount": 0
        }))
        .is_err()
    );
}
