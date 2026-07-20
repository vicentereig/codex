use super::display_identity::AgentDisplayIdentity;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use unicode_segmentation::UnicodeSegmentation;

const THREAD_ID: &str = "019dabc1-0ef5-7431-b81c-03037f51f62c";

fn thread_id() -> ThreadId {
    ThreadId::from_string(THREAD_ID).expect("valid thread id")
}

fn path(path: &str) -> AgentPath {
    AgentPath::try_from(path).expect("valid agent path")
}

#[test]
fn identity_normalizes_presentation_hints() {
    let identity = AgentDisplayIdentity::new(
        thread_id(),
        Some(path("/root/researcher")),
        Some("  Ada  ".to_string()),
        Some("  reviewer  ".to_string()),
    );

    assert_eq!(
        identity,
        AgentDisplayIdentity {
            thread_id: thread_id(),
            agent_path: Some(path("/root/researcher")),
            nickname: Some("Ada".to_string()),
            role: Some("reviewer".to_string()),
        }
    );
}

#[test]
fn identity_uses_friendly_label_and_canonical_path_when_available() {
    let identity = AgentDisplayIdentity::new(
        thread_id(),
        Some(path("/root/researcher")),
        Some("Ada".to_string()),
        Some("reviewer".to_string()),
    );

    assert_eq!(identity.primary_label(), "Ada [reviewer]");
    assert_eq!(
        identity.contextual_label(),
        "Ada [reviewer] · /root/researcher"
    );
    assert_eq!(
        identity.technical_detail(),
        "thread 019dabc1-0ef5-7431-b81c-03037f51f62c"
    );
    assert_eq!(
        identity.search_text(),
        "Ada [reviewer] · /root/researcher thread 019dabc1-0ef5-7431-b81c-03037f51f62c"
    );
}

#[test]
fn identity_uses_path_when_presentation_metadata_is_missing() {
    let identity = AgentDisplayIdentity::new(
        thread_id(),
        Some(path("/root/researcher")),
        Some("  ".to_string()),
        Some("worker".to_string()),
    );

    assert_eq!(identity.primary_label(), "/root/researcher");
    assert_eq!(identity.contextual_label(), "/root/researcher");
    assert!(!identity.contextual_label().contains(THREAD_ID));
    assert!(identity.search_text().contains(THREAD_ID));
}

#[test]
fn identity_uses_a_bounded_short_id_when_path_and_nickname_are_missing() {
    let identity = AgentDisplayIdentity::new(
        thread_id(),
        None,
        Some(" ".to_string()),
        Some("reviewer".to_string()),
    );

    assert_eq!(identity.primary_label(), "[reviewer] · Agent (7f51f62c)");
    assert_eq!(identity.contextual_label(), "[reviewer] · Agent (7f51f62c)");
    assert!(!identity.contextual_label().contains(THREAD_ID));
    assert_eq!(
        identity.technical_detail(),
        "thread 019dabc1-0ef5-7431-b81c-03037f51f62c"
    );
}

#[test]
fn bounded_fallback_uses_the_random_bearing_uuid_suffix() {
    let first = AgentDisplayIdentity::new(thread_id(), None, None, None);
    let second = AgentDisplayIdentity::new(
        ThreadId::from_string("019dabc1-0ef5-7431-b81c-03037f51f62d").expect("valid thread id"),
        None,
        None,
        None,
    );

    assert_eq!(first.primary_label(), "Agent (7f51f62c)");
    assert_eq!(second.primary_label(), "Agent (7f51f62d)");
    assert_ne!(first.contextual_label(), second.contextual_label());
}

#[test]
fn duplicate_nicknames_are_disambiguated_by_canonical_path() {
    let first = AgentDisplayIdentity::new(
        thread_id(),
        Some(path("/root/research")),
        Some("Scout".to_string()),
        None,
    );
    let second = AgentDisplayIdentity::new(
        ThreadId::from_string("019dabc1-0ef5-7431-b81c-03037f51f62d").expect("valid thread id"),
        Some(path("/root/review")),
        Some("Scout".to_string()),
        None,
    );

    assert_eq!(first.primary_label(), second.primary_label());
    assert_ne!(first.contextual_label(), second.contextual_label());
    assert_eq!(first.contextual_label(), "Scout · /root/research");
    assert_eq!(second.contextual_label(), "Scout · /root/review");
}

#[test]
fn identity_bounds_long_paths_while_preserving_both_ends() {
    let long_path = format!("/root/{}", "a".repeat(160));
    let identity = AgentDisplayIdentity::new(thread_id(), Some(path(&long_path)), None, None);

    let label = identity.contextual_label();
    assert_eq!(label.graphemes(true).count(), 96);
    assert!(label.starts_with("/root/"));
    assert!(label.contains('…'));
    assert!(label.ends_with(&"a".repeat(32)));
    assert!(identity.search_text().graphemes(true).count() < 160);
}

#[test]
fn truncated_paths_keep_a_stable_middle_discriminator() {
    let shared_prefix = "a".repeat(70);
    let shared_suffix = "z".repeat(70);
    let first = AgentDisplayIdentity::new(
        thread_id(),
        Some(path(&format!("/root/{shared_prefix}one{shared_suffix}"))),
        Some("Scout".to_string()),
        None,
    );
    let second = AgentDisplayIdentity::new(
        ThreadId::from_string("019dabc1-0ef5-7431-b81c-03037f51f62d").expect("valid thread id"),
        Some(path(&format!("/root/{shared_prefix}two{shared_suffix}"))),
        Some("Scout".to_string()),
        None,
    );

    assert_ne!(first.contextual_label(), second.contextual_label());
    assert_eq!(first.contextual_label().graphemes(true).count(), 104);
    assert_eq!(second.contextual_label().graphemes(true).count(), 104);
}

#[test]
fn identity_normalizes_controls_and_bounds_wide_presentation_hints() {
    let identity = AgentDisplayIdentity::new(
        thread_id(),
        None,
        Some(format!("  Ada\n\t\u{7} {}  ", "界".repeat(60))),
        Some(format!("reviewer\r{}", "界".repeat(40))),
    );

    let nickname = identity.nickname.as_deref().expect("normalized nickname");
    let role = identity.role.as_deref().expect("normalized role");
    assert_eq!(nickname.graphemes(true).count(), 48);
    assert_eq!(role.graphemes(true).count(), 32);
    assert!(!nickname.chars().any(char::is_control));
    assert!(!role.chars().any(char::is_control));
    assert!(nickname.starts_with("Ada 界"));
    assert!(role.starts_with("reviewer 界"));
}
