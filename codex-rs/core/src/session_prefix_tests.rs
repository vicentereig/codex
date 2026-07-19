use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use codex_utils_output_truncation::approx_token_count;

use super::COMPLETION_MESSAGE_MAX_TOKENS;
use super::ERROR_NEXT_ACTION;
use super::format_inter_agent_completion_message;

fn long_agent_path() -> AgentPath {
    AgentPath::try_from(format!("/root/{}", "a".repeat(10_000))).expect("valid agent path")
}

#[test]
fn successful_completion_message_stays_below_manual_review_threshold() {
    let message = format_inter_agent_completion_message(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("valid agent path"),
        &AgentStatus::Completed(Some("result ".repeat(1_000))),
    )
    .expect("completed status should produce a completion message");

    assert!(approx_token_count(&message) < COMPLETION_MESSAGE_MAX_TOKENS);
}

#[test]
fn completion_messages_with_long_agent_paths_stay_below_manual_review_threshold() {
    let path = long_agent_path();
    for status in [
        AgentStatus::Completed(Some("result ".repeat(1_000))),
        AgentStatus::Completed(None),
        AgentStatus::Errored("stream disconnected ".repeat(1_000)),
        AgentStatus::Shutdown,
        AgentStatus::NotFound,
    ] {
        let message = format_inter_agent_completion_message(path.clone(), path.clone(), &status)
            .expect("terminal status should produce a completion message");

        assert!(approx_token_count(&message) < COMPLETION_MESSAGE_MAX_TOKENS);
    }
}

#[test]
fn error_completion_message_stays_below_manual_review_threshold() {
    let message = format_inter_agent_completion_message(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("valid agent path"),
        &AgentStatus::Errored("stream disconnected ".repeat(1_000)),
    )
    .expect("error status should produce a completion message");

    assert!(approx_token_count(&message) < COMPLETION_MESSAGE_MAX_TOKENS);
    assert!(message.contains(ERROR_NEXT_ACTION));
}
