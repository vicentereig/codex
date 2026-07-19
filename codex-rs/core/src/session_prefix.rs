use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::TurnAbortReason;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::approx_token_count;
use codex_utils_output_truncation::truncate_text;

use crate::context::ContextualUserFragment;
use crate::context::InterAgentCompletionMessage;
use crate::context::SubagentNotification;

const COMPLETION_MESSAGE_MAX_TOKENS: usize = 1_000;
const COMPLETION_MESSAGE_TOKEN_SAFETY_RESERVE: usize = 32;
const ERROR_NEXT_ACTION: &str = "This agent's turn failed. If you still need this agent, use the available collaboration tools to give it another task.";

// Helpers for model-visible session state markers that are stored in user-role
// messages but are not user intent.

// TODO(jif) unify with structured schema
pub(crate) fn format_subagent_notification_message(
    agent_reference: &str,
    status: &AgentStatus,
) -> String {
    SubagentNotification::new(agent_reference, status.clone()).render()
}

pub(crate) fn format_inter_agent_completion_message(
    task_name: AgentPath,
    sender: AgentPath,
    status: &AgentStatus,
) -> Option<String> {
    let payload = match status {
        AgentStatus::Completed(Some(message)) => truncate_text(
            message,
            TruncationPolicy::Tokens(completion_payload_max_tokens(&task_name, &sender, "")),
        ),
        AgentStatus::Completed(None) => String::new(),
        AgentStatus::Errored(error) => {
            let error_envelope = format!("Agent errored: \n\n{ERROR_NEXT_ACTION}");
            let error = truncate_text(
                error,
                TruncationPolicy::Tokens(completion_payload_max_tokens(
                    &task_name,
                    &sender,
                    &error_envelope,
                )),
            );
            format!("Agent errored: {error}\n\n{ERROR_NEXT_ACTION}")
        }
        AgentStatus::Shutdown => "Agent shut down.".to_string(),
        AgentStatus::NotFound => "Agent was not found.".to_string(),
        AgentStatus::PendingInit | AgentStatus::Running | AgentStatus::Interrupted => return None,
    };
    Some(InterAgentCompletionMessage::new(task_name, sender, payload).render())
}

/// Renders the terminal outcome of a child turn that remains available for follow-up work.
///
/// Interrupted child threads are intentionally not terminal `AgentStatus` values: their parent
/// can send a later follow-up task. Their interrupted turn is nevertheless a terminal outcome
/// that the parent must see, including when the interruption was caused by a budget limit.
pub(crate) fn format_inter_agent_aborted_message(
    task_name: AgentPath,
    sender: AgentPath,
    reason: TurnAbortReason,
) -> Option<String> {
    let payload = match reason {
        TurnAbortReason::Interrupted => {
            "Agent turn interrupted. The agent can receive a follow-up task.".to_string()
        }
        TurnAbortReason::BudgetLimited => {
            "Agent turn budget limited. The agent can receive a follow-up task.".to_string()
        }
        _ => return None,
    };
    Some(InterAgentCompletionMessage::new(task_name, sender, payload).render())
}

fn completion_payload_max_tokens(
    task_name: &AgentPath,
    sender: &AgentPath,
    payload_envelope: &str,
) -> usize {
    let envelope =
        InterAgentCompletionMessage::new(task_name.clone(), sender.clone(), payload_envelope)
            .render();
    COMPLETION_MESSAGE_MAX_TOKENS
        .saturating_sub(approx_token_count(&envelope))
        .saturating_sub(COMPLETION_MESSAGE_TOKEN_SAFETY_RESERVE)
}

#[cfg(test)]
#[path = "session_prefix_tests.rs"]
mod tests;

pub(crate) fn format_subagent_context_line(
    agent_reference: &str,
    agent_nickname: Option<&str>,
) -> String {
    match agent_nickname.filter(|nickname| !nickname.is_empty()) {
        Some(agent_nickname) => format!("- {agent_reference}: {agent_nickname}"),
        None => format!("- {agent_reference}"),
    }
}
