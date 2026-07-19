use codex_protocol::AgentPath;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::truncate_text;

use super::ContextualUserFragment;

const MAX_RENDERED_AGENT_PATH_TOKENS: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InterAgentCompletionMessage {
    task_name: AgentPath,
    sender: AgentPath,
    payload: String,
}

impl InterAgentCompletionMessage {
    pub(crate) fn new(task_name: AgentPath, sender: AgentPath, payload: impl Into<String>) -> Self {
        Self {
            task_name,
            sender,
            payload: payload.into(),
        }
    }
}

impl ContextualUserFragment for InterAgentCompletionMessage {
    fn role(&self) -> &'static str {
        "assistant"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("", "")
    }

    fn body(&self) -> String {
        let task_name = truncate_text(
            self.task_name.as_str(),
            TruncationPolicy::Tokens(MAX_RENDERED_AGENT_PATH_TOKENS),
        );
        let sender = truncate_text(
            self.sender.as_str(),
            TruncationPolicy::Tokens(MAX_RENDERED_AGENT_PATH_TOKENS),
        );
        format!(
            "Message Type: FINAL_ANSWER\nTask name: {}\nSender: {}\nPayload:\n{}",
            task_name, sender, self.payload,
        )
    }
}
