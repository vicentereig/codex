use codex_protocol::ThreadId;
use codex_protocol::protocol::InterAgentCommunication;

const AGENT_COMMUNICATION_TARGET: &str = "codex_otel.agent_communication";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AgentCommunicationKind {
    Spawn,
    Message,
    Followup,
    Result,
}

impl AgentCommunicationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Spawn => "spawn",
            Self::Message => "message",
            Self::Followup => "followup",
            Self::Result => "result",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentCommunicationContext {
    kind: AgentCommunicationKind,
    sender_thread_id: ThreadId,
}

impl AgentCommunicationContext {
    pub(crate) fn new(kind: AgentCommunicationKind, sender_thread_id: ThreadId) -> Self {
        Self {
            kind,
            sender_thread_id,
        }
    }
}

pub(crate) fn logging_enabled() -> bool {
    tracing::enabled!(target: AGENT_COMMUNICATION_TARGET, tracing::Level::INFO)
}

pub(crate) fn emit_agent_communication_send(
    communication_id: &str,
    context: &AgentCommunicationContext,
    communication: &InterAgentCommunication,
    receiver_thread_id: ThreadId,
) {
    tracing::info!(
        target: AGENT_COMMUNICATION_TARGET,
        {
            event.name = "codex.agent_communication",
            communication_id,
            kind = context.kind.as_str(),
            state = "send",
            sender_thread_id = %context.sender_thread_id,
            receiver_thread_id = %receiver_thread_id,
            content = if communication.content.is_empty() {
                communication.encrypted_content.as_deref().unwrap_or_default()
            } else {
                communication.content.as_str()
            },
        },
        "agent communication"
    );
}

/// Metadata-only sibling of [`emit_agent_communication_send`] for the enabled coordination path
/// (Stage 3 contract freeze, Decision 9). `communication` is passed in full -- the same shape a
/// real call site has available -- but this function deliberately never reads its `content` or
/// `encrypted_content` fields; it logs identity/generation metadata only. See
/// `agent_communication_metadata_only_tests` for the sentinel test proving no plaintext/ciphertext
/// leak, including a positive-control comparison against `emit_agent_communication_send`.
pub(crate) fn emit_agent_communication_send_metadata_only(
    receipt_id: &str,
    operation_id: &str,
    context: &AgentCommunicationContext,
    communication: &InterAgentCommunication,
    receiver_thread_id: ThreadId,
    captured_generation: Option<u32>,
) {
    tracing::info!(
        target: AGENT_COMMUNICATION_TARGET,
        {
            event.name = "codex.agent_communication",
            receipt_id,
            operation_id,
            kind = context.kind.as_str(),
            state = "send",
            sender_thread_id = %context.sender_thread_id,
            receiver_thread_id = %receiver_thread_id,
            trigger_turn = communication.trigger_turn,
            captured_generation = captured_generation.unwrap_or_default(),
        },
        "agent communication (coordinated, metadata only)"
    );
}

pub(crate) fn emit_agent_communication_receive(communication_id: &str) {
    tracing::info!(
        target: AGENT_COMMUNICATION_TARGET,
        {
            event.name = "codex.agent_communication",
            communication_id,
            state = "receive",
        },
        "agent communication"
    );
}

#[cfg(test)]
mod agent_communication_metadata_only_tests {
    use super::*;
    use codex_protocol::AgentPath;

    const PLAINTEXT_CANARY: &str = "sentinel-plaintext-canary-do-not-log";
    const CIPHERTEXT_CANARY: &str = "sentinel-ciphertext-canary-do-not-log";

    fn thread_id(value: u128) -> ThreadId {
        ThreadId::from_string(&uuid::Uuid::from_u128(value).to_string())
            .expect("valid uuid string parses into ThreadId")
    }

    fn canary_communication() -> InterAgentCommunication {
        let mut communication = InterAgentCommunication::new(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            Vec::new(),
            PLAINTEXT_CANARY.to_string(),
            /*trigger_turn*/ true,
        );
        communication.encrypted_content = Some(CIPHERTEXT_CANARY.to_string());
        communication
    }

    /// Positive control: the *existing*, disabled-mode payload logger really does log the
    /// canary content today. This proves the sentinel test below is discriminating something
    /// real, not merely asserting a tautology about a function that was never given the payload.
    #[test]
    #[tracing_test::traced_test]
    fn existing_payload_logger_does_log_content() {
        let context = AgentCommunicationContext::new(AgentCommunicationKind::Message, thread_id(1));
        emit_agent_communication_send(
            "communication-id",
            &context,
            &canary_communication(),
            thread_id(2),
        );
        assert!(
            logs_contain(PLAINTEXT_CANARY),
            "expected the disabled-mode logger to contain the plaintext canary as a baseline"
        );
    }

    /// The actual guarantee Decision 9 requires: the enabled path's metadata-only logger, even
    /// though it is handed the identical communication (with the identical canary payload) in
    /// scope, must never emit that payload in its tracing output.
    #[test]
    #[tracing_test::traced_test]
    fn metadata_only_logger_never_logs_content_or_ciphertext() {
        let context =
            AgentCommunicationContext::new(AgentCommunicationKind::Followup, thread_id(1));
        emit_agent_communication_send_metadata_only(
            "receipt-id-canary",
            "operation-id-canary",
            &context,
            &canary_communication(),
            thread_id(2),
            Some(3),
        );
        assert!(
            !logs_contain(PLAINTEXT_CANARY),
            "metadata-only logger must never log plaintext content"
        );
        assert!(
            !logs_contain(CIPHERTEXT_CANARY),
            "metadata-only logger must never log encrypted content"
        );
        // The metadata itself must still be present -- this isn't merely a no-op logger.
        assert!(logs_contain("receipt-id-canary"));
        assert!(logs_contain("operation-id-canary"));
        assert!(logs_contain("trigger_turn=true"));
    }
}
