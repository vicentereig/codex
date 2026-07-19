use super::*;
use crate::tools::handlers::multi_agents_spec::create_detach_agent_tool_v2;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("detach_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_detach_agent_tool_v2()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move { handle_detach_agent(invocation).await.map(boxed_tool_output) })
    }
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

async fn handle_detach_agent(
    invocation: ToolInvocation,
) -> Result<DetachAgentResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        ..
    } = invocation;
    let args: DetachAgentArgs = parse_arguments(&function_arguments(payload)?)?;
    let agent_id = resolve_agent_target(&session, &turn, &args.target).await?;
    let agent = session
        .services
        .agent_control
        .ensure_agent_known(agent_id)
        .map_err(|err| collab_agent_error(agent_id, err))?;
    if agent_id == session.thread_id || agent.agent_path.as_ref().is_some_and(AgentPath::is_root) {
        return Err(FunctionCallError::RespondToModel(
            "only a spawned child can be detached".to_string(),
        ));
    }
    let agent_path = agent.agent_path.ok_or_else(|| {
        FunctionCallError::RespondToModel("target agent is missing an agent_path".to_string())
    })?;
    let caller_path = turn
        .session_source
        .get_agent_path()
        .unwrap_or_else(AgentPath::root);
    if agent_path
        .as_str()
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        != Some(caller_path.as_str())
    {
        return Err(FunctionCallError::RespondToModel(
            "only a direct child of this turn can be detached".to_string(),
        ));
    }
    let previous_status = session.services.agent_control.get_status(agent_id).await;
    if !turn.delegation_ledger.detach(agent_id).await {
        return Err(FunctionCallError::RespondToModel(
            "target is not a pending child owned by this turn".to_string(),
        ));
    }
    Ok(DetachAgentResult { previous_status })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DetachAgentArgs {
    target: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct DetachAgentResult {
    pub(crate) previous_status: AgentStatus,
}

impl ToolOutput for DetachAgentResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "detach_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "detach_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "detach_agent")
    }
}
