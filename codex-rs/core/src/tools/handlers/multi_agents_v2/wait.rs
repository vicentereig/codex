use super::*;
use crate::agent::status::is_final;
use crate::session::InputQueueActivity;
use crate::tools::handlers::multi_agents_spec::WaitAgentTimeoutOptions;
use crate::tools::handlers::multi_agents_spec::create_wait_agent_tool_v2;
use codex_tools::ToolSpec;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::timeout_at;

#[derive(Default)]
pub(crate) struct Handler {
    options: WaitAgentTimeoutOptions,
}

impl Handler {
    pub(crate) fn new(options: WaitAgentTimeoutOptions) -> Self {
        Self { options }
    }
}

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("wait_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_wait_agent_tool_v2(self.options)
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(self.handle_call(invocation))
    }
}

impl Handler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            payload,
            call_id,
            ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: WaitArgs = parse_arguments(&arguments)?;
        let min_timeout_ms = turn.config.multi_agent_v2.min_wait_timeout_ms;
        let max_timeout_ms = turn.config.multi_agent_v2.max_wait_timeout_ms;
        let default_timeout_ms = turn.config.multi_agent_v2.default_wait_timeout_ms;
        let timeout_ms = match args.timeout_ms {
            Some(ms) if ms < min_timeout_ms => {
                return Err(FunctionCallError::RespondToModel(format!(
                    "timeout_ms must be at least {min_timeout_ms}"
                )));
            }
            Some(ms) if ms > max_timeout_ms => {
                return Err(FunctionCallError::RespondToModel(format!(
                    "timeout_ms must be at most {max_timeout_ms}"
                )));
            }
            Some(ms) => ms,
            None => default_timeout_ms,
        };

        if let Some(targets) = args.targets {
            return self
                .wait_for_target_statuses(session, turn, call_id, targets, timeout_ms)
                .await;
        }

        let turn_state = session
            .input_queue
            .turn_state_for_sub_id(&session.active_turn, &turn.sub_id)
            .await;
        let (mut activity_rx, pending_activity) = session
            .input_queue
            .subscribe_activity(turn_state.as_deref())
            .await;

        session
            .emit_turn_item_started(
                &turn,
                &TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                    id: call_id.clone(),
                    tool: CollabAgentTool::Wait,
                    status: CollabAgentToolCallStatus::InProgress,
                    sender_thread_id: session.thread_id,
                    receiver_thread_ids: Vec::new(),
                    receiver_agents: Vec::new(),
                    prompt: None,
                    model: None,
                    reasoning_effort: None,
                    agents_states: Default::default(),
                }),
            )
            .await;

        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
        let outcome = wait_for_activity(&mut activity_rx, pending_activity, deadline).await;
        let result = WaitAgentResult::from_outcome(outcome);

        session
            .emit_turn_item_completed(
                &turn,
                TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                    id: call_id,
                    tool: CollabAgentTool::Wait,
                    status: CollabAgentToolCallStatus::Completed,
                    sender_thread_id: session.thread_id,
                    receiver_thread_ids: Vec::new(),
                    receiver_agents: Vec::new(),
                    prompt: None,
                    model: None,
                    reasoning_effort: None,
                    agents_states: HashMap::new(),
                }),
            )
            .await;

        Ok(boxed_tool_output(result))
    }

    async fn wait_for_target_statuses(
        &self,
        session: Arc<crate::session::session::Session>,
        turn: Arc<crate::session::turn_context::TurnContext>,
        call_id: String,
        targets: Vec<String>,
        timeout_ms: i64,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        if targets.is_empty() {
            return Err(FunctionCallError::RespondToModel(
                "targets must be non-empty".to_string(),
            ));
        }
        if targets.len() > MAX_TARGETS {
            return Err(FunctionCallError::RespondToModel(format!(
                "targets must contain at most {MAX_TARGETS} agents"
            )));
        }
        let mut target_by_thread_id = HashMap::new();
        let mut status_receivers = Vec::with_capacity(targets.len());
        let mut changed = Vec::new();

        session
            .services
            .agent_control
            .register_session_root(session.thread_id, turn.parent_thread_id);
        for target in targets {
            if !target.starts_with('/') {
                return Err(FunctionCallError::RespondToModel(
                    "targets must be canonical task paths".to_string(),
                ));
            }
            let target_path = AgentPath::try_from(target.as_str()).map_err(|err| {
                FunctionCallError::RespondToModel(format!("invalid target path: {err}"))
            })?;
            let caller_path = turn
                .session_source
                .get_agent_path()
                .unwrap_or_else(AgentPath::root);
            let caller_prefix = format!("{caller_path}/");
            if target_path != caller_path && !target_path.as_str().starts_with(&caller_prefix) {
                return Err(FunctionCallError::RespondToModel(
                    "target must be in the caller's agent subtree".to_string(),
                ));
            }
            if target.len() > MAX_TARGET_PATH_CHARS {
                return Err(FunctionCallError::RespondToModel(format!(
                    "target must be at most {MAX_TARGET_PATH_CHARS} characters"
                )));
            }
            let thread_id = session
                .services
                .agent_control
                .resolve_agent_reference(session.thread_id, &turn.session_source, &target)
                .await
                .map_err(|err| FunctionCallError::RespondToModel(err.to_string()))?;
            let agent_metadata = session
                .services
                .agent_control
                .get_agent_metadata(thread_id);
            if agent_metadata
                .as_ref()
                .and_then(|metadata| metadata.agent_path.as_ref())
                .is_some_and(AgentPath::is_root)
            {
                return Err(FunctionCallError::RespondToModel(
                    "root is not a spawned agent".to_string(),
                ));
            }
            if thread_id == session.thread_id {
                return Err(FunctionCallError::RespondToModel(
                    "an agent cannot wait for itself".to_string(),
                ));
            }
            let agent_name = agent_metadata
                .and_then(|metadata| metadata.agent_path.map(|path| path.to_string()))
                .unwrap_or_else(|| thread_id.to_string());
            if agent_name.len() > MAX_TARGET_PATH_CHARS {
                return Err(FunctionCallError::RespondToModel(
                    "target path is too long to report safely".to_string(),
                ));
            }
            if target_by_thread_id
                .insert(thread_id, agent_name.clone())
                .is_some()
            {
                continue;
            }
            match session
                .services
                .agent_control
                .subscribe_status(thread_id)
                .await
            {
                Ok(status_rx) => {
                    let status = status_rx.borrow().clone();
                    let status = visible_status(status);
                    if is_final(&status) {
                        changed.push(ChangedAgent {
                            agent: agent_name,
                            status,
                        });
                    } else {
                        status_receivers.push((thread_id, status_rx));
                    }
                }
                Err(_) => {
                    return Err(FunctionCallError::RespondToModel(format!(
                        "target agent {agent_name} is no longer live"
                    )));
                }
            }
        }

        session
            .emit_turn_item_started(
                &turn,
                TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                    id: call_id.clone(),
                    tool: CollabAgentTool::Wait,
                    status: CollabAgentToolCallStatus::InProgress,
                    sender_thread_id: session.thread_id,
                    receiver_thread_ids: target_by_thread_id.keys().copied().collect(),
                    receiver_agents: Vec::new(),
                    prompt: None,
                    model: None,
                    reasoning_effort: None,
                    agents_states: Default::default(),
                }),
            )
            .await;

        let turn_state = session
            .input_queue
            .turn_state_for_sub_id(&session.active_turn, &turn.sub_id)
            .await;
        let (mut activity_rx, pending_activity) = session
            .input_queue
            .subscribe_activity(turn_state.as_deref())
            .await;
        let mut interrupted = pending_activity == Some(InputQueueActivity::Steer);
        if changed.is_empty() {
            let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
            let mut pending = FuturesUnordered::new();
            for (thread_id, status_rx) in status_receivers {
                let session = session.clone();
                pending.push(wait_for_terminal_status(session, thread_id, status_rx));
            }
            while !interrupted {
                tokio::select! {
                    Some(Some((thread_id, status))) = pending.next() => {
                        let agent = target_by_thread_id
                            .get(&thread_id)
                            .expect("target status receiver should have a name")
                            .clone();
                        let status = visible_status(status);
                        changed.push(ChangedAgent { agent, status });
                        break;
                    }
                    Ok(()) = activity_rx.changed() => {
                        if *activity_rx.borrow_and_update() == InputQueueActivity::Steer {
                            interrupted = true;
                        }
                    }
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }
        }
        for (thread_id, agent) in &target_by_thread_id {
            let status = visible_status(session.services.agent_control.get_status(*thread_id).await);
            if is_final(&status) && !changed.iter().any(|changed| changed.agent == *agent) {
                changed.push(ChangedAgent {
                    agent: agent.clone(),
                    status,
                });
            }
        }
        changed.sort_by(|left, right| left.agent.cmp(&right.agent));
        let timed_out = changed.is_empty() && !interrupted;
        let agents_states = changed
            .iter()
            .filter_map(|changed| {
                target_by_thread_id.iter().find_map(|(id, agent)| {
                    (agent == &changed.agent).then_some((*id, changed.status.clone()))
                })
            })
            .collect();
        session
            .emit_turn_item_completed(
                &turn,
                TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                    id: call_id,
                    tool: CollabAgentTool::Wait,
                    status: CollabAgentToolCallStatus::Completed,
                    sender_thread_id: session.thread_id,
                    receiver_thread_ids: target_by_thread_id.keys().copied().collect(),
                    receiver_agents: Vec::new(),
                    prompt: None,
                    model: None,
                    reasoning_effort: None,
                    agents_states,
                }),
            )
            .await;
        Ok(boxed_tool_output(TargetWaitAgentResult {
            changed,
            timed_out,
            interrupted,
        }))
    }
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitArgs {
    targets: Option<Vec<String>>,
    timeout_ms: Option<i64>,
}

const MAX_TARGETS: usize = 8;
const MAX_TARGET_PATH_CHARS: usize = 256;

fn visible_status(status: AgentStatus) -> AgentStatus {
    match status {
        AgentStatus::Completed(_) => AgentStatus::Completed(None),
        AgentStatus::Errored(message) => AgentStatus::Errored(message.chars().take(200).collect()),
        status => status,
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct WaitAgentResult {
    pub(crate) message: String,
    pub(crate) timed_out: bool,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct TargetWaitAgentResult {
    pub(crate) changed: Vec<ChangedAgent>,
    pub(crate) timed_out: bool,
    pub(crate) interrupted: bool,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ChangedAgent {
    pub(crate) agent: String,
    pub(crate) status: AgentStatus,
}

impl ToolOutput for TargetWaitAgentResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "wait_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, /*success*/ None, "wait_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "wait_agent")
    }
}

impl WaitAgentResult {
    fn from_outcome(outcome: WaitOutcome) -> Self {
        let message = match outcome {
            WaitOutcome::MailboxActivity => "Wait completed.",
            WaitOutcome::Steered => "Wait interrupted by new input.",
            WaitOutcome::TimedOut => "Wait timed out.",
        };
        Self {
            message: message.to_string(),
            timed_out: outcome == WaitOutcome::TimedOut,
        }
    }
}

async fn wait_for_terminal_status(
    session: Arc<crate::session::session::Session>,
    thread_id: codex_protocol::ThreadId,
    mut status_rx: tokio::sync::watch::Receiver<AgentStatus>,
) -> Option<(codex_protocol::ThreadId, AgentStatus)> {
    loop {
        let status = status_rx.borrow().clone();
        if is_final(&status) {
            return Some((thread_id, status));
        }
        if status_rx.changed().await.is_err() {
            let status = session.services.agent_control.get_status(thread_id).await;
            return is_final(&status).then_some((thread_id, status));
        }
    }
}

impl ToolOutput for WaitAgentResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "wait_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, /*success*/ None, "wait_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "wait_agent")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitOutcome {
    MailboxActivity,
    Steered,
    TimedOut,
}

async fn wait_for_activity(
    activity_rx: &mut tokio::sync::watch::Receiver<InputQueueActivity>,
    pending_activity: Option<InputQueueActivity>,
    deadline: Instant,
) -> WaitOutcome {
    if let Some(activity) = pending_activity {
        return match activity {
            InputQueueActivity::Mailbox => WaitOutcome::MailboxActivity,
            InputQueueActivity::Steer => WaitOutcome::Steered,
        };
    }
    match timeout_at(deadline, activity_rx.changed()).await {
        Ok(Ok(())) => match *activity_rx.borrow_and_update() {
            InputQueueActivity::Mailbox => WaitOutcome::MailboxActivity,
            InputQueueActivity::Steer => WaitOutcome::Steered,
        },
        Ok(Err(_)) | Err(_) => WaitOutcome::TimedOut,
    }
}
