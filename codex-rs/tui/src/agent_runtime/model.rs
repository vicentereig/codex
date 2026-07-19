use super::cost_projection::EstimatedCostProjection;
use codex_app_server_protocol::TokenUsageBreakdown;
use codex_app_server_protocol::TurnPlanStepStatus;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AgentRuntimeParent {
    ProvisionalPath(Option<String>),
    Authoritative(ThreadId),
    Orphan(ThreadId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgentLifecycle {
    Starting,
    Working,
    NeedsApproval,
    NeedsInput,
    Finished,
    Interrupted,
    Failed,
    Idle,
    Closed,
    StatusUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgentFreshness {
    Live,
    Stale,
    Backfilled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentPlanStep {
    pub(crate) step: String,
    pub(crate) status: TurnPlanStepStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentPlanSnapshot {
    pub(crate) turn_id: String,
    pub(crate) explanation: Option<String>,
    pub(crate) steps: Vec<AgentPlanStep>,
    pub(crate) omitted_steps: usize,
    pub(crate) observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AgentTerminalTurn {
    pub(crate) turn_id: String,
    pub(crate) status: TurnStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AgentRuntimeIdentity {
    pub(crate) requested_model: Option<String>,
    pub(crate) effective_model: Option<String>,
    pub(crate) requested_effort: Option<ReasoningEffort>,
    pub(crate) effective_effort: Option<ReasoningEffort>,
    pub(crate) service_tier: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AgentTokenUsage {
    pub(crate) turn_id: String,
    pub(crate) cumulative: TokenUsageBreakdown,
    pub(crate) last: TokenUsageBreakdown,
    pub(crate) model_context_window: Option<i64>,
    pub(crate) observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentActivity {
    pub(crate) item_id: String,
    pub(crate) summary: String,
    pub(crate) observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AgentRuntimeSummary {
    pub(crate) thread_id: ThreadId,
    pub(crate) parent: AgentRuntimeParent,
    pub(crate) agent_path: Option<String>,
    pub(crate) agent_nickname: Option<String>,
    pub(crate) agent_role: Option<String>,
    pub(crate) lifecycle: AgentLifecycle,
    pub(crate) is_closed: bool,
    pub(crate) active_turn_id: Option<String>,
    pub(crate) latest_terminal_turn: Option<AgentTerminalTurn>,
    pub(crate) active_plan: Option<AgentPlanSnapshot>,
    pub(crate) latest_terminal_plan: Option<AgentPlanSnapshot>,
    pub(crate) runtime_identity: AgentRuntimeIdentity,
    pub(crate) token_usage: Option<AgentTokenUsage>,
    pub(crate) estimated_cost: Option<EstimatedCostProjection>,
    pub(crate) activity: Vec<AgentActivity>,
    pub(crate) freshness: AgentFreshness,
    pub(crate) revision: u64,
    pub(crate) last_observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AgentRuntimeSnapshot {
    pub(crate) revision: u64,
    pub(crate) last_observed_at_ms: Option<i64>,
    pub(crate) agents: Vec<AgentRuntimeSummary>,
    pub(crate) omitted_agents: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AgentTurnBackfill {
    pub(crate) lifecycle: AgentLifecycle,
    pub(crate) active_turn_id: Option<String>,
    pub(crate) latest_terminal_turn: Option<AgentTerminalTurn>,
    pub(crate) active_plan: Option<AgentPlanSnapshot>,
    pub(crate) latest_terminal_plan: Option<AgentPlanSnapshot>,
}
