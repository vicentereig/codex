use super::model::*;
use super::reducer::*;
use codex_app_server_protocol::ThreadTokenUsage;
use codex_app_server_protocol::TokenUsageBreakdown;
use codex_app_server_protocol::TurnPlanStepStatus;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use pretty_assertions::assert_eq;

const ROOT: &str = "00000000-0000-0000-0000-000000000001";
const CHILD: &str = "00000000-0000-0000-0000-000000000002";
const PARENT: &str = "00000000-0000-0000-0000-000000000003";
const OTHER: &str = "00000000-0000-0000-0000-000000000004";
const UNKNOWN: &str = "00000000-0000-0000-0000-000000000005";

fn id(value: &str) -> ThreadId {
    ThreadId::from_string(value).expect("stable test id")
}

fn state(limits: AgentRuntimeLimits) -> AgentRuntimeState {
    AgentRuntimeState::new(id(ROOT), limits)
}

#[test]
fn snapshot_reconciles_provisional_topology_and_orphans() {
    let (root, child, parent) = (id(ROOT), id(CHILD), id(PARENT));
    let mut state = state(AgentRuntimeLimits::default());
    state.observe_provisional_agent(
        child,
        Some("/root".to_string()),
        Some("/root/worker".to_string()),
        AgentObservation::live(100),
    );
    assert_eq!(
        state.snapshot().agents[0].parent,
        AgentRuntimeParent::ProvisionalPath(Some("/root".to_string()))
    );

    state.reconcile_agent(
        child,
        parent,
        Some("Ada".to_string()),
        Some("architect".to_string()),
        AgentObservation::backfill(200),
    );
    assert_eq!(
        state.snapshot().agents[0].parent,
        AgentRuntimeParent::Orphan(parent)
    );
    state.reconcile_agent(parent, root, None, None, AgentObservation::backfill(300));
    assert_eq!(
        state.snapshot().agents[0].parent,
        AgentRuntimeParent::Authoritative(parent)
    );
}

#[test]
fn lifecycle_is_conservative_and_plans_are_exact_turn() {
    let child = id(CHILD);
    let mut state = state(AgentRuntimeLimits {
        max_plan_steps: 2,
        ..AgentRuntimeLimits::default()
    });
    state.start_turn(child, "turn-1".to_string(), AgentObservation::live(1));
    state.observe_plan(
        child,
        "turn-1".to_string(),
        Some("reported plan".to_string()),
        (0..3)
            .map(|index| AgentPlanStep {
                step: format!("step {index}"),
                status: TurnPlanStepStatus::Pending,
            })
            .collect(),
        AgentObservation::live(2),
    );
    state.complete_turn(
        child,
        "turn-1".to_string(),
        TurnStatus::Failed,
        AgentObservation::live(3),
    );
    state.close_agent(child, AgentObservation::live(4));
    assert_eq!(state.snapshot().agents[0].lifecycle, AgentLifecycle::Failed);
    assert!(state.snapshot().agents[0].is_closed);
    assert_eq!(
        state.snapshot().agents[0]
            .latest_terminal_plan
            .as_ref()
            .expect("terminal plan")
            .omitted_steps,
        1
    );

    state.start_turn(child, "turn-2".to_string(), AgentObservation::live(7));
    let before = state.snapshot();
    state.observe_plan(
        child,
        "stale".to_string(),
        None,
        Vec::new(),
        AgentObservation::live(8),
    );
    assert_eq!(state.snapshot(), before);
}

#[test]
fn stale_completion_does_not_finish_a_newer_active_turn() {
    let child = id(CHILD);
    let mut state = state(AgentRuntimeLimits::default());
    state.start_turn(child, "turn-1".to_string(), AgentObservation::live(1));
    state.start_turn(child, "turn-2".to_string(), AgentObservation::live(2));

    state.complete_turn(
        child,
        "turn-1".to_string(),
        TurnStatus::Failed,
        AgentObservation::live(3),
    );

    let agent = &state.snapshot().agents[0];
    assert_eq!(agent.active_turn_id.as_deref(), Some("turn-2"));
    assert_eq!(agent.lifecycle, AgentLifecycle::Working);
    assert_eq!(agent.latest_terminal_turn, None);
}

#[test]
fn stream_lag_invalidates_every_open_agent_but_keeps_closed_terminal_agents() {
    let (child, parent, other) = (id(CHILD), id(PARENT), id(OTHER));
    let unknown = id(UNKNOWN);
    let mut state = state(AgentRuntimeLimits::default());
    state.start_turn(child, "turn-1".to_string(), AgentObservation::live(1));
    state.start_turn(parent, "turn-2".to_string(), AgentObservation::live(2));
    state.complete_turn(
        parent,
        "turn-2".to_string(),
        TurnStatus::Completed,
        AgentObservation::live(3),
    );
    state.start_turn(other, "turn-3".to_string(), AgentObservation::live(4));
    state.complete_turn(
        other,
        "turn-3".to_string(),
        TurnStatus::Failed,
        AgentObservation::live(5),
    );
    state.observe_provisional_agent(unknown, None, None, AgentObservation::live(6));
    state.close_agent(other, AgentObservation::live(7));

    state.mark_stream_stale(AgentObservation::stale(8));
    let stale = state.snapshot();
    let child = stale
        .agents
        .iter()
        .find(|agent| agent.thread_id == child)
        .expect("working child remains visible");
    assert_eq!(child.lifecycle, AgentLifecycle::StatusUnavailable);
    assert_eq!(child.freshness, AgentFreshness::Stale);

    let parent = stale
        .agents
        .iter()
        .find(|agent| agent.thread_id == parent)
        .expect("finished child remains visible");
    assert_eq!(parent.lifecycle, AgentLifecycle::StatusUnavailable);
    assert_eq!(parent.freshness, AgentFreshness::Stale);
    assert_eq!(
        parent.latest_terminal_turn,
        Some(AgentTerminalTurn {
            turn_id: "turn-2".to_string(),
            status: TurnStatus::Completed,
        })
    );

    let closed = stale
        .agents
        .iter()
        .find(|agent| agent.thread_id == other)
        .expect("closed child remains visible");
    assert_eq!(closed.lifecycle, AgentLifecycle::Failed);
    assert!(closed.is_closed);
    assert_eq!(closed.freshness, AgentFreshness::Live);

    let unknown = stale
        .agents
        .iter()
        .find(|agent| agent.thread_id == unknown)
        .expect("unknown child remains visible");
    assert_eq!(unknown.lifecycle, AgentLifecycle::StatusUnavailable);
    assert_eq!(unknown.freshness, AgentFreshness::Stale);

    state.start_turn(
        child.thread_id,
        "turn-4".to_string(),
        AgentObservation::live(9),
    );
    let recovered = state.snapshot();
    let recovered = recovered
        .agents
        .iter()
        .find(|agent| agent.thread_id == child.thread_id)
        .expect("working child remains visible");
    assert_eq!(recovered.lifecycle, AgentLifecycle::Working);
    assert_eq!(recovered.freshness, AgentFreshness::Live);
}

#[test]
fn authoritative_parent_must_reach_the_primary_agent_tree() {
    let mut state = state(AgentRuntimeLimits::default());
    assert!(state.accepts_parent(id(ROOT)));
    assert!(!state.accepts_parent(id(PARENT)));

    state.reconcile_agent(id(PARENT), id(ROOT), None, None, AgentObservation::live(1));
    assert!(state.accepts_parent(id(PARENT)));
}

#[test]
fn duplicate_activity_and_all_retained_collections_are_bounded() {
    let (child, parent, other) = (id(CHILD), id(PARENT), id(OTHER));
    let mut state = state(AgentRuntimeLimits {
        max_agents: 2,
        max_activity_per_agent: 2,
        max_seen_event_ids: 2,
        max_text_graphemes: 12,
        ..AgentRuntimeLimits::default()
    });
    for (index, thread_id) in [child, parent, other].into_iter().enumerate() {
        state.observe_provisional_agent(
            thread_id,
            None,
            None,
            AgentObservation::live(index as i64),
        );
    }
    for index in 0..3 {
        let event_id = format!("message-{index}");
        state.observe_activity(
            child,
            event_id.clone(),
            "a deliberately overlong summary".to_string(),
            AgentObservation::live(10 + index),
        );
        state.observe_activity(
            child,
            event_id,
            "duplicate".to_string(),
            AgentObservation::replay(20 + index),
        );
    }
    let snapshot = state.snapshot();
    assert_eq!((snapshot.agents.len(), snapshot.omitted_agents), (2, 1));
    assert_eq!(snapshot.agents[0].activity.len(), 2);
    assert!(
        snapshot.agents[0]
            .activity
            .iter()
            .all(|activity| activity.summary.chars().count() <= 12)
    );
}

#[test]
fn snapshot_separates_runtime_identity_and_cumulative_usage() {
    let child = id(CHILD);
    let mut state = state(AgentRuntimeLimits::default());
    state.reconcile_identity(
        child,
        AgentRuntimeIdentity {
            requested_model: Some("requested".to_string()),
            effective_model: Some("effective".to_string()),
            requested_effort: Some(ReasoningEffort::High),
            effective_effort: Some(ReasoningEffort::XHigh),
            service_tier: Some("priority".to_string()),
        },
        AgentObservation::live(1),
    );
    let usage = ThreadTokenUsage {
        total: TokenUsageBreakdown {
            total_tokens: 21,
            input_tokens: 13,
            cached_input_tokens: 5,
            cache_write_input_tokens: 0,
            output_tokens: 8,
            reasoning_output_tokens: 3,
        },
        last: TokenUsageBreakdown {
            total_tokens: 8,
            input_tokens: 5,
            cached_input_tokens: 2,
            cache_write_input_tokens: 0,
            output_tokens: 3,
            reasoning_output_tokens: 1,
        },
        model_context_window: Some(128_000),
    };
    state.reconcile_token_usage(
        child,
        "turn-1".to_string(),
        &usage,
        AgentObservation::live(2),
    );
    let once = state.snapshot();
    state.reconcile_token_usage(
        child,
        "turn-1".to_string(),
        &usage,
        AgentObservation::replay(3),
    );
    assert_eq!(state.snapshot(), once);
    assert_eq!(
        once.agents[0].runtime_identity.effective_model.as_deref(),
        Some("effective")
    );
    assert_eq!(
        once.agents[0]
            .token_usage
            .as_ref()
            .map(|usage| usage.cumulative.total_tokens),
        Some(21)
    );
}

#[test]
fn live_identity_supersedes_backfill_and_is_not_downgraded_by_a_later_backfill() {
    let child = id(CHILD);
    let mut state = state(AgentRuntimeLimits::default());
    state.reconcile_identity(
        child,
        AgentRuntimeIdentity {
            effective_model: Some("backfilled".to_string()),
            effective_effort: Some(ReasoningEffort::Medium),
            ..AgentRuntimeIdentity::default()
        },
        AgentObservation::backfill(1),
    );
    state.reconcile_identity(
        child,
        AgentRuntimeIdentity {
            effective_model: Some("live".to_string()),
            effective_effort: Some(ReasoningEffort::XHigh),
            ..AgentRuntimeIdentity::default()
        },
        AgentObservation::live(2),
    );
    state.reconcile_identity(
        child,
        AgentRuntimeIdentity {
            effective_model: Some("stale-backfill".to_string()),
            effective_effort: Some(ReasoningEffort::Low),
            ..AgentRuntimeIdentity::default()
        },
        AgentObservation::backfill(3),
    );

    let identity = &state.snapshot().agents[0].runtime_identity;
    assert_eq!(identity.effective_model.as_deref(), Some("live"));
    assert_eq!(identity.effective_effort, Some(ReasoningEffort::XHigh));
}
