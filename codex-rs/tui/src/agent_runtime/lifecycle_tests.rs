use super::model::AgentFreshness;
use super::model::AgentRuntimeParent;
use super::*;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::SubAgentActivityKind;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadActiveFlag;
use codex_app_server_protocol::ThreadClosedNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnPlanSnapshot;
use codex_app_server_protocol::TurnPlanStep;
use codex_app_server_protocol::TurnPlanStepStatus;
use codex_app_server_protocol::TurnPlanUpdatedNotification;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use codex_utils_absolute_path::test_support::PathBufExt;
use codex_utils_absolute_path::test_support::test_path_buf;
use pretty_assertions::assert_eq;

const ROOT: &str = "00000000-0000-0000-0000-000000000001";
const CHILD: &str = "00000000-0000-0000-0000-000000000002";
const GRANDCHILD: &str = "00000000-0000-0000-0000-000000000003";
const UNRELATED: &str = "00000000-0000-0000-0000-000000000099";

fn id(value: &str) -> ThreadId {
    ThreadId::from_string(value).expect("stable test id")
}

fn turn(turn_id: &str, status: TurnStatus) -> Turn {
    Turn {
        id: turn_id.to_string(),
        items: Vec::new(),
        items_view: TurnItemsView::Full,
        status,
        plan: None,
        error: None,
        started_at: Some(1),
        completed_at: None,
        duration_ms: None,
    }
}

fn planned_turn(turn_id: &str, status: TurnStatus, timestamp: i64, explanation: &str) -> Turn {
    Turn {
        id: turn_id.to_string(),
        items: Vec::new(),
        items_view: TurnItemsView::Full,
        status: status.clone(),
        plan: Some(TurnPlanSnapshot {
            explanation: Some(explanation.to_string()),
            plan: vec![TurnPlanStep {
                step: format!("{explanation} step"),
                status: TurnPlanStepStatus::InProgress,
            }],
            revision: Some(format!("{turn_id}-revision")),
            updated_at: Some(timestamp),
        }),
        error: None,
        started_at: Some(timestamp.saturating_sub(1)),
        completed_at: (status != TurnStatus::InProgress).then_some(timestamp),
        duration_ms: None,
    }
}

fn runtime_thread(
    thread_id: &str,
    parent_thread_id: &str,
    model: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
    turns: Vec<Turn>,
) -> Thread {
    Thread {
        id: thread_id.to_string(),
        extra: None,
        session_id: ROOT.to_string(),
        forked_from_id: None,
        parent_thread_id: Some(parent_thread_id.to_string()),
        preview: String::new(),
        ephemeral: false,
        history_mode: Default::default(),
        model_provider: "openai".to_string(),
        model: model.map(str::to_string),
        reasoning_effort,
        created_at: 1,
        updated_at: 40,
        recency_at: Some(40),
        status: ThreadStatus::Active {
            active_flags: vec![ThreadActiveFlag::WaitingOnApproval],
        },
        path: None,
        cwd: test_path_buf("/tmp").abs(),
        cli_version: "0.0.0".to_string(),
        source: SessionSource::Unknown,
        can_accept_direct_input: Some(false),
        thread_source: None,
        agent_nickname: Some(format!("agent-{thread_id}")),
        agent_role: Some("worker".to_string()),
        git_info: None,
        name: None,
        turns,
    }
}

fn activity(item_id: &str, kind: SubAgentActivityKind) -> ServerNotification {
    ServerNotification::ItemCompleted(ItemCompletedNotification {
        item: ThreadItem::SubAgentActivity {
            id: item_id.to_string(),
            kind,
            agent_thread_id: CHILD.to_string(),
            agent_path: "/root/worker".to_string(),
        },
        thread_id: ROOT.to_string(),
        turn_id: "root-turn".to_string(),
        completed_at_ms: 1,
    })
}

#[test]
fn controller_observes_each_lifecycle_before_routing_and_preserves_failure() {
    let mut controller = AgentRuntimeController::with_root(id(ROOT));
    controller.observe(
        Some(id(ROOT)),
        &activity("spawn", SubAgentActivityKind::Started),
    );
    controller.observe(
        Some(id(ROOT)),
        &ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: CHILD.to_string(),
            turn: turn("turn-1", TurnStatus::InProgress),
        }),
    );
    controller.observe(
        Some(id(ROOT)),
        &ServerNotification::TurnPlanUpdated(TurnPlanUpdatedNotification {
            thread_id: CHILD.to_string(),
            turn_id: "turn-1".to_string(),
            explanation: Some("reported".to_string()),
            plan: vec![TurnPlanStep {
                step: "inspect".to_string(),
                status: TurnPlanStepStatus::InProgress,
            }],
        }),
    );
    controller.observe(
        Some(id(ROOT)),
        &ServerNotification::TurnCompleted(TurnCompletedNotification {
            thread_id: CHILD.to_string(),
            turn: turn("turn-1", TurnStatus::Failed),
        }),
    );
    controller.observe(
        Some(id(ROOT)),
        &ServerNotification::ThreadClosed(ThreadClosedNotification {
            thread_id: CHILD.to_string(),
        }),
    );

    let snapshot = controller.snapshot().expect("runtime initialized");
    assert_eq!(snapshot.agents.len(), 1);
    assert_eq!(snapshot.agents[0].lifecycle, AgentLifecycle::Failed);
    assert!(snapshot.agents[0].is_closed);
    assert_eq!(
        snapshot.agents[0]
            .latest_terminal_plan
            .as_ref()
            .expect("reported plan")
            .turn_id,
        "turn-1"
    );

    controller.observe(
        Some(id(ROOT)),
        &activity("follow-up", SubAgentActivityKind::Interacted),
    );
    let snapshot = controller.snapshot().expect("runtime initialized");
    assert_eq!(snapshot.agents[0].lifecycle, AgentLifecycle::Failed);
    assert!(snapshot.agents[0].is_closed);
}

#[test]
fn controller_never_projects_the_root_as_a_subagent() {
    let root = id(ROOT);
    let mut controller = AgentRuntimeController::with_root(root);
    controller.observe(
        Some(root),
        &ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: ROOT.to_string(),
            turn: turn("root-turn", TurnStatus::InProgress),
        }),
    );

    assert_eq!(
        controller.snapshot().expect("runtime initialized").agents,
        Vec::new()
    );
}

#[test]
fn controller_does_not_project_unrelated_threads_as_subagents() {
    let root = id(ROOT);
    let mut controller = AgentRuntimeController::with_root(root);
    controller.observe(
        Some(root),
        &ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: "00000000-0000-0000-0000-000000000099".to_string(),
            turn: turn("side-turn", TurnStatus::InProgress),
        }),
    );

    assert_eq!(
        controller.snapshot().expect("runtime initialized").agents,
        Vec::new()
    );
}

#[test]
fn controller_backfills_authoritative_tree_identity_and_exact_turn_plans() {
    let root = id(ROOT);
    let mut controller = AgentRuntimeController::with_root(root);
    let child = runtime_thread(
        CHILD,
        ROOT,
        Some("gpt-5.6-terra"),
        Some(ReasoningEffort::XHigh),
        vec![
            planned_turn("terminal-new", TurnStatus::Failed, 20, "latest terminal"),
            planned_turn("active", TurnStatus::InProgress, 30, "active"),
            planned_turn("terminal-old", TurnStatus::Completed, 10, "old terminal"),
        ],
    );
    let grandchild = runtime_thread(
        GRANDCHILD,
        CHILD,
        Some("gpt-5.6-luna"),
        Some(ReasoningEffort::High),
        Vec::new(),
    );
    let unrelated = runtime_thread(
        UNRELATED,
        UNRELATED,
        Some("unrelated"),
        Some(ReasoningEffort::Low),
        Vec::new(),
    );

    controller.hydrate_loaded_threads(root, &[grandchild, unrelated, child]);

    let snapshot = controller.snapshot().expect("runtime initialized");
    assert_eq!(snapshot.agents.len(), 2);
    let child = snapshot
        .agents
        .iter()
        .find(|agent| agent.thread_id == id(CHILD))
        .expect("child hydrated");
    assert_eq!(child.parent, AgentRuntimeParent::Authoritative(root));
    assert_eq!(child.lifecycle, AgentLifecycle::NeedsApproval);
    assert_eq!(child.active_turn_id.as_deref(), Some("active"));
    assert_eq!(
        child
            .active_plan
            .as_ref()
            .and_then(|plan| plan.explanation.as_deref()),
        Some("active")
    );
    assert_eq!(
        child
            .latest_terminal_turn
            .as_ref()
            .map(|turn| (turn.turn_id.as_str(), turn.status.clone())),
        Some(("terminal-new", TurnStatus::Failed))
    );
    assert_eq!(
        child
            .latest_terminal_plan
            .as_ref()
            .and_then(|plan| plan.explanation.as_deref()),
        Some("latest terminal")
    );
    assert_eq!(
        child.runtime_identity.effective_model.as_deref(),
        Some("gpt-5.6-terra")
    );
    assert_eq!(
        child.runtime_identity.effective_effort,
        Some(ReasoningEffort::XHigh)
    );
    assert_eq!(child.freshness, AgentFreshness::Backfilled);

    let grandchild = snapshot
        .agents
        .iter()
        .find(|agent| agent.thread_id == id(GRANDCHILD))
        .expect("grandchild hydrated after its parent");
    assert_eq!(
        grandchild.parent,
        AgentRuntimeParent::Authoritative(id(CHILD))
    );
}
