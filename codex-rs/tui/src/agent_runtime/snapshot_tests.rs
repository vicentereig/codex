use super::cost_projection::CostCoverage;
use super::cost_projection::EstimatedCostProjection;
use super::cost_projection::PricingProvenance;
use super::cost_projection::PricingTarget;
use super::model::AgentPlanSnapshot;
use super::model::AgentPlanStep;
use super::model::AgentRuntimeParent;
use super::model::AgentRuntimeSnapshot;
use super::model::AgentRuntimeSummary;
use super::model::AgentTokenUsage;
use super::snapshot::AgentSnapshotHistoryCell;
use crate::history_cell::HistoryCell;
use codex_app_server_protocol::TokenUsageBreakdown;
use codex_app_server_protocol::TurnPlanStepStatus;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;

fn render_lines(lines: &[ratatui::text::Line<'static>]) -> Vec<String> {
    lines
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect()
        })
        .collect()
}

fn id(value: &str) -> ThreadId {
    ThreadId::from_string(value).expect("stable test id")
}

fn summary(path: &str, status: super::model::AgentLifecycle) -> AgentRuntimeSummary {
    AgentRuntimeSummary {
        thread_id: id("00000000-0000-0000-0000-000000000002"),
        parent: AgentRuntimeParent::Orphan(id("00000000-0000-0000-0000-000000000003")),
        agent_path: Some(path.to_string()),
        agent_nickname: Some("Ada".to_string()),
        agent_role: Some("reviewer".to_string()),
        lifecycle: status,
        is_closed: false,
        active_turn_id: None,
        latest_terminal_turn: None,
        active_plan: None,
        latest_terminal_plan: None,
        runtime_identity: Default::default(),
        token_usage: None,
        estimated_cost: None,
        activity: Vec::new(),
        freshness: super::model::AgentFreshness::Live,
        revision: 1,
        last_observed_at_ms: 7,
    }
}

#[test]
fn snapshot_history_cell_is_point_in_time_and_honest_when_empty() {
    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 4,
        last_observed_at_ms: Some(7),
        agents: vec![],
        omitted_agents: 0,
    });
    insta::assert_snapshot!(render_lines(&cell.display_lines(/*width*/ 80)).join("\n"));
}

#[test]
fn snapshot_history_cell_wraps_and_caps_unknown_agents() {
    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 3,
        last_observed_at_ms: Some(11),
        agents: vec![summary(
            "/root/research/very-long-agent-name",
            super::model::AgentLifecycle::NeedsApproval,
        )],
        omitted_agents: 2,
    });
    let lines = cell.display_lines(/*width*/ 40);
    let rendered = render_lines(&lines).join("\n");
    let normalized = rendered.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(normalized.contains("parent unknown"));
    assert!(rendered.contains("+2 agents not shown"));
    assert!(cell.desired_height(/*width*/ 40) >= lines.len() as u16);
}

#[test]
fn snapshot_history_cell_does_not_reuse_terminal_plan_for_active_turn() {
    let mut agent = summary("/root/active", super::model::AgentLifecycle::Working);
    agent.active_turn_id = Some("turn-new".to_string());
    agent.latest_terminal_plan = Some(AgentPlanSnapshot {
        turn_id: "turn-old".to_string(),
        explanation: Some("stale".to_string()),
        steps: vec![AgentPlanStep {
            step: "old step".to_string(),
            status: TurnPlanStepStatus::Completed,
        }],
        omitted_steps: 0,
        observed_at_ms: 1,
    });
    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 6,
        last_observed_at_ms: Some(8),
        agents: vec![agent],
        omitted_agents: 0,
    });
    let rendered = render_lines(&cell.display_lines(/*width*/ 80)).join("\n");
    assert!(!rendered.contains("reported checklist"));
    assert!(rendered.contains("no checklist observed"));
}

#[test]
fn snapshot_history_cell_preserves_failure_when_closed() {
    let mut agent = summary("/root/failed", super::model::AgentLifecycle::Failed);
    agent.is_closed = true;
    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 7,
        last_observed_at_ms: Some(9),
        agents: vec![agent],
        omitted_agents: 0,
    });
    let rendered = render_lines(&cell.display_lines(/*width*/ 80)).join("\n");
    assert!(rendered.contains("failed · closed"));
}

#[test]
fn snapshot_history_cell_uses_authoritative_parent_without_path() {
    let parent_id = id("00000000-0000-0000-0000-000000000002");
    let mut parent = summary("", super::model::AgentLifecycle::Working);
    parent.agent_path = None;
    parent.thread_id = parent_id;
    parent.parent = AgentRuntimeParent::Authoritative(id("00000000-0000-0000-0000-000000000001"));
    let mut child = summary("", super::model::AgentLifecycle::Working);
    child.agent_path = None;
    child.thread_id = id("00000000-0000-0000-0000-000000000004");
    child.parent = AgentRuntimeParent::Authoritative(parent_id);
    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 8,
        last_observed_at_ms: Some(10),
        agents: vec![parent, child],
        omitted_agents: 0,
    });
    let rendered = render_lines(&cell.display_lines(/*width*/ 80)).join("\n");
    assert!(rendered.lines().any(|line| line.contains("    └─ Ada")));
}

#[test]
fn snapshot_history_cell_orders_a_child_after_its_parent() {
    let parent_id = id("00000000-0000-0000-0000-000000000010");
    let mut parent = summary("parent", super::model::AgentLifecycle::Working);
    parent.thread_id = parent_id;
    parent.parent = AgentRuntimeParent::Authoritative(id("00000000-0000-0000-0000-000000000001"));

    let mut child = summary("child", super::model::AgentLifecycle::Working);
    child.thread_id = id("00000000-0000-0000-0000-000000000011");
    child.parent = AgentRuntimeParent::Authoritative(parent_id);

    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 9,
        last_observed_at_ms: Some(11),
        agents: vec![child, parent],
        omitted_agents: 0,
    });
    let rendered = render_lines(&cell.display_lines(/*width*/ 80)).join("\n");
    assert!(
        rendered.find("parent").expect("parent row") < rendered.find("child").expect("child row")
    );
    assert!(rendered.lines().any(|line| line.contains("    └─ child")));
}

#[test]
fn snapshot_history_cell_keeps_orphans_unattached() {
    let mut orphan = summary("orphan", super::model::AgentLifecycle::Working);
    orphan.thread_id = id("00000000-0000-0000-0000-000000000012");
    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 10,
        last_observed_at_ms: Some(12),
        agents: vec![orphan],
        omitted_agents: 0,
    });
    let rendered = render_lines(&cell.display_lines(/*width*/ 80)).join("\n");
    assert!(rendered.contains("unattached agents"));
    assert!(rendered.contains("orphan · working"));
    assert!(rendered.contains("parent unknown"));
    assert!(!rendered.contains("    └─ orphan"));
}

#[test]
fn snapshot_history_cell_keeps_cycles_unattached() {
    let first_id = id("00000000-0000-0000-0000-000000000013");
    let second_id = id("00000000-0000-0000-0000-000000000014");
    let mut first = summary("cycle-one", super::model::AgentLifecycle::Working);
    first.thread_id = first_id;
    first.parent = AgentRuntimeParent::Authoritative(second_id);
    let mut second = summary("cycle-two", super::model::AgentLifecycle::Working);
    second.thread_id = second_id;
    second.parent = AgentRuntimeParent::Authoritative(first_id);

    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 11,
        last_observed_at_ms: Some(13),
        agents: vec![first, second],
        omitted_agents: 0,
    });
    let rendered = render_lines(&cell.display_lines(/*width*/ 80)).join("\n");
    assert!(rendered.contains("unattached agents"));
    assert!(rendered.contains("cycle-one · working"));
    assert!(rendered.contains("cycle-two · working"));
    assert_eq!(rendered.matches("parent cycle").count(), 2);
    assert!(!rendered.contains("    ├─ cycle-one"));
    assert!(!rendered.contains("    └─ cycle-two"));
}

#[test]
fn snapshot_history_cell_renders_exact_turn_plan_and_telemetry() {
    let mut agent = summary("/root/research", super::model::AgentLifecycle::Working);
    agent.parent = AgentRuntimeParent::Authoritative(id("00000000-0000-0000-0000-000000000001"));
    agent.runtime_identity.requested_model = Some("requested-model".to_string());
    agent.runtime_identity.effective_model = Some("effective-model".to_string());
    agent.runtime_identity.requested_effort = Some(ReasoningEffort::Medium);
    agent.runtime_identity.effective_effort = Some(ReasoningEffort::High);
    agent.token_usage = Some(AgentTokenUsage {
        turn_id: "turn-1".to_string(),
        cumulative: TokenUsageBreakdown {
            total_tokens: 1_234,
            input_tokens: 900,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 334,
            reasoning_output_tokens: 0,
        },
        last: TokenUsageBreakdown {
            total_tokens: 1_234,
            input_tokens: 900,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 334,
            reasoning_output_tokens: 0,
        },
        model_context_window: None,
        observed_at_ms: 7,
    });
    agent.estimated_cost = Some(EstimatedCostProjection {
        amount_nanos: Some(12_345_678),
        currency: Some("USD".to_string()),
        provenance: Some(PricingProvenance {
            source: "fixture-pricing".to_string(),
            version: Some("v1".to_string()),
            effective_date: None,
        }),
        target: PricingTarget {
            provider: "fixture".to_string(),
            model: "effective-model".to_string(),
            service_tier: None,
        },
        covered_categories: Vec::new(),
        coverage: CostCoverage::Complete,
        usage_total_tokens: 1_234,
    });
    agent.active_plan = Some(AgentPlanSnapshot {
        turn_id: "turn-1".to_string(),
        explanation: Some("Reported work for this turn".to_string()),
        steps: vec![
            AgentPlanStep {
                step: "Inspect event flow".to_string(),
                status: TurnPlanStepStatus::Completed,
            },
            AgentPlanStep {
                step: "Preserve reconnect truth".to_string(),
                status: TurnPlanStepStatus::InProgress,
            },
        ],
        omitted_steps: 0,
        observed_at_ms: 7,
    });
    let cell = AgentSnapshotHistoryCell::new(AgentRuntimeSnapshot {
        revision: 5,
        last_observed_at_ms: Some(7),
        agents: vec![agent],
        omitted_agents: 0,
    });
    for width in [40, 80] {
        insta::assert_snapshot!(
            format!("snapshot_history_cell_plan_telemetry_{width}"),
            render_lines(&cell.display_lines(width)).join("\n")
        );
    }
}
