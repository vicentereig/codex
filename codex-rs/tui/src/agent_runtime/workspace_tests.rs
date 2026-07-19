use super::model::AgentFreshness;
use super::model::AgentLifecycle;
use super::model::AgentPlanSnapshot;
use super::model::AgentPlanStep;
use super::model::AgentRuntimeParent;
use super::model::AgentRuntimeSnapshot;
use super::model::AgentRuntimeSummary;
use super::workspace::AgentWorkspace;
use crate::app_event_sender::AppEventSender;
use crate::render::renderable::Renderable;
use codex_app_server_protocol::TurnPlanStepStatus;
use codex_protocol::ThreadId;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use tokio::sync::mpsc::unbounded_channel;

fn id(value: &str) -> ThreadId {
    ThreadId::from_string(value).expect("stable test id")
}

fn entry(path: &str, closed: bool) -> crate::multi_agents::AgentPickerThreadEntry {
    crate::multi_agents::AgentPickerThreadEntry {
        agent_nickname: None,
        agent_role: Some("worker".to_string()),
        agent_path: Some(path.to_string()),
        is_running: !closed,
        is_closed: closed,
    }
}

fn runtime_summary(
    thread_id: ThreadId,
    path: &str,
    parent: AgentRuntimeParent,
) -> AgentRuntimeSummary {
    AgentRuntimeSummary {
        thread_id,
        parent,
        agent_path: Some(path.to_string()),
        agent_nickname: None,
        agent_role: Some("worker".to_string()),
        lifecycle: AgentLifecycle::Working,
        is_closed: false,
        active_turn_id: None,
        latest_terminal_turn: None,
        active_plan: None,
        latest_terminal_plan: None,
        runtime_identity: Default::default(),
        token_usage: None,
        estimated_cost: None,
        activity: Vec::new(),
        freshness: AgentFreshness::Live,
        revision: 1,
        last_observed_at_ms: 1,
    }
}

#[test]
fn workspace_selection_updates_detail_without_live_refresh() {
    let first = id("00000000-0000-0000-0000-000000000001");
    let second = id("00000000-0000-0000-0000-000000000002");
    let workspace = AgentWorkspace::new(
        vec![
            (first, entry("/root/one", false)),
            (second, entry("/root/two", true)),
        ],
        Some(&AgentRuntimeSnapshot {
            revision: 1,
            last_observed_at_ms: Some(1),
            agents: Vec::new(),
            omitted_agents: 0,
        }),
        0,
    );
    let (tx, _rx) = unbounded_channel();
    let sender = AppEventSender::new(tx);
    (workspace.selection_callback().expect("callback"))(1, &sender);
    let mut buffer = Buffer::empty(Rect::new(0, 0, 40, 8));
    workspace
        .stacked_detail()
        .render(Rect::new(0, 0, 40, 8), &mut buffer);
    let text = buffer_to_string(&buffer);
    assert!(text.contains("/root/two"));
    assert!(text.contains("closed"));
}

#[test]
fn workspace_wide_and_narrow_snapshots_bound_tombstone_detail() {
    let workspace = AgentWorkspace::new(
        vec![(
            id("00000000-0000-0000-0000-000000000003"),
            entry("/root/closed", true),
        )],
        None,
        0,
    );
    let wide = workspace.wide_detail();
    let narrow = workspace.stacked_detail();
    insta::assert_snapshot!(render(&wide, 80, 12), @r###"
    /root/closed
    thread 00000000-0000-0000-0000-000000000003
    lifecycle · closed
    point-in-time telemetry unavailable
    "###);
    insta::assert_snapshot!(render(&narrow, 32, 8), @r###"
    /root/closed
    thread 00000000-0000-0000-0000-
    000000000003
    lifecycle · closed
    point-in-time telemetry
    unavailable
    "###);
}

#[test]
fn workspace_refresh_updates_selected_detail_in_place() {
    let thread_id = id("00000000-0000-0000-0000-000000000004");
    let workspace = AgentWorkspace::new(vec![(thread_id, entry("/root/live", false))], None, 0);
    let summary = AgentRuntimeSummary {
        thread_id,
        parent: AgentRuntimeParent::Orphan(thread_id),
        agent_path: Some("/root/live".to_string()),
        agent_nickname: None,
        agent_role: Some("worker".to_string()),
        lifecycle: AgentLifecycle::Working,
        is_closed: false,
        active_turn_id: Some("turn-live".to_string()),
        latest_terminal_turn: None,
        active_plan: Some(AgentPlanSnapshot {
            turn_id: "turn-live".to_string(),
            explanation: None,
            steps: vec![AgentPlanStep {
                step: "stream telemetry".to_string(),
                status: TurnPlanStepStatus::InProgress,
            }],
            omitted_steps: 0,
            observed_at_ms: 1,
        }),
        latest_terminal_plan: None,
        runtime_identity: Default::default(),
        token_usage: None,
        estimated_cost: None,
        activity: Vec::new(),
        freshness: AgentFreshness::Live,
        revision: 1,
        last_observed_at_ms: 1,
    };
    workspace.update_snapshot(Some(&AgentRuntimeSnapshot {
        revision: 2,
        last_observed_at_ms: Some(2),
        agents: vec![summary],
        omitted_agents: 0,
    }));
    let mut buffer = Buffer::empty(Rect::new(0, 0, 48, 10));
    workspace
        .wide_detail()
        .render(Rect::new(0, 0, 48, 10), &mut buffer);
    let rendered = buffer_to_string(&buffer);
    assert!(rendered.contains("lifecycle · working"));
    assert!(rendered.contains("reported checklist · turn turn-live"));
    assert!(rendered.contains("stream telemetry"));

    let mut narrow = Buffer::empty(Rect::new(0, 0, 32, 10));
    workspace
        .stacked_detail()
        .render(Rect::new(0, 0, 32, 10), &mut narrow);
    let narrow = buffer_to_string(&narrow);
    assert!(narrow.contains("checklist 0/1 complete"));
    assert!(narrow.contains("current: stream telemetry"));
}

#[test]
fn workspace_reconcile_adds_new_child_and_keeps_selected_thread() {
    let first = id("00000000-0000-0000-0000-000000000005");
    let second = id("00000000-0000-0000-0000-000000000006");
    let workspace = AgentWorkspace::new(vec![(first, entry("/root/first", false))], None, 0);
    workspace.reconcile_rows(
        vec![
            (first, entry("/root/first", false)),
            (second, entry("/root/second", false)),
        ],
        None,
    );
    let items = workspace.items(None, |_| Box::new(|_| {}));
    assert_eq!(items.len(), 2);
    let first_id = first.to_string();
    assert_eq!(items[0].logical_id.as_deref(), Some(first_id.as_str()));
    let mut buffer = Buffer::empty(Rect::new(0, 0, 40, 6));
    workspace
        .stacked_detail()
        .render(Rect::new(0, 0, 40, 6), &mut buffer);
    assert!(buffer_to_string(&buffer).contains("/root/first"));
}

#[test]
fn workspace_orders_children_after_parents_and_marks_unattached_agents() {
    let parent_id = id("00000000-0000-0000-0000-000000000010");
    let child_id = id("00000000-0000-0000-0000-000000000011");
    let orphan_id = id("00000000-0000-0000-0000-000000000012");
    let cycle_one_id = id("00000000-0000-0000-0000-000000000013");
    let cycle_two_id = id("00000000-0000-0000-0000-000000000014");
    let snapshot = AgentRuntimeSnapshot {
        revision: 3,
        last_observed_at_ms: Some(3),
        agents: vec![
            runtime_summary(
                child_id,
                "/root/child",
                AgentRuntimeParent::Authoritative(parent_id),
            ),
            runtime_summary(
                cycle_one_id,
                "/root/cycle-one",
                AgentRuntimeParent::Authoritative(cycle_two_id),
            ),
            runtime_summary(
                orphan_id,
                "/root/orphan",
                AgentRuntimeParent::Orphan(id("00000000-0000-0000-0000-000000000099")),
            ),
            runtime_summary(
                parent_id,
                "/root/parent",
                AgentRuntimeParent::Authoritative(id("00000000-0000-0000-0000-000000000001")),
            ),
            runtime_summary(
                cycle_two_id,
                "/root/cycle-two",
                AgentRuntimeParent::Authoritative(cycle_one_id),
            ),
        ],
        omitted_agents: 0,
    };
    let workspace = AgentWorkspace::new(
        vec![
            (child_id, entry("/root/child", false)),
            (cycle_one_id, entry("/root/cycle-one", false)),
            (orphan_id, entry("/root/orphan", false)),
            (parent_id, entry("/root/parent", false)),
            (cycle_two_id, entry("/root/cycle-two", false)),
        ],
        Some(&snapshot),
        0,
    );
    let items = workspace.items(None, |_| Box::new(|_| {}));
    let names = items
        .iter()
        .map(|item| item.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(names[0], "/root/parent");
    assert_eq!(names[1], "  /root/child");
    assert_eq!(names[2], "unattached · parent cycle · /root/cycle-one");
    assert_eq!(names[3], "unattached · parent unknown · /root/orphan");
    assert_eq!(names[4], "unattached · parent cycle · /root/cycle-two");
}

fn render(renderable: &dyn Renderable, width: u16, height: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("terminal");
    terminal
        .draw(|frame| renderable.render(frame.area(), frame.buffer_mut()))
        .expect("render");
    buffer_to_string(terminal.backend().buffer())
}

fn buffer_to_string(buffer: &Buffer) -> String {
    (0..buffer.area.height)
        .map(|y| {
            (0..buffer.area.width)
                .map(|x| buffer[(x, y)].symbol().to_string())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}
