use super::lifecycle::AgentRuntimeController;
use super::model::AgentRuntimeIdentity;
use super::reducer::AgentObservation;
use super::reducer::AgentRuntimeLimits;
use super::reducer::AgentRuntimeState;
use super::telemetry::observe_telemetry_notification;
use codex_app_server_protocol::ApprovalsReviewer;
use codex_app_server_protocol::AskForApproval;
use codex_app_server_protocol::CollabAgentTool;
use codex_app_server_protocol::CollabAgentToolCallStatus;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::SubAgentActivityKind;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadSettings;
use codex_app_server_protocol::ThreadSettingsUpdatedNotification;
use codex_app_server_protocol::ThreadTokenUsage;
use codex_app_server_protocol::ThreadTokenUsageUpdatedNotification;
use codex_app_server_protocol::TokenUsageBreakdown;
use codex_protocol::ThreadId;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::openai_models::ReasoningEffort;
use codex_utils_absolute_path::test_support::PathBufExt;
use codex_utils_absolute_path::test_support::test_path_buf;
use pretty_assertions::assert_eq;
use std::collections::HashMap;

const ROOT: &str = "00000000-0000-0000-0000-000000000001";
const CHILD: &str = "00000000-0000-0000-0000-000000000002";

fn id(value: &str) -> ThreadId {
    ThreadId::from_string(value).expect("stable test id")
}

fn state() -> AgentRuntimeState {
    AgentRuntimeState::new(id(ROOT), AgentRuntimeLimits::default())
}

fn settings(thread_id: ThreadId, model: &str, effort: ReasoningEffort) -> ServerNotification {
    ServerNotification::ThreadSettingsUpdated(ThreadSettingsUpdatedNotification {
        thread_id: thread_id.to_string(),
        thread_settings: ThreadSettings {
            cwd: test_path_buf("/tmp").abs(),
            approval_policy: AskForApproval::Never,
            approvals_reviewer: ApprovalsReviewer::User,
            sandbox_policy: codex_app_server_protocol::SandboxPolicy::ReadOnly {
                network_access: false,
            },
            active_permission_profile: None,
            model: model.to_string(),
            model_provider: "openai".to_string(),
            service_tier: Some("priority".to_string()),
            effort: Some(effort.clone()),
            summary: None,
            collaboration_mode: CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: model.to_string(),
                    reasoning_effort: Some(effort),
                    developer_instructions: None,
                },
            },
            multi_agent_mode: Default::default(),
            personality: None,
        },
    })
}

fn usage(thread_id: ThreadId, total_tokens: i64) -> ServerNotification {
    let breakdown = TokenUsageBreakdown {
        total_tokens,
        input_tokens: total_tokens.saturating_sub(3),
        cached_input_tokens: 2,
        cache_write_input_tokens: 0,
        output_tokens: 3,
        reasoning_output_tokens: 1,
    };
    ServerNotification::ThreadTokenUsageUpdated(ThreadTokenUsageUpdatedNotification {
        thread_id: thread_id.to_string(),
        turn_id: "turn-1".to_string(),
        token_usage: ThreadTokenUsage {
            total: breakdown.clone(),
            last: breakdown,
            model_context_window: Some(128_000),
        },
    })
}

#[test]
fn spawn_request_and_child_settings_remain_distinct_when_effective_values_differ() {
    let (root, child) = (id(ROOT), id(CHILD));
    let mut state = state();
    let spawn = ServerNotification::ItemCompleted(ItemCompletedNotification {
        thread_id: root.to_string(),
        turn_id: "parent-turn".to_string(),
        completed_at_ms: 1,
        item: ThreadItem::CollabAgentToolCall {
            id: "spawn-1".to_string(),
            tool: CollabAgentTool::SpawnAgent,
            status: CollabAgentToolCallStatus::Completed,
            sender_thread_id: root.to_string(),
            receiver_thread_ids: vec![child.to_string()],
            prompt: None,
            model: Some("requested".to_string()),
            reasoning_effort: Some(ReasoningEffort::High),
            agents_states: HashMap::new(),
        },
    });
    observe_telemetry_notification(&mut state, &spawn, /*observed_at_ms*/ 10);
    assert_eq!(
        state.snapshot().agents[0].runtime_identity,
        AgentRuntimeIdentity {
            requested_model: Some("requested".to_string()),
            requested_effort: Some(ReasoningEffort::High),
            ..AgentRuntimeIdentity::default()
        }
    );

    observe_telemetry_notification(
        &mut state,
        &settings(child, "effective", ReasoningEffort::XHigh),
        /*observed_at_ms*/ 20,
    );
    let identity = &state.snapshot().agents[0].runtime_identity;
    assert_eq!(identity.requested_model.as_deref(), Some("requested"));
    assert_eq!(identity.effective_model.as_deref(), Some("effective"));
    assert_eq!(identity.effective_effort, Some(ReasoningEffort::XHigh));
}

#[test]
fn unavailable_reconnect_does_not_infer_effective_runtime_from_parent_state() {
    let child = id(CHILD);
    let mut state = state();
    state.observe_provisional_agent(child, None, None, AgentObservation::backfill(/*at_ms*/ 10));
    assert_eq!(
        state.snapshot().agents[0].runtime_identity,
        AgentRuntimeIdentity::default()
    );

    observe_telemetry_notification(
        &mut state,
        &settings(child, "live", ReasoningEffort::High),
        /*observed_at_ms*/ 20,
    );
    state.reconcile_identity(
        child,
        AgentRuntimeIdentity {
            effective_model: Some("stale".to_string()),
            effective_effort: Some(ReasoningEffort::Low),
            ..AgentRuntimeIdentity::default()
        },
        AgentObservation::backfill(/*at_ms*/ 30),
    );
    assert_eq!(
        state.snapshot().agents[0]
            .runtime_identity
            .effective_model
            .as_deref(),
        Some("live")
    );
}

#[test]
fn cumulative_usage_ignores_duplicates_stale_totals_and_the_primary_thread() {
    let (root, child) = (id(ROOT), id(CHILD));
    let mut controller = AgentRuntimeController::with_root(root);
    controller.observe(
        Some(root),
        &ServerNotification::ItemCompleted(ItemCompletedNotification {
            item: ThreadItem::SubAgentActivity {
                id: "spawn-child".to_string(),
                kind: SubAgentActivityKind::Started,
                agent_thread_id: child.to_string(),
                agent_path: "/root/worker".to_string(),
            },
            thread_id: root.to_string(),
            turn_id: "root-turn".to_string(),
            completed_at_ms: 1,
        }),
    );
    controller.observe(Some(root), &usage(child, /*total_tokens*/ 21));
    let once = controller.snapshot().expect("runtime snapshot");
    controller.observe(Some(root), &usage(child, /*total_tokens*/ 21));
    controller.observe(Some(root), &usage(child, /*total_tokens*/ 13));
    assert_eq!(controller.snapshot(), Some(once));

    controller.observe(Some(root), &usage(child, /*total_tokens*/ 34));
    assert_eq!(
        controller.snapshot().expect("runtime snapshot").agents[0]
            .token_usage
            .as_ref()
            .map(|usage| usage.cumulative.total_tokens),
        Some(34)
    );
    controller.observe(Some(root), &usage(root, /*total_tokens*/ 55));
    assert_eq!(
        controller
            .snapshot()
            .expect("runtime snapshot")
            .agents
            .len(),
        1
    );
}
