use super::model::AgentRuntimeIdentity;
use super::reducer::AgentObservation;
use super::reducer::AgentRuntimeState;
use codex_app_server_protocol::CollabAgentStatus;
use codex_app_server_protocol::CollabAgentTool;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadItem;
use codex_protocol::ThreadId;

pub(crate) fn observe_telemetry_notification(
    state: &mut AgentRuntimeState,
    notification: &ServerNotification,
    observed_at_ms: i64,
) {
    match notification {
        ServerNotification::ItemStarted(event) => {
            observe_requested_runtime(state, &event.item, observed_at_ms);
        }
        ServerNotification::ItemCompleted(event) => {
            observe_requested_runtime(state, &event.item, observed_at_ms);
        }
        ServerNotification::ThreadSettingsUpdated(event) => {
            let Ok(thread_id) = ThreadId::from_string(&event.thread_id) else {
                return;
            };
            if !state.contains_agent(thread_id) {
                return;
            }
            let settings = &event.thread_settings;
            state.reconcile_identity(
                thread_id,
                AgentRuntimeIdentity {
                    effective_model: (!settings.model.trim().is_empty())
                        .then(|| settings.model.clone()),
                    effective_effort: settings.effort.clone(),
                    service_tier: settings.service_tier.clone(),
                    ..AgentRuntimeIdentity::default()
                },
                AgentObservation::live(observed_at_ms),
            );
        }
        ServerNotification::ThreadTokenUsageUpdated(event) => {
            let Ok(thread_id) = ThreadId::from_string(&event.thread_id) else {
                return;
            };
            if !state.contains_agent(thread_id) {
                return;
            }
            state.reconcile_token_usage(
                thread_id,
                event.turn_id.clone(),
                &event.token_usage,
                AgentObservation::live(observed_at_ms),
            );
        }
        _ => {}
    }
}

fn observe_requested_runtime(
    state: &mut AgentRuntimeState,
    item: &ThreadItem,
    observed_at_ms: i64,
) {
    let ThreadItem::CollabAgentToolCall {
        tool: CollabAgentTool::SpawnAgent,
        receiver_thread_ids,
        model,
        reasoning_effort,
        agents_states,
        ..
    } = item
    else {
        return;
    };
    for receiver in receiver_thread_ids {
        if agents_states
            .get(receiver)
            .is_some_and(|state| matches!(&state.status, CollabAgentStatus::NotFound))
        {
            continue;
        }
        let Ok(thread_id) = ThreadId::from_string(receiver) else {
            continue;
        };
        state.reconcile_identity(
            thread_id,
            AgentRuntimeIdentity {
                requested_model: model.clone(),
                requested_effort: reasoning_effort.clone(),
                ..AgentRuntimeIdentity::default()
            },
            AgentObservation::live(observed_at_ms),
        );
    }
}
