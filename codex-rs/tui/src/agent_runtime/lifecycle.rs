use super::model::AgentLifecycle;
use super::model::AgentPlanSnapshot;
use super::model::AgentPlanStep;
use super::model::AgentRuntimeIdentity;
use super::model::AgentRuntimeSnapshot;
use super::model::AgentTerminalTurn;
use super::model::AgentTurnBackfill;
use super::reducer::AgentObservation;
use super::reducer::AgentRuntimeLimits;
use super::reducer::AgentRuntimeState;
use super::telemetry::observe_telemetry_notification;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::SubAgentActivityKind;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadActiveFlag;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SubAgentSource;
use std::collections::VecDeque;

const MAX_PENDING_THREAD_STARTS: usize = 256;

/// Owns the single live-session projection consumed by agent UI surfaces.
#[derive(Debug, Default)]
pub(crate) struct AgentRuntimeController {
    state: Option<AgentRuntimeState>,
    observation_sequence: i64,
    pending_thread_starts: VecDeque<codex_app_server_protocol::ThreadStartedNotification>,
}

impl AgentRuntimeController {
    pub(crate) fn observe(
        &mut self,
        root_thread_id: Option<ThreadId>,
        notification: &ServerNotification,
    ) {
        if self.state.is_none()
            && let Some(root_thread_id) = root_thread_id
        {
            self.state = Some(AgentRuntimeState::new(
                root_thread_id,
                AgentRuntimeLimits::default(),
            ));
        }
        let Some(state) = self.state.as_mut() else {
            return;
        };
        self.observation_sequence = self.observation_sequence.saturating_add(1);
        if let ServerNotification::ThreadStarted(event) = notification
            && let Some(parent_thread_id) = thread_parent_thread_id(&event.thread)
            && !state.accepts_parent(parent_thread_id)
        {
            if self.pending_thread_starts.len() >= MAX_PENDING_THREAD_STARTS {
                self.pending_thread_starts.pop_front();
            }
            self.pending_thread_starts.push_back(event.clone());
            return;
        }
        observe_lifecycle_notification(state, notification, self.observation_sequence);
        observe_telemetry_notification(state, notification, self.observation_sequence);
        self.promote_pending_thread_starts();
    }

    pub(crate) fn hydrate_loaded_thread_metadata(
        &mut self,
        root_thread_id: ThreadId,
        threads: &[Thread],
    ) {
        self.hydrate_threads(
            root_thread_id,
            threads,
            ThreadHistoryHydration::MetadataOnly,
        );
    }

    pub(crate) fn hydrate_loaded_threads(&mut self, root_thread_id: ThreadId, threads: &[Thread]) {
        self.hydrate_threads(root_thread_id, threads, ThreadHistoryHydration::Included);
    }

    pub(crate) fn mark_stream_stale(&mut self) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        self.observation_sequence = self.observation_sequence.saturating_add(1);
        state.mark_stream_stale(AgentObservation::stale(self.observation_sequence));
    }

    pub(crate) fn snapshot(&self) -> Option<AgentRuntimeSnapshot> {
        self.state.as_ref().map(AgentRuntimeState::snapshot)
    }

    #[cfg(test)]
    pub(crate) fn with_root(root_thread_id: ThreadId) -> Self {
        Self {
            state: Some(AgentRuntimeState::new(
                root_thread_id,
                AgentRuntimeLimits::default(),
            )),
            observation_sequence: 0,
            pending_thread_starts: VecDeque::new(),
        }
    }

    fn promote_pending_thread_starts(&mut self) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let mut pending = std::mem::take(&mut self.pending_thread_starts);
        let mut made_progress = true;
        while made_progress && !pending.is_empty() {
            made_progress = false;
            let mut deferred = VecDeque::new();
            while let Some(event) = pending.pop_front() {
                let accepted = thread_parent_thread_id(&event.thread)
                    .is_some_and(|parent| state.accepts_parent(parent));
                if accepted {
                    observe_lifecycle_notification(
                        state,
                        &ServerNotification::ThreadStarted(event),
                        self.observation_sequence,
                    );
                    made_progress = true;
                } else {
                    deferred.push_back(event);
                }
            }
            pending = deferred;
        }
        self.pending_thread_starts = pending;
    }

    fn hydrate_threads(
        &mut self,
        root_thread_id: ThreadId,
        threads: &[Thread],
        history: ThreadHistoryHydration,
    ) {
        if self.state.is_none() {
            self.state = Some(AgentRuntimeState::new(
                root_thread_id,
                AgentRuntimeLimits::default(),
            ));
        }
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let mut observation_sequence = self.observation_sequence;
        let mut pending = threads.iter().collect::<Vec<_>>();
        let mut made_progress = true;
        while made_progress && !pending.is_empty() {
            made_progress = false;
            pending.retain(|thread| {
                let Some(parent_thread_id) = thread_parent_thread_id(thread) else {
                    return false;
                };
                if !state.accepts_parent(parent_thread_id) {
                    return true;
                }
                observation_sequence = observation_sequence.saturating_add(1);
                apply_thread_metadata(
                    state,
                    thread,
                    AgentObservation::backfill(observation_sequence),
                    history,
                );
                made_progress = true;
                false
            });
        }
        self.observation_sequence = observation_sequence;
        self.promote_pending_thread_starts();
    }
}

#[derive(Clone, Copy)]
enum ThreadHistoryHydration {
    MetadataOnly,
    Included,
}

fn observe_lifecycle_notification(
    state: &mut AgentRuntimeState,
    notification: &ServerNotification,
    observed_at_ms: i64,
) {
    let observation = AgentObservation::live(observed_at_ms);
    match notification {
        ServerNotification::ThreadStarted(event) => {
            apply_thread_metadata(
                state,
                &event.thread,
                observation,
                ThreadHistoryHydration::MetadataOnly,
            );
        }
        ServerNotification::ThreadStatusChanged(event) => {
            if let Some(thread_id) = parse_thread_id(&event.thread_id)
                && state.contains_agent(thread_id)
            {
                state.observe_lifecycle(
                    thread_id,
                    lifecycle_for_thread_status(&event.status),
                    observation,
                );
            }
        }
        ServerNotification::ThreadClosed(event) => {
            if let Some(thread_id) = parse_thread_id(&event.thread_id)
                && state.contains_agent(thread_id)
            {
                state.close_agent(thread_id, observation);
            }
        }
        ServerNotification::TurnStarted(event) => {
            if let Some(thread_id) = parse_thread_id(&event.thread_id)
                && state.contains_agent(thread_id)
            {
                state.start_turn(thread_id, event.turn.id.clone(), observation);
            }
        }
        ServerNotification::TurnCompleted(event) => {
            if let Some(thread_id) = parse_thread_id(&event.thread_id)
                && state.contains_agent(thread_id)
            {
                state.complete_turn(
                    thread_id,
                    event.turn.id.clone(),
                    event.turn.status.clone(),
                    observation,
                );
            }
        }
        ServerNotification::TurnPlanUpdated(event) => {
            if let Some(thread_id) = parse_thread_id(&event.thread_id)
                && state.contains_agent(thread_id)
            {
                state.observe_plan(
                    thread_id,
                    event.turn_id.clone(),
                    event.explanation.clone(),
                    event
                        .plan
                        .iter()
                        .map(|step| AgentPlanStep {
                            step: step.step.clone(),
                            status: step.status,
                        })
                        .collect(),
                    observation,
                );
            }
        }
        ServerNotification::ItemStarted(event) => {
            observe_subagent_activity(state, &event.item, observation);
        }
        ServerNotification::ItemCompleted(event) => {
            observe_subagent_activity(state, &event.item, observation);
        }
        _ => {}
    }
}

fn observe_subagent_activity(
    state: &mut AgentRuntimeState,
    item: &ThreadItem,
    observation: AgentObservation,
) {
    let ThreadItem::SubAgentActivity {
        id,
        kind,
        agent_thread_id,
        agent_path,
    } = item
    else {
        return;
    };
    let Some(thread_id) = parse_thread_id(agent_thread_id) else {
        return;
    };
    let parent_path = agent_path
        .rsplit_once('/')
        .map(|(parent, _)| parent.to_string());
    state.observe_provisional_agent(
        thread_id,
        parent_path,
        Some(agent_path.clone()),
        observation,
    );
    let action = match kind {
        SubAgentActivityKind::Started => "Started",
        SubAgentActivityKind::Interacted => "Interacted with",
        SubAgentActivityKind::Interrupted => "Interrupted",
    };
    state.observe_activity(
        thread_id,
        id.clone(),
        format!("{action} {agent_path}"),
        observation,
    );
    match kind {
        SubAgentActivityKind::Started => {}
        SubAgentActivityKind::Interacted => {}
        SubAgentActivityKind::Interrupted => {
            state.observe_lifecycle(thread_id, AgentLifecycle::Interrupted, observation);
        }
    }
}

fn lifecycle_for_thread_status(status: &ThreadStatus) -> AgentLifecycle {
    match status {
        ThreadStatus::NotLoaded => AgentLifecycle::StatusUnavailable,
        ThreadStatus::Idle => AgentLifecycle::Idle,
        ThreadStatus::SystemError => AgentLifecycle::Failed,
        ThreadStatus::Active { active_flags }
            if active_flags.contains(&ThreadActiveFlag::WaitingOnApproval) =>
        {
            AgentLifecycle::NeedsApproval
        }
        ThreadStatus::Active { active_flags }
            if active_flags.contains(&ThreadActiveFlag::WaitingOnUserInput) =>
        {
            AgentLifecycle::NeedsInput
        }
        ThreadStatus::Active { .. } => AgentLifecycle::Working,
    }
}

fn parse_thread_id(thread_id: &str) -> Option<ThreadId> {
    ThreadId::from_string(thread_id).ok()
}

fn apply_thread_metadata(
    state: &mut AgentRuntimeState,
    thread: &Thread,
    observation: AgentObservation,
    history: ThreadHistoryHydration,
) {
    let Some(thread_id) = parse_thread_id(&thread.id) else {
        return;
    };
    let Some(parent_thread_id) = thread_parent_thread_id(thread) else {
        return;
    };
    if let Some(agent_path) = thread_agent_path(thread) {
        state.observe_provisional_agent(
            thread_id,
            agent_path
                .rsplit_once('/')
                .map(|(parent, _)| parent.to_string()),
            Some(agent_path),
            observation,
        );
    }
    state.reconcile_agent(
        thread_id,
        parent_thread_id,
        thread.agent_nickname.clone(),
        thread.agent_role.clone(),
        observation,
    );
    state.reconcile_identity(
        thread_id,
        AgentRuntimeIdentity {
            effective_model: thread.model.clone(),
            effective_effort: thread.reasoning_effort.clone(),
            ..AgentRuntimeIdentity::default()
        },
        observation,
    );
    match history {
        ThreadHistoryHydration::MetadataOnly => state.observe_lifecycle(
            thread_id,
            lifecycle_for_thread_status(&thread.status),
            observation,
        ),
        ThreadHistoryHydration::Included => state.reconcile_turn_backfill(
            thread_id,
            turn_backfill(thread, observation),
            observation,
        ),
    }
}

fn turn_backfill(thread: &Thread, observation: AgentObservation) -> AgentTurnBackfill {
    let active = latest_turn(&thread.turns, |turn| turn.status == TurnStatus::InProgress);
    let terminal = latest_turn(&thread.turns, |turn| turn.status != TurnStatus::InProgress);
    let lifecycle = match (&thread.status, active, terminal) {
        (ThreadStatus::SystemError, _, _) => AgentLifecycle::Failed,
        (ThreadStatus::Active { .. }, _, _) => lifecycle_for_thread_status(&thread.status),
        (_, Some(_), _) => AgentLifecycle::Working,
        (_, None, Some(turn)) => lifecycle_for_turn_status(&turn.status),
        _ => lifecycle_for_thread_status(&thread.status),
    };
    AgentTurnBackfill {
        lifecycle,
        active_turn_id: active.map(|turn| turn.id.clone()),
        latest_terminal_turn: terminal.map(|turn| AgentTerminalTurn {
            turn_id: turn.id.clone(),
            status: turn.status.clone(),
        }),
        active_plan: active.and_then(|turn| plan_snapshot(turn, observation)),
        latest_terminal_plan: terminal.and_then(|turn| plan_snapshot(turn, observation)),
    }
}

fn latest_turn(turns: &[Turn], predicate: impl Fn(&Turn) -> bool) -> Option<&Turn> {
    turns
        .iter()
        .enumerate()
        .filter(|(_, turn)| predicate(turn))
        .max_by_key(|(index, turn)| {
            (
                turn.completed_at.or(turn.started_at).unwrap_or(i64::MIN),
                *index,
            )
        })
        .map(|(_, turn)| turn)
}

fn plan_snapshot(turn: &Turn, observation: AgentObservation) -> Option<AgentPlanSnapshot> {
    let plan = turn.plan.as_ref()?;
    Some(AgentPlanSnapshot {
        turn_id: turn.id.clone(),
        explanation: plan.explanation.clone(),
        steps: plan
            .plan
            .iter()
            .map(|step| AgentPlanStep {
                step: step.step.clone(),
                status: step.status,
            })
            .collect(),
        omitted_steps: 0,
        observed_at_ms: plan
            .updated_at
            .map(|updated_at| updated_at.saturating_mul(1_000))
            .unwrap_or(observation.at_ms()),
    })
}

fn lifecycle_for_turn_status(status: &TurnStatus) -> AgentLifecycle {
    match status {
        TurnStatus::Completed => AgentLifecycle::Finished,
        TurnStatus::Interrupted => AgentLifecycle::Interrupted,
        TurnStatus::Failed => AgentLifecycle::Failed,
        TurnStatus::InProgress => AgentLifecycle::Working,
    }
}

fn thread_parent_thread_id(thread: &Thread) -> Option<ThreadId> {
    thread
        .parent_thread_id
        .as_deref()
        .and_then(parse_thread_id)
        .or(match &thread.source {
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id, ..
            }) => Some(*parent_thread_id),
            _ => None,
        })
}

fn thread_agent_path(thread: &Thread) -> Option<String> {
    match &thread.source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_path, .. }) => {
            agent_path.clone().map(String::from)
        }
        _ => None,
    }
}
