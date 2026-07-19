use super::cost_projection::EstimatedCostProjection;
use super::model::*;
use crate::text_formatting::truncate_text;
use codex_app_server_protocol::ThreadTokenUsage;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy)]
pub(crate) struct AgentRuntimeLimits {
    pub(crate) max_agents: usize,
    pub(crate) max_activity_per_agent: usize,
    pub(crate) max_plan_steps: usize,
    pub(crate) max_seen_event_ids: usize,
    pub(crate) max_text_graphemes: usize,
}

impl Default for AgentRuntimeLimits {
    fn default() -> Self {
        Self {
            max_agents: 256,
            max_activity_per_agent: 12,
            max_plan_steps: 32,
            max_seen_event_ids: 2_048,
            max_text_graphemes: 240,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservationSource {
    Live,
    Stale,
    Backfill,
    #[cfg(test)]
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AgentObservation {
    at_ms: i64,
    source: ObservationSource,
}

impl AgentObservation {
    pub(crate) fn live(at_ms: i64) -> Self {
        Self {
            at_ms,
            source: ObservationSource::Live,
        }
    }

    pub(crate) fn stale(at_ms: i64) -> Self {
        Self {
            at_ms,
            source: ObservationSource::Stale,
        }
    }

    pub(crate) fn backfill(at_ms: i64) -> Self {
        Self {
            at_ms,
            source: ObservationSource::Backfill,
        }
    }

    pub(crate) fn at_ms(self) -> i64 {
        self.at_ms
    }

    #[cfg(test)]
    pub(crate) fn replay(at_ms: i64) -> Self {
        Self {
            at_ms,
            source: ObservationSource::Replay,
        }
    }

    fn freshness(self) -> AgentFreshness {
        match self.source {
            ObservationSource::Live => AgentFreshness::Live,
            ObservationSource::Stale => AgentFreshness::Stale,
            ObservationSource::Backfill => AgentFreshness::Backfilled,
            #[cfg(test)]
            ObservationSource::Replay => AgentFreshness::Backfilled,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct AgentRecord {
    thread_id: ThreadId,
    parent_thread_id: Option<ThreadId>,
    provisional_parent_path: Option<String>,
    agent_path: Option<String>,
    agent_nickname: Option<String>,
    agent_role: Option<String>,
    lifecycle: AgentLifecycle,
    is_closed: bool,
    active_turn_id: Option<String>,
    latest_terminal_turn: Option<AgentTerminalTurn>,
    active_plan: Option<AgentPlanSnapshot>,
    latest_terminal_plan: Option<AgentPlanSnapshot>,
    runtime_identity: AgentRuntimeIdentity,
    token_usage: Option<AgentTokenUsage>,
    estimated_cost: Option<EstimatedCostProjection>,
    activity: VecDeque<AgentActivity>,
    freshness: AgentFreshness,
    revision: u64,
    last_observed_at_ms: i64,
}

impl AgentRecord {
    fn new(thread_id: ThreadId) -> Self {
        Self {
            thread_id,
            parent_thread_id: None,
            provisional_parent_path: None,
            agent_path: None,
            agent_nickname: None,
            agent_role: None,
            lifecycle: AgentLifecycle::StatusUnavailable,
            is_closed: false,
            active_turn_id: None,
            latest_terminal_turn: None,
            active_plan: None,
            latest_terminal_plan: None,
            runtime_identity: AgentRuntimeIdentity::default(),
            token_usage: None,
            estimated_cost: None,
            activity: VecDeque::new(),
            freshness: AgentFreshness::Live,
            revision: 0,
            last_observed_at_ms: 0,
        }
    }
}

/// Live-session projection of normalized agent facts. App-server notifications
/// are adapted before they reach this reducer so rendering does not depend on
/// transport payload shapes.
#[derive(Debug)]
pub(crate) struct AgentRuntimeState {
    root_thread_id: ThreadId,
    limits: AgentRuntimeLimits,
    agents: HashMap<ThreadId, AgentRecord>,
    order: Vec<ThreadId>,
    seen_event_ids: HashSet<String>,
    seen_event_order: VecDeque<String>,
    omitted_agents: usize,
    revision: u64,
    last_observed_at_ms: Option<i64>,
}

impl AgentRuntimeState {
    pub(crate) fn new(root_thread_id: ThreadId, limits: AgentRuntimeLimits) -> Self {
        Self {
            root_thread_id,
            limits,
            agents: HashMap::new(),
            order: Vec::new(),
            seen_event_ids: HashSet::new(),
            seen_event_order: VecDeque::new(),
            omitted_agents: 0,
            revision: 0,
            last_observed_at_ms: None,
        }
    }

    pub(crate) fn observe_provisional_agent(
        &mut self,
        thread_id: ThreadId,
        parent_path: Option<String>,
        agent_path: Option<String>,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, |record, _| {
            if record.parent_thread_id.is_none() {
                record.provisional_parent_path = parent_path;
            }
            record.agent_path = agent_path.or(record.agent_path.take());
            if matches!(record.lifecycle, AgentLifecycle::StatusUnavailable) {
                record.lifecycle = AgentLifecycle::Starting;
            }
        });
    }

    pub(crate) fn reconcile_agent(
        &mut self,
        thread_id: ThreadId,
        parent_thread_id: ThreadId,
        agent_nickname: Option<String>,
        agent_role: Option<String>,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, |record, _| {
            record.parent_thread_id = Some(parent_thread_id);
            record.provisional_parent_path = None;
            record.agent_nickname = agent_nickname;
            record.agent_role = agent_role;
        });
    }

    pub(crate) fn observe_lifecycle(
        &mut self,
        thread_id: ThreadId,
        lifecycle: AgentLifecycle,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, |record, _| {
            let current_is_terminal = matches!(
                record.lifecycle,
                AgentLifecycle::Failed | AgentLifecycle::Finished | AgentLifecycle::Interrupted
            );
            let update_is_terminal = matches!(
                lifecycle,
                AgentLifecycle::Failed | AgentLifecycle::Finished | AgentLifecycle::Interrupted
            );
            if (record.is_closed || current_is_terminal) && !update_is_terminal {
                return;
            }
            record.lifecycle = lifecycle;
            record.is_closed = false;
        });
    }

    pub(crate) fn close_agent(&mut self, thread_id: ThreadId, observation: AgentObservation) {
        self.update(thread_id, observation, |record, _| {
            record.is_closed = true;
            if !matches!(
                record.lifecycle,
                AgentLifecycle::Failed | AgentLifecycle::Finished | AgentLifecycle::Interrupted
            ) {
                record.lifecycle = AgentLifecycle::Closed;
            }
        });
    }

    pub(crate) fn start_turn(
        &mut self,
        thread_id: ThreadId,
        turn_id: String,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, |record, _| {
            if record.active_turn_id.as_deref() != Some(&turn_id) {
                record.active_turn_id = Some(turn_id);
                record.active_plan = None;
            }
            record.lifecycle = AgentLifecycle::Working;
            record.is_closed = false;
        });
    }

    pub(crate) fn complete_turn(
        &mut self,
        thread_id: ThreadId,
        turn_id: String,
        status: TurnStatus,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, |record, _| {
            if record.active_turn_id.is_some() && record.active_turn_id.as_deref() != Some(&turn_id)
            {
                return;
            }
            if record.active_turn_id.as_deref() == Some(&turn_id) {
                record.active_turn_id = None;
                record.latest_terminal_plan = record.active_plan.take();
            }
            record.latest_terminal_turn = Some(AgentTerminalTurn {
                turn_id,
                status: status.clone(),
            });
            record.lifecycle = match status {
                TurnStatus::Completed => AgentLifecycle::Finished,
                TurnStatus::Interrupted => AgentLifecycle::Interrupted,
                TurnStatus::Failed => AgentLifecycle::Failed,
                TurnStatus::InProgress => AgentLifecycle::Working,
            };
        });
    }

    pub(crate) fn observe_plan(
        &mut self,
        thread_id: ThreadId,
        turn_id: String,
        explanation: Option<String>,
        steps: Vec<AgentPlanStep>,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, |record, limits| {
            let target = if record.active_turn_id.as_deref() == Some(&turn_id) {
                &mut record.active_plan
            } else if record
                .latest_terminal_turn
                .as_ref()
                .is_some_and(|turn| turn.turn_id == turn_id)
            {
                &mut record.latest_terminal_plan
            } else {
                return;
            };
            *target = Some(bound_plan_snapshot(
                AgentPlanSnapshot {
                    turn_id,
                    explanation,
                    steps,
                    omitted_steps: 0,
                    observed_at_ms: observation.at_ms,
                },
                limits,
            ));
        });
    }

    pub(crate) fn reconcile_turn_backfill(
        &mut self,
        thread_id: ThreadId,
        mut backfill: AgentTurnBackfill,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, |record, limits| {
            backfill.active_plan = backfill
                .active_plan
                .take()
                .map(|plan| bound_plan_snapshot(plan, limits));
            backfill.latest_terminal_plan = backfill
                .latest_terminal_plan
                .take()
                .map(|plan| bound_plan_snapshot(plan, limits));
            record.lifecycle = backfill.lifecycle;
            record.is_closed = false;
            record.active_turn_id = backfill.active_turn_id;
            record.latest_terminal_turn = backfill.latest_terminal_turn;
            record.active_plan = backfill.active_plan;
            record.latest_terminal_plan = backfill.latest_terminal_plan;
        });
    }

    pub(crate) fn reconcile_identity(
        &mut self,
        thread_id: ThreadId,
        identity: AgentRuntimeIdentity,
        observation: AgentObservation,
    ) {
        let overwrite = observation.source == ObservationSource::Live;
        self.update(thread_id, observation, move |record, _| {
            let previous_effective_model = record.runtime_identity.effective_model.clone();
            let previous_service_tier = record.runtime_identity.service_tier.clone();
            if identity.requested_model.is_some()
                && (overwrite || record.runtime_identity.requested_model.is_none())
            {
                record.runtime_identity.requested_model = identity.requested_model;
            }
            if identity.effective_model.is_some()
                && (overwrite || record.runtime_identity.effective_model.is_none())
            {
                record.runtime_identity.effective_model = identity.effective_model;
            }
            if identity.requested_effort.is_some()
                && (overwrite || record.runtime_identity.requested_effort.is_none())
            {
                record.runtime_identity.requested_effort = identity.requested_effort;
            }
            if identity.effective_effort.is_some()
                && (overwrite || record.runtime_identity.effective_effort.is_none())
            {
                record.runtime_identity.effective_effort = identity.effective_effort;
            }
            if identity.service_tier.is_some()
                && (overwrite || record.runtime_identity.service_tier.is_none())
            {
                record.runtime_identity.service_tier = identity.service_tier;
            }
            if previous_effective_model != record.runtime_identity.effective_model
                || previous_service_tier != record.runtime_identity.service_tier
            {
                record.estimated_cost = None;
            }
        });
    }

    pub(crate) fn reconcile_token_usage(
        &mut self,
        thread_id: ThreadId,
        turn_id: String,
        usage: &ThreadTokenUsage,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, |record, _| {
            if record
                .token_usage
                .as_ref()
                .is_some_and(|current| current.cumulative.total_tokens >= usage.total.total_tokens)
            {
                return;
            }
            record.token_usage = Some(AgentTokenUsage {
                turn_id,
                cumulative: usage.total.clone(),
                last: usage.last.clone(),
                model_context_window: usage.model_context_window,
                observed_at_ms: observation.at_ms,
            });
            if let Some(cost) = &mut record.estimated_cost
                && u64::try_from(usage.total.total_tokens)
                    .ok()
                    .is_none_or(|total_tokens| cost.usage_total_tokens != total_tokens)
                && cost.coverage == super::cost_projection::CostCoverage::Complete
            {
                cost.coverage = super::cost_projection::CostCoverage::Partial;
            }
        });
    }

    #[allow(
        dead_code,
        reason = "pricing integrations call this when they can supply a versioned quote"
    )]
    pub(crate) fn reconcile_estimated_cost(
        &mut self,
        thread_id: ThreadId,
        projection: EstimatedCostProjection,
        observation: AgentObservation,
    ) {
        self.update(thread_id, observation, move |record, _| {
            if let Some(current) = &mut record.estimated_cost {
                let quote_changed = current.provenance.is_some()
                    && (current.provenance != projection.provenance
                        || current.currency != projection.currency
                        || current.target != projection.target);
                if quote_changed
                    || current.usage_total_tokens > projection.usage_total_tokens
                    || current.usage_total_tokens == projection.usage_total_tokens
                        && current.coverage >= projection.coverage
                {
                    if quote_changed
                        && projection.usage_total_tokens > current.usage_total_tokens
                        && current.coverage == super::cost_projection::CostCoverage::Complete
                    {
                        current.coverage = super::cost_projection::CostCoverage::Partial;
                    }
                    return;
                }
            }
            record.estimated_cost = Some(projection);
        });
    }

    pub(crate) fn contains_agent(&self, thread_id: ThreadId) -> bool {
        self.agents.contains_key(&thread_id)
    }

    pub(crate) fn accepts_parent(&self, parent_thread_id: ThreadId) -> bool {
        parent_thread_id == self.root_thread_id || self.contains_agent(parent_thread_id)
    }

    pub(crate) fn mark_stream_stale(&mut self, observation: AgentObservation) {
        let affected = self
            .order
            .iter()
            .copied()
            .filter(|thread_id| {
                self.agents
                    .get(thread_id)
                    .is_some_and(|record| !record.is_closed)
            })
            .collect::<Vec<_>>();
        for thread_id in affected {
            self.update(thread_id, observation, |record, _| {
                record.lifecycle = AgentLifecycle::StatusUnavailable;
                record.freshness = AgentFreshness::Stale;
            });
        }
    }

    pub(crate) fn observe_activity(
        &mut self,
        thread_id: ThreadId,
        event_id: String,
        summary: String,
        observation: AgentObservation,
    ) {
        if !self.remember_event(event_id.clone()) {
            return;
        }
        self.update(thread_id, observation, |record, limits| {
            record.activity.push_back(AgentActivity {
                item_id: event_id,
                summary: truncate_text(&summary, limits.max_text_graphemes),
                observed_at_ms: observation.at_ms,
            });
            while record.activity.len() > limits.max_activity_per_agent {
                record.activity.pop_front();
            }
        });
    }

    pub(crate) fn snapshot(&self) -> AgentRuntimeSnapshot {
        AgentRuntimeSnapshot {
            revision: self.revision,
            last_observed_at_ms: self.last_observed_at_ms,
            agents: self
                .order
                .iter()
                .filter_map(|thread_id| self.agents.get(thread_id))
                .map(|record| AgentRuntimeSummary {
                    thread_id: record.thread_id,
                    parent: match record.parent_thread_id {
                        Some(parent) if parent == self.root_thread_id => {
                            AgentRuntimeParent::Authoritative(parent)
                        }
                        Some(parent) if self.agents.contains_key(&parent) => {
                            AgentRuntimeParent::Authoritative(parent)
                        }
                        Some(parent) => AgentRuntimeParent::Orphan(parent),
                        None => AgentRuntimeParent::ProvisionalPath(
                            record.provisional_parent_path.clone(),
                        ),
                    },
                    agent_path: record.agent_path.clone(),
                    agent_nickname: record.agent_nickname.clone(),
                    agent_role: record.agent_role.clone(),
                    lifecycle: record.lifecycle,
                    is_closed: record.is_closed,
                    active_turn_id: record.active_turn_id.clone(),
                    latest_terminal_turn: record.latest_terminal_turn.clone(),
                    active_plan: record.active_plan.clone(),
                    latest_terminal_plan: record.latest_terminal_plan.clone(),
                    runtime_identity: record.runtime_identity.clone(),
                    token_usage: record.token_usage.clone(),
                    estimated_cost: record.estimated_cost.clone(),
                    activity: record.activity.iter().cloned().collect(),
                    freshness: record.freshness,
                    revision: record.revision,
                    last_observed_at_ms: record.last_observed_at_ms,
                })
                .collect(),
            omitted_agents: self.omitted_agents,
        }
    }

    fn update(
        &mut self,
        thread_id: ThreadId,
        observation: AgentObservation,
        update: impl FnOnce(&mut AgentRecord, AgentRuntimeLimits),
    ) {
        if thread_id == self.root_thread_id {
            return;
        }
        if !self.agents.contains_key(&thread_id) {
            if self.agents.len() >= self.limits.max_agents {
                self.omitted_agents = self.omitted_agents.saturating_add(1);
                return;
            }
            self.agents.insert(thread_id, AgentRecord::new(thread_id));
            self.order.push(thread_id);
        }
        let Some(record) = self.agents.get_mut(&thread_id) else {
            return;
        };
        let before = record.clone();
        update(record, self.limits);
        if *record == before {
            return;
        }
        self.revision = self.revision.saturating_add(1);
        record.revision = self.revision;
        record.freshness = observation.freshness();
        record.last_observed_at_ms = observation.at_ms;
        self.last_observed_at_ms = Some(observation.at_ms);
    }

    fn remember_event(&mut self, event_id: String) -> bool {
        if !self.seen_event_ids.insert(event_id.clone()) {
            return false;
        }
        self.seen_event_order.push_back(event_id);
        while self.seen_event_order.len() > self.limits.max_seen_event_ids {
            if let Some(expired) = self.seen_event_order.pop_front() {
                self.seen_event_ids.remove(&expired);
            }
        }
        true
    }
}

fn bound_plan_snapshot(
    mut plan: AgentPlanSnapshot,
    limits: AgentRuntimeLimits,
) -> AgentPlanSnapshot {
    plan.omitted_steps = plan
        .omitted_steps
        .saturating_add(plan.steps.len().saturating_sub(limits.max_plan_steps));
    plan.steps.truncate(limits.max_plan_steps);
    plan.explanation = plan
        .explanation
        .map(|text| truncate_text(&text, limits.max_text_graphemes));
    for step in &mut plan.steps {
        step.step = truncate_text(&step.step, limits.max_text_graphemes);
    }
    plan
}
