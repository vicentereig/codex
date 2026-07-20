//! Point-in-time master/detail rendering for the agent picker.

use super::AgentDisplayIdentity;
use super::model::AgentLifecycle;
use super::model::AgentPlanSnapshot;
use super::model::AgentRuntimeSnapshot;
use super::model::AgentRuntimeSummary;
use super::snapshot::AgentHierarchyRelationship;
use super::snapshot::ordered_agent_hierarchy;
use crate::bottom_pane::OnSelectionChangedCallback;
use crate::bottom_pane::SelectionItem;
use crate::multi_agents::AgentPickerThreadEntry;
use crate::render::line_utils::push_owned_lines;
use crate::render::renderable::Renderable;
use crate::text_formatting::truncate_text;
use crate::wrapping::RtOptions;
use crate::wrapping::adaptive_wrap_line;
use codex_app_server_protocol::TurnPlanStepStatus;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::style::Styled;
use ratatui::style::Stylize;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Widget;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

const MAX_STEPS: usize = 4;
const MAX_ACTIVITY: usize = 3;
const MAX_DETAIL_LINES: usize = 24;

#[derive(Debug, Clone)]
struct WorkspaceAgent {
    thread_id: ThreadId,
    identity: AgentDisplayIdentity,
    entry: AgentPickerThreadEntry,
    summary: Option<AgentRuntimeSummary>,
    depth: usize,
    relationship: AgentHierarchyRelationship,
}

fn read_agents(agents: &RwLock<Vec<WorkspaceAgent>>) -> RwLockReadGuard<'_, Vec<WorkspaceAgent>> {
    agents.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_agents(agents: &RwLock<Vec<WorkspaceAgent>>) -> RwLockWriteGuard<'_, Vec<WorkspaceAgent>> {
    agents.write().unwrap_or_else(PoisonError::into_inner)
}

/// Static data and selection state shared by the wide and stacked detail panes.
#[derive(Clone)]
pub(crate) struct AgentWorkspace {
    agents: Arc<RwLock<Vec<WorkspaceAgent>>>,
    selected: Arc<AtomicUsize>,
}

impl AgentWorkspace {
    pub(crate) fn new(
        rows: impl IntoIterator<Item = (ThreadId, AgentPickerThreadEntry)>,
        snapshot: Option<&AgentRuntimeSnapshot>,
        initial_selected: usize,
    ) -> Self {
        let summaries = snapshot
            .map(|snapshot| {
                snapshot
                    .agents
                    .iter()
                    .map(|summary| (summary.thread_id, summary.clone()))
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .unwrap_or_default();
        let rows = rows.into_iter().collect::<Vec<_>>();
        let initial_selected_id = rows.get(initial_selected).map(|(thread_id, _)| *thread_id);
        let row_order = rows
            .iter()
            .map(|(thread_id, _)| *thread_id)
            .collect::<Vec<_>>();
        let mut rows_by_id = rows.into_iter().collect::<HashMap<_, _>>();
        let hierarchy = snapshot
            .map(|snapshot| ordered_agent_hierarchy(&snapshot.agents))
            .unwrap_or_default();
        let make_agent = |thread_id: ThreadId,
                          entry: AgentPickerThreadEntry,
                          relationship: AgentHierarchyRelationship| {
            let summary = summaries.get(&thread_id).cloned();
            let agent_path = summary
                .as_ref()
                .and_then(|summary| summary.agent_path.clone())
                .or_else(|| entry.agent_path.clone())
                .and_then(|path| AgentPath::try_from(path).ok());
            let identity = AgentDisplayIdentity::new(
                thread_id,
                agent_path,
                summary
                    .as_ref()
                    .and_then(|summary| summary.agent_nickname.clone())
                    .or_else(|| entry.agent_nickname.clone()),
                summary
                    .as_ref()
                    .and_then(|summary| summary.agent_role.clone())
                    .or_else(|| entry.agent_role.clone()),
            );
            WorkspaceAgent {
                thread_id,
                identity,
                entry,
                depth: relationship.depth().unwrap_or_default(),
                relationship,
                summary,
            }
        };
        let mut agents = hierarchy
            .into_iter()
            .filter_map(|entry| {
                rows_by_id
                    .remove(&entry.thread_id)
                    .map(|row| make_agent(entry.thread_id, row, entry.relationship))
            })
            .collect::<Vec<_>>();
        for thread_id in row_order {
            if let Some(entry) = rows_by_id.remove(&thread_id) {
                agents.push(make_agent(
                    thread_id,
                    entry,
                    AgentHierarchyRelationship::Attached { depth: 0 },
                ));
            }
        }
        let selected = initial_selected_id
            .and_then(|thread_id| agents.iter().position(|agent| agent.thread_id == thread_id))
            .unwrap_or_default();
        Self {
            agents: Arc::new(RwLock::new(agents)),
            selected: Arc::new(AtomicUsize::new(selected)),
        }
    }

    pub(crate) fn selection_callback(&self) -> OnSelectionChangedCallback {
        let selected = Arc::clone(&self.selected);
        Some(Box::new(move |index, _| {
            selected.store(index, Ordering::Relaxed);
        }))
    }

    pub(crate) fn wide_detail(&self) -> AgentWorkspaceDetail {
        AgentWorkspaceDetail {
            agents: Arc::clone(&self.agents),
            selected: Arc::clone(&self.selected),
            compact: false,
        }
    }

    pub(crate) fn stacked_detail(&self) -> AgentWorkspaceDetail {
        AgentWorkspaceDetail {
            agents: Arc::clone(&self.agents),
            selected: Arc::clone(&self.selected),
            compact: true,
        }
    }

    pub(crate) fn items<F>(
        &self,
        current_thread_id: Option<ThreadId>,
        mut action: F,
    ) -> Vec<SelectionItem>
    where
        F: FnMut(ThreadId) -> crate::bottom_pane::SelectionAction,
    {
        read_agents(&self.agents)
            .iter()
            .map(|agent| SelectionItem {
                logical_id: Some(agent.thread_id.to_string()),
                name: match agent.relationship {
                    AgentHierarchyRelationship::Attached { .. } => {
                        format!(
                            "{}{}",
                            "  ".repeat(agent.depth),
                            agent.identity.contextual_label()
                        )
                    }
                    AgentHierarchyRelationship::Unattached(reason) => {
                        format!(
                            "unattached · {} · {}",
                            reason.label(),
                            agent.identity.contextual_label()
                        )
                    }
                },
                description: None,
                name_prefix_spans: status_spans(agent),
                is_current: current_thread_id == Some(agent.thread_id),
                actions: vec![action(agent.thread_id)],
                dismiss_on_select: true,
                search_value: Some(agent.identity.search_text()),
                ..Default::default()
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn update_snapshot(&self, snapshot: Option<&AgentRuntimeSnapshot>) {
        let summaries = snapshot
            .map(|snapshot| {
                snapshot
                    .agents
                    .iter()
                    .map(|summary| (summary.thread_id, summary.clone()))
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .unwrap_or_default();
        let mut agents = write_agents(&self.agents);
        for agent in agents.iter_mut() {
            agent.summary = summaries.get(&agent.thread_id).cloned();
        }
    }

    pub(crate) fn reconcile_rows(
        &self,
        rows: impl IntoIterator<Item = (ThreadId, AgentPickerThreadEntry)>,
        snapshot: Option<&AgentRuntimeSnapshot>,
    ) {
        let selected_id = read_agents(&self.agents)
            .get(self.selected.load(Ordering::Relaxed))
            .map(|agent| agent.thread_id);
        let replacement = Self::new(rows, snapshot, 0);
        let replacement_agents = read_agents(&replacement.agents).clone();
        let selected = selected_id
            .and_then(|id| {
                replacement_agents
                    .iter()
                    .position(|agent| agent.thread_id == id)
            })
            .unwrap_or_default();
        *write_agents(&self.agents) = replacement_agents;
        self.selected.store(selected, Ordering::Relaxed);
    }
}

pub(crate) struct AgentWorkspaceDetail {
    agents: Arc<RwLock<Vec<WorkspaceAgent>>>,
    selected: Arc<AtomicUsize>,
    compact: bool,
}

impl Renderable for AgentWorkspaceDetail {
    fn render(&self, area: Rect, buf: &mut Buffer) {
        let selected = self.selected_agent();
        let lines = detail_lines(selected.as_ref(), area.width, self.compact);
        Paragraph::new(lines).render(area, buf);
    }

    fn desired_height(&self, width: u16) -> u16 {
        let selected = self.selected_agent();
        detail_lines(selected.as_ref(), width, self.compact).len() as u16
    }
}

impl AgentWorkspaceDetail {
    fn selected_agent(&self) -> Option<WorkspaceAgent> {
        let agents = read_agents(&self.agents);
        agents
            .get(
                self.selected
                    .load(Ordering::Relaxed)
                    .min(agents.len().saturating_sub(1)),
            )
            .cloned()
    }
}

fn status_spans(agent: &WorkspaceAgent) -> Vec<Span<'static>> {
    let mut entry = agent.entry.clone();
    if agent
        .summary
        .as_ref()
        .is_some_and(|summary| summary.is_closed)
    {
        entry.is_closed = true;
        entry.is_running = false;
    }
    crate::multi_agents::agent_picker_status_spans(lifecycle(agent), &entry)
}

fn lifecycle(agent: &WorkspaceAgent) -> Option<AgentLifecycle> {
    agent.summary.as_ref().map(|summary| summary.lifecycle)
}

fn detail_lines(agent: Option<&WorkspaceAgent>, width: u16, compact: bool) -> Vec<Line<'static>> {
    let Some(agent) = agent else {
        return vec![Line::from("Select an agent to inspect".dim())];
    };
    let detail_width = width.max(1) as usize;
    let mut lines = Vec::new();
    append_wrapped(
        &mut lines,
        detail_width,
        &agent.identity.contextual_label(),
        Style::default().bold(),
    );
    let status = lifecycle(agent)
        .map(|status| {
            lifecycle_label(
                status,
                agent.entry.is_closed
                    || agent
                        .summary
                        .as_ref()
                        .is_some_and(|summary| summary.is_closed),
            )
        })
        .unwrap_or_else(|| {
            if agent.entry.is_closed {
                "closed".to_string()
            } else if agent.entry.is_running {
                "working".to_string()
            } else {
                "status unknown".to_string()
            }
        });
    append_wrapped(
        &mut lines,
        detail_width,
        &format!("lifecycle · {status}"),
        lifecycle_style(lifecycle(agent)),
    );
    let Some(summary) = agent.summary.as_ref() else {
        append_wrapped(
            &mut lines,
            detail_width,
            "point-in-time telemetry unavailable",
            Style::default().dim().italic(),
        );
        return lines;
    };
    if let Some(plan) = current_plan(summary) {
        append_wrapped(
            &mut lines,
            detail_width,
            &format!("reported checklist · turn {}", plan.turn_id),
            Style::default().dim(),
        );
        if compact {
            let completed = plan
                .steps
                .iter()
                .filter(|step| step.status == TurnPlanStepStatus::Completed)
                .count();
            let total = plan.steps.len() + plan.omitted_steps;
            let current = plan
                .steps
                .iter()
                .find(|step| step.status == TurnPlanStepStatus::InProgress)
                .map(|step| format!(" · current: {}", truncate_text(&step.step, 80)))
                .unwrap_or_default();
            append_wrapped(
                &mut lines,
                detail_width,
                &format!("checklist {completed}/{total} complete{current}"),
                Style::default().cyan().bold(),
            );
        } else {
            for step in plan.steps.iter().take(MAX_STEPS) {
                let marker = match step.status {
                    TurnPlanStepStatus::Completed => "✓",
                    TurnPlanStepStatus::InProgress => "→",
                    TurnPlanStepStatus::Pending => "·",
                };
                append_wrapped(
                    &mut lines,
                    detail_width.saturating_sub(2),
                    &format!("  {marker} {}", truncate_text(&step.step, 120)),
                    Style::default(),
                );
            }
            if plan.omitted_steps > 0 {
                append_wrapped(
                    &mut lines,
                    detail_width,
                    &format!("+{} checklist steps not shown", plan.omitted_steps),
                    Style::default().dim().italic(),
                );
            }
        }
    } else {
        append_wrapped(
            &mut lines,
            detail_width,
            "no checklist observed",
            Style::default().dim().italic(),
        );
    }
    if let Some(identity) = identity_label(summary) {
        append_wrapped(&mut lines, detail_width, &identity, Style::default().dim());
    }
    append_wrapped(
        &mut lines,
        detail_width,
        &summary
            .token_usage
            .as_ref()
            .map(|usage| format!("tokens {} cumulative", usage.cumulative.total_tokens))
            .unwrap_or_else(|| "tokens unavailable".to_string()),
        Style::default().dim(),
    );
    append_wrapped(
        &mut lines,
        detail_width,
        &cost_label(summary),
        Style::default().dim(),
    );
    for activity in summary.activity.iter().rev().take(MAX_ACTIVITY).rev() {
        append_wrapped(
            &mut lines,
            detail_width,
            &format!("last observed · {}", truncate_text(&activity.summary, 120)),
            Style::default().dim(),
        );
    }
    lines.truncate(MAX_DETAIL_LINES);
    lines
}

fn append_wrapped(lines: &mut Vec<Line<'static>>, width: usize, text: &str, style: Style) {
    let opts = RtOptions::new(width.max(1));
    let line = Line::from(text.to_string().set_style(style));
    let wrapped = adaptive_wrap_line(&line, opts);
    push_owned_lines(&wrapped, lines);
}

fn lifecycle_label(lifecycle: AgentLifecycle, closed: bool) -> String {
    let label = match lifecycle {
        AgentLifecycle::Starting => "starting",
        AgentLifecycle::Working => "working",
        AgentLifecycle::NeedsApproval => "needs approval",
        AgentLifecycle::NeedsInput => "needs input",
        AgentLifecycle::Finished => "finished",
        AgentLifecycle::Interrupted => "interrupted",
        AgentLifecycle::Failed => "failed",
        AgentLifecycle::Idle => "idle",
        AgentLifecycle::Closed => "closed",
        AgentLifecycle::StatusUnavailable => "status unavailable",
    };
    if closed && !matches!(lifecycle, AgentLifecycle::Closed) {
        format!("{label} · closed")
    } else {
        label.to_string()
    }
}

fn lifecycle_style(lifecycle: Option<AgentLifecycle>) -> Style {
    match lifecycle {
        Some(AgentLifecycle::Working) => Style::default().cyan().bold(),
        Some(AgentLifecycle::NeedsApproval | AgentLifecycle::NeedsInput) => {
            Style::default().cyan().bold()
        }
        Some(AgentLifecycle::Failed) => Style::default().red().bold(),
        _ => Style::default().dim(),
    }
}

fn identity_label(summary: &AgentRuntimeSummary) -> Option<String> {
    let identity = &summary.runtime_identity;
    if identity.requested_model.is_none()
        && identity.effective_model.is_none()
        && identity.requested_effort.is_none()
        && identity.effective_effort.is_none()
    {
        return None;
    }
    Some(format!(
        "requested {}/{} · effective {}/{}",
        identity.requested_model.as_deref().unwrap_or("unavailable"),
        identity
            .requested_effort
            .as_ref()
            .map(|effort| format!("{effort:?}").to_lowercase())
            .unwrap_or_else(|| "unavailable".to_string()),
        identity.effective_model.as_deref().unwrap_or("unavailable"),
        identity
            .effective_effort
            .as_ref()
            .map(|effort| format!("{effort:?}").to_lowercase())
            .unwrap_or_else(|| "unavailable".to_string())
    ))
}

fn current_plan(summary: &AgentRuntimeSummary) -> Option<&AgentPlanSnapshot> {
    if summary.active_turn_id.is_some() {
        summary.active_plan.as_ref()
    } else {
        summary
            .active_plan
            .as_ref()
            .or(summary.latest_terminal_plan.as_ref())
    }
}

fn cost_label(summary: &AgentRuntimeSummary) -> String {
    let Some(cost) = summary.estimated_cost.as_ref() else {
        return "est. cost unavailable".to_string();
    };
    let (Some(amount), Some(currency), Some(provenance)) = (
        cost.amount_nanos,
        cost.currency.as_deref(),
        cost.provenance.as_ref(),
    ) else {
        return "est. cost unavailable".to_string();
    };
    if matches!(
        cost.coverage,
        super::cost_projection::CostCoverage::Unavailable
    ) {
        return "est. cost unavailable".to_string();
    }
    let source = provenance
        .version
        .as_deref()
        .or(provenance.effective_date.as_deref())
        .map(|value| format!("{} {value}", provenance.source))
        .unwrap_or_else(|| provenance.source.clone());
    format!(
        "est. {currency} {}.{:06} ({:?}, {source})",
        amount / 1_000_000_000,
        (amount % 1_000_000_000) / 1_000,
        cost.coverage
    )
}
