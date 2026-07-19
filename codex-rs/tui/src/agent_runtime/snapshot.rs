//! Immutable, point-in-time agent transcript snapshots.

use super::cost_projection::CostCoverage;
use super::model::AgentFreshness;
use super::model::AgentLifecycle;
use super::model::AgentPlanSnapshot;
use super::model::AgentRuntimeParent;
use super::model::AgentRuntimeSnapshot;
use super::model::AgentRuntimeSummary;
use crate::history_cell::HistoryCell;
use crate::history_cell::ReportedChecklistStatus;
use crate::history_cell::ReportedChecklistStep;
use crate::history_cell::plain_lines;
use crate::history_cell::render_reported_checklist;
use crate::render::line_utils::push_owned_lines;
use crate::text_formatting::truncate_text;
use crate::wrapping::RtOptions;
use crate::wrapping::adaptive_wrap_line;
use codex_app_server_protocol::TurnPlanStepStatus;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use ratatui::prelude::*;
use ratatui::style::Styled;
use std::collections::HashMap;
use unicode_width::UnicodeWidthStr;

const MAX_NODES: usize = 64;
const MAX_DEPTH: usize = 3;
const MAX_STEPS: usize = 3;
const MAX_ACTIVITY: usize = 2;
const MAX_LINES: usize = 120;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AgentHierarchyUnattachedReason {
    MissingParent,
    ProvisionalParent,
    Cycle,
}

impl AgentHierarchyUnattachedReason {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::MissingParent => "parent unknown",
            Self::ProvisionalParent => "parent provisional",
            Self::Cycle => "parent cycle",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AgentHierarchyRelationship {
    Attached { depth: usize },
    Unattached(AgentHierarchyUnattachedReason),
}

impl AgentHierarchyRelationship {
    pub(crate) fn depth(self) -> Option<usize> {
        match self {
            Self::Attached { depth } => Some(depth),
            Self::Unattached(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AgentHierarchyEntry {
    pub(crate) thread_id: ThreadId,
    pub(crate) relationship: AgentHierarchyRelationship,
}

#[derive(Debug)]
pub(crate) struct AgentSnapshotHistoryCell {
    snapshot: AgentRuntimeSnapshot,
}

impl AgentSnapshotHistoryCell {
    #[cfg(test)]
    pub(crate) fn new(snapshot: AgentRuntimeSnapshot) -> Self {
        Self { snapshot }
    }

    pub(crate) fn new_optional(snapshot: Option<AgentRuntimeSnapshot>) -> Self {
        Self {
            snapshot: snapshot.unwrap_or(AgentRuntimeSnapshot {
                revision: 0,
                last_observed_at_ms: None,
                agents: Vec::new(),
                omitted_agents: 0,
            }),
        }
    }
}

impl HistoryCell for AgentSnapshotHistoryCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        append_line(
            &mut lines,
            width,
            &format!(
                "• Agent snapshot · last observed event #{}",
                self.snapshot.revision
            ),
            Style::default().bold(),
            "",
            "  ",
        );
        let state_note = if self
            .snapshot
            .agents
            .iter()
            .any(|agent| agent.freshness == AgentFreshness::Stale)
        {
            "Partial after skipped events · checklists are agent-reported, not verified"
        } else {
            "Point-in-time reported state · exact-turn checklists are agent-reported, not verified"
        };
        append_line(
            &mut lines,
            width,
            state_note,
            Style::default().dim().italic(),
            "  ",
            "  ",
        );

        if self.snapshot.agents.is_empty() {
            append_line(
                &mut lines,
                width,
                "no agents observed in this session",
                Style::default().dim().italic(),
                "  └─ ",
                "     ",
            );
        }

        let summaries = self
            .snapshot
            .agents
            .iter()
            .map(|summary| (summary.thread_id, summary))
            .collect::<HashMap<_, _>>();
        let hierarchy = ordered_agent_hierarchy(&self.snapshot.agents);
        let mut hidden_depth = 0;
        let mut omitted_lines = 0;
        let mut rendered_unattached_heading = false;
        for (index, entry) in hierarchy.iter().take(MAX_NODES).enumerate() {
            if lines.len() >= MAX_LINES.saturating_sub(1) {
                omitted_lines += self.snapshot.agents.len().saturating_sub(index);
                break;
            }
            let Some(summary) = summaries.get(&entry.thread_id).copied() else {
                continue;
            };
            if matches!(
                entry.relationship,
                AgentHierarchyRelationship::Unattached(_)
            ) && !rendered_unattached_heading
            {
                append_line(
                    &mut lines,
                    width,
                    "unattached agents · parent relationship unresolved",
                    Style::default().dim().italic(),
                    "  ",
                    "  ",
                );
                rendered_unattached_heading = true;
            }
            let depth = entry.relationship.depth().unwrap_or_default();
            if depth > MAX_DEPTH {
                hidden_depth += 1;
                continue;
            }
            let connector = if index + 1 == self.snapshot.agents.len().min(MAX_NODES) {
                "└─"
            } else {
                "├─"
            };
            let prefix = format!("{}{} ", "  ".repeat(depth + 1), connector);
            let continuation = format!("{}  ", "  ".repeat(depth + 1));
            let label = summary_label(summary);
            let status = lifecycle_label(summary);
            let plan = current_plan(summary);
            let checklist = plan
                .map(|plan| {
                    let completed = plan
                        .steps
                        .iter()
                        .filter(|step| step.status == TurnPlanStepStatus::Completed)
                        .count();
                    format!(
                        "checklist {completed}/{}",
                        plan.steps.len() + plan.omitted_steps
                    )
                })
                .unwrap_or_else(|| "no checklist observed".to_string());
            let mut row = format!("{label} · {status} · {checklist}");
            if let AgentHierarchyRelationship::Unattached(reason) = entry.relationship {
                row.push_str(" · ");
                row.push_str(reason.label());
            }
            append_line(
                &mut lines,
                width,
                &row,
                lifecycle_style(summary.lifecycle),
                &prefix,
                &continuation,
            );
            append_details(&mut lines, width, summary);
        }

        let omitted = self
            .snapshot
            .omitted_agents
            .saturating_add(hidden_depth)
            .saturating_add(self.snapshot.agents.len().saturating_sub(MAX_NODES));
        if omitted > 0 && lines.len() < MAX_LINES.saturating_sub(1) {
            append_line(
                &mut lines,
                width,
                &format!("+{omitted} agents not shown"),
                Style::default().dim().italic(),
                "  … ",
                "     ",
            );
        }
        if omitted_lines > 0 && lines.len() < MAX_LINES {
            append_line(
                &mut lines,
                width,
                &format!("+{omitted_lines} lines not shown"),
                Style::default().dim().italic(),
                "  … ",
                "     ",
            );
        }
        lines.truncate(MAX_LINES);
        lines
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        plain_lines(self.display_lines(u16::MAX))
    }
}

fn append_details(lines: &mut Vec<Line<'static>>, width: u16, summary: &AgentRuntimeSummary) {
    let detail_prefix = "     ";
    if let Some(identity) = identity_label(summary) {
        append_line(
            lines,
            width,
            &identity,
            Style::default().dim(),
            detail_prefix,
            detail_prefix,
        );
    }
    let telemetry = summary
        .token_usage
        .as_ref()
        .map(|usage| format!("tokens {} cumulative", usage.cumulative.total_tokens))
        .unwrap_or_else(|| "tokens unavailable".to_string());
    append_line(
        lines,
        width,
        &telemetry,
        Style::default().dim(),
        detail_prefix,
        detail_prefix,
    );
    append_line(
        lines,
        width,
        &cost_label(summary),
        Style::default().dim(),
        detail_prefix,
        detail_prefix,
    );
    for activity in summary.activity.iter().rev().take(MAX_ACTIVITY).rev() {
        append_line(
            lines,
            width,
            &format!("last observed: {}", truncate_text(&activity.summary, 120)),
            Style::default().dim(),
            detail_prefix,
            detail_prefix,
        );
    }
    let Some(plan) = current_plan(summary) else {
        return;
    };
    append_line(
        lines,
        width,
        &format!("reported checklist · turn {}", plan.turn_id),
        Style::default().dim(),
        detail_prefix,
        detail_prefix,
    );
    let steps = plan
        .steps
        .iter()
        .take(MAX_STEPS)
        .map(|step| ReportedChecklistStep {
            step: &step.step,
            status: match step.status {
                TurnPlanStepStatus::Completed => ReportedChecklistStatus::Completed,
                TurnPlanStepStatus::InProgress => ReportedChecklistStatus::InProgress,
                TurnPlanStepStatus::Pending => ReportedChecklistStatus::Pending,
            },
        })
        .collect::<Vec<_>>();
    let checklist = render_reported_checklist(
        width.saturating_sub(detail_prefix.width() as u16 + 4),
        plan.explanation.as_deref(),
        &steps,
    );
    for line in checklist {
        lines.push(indent_line(line, detail_prefix));
    }
    if plan.omitted_steps > 0 {
        append_line(
            lines,
            width,
            &format!("+{} checklist steps not shown", plan.omitted_steps),
            Style::default().dim().italic(),
            detail_prefix,
            detail_prefix,
        );
    }
}

fn cost_label(summary: &AgentRuntimeSummary) -> String {
    let Some(cost) = summary.estimated_cost.as_ref() else {
        return "est. cost unavailable".to_string();
    };
    let Some(amount_nanos) = cost.amount_nanos else {
        return "est. cost unavailable".to_string();
    };
    let Some(currency) = cost.currency.as_deref() else {
        return "est. cost unavailable".to_string();
    };
    let Some(provenance) = cost.provenance.as_ref() else {
        return "est. cost unavailable".to_string();
    };
    if cost.coverage == CostCoverage::Unavailable {
        return "est. cost unavailable".to_string();
    }
    let whole = amount_nanos / 1_000_000_000;
    let micros = (amount_nanos % 1_000_000_000) / 1_000;
    let coverage = match cost.coverage {
        CostCoverage::Complete => "complete",
        CostCoverage::Partial => "partial",
        CostCoverage::Unavailable => "unavailable",
    };
    let source = provenance
        .version
        .as_deref()
        .or(provenance.effective_date.as_deref())
        .map(|value| format!("{} {value}", provenance.source))
        .unwrap_or_else(|| provenance.source.clone());
    format!("est. {currency} {whole}.{micros:06} ({coverage}, {source})")
}

fn append_line(
    lines: &mut Vec<Line<'static>>,
    width: u16,
    text: &str,
    style: Style,
    initial_indent: &str,
    subsequent_indent: &str,
) {
    let opts = RtOptions::new(width.saturating_sub(initial_indent.width() as u16).max(1) as usize)
        .initial_indent(initial_indent.to_string().into())
        .subsequent_indent(subsequent_indent.to_string().into());
    let line = Line::from(text.to_string().set_style(style));
    let wrapped = adaptive_wrap_line(&line, opts);
    push_owned_lines(&wrapped, lines);
}

fn indent_line(mut line: Line<'static>, prefix: &str) -> Line<'static> {
    line.spans.insert(0, prefix.to_string().into());
    line
}

fn summary_label(summary: &AgentRuntimeSummary) -> String {
    summary
        .agent_path
        .as_deref()
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| summary.agent_nickname.clone())
        .or_else(|| summary.agent_role.clone())
        .unwrap_or_else(|| format!("thread {}", summary.thread_id))
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

pub(crate) fn ordered_agent_hierarchy(agents: &[AgentRuntimeSummary]) -> Vec<AgentHierarchyEntry> {
    fn resolve_relationship(
        thread_id: ThreadId,
        summaries: &HashMap<ThreadId, &AgentRuntimeSummary>,
        resolved: &mut HashMap<ThreadId, AgentHierarchyRelationship>,
        visiting: &mut std::collections::HashSet<ThreadId>,
    ) -> AgentHierarchyRelationship {
        if let Some(relationship) = resolved.get(&thread_id) {
            return *relationship;
        }
        if !visiting.insert(thread_id) {
            return AgentHierarchyRelationship::Unattached(AgentHierarchyUnattachedReason::Cycle);
        }
        let relationship = match summaries.get(&thread_id) {
            Some(summary) => match &summary.parent {
                AgentRuntimeParent::Authoritative(parent) if summaries.contains_key(parent) => {
                    match resolve_relationship(*parent, summaries, resolved, visiting) {
                        AgentHierarchyRelationship::Attached { depth } => {
                            AgentHierarchyRelationship::Attached {
                                depth: depth.saturating_add(1),
                            }
                        }
                        AgentHierarchyRelationship::Unattached(reason) => {
                            AgentHierarchyRelationship::Unattached(reason)
                        }
                    }
                }
                AgentRuntimeParent::Authoritative(_) => {
                    AgentHierarchyRelationship::Attached { depth: 0 }
                }
                AgentRuntimeParent::ProvisionalPath(_) => AgentHierarchyRelationship::Unattached(
                    AgentHierarchyUnattachedReason::ProvisionalParent,
                ),
                AgentRuntimeParent::Orphan(_) => AgentHierarchyRelationship::Unattached(
                    AgentHierarchyUnattachedReason::MissingParent,
                ),
            },
            None => AgentHierarchyRelationship::Unattached(
                AgentHierarchyUnattachedReason::MissingParent,
            ),
        };
        visiting.remove(&thread_id);
        resolved.insert(thread_id, relationship);
        relationship
    }

    let summaries = agents
        .iter()
        .map(|summary| (summary.thread_id, summary))
        .collect::<HashMap<_, _>>();
    let mut resolved = HashMap::new();
    for summary in agents {
        let mut visiting = std::collections::HashSet::new();
        resolve_relationship(summary.thread_id, &summaries, &mut resolved, &mut visiting);
    }

    let mut children = HashMap::<ThreadId, Vec<ThreadId>>::new();
    let mut roots = Vec::new();
    for summary in agents {
        let relationship = resolved[&summary.thread_id];
        let parent = match &summary.parent {
            AgentRuntimeParent::Authoritative(parent)
                if summaries.contains_key(parent)
                    && matches!(
                        resolved.get(parent),
                        Some(AgentHierarchyRelationship::Attached { .. })
                    ) =>
            {
                Some(*parent)
            }
            AgentRuntimeParent::Authoritative(_)
            | AgentRuntimeParent::ProvisionalPath(_)
            | AgentRuntimeParent::Orphan(_) => None,
        };
        match (relationship, parent) {
            (AgentHierarchyRelationship::Attached { .. }, Some(parent)) => {
                children.entry(parent).or_default().push(summary.thread_id);
            }
            (AgentHierarchyRelationship::Attached { .. }, None) => roots.push(summary.thread_id),
            (AgentHierarchyRelationship::Unattached(_), _) => {}
        }
    }

    let mut ordered = Vec::with_capacity(agents.len());
    let mut pending = roots.into_iter().rev().collect::<Vec<_>>();
    while let Some(thread_id) = pending.pop() {
        ordered.push(AgentHierarchyEntry {
            thread_id,
            relationship: resolved[&thread_id],
        });
        if let Some(children) = children.get(&thread_id) {
            pending.extend(children.iter().rev().copied());
        }
    }
    ordered.extend(agents.iter().filter_map(|summary| {
        let relationship = resolved[&summary.thread_id];
        matches!(relationship, AgentHierarchyRelationship::Unattached(_)).then_some(
            AgentHierarchyEntry {
                thread_id: summary.thread_id,
                relationship,
            },
        )
    }));
    ordered
}

fn lifecycle_label(summary: &AgentRuntimeSummary) -> String {
    let label = match summary.lifecycle {
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
    if summary.is_closed && !matches!(summary.lifecycle, AgentLifecycle::Closed) {
        format!("{label} · closed")
    } else {
        label.to_string()
    }
}

fn lifecycle_style(lifecycle: AgentLifecycle) -> Style {
    match lifecycle {
        AgentLifecycle::NeedsApproval | AgentLifecycle::NeedsInput => {
            Style::default().cyan().bold()
        }
        AgentLifecycle::Working => Style::default().cyan().bold(),
        AgentLifecycle::Failed => Style::default().red().bold(),
        AgentLifecycle::Finished | AgentLifecycle::Closed | AgentLifecycle::Interrupted => {
            Style::default().dim()
        }
        AgentLifecycle::Starting | AgentLifecycle::Idle | AgentLifecycle::StatusUnavailable => {
            Style::default().dim()
        }
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
    let requested_model = identity.requested_model.as_deref().unwrap_or("unavailable");
    let effective_model = identity.effective_model.as_deref().unwrap_or("unavailable");
    Some(format!(
        "requested {requested_model}/{} · effective {effective_model}/{}",
        effort_label(identity.requested_effort.as_ref()),
        effort_label(identity.effective_effort.as_ref())
    ))
}

fn effort_label(effort: Option<&ReasoningEffort>) -> String {
    effort
        .map(|effort| format!("{effort:?}").to_lowercase())
        .unwrap_or_else(|| "unavailable".to_string())
}
