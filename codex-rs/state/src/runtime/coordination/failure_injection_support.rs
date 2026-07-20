use std::sync::Mutex;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use pretty_assertions::assert_eq;
use sqlx::Row;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use crate::StateRuntime;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Boundary {
    Aggregate(AggregateStep),
    Command(CommandStep),
    Inbox(InboxStep),
    Recovery(RecoveryStep),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CrashPoint {
    pub(super) boundary: Boundary,
    pub(super) occurrence: usize,
}

pub(super) struct CrashInjector {
    target: Option<CrashPoint>,
    trace: Mutex<Vec<Boundary>>,
    now_ms: AtomicI64,
}

impl CrashInjector {
    pub(super) fn recording(now_ms: i64) -> Self {
        Self {
            target: None,
            trace: Mutex::new(Vec::new()),
            now_ms: AtomicI64::new(now_ms),
        }
    }

    pub(super) fn fail_at(point: CrashPoint, now_ms: i64) -> Self {
        Self {
            target: Some(point),
            trace: Mutex::new(Vec::new()),
            now_ms: AtomicI64::new(now_ms),
        }
    }

    pub(super) fn advance(&self, millis: i64) {
        self.now_ms.fetch_add(millis, Ordering::SeqCst);
    }

    pub(super) fn trace(&self) -> Vec<CrashPoint> {
        let trace = self.trace.lock().expect("crash trace lock");
        trace
            .iter()
            .enumerate()
            .map(|(index, boundary)| CrashPoint {
                boundary: *boundary,
                occurrence: trace[..index]
                    .iter()
                    .filter(|candidate| *candidate == boundary)
                    .count()
                    + 1,
            })
            .collect()
    }

    fn visit(&self, boundary: Boundary) -> anyhow::Result<()> {
        let mut trace = self.trace.lock().expect("crash trace lock");
        let occurrence = trace
            .iter()
            .filter(|candidate| **candidate == boundary)
            .count()
            + 1;
        trace.push(boundary);
        if self.target
            == Some(CrashPoint {
                boundary,
                occurrence,
            })
        {
            anyhow::bail!("injected crash at {boundary:?} occurrence {occurrence}");
        }
        Ok(())
    }
}

impl AggregateFailureInjector for CrashInjector {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        self.visit(Boundary::Aggregate(step))
    }

    fn now_ms(&self) -> i64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

impl CommandFailureInjector for CrashInjector {
    fn after_command_step(&self, step: CommandStep) -> anyhow::Result<()> {
        self.visit(Boundary::Command(step))
    }
}

impl InboxFailureInjector for CrashInjector {
    fn after_inbox_step(&self, step: InboxStep) -> anyhow::Result<()> {
        self.visit(Boundary::Inbox(step))
    }
}

impl RecoveryFailureInjector for CrashInjector {
    fn after_recovery_step(&self, step: RecoveryStep) -> anyhow::Result<()> {
        self.visit(Boundary::Recovery(step))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct FrozenCoordinationState(Vec<(String, Vec<String>)>);

pub(super) async fn frozen_state(
    runtime: &StateRuntime,
) -> anyhow::Result<FrozenCoordinationState> {
    let tables = [
        ("coordination_authority", "state_epoch"),
        ("coordination_roots", "root_thread_id"),
        ("coordination_assignment_heads", "assignment_id"),
        (
            "coordination_assignment_generations",
            "assignment_id,generation",
        ),
        (
            "coordination_turn_bindings",
            "root_thread_id,turn_id,assignment_id,generation",
        ),
        ("coordination_waits", "operation_id"),
        (
            "coordination_turn_terminals",
            "root_thread_id,target_thread_id,target_turn_id",
        ),
        (
            "coordination_turn_terminal_generations",
            "root_thread_id,target_thread_id,target_turn_id,assignment_id,generation",
        ),
        ("coordination_dependencies", "operation_id"),
        ("coordination_results", "result_id"),
        ("coordination_handoffs", "handoff_id,attempt"),
        ("coordination_commands", "operation_id"),
        ("coordination_inbox", "receipt_id"),
        (
            "coordination_inbox_inclusions",
            "receipt_id,inference_attempt_id",
        ),
        ("coordination_events", "root_thread_id,revision,event_id"),
        ("coordination_projection_outbox", "event_id"),
        ("coordination_legacy_links", "compatibility_event_id"),
        (
            "coordination_legacy_scan_checkpoints",
            "root_thread_id,source_thread_id,adapter_version",
        ),
        ("coordination_degradation_records", "degradation_id"),
        (
            "coordination_degradation_publication_outbox",
            "degradation_id",
        ),
    ];
    let mut dumps = Vec::with_capacity(tables.len());
    for (table, order) in tables {
        let columns = sqlx::query(sqlx::AssertSqlSafe(format!("PRAGMA table_info({table})")))
            .fetch_all(&*runtime.pool)
            .await?
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>();
        let encoded = columns
            .iter()
            .map(|column| format!("quote(\"{}\")", column.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" || char(31) || ");
        let rows = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
            "SELECT {encoded} FROM {table} ORDER BY {order}"
        )))
        .fetch_all(&*runtime.pool)
        .await?;
        dumps.push((table.to_string(), rows));
    }
    Ok(FrozenCoordinationState(dumps))
}

pub(super) async fn assert_integrity(runtime: &StateRuntime) -> anyhow::Result<()> {
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_all(&*runtime.pool)
            .await?,
        vec!["ok".to_string()]
    );
    assert_eq!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&*runtime.pool)
            .await?
            .len(),
        0
    );
    let invalid_roots: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM coordination_roots r WHERE published_revision>committed_revision \
         OR (committed_revision>0 AND (SELECT COUNT(*) FROM coordination_events e \
         WHERE e.root_thread_id=r.root_thread_id)!=committed_revision) \
         OR (committed_revision>0 AND (SELECT MIN(revision) FROM coordination_events e \
         WHERE e.root_thread_id=r.root_thread_id)!=1) \
         OR (committed_revision>0 AND (SELECT MAX(revision) FROM coordination_events e \
         WHERE e.root_thread_id=r.root_thread_id)!=committed_revision)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(invalid_roots, 0);
    let journal_mismatch: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM coordination_events e LEFT JOIN \
         coordination_projection_outbox o USING(event_id) WHERE o.event_id IS NULL) + \
         (SELECT COUNT(*) FROM coordination_projection_outbox o LEFT JOIN \
         coordination_events e USING(event_id) WHERE e.event_id IS NULL)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(journal_mismatch, 0);
    Ok(())
}
