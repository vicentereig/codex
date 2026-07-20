use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use codex_coordination::BoundedList;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::Evidence;
use codex_coordination::WaitOutcome;
use codex_coordination::WaitTarget;
use serde_json::json;
use sqlx::Row;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::aggregate_test_support::context;
use super::aggregate_test_support::target;
use crate::StateRuntime;
use crate::model::coordination::EndCoordinationWait;
use crate::model::coordination::StartCoordinationWait;

pub(super) struct FailAt(pub(super) AggregateStep);

impl AggregateFailureInjector for FailAt {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        if step == self.0 {
            anyhow::bail!("injected failure at {step:?}");
        }
        Ok(())
    }
}

pub(super) struct FailOccurrence {
    pub(super) step: AggregateStep,
    pub(super) occurrence: usize,
    pub(super) seen: AtomicUsize,
}

impl AggregateFailureInjector for FailOccurrence {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        if step == self.step && self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.occurrence {
            anyhow::bail!(
                "injected failure at {step:?} occurrence {}",
                self.occurrence
            );
        }
        Ok(())
    }

    fn now_ms(&self) -> i64 {
        1_753_000_000_000
    }
}

pub(super) struct FailNth {
    nth: usize,
    seen: AtomicUsize,
    failed: Mutex<Option<AggregateStep>>,
}

impl FailNth {
    pub(super) fn new(nth: usize) -> Self {
        Self {
            nth,
            seen: AtomicUsize::new(0),
            failed: Mutex::new(None),
        }
    }

    pub(super) fn failed_step(&self) -> Option<AggregateStep> {
        *self.failed.lock().expect("failure step lock")
    }
}

impl AggregateFailureInjector for FailNth {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        if self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.nth {
            *self.failed.lock().expect("failure step lock") = Some(step);
            anyhow::bail!("injected failure at boundary {} ({step:?})", self.nth);
        }
        Ok(())
    }

    fn now_ms(&self) -> i64 {
        4_000_000_000_000
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct DurableSnapshot(Vec<(String, Vec<String>)>);

pub(super) async fn durable_snapshot(runtime: &StateRuntime) -> anyhow::Result<DurableSnapshot> {
    let tables = [
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
        ("coordination_events", "root_thread_id,revision,event_id"),
        ("coordination_projection_outbox", "event_id"),
        ("coordination_dependencies", "operation_id"),
        ("coordination_results", "result_id"),
        ("coordination_handoffs", "handoff_id,attempt"),
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
    Ok(DurableSnapshot(dumps))
}

pub(super) fn wait_params(
    slot: CoordinationSemanticSlot,
    event_id: &str,
    operation_id: &str,
    expected_root_revision: u64,
) -> anyhow::Result<(StartCoordinationWait, EndCoordinationWait)> {
    let operation_id_text = operation_id;
    let operation_id = CoordinationOperationId::parse(operation_id_text)?;
    let targets = BoundedList::new(
        vec![serde_json::from_value::<WaitTarget>(json!({
            "target": target(1),
            "observedState": {"status":"known","value":"active"}
        }))?],
        /*omitted_count*/ 0,
    )?;
    let start = StartCoordinationWait {
        context: context(
            slot,
            event_id,
            operation_id_text,
            false,
            expected_root_revision,
            Vec::new(),
        ),
        operation_id,
        targets: targets.clone(),
        timeout_ms: 30_000,
    };
    let end = EndCoordinationWait {
        context: context(
            CoordinationSemanticSlot::WaitEnded,
            "019f7c6c-1111-7000-8000-000000000735",
            operation_id_text,
            false,
            expected_root_revision + 1,
            Vec::new(),
        ),
        operation_id,
        targets,
        outcome: Evidence::Known {
            value: WaitOutcome::TargetTerminal,
        },
        failure: Evidence::NotApplicable,
        expected_wait_version: 0,
    };
    Ok((start, end))
}
