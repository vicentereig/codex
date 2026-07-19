//! Turn-local ownership records for delegated child spawns.
//!
//! A reservation is created before the child runtime exists. Once `AgentControl` creates and
//! registers that runtime, the caller binds its `ThreadId` before delivering any work. This is
//! deliberately in-memory: resumed turns must not infer that an unfinished delegation completed.

use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DelegationReservation(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DelegationState {
    Pending,
    Completed,
    Cancelled,
    Failed,
    Detached,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DelegationBinding {
    Active,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DelegationRecord {
    path: AgentPath,
    thread_id: Option<ThreadId>,
    state: DelegationState,
}

/// The turn-scoped source of truth for children the parent owns.
///
/// A record is reserved before child creation. `bind` is idempotent for the same child and makes
/// cancellation that arrived during creation observable to the caller before initial delivery.
#[derive(Debug, Default)]
pub(crate) struct DelegationLedger {
    next_reservation: AtomicU64,
    records: Mutex<BTreeMap<DelegationReservation, DelegationRecord>>,
    changed: Notify,
}

impl DelegationLedger {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) async fn reserve(&self, path: AgentPath) -> DelegationReservation {
        let reservation =
            DelegationReservation(self.next_reservation.fetch_add(1, Ordering::AcqRel));
        self.records.lock().await.insert(
            reservation,
            DelegationRecord {
                path,
                thread_id: None,
                state: DelegationState::Pending,
            },
        );
        reservation
    }

    pub(crate) async fn bind(
        &self,
        reservation: DelegationReservation,
        thread_id: ThreadId,
    ) -> DelegationBinding {
        let mut records = self.records.lock().await;
        let Some(record) = records.get_mut(&reservation) else {
            return DelegationBinding::Cancelled;
        };
        if record.state == DelegationState::Pending {
            record.thread_id = Some(thread_id);
            DelegationBinding::Active
        } else {
            DelegationBinding::Cancelled
        }
    }

    pub(crate) async fn fail(&self, reservation: DelegationReservation) {
        if let Some(record) = self.records.lock().await.get_mut(&reservation)
            && record.state == DelegationState::Pending
        {
            record.state = DelegationState::Failed;
            self.changed.notify_waiters();
        }
    }

    /// First terminal outcome wins over concurrent completion/cancellation.
    pub(crate) async fn settle(&self, thread_id: ThreadId, state: DelegationState) {
        let mut records = self.records.lock().await;
        if let Some(record) = records.values_mut().find(|record| {
            record.thread_id == Some(thread_id) && record.state == DelegationState::Pending
        }) {
            record.state = state;
            self.changed.notify_waiters();
        }
    }

    pub(crate) async fn detach(&self, thread_id: ThreadId) -> bool {
        let mut records = self.records.lock().await;
        let Some(record) = records.values_mut().find(|record| {
            record.thread_id == Some(thread_id) && record.state == DelegationState::Pending
        }) else {
            return false;
        };
        record.state = DelegationState::Detached;
        self.changed.notify_waiters();
        true
    }

    /// Cancel all still-pending delegations and return the children that were already bound.
    /// The caller performs interruption outside this mutex.
    pub(crate) async fn cancel_pending(&self) -> Vec<ThreadId> {
        let mut records = self.records.lock().await;
        let mut child_thread_ids = Vec::new();
        for record in records.values_mut() {
            if record.state == DelegationState::Pending {
                record.state = DelegationState::Cancelled;
                if let Some(thread_id) = record.thread_id {
                    child_thread_ids.push(thread_id);
                }
            }
        }
        self.changed.notify_waiters();
        child_thread_ids
    }

    /// Surface a failed cancellation as explicit partial work instead of claiming the child was
    /// cancelled while it may still be running.
    pub(crate) async fn mark_cancellation_failed(&self, thread_id: ThreadId) {
        let mut records = self.records.lock().await;
        if let Some(record) = records.values_mut().find(|record| {
            record.thread_id == Some(thread_id) && record.state == DelegationState::Cancelled
        }) {
            record.state = DelegationState::Failed;
            self.changed.notify_waiters();
        }
    }

    /// Wait for all required records to settle and return failed/cancelled paths for an explicit
    /// partial parent outcome. Detached records are intentionally excluded.
    pub(crate) async fn wait_for_required_outcome(&self) -> Vec<AgentPath> {
        loop {
            let notified = self.changed.notified();
            let records = self.records.lock().await;
            if records
                .values()
                .any(|record| record.state == DelegationState::Pending)
            {
                drop(records);
                notified.await;
                continue;
            }
            return records
                .values()
                .filter(|record| {
                    matches!(
                        record.state,
                        DelegationState::Failed | DelegationState::Cancelled
                    )
                })
                .map(|record| record.path.clone())
                .collect();
        }
    }

    #[cfg(test)]
    async fn record(
        &self,
        reservation: DelegationReservation,
    ) -> Option<(AgentPath, Option<ThreadId>, DelegationState)> {
        self.records
            .lock()
            .await
            .get(&reservation)
            .map(|record| (record.path.clone(), record.thread_id, record.state))
    }
}

#[cfg(test)]
#[path = "delegation_ledger_tests.rs"]
mod tests;
