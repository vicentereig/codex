use super::AgentControl;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;
use tokio::time::Instant;

/// Outcome of attempting to admit a turn start into the execution limiter.
pub(crate) enum ExecutionAdmission {
    /// The turn is not execution-limited (root or V1); no permit is required.
    Unlimited,
    /// A permit was acquired and must be held for the turn's lifetime.
    Admitted(AgentExecutionGuard),
    /// The limiter is at capacity; the caller must wait and retry.
    AtCapacity,
}

/// Returned when a bounded wait for an execution permit elapses while the
/// limiter is still at capacity.
#[derive(Debug)]
pub(crate) struct ExecutionSlotTimeout;

#[derive(Default)]
pub(super) struct AgentExecutionLimiter {
    active: AtomicUsize,
    max_threads: OnceLock<usize>,
    capacity_epoch: AtomicU64,
    capacity_changed: Notify,
}

pub(crate) struct AgentExecutionGuard {
    limiter: Arc<AgentExecutionLimiter>,
}

impl Drop for AgentExecutionGuard {
    fn drop(&mut self) {
        self.limiter.active.fetch_sub(1, Ordering::AcqRel);
        self.limiter.notify_capacity_changed();
    }
}

impl AgentControl {
    pub(crate) async fn ensure_execution_capacity_for_op(
        &self,
        thread_id: ThreadId,
        op: &Op,
    ) -> CodexResult<()> {
        self.ensure_execution_capacity_for_turn_start(thread_id, op_starts_turn(op))
            .await
    }

    pub(super) async fn ensure_execution_capacity_for_turn_start(
        &self,
        thread_id: ThreadId,
        starts_turn: bool,
    ) -> CodexResult<()> {
        if !starts_turn {
            return Ok(());
        }
        let state = self.upgrade()?;
        let thread = state.get_thread(thread_id).await?;
        if thread.session.active_turn.lock().await.is_some() {
            return Ok(());
        }
        let config = thread.session.get_config().await;
        let multi_agent_version = thread
            .multi_agent_version()
            .unwrap_or_else(|| config.multi_agent_version_from_features());
        self.ensure_execution_capacity(multi_agent_version, &thread.session_source)
    }

    pub(crate) fn ensure_execution_capacity(
        &self,
        multi_agent_version: MultiAgentVersion,
        session_source: &SessionSource,
    ) -> CodexResult<()> {
        if !is_execution_limited(multi_agent_version, session_source) {
            return Ok(());
        }
        let max_threads = self.agent_execution_limiter.max_threads();
        if self.agent_execution_limiter.has_capacity() {
            Ok(())
        } else {
            Err(CodexErr::AgentLimitReached { max_threads })
        }
    }

    /// Atomically admit a turn start.
    ///
    /// This is the single source of truth for the execution cap: the capacity
    /// check and the permit acquisition happen as one atomic compare-exchange
    /// inside [`AgentExecutionLimiter::try_guard`], so two concurrent turn
    /// starts can never both observe a free slot and both take it.
    pub(crate) fn try_execution_guard(
        &self,
        multi_agent_version: MultiAgentVersion,
        session_source: &SessionSource,
    ) -> ExecutionAdmission {
        if !is_execution_limited(multi_agent_version, session_source) {
            return ExecutionAdmission::Unlimited;
        }
        match Arc::clone(&self.agent_execution_limiter).try_guard() {
            Some(guard) => ExecutionAdmission::Admitted(guard),
            None => ExecutionAdmission::AtCapacity,
        }
    }

    /// Acquire an execution permit for a turn start, waiting when the limiter
    /// is at capacity.
    ///
    /// Returns:
    /// - `Ok(None)` when the turn is not execution-limited (no permit needed);
    /// - `Ok(Some(guard))` when a permit was acquired;
    /// - `Err(ExecutionSlotTimeout)` when the bounded wait elapsed while still
    ///   at capacity.
    ///
    /// The wait registers on the capacity `Notify` and re-runs the atomic
    /// admission after each wake, epoch-guarded so a release that lands between
    /// a failed admission and the registration is never missed. It holds no
    /// lock, so a blocked turn start never stalls other work on the same
    /// thread; a slot frees only when some other child's guard drops, which is
    /// independent of this caller, so the wait cannot self-deadlock.
    pub(crate) async fn acquire_execution_slot(
        &self,
        multi_agent_version: MultiAgentVersion,
        session_source: &SessionSource,
        deadline: Instant,
    ) -> Result<Option<AgentExecutionGuard>, ExecutionSlotTimeout> {
        loop {
            let observed_epoch = self.execution_capacity_epoch();
            match self.try_execution_guard(multi_agent_version, session_source) {
                ExecutionAdmission::Unlimited => return Ok(None),
                ExecutionAdmission::Admitted(guard) => return Ok(Some(guard)),
                ExecutionAdmission::AtCapacity => {
                    if tokio::time::timeout_at(
                        deadline,
                        self.wait_for_execution_capacity_change(observed_epoch),
                    )
                    .await
                    .is_err()
                    {
                        return Err(ExecutionSlotTimeout);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn execution_guard(
        &self,
        multi_agent_version: MultiAgentVersion,
        session_source: &SessionSource,
    ) -> Option<AgentExecutionGuard> {
        match self.try_execution_guard(multi_agent_version, session_source) {
            ExecutionAdmission::Admitted(guard) => Some(guard),
            ExecutionAdmission::Unlimited | ExecutionAdmission::AtCapacity => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn active_execution_count(&self) -> usize {
        self.agent_execution_limiter.active.load(Ordering::Acquire)
    }

    /// Return the current capacity lifecycle epoch.
    pub(crate) fn execution_capacity_epoch(&self) -> u64 {
        self.agent_execution_limiter.capacity_epoch()
    }

    /// Wait until V2 execution or residency eligibility changes.
    ///
    /// The caller must repeat its admission check after this returns. The epoch
    /// closes the race where capacity changes after admission fails but before
    /// the waiter registers.
    pub(crate) async fn wait_for_execution_capacity_change(&self, observed_epoch: u64) {
        self.agent_execution_limiter
            .wait_for_capacity_change(observed_epoch)
            .await;
    }

    pub(crate) fn notify_execution_capacity_changed(&self) {
        self.agent_execution_limiter.notify_capacity_changed();
    }
}

impl AgentExecutionLimiter {
    pub(super) fn initialize(&self, max_threads: usize) {
        self.max_threads.get_or_init(|| max_threads);
    }

    fn max_threads(&self) -> usize {
        self.max_threads.get().copied().unwrap_or(usize::MAX)
    }

    fn has_capacity(&self) -> bool {
        self.active.load(Ordering::Acquire) < self.max_threads()
    }

    /// Atomically acquire a permit if the limiter is below capacity.
    ///
    /// The capacity test and the increment are fused into a single
    /// compare-exchange loop so the check-then-acquire can never be split by a
    /// concurrent acquirer. Returns `None` when genuinely at capacity; the
    /// `compare_exchange_weak` retry only re-runs on a spurious failure or a
    /// racing acquirer, never overshooting `max_threads`.
    fn try_guard(self: Arc<Self>) -> Option<AgentExecutionGuard> {
        let max_threads = self.max_threads();
        let mut current = self.active.load(Ordering::Acquire);
        loop {
            if current >= max_threads {
                return None;
            }
            match self.active.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(AgentExecutionGuard { limiter: self }),
                Err(observed) => current = observed,
            }
        }
    }

    fn capacity_epoch(&self) -> u64 {
        self.capacity_epoch.load(Ordering::Acquire)
    }

    fn notify_capacity_changed(&self) {
        self.capacity_epoch.fetch_add(1, Ordering::AcqRel);
        self.capacity_changed.notify_waiters();
    }

    async fn wait_for_capacity_change(&self, observed_epoch: u64) {
        loop {
            // Register before testing the epoch so a transition cannot be
            // missed between the observation and the await.
            let notified = self.capacity_changed.notified();
            if self.capacity_epoch() != observed_epoch {
                return;
            }
            notified.await;
        }
    }
}

fn op_starts_turn(op: &Op) -> bool {
    matches!(op, Op::UserInput { .. })
        || matches!(op, Op::InterAgentCommunication { communication } if communication.trigger_turn)
}

fn is_execution_limited(
    multi_agent_version: MultiAgentVersion,
    session_source: &SessionSource,
) -> bool {
    multi_agent_version == MultiAgentVersion::V2
        && matches!(session_source, SessionSource::SubAgent(_))
}

#[cfg(test)]
#[path = "execution_tests.rs"]
mod tests;
