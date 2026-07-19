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

    pub(crate) fn execution_guard(
        &self,
        multi_agent_version: MultiAgentVersion,
        session_source: &SessionSource,
    ) -> Option<AgentExecutionGuard> {
        is_execution_limited(multi_agent_version, session_source)
            .then(|| Arc::clone(&self.agent_execution_limiter).guard())
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

    fn guard(self: Arc<Self>) -> AgentExecutionGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        AgentExecutionGuard { limiter: self }
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
