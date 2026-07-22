//! `CoordinationControl`: the crate-private capability switch held by `AgentControl`.
//!
//! `Disabled` is the unconditional production default (see `AgentControl::default`/`::new`,
//! both of which never construct anything but `Disabled`). There is no config flag, environment
//! flag, app-server method, public tool, or worker registration anywhere in this crate that can
//! produce `Enabled` -- the only constructor for it, [`CoordinationControl::enabled_for_tests`],
//! is `#[cfg(test)]`.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use codex_protocol::ThreadId;
use codex_state::CoordinationAuthorityStatus;

use super::operation_identity::OperationIdentityMap;
use super::spawn_gate::SpawnFailureInjector;
use super::spawn_gate::SpawnFailurePoint;
use super::spawn_gate::SpawnGateError;
use super::spawn_gate::SpawnReservationLedger;
use crate::StateDbHandle;

/// Coordination capability switch.
///
/// Cloning an `AgentControl` clones this too, same as every other piece of shared session
/// state; `Enabled` wraps its state in an `Arc` so every clone still observes the same
/// reservations/operation ids.
#[derive(Clone, Default)]
pub(crate) enum CoordinationControl {
    #[default]
    Disabled,
    Enabled(Arc<CoordinationState>),
}

impl CoordinationControl {
    /// Construct an `Enabled` control. Test-only: no production code path can call this.
    #[cfg(test)]
    pub(crate) fn enabled_for_tests(state: Arc<CoordinationState>) -> Self {
        Self::Enabled(state)
    }

    #[cfg(test)]
    pub(crate) fn as_enabled(&self) -> Option<&Arc<CoordinationState>> {
        match self {
            Self::Disabled => None,
            Self::Enabled(state) => Some(state),
        }
    }
}

/// Reason a controlled coordination operation must not proceed, checked at every transaction
/// boundary (not just a single upfront guard) per the Stage 3 freeze.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RootCoordinationError {
    /// `Enabled` was constructed without a real `state_db` backing it.
    #[error("coordination state is absent")]
    StateAbsent,
    /// The system-wide coordination authority is quarantined.
    #[error("coordination authority is quarantined")]
    Quarantined,
    /// This root has been marked poisoned (test-injected in this stage; see module docs).
    #[error("coordination root is poisoned")]
    Poisoned,
}

/// Shared state backing an `Enabled` `CoordinationControl`.
///
/// Constructible only by test code in Stage 3.3 (`CoordinationState::new_for_tests`). Holds:
/// - the durable live-operation identity mapping (Decision 5);
/// - the in-process spawn reservation ledger and failure injector used by the enabled spawn
///   orchestration (Decision 6, `agent/control/coordination_spawn.rs`);
/// - a real (optional) `StateDbHandle` used to answer the system-wide quarantine question, plus
///   a test-only per-root poison override (see `mark_root_poisoned_for_tests`) since no public
///   per-root poison query exists yet from `codex_state`.
pub(crate) struct CoordinationState {
    state_db: Option<StateDbHandle>,
    pub(crate) operation_identity: OperationIdentityMap,
    pub(crate) spawn_reservations: SpawnReservationLedger,
    // Interior mutability so a test can arm/disarm failure injection on the same shared
    // `Arc<CoordinationState>` across a failed attempt and its retry, without losing the
    // reservation ledger/operation identity map that retry needs to resume from.
    spawn_failure_injector: Mutex<SpawnFailureInjector>,
    poisoned_roots: Mutex<HashSet<ThreadId>>,
}

impl CoordinationState {
    /// Build an enabled coordination state. `state_db: None` represents "state absent" -- the
    /// same case a production caller would be in if it somehow enabled coordination without a
    /// local state database.
    #[cfg(test)]
    pub(crate) fn new_for_tests(state_db: Option<StateDbHandle>) -> Arc<Self> {
        Self::with_failure_injector(state_db, SpawnFailureInjector::none())
    }

    #[cfg(test)]
    pub(crate) fn with_failure_injector(
        state_db: Option<StateDbHandle>,
        spawn_failure_injector: SpawnFailureInjector,
    ) -> Arc<Self> {
        Arc::new(Self {
            state_db,
            operation_identity: OperationIdentityMap::new(),
            spawn_reservations: SpawnReservationLedger::new(),
            spawn_failure_injector: Mutex::new(spawn_failure_injector),
            poisoned_roots: Mutex::new(HashSet::new()),
        })
    }

    #[cfg(test)]
    pub(crate) fn mark_root_poisoned_for_tests(&self, root_thread_id: ThreadId) {
        self.poisoned_roots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(root_thread_id);
    }

    /// Replace the failure injector on an already-`Arc`-shared coordination state. Test-only:
    /// lets a test disarm injection before a retry while the retry still resumes the same
    /// reservation ledger/operation identity map the failed attempt used.
    #[cfg(test)]
    pub(crate) fn set_spawn_failure_injection_for_tests(&self, injector: SpawnFailureInjector) {
        *self
            .spawn_failure_injector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = injector;
    }

    /// Check the currently-armed failure injector for `point`. Called at each of the five
    /// boundaries the Stage 3 test gates require.
    pub(crate) fn check_spawn_failure_injection(
        &self,
        point: SpawnFailurePoint,
    ) -> Result<(), SpawnGateError> {
        self.spawn_failure_injector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .check(point)
    }

    /// Check that `root_thread_id` may accept a new controlled side effect right now. Called at
    /// every relevant transaction boundary of the enabled spawn orchestration, not only once
    /// upfront, per the Stage 3 freeze ("Root poison checking belongs inside every state
    /// transaction, not only a core preflight").
    pub(crate) fn ensure_root_usable(
        &self,
        root_thread_id: ThreadId,
    ) -> Result<(), RootCoordinationError> {
        // Poison is checked first and independently of `state_db` presence: it is a per-root
        // fact (test-injected in this stage; see the module doc comment on why no public
        // per-root poison query exists yet), so it must not be masked by the coarser
        // "state absent" case when both happen to apply to the same root.
        if self
            .poisoned_roots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&root_thread_id)
        {
            return Err(RootCoordinationError::Poisoned);
        }
        let Some(state_db) = self.state_db.as_ref() else {
            return Err(RootCoordinationError::StateAbsent);
        };
        if matches!(
            state_db.coordination_authority(),
            CoordinationAuthorityStatus::Quarantined { .. }
        ) {
            return Err(RootCoordinationError::Quarantined);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
