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

use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use codex_state::CoordinationAuthorityStatus;

use super::message_gate::MessageFailureInjector;
use super::message_gate::MessageFailurePoint;
use super::message_gate::MessageGateError;
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
    // Same rationale as `spawn_failure_injector`, for the message/follow-up delivery
    // orchestration (Stage 3.4). A distinct field/mutex so a test can arm spawn and message
    // failure injection independently on the same shared state.
    message_failure_injector: Mutex<MessageFailureInjector>,
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
            message_failure_injector: Mutex::new(MessageFailureInjector::none()),
            poisoned_roots: Mutex::new(HashSet::new()),
        })
    }

    /// Replace the message/follow-up failure injector on an already-`Arc`-shared coordination
    /// state. Test-only, mirrors `set_spawn_failure_injection_for_tests`.
    #[cfg(test)]
    pub(crate) fn set_message_failure_injection_for_tests(&self, injector: MessageFailureInjector) {
        *self
            .message_failure_injector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = injector;
    }

    /// Check the currently-armed message/follow-up failure injector for `point`.
    pub(crate) fn check_message_failure_injection(
        &self,
        point: MessageFailurePoint,
    ) -> Result<(), MessageGateError> {
        self.message_failure_injector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .check(point)
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

    /// Resolve the `state_db`/current active state epoch pair used by every message/follow-up
    /// delivery call below. Distinct from `ensure_root_usable`'s coarser check because callers of
    /// this need the epoch value itself, not just a yes/no answer.
    fn state_db_and_epoch(&self) -> Result<(&StateDbHandle, StateEpoch), RootCoordinationError> {
        let state_db = self
            .state_db
            .as_ref()
            .ok_or(RootCoordinationError::StateAbsent)?;
        match state_db.coordination_authority() {
            CoordinationAuthorityStatus::Active { state_epoch } => Ok((state_db, *state_epoch)),
            CoordinationAuthorityStatus::Quarantined { .. } => {
                Err(RootCoordinationError::Quarantined)
            }
        }
    }

    /// Capture a queue-only `send_message` receipt (Decision 9). See
    /// `codex_state::capture_queue_message_receipt` for the fencing/convergence contract.
    pub(crate) async fn capture_queue_message_receipt(
        &self,
        root_thread_id: ThreadId,
        params: codex_state::CaptureQueueMessageReceipt,
    ) -> Result<codex_state::CaptureReceiptOutcome, MessageGateError> {
        let (state_db, epoch) = self.state_db_and_epoch()?;
        Ok(
            codex_state::capture_queue_message_receipt(state_db, root_thread_id, epoch, params)
                .await?,
        )
    }

    /// Reserve+accept the next sequential generation for a `followup_task` target and capture its
    /// receipt, atomically (Decision 9). See `codex_state::accept_followup_generation`.
    pub(crate) async fn accept_followup_generation(
        &self,
        root_thread_id: ThreadId,
        params: codex_state::AcceptFollowupGeneration,
    ) -> Result<codex_state::CaptureReceiptOutcome, MessageGateError> {
        let (state_db, epoch) = self.state_db_and_epoch()?;
        Ok(
            codex_state::accept_followup_generation(state_db, root_thread_id, epoch, params)
                .await?,
        )
    }

    /// Advance a receipt from `committed` to `enqueued` -- the controlled queue side effect.
    pub(crate) async fn mark_receipt_enqueued(
        &self,
        receipt_id: uuid::Uuid,
        now_ms: i64,
    ) -> Result<(), MessageGateError> {
        let (state_db, _epoch) = self.state_db_and_epoch()?;
        codex_state::mark_receipt_enqueued(state_db, receipt_id, now_ms).await?;
        Ok(())
    }

    /// Restart recovery, case 1: every receipt still `committed` (not yet enqueued) for
    /// `root_thread_id`.
    pub(crate) async fn pending_committed_receipts(
        &self,
        root_thread_id: ThreadId,
        limit: u32,
    ) -> Result<Vec<codex_state::MessageReceipt>, MessageGateError> {
        let (state_db, _epoch) = self.state_db_and_epoch()?;
        Ok(codex_state::pending_committed_receipts(state_db, root_thread_id, limit).await?)
    }

    /// Commit a receipt-to-response-item materialization row (Decision 9).
    pub(crate) async fn commit_materialization(
        &self,
        root_thread_id: ThreadId,
        receipt_id: uuid::Uuid,
        target_turn_id: &str,
        response_item_id: uuid::Uuid,
        now_ms: i64,
    ) -> Result<codex_state::MessageMaterialization, MessageGateError> {
        let (state_db, _epoch) = self.state_db_and_epoch()?;
        Ok(codex_state::commit_materialization(
            state_db,
            root_thread_id,
            receipt_id,
            target_turn_id,
            response_item_id,
            now_ms,
        )
        .await?)
    }

    /// Restart recovery, case 2: advance `committed` -> `rollout_appended` once the append has
    /// actually landed.
    pub(crate) async fn mark_materialization_rollout_appended(
        &self,
        receipt_id: uuid::Uuid,
        target_turn_id: &str,
        response_item_id: uuid::Uuid,
        now_ms: i64,
    ) -> Result<(), MessageGateError> {
        let (state_db, _epoch) = self.state_db_and_epoch()?;
        codex_state::mark_materialization_rollout_appended(
            state_db,
            receipt_id,
            target_turn_id,
            response_item_id,
            now_ms,
        )
        .await?;
        Ok(())
    }

    /// Restart recovery, case 3: advance `rollout_appended` -> `selected` once the item has
    /// actually been selected into a prompt.
    pub(crate) async fn mark_materialization_selected(
        &self,
        receipt_id: uuid::Uuid,
        target_turn_id: &str,
        response_item_id: uuid::Uuid,
        now_ms: i64,
    ) -> Result<(), MessageGateError> {
        let (state_db, _epoch) = self.state_db_and_epoch()?;
        codex_state::mark_materialization_selected(
            state_db,
            receipt_id,
            target_turn_id,
            response_item_id,
            now_ms,
        )
        .await?;
        Ok(())
    }

    /// Restart recovery, case 2 query: every materialization still `committed` for
    /// `root_thread_id`.
    pub(crate) async fn pending_committed_materializations(
        &self,
        root_thread_id: ThreadId,
        limit: u32,
    ) -> Result<Vec<codex_state::MessageMaterialization>, MessageGateError> {
        let (state_db, _epoch) = self.state_db_and_epoch()?;
        Ok(
            codex_state::pending_committed_materializations(state_db, root_thread_id, limit)
                .await?,
        )
    }

    /// Restart recovery, case 3 query: every materialization `rollout_appended` (selectable, not
    /// yet `selected`) for `root_thread_id`.
    pub(crate) async fn pending_appended_materializations(
        &self,
        root_thread_id: ThreadId,
        limit: u32,
    ) -> Result<Vec<codex_state::MessageMaterialization>, MessageGateError> {
        let (state_db, _epoch) = self.state_db_and_epoch()?;
        Ok(codex_state::pending_appended_materializations(state_db, root_thread_id, limit).await?)
    }
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
