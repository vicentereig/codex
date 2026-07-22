//! In-process spawn reservation ledger and failure-injection points for the enabled coordinated
//! spawn orchestration (Stage 3 contract freeze, Decision 6).
//!
//! The ledger enforces, within one process, the ordering Decision 6 froze: a command
//! transaction reserves the assignment + sender intent, a recipient transaction accepts/binds
//! it and records a durable receipt, only then is the child created with the exact preallocated
//! identity, and only after that does the sender get acknowledged. A duplicate spawn attempt for
//! the identical operation id resumes from (and never repeats) an already-completed stage --
//! this is what makes "duplicate spawn never creates a second child" hold.
//!
//! Scope note: this ledger lives in `core/`, in memory, for the lifetime of the owning
//! `CoordinationState`. It does not (yet) drive the real `coordination_commands`/
//! `coordination_inbox` SQLite tables that Stage 2 already shipped in `codex_state` -- those are
//! not exposed as public API from that crate today. Wiring this orchestration to the real durable
//! command/inbox transactions is future work (most likely `.2.3.4`, which already owns "typed
//! message/follow-up target receipt delivery"); see the task report for the full reasoning.
//! Because of that, this ledger's crash-injection tests simulate a *retry within the same
//! process* (the same `Arc<CoordinationState>`), not a full process restart -- a real process
//! restart today would drop this ledger entirely, same as it already drops the in-memory
//! `AgentRegistry`/`ThreadManagerState` that back ordinary (uncoordinated) spawns.

use std::collections::HashMap;
use std::sync::Mutex;

use super::PreallocatedThreadIdentity;
use super::operation_identity::CoordinationOperationId;

/// Ordered stages of the frozen enabled-spawn sequence (Decision 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SpawnReservationStage {
    /// One command transaction reserved assignment g1 + sender intent.
    IntentReserved,
    /// One recipient transaction accepted/bound g1 + durable receipt.
    ReceiptAccepted,
    /// The child was created and registered with the exact preallocated identity.
    ChildCreated,
    /// The receipt ref/turn was enqueued/started and the sender was acknowledged.
    Acknowledged,
}

/// The five boundaries the Stage 3 test gates require failure injection at: before intent, after
/// intent, after receipt, before child creation, and after the controlled side effect but before
/// ack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpawnFailurePoint {
    BeforeIntent,
    AfterIntent,
    AfterReceipt,
    BeforeChildCreation,
    AfterSideEffectBeforeAck,
}

/// Failure the enabled spawn orchestration surfaces to its caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SpawnGateError {
    #[error("coordination root is not usable: {0}")]
    RootNotUsable(#[from] super::control::RootCoordinationError),
    #[error("injected failure at {0:?}")]
    InjectedFailure(SpawnFailurePoint),
}

/// Test-controlled failure injector for the five boundaries above. Adapted from the
/// `RecoveryFailureInjector`/crash-matrix idiom already established in
/// `state/src/runtime/coordination/failure_injection_support.rs`, simplified for an in-process,
/// single-shot ledger rather than a durable, replay-counted one.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SpawnFailureInjector {
    target: Option<SpawnFailurePoint>,
}

impl SpawnFailureInjector {
    pub(crate) fn none() -> Self {
        Self { target: None }
    }

    pub(crate) fn fail_at(point: SpawnFailurePoint) -> Self {
        Self {
            target: Some(point),
        }
    }

    pub(crate) fn check(self, point: SpawnFailurePoint) -> Result<(), SpawnGateError> {
        if self.target == Some(point) {
            return Err(SpawnGateError::InjectedFailure(point));
        }
        Ok(())
    }
}

struct SpawnReservation {
    stage: SpawnReservationStage,
    identity: PreallocatedThreadIdentity,
}

/// In-process ledger of spawn reservations keyed by [`CoordinationOperationId`].
#[derive(Default)]
pub(crate) struct SpawnReservationLedger {
    reservations: Mutex<HashMap<CoordinationOperationId, SpawnReservation>>,
}

impl SpawnReservationLedger {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Reserve (or resume) the spawn for `operation_id`. If a reservation already exists for
    /// this exact operation id, its previously allocated identity is returned unchanged and
    /// `allocate_identity` is never called -- this is what makes a duplicate/retried spawn
    /// attempt for the same operation key reuse the same thread/turn identity.
    pub(crate) fn reserve_intent(
        &self,
        operation_id: CoordinationOperationId,
        allocate_identity: impl FnOnce() -> PreallocatedThreadIdentity,
    ) -> PreallocatedThreadIdentity {
        let mut reservations = self.lock_reservations();
        reservations
            .entry(operation_id)
            .or_insert_with(|| SpawnReservation {
                stage: SpawnReservationStage::IntentReserved,
                identity: allocate_identity(),
            })
            .identity
            .clone()
    }

    /// Current stage of `operation_id`'s reservation, or `None` if it was never reserved.
    pub(crate) fn stage(
        &self,
        operation_id: CoordinationOperationId,
    ) -> Option<SpawnReservationStage> {
        self.lock_reservations()
            .get(&operation_id)
            .map(|reservation| reservation.stage)
    }

    /// Advance `operation_id`'s reservation to `stage`, if it is further along than the current
    /// stage. Advancing a nonexistent reservation is a no-op: `reserve_intent` must be called
    /// first.
    pub(crate) fn advance(
        &self,
        operation_id: CoordinationOperationId,
        stage: SpawnReservationStage,
    ) {
        let mut reservations = self.lock_reservations();
        if let Some(reservation) = reservations.get_mut(&operation_id)
            && reservation.stage < stage
        {
            reservation.stage = stage;
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock_reservations().len()
    }

    fn lock_reservations(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<CoordinationOperationId, SpawnReservation>> {
        self.reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
#[path = "spawn_gate_tests.rs"]
mod tests;
