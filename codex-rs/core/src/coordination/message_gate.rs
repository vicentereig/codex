//! Test-controlled failure injection for the enabled message/follow-up delivery orchestration
//! (Stage 3 contract freeze, Decision 9; `codex-9u5.2.3.4`).
//!
//! Unlike [`super::spawn_gate`]'s `SpawnReservationLedger`, message/follow-up delivery does not
//! need an in-process reservation ledger: durability lives entirely in `codex_state`'s
//! `coordination_message_receipts`/`coordination_message_target_generations`/
//! `coordination_message_materializations` tables (`state/src/runtime/coordination/message_api.rs`),
//! which are idempotent on `operation_id` by construction. This module only supplies the
//! boundary-checked failure injection the Stage 3 test gates require, adapted from
//! `SpawnFailureInjector`/`SpawnFailurePoint`.

/// The boundaries the Stage 3 test gates require failure injection at for message/follow-up
/// delivery: before the sender-side operation identity is resolved, after it, after the
/// target-side receipt (and, for follow-up, generation acceptance) is durably committed, before
/// the receipt is marked enqueued (the controlled queue side effect), and after that side effect
/// but before the caller is acknowledged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MessageFailurePoint {
    BeforeIntent,
    AfterIntent,
    AfterReceipt,
    BeforeEnqueue,
    AfterEnqueueBeforeAck,
}

/// Failure the enabled message/follow-up delivery orchestration surfaces to its caller.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MessageGateError {
    #[error("coordination root is not usable: {0}")]
    RootNotUsable(#[from] super::control::RootCoordinationError),
    #[error("injected failure at {0:?}")]
    InjectedFailure(MessageFailurePoint),
    #[error("coordination message delivery state error: {0}")]
    State(#[from] codex_state::MessageDeliveryError),
}

/// Test-controlled failure injector for the five boundaries above. Same shape as
/// `SpawnFailureInjector`, kept as a distinct type so a test can arm spawn and message injection
/// independently on the same shared `CoordinationState`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MessageFailureInjector {
    target: Option<MessageFailurePoint>,
}

impl MessageFailureInjector {
    pub(crate) fn none() -> Self {
        Self { target: None }
    }

    pub(crate) fn fail_at(point: MessageFailurePoint) -> Self {
        Self {
            target: Some(point),
        }
    }

    pub(crate) fn check(self, point: MessageFailurePoint) -> Result<(), MessageGateError> {
        if self.target == Some(point) {
            return Err(MessageGateError::InjectedFailure(point));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "message_gate_tests.rs"]
mod tests;
