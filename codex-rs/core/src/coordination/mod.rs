//! Stage 3.3 ("gate live identity mapping and spawn") crate-private coordination surface.
//!
//! Everything under this module is capability-off in production:
//!
//! - [`control::CoordinationControl`] defaults to `Disabled` unconditionally. There is no
//!   config flag, environment flag, app-server method, public tool, or worker registration
//!   that can ever produce the `Enabled` variant outside test code. See
//!   `CoordinationControl::enabled_for_tests`.
//! - [`PreallocatedThreadIdentity`] is a generic capability (Decision 6) that works identically
//!   whether or not coordination is enabled; it is simply unused (`None`) on every production
//!   spawn path today.
//!
//! See the Stage 3 contract freeze (bd task `codex-9u5.2.3`, comment "Stage 3 contract freeze —
//! re-verification against current code (2026-07-21)") for the binding decisions this module
//! implements, in particular Decision 5 (stable live-operation identity) and Decision 6 (spawn
//! preallocation).

mod control;
mod message_gate;
mod operation_identity;
mod spawn_gate;

pub(crate) use control::CoordinationControl;
pub(crate) use control::CoordinationState;
pub(crate) use message_gate::MessageFailureInjector;
pub(crate) use message_gate::MessageFailurePoint;
pub(crate) use operation_identity::OperationIdentityKey;
pub(crate) use operation_identity::SemanticSlot;
pub(crate) use spawn_gate::SpawnFailureInjector;
pub(crate) use spawn_gate::SpawnFailurePoint;
pub(crate) use spawn_gate::SpawnReservationStage;

use codex_protocol::ThreadId;

/// Explicit, typed, optional preallocated identity for a spawned child (Decision 6).
///
/// When present, `Session::new` binds to `thread_id` instead of minting a fresh
/// `ThreadId::default()`, and the first `SessionIo::submit` call on that session binds to
/// `turn_id` instead of calling `new_submission_id()`. This is a generic capability: it has no
/// dependency on `CoordinationControl` and behaves identically regardless of whether
/// coordination is enabled. Every production spawn path passes `None` today.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreallocatedThreadIdentity {
    pub(crate) thread_id: ThreadId,
    pub(crate) turn_id: String,
}

impl PreallocatedThreadIdentity {
    /// Mint a fresh preallocated identity: a random `ThreadId` and a UUIDv7 turn id.
    ///
    /// This intentionally mirrors what `Session::new`/`SessionIo::submit` would have generated
    /// on their own -- preallocation only changes *when* the identity is chosen (before child
    /// creation, so it can be named in a durable intent/receipt) and *who* chooses it, not the
    /// shape of the identity itself.
    pub(crate) fn generate() -> Self {
        Self {
            thread_id: ThreadId::default(),
            turn_id: uuid::Uuid::now_v7().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_produces_distinct_identities() {
        let first = PreallocatedThreadIdentity::generate();
        let second = PreallocatedThreadIdentity::generate();
        assert_ne!(first, second);
    }
}
