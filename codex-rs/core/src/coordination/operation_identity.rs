//! Durable live-operation identity mapping (Stage 3 contract freeze, Decision 5).
//!
//! `call_id` is a plain, model-generated `String` with no structural guarantee (see
//! `protocol/src/models.rs`, `protocol/src/approvals.rs`), and `turn.sub_id` is reused across
//! every tool call in a turn, so neither can serve as a per-call operation identity. This module
//! adds the missing piece: a mapping keyed by the exact bounded native source tuple
//! `(root_thread_id, actor_thread_id, actor_turn_id, call_id, semantic_slot)` that allocates a
//! [`CoordinationOperationId`] (UUIDv7) the first time a key is seen and returns the identical id
//! for every later lookup of the identical key. A key that differs in exactly one field names a
//! different operation entirely; it is never silently merged with another key.
//!
//! ## Durability scope (in-memory, not SQLite-backed)
//!
//! This mapping is held in memory only, scoped to the lifetime of the owning `AgentControl`'s
//! shared state for one root thread tree (i.e. for as long as that tree stays resident in this
//! process). It is *not* persisted to SQLite and does not survive a process restart. This is a
//! deliberate scoping decision for Stage 3.3, not an oversight -- see the task report for the
//! full reasoning, summarized here:
//!
//! - `call_id`/`actor_turn_id` are themselves process-session-scoped values with no durable
//!   contract of their own; nothing today persists them as authoritative identity across a
//!   restart.
//! - The durable idempotency the state layer actually enforces for coordination commands keys
//!   off the *operation id* embedded in the command intent plus a tuple fingerprint stored in
//!   `coordination_events`/`coordination_commands` (see
//!   `state/src/runtime/coordination/command_identity.rs::preflight_identity`). That DB-side
//!   check is what must ultimately provide cross-restart duplicate-safety once a real command
//!   transaction is wired to this path (Decision 6's ordered sequence); this in-process map's
//!   job is narrower -- giving a *stable* answer for repeated handler invocations within one
//!   running process (e.g. a client-side submission retry) without minting a fresh UUID each
//!   time, which is exactly the failure mode Decision 5 calls out ("generating a new UUIDv7 on
//!   each handler retry defeats idempotency").
//! - `CoordinationControl::Enabled` cannot be constructed by any production caller in this stage,
//!   so there is no in-production restart scenario yet that this scoping choice could affect.
//!
//! If a future stage needs this exact mapping to survive a process restart (for example, to
//! answer "what operation id did we already allocate for this call_id" after a crash mid-turn),
//! that is a genuine open question this comment flags rather than silently resolves -- see the
//! task report's explicit callout.

use std::collections::HashMap;
use std::sync::Mutex;

use codex_protocol::ThreadId;
use uuid::Uuid;

/// Stable identity for one live coordination operation.
///
/// Internal bookkeeping only: this is never serialized as a protocol field, and it carries no
/// payload -- identity only, per Decision 5.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CoordinationOperationId(Uuid);

impl CoordinationOperationId {
    fn allocate() -> Self {
        Self(Uuid::now_v7())
    }

    #[cfg(test)]
    pub(crate) fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for CoordinationOperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// The semantic kind of operation a live-operation identity was allocated for.
///
/// Only `Spawn` has a live producer in Stage 3.3. Later children (message/follow-up, interrupt,
/// wait, detach) add their own variants alongside their own producers; do not add unused
/// variants ahead of a real caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SemanticSlot {
    Spawn,
}

/// Exact composite key naming one live coordination operation (Decision 5).
///
/// Equality (and therefore lookup in [`OperationIdentityMap`]) is exact on every field: two keys
/// that differ in exactly one field name two different operations and never resolve to the same
/// [`CoordinationOperationId`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OperationIdentityKey {
    pub(crate) root_thread_id: ThreadId,
    pub(crate) actor_thread_id: ThreadId,
    pub(crate) actor_turn_id: String,
    pub(crate) call_id: String,
    pub(crate) semantic_slot: SemanticSlot,
}

/// In-process mapping from [`OperationIdentityKey`] to the [`CoordinationOperationId`] allocated
/// for it. See the module doc comment for the in-memory-vs-durable reasoning.
#[derive(Default)]
pub(crate) struct OperationIdentityMap {
    entries: Mutex<HashMap<OperationIdentityKey, CoordinationOperationId>>,
}

impl OperationIdentityMap {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Resolve `key` to its operation id, allocating a fresh UUIDv7 the first time this exact
    /// key is seen. Every later call with the identical key returns the same id.
    pub(crate) fn resolve(&self, key: OperationIdentityKey) -> CoordinationOperationId {
        let mut entries = self.lock_entries();
        *entries
            .entry(key)
            .or_insert_with(CoordinationOperationId::allocate)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock_entries().len()
    }

    fn lock_entries(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<OperationIdentityKey, CoordinationOperationId>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
#[path = "operation_identity_tests.rs"]
mod tests;
