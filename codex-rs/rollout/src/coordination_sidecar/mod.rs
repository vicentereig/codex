//! Stage 3 (`codex-9u5.2.3.2`) durable coordination sidecar writer and
//! dispatcher.
//!
//! Capability-off: nothing in this module is registered as a running
//! task/worker, exposed as a public API, or reachable outside test-driven
//! invocation. See `record.rs` for the on-disk JSONL shape and its
//! append-order contract, `writer.rs` for the durable append/dedupe
//! primitive, `observer.rs` for the internal notify seam, and
//! `dispatcher.rs` for the claim -> append -> notify -> ack orchestration
//! against `codex_state`'s native and degradation publication outboxes.

pub(crate) mod dispatcher;
pub(crate) mod observer;
pub(crate) mod record;
pub(crate) mod writer;

#[cfg(test)]
mod dispatcher_tests;
#[cfg(test)]
mod writer_tests;
