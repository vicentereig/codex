//! Internal observer seam for the coordination sidecar dispatcher.
//!
//! Stage 5 will attach a real subscriber here once an app-server-facing API
//! exists to notify. Stage 3 (`.2.3.2`) only ever wires [`NoopObserver`] in
//! production — no app-server API exists yet and none is added by this
//! task — plus a test spy (see `dispatcher_tests.rs`) used to assert the
//! seam fires with the right metadata, in the right order, and never
//! carries model/conversation content.
//!
//! Ordering contract: [`SidecarObserver::on_published`] fires after a
//! record has been durably appended (or found already durably present on a
//! replayed retry) but *before* the outbox ack. This lets a future
//! subscriber observe "durably on disk" strictly before "acknowledged", the
//! same append-before-ack ordering the outbox itself depends on.

/// Metadata describing one published sidecar record. Deliberately carries
/// only identities, revision/ordinal numbers, and root/epoch — never
/// model-visible content. See the sentinel test in `dispatcher_tests.rs`
/// asserting no plaintext/ciphertext ever reaches this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SidecarObservedEvent {
    pub root_thread_id: String,
    pub state_epoch: String,
    pub identity: String,
    pub kind: SidecarObservedKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SidecarObservedKind {
    Native {
        revision: u64,
    },
    Degradation {
        after_revision: u64,
        source_ordinal: u64,
    },
}

pub(crate) trait SidecarObserver: Send + Sync {
    fn on_published(&self, event: &SidecarObservedEvent);
}

/// Production default: no app-server API exists yet, so there is nothing to
/// notify. Capability stays off regardless (nothing calls the dispatcher
/// this observer is attached to outside of tests), but the seam is
/// deliberately real (not a stub `todo!()`) so Stage 5 only needs to swap
/// the implementation, not the call site.
pub(crate) struct NoopObserver;

impl SidecarObserver for NoopObserver {
    fn on_published(&self, _event: &SidecarObservedEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_observer_does_nothing_and_never_panics() {
        NoopObserver.on_published(&SidecarObservedEvent {
            root_thread_id: "019f7c6c-1111-7000-8000-000000000601".to_string(),
            state_epoch: "019f7c6c-0000-7000-8000-000000000001".to_string(),
            identity: "019f7c6c-2222-7000-8000-000000000701".to_string(),
            kind: SidecarObservedKind::Native { revision: 1 },
        });
    }
}
