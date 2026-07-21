//! Dispatcher: claims durable coordination publications from `codex-state`
//! (native `.2.3.1` R+1 outbox, and the degradation/compatibility outbox)
//! and turns each into a durable sidecar JSONL line before acking.
//!
//! # Append-before-ack
//!
//! For each claimed lease: append the record durably (create/open, write,
//! flush, file `fsync`, and — only on first creation — directory `fsync`;
//! see `writer.rs`), notify the observer, and only then ack (materialize)
//! the outbox row. If the process stops at any point before the ack, the
//! outbox row simply stays leased/pending; a later retry reopens the
//! sidecar file (rescanning it, per `writer.rs`), and the record's already-
//! durable presence makes the retried append a no-op — so the retry can
//! safely re-run the entire append/notify/ack sequence and reach the same
//! outcome without ever double-appending.
//!
//! # Per-root serialization, cross-root independence
//!
//! [`SidecarDispatcher`] holds one lock per root thread id. Dispatching root
//! A never blocks on root B's lock, so independent roots progress
//! concurrently; dispatching the same root twice concurrently serializes on
//! that root's lock, so a single root's sidecar file never receives
//! interleaved/torn writes from two concurrent dispatch attempts.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use codex_state::SidecarClaimOutcome;
use codex_state::SidecarResolution;
use codex_state::StateRuntime;
use tokio::sync::Mutex;
use tokio::sync::OwnedMutexGuard;

use super::observer::SidecarObservedEvent;
use super::observer::SidecarObservedKind;
use super::observer::SidecarObserver;
use super::record::DegradationSidecarRecord;
use super::record::NativeSidecarRecord;
use super::record::SidecarRecord;
use super::writer::NoSidecarFailure;
use super::writer::SidecarFailureInjector;
use super::writer::SidecarWriteError;
use super::writer::SidecarWriter;

/// One claim/lease batch per tick, per outbox. Native publications only
/// ever have one eligible row per root (the immediate R+1 successor), so
/// this bound only matters for the degradation outbox, which can have many
/// simultaneously-eligible rows.
const CLAIM_LIMIT: u32 = 16;
const LEASE_DURATION_MS: i64 = 30_000;
const RETRY_AFTER_MS: i64 = 1_000;

/// Supplies the root's own current rollout path, if currently known/loaded.
/// `None` represents "missing/deferred" (for example: an archived thread
/// not yet rehydrated, or a rollout not yet flushed to disk) — a retryable
/// condition per Decision 2 of the Stage 3 contract freeze, never a reason
/// to skip a revision. A real integration (a later stage) would back this
/// with the thread store; here it exists purely so this dispatcher is
/// test-driveable without one.
pub(crate) trait RootRolloutPathResolver: Send + Sync {
    fn resolve(&self, root_thread_id: ThreadId) -> Option<PathBuf>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) enum DispatchStep {
    AfterAppend,
    AfterNotify,
}

pub(crate) trait DispatchFailureInjector: Send + Sync {
    fn before_step(&self, step: DispatchStep) -> std::io::Result<()>;
}

pub(crate) struct NoDispatchFailure;

impl DispatchFailureInjector for NoDispatchFailure {
    fn before_step(&self, _step: DispatchStep) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DispatchOutcome {
    Dispatched {
        native: usize,
        degradation: usize,
    },
    /// The root's rollout path is not currently known; nothing was
    /// dispatched this tick. Not an error, and never a reason to skip a
    /// revision: the outbox rows remain pending for a later tick once the
    /// resolver can answer.
    Deferred,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DispatchError {
    #[error("root rollout path has no valid file name ending in .jsonl")]
    InvalidRolloutPath,
    #[error(transparent)]
    State(#[from] codex_state::SidecarStateError),
    #[error(transparent)]
    Write(#[from] SidecarWriteError),
    #[error("dispatch crash-injected at {0:?}")]
    Injected(DispatchStep),
}

impl From<std::io::Error> for DispatchError {
    fn from(error: std::io::Error) -> Self {
        Self::Write(SidecarWriteError::from(error))
    }
}

/// Compute `<root-rollout-stem>.coordination-v1-<epoch>.jsonl` (Decision 2).
/// Only ever called when no sidecar path has been persisted yet; the result
/// still goes through `codex_state::persist_root_sidecar_path`'s
/// first-writer-wins CAS before being trusted, so a race between two
/// dispatchers computing this concurrently is resolved there, not here.
pub(crate) fn candidate_sidecar_path(
    root_rollout_path: &Path,
    state_epoch: &StateEpoch,
) -> Result<PathBuf, DispatchError> {
    let file_name = root_rollout_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(DispatchError::InvalidRolloutPath)?;
    let stem = file_name
        .strip_suffix(".jsonl")
        .ok_or(DispatchError::InvalidRolloutPath)?;
    let sidecar_name = format!("{stem}.coordination-v1-{state_epoch}.jsonl");
    Ok(root_rollout_path.with_file_name(sidecar_name))
}

pub(crate) struct SidecarDispatcher<R: RootRolloutPathResolver, O: SidecarObserver> {
    resolver: R,
    observer: O,
    root_locks: Mutex<HashMap<ThreadId, Arc<Mutex<()>>>>,
}

impl<R: RootRolloutPathResolver, O: SidecarObserver> SidecarDispatcher<R, O> {
    pub(crate) fn new(resolver: R, observer: O) -> Self {
        Self {
            resolver,
            observer,
            root_locks: Mutex::new(HashMap::new()),
        }
    }

    async fn lock_for(&self, root_thread_id: ThreadId) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.root_locks.lock().await;
            locks
                .entry(root_thread_id)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    /// Resolve the root's sidecar path (Decision 2: computed once,
    /// persisted, never re-derived afterward), claim and durably dispatch
    /// one tick's worth of native and then degradation publications, and
    /// return how many of each were dispatched. Serialized per root;
    /// independent roots never block on each other.
    pub(crate) async fn dispatch_root(
        &self,
        runtime: &StateRuntime,
        root_thread_id: ThreadId,
        now_ms: i64,
    ) -> Result<DispatchOutcome, DispatchError> {
        let _guard = self.lock_for(root_thread_id).await;
        let epoch = codex_state::active_state_epoch(runtime).await?;
        let Some(sidecar_path) = self.resolve_path(runtime, root_thread_id, epoch).await? else {
            return Ok(DispatchOutcome::Deferred);
        };
        let mut writer = SidecarWriter::open(sidecar_path).await?;

        let native = self
            .dispatch_native(runtime, &mut writer, root_thread_id, epoch, now_ms)
            .await?;
        let degradation = self
            .dispatch_degradation(runtime, &mut writer, root_thread_id, epoch, now_ms)
            .await?;
        Ok(DispatchOutcome::Dispatched {
            native,
            degradation,
        })
    }

    async fn resolve_path(
        &self,
        runtime: &StateRuntime,
        root_thread_id: ThreadId,
        epoch: StateEpoch,
    ) -> Result<Option<PathBuf>, DispatchError> {
        if let Some(persisted) = codex_state::root_sidecar_path(runtime, root_thread_id).await? {
            return Ok(Some(PathBuf::from(persisted)));
        }
        let Some(root_rollout_path) = self.resolver.resolve(root_thread_id) else {
            return Ok(None);
        };
        let candidate = candidate_sidecar_path(&root_rollout_path, &epoch)?;
        let canonical = codex_state::persist_root_sidecar_path(
            runtime,
            root_thread_id,
            epoch,
            &candidate.to_string_lossy(),
        )
        .await?;
        Ok(Some(PathBuf::from(canonical)))
    }

    async fn dispatch_native(
        &self,
        runtime: &StateRuntime,
        writer: &mut SidecarWriter,
        root_thread_id: ThreadId,
        epoch: StateEpoch,
        now_ms: i64,
    ) -> Result<usize, DispatchError> {
        let claimed = codex_state::claim_native_publications(
            runtime,
            root_thread_id,
            epoch,
            now_ms,
            now_ms + LEASE_DURATION_MS,
            CLAIM_LIMIT,
        )
        .await?;
        let SidecarClaimOutcome::Claimed(leases) = claimed else {
            return Ok(0);
        };
        let mut dispatched = 0;
        for lease in leases {
            let revision = lease.revision;
            let event_id = lease.event_id.to_string();
            let record = SidecarRecord::Native(NativeSidecarRecord {
                event_id: event_id.clone(),
                root_thread_id: root_thread_id.to_string(),
                state_epoch: epoch.to_string(),
                revision,
                materialized_at_ms: now_ms,
            });
            match append_notify(
                writer,
                &self.observer,
                &record,
                root_thread_id,
                epoch,
                SidecarObservedKind::Native { revision },
                &NoSidecarFailure,
                &NoDispatchFailure,
            )
            .await
            {
                Ok(()) => {
                    codex_state::resolve_native_publication(
                        runtime,
                        lease,
                        epoch,
                        SidecarResolution::Materialized,
                        now_ms,
                    )
                    .await?;
                    dispatched += 1;
                }
                Err(DispatchError::Write(SidecarWriteError::Io(_))) => {
                    codex_state::resolve_native_publication(
                        runtime,
                        lease,
                        epoch,
                        SidecarResolution::Retry {
                            retry_after_ms: now_ms + RETRY_AFTER_MS,
                        },
                        now_ms,
                    )
                    .await?;
                }
                Err(DispatchError::Write(_)) => {
                    codex_state::resolve_native_publication(
                        runtime,
                        lease,
                        epoch,
                        SidecarResolution::Poisoned,
                        now_ms,
                    )
                    .await?;
                }
                Err(other) => return Err(other),
            }
        }
        Ok(dispatched)
    }

    async fn dispatch_degradation(
        &self,
        runtime: &StateRuntime,
        writer: &mut SidecarWriter,
        root_thread_id: ThreadId,
        epoch: StateEpoch,
        now_ms: i64,
    ) -> Result<usize, DispatchError> {
        let claimed = codex_state::claim_degradation_publications(
            runtime,
            root_thread_id,
            epoch,
            now_ms,
            now_ms + LEASE_DURATION_MS,
            CLAIM_LIMIT,
        )
        .await?;
        let SidecarClaimOutcome::Claimed(leases) = claimed else {
            return Ok(0);
        };
        let mut dispatched = 0;
        for lease in leases {
            let after_revision = lease.after_revision;
            let source_ordinal = lease.source_ordinal;
            let record = SidecarRecord::Degradation(DegradationSidecarRecord {
                degradation_id: lease.degradation_id.clone(),
                root_thread_id: root_thread_id.to_string(),
                state_epoch: epoch.to_string(),
                after_revision,
                source_ordinal,
                stable_record_id: lease.stable_record_id.clone(),
                materialized_at_ms: now_ms,
            });
            match append_notify(
                writer,
                &self.observer,
                &record,
                root_thread_id,
                epoch,
                SidecarObservedKind::Degradation {
                    after_revision,
                    source_ordinal,
                },
                &NoSidecarFailure,
                &NoDispatchFailure,
            )
            .await
            {
                Ok(()) => {
                    codex_state::resolve_degradation_publication(
                        runtime,
                        lease,
                        epoch,
                        SidecarResolution::Materialized,
                        now_ms,
                    )
                    .await?;
                    dispatched += 1;
                }
                Err(DispatchError::Write(SidecarWriteError::Io(_))) => {
                    codex_state::resolve_degradation_publication(
                        runtime,
                        lease,
                        epoch,
                        SidecarResolution::Retry {
                            retry_after_ms: now_ms + RETRY_AFTER_MS,
                        },
                        now_ms,
                    )
                    .await?;
                }
                Err(DispatchError::Write(_)) => {
                    codex_state::resolve_degradation_publication(
                        runtime,
                        lease,
                        epoch,
                        SidecarResolution::Poisoned,
                        now_ms,
                    )
                    .await?;
                }
                Err(other) => return Err(other),
            }
        }
        Ok(dispatched)
    }
}

/// The crash-simulation primitive shared by production dispatch and the
/// crash-matrix tests: append durably, notify, and stop — deliberately
/// *not* acking here. Any failure (a real write failure, or an injected
/// one) aborts immediately without acking, exactly modeling "the process
/// stopped right here"; the caller decides what an abort means (a
/// production caller maps it to retry/poison and still acks; a crash test
/// instead reopens a fresh writer and calls this again to prove replay
/// converges without duplication).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_notify(
    writer: &mut SidecarWriter,
    observer: &dyn SidecarObserver,
    record: &SidecarRecord,
    root_thread_id: ThreadId,
    epoch: StateEpoch,
    kind: SidecarObservedKind,
    write_injector: &dyn SidecarFailureInjector,
    dispatch_injector: &dyn DispatchFailureInjector,
) -> Result<(), DispatchError> {
    writer.append_if_new_with(record, write_injector).await?;
    dispatch_injector
        .before_step(DispatchStep::AfterAppend)
        .map_err(|_| DispatchError::Injected(DispatchStep::AfterAppend))?;
    observer.on_published(&SidecarObservedEvent {
        root_thread_id: root_thread_id.to_string(),
        state_epoch: epoch.to_string(),
        identity: record.identity().to_string(),
        kind,
    });
    dispatch_injector
        .before_step(DispatchStep::AfterNotify)
        .map_err(|_| DispatchError::Injected(DispatchStep::AfterNotify))?;
    Ok(())
}
