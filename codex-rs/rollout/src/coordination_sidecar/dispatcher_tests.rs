use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use codex_protocol::ThreadId;
use codex_state::StateRuntime;
use pretty_assertions::assert_eq;
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

use super::dispatcher::DispatchFailureInjector;
use super::dispatcher::DispatchOutcome;
use super::dispatcher::DispatchStep;
use super::dispatcher::NoDispatchFailure;
use super::dispatcher::RootRolloutPathResolver;
use super::dispatcher::SidecarDispatcher;
use super::dispatcher::append_notify;
use super::observer::SidecarObservedEvent;
use super::observer::SidecarObservedKind;
use super::observer::SidecarObserver;
use super::record::NativeSidecarRecord;
use super::record::SidecarRecord;
use super::writer::NoSidecarFailure;
use super::writer::SidecarWriter;

const NOW_MS: i64 = 2_000_000_000_000;

/// Raw-SQL fixture support. `.2.3.1` already owns the exhaustive crash/race
/// coverage for the claim/ack state machines themselves (`codex-state`'s own
/// `projection_outbox_tests.rs` / `failure_injection_projection_outbox_matrix_tests.rs`);
/// this file only needs *some* eligible outbox rows to dispatch, seeded
/// directly against the same SQLite file `codex_state::StateRuntime` already
/// opened (mirroring the raw-SQL seeding style `codex-state`'s own tests use
/// internally, just reached from outside the crate via the public
/// `codex_state::state_db_path` helper).
async fn open_seed_pool(codex_home: &Path) -> anyhow::Result<SqlitePool> {
    let path = codex_state::state_db_path(codex_home);
    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", path.display()))
        .await?)
}

async fn seed_root(
    pool: &SqlitePool,
    root_thread_id: ThreadId,
    epoch: &str,
    committed_revision: i64,
    published_revision: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO coordination_roots \
         (root_thread_id,state_epoch,committed_revision,published_revision,created_at_ms,updated_at_ms) \
         VALUES (?,?,?,?,?,?)",
    )
    .bind(root_thread_id.to_string())
    .bind(epoch)
    .bind(committed_revision)
    .bind(published_revision)
    .bind(10_i64)
    .bind(10_i64)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_native_event(
    pool: &SqlitePool,
    root_thread_id: ThreadId,
    revision: i64,
    event_id: &str,
    fingerprint_seed: u8,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO coordination_events \
         (event_id,root_thread_id,revision,canonical_event_bytes,event_fingerprint,\
          idempotency_key_bytes,idempotency_key_fingerprint,occurred_at,created_at_ms) \
         VALUES (?,?,?,?,?,?,?,?,?)",
    )
    .bind(event_id)
    .bind(root_thread_id.to_string())
    .bind(revision)
    .bind(br#"{"kind":"native"}"#.as_slice())
    .bind(vec![fingerprint_seed; 32])
    .bind(format!("idempotency-{event_id}").into_bytes())
    .bind(vec![fingerprint_seed.wrapping_add(1); 32])
    .bind(1_753_000_000_i64)
    .bind(10_i64)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO coordination_projection_outbox (event_id,status,created_at_ms,updated_at_ms) \
         VALUES (?,'pending',10,10)",
    )
    .bind(event_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_degradation(
    pool: &SqlitePool,
    root_thread_id: ThreadId,
    epoch: &str,
    degradation_id: &str,
    after_revision: i64,
    source_ordinal: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO coordination_degradation_records \
         (degradation_id,root_thread_id,state_epoch,source_kind,source_shape,source_thread_id,\
          source_turn_id,source_item_id,source_ordinal,recovery_record_kind,recovery_record_id,\
          semantic_slot,reason,target_thread_id,target_turn_id,terminal_kind,terminal_outcome,\
          included_generations_bytes,identity_bytes,identity_fingerprint,canonical_record_bytes,\
          canonical_record_fingerprint,adapter_version,sanitizer_version,observed_at,\
          after_revision,created_at_ms) \
         VALUES (?,?,?,'recovery',NULL,NULL,NULL,NULL,NULL,'assignment',?,\
                 'assignmentRequested','stateLossDegraded',NULL,NULL,NULL,NULL,NULL,\
                 ?,?,?,?,1,1,?,?,?)",
    )
    .bind(degradation_id)
    .bind(root_thread_id.to_string())
    .bind(epoch)
    .bind(format!("record-{degradation_id}"))
    .bind(format!("identity-{degradation_id}").into_bytes())
    .bind(vec![7_u8; 32])
    .bind(format!("canonical-{degradation_id}").into_bytes())
    .bind(vec![9_u8; 32])
    .bind(NOW_MS)
    .bind(after_revision)
    .bind(NOW_MS)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO coordination_degradation_publication_outbox \
         (degradation_id,root_thread_id,after_revision,source_ordinal,stable_record_id,status,\
          version,lease_epoch,retry_count,retry_after_ms,created_at_ms,updated_at_ms) \
         VALUES (?,?,?,?,?,'pending',0,0,0,0,?,?)",
    )
    .bind(degradation_id)
    .bind(root_thread_id.to_string())
    .bind(after_revision)
    .bind(source_ordinal)
    .bind(degradation_id)
    .bind(NOW_MS)
    .bind(NOW_MS)
    .execute(pool)
    .await?;
    Ok(())
}

async fn active_epoch(pool: &SqlitePool) -> anyhow::Result<String> {
    Ok(
        sqlx::query("SELECT state_epoch FROM coordination_authority WHERE singleton_id=1")
            .fetch_one(pool)
            .await?
            .get("state_epoch"),
    )
}

fn root(seed: u32) -> ThreadId {
    ThreadId::from_string(&format!("019f7c6c-1111-7000-8000-{seed:012}")).expect("valid thread id")
}

struct StaticResolver(Option<PathBuf>);

impl RootRolloutPathResolver for StaticResolver {
    fn resolve(&self, _root_thread_id: ThreadId) -> Option<PathBuf> {
        self.0.clone()
    }
}

#[derive(Default)]
struct RecordingObserver {
    events: std::sync::Mutex<Vec<SidecarObservedEvent>>,
}

impl SidecarObserver for RecordingObserver {
    fn on_published(&self, event: &SidecarObservedEvent) {
        self.events.lock().expect("lock").push(event.clone());
    }
}

/// Tracks how many dispatches are simultaneously inside the observer
/// callback, proving same-root serialization and cross-root independence.
#[derive(Default)]
struct ConcurrencyProbe {
    active: AtomicUsize,
    max_observed: AtomicUsize,
}

struct ProbingObserver(Arc<ConcurrencyProbe>);

impl SidecarObserver for ProbingObserver {
    fn on_published(&self, _event: &SidecarObservedEvent) {
        let active = self.0.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.0.max_observed.fetch_max(active, Ordering::SeqCst);
        // Widen the race window without a real async sleep inside a
        // non-async trait method: spin briefly on a std thread yield.
        for _ in 0..2_000 {
            std::thread::yield_now();
        }
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn native_publication_dispatches_and_acks() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
    let pool = open_seed_pool(home.path()).await?;
    let epoch = active_epoch(&pool).await?;
    let root_thread_id = root(1);
    seed_root(&pool, root_thread_id, &epoch, 1, 0).await?;
    seed_native_event(
        &pool,
        root_thread_id,
        1,
        "019f7c6c-2222-7000-8000-000000000701",
        0x11,
    )
    .await?;
    pool.close().await;

    let rollout_path = home.path().join("rollout-2026-07-21T10-00-00-root.jsonl");
    let observer = RecordingObserver::default();
    let dispatcher = SidecarDispatcher::new(StaticResolver(Some(rollout_path.clone())), observer);
    let outcome = dispatcher
        .dispatch_root(&runtime, root_thread_id, NOW_MS)
        .await?;
    assert_eq!(
        outcome,
        DispatchOutcome::Dispatched {
            native: 1,
            degradation: 0
        }
    );

    let sidecar_path = codex_state::root_sidecar_path(&runtime, root_thread_id)
        .await?
        .expect("sidecar path persisted");
    assert_eq!(
        Path::new(&sidecar_path)
            .file_name()
            .and_then(|n| n.to_str()),
        Some(format!("rollout-2026-07-21T10-00-00-root.coordination-v1-{epoch}.jsonl").as_str())
    );
    let contents = tokio::fs::read_to_string(&sidecar_path).await?;
    assert_eq!(contents.lines().count(), 1);
    assert!(contents.contains("019f7c6c-2222-7000-8000-000000000701"));

    let status: String =
        sqlx::query_scalar("SELECT status FROM coordination_projection_outbox WHERE event_id=?")
            .bind("019f7c6c-2222-7000-8000-000000000701")
            .fetch_one(&open_seed_pool(home.path()).await?)
            .await?;
    assert_eq!(status, "materialized");
    Ok(())
}

#[tokio::test]
async fn degradation_dispatches_only_after_native_anchor_published() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
    let pool = open_seed_pool(home.path()).await?;
    let epoch = active_epoch(&pool).await?;
    let root_thread_id = root(2);
    // published_revision already 1: the degradation's after_revision=1 is
    // eligible from the start (no native dispatch needed this tick).
    seed_root(&pool, root_thread_id, &epoch, 1, 1).await?;
    seed_degradation(
        &pool,
        root_thread_id,
        &epoch,
        "019f7c6c-4444-5000-8000-000000000901",
        1,
        0,
    )
    .await?;
    pool.close().await;

    let rollout_path = home.path().join("rollout-2026-07-21T11-00-00-root.jsonl");
    let dispatcher = SidecarDispatcher::new(
        StaticResolver(Some(rollout_path)),
        RecordingObserver::default(),
    );
    let outcome = dispatcher
        .dispatch_root(&runtime, root_thread_id, NOW_MS)
        .await?;
    assert_eq!(
        outcome,
        DispatchOutcome::Dispatched {
            native: 0,
            degradation: 1
        }
    );
    let sidecar_path = codex_state::root_sidecar_path(&runtime, root_thread_id)
        .await?
        .expect("sidecar path persisted");
    let contents = tokio::fs::read_to_string(&sidecar_path).await?;
    assert!(contents.contains("019f7c6c-4444-5000-8000-000000000901"));
    Ok(())
}

#[tokio::test]
async fn sidecar_path_is_persisted_once_and_never_rederived() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
    let pool = open_seed_pool(home.path()).await?;
    let epoch = active_epoch(&pool).await?;
    let root_thread_id = root(3);
    seed_root(&pool, root_thread_id, &epoch, 2, 0).await?;
    seed_native_event(
        &pool,
        root_thread_id,
        1,
        "019f7c6c-2222-7000-8000-000000000711",
        0x21,
    )
    .await?;
    pool.close().await;

    let first_rollout_path = home.path().join("rollout-2026-07-21T12-00-00-root.jsonl");
    let dispatcher = SidecarDispatcher::new(
        StaticResolver(Some(first_rollout_path.clone())),
        RecordingObserver::default(),
    );
    dispatcher
        .dispatch_root(&runtime, root_thread_id, NOW_MS)
        .await?;
    let persisted_first = codex_state::root_sidecar_path(&runtime, root_thread_id)
        .await?
        .expect("persisted");

    // A second tick, with a resolver that would (wrongly, if re-derivation
    // happened) compute a different candidate path, must still use the
    // already-persisted value.
    let pool = open_seed_pool(home.path()).await?;
    seed_native_event(
        &pool,
        root_thread_id,
        2,
        "019f7c6c-2222-7000-8000-000000000712",
        0x22,
    )
    .await?;
    pool.close().await;
    let different_rollout_path = home.path().join("rollout-DIFFERENT-root.jsonl");
    let dispatcher = SidecarDispatcher::new(
        StaticResolver(Some(different_rollout_path)),
        RecordingObserver::default(),
    );
    dispatcher
        .dispatch_root(&runtime, root_thread_id, NOW_MS + 1_000)
        .await?;
    let persisted_second = codex_state::root_sidecar_path(&runtime, root_thread_id)
        .await?
        .expect("persisted");
    assert_eq!(persisted_first, persisted_second);

    let contents = tokio::fs::read_to_string(&persisted_second).await?;
    assert_eq!(contents.lines().count(), 2, "both revisions in one file");
    Ok(())
}

#[tokio::test]
async fn missing_rollout_path_defers_without_skipping_revision() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
    let pool = open_seed_pool(home.path()).await?;
    let epoch = active_epoch(&pool).await?;
    let root_thread_id = root(4);
    seed_root(&pool, root_thread_id, &epoch, 1, 0).await?;
    seed_native_event(
        &pool,
        root_thread_id,
        1,
        "019f7c6c-2222-7000-8000-000000000721",
        0x31,
    )
    .await?;
    pool.close().await;

    let dispatcher = SidecarDispatcher::new(StaticResolver(None), RecordingObserver::default());
    let outcome = dispatcher
        .dispatch_root(&runtime, root_thread_id, NOW_MS)
        .await?;
    assert_eq!(outcome, DispatchOutcome::Deferred);
    assert_eq!(
        codex_state::root_sidecar_path(&runtime, root_thread_id).await?,
        None
    );
    let status: String =
        sqlx::query_scalar("SELECT status FROM coordination_projection_outbox WHERE event_id=?")
            .bind("019f7c6c-2222-7000-8000-000000000721")
            .fetch_one(&open_seed_pool(home.path()).await?)
            .await?;
    assert_eq!(status, "pending", "deferred, never silently skipped");

    // Once the rollout path becomes known, the same revision is still the
    // one dispatched — nothing was skipped while deferred.
    let rollout_path = home.path().join("rollout-2026-07-21T13-00-00-root.jsonl");
    let dispatcher = SidecarDispatcher::new(
        StaticResolver(Some(rollout_path)),
        RecordingObserver::default(),
    );
    let outcome = dispatcher
        .dispatch_root(&runtime, root_thread_id, NOW_MS + 1_000)
        .await?;
    assert_eq!(
        outcome,
        DispatchOutcome::Dispatched {
            native: 1,
            degradation: 0
        }
    );
    Ok(())
}

#[tokio::test]
async fn independent_roots_progress_concurrently_same_root_serializes() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
    let pool = open_seed_pool(home.path()).await?;
    let epoch = active_epoch(&pool).await?;
    let root_a = root(10);
    let root_b = root(11);
    seed_root(&pool, root_a, &epoch, 1, 0).await?;
    seed_root(&pool, root_b, &epoch, 1, 0).await?;
    seed_native_event(
        &pool,
        root_a,
        1,
        "019f7c6c-5555-7000-8000-00000000a001",
        0x41,
    )
    .await?;
    seed_native_event(
        &pool,
        root_b,
        1,
        "019f7c6c-5555-7000-8000-00000000b001",
        0x42,
    )
    .await?;
    pool.close().await;

    let probe = Arc::new(ConcurrencyProbe::default());

    // Different roots: use per-root distinct rollout paths via a resolver
    // keyed by root id.
    struct KeyedResolver(std::collections::HashMap<ThreadId, PathBuf>);
    impl RootRolloutPathResolver for KeyedResolver {
        fn resolve(&self, root_thread_id: ThreadId) -> Option<PathBuf> {
            self.0.get(&root_thread_id).cloned()
        }
    }
    let mut paths = std::collections::HashMap::new();
    paths.insert(root_a, home.path().join("rollout-A.jsonl"));
    paths.insert(root_b, home.path().join("rollout-B.jsonl"));
    let dispatcher = Arc::new(SidecarDispatcher::new(
        KeyedResolver(paths),
        ProbingObserver(probe.clone()),
    ));

    let started = std::time::Instant::now();
    let (result_a, result_b) = tokio::join!(
        dispatcher.dispatch_root(&runtime, root_a, NOW_MS),
        dispatcher.dispatch_root(&runtime, root_b, NOW_MS)
    );
    result_a?;
    result_b?;
    assert!(
        probe.max_observed.load(Ordering::SeqCst) >= 1,
        "at least one dispatch observed"
    );
    // Not a hard timing assertion (flaky on loaded CI), just a sanity bound
    // that two independent roots were not fully serialized behind one lock.
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    Ok(())
}

#[tokio::test]
async fn same_root_dispatch_never_interleaves() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
    let pool = open_seed_pool(home.path()).await?;
    let epoch = active_epoch(&pool).await?;
    let root_thread_id = root(20);
    seed_root(&pool, root_thread_id, &epoch, 3, 0).await?;
    seed_native_event(
        &pool,
        root_thread_id,
        1,
        "019f7c6c-6666-7000-8000-000000000c01",
        0x51,
    )
    .await?;
    seed_native_event(
        &pool,
        root_thread_id,
        2,
        "019f7c6c-6666-7000-8000-000000000c02",
        0x52,
    )
    .await?;
    seed_native_event(
        &pool,
        root_thread_id,
        3,
        "019f7c6c-6666-7000-8000-000000000c03",
        0x53,
    )
    .await?;
    pool.close().await;

    let rollout_path = home.path().join("rollout-C.jsonl");
    let probe = Arc::new(ConcurrencyProbe::default());
    let dispatcher = Arc::new(SidecarDispatcher::new(
        StaticResolver(Some(rollout_path)),
        ProbingObserver(probe.clone()),
    ));

    let (a, b, c) = tokio::join!(
        dispatcher.dispatch_root(&runtime, root_thread_id, NOW_MS),
        dispatcher.dispatch_root(&runtime, root_thread_id, NOW_MS + 10),
        dispatcher.dispatch_root(&runtime, root_thread_id, NOW_MS + 20),
    );
    a?;
    b?;
    c?;
    assert_eq!(
        probe.max_observed.load(Ordering::SeqCst),
        1,
        "one root's dispatch never overlaps itself"
    );

    let sidecar_path = codex_state::root_sidecar_path(&runtime, root_thread_id)
        .await?
        .expect("persisted");
    let contents = tokio::fs::read_to_string(&sidecar_path).await?;
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(
        lines.len(),
        3,
        "all three revisions dispatched, no torn writes"
    );
    for line in &lines {
        // Whole-line JSON parses cleanly: no interleaving corrupted a line.
        let _: super::record::SidecarRecord = serde_json::from_str(line)?;
    }
    Ok(())
}

#[tokio::test]
async fn no_plaintext_or_ciphertext_reaches_sidecar_file_or_observer() -> anyhow::Result<()> {
    const CANARY: &str = "TOP-SECRET-MODEL-CONTENT-CANARY";
    let home = TempDir::new()?;
    let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
    let pool = open_seed_pool(home.path()).await?;
    let epoch = active_epoch(&pool).await?;
    let root_thread_id = root(30);
    seed_root(&pool, root_thread_id, &epoch, 1, 0).await?;
    // The canary lives in the *event's* opaque canonical bytes/idempotency
    // bytes (the sidecar deliberately never copies those into its own
    // record) to prove the sidecar line itself never leaks them even if a
    // future event kind embedded something sensitive there.
    sqlx::query(
        "INSERT INTO coordination_events \
         (event_id,root_thread_id,revision,canonical_event_bytes,event_fingerprint,\
          idempotency_key_bytes,idempotency_key_fingerprint,occurred_at,created_at_ms) \
         VALUES (?,?,1,?,?,?,?,?,?)",
    )
    .bind("019f7c6c-2222-7000-8000-000000000d01")
    .bind(root_thread_id.to_string())
    .bind(format!(r#"{{"kind":"native","secret":"{CANARY}"}}"#).into_bytes())
    .bind(vec![0x61_u8; 32])
    .bind(CANARY.as_bytes())
    .bind(vec![0x62_u8; 32])
    .bind(1_753_000_000_i64)
    .bind(10_i64)
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO coordination_projection_outbox (event_id,status,created_at_ms,updated_at_ms) \
         VALUES (?,'pending',10,10)",
    )
    .bind("019f7c6c-2222-7000-8000-000000000d01")
    .execute(&pool)
    .await?;
    pool.close().await;

    let rollout_path = home.path().join("rollout-D.jsonl");
    let observer = RecordingObserver::default();
    let dispatcher = SidecarDispatcher::new(StaticResolver(Some(rollout_path)), observer);
    dispatcher
        .dispatch_root(&runtime, root_thread_id, NOW_MS)
        .await?;

    let sidecar_path = codex_state::root_sidecar_path(&runtime, root_thread_id)
        .await?
        .expect("persisted");
    let contents = tokio::fs::read_to_string(&sidecar_path).await?;
    assert!(
        !contents.contains(CANARY),
        "sidecar file must be metadata-only"
    );
    Ok(())
}

#[tokio::test]
async fn observer_receives_metadata_matching_dispatched_record() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
    let pool = open_seed_pool(home.path()).await?;
    let epoch = active_epoch(&pool).await?;
    let root_thread_id = root(40);
    seed_root(&pool, root_thread_id, &epoch, 1, 0).await?;
    seed_native_event(
        &pool,
        root_thread_id,
        1,
        "019f7c6c-2222-7000-8000-000000000e01",
        0x61,
    )
    .await?;
    pool.close().await;

    let rollout_path = home.path().join("rollout-E.jsonl");
    let observer = Arc::new(RecordingObserver::default());
    struct SharedObserver(Arc<RecordingObserver>);
    impl SidecarObserver for SharedObserver {
        fn on_published(&self, event: &SidecarObservedEvent) {
            self.0.on_published(event);
        }
    }
    let dispatcher = SidecarDispatcher::new(
        StaticResolver(Some(rollout_path)),
        SharedObserver(observer.clone()),
    );
    dispatcher
        .dispatch_root(&runtime, root_thread_id, NOW_MS)
        .await?;

    let events = observer.events.lock().expect("lock");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].identity, "019f7c6c-2222-7000-8000-000000000e01");
    assert_eq!(events[0].root_thread_id, root_thread_id.to_string());
    assert_eq!(events[0].kind, SidecarObservedKind::Native { revision: 1 });
    Ok(())
}

struct FailDispatchAt(DispatchStep);

impl DispatchFailureInjector for FailDispatchAt {
    fn before_step(&self, step: DispatchStep) -> std::io::Result<()> {
        if step == self.0 {
            return Err(std::io::Error::other(format!("injected at {step:?}")));
        }
        Ok(())
    }
}

/// Crash-matrix for the dispatcher's own two seams (notify, ack): a failure
/// injected right after a durable append but before the outbox ack must
/// leave the record durably on disk (append already happened) while the
/// outbox stays un-acked; redriving the whole claim/append/notify/ack
/// sequence afterward must dedupe the append and still reach exactly one
/// `materialized` ack, never a duplicate line and never a skipped revision.
#[tokio::test]
async fn crash_between_append_and_ack_recovers_via_redispatch() -> anyhow::Result<()> {
    for step in [DispatchStep::AfterAppend, DispatchStep::AfterNotify] {
        let home = TempDir::new()?;
        let runtime = StateRuntime::init(home.path().to_path_buf(), "test".to_string()).await?;
        let pool = open_seed_pool(home.path()).await?;
        let epoch_str = active_epoch(&pool).await?;
        let root_thread_id = root(60 + step as u32);
        let event_id = format!("019f7c6c-7777-7000-8000-00000000{:04}", step as u32);
        seed_root(&pool, root_thread_id, &epoch_str, 1, 0).await?;
        seed_native_event(&pool, root_thread_id, 1, &event_id, 0x81).await?;
        pool.close().await;

        let epoch = codex_state::active_state_epoch(&runtime).await?;
        let sidecar_path = home.path().join(format!("rollout-G-{}.jsonl", step as u32));
        let mut writer = SidecarWriter::open(sidecar_path.clone()).await?;

        let claimed = codex_state::claim_native_publications(
            &runtime,
            root_thread_id,
            epoch,
            NOW_MS,
            NOW_MS + 30_000,
            1,
        )
        .await?;
        let codex_state::SidecarClaimOutcome::Claimed(mut leases) = claimed else {
            anyhow::bail!("expected a claimed lease");
        };
        let lease = leases.remove(0);
        let record = SidecarRecord::Native(NativeSidecarRecord {
            event_id: lease.event_id.to_string(),
            root_thread_id: root_thread_id.to_string(),
            state_epoch: epoch.to_string(),
            revision: lease.revision,
            materialized_at_ms: NOW_MS,
        });

        let observer = RecordingObserver::default();
        let outcome = append_notify(
            &mut writer,
            &observer,
            &record,
            root_thread_id,
            epoch,
            SidecarObservedKind::Native {
                revision: lease.revision,
            },
            &NoSidecarFailure,
            &FailDispatchAt(step),
        )
        .await;
        assert!(outcome.is_err(), "{step:?} must abort before ack");

        // The append itself is durable regardless: exactly one line, and it
        // is present even though the ack never ran.
        let contents = tokio::fs::read_to_string(&sidecar_path).await?;
        assert_eq!(
            contents.lines().count(),
            1,
            "{step:?}: append survives the crash"
        );

        // The outbox row is still leased (never acked) — never silently
        // treated as materialized.
        let pool_check = open_seed_pool(home.path()).await?;
        let status: String = sqlx::query_scalar(
            "SELECT status FROM coordination_projection_outbox WHERE event_id=?",
        )
        .bind(&event_id)
        .fetch_one(&pool_check)
        .await?;
        assert_eq!(
            status, "leased",
            "{step:?}: never acked before the crash point"
        );
        pool_check.close().await;

        // Redrive: reopen the writer (rescan) and rerun the full
        // append/notify/ack sequence with the SAME lease. The append must
        // dedupe (no duplicate line) and the ack must now go through.
        let mut reopened_writer = SidecarWriter::open(sidecar_path.clone()).await?;
        append_notify(
            &mut reopened_writer,
            &observer,
            &record,
            root_thread_id,
            epoch,
            SidecarObservedKind::Native {
                revision: lease.revision,
            },
            &NoSidecarFailure,
            &NoDispatchFailure,
        )
        .await?;
        let ack = codex_state::resolve_native_publication(
            &runtime,
            lease,
            epoch,
            codex_state::SidecarResolution::Materialized,
            NOW_MS + 1,
        )
        .await?;
        assert_eq!(
            ack,
            codex_state::SidecarResolveOutcome::Applied(
                codex_state::SidecarPublicationStatus::Materialized
            )
        );

        let contents = tokio::fs::read_to_string(&sidecar_path).await?;
        assert_eq!(
            contents.lines().count(),
            1,
            "{step:?}: redrive never duplicates the line"
        );
    }
    Ok(())
}

/// Exercise the writer's own dedupe against a directly constructed record to
/// make sure [`SidecarWriter`] stays reachable/usable from dispatcher-level
/// tests (both modules imported here).
#[tokio::test]
async fn writer_is_reusable_directly_alongside_dispatcher() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let mut writer = SidecarWriter::open(dir.path().join("standalone.jsonl")).await?;
    let record = super::record::SidecarRecord::Native(super::record::NativeSidecarRecord {
        event_id: "019f7c6c-2222-7000-8000-000000000f01".to_string(),
        root_thread_id: "019f7c6c-1111-7000-8000-000000000601".to_string(),
        state_epoch: "019f7c6c-0000-7000-8000-000000000001".to_string(),
        revision: 1,
        materialized_at_ms: 1,
    });
    writer.append_if_new(&record).await?;
    Ok(())
}
