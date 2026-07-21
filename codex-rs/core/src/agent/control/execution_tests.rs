use super::ExecutionAdmission;
use crate::agent::AgentControl;
use codex_protocol::error::CodexErr;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

async fn recv_timeout<T>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<T>,
    dur: Duration,
) -> Option<T> {
    tokio::time::timeout(dur, rx.recv()).await.ok().flatten()
}

fn control_with_limit(max_threads: usize) -> AgentControl {
    let control = AgentControl::default();
    control.agent_execution_limiter.initialize(max_threads);
    control
}

#[test]
fn execution_guards_count_active_v2_subagent_turns() {
    let control = control_with_limit(/*max_threads*/ 1);
    // Child role configs cannot replace the root-derived session limit.
    control
        .agent_execution_limiter
        .initialize(/*max_threads*/ 2);
    let source = SessionSource::SubAgent(SubAgentSource::Other("worker".to_string()));

    control
        .ensure_execution_capacity(MultiAgentVersion::V2, &source)
        .expect("first active turn should fit");
    let first = control
        .execution_guard(MultiAgentVersion::V2, &source)
        .expect("v2 subagent execution should be counted");
    let Err(err) = control.ensure_execution_capacity(MultiAgentVersion::V2, &source) else {
        panic!("second active turn should exceed the derived non-root cap");
    };
    let CodexErr::AgentLimitReached { max_threads } = err else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(max_threads, 1);

    drop(first);
    control
        .ensure_execution_capacity(MultiAgentVersion::V2, &source)
        .expect("capacity should be released when the running task drops");
}

#[test]
fn execution_guards_ignore_root_and_v1_turns() {
    let control = control_with_limit(/*max_threads*/ 0);

    assert!(
        control
            .execution_guard(MultiAgentVersion::V2, &SessionSource::Cli)
            .is_none()
    );
    assert!(
        control
            .execution_guard(
                MultiAgentVersion::V1,
                &SessionSource::SubAgent(SubAgentSource::Other("worker".to_string())),
            )
            .is_none()
    );
}

#[tokio::test]
async fn waiting_for_execution_capacity_wakes_when_a_turn_finishes() {
    let control = control_with_limit(/*max_threads*/ 1);
    let source = SessionSource::SubAgent(SubAgentSource::Other("worker".to_string()));
    let guard = control
        .execution_guard(MultiAgentVersion::V2, &source)
        .expect("v2 subagent execution should be counted");
    let observed_epoch = control.execution_capacity_epoch();
    let waiting_control = control.clone();
    let (ready_tx, mut ready_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        waiting_control
            .wait_for_execution_capacity_change(observed_epoch)
            .await;
        let _ = ready_tx.send(());
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut ready_rx)
            .await
            .is_err(),
        "waiter should remain blocked while the only slot is active"
    );

    drop(guard);
    tokio::time::timeout(Duration::from_secs(1), ready_rx)
        .await
        .expect("waiter should wake after the turn finishes")
        .expect("waiter task should remain alive");
}

fn subagent_source() -> SessionSource {
    SessionSource::SubAgent(SubAgentSource::Other("worker".to_string()))
}

/// Fairness contract (epic item: "enforce FIFO or a documented fairness rule to prevent
/// starvation"). The handler wraps `wait_for_execution_capacity_change` in `timeout_at`, so a
/// queued required spawn can never starve forever: when no slot ever frees the wait elapses and the
/// caller falls through to an explicit `AgentLimitReached` outcome instead of hanging.
#[tokio::test]
async fn waiting_for_execution_capacity_is_bounded_when_no_slot_frees() {
    let control = control_with_limit(/*max_threads*/ 1);
    let source = subagent_source();
    let _guard = control
        .execution_guard(MultiAgentVersion::V2, &source)
        .expect("v2 subagent execution should be counted");
    let observed_epoch = control.execution_capacity_epoch();

    let outcome = tokio::time::timeout(
        Duration::from_millis(50),
        control.wait_for_execution_capacity_change(observed_epoch),
    )
    .await;

    assert!(
        outcome.is_err(),
        "the wait must elapse when no capacity is ever released"
    );
    // Capacity is still exhausted, so the caller surfaces an explicit failure.
    assert!(
        control
            .ensure_execution_capacity(MultiAgentVersion::V2, &source)
            .is_err(),
        "the single slot is still occupied after the bounded wait"
    );
}

/// A queued required spawn can be cancelled while waiting (its handler future is dropped by turn
/// abort). Dropping the in-flight wait must not consume the pending wake-up nor corrupt the epoch:
/// a fresh waiter observing the same epoch still wakes on the next release.
#[tokio::test]
async fn dropping_a_queued_capacity_waiter_leaves_the_limiter_consistent() {
    let control = control_with_limit(/*max_threads*/ 1);
    let source = subagent_source();
    let guard = control
        .execution_guard(MultiAgentVersion::V2, &source)
        .expect("v2 subagent execution should be counted");
    let observed_epoch = control.execution_capacity_epoch();

    // Simulate a cancelled queued waiter: it observes no change and its future is dropped on the
    // timeout before any capacity is released.
    let cancelled = tokio::time::timeout(
        Duration::from_millis(20),
        control.wait_for_execution_capacity_change(observed_epoch),
    )
    .await;
    assert!(cancelled.is_err(), "the queued waiter was still pending");

    // Releasing capacity must still wake a fresh waiter registered against the same epoch.
    drop(guard);
    tokio::time::timeout(
        Duration::from_secs(1),
        control.wait_for_execution_capacity_change(observed_epoch),
    )
    .await
    .expect("a fresh waiter wakes after the release the cancelled waiter never consumed");
    assert!(
        control
            .ensure_execution_capacity(MultiAgentVersion::V2, &source)
            .is_ok(),
        "the released slot is available exactly once"
    );
}

/// Concurrent terminal children must release their permits exactly once: N guards dropped in
/// parallel free exactly N slots, never more (a double-release would let more than `max_threads`
/// re-acquire and would underflow the active counter).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_guard_release_frees_capacity_exactly_once() {
    let control = control_with_limit(/*max_threads*/ 3);
    let source = subagent_source();
    let guards: Vec<_> = (0..3)
        .map(|_| {
            control
                .execution_guard(MultiAgentVersion::V2, &source)
                .expect("v2 subagent execution should be counted")
        })
        .collect();
    assert!(
        control
            .ensure_execution_capacity(MultiAgentVersion::V2, &source)
            .is_err(),
        "all three slots are occupied"
    );

    let mut releases = Vec::new();
    for guard in guards {
        let mut guard = Some(guard);
        releases.push(tokio::spawn(async move {
            drop(guard.take());
        }));
    }
    for release in releases {
        release.await.expect("release task should not panic");
    }

    // Exactly three slots freed: re-acquiring three fills the cap and a fourth check fails. A
    // double-release would have let a fourth guard through (or underflowed the counter).
    let _reacquired: Vec<_> = (0..3)
        .map(|_| {
            control
                .execution_guard(MultiAgentVersion::V2, &source)
                .expect("capacity should be restored for exactly three re-acquisitions")
        })
        .collect();
    assert!(
        control
            .ensure_execution_capacity(MultiAgentVersion::V2, &source)
            .is_err(),
        "capacity must not exceed max_threads after concurrent releases"
    );
}

/// The wait path is bounded: when no slot ever frees, `acquire_execution_slot` returns a timeout
/// (the honest "turn did not start" signal the caller uses to defer instead of running over-cap)
/// rather than hanging forever.
#[tokio::test]
async fn acquire_execution_slot_times_out_at_capacity() {
    let control = control_with_limit(/*max_threads*/ 1);
    let source = subagent_source();
    let _held = match control.try_execution_guard(MultiAgentVersion::V2, &source) {
        ExecutionAdmission::Admitted(guard) => guard,
        _ => panic!("the first slot admits"),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_millis(30);
    let outcome = control
        .acquire_execution_slot(MultiAgentVersion::V2, &source, deadline)
        .await;
    assert!(
        outcome.is_err(),
        "the bounded wait must elapse into a timeout when no slot frees"
    );
}

/// Root and V1 turns are not execution-limited: `acquire_execution_slot` returns immediately with
/// no permit and never waits, even against an exhausted cap.
#[tokio::test]
async fn acquire_execution_slot_is_immediate_for_unlimited_turns() {
    let control = control_with_limit(/*max_threads*/ 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let outcome = control
        .acquire_execution_slot(MultiAgentVersion::V2, &SessionSource::Cli, deadline)
        .await;
    assert!(
        matches!(outcome, Ok(None)),
        "unlimited turns bypass the limiter without a permit or a wait"
    );
}

/// The atomic acquire is the epic's core correctness guarantee: under a storm of concurrent
/// acquire/release racers against a fixed cap, the number of simultaneously-held permits must never
/// exceed the cap. The prior unconditional `guard()` (a read-only `has_capacity` check followed by
/// a separate unconditional `fetch_add`) could overshoot here; the compare-exchange loop cannot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_execution_guard_cannot_overshoot_under_concurrency() {
    const CAP: usize = 3;
    const RACERS: usize = 32;
    let control = control_with_limit(CAP);
    let source = subagent_source();

    // A shared live-counter with a high-water mark: every racer bumps it while holding a permit, so
    // the observed maximum is the true peak concurrency regardless of interleaving.
    let live = Arc::new(AtomicUsize::new(0));
    let max_live = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(RACERS));

    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let control = control.clone();
        let source = source.clone();
        let live = Arc::clone(&live);
        let max_live = Arc::clone(&max_live);
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..200 {
                loop {
                    match control.try_execution_guard(MultiAgentVersion::V2, &source) {
                        ExecutionAdmission::Admitted(guard) => {
                            let now = live.fetch_add(1, Ordering::AcqRel) + 1;
                            max_live.fetch_max(now, Ordering::AcqRel);
                            // Yield while holding the permit to force interleaving with racers.
                            tokio::task::yield_now().await;
                            live.fetch_sub(1, Ordering::AcqRel);
                            drop(guard);
                            break;
                        }
                        ExecutionAdmission::AtCapacity => tokio::task::yield_now().await,
                        ExecutionAdmission::Unlimited => {
                            unreachable!("a v2 subagent turn is execution-limited")
                        }
                    }
                }
            }
        }));
    }
    for handle in handles {
        handle.await.expect("racer task should not panic");
    }

    assert!(
        max_live.load(Ordering::Acquire) <= CAP,
        "atomic acquire overshot the cap: peak {} simultaneous permits (cap {CAP})",
        max_live.load(Ordering::Acquire),
    );
    assert_eq!(
        control.active_execution_count(),
        0,
        "every racer released its permit"
    );
}

/// The wait path (`acquire_execution_slot`) defers a turn start instead of overshooting: queued
/// waiters block while the cap is full, then are admitted exactly one-per-freed-slot, never more.
/// This is the deterministic proof (explicit channel synchronization, no timing assertions on the
/// positive path) that the enforcement point now both blocks and never exceeds the cap. Because
/// every waiter runs its own `acquire_execution_slot` future concurrently and none blocks the
/// others (there is no shared lock held across the await), it also demonstrates the wait holds no
/// lock that would serialize other turn starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_turn_starts_admit_exactly_as_permits_free() {
    const CAP: usize = 3;
    const WAITERS: usize = 5;
    let control = control_with_limit(CAP);
    let source = subagent_source();

    // Fill the cap; exactly CAP admissions succeed immediately.
    let held: Vec<_> = (0..CAP)
        .map(
            |_| match control.try_execution_guard(MultiAgentVersion::V2, &source) {
                ExecutionAdmission::Admitted(guard) => guard,
                _ => panic!("initial fill should admit exactly the cap"),
            },
        )
        .collect();
    assert!(
        matches!(
            control.try_execution_guard(MultiAgentVersion::V2, &source),
            ExecutionAdmission::AtCapacity
        ),
        "a (CAP+1)th immediate acquire must be refused"
    );

    let (admit_tx, mut admit_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut release_tx = Vec::with_capacity(WAITERS);
    let far_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    for id in 0..WAITERS {
        let (rel_tx, rel_rx) = tokio::sync::oneshot::channel::<()>();
        release_tx.push(Some(rel_tx));
        let control = control.clone();
        let source = source.clone();
        let admit_tx = admit_tx.clone();
        tokio::spawn(async move {
            let guard = control
                .acquire_execution_slot(MultiAgentVersion::V2, &source, far_deadline)
                .await
                .expect("far deadline never elapses")
                .expect("a v2 subagent turn requires a permit");
            admit_tx.send(id).expect("admission receiver alive");
            // Hold the permit until told to release so admissions stay pegged at the cap.
            let _ = rel_rx.await;
            drop(guard);
        });
    }
    drop(admit_tx);

    // While full, no queued waiter is admitted and there is no busy-poll overshoot.
    assert!(
        recv_timeout(&mut admit_rx, Duration::from_millis(50))
            .await
            .is_none(),
        "a waiter was admitted while the cap was full"
    );
    assert_eq!(control.active_execution_count(), CAP);

    // Free all CAP slots at once: exactly CAP waiters are admitted, never more.
    drop(held);
    let mut admitted = Vec::new();
    for _ in 0..CAP {
        admitted.push(
            recv_timeout(&mut admit_rx, Duration::from_secs(1))
                .await
                .expect("a freed slot must admit a queued waiter"),
        );
    }
    assert!(
        recv_timeout(&mut admit_rx, Duration::from_millis(50))
            .await
            .is_none(),
        "more than CAP waiters were admitted for CAP freed slots"
    );
    assert_eq!(
        control.active_execution_count(),
        CAP,
        "admitted waiters hold exactly the cap in permits"
    );

    // Each release frees exactly one slot for a still-parked waiter; the cap is never exceeded
    // during the hand-off.
    let remaining = WAITERS - CAP;
    for &id in admitted.iter().take(remaining) {
        release_tx[id]
            .take()
            .expect("release channel present")
            .send(())
            .expect("admitted waiter is alive");
        assert!(
            recv_timeout(&mut admit_rx, Duration::from_secs(1))
                .await
                .is_some(),
            "releasing a permit must admit a parked waiter"
        );
        assert!(
            control.active_execution_count() <= CAP,
            "permit count exceeded the cap during hand-off"
        );
    }
    assert!(
        recv_timeout(&mut admit_rx, Duration::from_millis(50))
            .await
            .is_none(),
        "no queued waiters remain to admit"
    );
}

/// A single capacity release wakes every queued waiter (`Notify::notify_waiters` broadcast) so no
/// queued required spawn is silently denied the wake-up. Each waiter then re-checks its own
/// admission predicate; the deadline (exercised above) bounds any that lose the re-attempt race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_queued_capacity_waiters_wake_on_a_single_release() {
    let control = control_with_limit(/*max_threads*/ 1);
    let source = subagent_source();
    let guard = control
        .execution_guard(MultiAgentVersion::V2, &source)
        .expect("v2 subagent execution should be counted");
    let observed_epoch = control.execution_capacity_epoch();

    let mut waiters = Vec::new();
    for _ in 0..5 {
        let control = control.clone();
        waiters.push(tokio::spawn(async move {
            control
                .wait_for_execution_capacity_change(observed_epoch)
                .await;
        }));
    }
    // Give the waiters a chance to register their `notified()` future before the release, so the
    // broadcast wake-up path (not just the epoch fast-path) is exercised.
    tokio::time::sleep(Duration::from_millis(20)).await;

    drop(guard);

    for waiter in waiters {
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("every queued waiter wakes on the single release")
            .expect("waiter task should remain alive");
    }
}
