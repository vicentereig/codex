use pretty_assertions::assert_eq;
use sqlx::SqlitePool;

use super::projection_outbox::claim_projection_publications;
use super::projection_outbox::resolve_projection_publication;
use super::recovery_test_support::runtime_with_root;
use super::recovery_test_support::thread_id;
use crate::model::coordination_recovery_state::ClaimProjectionPublications;
use crate::model::coordination_recovery_state::ClaimProjectionPublicationsOutcome;
use crate::model::coordination_recovery_state::ProjectionPublicationLease;
use crate::model::coordination_recovery_state::ProjectionPublicationResolution;
use crate::model::coordination_recovery_state::ProjectionPublicationStatus;
use crate::model::coordination_recovery_state::ResolveProjectionPublication;
use crate::model::coordination_recovery_state::ResolveProjectionPublicationOutcome;

const ROOT: &str = super::aggregate_test_support::ROOT;
const ROOT_B: &str = "019f7c6c-2222-7000-8000-000000000601";
const EVENT_B1: &str = "019f7c6c-2222-7000-8000-000000000701";
const EVENT_A2: &str = "019f7c6c-1111-7000-8000-000000000702";

/// Base timestamp comfortably after wall-clock, since `runtime_with_root`
/// stamps its native events with real `chrono::Utc::now` milliseconds.
const NOW: i64 = 2_000_000_000_000;

const INSERT_EVENT: &str = "INSERT INTO coordination_events (\
     event_id, root_thread_id, revision, canonical_event_bytes, event_fingerprint,\
     idempotency_key_bytes, idempotency_key_fingerprint, occurred_at, created_at_ms\
 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

/// Insert an additional native event + its pending projection outbox row for
/// `root` at `revision`, advancing `committed_revision` to at least `revision`.
async fn seed_native_revision(
    pool: &SqlitePool,
    root: &str,
    revision: i64,
    event_id: &str,
    fingerprint_seed: u8,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE coordination_roots SET committed_revision=?, updated_at_ms=updated_at_ms+1 \
         WHERE root_thread_id=?",
    )
    .bind(revision)
    .bind(root)
    .execute(pool)
    .await?;
    sqlx::query(INSERT_EVENT)
        .bind(event_id)
        .bind(root)
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
        "INSERT INTO coordination_projection_outbox (event_id, status, created_at_ms, updated_at_ms) \
         VALUES (?, 'pending', 10, 10)",
    )
    .bind(event_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Seed a fresh sibling root in the active epoch with a single pending native
/// revision 1, so cross-root independence can be exercised.
async fn seed_root_with_revision_one(
    pool: &SqlitePool,
    epoch: &str,
    root: &str,
    event_id: &str,
    fingerprint_seed: u8,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO coordination_roots (root_thread_id, state_epoch, committed_revision, \
         published_revision, created_at_ms, updated_at_ms) VALUES (?, ?, 0, 0, 10, 10)",
    )
    .bind(root)
    .bind(epoch)
    .execute(pool)
    .await?;
    seed_native_revision(pool, root, 1, event_id, fingerprint_seed).await
}

async fn published_revision(pool: &SqlitePool, root: &str) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT published_revision FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(root)
    .fetch_one(pool)
    .await?)
}

fn claim(
    root: &str,
    epoch: codex_coordination::StateEpoch,
    now_ms: i64,
) -> ClaimProjectionPublications {
    ClaimProjectionPublications {
        root_thread_id: thread_id(root),
        expected_state_epoch: epoch,
        now_ms,
        lease_expires_at_ms: now_ms + 1_000,
        limit: 10,
    }
}

async fn claim_leases(
    pool: &SqlitePool,
    params: &ClaimProjectionPublications,
) -> anyhow::Result<Vec<ProjectionPublicationLease>> {
    match claim_projection_publications(pool, params).await? {
        ClaimProjectionPublicationsOutcome::Claimed(leases) => Ok(leases),
        ClaimProjectionPublicationsOutcome::Deferred => anyhow::bail!("claim deferred"),
    }
}

#[tokio::test]
async fn materialize_claims_only_next_revision_and_advances_watermark() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    assert_eq!(published_revision(&runtime.pool, ROOT).await?, 0);

    let leases = claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW)).await?;
    assert_eq!(leases.len(), 1, "exactly the R+1 row is claimable per root");
    assert_eq!(leases[0].revision, 1);

    assert_eq!(
        resolve_projection_publication(
            &runtime.pool,
            &ResolveProjectionPublication {
                lease: leases[0].clone(),
                expected_state_epoch: epoch,
                resolution: ProjectionPublicationResolution::Materialized,
                now_ms: NOW + 500,
            },
        )
        .await?,
        ResolveProjectionPublicationOutcome::Applied(ProjectionPublicationStatus::Materialized)
    );
    assert_eq!(
        published_revision(&runtime.pool, ROOT).await?,
        1,
        "native materialization advances the published watermark exactly one revision"
    );

    // R+1 has advanced to revision 2: only a freshly seeded revision 2 is now claimable.
    assert!(
        claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW + 2_000))
            .await?
            .is_empty()
    );
    seed_native_revision(&runtime.pool, ROOT, 2, EVENT_A2, 0x40).await?;
    let next = claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW + 2_100)).await?;
    assert_eq!(next.len(), 1);
    assert_eq!(
        next[0].revision, 2,
        "the next claim targets published_revision + 1"
    );
    Ok(())
}

#[tokio::test]
async fn independent_roots_progress_without_cross_serialization() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    seed_root_with_revision_one(&runtime.pool, &epoch.to_string(), ROOT_B, EVENT_B1, 0x50).await?;

    let a = claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW)).await?;
    let b = claim_leases(&runtime.pool, &claim(ROOT_B, epoch, NOW)).await?;
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_eq!(a[0].root_thread_id, thread_id(ROOT));
    assert_eq!(b[0].root_thread_id, thread_id(ROOT_B));
    assert_ne!(a[0].event_id, b[0].event_id, "claims never cross roots");

    // Materializing one root leaves the other's watermark untouched.
    resolve_projection_publication(
        &runtime.pool,
        &ResolveProjectionPublication {
            lease: a[0].clone(),
            expected_state_epoch: epoch,
            resolution: ProjectionPublicationResolution::Materialized,
            now_ms: NOW + 500,
        },
    )
    .await?;
    assert_eq!(published_revision(&runtime.pool, ROOT).await?, 1);
    assert_eq!(published_revision(&runtime.pool, ROOT_B).await?, 0);
    Ok(())
}

#[tokio::test]
async fn no_claim_while_target_revision_is_leased_or_poisoned() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let leases = claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW)).await?;
    assert_eq!(leases.len(), 1);

    // A live (unexpired) lease blocks a second claim of the same revision.
    assert!(
        claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW + 100))
            .await?
            .is_empty(),
        "no claim while the revision is leased and unexpired"
    );

    // Poison the revision; it must never be claimable again, and the watermark
    // must not advance (poison blocks all subsequent mutation on that root).
    assert_eq!(
        resolve_projection_publication(
            &runtime.pool,
            &ResolveProjectionPublication {
                lease: leases[0].clone(),
                expected_state_epoch: epoch,
                resolution: ProjectionPublicationResolution::Poisoned,
                now_ms: NOW + 200,
            },
        )
        .await?,
        ResolveProjectionPublicationOutcome::Applied(ProjectionPublicationStatus::Poisoned)
    );
    assert_eq!(published_revision(&runtime.pool, ROOT).await?, 0);
    assert!(
        claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW + 5_000))
            .await?
            .is_empty(),
        "a poisoned revision is never re-claimable"
    );

    // Even a freshly journaled successor stays unclaimable: the root is frozen.
    seed_native_revision(&runtime.pool, ROOT, 2, EVENT_A2, 0x60).await?;
    assert!(
        claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW + 6_000))
            .await?
            .is_empty(),
        "poison at revision R blocks revision R+1"
    );
    Ok(())
}

#[tokio::test]
async fn stale_lease_tokens_are_fenced_not_applied() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let leases = claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW)).await?;
    let lease = leases[0].clone();

    // A superseded version token is fenced.
    let mut stale_version = lease.clone();
    stale_version.version = lease.version + 1;
    assert_eq!(
        resolve_projection_publication(
            &runtime.pool,
            &ResolveProjectionPublication {
                lease: stale_version,
                expected_state_epoch: epoch,
                resolution: ProjectionPublicationResolution::Materialized,
                now_ms: NOW + 100,
            },
        )
        .await?,
        ResolveProjectionPublicationOutcome::Fenced
    );

    // A stale lease epoch is fenced.
    let mut stale_epoch = lease.clone();
    stale_epoch.lease_epoch = lease.lease_epoch + 1;
    assert_eq!(
        resolve_projection_publication(
            &runtime.pool,
            &ResolveProjectionPublication {
                lease: stale_epoch,
                expected_state_epoch: epoch,
                resolution: ProjectionPublicationResolution::Materialized,
                now_ms: NOW + 100,
            },
        )
        .await?,
        ResolveProjectionPublicationOutcome::Fenced
    );

    // A resolution presented after the lease has expired is fenced.
    assert_eq!(
        resolve_projection_publication(
            &runtime.pool,
            &ResolveProjectionPublication {
                lease: lease.clone(),
                expected_state_epoch: epoch,
                resolution: ProjectionPublicationResolution::Materialized,
                now_ms: lease.lease_expires_at_ms,
            },
        )
        .await?,
        ResolveProjectionPublicationOutcome::Fenced
    );

    // A forged revision token (different revision, same event) is fenced.
    let mut forged_revision = lease.clone();
    forged_revision.revision = lease.revision + 5;
    assert_eq!(
        resolve_projection_publication(
            &runtime.pool,
            &ResolveProjectionPublication {
                lease: forged_revision,
                expected_state_epoch: epoch,
                resolution: ProjectionPublicationResolution::Materialized,
                now_ms: NOW + 100,
            },
        )
        .await?,
        ResolveProjectionPublicationOutcome::Fenced
    );

    // The genuine, unexpired lease still applies (nothing above mutated state).
    assert_eq!(published_revision(&runtime.pool, ROOT).await?, 0);
    assert_eq!(
        resolve_projection_publication(
            &runtime.pool,
            &ResolveProjectionPublication {
                lease,
                expected_state_epoch: epoch,
                resolution: ProjectionPublicationResolution::Materialized,
                now_ms: NOW + 100,
            },
        )
        .await?,
        ResolveProjectionPublicationOutcome::Applied(ProjectionPublicationStatus::Materialized)
    );
    Ok(())
}

#[tokio::test]
async fn poison_occurs_strictly_on_the_ninth_failure() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let mut lease = claim_leases(&runtime.pool, &claim(ROOT, epoch, NOW))
        .await?
        .remove(0);

    // Eight scheduled retries: retry_count climbs 0 -> 8, staying Pending each time.
    for index in 0..8 {
        let now_ms = NOW + 10 + index * 20;
        let retry_after_ms = now_ms + 10;
        assert_eq!(
            resolve_projection_publication(
                &runtime.pool,
                &ResolveProjectionPublication {
                    lease: lease.clone(),
                    expected_state_epoch: epoch,
                    resolution: ProjectionPublicationResolution::Retry { retry_after_ms },
                    now_ms,
                },
            )
            .await?,
            ResolveProjectionPublicationOutcome::Applied(ProjectionPublicationStatus::Pending),
            "retry #{index} must remain pending, never poison early"
        );
        let retry_count: i64 = sqlx::query_scalar(
            "SELECT retry_count FROM coordination_projection_outbox WHERE event_id=?",
        )
        .bind(lease.event_id.to_string())
        .fetch_one(&*runtime.pool)
        .await?;
        assert_eq!(retry_count, index + 1);
        lease = claim_leases(&runtime.pool, &claim(ROOT, epoch, retry_after_ms))
            .await?
            .remove(0);
    }

    // retry_count is now 8; the ninth failure poisons.
    assert_eq!(
        resolve_projection_publication(
            &runtime.pool,
            &ResolveProjectionPublication {
                lease,
                expected_state_epoch: epoch,
                resolution: ProjectionPublicationResolution::Retry {
                    retry_after_ms: NOW + 1_000,
                },
                now_ms: NOW + 300,
            },
        )
        .await?,
        ResolveProjectionPublicationOutcome::Applied(ProjectionPublicationStatus::Poisoned),
        "the ninth failure (retry_count already 8) poisons"
    );
    assert_eq!(published_revision(&runtime.pool, ROOT).await?, 0);
    Ok(())
}

#[tokio::test]
async fn concurrent_claims_of_next_revision_have_one_winner() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let pool_a = runtime.pool.clone();
    let pool_b = runtime.pool.clone();
    let claim_a = claim(ROOT, epoch, NOW);
    let claim_b = claim(ROOT, epoch, NOW);

    let (first, second) = tokio::join!(
        async move { claim_projection_publications(&pool_a, &claim_a).await },
        async move { claim_projection_publications(&pool_b, &claim_b).await },
    );

    let leased = [first, second]
        .into_iter()
        .map(|outcome| match outcome {
            Ok(ClaimProjectionPublicationsOutcome::Claimed(leases)) => leases.len(),
            // A loser may observe the row already leased (empty) or briefly defer.
            Ok(ClaimProjectionPublicationsOutcome::Deferred) | Err(_) => 0,
        })
        .sum::<usize>();
    assert_eq!(
        leased, 1,
        "exactly one concurrent claim wins the R+1 revision"
    );

    let leased_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM coordination_projection_outbox WHERE status='leased'",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(leased_rows, 1);
    Ok(())
}
