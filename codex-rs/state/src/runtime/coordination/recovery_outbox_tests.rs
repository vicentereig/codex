use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use pretty_assertions::assert_eq;

use super::degradation::record_exogenous_terminal_degradation;
use super::degradation_outbox::claim_degradation_publications;
use super::degradation_outbox::resolve_degradation_publication;
use super::recovery_test_support::*;
use crate::model::coordination_recovery::ExogenousTerminalObservation;
use crate::model::coordination_recovery::LegacySourceIdentity;
use crate::model::coordination_recovery::TerminalEvidenceKind;
use crate::model::coordination_recovery::TerminalEvidenceOutcome;
use crate::model::coordination_recovery::TerminalProvenance;
use crate::model::coordination_recovery_state::ClaimDegradationPublications;
use crate::model::coordination_recovery_state::ClaimDegradationPublicationsOutcome;
use crate::model::coordination_recovery_state::DegradationPublicationResolution;
use crate::model::coordination_recovery_state::DegradationPublicationStatus;
use crate::model::coordination_recovery_state::ResolveDegradationPublication;
use crate::model::coordination_recovery_state::ResolveDegradationPublicationOutcome;

#[tokio::test]
async fn outbox_is_ordered_by_full_tuple_and_lease_fenced() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    for ordinal in [17, 17] {
        let event = compatibility_event(CoordinationSemanticSlot::TurnCompleted, ordinal);
        let mut source = LegacySourceIdentity::from_event(&event)?;
        if sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
            .fetch_one(&*runtime.pool)
            .await?
            == 1
        {
            source.source_item_id = Some(BoundedId::new("item-recovery-2")?);
        }
        record_exogenous_terminal_degradation(
            &runtime.pool,
            ExogenousTerminalObservation {
                root_thread_id: thread_id(super::aggregate_test_support::ROOT),
                captured_state_epoch: Some(epoch),
                provenance: TerminalProvenance::Known(source),
                target_thread_id: thread_id(CHILD),
                target_turn_id: BoundedId::<MAX_ID_BYTES>::new("turn-b")?,
                terminal_kind: TerminalEvidenceKind::Completed,
                terminal_outcome: TerminalEvidenceOutcome::Succeeded,
                included_generations: codex_coordination::Evidence::Known {
                    value: vec![codex_coordination::AssignmentGeneration::new(1)?],
                },
                observed_at: 20,
                after_revision: 1,
            },
        )
        .await?;
    }
    sqlx::query(
        "UPDATE coordination_roots SET published_revision=committed_revision,\
         updated_at_ms=updated_at_ms+1 WHERE root_thread_id=?",
    )
    .bind(super::aggregate_test_support::ROOT)
    .execute(&*runtime.pool)
    .await?;
    let now_ms: i64 = sqlx::query_scalar(
        "SELECT MAX(created_at_ms)+1 FROM coordination_degradation_publication_outbox",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    sqlx::query(
        "UPDATE coordination_degradation_publication_outbox \
         SET status='materialized',version=version+1,updated_at_ms=? WHERE status='pending'",
    )
    .bind(now_ms)
    .execute(&*runtime.pool)
    .await
    .expect_err("raw SQL cannot materialize an unleased degradation publication");
    let ClaimDegradationPublicationsOutcome::Claimed(leases) = claim_degradation_publications(
        &runtime.pool,
        &ClaimDegradationPublications {
            root_thread_id: thread_id(super::aggregate_test_support::ROOT),
            expected_state_epoch: epoch,
            now_ms,
            lease_expires_at_ms: now_ms + 100,
            limit: 10,
        },
    )
    .await?
    else {
        anyhow::bail!("claim deferred");
    };
    assert_eq!(leases.len(), 2);
    assert_eq!(leases[0].source_ordinal, leases[1].source_ordinal);
    assert!(leases[0].stable_record_id < leases[1].stable_record_id);
    assert_eq!(
        resolve_degradation_publication(
            &runtime.pool,
            &ResolveDegradationPublication {
                lease: leases[1].clone(),
                expected_state_epoch: epoch,
                resolution: DegradationPublicationResolution::Materialized,
                now_ms: now_ms + 100,
            },
        )
        .await?,
        ResolveDegradationPublicationOutcome::Fenced
    );
    let lease = leases[0].clone();
    let mut forged = lease.clone();
    forged.stable_record_id = leases[1].stable_record_id;
    assert_eq!(
        resolve_degradation_publication(
            &runtime.pool,
            &ResolveDegradationPublication {
                lease: forged,
                expected_state_epoch: epoch,
                resolution: DegradationPublicationResolution::Materialized,
                now_ms: now_ms + 9,
            },
        )
        .await?,
        ResolveDegradationPublicationOutcome::Fenced
    );
    assert_eq!(
        resolve_degradation_publication(
            &runtime.pool,
            &ResolveDegradationPublication {
                lease: lease.clone(),
                expected_state_epoch: epoch,
                resolution: DegradationPublicationResolution::Materialized,
                now_ms: now_ms + 10,
            },
        )
        .await?,
        ResolveDegradationPublicationOutcome::Applied(DegradationPublicationStatus::Materialized)
    );
    assert_eq!(
        resolve_degradation_publication(
            &runtime.pool,
            &ResolveDegradationPublication {
                lease,
                expected_state_epoch: epoch,
                resolution: DegradationPublicationResolution::Poisoned,
                now_ms: now_ms + 11,
            },
        )
        .await?,
        ResolveDegradationPublicationOutcome::Terminal(DegradationPublicationStatus::Materialized)
    );
    Ok(())
}
