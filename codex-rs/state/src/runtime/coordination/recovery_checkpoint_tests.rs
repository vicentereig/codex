use codex_coordination::CoordinationSemanticSlot;
use pretty_assertions::assert_eq;

use super::legacy_checkpoints::advance_legacy_scan_checkpoint;
use super::recovery_test_support::*;
use crate::model::coordination_legacy_degradation::CheckedLegacyReductionDegradation;
use crate::model::coordination_recovery::CheckedLegacyLink;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery::LegacySourceIdentity;
use crate::model::coordination_recovery_state::AdvanceLegacyScanOutcome;
use crate::model::coordination_recovery_state::LegacyScanPage;

#[tokio::test]
async fn page_commit_and_replay_are_atomic_and_prefix_changes_fail_closed() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let root = thread_id(super::aggregate_test_support::ROOT);
    let source = root;
    let event = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 11);
    let link = CheckedLegacyLink::new(root, epoch, &event)?;
    let degradation = CheckedLegacyReductionDegradation::new(
        root,
        epoch,
        LegacySourceIdentity::from_event(&event)?,
        DegradationReason::CorruptSource,
        99,
        1,
    )?;
    let page = LegacyScanPage {
        root_thread_id: root,
        expected_state_epoch: epoch,
        source_thread_id: source,
        expected_version: 0,
        expected_prefix_fingerprint: None,
        next_physical_ordinal: 12,
        scanned_prefix_fingerprint: [1; 32],
        last_order: Some((11, event.envelope().event_id)),
        complete: false,
        links: vec![link],
        degradations: vec![degradation],
        now_ms: 100,
    };
    let AdvanceLegacyScanOutcome::Advanced(checkpoint) =
        advance_legacy_scan_checkpoint(&runtime.pool, &page).await?
    else {
        anyhow::bail!("page was not advanced");
    };
    assert_eq!(checkpoint.version, 0);
    assert!(matches!(
        advance_legacy_scan_checkpoint(&runtime.pool, &page).await?,
        AdvanceLegacyScanOutcome::Duplicate(_)
    ));
    let conflict_event =
        compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 12);
    let conflict = CheckedLegacyReductionDegradation::new(
        root,
        epoch,
        LegacySourceIdentity::from_event(&conflict_event)?,
        DegradationReason::StateLossDegraded,
        101,
        1,
    )?;
    let stale = LegacyScanPage {
        expected_version: 1,
        expected_prefix_fingerprint: Some([9; 32]),
        next_physical_ordinal: 13,
        scanned_prefix_fingerprint: [2; 32],
        last_order: Some((12, conflict_event.envelope().event_id)),
        links: Vec::new(),
        degradations: vec![conflict.clone()],
        now_ms: 101,
        ..page.clone()
    };
    assert!(matches!(
        advance_legacy_scan_checkpoint(&runtime.pool, &stale).await?,
        AdvanceLegacyScanOutcome::Fenced(_)
    ));
    assert_eq!(
        (
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
                .fetch_one(&*runtime.pool)
                .await?,
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM coordination_degradation_publication_outbox",
            )
            .fetch_one(&*runtime.pool)
            .await?,
        ),
        (1, 1)
    );
    let changed = LegacyScanPage {
        expected_version: 0,
        expected_prefix_fingerprint: Some([9; 32]),
        next_physical_ordinal: 13,
        scanned_prefix_fingerprint: [2; 32],
        last_order: page.last_order,
        links: Vec::new(),
        degradations: vec![conflict],
        now_ms: 101,
        ..page
    };
    assert!(matches!(
        advance_legacy_scan_checkpoint(&runtime.pool, &changed).await?,
        AdvanceLegacyScanOutcome::SourceChanged(_)
    ));
    assert!(matches!(
        advance_legacy_scan_checkpoint(&runtime.pool, &changed).await?,
        AdvanceLegacyScanOutcome::SourceChanged(_)
    ));
    let counts = (
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_legacy_links")
            .fetch_one(&*runtime.pool)
            .await?,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
            .fetch_one(&*runtime.pool)
            .await?,
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM coordination_degradation_publication_outbox",
        )
        .fetch_one(&*runtime.pool)
        .await?,
    );
    assert_eq!(counts, (1, 2, 2));
    Ok(())
}
