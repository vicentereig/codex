use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use pretty_assertions::assert_eq;

use super::degradation::record_exogenous_terminal_degradation;
use super::legacy_links::correlate_legacy_link_with_native;
use super::legacy_links::record_legacy_link;
use super::recovery::RecoveryWriteError;
use super::recovery_test_support::*;
use crate::model::coordination_recovery::CheckedLegacyLink;
use crate::model::coordination_recovery::ExogenousTerminalObservation;
use crate::model::coordination_recovery::RecordExogenousTerminalOutcome;
use crate::model::coordination_recovery::RecordLegacyLinkOutcome;
use crate::model::coordination_recovery::TerminalEvidenceKind;
use crate::model::coordination_recovery::TerminalEvidenceOutcome;
use crate::model::coordination_recovery::TerminalProvenance;

#[tokio::test]
async fn exact_link_replay_and_native_suppression_converge() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let event = compatibility_event(CoordinationSemanticSlot::AssignmentRequested, 7);
    let link = CheckedLegacyLink::new(
        thread_id(super::aggregate_test_support::ROOT),
        epoch,
        &event,
    )?;
    let native_event =
        codex_coordination::CoordinationEventId::parse("019f7c6c-1111-7000-8000-000000000701")?;
    let suppressed = link.clone().with_native_suppression(native_event, 20)?;
    assert!(matches!(
        record_legacy_link(&runtime.pool, &suppressed).await?,
        RecordLegacyLinkOutcome::Suppressed(_, id) if id == native_event
    ));
    assert!(matches!(
        record_legacy_link(&runtime.pool, &link).await?,
        RecordLegacyLinkOutcome::Suppressed(_, id) if id == native_event
    ));
    assert!(matches!(
        correlate_legacy_link_with_native(&runtime.pool, &link, native_event, 20).await?,
        RecordLegacyLinkOutcome::Suppressed(_, id) if id == native_event
    ));
    assert!(matches!(
        correlate_legacy_link_with_native(&runtime.pool, &link, native_event, 21).await?,
        RecordLegacyLinkOutcome::Suppressed(_, id) if id == native_event
    ));

    let unrelated = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 8);
    let unrelated = CheckedLegacyLink::new(
        thread_id(super::aggregate_test_support::ROOT),
        epoch,
        &unrelated,
    )?;
    assert!(matches!(
        correlate_legacy_link_with_native(&runtime.pool, &unrelated, native_event, 20).await,
        Err(RecoveryWriteError::NativeCorrelationConflict)
    ));

    let (after_runtime, after_epoch) = runtime_with_root().await?;
    let after_link = CheckedLegacyLink::new(
        thread_id(super::aggregate_test_support::ROOT),
        after_epoch,
        &event,
    )?;
    assert!(matches!(
        record_legacy_link(&after_runtime.pool, &after_link).await?,
        RecordLegacyLinkOutcome::Linked(_)
    ));
    assert!(matches!(
        correlate_legacy_link_with_native(
            &after_runtime.pool,
            &after_link,
            native_event,
            20,
        )
        .await?,
        RecordLegacyLinkOutcome::Suppressed(_, id) if id == native_event
    ));
    let event_id = event.envelope().event_id.to_string();
    let before = sqlx::query_as::<_, (Vec<u8>, Vec<u8>, Option<String>, Option<i64>)>(
        "SELECT source_identity_bytes,canonical_event_bytes,suppressed_by_native_event_id,\
         suppressed_at_ms FROM coordination_legacy_links WHERE compatibility_event_id=?",
    )
    .bind(&event_id)
    .fetch_one(&*runtime.pool)
    .await?;
    let after = sqlx::query_as::<_, (Vec<u8>, Vec<u8>, Option<String>, Option<i64>)>(
        "SELECT source_identity_bytes,canonical_event_bytes,suppressed_by_native_event_id,\
         suppressed_at_ms FROM coordination_legacy_links WHERE compatibility_event_id=?",
    )
    .bind(event_id)
    .fetch_one(&*after_runtime.pool)
    .await?;
    assert_eq!(before, after);
    Ok(())
}

#[tokio::test]
async fn terminal_observation_is_atomic_duplicate_or_explicit_unknown() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let event = compatibility_event(CoordinationSemanticSlot::TurnCompleted, 8);
    let source = crate::model::coordination_recovery::LegacySourceIdentity::from_event(&event)?;
    let observation = ExogenousTerminalObservation {
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
        observed_at: 1_753_000_100,
        after_revision: 1,
    };
    assert!(matches!(
        record_exogenous_terminal_degradation(&runtime.pool, observation.clone()).await?,
        RecordExogenousTerminalOutcome::Applied(_)
    ));
    assert!(matches!(
        record_exogenous_terminal_degradation(&runtime.pool, observation.clone()).await?,
        RecordExogenousTerminalOutcome::Duplicate(_)
    ));
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM coordination_degradation_records")
        .fetch_one(&*runtime.pool)
        .await?;
    let outbox: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM coordination_degradation_publication_outbox")
            .fetch_one(&*runtime.pool)
            .await?;
    assert_eq!((rows, outbox), (1, 1));

    let unknown = ExogenousTerminalObservation {
        provenance: TerminalProvenance::Unknown,
        ..observation
    };
    assert_eq!(
        record_exogenous_terminal_degradation(&runtime.pool, unknown).await?,
        RecordExogenousTerminalOutcome::UnknownProvenance
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_degradation_records")
            .fetch_one(&*runtime.pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn quarantine_is_read_only_even_for_an_exact_duplicate() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let event = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 9);
    let link = CheckedLegacyLink::new(
        thread_id(super::aggregate_test_support::ROOT),
        epoch,
        &event,
    )?;
    record_legacy_link(&runtime.pool, &link).await?;
    sqlx::query(
        "UPDATE coordination_authority SET status='quarantined',quarantine_reason='test',\
         updated_at_ms=updated_at_ms+1 WHERE singleton_id=1",
    )
    .execute(&*runtime.pool)
    .await?;
    assert!(matches!(
        record_legacy_link(&runtime.pool, &link).await,
        Err(RecoveryWriteError::Quarantined)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM coordination_legacy_links")
            .fetch_one(&*runtime.pool)
            .await?,
        1
    );
    Ok(())
}

#[test]
fn checked_bytes_debug_never_exposes_payload() {
    let sentinel = "PRIVATE_COMMAND_/tmp/secret_tool_output";
    let checked = crate::model::coordination_recovery::CheckedBytes::<1024>::new(
        sentinel.as_bytes().to_vec(),
    )
    .expect("bounded bytes");
    let debug = format!("{checked:?}");
    assert!(!debug.contains(sentinel));
    assert!(debug.contains("[REDACTED]"));
}
