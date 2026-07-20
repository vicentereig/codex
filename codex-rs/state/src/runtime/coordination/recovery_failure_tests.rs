use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use pretty_assertions::assert_eq;

use super::degradation::record_exogenous_terminal_degradation_with;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery_test_support::*;
use crate::model::coordination_recovery::ExogenousTerminalObservation;
use crate::model::coordination_recovery::LegacySourceIdentity;
use crate::model::coordination_recovery::RecordExogenousTerminalOutcome;
use crate::model::coordination_recovery::TerminalEvidenceKind;
use crate::model::coordination_recovery::TerminalEvidenceOutcome;
use crate::model::coordination_recovery::TerminalProvenance;

struct FailAt(RecoveryStep);

impl RecoveryFailureInjector for FailAt {
    fn after_recovery_step(&self, step: RecoveryStep) -> anyhow::Result<()> {
        if step == self.0 {
            anyhow::bail!("injected recovery boundary failure");
        }
        Ok(())
    }
}

#[tokio::test]
async fn precommit_failures_roll_back_record_and_outbox_together() -> anyhow::Result<()> {
    for step in [
        RecoveryStep::DegradationInsert,
        RecoveryStep::DegradationOutboxInsert,
        RecoveryStep::BeforeCommit,
    ] {
        let (runtime, epoch) = runtime_with_root().await?;
        assert!(
            record_exogenous_terminal_degradation_with(
                &runtime.pool,
                observation(epoch)?,
                &FailAt(step),
            )
            .await
            .is_err()
        );
        assert_eq!(counts(&runtime.pool).await?, (0, 0));
    }
    Ok(())
}

#[tokio::test]
async fn commit_response_loss_replays_as_one_exact_duplicate() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root().await?;
    let observation = observation(epoch)?;
    assert!(
        record_exogenous_terminal_degradation_with(
            &runtime.pool,
            observation.clone(),
            &FailAt(RecoveryStep::AfterCommit),
        )
        .await
        .is_err()
    );
    assert_eq!(counts(&runtime.pool).await?, (1, 1));
    assert!(matches!(
        super::degradation::record_exogenous_terminal_degradation(&runtime.pool, observation,)
            .await?,
        RecordExogenousTerminalOutcome::Duplicate(_)
    ));
    assert_eq!(counts(&runtime.pool).await?, (1, 1));
    Ok(())
}

fn observation(
    epoch: codex_coordination::StateEpoch,
) -> anyhow::Result<ExogenousTerminalObservation> {
    let event = compatibility_event(CoordinationSemanticSlot::TurnCompleted, 23);
    Ok(ExogenousTerminalObservation {
        root_thread_id: thread_id(super::aggregate_test_support::ROOT),
        captured_state_epoch: Some(epoch),
        provenance: TerminalProvenance::Known(LegacySourceIdentity::from_event(&event)?),
        target_thread_id: thread_id(CHILD),
        target_turn_id: BoundedId::<MAX_ID_BYTES>::new("turn-b")?,
        terminal_kind: TerminalEvidenceKind::Completed,
        terminal_outcome: TerminalEvidenceOutcome::Succeeded,
        included_generations: codex_coordination::Evidence::Known {
            value: vec![codex_coordination::AssignmentGeneration::new(1)?],
        },
        observed_at: 1_753_000_100,
        after_revision: 1,
    })
}

async fn counts(pool: &sqlx::SqlitePool) -> anyhow::Result<(i64, i64)> {
    Ok((
        sqlx::query_scalar("SELECT COUNT(*) FROM coordination_degradation_records")
            .fetch_one(pool)
            .await?,
        sqlx::query_scalar("SELECT COUNT(*) FROM coordination_degradation_publication_outbox")
            .fetch_one(pool)
            .await?,
    ))
}
