use codex_coordination::AssignmentId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::GenerationCloseReason;
use pretty_assertions::assert_eq;

use super::aggregate_journal::CoordinationWriteError;
use super::aggregate_journal::validate_identities;
use super::aggregate_test_support::*;
use super::aggregates::AssignmentTransitionOutcome;
use crate::StateRuntime;
use crate::model::coordination::CloseReservedAssignment;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn abandoned_reservation_closes_once_and_identity_bundles_are_pairwise_distinct()
-> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let assignment_id = AssignmentId::parse(ASSIGNMENT)?;
    let close = CloseReservedAssignment {
        context: context(
            CoordinationSemanticSlot::AssignmentGenerationClosed,
            "019f7c6c-1111-7000-8000-000000000711",
            OPERATION,
            false,
            1,
            Vec::new(),
        ),
        assignment_id,
        generation: generation(1),
        reason: GenerationCloseReason::AbandonedBeforeAcceptance,
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 0,
    };
    assert!(matches!(
        runtime
            .close_reserved_coordination_assignment(close.clone())
            .await?,
        AssignmentTransitionOutcome::Applied { .. }
    ));
    assert!(matches!(
        runtime
            .close_reserved_coordination_assignment(close)
            .await?,
        AssignmentTransitionOutcome::Duplicate { .. }
    ));
    let record = runtime
        .coordination_assignment_aggregate(assignment_id)
        .await?
        .expect("aggregate");
    assert_eq!(
        record.generations[0].lifecycle,
        crate::model::coordination::GenerationLifecycle::Abandoned
    );
    assert_eq!(record.head.accepted_generation, None);

    let duplicate_event = "019f7c6c-1111-7000-8000-000000000712";
    let duplicate_context = context(
        CoordinationSemanticSlot::TurnCompleted,
        duplicate_event,
        "019f7c6c-1111-7000-8000-000000000112",
        false,
        2,
        vec![(
            CoordinationSemanticSlot::AssignmentGenerationClosed,
            duplicate_event,
            "019f7c6c-1111-7000-8000-000000000113",
        )],
    );
    assert!(matches!(
        validate_identities(&duplicate_context),
        Err(CoordinationWriteError::IdentityCollision)
    ));
    Ok(())
}
