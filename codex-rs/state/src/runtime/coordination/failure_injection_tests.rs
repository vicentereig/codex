use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::StateEpoch;
use pretty_assertions::assert_eq;
use sqlx::Row;

use super::aggregate_journal::AggregateStep;
use super::commands::CommandStep;
use super::commands::CommandWriteError;
use super::commands_tests::assignment_command;
use super::degradation::record_exogenous_terminal_degradation;
use super::degradation::record_exogenous_terminal_degradation_with;
use super::failure_injection_support::*;
use super::inbox_test_support::*;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_test_support::CHILD;
use super::recovery_test_support::compatibility_event;
use crate::StateRuntime;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::model::coordination_recovery::ExogenousTerminalObservation;
use crate::model::coordination_recovery::LegacySourceIdentity;
use crate::model::coordination_recovery::RecordExogenousTerminalOutcome;
use crate::model::coordination_recovery::TerminalEvidenceKind;
use crate::model::coordination_recovery::TerminalEvidenceOutcome;
use crate::model::coordination_recovery::TerminalProvenance;
use crate::runtime::test_support::unique_temp_dir;

const NOW_MS: i64 = 1_753_000_000_000;

pub(super) fn receipt_params_for_matrix()
-> crate::model::coordination_inbox::PersistRecipientReceipt {
    receipt_params(
        super::aggregate_test_support::OPERATION,
        RECEIPT_ONE,
        "019f7c6c-1111-7000-8000-000000000702",
        1,
        0,
        Vec::new(),
    )
}

fn committed_response_loss(point: CrashPoint) -> bool {
    matches!(
        point.boundary,
        Boundary::Aggregate(AggregateStep::AfterCommit)
            | Boundary::Recovery(RecoveryStep::AfterCommit)
    )
}

async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}

#[tokio::test]
async fn command_trace_reopens_at_every_counted_boundary_and_converges() -> anyhow::Result<()> {
    let recorder = CrashInjector::recording(NOW_MS);
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent_with(assignment_command(), &recorder)
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    let trace = recorder.trace();
    assert_eq!(trace.len(), 18);
    assert!(trace.iter().any(|point| {
        point
            == &CrashPoint {
                boundary: Boundary::Command(super::commands::CommandStep::CommandInsert),
                occurrence: 1,
            }
    }));
    assert!(trace.iter().any(|point| committed_response_loss(*point)));

    for (index, point) in trace.iter().copied().enumerate() {
        let home = unique_temp_dir();
        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let before = snapshot(&runtime).await?;
        runtime.close().await;
        drop(runtime);
        let control_home = unique_temp_dir();
        copy_closed_home(&home, &control_home).await?;
        let control = StateRuntime::init(control_home, "test".to_string()).await?;
        let control_clock = CrashInjector::recording(NOW_MS);
        let RecordCoordinationCommandOutcome::Applied(expected) = control
            .record_coordination_command_intent_with(assignment_command(), &control_clock)
            .await?
        else {
            anyhow::bail!("control command should apply");
        };
        let expected_committed = snapshot(&control).await?;
        control.close().await;
        drop(control);

        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let injector = CrashInjector::fail_at(point, NOW_MS);
        assert!(
            matches!(
                runtime
                    .record_coordination_command_intent_with(assignment_command(), &injector)
                    .await,
                Err(CommandWriteError::Internal(_))
            ),
            "{point:?}"
        );
        assert_owned_rollback_trace(
            &injector,
            &trace,
            index,
            Boundary::Command(CommandStep::Rollback),
        );
        drop(runtime);
        let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
        if committed_response_loss(point) {
            assert_eq!(snapshot(&reopened).await?, expected_committed, "{point:?}");
            assert_eq!(
                reopened
                    .record_coordination_command_intent_with(
                        assignment_command(),
                        &CrashInjector::recording(NOW_MS),
                    )
                    .await?,
                RecordCoordinationCommandOutcome::Duplicate(expected),
                "{point:?}"
            );
            assert_eq!(snapshot(&reopened).await?, expected_committed, "{point:?}");
        } else {
            assert_eq!(snapshot(&reopened).await?, before, "{point:?}");
            assert_eq!(
                reopened
                    .record_coordination_command_intent_with(
                        assignment_command(),
                        &CrashInjector::recording(NOW_MS),
                    )
                    .await?,
                RecordCoordinationCommandOutcome::Applied(expected.clone()),
                "{point:?}"
            );
            let committed = snapshot(&reopened).await?;
            assert_eq!(committed, expected_committed, "{point:?}");
            drop(reopened);
            let reopened = StateRuntime::init(home, "test".to_string()).await?;
            assert_eq!(
                reopened
                    .record_coordination_command_intent(assignment_command())
                    .await?,
                RecordCoordinationCommandOutcome::Duplicate(expected),
                "{point:?}"
            );
            assert_eq!(snapshot(&reopened).await?, committed, "{point:?}");
            assert_integrity(&reopened).await?;
            continue;
        }
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

pub(super) async fn runtime_with_command_at(
    home: std::path::PathBuf,
) -> anyhow::Result<std::sync::Arc<StateRuntime>> {
    let runtime = StateRuntime::init(home, "test".to_string()).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(assignment_command())
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    Ok(runtime)
}

#[tokio::test]
async fn recipient_trace_reopens_at_every_counted_boundary_and_converges() -> anyhow::Result<()> {
    let runtime = runtime_with_command_at(unique_temp_dir()).await?;
    let now_ms = delivery_now(&runtime).await?;
    let recorder = CrashInjector::recording(now_ms);
    assert!(matches!(
        runtime
            .persist_coordination_recipient_receipt_with(receipt_params_for_matrix(), &recorder)
            .await?,
        PersistRecipientReceiptOutcome::Applied(_)
    ));
    let trace = recorder.trace();
    assert_eq!(trace.len(), 24);
    assert!(trace.iter().any(|point| {
        point
            == &CrashPoint {
                boundary: Boundary::Inbox(super::inbox::InboxStep::ReceiptInsert),
                occurrence: 1,
            }
    }));
    assert!(
        trace
            .iter()
            .filter(|point| {
                point.boundary == Boundary::Aggregate(AggregateStep::AggregateMutation)
            })
            .count()
            >= 3
    );

    for (index, point) in trace.iter().copied().enumerate() {
        let home = unique_temp_dir();
        let runtime = runtime_with_command_at(home.clone()).await?;
        let now_ms = delivery_now(&runtime).await?;
        let before = snapshot(&runtime).await?;
        runtime.close().await;
        drop(runtime);
        let control_home = unique_temp_dir();
        copy_closed_home(&home, &control_home).await?;
        let control = StateRuntime::init(control_home, "test".to_string()).await?;
        let control_clock = CrashInjector::recording(now_ms);
        let PersistRecipientReceiptOutcome::Applied(expected) = control
            .persist_coordination_recipient_receipt_with(
                receipt_params_for_matrix(),
                &control_clock,
            )
            .await?
        else {
            anyhow::bail!("control recipient receipt should apply");
        };
        let expected_committed = snapshot(&control).await?;
        control.close().await;
        drop(control);

        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let injector = CrashInjector::fail_at(point, now_ms);
        assert!(
            matches!(
                runtime
                    .persist_coordination_recipient_receipt_with(
                        receipt_params_for_matrix(),
                        &injector,
                    )
                    .await,
                Err(InboxWriteError::Internal(_))
            ),
            "{point:?}"
        );
        assert_owned_rollback_trace(
            &injector,
            &trace,
            index,
            Boundary::Inbox(InboxStep::Rollback),
        );
        drop(runtime);
        let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
        if committed_response_loss(point) {
            assert_eq!(snapshot(&reopened).await?, expected_committed, "{point:?}");
            assert_eq!(
                reopened
                    .persist_coordination_recipient_receipt_with(
                        receipt_params_for_matrix(),
                        &CrashInjector::recording(now_ms),
                    )
                    .await?,
                PersistRecipientReceiptOutcome::Duplicate(expected),
                "{point:?}"
            );
            assert_eq!(snapshot(&reopened).await?, expected_committed, "{point:?}");
        } else {
            assert_eq!(snapshot(&reopened).await?, before, "{point:?}");
            assert_eq!(
                reopened
                    .persist_coordination_recipient_receipt_with(
                        receipt_params_for_matrix(),
                        &CrashInjector::recording(now_ms),
                    )
                    .await?,
                PersistRecipientReceiptOutcome::Applied(expected.clone()),
                "{point:?}"
            );
            let committed = snapshot(&reopened).await?;
            assert_eq!(committed, expected_committed, "{point:?}");
            drop(reopened);
            let reopened = StateRuntime::init(home, "test".to_string()).await?;
            assert_eq!(
                reopened
                    .persist_coordination_recipient_receipt(receipt_params_for_matrix())
                    .await?,
                PersistRecipientReceiptOutcome::Duplicate(expected),
                "{point:?}"
            );
            assert_eq!(snapshot(&reopened).await?, committed, "{point:?}");
            assert_integrity(&reopened).await?;
            continue;
        }
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

fn assert_owned_rollback_trace(
    injector: &CrashInjector,
    successful_trace: &[CrashPoint],
    failed_index: usize,
    rollback: Boundary,
) {
    if committed_response_loss(successful_trace[failed_index]) {
        assert_eq!(injector.trace(), successful_trace);
        return;
    }
    let mut expected = successful_trace[..=failed_index].to_vec();
    expected.push(CrashPoint {
        boundary: rollback,
        occurrence: 1,
    });
    assert_eq!(injector.trace(), expected);
}

pub(super) async fn delivery_now(runtime: &StateRuntime) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT intent_at_ms + 1 FROM coordination_commands WHERE operation_id=?",
    )
    .bind(super::aggregate_test_support::OPERATION)
    .fetch_one(&*runtime.pool)
    .await?)
}

#[tokio::test]
async fn degradation_trace_reopens_atomically_and_replays_one_pair() -> anyhow::Result<()> {
    let recorder = CrashInjector::recording(NOW_MS);
    let (runtime, epoch) = runtime_with_root_at(unique_temp_dir()).await?;
    assert!(matches!(
        record_exogenous_terminal_degradation_with(&runtime.pool, observation(epoch)?, &recorder,)
            .await?,
        RecordExogenousTerminalOutcome::Applied(_)
    ));
    let trace = recorder.trace();
    assert_eq!(trace.len(), 11);
    assert_eq!(
        trace.iter().map(|point| point.boundary).collect::<Vec<_>>(),
        vec![
            Boundary::Recovery(RecoveryStep::TransactionBegin),
            Boundary::Recovery(RecoveryStep::MarkerRead),
            Boundary::Recovery(RecoveryStep::MarkerRead),
            Boundary::Recovery(RecoveryStep::AuthorityRead),
            Boundary::Recovery(RecoveryStep::AuthorityRead),
            Boundary::Recovery(RecoveryStep::AnchorRead),
            Boundary::Recovery(RecoveryStep::LegacyRead),
            Boundary::Recovery(RecoveryStep::DegradationInsert),
            Boundary::Recovery(RecoveryStep::DegradationOutboxInsert),
            Boundary::Recovery(RecoveryStep::BeforeCommit),
            Boundary::Recovery(RecoveryStep::AfterCommit),
        ]
    );

    for point in trace {
        let home = unique_temp_dir();
        let (runtime, epoch) = runtime_with_root_at(home.clone()).await?;
        let before = snapshot(&runtime).await?;
        let evidence = observation(epoch)?;
        assert!(
            record_exogenous_terminal_degradation_with(
                &runtime.pool,
                evidence.clone(),
                &CrashInjector::fail_at(point, NOW_MS),
            )
            .await
            .is_err(),
            "{point:?}"
        );
        drop(runtime);
        let reopened = StateRuntime::init(home, "test".to_string()).await?;
        if committed_response_loss(point) {
            let committed = snapshot(&reopened).await?;
            assert!(matches!(
                record_exogenous_terminal_degradation(&reopened.pool, evidence).await?,
                RecordExogenousTerminalOutcome::Duplicate(_)
            ));
            assert_eq!(snapshot(&reopened).await?, committed);
        } else {
            assert_eq!(snapshot(&reopened).await?, before, "{point:?}");
            assert!(matches!(
                record_exogenous_terminal_degradation(&reopened.pool, evidence.clone()).await?,
                RecordExogenousTerminalOutcome::Applied(_)
            ));
            let committed = snapshot(&reopened).await?;
            assert!(matches!(
                record_exogenous_terminal_degradation(&reopened.pool, evidence).await?,
                RecordExogenousTerminalOutcome::Duplicate(_)
            ));
            assert_eq!(snapshot(&reopened).await?, committed);
        }
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

#[tokio::test]
async fn committed_marker_quarantine_never_traces_rollback() -> anyhow::Result<()> {
    let expected = vec![
        Boundary::Recovery(RecoveryStep::TransactionBegin),
        Boundary::Recovery(RecoveryStep::MarkerRead),
        Boundary::Recovery(RecoveryStep::MarkerRead),
        Boundary::Recovery(RecoveryStep::MarkerUpdate),
        Boundary::Recovery(RecoveryStep::MarkerCommit),
    ];
    for fail_after_commit in [false, true] {
        let home = unique_temp_dir();
        let (runtime, epoch) = runtime_with_root_at(home.clone()).await?;
        tokio::fs::remove_file(home.join(super::authority_marker::MARKER_FILE_NAME)).await?;
        let injector = if fail_after_commit {
            CrashInjector::fail_at(
                CrashPoint {
                    boundary: Boundary::Recovery(RecoveryStep::MarkerCommit),
                    occurrence: 1,
                },
                NOW_MS,
            )
        } else {
            CrashInjector::recording(NOW_MS)
        };
        let result = record_exogenous_terminal_degradation_with(
            &runtime.pool,
            observation(epoch)?,
            &injector,
        )
        .await;
        if fail_after_commit {
            assert!(matches!(result, Err(RecoveryWriteError::Internal(_))));
        } else {
            assert!(matches!(result, Err(RecoveryWriteError::Quarantined)));
        }
        assert_eq!(
            injector
                .trace()
                .into_iter()
                .map(|point| point.boundary)
                .collect::<Vec<_>>(),
            expected
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM coordination_authority WHERE singleton_id=1")
                .fetch_one(&*runtime.pool)
                .await?;
        assert_eq!(status, "quarantined");
    }
    Ok(())
}

pub(super) async fn runtime_with_root_at(
    home: std::path::PathBuf,
) -> anyhow::Result<(std::sync::Arc<StateRuntime>, StateEpoch)> {
    let runtime = StateRuntime::init(home, "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(super::aggregate_test_support::reserve_params())
        .await?;
    let row = sqlx::query("SELECT state_epoch FROM coordination_authority WHERE singleton_id=1")
        .fetch_one(&*runtime.pool)
        .await?;
    Ok((
        runtime,
        StateEpoch::parse(&row.get::<String, _>("state_epoch"))?,
    ))
}

pub(super) fn observation(epoch: StateEpoch) -> anyhow::Result<ExogenousTerminalObservation> {
    let event = compatibility_event(CoordinationSemanticSlot::TurnCompleted, 23);
    Ok(ExogenousTerminalObservation {
        root_thread_id: super::aggregate_test_support::thread(super::aggregate_test_support::ROOT),
        captured_state_epoch: Some(epoch),
        provenance: TerminalProvenance::Known(LegacySourceIdentity::from_event(&event)?),
        target_thread_id: super::aggregate_test_support::thread(CHILD),
        target_turn_id: BoundedId::<MAX_ID_BYTES>::new("turn-b")?,
        terminal_kind: TerminalEvidenceKind::Completed,
        terminal_outcome: TerminalEvidenceOutcome::Succeeded,
        included_generations: codex_coordination::Evidence::Known {
            value: vec![super::aggregate_test_support::generation(1)],
        },
        observed_at: 1_753_000_100,
        after_revision: 1,
    })
}
