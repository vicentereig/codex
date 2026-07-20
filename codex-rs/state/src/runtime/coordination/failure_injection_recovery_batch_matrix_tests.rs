use pretty_assertions::assert_eq;

use super::authority_marker::MARKER_FILE_NAME;
use super::failure_injection_recovery_matrix_support::*;
use super::failure_injection_support::*;
use super::recovery::RecoveryBatch;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use crate::StateRuntime;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn recovery_batch_exhausts_distinct_counted_mutation_matrices() -> anyhow::Result<()> {
    for case in BATCH_CASES {
        run_batch(case).await?;
    }
    Ok(())
}

async fn run_batch(case: BatchCase) -> anyhow::Result<()> {
    let expected_trace = case.trace();
    for (index, point) in expected_trace.iter().copied().enumerate() {
        let (runtime, input) = case
            .setup()
            .await
            .map_err(|error| anyhow::anyhow!("{case:?}: {error:#}"))?;
        let home = runtime.codex_home().to_path_buf();
        runtime.close().await;
        drop(runtime);
        let control_home = unique_temp_dir();
        copy_closed_home(&home, &control_home).await?;
        let control = StateRuntime::init(control_home, "test".to_string()).await?;
        let recorder = CrashInjector::recording(NOW_MS);
        let expected_output = case.invoke(&control, &input, &recorder).await?;
        assert_eq!(expected_output.dispositions, case.expected(), "{case:?}");
        assert_eq!(recorder.trace(), expected_trace, "{case:?}");
        let committed = snapshot(&control).await?;
        assert_artifacts(case, &control, &input, /*committed*/ true).await?;
        assert_integrity(&control).await?;
        drop(control);

        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let before = snapshot(&runtime).await?;
        let injector = CrashInjector::fail_at(point, NOW_MS);
        assert!(
            matches!(
                case.invoke(&runtime, &input, &injector).await,
                Err(RecoveryWriteError::Internal(_))
            ),
            "{case:?} {point:?}"
        );
        if point.boundary == Boundary::Recovery(RecoveryStep::AfterCommit) {
            assert_eq!(injector.trace(), expected_trace, "{case:?} {point:?}");
            drop(runtime);
            let reopened = StateRuntime::init(home, "test".to_string()).await?;
            assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
            assert_stable(case, &reopened, &input).await?;
            assert_artifacts(case, &reopened, &input, /*committed*/ true).await?;
            assert_integrity(&reopened).await?;
            continue;
        }
        let mut rolled_back = expected_trace[..=index].to_vec();
        rolled_back.push(CrashPoint {
            boundary: Boundary::Recovery(RecoveryStep::Rollback),
            occurrence: 1,
        });
        assert_eq!(injector.trace(), rolled_back, "{case:?} {point:?}");
        drop(runtime);
        let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
        assert_eq!(snapshot(&reopened).await?, before, "{case:?} {point:?}");
        assert_artifacts(case, &reopened, &input, /*committed*/ false).await?;
        let retry = CrashInjector::recording(NOW_MS);
        assert_eq!(
            case.invoke(&reopened, &input, &retry).await?,
            expected_output,
            "{case:?} {point:?}"
        );
        assert_eq!(retry.trace(), expected_trace, "{case:?} {point:?}");
        assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
        drop(reopened);
        let reopened = StateRuntime::init(home, "test".to_string()).await?;
        assert_stable(case, &reopened, &input).await?;
        assert_artifacts(case, &reopened, &input, /*committed*/ true).await?;
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

async fn assert_stable(
    case: BatchCase,
    runtime: &StateRuntime,
    input: &BatchInput,
) -> anyhow::Result<()> {
    let before = snapshot(runtime).await?;
    assert_eq!(
        case.invoke(runtime, input, &CrashInjector::recording(NOW_MS))
            .await?,
        RecoveryBatch {
            dispositions: Vec::new()
        },
        "{case:?}"
    );
    assert_eq!(snapshot(runtime).await?, before, "{case:?}");
    Ok(())
}

async fn assert_artifacts(
    case: BatchCase,
    runtime: &StateRuntime,
    input: &BatchInput,
    committed: bool,
) -> anyhow::Result<()> {
    for ack in &input.acks {
        assert_eq!(
            runtime
                .coordination_durable_receipt_ack(ack.receipt_id)
                .await?,
            ack.clone()
        );
    }
    let commands: Vec<(String, Vec<u8>)> = sqlx::query_as("SELECT operation_id,ciphertext FROM coordination_commands WHERE ciphertext IS NOT NULL ORDER BY operation_id").fetch_all(&*runtime.pool).await?;
    if !committed || !matches!(case, BatchCase::InterruptExpire) {
        assert!(commands == input.command_ciphertexts, "{case:?}");
    }
    let inbox: Vec<(String, Vec<u8>)> = sqlx::query_as("SELECT receipt_id,ciphertext FROM coordination_inbox WHERE ciphertext IS NOT NULL ORDER BY receipt_id").fetch_all(&*runtime.pool).await?;
    if committed && matches!(case, BatchCase::InboxExpireReceipts) {
        assert!(inbox.is_empty(), "{case:?}");
    } else {
        assert!(inbox == input.inbox_ciphertexts, "{case:?}");
    }
    let authority: (String, Option<String>) = sqlx::query_as(
        "SELECT status,quarantine_reason FROM coordination_authority WHERE singleton_id=1",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(authority, ("active".to_string(), None), "{case:?}");
    Ok(())
}

#[tokio::test]
async fn marker_commit_is_the_only_committed_failure_without_rollback() -> anyhow::Result<()> {
    let expected = counted(&[
        Boundary::Recovery(RecoveryStep::TransactionBegin),
        Boundary::Recovery(RecoveryStep::MarkerRead),
        Boundary::Recovery(RecoveryStep::MarkerRead),
        Boundary::Recovery(RecoveryStep::MarkerUpdate),
        Boundary::Recovery(RecoveryStep::MarkerCommit),
    ]);
    for (index, point) in expected.iter().copied().enumerate() {
        let (runtime, input) = BatchCase::PendingAssignment.setup().await?;
        let home = runtime.codex_home().to_path_buf();
        runtime.close().await;
        drop(runtime);
        let control_home = unique_temp_dir();
        copy_closed_home(&home, &control_home).await?;
        let init_control_home = unique_temp_dir();
        copy_closed_home(&home, &init_control_home).await?;
        tokio::fs::remove_file(init_control_home.join(MARKER_FILE_NAME)).await?;
        let init_control = StateRuntime::init(init_control_home, "test".to_string()).await?;
        let initialized_quarantine = normalized_authority_clock(snapshot(&init_control).await?);
        assert_quarantined(
            &init_control,
            "coordination DB facts exist without an authority marker",
        )
        .await?;
        drop(init_control);
        let control = StateRuntime::init(control_home.clone(), "test".to_string()).await?;
        tokio::fs::remove_file(control_home.join(MARKER_FILE_NAME)).await?;
        let recorder = CrashInjector::recording(NOW_MS);
        assert!(matches!(
            BatchCase::PendingAssignment
                .invoke(&control, &input, &recorder)
                .await,
            Err(RecoveryWriteError::Quarantined)
        ));
        assert_eq!(recorder.trace(), expected);
        assert_quarantined(
            &control,
            "coordination authority marker changed during recovery",
        )
        .await?;
        let recovery_quarantine = snapshot(&control).await?;
        drop(control);
        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        tokio::fs::remove_file(home.join(MARKER_FILE_NAME)).await?;
        let before = snapshot(&runtime).await?;
        let injector = CrashInjector::fail_at(point, NOW_MS);
        assert!(
            matches!(
                BatchCase::PendingAssignment
                    .invoke(&runtime, &input, &injector)
                    .await,
                Err(RecoveryWriteError::Internal(_))
            ),
            "{point:?}"
        );
        if point.boundary == Boundary::Recovery(RecoveryStep::MarkerCommit) {
            assert_eq!(injector.trace(), expected);
            drop(runtime);
            let reopened = StateRuntime::init(home, "test".to_string()).await?;
            assert_eq!(snapshot(&reopened).await?, recovery_quarantine);
            assert_quarantined(
                &reopened,
                "coordination authority marker changed during recovery",
            )
            .await?;
            assert_integrity(&reopened).await?;
        } else {
            let mut rolled_back = expected[..=index].to_vec();
            rolled_back.push(CrashPoint {
                boundary: Boundary::Recovery(RecoveryStep::Rollback),
                occurrence: 1,
            });
            assert_eq!(injector.trace(), rolled_back);
            assert_eq!(snapshot(&runtime).await?, before);
            drop(runtime);
            let reopened = StateRuntime::init(home, "test".to_string()).await?;
            assert_eq!(
                normalized_authority_clock(snapshot(&reopened).await?),
                initialized_quarantine
            );
            assert_quarantined(
                &reopened,
                "coordination DB facts exist without an authority marker",
            )
            .await?;
            assert!(matches!(
                BatchCase::PendingAssignment
                    .invoke(&reopened, &input, &CrashInjector::recording(NOW_MS))
                    .await,
                Err(RecoveryWriteError::Quarantined)
            ));
            assert_eq!(
                normalized_authority_clock(snapshot(&reopened).await?),
                initialized_quarantine
            );
            assert_integrity(&reopened).await?;
        }
    }
    Ok(())
}

fn normalized_authority_clock(mut state: FrozenCoordinationState) -> FrozenCoordinationState {
    let authority = state
        .tables
        .iter_mut()
        .find(|table| table.name == "coordination_authority")
        .expect("authority table");
    let updated_at = authority
        .columns
        .iter()
        .position(|column| column == "updated_at_ms")
        .expect("authority updated_at_ms");
    authority.rows[0][updated_at] = FrozenCell::Integer(0);
    state
}

async fn assert_quarantined(runtime: &StateRuntime, reason: &str) -> anyhow::Result<()> {
    let authority: (String, Option<String>) = sqlx::query_as(
        "SELECT status,quarantine_reason FROM coordination_authority WHERE singleton_id=1",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    assert_eq!(
        authority,
        ("quarantined".to_string(), Some(reason.to_string()))
    );
    Ok(())
}

async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}
