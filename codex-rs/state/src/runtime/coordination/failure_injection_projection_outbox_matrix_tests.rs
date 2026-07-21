use pretty_assertions::assert_eq;

use super::failure_injection_projection_outbox_matrix_support::*;
use super::failure_injection_support::*;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use crate::StateRuntime;
use crate::model::coordination_recovery_state::*;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn projection_publication_wrappers_exhaust_counted_crash_recovery_matrix()
-> anyhow::Result<()> {
    for case in PROJECTION_OUTBOX_CASES {
        run_outbox(case).await?;
    }
    Ok(())
}

async fn run_outbox(case: ProjectionOutboxCase) -> anyhow::Result<()> {
    let expected_trace = case.trace();
    for (index, point) in expected_trace.iter().copied().enumerate() {
        let (runtime, input) = case.setup().await?;
        let home = runtime.codex_home().to_path_buf();
        runtime.close().await;
        drop(runtime);
        let control_home = unique_temp_dir();
        copy_closed_home(&home, &control_home).await?;
        let control = StateRuntime::init(control_home, "test".to_string()).await?;
        let recorder = CrashInjector::recording(NOW_MS);
        let expected_output = case.invoke(&control, &input, &recorder).await?;
        assert_outbox_success(case, &expected_output);
        assert_eq!(recorder.trace(), expected_trace, "{case:?}");
        let committed = snapshot(&control).await?;
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
            assert_outbox_stable(case, &reopened, &input, &expected_output).await?;
            assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
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
        assert_outbox_stable(case, &reopened, &input, &expected_output).await?;
        assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

fn assert_outbox_success(case: ProjectionOutboxCase, output: &ProjectionOutboxOutput) {
    let valid = match (case, output) {
        (
            ProjectionOutboxCase::Claim,
            ProjectionOutboxOutput::Claim(ClaimProjectionPublicationsOutcome::Claimed(values)),
        ) => values.len() == 1,
        (
            ProjectionOutboxCase::Materialized,
            ProjectionOutboxOutput::Resolve(ResolveProjectionPublicationOutcome::Applied(
                ProjectionPublicationStatus::Materialized,
            )),
        )
        | (
            ProjectionOutboxCase::Retry,
            ProjectionOutboxOutput::Resolve(ResolveProjectionPublicationOutcome::Applied(
                ProjectionPublicationStatus::Pending,
            )),
        )
        | (
            ProjectionOutboxCase::Poisoned | ProjectionOutboxCase::RetryExhausted,
            ProjectionOutboxOutput::Resolve(ResolveProjectionPublicationOutcome::Applied(
                ProjectionPublicationStatus::Poisoned,
            )),
        ) => true,
        _ => false,
    };
    assert!(valid, "{case:?}: {output:?}");
}

async fn assert_outbox_stable(
    case: ProjectionOutboxCase,
    runtime: &StateRuntime,
    input: &ProjectionOutboxInput,
    output: &ProjectionOutboxOutput,
) -> anyhow::Result<()> {
    let before = snapshot(runtime).await?;
    assert_eq!(
        case.invoke(runtime, input, &CrashInjector::recording(NOW_MS))
            .await?,
        case.stable(output),
        "{case:?}"
    );
    assert_eq!(snapshot(runtime).await?, before, "{case:?}");
    Ok(())
}

async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}
