use pretty_assertions::assert_eq;

use super::failure_injection_recovery_semantic_matrix_support::*;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashInjector;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_support::assert_integrity;
use super::failure_injection_support::copy_closed_home;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use crate::StateRuntime;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn recovery_semantic_ingestion_crash_matrix_reopens_and_converges() -> anyhow::Result<()> {
    for case in CASES {
        run_case(case).await?;
    }
    Ok(())
}

async fn run_case(case: SemanticCase) -> anyhow::Result<()> {
    assert_success_trace(case).await?;
    let success_trace = case.success_trace();

    for (index, point) in success_trace.iter().copied().enumerate() {
        let (runtime, input) = case.setup().await?;
        let home = runtime.codex_home().to_path_buf();
        runtime.close().await;
        drop(runtime);

        let control_home = unique_temp_dir();
        copy_closed_home(&home, &control_home).await?;
        let control = StateRuntime::init(control_home, "test".to_string()).await?;
        let control_injector = CrashInjector::recording(NOW_MS);
        let expected_output = case.invoke(&control, &input, &control_injector).await?;
        case.assert_success(&expected_output);
        assert_eq!(control_injector.trace(), success_trace);
        let expected_committed = snapshot(&control).await?;
        assert_snapshot_private(&expected_committed);
        assert_integrity(&control).await?;
        control.close().await;
        drop(control);

        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let before = snapshot(&runtime).await?;
        assert_snapshot_private(&before);
        let injector = CrashInjector::fail_at(point, NOW_MS);
        let result = case.invoke(&runtime, &input, &injector).await;
        assert!(matches!(result, Err(RecoveryWriteError::Internal(_))));

        if point.boundary == Boundary::Recovery(RecoveryStep::AfterCommit) {
            assert_eq!(injector.trace(), success_trace);
            drop(runtime);

            let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
            let committed = snapshot(&reopened).await?;
            assert_eq!(committed, expected_committed);
            assert_snapshot_private(&committed);
            assert_stable(case, &reopened, &input, &expected_output).await?;
            assert_eq!(snapshot(&reopened).await?, expected_committed);
            assert_integrity(&reopened).await?;
            reopened.close().await;
            continue;
        }

        let mut expected_failure_trace = success_trace[..=index].to_vec();
        expected_failure_trace.push(CrashPoint {
            boundary: Boundary::Recovery(RecoveryStep::Rollback),
            occurrence: 1,
        });
        assert_eq!(injector.trace(), expected_failure_trace);
        drop(runtime);

        let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
        assert_eq!(snapshot(&reopened).await?, before);
        let retry_injector = CrashInjector::recording(NOW_MS);
        let retry_output = case.invoke(&reopened, &input, &retry_injector).await?;
        assert_eq!(retry_output, expected_output);
        assert_eq!(retry_injector.trace(), success_trace);
        let committed = snapshot(&reopened).await?;
        assert_eq!(committed, expected_committed);
        assert_snapshot_private(&committed);
        drop(reopened);

        let stable = StateRuntime::init(home, "test".to_string()).await?;
        assert_stable(case, &stable, &input, &expected_output).await?;
        assert_eq!(snapshot(&stable).await?, expected_committed);
        assert_integrity(&stable).await?;
        stable.close().await;
    }
    Ok(())
}

async fn assert_success_trace(case: SemanticCase) -> anyhow::Result<()> {
    let (runtime, input) = case.setup().await?;
    let injector = CrashInjector::recording(NOW_MS);
    let output = case.invoke(&runtime, &input, &injector).await?;
    case.assert_success(&output);
    assert_eq!(injector.trace(), case.success_trace());
    assert_snapshot_private(&snapshot(&runtime).await?);
    assert_integrity(&runtime).await?;
    runtime.close().await;
    Ok(())
}

async fn assert_stable(
    case: SemanticCase,
    runtime: &StateRuntime,
    input: &SemanticInput,
    expected_success: &SemanticOutput,
) -> anyhow::Result<()> {
    let injector = CrashInjector::recording(NOW_MS);
    let output = case.invoke(runtime, input, &injector).await?;
    assert_eq!(output, stable_output(expected_success));
    assert_eq!(injector.trace(), case.stable_trace());
    Ok(())
}
