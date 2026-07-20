use pretty_assertions::assert_eq;

use super::aggregate_journal::AggregateStep;
use super::aggregate_journal::CoordinationWriteError;
use super::failure_injection_aggregate_matrix_support::*;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashInjector;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_support::assert_integrity;
use super::failure_injection_support::copy_closed_home;
use crate::StateRuntime;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn all_aggregate_wrappers_exhaust_counted_crash_recovery_matrix() -> anyhow::Result<()> {
    for case in CASES {
        run_case(case).await?;
    }
    Ok(())
}

async fn run_case(case: AggregateCase) -> anyhow::Result<()> {
    assert_success_trace(case).await?;
    let expected_trace = case.expected_trace();

    for (index, point) in expected_trace.iter().copied().enumerate() {
        let (runtime, input) = case.setup().await?;
        let home = runtime.codex_home().to_path_buf();
        runtime.close().await;
        drop(runtime);

        let control_home = unique_temp_dir();
        copy_closed_home(&home, &control_home).await?;
        let control = StateRuntime::init(control_home, "test".to_string()).await?;
        let control_injector = CrashInjector::recording(NOW_MS);
        let expected_output = case.invoke(&control, &input, &control_injector).await?;
        assert_success(case, &expected_output);
        assert_eq!(control_injector.trace(), expected_trace, "{case:?}");
        let expected_committed = snapshot(&control).await?;
        assert_integrity(&control).await?;
        drop(control);

        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let before = snapshot(&runtime).await?;
        let injector = CrashInjector::fail_at(point, NOW_MS);
        assert!(
            matches!(
                case.invoke(&runtime, &input, &injector).await,
                Err(CoordinationWriteError::Internal(_))
            ),
            "{case:?} {point:?}"
        );

        if point.boundary == Boundary::Aggregate(AggregateStep::AfterCommit) {
            assert_eq!(injector.trace(), expected_trace, "{case:?} {point:?}");
            drop(runtime);
            let reopened = StateRuntime::init(home, "test".to_string()).await?;
            assert_eq!(
                snapshot(&reopened).await?,
                expected_committed,
                "{case:?} {point:?}"
            );
            let replay = CrashInjector::recording(NOW_MS);
            let actual = case.invoke(&reopened, &input, &replay).await?;
            assert_stable_replay(case, &expected_output, actual);
            assert_eq!(
                snapshot(&reopened).await?,
                expected_committed,
                "{case:?} {point:?}"
            );
            assert_integrity(&reopened).await?;
            continue;
        }

        let mut expected_rollback = expected_trace[..=index].to_vec();
        expected_rollback.push(CrashPoint {
            boundary: Boundary::Aggregate(AggregateStep::Rollback),
            occurrence: 1,
        });
        assert_eq!(injector.trace(), expected_rollback, "{case:?} {point:?}");
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
        let committed = snapshot(&reopened).await?;
        assert_eq!(committed, expected_committed, "{case:?} {point:?}");
        drop(reopened);

        let reopened = StateRuntime::init(home, "test".to_string()).await?;
        let replay = CrashInjector::recording(NOW_MS);
        let actual = case.invoke(&reopened, &input, &replay).await?;
        assert_stable_replay(case, &expected_output, actual);
        assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

async fn assert_success_trace(case: AggregateCase) -> anyhow::Result<()> {
    let (runtime, input) = case.setup().await?;
    let recorder = CrashInjector::recording(NOW_MS);
    let output = case.invoke(&runtime, &input, &recorder).await?;
    assert_success(case, &output);
    assert_eq!(recorder.trace(), case.expected_trace(), "{case:?}");
    assert_integrity(&runtime).await?;
    Ok(())
}
