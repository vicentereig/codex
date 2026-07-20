use pretty_assertions::assert_eq;

use super::aggregate_journal::AggregateStep;
use super::failure_injection_inbox_matrix_support::*;
use super::failure_injection_support::*;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use crate::StateRuntime;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn all_inbox_wrappers_exhaust_counted_crash_recovery_matrix() -> anyhow::Result<()> {
    for case in CASES {
        run_case(case).await?;
    }
    Ok(())
}

async fn run_case(case: InboxCase) -> anyhow::Result<()> {
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
        assert_artifacts(case, &control, &input, /*committed*/ true).await?;
        assert_integrity(&control).await?;
        drop(control);

        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let before = snapshot(&runtime).await?;
        let injector = CrashInjector::fail_at(point, NOW_MS);
        assert!(
            matches!(
                case.invoke(&runtime, &input, &injector).await,
                Err(InboxWriteError::Internal(_))
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
            assert_artifacts(case, &reopened, &input, /*committed*/ true).await?;
            assert_stable(case, &reopened, &input, &expected_output).await?;
            assert_eq!(
                snapshot(&reopened).await?,
                expected_committed,
                "{case:?} {point:?}"
            );
            assert_integrity(&reopened).await?;
            continue;
        }

        let mut rolled_back = expected_trace[..=index].to_vec();
        rolled_back.push(CrashPoint {
            boundary: Boundary::Inbox(InboxStep::Rollback),
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
        let committed = snapshot(&reopened).await?;
        assert_eq!(committed, expected_committed, "{case:?} {point:?}");
        assert_artifacts(case, &reopened, &input, /*committed*/ true).await?;
        drop(reopened);

        let reopened = StateRuntime::init(home, "test".to_string()).await?;
        assert_stable(case, &reopened, &input, &expected_output).await?;
        assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
        assert_artifacts(case, &reopened, &input, /*committed*/ true).await?;
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

async fn assert_success_trace(case: InboxCase) -> anyhow::Result<()> {
    let (runtime, input) = case.setup().await?;
    let recorder = CrashInjector::recording(NOW_MS);
    let output = case.invoke(&runtime, &input, &recorder).await?;
    assert_success(case, &output);
    assert_eq!(recorder.trace(), case.expected_trace(), "{case:?}");
    assert_artifacts(case, &runtime, &input, /*committed*/ true).await?;
    assert_integrity(&runtime).await?;
    Ok(())
}

async fn assert_stable(
    case: InboxCase,
    runtime: &StateRuntime,
    input: &InboxInput,
    successful: &InboxOutput,
) -> anyhow::Result<()> {
    let replay = CrashInjector::recording(NOW_MS);
    assert_eq!(
        case.invoke(runtime, input, &replay).await?,
        case.stable_output(successful),
        "{case:?}"
    );
    Ok(())
}

async fn assert_artifacts(
    case: InboxCase,
    runtime: &StateRuntime,
    input: &InboxInput,
    committed: bool,
) -> anyhow::Result<()> {
    assert_eq!(
        runtime
            .coordination_durable_receipt_ack(input.ack.receipt_id)
            .await?,
        input.ack
    );
    let ciphertext: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT ciphertext FROM coordination_inbox WHERE receipt_id=?")
            .bind(input.ack.receipt_id.to_string())
            .fetch_one(&*runtime.pool)
            .await?;
    if committed && !case.keeps_ciphertext() {
        assert!(ciphertext.is_none(), "{case:?}");
    } else {
        let ciphertext = ciphertext.as_deref().expect("retained ciphertext");
        assert!(ciphertext == input.ciphertext.as_slice(), "{case:?}");
    }
    Ok(())
}

async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}
