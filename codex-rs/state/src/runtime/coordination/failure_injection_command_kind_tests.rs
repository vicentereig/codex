use std::sync::Arc;

use pretty_assertions::assert_eq;

use super::aggregate_journal::AggregateStep;
use super::commands::CommandStep;
use super::commands::CommandWriteError;
use super::commands_tests::accepted_runtime;
use super::failure_injection_support::*;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::inbox_test_support::*;
use crate::StateRuntime;
use crate::model::coordination_commands::RecordCoordinationCommand;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::PersistRecipientReceipt;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::runtime::test_support::unique_temp_dir;

const NOW_MS: i64 = 1_753_000_000_000;

#[derive(Clone, Copy, Debug)]
enum DeliveryCase {
    MessageIntent,
    InterruptIntent,
    MessageReceipt,
    InterruptReceipt,
}

const CASES: [DeliveryCase; 4] = [
    DeliveryCase::MessageIntent,
    DeliveryCase::InterruptIntent,
    DeliveryCase::MessageReceipt,
    DeliveryCase::InterruptReceipt,
];

#[derive(Clone)]
enum DeliveryInput {
    Intent(RecordCoordinationCommand),
    Receipt(PersistRecipientReceipt),
}

#[derive(Debug, Eq, PartialEq)]
enum DeliveryOutput {
    Intent(RecordCoordinationCommandOutcome),
    Receipt(PersistRecipientReceiptOutcome),
}

#[derive(Debug)]
enum DeliveryError {
    Intent(CommandWriteError),
    Receipt(InboxWriteError),
}

impl DeliveryCase {
    async fn setup(self) -> anyhow::Result<(Arc<StateRuntime>, DeliveryInput)> {
        let fixture = accepted_runtime().await?;
        let runtime = fixture.runtime;
        let command = match self {
            Self::MessageIntent | Self::MessageReceipt => message_command(2),
            Self::InterruptIntent | Self::InterruptReceipt => interrupt_command(2),
        };
        if matches!(self, Self::MessageIntent | Self::InterruptIntent) {
            return Ok((runtime, DeliveryInput::Intent(command)));
        }
        let setup_clock = CrashInjector::recording(NOW_MS);
        assert!(matches!(
            runtime
                .record_coordination_command_intent_with(command, &setup_clock)
                .await?,
            RecordCoordinationCommandOutcome::Applied(_)
        ));
        let operation = match self {
            Self::MessageReceipt => MESSAGE_OPERATION,
            Self::InterruptReceipt => INTERRUPT_OPERATION,
            Self::MessageIntent | Self::InterruptIntent => unreachable!("intent returned above"),
        };
        Ok((
            runtime,
            DeliveryInput::Receipt(receipt_params(
                operation,
                RECEIPT_TWO,
                "019f7c6c-1111-7000-8000-000000000706",
                3,
                1,
                Vec::new(),
            )),
        ))
    }

    async fn invoke(
        self,
        runtime: &StateRuntime,
        input: &DeliveryInput,
        injector: &CrashInjector,
    ) -> Result<DeliveryOutput, DeliveryError> {
        match input {
            DeliveryInput::Intent(command) => runtime
                .record_coordination_command_intent_with(command.clone(), injector)
                .await
                .map(DeliveryOutput::Intent)
                .map_err(DeliveryError::Intent),
            DeliveryInput::Receipt(receipt) => runtime
                .persist_coordination_recipient_receipt_with(receipt.clone(), injector)
                .await
                .map(DeliveryOutput::Receipt)
                .map_err(DeliveryError::Receipt),
        }
    }

    fn rollback(self) -> Boundary {
        match self {
            Self::MessageIntent | Self::InterruptIntent => {
                Boundary::Command(CommandStep::Rollback)
            }
            Self::MessageReceipt | Self::InterruptReceipt => {
                Boundary::Inbox(InboxStep::Rollback)
            }
        }
    }

    fn crash_point_count(self) -> usize {
        match self {
            Self::MessageIntent | Self::InterruptIntent => 15,
            Self::MessageReceipt | Self::InterruptReceipt => 14,
        }
    }

    fn stable(self, successful: &DeliveryOutput) -> DeliveryOutput {
        match successful {
            DeliveryOutput::Intent(RecordCoordinationCommandOutcome::Applied(metadata)) => {
                DeliveryOutput::Intent(RecordCoordinationCommandOutcome::Duplicate(
                    metadata.clone(),
                ))
            }
            DeliveryOutput::Receipt(PersistRecipientReceiptOutcome::Applied(metadata)) => {
                DeliveryOutput::Receipt(PersistRecipientReceiptOutcome::Duplicate(
                    metadata.clone(),
                ))
            }
            DeliveryOutput::Intent(RecordCoordinationCommandOutcome::Duplicate(_))
            | DeliveryOutput::Receipt(
                PersistRecipientReceiptOutcome::Duplicate(_)
                | PersistRecipientReceiptOutcome::Deferred,
            ) => panic!("{self:?}: successful control had a non-applied outcome"),
        }
    }
}

#[tokio::test]
async fn message_and_interrupt_intent_and_receipt_crash_matrices() -> anyhow::Result<()> {
    for case in CASES {
        run_case(case).await?;
    }
    Ok(())
}

async fn run_case(case: DeliveryCase) -> anyhow::Result<()> {
    let (reference, reference_input) = case.setup().await?;
    let recorder = CrashInjector::recording(NOW_MS + 1);
    let reference_output = case
        .invoke(&reference, &reference_input, &recorder)
        .await
        .map_err(delivery_error)?;
    assert_applied(case, &reference_output);
    let expected_trace = recorder.trace();
    assert_eq!(expected_trace.len(), case.crash_point_count(), "{case:?}");
    assert_trace_shape(case, &expected_trace);
    assert_integrity(&reference).await?;
    reference.close().await;
    drop(reference);

    for (index, point) in expected_trace.iter().copied().enumerate() {
        let (runtime, input) = case.setup().await?;
        let home = runtime.codex_home().to_path_buf();
        let before = snapshot(&runtime).await?;
        runtime.close().await;
        drop(runtime);

        let control_home = unique_temp_dir();
        copy_closed_home(&home, &control_home).await?;
        let control = StateRuntime::init(control_home, "test".to_string()).await?;
        let control_clock = CrashInjector::recording(NOW_MS + 1);
        let expected_output = case
            .invoke(&control, &input, &control_clock)
            .await
            .map_err(delivery_error)?;
        assert_eq!(control_clock.trace(), expected_trace, "{case:?}");
        assert_applied(case, &expected_output);
        let expected_committed = snapshot(&control).await?;
        assert_integrity(&control).await?;
        control.close().await;
        drop(control);

        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let injector = CrashInjector::fail_at(point, NOW_MS + 1);
        let error = case
            .invoke(&runtime, &input, &injector)
            .await
            .expect_err("injected boundary should fail");
        assert_internal(error, case, point);
        if point.boundary == Boundary::Aggregate(AggregateStep::AfterCommit) {
            assert_eq!(injector.trace(), expected_trace, "{case:?} {point:?}");
            drop(runtime);
            let reopened = StateRuntime::init(home, "test".to_string()).await?;
            assert_eq!(snapshot(&reopened).await?, expected_committed, "{case:?}");
            assert_stable(case, &reopened, &input, &expected_output).await?;
            assert_eq!(snapshot(&reopened).await?, expected_committed, "{case:?}");
            assert_integrity(&reopened).await?;
            continue;
        }

        let mut rolled_back = expected_trace[..=index].to_vec();
        rolled_back.push(CrashPoint {
            boundary: case.rollback(),
            occurrence: 1,
        });
        assert_eq!(injector.trace(), rolled_back, "{case:?} {point:?}");
        drop(runtime);
        let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
        assert_eq!(snapshot(&reopened).await?, before, "{case:?} {point:?}");
        let retry_clock = CrashInjector::recording(NOW_MS + 1);
        assert_eq!(
            case.invoke(&reopened, &input, &retry_clock)
                .await
                .map_err(delivery_error)?,
            expected_output,
            "{case:?} {point:?}"
        );
        assert_eq!(retry_clock.trace(), expected_trace, "{case:?} {point:?}");
        let committed = snapshot(&reopened).await?;
        assert_eq!(committed, expected_committed, "{case:?} {point:?}");
        drop(reopened);

        let reopened = StateRuntime::init(home, "test".to_string()).await?;
        assert_stable(case, &reopened, &input, &expected_output).await?;
        assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

async fn assert_stable(
    case: DeliveryCase,
    runtime: &StateRuntime,
    input: &DeliveryInput,
    expected: &DeliveryOutput,
) -> anyhow::Result<()> {
    let replay = CrashInjector::recording(NOW_MS + 1);
    assert_eq!(
        case.invoke(runtime, input, &replay)
            .await
            .map_err(delivery_error)?,
        case.stable(expected),
        "{case:?}"
    );
    Ok(())
}

fn assert_applied(case: DeliveryCase, output: &DeliveryOutput) {
    assert!(
        matches!(
            output,
            DeliveryOutput::Intent(RecordCoordinationCommandOutcome::Applied(_))
                | DeliveryOutput::Receipt(PersistRecipientReceiptOutcome::Applied(_))
        ),
        "{case:?}: {output:?}"
    );
}

fn assert_internal(error: DeliveryError, case: DeliveryCase, point: CrashPoint) {
    assert!(
        matches!(
            error,
            DeliveryError::Intent(CommandWriteError::Internal(_))
                | DeliveryError::Receipt(InboxWriteError::Internal(_))
        ),
        "{case:?} {point:?}: unexpected error class"
    );
}

fn assert_trace_shape(case: DeliveryCase, trace: &[CrashPoint]) {
    let begin = match case {
        DeliveryCase::MessageIntent | DeliveryCase::InterruptIntent => {
            Boundary::Command(CommandStep::TransactionBegin)
        }
        DeliveryCase::MessageReceipt | DeliveryCase::InterruptReceipt => {
            Boundary::Inbox(InboxStep::TransactionBegin)
        }
    };
    assert_eq!(trace.first().map(|point| point.boundary), Some(begin));
    assert_eq!(
        trace.last().map(|point| point.boundary),
        Some(Boundary::Aggregate(AggregateStep::AfterCommit))
    );
    assert!(!trace.iter().any(|point| point.boundary == case.rollback()));
}

fn delivery_error(error: DeliveryError) -> anyhow::Error {
    match error {
        DeliveryError::Intent(error) => anyhow::anyhow!("command delivery matrix: {error}"),
        DeliveryError::Receipt(error) => anyhow::anyhow!("receipt delivery matrix: {error}"),
    }
}

async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}
