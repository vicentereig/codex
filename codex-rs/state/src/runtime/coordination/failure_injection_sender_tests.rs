use std::path::PathBuf;
use std::sync::Arc;

use codex_coordination::CoordinationFailureCode;
use pretty_assertions::assert_eq;

use super::aggregate_journal::AggregateStep;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::commands::CommandWriteError;
use super::commands::NoCommandFailure;
use super::commands_tests::assignment_command;
use super::failure_injection_support::*;
use super::failure_injection_tests::receipt_params_for_matrix;
use super::inbox_test_support::RECEIPT_ONE;
use crate::StateRuntime;
use crate::model::coordination_commands::*;
use crate::model::coordination_inbox::CommittedReceiptAck;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::runtime::test_support::unique_temp_dir;

const NOW_MS: i64 = 1_753_000_000_000;
const LEASE_MS: i64 = 100;

#[derive(Clone, Copy, Debug)]
enum SenderCase {
    Claim,
    BeginAttempt,
    ResolveRetry,
    ResolvePoisoned,
    ResolveSucceeded,
    ReclaimLease,
    PurgePayload,
    ClaimAtExpiry,
}

const CASES: [SenderCase; 8] = [
    SenderCase::Claim,
    SenderCase::BeginAttempt,
    SenderCase::ResolveRetry,
    SenderCase::ResolvePoisoned,
    SenderCase::ResolveSucceeded,
    SenderCase::ReclaimLease,
    SenderCase::PurgePayload,
    SenderCase::ClaimAtExpiry,
];

#[derive(Clone)]
struct SenderInput {
    pending: CoordinationCommandMetadata,
    lease: Option<CommandLeaseToken>,
    attempt: Option<BegunCommandAttempt>,
    ack: Option<CommittedReceiptAck>,
}

#[derive(Debug, Eq, PartialEq)]
enum SenderOutput {
    Claim(ClaimCoordinationCommandOutcome),
    Begin(BegunCommandAttempt),
    Resolve(ResolveCommandAttemptOutcome),
    Count(u64),
}

#[derive(Debug, Eq, PartialEq)]
enum SenderSettled {
    Output(Box<SenderOutput>),
    LeaseFenced,
}

impl SenderCase {
    async fn setup(self, home: PathBuf) -> anyhow::Result<(Arc<StateRuntime>, SenderInput)> {
        let runtime = StateRuntime::init(home, "test".to_string()).await?;
        let setup_clock = CrashInjector::recording(NOW_MS);
        let RecordCoordinationCommandOutcome::Applied(pending) = runtime
            .record_coordination_command_intent_with(assignment_command(), &setup_clock)
            .await?
        else {
            anyhow::bail!("{self:?}: setup command was not applied");
        };
        let needs_lease = matches!(
            self,
            Self::BeginAttempt
                | Self::ResolveRetry
                | Self::ResolvePoisoned
                | Self::ResolveSucceeded
                | Self::ReclaimLease
        );
        let lease = if needs_lease {
            let ClaimCoordinationCommandOutcome::Claimed(claimed) = runtime
                .claim_coordination_command(
                    pending.operation_id,
                    /*expected_version*/ 0,
                    /*expected_lease_epoch*/ 0,
                    /*now_ms*/ pending.retry_after_ms,
                    /*requested_lease_deadline_ms*/ pending.retry_after_ms + LEASE_MS,
                )
                .await?
            else {
                anyhow::bail!("{self:?}: setup command was not claimed");
            };
            Some(claimed.lease)
        } else {
            None
        };
        let needs_attempt = matches!(
            self,
            Self::ResolveRetry | Self::ResolvePoisoned | Self::ResolveSucceeded
        );
        let attempt = if needs_attempt {
            Some(
                runtime
                    .begin_coordination_command_attempt(
                        lease.clone().expect("setup lease"),
                        /*now_ms*/ pending.retry_after_ms + 1,
                    )
                    .await?,
            )
        } else {
            None
        };
        let ack = if matches!(self, Self::ResolveSucceeded) {
            let receipt_clock = CrashInjector::recording(NOW_MS + 1);
            assert!(matches!(
                runtime
                    .persist_coordination_recipient_receipt_with(
                        receipt_params_for_matrix(),
                        &receipt_clock,
                    )
                    .await?,
                PersistRecipientReceiptOutcome::Applied(_)
            ));
            Some(
                runtime
                    .coordination_durable_receipt_ack(codex_coordination::ReceiptId::parse(
                        RECEIPT_ONE,
                    )?)
                    .await?,
            )
        } else {
            None
        };
        Ok((
            runtime,
            SenderInput {
                pending,
                lease,
                attempt,
                ack,
            },
        ))
    }

    async fn invoke(
        self,
        runtime: &StateRuntime,
        input: &SenderInput,
        injector: &dyn CommandFailureInjector,
    ) -> Result<SenderOutput, CommandWriteError> {
        let now_ms = input.pending.retry_after_ms;
        match self {
            Self::Claim => runtime
                .claim_coordination_command_with(
                    input.pending.operation_id,
                    /*expected_version*/ 0,
                    /*expected_lease_epoch*/ 0,
                    now_ms,
                    /*requested_lease_deadline_ms*/ now_ms + LEASE_MS,
                    injector,
                )
                .await
                .map(SenderOutput::Claim),
            Self::BeginAttempt => runtime
                .begin_coordination_command_attempt_with(
                    input.lease.clone().expect("begin lease"),
                    /*now_ms*/ now_ms + 1,
                    injector,
                )
                .await
                .map(SenderOutput::Begin),
            Self::ResolveRetry | Self::ResolvePoisoned | Self::ResolveSucceeded => {
                let resolution = match self {
                    Self::ResolveRetry => CommandAttemptResolution::RetryAt {
                        retry_at_ms: now_ms + 50,
                        code: CoordinationFailureCode::StateUnavailable,
                    },
                    Self::ResolvePoisoned => CommandAttemptResolution::Poisoned {
                        code: CoordinationFailureCode::RetryExhausted,
                    },
                    Self::ResolveSucceeded => CommandAttemptResolution::Succeeded {
                        ack: input.ack.clone().expect("success ack"),
                    },
                    Self::Claim
                    | Self::BeginAttempt
                    | Self::ReclaimLease
                    | Self::PurgePayload
                    | Self::ClaimAtExpiry => unreachable!("non-resolution case"),
                };
                runtime
                    .resolve_coordination_command_attempt_with(
                        input.attempt.clone().expect("begun attempt"),
                        resolution,
                        /*now_ms*/ now_ms + 2,
                        injector,
                    )
                    .await
                    .map(SenderOutput::Resolve)
            }
            Self::ReclaimLease => runtime
                .reclaim_expired_coordination_command_leases_with(
                    /*now_ms*/ now_ms + LEASE_MS,
                    /*limit*/ 1,
                    injector,
                )
                .await
                .map(SenderOutput::Count),
            Self::PurgePayload => runtime
                .expire_coordination_command_payloads_with(
                    /*now_ms*/ input.pending.expires_at_ms,
                    /*limit*/ 1,
                    injector,
                )
                .await
                .map(SenderOutput::Count),
            Self::ClaimAtExpiry => runtime
                .claim_coordination_command_with(
                    input.pending.operation_id,
                    /*expected_version*/ 0,
                    /*expected_lease_epoch*/ 0,
                    /*now_ms*/ input.pending.expires_at_ms,
                    /*requested_lease_deadline_ms*/ input.pending.expires_at_ms + 1,
                    injector,
                )
                .await
                .map(SenderOutput::Claim),
        }
    }

    fn expected_trace(self) -> Vec<CrashPoint> {
        let transaction = Boundary::Command(CommandStep::TransactionBegin);
        let authority = Boundary::Aggregate(AggregateStep::AuthorityRead);
        let lease = Boundary::Command(CommandStep::LeaseRead);
        let before = Boundary::Aggregate(AggregateStep::BeforeCommit);
        let after = Boundary::Aggregate(AggregateStep::AfterCommit);
        let middle: &[Boundary] = match self {
            Self::Claim => &[
                lease,
                lease,
                Boundary::Command(CommandStep::ClaimUpdate),
                lease,
            ],
            Self::BeginAttempt => &[
                lease,
                lease,
                Boundary::Command(CommandStep::AttemptUpdate),
                lease,
            ],
            Self::ResolveRetry | Self::ResolvePoisoned => &[
                lease,
                Boundary::Command(CommandStep::ResolutionUpdate),
                lease,
            ],
            Self::ResolveSucceeded => &[
                lease,
                lease,
                Boundary::Command(CommandStep::ResolutionUpdate),
                lease,
            ],
            Self::ReclaimLease => &[Boundary::Command(CommandStep::ReclaimUpdate)],
            Self::PurgePayload => &[Boundary::Command(CommandStep::PayloadPurgeUpdate)],
            Self::ClaimAtExpiry => &[lease, Boundary::Command(CommandStep::PayloadPurgeUpdate)],
        };
        let boundaries = [transaction, authority]
            .into_iter()
            .chain(middle.iter().copied())
            .chain([before, after])
            .collect::<Vec<_>>();
        counted(&boundaries)
    }

    fn expected_stable(self) -> SenderSettled {
        let output = match self {
            Self::Claim => SenderOutput::Claim(ClaimCoordinationCommandOutcome::NotReady),
            Self::BeginAttempt => return SenderSettled::LeaseFenced,
            Self::ResolveRetry => SenderOutput::Resolve(ResolveCommandAttemptOutcome::Fenced),
            Self::ResolvePoisoned => SenderOutput::Resolve(ResolveCommandAttemptOutcome::Terminal(
                CommandLifecycle::Poisoned,
            )),
            Self::ResolveSucceeded => SenderOutput::Resolve(
                ResolveCommandAttemptOutcome::Terminal(CommandLifecycle::Succeeded),
            ),
            Self::ReclaimLease | Self::PurgePayload => SenderOutput::Count(0),
            Self::ClaimAtExpiry => SenderOutput::Claim(ClaimCoordinationCommandOutcome::Expired),
        };
        SenderSettled::Output(Box::new(output))
    }
}

#[tokio::test]
async fn assignment_sender_crash_matrix_reopens_and_converges() -> anyhow::Result<()> {
    for case in CASES {
        run_case(case).await?;
    }
    Ok(())
}

async fn run_case(case: SenderCase) -> anyhow::Result<()> {
    let (reference, reference_input) = case.setup(unique_temp_dir()).await?;
    let recorder = CrashInjector::recording(NOW_MS);
    let recorded_output = case.invoke(&reference, &reference_input, &recorder).await?;
    assert_success(case, &recorded_output);
    let expected_trace = case.expected_trace();
    assert_eq!(recorder.trace(), expected_trace, "{case:?}");
    assert_integrity(&reference).await?;
    reference.close().await;
    drop(reference);

    for (index, point) in expected_trace.iter().copied().enumerate() {
        let home = unique_temp_dir();
        let (runtime, input) = case.setup(home.clone()).await?;
        runtime.close().await;
        drop(runtime);
        let control_home = unique_temp_dir();
        copy_home(&home, &control_home).await?;
        let control = StateRuntime::init(control_home, "test".to_string()).await?;
        let expected_output = case.invoke(&control, &input, &NoCommandFailure).await?;
        assert_success(case, &expected_output);
        let expected_committed = snapshot(&control).await?;
        assert_ack_unchanged(&control, input.ack.as_ref()).await?;
        assert_integrity(&control).await?;
        drop(control);

        let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
        let before = snapshot(&runtime).await?;
        let injector = CrashInjector::fail_at(point, NOW_MS);
        assert!(
            matches!(
                case.invoke(&runtime, &input, &injector).await,
                Err(CommandWriteError::Internal(_))
            ),
            "{case:?} {point:?}"
        );
        if response_loss(point) {
            assert_eq!(injector.trace(), expected_trace, "{case:?} {point:?}");
            let committed = snapshot(&runtime).await?;
            assert_eq!(committed, expected_committed, "{case:?} {point:?}");
            drop(runtime);
            let reopened = StateRuntime::init(home, "test".to_string()).await?;
            assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
            assert_ack_unchanged(&reopened, input.ack.as_ref()).await?;
            assert_stable(case, &reopened, &input).await?;
            assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
            assert_integrity(&reopened).await?;
            continue;
        }

        let mut rolled_back = expected_trace[..=index].to_vec();
        rolled_back.push(CrashPoint {
            boundary: Boundary::Command(CommandStep::Rollback),
            occurrence: 1,
        });
        assert_eq!(injector.trace(), rolled_back, "{case:?} {point:?}");
        drop(runtime);
        let reopened = StateRuntime::init(home.clone(), "test".to_string()).await?;
        assert_eq!(snapshot(&reopened).await?, before, "{case:?} {point:?}");
        assert_ack_unchanged(&reopened, input.ack.as_ref()).await?;
        assert_eq!(
            case.invoke(&reopened, &input, &NoCommandFailure).await?,
            expected_output,
            "{case:?} {point:?}"
        );
        let committed = snapshot(&reopened).await?;
        assert_eq!(committed, expected_committed, "{case:?} {point:?}");
        drop(reopened);
        let reopened = StateRuntime::init(home, "test".to_string()).await?;
        assert_stable(case, &reopened, &input).await?;
        assert_eq!(snapshot(&reopened).await?, committed, "{case:?} {point:?}");
        assert_ack_unchanged(&reopened, input.ack.as_ref()).await?;
        assert_integrity(&reopened).await?;
    }
    Ok(())
}

fn counted(boundaries: &[Boundary]) -> Vec<CrashPoint> {
    boundaries
        .iter()
        .enumerate()
        .map(|(index, boundary)| CrashPoint {
            boundary: *boundary,
            occurrence: boundaries[..index]
                .iter()
                .filter(|candidate| *candidate == boundary)
                .count()
                + 1,
        })
        .collect()
}

fn response_loss(point: CrashPoint) -> bool {
    point.boundary == Boundary::Aggregate(AggregateStep::AfterCommit)
}

fn assert_success(case: SenderCase, output: &SenderOutput) {
    assert!(
        matches!(
            (case, output),
            (
                SenderCase::Claim,
                SenderOutput::Claim(ClaimCoordinationCommandOutcome::Claimed(_))
            ) | (SenderCase::BeginAttempt, SenderOutput::Begin(_))
                | (
                    SenderCase::ResolveRetry
                        | SenderCase::ResolvePoisoned
                        | SenderCase::ResolveSucceeded,
                    SenderOutput::Resolve(ResolveCommandAttemptOutcome::Applied(_))
                )
                | (
                    SenderCase::ReclaimLease | SenderCase::PurgePayload,
                    SenderOutput::Count(1)
                )
                | (
                    SenderCase::ClaimAtExpiry,
                    SenderOutput::Claim(ClaimCoordinationCommandOutcome::Expired)
                )
        ),
        "{case:?}: {output:?}"
    );
}

async fn assert_stable(
    case: SenderCase,
    runtime: &StateRuntime,
    input: &SenderInput,
) -> anyhow::Result<()> {
    let actual = match case.invoke(runtime, input, &NoCommandFailure).await {
        Ok(output) => SenderSettled::Output(Box::new(output)),
        Err(CommandWriteError::LeaseFenced) => SenderSettled::LeaseFenced,
        Err(error) => anyhow::bail!("{case:?}: unexpected stable error: {error:?}"),
    };
    assert_eq!(actual, case.expected_stable(), "{case:?}");
    Ok(())
}

async fn assert_ack_unchanged(
    runtime: &StateRuntime,
    expected: Option<&CommittedReceiptAck>,
) -> anyhow::Result<()> {
    if let Some(expected) = expected {
        assert_eq!(
            runtime
                .coordination_durable_receipt_ack(expected.receipt_id)
                .await?,
            *expected
        );
    }
    Ok(())
}

async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}

async fn copy_home(source: &std::path::Path, target: &std::path::Path) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(target).await?;
    let mut entries = tokio::fs::read_dir(source).await?;
    let mut copied = 0;
    while let Some(entry) = entries.next_entry().await? {
        if copied == 8 || !entry.file_type().await?.is_file() {
            anyhow::bail!("unexpected non-file state entry");
        }
        tokio::fs::copy(entry.path(), target.join(entry.file_name())).await?;
        copied += 1;
    }
    Ok(())
}
