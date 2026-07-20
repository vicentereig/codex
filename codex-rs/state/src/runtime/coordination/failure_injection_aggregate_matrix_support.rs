use std::sync::Arc;

use codex_coordination::AssignmentId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::GenerationCloseReason;

use super::aggregate_failure_support::wait_params;
use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::aggregate_journal::CoordinationWriteError;
use super::aggregate_race_tests::accepted_one_reserved_two;
use super::aggregate_race_tests::terminal_for_generation_one;
use super::aggregate_test_support::*;
use super::aggregates::AssignmentTransitionOutcome;
use super::aggregates::ReserveAssignmentOutcome;
use super::aggregates::WaitTransitionOutcome;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_support::FrozenCoordinationState;
use super::failure_injection_support::FrozenStateInputs;
use super::failure_injection_support::frozen_state;
use crate::StateRuntime;
use crate::model::coordination::AcceptAssignment;
use crate::model::coordination::CloseReservedAssignment;
use crate::model::coordination::EndCoordinationWait;
use crate::model::coordination::ReserveAssignment;
use crate::model::coordination::StartCoordinationWait;
use crate::model::coordination::TerminalAssignment;
use crate::runtime::test_support::unique_temp_dir;

pub(super) const NOW_MS: i64 = 4_000_000_000_000;

#[derive(Clone, Copy, Debug)]
pub(super) enum AggregateCase {
    Reserve,
    Accept,
    CloseReserved,
    Terminal,
    WaitStart,
    WaitEnd,
}

pub(super) const CASES: [AggregateCase; 6] = [
    AggregateCase::Reserve,
    AggregateCase::Accept,
    AggregateCase::CloseReserved,
    AggregateCase::Terminal,
    AggregateCase::WaitStart,
    AggregateCase::WaitEnd,
];

#[derive(Clone)]
pub(super) enum AggregateInput {
    Reserve(ReserveAssignment),
    Accept(AcceptAssignment),
    CloseReserved(CloseReservedAssignment),
    Terminal(TerminalAssignment),
    WaitStart(StartCoordinationWait),
    WaitEnd(EndCoordinationWait),
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum AggregateOutput {
    Reserve(ReserveAssignmentOutcome),
    Assignment(AssignmentTransitionOutcome),
    Wait(WaitTransitionOutcome),
}

impl AggregateCase {
    pub(super) async fn setup(self) -> anyhow::Result<(Arc<StateRuntime>, AggregateInput)> {
        match self {
            Self::Reserve => Ok((
                StateRuntime::init(unique_temp_dir(), "test".to_string()).await?,
                AggregateInput::Reserve(reserve_params()),
            )),
            Self::Accept => {
                let (runtime, _, params) = accepted_one_reserved_two().await?;
                Ok((runtime, AggregateInput::Accept(params)))
            }
            Self::CloseReserved => {
                let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
                runtime
                    .reserve_coordination_assignment(reserve_params())
                    .await?;
                Ok((runtime, AggregateInput::CloseReserved(close_params()?)))
            }
            Self::Terminal => {
                let (runtime, _, _) = accepted_one_reserved_two().await?;
                Ok((
                    runtime,
                    AggregateInput::Terminal(terminal_for_generation_one(3, 2, true)?),
                ))
            }
            Self::WaitStart => {
                let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
                runtime
                    .reserve_coordination_assignment(reserve_params())
                    .await?;
                let (start, _) = wait_params(
                    CoordinationSemanticSlot::WaitStarted,
                    "019f7c6c-1111-7000-8000-000000000734",
                    "019f7c6c-1111-7000-8000-000000000134",
                    1,
                )?;
                Ok((runtime, AggregateInput::WaitStart(start)))
            }
            Self::WaitEnd => {
                let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
                runtime
                    .reserve_coordination_assignment(reserve_params())
                    .await?;
                let (start, end) = wait_params(
                    CoordinationSemanticSlot::WaitStarted,
                    "019f7c6c-1111-7000-8000-000000000734",
                    "019f7c6c-1111-7000-8000-000000000134",
                    1,
                )?;
                runtime.start_coordination_wait(start).await?;
                Ok((runtime, AggregateInput::WaitEnd(end)))
            }
        }
    }

    pub(super) async fn invoke(
        self,
        runtime: &StateRuntime,
        input: &AggregateInput,
        injector: &dyn AggregateFailureInjector,
    ) -> Result<AggregateOutput, CoordinationWriteError> {
        match (self, input) {
            (Self::Reserve, AggregateInput::Reserve(params)) => runtime
                .reserve_coordination_assignment_with(params.clone(), injector)
                .await
                .map(AggregateOutput::Reserve),
            (Self::Accept, AggregateInput::Accept(params)) => runtime
                .accept_coordination_assignment_with(params.clone(), injector)
                .await
                .map(AggregateOutput::Assignment),
            (Self::CloseReserved, AggregateInput::CloseReserved(params)) => runtime
                .close_reserved_coordination_assignment_with(params.clone(), injector)
                .await
                .map(AggregateOutput::Assignment),
            (Self::Terminal, AggregateInput::Terminal(params)) => runtime
                .terminal_coordination_assignment_with(params.clone(), injector)
                .await
                .map(AggregateOutput::Assignment),
            (Self::WaitStart, AggregateInput::WaitStart(params)) => runtime
                .start_coordination_wait_with(params.clone(), injector)
                .await
                .map(AggregateOutput::Wait),
            (Self::WaitEnd, AggregateInput::WaitEnd(params)) => runtime
                .end_coordination_wait_with(params.clone(), injector)
                .await
                .map(AggregateOutput::Wait),
            _ => unreachable!("aggregate case/input mismatch"),
        }
    }

    pub(super) fn expected_trace(self) -> Vec<CrashPoint> {
        use AggregateStep as S;

        let steps: &[AggregateStep] = match self {
            Self::Reserve => &[
                S::TransactionBegin,
                S::AuthorityRead,
                S::IdempotencyRead,
                S::IdempotencyRead,
                S::AggregateRead,
                S::RootCreate,
                S::AggregateRead,
                S::AggregateRead,
                S::RevisionAllocation,
                S::AggregateMutation,
                S::AggregateMutation,
                S::EventInsert,
                S::OutboxInsert,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::Accept => &[
                S::TransactionBegin,
                S::AuthorityRead,
                S::AggregateRead,
                S::AggregateRead,
                S::AggregateRead,
                S::AggregateRead,
                S::AggregateRead,
                S::IdempotencyRead,
                S::IdempotencyRead,
                S::AggregateRead,
                S::AggregateRead,
                S::IdempotencyRead,
                S::IdempotencyRead,
                S::RevisionAllocation,
                S::AggregateMutation,
                S::AggregateMutation,
                S::AggregateMutation,
                S::AggregateMutation,
                S::EventInsert,
                S::OutboxInsert,
                S::EventInsert,
                S::OutboxInsert,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::CloseReserved => &[
                S::TransactionBegin,
                S::AuthorityRead,
                S::AggregateRead,
                S::AggregateRead,
                S::AggregateRead,
                S::AggregateRead,
                S::AggregateRead,
                S::IdempotencyRead,
                S::IdempotencyRead,
                S::AggregateRead,
                S::RevisionAllocation,
                S::AggregateMutation,
                S::EventInsert,
                S::OutboxInsert,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::Terminal => terminal_trace(),
            Self::WaitStart => &[
                S::TransactionBegin,
                S::AuthorityRead,
                S::AggregateRead,
                S::IdempotencyRead,
                S::IdempotencyRead,
                S::AggregateRead,
                S::AggregateRead,
                S::RevisionAllocation,
                S::AggregateMutation,
                S::EventInsert,
                S::OutboxInsert,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::WaitEnd => &[
                S::TransactionBegin,
                S::AuthorityRead,
                S::AggregateRead,
                S::AggregateRead,
                S::AggregateRead,
                S::AggregateRead,
                S::IdempotencyRead,
                S::IdempotencyRead,
                S::AggregateRead,
                S::RevisionAllocation,
                S::AggregateMutation,
                S::EventInsert,
                S::OutboxInsert,
                S::BeforeCommit,
                S::AfterCommit,
            ],
        };
        counted(steps)
    }
}

pub(super) fn assert_success(case: AggregateCase, output: &AggregateOutput) {
    assert!(
        matches!(
            (case, output),
            (
                AggregateCase::Reserve,
                AggregateOutput::Reserve(ReserveAssignmentOutcome::Reserved { .. })
            ) | (
                AggregateCase::Accept | AggregateCase::CloseReserved | AggregateCase::Terminal,
                AggregateOutput::Assignment(AssignmentTransitionOutcome::Applied { .. })
            ) | (
                AggregateCase::WaitStart | AggregateCase::WaitEnd,
                AggregateOutput::Wait(WaitTransitionOutcome::Applied { .. })
            )
        ),
        "{case:?}: {output:?}"
    );
}

pub(super) fn assert_stable_replay(
    case: AggregateCase,
    expected: &AggregateOutput,
    actual: AggregateOutput,
) {
    let matches = match (expected, actual) {
        (
            AggregateOutput::Reserve(ReserveAssignmentOutcome::Reserved { generation, event }),
            AggregateOutput::Reserve(ReserveAssignmentOutcome::Duplicate {
                generation: actual_generation,
                event: actual_event,
            }),
        ) => (*generation, event) == (actual_generation, &actual_event),
        (
            AggregateOutput::Assignment(AssignmentTransitionOutcome::Applied { events }),
            AggregateOutput::Assignment(AssignmentTransitionOutcome::Duplicate { events: actual }),
        ) => events == &actual,
        (
            AggregateOutput::Wait(WaitTransitionOutcome::Applied { event }),
            AggregateOutput::Wait(WaitTransitionOutcome::Duplicate { event: actual }),
        ) => event == &actual,
        _ => false,
    };
    assert!(matches, "{case:?}: replay diverged from {expected:?}");
}

pub(super) async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}

fn counted(steps: &[AggregateStep]) -> Vec<CrashPoint> {
    steps
        .iter()
        .enumerate()
        .map(|(index, step)| CrashPoint {
            boundary: Boundary::Aggregate(*step),
            occurrence: steps[..index]
                .iter()
                .filter(|candidate| *candidate == step)
                .count()
                + 1,
        })
        .collect()
}

fn terminal_trace() -> &'static [AggregateStep] {
    use AggregateStep as S;
    &[
        S::TransactionBegin,
        S::AuthorityRead,
        S::AggregateRead,
        S::AggregateRead,
        S::IdempotencyRead,
        S::IdempotencyRead,
        S::AggregateRead,
        S::AggregateRead,
        S::AggregateRead,
        S::AggregateRead,
        S::AggregateRead,
        S::AggregateRead,
        S::AggregateRead,
        S::IdempotencyRead,
        S::IdempotencyRead,
        S::RevisionAllocation,
        S::AggregateMutation,
        S::AggregateMutation,
        S::AggregateMutation,
        S::AggregateMutation,
        S::AggregateMutation,
        S::EventInsert,
        S::OutboxInsert,
        S::EventInsert,
        S::OutboxInsert,
        S::BeforeCommit,
        S::AfterCommit,
    ]
}

fn close_params() -> anyhow::Result<CloseReservedAssignment> {
    Ok(CloseReservedAssignment {
        context: context(
            CoordinationSemanticSlot::AssignmentGenerationClosed,
            "019f7c6c-1111-7000-8000-000000000731",
            "019f7c6c-1111-7000-8000-000000000131",
            false,
            1,
            Vec::new(),
        ),
        assignment_id: AssignmentId::parse(ASSIGNMENT)?,
        generation: generation(1),
        reason: GenerationCloseReason::AbandonedBeforeAcceptance,
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 0,
    })
}
