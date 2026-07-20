use codex_coordination::AssignmentGeneration;
use codex_coordination::CoordinationEvent;

use super::accept_transitions::accept;
use super::accept_transitions::close_reserved;
use super::aggregate_journal::*;
use super::reserve_transition::reserve;
use super::terminal_transition::terminal;
use super::wait_transitions::end_wait;
use super::wait_transitions::start_wait;
use crate::StateRuntime;
use crate::model::coordination::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReserveAssignmentOutcome {
    Reserved {
        generation: AssignmentGeneration,
        event: CoordinationEvent,
    },
    Duplicate {
        generation: AssignmentGeneration,
        event: CoordinationEvent,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AssignmentTransitionOutcome {
    Applied {
        events: Vec<CoordinationEvent>,
    },
    Duplicate {
        events: Vec<CoordinationEvent>,
    },
    Fenced {
        current_generation: AssignmentGeneration,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WaitTransitionOutcome {
    Applied { event: CoordinationEvent },
    Duplicate { event: CoordinationEvent },
}

impl StateRuntime {
    pub(crate) async fn reserve_coordination_assignment(
        &self,
        params: ReserveAssignment,
    ) -> Result<ReserveAssignmentOutcome, CoordinationWriteError> {
        self.reserve_coordination_assignment_with(params, &NoFailure)
            .await
    }

    pub(super) async fn reserve_coordination_assignment_with(
        &self,
        params: ReserveAssignment,
        injector: &dyn AggregateFailureInjector,
    ) -> Result<ReserveAssignmentOutcome, CoordinationWriteError> {
        let mut connection = self.begin_aggregate(injector).await?;
        let result = reserve(&mut connection, params, injector).await;
        finish(connection, result, injector).await
    }

    pub(crate) async fn accept_coordination_assignment(
        &self,
        params: AcceptAssignment,
    ) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
        self.accept_coordination_assignment_with(params, &NoFailure)
            .await
    }

    pub(super) async fn accept_coordination_assignment_with(
        &self,
        params: AcceptAssignment,
        injector: &dyn AggregateFailureInjector,
    ) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
        let mut connection = self.begin_aggregate(injector).await?;
        let result = accept(&mut connection, params, injector).await;
        finish(connection, result, injector).await
    }

    pub(crate) async fn close_reserved_coordination_assignment(
        &self,
        params: CloseReservedAssignment,
    ) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
        self.close_reserved_coordination_assignment_with(params, &NoFailure)
            .await
    }

    pub(super) async fn close_reserved_coordination_assignment_with(
        &self,
        params: CloseReservedAssignment,
        injector: &dyn AggregateFailureInjector,
    ) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
        let mut connection = self.begin_aggregate(injector).await?;
        let result = close_reserved(&mut connection, params, injector).await;
        finish(connection, result, injector).await
    }

    pub(crate) async fn terminal_coordination_assignment(
        &self,
        params: TerminalAssignment,
    ) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
        self.terminal_coordination_assignment_with(params, &NoFailure)
            .await
    }

    pub(super) async fn terminal_coordination_assignment_with(
        &self,
        params: TerminalAssignment,
        injector: &dyn AggregateFailureInjector,
    ) -> Result<AssignmentTransitionOutcome, CoordinationWriteError> {
        let mut connection = self.begin_aggregate(injector).await?;
        let result = terminal(&mut connection, params, injector).await;
        finish(connection, result, injector).await
    }

    pub(crate) async fn start_coordination_wait(
        &self,
        params: StartCoordinationWait,
    ) -> Result<WaitTransitionOutcome, CoordinationWriteError> {
        self.start_coordination_wait_with(params, &NoFailure).await
    }

    pub(super) async fn start_coordination_wait_with(
        &self,
        params: StartCoordinationWait,
        injector: &dyn AggregateFailureInjector,
    ) -> Result<WaitTransitionOutcome, CoordinationWriteError> {
        let mut connection = self.begin_aggregate(injector).await?;
        let result = start_wait(&mut connection, params, injector).await;
        finish(connection, result, injector).await
    }

    pub(crate) async fn end_coordination_wait(
        &self,
        params: EndCoordinationWait,
    ) -> Result<WaitTransitionOutcome, CoordinationWriteError> {
        self.end_coordination_wait_with(params, &NoFailure).await
    }

    pub(super) async fn end_coordination_wait_with(
        &self,
        params: EndCoordinationWait,
        injector: &dyn AggregateFailureInjector,
    ) -> Result<WaitTransitionOutcome, CoordinationWriteError> {
        let mut connection = self.begin_aggregate(injector).await?;
        let result = end_wait(&mut connection, params, injector).await;
        finish(connection, result, injector).await
    }

    async fn begin_aggregate(
        &self,
        injector: &dyn AggregateFailureInjector,
    ) -> Result<sqlx::Transaction<'static, sqlx::Sqlite>, CoordinationWriteError> {
        let transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(internal)?;
        if let Err(error) = injector.after_step(AggregateStep::TransactionBegin) {
            rollback(transaction, injector).await?;
            return Err(internal(error));
        }
        Ok(transaction)
    }
}
