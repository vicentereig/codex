use sqlx::SqliteConnection;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::aggregate_journal::CoordinationWriteError;
use super::aggregate_journal::NoFailure as NoAggregateFailure;
use super::aggregate_journal::authority;
use super::aggregate_journal::finish;
use super::commands::CommandWriteError;
use super::inbox_receipt::persist_receipt;
use super::inbox_rows::InboxPayloadAccess;
use super::inbox_rows::committed_ack;
use super::inbox_rows::load_inbox_by_receipt;
use crate::StateRuntime;
use crate::model::coordination_inbox::CommittedReceiptAck;
use crate::model::coordination_inbox::InboxInputError;
use crate::model::coordination_inbox::PersistRecipientReceipt;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;

#[derive(Debug, thiserror::Error)]
pub(crate) enum InboxWriteError {
    #[error("coordination authority is quarantined")]
    Quarantined,
    #[error("coordination inbox root is missing")]
    RootMissing,
    #[error("coordination inbox target generation is fenced")]
    GenerationFenced,
    #[error("coordination inbox target turn is fenced")]
    TurnFenced,
    #[error("coordination inbox tuple fingerprint collides")]
    IdempotencyCollision,
    #[error("coordination inbox idempotency content conflicts")]
    IdempotencyConflict,
    #[error("coordination inbox receipt, event, or operation identity conflicts")]
    IdentityConflict,
    #[error("coordination inbox lease or row version is fenced")]
    LeaseFenced,
    #[error("coordination inbox item is not ready")]
    NotReady,
    #[error("coordination inbox payload has expired")]
    Expired,
    #[error("coordination inbox terminal outcome conflicts with the first outcome")]
    TerminalConflict,
    #[error("stored coordination inbox state is corrupt")]
    CorruptStoredInbox,
    #[error(transparent)]
    Input(#[from] InboxInputError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<CoordinationWriteError> for InboxWriteError {
    fn from(error: CoordinationWriteError) -> Self {
        match error {
            CoordinationWriteError::Quarantined => Self::Quarantined,
            CoordinationWriteError::RootMismatch => Self::RootMissing,
            CoordinationWriteError::VersionFenced
            | CoordinationWriteError::RevisionFenced
            | CoordinationWriteError::OwnerFenced
            | CoordinationWriteError::GenerationFenced
            | CoordinationWriteError::AssignmentConflict => Self::GenerationFenced,
            CoordinationWriteError::IdempotencyConflict
            | CoordinationWriteError::DivergentIntent => Self::IdempotencyConflict,
            CoordinationWriteError::IdentityCollision => Self::IdentityConflict,
            CoordinationWriteError::TerminalConflict | CoordinationWriteError::WaitConflict => {
                Self::TerminalConflict
            }
            CoordinationWriteError::CorruptStoredEvent => Self::CorruptStoredInbox,
            CoordinationWriteError::Internal(error) => Self::Internal(error),
        }
    }
}

impl From<CommandWriteError> for InboxWriteError {
    fn from(error: CommandWriteError) -> Self {
        match error {
            CommandWriteError::Quarantined => Self::Quarantined,
            CommandWriteError::RootMissing => Self::RootMissing,
            CommandWriteError::GenerationFenced => Self::GenerationFenced,
            CommandWriteError::IdempotencyCollision => Self::IdempotencyCollision,
            CommandWriteError::IdempotencyConflict => Self::IdempotencyConflict,
            CommandWriteError::IdentityConflict => Self::IdentityConflict,
            CommandWriteError::LeaseFenced => Self::LeaseFenced,
            CommandWriteError::NotReady => Self::NotReady,
            CommandWriteError::Expired => Self::Expired,
            CommandWriteError::CorruptStoredCommand => Self::CorruptStoredInbox,
            CommandWriteError::Input(error) => Self::Internal(anyhow::Error::new(error)),
            CommandWriteError::Internal(error) => Self::Internal(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboxStep {
    DuplicateRead,
    CommandRead,
    TargetFence,
    ReceiptEvent,
    ReceiptInsert,
    ClaimUpdate,
    SelectionInsert,
    SelectionUpdate,
    InboxUpdate,
    MaintenanceUpdate,
}

/// Supplies deterministic failures at inbox-specific transaction boundaries.
///
/// Implementations are test probes only. Production always uses the no-failure
/// implementation and derives no behavior from an injector.
pub(crate) trait InboxFailureInjector: AggregateFailureInjector {
    fn after_inbox_step(&self, step: InboxStep) -> anyhow::Result<()>;
}

pub(super) struct NoInboxFailure;

impl AggregateFailureInjector for NoInboxFailure {
    fn after_step(&self, _step: AggregateStep) -> anyhow::Result<()> {
        Ok(())
    }
}

impl InboxFailureInjector for NoInboxFailure {
    fn after_inbox_step(&self, _step: InboxStep) -> anyhow::Result<()> {
        Ok(())
    }
}

impl StateRuntime {
    pub(crate) async fn persist_coordination_recipient_receipt(
        &self,
        params: PersistRecipientReceipt,
    ) -> Result<PersistRecipientReceiptOutcome, InboxWriteError> {
        self.persist_coordination_recipient_receipt_with(params, &NoInboxFailure)
            .await
    }

    pub(super) async fn persist_coordination_recipient_receipt_with(
        &self,
        params: PersistRecipientReceipt,
        injector: &dyn InboxFailureInjector,
    ) -> Result<PersistRecipientReceiptOutcome, InboxWriteError> {
        let mut connection = self.pool.acquire().await.map_err(internal)?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .map_err(internal)?;
        let result = persist_receipt(&mut connection, params, injector).await;
        finish_inbox(&mut connection, result, injector).await
    }

    pub(crate) async fn coordination_durable_receipt_ack(
        &self,
        receipt_id: codex_coordination::ReceiptId,
    ) -> Result<CommittedReceiptAck, InboxWriteError> {
        let mut connection = self.pool.acquire().await.map_err(internal)?;
        authority(&mut connection, &NoAggregateFailure).await?;
        let stored = load_inbox_by_receipt(
            &mut connection,
            receipt_id,
            InboxPayloadAccess::MetadataOnly,
        )
        .await?
        .ok_or(InboxWriteError::IdentityConflict)?;
        Ok(committed_ack(&stored))
    }
}

pub(super) async fn finish_inbox<T>(
    connection: &mut SqliteConnection,
    result: Result<T, InboxWriteError>,
    injector: &dyn InboxFailureInjector,
) -> Result<T, InboxWriteError> {
    match result {
        Ok(value) => finish(connection, Ok(value), injector)
            .await
            .map_err(InboxWriteError::from),
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

pub(super) fn internal(error: impl Into<anyhow::Error>) -> InboxWriteError {
    InboxWriteError::Internal(error.into())
}
