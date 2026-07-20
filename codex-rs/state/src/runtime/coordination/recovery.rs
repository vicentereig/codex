#[derive(Debug, thiserror::Error)]
pub(crate) enum RecoveryWriteError {
    #[error(transparent)]
    InvalidInput(#[from] crate::model::coordination_recovery::RecoveryInputError),
    #[error("coordination authority is quarantined")]
    Quarantined,
    #[error("coordination authority epoch or root does not match")]
    EpochMismatch,
    #[error("a recovery identity fingerprint collided with different exact bytes")]
    IdentityCollision,
    #[error("the same legacy identity reduced to different canonical evidence")]
    DivergentReduction,
    #[error("the same terminal provenance recorded a different observation")]
    DivergentObservation,
    #[error("native suppression correlation conflicts with durable state")]
    NativeCorrelationConflict,
    #[error("stored coordination recovery evidence is corrupt")]
    CorruptState,
    #[error("recovery evidence is anchored beyond the committed root revision")]
    AnchorAheadOfRoot,
    #[error("recovery work is temporarily unavailable and retains its identity")]
    Deferred,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryDisposition {
    AssignmentStranded,
    CommandLeaseReclaimed,
    CommandPoisoned,
    CommandPayloadExpired,
    InboxLeaseReclaimed,
    InboxPayloadExpired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryBatch {
    pub dispositions: Vec<RecoveryDisposition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryStep {
    TransactionBegin,
    Rollback,
    AuthorityRead,
    MarkerRead,
    MarkerUpdate,
    MarkerCommit,
    AnchorRead,
    LegacyRead,
    LegacyInsert,
    LegacyUpdate,
    CheckpointRead,
    CheckpointInsert,
    CheckpointUpdate,
    DegradationInsert,
    DegradationOutboxInsert,
    PublicationRead,
    PublicationUpdate,
    RecoveryRead,
    RecoveryUpdate,
    RecoveryBatchMutation,
    BeforeCommit,
    AfterCommit,
}

/// Supplies deterministic storage-boundary failures for recovery crash tests.
/// Production implementations must be side-effect free and always continue.
pub(crate) trait RecoveryFailureInjector: Send + Sync {
    fn after_recovery_step(&self, step: RecoveryStep) -> anyhow::Result<()>;

    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis().max(0)
    }
}

pub(super) struct NoRecoveryFailure;

impl RecoveryFailureInjector for NoRecoveryFailure {
    fn after_recovery_step(&self, _step: RecoveryStep) -> anyhow::Result<()> {
        Ok(())
    }
}
