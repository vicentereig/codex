use codex_coordination::AssignmentEvidence;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationOrder;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::CoordinationTarget;
use codex_coordination::Evidence;
use codex_coordination::IdempotencyKey;
use sqlx::SqliteConnection;

use super::aggregate_journal::*;
use super::aggregates::ReserveAssignmentOutcome;
use super::command_event::expected_event_kind;
use super::command_identity::preflight_identity;
use super::command_identity::validate_stored_tuple;
use super::command_rows::*;
use super::command_transaction::begin_command;
use super::command_transaction::finish_command;
use super::reserve_transition::reserve;
use crate::StateRuntime;
use crate::model::coordination::NativeEventContext;
use crate::model::coordination_commands::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommandWriteError {
    #[error("coordination authority is quarantined")]
    Quarantined,
    #[error("coordination command root is missing")]
    RootMissing,
    #[error("coordination command target generation is fenced")]
    GenerationFenced,
    #[error("coordination command idempotency fingerprint collides")]
    IdempotencyCollision,
    #[error("coordination command idempotency content conflicts")]
    IdempotencyConflict,
    #[error("coordination command operation or event identity conflicts")]
    IdentityConflict,
    #[error("coordination command lease or row version is fenced")]
    LeaseFenced,
    #[error("coordination command is not ready")]
    NotReady,
    #[error("coordination command payload has expired")]
    Expired,
    #[error("stored coordination command is corrupt")]
    CorruptStoredCommand,
    #[error(transparent)]
    Input(#[from] CommandInputError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<CoordinationWriteError> for CommandWriteError {
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
            | CoordinationWriteError::DivergentIntent
            | CoordinationWriteError::TerminalConflict
            | CoordinationWriteError::WaitConflict => Self::IdempotencyConflict,
            CoordinationWriteError::IdentityCollision => Self::IdentityConflict,
            CoordinationWriteError::CorruptStoredEvent => Self::CorruptStoredCommand,
            CoordinationWriteError::Internal(error) => Self::Internal(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandStep {
    TransactionBegin,
    Rollback,
    IdentityRead,
    TargetCapture,
    CommandInsert,
    LeaseRead,
    ClaimUpdate,
    AttemptUpdate,
    ResolutionUpdate,
    ReclaimUpdate,
    PayloadPurgeUpdate,
}

/// Injects deterministic failures at command-specific SQL boundaries.
///
/// Implementations are test-only transaction probes; production uses the
/// no-failure implementation and must never derive behavior from an injector.
pub(crate) trait CommandFailureInjector: AggregateFailureInjector {
    fn after_command_step(&self, step: CommandStep) -> anyhow::Result<()>;
}

pub(super) struct NoCommandFailure;

impl AggregateFailureInjector for NoCommandFailure {
    fn after_step(&self, _step: AggregateStep) -> anyhow::Result<()> {
        Ok(())
    }
}

impl CommandFailureInjector for NoCommandFailure {
    fn after_command_step(&self, _step: CommandStep) -> anyhow::Result<()> {
        Ok(())
    }
}

impl StateRuntime {
    pub(crate) async fn record_coordination_command_intent(
        &self,
        params: RecordCoordinationCommand,
    ) -> Result<RecordCoordinationCommandOutcome, CommandWriteError> {
        self.record_coordination_command_intent_with(params, &NoCommandFailure)
            .await
    }

    pub(super) async fn record_coordination_command_intent_with(
        &self,
        params: RecordCoordinationCommand,
        injector: &dyn CommandFailureInjector,
    ) -> Result<RecordCoordinationCommandOutcome, CommandWriteError> {
        let mut connection = begin_command(self, injector).await?;
        let result = record(&mut connection, params, injector).await;
        finish_command(connection, result, injector).await
    }
}

async fn record(
    connection: &mut SqliteConnection,
    params: RecordCoordinationCommand,
    injector: &dyn CommandFailureInjector,
) -> Result<RecordCoordinationCommandOutcome, CommandWriteError> {
    validate(&params)?;
    authority(connection, injector).await?;
    let context = params.intent.context();
    let kind = CommandKind::from_intent(&params.intent);
    let slot = semantic_slot(kind);
    let key = IdempotencyKey::new(
        context.root_thread_id,
        context.actor.thread_id,
        known_turn(&context.actor)?.clone(),
        params.intent.operation_id(),
        slot,
    );
    if let Some(stored) = load_command(
        connection,
        &context.root_thread_id,
        key.fingerprint().as_slice(),
    )
    .await?
    {
        injector
            .after_command_step(CommandStep::IdentityRead)
            .map_err(internal_command)?;
        validate_stored_tuple(&stored.tuple_bytes, &stored.tuple_fingerprint, &key)?;
        let CoordinationOrder::Native {
            state_epoch,
            revision,
        } = &stored.event.envelope().order
        else {
            return Err(CommandWriteError::CorruptStoredCommand);
        };
        compare_event(
            context,
            &context.primary,
            *state_epoch,
            *revision,
            expected_event_kind(&params.intent, stored.metadata.target.generation),
            &[],
            &stored.event,
        )?;
        validate_duplicate(&stored, &params, &key, &stored.event)?;
        return Ok(RecordCoordinationCommandOutcome::Duplicate(stored.metadata));
    }
    preflight_identity(connection, &params, &key, injector).await?;
    let (duplicate_event, event) = semantic_event(connection, &params.intent, injector).await?;
    if duplicate_event {
        return Err(CommandWriteError::CorruptStoredCommand);
    }
    let target = capture_target(connection, &params.intent, &event, injector).await?;
    let ciphertext_fingerprint = sha256(params.ciphertext.as_bytes());
    let command_fingerprint = command_fingerprint(
        &key,
        &event,
        kind,
        &target,
        params.ciphertext.encoded_len(),
        &ciphertext_fingerprint,
    );
    insert_command(
        connection,
        &params,
        &key,
        &event,
        kind,
        &target,
        ciphertext_fingerprint,
        command_fingerprint,
        injector,
    )
    .await?;
    let stored = load_command(
        connection,
        &context.root_thread_id,
        key.fingerprint().as_slice(),
    )
    .await?
    .ok_or(CommandWriteError::CorruptStoredCommand)?;
    Ok(RecordCoordinationCommandOutcome::Applied(stored.metadata))
}

fn validate(params: &RecordCoordinationCommand) -> Result<(), CommandWriteError> {
    let context = params.intent.context();
    validate_identities(context)?;
    if !context.secondary.items().is_empty()
        || context.primary.operation_id != params.intent.operation_id()
        || params.intent.encoded_payload_bytes() != params.ciphertext.encoded_len()
    {
        return Err(CommandWriteError::IdempotencyConflict);
    }
    Ok(())
}

async fn semantic_event(
    connection: &mut SqliteConnection,
    intent: &CoordinationCommandIntent,
    injector: &dyn CommandFailureInjector,
) -> Result<(bool, CoordinationEvent), CommandWriteError> {
    match intent {
        CoordinationCommandIntent::Assignment { reservation } => {
            match reserve(connection, reservation.clone(), injector).await? {
                ReserveAssignmentOutcome::Reserved { event, .. } => Ok((false, event)),
                ReserveAssignmentOutcome::Duplicate { event, .. } => Ok((true, event)),
            }
        }
        CoordinationCommandIntent::Message {
            context,
            operation_id,
            target,
            content,
            encoded_payload_bytes,
        } => {
            let kind = CoordinationEventKind::MessageSubmissionRecorded {
                operation_id: *operation_id,
                target: target.clone(),
                content: content.clone(),
                encoded_payload_bytes: *encoded_payload_bytes,
            };
            journal_command_event(connection, context, kind, injector).await
        }
        CoordinationCommandIntent::Interrupt {
            context,
            operation_id,
            target,
        } => {
            let kind = CoordinationEventKind::InterruptRequested {
                operation_id: *operation_id,
                target: target.clone(),
            };
            journal_command_event(connection, context, kind, injector).await
        }
    }
}

async fn journal_command_event(
    connection: &mut SqliteConnection,
    context: &NativeEventContext,
    kind: CoordinationEventKind,
    injector: &dyn CommandFailureInjector,
) -> Result<(bool, CoordinationEvent), CommandWriteError> {
    let epoch = authority(connection, injector).await?;
    if let Some(stored) = load_idempotent(
        connection,
        context,
        &context.primary,
        kind.semantic_slot(),
        injector,
    )
    .await?
    {
        compare_event(
            context,
            &context.primary,
            epoch,
            stored.revision,
            kind,
            &[],
            &stored.event,
        )?;
        return Ok((true, stored.event));
    }
    ensure_root(
        connection,
        &context.root_thread_id,
        epoch,
        /*create*/ false,
        injector,
    )
    .await?;
    fence_root_revision(connection, context, injector).await?;
    let revision = allocate(connection, &context.root_thread_id, 1, injector).await?[0];
    let event = make_event(context, &context.primary, epoch, revision, kind, &[])?;
    journal(connection, context, std::slice::from_ref(&event), injector).await?;
    Ok((false, event))
}

async fn capture_target(
    connection: &mut SqliteConnection,
    intent: &CoordinationCommandIntent,
    event: &CoordinationEvent,
    injector: &dyn CommandFailureInjector,
) -> Result<CapturedCommandTarget, CommandWriteError> {
    let target = event_target(event)?;
    let AssignmentEvidence::Known {
        assignment_id,
        generation,
    } = target.assignment
    else {
        return Err(CommandWriteError::GenerationFenced);
    };
    if matches!(intent, CoordinationCommandIntent::Assignment { .. }) {
        return Ok(CapturedCommandTarget {
            target_thread_id: target.principal.thread_id,
            assignment_id,
            generation,
            turn_id: None,
            captured_head_generation: None,
            captured_turn_set: None,
        });
    }
    let turn_id = match &target.principal.turn_id {
        Evidence::Known { value } => value.clone(),
        Evidence::Unavailable { .. } | Evidence::NotApplicable => {
            return Err(CommandWriteError::GenerationFenced);
        }
    };
    let valid: Option<i64> = sqlx::query_scalar(
        "SELECT h.accepted_generation FROM coordination_assignment_heads h \
         JOIN coordination_assignment_generations g USING (assignment_id) \
         JOIN coordination_turn_bindings b USING (assignment_id,generation) \
         WHERE h.assignment_id=? AND h.root_thread_id=? AND h.child_thread_id=? \
         AND h.accepted_generation=? AND g.generation=? AND g.lifecycle='accepted' \
         AND b.root_thread_id=h.root_thread_id AND b.turn_id=?",
    )
    .bind(assignment_id.to_string())
    .bind(intent.context().root_thread_id.to_string())
    .bind(target.principal.thread_id.to_string())
    .bind(generation.get() as i64)
    .bind(generation.get() as i64)
    .bind(turn_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal_command)?;
    injector
        .after_command_step(CommandStep::TargetCapture)
        .map_err(internal_command)?;
    if valid != Some(generation.get() as i64) {
        return Err(CommandWriteError::GenerationFenced);
    }
    let captured_turn_set = if matches!(intent, CoordinationCommandIntent::Interrupt { .. }) {
        let rows: Vec<i64> = sqlx::query_scalar(
            "SELECT g.generation FROM coordination_turn_bindings b \
             JOIN coordination_assignment_generations g USING (assignment_id,generation) \
             WHERE b.root_thread_id=? AND b.turn_id=? AND b.assignment_id=? \
             AND g.accepted_event_id IS NOT NULL ORDER BY g.generation",
        )
        .bind(intent.context().root_thread_id.to_string())
        .bind(turn_id.as_str())
        .bind(assignment_id.to_string())
        .fetch_all(&mut *connection)
        .await
        .map_err(internal_command)?;
        Some(CapturedGenerationSet::new(
            rows.into_iter()
                .map(super::aggregate_journal::generation)
                .collect::<Result<Vec<_>, _>>()?,
        )?)
    } else {
        None
    };
    Ok(CapturedCommandTarget {
        target_thread_id: target.principal.thread_id,
        assignment_id,
        generation,
        turn_id: Some(turn_id),
        captured_head_generation: Some(generation),
        captured_turn_set,
    })
}

#[allow(clippy::too_many_arguments)]
async fn insert_command(
    connection: &mut SqliteConnection,
    params: &RecordCoordinationCommand,
    key: &IdempotencyKey,
    event: &CoordinationEvent,
    kind: CommandKind,
    target: &CapturedCommandTarget,
    ciphertext_fingerprint: [u8; 32],
    command_fingerprint: [u8; 32],
    injector: &dyn CommandFailureInjector,
) -> Result<(), CommandWriteError> {
    let now = now_ms(injector);
    let expires = now
        .checked_add(MAX_INTENT_TTL_MS)
        .ok_or_else(|| CommandWriteError::Internal(anyhow::anyhow!("command expiry overflow")))?;
    let actor_turn = known_turn(&params.intent.context().actor)?;
    let set_bytes = target
        .captured_turn_set
        .as_ref()
        .map(CapturedGenerationSet::canonical_bytes);
    let set_fingerprint = set_bytes.map(sha256);
    sqlx::query("INSERT INTO coordination_commands (operation_id,root_thread_id,intent_event_id,sender_thread_id,sender_turn_id,operation_kind,target_thread_id,target_assignment_id,target_generation,target_turn_id,captured_head_generation,captured_turn_set_bytes,captured_turn_set_fingerprint,idempotency_tuple_bytes,idempotency_tuple_fingerprint,command_fingerprint,encoded_payload_bytes,ciphertext,ciphertext_fingerprint,lifecycle,version,claim_count,attempt_count,attempted_lease_epoch,lease_epoch,retry_after_ms,lease_expires_at_ms,failure_code,intent_at_ms,terminal_at_ms,expires_at_ms,purged_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',0,0,0,NULL,0,?,NULL,NULL,?,NULL,?,NULL,?)")
        .bind(params.intent.operation_id().to_string()).bind(params.intent.context().root_thread_id.to_string()).bind(event.envelope().event_id.to_string())
        .bind(params.intent.context().actor.thread_id.to_string()).bind(actor_turn.as_str()).bind(kind.as_sql()).bind(target.target_thread_id.to_string())
        .bind(target.assignment_id.to_string()).bind(target.generation.get() as i64).bind(target.turn_id.as_ref().map(BoundedId::as_str))
        .bind(target.captured_head_generation.map(|generation| generation.get() as i64)).bind(set_bytes).bind(set_fingerprint.as_ref().map(<[u8; 32]>::as_slice))
        .bind(key.tuple_bytes()).bind(key.fingerprint().as_slice()).bind(command_fingerprint.as_slice()).bind(params.ciphertext.encoded_len() as i64)
        .bind(params.ciphertext.as_bytes()).bind(ciphertext_fingerprint.as_slice()).bind(now).bind(now).bind(expires).bind(now)
        .execute(&mut *connection).await.map_err(internal_command)?;
    injector
        .after_command_step(CommandStep::CommandInsert)
        .map_err(internal_command)
}

fn validate_duplicate(
    stored: &StoredCommand,
    params: &RecordCoordinationCommand,
    key: &IdempotencyKey,
    event: &CoordinationEvent,
) -> Result<(), CommandWriteError> {
    validate_stored_tuple(&stored.tuple_bytes, &stored.tuple_fingerprint, key)?;
    if stored.metadata.operation_id != params.intent.operation_id()
        || stored.metadata.intent_event_id != event.envelope().event_id
    {
        return Err(CommandWriteError::IdentityConflict);
    }
    let ciphertext_fingerprint = sha256(params.ciphertext.as_bytes());
    let expected = command_fingerprint(
        key,
        event,
        CommandKind::from_intent(&params.intent),
        &stored.metadata.target,
        params.ciphertext.encoded_len(),
        &ciphertext_fingerprint,
    );
    if stored.command_fingerprint.as_slice() != expected
        || stored.ciphertext_fingerprint.as_slice() != ciphertext_fingerprint
    {
        return Err(CommandWriteError::IdempotencyConflict);
    }
    Ok(())
}

fn event_target(event: &CoordinationEvent) -> Result<CoordinationTarget, CommandWriteError> {
    match event.kind() {
        CoordinationEventKind::AssignmentRequested { target, .. }
        | CoordinationEventKind::MessageSubmissionRecorded { target, .. }
        | CoordinationEventKind::InterruptRequested { target, .. } => Ok(target.clone()),
        _ => Err(CommandWriteError::IdempotencyConflict),
    }
}

fn semantic_slot(kind: CommandKind) -> CoordinationSemanticSlot {
    match kind {
        CommandKind::AssignmentSpawn | CommandKind::AssignmentFollowup => {
            CoordinationSemanticSlot::AssignmentRequested
        }
        CommandKind::Message => CoordinationSemanticSlot::MessageSubmissionRecorded,
        CommandKind::Interrupt => CoordinationSemanticSlot::InterruptRequested,
    }
}

fn internal_command(error: impl Into<anyhow::Error>) -> CommandWriteError {
    CommandWriteError::Internal(error.into())
}
