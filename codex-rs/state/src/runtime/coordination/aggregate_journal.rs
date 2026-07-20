use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentMode;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationOrder;
use codex_coordination::CoordinationPrincipal;
use codex_coordination::CoordinationRevision;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::Evidence;
use codex_coordination::IdempotencyKey;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqliteConnection;

pub(super) use super::aggregate_event::compare_event;
pub(super) use super::aggregate_event::make_event;
use crate::model::coordination::NativeEventContext;
use crate::model::coordination::NativeEventIdentity;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CoordinationWriteError {
    #[error("coordination authority is quarantined")]
    Quarantined,
    #[error("coordination root is missing or belongs to another state epoch")]
    RootMismatch,
    #[error("assignment identity or ownership conflicts with durable state")]
    AssignmentConflict,
    #[error("assignment head version is stale")]
    VersionFenced,
    #[error("coordination root revision is stale")]
    RevisionFenced,
    #[error("assignment owner is stale")]
    OwnerFenced,
    #[error("assignment generation is stale")]
    GenerationFenced,
    #[error("assignment terminal outcome conflicts with the first outcome")]
    TerminalConflict,
    #[error("wait outcome conflicts with the first outcome")]
    WaitConflict,
    #[error("idempotency tuple, event identity, or content conflicts")]
    IdempotencyConflict,
    #[error("event or operation identity collides with another event in the bundle")]
    IdentityCollision,
    #[error("an existing identity records different semantic intent")]
    DivergentIntent,
    #[error("stored coordination event or outbox record is corrupt")]
    CorruptStoredEvent,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregateStep {
    TransactionBegin,
    Rollback,
    AuthorityRead,
    IdempotencyRead,
    AggregateRead,
    RootCreate,
    RevisionAllocation,
    AggregateMutation,
    EventInsert,
    OutboxInsert,
    BeforeCommit,
    AfterCommit,
}

/// Supplies deterministic transaction-boundary failures and wall-clock values.
///
/// Production uses [`NoFailure`]. Tests implement this trait to prove that each
/// SQL boundary rolls back without consuming revisions and that response loss
/// after commit replays through the normal idempotency path.
pub(crate) trait AggregateFailureInjector: Send + Sync {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()>;

    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis().max(0)
    }
}

pub(super) struct NoFailure;
impl AggregateFailureInjector for NoFailure {
    fn after_step(&self, _step: AggregateStep) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(super) struct StoredEvent {
    pub(super) event: CoordinationEvent,
    pub(super) revision: CoordinationRevision,
}

pub(super) fn validate_identities(
    context: &NativeEventContext,
) -> Result<(), CoordinationWriteError> {
    validate_context(context)?;
    if context.secondary.omitted_count() != 0 {
        return Err(CoordinationWriteError::IdentityCollision);
    }
    let identities = std::iter::once(&context.primary).chain(context.secondary.items());
    let mut event_ids = std::collections::BTreeSet::new();
    let mut operation_ids = std::collections::BTreeSet::new();
    for identity in identities {
        if !event_ids.insert(identity.event_id) || !operation_ids.insert(identity.operation_id) {
            return Err(CoordinationWriteError::IdentityCollision);
        }
    }
    Ok(())
}

fn validate_context(context: &NativeEventContext) -> Result<(), CoordinationWriteError> {
    if !matches!(
        context.source,
        codex_coordination::CoordinationSource::Native { .. }
    ) {
        return Err(CoordinationWriteError::DivergentIntent);
    }
    known_turn(&context.actor)?;
    Ok(())
}

fn idempotency_key(
    context: &NativeEventContext,
    identity: &NativeEventIdentity,
    semantic_slot: CoordinationSemanticSlot,
) -> Result<IdempotencyKey, CoordinationWriteError> {
    Ok(IdempotencyKey::new(
        context.root_thread_id,
        context.actor.thread_id,
        known_turn(&context.actor)?.clone(),
        identity.operation_id,
        semantic_slot,
    ))
}

pub(super) async fn finish<T>(
    connection: &mut SqliteConnection,
    result: Result<T, CoordinationWriteError>,
    injector: &dyn AggregateFailureInjector,
) -> Result<T, CoordinationWriteError> {
    match result {
        Ok(value) => {
            if let Err(err) = injector.after_step(AggregateStep::BeforeCommit) {
                rollback(connection, injector).await?;
                return Err(internal(err));
            }
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(internal)?;
            injector
                .after_step(AggregateStep::AfterCommit)
                .map_err(internal)?;
            Ok(value)
        }
        Err(err) => {
            rollback(connection, injector).await?;
            Err(err)
        }
    }
}

pub(super) async fn rollback(
    connection: &mut SqliteConnection,
    injector: &dyn AggregateFailureInjector,
) -> Result<(), CoordinationWriteError> {
    sqlx::query("ROLLBACK")
        .execute(&mut *connection)
        .await
        .map_err(internal)?;
    injector
        .after_step(AggregateStep::Rollback)
        .map_err(internal)
}

pub(super) async fn authority(
    connection: &mut SqliteConnection,
    injector: &dyn AggregateFailureInjector,
) -> Result<StateEpoch, CoordinationWriteError> {
    let row =
        sqlx::query("SELECT state_epoch,status FROM coordination_authority WHERE singleton_id=1")
            .fetch_one(&mut *connection)
            .await
            .map_err(internal)?;
    injector
        .after_step(AggregateStep::AuthorityRead)
        .map_err(internal)?;
    if row.get::<String, _>("status") != "active" {
        return Err(CoordinationWriteError::Quarantined);
    }
    StateEpoch::parse(&row.get::<String, _>("state_epoch")).map_err(internal)
}

pub(super) async fn ensure_root(
    connection: &mut SqliteConnection,
    root: &ThreadId,
    epoch: StateEpoch,
    create: bool,
    injector: &dyn AggregateFailureInjector,
) -> Result<(), CoordinationWriteError> {
    let existing = sqlx::query_scalar::<_, String>(
        "SELECT state_epoch FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(root.to_string())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    match existing {
        Some(value) if value == epoch.to_string() => Ok(()),
        Some(_) => Err(CoordinationWriteError::RootMismatch),
        None if create => {
            let now = now_ms(injector);
            sqlx::query("INSERT INTO coordination_roots (root_thread_id,state_epoch,committed_revision,published_revision,created_at_ms,updated_at_ms) VALUES (?,?,0,0,?,?)")
                .bind(root.to_string()).bind(epoch.to_string()).bind(now).bind(now).execute(&mut *connection).await.map_err(internal)?;
            injector
                .after_step(AggregateStep::RootCreate)
                .map_err(internal)
        }
        None => Err(CoordinationWriteError::RootMismatch),
    }
}

pub(super) async fn allocate(
    connection: &mut SqliteConnection,
    root: &ThreadId,
    count: usize,
    injector: &dyn AggregateFailureInjector,
) -> Result<Vec<CoordinationRevision>, CoordinationWriteError> {
    let end: i64 = sqlx::query_scalar("UPDATE coordination_roots SET committed_revision=committed_revision+?,updated_at_ms=MAX(updated_at_ms,?) WHERE root_thread_id=? RETURNING committed_revision")
        .bind(count as i64).bind(now_ms(injector)).bind(root.to_string()).fetch_one(&mut *connection).await.map_err(internal)?;
    injector
        .after_step(AggregateStep::RevisionAllocation)
        .map_err(internal)?;
    ((end - count as i64 + 1)..=end).map(revision).collect()
}

pub(super) async fn fence_root_revision(
    connection: &mut SqliteConnection,
    context: &NativeEventContext,
    injector: &dyn AggregateFailureInjector,
) -> Result<(), CoordinationWriteError> {
    let revision: i64 = sqlx::query_scalar(
        "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
    )
    .bind(context.root_thread_id.to_string())
    .fetch_one(&mut *connection)
    .await
    .map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    let expected = i64::try_from(context.expected_root_revision)
        .map_err(|_| CoordinationWriteError::RevisionFenced)?;
    if revision != expected {
        return Err(CoordinationWriteError::RevisionFenced);
    }
    Ok(())
}

pub(super) async fn load_idempotent(
    connection: &mut SqliteConnection,
    context: &NativeEventContext,
    identity: &NativeEventIdentity,
    semantic_slot: CoordinationSemanticSlot,
    injector: &dyn AggregateFailureInjector,
) -> Result<Option<StoredEvent>, CoordinationWriteError> {
    let key = idempotency_key(context, identity, semantic_slot)?;
    let row = sqlx::query("SELECT event_id,revision,canonical_event_bytes,event_fingerprint,idempotency_key_bytes FROM coordination_events WHERE root_thread_id=? AND idempotency_key_fingerprint=?")
        .bind(context.root_thread_id.to_string()).bind(key.fingerprint().as_slice()).fetch_optional(&mut *connection).await.map_err(internal)?;
    injector
        .after_step(AggregateStep::IdempotencyRead)
        .map_err(internal)?;
    let Some(row) = row else {
        if sqlx::query_scalar::<_, i64>("SELECT 1 FROM coordination_events WHERE event_id=?")
            .bind(identity.event_id.to_string())
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?
            .is_some()
        {
            injector
                .after_step(AggregateStep::IdempotencyRead)
                .map_err(internal)?;
            return Err(CoordinationWriteError::IdentityCollision);
        }
        injector
            .after_step(AggregateStep::IdempotencyRead)
            .map_err(internal)?;
        return Ok(None);
    };
    if row.get::<Vec<u8>, _>("idempotency_key_bytes") != key.tuple_bytes() {
        return Err(CoordinationWriteError::IdentityCollision);
    }
    let bytes: Vec<u8> = row.get("canonical_event_bytes");
    let event: CoordinationEvent =
        serde_json::from_slice(&bytes).map_err(|_| CoordinationWriteError::CorruptStoredEvent)?;
    let stored_revision = revision(row.get("revision"))?;
    if event.canonical_bytes() != bytes
        || event.fingerprint().as_slice() != row.get::<Vec<u8>, _>("event_fingerprint")
        || event.envelope().event_id.to_string() != row.get::<String, _>("event_id")
        || event.envelope().root_thread_id != context.root_thread_id
        || !matches!(event.envelope().order, CoordinationOrder::Native { revision, .. } if revision == stored_revision)
    {
        return Err(CoordinationWriteError::CorruptStoredEvent);
    }
    let outbox =
        sqlx::query("SELECT event_id,status FROM coordination_projection_outbox WHERE event_id=?")
            .bind(event.envelope().event_id.to_string())
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?;
    injector
        .after_step(AggregateStep::IdempotencyRead)
        .map_err(internal)?;
    let Some(outbox) = outbox else {
        return Err(CoordinationWriteError::CorruptStoredEvent);
    };
    if outbox.get::<String, _>("event_id") != event.envelope().event_id.to_string()
        || !matches!(
            outbox.get::<String, _>("status").as_str(),
            "pending" | "leased" | "materialized"
        )
    {
        return Err(CoordinationWriteError::CorruptStoredEvent);
    }
    Ok(Some(StoredEvent {
        event,
        revision: stored_revision,
    }))
}

pub(super) async fn load_event_id(
    connection: &mut SqliteConnection,
    event_id: &str,
    injector: &dyn AggregateFailureInjector,
) -> Result<StoredEvent, CoordinationWriteError> {
    let row = sqlx::query("SELECT revision,canonical_event_bytes,event_fingerprint FROM coordination_events WHERE event_id=?").bind(event_id).fetch_optional(&mut *connection).await.map_err(internal)?.ok_or(CoordinationWriteError::CorruptStoredEvent)?;
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    let bytes: Vec<u8> = row.get("canonical_event_bytes");
    let event: CoordinationEvent =
        serde_json::from_slice(&bytes).map_err(|_| CoordinationWriteError::CorruptStoredEvent)?;
    let stored_revision = revision(row.get("revision"))?;
    if event.canonical_bytes() != bytes
        || event.fingerprint().as_slice() != row.get::<Vec<u8>, _>("event_fingerprint")
        || event.envelope().event_id.to_string() != event_id
        || !matches!(event.envelope().order, CoordinationOrder::Native { revision, .. } if revision == stored_revision)
    {
        return Err(CoordinationWriteError::CorruptStoredEvent);
    }
    let outbox = sqlx::query_scalar::<_, String>(
        "SELECT status FROM coordination_projection_outbox WHERE event_id=?",
    )
    .bind(event_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?
    .ok_or(CoordinationWriteError::CorruptStoredEvent)?;
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    if !matches!(outbox.as_str(), "pending" | "leased" | "materialized") {
        return Err(CoordinationWriteError::CorruptStoredEvent);
    }
    Ok(StoredEvent {
        event,
        revision: stored_revision,
    })
}

/// Append already-checked native events to the immutable journal and projection
/// outbox on an existing `BEGIN IMMEDIATE` transaction. Sibling command storage
/// uses this seam so command intent and semantic facts share one commit.
pub(super) async fn journal(
    connection: &mut SqliteConnection,
    context: &NativeEventContext,
    events: &[CoordinationEvent],
    injector: &dyn AggregateFailureInjector,
) -> Result<(), CoordinationWriteError> {
    for (index, event) in events.iter().enumerate() {
        let identity = if index == 0 {
            &context.primary
        } else {
            context
                .secondary
                .items()
                .get(index - 1)
                .ok_or(CoordinationWriteError::IdempotencyConflict)?
        };
        let key = idempotency_key(context, identity, event.kind().semantic_slot())?;
        let revision = match event.envelope().order {
            CoordinationOrder::Native { revision, .. } => revision,
            CoordinationOrder::Compatibility { .. } => {
                return Err(internal(anyhow::anyhow!(
                    "native journal received compatibility event"
                )));
            }
        };
        sqlx::query("INSERT INTO coordination_events (event_id,root_thread_id,revision,canonical_event_bytes,event_fingerprint,idempotency_key_bytes,idempotency_key_fingerprint,occurred_at,created_at_ms) VALUES (?,?,?,?,?,?,?,?,?)")
            .bind(event.envelope().event_id.to_string()).bind(context.root_thread_id.to_string()).bind(revision.get() as i64).bind(event.canonical_bytes()).bind(event.fingerprint().as_slice())
            .bind(key.tuple_bytes()).bind(key.fingerprint().as_slice()).bind(context.occurred_at).bind(now_ms(injector)).execute(&mut *connection).await.map_err(internal)?;
        injector
            .after_step(AggregateStep::EventInsert)
            .map_err(internal)?;
        let now = now_ms(injector);
        sqlx::query("INSERT INTO coordination_projection_outbox (event_id,status,version,lease_epoch,retry_count,retry_after_ms,created_at_ms,updated_at_ms) VALUES (?,'pending',0,0,0,0,?,?)")
            .bind(event.envelope().event_id.to_string()).bind(now).bind(now).execute(&mut *connection).await.map_err(internal)?;
        injector
            .after_step(AggregateStep::OutboxInsert)
            .map_err(internal)?;
    }
    Ok(())
}

pub(super) fn revision(value: i64) -> Result<CoordinationRevision, CoordinationWriteError> {
    CoordinationRevision::new(value.try_into().map_err(anyhow::Error::from)?).map_err(internal)
}
pub(super) fn generation(value: i64) -> Result<AssignmentGeneration, CoordinationWriteError> {
    AssignmentGeneration::new(value.try_into().map_err(anyhow::Error::from)?).map_err(internal)
}
pub(super) fn mode_sql(mode: AssignmentMode) -> &'static str {
    match mode {
        AssignmentMode::Spawn => "spawn",
        AssignmentMode::Followup => "followup",
    }
}
pub(super) fn internal(err: impl Into<anyhow::Error>) -> CoordinationWriteError {
    CoordinationWriteError::Internal(err.into())
}
pub(super) fn now_ms(injector: &dyn AggregateFailureInjector) -> i64 {
    injector.now_ms().max(0)
}

pub(super) fn known_turn(
    principal: &CoordinationPrincipal,
) -> Result<&BoundedId<MAX_ID_BYTES>, CoordinationWriteError> {
    match &principal.turn_id {
        Evidence::Known { value } => Ok(value),
        Evidence::Unavailable { .. } | Evidence::NotApplicable => {
            Err(CoordinationWriteError::AssignmentConflict)
        }
    }
}
