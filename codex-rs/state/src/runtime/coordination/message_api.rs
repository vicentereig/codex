//! Crate-external facade for the Stage 3.4 message/follow-up receipt-ref mailbox and
//! receipt-to-response-item materialization (`codex-9u5.2.3.4`, Stage 3 contract freeze,
//! Decision 9).
//!
//! Like [`super::sidecar_api`], this is a deliberate, narrow widening of the `coordination`
//! module boundary: everything else in `runtime::coordination` stays crate-internal to
//! `codex_state`. This module exists purely so `core/src/coordination/message_gate.rs` and
//! `core/src/agent/control/coordination_message.rs` (the enabled-only, test-constructible
//! message/follow-up delivery orchestration) can commit and recover durable state without
//! collapsing the module boundary entirely. It is not a production API: nothing calls it outside
//! test-driven invocation, since `CoordinationControl::Enabled` cannot be constructed by any
//! production caller in this stage.
//!
//! ## Generation fencing: a deliberately new table, not a reuse of `coordination_assignment_heads`
//!
//! `coordination_assignment_heads`/`coordination_assignment_generations` (migration 0047) already
//! model a "spawn is generation 1, follow-ups are sequential generations after it" sequence, and
//! at first glance this looks like the exact table Decision 9's generation fencing should reuse.
//! It is not: those tables are keyed by `assignment_id` and exist only for a *spawned* child
//! (`coordination_assignment_heads.child_thread_id`). `send_message` can target the *root* agent
//! (`message_tool.rs` only forbids this for follow-ups, not plain messages), and the root has no
//! assignment row at all. Reusing assignment heads literally would leave root-targeted messages
//! structurally unable to participate in generation fencing. `coordination_message_target_generations`
//! is therefore a new, message/follow-up-scoped table -- but it reuses the *concept* 0047 already
//! established: a monotonic generation counter CASed via an optimistic-concurrency `version`
//! column inside a single writer transaction. See the task report for the full reasoning; this is
//! flagged there as an explicit judgment call the frozen decision did not resolve unambiguously.

use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqliteConnection;
use uuid::Uuid;

use crate::StateRuntime;

/// Public error surface for message/follow-up delivery calls. Mirrors
/// [`super::sidecar_api::SidecarStateError`]'s shape.
#[derive(Debug, thiserror::Error)]
pub enum MessageDeliveryError {
    #[error("coordination authority is quarantined")]
    Quarantined,
    #[error("coordination authority epoch or root does not match")]
    EpochMismatch,
    #[error("coordination root is missing")]
    RootMissing,
    #[error("stored coordination message state is corrupt")]
    CorruptState,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn internal(error: impl Into<anyhow::Error>) -> MessageDeliveryError {
    MessageDeliveryError::Internal(error.into())
}

/// Distinguishes a `send_message` (`QueueOnly`) delivery from a `followup_task`
/// (`TriggerTurn`) delivery. Mirrors the distinction `message_tool.rs::MessageDeliveryMode`
/// already encodes at the handler layer; this is the state-layer sibling of that same fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageSemanticSlot {
    Message,
    Followup,
}

impl MessageSemanticSlot {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Followup => "followup",
        }
    }
}

/// Durable status of a receipt-ref mailbox row. `Committed` means the receipt exists but has not
/// yet been durably enqueued for delivery; `Enqueued` means the controlled queue side effect has
/// been recorded. Restart re-enqueues every `Committed` row (Decision 9's first recovery case).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageReceiptStatus {
    Committed,
    Enqueued,
}

impl MessageReceiptStatus {
    fn parse(value: &str) -> Result<Self, MessageDeliveryError> {
        match value {
            "committed" => Ok(Self::Committed),
            "enqueued" => Ok(Self::Enqueued),
            _ => Err(MessageDeliveryError::CorruptState),
        }
    }
}

/// Durable status of a receipt-to-response-item materialization row. Restart distinguishes:
/// `Committed` -> the materialization exists but the rollout append has not landed yet (second
/// recovery case); `RolloutAppended` -> the append landed but the item has not been selected into
/// a prompt yet (third recovery case); `Selected` -> fully consumed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationStatus {
    Committed,
    RolloutAppended,
    Selected,
}

impl MaterializationStatus {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolloutAppended => "rollout_appended",
            Self::Selected => "selected",
        }
    }

    fn parse(value: &str) -> Result<Self, MessageDeliveryError> {
        match value {
            "committed" => Ok(Self::Committed),
            "rollout_appended" => Ok(Self::RolloutAppended),
            "selected" => Ok(Self::Selected),
            _ => Err(MessageDeliveryError::CorruptState),
        }
    }
}

/// One durable receipt-ref mailbox row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageReceipt {
    pub receipt_id: Uuid,
    pub root_thread_id: ThreadId,
    pub operation_id: Uuid,
    pub sender_thread_id: ThreadId,
    pub sender_turn_id: String,
    pub target_thread_id: ThreadId,
    pub semantic_slot: MessageSemanticSlot,
    pub trigger_turn: bool,
    pub captured_generation: Option<u32>,
    pub bound_turn_id: Option<String>,
    pub status: MessageReceiptStatus,
}

/// Outcome of capturing a receipt: fresh, or the already-committed row for a retried
/// `operation_id` (exact-duplicate-safe; the generation/turn-binding side effect is never
/// repeated for a duplicate).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CaptureReceiptOutcome {
    Captured(MessageReceipt),
    Duplicate(MessageReceipt),
}

fn row_to_receipt(row: &sqlx::sqlite::SqliteRow) -> Result<MessageReceipt, MessageDeliveryError> {
    let receipt_id = Uuid::parse_str(&row.get::<String, _>("receipt_id"))
        .map_err(|_| MessageDeliveryError::CorruptState)?;
    let operation_id = Uuid::parse_str(&row.get::<String, _>("operation_id"))
        .map_err(|_| MessageDeliveryError::CorruptState)?;
    let root_thread_id = ThreadId::try_from(row.get::<String, _>("root_thread_id").as_str())
        .map_err(|_| MessageDeliveryError::CorruptState)?;
    let sender_thread_id = ThreadId::try_from(row.get::<String, _>("sender_thread_id").as_str())
        .map_err(|_| MessageDeliveryError::CorruptState)?;
    let target_thread_id = ThreadId::try_from(row.get::<String, _>("target_thread_id").as_str())
        .map_err(|_| MessageDeliveryError::CorruptState)?;
    let semantic_slot = match row.get::<String, _>("semantic_slot").as_str() {
        "message" => MessageSemanticSlot::Message,
        "followup" => MessageSemanticSlot::Followup,
        _ => return Err(MessageDeliveryError::CorruptState),
    };
    let captured_generation = row
        .get::<Option<i64>, _>("captured_generation")
        .map(u32::try_from)
        .transpose()
        .map_err(|_| MessageDeliveryError::CorruptState)?;
    Ok(MessageReceipt {
        receipt_id,
        root_thread_id,
        operation_id,
        sender_thread_id,
        sender_turn_id: row.get::<String, _>("sender_turn_id"),
        target_thread_id,
        semantic_slot,
        trigger_turn: row.get::<i64, _>("trigger_turn") != 0,
        captured_generation,
        bound_turn_id: row.get::<Option<String>, _>("bound_turn_id"),
        status: MessageReceiptStatus::parse(&row.get::<String, _>("status"))?,
    })
}

async fn checked_state_epoch(
    connection: &mut SqliteConnection,
    expected_state_epoch: StateEpoch,
) -> Result<(), MessageDeliveryError> {
    let row =
        sqlx::query("SELECT state_epoch,status FROM coordination_authority WHERE singleton_id=1")
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?
            .ok_or(MessageDeliveryError::EpochMismatch)?;
    if row.get::<String, _>("status") != "active" {
        return Err(MessageDeliveryError::Quarantined);
    }
    if row.get::<String, _>("state_epoch") != expected_state_epoch.to_string() {
        return Err(MessageDeliveryError::EpochMismatch);
    }
    Ok(())
}

async fn ensure_root_exists(
    connection: &mut SqliteConnection,
    root_thread_id: ThreadId,
) -> Result<(), MessageDeliveryError> {
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM coordination_roots WHERE root_thread_id=?")
            .bind(root_thread_id.to_string())
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?;
    exists.ok_or(MessageDeliveryError::RootMissing).map(|_| ())
}

/// Parameters for capturing a plain `send_message` (`QueueOnly`) receipt. Read-only with respect
/// to the target's generation head: the receipt simply captures whatever generation/turn is
/// currently accepted (possibly none yet), and is forever bound to it.
#[derive(Clone, Debug)]
pub struct CaptureQueueMessageReceipt {
    pub receipt_id: Uuid,
    pub operation_id: Uuid,
    pub sender_thread_id: ThreadId,
    pub sender_turn_id: String,
    pub target_thread_id: ThreadId,
    pub now_ms: i64,
}

/// Capture a queue-only message receipt (Decision 9 + the "queue message N versus acceptance
/// N+1" acceptance criterion). Runs inside one `BEGIN IMMEDIATE` transaction: SQLite's single
/// writer serializes this against any concurrent [`accept_followup_generation`] call for the same
/// target, so whichever call commits first determines what the other observes -- both orders
/// converge on a single, deterministic, forever-bound outcome. Idempotent on `operation_id`.
pub async fn capture_queue_message_receipt(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    expected_state_epoch: StateEpoch,
    params: CaptureQueueMessageReceipt,
) -> Result<CaptureReceiptOutcome, MessageDeliveryError> {
    let mut transaction = runtime
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(internal)?;
    checked_state_epoch(&mut transaction, expected_state_epoch).await?;
    ensure_root_exists(&mut transaction, root_thread_id).await?;

    if let Some(row) =
        sqlx::query("SELECT * FROM coordination_message_receipts WHERE operation_id=?")
            .bind(params.operation_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(internal)?
    {
        let receipt = row_to_receipt(&row)?;
        transaction.commit().await.map_err(internal)?;
        return Ok(CaptureReceiptOutcome::Duplicate(receipt));
    }

    let generation_row = sqlx::query(
        "SELECT accepted_generation,accepted_turn_id FROM coordination_message_target_generations \
         WHERE root_thread_id=? AND target_thread_id=?",
    )
    .bind(root_thread_id.to_string())
    .bind(params.target_thread_id.to_string())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(internal)?;
    let (captured_generation, bound_turn_id) = match generation_row {
        Some(row) => (
            row.get::<Option<i64>, _>("accepted_generation"),
            row.get::<Option<String>, _>("accepted_turn_id"),
        ),
        None => (None, None),
    };

    sqlx::query(
        "INSERT INTO coordination_message_receipts \
         (receipt_id,root_thread_id,state_epoch,operation_id,sender_thread_id,sender_turn_id,\
          target_thread_id,semantic_slot,trigger_turn,captured_generation,bound_turn_id,status,\
          created_at_ms,updated_at_ms) \
         VALUES (?,?,?,?,?,?,?,?,0,?,?,'committed',?,?)",
    )
    .bind(params.receipt_id.to_string())
    .bind(root_thread_id.to_string())
    .bind(expected_state_epoch.to_string())
    .bind(params.operation_id.to_string())
    .bind(params.sender_thread_id.to_string())
    .bind(&params.sender_turn_id)
    .bind(params.target_thread_id.to_string())
    .bind(MessageSemanticSlot::Message.as_sql())
    .bind(captured_generation)
    .bind(&bound_turn_id)
    .bind(params.now_ms)
    .bind(params.now_ms)
    .execute(&mut *transaction)
    .await
    .map_err(internal)?;
    transaction.commit().await.map_err(internal)?;

    Ok(CaptureReceiptOutcome::Captured(MessageReceipt {
        receipt_id: params.receipt_id,
        root_thread_id,
        operation_id: params.operation_id,
        sender_thread_id: params.sender_thread_id,
        sender_turn_id: params.sender_turn_id,
        target_thread_id: params.target_thread_id,
        semantic_slot: MessageSemanticSlot::Message,
        trigger_turn: false,
        captured_generation: captured_generation
            .map(u32::try_from)
            .transpose()
            .map_err(|_| MessageDeliveryError::CorruptState)?,
        bound_turn_id,
        status: MessageReceiptStatus::Committed,
    }))
}

/// Parameters for accepting a `followup_task` (`TriggerTurn`) generation and capturing its
/// receipt in the same transaction. `bound_turn_id` is caller-decided (a fresh turn id, or the
/// target's existing active turn id -- "follow-up generations accept sequentially and may bind
/// same active turn").
#[derive(Clone, Debug)]
pub struct AcceptFollowupGeneration {
    pub receipt_id: Uuid,
    pub operation_id: Uuid,
    pub sender_thread_id: ThreadId,
    pub sender_turn_id: String,
    pub target_thread_id: ThreadId,
    pub bound_turn_id: String,
    pub now_ms: i64,
}

/// Reserve+accept the next sequential generation for `target_thread_id` and capture its receipt,
/// atomically. See [`capture_queue_message_receipt`] for the fencing/convergence argument; this
/// is the other side of that same single-writer transaction guarantee. Idempotent on
/// `operation_id`: a retried call never advances the generation counter twice.
pub async fn accept_followup_generation(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    expected_state_epoch: StateEpoch,
    params: AcceptFollowupGeneration,
) -> Result<CaptureReceiptOutcome, MessageDeliveryError> {
    let mut transaction = runtime
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(internal)?;
    checked_state_epoch(&mut transaction, expected_state_epoch).await?;
    ensure_root_exists(&mut transaction, root_thread_id).await?;

    if let Some(row) =
        sqlx::query("SELECT * FROM coordination_message_receipts WHERE operation_id=?")
            .bind(params.operation_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(internal)?
    {
        let receipt = row_to_receipt(&row)?;
        transaction.commit().await.map_err(internal)?;
        return Ok(CaptureReceiptOutcome::Duplicate(receipt));
    }

    let generation_row = sqlx::query(
        "SELECT next_generation,version FROM coordination_message_target_generations \
         WHERE root_thread_id=? AND target_thread_id=?",
    )
    .bind(root_thread_id.to_string())
    .bind(params.target_thread_id.to_string())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(internal)?;

    let next_generation = match generation_row {
        Some(row) => {
            let next_generation: i64 = row.get("next_generation");
            let version: i64 = row.get("version");
            let changed = sqlx::query(
                "UPDATE coordination_message_target_generations \
                 SET accepted_generation=?,next_generation=next_generation+1,accepted_turn_id=?,\
                     version=version+1,updated_at_ms=? \
                 WHERE root_thread_id=? AND target_thread_id=? AND version=?",
            )
            .bind(next_generation)
            .bind(&params.bound_turn_id)
            .bind(params.now_ms)
            .bind(root_thread_id.to_string())
            .bind(params.target_thread_id.to_string())
            .bind(version)
            .execute(&mut *transaction)
            .await
            .map_err(internal)?
            .rows_affected();
            if changed != 1 {
                return Err(internal(anyhow::anyhow!(
                    "concurrent write to message target generation row within one transaction"
                )));
            }
            next_generation
        }
        None => {
            sqlx::query(
                "INSERT INTO coordination_message_target_generations \
                 (root_thread_id,target_thread_id,accepted_generation,next_generation,\
                  accepted_turn_id,version,created_at_ms,updated_at_ms) \
                 VALUES (?,?,1,2,?,0,?,?)",
            )
            .bind(root_thread_id.to_string())
            .bind(params.target_thread_id.to_string())
            .bind(&params.bound_turn_id)
            .bind(params.now_ms)
            .bind(params.now_ms)
            .execute(&mut *transaction)
            .await
            .map_err(internal)?;
            1
        }
    };

    sqlx::query(
        "INSERT INTO coordination_message_receipts \
         (receipt_id,root_thread_id,state_epoch,operation_id,sender_thread_id,sender_turn_id,\
          target_thread_id,semantic_slot,trigger_turn,captured_generation,bound_turn_id,status,\
          created_at_ms,updated_at_ms) \
         VALUES (?,?,?,?,?,?,?,?,1,?,?,'committed',?,?)",
    )
    .bind(params.receipt_id.to_string())
    .bind(root_thread_id.to_string())
    .bind(expected_state_epoch.to_string())
    .bind(params.operation_id.to_string())
    .bind(params.sender_thread_id.to_string())
    .bind(&params.sender_turn_id)
    .bind(params.target_thread_id.to_string())
    .bind(MessageSemanticSlot::Followup.as_sql())
    .bind(next_generation)
    .bind(&params.bound_turn_id)
    .bind(params.now_ms)
    .bind(params.now_ms)
    .execute(&mut *transaction)
    .await
    .map_err(internal)?;
    transaction.commit().await.map_err(internal)?;

    Ok(CaptureReceiptOutcome::Captured(MessageReceipt {
        receipt_id: params.receipt_id,
        root_thread_id,
        operation_id: params.operation_id,
        sender_thread_id: params.sender_thread_id,
        sender_turn_id: params.sender_turn_id,
        target_thread_id: params.target_thread_id,
        semantic_slot: MessageSemanticSlot::Followup,
        trigger_turn: true,
        captured_generation: Some(
            u32::try_from(next_generation).map_err(|_| MessageDeliveryError::CorruptState)?,
        ),
        bound_turn_id: Some(params.bound_turn_id),
        status: MessageReceiptStatus::Committed,
    }))
}

/// Advance a receipt from `committed` to `enqueued` -- the controlled queue side effect. A no-op
/// (not an error) if already `enqueued`, so restart recovery can call this unconditionally.
pub async fn mark_receipt_enqueued(
    runtime: &StateRuntime,
    receipt_id: Uuid,
    now_ms: i64,
) -> Result<(), MessageDeliveryError> {
    sqlx::query(
        "UPDATE coordination_message_receipts SET status='enqueued',updated_at_ms=MAX(updated_at_ms,?) \
         WHERE receipt_id=? AND status='committed'",
    )
    .bind(now_ms)
    .bind(receipt_id.to_string())
    .execute(&*runtime.pool)
    .await
    .map_err(internal)?;
    Ok(())
}

/// Restart recovery, case 1 ("receipt committed before enqueue"): every receipt still `committed`
/// for `root_thread_id` must be re-enqueued.
pub async fn pending_committed_receipts(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    limit: u32,
) -> Result<Vec<MessageReceipt>, MessageDeliveryError> {
    let rows = sqlx::query(
        "SELECT * FROM coordination_message_receipts WHERE root_thread_id=? AND status='committed' \
         ORDER BY created_at_ms ASC LIMIT ?",
    )
    .bind(root_thread_id.to_string())
    .bind(limit)
    .fetch_all(&*runtime.pool)
    .await
    .map_err(internal)?;
    rows.iter().map(row_to_receipt).collect()
}

/// One durable receipt-to-response-item materialization row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageMaterialization {
    pub receipt_id: Uuid,
    pub target_turn_id: String,
    pub response_item_id: Uuid,
    pub root_thread_id: ThreadId,
    pub status: MaterializationStatus,
}

fn row_to_materialization(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<MessageMaterialization, MessageDeliveryError> {
    Ok(MessageMaterialization {
        receipt_id: Uuid::parse_str(&row.get::<String, _>("receipt_id"))
            .map_err(|_| MessageDeliveryError::CorruptState)?,
        target_turn_id: row.get::<String, _>("target_turn_id"),
        response_item_id: Uuid::parse_str(&row.get::<String, _>("response_item_id"))
            .map_err(|_| MessageDeliveryError::CorruptState)?,
        root_thread_id: ThreadId::try_from(row.get::<String, _>("root_thread_id").as_str())
            .map_err(|_| MessageDeliveryError::CorruptState)?,
        status: MaterializationStatus::parse(&row.get::<String, _>("status"))?,
    })
}

/// Commit a materialization row (idempotent: a duplicate call for the identical
/// `(receipt_id, target_turn_id, response_item_id)` returns the already-committed row rather than
/// erroring or duplicating).
pub async fn commit_materialization(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    receipt_id: Uuid,
    target_turn_id: &str,
    response_item_id: Uuid,
    now_ms: i64,
) -> Result<MessageMaterialization, MessageDeliveryError> {
    let mut transaction = runtime
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(internal)?;
    if let Some(row) = sqlx::query(
        "SELECT * FROM coordination_message_materializations \
         WHERE receipt_id=? AND target_turn_id=? AND response_item_id=?",
    )
    .bind(receipt_id.to_string())
    .bind(target_turn_id)
    .bind(response_item_id.to_string())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(internal)?
    {
        let materialization = row_to_materialization(&row)?;
        transaction.commit().await.map_err(internal)?;
        return Ok(materialization);
    }
    sqlx::query(
        "INSERT INTO coordination_message_materializations \
         (receipt_id,target_turn_id,response_item_id,root_thread_id,status,created_at_ms,updated_at_ms) \
         VALUES (?,?,?,?,'committed',?,?)",
    )
    .bind(receipt_id.to_string())
    .bind(target_turn_id)
    .bind(response_item_id.to_string())
    .bind(root_thread_id.to_string())
    .bind(now_ms)
    .bind(now_ms)
    .execute(&mut *transaction)
    .await
    .map_err(internal)?;
    transaction.commit().await.map_err(internal)?;
    Ok(MessageMaterialization {
        receipt_id,
        target_turn_id: target_turn_id.to_string(),
        response_item_id,
        root_thread_id,
        status: MaterializationStatus::Committed,
    })
}

async fn advance_materialization_status(
    runtime: &StateRuntime,
    receipt_id: Uuid,
    target_turn_id: &str,
    response_item_id: Uuid,
    from: MaterializationStatus,
    to: MaterializationStatus,
    now_ms: i64,
) -> Result<(), MessageDeliveryError> {
    sqlx::query(
        "UPDATE coordination_message_materializations SET status=?,updated_at_ms=MAX(updated_at_ms,?) \
         WHERE receipt_id=? AND target_turn_id=? AND response_item_id=? AND status=?",
    )
    .bind(to.as_sql())
    .bind(now_ms)
    .bind(receipt_id.to_string())
    .bind(target_turn_id)
    .bind(response_item_id.to_string())
    .bind(from.as_sql())
    .execute(&*runtime.pool)
    .await
    .map_err(internal)?;
    Ok(())
}

/// Restart recovery, case 2 ("materialization committed before rollout append"): advance
/// `committed` -> `rollout_appended` once the append has actually landed. A no-op if already past
/// `committed`, so recovery can call this unconditionally for every pending row it finds.
pub async fn mark_materialization_rollout_appended(
    runtime: &StateRuntime,
    receipt_id: Uuid,
    target_turn_id: &str,
    response_item_id: Uuid,
    now_ms: i64,
) -> Result<(), MessageDeliveryError> {
    advance_materialization_status(
        runtime,
        receipt_id,
        target_turn_id,
        response_item_id,
        MaterializationStatus::Committed,
        MaterializationStatus::RolloutAppended,
        now_ms,
    )
    .await
}

/// Restart recovery, case 3 ("rollout append before selection"): advance `rollout_appended` ->
/// `selected` once the item has actually been selected into a prompt.
pub async fn mark_materialization_selected(
    runtime: &StateRuntime,
    receipt_id: Uuid,
    target_turn_id: &str,
    response_item_id: Uuid,
    now_ms: i64,
) -> Result<(), MessageDeliveryError> {
    advance_materialization_status(
        runtime,
        receipt_id,
        target_turn_id,
        response_item_id,
        MaterializationStatus::RolloutAppended,
        MaterializationStatus::Selected,
        now_ms,
    )
    .await
}

/// Restart recovery, case 2 query: every materialization still `committed` for `root_thread_id`.
pub async fn pending_committed_materializations(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    limit: u32,
) -> Result<Vec<MessageMaterialization>, MessageDeliveryError> {
    let rows = sqlx::query(
        "SELECT * FROM coordination_message_materializations \
         WHERE root_thread_id=? AND status='committed' ORDER BY created_at_ms ASC LIMIT ?",
    )
    .bind(root_thread_id.to_string())
    .bind(limit)
    .fetch_all(&*runtime.pool)
    .await
    .map_err(internal)?;
    rows.iter().map(row_to_materialization).collect()
}

/// Restart recovery, case 3 query: every materialization `rollout_appended` (not yet `selected`)
/// for `root_thread_id` -- these are selectable.
pub async fn pending_appended_materializations(
    runtime: &StateRuntime,
    root_thread_id: ThreadId,
    limit: u32,
) -> Result<Vec<MessageMaterialization>, MessageDeliveryError> {
    let rows = sqlx::query(
        "SELECT * FROM coordination_message_materializations \
         WHERE root_thread_id=? AND status='rollout_appended' ORDER BY created_at_ms ASC LIMIT ?",
    )
    .bind(root_thread_id.to_string())
    .bind(limit)
    .fetch_all(&*runtime.pool)
    .await
    .map_err(internal)?;
    rows.iter().map(row_to_materialization).collect()
}
