use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationFailureCode;
use codex_coordination::CoordinationOperationId;
use codex_coordination::IdempotencyKey;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::ReceiptId;
use codex_protocol::ThreadId;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::aggregate_journal::AggregateStep;
use super::commands::CommandFailureInjector;
use super::commands::CommandWriteError;
use crate::model::coordination_commands::CapturedCommandTarget;
use crate::model::coordination_commands::CapturedGenerationSet;
use crate::model::coordination_commands::CommandCiphertext;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_commands::CommandLifecycle;
use crate::model::coordination_commands::CoordinationCommandMetadata;

pub(super) const MAX_INTENT_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub(super) const TERMINAL_PAYLOAD_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

pub(super) struct StoredCommand {
    pub metadata: CoordinationCommandMetadata,
    pub event: CoordinationEvent,
    pub tuple_bytes: Vec<u8>,
    pub tuple_fingerprint: Vec<u8>,
    pub command_fingerprint: Vec<u8>,
    pub ciphertext_fingerprint: Vec<u8>,
    pub ciphertext: Option<Vec<u8>>,
}

#[derive(Clone, Copy)]
pub(super) enum CommandPayloadAccess {
    MetadataOnly,
    Claim,
}

pub(super) async fn load_command(
    connection: &mut SqliteConnection,
    root: &ThreadId,
    key_fingerprint: &[u8],
) -> Result<Option<StoredCommand>, CommandWriteError> {
    let row = sqlx::query(
        "SELECT operation_id,c.root_thread_id AS root_thread_id,intent_event_id,operation_kind,target_thread_id,\
         target_assignment_id,target_generation,target_turn_id,captured_head_generation,\
         captured_turn_set_bytes,captured_turn_set_fingerprint,idempotency_tuple_bytes,\
         idempotency_tuple_fingerprint,command_fingerprint,encoded_payload_bytes,NULL AS ciphertext,\
         ciphertext_fingerprint,lifecycle,version,claim_count,attempt_count,attempted_lease_epoch,lease_epoch,\
         retry_after_ms,lease_expires_at_ms,expires_at_ms,terminal_receipt_id,terminal_receipt_fingerprint,e.canonical_event_bytes,\
         e.event_fingerprint FROM coordination_commands c JOIN coordination_events e \
         ON e.event_id=c.intent_event_id WHERE c.root_thread_id=? \
         AND c.idempotency_tuple_fingerprint=?",
    )
    .bind(root.to_string())
    .bind(key_fingerprint)
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    row.map(stored_from_row).transpose()
}

pub(super) async fn load_command_by_operation(
    connection: &mut SqliteConnection,
    operation_id: CoordinationOperationId,
    payload_access: CommandPayloadAccess,
) -> Result<Option<StoredCommand>, CommandWriteError> {
    let metadata_sql = "SELECT operation_id,c.root_thread_id AS root_thread_id,intent_event_id,operation_kind,target_thread_id,target_assignment_id,target_generation,target_turn_id,captured_head_generation,captured_turn_set_bytes,captured_turn_set_fingerprint,idempotency_tuple_bytes,idempotency_tuple_fingerprint,command_fingerprint,encoded_payload_bytes,NULL AS ciphertext,ciphertext_fingerprint,lifecycle,version,claim_count,attempt_count,attempted_lease_epoch,lease_epoch,retry_after_ms,lease_expires_at_ms,expires_at_ms,terminal_receipt_id,terminal_receipt_fingerprint,e.canonical_event_bytes,e.event_fingerprint FROM coordination_commands c JOIN coordination_events e ON e.event_id=c.intent_event_id WHERE c.operation_id=?";
    let claim_sql = "SELECT operation_id,c.root_thread_id AS root_thread_id,intent_event_id,operation_kind,target_thread_id,target_assignment_id,target_generation,target_turn_id,captured_head_generation,captured_turn_set_bytes,captured_turn_set_fingerprint,idempotency_tuple_bytes,idempotency_tuple_fingerprint,command_fingerprint,encoded_payload_bytes,ciphertext,ciphertext_fingerprint,lifecycle,version,claim_count,attempt_count,attempted_lease_epoch,lease_epoch,retry_after_ms,lease_expires_at_ms,expires_at_ms,terminal_receipt_id,terminal_receipt_fingerprint,e.canonical_event_bytes,e.event_fingerprint FROM coordination_commands c JOIN coordination_events e ON e.event_id=c.intent_event_id WHERE c.operation_id=?";
    let row = match payload_access {
        CommandPayloadAccess::MetadataOnly => {
            sqlx::query(metadata_sql)
                .bind(operation_id.to_string())
                .fetch_optional(&mut *connection)
                .await
        }
        CommandPayloadAccess::Claim => {
            sqlx::query(claim_sql)
                .bind(operation_id.to_string())
                .fetch_optional(&mut *connection)
                .await
        }
    }
    .map_err(internal)?;
    row.map(stored_from_row).transpose()
}

fn stored_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredCommand, CommandWriteError> {
    let operation_id =
        CoordinationOperationId::parse(&row.get::<String, _>("operation_id")).map_err(corrupt)?;
    let root_thread_id =
        ThreadId::try_from(row.get::<String, _>("root_thread_id")).map_err(corrupt)?;
    let intent_event_id =
        CoordinationEventId::parse(&row.get::<String, _>("intent_event_id")).map_err(corrupt)?;
    let kind = command_kind(&row.get::<String, _>("operation_kind"))?;
    let assignment_id =
        AssignmentId::parse(&row.get::<String, _>("target_assignment_id")).map_err(corrupt)?;
    let generation = parse_generation(row.get("target_generation"))?;
    let turn_id = row
        .get::<Option<String>, _>("target_turn_id")
        .map(BoundedId::<MAX_ID_BYTES>::new)
        .transpose()
        .map_err(corrupt)?;
    let captured_head_generation = row
        .get::<Option<i64>, _>("captured_head_generation")
        .map(parse_generation)
        .transpose()?;
    let set_bytes = row.get::<Option<Vec<u8>>, _>("captured_turn_set_bytes");
    let set_fingerprint = row.get::<Option<Vec<u8>>, _>("captured_turn_set_fingerprint");
    let captured_turn_set = match (set_bytes, set_fingerprint) {
        (None, None) => None,
        (Some(bytes), Some(fingerprint)) if fingerprint.as_slice() == sha256(&bytes) => {
            Some(decode_generation_set(&bytes)?)
        }
        _ => return Err(CommandWriteError::CorruptStoredCommand),
    };
    let ciphertext = row.get::<Option<Vec<u8>>, _>("ciphertext");
    let ciphertext_fingerprint = row.get::<Vec<u8>, _>("ciphertext_fingerprint");
    let encoded_payload_bytes = row.get::<i64, _>("encoded_payload_bytes");
    if ciphertext.as_ref().is_some_and(|bytes| {
        bytes.len() as i64 != encoded_payload_bytes
            || ciphertext_fingerprint.as_slice() != sha256(bytes)
    }) {
        return Err(CommandWriteError::CorruptStoredCommand);
    }
    let metadata = CoordinationCommandMetadata {
        operation_id,
        root_thread_id,
        intent_event_id,
        kind,
        target: CapturedCommandTarget {
            target_thread_id: ThreadId::try_from(row.get::<String, _>("target_thread_id"))
                .map_err(corrupt)?,
            assignment_id,
            generation,
            turn_id,
            captured_head_generation,
            captured_turn_set,
        },
        lifecycle: command_lifecycle(&row.get::<String, _>("lifecycle"))?,
        version: unsigned(row.get("version"))?,
        claim_count: unsigned(row.get("claim_count"))?,
        attempt_count: unsigned(row.get("attempt_count"))?,
        attempted_lease_epoch: row
            .get::<Option<i64>, _>("attempted_lease_epoch")
            .map(unsigned)
            .transpose()?,
        lease_epoch: unsigned(row.get("lease_epoch"))?,
        retry_after_ms: row.get("retry_after_ms"),
        expires_at_ms: row.get("expires_at_ms"),
        terminal_receipt_id: row
            .get::<Option<String>, _>("terminal_receipt_id")
            .map(|value| ReceiptId::parse(&value))
            .transpose()
            .map_err(corrupt)?,
        terminal_receipt_fingerprint: row
            .get::<Option<Vec<u8>>, _>("terminal_receipt_fingerprint")
            .map(std::convert::TryInto::try_into)
            .transpose()
            .map_err(|_| CommandWriteError::CorruptStoredCommand)?,
    };
    let tuple_bytes: Vec<u8> = row.get("idempotency_tuple_bytes");
    let tuple_fingerprint: Vec<u8> = row.get("idempotency_tuple_fingerprint");
    let command_fingerprint: Vec<u8> = row.get("command_fingerprint");
    let event_bytes: Vec<u8> = row.get("canonical_event_bytes");
    let event: CoordinationEvent = serde_json::from_slice(&event_bytes)
        .map_err(|_| CommandWriteError::CorruptStoredCommand)?;
    let event_fingerprint: Vec<u8> = row.get("event_fingerprint");
    let expected_command = command_fingerprint_from_parts(
        &tuple_bytes,
        &event,
        metadata.kind,
        &metadata.target,
        encoded_payload_bytes
            .try_into()
            .map_err(|_| CommandWriteError::CorruptStoredCommand)?,
        ciphertext_fingerprint
            .as_slice()
            .try_into()
            .map_err(|_| CommandWriteError::CorruptStoredCommand)?,
    );
    if tuple_fingerprint.as_slice() != sha256(&tuple_bytes)
        || event.canonical_bytes() != event_bytes
        || event.fingerprint().as_slice() != event_fingerprint
        || event.envelope().event_id != metadata.intent_event_id
        || command_fingerprint.as_slice() != expected_command
    {
        return Err(CommandWriteError::CorruptStoredCommand);
    }
    Ok(StoredCommand {
        metadata,
        event,
        tuple_bytes,
        tuple_fingerprint,
        command_fingerprint,
        ciphertext_fingerprint,
        ciphertext,
    })
}

pub(super) fn command_fingerprint(
    key: &IdempotencyKey,
    event: &CoordinationEvent,
    kind: CommandKind,
    target: &CapturedCommandTarget,
    encoded_payload_bytes: u32,
    ciphertext_fingerprint: &[u8; 32],
) -> [u8; 32] {
    command_fingerprint_from_parts(
        key.tuple_bytes(),
        event,
        kind,
        target,
        encoded_payload_bytes,
        ciphertext_fingerprint,
    )
}

fn command_fingerprint_from_parts(
    tuple_bytes: &[u8],
    event: &CoordinationEvent,
    kind: CommandKind,
    target: &CapturedCommandTarget,
    encoded_payload_bytes: u32,
    ciphertext_fingerprint: &[u8; 32],
) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(512);
    bytes.extend_from_slice(b"codex-coordination-command\0\x00\x01");
    field(&mut bytes, tuple_bytes);
    field(&mut bytes, event.envelope().event_id.to_string().as_bytes());
    field(&mut bytes, event.fingerprint().as_slice());
    field(&mut bytes, kind.as_sql().as_bytes());
    field(&mut bytes, target.target_thread_id.to_string().as_bytes());
    field(&mut bytes, target.assignment_id.to_string().as_bytes());
    bytes.extend_from_slice(&target.generation.get().to_be_bytes());
    optional_field(&mut bytes, target.turn_id.as_ref().map(BoundedId::as_str));
    optional_u32(
        &mut bytes,
        target
            .captured_head_generation
            .map(AssignmentGeneration::get),
    );
    optional_bytes(
        &mut bytes,
        target
            .captured_turn_set
            .as_ref()
            .map(CapturedGenerationSet::canonical_bytes),
    );
    bytes.extend_from_slice(&encoded_payload_bytes.to_be_bytes());
    bytes.extend_from_slice(ciphertext_fingerprint);
    sha256(&bytes)
}

pub(super) fn ciphertext(stored: &StoredCommand) -> Result<CommandCiphertext, CommandWriteError> {
    CommandCiphertext::new(
        stored
            .ciphertext
            .clone()
            .ok_or(CommandWriteError::Expired)?,
    )
    .map_err(corrupt)
}

pub(super) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn failure_code_sql(code: CoordinationFailureCode) -> &'static str {
    match code {
        CoordinationFailureCode::Unauthorized => "unauthorized",
        CoordinationFailureCode::StateUnavailable => "stateUnavailable",
        CoordinationFailureCode::StateQuarantined => "stateQuarantined",
        CoordinationFailureCode::InvalidPayload => "invalidPayload",
        CoordinationFailureCode::PayloadOverLimit => "payloadOverLimit",
        CoordinationFailureCode::TargetUnavailable => "targetUnavailable",
        CoordinationFailureCode::GenerationFenced => "generationFenced",
        CoordinationFailureCode::TerminalConflict => "terminalConflict",
        CoordinationFailureCode::OwnershipConflict => "ownershipConflict",
        CoordinationFailureCode::IdempotencyConflict => "idempotencyConflict",
        CoordinationFailureCode::RetryExhausted => "retryExhausted",
        CoordinationFailureCode::CorruptEvidence => "corruptEvidence",
        CoordinationFailureCode::Internal => "internal",
    }
}

pub(super) async fn finish_command<T>(
    connection: &mut SqliteConnection,
    result: Result<T, CommandWriteError>,
    injector: &dyn CommandFailureInjector,
) -> Result<T, CommandWriteError> {
    match result {
        Ok(value) => {
            if let Err(error) = injector.after_step(AggregateStep::BeforeCommit) {
                let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
                return Err(internal(error));
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
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

pub(super) async fn target_is_current(
    connection: &mut SqliteConnection,
    metadata: &CoordinationCommandMetadata,
) -> Result<bool, CommandWriteError> {
    if matches!(
        metadata.kind,
        CommandKind::AssignmentSpawn | CommandKind::AssignmentFollowup
    ) {
        let valid: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM coordination_assignment_heads h \
             JOIN coordination_assignment_generations g USING (assignment_id) \
             WHERE h.assignment_id=? AND h.root_thread_id=? AND h.child_thread_id=? \
             AND g.generation=? AND g.operation_id=? AND g.request_event_id=? \
             AND g.lifecycle = 'reserved'",
        )
        .bind(metadata.target.assignment_id.to_string())
        .bind(metadata.root_thread_id.to_string())
        .bind(metadata.target.target_thread_id.to_string())
        .bind(metadata.target.generation.get() as i64)
        .bind(metadata.operation_id.to_string())
        .bind(metadata.intent_event_id.to_string())
        .fetch_optional(&mut *connection)
        .await
        .map_err(internal)?;
        return Ok(valid == Some(1));
    }
    let Some(turn_id) = &metadata.target.turn_id else {
        return Err(CommandWriteError::CorruptStoredCommand);
    };
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT h.accepted_generation FROM coordination_assignment_heads h \
         JOIN coordination_turn_bindings b ON b.assignment_id=h.assignment_id \
          AND b.generation=h.accepted_generation WHERE h.assignment_id=? \
          AND h.root_thread_id=? AND h.child_thread_id=? AND b.turn_id=?",
    )
    .bind(metadata.target.assignment_id.to_string())
    .bind(metadata.root_thread_id.to_string())
    .bind(metadata.target.target_thread_id.to_string())
    .bind(turn_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    if current != Some(metadata.target.generation.get() as i64)
        || metadata.target.captured_head_generation != Some(metadata.target.generation)
    {
        return Ok(false);
    }
    if metadata.kind == CommandKind::Message {
        return Ok(metadata.target.captured_turn_set.is_none());
    }
    let stored = metadata
        .target
        .captured_turn_set
        .as_ref()
        .ok_or(CommandWriteError::CorruptStoredCommand)?;
    let rows: Vec<i64> = sqlx::query_scalar(
        "SELECT g.generation FROM coordination_turn_bindings b \
         JOIN coordination_assignment_generations g USING (assignment_id,generation) \
         WHERE b.root_thread_id=? AND b.turn_id=? AND b.assignment_id=? \
         AND g.accepted_event_id IS NOT NULL ORDER BY g.generation",
    )
    .bind(metadata.root_thread_id.to_string())
    .bind(turn_id.as_str())
    .bind(metadata.target.assignment_id.to_string())
    .fetch_all(&mut *connection)
    .await
    .map_err(internal)?;
    let current = CapturedGenerationSet::new(
        rows.into_iter()
            .map(|value| {
                AssignmentGeneration::new(value.try_into().map_err(internal)?).map_err(internal)
            })
            .collect::<Result<Vec<_>, _>>()?,
    )?;
    Ok(&current == stored)
}

fn decode_generation_set(bytes: &[u8]) -> Result<CapturedGenerationSet, CommandWriteError> {
    let Some((&count, body)) = bytes.split_first() else {
        return Err(CommandWriteError::CorruptStoredCommand);
    };
    if usize::from(count) * 4 != body.len() {
        return Err(CommandWriteError::CorruptStoredCommand);
    }
    let generations = body
        .chunks_exact(4)
        .map(|chunk| {
            let mut encoded = [0_u8; 4];
            encoded.copy_from_slice(chunk);
            parse_generation(i64::from(u32::from_be_bytes(encoded)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let set = CapturedGenerationSet::new(generations).map_err(corrupt)?;
    if set.canonical_bytes() != bytes {
        return Err(CommandWriteError::CorruptStoredCommand);
    }
    Ok(set)
}

fn command_kind(value: &str) -> Result<CommandKind, CommandWriteError> {
    match value {
        "assignmentSpawn" => Ok(CommandKind::AssignmentSpawn),
        "assignmentFollowup" => Ok(CommandKind::AssignmentFollowup),
        "message" => Ok(CommandKind::Message),
        "interrupt" => Ok(CommandKind::Interrupt),
        _ => Err(CommandWriteError::CorruptStoredCommand),
    }
}

fn command_lifecycle(value: &str) -> Result<CommandLifecycle, CommandWriteError> {
    match value {
        "pending" => Ok(CommandLifecycle::Pending),
        "leased" => Ok(CommandLifecycle::Leased),
        "succeeded" => Ok(CommandLifecycle::Succeeded),
        "poisoned" => Ok(CommandLifecycle::Poisoned),
        "expired" => Ok(CommandLifecycle::Expired),
        _ => Err(CommandWriteError::CorruptStoredCommand),
    }
}

fn parse_generation(value: i64) -> Result<AssignmentGeneration, CommandWriteError> {
    AssignmentGeneration::new(value.try_into().map_err(corrupt)?).map_err(corrupt)
}

fn unsigned(value: i64) -> Result<u64, CommandWriteError> {
    value.try_into().map_err(corrupt)
}

fn field(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u32).to_be_bytes());
    target.extend_from_slice(value);
}

fn optional_field(target: &mut Vec<u8>, value: Option<&str>) {
    optional_bytes(target, value.map(str::as_bytes));
}

fn optional_bytes(target: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => field(target, value),
        None => target.extend_from_slice(&u32::MAX.to_be_bytes()),
    }
}

fn optional_u32(target: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            target.push(1);
            target.extend_from_slice(&value.to_be_bytes());
        }
        None => target.push(0),
    }
}

fn internal(error: impl Into<anyhow::Error>) -> CommandWriteError {
    CommandWriteError::Internal(error.into())
}

fn corrupt(error: impl Into<anyhow::Error>) -> CommandWriteError {
    let _ = error.into();
    CommandWriteError::CorruptStoredCommand
}
