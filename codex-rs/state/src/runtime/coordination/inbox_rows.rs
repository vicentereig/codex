use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationOperationId;
use codex_coordination::ReceiptId;
use codex_protocol::ThreadId;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::inbox::InboxWriteError;
use crate::model::coordination_commands::CommandKind;
use crate::model::coordination_inbox::CommittedReceiptAck;
use crate::model::coordination_inbox::InboxCiphertext;
use crate::model::coordination_inbox::InboxLifecycle;
use crate::model::coordination_inbox::InboxReceiptMetadata;

pub(super) const TERMINAL_INBOX_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

const RECEIPT_METADATA_SELECT: &str = "SELECT i.*,NULL AS selected_ciphertext,e.canonical_event_bytes,e.event_fingerprint FROM coordination_inbox i JOIN coordination_events e ON e.event_id=i.receipt_event_id WHERE i.receipt_id=?";
const RECEIPT_PAYLOAD_SELECT: &str = "SELECT i.*,i.ciphertext AS selected_ciphertext,e.canonical_event_bytes,e.event_fingerprint FROM coordination_inbox i JOIN coordination_events e ON e.event_id=i.receipt_event_id WHERE i.receipt_id=?";
const COMMAND_METADATA_SELECT: &str = "SELECT i.*,NULL AS selected_ciphertext,e.canonical_event_bytes,e.event_fingerprint FROM coordination_inbox i JOIN coordination_events e ON e.event_id=i.receipt_event_id WHERE i.command_operation_id=?";
const COMMAND_PAYLOAD_SELECT: &str = "SELECT i.*,i.ciphertext AS selected_ciphertext,e.canonical_event_bytes,e.event_fingerprint FROM coordination_inbox i JOIN coordination_events e ON e.event_id=i.receipt_event_id WHERE i.command_operation_id=?";
const TUPLE_METADATA_SELECT: &str = "SELECT i.*,NULL AS selected_ciphertext,e.canonical_event_bytes,e.event_fingerprint FROM coordination_inbox i JOIN coordination_events e ON e.event_id=i.receipt_event_id WHERE i.root_thread_id=? AND i.recipient_thread_id=? AND i.receipt_tuple_fingerprint=?";

#[derive(Clone, Copy)]
pub(super) enum InboxPayloadAccess {
    MetadataOnly,
    Claim,
}

pub(super) struct StoredInbox {
    pub metadata: InboxReceiptMetadata,
    pub receipt_event: CoordinationEvent,
    pub tuple_bytes: Vec<u8>,
    pub tuple_fingerprint: [u8; 32],
    pub command_fingerprint: [u8; 32],
    pub ciphertext_fingerprint: [u8; 32],
    pub ciphertext: Option<Vec<u8>>,
    pub captured_head_generation: Option<AssignmentGeneration>,
    pub captured_turn_set_bytes: Option<Vec<u8>>,
    pub captured_turn_set_fingerprint: Option<[u8; 32]>,
    pub sender_intent_at_ms: i64,
    pub durable_received_at_ms: i64,
    pub terminal_at_ms: Option<i64>,
    pub resolution_event_id: Option<CoordinationEventId>,
    pub absolute_expires_at_ms: i64,
    pub lease_expires_at_ms: Option<i64>,
    pub lease_claim_operation_id: Option<CoordinationOperationId>,
    pub updated_at_ms: i64,
}

pub(super) async fn load_inbox_by_receipt(
    connection: &mut SqliteConnection,
    receipt_id: ReceiptId,
    access: InboxPayloadAccess,
) -> Result<Option<StoredInbox>, InboxWriteError> {
    let sql = match access {
        InboxPayloadAccess::MetadataOnly => RECEIPT_METADATA_SELECT,
        InboxPayloadAccess::Claim => RECEIPT_PAYLOAD_SELECT,
    };
    load_one(connection, sql, receipt_id.to_string()).await
}

pub(super) async fn load_inbox_by_command(
    connection: &mut SqliteConnection,
    operation_id: CoordinationOperationId,
    access: InboxPayloadAccess,
) -> Result<Option<StoredInbox>, InboxWriteError> {
    let sql = match access {
        InboxPayloadAccess::MetadataOnly => COMMAND_METADATA_SELECT,
        InboxPayloadAccess::Claim => COMMAND_PAYLOAD_SELECT,
    };
    load_one(connection, sql, operation_id.to_string()).await
}

pub(super) async fn load_inbox_by_tuple(
    connection: &mut SqliteConnection,
    root: &ThreadId,
    recipient: &ThreadId,
    fingerprint: &[u8; 32],
) -> Result<Option<StoredInbox>, InboxWriteError> {
    let row = sqlx::query(TUPLE_METADATA_SELECT)
        .bind(root.to_string())
        .bind(recipient.to_string())
        .bind(fingerprint.as_slice())
        .fetch_optional(&mut *connection)
        .await
        .map_err(internal)?;
    row.map(stored_from_row).transpose()
}

async fn load_one(
    connection: &mut SqliteConnection,
    sql: &'static str,
    identity: String,
) -> Result<Option<StoredInbox>, InboxWriteError> {
    let row = sqlx::query(sql)
        .bind(identity)
        .fetch_optional(&mut *connection)
        .await
        .map_err(internal)?;
    row.map(stored_from_row).transpose()
}

fn stored_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredInbox, InboxWriteError> {
    let receipt_id = ReceiptId::parse(&row.get::<String, _>("receipt_id")).map_err(corrupt)?;
    let command_operation_id =
        CoordinationOperationId::parse(&row.get::<String, _>("command_operation_id"))
            .map_err(corrupt)?;
    let root_thread_id =
        ThreadId::try_from(row.get::<String, _>("root_thread_id")).map_err(corrupt)?;
    let intent_event_id =
        CoordinationEventId::parse(&row.get::<String, _>("intent_event_id")).map_err(corrupt)?;
    let receipt_event_id =
        CoordinationEventId::parse(&row.get::<String, _>("receipt_event_id")).map_err(corrupt)?;
    let sender_thread_id =
        ThreadId::try_from(row.get::<String, _>("sender_thread_id")).map_err(corrupt)?;
    let sender_turn_id = BoundedId::new(row.get::<String, _>("sender_turn_id")).map_err(corrupt)?;
    let recipient_thread_id =
        ThreadId::try_from(row.get::<String, _>("recipient_thread_id")).map_err(corrupt)?;
    let recipient_turn_id =
        BoundedId::new(row.get::<String, _>("recipient_turn_id")).map_err(corrupt)?;
    let kind = command_kind(&row.get::<String, _>("operation_kind"))?;
    let target_assignment_id =
        AssignmentId::parse(&row.get::<String, _>("target_assignment_id")).map_err(corrupt)?;
    let target_generation = generation(row.get("target_generation"))?;
    let delivery_fingerprint = array32(row.get("delivery_fingerprint"))?;
    let encoded_payload_bytes = unsigned32(row.get("encoded_payload_bytes"))?;
    let metadata = InboxReceiptMetadata {
        receipt_id,
        command_operation_id,
        root_thread_id,
        intent_event_id,
        receipt_event_id,
        sender_thread_id,
        sender_turn_id,
        recipient_thread_id,
        recipient_turn_id,
        kind,
        target_assignment_id,
        target_generation,
        lifecycle: lifecycle(&row.get::<String, _>("lifecycle"))?,
        version: unsigned(row.get("version"))?,
        claim_count: unsigned(row.get("claim_count"))?,
        retry_count: unsigned(row.get("retry_count"))?,
        lease_epoch: unsigned(row.get("lease_epoch"))?,
        retry_after_ms: row.get("retry_after_ms"),
        expires_at_ms: row.get("expires_at_ms"),
        encoded_payload_bytes,
        delivery_fingerprint,
    };
    let tuple_bytes: Vec<u8> = row.get("receipt_tuple_bytes");
    let tuple_fingerprint = array32(row.get("receipt_tuple_fingerprint"))?;
    let command_fingerprint = array32(row.get("sender_command_fingerprint"))?;
    let ciphertext_fingerprint = array32(row.get("ciphertext_fingerprint"))?;
    let ciphertext = row.get::<Option<Vec<u8>>, _>("selected_ciphertext");
    let captured_head_generation = row
        .get::<Option<i64>, _>("captured_head_generation")
        .map(generation)
        .transpose()?;
    let captured_turn_set_bytes = row.get::<Option<Vec<u8>>, _>("captured_turn_set_bytes");
    let captured_turn_set_fingerprint = row
        .get::<Option<Vec<u8>>, _>("captured_turn_set_fingerprint")
        .map(array32)
        .transpose()?;
    let event_bytes: Vec<u8> = row.get("canonical_event_bytes");
    let receipt_event: CoordinationEvent =
        serde_json::from_slice(&event_bytes).map_err(|_| InboxWriteError::CorruptStoredInbox)?;
    let event_fingerprint: Vec<u8> = row.get("event_fingerprint");
    let sender_intent_at_ms = row.get("sender_intent_at_ms");
    let durable_received_at_ms = row.get("durable_received_at_ms");
    let absolute_expires_at_ms = row.get("absolute_expires_at_ms");
    let expected_delivery = delivery_fingerprint_from_parts(
        &tuple_bytes,
        &metadata,
        &command_fingerprint,
        &ciphertext_fingerprint,
        captured_head_generation,
        captured_turn_set_bytes.as_deref(),
        sender_intent_at_ms,
        absolute_expires_at_ms,
    );
    if tuple_fingerprint != sha256(&tuple_bytes)
        || expected_delivery != delivery_fingerprint
        || receipt_event.canonical_bytes() != event_bytes
        || receipt_event.fingerprint().as_slice() != event_fingerprint
        || receipt_event.envelope().event_id != receipt_event_id
        || receipt_event.envelope().root_thread_id != root_thread_id
        || ciphertext.as_ref().is_some_and(|bytes| {
            bytes.len() != encoded_payload_bytes as usize || sha256(bytes) != ciphertext_fingerprint
        })
        || captured_turn_set_bytes
            .as_ref()
            .is_some_and(|bytes| captured_turn_set_fingerprint != Some(sha256(bytes)))
    {
        return Err(InboxWriteError::CorruptStoredInbox);
    }
    Ok(StoredInbox {
        metadata,
        receipt_event,
        tuple_bytes,
        tuple_fingerprint,
        command_fingerprint,
        ciphertext_fingerprint,
        ciphertext,
        captured_head_generation,
        captured_turn_set_bytes,
        captured_turn_set_fingerprint,
        sender_intent_at_ms,
        durable_received_at_ms,
        terminal_at_ms: row.get("terminal_at_ms"),
        resolution_event_id: row
            .get::<Option<String>, _>("resolution_event_id")
            .map(|value| CoordinationEventId::parse(&value))
            .transpose()
            .map_err(corrupt)?,
        absolute_expires_at_ms,
        lease_expires_at_ms: row.get("lease_expires_at_ms"),
        lease_claim_operation_id: row
            .get::<Option<String>, _>("lease_claim_operation_id")
            .map(|value| CoordinationOperationId::parse(&value))
            .transpose()
            .map_err(corrupt)?,
        updated_at_ms: row.get("updated_at_ms"),
    })
}

pub(super) fn delivery_fingerprint_from_parts(
    tuple_bytes: &[u8],
    metadata: &InboxReceiptMetadata,
    command_fingerprint: &[u8; 32],
    ciphertext_fingerprint: &[u8; 32],
    captured_head_generation: Option<AssignmentGeneration>,
    captured_turn_set_bytes: Option<&[u8]>,
    sender_intent_at_ms: i64,
    absolute_expires_at_ms: i64,
) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(512);
    bytes.extend_from_slice(b"codex-coordination-inbox-delivery\0\x00\x01");
    field(&mut bytes, tuple_bytes);
    field(&mut bytes, metadata.receipt_id.to_string().as_bytes());
    field(
        &mut bytes,
        metadata.command_operation_id.to_string().as_bytes(),
    );
    field(&mut bytes, metadata.intent_event_id.to_string().as_bytes());
    field(&mut bytes, metadata.receipt_event_id.to_string().as_bytes());
    field(&mut bytes, metadata.root_thread_id.to_string().as_bytes());
    field(&mut bytes, metadata.sender_thread_id.to_string().as_bytes());
    field(&mut bytes, metadata.sender_turn_id.as_str().as_bytes());
    field(
        &mut bytes,
        metadata.recipient_thread_id.to_string().as_bytes(),
    );
    field(&mut bytes, metadata.recipient_turn_id.as_str().as_bytes());
    field(&mut bytes, metadata.kind.as_sql().as_bytes());
    field(
        &mut bytes,
        metadata.target_assignment_id.to_string().as_bytes(),
    );
    bytes.extend_from_slice(&metadata.target_generation.get().to_be_bytes());
    optional_generation(&mut bytes, captured_head_generation);
    optional_bytes(&mut bytes, captured_turn_set_bytes);
    bytes.extend_from_slice(&metadata.encoded_payload_bytes.to_be_bytes());
    bytes.extend_from_slice(command_fingerprint);
    bytes.extend_from_slice(ciphertext_fingerprint);
    bytes.extend_from_slice(&sender_intent_at_ms.to_be_bytes());
    bytes.extend_from_slice(&absolute_expires_at_ms.to_be_bytes());
    sha256(&bytes)
}

pub(super) fn committed_ack(stored: &StoredInbox) -> CommittedReceiptAck {
    CommittedReceiptAck {
        receipt_id: stored.metadata.receipt_id,
        command_operation_id: stored.metadata.command_operation_id,
        receipt_event_id: stored.metadata.receipt_event_id,
        delivery_fingerprint: stored.metadata.delivery_fingerprint,
        encoded_payload_bytes: stored.metadata.encoded_payload_bytes,
        durable_received_at_ms: stored.durable_received_at_ms,
        expires_at_ms: stored.absolute_expires_at_ms,
    }
}

pub(super) fn inbox_ciphertext(stored: &StoredInbox) -> Result<InboxCiphertext, InboxWriteError> {
    InboxCiphertext::from_stored(stored.ciphertext.clone().ok_or(InboxWriteError::Expired)?)
        .map_err(InboxWriteError::Input)
}

pub(super) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn lifecycle(value: &str) -> Result<InboxLifecycle, InboxWriteError> {
    match value {
        "received" => Ok(InboxLifecycle::Received),
        "leased" => Ok(InboxLifecycle::Leased),
        "selected" => Ok(InboxLifecycle::Selected),
        "processed" => Ok(InboxLifecycle::Processed),
        "poisoned" => Ok(InboxLifecycle::Poisoned),
        "expired" => Ok(InboxLifecycle::Expired),
        _ => Err(InboxWriteError::CorruptStoredInbox),
    }
}

fn command_kind(value: &str) -> Result<CommandKind, InboxWriteError> {
    match value {
        "assignmentSpawn" => Ok(CommandKind::AssignmentSpawn),
        "assignmentFollowup" => Ok(CommandKind::AssignmentFollowup),
        "message" => Ok(CommandKind::Message),
        "interrupt" => Ok(CommandKind::Interrupt),
        _ => Err(InboxWriteError::CorruptStoredInbox),
    }
}

fn generation(value: i64) -> Result<AssignmentGeneration, InboxWriteError> {
    AssignmentGeneration::new(value.try_into().map_err(corrupt)?).map_err(corrupt)
}

fn array32(value: Vec<u8>) -> Result<[u8; 32], InboxWriteError> {
    value
        .try_into()
        .map_err(|_| InboxWriteError::CorruptStoredInbox)
}

fn unsigned(value: i64) -> Result<u64, InboxWriteError> {
    value.try_into().map_err(corrupt)
}

fn unsigned32(value: i64) -> Result<u32, InboxWriteError> {
    value.try_into().map_err(corrupt)
}

fn field(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
    bytes.extend_from_slice(value);
}

fn optional_bytes(bytes: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => field(bytes, value),
        None => bytes.extend_from_slice(&u32::MAX.to_be_bytes()),
    }
}

fn optional_generation(bytes: &mut Vec<u8>, generation: Option<AssignmentGeneration>) {
    match generation {
        Some(generation) => {
            bytes.push(1);
            bytes.extend_from_slice(&generation.get().to_be_bytes());
        }
        None => bytes.push(0),
    }
}

fn internal(error: impl Into<anyhow::Error>) -> InboxWriteError {
    InboxWriteError::Internal(error.into())
}

fn corrupt(error: impl Into<anyhow::Error>) -> InboxWriteError {
    let _ = error.into();
    InboxWriteError::CorruptStoredInbox
}
