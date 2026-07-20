use codex_coordination::BoundedId;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationFailureCode;
use codex_coordination::CoordinationOperationId;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::ReceiptId;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::inbox::InboxWriteError;
use super::inbox::internal;
use crate::model::coordination_inbox::CommittedInboxSelection;
use crate::model::coordination_inbox::InboxSelectionToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransportState {
    Selected,
    SendSucceeded,
    SendFailed,
    SendUnknown,
}

pub(super) struct StoredInclusion {
    pub receipt_id: ReceiptId,
    pub inference_attempt_id: BoundedId<MAX_ID_BYTES>,
    pub root_thread_id: ThreadId,
    pub target_turn_id: BoundedId<MAX_ID_BYTES>,
    pub delivery_fingerprint: [u8; 32],
    pub selected_at_ms: i64,
    pub lease_expires_at_ms: i64,
    pub semantic_claim: bool,
    pub semantic_event_id: Option<CoordinationEventId>,
    pub inbox_version: u64,
    pub lease_epoch: u64,
    pub claim_operation_id: CoordinationOperationId,
    pub transport_state: TransportState,
    pub transport_completed_at_ms: Option<i64>,
    pub retry_after_ms: Option<i64>,
    pub version: u64,
    pub failure_code: Option<CoordinationFailureCode>,
}

pub(super) async fn load_selection(
    connection: &mut SqliteConnection,
    receipt_id: ReceiptId,
    inference_attempt_id: &BoundedId<MAX_ID_BYTES>,
) -> Result<Option<StoredInclusion>, InboxWriteError> {
    let row = sqlx::query(
        "SELECT * FROM coordination_inbox_inclusions WHERE receipt_id=? AND inference_attempt_id=?",
    )
    .bind(receipt_id.to_string())
    .bind(inference_attempt_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(internal)?;
    row.map(stored_from_row).transpose()
}

pub(super) async fn latest_selection(
    connection: &mut SqliteConnection,
    receipt_id: ReceiptId,
) -> Result<Option<StoredInclusion>, InboxWriteError> {
    let row = sqlx::query("SELECT * FROM coordination_inbox_inclusions WHERE receipt_id=? ORDER BY selected_at_ms DESC,inference_attempt_id DESC LIMIT 1")
        .bind(receipt_id.to_string()).fetch_optional(&mut *connection).await.map_err(internal)?;
    row.map(stored_from_row).transpose()
}

fn stored_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredInclusion, InboxWriteError> {
    Ok(StoredInclusion {
        receipt_id: ReceiptId::parse(&row.get::<String, _>("receipt_id")).map_err(corrupt)?,
        inference_attempt_id: BoundedId::new(row.get::<String, _>("inference_attempt_id"))
            .map_err(corrupt)?,
        root_thread_id: ThreadId::try_from(row.get::<String, _>("root_thread_id"))
            .map_err(corrupt)?,
        target_turn_id: BoundedId::new(row.get::<String, _>("target_turn_id")).map_err(corrupt)?,
        delivery_fingerprint: row
            .get::<Vec<u8>, _>("delivery_fingerprint")
            .try_into()
            .map_err(|_| InboxWriteError::CorruptStoredInbox)?,
        selected_at_ms: row.get("selected_at_ms"),
        lease_expires_at_ms: row.get("lease_expires_at_ms"),
        semantic_claim: match row.get::<i64, _>("semantic_claim") {
            0 => false,
            1 => true,
            _ => return Err(InboxWriteError::CorruptStoredInbox),
        },
        semantic_event_id: row
            .get::<Option<String>, _>("semantic_event_id")
            .map(|value| CoordinationEventId::parse(&value))
            .transpose()
            .map_err(corrupt)?,
        inbox_version: row
            .get::<i64, _>("inbox_version")
            .try_into()
            .map_err(corrupt)?,
        lease_epoch: row
            .get::<i64, _>("lease_epoch")
            .try_into()
            .map_err(corrupt)?,
        claim_operation_id: CoordinationOperationId::parse(
            &row.get::<String, _>("claim_operation_id"),
        )
        .map_err(corrupt)?,
        transport_state: transport_state(&row.get::<String, _>("transport_state"))?,
        transport_completed_at_ms: row.get("transport_completed_at_ms"),
        retry_after_ms: row.get("retry_after_ms"),
        version: row.get::<i64, _>("version").try_into().map_err(corrupt)?,
        failure_code: row
            .get::<Option<String>, _>("failure_code")
            .map(|value| failure_code(&value))
            .transpose()?,
    })
}

pub(super) fn committed_selection(stored: &StoredInclusion) -> CommittedInboxSelection {
    CommittedInboxSelection {
        token: InboxSelectionToken {
            receipt_id: stored.receipt_id,
            claim_operation_id: stored.claim_operation_id,
            inference_attempt_id: stored.inference_attempt_id.clone(),
            inbox_version: stored.inbox_version,
            inclusion_version: stored.version,
            lease_epoch: stored.lease_epoch,
            target_turn_id: stored.target_turn_id.clone(),
            delivery_fingerprint: stored.delivery_fingerprint,
        },
        semantic_claim: stored.semantic_claim,
        semantic_event_id: stored.semantic_event_id,
        selected_at_ms: stored.selected_at_ms,
    }
}

pub(super) fn transport_state_sql(state: TransportState) -> &'static str {
    match state {
        TransportState::Selected => "selected",
        TransportState::SendSucceeded => "sendSucceeded",
        TransportState::SendFailed => "sendFailed",
        TransportState::SendUnknown => "sendUnknown",
    }
}

fn transport_state(value: &str) -> Result<TransportState, InboxWriteError> {
    match value {
        "selected" => Ok(TransportState::Selected),
        "sendSucceeded" => Ok(TransportState::SendSucceeded),
        "sendFailed" => Ok(TransportState::SendFailed),
        "sendUnknown" => Ok(TransportState::SendUnknown),
        _ => Err(InboxWriteError::CorruptStoredInbox),
    }
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

fn failure_code(value: &str) -> Result<CoordinationFailureCode, InboxWriteError> {
    match value {
        "unauthorized" => Ok(CoordinationFailureCode::Unauthorized),
        "stateUnavailable" => Ok(CoordinationFailureCode::StateUnavailable),
        "stateQuarantined" => Ok(CoordinationFailureCode::StateQuarantined),
        "invalidPayload" => Ok(CoordinationFailureCode::InvalidPayload),
        "payloadOverLimit" => Ok(CoordinationFailureCode::PayloadOverLimit),
        "targetUnavailable" => Ok(CoordinationFailureCode::TargetUnavailable),
        "generationFenced" => Ok(CoordinationFailureCode::GenerationFenced),
        "terminalConflict" => Ok(CoordinationFailureCode::TerminalConflict),
        "ownershipConflict" => Ok(CoordinationFailureCode::OwnershipConflict),
        "idempotencyConflict" => Ok(CoordinationFailureCode::IdempotencyConflict),
        "retryExhausted" => Ok(CoordinationFailureCode::RetryExhausted),
        "corruptEvidence" => Ok(CoordinationFailureCode::CorruptEvidence),
        "internal" => Ok(CoordinationFailureCode::Internal),
        _ => Err(InboxWriteError::CorruptStoredInbox),
    }
}

fn corrupt(error: impl Into<anyhow::Error>) -> InboxWriteError {
    let _ = error.into();
    InboxWriteError::CorruptStoredInbox
}
