use sqlx::SqliteConnection;

use super::aggregate_journal::authority;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::inbox::internal;
use super::inbox_rows::InboxPayloadAccess;
use super::inbox_rows::TERMINAL_INBOX_TTL_MS;
use super::inbox_rows::load_inbox_by_receipt;
use super::inclusion_rows::StoredInclusion;
use super::inclusion_rows::TransportState;
use super::inclusion_rows::failure_code_sql;
use super::inclusion_rows::load_selection;
use super::inclusion_rows::transport_state_sql;
use crate::model::coordination_inbox::InboxInputError;
use crate::model::coordination_inbox::InboxLifecycle;
use crate::model::coordination_inbox::InboxTransportResolution;
use crate::model::coordination_inbox::RecordInboxTransportOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcomeResult;

pub(super) async fn record_transport_outcome(
    connection: &mut SqliteConnection,
    params: RecordInboxTransportOutcome,
    injector: &dyn InboxFailureInjector,
) -> Result<RecordInboxTransportOutcomeResult, InboxWriteError> {
    authority(connection, injector).await?;
    validate_resolution(&params)?;
    let inclusion = load_selection(
        connection,
        params.selection.receipt_id,
        &params.selection.inference_attempt_id,
    )
    .await?
    .ok_or(InboxWriteError::IdentityConflict)?;
    let inbox = load_inbox_by_receipt(
        connection,
        params.selection.receipt_id,
        InboxPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(InboxWriteError::CorruptStoredInbox)?;
    validate_token(&params, &inclusion)?;
    if inclusion.transport_state != TransportState::Selected {
        validate_duplicate_outcome(&params, &inclusion)?;
        return Ok(RecordInboxTransportOutcomeResult::Duplicate(inbox.metadata));
    }
    let Some(lease_expires_at_ms) = inbox.lease_expires_at_ms else {
        return Ok(RecordInboxTransportOutcomeResult::Fenced);
    };
    if inclusion.lease_expires_at_ms != lease_expires_at_ms
        || params.completed_at_ms >= lease_expires_at_ms
        || params.completed_at_ms >= inbox.metadata.expires_at_ms
    {
        return Ok(RecordInboxTransportOutcomeResult::Expired);
    }
    if inbox.metadata.lifecycle != InboxLifecycle::Selected
        || inbox.metadata.version != params.selection.inbox_version
        || inbox.metadata.lease_epoch != params.selection.lease_epoch
        || inbox.metadata.delivery_fingerprint != params.selection.delivery_fingerprint
        || inbox.lease_claim_operation_id != Some(params.selection.claim_operation_id)
    {
        return Ok(RecordInboxTransportOutcomeResult::Fenced);
    }
    let (transport, failure_code, retry_at_ms) = resolution_fields(&params.resolution);
    let changed = sqlx::query("UPDATE coordination_inbox_inclusions SET transport_state=?,transport_completed_at_ms=?,retry_after_ms=?,version=version+1,failure_code=? WHERE receipt_id=? AND inference_attempt_id=? AND transport_state='selected' AND version=?")
        .bind(transport_state_sql(transport))
        .bind(params.completed_at_ms)
        .bind(retry_at_ms)
        .bind(failure_code)
        .bind(params.selection.receipt_id.to_string())
        .bind(params.selection.inference_attempt_id.as_str())
        .bind(i64::try_from(params.selection.inclusion_version).map_err(|_| InboxWriteError::LeaseFenced)?)
        .execute(&mut *connection).await.map_err(internal)?.rows_affected();
    if changed != 1 {
        return Ok(RecordInboxTransportOutcomeResult::Fenced);
    }
    injector
        .after_inbox_step(InboxStep::SelectionUpdate)
        .map_err(internal)?;
    update_inbox(connection, &params, &inbox, failure_code).await?;
    injector
        .after_inbox_step(InboxStep::InboxUpdate)
        .map_err(internal)?;
    let updated = load_inbox_by_receipt(
        connection,
        params.selection.receipt_id,
        InboxPayloadAccess::MetadataOnly,
    )
    .await?
    .ok_or(InboxWriteError::CorruptStoredInbox)?;
    Ok(RecordInboxTransportOutcomeResult::Applied(updated.metadata))
}

fn validate_resolution(params: &RecordInboxTransportOutcome) -> Result<(), InboxWriteError> {
    if params.completed_at_ms < 0 {
        return Err(InboxInputError::InvalidRetryDeadline.into());
    }
    let retry = match params.resolution {
        InboxTransportResolution::SendSucceeded => None,
        InboxTransportResolution::SendFailed { retry_at_ms, .. }
        | InboxTransportResolution::SendUnknown { retry_at_ms } => Some(retry_at_ms),
    };
    if retry.is_some_and(|retry| retry < params.completed_at_ms) {
        return Err(InboxInputError::InvalidRetryDeadline.into());
    }
    Ok(())
}

fn validate_token(
    params: &RecordInboxTransportOutcome,
    inclusion: &StoredInclusion,
) -> Result<(), InboxWriteError> {
    let expected_version = if inclusion.transport_state == TransportState::Selected {
        params.selection.inclusion_version
    } else {
        params
            .selection
            .inclusion_version
            .checked_add(1)
            .ok_or(InboxWriteError::LeaseFenced)?
    };
    if inclusion.receipt_id != params.selection.receipt_id
        || inclusion.inference_attempt_id != params.selection.inference_attempt_id
        || inclusion.target_turn_id != params.selection.target_turn_id
        || inclusion.delivery_fingerprint != params.selection.delivery_fingerprint
        || inclusion.inbox_version != params.selection.inbox_version
        || inclusion.lease_epoch != params.selection.lease_epoch
        || inclusion.claim_operation_id != params.selection.claim_operation_id
        || inclusion.version != expected_version
    {
        return Err(InboxWriteError::IdempotencyConflict);
    }
    Ok(())
}

fn validate_duplicate_outcome(
    params: &RecordInboxTransportOutcome,
    inclusion: &StoredInclusion,
) -> Result<(), InboxWriteError> {
    let (state, failure, retry) = resolution_fields(&params.resolution);
    let stored_failure = inclusion.failure_code.map(failure_code_sql);
    if inclusion.transport_state != state
        || inclusion.transport_completed_at_ms != Some(params.completed_at_ms)
        || inclusion.retry_after_ms != retry
        || stored_failure != failure
    {
        return Err(InboxWriteError::TerminalConflict);
    }
    Ok(())
}

fn resolution_fields(
    resolution: &InboxTransportResolution,
) -> (TransportState, Option<&'static str>, Option<i64>) {
    match resolution {
        InboxTransportResolution::SendSucceeded => (TransportState::SendSucceeded, None, None),
        InboxTransportResolution::SendFailed { code, retry_at_ms } => (
            TransportState::SendFailed,
            Some(failure_code_sql(*code)),
            Some(*retry_at_ms),
        ),
        InboxTransportResolution::SendUnknown { retry_at_ms } => {
            (TransportState::SendUnknown, None, Some(*retry_at_ms))
        }
    }
}

async fn update_inbox(
    connection: &mut SqliteConnection,
    params: &RecordInboxTransportOutcome,
    inbox: &super::inbox_rows::StoredInbox,
    failure_code: Option<&str>,
) -> Result<(), InboxWriteError> {
    let expected_version =
        i64::try_from(params.selection.inbox_version).map_err(|_| InboxWriteError::LeaseFenced)?;
    let expected_epoch =
        i64::try_from(params.selection.lease_epoch).map_err(|_| InboxWriteError::LeaseFenced)?;
    let affected = match &params.resolution {
        InboxTransportResolution::SendSucceeded => {
            let terminal_expiry = params
                .completed_at_ms
                .saturating_add(TERMINAL_INBOX_TTL_MS)
                .min(inbox.metadata.expires_at_ms);
            sqlx::query("UPDATE coordination_inbox SET lifecycle='processed',version=version+1,lease_expires_at_ms=NULL,lease_claim_operation_id=NULL,failure_code=NULL,terminal_at_ms=?,expires_at_ms=?,updated_at_ms=? WHERE receipt_id=? AND lifecycle='selected' AND version=? AND lease_epoch=?")
                .bind(params.completed_at_ms).bind(terminal_expiry).bind(params.completed_at_ms)
                .bind(params.selection.receipt_id.to_string()).bind(expected_version).bind(expected_epoch)
                .execute(&mut *connection).await.map_err(internal)?.rows_affected()
        }
        InboxTransportResolution::SendFailed { retry_at_ms, .. }
        | InboxTransportResolution::SendUnknown { retry_at_ms } => {
            if params.completed_at_ms >= inbox.metadata.expires_at_ms
                || *retry_at_ms >= inbox.metadata.expires_at_ms
            {
                sqlx::query("UPDATE coordination_inbox SET lifecycle='expired',version=version+1,retry_count=retry_count+1,lease_expires_at_ms=NULL,lease_claim_operation_id=NULL,retry_after_ms=?,failure_code=?,ciphertext=NULL,purged_at_ms=?,updated_at_ms=? WHERE receipt_id=? AND lifecycle='selected' AND version=? AND lease_epoch=?")
                    .bind(*retry_at_ms).bind(failure_code).bind(params.completed_at_ms).bind(params.completed_at_ms)
                    .bind(params.selection.receipt_id.to_string()).bind(expected_version).bind(expected_epoch)
                    .execute(&mut *connection).await.map_err(internal)?.rows_affected()
            } else {
                sqlx::query("UPDATE coordination_inbox SET lifecycle='received',version=version+1,retry_count=retry_count+1,lease_expires_at_ms=NULL,lease_claim_operation_id=NULL,retry_after_ms=?,failure_code=?,updated_at_ms=? WHERE receipt_id=? AND lifecycle='selected' AND version=? AND lease_epoch=?")
                    .bind(*retry_at_ms).bind(failure_code).bind(params.completed_at_ms)
                    .bind(params.selection.receipt_id.to_string()).bind(expected_version).bind(expected_epoch)
                    .execute(&mut *connection).await.map_err(internal)?.rows_affected()
            }
        }
    };
    if affected != 1 {
        return Err(InboxWriteError::LeaseFenced);
    }
    Ok(())
}
