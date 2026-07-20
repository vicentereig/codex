use codex_coordination::ReceiptId;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::aggregate_journal::authority;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::inbox::internal;
use crate::model::coordination_inbox::InboxMaintenanceBatch;
use crate::model::coordination_inbox::InboxMaintenanceOutcome;

pub(super) async fn reclaim_leases(
    connection: &mut SqliteConnection,
    params: InboxMaintenanceBatch,
    injector: &dyn InboxFailureInjector,
) -> Result<InboxMaintenanceOutcome, InboxWriteError> {
    params.validate()?;
    authority(connection, injector).await?;
    let rows = sqlx::query("SELECT receipt_id,lifecycle,expires_at_ms FROM coordination_inbox WHERE lifecycle IN ('leased','selected') AND lease_expires_at_ms<=? ORDER BY lease_expires_at_ms,receipt_id LIMIT ?")
        .bind(params.now_ms).bind(i64::from(params.limit)).fetch_all(&mut *connection).await.map_err(internal)?;
    let mut changed_receipts = Vec::with_capacity(rows.len());
    for row in rows {
        let receipt = ReceiptId::parse(&row.get::<String, _>("receipt_id"))
            .map_err(|_| InboxWriteError::CorruptStoredInbox)?;
        let lifecycle: String = row.get("lifecycle");
        let expires_at_ms: i64 = row.get("expires_at_ms");
        if lifecycle == "selected" {
            let changed = sqlx::query("UPDATE coordination_inbox_inclusions SET transport_state='sendUnknown',transport_completed_at_ms=?,retry_after_ms=?,version=version+1,failure_code=NULL WHERE receipt_id=? AND transport_state='selected'")
                .bind(params.now_ms).bind(params.now_ms).bind(receipt.to_string())
                .execute(&mut *connection).await.map_err(internal)?.rows_affected();
            if changed != 1 {
                return Err(InboxWriteError::CorruptStoredInbox);
            }
            injector
                .after_inbox_step(InboxStep::SelectionUpdate)
                .map_err(internal)?;
        }
        let affected = if params.now_ms >= expires_at_ms {
            sqlx::query("UPDATE coordination_inbox SET lifecycle='expired',version=version+1,retry_count=retry_count+?,lease_expires_at_ms=NULL,lease_claim_operation_id=NULL,retry_after_ms=?,failure_code=CASE WHEN lifecycle='selected' THEN NULL ELSE failure_code END,ciphertext=NULL,purged_at_ms=?,updated_at_ms=? WHERE receipt_id=? AND lifecycle=? AND lease_expires_at_ms<=?")
                .bind(i64::from(lifecycle == "selected")).bind(params.now_ms)
                .bind(params.now_ms).bind(params.now_ms).bind(receipt.to_string()).bind(&lifecycle).bind(params.now_ms)
                .execute(&mut *connection).await.map_err(internal)?.rows_affected()
        } else {
            sqlx::query("UPDATE coordination_inbox SET lifecycle='received',version=version+1,retry_count=retry_count+?,lease_expires_at_ms=NULL,lease_claim_operation_id=NULL,retry_after_ms=?,failure_code=CASE WHEN lifecycle='selected' THEN NULL ELSE failure_code END,updated_at_ms=? WHERE receipt_id=? AND lifecycle=? AND lease_expires_at_ms<=?")
                .bind(i64::from(lifecycle == "selected")).bind(params.now_ms)
                .bind(params.now_ms).bind(receipt.to_string()).bind(&lifecycle).bind(params.now_ms)
                .execute(&mut *connection).await.map_err(internal)?.rows_affected()
        };
        if affected != 1 {
            return Err(InboxWriteError::LeaseFenced);
        }
        injector
            .after_inbox_step(InboxStep::MaintenanceUpdate)
            .map_err(internal)?;
        changed_receipts.push(receipt);
    }
    Ok(InboxMaintenanceOutcome { changed_receipts })
}

pub(super) async fn expire_payloads(
    connection: &mut SqliteConnection,
    params: InboxMaintenanceBatch,
    injector: &dyn InboxFailureInjector,
) -> Result<InboxMaintenanceOutcome, InboxWriteError> {
    params.validate()?;
    authority(connection, injector).await?;
    let rows = sqlx::query("SELECT receipt_id,lifecycle FROM coordination_inbox WHERE ciphertext IS NOT NULL AND expires_at_ms<=? ORDER BY expires_at_ms,receipt_id LIMIT ?")
        .bind(params.now_ms).bind(i64::from(params.limit)).fetch_all(&mut *connection).await.map_err(internal)?;
    let mut changed_receipts = Vec::with_capacity(rows.len());
    for row in rows {
        let receipt = ReceiptId::parse(&row.get::<String, _>("receipt_id"))
            .map_err(|_| InboxWriteError::CorruptStoredInbox)?;
        let lifecycle: String = row.get("lifecycle");
        if lifecycle == "selected" {
            let changed = sqlx::query("UPDATE coordination_inbox_inclusions SET transport_state='sendUnknown',transport_completed_at_ms=?,retry_after_ms=?,version=version+1,failure_code=NULL WHERE receipt_id=? AND transport_state='selected'")
                .bind(params.now_ms).bind(params.now_ms).bind(receipt.to_string())
                .execute(&mut *connection).await.map_err(internal)?.rows_affected();
            if changed != 1 {
                return Err(InboxWriteError::CorruptStoredInbox);
            }
            injector
                .after_inbox_step(InboxStep::SelectionUpdate)
                .map_err(internal)?;
        }
        let terminal = matches!(lifecycle.as_str(), "processed" | "poisoned");
        let new_lifecycle = if terminal {
            lifecycle.as_str()
        } else {
            "expired"
        };
        let affected = sqlx::query("UPDATE coordination_inbox SET lifecycle=?,version=version+1,retry_count=retry_count+?,lease_expires_at_ms=NULL,lease_claim_operation_id=NULL,retry_after_ms=CASE WHEN lifecycle IN ('processed','poisoned') THEN retry_after_ms ELSE MAX(retry_after_ms,?) END,failure_code=CASE WHEN lifecycle='selected' THEN NULL ELSE failure_code END,ciphertext=NULL,purged_at_ms=?,updated_at_ms=? WHERE receipt_id=? AND lifecycle=? AND ciphertext IS NOT NULL AND expires_at_ms<=?")
            .bind(new_lifecycle).bind(i64::from(lifecycle == "selected")).bind(params.now_ms)
            .bind(params.now_ms).bind(params.now_ms).bind(receipt.to_string()).bind(&lifecycle).bind(params.now_ms)
            .execute(&mut *connection).await.map_err(internal)?.rows_affected();
        if affected != 1 {
            return Err(InboxWriteError::LeaseFenced);
        }
        injector
            .after_inbox_step(InboxStep::MaintenanceUpdate)
            .map_err(internal)?;
        changed_receipts.push(receipt);
    }
    Ok(InboxMaintenanceOutcome { changed_receipts })
}
