use codex_coordination::StateEpoch;
use sqlx::Row;
use sqlx::SqliteConnection;
use sqlx::SqlitePool;
use std::io;
use std::path::Path;

use super::authority_marker::MARKER_FILE_NAME;
use super::authority_marker::MarkerDisposition;
use super::authority_marker::MarkerRead;
use super::authority_marker::marker_epoch;
use super::authority_marker::persist_marker;
use super::authority_marker::read_marker;
use crate::FreshAfterCorruption;

const FRESH_AFTER_CORRUPTION_REASON: &str =
    "fresh_after_corruption: prior coordination authority is unavailable";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoordinationAuthorityStatus {
    Active {
        state_epoch: StateEpoch,
    },
    Quarantined {
        state_epoch: Option<StateEpoch>,
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuthorityWriteStep {
    TempWrite,
    FileSync,
    Rename,
    DirectorySync,
    AuthorityInsert,
}

/// Injects failures between durability steps without weakening the production writer.
pub(crate) trait AuthorityFailureInjector: Send + Sync {
    fn check(&self, step: AuthorityWriteStep) -> io::Result<()>;
}

pub(super) struct NoFailure;

impl AuthorityFailureInjector for NoFailure {
    fn check(&self, _step: AuthorityWriteStep) -> io::Result<()> {
        Ok(())
    }
}

struct AuthorityRow {
    epoch: StateEpoch,
    status: String,
    reason: Option<String>,
}

pub(crate) async fn initialize_authority(
    pool: &SqlitePool,
    sqlite_home: &Path,
    fresh_after_corruption: Option<FreshAfterCorruption>,
) -> CoordinationAuthorityStatus {
    match initialize_authority_with(pool, sqlite_home, fresh_after_corruption, &NoFailure).await {
        Ok(status) => status,
        Err(err) => CoordinationAuthorityStatus::Quarantined {
            state_epoch: None,
            reason: format!("coordination authority initialization failed: {err}"),
        },
    }
}

pub(crate) async fn initialize_authority_with(
    pool: &SqlitePool,
    sqlite_home: &Path,
    fresh_after_corruption: Option<FreshAfterCorruption>,
    injector: &dyn AuthorityFailureInjector,
) -> anyhow::Result<CoordinationAuthorityStatus> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let result = initialize_authority_transaction(
        &mut transaction,
        sqlite_home,
        fresh_after_corruption,
        injector,
    )
    .await;
    match result {
        Ok(status) => {
            transaction.commit().await?;
            Ok(status)
        }
        Err(err) => {
            transaction.rollback().await?;
            Err(err)
        }
    }
}

async fn initialize_authority_transaction(
    connection: &mut SqliteConnection,
    sqlite_home: &Path,
    fresh_after_corruption: Option<FreshAfterCorruption>,
    injector: &dyn AuthorityFailureInjector,
) -> anyhow::Result<CoordinationAuthorityStatus> {
    let marker_path = sqlite_home.join(MARKER_FILE_NAME);
    let marker = read_marker(marker_path.as_path()).await?;
    let authority = read_authority(connection).await?;
    let facts = coordination_fact_count(connection).await?;

    if let Some(authority) = authority {
        if authority.status == "quarantined" {
            return Ok(CoordinationAuthorityStatus::Quarantined {
                state_epoch: Some(authority.epoch),
                reason: authority
                    .reason
                    .unwrap_or_else(|| "coordination authority is quarantined".to_string()),
            });
        }
        if fresh_after_corruption.is_some()
            || matches!(
                marker,
                MarkerRead::Valid {
                    disposition: MarkerDisposition::FreshAfterCorruption,
                    ..
                }
            )
        {
            quarantine(connection, FRESH_AFTER_CORRUPTION_REASON).await?;
            return Ok(CoordinationAuthorityStatus::Quarantined {
                state_epoch: Some(authority.epoch),
                reason: FRESH_AFTER_CORRUPTION_REASON.to_string(),
            });
        }
        let mismatch = match marker {
            MarkerRead::Valid { state_epoch, .. } if state_epoch == authority.epoch => None,
            MarkerRead::Valid { .. } => {
                Some("coordination marker and DB epochs differ".to_string())
            }
            MarkerRead::Missing => {
                Some("coordination DB facts exist without an authority marker".to_string())
            }
            MarkerRead::Rejected(reason) => Some(reason),
        };
        if let Some(reason) = mismatch {
            quarantine(connection, reason.as_str()).await?;
            return Ok(CoordinationAuthorityStatus::Quarantined {
                state_epoch: Some(authority.epoch),
                reason: reason.to_string(),
            });
        }
        return Ok(CoordinationAuthorityStatus::Active {
            state_epoch: authority.epoch,
        });
    }

    if facts > 0 {
        let epoch = marker_epoch(&marker).unwrap_or_else(StateEpoch::new_v7);
        let reason = "coordination_facts_without_authority: coordination facts exist without authority metadata";
        injector.check(AuthorityWriteStep::AuthorityInsert)?;
        insert_authority(connection, epoch, Some(reason)).await?;
        return Ok(CoordinationAuthorityStatus::Quarantined {
            state_epoch: Some(epoch),
            reason: reason.to_string(),
        });
    }

    let fresh_epoch = fresh_after_corruption.map(|fresh| fresh.state_epoch);
    let (epoch, marker_to_write, quarantine_reason) = match (marker, fresh_epoch) {
        (
            MarkerRead::Valid {
                state_epoch,
                disposition: MarkerDisposition::FreshAfterCorruption,
            },
            Some(fresh_epoch),
        ) if state_epoch == fresh_epoch => (fresh_epoch, None, Some(FRESH_AFTER_CORRUPTION_REASON)),
        (MarkerRead::Valid { .. } | MarkerRead::Missing, Some(fresh_epoch)) => (
            fresh_epoch,
            Some(MarkerDisposition::FreshAfterCorruption),
            Some(FRESH_AFTER_CORRUPTION_REASON),
        ),
        (
            MarkerRead::Valid {
                state_epoch,
                disposition: MarkerDisposition::FreshAfterCorruption,
            },
            None,
        ) => (state_epoch, None, Some(FRESH_AFTER_CORRUPTION_REASON)),
        (
            MarkerRead::Valid {
                state_epoch,
                disposition: MarkerDisposition::Ordinary,
            },
            None,
        ) => (state_epoch, None, None),
        (MarkerRead::Missing, None) => (
            StateEpoch::new_v7(),
            Some(MarkerDisposition::Ordinary),
            None,
        ),
        (MarkerRead::Rejected(reason), fresh_epoch) => {
            let reason = if fresh_epoch.is_some() {
                FRESH_AFTER_CORRUPTION_REASON
            } else {
                reason.as_str()
            };
            let epoch = fresh_epoch.unwrap_or_else(StateEpoch::new_v7);
            injector.check(AuthorityWriteStep::AuthorityInsert)?;
            insert_authority(connection, epoch, Some(reason)).await?;
            return Ok(CoordinationAuthorityStatus::Quarantined {
                state_epoch: Some(epoch),
                reason: reason.to_string(),
            });
        }
    };
    if let Some(disposition) = marker_to_write {
        persist_marker(marker_path.as_path(), epoch, disposition, injector).await?;
    }
    injector.check(AuthorityWriteStep::AuthorityInsert)?;
    insert_authority(connection, epoch, quarantine_reason).await?;
    if let Some(reason) = quarantine_reason {
        Ok(CoordinationAuthorityStatus::Quarantined {
            state_epoch: Some(epoch),
            reason: reason.to_string(),
        })
    } else {
        Ok(CoordinationAuthorityStatus::Active { state_epoch: epoch })
    }
}

async fn read_authority(connection: &mut SqliteConnection) -> anyhow::Result<Option<AuthorityRow>> {
    let row = sqlx::query(
        "SELECT state_epoch, status, quarantine_reason FROM coordination_authority \
         WHERE singleton_id = 1",
    )
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let status: String = row.get("status");
    let reason: Option<String> = row.get("quarantine_reason");
    let valid = matches!((status.as_str(), reason.as_deref()), ("active", None))
        || matches!(
            (status.as_str(), reason.as_deref()),
            ("quarantined", Some(reason)) if !reason.is_empty()
        );
    if !valid {
        anyhow::bail!("coordination authority row has an invalid status/reason state");
    }
    Ok(Some(AuthorityRow {
        epoch: StateEpoch::parse(row.get::<String, _>("state_epoch").as_str())?,
        status,
        reason,
    }))
}

async fn coordination_fact_count(connection: &mut SqliteConnection) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM coordination_roots) \
         + (SELECT COUNT(*) FROM coordination_events) \
         + (SELECT COUNT(*) FROM coordination_projection_outbox)",
    )
    .fetch_one(&mut *connection)
    .await?)
}

async fn insert_authority(
    connection: &mut SqliteConnection,
    epoch: StateEpoch,
    quarantine_reason: Option<&str>,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now().timestamp_millis().max(0);
    let status = if quarantine_reason.is_some() {
        "quarantined"
    } else {
        "active"
    };
    sqlx::query(
        "INSERT INTO coordination_authority \
         (singleton_id, state_epoch, status, quarantine_reason, created_at_ms, updated_at_ms) \
         VALUES (1, ?, ?, ?, ?, ?)",
    )
    .bind(epoch.to_string())
    .bind(status)
    .bind(quarantine_reason)
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn quarantine(connection: &mut SqliteConnection, reason: &str) -> anyhow::Result<()> {
    let now = chrono::Utc::now().timestamp_millis().max(0);
    sqlx::query(
        "UPDATE coordination_authority SET status = 'quarantined', \
         quarantine_reason = ?, updated_at_ms = MAX(updated_at_ms, ?) \
         WHERE singleton_id = 1 AND status = 'active'",
    )
    .bind(reason)
    .bind(now)
    .execute(&mut *connection)
    .await?;
    Ok(())
}
