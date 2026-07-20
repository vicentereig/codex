use std::io;
use std::path::Path;

use codex_coordination::StateEpoch;
use pretty_assertions::assert_eq;
use sqlx::Row;
use sqlx::SqlitePool;

use super::authority::AuthorityFailureInjector;
use super::authority::AuthorityWriteStep;
use super::authority::CoordinationAuthorityStatus;
use super::authority::NoFailure;
use super::authority::initialize_authority;
use super::authority::initialize_authority_with;
use super::authority_marker::MARKER_FILE_NAME;
use super::authority_marker::MarkerDisposition;
use super::authority_marker::persist_marker;
use crate::SqliteConfig;
use crate::backup_state_db_with_fresh_start_provenance;
use crate::migrations::runtime_state_migrator;
use crate::runtime::test_support::unique_temp_dir;
use crate::state_db_path;

struct FailAt(AuthorityWriteStep);

impl AuthorityFailureInjector for FailAt {
    fn check(&self, step: AuthorityWriteStep) -> io::Result<()> {
        if step == self.0 {
            Err(io::Error::other(format!("injected failure at {step:?}")))
        } else {
            Ok(())
        }
    }
}

async fn migrated_pool(sqlite_home: &Path) -> anyhow::Result<SqlitePool> {
    tokio::fs::create_dir_all(sqlite_home).await?;
    let sqlite = SqliteConfig::new_for_testing(sqlite_home.to_path_buf());
    let pool = sqlite
        .open_read_write_pool(state_db_path(sqlite_home).as_path())
        .await?;
    runtime_state_migrator().run(&pool).await?;
    Ok(pool)
}

async fn fresh_provenance(sqlite_home: &Path) -> anyhow::Result<crate::FreshAfterCorruption> {
    let recovery_home = sqlite_home.join("recovery-source");
    tokio::fs::create_dir_all(recovery_home.as_path()).await?;
    let path = state_db_path(recovery_home.as_path());
    tokio::fs::write(path.as_path(), b"corrupt primary").await?;
    backup_state_db_with_fresh_start_provenance(path.as_path())
        .await?
        .provenance
        .ok_or_else(|| anyhow::anyhow!("primary backup should provide provenance"))
}

fn active_epoch(status: CoordinationAuthorityStatus) -> StateEpoch {
    match status {
        CoordinationAuthorityStatus::Active { state_epoch } => state_epoch,
        CoordinationAuthorityStatus::Quarantined { reason, .. } => {
            panic!("expected active authority, got quarantine: {reason}")
        }
    }
}

fn quarantined_epoch(status: CoordinationAuthorityStatus) -> StateEpoch {
    match status {
        CoordinationAuthorityStatus::Quarantined {
            state_epoch: Some(state_epoch),
            ..
        } => state_epoch,
        status => panic!("expected persisted quarantine, got {status:?}"),
    }
}

#[tokio::test]
async fn empty_startup_creates_durable_authority_and_reuses_it() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;

    let first = active_epoch(
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?,
    );
    let second = active_epoch(
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?,
    );

    assert_eq!(second, first);
    assert!(tokio::fs::try_exists(sqlite_home.join(MARKER_FILE_NAME)).await?);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn marker_only_precommit_residue_activates_exact_epoch() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;

    let failed = initialize_authority_with(
        &pool,
        sqlite_home.as_path(),
        None,
        &FailAt(AuthorityWriteStep::AuthorityInsert),
    )
    .await;
    assert!(failed.is_err());
    let marker: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(sqlite_home.join(MARKER_FILE_NAME)).await?)?;
    let marker_epoch = StateEpoch::parse(
        marker["stateEpoch"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("marker should contain stateEpoch"))?,
    )?;

    let restarted = active_epoch(
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?,
    );

    assert_eq!(restarted, marker_epoch);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn failures_before_commit_restart_safely() -> anyhow::Result<()> {
    for step in [
        AuthorityWriteStep::TempWrite,
        AuthorityWriteStep::FileSync,
        AuthorityWriteStep::Rename,
        AuthorityWriteStep::DirectorySync,
        AuthorityWriteStep::AuthorityInsert,
    ] {
        let sqlite_home = unique_temp_dir();
        let pool = migrated_pool(sqlite_home.as_path()).await?;
        assert!(
            initialize_authority_with(&pool, sqlite_home.as_path(), None, &FailAt(step))
                .await
                .is_err(),
            "{step:?} should fail"
        );

        let restarted =
            initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;
        assert!(
            matches!(restarted, CoordinationAuthorityStatus::Active { .. }),
            "restart after {step:?} should be active: {restarted:?}"
        );
        pool.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn fresh_after_corruption_never_adopts_surviving_marker() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;
    let old_epoch = StateEpoch::new_v7();
    persist_marker(
        sqlite_home.join(MARKER_FILE_NAME).as_path(),
        old_epoch,
        MarkerDisposition::Ordinary,
        &NoFailure,
    )
    .await?;
    let provenance = fresh_provenance(sqlite_home.as_path()).await?;

    let quarantined = quarantined_epoch(
        initialize_authority_with(&pool, sqlite_home.as_path(), Some(provenance), &NoFailure)
            .await?,
    );
    assert_ne!(quarantined, old_epoch);
    let restarted =
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;
    assert_eq!(quarantined_epoch(restarted), quarantined);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn malformed_marker_is_quarantined_without_overwrite() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;
    let marker_path = sqlite_home.join(MARKER_FILE_NAME);
    let malformed = b"{\"version\":1,\"unknown\":true}";
    tokio::fs::write(marker_path.as_path(), malformed).await?;

    let status = initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;

    assert!(matches!(
        status,
        CoordinationAuthorityStatus::Quarantined { .. }
    ));
    assert_eq!(tokio::fs::read(marker_path).await?, malformed);
    pool.close().await;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_marker_is_quarantined_without_following_it() -> anyhow::Result<()> {
    use std::os::unix::fs::symlink;

    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;
    let target = sqlite_home.join("target.json");
    tokio::fs::write(target.as_path(), b"target stays untouched").await?;
    symlink(target.as_path(), sqlite_home.join(MARKER_FILE_NAME))?;

    let status = initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;

    assert!(matches!(
        status,
        CoordinationAuthorityStatus::Quarantined { .. }
    ));
    assert_eq!(tokio::fs::read(target).await?, b"target stays untouched");
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn active_authority_without_marker_is_persistently_quarantined() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;
    active_epoch(initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?);
    tokio::fs::remove_file(sqlite_home.join(MARKER_FILE_NAME)).await?;

    let quarantined =
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;
    let restarted =
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;

    assert!(matches!(
        quarantined,
        CoordinationAuthorityStatus::Quarantined { .. }
    ));
    assert_eq!(restarted, quarantined);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn marker_and_database_epoch_mismatch_is_quarantined() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;
    let authority_epoch = active_epoch(
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?,
    );
    let different_epoch = StateEpoch::new_v7();
    persist_marker(
        sqlite_home.join(MARKER_FILE_NAME).as_path(),
        different_epoch,
        MarkerDisposition::Ordinary,
        &NoFailure,
    )
    .await?;

    let quarantined = quarantined_epoch(
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?,
    );

    assert_eq!(quarantined, authority_epoch);
    assert_ne!(quarantined, different_epoch);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn existing_quarantine_is_terminal_across_restarts() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;
    active_epoch(initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?);
    tokio::fs::remove_file(sqlite_home.join(MARKER_FILE_NAME)).await?;
    initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;
    let before = authority_row(&pool).await?;

    let provenance = fresh_provenance(sqlite_home.as_path()).await?;
    let restarted =
        initialize_authority_with(&pool, sqlite_home.as_path(), Some(provenance), &NoFailure)
            .await?;

    assert!(matches!(
        restarted,
        CoordinationAuthorityStatus::Quarantined { .. }
    ));
    assert_eq!(authority_row(&pool).await?, before);
    pool.close().await;
    Ok(())
}

async fn authority_row(pool: &SqlitePool) -> anyhow::Result<(String, String, String, i64)> {
    let row = sqlx::query(
        "SELECT state_epoch, status, quarantine_reason, updated_at_ms \
         FROM coordination_authority WHERE singleton_id = 1",
    )
    .fetch_one(pool)
    .await?;
    Ok((
        row.get("state_epoch"),
        row.get("status"),
        row.get("quarantine_reason"),
        row.get("updated_at_ms"),
    ))
}

#[tokio::test]
async fn oversized_unsupported_and_non_v7_markers_are_preserved() -> anyhow::Result<()> {
    let invalid_markers = [
        vec![b'x'; 513],
        format!(
            "{{\"version\":2,\"stateEpoch\":\"{}\",\"disposition\":\"ordinary\"}}",
            StateEpoch::new_v7()
        )
        .into_bytes(),
        b"{\"version\":1,\"stateEpoch\":\"550e8400-e29b-41d4-a716-446655440000\",\"disposition\":\"ordinary\"}".to_vec(),
    ];
    for invalid in invalid_markers {
        let sqlite_home = unique_temp_dir();
        let pool = migrated_pool(sqlite_home.as_path()).await?;
        let marker_path = sqlite_home.join(MARKER_FILE_NAME);
        tokio::fs::write(marker_path.as_path(), invalid.as_slice()).await?;

        let status =
            initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;

        assert!(matches!(
            status,
            CoordinationAuthorityStatus::Quarantined { .. }
        ));
        assert_eq!(tokio::fs::read(marker_path).await?, invalid);
        pool.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn facts_without_authority_are_persistently_quarantined() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;
    let mut connection = pool.acquire().await?;
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await?;
    sqlx::query(
        "INSERT INTO coordination_roots \
         (root_thread_id, state_epoch, committed_revision, published_revision, created_at_ms, updated_at_ms) \
         VALUES (?, ?, 0, 0, 1, 1)",
    )
    .bind(StateEpoch::new_v7().to_string())
    .bind(StateEpoch::new_v7().to_string())
    .execute(&mut *connection)
    .await?;
    drop(connection);

    let first = initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;
    let second = initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;

    assert!(matches!(
        first,
        CoordinationAuthorityStatus::Quarantined { .. }
    ));
    assert_eq!(second, first);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn concurrent_initializers_serialize_to_one_epoch() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;

    let (first, second) = tokio::join!(
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure),
        initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure),
    );

    assert_eq!(active_epoch(first?), active_epoch(second?));
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn fresh_after_corruption_without_marker_stays_quarantined() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    let pool = migrated_pool(sqlite_home.as_path()).await?;
    let provenance = fresh_provenance(sqlite_home.as_path()).await?;

    let first =
        initialize_authority_with(&pool, sqlite_home.as_path(), Some(provenance), &NoFailure)
            .await?;
    let second = initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;

    assert!(tokio::fs::try_exists(sqlite_home.join(MARKER_FILE_NAME)).await?);
    assert!(matches!(
        first,
        CoordinationAuthorityStatus::Quarantined { .. }
    ));
    assert_eq!(second, first);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn fresh_insert_failure_survives_without_retry_token() -> anyhow::Result<()> {
    for starts_with_ordinary_marker in [false, true] {
        let sqlite_home = unique_temp_dir();
        let pool = migrated_pool(sqlite_home.as_path()).await?;
        if starts_with_ordinary_marker {
            persist_marker(
                sqlite_home.join(MARKER_FILE_NAME).as_path(),
                StateEpoch::new_v7(),
                MarkerDisposition::Ordinary,
                &NoFailure,
            )
            .await?;
        }
        let provenance = fresh_provenance(sqlite_home.as_path()).await?;
        let fresh_epoch = provenance.state_epoch;

        assert!(
            initialize_authority_with(
                &pool,
                sqlite_home.as_path(),
                Some(provenance),
                &FailAt(AuthorityWriteStep::AuthorityInsert),
            )
            .await
            .is_err()
        );
        let restarted =
            initialize_authority_with(&pool, sqlite_home.as_path(), None, &NoFailure).await?;

        assert_eq!(quarantined_epoch(restarted), fresh_epoch);
        pool.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn invalid_authority_status_reason_state_fails_closed() -> anyhow::Result<()> {
    for (status, reason) in [("active", Some("invalid reason")), ("quarantined", None)] {
        let sqlite_home = unique_temp_dir();
        let pool = migrated_pool(sqlite_home.as_path()).await?;
        let mut connection = pool.acquire().await?;
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(&mut *connection)
            .await?;
        sqlx::query(
            "INSERT INTO coordination_authority \
             (singleton_id, state_epoch, status, quarantine_reason, created_at_ms, updated_at_ms) \
             VALUES (1, ?, ?, ?, 1, 1)",
        )
        .bind(StateEpoch::new_v7().to_string())
        .bind(status)
        .bind(reason)
        .execute(&mut *connection)
        .await?;
        drop(connection);

        let result = initialize_authority(&pool, sqlite_home.as_path(), None).await;

        assert!(matches!(
            result,
            CoordinationAuthorityStatus::Quarantined {
                state_epoch: None,
                ..
            }
        ));
        pool.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn state_runtime_fresh_bridge_persists_quarantine_and_remains_usable() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let state_path = state_db_path(sqlite_home.as_path());
    tokio::fs::write(state_path.as_path(), b"corrupt primary").await?;
    let fresh_start = backup_state_db_with_fresh_start_provenance(state_path.as_path()).await?;
    let provenance = fresh_start
        .provenance
        .ok_or_else(|| anyhow::anyhow!("state backup should mint provenance"))?;

    let runtime = crate::StateRuntime::init_fresh_after_corruption(
        sqlite_home,
        "test-provider".to_string(),
        provenance,
    )
    .await?;

    assert!(matches!(
        runtime.coordination_authority(),
        CoordinationAuthorityStatus::Quarantined { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM coordination_authority WHERE singleton_id = 1"
        )
        .fetch_one(runtime.pool.as_ref())
        .await?,
        "quarantined"
    );
    runtime.get_backfill_state().await?;
    runtime.close().await;
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn windows_marker_write_uses_real_replace_and_directory_flush() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let marker_path = sqlite_home.join(MARKER_FILE_NAME);

    persist_marker(
        marker_path.as_path(),
        StateEpoch::new_v7(),
        MarkerDisposition::Ordinary,
        &NoFailure,
    )
    .await?;
    persist_marker(
        marker_path.as_path(),
        StateEpoch::new_v7(),
        MarkerDisposition::Ordinary,
        &NoFailure,
    )
    .await?;

    assert!(tokio::fs::metadata(marker_path).await?.is_file());
    Ok(())
}
