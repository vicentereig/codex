use super::*;
use crate::runtime::test_support::unique_temp_dir;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn backup_moves_only_requested_runtime_db_files_to_backup_folder() -> std::io::Result<()> {
    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let runtime_paths = super::super::runtime_db_paths(sqlite_home.as_path());
    let mut expected_paths = Vec::new();
    for db_path in runtime_paths.iter().map(|db| db.path.as_path()) {
        for path in sqlite_paths(db_path) {
            tokio::fs::write(path.as_path(), path.display().to_string()).await?;
            expected_paths.push(path);
        }
    }
    let failed_db_path = super::super::logs_db_path(sqlite_home.as_path());
    let failed_paths = sqlite_paths(failed_db_path.as_path());

    let backups = backup_runtime_db_for_fresh_start(failed_db_path.as_path()).await?;

    assert_eq!(backups.len(), failed_paths.len());
    for path in &failed_paths {
        assert!(!tokio::fs::try_exists(path.as_path()).await?);
    }
    for path in expected_paths
        .iter()
        .filter(|path| !failed_paths.contains(path))
    {
        assert!(tokio::fs::try_exists(path.as_path()).await?);
    }
    for backup in backups {
        assert!(
            backup
                .backup_path
                .starts_with(sqlite_home.join(BACKUP_DIR_NAME))
        );
        assert!(tokio::fs::try_exists(backup.backup_path.as_path()).await?);
    }
    Ok(())
}

#[tokio::test]
async fn backup_replaces_blocking_sqlite_home_file() -> std::io::Result<()> {
    let temp_dir = unique_temp_dir();
    tokio::fs::create_dir_all(temp_dir.as_path()).await?;
    let sqlite_home = temp_dir.join("sqlite-home");
    tokio::fs::write(sqlite_home.as_path(), b"not-a-directory").await?;

    let backups = backup_runtime_db_for_fresh_start(
        super::super::state_db_path(sqlite_home.as_path()).as_path(),
    )
    .await?;

    assert_eq!(backups.len(), 1);
    assert!(tokio::fs::metadata(sqlite_home.as_path()).await?.is_dir());
    assert!(
        backups[0]
            .backup_path
            .starts_with(temp_dir.join(format!("sqlite-home.{BACKUP_DIR_NAME}")))
    );
    assert!(tokio::fs::try_exists(backups[0].backup_path.as_path()).await?);
    Ok(())
}

#[tokio::test]
async fn primary_database_backup_mints_fresh_after_corruption() -> std::io::Result<()> {
    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let state_path = super::super::state_db_path(sqlite_home.as_path());
    tokio::fs::write(state_path.as_path(), b"primary").await?;

    let fresh_start = backup_state_db_with_fresh_start_provenance(state_path.as_path()).await?;

    assert!(fresh_start.provenance.is_some());
    assert_eq!(fresh_start.backups.len(), 1);
    Ok(())
}

#[tokio::test]
async fn sidecar_only_backup_mints_fresh_after_corruption() -> std::io::Result<()> {
    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let state_path = super::super::state_db_path(sqlite_home.as_path());
    let wal_path = sqlite_paths(state_path.as_path()).remove(1);
    tokio::fs::write(wal_path.as_path(), b"orphaned wal").await?;

    let fresh_start = backup_state_db_with_fresh_start_provenance(state_path.as_path()).await?;

    assert!(fresh_start.provenance.is_some());
    assert_eq!(
        fresh_start
            .backups
            .iter()
            .map(|backup| backup.original_path.clone())
            .collect::<Vec<_>>(),
        vec![wal_path]
    );
    Ok(())
}

#[tokio::test]
async fn blocking_sqlite_home_uses_generic_backup_without_provenance() -> std::io::Result<()> {
    let temp_dir = unique_temp_dir();
    tokio::fs::create_dir_all(temp_dir.as_path()).await?;
    let sqlite_home = temp_dir.join("sqlite-home");
    tokio::fs::write(sqlite_home.as_path(), b"not-a-directory").await?;

    let backups = backup_runtime_db_for_fresh_start(
        super::super::state_db_path(sqlite_home.as_path()).as_path(),
    )
    .await?;

    assert_eq!(backups.len(), 1);
    Ok(())
}

#[tokio::test]
async fn failed_state_backup_leaves_fresh_marker_that_forces_quarantine() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let state_path = super::super::state_db_path(sqlite_home.as_path());

    assert!(
        backup_state_db_with_fresh_start_provenance(state_path.as_path())
            .await
            .is_err()
    );
    let runtime =
        super::super::StateRuntime::init(sqlite_home, "test-provider".to_string()).await?;

    assert!(matches!(
        runtime.coordination_authority(),
        super::super::CoordinationAuthorityStatus::Quarantined { .. }
    ));
    runtime.close().await;
    Ok(())
}

#[tokio::test]
async fn malformed_marker_is_preserved_while_corrupt_state_is_backed_up() -> anyhow::Result<()> {
    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let state_path = super::super::state_db_path(sqlite_home.as_path());
    tokio::fs::write(state_path.as_path(), b"corrupt primary").await?;
    let marker_path = sqlite_home.join(super::super::coordination::MARKER_FILE_NAME);
    let malformed = b"{malformed marker";
    tokio::fs::write(marker_path.as_path(), malformed).await?;

    let fresh_start = backup_state_db_with_fresh_start_provenance(state_path.as_path()).await?;
    assert_eq!(tokio::fs::read(marker_path.as_path()).await?, malformed);
    let provenance = fresh_start
        .provenance
        .ok_or_else(|| anyhow::anyhow!("state backup should mint provenance"))?;
    let runtime = super::super::StateRuntime::init_fresh_after_corruption(
        sqlite_home,
        "test-provider".to_string(),
        provenance,
    )
    .await?;

    assert!(matches!(
        runtime.coordination_authority(),
        super::super::CoordinationAuthorityStatus::Quarantined { .. }
    ));
    runtime.get_backfill_state().await?;
    runtime.close().await;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_marker_is_preserved_while_corrupt_state_is_backed_up() -> anyhow::Result<()> {
    use std::os::unix::fs::symlink;

    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let state_path = super::super::state_db_path(sqlite_home.as_path());
    tokio::fs::write(state_path.as_path(), b"corrupt primary").await?;
    let target = sqlite_home.join("marker-target");
    tokio::fs::write(target.as_path(), b"preserved target").await?;
    let marker_path = sqlite_home.join(super::super::coordination::MARKER_FILE_NAME);
    symlink(target.as_path(), marker_path.as_path())?;

    let fresh_start = backup_state_db_with_fresh_start_provenance(state_path.as_path()).await?;
    assert!(
        tokio::fs::symlink_metadata(marker_path.as_path())
            .await?
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        tokio::fs::read(target.as_path()).await?,
        b"preserved target"
    );
    let provenance = fresh_start
        .provenance
        .ok_or_else(|| anyhow::anyhow!("state backup should mint provenance"))?;
    let runtime = super::super::StateRuntime::init_fresh_after_corruption(
        sqlite_home,
        "test-provider".to_string(),
        provenance,
    )
    .await?;

    assert!(matches!(
        runtime.coordination_authority(),
        super::super::CoordinationAuthorityStatus::Quarantined { .. }
    ));
    runtime.get_backfill_state().await?;
    runtime.close().await;
    Ok(())
}

#[test]
fn sqlite_error_detail_classifies_corruption_and_lock_errors() {
    assert!(sqlite_error_detail_is_corruption("file is not a database"));
    assert!(sqlite_error_detail_is_corruption(
        "error returned from database: (code: 11) database disk image is malformed"
    ));
    assert!(!sqlite_error_detail_is_corruption("database is locked"));
    assert!(sqlite_error_detail_is_lock("database is locked"));
    assert!(sqlite_error_detail_is_lock("database is busy"));
}

#[tokio::test]
async fn runtime_db_path_for_corruption_error_returns_failed_database_path() -> std::io::Result<()>
{
    let sqlite_home = unique_temp_dir();
    tokio::fs::create_dir_all(sqlite_home.as_path()).await?;
    let path = super::super::state_db_path(sqlite_home.as_path());
    tokio::fs::write(path.as_path(), b"not sqlite").await?;

    let err = match super::super::StateRuntime::init(sqlite_home, "openai".to_string()).await {
        Ok(_) => panic!("malformed sqlite should fail to initialize"),
        Err(err) => err,
    };

    assert_eq!(runtime_db_path_for_corruption_error(&err), Some(path));
    Ok(())
}

#[test]
fn runtime_db_path_for_corruption_error_ignores_corrupt_word_in_path() {
    let path = PathBuf::from("/tmp/sqlite_corrupt/state_5.sqlite");
    let err = anyhow::Error::new(RuntimeDbInitError::new(
        "state DB",
        "open",
        path.as_path(),
        anyhow::anyhow!("permission denied"),
    ));

    assert_eq!(runtime_db_path_for_corruption_error(&err), None);
}
