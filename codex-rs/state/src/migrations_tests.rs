use sqlx::Connection;
use sqlx::Row;
use sqlx::migrate::Migration;
use sqlx::migrate::Migrator;
use std::borrow::Cow;

use super::STATE_MIGRATOR;
use super::repair_legacy_recency_migration_version;
use crate::state_db_path;

fn migrator_through(version: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            STATE_MIGRATOR
                .migrations
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ignore_missing: STATE_MIGRATOR.ignore_missing,
        locking: STATE_MIGRATOR.locking,
        table_name: STATE_MIGRATOR.table_name.clone(),
        create_schemas: STATE_MIGRATOR.create_schemas.clone(),
        no_tx: STATE_MIGRATOR.no_tx,
    }
}

#[tokio::test]
async fn recency_migration_backfills_and_seeds_old_binary_inserts() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.clone());
    let pool = sqlite
        .open_read_write_pool(&state_db_path(&sqlite_home))
        .await
        .expect("sqlite database should open");
    migrator_through(/*version*/ 37)
        .run(&pool)
        .await
        .expect("pre-recency migrations should apply");

    sqlx::query(
        r#"
INSERT INTO threads (
    id,
    rollout_path,
    created_at,
    updated_at,
    created_at_ms,
    updated_at_ms,
    source,
    model_provider,
    cwd,
    title,
    sandbox_policy,
    approval_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind("00000000-0000-0000-0000-000000000001")
    .bind("/tmp/first.jsonl")
    .bind(1_700_000_000_i64)
    .bind(1_700_000_100_i64)
    .bind(1_700_000_000_123_i64)
    .bind(1_700_000_100_456_i64)
    .bind("cli")
    .bind("openai")
    .bind("/tmp")
    .bind("")
    .bind("read-only")
    .bind("on-request")
    .execute(&pool)
    .await
    .expect("legacy row should insert");

    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("recency migration should apply");

    let backfilled = sqlx::query(
        "SELECT updated_at, updated_at_ms, recency_at, recency_at_ms FROM threads WHERE id = ?",
    )
    .bind("00000000-0000-0000-0000-000000000001")
    .fetch_one(&pool)
    .await
    .expect("backfilled row should load");
    assert_eq!(backfilled.get::<i64, _>("recency_at"), 1_700_000_100);
    assert_eq!(backfilled.get::<i64, _>("recency_at_ms"), 1_700_000_100_456);

    sqlx::query(
        r#"
INSERT INTO threads (
    id,
    rollout_path,
    created_at,
    updated_at,
    created_at_ms,
    updated_at_ms,
    source,
    model_provider,
    cwd,
    title,
    sandbox_policy,
    approval_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind("00000000-0000-0000-0000-000000000002")
    .bind("/tmp/second.jsonl")
    .bind(1_700_000_200_i64)
    .bind(1_700_000_300_i64)
    .bind(1_700_000_200_123_i64)
    .bind(1_700_000_300_456_i64)
    .bind("cli")
    .bind("openai")
    .bind("/tmp")
    .bind("")
    .bind("read-only")
    .bind("on-request")
    .execute(&pool)
    .await
    .expect("old-binary row should insert");

    let seeded = sqlx::query("SELECT recency_at, recency_at_ms FROM threads WHERE id = ?")
        .bind("00000000-0000-0000-0000-000000000002")
        .fetch_one(&pool)
        .await
        .expect("old-binary row should load");
    assert_eq!(seeded.get::<i64, _>("recency_at"), 1_700_000_300);
    assert_eq!(seeded.get::<i64, _>("recency_at_ms"), 1_700_000_300_456);

    pool.close().await;
}

#[tokio::test]
async fn repairs_recency_migration_that_was_applied_as_version_38() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.clone());
    let pool = sqlite
        .open_read_write_pool(&state_db_path(&sqlite_home))
        .await
        .expect("sqlite database should open");
    migrator_through(/*version*/ 37)
        .run(&pool)
        .await
        .expect("pre-recency migrations should apply");

    let recency_migration = STATE_MIGRATOR
        .migrations
        .iter()
        .find(|migration| migration.version == 39)
        .expect("recency migration should exist");
    let mut legacy_migrations = STATE_MIGRATOR
        .migrations
        .iter()
        .filter(|migration| migration.version <= 37)
        .cloned()
        .collect::<Vec<_>>();
    legacy_migrations.push(Migration::new(
        38,
        recency_migration.description.clone(),
        recency_migration.migration_type,
        recency_migration.sql.clone(),
        recency_migration.no_tx,
    ));
    let legacy_recency_migrator = Migrator::with_migrations(legacy_migrations);
    legacy_recency_migrator
        .run(&pool)
        .await
        .expect("legacy recency migration should apply as version 38");

    repair_legacy_recency_migration_version(&pool, &STATE_MIGRATOR)
        .await
        .expect("legacy migration history should be repaired");
    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("current migrations should apply after repair");

    let applied = sqlx::query(
        "SELECT version, checksum FROM _sqlx_migrations WHERE version >= 38 ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .expect("applied migrations should load")
    .into_iter()
    .map(|row| {
        (
            row.get::<i64, _>("version"),
            row.get::<Vec<u8>, _>("checksum"),
        )
    })
    .collect::<Vec<_>>();
    let expected = STATE_MIGRATOR
        .migrations
        .iter()
        .filter(|migration| migration.version >= 38)
        .map(|migration| (migration.version, migration.checksum.to_vec()))
        .collect::<Vec<_>>();
    assert_eq!(applied, expected);

    pool.close().await;
}

#[tokio::test]
async fn repair_recency_migration_succeeds_while_another_connection_holds_writer_slot() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.clone());
    let state_path = state_db_path(&sqlite_home);
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("database should open");
    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("current migrations should apply");
    let read_pool = sqlite
        .open_read_only_pool(&state_path)
        .await
        .expect("read-only pool should open");
    let mut write_connection = pool.acquire().await.expect("write connection should open");
    let write_transaction = write_connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("write transaction should acquire the writer slot");

    let repair_result = repair_legacy_recency_migration_version(&read_pool, &STATE_MIGRATOR).await;

    write_transaction
        .rollback()
        .await
        .expect("write transaction should roll back");
    drop(write_connection);
    read_pool.close().await;
    pool.close().await;
    repair_result.expect("current migration history should not need the writer slot");
}

#[tokio::test]
async fn coordination_authority_and_journal_are_independent_and_guarded() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.clone());
    let pool = sqlite
        .open_read_write_pool(&state_db_path(&sqlite_home))
        .await
        .expect("sqlite database should open");
    migrator_through(/*version*/ 45)
        .run(&pool)
        .await
        .expect("migrations through the previous namespace should apply");

    sqlx::query(
        r#"
INSERT INTO delegations (
    delegation_id, run_id, parent_thread_id, parent_turn_id, owner_session_id,
    agent_path, status, version, attempt, lease_epoch, created_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind("legacy-delegation")
    .bind("legacy-run")
    .bind("legacy-parent")
    .bind("legacy-turn")
    .bind("legacy-owner")
    .bind("/root/legacy")
    .bind("running")
    .bind(7_i64)
    .bind(3_i64)
    .bind(5_i64)
    .bind(100_i64)
    .bind(200_i64)
    .execute(&pool)
    .await
    .expect("pre-0046 delegation should insert");

    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("coordination namespace migration should apply after 0045");

    let epoch = "019f7c6c-1111-7000-8000-000000000801";
    let root = "019f7c6c-1111-7000-8000-000000000601";
    let event = "019f7c6c-1111-7000-8000-000000000701";
    sqlx::query(
        "INSERT INTO coordination_authority \
         (state_epoch, status, created_at_ms, updated_at_ms) VALUES (?, 'active', 10, 10)",
    )
    .bind(epoch)
    .execute(&pool)
    .await
    .expect("singleton authority should insert");
    assert!(
        sqlx::query(
            "INSERT INTO coordination_authority \
             (state_epoch, status, created_at_ms, updated_at_ms) VALUES (?, 'active', 10, 10)",
        )
        .bind("019f7c6c-1111-7000-8000-000000000802")
        .execute(&pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query("UPDATE coordination_authority SET state_epoch = ?")
            .bind("019f7c6c-1111-7000-8000-000000000802")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query(
            "INSERT OR REPLACE INTO coordination_authority \
             (state_epoch, status, created_at_ms, updated_at_ms) \
             VALUES (?, 'active', 10, 10)",
        )
        .bind("019f7c6c-1111-7000-8000-000000000802")
        .execute(&pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM coordination_authority")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query(
        "UPDATE coordination_authority SET status = 'quarantined', \
         quarantine_reason = 'marker mismatch', updated_at_ms = 11",
    )
    .execute(&pool)
    .await
    .expect("authority may transition to quarantined");
    assert!(
        sqlx::query("UPDATE coordination_authority SET status = 'active', updated_at_ms = 12",)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query(
            "UPDATE coordination_authority SET status = 'active', \
             quarantine_reason = NULL, updated_at_ms = 12",
        )
        .execute(&pool)
        .await
        .is_err()
    );

    sqlx::query(
        "INSERT INTO coordination_roots \
         (root_thread_id, state_epoch, committed_revision, published_revision, \
          created_at_ms, updated_at_ms) VALUES (?, ?, 1, 0, 20, 20)",
    )
    .bind(root)
    .bind(epoch)
    .execute(&pool)
    .await
    .expect("coordination root should insert");
    assert!(
        sqlx::query(
            "INSERT OR REPLACE INTO coordination_roots \
             (root_thread_id, state_epoch, committed_revision, published_revision, \
              created_at_ms, updated_at_ms) VALUES (?, ?, 0, 0, 20, 20)",
        )
        .bind(root)
        .bind(epoch)
        .execute(&pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query(
            "INSERT INTO coordination_roots \
             (root_thread_id, state_epoch, committed_revision, published_revision, \
              created_at_ms, updated_at_ms) VALUES (?, ?, 0, 1, 20, 20)",
        )
        .bind("019f7c6c-1111-7000-8000-000000000602")
        .bind(epoch)
        .execute(&pool)
        .await
        .is_err()
    );

    let insert_event = r#"
INSERT INTO coordination_events (
    event_id, root_thread_id, revision, canonical_event_bytes, event_fingerprint,
    idempotency_key_bytes, idempotency_key_fingerprint, occurred_at, created_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    "#;
    sqlx::query(insert_event)
        .bind(event)
        .bind(root)
        .bind(1_i64)
        .bind(br#"{"kind":"assignmentRequested"}"#.as_slice())
        .bind(vec![0x11_u8; 32])
        .bind(b"canonical-idempotency-tuple".as_slice())
        .bind(vec![0x22_u8; 32])
        .bind(1_753_000_000_i64)
        .bind(30_i64)
        .execute(&pool)
        .await
        .expect("bounded immutable event should insert");
    assert!(
        sqlx::query(
            r#"
INSERT OR REPLACE INTO coordination_events (
    event_id, root_thread_id, revision, canonical_event_bytes, event_fingerprint,
    idempotency_key_bytes, idempotency_key_fingerprint, occurred_at, created_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(event)
        .bind(root)
        .bind(1_i64)
        .bind(b"{}".as_slice())
        .bind(vec![0x99_u8; 32])
        .bind(b"replacement-key".as_slice())
        .bind(vec![0x98_u8; 32])
        .bind(0_i64)
        .bind(31_i64)
        .execute(&pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query(
            "UPDATE coordination_roots SET committed_revision = 0, updated_at_ms = 21 \
             WHERE root_thread_id = ?",
        )
        .bind(root)
        .execute(&pool)
        .await
        .is_err()
    );
    sqlx::query(
        "UPDATE coordination_roots SET published_revision = 1, updated_at_ms = 21 \
         WHERE root_thread_id = ?",
    )
    .bind(root)
    .execute(&pool)
    .await
    .expect("published watermark may advance to committed revision");
    assert!(
        sqlx::query(
            "UPDATE coordination_roots SET published_revision = 0, updated_at_ms = 22 \
             WHERE root_thread_id = ?",
        )
        .bind(root)
        .execute(&pool)
        .await
        .is_err()
    );
    sqlx::query(
        "INSERT INTO coordination_projection_outbox \
         (event_id, status, created_at_ms, updated_at_ms) VALUES (?, 'pending', 30, 30)",
    )
    .bind(event)
    .execute(&pool)
    .await
    .expect("projection work should insert separately");
    sqlx::query(
        "UPDATE coordination_projection_outbox SET status = 'leased', version = 1, \
         lease_epoch = 1, lease_expires_at_ms = 100, updated_at_ms = 31 WHERE event_id = ?",
    )
    .bind(event)
    .execute(&pool)
    .await
    .expect("projection lease state should remain mutable");

    assert!(
        sqlx::query("UPDATE coordination_events SET occurred_at = 0 WHERE event_id = ?")
            .bind(event)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM coordination_events WHERE event_id = ?")
            .bind(event)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query(insert_event)
            .bind("019f7c6c-1111-7000-8000-000000000702")
            .bind(root)
            .bind(1_i64)
            .bind(b"{}".as_slice())
            .bind(vec![0x33_u8; 32])
            .bind(b"different-key".as_slice())
            .bind(vec![0x44_u8; 32])
            .bind(1_i64)
            .bind(31_i64)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query(insert_event)
            .bind("019f7c6c-1111-7000-8000-000000000703")
            .bind(root)
            .bind(2_i64)
            .bind(vec![b'x'; 8193])
            .bind(vec![0x55_u8; 32])
            .bind(b"oversized-event".as_slice())
            .bind(vec![0x66_u8; 32])
            .bind(2_i64)
            .bind(32_i64)
            .execute(&pool)
            .await
            .is_err()
    );

    let delegation = sqlx::query(
        "SELECT run_id, status, version, attempt, lease_epoch \
         FROM delegations WHERE delegation_id = 'legacy-delegation'",
    )
    .fetch_one(&pool)
    .await
    .expect("existing delegation should remain");
    assert_eq!(delegation.get::<String, _>("run_id"), "legacy-run");
    assert_eq!(delegation.get::<String, _>("status"), "running");
    assert_eq!(delegation.get::<i64, _>("version"), 7);
    assert_eq!(delegation.get::<i64, _>("attempt"), 3);
    assert_eq!(delegation.get::<i64, _>("lease_epoch"), 5);

    pool.close().await;
}
