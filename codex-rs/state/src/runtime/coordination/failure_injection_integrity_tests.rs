use codex_coordination::CoordinationSemanticSlot;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;

use super::degradation::record_exogenous_terminal_degradation;
use super::failure_injection_integrity::assert_integrity;
use super::failure_injection_tests::delivery_now;
use super::failure_injection_tests::observation;
use super::failure_injection_tests::runtime_with_command_at;
use super::failure_injection_tests::runtime_with_root_at;
use super::legacy_links::record_legacy_link;
use super::recovery_test_support::compatibility_event;
use crate::model::coordination_recovery::CheckedLegacyLink;
use crate::runtime::test_support::unique_temp_dir;

#[tokio::test]
async fn integrity_rejects_noncanonical_event_storage() -> anyhow::Result<()> {
    let runtime = runtime_with_command_at(unique_temp_dir()).await?;
    sqlx::query("DROP TRIGGER coordination_events_immutable_update")
        .execute(&*runtime.pool)
        .await?;
    sqlx::query(
        "UPDATE coordination_events SET canonical_event_bytes=CAST(canonical_event_bytes || ' ' AS BLOB)",
    )
    .execute(&*runtime.pool)
    .await?;
    let error = assert_integrity(&runtime)
        .await
        .expect_err("noncanonical event");
    assert!(error.to_string().contains("not exact canonical bytes"));
    Ok(())
}

#[tokio::test]
async fn integrity_accepts_checked_counter_maximum() -> anyhow::Result<()> {
    let runtime = runtime_with_command_at(unique_temp_dir()).await?;
    let now_ms = delivery_now(&runtime).await?;
    sqlx::query("DROP TRIGGER coordination_command_transition_guard")
        .execute(&*runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_commands SET claim_count=?,updated_at_ms=?")
        .bind(i64::MAX)
        .bind(now_ms)
        .execute(&*runtime.pool)
        .await?;
    assert_integrity(&runtime).await?;
    Ok(())
}

#[tokio::test]
async fn integrity_checks_compatibility_and_degradation_canonical_records() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let (runtime, epoch) = runtime_with_root_at(home).await?;
    let event = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 23);
    let root = event.envelope().root_thread_id;
    record_legacy_link(&runtime.pool, &CheckedLegacyLink::new(root, epoch, &event)?).await?;
    record_exogenous_terminal_degradation(&runtime.pool, observation(epoch)?).await?;
    assert_integrity(&runtime).await?;

    sqlx::query("DROP TRIGGER coordination_degradation_immutable")
        .execute(&*runtime.pool)
        .await?;
    let row = sqlx::query("SELECT degradation_id,canonical_record_bytes FROM coordination_degradation_records LIMIT 1")
        .fetch_one(&*runtime.pool)
        .await?;
    let mut bytes = row.get::<Vec<u8>, _>("canonical_record_bytes");
    bytes.push(b' ');
    let fingerprint: [u8; 32] = Sha256::digest(bytes.as_slice()).into();
    sqlx::query("UPDATE coordination_degradation_records SET canonical_record_bytes=?,canonical_record_fingerprint=? WHERE degradation_id=?")
        .bind(bytes).bind(fingerprint.as_slice()).bind(row.get::<String, _>("degradation_id"))
        .execute(&*runtime.pool).await?;
    assert!(assert_integrity(&runtime).await.is_err());
    Ok(())
}

#[tokio::test]
async fn integrity_rejects_degradation_structured_column_drift() -> anyhow::Result<()> {
    let (runtime, epoch) = runtime_with_root_at(unique_temp_dir()).await?;
    record_exogenous_terminal_degradation(&runtime.pool, observation(epoch)?).await?;
    assert_integrity(&runtime).await?;

    sqlx::query("DROP TRIGGER coordination_degradation_immutable")
        .execute(&*runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_degradation_records SET terminal_outcome='failed'")
        .execute(&*runtime.pool)
        .await?;
    assert!(assert_integrity(&runtime).await.is_err());
    Ok(())
}

#[tokio::test]
async fn integrity_rejects_fk_valid_cross_root_command_reference() -> anyhow::Result<()> {
    let runtime = runtime_with_command_at(unique_temp_dir()).await?;
    let authority = sqlx::query("SELECT state_epoch,created_at_ms FROM coordination_authority")
        .fetch_one(&*runtime.pool)
        .await?;
    let other_root = "019f7c6c-1111-7000-8000-000000000699";
    sqlx::query("INSERT INTO coordination_roots (root_thread_id,state_epoch,committed_revision,published_revision,created_at_ms,updated_at_ms) VALUES (?,?,0,0,?,?)")
        .bind(other_root).bind(authority.get::<String, _>("state_epoch"))
        .bind(authority.get::<i64, _>("created_at_ms")).bind(authority.get::<i64, _>("created_at_ms"))
        .execute(&*runtime.pool).await?;
    sqlx::query("DROP TRIGGER coordination_command_identity_immutable")
        .execute(&*runtime.pool)
        .await?;
    sqlx::query("DROP TRIGGER coordination_command_transition_guard")
        .execute(&*runtime.pool)
        .await?;
    sqlx::query("UPDATE coordination_commands SET root_thread_id=?")
        .bind(other_root)
        .execute(&*runtime.pool)
        .await?;
    assert!(assert_integrity(&runtime).await.is_err());
    Ok(())
}
