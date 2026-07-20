use crate::StateRuntime;

pub(super) async fn assert_checked_counters(runtime: &StateRuntime) -> anyhow::Result<()> {
    const MAX_INCREMENTABLE: i64 = i64::MAX;
    const MAX_REVISION: i64 = i64::MAX;
    const MAX_GENERATION: i64 = i32::MAX as i64;
    const COUNTERS: &[(&str, &str, i64, i64)] = &[
        ("coordination_roots", "committed_revision", 0, MAX_REVISION),
        ("coordination_roots", "published_revision", 0, MAX_REVISION),
        ("coordination_events", "revision", 1, MAX_REVISION),
        (
            "coordination_projection_outbox",
            "version",
            0,
            MAX_INCREMENTABLE,
        ),
        (
            "coordination_projection_outbox",
            "lease_epoch",
            0,
            MAX_INCREMENTABLE,
        ),
        ("coordination_projection_outbox", "retry_count", 0, 8),
        (
            "coordination_assignment_heads",
            "accepted_generation",
            1,
            MAX_GENERATION,
        ),
        (
            "coordination_assignment_heads",
            "next_generation",
            2,
            MAX_GENERATION,
        ),
        (
            "coordination_assignment_heads",
            "version",
            0,
            MAX_INCREMENTABLE,
        ),
        (
            "coordination_assignment_heads",
            "last_revision",
            1,
            MAX_REVISION,
        ),
        (
            "coordination_assignment_generations",
            "generation",
            1,
            MAX_GENERATION,
        ),
        (
            "coordination_assignment_generations",
            "created_revision",
            1,
            MAX_REVISION,
        ),
        (
            "coordination_assignment_generations",
            "last_revision",
            1,
            MAX_REVISION,
        ),
        (
            "coordination_turn_bindings",
            "generation",
            1,
            MAX_GENERATION,
        ),
        ("coordination_waits", "version", 0, MAX_INCREMENTABLE),
        ("coordination_waits", "last_revision", 1, MAX_REVISION),
        ("coordination_turn_terminals", "revision", 1, MAX_REVISION),
        (
            "coordination_turn_terminal_generations",
            "generation",
            1,
            MAX_GENERATION,
        ),
        ("coordination_handoffs", "attempt", 1, MAX_GENERATION),
        (
            "coordination_commands",
            "target_generation",
            1,
            MAX_GENERATION,
        ),
        (
            "coordination_commands",
            "captured_head_generation",
            1,
            MAX_GENERATION,
        ),
        ("coordination_commands", "version", 0, MAX_INCREMENTABLE),
        ("coordination_commands", "claim_count", 0, MAX_INCREMENTABLE),
        (
            "coordination_commands",
            "attempt_count",
            0,
            MAX_INCREMENTABLE,
        ),
        (
            "coordination_commands",
            "attempted_lease_epoch",
            1,
            MAX_INCREMENTABLE,
        ),
        ("coordination_commands", "lease_epoch", 0, MAX_INCREMENTABLE),
        ("coordination_inbox", "target_generation", 1, MAX_GENERATION),
        (
            "coordination_inbox",
            "captured_head_generation",
            1,
            MAX_GENERATION,
        ),
        ("coordination_inbox", "version", 0, MAX_INCREMENTABLE),
        ("coordination_inbox", "claim_count", 0, MAX_INCREMENTABLE),
        ("coordination_inbox", "retry_count", 0, MAX_INCREMENTABLE),
        ("coordination_inbox", "lease_epoch", 0, MAX_INCREMENTABLE),
        (
            "coordination_inbox_inclusions",
            "inbox_version",
            1,
            MAX_INCREMENTABLE,
        ),
        (
            "coordination_inbox_inclusions",
            "lease_epoch",
            1,
            MAX_INCREMENTABLE,
        ),
        (
            "coordination_inbox_inclusions",
            "version",
            0,
            MAX_INCREMENTABLE,
        ),
        ("coordination_legacy_links", "source_ordinal", 0, i64::MAX),
        (
            "coordination_legacy_links",
            "after_revision",
            0,
            MAX_REVISION,
        ),
        (
            "coordination_legacy_scan_checkpoints",
            "next_physical_ordinal",
            0,
            i64::MAX,
        ),
        (
            "coordination_legacy_scan_checkpoints",
            "last_source_ordinal",
            0,
            i64::MAX,
        ),
        (
            "coordination_legacy_scan_checkpoints",
            "version",
            0,
            MAX_INCREMENTABLE,
        ),
        (
            "coordination_degradation_records",
            "source_ordinal",
            0,
            i64::MAX,
        ),
        (
            "coordination_degradation_records",
            "after_revision",
            0,
            MAX_REVISION,
        ),
        (
            "coordination_degradation_publication_outbox",
            "after_revision",
            0,
            MAX_REVISION,
        ),
        (
            "coordination_degradation_publication_outbox",
            "source_ordinal",
            0,
            i64::MAX,
        ),
        (
            "coordination_degradation_publication_outbox",
            "version",
            0,
            MAX_INCREMENTABLE,
        ),
        (
            "coordination_degradation_publication_outbox",
            "lease_epoch",
            0,
            MAX_INCREMENTABLE,
        ),
        (
            "coordination_degradation_publication_outbox",
            "retry_count",
            0,
            8,
        ),
    ];
    for &(table, column, min, max) in COUNTERS {
        let invalid: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {table} WHERE \"{column}\" IS NOT NULL AND \
             (typeof(\"{column}\")!='integer' OR \"{column}\"<? OR \"{column}\">?)"
        )))
        .bind(min)
        .bind(max)
        .fetch_one(&*runtime.pool)
        .await?;
        anyhow::ensure!(invalid == 0, "unsafe counter {table}.{column}");
    }
    Ok(())
}
