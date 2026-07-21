use std::collections::HashMap;

use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationOrder;
use codex_coordination::CoordinationSource;
use pretty_assertions::assert_eq;
use sqlx::Row;

use crate::StateRuntime;
use crate::model::coordination_recovery::LegacySourceIdentity;
use crate::model::coordination_recovery::semantic_slot_sql;
use crate::model::coordination_recovery::source_shape_sql;

pub(super) async fn assert_integrity(runtime: &StateRuntime) -> anyhow::Result<()> {
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_all(&*runtime.pool)
            .await?,
        vec!["ok".to_string()]
    );
    assert_eq!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&*runtime.pool)
            .await?
            .len(),
        0
    );
    assert_roots_and_events(runtime).await?;
    assert_compatibility_events(runtime).await?;
    assert_logical_references(runtime).await?;
    assert_outbox_bijections(runtime).await?;
    super::failure_injection_degradation_integrity::assert_degradation_canonical(runtime).await?;
    super::failure_injection_counter_integrity::assert_checked_counters(runtime).await?;
    Ok(())
}

async fn assert_compatibility_events(runtime: &StateRuntime) -> anyhow::Result<()> {
    let rows = sqlx::query(
        "SELECT compatibility_event_id,root_thread_id,after_revision,source_ordinal,\
         adapter_version,sanitizer_version,source_shape,source_thread_id,source_turn_id,\
         source_item_id,semantic_slot,\
         source_identity_bytes,source_identity_fingerprint,canonical_event_bytes,\
         canonical_event_fingerprint FROM coordination_legacy_links",
    )
    .fetch_all(&*runtime.pool)
    .await?;
    for row in rows {
        let event_id: String = row.get("compatibility_event_id");
        let root_thread_id: String = row.get("root_thread_id");
        let bytes: Vec<u8> = row.get("canonical_event_bytes");
        let fingerprint: Vec<u8> = row.get("canonical_event_fingerprint");
        let event: CoordinationEvent = serde_json::from_slice(bytes.as_slice())?;
        let source = LegacySourceIdentity::from_event(&event)?;
        let source_identity = source.canonical_bytes()?;
        let CoordinationSource::Compatibility {
            adapter_version,
            sanitizer_version,
            key,
        } = &event.envelope().source
        else {
            anyhow::bail!("compatibility event {event_id} has native source");
        };
        let stored_source_item: Option<String> = row.get("source_item_id");
        let source_item_matches = match &key.source_item_id {
            codex_coordination::Evidence::Known { value } => {
                stored_source_item.as_deref() == Some(value.as_str())
            }
            codex_coordination::Evidence::Unavailable { .. }
            | codex_coordination::Evidence::NotApplicable => stored_source_item.is_none(),
        };
        let CoordinationOrder::Compatibility {
            after_revision,
            source_ordinal,
        } = event.envelope().order
        else {
            anyhow::bail!("compatibility event {event_id} has native order");
        };
        anyhow::ensure!(
            event.canonical_bytes() == bytes
                && event.fingerprint().as_slice() == fingerprint
                && event.envelope().event_id.to_string() == event_id
                && event.envelope().root_thread_id.to_string() == root_thread_id
                && source_identity.as_slice() == row.get::<Vec<u8>, _>("source_identity_bytes")
                && source_identity.fingerprint().as_slice()
                    == row.get::<Vec<u8>, _>("source_identity_fingerprint")
                && adapter_version.get() as i64 == row.get::<i64, _>("adapter_version")
                && sanitizer_version.get() as i64 == row.get::<i64, _>("sanitizer_version")
                && source_shape_sql(key.shape) == row.get::<String, _>("source_shape")
                && source.source_thread_id.as_ref().map(ToString::to_string)
                    == row.get::<Option<String>, _>("source_thread_id")
                && source
                    .source_turn_id
                    .as_ref()
                    .map(codex_coordination::BoundedId::as_str)
                    == row.get::<Option<String>, _>("source_turn_id").as_deref()
                && semantic_slot_sql(event.kind().semantic_slot())
                    == row.get::<String, _>("semantic_slot")
                && source_item_matches
                && after_revision.get() == row.get::<i64, _>("after_revision") as u64
                && source_ordinal.get() == row.get::<i64, _>("source_ordinal") as u64,
            "compatibility event {event_id} storage mismatch"
        );
    }
    Ok(())
}

async fn assert_roots_and_events(runtime: &StateRuntime) -> anyhow::Result<()> {
    let invalid_roots: i64 = sqlx::query_scalar(
        // Stage 3.1 lets native materialization advance `published_revision` up to
        // `committed_revision`; only a watermark beyond the committed journal is corrupt.
        "SELECT COUNT(*) FROM coordination_roots r WHERE published_revision>committed_revision \
         OR (SELECT COUNT(*) FROM coordination_events e \
             WHERE e.root_thread_id=r.root_thread_id)!=r.committed_revision \
         OR (r.committed_revision>0 AND (SELECT MIN(revision) FROM coordination_events e \
             WHERE e.root_thread_id=r.root_thread_id)!=1) \
         OR (r.committed_revision>0 AND (SELECT MAX(revision) FROM coordination_events e \
             WHERE e.root_thread_id=r.root_thread_id)!=r.committed_revision)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    anyhow::ensure!(invalid_roots == 0, "invalid Stage 2 root journal");

    let rows = sqlx::query(
        "SELECT e.event_id,e.root_thread_id,e.revision,e.canonical_event_bytes,\
         e.event_fingerprint,e.occurred_at,r.state_epoch FROM coordination_events e \
         JOIN coordination_roots r USING(root_thread_id) ORDER BY e.root_thread_id,e.revision",
    )
    .fetch_all(&*runtime.pool)
    .await?;
    let mut identities = HashMap::with_capacity(rows.len());
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let event_id: String = row.get("event_id");
        let root_thread_id: String = row.get("root_thread_id");
        let revision: i64 = row.get("revision");
        let state_epoch: String = row.get("state_epoch");
        let bytes: Vec<u8> = row.get("canonical_event_bytes");
        let fingerprint: Vec<u8> = row.get("event_fingerprint");
        let event: CoordinationEvent = serde_json::from_slice(bytes.as_slice())?;
        anyhow::ensure!(
            event.canonical_bytes() == bytes,
            "event {event_id} is not exact canonical bytes"
        );
        anyhow::ensure!(
            event.fingerprint().as_slice() == fingerprint,
            "event {event_id} fingerprint mismatch"
        );
        let envelope = event.envelope();
        anyhow::ensure!(
            envelope.event_id.to_string() == event_id
                && envelope.root_thread_id.to_string() == root_thread_id
                && envelope.occurred_at == row.get::<i64, _>("occurred_at"),
            "event {event_id} envelope mismatch"
        );
        let CoordinationOrder::Native {
            state_epoch: event_epoch,
            revision: event_revision,
        } = envelope.order
        else {
            anyhow::bail!("event {event_id} does not use native order");
        };
        anyhow::ensure!(
            matches!(envelope.source, CoordinationSource::Native { .. })
                && event_epoch.to_string() == state_epoch
                && event_revision.get() == revision as u64,
            "event {event_id} native authority mismatch"
        );
        identities.insert(event_id.clone(), (root_thread_id.clone(), revision));
        events.push((event_id, root_thread_id, revision, event));
    }
    for (event_id, root_thread_id, revision, event) in events {
        for cause in event.envelope().causes.items() {
            let Some((cause_root, cause_revision)) = identities.get(&cause.to_string()) else {
                anyhow::bail!("event {event_id} cause does not resolve");
            };
            anyhow::ensure!(
                cause_root == &root_thread_id && *cause_revision < revision,
                "event {event_id} cause is outside its prior root history"
            );
        }
    }
    Ok(())
}

async fn assert_logical_references(runtime: &StateRuntime) -> anyhow::Result<()> {
    let invalid_event_refs: i64 = sqlx::query_scalar(
        "WITH refs(root_thread_id,event_id) AS (\
           SELECT h.root_thread_id,g.request_event_id FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING(assignment_id) UNION ALL \
           SELECT h.root_thread_id,g.accepted_event_id FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING(assignment_id) UNION ALL \
           SELECT h.root_thread_id,g.superseded_event_id FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING(assignment_id) UNION ALL \
           SELECT h.root_thread_id,g.terminal_event_id FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING(assignment_id) UNION ALL \
           SELECT h.root_thread_id,g.close_event_id FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING(assignment_id) UNION ALL \
           SELECT root_thread_id,accepted_event_id FROM coordination_turn_bindings UNION ALL \
           SELECT root_thread_id,start_event_id FROM coordination_waits UNION ALL \
           SELECT root_thread_id,end_event_id FROM coordination_waits UNION ALL \
           SELECT root_thread_id,terminal_event_id FROM coordination_turn_terminals UNION ALL \
           SELECT root_thread_id,close_event_id FROM coordination_turn_terminal_generations UNION ALL \
           SELECT root_thread_id,event_id FROM coordination_dependencies UNION ALL \
           SELECT root_thread_id,terminal_event_id FROM coordination_results UNION ALL \
           SELECT root_thread_id,observed_event_id FROM coordination_results UNION ALL \
           SELECT root_thread_id,attempted_event_id FROM coordination_handoffs UNION ALL \
           SELECT root_thread_id,received_event_id FROM coordination_handoffs UNION ALL \
           SELECT root_thread_id,failed_event_id FROM coordination_handoffs UNION ALL \
           SELECT root_thread_id,intent_event_id FROM coordination_commands UNION ALL \
           SELECT root_thread_id,intent_event_id FROM coordination_inbox UNION ALL \
           SELECT root_thread_id,receipt_event_id FROM coordination_inbox UNION ALL \
           SELECT root_thread_id,resolution_event_id FROM coordination_inbox UNION ALL \
           SELECT root_thread_id,semantic_event_id FROM coordination_inbox_inclusions UNION ALL \
           SELECT root_thread_id,suppressed_by_native_event_id FROM coordination_legacy_links) \
         SELECT COUNT(*) FROM refs LEFT JOIN coordination_events e USING(event_id) \
         WHERE refs.event_id IS NOT NULL AND (e.event_id IS NULL OR e.root_thread_id!=refs.root_thread_id)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    anyhow::ensure!(
        invalid_event_refs == 0,
        "aggregate event reference mismatch"
    );

    let invalid_revision_refs: i64 = sqlx::query_scalar(
        "WITH refs(root_thread_id,revision) AS (\
           SELECT root_thread_id,last_revision FROM coordination_assignment_heads UNION ALL \
           SELECT h.root_thread_id,g.created_revision FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING(assignment_id) UNION ALL \
           SELECT h.root_thread_id,g.last_revision FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING(assignment_id) UNION ALL \
           SELECT root_thread_id,last_revision FROM coordination_waits UNION ALL \
           SELECT root_thread_id,revision FROM coordination_turn_terminals) \
         SELECT COUNT(*) FROM refs LEFT JOIN coordination_events e \
         ON e.root_thread_id=refs.root_thread_id AND e.revision=refs.revision WHERE e.event_id IS NULL",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    anyhow::ensure!(
        invalid_revision_refs == 0,
        "aggregate revision reference mismatch"
    );

    let invalid_terminal_receipts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM coordination_commands c LEFT JOIN coordination_inbox i \
         ON i.receipt_id=c.terminal_receipt_id WHERE c.terminal_receipt_id IS NOT NULL AND (\
         i.receipt_id IS NULL OR i.command_operation_id!=c.operation_id \
         OR i.root_thread_id!=c.root_thread_id OR i.delivery_fingerprint!=c.terminal_receipt_fingerprint)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    anyhow::ensure!(
        invalid_terminal_receipts == 0,
        "sender terminal receipt reference mismatch"
    );
    let invalid_coherence: i64 = sqlx::query_scalar(
        "SELECT \
         (SELECT COUNT(*) FROM coordination_handoffs h JOIN coordination_results r USING(result_id) WHERE h.root_thread_id!=r.root_thread_id) + \
         (SELECT COUNT(*) FROM coordination_inbox_inclusions x JOIN coordination_inbox i USING(receipt_id) WHERE x.root_thread_id!=i.root_thread_id) + \
         (SELECT COUNT(*) FROM coordination_commands c WHERE NOT EXISTS (SELECT 1 FROM coordination_assignment_heads h JOIN coordination_assignment_generations g USING(assignment_id) \
          WHERE h.assignment_id=c.target_assignment_id AND h.root_thread_id=c.root_thread_id AND h.child_thread_id=c.target_thread_id AND g.generation=c.target_generation)) + \
         (SELECT COUNT(*) FROM coordination_inbox i JOIN coordination_commands c ON c.operation_id=i.command_operation_id WHERE \
          i.root_thread_id!=c.root_thread_id OR i.intent_event_id!=c.intent_event_id OR i.sender_thread_id!=c.sender_thread_id \
          OR i.sender_turn_id!=c.sender_turn_id OR i.recipient_thread_id!=c.target_thread_id \
          OR i.target_assignment_id!=c.target_assignment_id OR i.target_generation!=c.target_generation) + \
         (SELECT COUNT(*) FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING(assignment_id) \
          JOIN coordination_events e ON e.event_id=g.accepted_event_id LEFT JOIN coordination_turn_bindings b \
          ON b.assignment_id=g.assignment_id AND b.generation=g.generation WHERE g.accepted_event_id IS NOT NULL AND (\
          b.accepted_event_id IS NOT g.accepted_event_id OR json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.receiptId') IS NOT g.accepted_receipt_id \
          OR json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.operationId') IS NOT g.operation_id \
          OR json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId') IS NOT g.assignment_id \
          OR json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.generation') IS NOT g.generation \
          OR json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.boundTurnId.value') IS NOT b.turn_id)) + \
         (SELECT COUNT(*) FROM coordination_legacy_links l JOIN coordination_roots r USING(root_thread_id) \
          WHERE l.state_epoch!=r.state_epoch OR l.after_revision>r.committed_revision) + \
         (SELECT COUNT(*) FROM coordination_legacy_scan_checkpoints c JOIN coordination_roots r USING(root_thread_id) \
          WHERE c.state_epoch!=r.state_epoch OR (c.last_source_ordinal IS NOT NULL AND NOT EXISTS (\
            SELECT 1 FROM coordination_legacy_links l WHERE l.root_thread_id=c.root_thread_id \
            AND l.source_thread_id=c.source_thread_id AND l.adapter_version=c.adapter_version \
            AND l.source_ordinal=c.last_source_ordinal AND l.compatibility_event_id=c.last_compatibility_event_id)))",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    anyhow::ensure!(
        invalid_coherence == 0,
        "aggregate logical coherence mismatch"
    );
    Ok(())
}

async fn assert_outbox_bijections(runtime: &StateRuntime) -> anyhow::Result<()> {
    let mismatch: i64 = sqlx::query_scalar(
        "SELECT \
         (SELECT COUNT(*) FROM coordination_events e LEFT JOIN coordination_projection_outbox o USING(event_id) WHERE o.event_id IS NULL) + \
         (SELECT COUNT(*) FROM coordination_projection_outbox o LEFT JOIN coordination_events e USING(event_id) WHERE e.event_id IS NULL) + \
         (SELECT COUNT(*) FROM coordination_degradation_records d LEFT JOIN coordination_degradation_publication_outbox o USING(degradation_id) WHERE o.degradation_id IS NULL) + \
         (SELECT COUNT(*) FROM coordination_degradation_publication_outbox o LEFT JOIN coordination_degradation_records d USING(degradation_id) WHERE d.degradation_id IS NULL) + \
         (SELECT COUNT(*) FROM coordination_events e JOIN coordination_degradation_records d ON d.degradation_id=e.event_id) + \
         (SELECT COUNT(*) FROM coordination_events e JOIN coordination_legacy_links l ON l.compatibility_event_id=e.event_id) + \
         (SELECT COUNT(*) FROM coordination_legacy_links l JOIN coordination_degradation_records d ON d.degradation_id=l.compatibility_event_id) + \
         (SELECT COUNT(*) FROM coordination_projection_outbox p JOIN coordination_degradation_publication_outbox d ON d.degradation_id=p.event_id) + \
         (SELECT COUNT(*) FROM coordination_degradation_records d JOIN coordination_roots r USING(root_thread_id) \
          WHERE d.after_revision>r.committed_revision OR (d.state_epoch IS NOT NULL AND d.state_epoch!=r.state_epoch)) + \
         (SELECT COUNT(*) FROM coordination_degradation_publication_outbox o JOIN coordination_degradation_records d USING(degradation_id) \
          WHERE o.root_thread_id!=d.root_thread_id OR o.after_revision!=d.after_revision \
          OR o.source_ordinal!=COALESCE(d.source_ordinal,0) OR o.stable_record_id!=d.degradation_id)",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    anyhow::ensure!(
        mismatch == 0,
        "native and degradation outboxes are not disjoint bijections"
    );
    Ok(())
}
