CREATE TABLE coordination_legacy_links (
    compatibility_event_id TEXT PRIMARY KEY NOT NULL CHECK (length(compatibility_event_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    state_epoch TEXT NOT NULL REFERENCES coordination_authority(state_epoch),
    source_shape TEXT NOT NULL CHECK (source_shape IN (
        'collabToolItem','derivedCollabEvent','subAgentActivity',
        'interAgentCommunication','turnComplete','turnAborted'
    )),
    source_thread_id TEXT CHECK (source_thread_id IS NULL OR length(source_thread_id) = 36),
    source_turn_id TEXT CHECK (source_turn_id IS NULL OR length(source_turn_id) BETWEEN 1 AND 128),
    source_item_id TEXT CHECK (source_item_id IS NULL OR length(source_item_id) BETWEEN 1 AND 128),
    source_ordinal INTEGER NOT NULL CHECK (
        typeof(source_ordinal) = 'integer' AND source_ordinal BETWEEN 0 AND 9223372036854775807
    ),
    semantic_slot TEXT NOT NULL CHECK (semantic_slot IN (
        'assignmentRequested','assignmentAccepted','assignmentGenerationClosed',
        'messageSubmissionRecorded','messageDurablyReceived','messageIncludedInModelInput',
        'waitStarted','waitEnded','interruptRequested','interruptDurablyReceived',
        'turnInterrupted','detached','dependencyDeclared','ownershipChanged','turnCompleted',
        'terminalResultObserved','handoffDeliveryAttempted','handoffDurablyReceived',
        'handoffIncludedInModelInput','handoffDeliveryFailed','legacyInteractionObserved'
    )),
    source_identity_bytes BLOB NOT NULL CHECK (
        typeof(source_identity_bytes) = 'blob' AND length(source_identity_bytes) BETWEEN 1 AND 1024
    ),
    source_identity_fingerprint BLOB NOT NULL CHECK (
        typeof(source_identity_fingerprint) = 'blob' AND length(source_identity_fingerprint) = 32
    ),
    canonical_event_bytes BLOB NOT NULL CHECK (
        typeof(canonical_event_bytes) = 'blob' AND length(canonical_event_bytes) BETWEEN 1 AND 8192
    ),
    canonical_event_fingerprint BLOB NOT NULL CHECK (
        typeof(canonical_event_fingerprint) = 'blob' AND length(canonical_event_fingerprint) = 32
    ),
    adapter_version INTEGER NOT NULL CHECK (adapter_version = 1),
    sanitizer_version INTEGER NOT NULL CHECK (sanitizer_version = 1),
    after_revision INTEGER NOT NULL CHECK (
        typeof(after_revision) = 'integer' AND after_revision BETWEEN 0 AND 9223372036854775807
    ),
    suppressed_by_native_event_id TEXT REFERENCES coordination_events(event_id),
    suppressed_at_ms INTEGER CHECK (
        suppressed_at_ms IS NULL OR
        (typeof(suppressed_at_ms) = 'integer' AND suppressed_at_ms BETWEEN 0 AND 9223372036854775807)
    ),
    created_at_ms INTEGER NOT NULL CHECK (
        typeof(created_at_ms) = 'integer' AND created_at_ms BETWEEN 0 AND 9223372036854775807
    ),
    UNIQUE (root_thread_id, source_identity_fingerprint),
    CHECK ((suppressed_by_native_event_id IS NULL) = (suppressed_at_ms IS NULL))
);

CREATE INDEX idx_coordination_legacy_replay
    ON coordination_legacy_links(root_thread_id,after_revision,source_ordinal,compatibility_event_id);

CREATE TABLE coordination_legacy_scan_checkpoints (
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    state_epoch TEXT NOT NULL REFERENCES coordination_authority(state_epoch),
    source_thread_id TEXT NOT NULL CHECK (length(source_thread_id) = 36),
    adapter_version INTEGER NOT NULL CHECK (adapter_version = 1),
    next_physical_ordinal INTEGER NOT NULL CHECK (
        typeof(next_physical_ordinal) = 'integer'
        AND next_physical_ordinal BETWEEN 0 AND 9223372036854775807
    ),
    scanned_prefix_fingerprint BLOB NOT NULL CHECK (
        typeof(scanned_prefix_fingerprint) = 'blob' AND length(scanned_prefix_fingerprint) = 32
    ),
    last_source_ordinal INTEGER CHECK (
        last_source_ordinal IS NULL OR
        (typeof(last_source_ordinal) = 'integer'
            AND last_source_ordinal BETWEEN 0 AND 9223372036854775807)
    ),
    last_compatibility_event_id TEXT CHECK (
        last_compatibility_event_id IS NULL OR length(last_compatibility_event_id) = 36
    ),
    complete INTEGER NOT NULL CHECK (complete IN (0,1)),
    version INTEGER NOT NULL CHECK (
        typeof(version) = 'integer' AND version BETWEEN 0 AND 9223372036854775807
    ),
    created_at_ms INTEGER NOT NULL CHECK (
        typeof(created_at_ms) = 'integer' AND created_at_ms BETWEEN 0 AND 9223372036854775807
    ),
    updated_at_ms INTEGER NOT NULL CHECK (
        typeof(updated_at_ms) = 'integer' AND updated_at_ms BETWEEN created_at_ms AND 9223372036854775807
    ),
    PRIMARY KEY (root_thread_id,source_thread_id,adapter_version),
    CHECK ((last_source_ordinal IS NULL) = (last_compatibility_event_id IS NULL))
);

CREATE TABLE coordination_degradation_records (
    degradation_id TEXT PRIMARY KEY NOT NULL CHECK (length(degradation_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    state_epoch TEXT REFERENCES coordination_authority(state_epoch),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('exogenousTerminal','legacyReduction','recovery')),
    source_shape TEXT CHECK (source_shape IS NULL OR source_shape IN (
        'collabToolItem','derivedCollabEvent','subAgentActivity',
        'interAgentCommunication','turnComplete','turnAborted'
    )),
    source_thread_id TEXT CHECK (source_thread_id IS NULL OR length(source_thread_id) = 36),
    source_turn_id TEXT CHECK (source_turn_id IS NULL OR length(source_turn_id) BETWEEN 1 AND 128),
    source_item_id TEXT CHECK (source_item_id IS NULL OR length(source_item_id) BETWEEN 1 AND 128),
    source_ordinal INTEGER CHECK (
        source_ordinal IS NULL OR
        (typeof(source_ordinal) = 'integer' AND source_ordinal BETWEEN 0 AND 9223372036854775807)
    ),
    recovery_record_kind TEXT CHECK (
        recovery_record_kind IS NULL OR recovery_record_kind IN ('assignment','command','inbox')
    ),
    recovery_record_id TEXT CHECK (
        recovery_record_id IS NULL OR length(recovery_record_id) BETWEEN 1 AND 128
    ),
    semantic_slot TEXT NOT NULL CHECK (semantic_slot IN (
        'assignmentRequested','assignmentAccepted','assignmentGenerationClosed',
        'messageSubmissionRecorded','messageDurablyReceived','messageIncludedInModelInput',
        'waitStarted','waitEnded','interruptRequested','interruptDurablyReceived',
        'turnInterrupted','detached','dependencyDeclared','ownershipChanged','turnCompleted',
        'terminalResultObserved','handoffDeliveryAttempted','handoffDurablyReceived',
        'handoffIncludedInModelInput','handoffDeliveryFailed','legacyInteractionObserved'
    )),
    reason TEXT NOT NULL CHECK (reason IN (
        'coordinationTemporarilyUnavailable','missingProvenance','ambiguousSource','overLimit',
        'invalidLegacyValue','corruptSource','poisonedAttempt','expiredPayload','stateLossDegraded'
    )),
    target_thread_id TEXT CHECK (target_thread_id IS NULL OR length(target_thread_id) = 36),
    target_turn_id TEXT CHECK (target_turn_id IS NULL OR length(target_turn_id) BETWEEN 1 AND 128),
    terminal_kind TEXT CHECK (terminal_kind IS NULL OR terminal_kind IN ('completed','interrupted')),
    terminal_outcome TEXT CHECK (terminal_outcome IS NULL OR terminal_outcome IN (
        'succeeded','failed','cancelled','interrupted','unknown'
    )),
    included_generations_bytes BLOB CHECK (
        included_generations_bytes IS NULL OR
        (typeof(included_generations_bytes) = 'blob' AND length(included_generations_bytes) BETWEEN 1 AND 128)
    ),
    identity_bytes BLOB NOT NULL CHECK (
        typeof(identity_bytes) = 'blob' AND length(identity_bytes) BETWEEN 1 AND 1024
    ),
    identity_fingerprint BLOB NOT NULL CHECK (
        typeof(identity_fingerprint) = 'blob' AND length(identity_fingerprint) = 32
    ),
    canonical_record_bytes BLOB NOT NULL CHECK (
        typeof(canonical_record_bytes) = 'blob' AND length(canonical_record_bytes) BETWEEN 1 AND 4096
    ),
    canonical_record_fingerprint BLOB NOT NULL CHECK (
        typeof(canonical_record_fingerprint) = 'blob' AND length(canonical_record_fingerprint) = 32
    ),
    adapter_version INTEGER NOT NULL CHECK (adapter_version = 1),
    sanitizer_version INTEGER NOT NULL CHECK (sanitizer_version = 1),
    observed_at INTEGER NOT NULL CHECK (
        typeof(observed_at) = 'integer' AND observed_at BETWEEN 0 AND 9223372036854775807
    ),
    after_revision INTEGER NOT NULL CHECK (
        typeof(after_revision) = 'integer' AND after_revision BETWEEN 0 AND 9223372036854775807
    ),
    created_at_ms INTEGER NOT NULL CHECK (
        typeof(created_at_ms) = 'integer' AND created_at_ms BETWEEN 0 AND 9223372036854775807
    ),
    UNIQUE (root_thread_id, identity_fingerprint),
    CHECK (
        (source_kind = 'recovery' AND source_shape IS NULL AND source_thread_id IS NULL
            AND source_turn_id IS NULL AND source_item_id IS NULL AND source_ordinal IS NULL)
        OR (source_kind IN ('exogenousTerminal','legacyReduction')
            AND source_shape IS NOT NULL AND source_ordinal IS NOT NULL)
    ),
    CHECK (
        (source_kind = 'recovery' AND recovery_record_kind IS NOT NULL
            AND recovery_record_id IS NOT NULL)
        OR (source_kind != 'recovery' AND recovery_record_kind IS NULL
            AND recovery_record_id IS NULL)
    ),
    CHECK (
        (source_kind = 'exogenousTerminal' AND target_thread_id IS NOT NULL
            AND target_turn_id IS NOT NULL AND terminal_kind IS NOT NULL
            AND terminal_outcome IS NOT NULL AND included_generations_bytes IS NOT NULL)
        OR (source_kind != 'exogenousTerminal' AND target_thread_id IS NULL
            AND target_turn_id IS NULL AND terminal_kind IS NULL
            AND terminal_outcome IS NULL AND included_generations_bytes IS NULL)
    ),
    CHECK (
        (source_kind = 'exogenousTerminal' AND reason='coordinationTemporarilyUnavailable')
        OR (source_kind = 'legacyReduction' AND reason IN (
            'ambiguousSource','overLimit','invalidLegacyValue','corruptSource','stateLossDegraded'
        ))
        OR (source_kind = 'recovery' AND reason IN (
            'poisonedAttempt','expiredPayload','stateLossDegraded'
        ))
    )
);

CREATE INDEX idx_coordination_degradation_replay
    ON coordination_degradation_records(root_thread_id,after_revision,source_ordinal,degradation_id);

CREATE TABLE coordination_degradation_publication_outbox (
    degradation_id TEXT PRIMARY KEY NOT NULL
        REFERENCES coordination_degradation_records(degradation_id),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    after_revision INTEGER NOT NULL CHECK (
        typeof(after_revision) = 'integer' AND after_revision BETWEEN 0 AND 9223372036854775807
    ),
    source_ordinal INTEGER NOT NULL CHECK (
        typeof(source_ordinal) = 'integer' AND source_ordinal BETWEEN 0 AND 9223372036854775807
    ),
    stable_record_id TEXT NOT NULL CHECK (stable_record_id = degradation_id),
    status TEXT NOT NULL CHECK (status IN ('pending','leased','materialized','poisoned')),
    version INTEGER NOT NULL CHECK (typeof(version) = 'integer' AND version >= 0),
    lease_epoch INTEGER NOT NULL CHECK (typeof(lease_epoch) = 'integer' AND lease_epoch >= 0),
    retry_count INTEGER NOT NULL CHECK (retry_count BETWEEN 0 AND 8),
    retry_after_ms INTEGER NOT NULL CHECK (
        typeof(retry_after_ms) = 'integer' AND retry_after_ms BETWEEN 0 AND 9223372036854775807
    ),
    lease_expires_at_ms INTEGER CHECK (
        lease_expires_at_ms IS NULL OR
        (typeof(lease_expires_at_ms) = 'integer'
            AND lease_expires_at_ms BETWEEN 0 AND 9223372036854775807)
    ),
    failure_code TEXT CHECK (failure_code IS NULL OR failure_code IN (
        'stateUnavailable','stateQuarantined','corruptEvidence','retryExhausted','internal'
    )),
    created_at_ms INTEGER NOT NULL CHECK (
        typeof(created_at_ms) = 'integer' AND created_at_ms BETWEEN 0 AND 9223372036854775807
    ),
    updated_at_ms INTEGER NOT NULL CHECK (
        typeof(updated_at_ms) = 'integer' AND updated_at_ms BETWEEN created_at_ms AND 9223372036854775807
    ),
    CHECK ((status = 'leased') = (lease_expires_at_ms IS NOT NULL)),
    CHECK (status != 'poisoned' OR failure_code IS NOT NULL)
);

CREATE INDEX idx_coordination_degradation_outbox_claim
    ON coordination_degradation_publication_outbox(
        status,retry_after_ms,after_revision,source_ordinal,stable_record_id
    );

CREATE TRIGGER coordination_legacy_link_authority_guard BEFORE INSERT ON coordination_legacy_links
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_authority a JOIN coordination_roots r
      ON r.state_epoch=a.state_epoch
    WHERE a.singleton_id=1 AND a.status='active'
      AND a.state_epoch=NEW.state_epoch AND r.root_thread_id=NEW.root_thread_id
      AND NEW.after_revision<=r.committed_revision
      AND (NEW.suppressed_by_native_event_id IS NULL OR EXISTS (
          SELECT 1 FROM coordination_events e,
            json_each(CAST(e.canonical_event_bytes AS TEXT),'$.source.suppressionKeys.items') key
          WHERE e.event_id=NEW.suppressed_by_native_event_id
            AND e.root_thread_id=NEW.root_thread_id
            AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.source.source')='native'
            AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind')=NEW.semantic_slot
            AND json_extract(key.value,'$.shape')=NEW.source_shape
            AND json_extract(key.value,'$.sourceOrdinal')=NEW.source_ordinal
            AND ((NEW.source_item_id IS NULL
                    AND json_extract(key.value,'$.sourceItemId.status')!='known')
              OR (NEW.source_item_id IS NOT NULL
                    AND json_extract(key.value,'$.sourceItemId.status')='known'
                    AND json_extract(key.value,'$.sourceItemId.value')=NEW.source_item_id))
      ))
)
BEGIN SELECT RAISE(ABORT, 'coordination authority is not active for legacy link'); END;

CREATE TRIGGER coordination_legacy_link_immutable BEFORE UPDATE ON coordination_legacy_links
WHEN NEW.compatibility_event_id != OLD.compatibility_event_id
  OR NEW.root_thread_id != OLD.root_thread_id OR NEW.state_epoch != OLD.state_epoch
  OR NEW.source_shape != OLD.source_shape OR NEW.source_thread_id IS NOT OLD.source_thread_id
  OR NEW.source_turn_id IS NOT OLD.source_turn_id OR NEW.source_item_id IS NOT OLD.source_item_id
  OR NEW.source_ordinal != OLD.source_ordinal OR NEW.semantic_slot != OLD.semantic_slot
  OR NEW.source_identity_bytes != OLD.source_identity_bytes
  OR NEW.source_identity_fingerprint != OLD.source_identity_fingerprint
  OR NEW.canonical_event_bytes != OLD.canonical_event_bytes
  OR NEW.canonical_event_fingerprint != OLD.canonical_event_fingerprint
  OR NEW.adapter_version != OLD.adapter_version OR NEW.sanitizer_version != OLD.sanitizer_version
  OR NEW.after_revision != OLD.after_revision OR NEW.created_at_ms != OLD.created_at_ms
  OR (OLD.suppressed_by_native_event_id IS NOT NULL
      AND (NEW.suppressed_by_native_event_id IS NOT OLD.suppressed_by_native_event_id
          OR NEW.suppressed_at_ms IS NOT OLD.suppressed_at_ms))
  OR (OLD.suppressed_by_native_event_id IS NULL
      AND NEW.suppressed_by_native_event_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM coordination_events e,
            json_each(CAST(e.canonical_event_bytes AS TEXT),'$.source.suppressionKeys.items') key
          WHERE e.event_id=NEW.suppressed_by_native_event_id
            AND e.root_thread_id=OLD.root_thread_id
            AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.source.source')='native'
            AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind')=OLD.semantic_slot
            AND json_extract(key.value,'$.shape')=OLD.source_shape
            AND json_extract(key.value,'$.sourceOrdinal')=OLD.source_ordinal
            AND ((OLD.source_item_id IS NULL
                    AND json_extract(key.value,'$.sourceItemId.status')!='known')
              OR (OLD.source_item_id IS NOT NULL
                    AND json_extract(key.value,'$.sourceItemId.status')='known'
                    AND json_extract(key.value,'$.sourceItemId.value')=OLD.source_item_id))
      ))
BEGIN SELECT RAISE(ABORT, 'coordination legacy link is immutable or suppression is invalid'); END;

CREATE TRIGGER coordination_legacy_link_update_authority_guard BEFORE UPDATE ON coordination_legacy_links
WHEN NOT EXISTS (SELECT 1 FROM coordination_authority WHERE singleton_id=1 AND status='active')
BEGIN SELECT RAISE(ABORT, 'quarantined coordination authority is read-only'); END;

CREATE TRIGGER coordination_legacy_link_no_delete BEFORE DELETE ON coordination_legacy_links
BEGIN SELECT RAISE(ABORT, 'coordination legacy links are immutable'); END;

CREATE TRIGGER coordination_scan_checkpoint_authority_guard
BEFORE INSERT ON coordination_legacy_scan_checkpoints
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_authority a JOIN coordination_roots r ON r.state_epoch=a.state_epoch
    WHERE a.singleton_id=1 AND a.status='active' AND a.state_epoch=NEW.state_epoch
      AND r.root_thread_id=NEW.root_thread_id
)
BEGIN SELECT RAISE(ABORT, 'coordination authority is not active for scan checkpoint'); END;

CREATE TRIGGER coordination_scan_checkpoint_monotonic
BEFORE UPDATE ON coordination_legacy_scan_checkpoints
WHEN NOT EXISTS (SELECT 1 FROM coordination_authority WHERE singleton_id=1 AND status='active')
  OR NEW.root_thread_id != OLD.root_thread_id OR NEW.state_epoch != OLD.state_epoch
  OR NEW.source_thread_id != OLD.source_thread_id OR NEW.adapter_version != OLD.adapter_version
  OR NEW.version != OLD.version + 1 OR NEW.next_physical_ordinal < OLD.next_physical_ordinal
  OR NEW.created_at_ms != OLD.created_at_ms OR NEW.updated_at_ms < OLD.updated_at_ms
  OR (OLD.complete=1 AND NEW.complete != 1)
BEGIN SELECT RAISE(ABORT, 'coordination scan checkpoint is fenced or non-monotonic'); END;

CREATE TRIGGER coordination_scan_checkpoint_no_delete BEFORE DELETE ON coordination_legacy_scan_checkpoints
BEGIN SELECT RAISE(ABORT, 'coordination scan checkpoints are durable'); END;

CREATE TRIGGER coordination_degradation_authority_guard
BEFORE INSERT ON coordination_degradation_records
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_authority a JOIN coordination_roots r ON r.state_epoch=a.state_epoch
    WHERE a.singleton_id=1 AND a.status='active' AND r.root_thread_id=NEW.root_thread_id
      AND (NEW.state_epoch IS NULL OR NEW.state_epoch=a.state_epoch)
      AND NEW.after_revision<=r.committed_revision
)
BEGIN SELECT RAISE(ABORT, 'coordination authority is not active for degradation'); END;

CREATE TRIGGER coordination_degradation_immutable BEFORE UPDATE ON coordination_degradation_records
BEGIN SELECT RAISE(ABORT, 'coordination degradation records are immutable'); END;
CREATE TRIGGER coordination_degradation_no_delete BEFORE DELETE ON coordination_degradation_records
BEGIN SELECT RAISE(ABORT, 'coordination degradation records are immutable'); END;

CREATE TRIGGER coordination_degradation_outbox_insert_guard
BEFORE INSERT ON coordination_degradation_publication_outbox
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_degradation_records d
    JOIN coordination_authority a ON a.singleton_id=1 AND a.status='active'
    WHERE d.degradation_id=NEW.degradation_id AND d.root_thread_id=NEW.root_thread_id
      AND d.after_revision=NEW.after_revision AND COALESCE(d.source_ordinal,0)=NEW.source_ordinal
)
  OR NEW.status!='pending' OR NEW.version!=0 OR NEW.lease_epoch!=0 OR NEW.retry_count!=0
  OR NEW.retry_after_ms!=0 OR NEW.lease_expires_at_ms IS NOT NULL
  OR NEW.failure_code IS NOT NULL OR NEW.updated_at_ms!=NEW.created_at_ms
BEGIN SELECT RAISE(ABORT, 'coordination degradation outbox row is incoherent'); END;

CREATE TRIGGER coordination_degradation_outbox_transition
BEFORE UPDATE ON coordination_degradation_publication_outbox
WHEN NOT EXISTS (SELECT 1 FROM coordination_authority WHERE singleton_id=1 AND status='active')
  OR NEW.degradation_id != OLD.degradation_id OR NEW.root_thread_id != OLD.root_thread_id
  OR NEW.after_revision != OLD.after_revision OR NEW.source_ordinal != OLD.source_ordinal
  OR NEW.stable_record_id != OLD.stable_record_id OR NEW.created_at_ms != OLD.created_at_ms
  OR NEW.version != OLD.version + 1 OR NEW.lease_epoch < OLD.lease_epoch
  OR NEW.retry_count < OLD.retry_count OR NEW.updated_at_ms < OLD.updated_at_ms
  OR OLD.status IN ('materialized','poisoned')
  OR (OLD.status='pending' AND NEW.status NOT IN ('leased','poisoned'))
  OR (OLD.status='leased' AND NEW.status NOT IN ('leased','pending','materialized','poisoned'))
  OR COALESCE(NOT (
      (OLD.status='pending' AND NEW.status='leased'
        AND NEW.lease_epoch=OLD.lease_epoch+1 AND NEW.retry_count=OLD.retry_count
        AND NEW.retry_after_ms=OLD.retry_after_ms AND NEW.lease_expires_at_ms>NEW.updated_at_ms
        AND NEW.failure_code IS NULL)
      OR (OLD.status='leased' AND NEW.status='leased'
        AND OLD.lease_expires_at_ms<=NEW.updated_at_ms
        AND NEW.lease_epoch=OLD.lease_epoch+1 AND NEW.retry_count=OLD.retry_count
        AND NEW.retry_after_ms=OLD.retry_after_ms AND NEW.lease_expires_at_ms>NEW.updated_at_ms
        AND NEW.failure_code IS NULL)
      OR (OLD.status='leased' AND NEW.status='pending'
        AND OLD.lease_expires_at_ms>NEW.updated_at_ms
        AND NEW.lease_epoch=OLD.lease_epoch AND NEW.retry_count=OLD.retry_count+1
        AND NEW.retry_after_ms>NEW.updated_at_ms AND NEW.retry_after_ms>=OLD.retry_after_ms
        AND NEW.lease_expires_at_ms IS NULL AND NEW.failure_code IS NOT NULL)
      OR (OLD.status='leased' AND NEW.status='materialized'
        AND OLD.lease_expires_at_ms>NEW.updated_at_ms
        AND NEW.lease_epoch=OLD.lease_epoch AND NEW.retry_count=OLD.retry_count
        AND NEW.retry_after_ms=OLD.retry_after_ms AND NEW.lease_expires_at_ms IS NULL
        AND NEW.failure_code IS NULL)
      OR (OLD.status='leased' AND NEW.status='poisoned'
        AND OLD.lease_expires_at_ms>NEW.updated_at_ms
        AND NEW.lease_epoch=OLD.lease_epoch AND NEW.retry_count=OLD.retry_count
        AND NEW.retry_after_ms=OLD.retry_after_ms AND NEW.lease_expires_at_ms IS NULL
        AND NEW.failure_code IS NOT NULL)
  ),1)
BEGIN SELECT RAISE(ABORT, 'coordination degradation outbox transition is invalid'); END;

CREATE TRIGGER coordination_degradation_outbox_no_delete
BEFORE DELETE ON coordination_degradation_publication_outbox
BEGIN SELECT RAISE(ABORT, 'coordination degradation outbox is durable'); END;
