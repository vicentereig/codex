CREATE TABLE coordination_inbox (
    receipt_id TEXT PRIMARY KEY NOT NULL CHECK (length(receipt_id) = 36),
    command_operation_id TEXT NOT NULL UNIQUE
        REFERENCES coordination_commands(operation_id),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    intent_event_id TEXT NOT NULL REFERENCES coordination_events(event_id),
    receipt_event_id TEXT NOT NULL UNIQUE REFERENCES coordination_events(event_id),
    resolution_event_id TEXT UNIQUE REFERENCES coordination_events(event_id),
    sender_thread_id TEXT NOT NULL CHECK (length(sender_thread_id) = 36),
    sender_turn_id TEXT NOT NULL CHECK (length(sender_turn_id) BETWEEN 1 AND 128),
    recipient_thread_id TEXT NOT NULL CHECK (length(recipient_thread_id) = 36),
    recipient_turn_id TEXT NOT NULL CHECK (length(recipient_turn_id) BETWEEN 1 AND 128),
    operation_kind TEXT NOT NULL CHECK (operation_kind IN (
        'assignmentSpawn','assignmentFollowup','message','interrupt'
    )),
    target_assignment_id TEXT NOT NULL CHECK (length(target_assignment_id) = 36),
    target_generation INTEGER NOT NULL CHECK (
        typeof(target_generation) = 'integer' AND target_generation BETWEEN 1 AND 2147483647
    ),
    captured_head_generation INTEGER CHECK (
        captured_head_generation IS NULL
        OR (typeof(captured_head_generation) = 'integer'
            AND captured_head_generation BETWEEN 1 AND 2147483647)
    ),
    captured_turn_set_bytes BLOB CHECK (
        captured_turn_set_bytes IS NULL
        OR (typeof(captured_turn_set_bytes) = 'blob'
            AND hex(substr(captured_turn_set_bytes, 1, 1)) IN ('01','02','03','04')
            AND length(captured_turn_set_bytes) = CASE hex(substr(captured_turn_set_bytes, 1, 1))
                WHEN '01' THEN 5 WHEN '02' THEN 9 WHEN '03' THEN 13 WHEN '04' THEN 17 END)
    ),
    captured_turn_set_fingerprint BLOB CHECK (
        captured_turn_set_fingerprint IS NULL
        OR (typeof(captured_turn_set_fingerprint) = 'blob'
            AND length(captured_turn_set_fingerprint) = 32)
    ),
    receipt_tuple_bytes BLOB NOT NULL CHECK (
        typeof(receipt_tuple_bytes) = 'blob' AND length(receipt_tuple_bytes) BETWEEN 1 AND 1024
    ),
    receipt_tuple_fingerprint BLOB NOT NULL CHECK (
        typeof(receipt_tuple_fingerprint) = 'blob' AND length(receipt_tuple_fingerprint) = 32
    ),
    delivery_fingerprint BLOB NOT NULL CHECK (
        typeof(delivery_fingerprint) = 'blob' AND length(delivery_fingerprint) = 32
    ),
    sender_command_fingerprint BLOB NOT NULL CHECK (
        typeof(sender_command_fingerprint) = 'blob' AND length(sender_command_fingerprint) = 32
    ),
    encoded_payload_bytes INTEGER NOT NULL CHECK (
        typeof(encoded_payload_bytes) = 'integer' AND encoded_payload_bytes BETWEEN 0 AND 65536
    ),
    ciphertext BLOB CHECK (
        ciphertext IS NULL
        OR (typeof(ciphertext) = 'blob' AND length(ciphertext) = encoded_payload_bytes)
    ),
    ciphertext_fingerprint BLOB NOT NULL CHECK (
        typeof(ciphertext_fingerprint) = 'blob' AND length(ciphertext_fingerprint) = 32
    ),
    lifecycle TEXT NOT NULL CHECK (
        lifecycle IN ('received','leased','selected','processed','poisoned','expired')
    ),
    version INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(version) = 'integer' AND version BETWEEN 0 AND 9223372036854775807
    ),
    claim_count INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(claim_count) = 'integer' AND claim_count BETWEEN 0 AND 9223372036854775807
    ),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(retry_count) = 'integer' AND retry_count BETWEEN 0 AND 9223372036854775807
    ),
    lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(lease_epoch) = 'integer' AND lease_epoch BETWEEN 0 AND 9223372036854775807
    ),
    retry_after_ms INTEGER NOT NULL CHECK (
        typeof(retry_after_ms) = 'integer' AND retry_after_ms BETWEEN 0 AND 9223372036854775807
    ),
    lease_expires_at_ms INTEGER CHECK (
        lease_expires_at_ms IS NULL
        OR (typeof(lease_expires_at_ms) = 'integer'
            AND lease_expires_at_ms BETWEEN 0 AND 9223372036854775807)
    ),
    lease_claim_operation_id TEXT CHECK (
        lease_claim_operation_id IS NULL OR length(lease_claim_operation_id) = 36
    ),
    failure_code TEXT CHECK (failure_code IS NULL OR failure_code IN (
        'unauthorized','stateUnavailable','stateQuarantined','invalidPayload',
        'payloadOverLimit','targetUnavailable','generationFenced','terminalConflict',
        'ownershipConflict','idempotencyConflict','retryExhausted','corruptEvidence','internal'
    )),
    sender_intent_at_ms INTEGER NOT NULL CHECK (
        typeof(sender_intent_at_ms) = 'integer'
        AND sender_intent_at_ms BETWEEN 0 AND 9223372036854775807
    ),
    durable_received_at_ms INTEGER NOT NULL CHECK (
        typeof(durable_received_at_ms) = 'integer'
        AND durable_received_at_ms BETWEEN sender_intent_at_ms AND 9223372036854775807
    ),
    terminal_at_ms INTEGER CHECK (
        terminal_at_ms IS NULL
        OR (typeof(terminal_at_ms) = 'integer'
            AND terminal_at_ms BETWEEN durable_received_at_ms AND 9223372036854775807)
    ),
    absolute_expires_at_ms INTEGER NOT NULL CHECK (
        typeof(absolute_expires_at_ms) = 'integer'
        AND absolute_expires_at_ms BETWEEN durable_received_at_ms
            AND MIN(9223372036854775807, sender_intent_at_ms + 604800000)
    ),
    expires_at_ms INTEGER NOT NULL CHECK (
        typeof(expires_at_ms) = 'integer'
        AND expires_at_ms BETWEEN durable_received_at_ms AND absolute_expires_at_ms
    ),
    purged_at_ms INTEGER CHECK (
        purged_at_ms IS NULL
        OR (typeof(purged_at_ms) = 'integer'
            AND purged_at_ms BETWEEN durable_received_at_ms AND 9223372036854775807)
    ),
    updated_at_ms INTEGER NOT NULL CHECK (
        typeof(updated_at_ms) = 'integer'
        AND updated_at_ms BETWEEN durable_received_at_ms AND 9223372036854775807
    ),
    UNIQUE (root_thread_id, recipient_thread_id, receipt_tuple_fingerprint),
    FOREIGN KEY (target_assignment_id, target_generation)
        REFERENCES coordination_assignment_generations(assignment_id, generation),
    CHECK ((ciphertext IS NULL) = (purged_at_ms IS NOT NULL)),
    CHECK (lifecycle NOT IN ('received','leased','selected') OR ciphertext IS NOT NULL),
    CHECK ((lifecycle IN ('processed','poisoned')) = (terminal_at_ms IS NOT NULL)),
    CHECK (lifecycle != 'poisoned' OR failure_code IS NOT NULL),
    CHECK (lifecycle != 'expired' OR ciphertext IS NULL),
    CHECK (resolution_event_id IS NULL OR operation_kind = 'interrupt'),
    CHECK (operation_kind != 'interrupt' OR ((lifecycle = 'processed') = (resolution_event_id IS NOT NULL))),
    CHECK ((lifecycle IN ('leased','selected')) = (lease_expires_at_ms IS NOT NULL)),
    CHECK ((lifecycle IN ('leased','selected')) = (lease_claim_operation_id IS NOT NULL)),
    CHECK (lease_expires_at_ms IS NULL OR lease_expires_at_ms <= expires_at_ms),
    CHECK (
        (operation_kind IN ('assignmentSpawn','assignmentFollowup')
            AND captured_head_generation IS NULL
            AND captured_turn_set_bytes IS NULL
            AND captured_turn_set_fingerprint IS NULL)
        OR (operation_kind = 'message'
            AND captured_head_generation = target_generation
            AND captured_turn_set_bytes IS NULL
            AND captured_turn_set_fingerprint IS NULL)
        OR (operation_kind = 'interrupt'
            AND captured_head_generation = target_generation
            AND captured_turn_set_bytes IS NOT NULL
            AND captured_turn_set_fingerprint IS NOT NULL)
    )
);

CREATE TABLE coordination_inbox_inclusions (
    receipt_id TEXT NOT NULL REFERENCES coordination_inbox(receipt_id),
    inference_attempt_id TEXT NOT NULL CHECK (length(inference_attempt_id) BETWEEN 1 AND 128),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    target_turn_id TEXT NOT NULL CHECK (length(target_turn_id) BETWEEN 1 AND 128),
    delivery_fingerprint BLOB NOT NULL CHECK (
        typeof(delivery_fingerprint) = 'blob' AND length(delivery_fingerprint) = 32
    ),
    selected_at_ms INTEGER NOT NULL CHECK (
        typeof(selected_at_ms) = 'integer' AND selected_at_ms BETWEEN 0 AND 9223372036854775807
    ),
    lease_expires_at_ms INTEGER NOT NULL CHECK (
        typeof(lease_expires_at_ms) = 'integer'
        AND lease_expires_at_ms BETWEEN selected_at_ms + 1 AND 9223372036854775807
    ),
    semantic_claim INTEGER NOT NULL CHECK (semantic_claim IN (0,1)),
    semantic_event_id TEXT UNIQUE REFERENCES coordination_events(event_id),
    inbox_version INTEGER NOT NULL CHECK (
        typeof(inbox_version) = 'integer' AND inbox_version BETWEEN 1 AND 9223372036854775807
    ),
    lease_epoch INTEGER NOT NULL CHECK (
        typeof(lease_epoch) = 'integer' AND lease_epoch BETWEEN 1 AND 9223372036854775807
    ),
    claim_operation_id TEXT NOT NULL CHECK (length(claim_operation_id) = 36),
    transport_state TEXT NOT NULL CHECK (
        transport_state IN ('selected','sendSucceeded','sendFailed','sendUnknown')
    ),
    transport_completed_at_ms INTEGER CHECK (
        transport_completed_at_ms IS NULL
        OR (typeof(transport_completed_at_ms) = 'integer'
            AND transport_completed_at_ms BETWEEN selected_at_ms AND 9223372036854775807)
    ),
    retry_after_ms INTEGER CHECK (
        retry_after_ms IS NULL
        OR (typeof(retry_after_ms) = 'integer'
            AND retry_after_ms BETWEEN selected_at_ms AND 9223372036854775807)
    ),
    version INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(version) = 'integer' AND version BETWEEN 0 AND 9223372036854775807
    ),
    failure_code TEXT CHECK (failure_code IS NULL OR failure_code IN (
        'unauthorized','stateUnavailable','stateQuarantined','invalidPayload',
        'payloadOverLimit','targetUnavailable','generationFenced','terminalConflict',
        'ownershipConflict','idempotencyConflict','retryExhausted','corruptEvidence','internal'
    )),
    PRIMARY KEY (receipt_id, inference_attempt_id),
    CHECK ((transport_state = 'selected') = (transport_completed_at_ms IS NULL)),
    CHECK ((transport_state = 'sendFailed') = (failure_code IS NOT NULL)),
    CHECK ((transport_state IN ('sendFailed','sendUnknown')) = (retry_after_ms IS NOT NULL)),
    CHECK (semantic_claim = 1 OR semantic_event_id IS NULL)
);

CREATE UNIQUE INDEX idx_coordination_inbox_one_semantic_claim
    ON coordination_inbox_inclusions(receipt_id) WHERE semantic_claim = 1;
CREATE UNIQUE INDEX idx_coordination_inbox_one_live_selection
    ON coordination_inbox_inclusions(receipt_id) WHERE transport_state = 'selected';
CREATE INDEX idx_coordination_inbox_claimable
    ON coordination_inbox(lifecycle,retry_after_ms,expires_at_ms,receipt_id);
CREATE INDEX idx_coordination_inbox_reclaimable
    ON coordination_inbox(lifecycle,lease_expires_at_ms,receipt_id)
    WHERE lifecycle IN ('leased','selected');
CREATE INDEX idx_coordination_inbox_expirable
    ON coordination_inbox(expires_at_ms,receipt_id) WHERE ciphertext IS NOT NULL;
CREATE INDEX idx_coordination_inbox_interrupt_barrier
    ON coordination_inbox(root_thread_id,recipient_turn_id,lifecycle,operation_kind);

CREATE TRIGGER coordination_inbox_command_guard
BEFORE INSERT ON coordination_inbox
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_commands c
    WHERE c.operation_id = NEW.command_operation_id
      AND c.root_thread_id = NEW.root_thread_id
      AND c.intent_event_id = NEW.intent_event_id
      AND c.sender_thread_id = NEW.sender_thread_id
      AND c.sender_turn_id = NEW.sender_turn_id
      AND c.operation_kind = NEW.operation_kind
      AND c.target_thread_id = NEW.recipient_thread_id
      AND c.target_assignment_id = NEW.target_assignment_id
      AND c.target_generation = NEW.target_generation
      AND (c.target_turn_id IS NULL OR c.target_turn_id = NEW.recipient_turn_id)
      AND c.captured_head_generation IS NEW.captured_head_generation
      AND c.captured_turn_set_bytes IS NEW.captured_turn_set_bytes
      AND c.captured_turn_set_fingerprint IS NEW.captured_turn_set_fingerprint
      AND c.command_fingerprint = NEW.sender_command_fingerprint
      AND c.encoded_payload_bytes = NEW.encoded_payload_bytes
      AND c.ciphertext_fingerprint = NEW.ciphertext_fingerprint
      AND c.ciphertext IS NOT NULL
      AND c.ciphertext = NEW.ciphertext
      AND c.expires_at_ms = NEW.absolute_expires_at_ms
      AND NEW.expires_at_ms = NEW.absolute_expires_at_ms
      AND c.intent_at_ms = NEW.sender_intent_at_ms
      AND NEW.durable_received_at_ms < c.expires_at_ms
)
BEGIN SELECT RAISE(ABORT, 'coordination inbox command is incoherent or inaccessible'); END;

CREATE TRIGGER coordination_inbox_receipt_event_guard
BEFORE INSERT ON coordination_inbox
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_events e
    WHERE e.event_id = NEW.receipt_event_id
      AND e.root_thread_id = NEW.root_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.operationId') = NEW.command_operation_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.actor.threadId') = NEW.recipient_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.actor.turnId.status') = 'known'
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.actor.turnId.value') = NEW.recipient_turn_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.receiptId') = NEW.receipt_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.causes.omittedCount') = 0
      AND json_array_length(CAST(e.canonical_event_bytes AS TEXT),'$.causes.items') = 1
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.causes.items[0]') = NEW.intent_event_id
      AND EXISTS (
          SELECT 1 FROM coordination_projection_outbox o
          WHERE o.event_id = e.event_id
      )
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.principal.threadId') = NEW.recipient_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.status') = 'known'
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId') = NEW.target_assignment_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.generation') = NEW.target_generation
      AND (
          (NEW.operation_kind IN ('assignmentSpawn','assignmentFollowup')
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'assignmentAccepted'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.boundTurnId.status') = 'known'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.boundTurnId.value') = NEW.recipient_turn_id)
          OR (NEW.operation_kind = 'message'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'messageDurablyReceived')
          OR (NEW.operation_kind = 'interrupt'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'interruptDurablyReceived')
      )
)
BEGIN SELECT RAISE(ABORT, 'coordination inbox receipt event is incoherent'); END;

CREATE TRIGGER coordination_inbox_identity_immutable
BEFORE UPDATE ON coordination_inbox
WHEN NEW.receipt_id != OLD.receipt_id
  OR NEW.command_operation_id != OLD.command_operation_id
  OR NEW.root_thread_id != OLD.root_thread_id
  OR NEW.intent_event_id != OLD.intent_event_id
  OR NEW.receipt_event_id != OLD.receipt_event_id
  OR NEW.sender_thread_id != OLD.sender_thread_id
  OR NEW.sender_turn_id != OLD.sender_turn_id
  OR NEW.recipient_thread_id != OLD.recipient_thread_id
  OR NEW.recipient_turn_id != OLD.recipient_turn_id
  OR NEW.operation_kind != OLD.operation_kind
  OR NEW.target_assignment_id != OLD.target_assignment_id
  OR NEW.target_generation != OLD.target_generation
  OR NEW.captured_head_generation IS NOT OLD.captured_head_generation
  OR NEW.captured_turn_set_bytes IS NOT OLD.captured_turn_set_bytes
  OR NEW.captured_turn_set_fingerprint IS NOT OLD.captured_turn_set_fingerprint
  OR NEW.receipt_tuple_bytes != OLD.receipt_tuple_bytes
  OR NEW.receipt_tuple_fingerprint != OLD.receipt_tuple_fingerprint
  OR NEW.delivery_fingerprint != OLD.delivery_fingerprint
  OR NEW.sender_command_fingerprint != OLD.sender_command_fingerprint
  OR NEW.encoded_payload_bytes != OLD.encoded_payload_bytes
  OR NEW.ciphertext_fingerprint != OLD.ciphertext_fingerprint
  OR NEW.sender_intent_at_ms != OLD.sender_intent_at_ms
  OR NEW.durable_received_at_ms != OLD.durable_received_at_ms
  OR NEW.absolute_expires_at_ms != OLD.absolute_expires_at_ms
BEGIN SELECT RAISE(ABORT, 'coordination inbox identity is immutable'); END;

CREATE TRIGGER coordination_inbox_transition_guard
BEFORE UPDATE ON coordination_inbox
WHEN NEW.version != OLD.version + 1
  OR NEW.claim_count < OLD.claim_count
  OR NEW.retry_count < OLD.retry_count
  OR NEW.lease_epoch < OLD.lease_epoch
  OR NEW.expires_at_ms > OLD.expires_at_ms
  OR NEW.updated_at_ms < OLD.updated_at_ms
  OR (OLD.ciphertext IS NULL AND NEW.ciphertext IS NOT NULL)
  OR (OLD.purged_at_ms IS NOT NULL AND NEW.purged_at_ms IS NOT OLD.purged_at_ms)
  OR (OLD.terminal_at_ms IS NOT NULL AND NEW.terminal_at_ms IS NOT OLD.terminal_at_ms)
  OR COALESCE(NOT (
      (OLD.lifecycle = 'received' AND NEW.lifecycle = 'leased'
          AND NEW.claim_count = OLD.claim_count + 1
          AND NEW.retry_count = OLD.retry_count
          AND NEW.lease_epoch = OLD.lease_epoch + 1
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND NEW.lease_expires_at_ms > NEW.updated_at_ms
          AND NEW.lease_claim_operation_id IS NOT NULL
          AND NEW.updated_at_ms < OLD.expires_at_ms
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms
          AND NEW.resolution_event_id IS OLD.resolution_event_id)
      OR (OLD.lifecycle = 'leased' AND NEW.lifecycle = 'selected'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.retry_count = OLD.retry_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.lease_expires_at_ms IS OLD.lease_expires_at_ms
          AND NEW.lease_claim_operation_id = OLD.lease_claim_operation_id
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND NEW.updated_at_ms < OLD.lease_expires_at_ms
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms
          AND NEW.resolution_event_id IS OLD.resolution_event_id)
      OR (OLD.lifecycle = 'leased' AND NEW.lifecycle = 'received'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.retry_count = OLD.retry_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.retry_after_ms >= OLD.retry_after_ms
          AND NEW.lease_expires_at_ms IS NULL
          AND NEW.lease_claim_operation_id IS NULL
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND NEW.updated_at_ms >= OLD.lease_expires_at_ms
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms
          AND NEW.resolution_event_id IS OLD.resolution_event_id)
      OR (OLD.lifecycle = 'selected' AND NEW.lifecycle = 'received'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.retry_count = OLD.retry_count + 1
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.retry_after_ms >= NEW.updated_at_ms
          AND NEW.lease_expires_at_ms IS NULL
          AND NEW.lease_claim_operation_id IS NULL
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms
          AND NEW.resolution_event_id IS OLD.resolution_event_id
          AND EXISTS (SELECT 1 FROM coordination_inbox_inclusions x
              WHERE x.receipt_id = OLD.receipt_id
                AND x.transport_state IN ('sendFailed','sendUnknown')
                AND x.transport_completed_at_ms = NEW.updated_at_ms
                AND x.retry_after_ms = NEW.retry_after_ms
                AND x.failure_code IS NEW.failure_code))
      OR (OLD.lifecycle = 'selected' AND NEW.lifecycle = 'processed'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.retry_count = OLD.retry_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.lease_expires_at_ms IS NULL
          AND NEW.lease_claim_operation_id IS NULL
          AND NEW.failure_code IS NULL
          AND OLD.terminal_at_ms IS NULL
          AND NEW.terminal_at_ms = NEW.updated_at_ms
          AND NEW.expires_at_ms = MIN(OLD.expires_at_ms, NEW.terminal_at_ms + 86400000)
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms
          AND NEW.resolution_event_id IS OLD.resolution_event_id
          AND EXISTS (SELECT 1 FROM coordination_inbox_inclusions x
              WHERE x.receipt_id = OLD.receipt_id
                AND x.transport_state = 'sendSucceeded'
                AND x.transport_completed_at_ms = NEW.updated_at_ms))
      OR (OLD.lifecycle IN ('received','leased','selected') AND NEW.lifecycle = 'expired'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.retry_count = OLD.retry_count + CASE WHEN OLD.lifecycle = 'selected' THEN 1 ELSE 0 END
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.retry_after_ms >= OLD.retry_after_ms
          AND NEW.lease_expires_at_ms IS NULL
          AND NEW.lease_claim_operation_id IS NULL
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND NEW.ciphertext IS NULL
          AND OLD.ciphertext IS NOT NULL
          AND NEW.purged_at_ms = NEW.updated_at_ms
          AND NEW.resolution_event_id IS OLD.resolution_event_id
          AND (OLD.lifecycle = 'selected' OR NEW.failure_code IS OLD.failure_code)
          AND (NEW.updated_at_ms >= OLD.expires_at_ms
              OR (OLD.lifecycle = 'selected' AND NEW.retry_after_ms >= OLD.expires_at_ms))
          AND (OLD.lifecycle != 'selected' OR EXISTS (
              SELECT 1 FROM coordination_inbox_inclusions x
              WHERE x.receipt_id = OLD.receipt_id
                AND x.transport_state IN ('sendFailed','sendUnknown')
                AND x.transport_completed_at_ms = NEW.updated_at_ms
                AND x.retry_after_ms = NEW.retry_after_ms
                AND x.failure_code IS NEW.failure_code)))
      OR (OLD.lifecycle = 'received' AND OLD.operation_kind = 'interrupt'
          AND NEW.lifecycle = 'processed'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.retry_count = OLD.retry_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.lease_expires_at_ms IS NULL
          AND NEW.lease_claim_operation_id IS NULL
          AND NEW.failure_code IS OLD.failure_code
          AND OLD.terminal_at_ms IS NULL
          AND NEW.terminal_at_ms = NEW.updated_at_ms
          AND NEW.expires_at_ms = MIN(OLD.expires_at_ms, NEW.terminal_at_ms + 86400000)
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms
          AND OLD.resolution_event_id IS NULL
          AND NEW.resolution_event_id IS NOT NULL)
      OR (OLD.lifecycle IN ('processed','poisoned') AND NEW.lifecycle = OLD.lifecycle
          AND NEW.claim_count = OLD.claim_count
          AND NEW.retry_count = OLD.retry_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.lease_expires_at_ms IS OLD.lease_expires_at_ms
          AND NEW.lease_claim_operation_id IS OLD.lease_claim_operation_id
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND OLD.ciphertext IS NOT NULL
          AND NEW.ciphertext IS NULL
          AND NEW.purged_at_ms = NEW.updated_at_ms
          AND NEW.updated_at_ms >= OLD.expires_at_ms
          AND NEW.resolution_event_id IS OLD.resolution_event_id)
  ), 1)
BEGIN SELECT RAISE(ABORT, 'invalid coordination inbox transition'); END;

CREATE TRIGGER coordination_inbox_resolution_guard
BEFORE UPDATE ON coordination_inbox
WHEN (OLD.resolution_event_id IS NOT NULL AND NEW.resolution_event_id IS NOT OLD.resolution_event_id)
  OR (OLD.resolution_event_id IS NULL AND NEW.resolution_event_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM coordination_events e
      WHERE e.event_id = NEW.resolution_event_id
        AND e.root_thread_id = NEW.root_thread_id
        AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'turnInterrupted'
        AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.targetTurnId') = NEW.recipient_turn_id
        AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.principal.threadId') = NEW.recipient_thread_id
        AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.status') = 'known'
        AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId') = NEW.target_assignment_id
        AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.generation') = NEW.target_generation
        AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.interruptionReason.reason') = 'requested'
        AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.interruptionReason.operationId') = NEW.command_operation_id
        AND EXISTS (
            SELECT 1 FROM json_each(CAST(e.canonical_event_bytes AS TEXT),'$.causes.items') cause
            WHERE cause.value = NEW.receipt_event_id
        )
  ))
BEGIN SELECT RAISE(ABORT, 'coordination interrupt resolution is incoherent'); END;

CREATE TRIGGER coordination_inbox_no_delete
BEFORE DELETE ON coordination_inbox
BEGIN SELECT RAISE(ABORT, 'coordination inbox cannot be deleted'); END;

CREATE TRIGGER coordination_inclusion_insert_guard
BEFORE INSERT ON coordination_inbox_inclusions
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_inbox i
    WHERE i.receipt_id = NEW.receipt_id
      AND i.root_thread_id = NEW.root_thread_id
      AND i.recipient_turn_id = NEW.target_turn_id
      AND i.delivery_fingerprint = NEW.delivery_fingerprint
      AND i.operation_kind != 'interrupt'
      AND i.lifecycle = 'leased'
      AND NEW.inbox_version = i.version + 1
      AND NEW.lease_epoch = i.lease_epoch
      AND i.ciphertext IS NOT NULL
      AND i.expires_at_ms > NEW.selected_at_ms
      AND i.lease_expires_at_ms = NEW.lease_expires_at_ms
      AND i.lease_claim_operation_id = NEW.claim_operation_id
      AND (
          (NEW.semantic_claim = 1 AND NOT EXISTS (
              SELECT 1 FROM coordination_inbox_inclusions prior
              WHERE prior.receipt_id = NEW.receipt_id))
          OR (NEW.semantic_claim = 0 AND EXISTS (
              SELECT 1 FROM coordination_inbox_inclusions prior
              WHERE prior.receipt_id = NEW.receipt_id
                AND prior.semantic_claim = 1
                AND prior.transport_state IN ('sendFailed','sendUnknown')))
      )
      AND (
          (i.operation_kind IN ('assignmentSpawn','assignmentFollowup')
              AND NEW.semantic_event_id IS NULL)
          OR (i.operation_kind = 'message'
              AND ((NEW.semantic_claim = 1 AND NEW.semantic_event_id IS NOT NULL)
                  OR (NEW.semantic_claim = 0 AND NEW.semantic_event_id IS NULL)))
      )
)
BEGIN SELECT RAISE(ABORT, 'coordination inclusion is incoherent'); END;

CREATE TRIGGER coordination_inclusion_event_guard
BEFORE INSERT ON coordination_inbox_inclusions
WHEN NEW.semantic_event_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM coordination_inbox i
    JOIN coordination_events e ON e.event_id = NEW.semantic_event_id
    WHERE i.receipt_id = NEW.receipt_id
      AND e.root_thread_id = NEW.root_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'messageIncludedInModelInput'
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.operationId') = i.command_operation_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.receiptId') = i.receipt_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.inferenceAttemptId') = NEW.inference_attempt_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.causes.omittedCount') = 0
      AND json_array_length(CAST(e.canonical_event_bytes AS TEXT),'$.causes.items') = 1
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.causes.items[0]') = i.receipt_event_id
      AND EXISTS (
          SELECT 1 FROM coordination_projection_outbox o
          WHERE o.event_id = e.event_id
      )
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.principal.threadId') = i.recipient_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.status') = 'known'
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId') = i.target_assignment_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.generation') = i.target_generation
)
BEGIN SELECT RAISE(ABORT, 'coordination inclusion event is incoherent'); END;

CREATE TRIGGER coordination_inclusion_identity_immutable
BEFORE UPDATE ON coordination_inbox_inclusions
WHEN NEW.receipt_id != OLD.receipt_id
  OR NEW.inference_attempt_id != OLD.inference_attempt_id
  OR NEW.root_thread_id != OLD.root_thread_id
  OR NEW.target_turn_id != OLD.target_turn_id
  OR NEW.delivery_fingerprint != OLD.delivery_fingerprint
  OR NEW.selected_at_ms != OLD.selected_at_ms
  OR NEW.lease_expires_at_ms != OLD.lease_expires_at_ms
  OR NEW.semantic_claim != OLD.semantic_claim
  OR NEW.semantic_event_id IS NOT OLD.semantic_event_id
  OR NEW.inbox_version != OLD.inbox_version
  OR NEW.lease_epoch != OLD.lease_epoch
  OR NEW.claim_operation_id != OLD.claim_operation_id
BEGIN SELECT RAISE(ABORT, 'coordination inclusion identity is immutable'); END;

CREATE TRIGGER coordination_inclusion_transition_guard
BEFORE UPDATE ON coordination_inbox_inclusions
WHEN OLD.transport_state != 'selected'
  OR NEW.transport_state NOT IN ('sendSucceeded','sendFailed','sendUnknown')
  OR NEW.version != OLD.version + 1
  OR NEW.transport_completed_at_ms IS NULL
  OR NEW.transport_completed_at_ms < OLD.selected_at_ms
  OR OLD.retry_after_ms IS NOT NULL
  OR NOT EXISTS (
      SELECT 1 FROM coordination_inbox i
      WHERE i.receipt_id = OLD.receipt_id
        AND i.root_thread_id = OLD.root_thread_id
        AND i.recipient_turn_id = OLD.target_turn_id
        AND i.delivery_fingerprint = OLD.delivery_fingerprint
        AND i.lifecycle = 'selected'
        AND i.version = OLD.inbox_version
        AND i.lease_epoch = OLD.lease_epoch
        AND i.lease_claim_operation_id = OLD.claim_operation_id
        AND i.lease_expires_at_ms = OLD.lease_expires_at_ms
        AND i.lease_expires_at_ms > NEW.transport_completed_at_ms
        AND i.expires_at_ms > NEW.transport_completed_at_ms
  )
BEGIN SELECT RAISE(ABORT, 'invalid coordination inclusion transition'); END;

CREATE TRIGGER coordination_inclusion_no_delete
BEFORE DELETE ON coordination_inbox_inclusions
BEGIN SELECT RAISE(ABORT, 'coordination inclusion cannot be deleted'); END;

CREATE TRIGGER coordination_inbox_active_authority_insert
BEFORE INSERT ON coordination_inbox
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_authority WHERE singleton_id=1 AND status='active'
)
BEGIN SELECT RAISE(ABORT, 'quarantined coordination authority is read-only'); END;

CREATE TRIGGER coordination_inbox_active_authority_update
BEFORE UPDATE ON coordination_inbox
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_authority WHERE singleton_id=1 AND status='active'
)
BEGIN SELECT RAISE(ABORT, 'quarantined coordination authority is read-only'); END;

CREATE TRIGGER coordination_inclusion_active_authority_insert
BEFORE INSERT ON coordination_inbox_inclusions
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_authority WHERE singleton_id=1 AND status='active'
)
BEGIN SELECT RAISE(ABORT, 'quarantined coordination authority is read-only'); END;

CREATE TRIGGER coordination_inclusion_active_authority_update
BEFORE UPDATE ON coordination_inbox_inclusions
WHEN NOT EXISTS (
    SELECT 1 FROM coordination_authority WHERE singleton_id=1 AND status='active'
)
BEGIN SELECT RAISE(ABORT, 'quarantined coordination authority is read-only'); END;
