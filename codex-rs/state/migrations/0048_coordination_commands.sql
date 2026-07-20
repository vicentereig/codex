CREATE TABLE coordination_commands (
    operation_id TEXT PRIMARY KEY NOT NULL CHECK (length(operation_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    intent_event_id TEXT NOT NULL UNIQUE REFERENCES coordination_events(event_id),
    sender_thread_id TEXT NOT NULL CHECK (length(sender_thread_id) = 36),
    sender_turn_id TEXT NOT NULL CHECK (length(sender_turn_id) BETWEEN 1 AND 128),
    operation_kind TEXT NOT NULL CHECK (operation_kind IN (
        'assignmentSpawn','assignmentFollowup','message','interrupt'
    )),
    target_thread_id TEXT NOT NULL CHECK (length(target_thread_id) = 36),
    target_assignment_id TEXT NOT NULL CHECK (length(target_assignment_id) = 36),
    target_generation INTEGER NOT NULL CHECK (
        typeof(target_generation) = 'integer' AND target_generation BETWEEN 1 AND 2147483647
    ),
    target_turn_id TEXT CHECK (
        target_turn_id IS NULL OR length(target_turn_id) BETWEEN 1 AND 128
    ),
    captured_head_generation INTEGER CHECK (
        captured_head_generation IS NULL
        OR (typeof(captured_head_generation) = 'integer'
            AND captured_head_generation BETWEEN 1 AND 2147483647)
    ),
    captured_turn_set_bytes BLOB CHECK (
        captured_turn_set_bytes IS NULL
        OR (
            typeof(captured_turn_set_bytes) = 'blob'
            AND hex(substr(captured_turn_set_bytes, 1, 1)) IN ('01','02','03','04')
            AND length(captured_turn_set_bytes) = CASE hex(substr(captured_turn_set_bytes, 1, 1))
                WHEN '01' THEN 5 WHEN '02' THEN 9 WHEN '03' THEN 13 WHEN '04' THEN 17 END
        )
    ),
    captured_turn_set_fingerprint BLOB CHECK (
        captured_turn_set_fingerprint IS NULL
        OR (
            typeof(captured_turn_set_fingerprint) = 'blob'
            AND length(captured_turn_set_fingerprint) = 32
        )
    ),
    idempotency_tuple_bytes BLOB NOT NULL CHECK (
        typeof(idempotency_tuple_bytes) = 'blob'
        AND length(idempotency_tuple_bytes) BETWEEN 1 AND 1024
    ),
    idempotency_tuple_fingerprint BLOB NOT NULL CHECK (
        typeof(idempotency_tuple_fingerprint) = 'blob'
        AND length(idempotency_tuple_fingerprint) = 32
    ),
    command_fingerprint BLOB NOT NULL CHECK (
        typeof(command_fingerprint) = 'blob' AND length(command_fingerprint) = 32
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
        lifecycle IN ('pending','leased','succeeded','poisoned','expired')
    ),
    version INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(version) = 'integer' AND version BETWEEN 0 AND 9223372036854775807
    ),
    claim_count INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(claim_count) = 'integer' AND claim_count BETWEEN 0 AND 9223372036854775807
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(attempt_count) = 'integer' AND attempt_count BETWEEN 0 AND 9223372036854775807
    ),
    attempted_lease_epoch INTEGER CHECK (
        attempted_lease_epoch IS NULL OR (typeof(attempted_lease_epoch) = 'integer'
            AND attempted_lease_epoch BETWEEN 1 AND lease_epoch)
    ),
    lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(lease_epoch) = 'integer' AND lease_epoch BETWEEN 0 AND 9223372036854775807
    ),
    retry_after_ms INTEGER NOT NULL CHECK (
        typeof(retry_after_ms) = 'integer' AND retry_after_ms BETWEEN 0 AND 9223372036854775807
    ),
    lease_expires_at_ms INTEGER CHECK (
        lease_expires_at_ms IS NULL OR (typeof(lease_expires_at_ms) = 'integer'
            AND lease_expires_at_ms BETWEEN 0 AND 9223372036854775807)
    ),
    failure_code TEXT CHECK (failure_code IS NULL OR failure_code IN (
        'unauthorized','stateUnavailable','stateQuarantined','invalidPayload',
        'payloadOverLimit','targetUnavailable','generationFenced','terminalConflict',
        'ownershipConflict','idempotencyConflict','retryExhausted','corruptEvidence','internal'
    )),
    intent_at_ms INTEGER NOT NULL CHECK (
        typeof(intent_at_ms) = 'integer' AND intent_at_ms BETWEEN 0 AND 9223372036854775807
    ),
    terminal_at_ms INTEGER CHECK (
        terminal_at_ms IS NULL OR (typeof(terminal_at_ms) = 'integer'
            AND terminal_at_ms BETWEEN intent_at_ms AND 9223372036854775807)
    ),
    expires_at_ms INTEGER NOT NULL CHECK (
        typeof(expires_at_ms) = 'integer'
        AND expires_at_ms BETWEEN intent_at_ms AND MIN(9223372036854775807, intent_at_ms + 604800000)
    ),
    purged_at_ms INTEGER CHECK (
        purged_at_ms IS NULL OR (typeof(purged_at_ms) = 'integer'
            AND purged_at_ms BETWEEN intent_at_ms AND 9223372036854775807)
    ),
    updated_at_ms INTEGER NOT NULL CHECK (
        typeof(updated_at_ms) = 'integer'
        AND updated_at_ms BETWEEN intent_at_ms AND 9223372036854775807
    ),
    UNIQUE (root_thread_id, idempotency_tuple_fingerprint),
    FOREIGN KEY (target_assignment_id, target_generation)
        REFERENCES coordination_assignment_generations(assignment_id, generation),
    CHECK ((ciphertext IS NULL) = (purged_at_ms IS NOT NULL)),
    CHECK (lifecycle NOT IN ('pending','leased') OR ciphertext IS NOT NULL),
    CHECK (
        (lifecycle = 'leased' AND lease_expires_at_ms IS NOT NULL
            AND lease_expires_at_ms <= expires_at_ms)
        OR (lifecycle != 'leased' AND lease_expires_at_ms IS NULL)
    ),
    CHECK ((lifecycle IN ('succeeded','poisoned')) = (terminal_at_ms IS NOT NULL)),
    CHECK (lifecycle != 'poisoned' OR failure_code IS NOT NULL),
    CHECK (lifecycle != 'expired' OR ciphertext IS NULL),
    CHECK (
        (operation_kind IN ('assignmentSpawn','assignmentFollowup')
            AND captured_head_generation IS NULL
            AND captured_turn_set_bytes IS NULL
            AND captured_turn_set_fingerprint IS NULL)
        OR (operation_kind = 'message'
            AND target_turn_id IS NOT NULL
            AND captured_head_generation = target_generation
            AND captured_turn_set_bytes IS NULL
            AND captured_turn_set_fingerprint IS NULL)
        OR (operation_kind = 'interrupt'
            AND target_turn_id IS NOT NULL
            AND captured_head_generation = target_generation
            AND captured_turn_set_bytes IS NOT NULL
            AND captured_turn_set_fingerprint IS NOT NULL)
    ),
    CHECK (
        operation_kind IN ('assignmentSpawn','assignmentFollowup')
        OR target_turn_id IS NOT NULL
    )
);

CREATE INDEX idx_coordination_commands_claimable
    ON coordination_commands(lifecycle, retry_after_ms, expires_at_ms, operation_id);
CREATE INDEX idx_coordination_commands_expirable
    ON coordination_commands(expires_at_ms, operation_id) WHERE ciphertext IS NOT NULL;

CREATE TRIGGER coordination_command_event_guard
BEFORE INSERT ON coordination_commands
WHEN NOT EXISTS (
    SELECT 1
    FROM coordination_events e
    WHERE e.event_id = NEW.intent_event_id
      AND e.root_thread_id = NEW.root_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.operationId') = NEW.operation_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.actor.threadId') = NEW.sender_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.actor.turnId.status') = 'known'
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.actor.turnId.value') = NEW.sender_turn_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.principal.threadId') = NEW.target_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.status') = 'known'
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId') = NEW.target_assignment_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.target.assignment.generation') = NEW.target_generation
      AND (
          (NEW.operation_kind = 'assignmentSpawn'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'assignmentRequested'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.mode') = 'spawn'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.encodedPayloadBytes') = NEW.encoded_payload_bytes)
          OR (NEW.operation_kind = 'assignmentFollowup'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'assignmentRequested'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.mode') = 'followup'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.encodedPayloadBytes') = NEW.encoded_payload_bytes)
          OR (NEW.operation_kind = 'message'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'messageSubmissionRecorded'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.encodedPayloadBytes') = NEW.encoded_payload_bytes)
          OR (NEW.operation_kind = 'interrupt'
              AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') = 'interruptRequested'
              AND NEW.encoded_payload_bytes = 0)
      )
)
BEGIN SELECT RAISE(ABORT, 'coordination command intent event is incoherent'); END;

CREATE TRIGGER coordination_command_generation_guard
BEFORE INSERT ON coordination_commands
WHEN NOT EXISTS (
    SELECT 1
    FROM coordination_assignment_heads h
    JOIN coordination_assignment_generations g USING (assignment_id)
    WHERE h.assignment_id = NEW.target_assignment_id
      AND h.root_thread_id = NEW.root_thread_id
      AND h.child_thread_id = NEW.target_thread_id
      AND g.generation = NEW.target_generation
      AND g.request_event_id = CASE
          WHEN NEW.operation_kind IN ('assignmentSpawn','assignmentFollowup')
          THEN NEW.intent_event_id ELSE g.request_event_id END
      AND (
          NEW.operation_kind IN ('assignmentSpawn','assignmentFollowup')
          OR (g.accepted_event_id IS NOT NULL AND EXISTS (
              SELECT 1 FROM coordination_turn_bindings b
              WHERE b.assignment_id = g.assignment_id
                AND b.generation = g.generation
                AND b.root_thread_id = NEW.root_thread_id
                AND b.turn_id = NEW.target_turn_id
          ))
      )
)
BEGIN SELECT RAISE(ABORT, 'coordination command generation fence is invalid'); END;

CREATE TRIGGER coordination_command_identity_immutable
BEFORE UPDATE ON coordination_commands
WHEN NEW.operation_id != OLD.operation_id
  OR NEW.root_thread_id != OLD.root_thread_id
  OR NEW.intent_event_id != OLD.intent_event_id
  OR NEW.sender_thread_id != OLD.sender_thread_id
  OR NEW.sender_turn_id != OLD.sender_turn_id
  OR NEW.operation_kind != OLD.operation_kind
  OR NEW.target_thread_id != OLD.target_thread_id
  OR NEW.target_assignment_id != OLD.target_assignment_id
  OR NEW.target_generation != OLD.target_generation
  OR NEW.target_turn_id IS NOT OLD.target_turn_id
  OR NEW.captured_head_generation IS NOT OLD.captured_head_generation
  OR NEW.captured_turn_set_bytes IS NOT OLD.captured_turn_set_bytes
  OR NEW.captured_turn_set_fingerprint IS NOT OLD.captured_turn_set_fingerprint
  OR NEW.idempotency_tuple_bytes != OLD.idempotency_tuple_bytes
  OR NEW.idempotency_tuple_fingerprint != OLD.idempotency_tuple_fingerprint
  OR NEW.command_fingerprint != OLD.command_fingerprint
  OR NEW.encoded_payload_bytes != OLD.encoded_payload_bytes
  OR NEW.ciphertext_fingerprint != OLD.ciphertext_fingerprint
  OR NEW.intent_at_ms != OLD.intent_at_ms
BEGIN SELECT RAISE(ABORT, 'coordination command identity is immutable'); END;

CREATE TRIGGER coordination_command_transition_guard
BEFORE UPDATE ON coordination_commands
WHEN NEW.version != OLD.version + 1
  OR NEW.claim_count < OLD.claim_count
  OR NEW.attempt_count < OLD.attempt_count
  OR NEW.lease_epoch < OLD.lease_epoch
  OR NEW.retry_after_ms < 0
  OR NEW.expires_at_ms > OLD.expires_at_ms
  OR NEW.updated_at_ms < OLD.updated_at_ms
  OR (OLD.ciphertext IS NULL AND NEW.ciphertext IS NOT NULL)
  OR (OLD.purged_at_ms IS NOT NULL AND NEW.purged_at_ms IS NOT OLD.purged_at_ms)
  OR (OLD.terminal_at_ms IS NOT NULL AND NEW.terminal_at_ms IS NOT OLD.terminal_at_ms)
  OR (OLD.lifecycle = 'pending' AND NEW.lifecycle NOT IN ('leased','expired'))
  OR (OLD.lifecycle = 'leased' AND NEW.lifecycle NOT IN ('leased','pending','succeeded','poisoned','expired'))
  OR (OLD.lifecycle IN ('succeeded','poisoned') AND NEW.lifecycle != OLD.lifecycle)
  OR (OLD.lifecycle = 'expired' AND NEW.lifecycle != 'expired')
  OR COALESCE(NOT (
      (OLD.lifecycle = 'pending' AND NEW.lifecycle = 'leased'
          AND NEW.claim_count = OLD.claim_count + 1
          AND NEW.lease_epoch = OLD.lease_epoch + 1
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.attempted_lease_epoch IS OLD.attempted_lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND OLD.retry_after_ms <= NEW.updated_at_ms
          AND NEW.updated_at_ms < OLD.expires_at_ms
          AND NEW.lease_expires_at_ms > NEW.updated_at_ms
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms)
      OR (OLD.lifecycle = 'leased' AND NEW.lifecycle = 'leased'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.attempt_count = OLD.attempt_count + 1
          AND NEW.attempted_lease_epoch = NEW.lease_epoch
          AND (OLD.attempted_lease_epoch IS NULL OR OLD.attempted_lease_epoch < OLD.lease_epoch)
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.lease_expires_at_ms IS OLD.lease_expires_at_ms
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND NEW.updated_at_ms < OLD.expires_at_ms
          AND OLD.lease_expires_at_ms > NEW.updated_at_ms
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms)
      OR (OLD.lifecycle = 'leased' AND NEW.lifecycle = 'pending'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.attempted_lease_epoch IS OLD.attempted_lease_epoch
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms
          AND (
              (OLD.attempted_lease_epoch = OLD.lease_epoch
                  AND NEW.retry_after_ms > OLD.retry_after_ms
                  AND NEW.retry_after_ms > NEW.updated_at_ms
                  AND NEW.updated_at_ms < OLD.expires_at_ms
                  AND OLD.lease_expires_at_ms > NEW.updated_at_ms
                  AND NEW.failure_code IS NOT NULL)
              OR (NEW.retry_after_ms = OLD.retry_after_ms
                  AND NEW.failure_code IS OLD.failure_code
                  AND NEW.updated_at_ms < OLD.expires_at_ms
                  AND OLD.lease_expires_at_ms <= NEW.updated_at_ms)
          ))
      OR (OLD.lifecycle = 'leased' AND NEW.lifecycle IN ('succeeded','poisoned')
          AND NEW.claim_count = OLD.claim_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.attempted_lease_epoch IS OLD.attempted_lease_epoch
          AND OLD.attempted_lease_epoch = OLD.lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND ((NEW.lifecycle = 'succeeded' AND NEW.failure_code IS NULL)
              OR (NEW.lifecycle = 'poisoned' AND NEW.failure_code IS NOT NULL))
          AND OLD.terminal_at_ms IS NULL AND NEW.terminal_at_ms IS NOT NULL
          AND NEW.terminal_at_ms = NEW.updated_at_ms
          AND NEW.updated_at_ms < OLD.expires_at_ms
          AND OLD.lease_expires_at_ms > NEW.updated_at_ms
          AND NEW.expires_at_ms = MIN(
              OLD.expires_at_ms, NEW.terminal_at_ms + 86400000
          )
          AND NEW.ciphertext IS OLD.ciphertext
          AND NEW.purged_at_ms IS OLD.purged_at_ms)
      OR (OLD.lifecycle = 'leased' AND NEW.lifecycle = 'expired'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.attempted_lease_epoch IS OLD.attempted_lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND OLD.expires_at_ms <= NEW.updated_at_ms
          AND NEW.ciphertext IS NULL AND NEW.purged_at_ms IS NOT NULL)
      OR (OLD.lifecycle = 'pending' AND NEW.lifecycle = 'expired'
          AND NEW.claim_count = OLD.claim_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.attempted_lease_epoch IS OLD.attempted_lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND OLD.expires_at_ms <= NEW.updated_at_ms
          AND NEW.ciphertext IS NULL AND NEW.purged_at_ms IS NOT NULL)
      OR (OLD.lifecycle IN ('succeeded','poisoned') AND NEW.lifecycle = OLD.lifecycle
          AND NEW.claim_count = OLD.claim_count
          AND NEW.lease_epoch = OLD.lease_epoch
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.attempted_lease_epoch IS OLD.attempted_lease_epoch
          AND NEW.retry_after_ms = OLD.retry_after_ms
          AND NEW.failure_code IS OLD.failure_code
          AND NEW.terminal_at_ms IS OLD.terminal_at_ms
          AND NEW.expires_at_ms = OLD.expires_at_ms
          AND OLD.expires_at_ms <= NEW.updated_at_ms
          AND OLD.ciphertext IS NOT NULL AND NEW.ciphertext IS NULL
          AND OLD.purged_at_ms IS NULL AND NEW.purged_at_ms IS NOT NULL)
  ), 1)
  OR (NEW.claim_count > OLD.claim_count AND (
      OLD.lifecycle != 'pending' OR NEW.lifecycle != 'leased'
      OR NEW.claim_count != OLD.claim_count + 1
      OR NEW.lease_epoch != OLD.lease_epoch + 1
      OR NEW.attempt_count != OLD.attempt_count
  ))
  OR (NEW.attempt_count > OLD.attempt_count AND (
      OLD.lifecycle != 'leased' OR NEW.lifecycle != 'leased'
      OR NEW.attempt_count != OLD.attempt_count + 1
      OR NEW.claim_count != OLD.claim_count
      OR NEW.lease_epoch != OLD.lease_epoch
  ))
  OR (NEW.lifecycle = 'pending' AND NEW.claim_count != OLD.claim_count)
BEGIN SELECT RAISE(ABORT, 'coordination command transition is invalid'); END;

CREATE TRIGGER coordination_command_no_delete
BEFORE DELETE ON coordination_commands
BEGIN SELECT RAISE(ABORT, 'coordination commands cannot be deleted'); END;
