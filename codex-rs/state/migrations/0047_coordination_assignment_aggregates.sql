CREATE TABLE coordination_assignment_heads (
    assignment_id TEXT PRIMARY KEY NOT NULL CHECK (length(assignment_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    child_thread_id TEXT NOT NULL CHECK (length(child_thread_id) = 36),
    accepted_generation INTEGER CHECK (accepted_generation BETWEEN 1 AND 2147483647),
    next_generation INTEGER NOT NULL CHECK (next_generation BETWEEN 2 AND 2147483647),
    owner_thread_id TEXT NOT NULL CHECK (length(owner_thread_id) = 36),
    owner_turn_id TEXT NOT NULL CHECK (length(owner_turn_id) BETWEEN 1 AND 128),
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    last_revision INTEGER NOT NULL CHECK (last_revision >= 1),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    CHECK (accepted_generation IS NULL OR accepted_generation < next_generation),
    UNIQUE (root_thread_id, assignment_id)
);

CREATE TRIGGER coordination_head_monotonic
BEFORE UPDATE ON coordination_assignment_heads WHEN
    NEW.assignment_id != OLD.assignment_id OR NEW.root_thread_id != OLD.root_thread_id OR
    NEW.child_thread_id != OLD.child_thread_id OR NEW.owner_thread_id != OLD.owner_thread_id OR
    NEW.owner_turn_id != OLD.owner_turn_id OR NEW.created_at_ms != OLD.created_at_ms OR
    NEW.next_generation < OLD.next_generation OR NEW.version <= OLD.version OR
    NEW.last_revision < OLD.last_revision OR NEW.updated_at_ms < OLD.updated_at_ms OR
    (OLD.accepted_generation IS NOT NULL AND NEW.accepted_generation IS NOT NULL
     AND NEW.accepted_generation < OLD.accepted_generation) OR
    (NEW.accepted_generation IS NULL AND OLD.accepted_generation IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM coordination_assignment_generations g
        WHERE g.assignment_id=OLD.assignment_id AND g.generation=OLD.accepted_generation AND g.lifecycle IN ('terminal','superseded')
    )) OR
    (NEW.accepted_generation IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM coordination_assignment_generations g
        WHERE g.assignment_id=OLD.assignment_id AND g.generation=NEW.accepted_generation AND g.lifecycle='accepted'
    ))
BEGIN SELECT RAISE(ABORT, 'coordination assignment head is not monotonic'); END;
CREATE TRIGGER coordination_head_no_delete BEFORE DELETE ON coordination_assignment_heads
BEGIN SELECT RAISE(ABORT, 'coordination assignment heads cannot be deleted'); END;

CREATE TABLE coordination_assignment_generations (
    assignment_id TEXT NOT NULL REFERENCES coordination_assignment_heads(assignment_id),
    generation INTEGER NOT NULL CHECK (generation BETWEEN 1 AND 2147483647),
    operation_id TEXT NOT NULL CHECK (length(operation_id) = 36),
    mode TEXT NOT NULL CHECK (mode IN ('spawn', 'followup')),
    lifecycle TEXT NOT NULL CHECK (lifecycle IN ('reserved','accepted','abandoned','superseded','terminal')),
    request_event_id TEXT NOT NULL UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    accepted_event_id TEXT UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    superseded_event_id TEXT UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    terminal_event_id TEXT REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    close_event_id TEXT UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    accepted_receipt_id TEXT CHECK (accepted_receipt_id IS NULL OR length(accepted_receipt_id) = 36),
    terminal_kind TEXT CHECK (terminal_kind IS NULL OR terminal_kind IN ('completed','interrupted')),
    terminal_reason_json TEXT CHECK (terminal_reason_json IS NULL OR json_valid(terminal_reason_json)),
    close_reason_json TEXT CHECK (close_reason_json IS NULL OR json_valid(close_reason_json)),
    created_revision INTEGER NOT NULL CHECK (created_revision >= 1),
    last_revision INTEGER NOT NULL CHECK (last_revision >= created_revision),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    PRIMARY KEY (assignment_id, generation),
    CHECK ((generation = 1 AND mode = 'spawn') OR (generation > 1 AND mode = 'followup')),
    CHECK ((lifecycle IN ('accepted','superseded','terminal')) = (accepted_event_id IS NOT NULL)),
    CHECK ((lifecycle IN ('accepted','superseded','terminal')) = (accepted_receipt_id IS NOT NULL)),
    CHECK ((lifecycle = 'superseded') = (superseded_event_id IS NOT NULL)),
    CHECK ((lifecycle IN ('abandoned','terminal')) = (close_event_id IS NOT NULL)),
    CHECK ((lifecycle IN ('abandoned','superseded','terminal')) = (close_reason_json IS NOT NULL)),
    CHECK ((lifecycle = 'terminal') = (terminal_event_id IS NOT NULL)),
    CHECK ((lifecycle = 'terminal') = (terminal_kind IS NOT NULL)),
    CHECK ((lifecycle = 'terminal') = (terminal_reason_json IS NOT NULL)),
    CHECK (lifecycle != 'terminal' OR json(close_reason_json) = json(terminal_reason_json)),
    CHECK (lifecycle != 'superseded' OR json_extract(close_reason_json,'$.reason') = 'superseded'),
    CHECK (lifecycle != 'abandoned' OR json_extract(close_reason_json,'$.reason') IN ('deliveryFailed','abandonedBeforeAcceptance'))
);

CREATE TRIGGER coordination_generation_monotonic BEFORE UPDATE ON coordination_assignment_generations WHEN
    NEW.assignment_id != OLD.assignment_id OR NEW.generation != OLD.generation OR NEW.mode != OLD.mode OR
    NEW.operation_id != OLD.operation_id OR NEW.request_event_id != OLD.request_event_id OR NEW.created_revision != OLD.created_revision OR
    NEW.created_at_ms != OLD.created_at_ms OR NEW.last_revision < OLD.last_revision OR
    NEW.updated_at_ms < OLD.updated_at_ms OR
    (OLD.accepted_event_id IS NOT NULL AND NEW.accepted_event_id IS NOT OLD.accepted_event_id) OR
    (OLD.accepted_receipt_id IS NOT NULL AND NEW.accepted_receipt_id IS NOT OLD.accepted_receipt_id) OR
    (OLD.superseded_event_id IS NOT NULL AND NEW.superseded_event_id IS NOT OLD.superseded_event_id) OR
    (OLD.terminal_event_id IS NOT NULL AND NEW.terminal_event_id IS NOT OLD.terminal_event_id) OR
    (OLD.terminal_kind IS NOT NULL AND NEW.terminal_kind IS NOT OLD.terminal_kind) OR
    (OLD.terminal_reason_json IS NOT NULL AND NEW.terminal_reason_json IS NOT OLD.terminal_reason_json) OR
    (OLD.close_reason_json IS NOT NULL AND NEW.close_reason_json IS NOT OLD.close_reason_json) OR
    (OLD.close_event_id IS NOT NULL AND NEW.close_event_id IS NOT OLD.close_event_id) OR
    (OLD.lifecycle = 'reserved' AND NEW.lifecycle NOT IN ('accepted','abandoned')) OR
    (OLD.lifecycle = 'accepted' AND NEW.lifecycle NOT IN ('superseded','terminal')) OR
    (OLD.lifecycle IN ('abandoned','superseded','terminal') AND NEW.lifecycle != OLD.lifecycle)
BEGIN SELECT RAISE(ABORT, 'coordination generation is not monotonic'); END;
CREATE TRIGGER coordination_generation_no_delete BEFORE DELETE ON coordination_assignment_generations
BEGIN SELECT RAISE(ABORT, 'coordination generations cannot be deleted'); END;
CREATE TRIGGER coordination_generation_new_events_only BEFORE INSERT ON coordination_assignment_generations WHEN
    EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.request_event_id)
BEGIN SELECT RAISE(ABORT, 'assignment request event must be linked before insertion'); END;
CREATE TRIGGER coordination_generation_new_transition_events_only BEFORE UPDATE ON coordination_assignment_generations WHEN
    (OLD.accepted_event_id IS NULL AND NEW.accepted_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.accepted_event_id)) OR
    (OLD.superseded_event_id IS NULL AND NEW.superseded_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.superseded_event_id)) OR
    (OLD.terminal_event_id IS NULL AND NEW.terminal_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.terminal_event_id)) OR
    (OLD.close_event_id IS NULL AND NEW.close_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.close_event_id))
BEGIN SELECT RAISE(ABORT, 'assignment transition event must be linked before insertion'); END;

CREATE TABLE coordination_turn_bindings (
    assignment_id TEXT NOT NULL, generation INTEGER NOT NULL,
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    turn_id TEXT NOT NULL CHECK (length(turn_id) BETWEEN 1 AND 128),
    accepted_event_id TEXT NOT NULL REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    PRIMARY KEY (root_thread_id, turn_id, assignment_id, generation),
    UNIQUE (assignment_id, generation),
    FOREIGN KEY (assignment_id, generation) REFERENCES coordination_assignment_generations(assignment_id, generation)
);
CREATE INDEX idx_coordination_turn_bindings_turn ON coordination_turn_bindings(root_thread_id, turn_id, assignment_id, generation);
CREATE TRIGGER coordination_turn_binding_root_guard BEFORE INSERT ON coordination_turn_bindings WHEN NOT EXISTS (
    SELECT 1 FROM coordination_assignment_heads h WHERE h.assignment_id=NEW.assignment_id AND h.root_thread_id=NEW.root_thread_id
) BEGIN SELECT RAISE(ABORT, 'turn binding root does not match assignment'); END;
CREATE TRIGGER coordination_turn_binding_event_guard BEFORE INSERT ON coordination_turn_bindings WHEN
    EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.accepted_event_id) OR NOT EXISTS (
        SELECT 1 FROM coordination_assignment_generations g WHERE g.assignment_id=NEW.assignment_id
          AND g.generation=NEW.generation AND g.accepted_event_id=NEW.accepted_event_id
    )
BEGIN SELECT RAISE(ABORT, 'turn binding acceptance event is invalid'); END;
CREATE TRIGGER coordination_turn_binding_no_update BEFORE UPDATE ON coordination_turn_bindings
BEGIN SELECT RAISE(ABORT, 'coordination turn bindings are immutable'); END;
CREATE TRIGGER coordination_turn_binding_no_delete BEFORE DELETE ON coordination_turn_bindings
BEGIN SELECT RAISE(ABORT, 'coordination turn bindings cannot be deleted'); END;

CREATE TABLE coordination_waits (
    operation_id TEXT PRIMARY KEY NOT NULL CHECK (length(operation_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    actor_thread_id TEXT NOT NULL CHECK (length(actor_thread_id) = 36),
    actor_turn_id TEXT NOT NULL CHECK (length(actor_turn_id) BETWEEN 1 AND 128),
    start_event_id TEXT NOT NULL UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    end_event_id TEXT UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    outcome_json TEXT CHECK (outcome_json IS NULL OR json_valid(outcome_json)),
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    last_revision INTEGER NOT NULL CHECK (last_revision >= 1),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    CHECK ((end_event_id IS NULL) = (outcome_json IS NULL))
);
CREATE TRIGGER coordination_wait_first_wins BEFORE UPDATE ON coordination_waits WHEN
    NEW.operation_id != OLD.operation_id OR NEW.root_thread_id != OLD.root_thread_id OR
    NEW.actor_thread_id != OLD.actor_thread_id OR NEW.actor_turn_id != OLD.actor_turn_id OR
    NEW.start_event_id != OLD.start_event_id OR NEW.created_at_ms != OLD.created_at_ms OR
    OLD.end_event_id IS NOT NULL OR NEW.end_event_id IS NULL OR NEW.version != OLD.version + 1 OR
    NEW.last_revision < OLD.last_revision OR NEW.updated_at_ms < OLD.updated_at_ms
BEGIN SELECT RAISE(ABORT, 'coordination wait is immutable or already ended'); END;
CREATE TRIGGER coordination_wait_no_delete BEFORE DELETE ON coordination_waits
BEGIN SELECT RAISE(ABORT, 'coordination waits cannot be deleted'); END;
CREATE TRIGGER coordination_wait_new_start_event_only BEFORE INSERT ON coordination_waits WHEN
    EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.start_event_id)
BEGIN SELECT RAISE(ABORT, 'wait start event must be linked before insertion'); END;
CREATE TRIGGER coordination_wait_new_end_event_only BEFORE UPDATE ON coordination_waits WHEN
    OLD.end_event_id IS NULL AND NEW.end_event_id IS NOT NULL AND EXISTS (
        SELECT 1 FROM coordination_events WHERE event_id=NEW.end_event_id
    )
BEGIN SELECT RAISE(ABORT, 'wait end event must be linked before insertion'); END;

CREATE TABLE coordination_turn_terminals (
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    target_thread_id TEXT NOT NULL CHECK (length(target_thread_id) = 36),
    target_turn_id TEXT NOT NULL CHECK (length(target_turn_id) BETWEEN 1 AND 128),
    terminal_kind TEXT NOT NULL CHECK (terminal_kind IN ('completed','interrupted')),
    terminal_event_id TEXT NOT NULL UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    included_generations_json TEXT NOT NULL CHECK (json_valid(included_generations_json)),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    PRIMARY KEY (root_thread_id, target_thread_id, target_turn_id)
);
CREATE TABLE coordination_turn_terminal_generations (
    root_thread_id TEXT NOT NULL,
    target_thread_id TEXT NOT NULL,
    target_turn_id TEXT NOT NULL,
    assignment_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation BETWEEN 1 AND 2147483647),
    close_event_id TEXT UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (root_thread_id,target_thread_id,target_turn_id,assignment_id,generation),
    FOREIGN KEY (root_thread_id,target_thread_id,target_turn_id)
        REFERENCES coordination_turn_terminals(root_thread_id,target_thread_id,target_turn_id),
    FOREIGN KEY (assignment_id,generation)
        REFERENCES coordination_assignment_generations(assignment_id,generation)
);
CREATE TRIGGER coordination_turn_terminal_no_update BEFORE UPDATE ON coordination_turn_terminals BEGIN SELECT RAISE(ABORT, 'coordination turn terminal is immutable'); END;
CREATE TRIGGER coordination_turn_terminal_no_delete BEFORE DELETE ON coordination_turn_terminals BEGIN SELECT RAISE(ABORT, 'coordination turn terminal cannot be deleted'); END;
CREATE TRIGGER coordination_turn_terminal_generation_no_update BEFORE UPDATE ON coordination_turn_terminal_generations BEGIN SELECT RAISE(ABORT, 'coordination terminal inclusion is immutable'); END;
CREATE TRIGGER coordination_turn_terminal_generation_no_delete BEFORE DELETE ON coordination_turn_terminal_generations BEGIN SELECT RAISE(ABORT, 'coordination terminal inclusion cannot be deleted'); END;
CREATE TRIGGER coordination_turn_terminal_generation_root_guard BEFORE INSERT ON coordination_turn_terminal_generations WHEN NOT EXISTS (
    SELECT 1 FROM coordination_assignment_heads h WHERE h.assignment_id=NEW.assignment_id AND h.root_thread_id=NEW.root_thread_id
) BEGIN SELECT RAISE(ABORT, 'terminal inclusion root does not match assignment'); END;
CREATE TRIGGER coordination_turn_terminal_generation_binding_guard BEFORE INSERT ON coordination_turn_terminal_generations WHEN NOT EXISTS (
    SELECT 1 FROM coordination_assignment_generations g
    JOIN coordination_turn_bindings b USING (assignment_id,generation)
    WHERE g.assignment_id=NEW.assignment_id AND g.generation=NEW.generation
      AND g.lifecycle IN ('accepted','superseded','terminal')
      AND b.root_thread_id=NEW.root_thread_id AND b.turn_id=NEW.target_turn_id
)
BEGIN SELECT RAISE(ABORT, 'terminal inclusion must name an accepted bound generation'); END;
CREATE TRIGGER coordination_turn_terminal_new_event_only BEFORE INSERT ON coordination_turn_terminals WHEN
    EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.terminal_event_id)
BEGIN SELECT RAISE(ABORT, 'turn terminal event must be linked before insertion'); END;
CREATE TRIGGER coordination_turn_terminal_generation_new_event_only BEFORE INSERT ON coordination_turn_terminal_generations WHEN
    NEW.close_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.close_event_id)
BEGIN SELECT RAISE(ABORT, 'terminal close event must be linked before insertion'); END;

-- Frozen-schema storage shells. They deliberately have no runtime producer in this milestone.
CREATE TABLE coordination_dependencies (
    operation_id TEXT PRIMARY KEY NOT NULL CHECK (length(operation_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    event_id TEXT NOT NULL UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0)
);
CREATE TABLE coordination_results (
    result_id TEXT PRIMARY KEY NOT NULL CHECK (length(result_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    terminal_event_id TEXT NOT NULL REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    observed_event_id TEXT UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0)
);
CREATE TABLE coordination_handoffs (
    handoff_id TEXT NOT NULL CHECK (length(handoff_id) = 36),
    attempt INTEGER NOT NULL CHECK (attempt BETWEEN 1 AND 2147483647),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id),
    result_id TEXT NOT NULL REFERENCES coordination_results(result_id),
    attempted_event_id TEXT NOT NULL UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    received_event_id TEXT UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    failed_event_id TEXT UNIQUE REFERENCES coordination_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    PRIMARY KEY (handoff_id, attempt),
    CHECK (received_event_id IS NULL OR failed_event_id IS NULL)
);

CREATE TRIGGER coordination_dependency_no_mutation BEFORE UPDATE ON coordination_dependencies BEGIN SELECT RAISE(ABORT, 'coordination dependencies are immutable'); END;
CREATE TRIGGER coordination_dependency_no_delete BEFORE DELETE ON coordination_dependencies BEGIN SELECT RAISE(ABORT, 'coordination dependencies cannot be deleted'); END;
CREATE TRIGGER coordination_result_no_delete BEFORE DELETE ON coordination_results BEGIN SELECT RAISE(ABORT, 'coordination results cannot be deleted'); END;
CREATE TRIGGER coordination_handoff_no_delete BEFORE DELETE ON coordination_handoffs BEGIN SELECT RAISE(ABORT, 'coordination handoffs cannot be deleted'); END;
CREATE TRIGGER coordination_result_first_wins BEFORE UPDATE ON coordination_results WHEN
    NEW.result_id IS NOT OLD.result_id OR NEW.root_thread_id IS NOT OLD.root_thread_id OR NEW.terminal_event_id IS NOT OLD.terminal_event_id OR
    NEW.created_at_ms IS NOT OLD.created_at_ms OR OLD.observed_event_id IS NOT NULL OR NEW.observed_event_id IS NULL
BEGIN SELECT RAISE(ABORT, 'coordination result is immutable or already observed'); END;
CREATE TRIGGER coordination_handoff_first_wins BEFORE UPDATE ON coordination_handoffs WHEN
    NEW.handoff_id IS NOT OLD.handoff_id OR NEW.attempt IS NOT OLD.attempt OR NEW.root_thread_id IS NOT OLD.root_thread_id OR
    NEW.result_id IS NOT OLD.result_id OR NEW.attempted_event_id IS NOT OLD.attempted_event_id OR NEW.created_at_ms IS NOT OLD.created_at_ms OR
    OLD.received_event_id IS NOT NULL OR OLD.failed_event_id IS NOT NULL OR
    ((NEW.received_event_id IS NULL) = (NEW.failed_event_id IS NULL))
BEGIN SELECT RAISE(ABORT, 'coordination handoff is immutable or already resolved'); END;

CREATE TRIGGER coordination_dependency_new_event_only BEFORE INSERT ON coordination_dependencies WHEN
    EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.event_id)
BEGIN SELECT RAISE(ABORT, 'dependency event must be linked before insertion'); END;
CREATE TRIGGER coordination_result_terminal_guard BEFORE INSERT ON coordination_results WHEN NOT EXISTS (
    SELECT 1 FROM coordination_events e WHERE e.event_id=NEW.terminal_event_id AND e.root_thread_id=NEW.root_thread_id
      AND json_extract(CAST(e.canonical_event_bytes AS TEXT),'$.kind') IN ('turnCompleted','turnInterrupted')
)
BEGIN SELECT RAISE(ABORT, 'result terminal event is invalid'); END;
CREATE TRIGGER coordination_result_new_observation_only BEFORE INSERT ON coordination_results WHEN
    NEW.observed_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.observed_event_id)
BEGIN SELECT RAISE(ABORT, 'result observation event must be linked before insertion'); END;
CREATE TRIGGER coordination_result_new_observation_update_only BEFORE UPDATE ON coordination_results WHEN
    OLD.observed_event_id IS NULL AND NEW.observed_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.observed_event_id)
BEGIN SELECT RAISE(ABORT, 'result observation event must be linked before insertion'); END;
CREATE TRIGGER coordination_handoff_new_attempt_only BEFORE INSERT ON coordination_handoffs WHEN
    NOT EXISTS (SELECT 1 FROM coordination_results r WHERE r.result_id=NEW.result_id AND r.root_thread_id=NEW.root_thread_id AND r.observed_event_id IS NOT NULL) OR
    EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.attempted_event_id) OR
    (NEW.received_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.received_event_id)) OR
    (NEW.failed_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.failed_event_id))
BEGIN SELECT RAISE(ABORT, 'handoff event must be linked before insertion'); END;
CREATE TRIGGER coordination_handoff_new_resolution_only BEFORE UPDATE ON coordination_handoffs WHEN
    (OLD.received_event_id IS NULL AND NEW.received_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.received_event_id)) OR
    (OLD.failed_event_id IS NULL AND NEW.failed_event_id IS NOT NULL AND EXISTS (SELECT 1 FROM coordination_events WHERE event_id=NEW.failed_event_id))
BEGIN SELECT RAISE(ABORT, 'handoff resolution event must be linked before insertion'); END;

-- Event insertion is the deferred-FK synchronization point: linked aggregate rows
-- must agree with the immutable event's root and semantic kind before commit.
CREATE TRIGGER coordination_aggregate_event_coherence AFTER INSERT ON coordination_events
BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_assignment_generations g
        JOIN coordination_assignment_heads h USING (assignment_id)
        WHERE g.request_event_id=NEW.event_id
          AND (h.root_thread_id!=NEW.root_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'assignmentRequested'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.operationId') IS NOT g.operation_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.mode') IS NOT g.mode
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.principal.threadId') IS NOT h.child_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.status') IS NOT 'known'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId') IS NOT g.assignment_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.generation') IS NOT g.generation)
    ) THEN RAISE(ABORT, 'assignment request event is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_assignment_generations g
        JOIN coordination_assignment_heads h USING (assignment_id)
        WHERE g.accepted_event_id=NEW.event_id
          AND (h.root_thread_id!=NEW.root_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'assignmentAccepted'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.operationId') IS NOT g.operation_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.mode') IS NOT g.mode
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.principal.threadId') IS NOT h.child_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.status') IS NOT 'known'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId') IS NOT g.assignment_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.generation') IS NOT g.generation
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.receiptId') IS NOT g.accepted_receipt_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.omittedCount') IS NOT 0
            OR json_array_length(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items') IS NOT 1
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items[0]') IS NOT g.request_event_id)
    ) THEN RAISE(ABORT, 'assignment acceptance event is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_turn_bindings b JOIN coordination_assignment_heads h USING (assignment_id)
        WHERE b.accepted_event_id=NEW.event_id AND (b.root_thread_id!=h.root_thread_id OR b.root_thread_id!=NEW.root_thread_id
          OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.boundTurnId.status') IS NOT 'known'
          OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.boundTurnId.value') IS NOT b.turn_id)
    ) THEN RAISE(ABORT, 'turn binding root is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING (assignment_id)
        WHERE (g.superseded_event_id=NEW.event_id OR g.close_event_id=NEW.event_id)
          AND (h.root_thread_id!=NEW.root_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'assignmentGenerationClosed'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.assignment.status') IS NOT 'known'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.assignment.assignmentId') IS NOT g.assignment_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.assignment.generation') IS NOT g.generation
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.closeReason.reason') IS NOT json_extract(g.close_reason_json,'$.reason')
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.closeReason.byGeneration') IS NOT json_extract(g.close_reason_json,'$.byGeneration')
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.closeReason.turnId') IS NOT json_extract(g.close_reason_json,'$.turnId')
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.closeReason.code') IS NOT json_extract(g.close_reason_json,'$.code')
            OR (SELECT count(*) FROM json_each(CAST(NEW.canonical_event_bytes AS TEXT),'$.closeReason')) IS NOT
               (SELECT count(*) FROM json_each(g.close_reason_json))
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.omittedCount') IS NOT 0
            OR json_array_length(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items') IS NOT 1
            OR NOT EXISTS (SELECT 1 FROM coordination_events cause WHERE cause.event_id=json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items[0]'))
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items[0]') IS NOT CASE
                WHEN json_extract(g.close_reason_json,'$.reason') IN ('abandonedBeforeAcceptance','deliveryFailed') THEN g.request_event_id
                WHEN json_extract(g.close_reason_json,'$.reason') IN ('turnCompleted','turnInterrupted') THEN g.terminal_event_id
                WHEN json_extract(g.close_reason_json,'$.reason')='superseded' THEN coalesce(
                    (SELECT later.accepted_event_id FROM coordination_assignment_generations later
                     WHERE later.assignment_id=g.assignment_id AND later.generation=json_extract(g.close_reason_json,'$.byGeneration')),
                    (SELECT later.request_event_id FROM coordination_assignment_generations later
                     WHERE later.assignment_id=g.assignment_id AND later.generation=json_extract(g.close_reason_json,'$.byGeneration'))
                )
                ELSE NULL
            END
            OR (json_extract(g.close_reason_json,'$.reason') IN ('turnCompleted','turnInterrupted') AND EXISTS (
                SELECT 1 FROM coordination_events terminal WHERE terminal.event_id=g.terminal_event_id AND
                  (json_extract(g.close_reason_json,'$.turnId') IS NOT json_extract(CAST(terminal.canonical_event_bytes AS TEXT),'$.targetTurnId')
                    OR json_extract(CAST(terminal.canonical_event_bytes AS TEXT),'$.kind') IS NOT
                      CASE json_extract(g.close_reason_json,'$.reason') WHEN 'turnCompleted' THEN 'turnCompleted' ELSE 'turnInterrupted' END)
            )))
    ) THEN RAISE(ABORT, 'assignment close event is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_assignment_generations g JOIN coordination_assignment_heads h USING (assignment_id)
        WHERE g.terminal_event_id=NEW.event_id AND (h.root_thread_id!=NEW.root_thread_id OR
          coalesce(json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') NOT IN ('turnCompleted','turnInterrupted'),1) OR
          json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.principal.threadId') IS NOT h.child_thread_id)
    ) THEN RAISE(ABORT, 'assignment terminal event is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_turn_terminals t WHERE t.terminal_event_id=NEW.event_id AND
          (t.root_thread_id!=NEW.root_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT CASE t.terminal_kind WHEN 'completed' THEN 'turnCompleted' ELSE 'turnInterrupted' END
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.principal.threadId') IS NOT t.target_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.targetTurnId') IS NOT t.target_turn_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.includedGenerations.items') IS NOT json(t.included_generations_json)
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.omittedCount') IS NOT 0
            OR json_array_length(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items') IS NOT CASE
                WHEN json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind')='turnInterrupted'
                  AND json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.interruptionReason.reason')='requested'
                THEN 1 ELSE 0 END
            OR (json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind')='turnInterrupted'
                AND json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.interruptionReason.reason')='requested'
                AND NOT EXISTS (
                    SELECT 1 FROM coordination_inbox i
                    WHERE i.root_thread_id=t.root_thread_id
                      AND i.recipient_thread_id=t.target_thread_id
                      AND i.recipient_turn_id=t.target_turn_id
                      AND i.operation_kind='interrupt'
                      AND i.command_operation_id=json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.interruptionReason.operationId')
                      AND i.receipt_event_id=json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items[0]')
                      AND i.target_assignment_id=json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId')
                      AND i.target_generation=json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.generation')
                ))
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.includedGenerations.omittedCount') IS NOT 0
            OR NOT EXISTS (
                SELECT 1 FROM json_each(CAST(NEW.canonical_event_bytes AS TEXT),'$.includedGenerations.items') target_generation
                WHERE target_generation.value=json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.generation')
            )
            OR json_array_length(CAST(NEW.canonical_event_bytes AS TEXT),'$.includedGenerations.items') IS NOT (
                SELECT count(*) FROM coordination_turn_terminal_generations i
                WHERE i.root_thread_id=t.root_thread_id AND i.target_thread_id=t.target_thread_id AND i.target_turn_id=t.target_turn_id
            )
            OR EXISTS (
                SELECT 1 FROM coordination_turn_terminal_generations i
                WHERE i.root_thread_id=t.root_thread_id AND i.target_thread_id=t.target_thread_id AND i.target_turn_id=t.target_turn_id
                  AND (json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.status') IS NOT 'known'
                    OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target.assignment.assignmentId') IS NOT i.assignment_id
                    OR NOT EXISTS (SELECT 1 FROM json_each(CAST(NEW.canonical_event_bytes AS TEXT),'$.includedGenerations.items') j WHERE j.value=i.generation)
                    OR (i.close_event_id IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM coordination_assignment_generations g
                        WHERE g.assignment_id=i.assignment_id AND g.generation=i.generation
                          AND i.close_event_id IN (g.close_event_id,g.superseded_event_id)
                    )))
            ))
    ) THEN RAISE(ABORT, 'turn terminal event is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_waits w WHERE w.start_event_id=NEW.event_id
          AND (w.root_thread_id!=NEW.root_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'waitStarted'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.operationId') IS NOT w.operation_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.actor.threadId') IS NOT w.actor_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.actor.turnId.status') IS NOT 'known'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.actor.turnId.value') IS NOT w.actor_turn_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.omittedCount') IS NOT 0
            OR json_array_length(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items') IS NOT 0)
    ) THEN RAISE(ABORT, 'wait start event is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_waits w WHERE w.end_event_id=NEW.event_id
          AND (w.root_thread_id!=NEW.root_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'waitEnded'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.operationId') IS NOT w.operation_id
            OR EXISTS (
                SELECT substr(fullkey,length('$.outcome')+1),type,atom FROM json_tree(CAST(NEW.canonical_event_bytes AS TEXT),'$.outcome')
                EXCEPT SELECT substr(fullkey,length('$[0]')+1),type,atom FROM json_tree(w.outcome_json,'$[0]')
            )
            OR EXISTS (
                SELECT substr(fullkey,length('$[0]')+1),type,atom FROM json_tree(w.outcome_json,'$[0]')
                EXCEPT SELECT substr(fullkey,length('$.outcome')+1),type,atom FROM json_tree(CAST(NEW.canonical_event_bytes AS TEXT),'$.outcome')
            )
            OR EXISTS (
                SELECT substr(fullkey,length('$.failure')+1),type,atom FROM json_tree(CAST(NEW.canonical_event_bytes AS TEXT),'$.failure')
                EXCEPT SELECT substr(fullkey,length('$[1]')+1),type,atom FROM json_tree(w.outcome_json,'$[1]')
            )
            OR EXISTS (
                SELECT substr(fullkey,length('$[1]')+1),type,atom FROM json_tree(w.outcome_json,'$[1]')
                EXCEPT SELECT substr(fullkey,length('$.failure')+1),type,atom FROM json_tree(CAST(NEW.canonical_event_bytes AS TEXT),'$.failure')
            )
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.omittedCount') IS NOT 0
            OR json_array_length(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items') IS NOT 1
            OR NOT EXISTS (SELECT 1 FROM coordination_events cause WHERE cause.event_id=w.start_event_id)
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items[0]') IS NOT w.start_event_id)
    ) THEN RAISE(ABORT, 'wait event root is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_dependencies d WHERE d.event_id=NEW.event_id AND
          (d.root_thread_id!=NEW.root_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'dependencyDeclared'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.operationId') IS NOT d.operation_id)
    ) THEN RAISE(ABORT, 'dependency event is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_results r JOIN coordination_events t ON t.event_id=r.terminal_event_id
        WHERE r.observed_event_id=NEW.event_id AND
          (r.root_thread_id!=NEW.root_thread_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'terminalResultObserved'
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.resultId') IS NOT r.result_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.target') IS NOT json_extract(CAST(t.canonical_event_bytes AS TEXT),'$.target')
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.targetTurnId') IS NOT json_extract(CAST(t.canonical_event_bytes AS TEXT),'$.targetTurnId')
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.omittedCount') IS NOT 0
            OR json_array_length(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items') IS NOT 1
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items[0]') IS NOT r.terminal_event_id)
    ) THEN RAISE(ABORT, 'result observation event is incoherent') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM coordination_handoffs h JOIN coordination_results r USING(result_id)
        LEFT JOIN coordination_events attempted ON attempted.event_id=h.attempted_event_id
        LEFT JOIN coordination_events observed ON observed.event_id=r.observed_event_id
        WHERE (h.attempted_event_id=NEW.event_id OR h.received_event_id=NEW.event_id OR h.failed_event_id=NEW.event_id) AND
          (h.root_thread_id!=NEW.root_thread_id OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.handoffId') IS NOT h.handoff_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.resultId') IS NOT h.result_id
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.attempt') IS NOT h.attempt
            OR (h.attempted_event_id=NEW.event_id AND json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'handoffDeliveryAttempted')
            OR (h.received_event_id=NEW.event_id AND json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'handoffDurablyReceived')
            OR (h.failed_event_id=NEW.event_id AND json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.kind') IS NOT 'handoffDeliveryFailed')
            OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.omittedCount') IS NOT 0
            OR json_array_length(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items') IS NOT 1
            OR (h.attempted_event_id=NEW.event_id AND
              (r.observed_event_id IS NULL
                OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items[0]') IS NOT r.observed_event_id
                OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.from') IS NOT json_extract(CAST(observed.canonical_event_bytes AS TEXT),'$.target')))
            OR ((h.received_event_id=NEW.event_id OR h.failed_event_id=NEW.event_id) AND
              (json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.causes.items[0]') IS NOT h.attempted_event_id
                OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.from') IS NOT json_extract(CAST(attempted.canonical_event_bytes AS TEXT),'$.from')
                OR json_extract(CAST(NEW.canonical_event_bytes AS TEXT),'$.to') IS NOT json_extract(CAST(attempted.canonical_event_bytes AS TEXT),'$.to'))))
    ) THEN RAISE(ABORT, 'handoff event is incoherent') END;
END;
