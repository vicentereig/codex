-- Stage 3.4 (codex-9u5.2.3.4): durable message/follow-up receipt-ref mailbox and
-- receipt-to-response-item materialization (Stage 3 contract freeze, Decision 9).
--
-- Three tables, all capability-off (nothing writes to them outside
-- `CoordinationControl::Enabled`, which only test code can construct):
--
--   1. `coordination_message_target_generations` -- one row per (root, target) naming the
--      currently accepted follow-up generation (if any) and the turn it is bound to, plus the
--      next generation a follow-up would reserve. This is a deliberately narrower, message/
--      follow-up-scoped sibling of `coordination_assignment_heads`/`_generations` (migration
--      0047), not a reuse of those literal rows: assignment heads are keyed by `assignment_id`
--      and only exist for spawned children, so they cannot represent a plain message sent to the
--      root agent (a real target of `send_message`, which has no assignment row). The *concept*
--      -- a monotonic generation counter with an optimistic-concurrency `version` column CASed
--      inside a single writer transaction -- is the one reused from 0047; seeding this from a
--      fresh table is the deliberate, documented judgment call (see the task report).
--   2. `coordination_message_receipts` -- the durable receipt-ref mailbox itself: one row per
--      message/follow-up delivery attempt, keyed by `receipt_id`, idempotent on `operation_id`
--      (the Decision-5 live-operation identity), capturing whichever generation/turn the receipt
--      is forever bound to and whether it has been durably enqueued yet.
--   3. `coordination_message_materializations` -- the receipt-to-response-item correlation,
--      keyed by `(receipt_id, target_turn_id, response_item_id)` per Decision 9's exact wording,
--      with a three-phase status (`committed` -> `rollout_appended` -> `selected`) that lets
--      restart distinguish the three cases the freeze calls out.
--
-- Stable carriers (`InterAgentCommunication`, `ResponseItem`, `ThreadItem`, rollout items) are
-- untouched by this migration; nothing here is serialized onto them.

CREATE TABLE coordination_message_target_generations (
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    target_thread_id TEXT NOT NULL CHECK (length(target_thread_id) = 36),
    accepted_generation INTEGER CHECK (accepted_generation IS NULL OR accepted_generation BETWEEN 1 AND 2147483647),
    next_generation INTEGER NOT NULL DEFAULT 1 CHECK (next_generation BETWEEN 1 AND 2147483647),
    accepted_turn_id TEXT CHECK (accepted_turn_id IS NULL OR length(accepted_turn_id) BETWEEN 1 AND 128),
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    PRIMARY KEY (root_thread_id, target_thread_id),
    CHECK (accepted_generation IS NULL OR accepted_generation < next_generation),
    CHECK ((accepted_generation IS NULL) = (accepted_turn_id IS NULL))
);

CREATE TRIGGER coordination_message_target_generations_monotonic
BEFORE UPDATE ON coordination_message_target_generations WHEN
    NEW.root_thread_id != OLD.root_thread_id OR NEW.target_thread_id != OLD.target_thread_id OR
    NEW.created_at_ms != OLD.created_at_ms OR NEW.next_generation < OLD.next_generation OR
    NEW.version <= OLD.version OR NEW.updated_at_ms < OLD.updated_at_ms OR
    (OLD.accepted_generation IS NOT NULL AND NEW.accepted_generation IS NULL) OR
    (OLD.accepted_generation IS NOT NULL AND NEW.accepted_generation IS NOT NULL
        AND NEW.accepted_generation < OLD.accepted_generation)
BEGIN SELECT RAISE(ABORT, 'coordination message target generation is not monotonic'); END;
CREATE TRIGGER coordination_message_target_generations_no_delete
BEFORE DELETE ON coordination_message_target_generations
BEGIN SELECT RAISE(ABORT, 'coordination message target generations cannot be deleted'); END;

CREATE TABLE coordination_message_receipts (
    receipt_id TEXT PRIMARY KEY NOT NULL CHECK (length(receipt_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    state_epoch TEXT NOT NULL REFERENCES coordination_authority(state_epoch)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    operation_id TEXT NOT NULL UNIQUE CHECK (length(operation_id) = 36),
    sender_thread_id TEXT NOT NULL CHECK (length(sender_thread_id) = 36),
    sender_turn_id TEXT NOT NULL CHECK (length(sender_turn_id) BETWEEN 1 AND 128),
    target_thread_id TEXT NOT NULL CHECK (length(target_thread_id) = 36),
    semantic_slot TEXT NOT NULL CHECK (semantic_slot IN ('message', 'followup')),
    trigger_turn INTEGER NOT NULL CHECK (trigger_turn IN (0, 1)),
    captured_generation INTEGER CHECK (captured_generation IS NULL OR captured_generation BETWEEN 1 AND 2147483647),
    bound_turn_id TEXT CHECK (bound_turn_id IS NULL OR length(bound_turn_id) BETWEEN 1 AND 128),
    status TEXT NOT NULL DEFAULT 'committed' CHECK (status IN ('committed', 'enqueued')),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    CHECK (semantic_slot = 'message' OR bound_turn_id IS NOT NULL),
    CHECK ((trigger_turn = 1) = (semantic_slot = 'followup'))
);

CREATE INDEX idx_coordination_message_receipts_pending
    ON coordination_message_receipts(root_thread_id, status);
CREATE INDEX idx_coordination_message_receipts_target
    ON coordination_message_receipts(root_thread_id, target_thread_id);

CREATE TRIGGER coordination_message_receipts_immutable_fields
BEFORE UPDATE ON coordination_message_receipts WHEN
    NEW.receipt_id != OLD.receipt_id OR NEW.root_thread_id != OLD.root_thread_id OR
    NEW.state_epoch != OLD.state_epoch OR NEW.operation_id != OLD.operation_id OR
    NEW.sender_thread_id != OLD.sender_thread_id OR NEW.sender_turn_id != OLD.sender_turn_id OR
    NEW.target_thread_id != OLD.target_thread_id OR NEW.semantic_slot != OLD.semantic_slot OR
    NEW.trigger_turn != OLD.trigger_turn OR NEW.created_at_ms != OLD.created_at_ms OR
    NEW.updated_at_ms < OLD.updated_at_ms OR
    (OLD.captured_generation IS NOT NULL AND NEW.captured_generation IS NOT OLD.captured_generation) OR
    (OLD.bound_turn_id IS NOT NULL AND NEW.bound_turn_id IS NOT OLD.bound_turn_id) OR
    (OLD.status = 'enqueued' AND NEW.status != 'enqueued')
BEGIN SELECT RAISE(ABORT, 'coordination message receipt fields are immutable once set'); END;
CREATE TRIGGER coordination_message_receipts_no_delete
BEFORE DELETE ON coordination_message_receipts
BEGIN SELECT RAISE(ABORT, 'coordination message receipts cannot be deleted'); END;

CREATE TABLE coordination_message_materializations (
    receipt_id TEXT NOT NULL REFERENCES coordination_message_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    target_turn_id TEXT NOT NULL CHECK (length(target_turn_id) BETWEEN 1 AND 128),
    response_item_id TEXT NOT NULL CHECK (length(response_item_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    status TEXT NOT NULL DEFAULT 'committed'
        CHECK (status IN ('committed', 'rollout_appended', 'selected')),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    PRIMARY KEY (receipt_id, target_turn_id, response_item_id)
);

CREATE INDEX idx_coordination_message_materializations_pending
    ON coordination_message_materializations(root_thread_id, status);

CREATE TRIGGER coordination_message_materializations_forward_only
BEFORE UPDATE ON coordination_message_materializations WHEN
    NEW.receipt_id != OLD.receipt_id OR NEW.target_turn_id != OLD.target_turn_id OR
    NEW.response_item_id != OLD.response_item_id OR NEW.root_thread_id != OLD.root_thread_id OR
    NEW.created_at_ms != OLD.created_at_ms OR NEW.updated_at_ms < OLD.updated_at_ms OR
    (OLD.status = 'committed' AND NEW.status NOT IN ('committed', 'rollout_appended', 'selected')) OR
    (OLD.status = 'rollout_appended' AND NEW.status NOT IN ('rollout_appended', 'selected')) OR
    (OLD.status = 'selected' AND NEW.status != 'selected')
BEGIN SELECT RAISE(ABORT, 'coordination message materialization status must move forward'); END;
CREATE TRIGGER coordination_message_materializations_no_delete
BEFORE DELETE ON coordination_message_materializations
BEGIN SELECT RAISE(ABORT, 'coordination message materializations cannot be deleted'); END;
