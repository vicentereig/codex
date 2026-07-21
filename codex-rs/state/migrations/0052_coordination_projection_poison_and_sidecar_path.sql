-- Stage 3.1 (codex-9u5.2.3.1): freeze the coordination projection contract.
--
-- Two schema amendments, both capability-off:
--
--   1. Decision 2 (sidecar path): add a nullable, persisted `sidecar_path` to
--      `coordination_roots`. The resolved sidecar path is written the first time
--      it is computed (by the Stage 3.2 writer) and read verbatim thereafter, so
--      it is never re-derived from the live rollout directory. This migration only
--      introduces the storage column; no writer is added here.
--
--   2. Decision 3 (poison status): the native per-root publication outbox
--      `coordination_projection_outbox` (migration 0046) already carries the same
--      `retry_count BETWEEN 0 AND 8` bound as the degradation publication outbox
--      (migration 0050) but is missing the terminal `'poisoned'` status value.
--      Add it, mirroring 0050's pattern, together with the matching
--      `status != 'poisoned' OR last_error IS NOT NULL` coherence CHECK (0050 uses
--      `failure_code`; 0046's outbox already ships an equivalent `last_error`
--      column, so no new column is required). The `retry_count` bound is left
--      untouched: eight scheduled retries, poison on the ninth failure.
--
-- A CHECK constraint cannot be altered in place in SQLite, so the outbox is
-- rebuilt via the standard copy/rename idiom (see 0033). No triggers are added:
-- the projection outbox has deliberately never carried row-level state-machine
-- triggers (unlike 0050's degradation outbox), and existing callers and tests
-- rely on it remaining directly mutable; the R+1 claim/ack/retry/poison state
-- machine is enforced in the crate-private resolver instead.

PRAGMA foreign_keys=OFF;
-- `coordination_inbox_receipt_event_guard` (migration 0049) references
-- `coordination_projection_outbox` from its body. Under the default
-- `legacy_alter_table=OFF`, dropping the referenced table reparses that trigger
-- and aborts with "no such table". Enable legacy alter-table semantics for the
-- duration of the rebuild so the drop/rename does not validate the sibling
-- trigger against the transient missing table; the trigger resolves correctly
-- again once the rebuilt table is renamed into place.
PRAGMA legacy_alter_table=ON;

ALTER TABLE coordination_roots ADD COLUMN sidecar_path TEXT
    CHECK (sidecar_path IS NULL OR length(sidecar_path) BETWEEN 1 AND 1024);

CREATE TABLE coordination_projection_outbox_new (
    event_id TEXT PRIMARY KEY NOT NULL REFERENCES coordination_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'leased', 'materialized', 'poisoned')),
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count BETWEEN 0 AND 8),
    retry_after_ms INTEGER NOT NULL DEFAULT 0 CHECK (retry_after_ms >= 0),
    lease_expires_at_ms INTEGER CHECK (lease_expires_at_ms >= 0),
    last_error TEXT,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    CHECK (
        (status = 'leased' AND lease_expires_at_ms IS NOT NULL)
        OR (status != 'leased' AND lease_expires_at_ms IS NULL)
    ),
    CHECK (status != 'poisoned' OR last_error IS NOT NULL)
);

INSERT INTO coordination_projection_outbox_new (
    event_id, status, version, lease_epoch, retry_count, retry_after_ms,
    lease_expires_at_ms, last_error, created_at_ms, updated_at_ms
)
SELECT
    event_id, status, version, lease_epoch, retry_count, retry_after_ms,
    lease_expires_at_ms, last_error, created_at_ms, updated_at_ms
FROM coordination_projection_outbox;

DROP TABLE coordination_projection_outbox;
ALTER TABLE coordination_projection_outbox_new RENAME TO coordination_projection_outbox;

CREATE INDEX idx_coordination_projection_claimable
    ON coordination_projection_outbox(status, retry_after_ms, lease_epoch);

PRAGMA legacy_alter_table=OFF;
PRAGMA foreign_keys=ON;
