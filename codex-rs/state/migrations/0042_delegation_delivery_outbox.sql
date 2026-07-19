CREATE TABLE IF NOT EXISTS delegation_delivery_outbox (
    delivery_id TEXT PRIMARY KEY NOT NULL,
    delegation_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    payload_receipt TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'acked')),
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(delegation_id, attempt)
);

CREATE INDEX IF NOT EXISTS idx_delegation_outbox_pending
    ON delegation_delivery_outbox(status, updated_at_ms);
