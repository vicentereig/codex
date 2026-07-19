ALTER TABLE delegation_delivery_outbox ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE delegation_delivery_outbox ADD COLUMN retry_after_ms INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_delegation_outbox_claimable
    ON delegation_delivery_outbox(status, retry_after_ms, lease_epoch);
