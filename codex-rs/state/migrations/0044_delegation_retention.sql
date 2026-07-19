ALTER TABLE delegations ADD COLUMN acknowledged_at_ms INTEGER;
ALTER TABLE delegations ADD COLUMN retained_until_ms INTEGER;

CREATE INDEX IF NOT EXISTS idx_delegations_retention
    ON delegations(status, acknowledged_at_ms, retained_until_ms);
