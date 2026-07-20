CREATE TABLE coordination_authority (
    singleton_id INTEGER PRIMARY KEY NOT NULL DEFAULT 1 CHECK (singleton_id = 1),
    state_epoch TEXT NOT NULL UNIQUE CHECK (length(state_epoch) = 36),
    status TEXT NOT NULL CHECK (status IN ('active', 'quarantined')),
    quarantine_reason TEXT,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    CHECK (
        (status = 'active' AND quarantine_reason IS NULL)
        OR (status = 'quarantined' AND length(quarantine_reason) > 0)
    )
);

CREATE TRIGGER coordination_authority_identity_immutable
BEFORE UPDATE OF singleton_id, state_epoch, created_at_ms ON coordination_authority
BEGIN
    SELECT RAISE(ABORT, 'coordination authority identity is immutable');
END;

CREATE TRIGGER coordination_authority_insert_conflict_guard
BEFORE INSERT ON coordination_authority
WHEN EXISTS (SELECT 1 FROM coordination_authority)
BEGIN
    SELECT RAISE(ABORT, 'coordination authority already exists');
END;

CREATE TRIGGER coordination_authority_quarantine_terminal
BEFORE UPDATE ON coordination_authority
WHEN OLD.status = 'quarantined'
BEGIN
    SELECT RAISE(ABORT, 'quarantined coordination authority is read-only');
END;

CREATE TRIGGER coordination_authority_delete_guard
BEFORE DELETE ON coordination_authority
BEGIN
    SELECT RAISE(ABORT, 'coordination authority cannot be deleted');
END;

CREATE TABLE coordination_roots (
    root_thread_id TEXT PRIMARY KEY NOT NULL CHECK (length(root_thread_id) = 36),
    state_epoch TEXT NOT NULL REFERENCES coordination_authority(state_epoch)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    committed_revision INTEGER NOT NULL DEFAULT 0
        CHECK (committed_revision BETWEEN 0 AND 9223372036854775807),
    published_revision INTEGER NOT NULL DEFAULT 0
        CHECK (published_revision BETWEEN 0 AND committed_revision),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms)
);

CREATE TRIGGER coordination_roots_insert_conflict_guard
BEFORE INSERT ON coordination_roots
WHEN EXISTS (
    SELECT 1 FROM coordination_roots WHERE root_thread_id = NEW.root_thread_id
)
BEGIN
    SELECT RAISE(ABORT, 'coordination root already exists');
END;

CREATE TRIGGER coordination_roots_monotonic_update
BEFORE UPDATE ON coordination_roots
WHEN NEW.root_thread_id != OLD.root_thread_id
    OR NEW.state_epoch != OLD.state_epoch
    OR NEW.created_at_ms != OLD.created_at_ms
    OR NEW.committed_revision < OLD.committed_revision
    OR NEW.published_revision < OLD.published_revision
    OR NEW.updated_at_ms < OLD.updated_at_ms
BEGIN
    SELECT RAISE(ABORT, 'coordination root identity and watermarks are monotonic');
END;

CREATE TRIGGER coordination_roots_delete_guard
BEFORE DELETE ON coordination_roots
BEGIN
    SELECT RAISE(ABORT, 'coordination root cannot be deleted');
END;

CREATE TABLE coordination_events (
    event_id TEXT PRIMARY KEY NOT NULL CHECK (length(event_id) = 36),
    root_thread_id TEXT NOT NULL REFERENCES coordination_roots(root_thread_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 9223372036854775807),
    canonical_event_bytes BLOB NOT NULL
        CHECK (typeof(canonical_event_bytes) = 'blob')
        CHECK (length(canonical_event_bytes) BETWEEN 1 AND 8192),
    event_fingerprint BLOB NOT NULL
        CHECK (typeof(event_fingerprint) = 'blob' AND length(event_fingerprint) = 32),
    idempotency_key_bytes BLOB NOT NULL
        CHECK (typeof(idempotency_key_bytes) = 'blob')
        CHECK (length(idempotency_key_bytes) BETWEEN 1 AND 1024),
    idempotency_key_fingerprint BLOB NOT NULL
        CHECK (
            typeof(idempotency_key_fingerprint) = 'blob'
            AND length(idempotency_key_fingerprint) = 32
        ),
    occurred_at INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    UNIQUE (root_thread_id, revision),
    UNIQUE (root_thread_id, idempotency_key_fingerprint)
);

CREATE TRIGGER coordination_events_immutable_update
BEFORE UPDATE ON coordination_events
BEGIN
    SELECT RAISE(ABORT, 'coordination events are immutable');
END;

CREATE TRIGGER coordination_events_insert_conflict_guard
BEFORE INSERT ON coordination_events
WHEN EXISTS (
    SELECT 1
    FROM coordination_events
    WHERE event_id = NEW.event_id
        OR (
            root_thread_id = NEW.root_thread_id
            AND revision = NEW.revision
        )
        OR (
            root_thread_id = NEW.root_thread_id
            AND idempotency_key_fingerprint = NEW.idempotency_key_fingerprint
        )
)
BEGIN
    SELECT RAISE(ABORT, 'coordination event identity already exists');
END;

CREATE TRIGGER coordination_events_immutable_delete
BEFORE DELETE ON coordination_events
BEGIN
    SELECT RAISE(ABORT, 'coordination events are immutable');
END;

CREATE TABLE coordination_projection_outbox (
    event_id TEXT PRIMARY KEY NOT NULL REFERENCES coordination_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'leased', 'materialized')),
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
    )
);

CREATE INDEX idx_coordination_projection_claimable
    ON coordination_projection_outbox(status, retry_after_ms, lease_epoch);
