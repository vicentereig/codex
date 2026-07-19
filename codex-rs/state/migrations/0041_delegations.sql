CREATE TABLE IF NOT EXISTS delegations (
    delegation_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL,
    parent_thread_id TEXT NOT NULL,
    parent_turn_id TEXT NOT NULL DEFAULT '',
    owner_session_id TEXT NOT NULL DEFAULT '',
    child_thread_id TEXT,
    agent_path TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'unknown', 'reserved', 'bound', 'running', 'cancel_requested', 'retryable',
        'completed', 'failed', 'cancelled', 'detached'
    )),
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    last_error TEXT,
    outcome TEXT,
    terminal_event_id TEXT,
    delivery_receipt TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_delegations_parent_status
    ON delegations(parent_thread_id, status, updated_at_ms);

CREATE INDEX IF NOT EXISTS idx_delegations_child
    ON delegations(child_thread_id);
