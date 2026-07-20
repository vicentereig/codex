DROP TRIGGER coordination_inclusion_transition_guard;

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
        AND (
            (i.lease_expires_at_ms > NEW.transport_completed_at_ms
                AND i.expires_at_ms > NEW.transport_completed_at_ms)
            OR (NEW.transport_state = 'sendUnknown'
                AND NEW.failure_code IS NULL
                AND NEW.retry_after_ms = NEW.transport_completed_at_ms
                AND (i.lease_expires_at_ms <= NEW.transport_completed_at_ms
                    OR i.expires_at_ms <= NEW.transport_completed_at_ms))
        )
  )
BEGIN SELECT RAISE(ABORT, 'invalid coordination inclusion transition'); END;
