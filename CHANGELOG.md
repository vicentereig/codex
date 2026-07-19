The changelog can be found on the [releases page](https://github.com/openai/codex/releases).

## Vicente Codex 0.7.0

- Persist delegation intents, delivery receipts, leases, retries, and terminal outcomes across restarts.
- Reconcile due deliveries in bounded turn-completion maintenance and retain acknowledged records for recovery.
- Reconstruct partial or blocked parent delegation state with explicit unavailable outcomes.

Implementation: `6fe7f11837`.

## Vicente Codex 0.5.0

- Add targetable waits for live caller-subtree agents, with bounded deterministic summaries of terminal changes.
- Distinguish timeout from user steering; cursor checkpoints and unloaded-agent tombstones remain deferred.

Implementation: `28ecd3bb1d`.

## Vicente Codex 0.4.0

- Forward already-emitted `Interrupted` and `BudgetLimited` child terminal outcomes to the parent lifecycle (commit `2cd7183c2d`). This release does not create or enforce new budget-limited events.

## Vicente Codex 0.3.0

- Bound multi-agent completion context to keep delegation results within safe limits (commit `96883a8f7d`).
