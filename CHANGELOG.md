The changelog can be found on the [releases page](https://github.com/openai/codex/releases).

## Vicente Codex 0.4.0

- Forward already-emitted `Interrupted` and `BudgetLimited` child terminal outcomes to the parent lifecycle (commit `2cd7183c2d`). This release does not create or enforce new budget-limited events.

## Vicente Codex 0.3.0

- Bound multi-agent completion context to keep delegation results within safe limits (commit `96883a8f7d`).
