# Local Luna-aware Codex

This fork runs `gpt-5.6-luna` on the `multi_agent_v2` runtime and also allows
Luna child agents, while keeping the upstream model catalog unchanged.

## Build

Install the Rust toolchain and the repository helpers once:

```sh
rustup component add rustfmt clippy
cargo install --locked just dotslash cargo-nextest
```

Build the release binary from the Rust workspace:

```sh
cd codex-rs
cargo build --release -p codex-cli
```

The resulting executable is `codex-rs/target/release/codex`.
It reports `codex-cli 0.145.0-alpha.11-vicentes-version` so the personal build
is distinguishable from the upstream package.

## Install locally

Remove the npm distribution and install the compiled binary as the system
`codex` command:

```sh
npm uninstall -g @openai/codex
mkdir -p "$HOME/.local/bin"
install -m 0755 target/release/codex "$HOME/.local/bin/codex"
hash -r
codex --version
```

Ensure `~/.local/bin` precedes other package-manager binary directories in
`PATH`. A clean login shell should resolve `codex` to `~/.local/bin/codex`.

Rebuild and reinstall after changing or updating the fork:

```sh
cd codex-rs
cargo build --release -p codex-cli
install -m 0755 target/release/codex "$HOME/.local/bin/codex"
```

## Configure Luna subagents

Add this to `~/.codex/config.toml`:

```toml
model = "gpt-5.6-luna"

[features.multi_agent_v2]
enabled = true
expose_spawn_agent_model_overrides = true
```

The feature setting forces the root session onto the v2 runtime. Spawn another
Luna agent with a non-full-history fork because v2 deliberately rejects model
overrides when `fork_turns = "all"`:

```json
{
  "task_name": "luna_worker",
  "message": "Review this implementation",
  "model": "gpt-5.6-luna",
  "fork_turns": "none"
}
```

## Keep up with upstream

Keep `origin` pointed at this fork and `upstream` pointed at OpenAI:

```sh
git fetch upstream
git rebase upstream/main
git push origin main
```

The Luna compatibility change is intentionally small, making upstream rebases
straightforward.
