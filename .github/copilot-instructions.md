# Copilot Instructions for hotreload (watchd)

## Build, Test, and Lint Commands

- **Build watcher binary:**
  - `cargo build --manifest-path watcher/Cargo.toml`
- **Run watcher (development):**
  - `cargo run --manifest-path watcher/Cargo.toml -- --path .`
- **Run produced binary:**
  - `./watcher/target/debug/watchd --path .`
- **Run tests for watcher crate:**
  - `cargo test --manifest-path watcher/Cargo.toml`
- **Run all workspace tests:**
  - `cargo test --workspace`
- **Run a single test (example):**
  - `cargo test --manifest-path watcher/Cargo.toml <test_name>`
- **Run tests with limited parallelism (Windows socket tests):**
  - `cargo test --manifest-path watcher/Cargo.toml -- --test-threads=4`
- **Logging verbosity:**
  - `RUST_LOG=debug cargo run ...` (debug)
  - `RUST_LOG=warn cargo run ...` (warnings/errors only)

## High-Level Architecture

- **Main binary crate:** `watcher/` (bin: `watchd`)
- **Modules:**
  - `cli.rs`: CLI argument definitions (clap derive)
  - `client.rs`: Embedded JS hot-reload client & snippet helpers
  - `command.rs`: Shell escaping and blocking command execution
  - `config.rs`: YAML config parsing (serde)
  - `control.rs`: TCP control socket server, command handlers, client helpers
  - `debounce.rs`: Per-path event debouncer
  - `error.rs`: Centralised error type (`WatchdError`)
  - `filter.rs`: Glob-based include/exclude path filtering
  - `health.rs`: Worker thread health monitoring
  - `kv.rs`: Bounded in-memory diff store
  - `server.rs`: WebSocket server, status map infrastructure
  - `validate.rs`: Input validation helpers
  - `watcher.rs`: File-system watcher, event handling, worker context
  - `main.rs`: Entry point, startup orchestration
- **Config:** Place `hotreload.yaml` at the watch root. Supports single or multiple configs (YAML sequence).
- **WebSocket server:** Broadcasts `reload` and `inject-css` messages to browser clients.
- **Control socket:** Listens on port+1 for CLI subcommands (`status`, `reload`, `diff`, `diffs`, `diff-clear`).
- **Diff store:** Enabled with `--diff`, stores unified diffs for file changes.
- **Client libraries:** Example clients in `examples/` for JavaScript, Node.js, Python, Go, Rust, Ruby, C#/.NET.

## Key Conventions

- **Shell escaping:** All file paths interpolated into command templates use canonical escaping (see `command.rs`).
- **Error handling:** Centralised via `WatchdError` (`thiserror`). No bare `unwrap()`/`expect()` in production code.
- **Logging:** Uses `tracing` macros. Control verbosity with `RUST_LOG`.
- **Config structs:** Use `serde::Deserialize` for YAML parsing.
- **Modules:** Flat structure, one file per concern. Register new modules in `main.rs`.
- **Tests:** Co-located with their module in `#[cfg(test)] mod tests` blocks.
- **Platform-conditional compilation:** Used for shell escaping and command execution.
- **Default excludes:** `target/`, `.git/`, `node_modules/`, `*.tmp`, `*.swp`, `.DS_Store`, `Thumbs.db`, `*.lock`, `hotreload.yaml` (unless explicitly watched).
- **Event filtering:** Only data-affecting events (create, modify content, remove, rename) are processed.
- **Config hot-reload:** Editing `hotreload.yaml` reloads configs automatically without restarting the server.

---

If you want to configure MCP servers (e.g., Playwright for web testing), let me know!

---

Copilot instructions file created. Would you like to adjust anything or add coverage for areas I may have missed?
