# CLAUDE.md — Project Intelligence

## Project Overview

**quay** (quay) is a minimal, language-agnostic file watcher written in Rust. It runs commands when files change and broadcasts `reload` or `inject-css` messages to browser clients via WebSocket.

## Repository Structure

```
quay/
├── Cargo.toml              # Workspace root (resolver = "2")
├── watcher/                # Main binary crate ("quay")
│   ├── Cargo.toml          # Dependencies and [bin] definition (binary: quay)
│   ├── src/
│   │   ├── main.rs         # Entry point, startup orchestration, module registration
│   │   ├── cli.rs          # CLI argument definitions (clap derive)
│   │   ├── client.rs       # Embedded JS hot-reload client & snippet helpers
│   │   ├── command.rs      # Shell escaping and blocking command execution (canonical impl)
│   │   ├── config.rs       # YAML config parsing (serde), uses command::shell_escape
│   │   ├── control.rs      # TCP control socket server, command handlers, client helpers
│   │   ├── debounce.rs     # Per-path event debouncer with periodic pruning
│   │   ├── error.rs        # Centralised error type (WatchdError, thiserror)
│   │   ├── filter.rs       # Glob-based include/exclude path filtering (globset)
│   │   ├── health.rs       # Worker thread health monitoring / quayog
│   │   ├── kv.rs           # Bounded in-memory diff store (similar crate)
│   │   ├── server.rs       # WebSocket server, status map infrastructure
│   │   ├── validate.rs     # Input validation helpers for CLI arguments
│   │   └── watcher.rs      # File-system watcher, event handling, worker context
│   └── README.md           # Detailed watcher documentation
├── examples/               # Client implementations in multiple languages
│   ├── javascript/         # Browser <script> client with auto-reconnect
│   ├── html/               # Minimal demo page
│   ├── nodejs/             # Node.js client (ws package)
│   ├── python/             # Python 3 async client (websockets)
│   ├── go/                 # Go client (gorilla/websocket)
│   ├── rust/               # Rust client (tungstenite)
│   ├── ruby/               # Ruby client (websocket-client-simple)
│   ├── csharp/             # C# / .NET client
│   └── README.md           # WebSocket protocol spec & quickstart for each client
└── README.md               # Top-level project README
```

## Build & Run

```bash
# Build the watcher binary
cargo build --manifest-path watcher/Cargo.toml

# Run the watcher (development)
cargo run --manifest-path watcher/Cargo.toml -- --path .

# Run the produced binary directly
./watcher/target/debug/quay --path .

# Run with debug logging
RUST_LOG=debug cargo run --manifest-path watcher/Cargo.toml -- --path .
```

## Testing

```bash
# Run tests for the watcher crate
cargo test --manifest-path watcher/Cargo.toml

# Run all workspace tests
cargo test --workspace

# Run tests with limited parallelism (useful on Windows for socket tests)
cargo test --manifest-path watcher/Cargo.toml -- --test-threads=4
```

## Key Technical Details

### Language & Tooling
- **Rust edition 2021**, workspace with resolver v2
- Binary name: `quay` (defined in `watcher/Cargo.toml` as `[[bin]]`)
- Crate name: `quay` (v0.2.0)

### Core Dependencies
- `notify 6.1` — cross-platform filesystem watching
- `tokio 1.36` (full features) — async runtime
- `tokio-tungstenite 0.20` — async WebSocket server
- `tokio-util 0.7` — `CancellationToken` for coordinated shutdown
- `clap 4.3` (derive) — CLI argument parsing
- `serde + serde_yaml 0.9` — YAML config deserialization
- `serde_json 1.0` — JSON for WebSocket messages and control protocol
- `globset 0.4` — glob pattern matching for file filtering
- `tracing + tracing-subscriber 0.3` — structured logging with env-filter
- `thiserror 2` — ergonomic error types
- `similar 2.6` — text diffing for the in-memory diff store
- `base64 0.22` — encoding CSS content for injection messages
- `futures 0.3` — stream/sink extensions for WebSocket handling

### Architecture Patterns
- **Async Tokio tasks** for WebSocket server and control socket
- **Dedicated OS thread** (`quay-worker`) for the blocking event loop — avoids starving the async runtime
- **Graceful shutdown** via Ctrl-C with `CancellationToken` (tokio-util) propagated to all tasks
- **Worker quayog** (`health.rs`) — background Tokio task polls the worker thread's `JoinHandle`; triggers shutdown if the thread dies
- **Panic recovery** — individual event handlers are wrapped in `catch_unwind` so one bad path doesn't kill the worker
- **Debounced file events** — configurable debounce delay with periodic pruning of stale entries
- **Config hot-reload** — editing `quay.yaml` at the watch root reloads without restart
- **Control socket** on port+1 for CLI subcommands (`status`, `reload`, `diff`, `diffs`, `diff-clear`)

### Module Responsibilities (after refactoring)

| Module        | What it owns                                                          |
|---------------|-----------------------------------------------------------------------|
| `command.rs`  | **Single canonical** `shell_escape` + `run_command_blocking` — no duplication |
| `debounce.rs` | `Debouncer` struct — extracted from `watcher.rs` for single responsibility |
| `control.rs`  | Control socket server + all command handlers + CLI client helpers — extracted from `server.rs` |
| `server.rs`   | WebSocket accept loop + `StatusMap` type + `set_status` helper |
| `health.rs`   | `WorkerWatchdog` — monitors worker thread, triggers shutdown on failure |
| `validate.rs` | Input validation: bind address, debounce range, port warnings, diff flag consistency |
| `watcher.rs`  | `WatcherParams`, `WorkerContext`, `start()`, event filtering, notification, config hot-reload |
| `config.rs`   | YAML parsing, `ConfigEntry`, `NotifyMode`, imports `command::shell_escape` |
| `filter.rs`   | `PathFilter`, `normalize_path`, `DEFAULT_EXCLUDES`, glob set building |

### Security Model
- Shell escaping prevents command injection via `{path}` placeholder
- Control socket has **no authentication** — local-only by default (`127.0.0.1`)
- WebSocket handshake timeout (10s) prevents resource exhaustion from raw TCP connections
- Control socket read timeout (5s) and size limit (64 KiB) prevent slow/malicious clients
- Lock poisoning handled gracefully with warnings (no panics)
- Accept errors trigger 1s backoff and retry (no crash loops)

### WebSocket Protocol
Messages are JSON with a `type` field:
- `"reload"` — full page reload
- `"inject-css"` — CSS hot injection (includes base64 `content` and `path` fields)

### Control Socket Protocol
JSON-over-TCP on port+1. One newline-terminated command per connection:
- `{"cmd":"reload"}` → `{"status":"ok"}`
- `{"cmd":"status"}` → `{"configs":[...],"connections":N}`
- `{"cmd":"diff","path":"..."}` → diff entry JSON
- `{"cmd":"diffs"}` → summary + file list
- `{"cmd":"diff-clear"}` → `{"status":"ok"}`

### Configuration
- Project config lives in `quay.yaml` at the watch root
- CLI flags override config file values
- Default WebSocket port: `3012`, default bind: `127.0.0.1`
- Control socket port: WebSocket port + 1

## Code Conventions

- **No duplicated logic** — `shell_escape` lives in `command.rs` and is imported by both `config.rs` and `watcher.rs`
- **Error handling** uses a centralised `WatchdError` type via `thiserror`
- **Logging** uses `tracing` macros (`info!`, `debug!`, `warn!`, `error!`)
- **Log level** controlled by `RUST_LOG` environment variable
- **CLI** uses `clap` derive macros for argument definitions
- **Config structs** use `serde::Deserialize` for YAML parsing
- **Modules** are flat (no nested module directories), one file per concern
- **No bare `unwrap()`/`expect()`** in production code — all error paths use proper handling or `unwrap_or_else`
- **`pub(crate)` visibility** for internal types that don't need to be publicly exported (e.g. `Debouncer`)
- **Tests co-located** with their module in `#[cfg(test)] mod tests` blocks
- **Platform-conditional compilation** via `#[cfg(target_os = "windows")]` for shell escaping and command execution

## Important Notes

- The workspace `Cargo.toml` is at the repo root; the actual crate is in `watcher/`
- Build artifacts go to `target/` (gitignored)
- `tmp_test_watch/` is also gitignored (used in testing)
- The `examples/` directory is documentation/reference only — not part of the Rust build
- When adding new modules, register them in `watcher/src/main.rs`
- `watcher::start()` returns `(RecommendedWatcher, JoinHandle<()>)` — the watcher must be kept alive and the handle is passed to the health quayog
- On Windows, control socket integration tests may need `--test-threads=4` to avoid flakiness from connection resets
- The `normalize_path` function in `filter.rs` and `normalize_pattern` in `config.rs` both convert backslashes to forward slashes — they are kept separate because they operate on different semantic types (file paths vs. glob patterns)