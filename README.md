# quay

A minimal, language-agnostic file watcher that runs commands when files change and broadcasts reload or CSS-inject messages to browser clients via WebSocket.

## Features

- **Recursive directory watching** with configurable debounce
- **YAML-based configuration** (`quay.yaml`) for per-project build/run logic
- **WebSocket server** broadcasts `reload` and `inject-css` messages to connected browsers
- **CSS hot-injection** — CSS changes are applied without a full page reload
- **Embeddable JavaScript client** with automatic reconnection and exponential backoff
- **Config hot-reload** — editing `quay.yaml` reloads configs automatically without restarting the server
- **Event kind filtering** — only data-affecting events (create, modify content, remove, rename) are processed; pure metadata and access events are ignored
- **Command timeout** — optional per-command timeout prevents stuck build processes from blocking the worker
- **Control socket** for CLI subcommands (`status`, `reload`, `diff`) against a running instance
- **In-memory diff store** — optionally records unified diffs for every file change, queryable via control socket
- **Connection tracking** — active WebSocket client count is reported in status responses
- **Watch root validation** — the watch path is validated and canonicalized on startup for clear error messages
- **Input validation** — bind address, debounce delay, port, and timeout values are validated at startup with actionable error messages
- **Shell escaping** — file paths interpolated into command templates are escaped to prevent command injection
- **Sensible defaults** — common directories (`target/`, `.git/`, `node_modules/`, etc.) are excluded automatically
- **Worker health monitoring** — a background quayog detects if the file-watcher thread terminates unexpectedly and initiates graceful shutdown
- **Graceful shutdown** via Ctrl-C with coordinated cancellation across all tasks
- **Structured logging** via `tracing` (configurable with `RUST_LOG` environment variable)
- **Self-healing** — accept errors on WebSocket and control sockets trigger a 1-second backoff and retry rather than crashing; the worker thread catches panics in individual event handlers and continues processing

## Quick start

### Setup

1. Install Rust (https://rustup.rs) if not already installed.
2. Clone the repository: `git clone <repo-url>`
3. Build the watcher binary:
   ```bash
   cargo build --manifest-path watcher/Cargo.toml
   ```
4. Run the watcher:
   ```bash
   cargo run --manifest-path watcher/Cargo.toml -- --path .
   ```
5. Optionally, copy the produced binary from `watcher/target/debug/quay` to your PATH for easier usage.

### Configuration

- Place a `quay.yaml` file in your project root. See below and `watcher/README.md` for config details.
- Use CLI flags to override defaults (see below).

### Troubleshooting

- If the watcher does not start, check for errors in the console. Common issues:
  - Invalid YAML config: check for syntax errors in `quay.yaml`.
  - Port already in use: change the `--port` flag or stop other processes.
  - Permission denied: ensure you have access to the watch path.
- For detailed logs, set `RUST_LOG=debug`.

### Common Errors

- `Failed to parse quay.yaml`: Check YAML syntax and required fields.
- `Bind error`: Port/address in use or invalid. Try a different port/address.
- `Command failed`: Check your `on_change` command for correctness.
- `Diff store disabled`: Use `--diff` flag to enable diff tracking.

### FAQ

**Q: How do I add quay to my HTML page?**
A: Use `quay --print-snippet` to get the script tag, or copy `quay-client.js` to your static assets.

**Q: Can I use this with any language?**
A: Yes, configs are language-agnostic. Use `on_change` to run any build command.

**Q: How do I debug file events?**
A: Set `RUST_LOG=debug` and check the console output for event details.

**Q: How do I run multiple configs?**
A: Use a YAML sequence in `quay.yaml` (see examples).

**Q: What if my build command hangs?**
A: Use `--cmd-timeout-ms` to kill stuck commands automatically.

**Q: How do I clear diffs?**
A: Use the `diff-clear` control command: `quay --port <port> diff-clear`.

**Q: How do I expose the server to other machines?**
A: Use `--bind 0.0.0.0` but beware of security risks (see Security Considerations).

**Q: How do I get help?**
A: Run `quay --help` or consult `watcher/README.md` for full documentation.

---

For more practical guidance, see the expanded sections below and `watcher/README.md`.

Build the watcher binary and run:

```bash
cargo build --manifest-path watcher/Cargo.toml
cargo run --manifest-path watcher/Cargo.toml -- --path .
```

Or run the produced binary directly (built at `watcher/target/debug/quay`):

```bash
./watcher/target/debug/quay --path .
```

Configuration is read from `quay.yaml` located at the watch root. See `watcher/README.md` for full documentation and examples.

## CLI

The `quay` program supports flags and subcommands. Run the watcher server (default) or invoke control subcommands which contact the running quay control socket on port+1.

### Server mode (default)

```
quay [OPTIONS] [CMD_TEMPLATE]
```

| Flag / Argument           | Default              | Description                                                       |
|---------------------------|----------------------|-------------------------------------------------------------------|
| `[CMD_TEMPLATE]`          | `echo files changed` | Command to run on changes. Use `{path}` for the changed file path |
| `-p, --path <PATH>`       | `.`                  | Directory to watch and where to look for `quay.yaml`         |
| `--port <PORT>`           | `3012`               | WebSocket server port                                             |
| `--bind <ADDR>`           | `127.0.0.1`          | Address to bind the WebSocket and control servers to              |
| `--debounce-ms <MS>`      | `200`                | Debounce delay in milliseconds                                    |
| `--no-run-on-start`       |                      | Do not run configured commands on startup                         |
| `--cmd-timeout-ms <MS>`   |                      | Maximum time to wait for a command before killing it              |
| `--print-snippet`         |                      | Print the HTML `<script>` snippet for embedding the client, then exit |
| `--diff`                  |                      | Enable the in-memory diff store for file change tracking          |
| `--diff-max-file-size <B>`| `524288`             | Maximum file size (bytes) the diff store will process (ignored without `--diff`) |

### Client-mode subcommands

These contact the running `quay` instance via the control socket (port + 1):

- `status` — query the running quay for status of loaded configs and active connections
- `reload` — request an immediate reload; broadcasts a `reload` message to all connected clients
- `diff --path <FILE>` — show the latest diff for a specific file (requires `--diff` on the server)
- `diff` — list all tracked files with a summary (requires `--diff` on the server)

## Examples

```bash
# run the server and watch a directory
cargo run --manifest-path watcher/Cargo.toml -- --path /path/to/project --port 3012

# run the produced binary directly
./watcher/target/debug/quay --path .

# use a command timeout to kill stuck builds after 30 seconds
./watcher/target/debug/quay --path . --cmd-timeout-ms 30000

# enable the diff store to track file changes
./watcher/target/debug/quay --path . --diff

# enable diff with a custom max file size (1 MiB)
./watcher/target/debug/quay --path . --diff --diff-max-file-size 1048576

# bind to all interfaces (accessible from other machines on the network)
./watcher/target/debug/quay --path . --bind 0.0.0.0

# print the browser client snippet for embedding
./watcher/target/debug/quay --print-snippet

# query status (client-mode subcommand)
./watcher/target/debug/quay --port 3012 status

# trigger reload (client-mode subcommand)
./watcher/target/debug/quay --port 3012 reload

# query the latest diff for a file (client-mode subcommand)
./watcher/target/debug/quay --port 3012 diff --path src/styles/main.css

# list all tracked file diffs (client-mode subcommand)
./watcher/target/debug/quay --port 3012 diff
```

## Client Libraries

The `quay` server is **language-agnostic** — any WebSocket client that understands the simple JSON protocol can integrate with it. Example clients are provided in the [`examples/`](examples/) directory for multiple languages and platforms:

| Directory                              | Language / Platform        | Description                                        |
|----------------------------------------|----------------------------|----------------------------------------------------|
| [`examples/javascript/`](examples/javascript/) | JavaScript (Browser) | Drop-in browser `<script>` with auto-reconnect     |
| [`examples/html/`](examples/html/)     | HTML                       | Minimal demo page using the browser client         |
| [`examples/nodejs/`](examples/nodejs/) | Node.js                    | Server-side client using the `ws` package          |
| [`examples/python/`](examples/python/) | Python 3                   | Async client using `websockets`                    |
| [`examples/go/`](examples/go/)         | Go                         | Client using `gorilla/websocket`                   |
| [`examples/rust/`](examples/rust/)     | Rust                       | Client using `tungstenite`                         |
| [`examples/ruby/`](examples/ruby/)     | Ruby                       | Client using `websocket-client-simple`             |
| [`examples/csharp/`](examples/csharp/) | C# / .NET                  | Client using `System.Net.WebSockets`               |

See [`examples/README.md`](examples/README.md) for the full WebSocket protocol specification and quick-start instructions for each client.

### Browser client

Add the hot-reload client to your HTML pages to receive live updates. Use `--print-snippet` to generate the appropriate `<script>` tag, or include the standalone file from `examples/javascript/`:

```html
<script src="/quay-client.js"></script>
```

Override the default port with a `data-port` attribute:

```html
<script src="/quay-client.js" data-port="4000"></script>
```

The browser client will:

- **Auto-reconnect** with exponential backoff (1 s → 30 s cap)
- **Reload the page** on `reload` messages
- **Inject CSS** on `inject-css` messages without a full page reload
- **Cache-bust** linked stylesheets whose `href` matches the changed path
- **Log** connection lifecycle events to the browser console

### Writing your own client

Implementing a quay client in any language is straightforward:

1. Open a WebSocket connection to `ws://localhost:3012`.
2. Listen for incoming text messages.
3. Parse each message as JSON.
4. Inspect the `type` field:
   - `"reload"` → perform your reload action.
   - `"inject-css"` → base64-decode the `content` field and apply the CSS.
   - Unknown types → log and ignore (forward compatibility).
5. On disconnect, reconnect with exponential backoff.

## WebSocket Protocol

### Messages (server → client)

All messages are JSON objects with a `type` field.

#### `reload`

Indicates that a watched file has changed and the client should perform a full reload.

```json
{
  "type": "reload"
}
```

#### `inject-css`

Indicates that a CSS file has changed. The new content is included as a base64-encoded string so the client can apply it without a full reload.

```json
{
  "type": "inject-css",
  "path": "src/styles/main.css",
  "content": "Ym9keSB7IGJhY2tncm91bmQ6IHJlZDsgfQ=="
}
```

| Field     | Type   | Description                              |
|-----------|--------|------------------------------------------|
| `type`    | string | Always `"inject-css"`                    |
| `path`    | string | Normalised path of the changed CSS file  |
| `content` | string | Base64-encoded CSS content               |

### Messages (client → server)

The server does not currently expect any messages from clients. Client frames are silently ignored. Future protocol versions may add client-to-server commands.

## Control Socket Protocol

The control socket listens on `port + 1` (e.g. `3013` when the WebSocket port is `3012`). It uses a simple JSON-over-TCP protocol: each connection sends one newline-terminated JSON command and receives one newline-terminated JSON response.

### Supported commands

| Command                              | Description                                    |
|--------------------------------------|------------------------------------------------|
| `{"cmd":"reload"}`                   | Broadcast a reload message, reset statuses     |
| `{"cmd":"status"}`                   | Return config statuses and connection count    |
| `{"cmd":"diff","path":"<path>"}`     | Return the latest diff for the given file      |
| `{"cmd":"diffs"}`                    | List all tracked files with summary statistics |
| `{"cmd":"diff-clear"}`              | Clear all entries from the diff store           |

### Example responses

**status:**

```json
{
  "configs": [
    { "name": "styles", "status": "up to date" },
    { "name": "scripts", "status": "reloading" }
  ],
  "connections": 2
}
```

**reload:**

```json
{ "status": "ok" }
```

**diff:**

```json
{
  "path": "src/main.css",
  "timestamp": 1700000000,
  "diff": "- .old { color: red; }\n+ .new { color: blue; }\n",
  "old_size": 24,
  "new_size": 25,
  "binary": false,
  "truncated": false
}
```

**diffs:**

```json
{
  "summary": {
    "tracked_files": 3,
    "total_diffs": 7,
    "capacity": 50,
    "max_keys": 500,
    "max_file_size": 524288
  },
  "files": [
    {
      "path": "src/main.css",
      "latest_diff_size": 48,
      "old_size": 24,
      "new_size": 25,
      "binary": false,
      "truncated": false
    }
  ]
}
```

### Guard rails

- **Read timeout** (5 s) prevents stalled clients from holding a socket open.
- **Size limit** (64 KiB) prevents malicious or buggy clients from sending multi-gigabyte payloads.
- **Lock poisoning** is handled gracefully — a poisoned mutex logs a warning and returns an internal-error JSON response rather than panicking.
- **Accept errors** trigger a 1-second backoff and retry rather than crashing.

## Configuration

Place a `quay.yaml` file in the watch root directory. It can contain a single config mapping or a YAML sequence of configs.

### Config fields

| Field       | Type                 | Description                                                                 |
|-------------|----------------------|-----------------------------------------------------------------------------|
| `name`      | `string`             | Human-readable identifier (default: `"unnamed"`)                            |
| `watch`     | `string \| [string]` | Glob pattern(s) to match changed files                                     |
| `on_change` | `string`             | Command template to run on change (use `{path}` placeholder)               |
| `build`     | `string`             | Alternative build command (used when `on_change` is absent)                 |
| `notify`    | `string`             | `auto`, `reload`, `inject-css`, or `none`                                   |
| `ignore`    | `string \| [string]` | Additional exclude globs merged into the watcher's default excludes         |

### Single config example

```yaml
name: typescript
watch:
  - "src/**/*.ts"
  - "src/**/*.tsx"
on_change: "npm run build -- {path}"
notify: reload
ignore:
  - "dist/**"
```

### Multiple configs example

```yaml
- name: styles
  watch: "src/**/*.css"
  notify: inject-css

- name: scripts
  watch:
    - "src/**/*.ts"
    - "src/**/*.js"
  on_change: "npm run build"
  notify: reload
```

### Config hot-reload

When `quay.yaml` itself is modified, quay automatically reloads its config entries without requiring a server restart. The status map is updated and new configs take effect immediately.

### Notification modes

| Mode         | Behaviour                                                    |
|--------------|--------------------------------------------------------------|
| `auto`       | CSS files → inject; everything else → reload (default)       |
| `reload`     | Always send a full-page reload                               |
| `inject-css` | Always inject CSS content (base64-encoded over WebSocket)    |
| `none`       | Run the command but do not notify browser clients             |

### Default excludes

The following patterns are always excluded unless the file matches an explicit `watch` pattern in a config:

- `target/**`
- `.git/**`
- `node_modules/**`
- `**/*.tmp`
- `**/*.swp`
- `**/.DS_Store`
- `**/Thumbs.db`
- `**/*.lock`
- `**/quay.yaml`

### Fallback behaviour

When no config matches a changed file, quay uses built-in heuristics:

1. **`.css` files** → inject CSS via WebSocket (no full reload).
2. **`.html` / `.htm` files** → broadcast a full-page reload.
3. **All other files** → run the `CMD_TEMPLATE` positional argument (with `{path}` replaced by the shell-escaped file path) and broadcast a reload.

### Event filtering

Only data-affecting filesystem events are processed:

| Processed               | Ignored                    |
|-------------------------|----------------------------|
| File created            | File accessed (read)       |
| File content modified   | Metadata-only changes      |
| File removed            | Permission changes         |
| File renamed            | Timestamp-only changes     |

This reduces noise and prevents unnecessary rebuilds from editors that touch file metadata without changing content.

## Diff Store

When started with `--diff`, quay records a unified diff for every file change in a bounded in-memory store. Diffs can be queried via the control socket using the `diff` subcommand.

### Enabling

```bash
quay --path . --diff
```

Optionally adjust the maximum file size (files larger than this are recorded with a placeholder instead of a real diff):

```bash
quay --path . --diff --diff-max-file-size 1048576
```

### Querying diffs

```bash
# Latest diff for a specific file
quay --port 3012 diff --path src/main.css

# Summary of all tracked files
quay --port 3012 diff
```

### Limits

The diff store is bounded to prevent unbounded memory growth:

- **Per-key capacity**: 50 diff entries per file (oldest evicted first)
- **Max tracked files**: 500 distinct paths (least-recently-inserted evicted)
- **Max file size**: 512 KiB by default (configurable with `--diff-max-file-size`)

Files exceeding the size limit are recorded with a `<file too large>` placeholder and their content is not stored in memory.

## Command Timeout

Use `--cmd-timeout-ms` to prevent stuck build commands from blocking the worker thread indefinitely:

```bash
quay --path . --cmd-timeout-ms 30000
```

If a command exceeds the timeout, the process is killed and the worker continues processing subsequent events. Commands that finish within the timeout are unaffected.

## Security Considerations

### Network exposure

By default, quay binds to `127.0.0.1` (localhost only). This means:

- The WebSocket server is accessible only from the local machine.
- The control socket is accessible only from the local machine.

If you bind to `0.0.0.0` or `::`, both the WebSocket server and the control socket become accessible from other machines on the network. **Do not do this in untrusted environments** without adding an authentication layer.

### Control socket authentication

The control socket has **no authentication**. Any process on the local machine can send commands (reload, status, diff, diff-clear). This is acceptable for a development tool but should be considered if deploying in shared environments.

### No TLS

All communication (WebSocket and control socket) is unencrypted plaintext. This is appropriate for local development but should not be used over untrusted networks without a TLS-terminating proxy.

### Command injection prevention

File paths interpolated into command templates via the `{path}` placeholder are shell-escaped to prevent command injection. On Unix, paths are wrapped in single quotes with internal single quotes escaped. On Windows, paths are wrapped in double quotes with internal double quotes and percent signs escaped.

### WebSocket handshake timeout

A 10-second timeout is applied to the WebSocket handshake to prevent raw TCP connections from holding resources indefinitely.

### Control socket size limit

The control socket limits incoming requests to 64 KiB to prevent denial-of-service attacks via memory exhaustion.

## Recovery and Self-Healing

### Worker thread quayog

A background quayog monitors the file-watcher worker thread. If the thread terminates unexpectedly (panic that escapes `catch_unwind`, channel closure, or any other fatal error), the quayog:

1. Logs the failure and any panic payload at `error` level.
2. Triggers the cancellation token for coordinated shutdown.
3. All server tasks (WebSocket, control socket) shut down gracefully.

This prevents the server from running in a degraded state where no file changes are being detected.

### Worker thread panic recovery

Within the worker thread, each file-system event is processed inside `catch_unwind`. If processing a single event panics (e.g. due to a malformed path), the panic is caught, logged, and the worker continues processing subsequent events. Statistics (total events received, processed, errors, panics) are logged when the worker exits.

### Accept error resilience

Both the WebSocket server and control socket handle `accept()` errors (e.g. `EMFILE` — too many open files) by sleeping for 1 second and retrying, rather than crashing.

### Debouncer memory management

The per-path debouncer periodically prunes stale entries (older than 10× the debounce window) every 1000 events to prevent unbounded memory growth during long-running sessions.

### Lock poisoning

All mutex operations (status map, diff store) handle lock poisoning gracefully by logging a warning and continuing with degraded functionality rather than panicking.

### Process supervisor integration

For production deployments, use a process supervisor (systemd, Docker restart policy, etc.) to restart quay automatically on exit. The quayog ensures quay exits cleanly when the worker thread fails, making it compatible with restart-on-exit policies.

## Logging

Control verbosity with the `RUST_LOG` environment variable:

```bash
# Default (info level)
quay --path .

# Debug output (includes skipped paths, event details, debouncer activity)
RUST_LOG=debug quay --path .

# Quiet (warnings and errors only)
RUST_LOG=warn quay --path .
```

## Architecture

The codebase is organised into focused modules:

| Module        | Responsibility                                              |
|---------------|-------------------------------------------------------------|
| `cli.rs`      | Command-line argument definitions (clap)                    |
| `client.rs`   | Embedded JavaScript hot-reload client and snippet helpers   |
| `command.rs`  | Shell escaping and blocking command execution               |
| `config.rs`   | YAML config parsing with serde deserialization              |
| `control.rs`  | TCP control socket server and command handlers              |
| `debounce.rs` | Per-path event debouncer with periodic pruning              |
| `error.rs`    | Centralised error type (`WatchdError`)                      |
| `filter.rs`   | Glob-based include/exclude path filtering                   |
| `health.rs`   | Worker thread health monitoring and self-healing            |
| `kv.rs`       | Bounded in-memory diff store for file change tracking       |
| `server.rs`   | WebSocket server and shared status-map infrastructure       |
| `validate.rs` | Input validation helpers for CLI arguments                  |
| `watcher.rs`  | File-system watcher, event handling, and orchestration      |
| `main.rs`     | Entry point and startup orchestration                       |

The `examples/` directory contains ready-to-use client implementations for
JavaScript, Node.js, Python, Go, Rust, Ruby, and C#/.NET.

## Extensibility

This tool is intentionally language-agnostic — it does not parse or compile any language-specific files. Use `on_change` or `build` commands to invoke any build step you need:

```yaml
# Rust project
- name: rust
  watch: "src/**/*.rs"
  on_change: "cargo build"
  notify: reload

# Sass compilation
- name: sass
  watch: "styles/**/*.scss"
  on_change: "sass styles/main.scss public/main.css"
  notify: inject-css

# Go project with timeout
- name: go
  watch: "**/*.go"
  on_change: "go build ./..."
  notify: reload
```

## License

MIT