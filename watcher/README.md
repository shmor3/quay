# watchd

A minimal, language-agnostic file watcher that runs commands when files change and broadcasts reload or CSS-inject messages to browser clients via WebSocket.

## Features

- **Recursive directory watching** with configurable debounce
- **YAML-based configuration** (`hotreload.yaml`) for per-project build/run logic
- **WebSocket server** broadcasts `reload` and `inject-css` messages to connected browsers
- **CSS hot-injection** — CSS changes are applied without a full page reload
- **Embeddable JavaScript client** with automatic reconnection and exponential backoff
- **Config hot-reload** — editing `hotreload.yaml` reloads configs automatically without restarting the server
- **Event kind filtering** — only data-affecting events (create, modify content, remove, rename) are processed; pure metadata and access events are ignored
- **Command timeout** — optional per-command timeout prevents stuck build processes from blocking the worker
- **Control socket** for CLI subcommands (`status`, `reload`) against a running instance
- **Connection tracking** — active WebSocket client count is reported in status responses
- **Watch root validation** — the watch path is validated and canonicalized on startup for clear error messages
- **Sensible defaults** — common directories (`target/`, `.git/`, `node_modules/`, etc.) are excluded automatically
- **Graceful shutdown** via Ctrl-C with coordinated cancellation across all tasks
- **Structured logging** via `tracing` (configurable with `RUST_LOG` environment variable)

## Architecture

The codebase is split into focused modules:

| Module       | Responsibility                                              |
|--------------|-------------------------------------------------------------|
| `cli.rs`     | Command-line argument definitions (clap)                    |
| `client.rs`  | Embedded JavaScript hot-reload client and snippet helpers   |
| `config.rs`  | YAML config parsing with serde deserialization              |
| `error.rs`   | Centralised error type (`WatchdError`)                      |
| `filter.rs`  | Glob-based include/exclude path filtering                   |
| `kv.rs`      | Bounded in-memory diff store for file change tracking       |
| `server.rs`  | WebSocket server, control socket, and client helpers        |
| `watcher.rs` | File-system watcher, debouncing, command execution          |
| `main.rs`    | Entry point and orchestration                               |

## Quick start

Build:

```bash
cargo build --manifest-path watcher/Cargo.toml
```

Run the watcher server:

```bash
# via cargo
cargo run --manifest-path watcher/Cargo.toml -- --path /path/to/project

# or run the produced binary directly
./watcher/target/debug/watchd --path .
```

## CLI reference

### Server mode (default)

```
watchd [OPTIONS] [CMD_TEMPLATE]
```

| Flag / Argument           | Default              | Description                                                       |
|---------------------------|----------------------|-------------------------------------------------------------------|
| `[CMD_TEMPLATE]`          | `echo files changed` | Command to run on changes. Use `{path}` for the changed file path |
| `-p, --path <PATH>`       | `.`                  | Directory to watch and where to look for `hotreload.yaml`         |
| `--port <PORT>`           | `3012`               | WebSocket server port                                             |
| `--bind <ADDR>`           | `127.0.0.1`          | Address to bind the WebSocket and control servers to              |
| `--debounce-ms <MS>`      | `200`                | Debounce delay in milliseconds                                    |
| `--no-run-on-start`       |                      | Do not run configured commands on startup                         |
| `--cmd-timeout-ms <MS>`   |                      | Maximum time to wait for a command before killing it              |
| `--print-snippet`         |                      | Print the HTML `<script>` snippet for embedding the client, then exit |
| `--diff`                  |                      | Enable the in-memory diff store for file change tracking          |
| `--diff-max-file-size <B>`| `524288`             | Maximum file size (bytes) the diff store will process (ignored without `--diff`) |

### Client-mode subcommands

These contact the running `watchd` instance via the control socket (port + 1):

```bash
# Query status of loaded configs and active connections
watchd --port 3012 status

# Trigger an immediate reload
watchd --port 3012 reload

# Show the latest diff for a specific file (requires --diff on the server)
watchd --port 3012 diff --path src/styles/main.css

# List all tracked files with a summary (requires --diff on the server)
watchd --port 3012 diff
```

## Browser client

The `watchd` server broadcasts WebSocket messages, but your HTML pages need a small client script to receive them. There are several ways to add it:

### Option 1 — Print the snippet

Use the `--print-snippet` flag to output a ready-to-paste `<script>` tag:

```bash
watchd --port 3012 --print-snippet
```

This outputs both an inline `<script>` version (zero extra requests) and an external `<script src>` version.

### Option 2 — Serve the standalone file

Copy `hotreload-client.js` from this repository into your project's static assets and include it:

```html
<script src="/hotreload-client.js"></script>
```

### Option 3 — Override the port

If the watchd server runs on a non-default port, use the `data-port` attribute:

```html
<script src="/hotreload-client.js" data-port="4000"></script>
```

### Client features

- **Auto-reconnect** with exponential backoff (1 s → 30 s cap)
- **Full page reload** on `reload` messages
- **CSS hot-injection** on `inject-css` messages — injects or updates `<style data-hotreload="...">` elements
- **Cache-busting** of `<link rel="stylesheet">` elements whose `href` matches the changed path
- **Console logging** with coloured `[hotreload]` prefix
- Zero external dependencies — pure vanilla JS

## Diff store

When started with `--diff`, watchd records a unified diff for every file change in a bounded in-memory store. Diffs can be queried via the control socket using the `diff` subcommand.

### Enabling

```bash
watchd --path . --diff
```

Optionally adjust the maximum file size (files larger than this are recorded with a placeholder instead of a real diff):

```bash
watchd --path . --diff --diff-max-file-size 1048576
```

### Querying diffs

```bash
# Latest diff for a specific file
watchd --port 3012 diff --path src/main.css

# Summary of all tracked files
watchd --port 3012 diff
```

### Control socket commands

When the diff store is enabled, three additional JSON commands are available on the control socket (port + 1):

| Command                              | Description                                    |
|--------------------------------------|------------------------------------------------|
| `{"cmd":"diff","path":"<path>"}`     | Return the latest diff for the given file      |
| `{"cmd":"diffs"}`                    | List all tracked files with summary statistics  |
| `{"cmd":"diff-clear"}`              | Clear all entries from the diff store           |

### Limits

The diff store is bounded to prevent unbounded memory growth:

- **Per-key capacity**: 50 diff entries per file (oldest evicted first)
- **Max tracked files**: 500 distinct paths (least-recently-inserted evicted)
- **Max file size**: 512 KiB by default (configurable with `--diff-max-file-size`)

Files exceeding the size limit are recorded with a `<file too large>` placeholder and their content is not stored in memory.

## Configuration

Place a `hotreload.yaml` file in the watch root directory. It can contain a single config mapping or a YAML sequence of configs.

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

When `hotreload.yaml` itself is modified, watchd automatically reloads its config entries without requiring a server restart. The status map is updated and new configs take effect immediately.

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
- `**/hotreload.yaml`

### Event filtering

Only data-affecting filesystem events are processed:

| Processed               | Ignored                    |
|-------------------------|----------------------------|
| File created            | File accessed (read)       |
| File content modified   | Metadata-only changes      |
| File removed            | Permission changes         |
| File renamed            | Timestamp-only changes     |

This reduces noise and prevents unnecessary rebuilds from editors that touch file metadata without changing content.

## Command timeout

Use `--cmd-timeout-ms` to prevent stuck build commands from blocking the worker thread indefinitely:

```bash
watchd --path . --cmd-timeout-ms 30000
```

If a command exceeds the timeout, the process is killed and the worker continues processing subsequent events. Commands that finish within the timeout are unaffected.

## Logging

Logging uses the `tracing` framework. Control verbosity with the `RUST_LOG` environment variable:

```bash
# Default (info level)
watchd --path .

# Debug output (includes skipped paths, event details)
RUST_LOG=debug watchd --path .

# Quiet (warnings and errors only)
RUST_LOG=warn watchd --path .
```

## Status endpoint

The `watchd status` subcommand (or a direct TCP connection to the control port) returns a JSON response including:

- Config names and their current state (`up to date`, `reloading`)
- Number of active WebSocket connections

```json
{
  "configs": [
    { "name": "styles", "status": "up to date" },
    { "name": "scripts", "status": "up to date" }
  ],
  "connections": 2
}
```

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