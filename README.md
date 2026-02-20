# watchd

A minimal, language-agnostic file watcher that runs commands when files change and broadcasts reload or CSS-inject messages to browser clients via WebSocket.

## Features

- **Recursive directory watching** with configurable debounce
- **YAML-based configuration** (`hotreload.yaml`) for per-project build/run logic
- **WebSocket server** broadcasts `reload` and `inject-css` messages to connected browsers
- **CSS hot-injection** — CSS changes are applied without a full page reload
- **Embeddable JavaScript client** with automatic reconnection and exponential backoff
- **Config hot-reload** — editing `hotreload.yaml` reloads configs without restarting the server
- **Event kind filtering** — only data-affecting events are processed; metadata and access events are ignored
- **Command timeout** — optional per-command timeout prevents stuck build processes
- **Control socket** for CLI subcommands (`status`, `reload`) against a running instance
- **Connection tracking** — active WebSocket client count reported in status responses
- **Watch root validation** — the watch path is validated and canonicalized on startup
- **Graceful shutdown** via Ctrl-C with coordinated cancellation across all tasks
- **Structured logging** via `tracing` (configurable with `RUST_LOG` environment variable)

## Quick start

Build the watcher binary and run:

```bash
cargo build --manifest-path watcher/Cargo.toml
cargo run --manifest-path watcher/Cargo.toml -- --path .
```

Or run the produced binary directly (built at `watcher/target/debug/watchd`):

```bash
./watcher/target/debug/watchd --path .
```

Configuration is read from `hotreload.yaml` located at the watch root. See `watcher/README.md` for full documentation and examples.

## CLI

The `watchd` program supports flags and subcommands. Run the watcher server (default) or invoke control subcommands which contact the running watchd control socket on port+1.

### Server flags

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

### Client-mode subcommands

These contact the running `watchd` instance via the control socket (port + 1):

- `status` — query the running watchd for status of loaded configs and active connections
- `reload` — request an immediate reload; broadcasts a `reload` message to all connected clients

## Examples

```bash
# run the server and watch a directory
cargo run --manifest-path watcher/Cargo.toml -- --path /path/to/project --port 3012

# run the produced binary directly
./watcher/target/debug/watchd --path .

# use a command timeout to kill stuck builds after 30 seconds
./watcher/target/debug/watchd --path . --cmd-timeout-ms 30000

# print the browser client snippet for embedding
./watcher/target/debug/watchd --print-snippet

# query status (client-mode subcommand)
./watcher/target/debug/watchd --port 3012 status

# trigger reload (client-mode subcommand)
./watcher/target/debug/watchd --port 3012 reload
```

## Client Libraries

The `watchd` server is **language-agnostic** — any WebSocket client that understands the simple JSON protocol can integrate with it. Example clients are provided in the [`examples/`](examples/) directory for multiple languages and platforms:

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
<script src="/hotreload-client.js"></script>
```

Override the default port with a `data-port` attribute:

```html
<script src="/hotreload-client.js" data-port="4000"></script>
```

The browser client will:

- **Auto-reconnect** with exponential backoff (1 s → 30 s cap)
- **Reload the page** on `reload` messages
- **Inject CSS** on `inject-css` messages without a full page reload
- **Cache-bust** linked stylesheets whose `href` matches the changed path
- **Log** connection lifecycle events to the browser console

### Writing your own client

Implementing a watchd client in any language is straightforward:

1. Open a WebSocket connection to `ws://localhost:3012`.
2. Listen for incoming text messages.
3. Parse each message as JSON.
4. Inspect the `type` field:
   - `"reload"` → perform your reload action.
   - `"inject-css"` → base64-decode the `content` field and apply the CSS.
   - Unknown types → log and ignore (forward compatibility).
5. On disconnect, reconnect with exponential backoff.

## Logging

Control verbosity with the `RUST_LOG` environment variable:

```bash
# Default (info level)
watchd --path .

# Debug output (includes skipped paths, event details)
RUST_LOG=debug watchd --path .

# Quiet (warnings and errors only)
RUST_LOG=warn watchd --path .
```

## Architecture

The codebase is organised into focused modules:

| Module       | Responsibility                                              |
|--------------|-------------------------------------------------------------|
| `cli.rs`     | Command-line argument definitions (clap)                    |
| `client.rs`  | Embedded JavaScript hot-reload client and snippet helpers   |
| `config.rs`  | YAML config parsing with serde deserialization              |
| `error.rs`   | Centralised error type (`WatchdError`)                      |
| `filter.rs`  | Glob-based include/exclude path filtering                   |
| `server.rs`  | WebSocket server, control socket, and client helpers        |
| `watcher.rs` | File-system watcher, debouncing, command execution          |
| `main.rs`    | Entry point and orchestration                               |

The `examples/` directory contains ready-to-use client implementations for
JavaScript, Node.js, Python, Go, Rust, Ruby, and C#/.NET.

See `watcher/README.md` for detailed configuration, notification modes, and extensibility documentation.

## License

MIT