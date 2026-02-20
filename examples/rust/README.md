# Rust Hot-Reload Client

A standalone WebSocket client that connects to a running `watchd` server and reacts to `reload` and `inject-css` messages.

## Requirements

- Rust 1.70+
- Dependencies are managed via `Cargo.toml` (no system-level requirements)

## Quick Start

```bash
# From this directory
cargo run

# Connect to a custom address
cargo run -- --addr ws://192.168.1.10:4000

# Or pass the URL directly
cargo run -- ws://localhost:3012
```

## Dependencies

| Crate         | Purpose                          |
|---------------|----------------------------------|
| `tungstenite` | WebSocket client implementation  |
| `serde`       | JSON deserialization             |
| `serde_json`  | JSON parsing                     |
| `base64`      | Decoding CSS content             |

## How It Works

1. Opens a WebSocket connection to the `watchd` server (default `ws://127.0.0.1:3012`).
2. Listens for incoming JSON messages.
3. Dispatches to handler functions based on the message `type`:
   - `"reload"` → calls `on_reload()`
   - `"inject-css"` → decodes the base64 content and calls `on_inject_css(path, css)`
4. On disconnection, reconnects automatically with exponential backoff (1s → 30s cap).

## Customisation

Edit the handler functions in `src/main.rs` to implement your own logic:

```rust
fn on_reload() {
    // Restart a subprocess, trigger a rebuild, etc.
    std::process::Command::new("cargo").arg("build").status().ok();
}

fn on_inject_css(path: &str, css: &str) {
    // Write the updated CSS to disk
    std::fs::write(path, css).ok();
}
```

## Using as a Library

To integrate into your own Rust project, copy the `WatchdMessage` enum and the `connect_and_listen` function into your codebase, or extract them into a shared crate:

```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum WatchdMessage {
    Reload,
    InjectCss { path: String, content: String },
}
```

## License

MIT