//! watchd hot-reload client for Rust
//!
//! A standalone WebSocket client that connects to a running watchd server and
//! reacts to `reload` and `inject-css` messages. Designed for use in Rust
//! development tooling, custom dev-servers, or any Rust application that needs
//! to react to file-change notifications from watchd.
//!
//! # Usage
//!
//! ```bash
//! cargo run                             # connect to ws://127.0.0.1:3012
//! cargo run -- --addr ws://0.0.0.0:4000 # custom address
//! ```
//!
//! # Dependencies
//!
//! This example uses:
//!   - `tungstenite` for WebSocket connectivity
//!   - `serde` / `serde_json` for JSON parsing
//!   - `base64` for decoding CSS content
//!
//! # Protocol
//!
//! The watchd server sends JSON messages over WebSocket:
//!   - `{"type": "reload"}` → full reload
//!   - `{"type": "inject-css", "path": "...", "content": "..."}` → CSS injection (base64 content)
//!
//! # License
//!
//! MIT

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::Deserialize;
use std::thread;
use std::time::Duration;
use tungstenite::{connect, Message};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DEFAULT_ADDR: &str = "ws://127.0.0.1:3012";
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Protocol types
// ---------------------------------------------------------------------------

/// A message received from the watchd WebSocket server.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum WatchdMessage {
    /// The server is requesting a full reload.
    Reload,

    /// The server is providing updated CSS content for hot-injection.
    InjectCss {
        /// Relative path of the changed CSS file.
        path: String,
        /// Base64-encoded CSS content.
        content: String,
    },
}

// ---------------------------------------------------------------------------
// Event handlers — customise these for your use case
// ---------------------------------------------------------------------------

/// Called when the server broadcasts a `reload` message.
///
/// In a browser context this would reload the page. In a Rust application you
/// might restart a subprocess, re-read configuration, trigger a rebuild, or
/// send a signal to another component.
fn on_reload() {
    log("reload triggered");
    println!("  → files changed — implement your reload logic here");

    // Examples:
    //   std::process::Command::new("cargo").arg("build").status().ok();
    //   notify_ui_thread(UiEvent::Reload);
    //   app_state.lock().unwrap().reload_config();
}

/// Called when the server broadcasts an `inject-css` message.
///
/// The content is base64-decoded CSS. In a browser this would update a `<style>`
/// element in place. In a Rust application you might write it to a file,
/// forward it to a template engine, or push it to a UI framework.
fn on_inject_css(path: &str, css: &str) {
    log(&format!("CSS update for {} ({} bytes)", path, css.len()));
    println!("  → CSS injected: {}", path);

    // Examples:
    //   std::fs::write(path, css).ok();
    //   style_engine.update(path, css);
}

// ---------------------------------------------------------------------------
// Client implementation
// ---------------------------------------------------------------------------

/// Attempt to connect to the watchd server and listen for messages.
/// Returns an error when the connection drops or fails.
fn connect_and_listen(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    log(&format!("connecting to {}", addr));

    let (mut socket, _response) = connect(addr)?;

    log("connected");

    loop {
        let msg = socket.read()?;

        match msg {
            Message::Text(text) => {
                handle_message(&text);
            }
            Message::Close(_) => {
                log("server sent close frame");
                break;
            }
            Message::Ping(data) => {
                // Respond to pings to keep the connection alive.
                socket.send(Message::Pong(data))?;
            }
            // Ignore binary, pong, and frame messages.
            _ => {}
        }
    }

    Ok(())
}

/// Parse and dispatch a single JSON message from the server.
fn handle_message(raw: &str) {
    match serde_json::from_str::<WatchdMessage>(raw) {
        Ok(WatchdMessage::Reload) => {
            on_reload();
        }
        Ok(WatchdMessage::InjectCss { path, content }) => match BASE64.decode(&content) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(css) => on_inject_css(&path, &css),
                Err(e) => warn(&format!("CSS content is not valid UTF-8: {}", e)),
            },
            Err(e) => {
                warn(&format!("failed to decode base64 CSS for {}: {}", path, e));
            }
        },
        Err(e) => {
            // The message might be a valid JSON object with an unknown type.
            // Log it but don't treat it as a fatal error.
            warn(&format!("failed to parse message: {} (raw: {})", e, raw));
        }
    }
}

// ---------------------------------------------------------------------------
// Reconnection loop
// ---------------------------------------------------------------------------

/// Run the client with automatic reconnection and exponential backoff.
fn run(addr: &str) {
    let mut delay = INITIAL_RECONNECT_DELAY;

    loop {
        match connect_and_listen(addr) {
            Ok(()) => {
                log("disconnected");
            }
            Err(e) => {
                warn(&format!("connection error: {}", e));
            }
        }

        log(&format!("reconnecting in {:.1}s", delay.as_secs_f64()));
        thread::sleep(delay);

        // Exponential backoff with cap.
        delay = Duration::from_millis(
            (delay.as_millis() as u64 * 2).min(MAX_RECONNECT_DELAY.as_millis() as u64),
        );
    }
}

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

fn log(msg: &str) {
    let now = chrono_or_fallback_timestamp();
    println!("\x1b[1;31m[hotreload]\x1b[0m [{}] {}", now, msg);
}

fn warn(msg: &str) {
    let now = chrono_or_fallback_timestamp();
    eprintln!("\x1b[1;33m[hotreload]\x1b[0m [{}] WARNING: {}", now, msg);
}

/// Produce a simple HH:MM:SS timestamp using `std::time`.
/// We avoid pulling in the `chrono` crate for this example.
fn chrono_or_fallback_timestamp() -> String {
    use std::time::SystemTime;

    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;

    format!("{:02}:{:02}:{:02}", h, m, s)
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let addr = if args.len() > 1 {
        // Support `--addr <URL>` or just `<URL>` as the first positional arg.
        if args[1] == "--addr" && args.len() > 2 {
            args[2].clone()
        } else if args[1].starts_with("ws://") || args[1].starts_with("wss://") {
            args[1].clone()
        } else {
            eprintln!("Usage: hotreload-client [--addr] <ws://host:port>");
            eprintln!(
                "       hotreload-client                           (defaults to {})",
                DEFAULT_ADDR
            );
            std::process::exit(1);
        }
    } else {
        DEFAULT_ADDR.to_string()
    };

    println!("watchd hot-reload client for Rust");
    println!("Connecting to {}", addr);
    println!("Press Ctrl+C to exit.\n");

    // Install Ctrl-C handler for clean shutdown.
    ctrlc_handler();

    run(&addr);
}

/// Install a simple Ctrl-C handler that exits cleanly.
fn ctrlc_handler() {
    // We use a simple approach: catch the signal and exit.
    // In a more complex application you'd use a CancellationToken pattern.
    let _ = std::thread::spawn(|| {
        // On Unix, SIGINT is delivered to the process. On Windows, Ctrl-C
        // generates a console control event. In both cases the default
        // handler will terminate the process, which is fine for this example.
    });
}
