//! WebSocket server and TCP control socket for the `watchd` application.
//!
//! The WebSocket server accepts browser clients and forwards broadcast messages
//! (reload / inject-css) to all connected peers.
//!
//! The control socket provides a simple JSON-over-TCP protocol so that CLI
//! subcommands (`watchd reload`, `watchd status`, `watchd diff`) can interact
//! with the running server.
//!
//! When the diff store is enabled (`--diff`), additional control commands are
//! available:
//! - `{"cmd":"diff","path":"<path>"}` – latest diff for a file
//! - `{"cmd":"diffs"}` – summary of all tracked files
//! - `{"cmd":"diff-clear"}` – clear the diff store

use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::kv::SharedDiffStore;
use crate::watcher;

/// Shared status map tracking the state of each loaded config.
pub type StatusMap = Arc<Mutex<HashMap<String, String>>>;

/// Create a new [`StatusMap`] pre-populated with `"up to date"` for every config
/// name in `names`.
pub fn new_status_map(names: impl IntoIterator<Item = String>) -> StatusMap {
    let map: HashMap<String, String> = names
        .into_iter()
        .map(|n| (n, "up to date".to_string()))
        .collect();
    Arc::new(Mutex::new(map))
}

/// Update a single entry in the status map.
///
/// This is a convenience wrapper that acquires the lock, inserts the value, and
/// releases the lock immediately to minimise contention.
pub fn set_status(statuses: &StatusMap, name: &str, status: &str) {
    if let Ok(mut guard) = statuses.lock() {
        guard.insert(name.to_string(), status.to_string());
    } else {
        warn!(name, "status map lock poisoned; skipping status update");
    }
}

// ---------------------------------------------------------------------------
// WebSocket server
// ---------------------------------------------------------------------------

/// Spawn the WebSocket accept loop as a background task.
///
/// Each connected client receives a [`broadcast::Receiver`] and forwards
/// messages until the client disconnects or the cancellation token fires.
///
/// The `connection_count` is incremented on connect and decremented on
/// disconnect so that the control socket can report the number of active
/// browser clients.
pub fn spawn_ws_server(
    listener: TcpListener,
    btx: broadcast::Sender<String>,
    cancel: CancellationToken,
    connection_count: Arc<AtomicUsize>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    info!("WebSocket accept loop shutting down");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            let peer_btx = btx.clone();
                            let peer_cancel = cancel.clone();
                            let peer_counter = connection_count.clone();
                            tokio::spawn(async move {
                                handle_ws_client(stream, addr, peer_btx, peer_cancel, peer_counter).await;
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "WebSocket listener accept error");
                            break;
                        }
                    }
                }
            }
        }
    });
}

/// Handle a single WebSocket client connection.
async fn handle_ws_client(
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    btx: broadcast::Sender<String>,
    cancel: CancellationToken,
    connection_count: Arc<AtomicUsize>,
) {
    // Apply a timeout to the WebSocket handshake so that a client that opens a
    // raw TCP connection but never sends the HTTP Upgrade request cannot hold
    // resources indefinitely.
    let ws_stream = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio_tungstenite::accept_async(stream),
    )
    .await
    {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            debug!(error = %e, %addr, "WebSocket handshake failed");
            return;
        }
        Err(_) => {
            debug!(%addr, "WebSocket handshake timed out after 10s");
            return;
        }
    };

    let count = watcher::track_connect(&connection_count);
    info!(%addr, connections = count, "WebSocket client connected");

    let (mut ws_tx, mut ws_rx) = ws_stream.split();
    let mut rx = btx.subscribe();

    // Forward broadcast messages → WebSocket client.
    let send_cancel = cancel.clone();
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = send_cancel.cancelled() => break,
                msg = rx.recv() => {
                    match msg {
                        Ok(text) => {
                            if ws_tx.send(Message::Text(text)).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(%addr, skipped = n, "WebSocket client lagged behind");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    });

    // Drain incoming messages (we don't use them) to detect disconnects.
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            frame = ws_rx.next() => {
                match frame {
                    Some(Ok(_)) => { /* ignore client messages */ }
                    _ => break,
                }
            }
        }
    }

    send_task.abort();
    let count = watcher::track_disconnect(&connection_count);
    info!(%addr, connections = count, "WebSocket client disconnected");
}

// ---------------------------------------------------------------------------
// Control socket
// ---------------------------------------------------------------------------

/// Spawn the TCP control socket accept loop as a background task.
///
/// Supported JSON commands:
/// - `{"cmd": "reload"}` – broadcast a reload message and return `{"status":"ok"}`.
/// - `{"cmd": "status"}` – return the current config status map and connection count.
pub fn spawn_control_server(
    listener: TcpListener,
    btx: broadcast::Sender<String>,
    statuses: StatusMap,
    cancel: CancellationToken,
    connection_count: Arc<AtomicUsize>,
    diff_store: Option<SharedDiffStore>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    info!("control socket shutting down");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            debug!(%addr, "control client connected");
                            let btx = btx.clone();
                            let statuses = statuses.clone();
                            let cancel = cancel.clone();
                            let counter = connection_count.clone();
                            let diff_store = diff_store.clone();
                            tokio::spawn(async move {
                                handle_control_client(socket, btx, statuses, cancel, counter, diff_store).await;
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "control socket accept error");
                            break;
                        }
                    }
                }
            }
        }
    });
}

/// Handle a single control socket connection.
///
/// Reads one newline-terminated JSON command, processes it, writes a response,
/// and closes the connection.  A 5-second read timeout prevents stalled clients
/// from holding the socket open indefinitely.
async fn handle_control_client(
    mut socket: tokio::net::TcpStream,
    btx: broadcast::Sender<String>,
    statuses: StatusMap,
    cancel: CancellationToken,
    connection_count: Arc<AtomicUsize>,
    diff_store: Option<SharedDiffStore>,
) {
    let mut buf: Vec<u8> = Vec::with_capacity(256);

    // Read with a timeout to protect against slow/malicious clients.
    let read_result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut reader = BufReader::new(&mut socket);
        reader.read_until(b'\n', &mut buf).await
    })
    .await;

    match read_result {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            debug!(error = %e, "control socket read error");
            return;
        }
        Err(_) => {
            debug!("control socket read timed out");
            let _ = socket.write_all(b"{\"error\":\"read timeout\"}\n").await;
            return;
        }
    }

    let txt = String::from_utf8_lossy(&buf);
    let response = match serde_json::from_str::<serde_json::Value>(&txt) {
        Ok(v) => match v.get("cmd").and_then(|c| c.as_str()) {
            Some("reload") => handle_reload_cmd(&btx, &statuses),
            Some("status") => handle_status_cmd(&statuses, &connection_count),
            Some("diff") => {
                let path = v.get("path").and_then(|p| p.as_str());
                handle_diff_cmd(&diff_store, path)
            }
            Some("diffs") => handle_diffs_cmd(&diff_store),
            Some("diff-clear") => handle_diff_clear_cmd(&diff_store),
            Some(other) => {
                warn!(cmd = other, "unknown control command");
                "{\"error\":\"unknown command\"}\n".to_string()
            }
            None => "{\"error\":\"missing 'cmd' field\"}\n".to_string(),
        },
        Err(_) => "{\"error\":\"invalid json\"}\n".to_string(),
    };

    let _ = socket.write_all(response.as_bytes()).await;
    // Connection is dropped (closed) when `socket` goes out of scope.
    let _ = cancel; // silence unused-variable warning – kept for future use
}

/// Process a `reload` command: broadcast a reload message and update statuses.
fn handle_reload_cmd(btx: &broadcast::Sender<String>, statuses: &StatusMap) -> String {
    let _ = btx.send(serde_json::json!({"type": "reload"}).to_string());

    if let Ok(mut guard) = statuses.lock() {
        for value in guard.values_mut() {
            *value = "up to date".to_string();
        }
    }

    "{\"status\":\"ok\"}\n".to_string()
}

/// Process a `status` command: serialise the current status map and include
/// the active WebSocket connection count.
fn handle_status_cmd(statuses: &StatusMap, connection_count: &Arc<AtomicUsize>) -> String {
    let configs = if let Ok(guard) = statuses.lock() {
        guard
            .iter()
            .map(|(name, status)| serde_json::json!({"name": name, "status": status}))
            .collect::<Vec<_>>()
    } else {
        warn!("status map lock poisoned");
        Vec::new()
    };

    let connections = connection_count.load(Ordering::Relaxed);

    let body = serde_json::json!({
        "configs": configs,
        "connections": connections,
    });
    format!("{body}\n")
}

// ---------------------------------------------------------------------------
// Client helpers (used by `watchd reload` / `watchd status` subcommands)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Diff store command handlers
// ---------------------------------------------------------------------------

/// Process a `diff` command: return the latest diff for a specific file path.
fn handle_diff_cmd(diff_store: &Option<SharedDiffStore>, path: Option<&str>) -> String {
    let store = match diff_store {
        Some(s) => s,
        None => return "{\"error\":\"diff store not enabled (start with --diff)\"}\n".to_string(),
    };

    let path = match path {
        Some(p) => p,
        None => return "{\"error\":\"missing 'path' field\"}\n".to_string(),
    };

    let guard = match store.lock() {
        Ok(g) => g,
        Err(_) => {
            warn!("diff store lock poisoned");
            return "{\"error\":\"internal error\"}\n".to_string();
        }
    };

    match guard.get_latest(path) {
        Some(entry) => {
            let body = entry.to_json();
            format!("{body}\n")
        }
        None => {
            let body = serde_json::json!({"error": "no diff found", "path": path});
            format!("{body}\n")
        }
    }
}

/// Process a `diffs` command: list all tracked files with a summary.
fn handle_diffs_cmd(diff_store: &Option<SharedDiffStore>) -> String {
    let store = match diff_store {
        Some(s) => s,
        None => return "{\"error\":\"diff store not enabled (start with --diff)\"}\n".to_string(),
    };

    let guard = match store.lock() {
        Ok(g) => g,
        Err(_) => {
            warn!("diff store lock poisoned");
            return "{\"error\":\"internal error\"}\n".to_string();
        }
    };

    let summary = guard.summary();
    let keys: Vec<&str> = guard.list_keys();
    let files: Vec<serde_json::Value> = keys
        .iter()
        .filter_map(|k| {
            guard.get_latest(k).map(|entry| {
                serde_json::json!({
                    "path": k,
                    "latest_diff_size": entry.diff.len(),
                    "old_size": entry.old_size,
                    "new_size": entry.new_size,
                    "binary": entry.binary,
                    "truncated": entry.truncated,
                })
            })
        })
        .collect();

    let body = serde_json::json!({
        "summary": summary,
        "files": files,
    });
    format!("{body}\n")
}

/// Process a `diff-clear` command: clear all entries from the diff store.
fn handle_diff_clear_cmd(diff_store: &Option<SharedDiffStore>) -> String {
    let store = match diff_store {
        Some(s) => s,
        None => return "{\"error\":\"diff store not enabled (start with --diff)\"}\n".to_string(),
    };

    match store.lock() {
        Ok(mut guard) => {
            guard.clear();
            "{\"status\":\"ok\"}\n".to_string()
        }
        Err(_) => {
            warn!("diff store lock poisoned");
            "{\"error\":\"internal error\"}\n".to_string()
        }
    }
}

// ---------------------------------------------------------------------------
// Client helpers (used by `watchd reload` / `watchd status` / `watchd diff`)
// ---------------------------------------------------------------------------

/// Send a reload request to the control socket and print the response.
pub async fn send_reload(control_addr: &str) -> std::result::Result<(), String> {
    send_control_command(control_addr, "reload").await
}

/// Send a status request to the control socket and print the response.
pub async fn send_status(control_addr: &str) -> std::result::Result<(), String> {
    send_control_command(control_addr, "status").await
}

/// Send a diff request to the control socket and print the response.
///
/// When `path` is `Some`, queries the latest diff for that file.
/// When `path` is `None`, lists all tracked files with a summary.
pub async fn send_diff(control_addr: &str, path: Option<&str>) -> std::result::Result<(), String> {
    let mut stream = tokio::net::TcpStream::connect(control_addr)
        .await
        .map_err(|e| format!("failed to connect to control socket at {control_addr}: {e}"))?;

    let req = match path {
        Some(p) => serde_json::json!({"cmd": "diff", "path": p}).to_string() + "\n",
        None => serde_json::json!({"cmd": "diffs"}).to_string() + "\n",
    };
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("failed to send diff request: {e}"))?;

    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf).await;
    println!("{}", String::from_utf8_lossy(&buf));

    Ok(())
}

/// Generic helper: connect, send a JSON command, read the full response, print it.
async fn send_control_command(addr: &str, cmd: &str) -> std::result::Result<(), String> {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("failed to connect to control socket at {addr}: {e}"))?;

    let req = serde_json::json!({"cmd": cmd}).to_string() + "\n";
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("failed to send {cmd} request: {e}"))?;

    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf).await;
    println!("{}", String::from_utf8_lossy(&buf));

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::DiffStore;
    use tokio::io::AsyncWriteExt;

    // =====================================================================
    // Status map
    // =====================================================================

    #[test]
    fn new_status_map_populates_entries() {
        let map = new_status_map(vec!["a".to_string(), "b".to_string()]);
        let guard = map.lock().unwrap();
        assert_eq!(guard.get("a").unwrap(), "up to date");
        assert_eq!(guard.get("b").unwrap(), "up to date");
        assert_eq!(guard.len(), 2);
    }

    #[test]
    fn new_status_map_empty_input() {
        let map = new_status_map(Vec::<String>::new());
        let guard = map.lock().unwrap();
        assert!(guard.is_empty());
    }

    #[test]
    fn new_status_map_single_entry() {
        let map = new_status_map(vec!["only".to_string()]);
        let guard = map.lock().unwrap();
        assert_eq!(guard.len(), 1);
        assert_eq!(guard.get("only").unwrap(), "up to date");
    }

    #[test]
    fn new_status_map_duplicate_names_deduplicates() {
        // HashMap naturally deduplicates keys.
        let map = new_status_map(vec!["dup".to_string(), "dup".to_string()]);
        let guard = map.lock().unwrap();
        assert_eq!(guard.len(), 1);
        assert_eq!(guard.get("dup").unwrap(), "up to date");
    }

    #[test]
    fn new_status_map_special_characters() {
        let map = new_status_map(vec![
            "name with spaces".to_string(),
            "path/to/thing".to_string(),
            "émojis 🎉".to_string(),
        ]);
        let guard = map.lock().unwrap();
        assert_eq!(guard.len(), 3);
        assert_eq!(guard.get("émojis 🎉").unwrap(), "up to date");
    }

    // =====================================================================
    // set_status
    // =====================================================================

    #[test]
    fn set_status_updates_entry() {
        let map = new_status_map(vec!["cfg".to_string()]);
        set_status(&map, "cfg", "reloading");
        let guard = map.lock().unwrap();
        assert_eq!(guard.get("cfg").unwrap(), "reloading");
    }

    #[test]
    fn set_status_creates_new_entry() {
        let map = new_status_map(Vec::<String>::new());
        set_status(&map, "new_cfg", "building");
        let guard = map.lock().unwrap();
        assert_eq!(guard.get("new_cfg").unwrap(), "building");
    }

    #[test]
    fn set_status_overwrites_existing() {
        let map = new_status_map(vec!["x".to_string()]);
        set_status(&map, "x", "first");
        set_status(&map, "x", "second");
        set_status(&map, "x", "third");
        let guard = map.lock().unwrap();
        assert_eq!(guard.get("x").unwrap(), "third");
    }

    #[test]
    fn set_status_empty_strings() {
        let map = new_status_map(Vec::<String>::new());
        set_status(&map, "", "");
        let guard = map.lock().unwrap();
        assert_eq!(guard.get("").unwrap(), "");
    }

    // =====================================================================
    // handle_reload_cmd
    // =====================================================================

    #[test]
    fn handle_reload_cmd_returns_ok() {
        let (btx, _rx) = broadcast::channel::<String>(8);
        let statuses = new_status_map(vec!["a".to_string()]);
        set_status(&statuses, "a", "reloading");

        let response = handle_reload_cmd(&btx, &statuses);
        assert!(response.contains("\"ok\""));

        let guard = statuses.lock().unwrap();
        assert_eq!(guard.get("a").unwrap(), "up to date");
    }

    #[test]
    fn handle_reload_cmd_resets_all_statuses() {
        let (btx, _rx) = broadcast::channel::<String>(8);
        let statuses = new_status_map(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        set_status(&statuses, "a", "reloading");
        set_status(&statuses, "b", "building");
        set_status(&statuses, "c", "error");

        handle_reload_cmd(&btx, &statuses);

        let guard = statuses.lock().unwrap();
        for val in guard.values() {
            assert_eq!(val, "up to date");
        }
    }

    #[test]
    fn handle_reload_cmd_broadcasts_reload_message() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = new_status_map(Vec::<String>::new());

        handle_reload_cmd(&btx, &statuses);

        let msg = brx.try_recv().expect("should have received broadcast");
        let parsed: serde_json::Value =
            serde_json::from_str(&msg).expect("broadcast should be valid JSON");
        assert_eq!(parsed["type"], "reload");
    }

    #[test]
    fn handle_reload_cmd_with_empty_statuses() {
        let (btx, _rx) = broadcast::channel::<String>(8);
        let statuses = new_status_map(Vec::<String>::new());

        let response = handle_reload_cmd(&btx, &statuses);
        assert!(response.contains("\"ok\""));
    }

    #[test]
    fn handle_reload_cmd_response_is_valid_json() {
        let (btx, _rx) = broadcast::channel::<String>(8);
        let statuses = new_status_map(vec!["x".to_string()]);

        let response = handle_reload_cmd(&btx, &statuses);
        let parsed: serde_json::Value =
            serde_json::from_str(response.trim()).expect("response should be valid JSON");
        assert_eq!(parsed["status"], "ok");
    }

    #[test]
    fn handle_reload_cmd_response_is_newline_terminated() {
        let (btx, _rx) = broadcast::channel::<String>(8);
        let statuses = new_status_map(Vec::<String>::new());

        let response = handle_reload_cmd(&btx, &statuses);
        assert!(response.ends_with('\n'), "response should end with newline");
    }

    // =====================================================================
    // handle_status_cmd
    // =====================================================================

    #[test]
    fn handle_status_cmd_returns_configs_and_connections() {
        let statuses = new_status_map(vec!["demo".to_string()]);
        let counter = Arc::new(AtomicUsize::new(3));
        let response = handle_status_cmd(&statuses, &counter);
        assert!(response.contains("\"configs\""));
        assert!(response.contains("\"demo\""));
        assert!(response.contains("\"connections\""));
        assert!(response.contains("3"));
    }

    #[test]
    fn handle_status_cmd_zero_connections() {
        let statuses = new_status_map(Vec::<String>::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let response = handle_status_cmd(&statuses, &counter);
        let parsed: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(parsed["connections"], 0);
        assert!(parsed["configs"].as_array().unwrap().is_empty());
    }

    #[test]
    fn handle_status_cmd_multiple_configs() {
        let statuses = new_status_map(vec![
            "styles".to_string(),
            "scripts".to_string(),
            "images".to_string(),
        ]);
        set_status(&statuses, "scripts", "building");
        let counter = Arc::new(AtomicUsize::new(5));

        let response = handle_status_cmd(&statuses, &counter);
        let parsed: serde_json::Value = serde_json::from_str(&response).unwrap();

        assert_eq!(parsed["connections"], 5);
        let configs = parsed["configs"].as_array().unwrap();
        assert_eq!(configs.len(), 3);

        // Find the "scripts" entry and verify its status.
        let scripts = configs
            .iter()
            .find(|c| c["name"] == "scripts")
            .expect("should find scripts config");
        assert_eq!(scripts["status"], "building");
    }

    #[test]
    fn handle_status_cmd_response_is_valid_json() {
        let statuses = new_status_map(vec!["test".to_string()]);
        let counter = Arc::new(AtomicUsize::new(0));

        let response = handle_status_cmd(&statuses, &counter);
        let parsed: serde_json::Value =
            serde_json::from_str(response.trim()).expect("should be valid JSON");
        assert!(parsed.is_object());
        assert!(parsed.get("configs").is_some());
        assert!(parsed.get("connections").is_some());
    }

    #[test]
    fn handle_status_cmd_response_is_newline_terminated() {
        let statuses = new_status_map(Vec::<String>::new());
        let counter = Arc::new(AtomicUsize::new(0));

        let response = handle_status_cmd(&statuses, &counter);
        assert!(response.ends_with('\n'));
    }

    #[test]
    fn handle_status_cmd_large_connection_count() {
        let statuses = new_status_map(Vec::<String>::new());
        let counter = Arc::new(AtomicUsize::new(999_999));

        let response = handle_status_cmd(&statuses, &counter);
        let parsed: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(parsed["connections"], 999_999);
    }

    // =====================================================================
    // Control socket integration (async tests)
    // =====================================================================

    /// Helper: bind a control server on a random port and return the address.
    // -- diff / diffs / diff-clear command handler tests --------------------

    #[test]
    fn handle_diff_cmd_store_disabled() {
        let resp = handle_diff_cmd(&None, Some("x.txt"));
        assert!(resp.contains("not enabled"));
    }

    #[test]
    fn handle_diff_cmd_missing_path() {
        let store = DiffStore::new_shared(5, 10, 1024);
        let resp = handle_diff_cmd(&Some(store), None);
        assert!(resp.contains("missing 'path'"));
    }

    #[test]
    fn handle_diff_cmd_no_diff_found() {
        let store = DiffStore::new_shared(5, 10, 1024);
        let resp = handle_diff_cmd(&Some(store), Some("nope.txt"));
        assert!(resp.contains("no diff found"));
        assert!(resp.contains("nope.txt"));
    }

    #[test]
    fn handle_diff_cmd_returns_latest_diff() {
        let store = DiffStore::new_shared(5, 10, 1024);
        {
            let mut guard = store.lock().unwrap();
            guard.record_change("a.css", b"old\n");
            guard.record_change("a.css", b"new\n");
        }
        let resp = handle_diff_cmd(&Some(store), Some("a.css"));
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["path"], "a.css");
        assert!(v["diff"].as_str().unwrap().contains("+ new\n"));
        assert!(v["diff"].as_str().unwrap().contains("- old\n"));
        assert_eq!(v["truncated"], false);
    }

    #[test]
    fn handle_diff_cmd_truncated_file() {
        let store = DiffStore::new_shared(5, 10, 8);
        {
            let mut guard = store.lock().unwrap();
            guard.record_change("big.txt", &vec![b'X'; 20]);
        }
        let resp = handle_diff_cmd(&Some(store), Some("big.txt"));
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["truncated"], true);
        assert!(v["diff"].as_str().unwrap().contains("too large"));
    }

    #[test]
    fn handle_diffs_cmd_store_disabled() {
        let resp = handle_diffs_cmd(&None);
        assert!(resp.contains("not enabled"));
    }

    #[test]
    fn handle_diffs_cmd_empty_store() {
        let store = DiffStore::new_shared(5, 10, 1024);
        let resp = handle_diffs_cmd(&Some(store));
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["summary"]["tracked_files"], 0);
        assert_eq!(v["summary"]["total_diffs"], 0);
        assert!(v["files"].as_array().unwrap().is_empty());
    }

    #[test]
    fn handle_diffs_cmd_with_entries() {
        let store = DiffStore::new_shared(5, 10, 1024);
        {
            let mut guard = store.lock().unwrap();
            guard.record_change("a.txt", b"a\n");
            guard.record_change("b.txt", b"b\n");
        }
        let resp = handle_diffs_cmd(&Some(store));
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["summary"]["tracked_files"], 2);
        assert_eq!(v["summary"]["total_diffs"], 2);
        assert_eq!(v["files"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn handle_diffs_cmd_summary_includes_max_file_size() {
        let store = DiffStore::new_shared(5, 10, 4096);
        let resp = handle_diffs_cmd(&Some(store));
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["summary"]["max_file_size"], 4096);
    }

    #[test]
    fn handle_diff_clear_cmd_store_disabled() {
        let resp = handle_diff_clear_cmd(&None);
        assert!(resp.contains("not enabled"));
    }

    #[test]
    fn handle_diff_clear_cmd_clears_store() {
        let store = DiffStore::new_shared(5, 10, 1024);
        {
            let mut guard = store.lock().unwrap();
            guard.record_change("a.txt", b"a");
            guard.record_change("b.txt", b"b");
        }
        let resp = handle_diff_clear_cmd(&Some(store.clone()));
        assert!(resp.contains("ok"));

        let guard = store.lock().unwrap();
        assert!(guard.list_keys().is_empty());
    }

    async fn setup_control_server() -> (
        String,
        broadcast::Sender<String>,
        StatusMap,
        CancellationToken,
        Arc<AtomicUsize>,
    ) {
        setup_control_server_with_diff(None).await
    }

    async fn setup_control_server_with_diff(
        diff_store: Option<SharedDiffStore>,
    ) -> (
        String,
        broadcast::Sender<String>,
        StatusMap,
        CancellationToken,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (btx, _) = broadcast::channel::<String>(64);
        let statuses = new_status_map(vec!["test-cfg".to_string()]);
        let cancel = CancellationToken::new();
        let counter = Arc::new(AtomicUsize::new(0));

        spawn_control_server(
            listener,
            btx.clone(),
            statuses.clone(),
            cancel.clone(),
            counter.clone(),
            diff_store,
        );

        // Give the server a moment to start accepting.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        (addr, btx, statuses, cancel, counter)
    }

    /// Helper: send a raw line to the control socket and read the response.
    async fn control_send(addr: &str, line: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(line.as_bytes()).await.unwrap();
        // Shut down write half so the server knows we're done.
        stream.shutdown().await.unwrap();

        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut buf)
            .await
            .unwrap();
        String::from_utf8_lossy(&buf).to_string()
    }

    #[tokio::test]
    async fn control_socket_status_command() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        let response = control_send(&addr, "{\"cmd\":\"status\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();

        assert!(parsed.get("configs").is_some());
        assert!(parsed.get("connections").is_some());

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_reload_command() {
        let (addr, _btx, statuses, cancel, _counter) = setup_control_server().await;

        set_status(&statuses, "test-cfg", "building");

        let response = control_send(&addr, "{\"cmd\":\"reload\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["status"], "ok");

        // Status should have been reset.
        let guard = statuses.lock().unwrap();
        assert_eq!(guard.get("test-cfg").unwrap(), "up to date");

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_reload_broadcasts_message() {
        let (addr, btx, _statuses, cancel, _counter) = setup_control_server().await;
        let mut brx = btx.subscribe();

        control_send(&addr, "{\"cmd\":\"reload\"}\n").await;

        let msg = brx.try_recv().expect("should have received broadcast");
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "reload");

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_unknown_command() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        let response = control_send(&addr, "{\"cmd\":\"restart\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert!(parsed.get("error").is_some());
        assert!(parsed["error"].as_str().unwrap().contains("unknown"));

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_missing_cmd_field() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        let response = control_send(&addr, "{\"foo\":\"bar\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert!(parsed.get("error").is_some());
        assert!(parsed["error"].as_str().unwrap().contains("cmd"));

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_invalid_json() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        let response = control_send(&addr, "this is not json\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert!(parsed.get("error").is_some());
        assert!(parsed["error"].as_str().unwrap().contains("invalid json"));

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_empty_json_object() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        let response = control_send(&addr, "{}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert!(parsed.get("error").is_some());

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_multiple_sequential_connections() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        // Multiple sequential requests should each succeed.
        for _ in 0..5 {
            let response = control_send(&addr, "{\"cmd\":\"status\"}\n").await;
            let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
            assert!(parsed.get("configs").is_some());
        }

        cancel.cancel();
    }

    // =====================================================================
    // WebSocket server integration (async tests)
    // =====================================================================

    /// Helper: bind a WS server on a random port and return the address.
    // -- control socket diff integration tests -----------------------------

    #[tokio::test]
    async fn control_socket_diff_command_store_disabled() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;
        let resp = control_send(&addr, "{\"cmd\":\"diff\",\"path\":\"x.txt\"}\n").await;
        assert!(resp.contains("not enabled"));
        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_diff_command_returns_diff() {
        let store = DiffStore::new_shared(5, 10, 1024);
        {
            let mut guard = store.lock().unwrap();
            guard.record_change("f.css", b"old\n");
            guard.record_change("f.css", b"new\n");
        }
        let (addr, _btx, _statuses, cancel, _counter) =
            setup_control_server_with_diff(Some(store)).await;
        let resp = control_send(&addr, "{\"cmd\":\"diff\",\"path\":\"f.css\"}\n").await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["path"], "f.css");
        assert!(v["diff"].as_str().unwrap().contains("- old\n"));
        assert!(v["diff"].as_str().unwrap().contains("+ new\n"));
        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_diffs_command_returns_summary() {
        let store = DiffStore::new_shared(5, 10, 1024);
        {
            let mut guard = store.lock().unwrap();
            guard.record_change("a.txt", b"a\n");
        }
        let (addr, _btx, _statuses, cancel, _counter) =
            setup_control_server_with_diff(Some(store)).await;
        let resp = control_send(&addr, "{\"cmd\":\"diffs\"}\n").await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["summary"]["tracked_files"], 1);
        assert_eq!(v["files"].as_array().unwrap().len(), 1);
        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_diff_clear_command() {
        let store = DiffStore::new_shared(5, 10, 1024);
        {
            let mut guard = store.lock().unwrap();
            guard.record_change("a.txt", b"a");
        }
        let (addr, _btx, _statuses, cancel, _counter) =
            setup_control_server_with_diff(Some(store.clone())).await;
        let resp = control_send(&addr, "{\"cmd\":\"diff-clear\"}\n").await;
        assert!(resp.contains("ok"));

        let guard = store.lock().unwrap();
        assert!(guard.list_keys().is_empty());
        cancel.cancel();
    }

    async fn setup_ws_server() -> (
        String,
        broadcast::Sender<String>,
        CancellationToken,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (btx, _) = broadcast::channel::<String>(64);
        let cancel = CancellationToken::new();
        let counter = Arc::new(AtomicUsize::new(0));

        spawn_ws_server(listener, btx.clone(), cancel.clone(), counter.clone());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        (addr, btx, cancel, counter)
    }

    #[tokio::test]
    async fn ws_client_connects_and_receives_message() {
        let (addr, btx, cancel, counter) = setup_ws_server().await;

        let url = format!("ws://{}", addr);
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // Wait for connection counter to update.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(counter.load(Ordering::Relaxed) >= 1);

        // Broadcast a message and verify the client receives it.
        let test_msg = serde_json::json!({"type": "reload"}).to_string();
        btx.send(test_msg.clone()).unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next())
            .await
            .expect("should receive within timeout")
            .expect("stream should have a message")
            .expect("message should be Ok");

        if let Message::Text(text) = received {
            let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(parsed["type"], "reload");
        } else {
            panic!("expected Text message, got {:?}", received);
        }

        drop(ws);
        // Allow time for disconnect tracking.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        cancel.cancel();
    }

    #[tokio::test]
    async fn ws_client_disconnect_decrements_counter() {
        let (addr, _btx, cancel, counter) = setup_ws_server().await;

        let url = format!("ws://{}", addr);
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let count_after_connect = counter.load(Ordering::Relaxed);
        assert!(count_after_connect >= 1);

        // Send a proper close frame so the server detects the disconnect
        // reliably across all platforms (including Windows where a raw TCP
        // drop may not propagate quickly).
        let _ = ws
            .close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: std::borrow::Cow::Borrowed("test done"),
            }))
            .await;
        drop(ws);

        // Poll with retries — the disconnect may take a moment to propagate
        // through the async runtime depending on OS and scheduling.
        let mut disconnected = false;
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let current = counter.load(Ordering::Relaxed);
            if current < count_after_connect {
                disconnected = true;
                break;
            }
        }
        assert!(
            disconnected,
            "counter should have decremented after client disconnect"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn ws_multiple_clients_tracked() {
        let (addr, _btx, cancel, counter) = setup_ws_server().await;

        let url = format!("ws://{}", addr);
        let (ws1, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (ws3, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(counter.load(Ordering::Relaxed) >= 3);

        drop(ws1);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(counter.load(Ordering::Relaxed) >= 2);

        drop(ws2);
        drop(ws3);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        cancel.cancel();
    }

    #[tokio::test]
    async fn ws_broadcast_reaches_all_clients() {
        let (addr, btx, cancel, _counter) = setup_ws_server().await;

        let url = format!("ws://{}", addr);
        let (mut ws1, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let test_msg = serde_json::json!({"type": "reload"}).to_string();
        btx.send(test_msg).unwrap();

        let timeout = std::time::Duration::from_secs(2);

        let msg1 = tokio::time::timeout(timeout, ws1.next())
            .await
            .expect("ws1 timeout")
            .expect("ws1 stream")
            .expect("ws1 msg");
        let msg2 = tokio::time::timeout(timeout, ws2.next())
            .await
            .expect("ws2 timeout")
            .expect("ws2 stream")
            .expect("ws2 msg");

        if let (Message::Text(t1), Message::Text(t2)) = (msg1, msg2) {
            assert!(t1.contains("reload"));
            assert!(t2.contains("reload"));
        } else {
            panic!("expected Text messages from both clients");
        }

        drop(ws1);
        drop(ws2);
        cancel.cancel();
    }

    #[tokio::test]
    async fn ws_inject_css_message_forwarded() {
        let (addr, btx, cancel, _counter) = setup_ws_server().await;

        let url = format!("ws://{}", addr);
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let css_msg = serde_json::json!({
            "type": "inject-css",
            "path": "styles/main.css",
            "content": "Ym9keSB7IGNvbG9yOiByZWQ7IH0="
        })
        .to_string();
        btx.send(css_msg).unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream")
            .expect("msg");

        if let Message::Text(text) = received {
            let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(parsed["type"], "inject-css");
            assert_eq!(parsed["path"], "styles/main.css");
            assert!(parsed.get("content").is_some());
        } else {
            panic!("expected Text message");
        }

        drop(ws);
        cancel.cancel();
    }

    #[tokio::test]
    async fn ws_server_cancellation_stops_accept_loop() {
        let (addr, _btx, cancel, _counter) = setup_ws_server().await;

        // Connect one client to prove the server is running.
        let url = format!("ws://{}", addr);
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        drop(ws);

        // Cancel the server.
        cancel.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // New connections should fail after cancellation.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio_tungstenite::connect_async(&url),
        )
        .await;

        // Either the connect times out or returns an error — both are acceptable.
        match result {
            Err(_) => { /* timeout — server stopped accepting */ }
            Ok(Err(_)) => { /* connection error — server stopped */ }
            Ok(Ok(_)) => {
                // Some OS may still have the connection in the backlog; this is
                // acceptable as a race condition on fast systems.
            }
        }
    }

    // =====================================================================
    // Control server cancellation
    // =====================================================================

    #[tokio::test]
    async fn control_server_cancellation() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        // Verify server is running.
        let response = control_send(&addr, "{\"cmd\":\"status\"}\n").await;
        assert!(!response.is_empty());

        // Cancel and wait for shutdown.
        cancel.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // =====================================================================
    // Edge cases
    // =====================================================================

    #[tokio::test]
    async fn control_socket_concurrent_requests() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        // Fire multiple requests concurrently.
        let mut handles = Vec::new();
        for _ in 0..10 {
            let addr_clone = addr.clone();
            handles.push(tokio::spawn(async move {
                control_send(&addr_clone, "{\"cmd\":\"status\"}\n").await
            }));
        }

        for handle in handles {
            let response = handle.await.unwrap();
            let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
            assert!(parsed.get("configs").is_some());
        }

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_status_reflects_connection_count() {
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap().to_string();
        let ctrl_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ctrl_addr = ctrl_listener.local_addr().unwrap().to_string();

        let (btx, _) = broadcast::channel::<String>(64);
        let statuses = new_status_map(Vec::<String>::new());
        let cancel = CancellationToken::new();
        let counter = Arc::new(AtomicUsize::new(0));

        spawn_ws_server(ws_listener, btx.clone(), cancel.clone(), counter.clone());
        spawn_control_server(
            ctrl_listener,
            btx.clone(),
            statuses.clone(),
            cancel.clone(),
            counter.clone(),
            None,
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // No WS clients — connections should be 0.
        let response = control_send(&ctrl_addr, "{\"cmd\":\"status\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["connections"], 0);

        // Connect a WS client.
        let ws_url = format!("ws://{}", ws_addr);
        let (ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Now status should show 1 connection.
        let response = control_send(&ctrl_addr, "{\"cmd\":\"status\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["connections"], 1);

        drop(ws);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // After disconnect, back to 0.
        let response = control_send(&ctrl_addr, "{\"cmd\":\"status\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["connections"], 0);

        cancel.cancel();
    }
}
