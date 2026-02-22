//! TCP control socket for the `watchd` application.
//!
//! The control socket provides a simple JSON-over-TCP protocol so that CLI
//! subcommands (`watchd reload`, `watchd status`, `watchd diff`) can interact
//! with the running server without going through the WebSocket interface.
//!
//! ## Protocol
//!
//! Each connection sends a single newline-terminated JSON object and receives a
//! single newline-terminated JSON response.  The connection is closed after the
//! response is written.
//!
//! ### Supported commands
//!
//! | Command                              | Description                                    |
//! |--------------------------------------|------------------------------------------------|
//! | `{"cmd":"reload"}`                   | Broadcast a reload message, reset statuses     |
//! | `{"cmd":"status"}`                   | Return config statuses and connection count    |
//! | `{"cmd":"diff","path":"<path>"}`     | Return the latest diff for the given file      |
//! | `{"cmd":"diffs"}`                    | List all tracked files with summary statistics |
//! | `{"cmd":"diff-clear"}`               | Clear all entries from the diff store          |
//!
//! ## Security
//!
//! The control socket binds to `127.0.0.1` by default and is intended for
//! local-only access.  There is no authentication — any process on the machine
//! can send commands.  Do **not** bind to `0.0.0.0` in untrusted environments
//! without adding an authentication layer.
//!
//! ## Guard rails
//!
//! - **Read timeout** (5 s) prevents stalled clients from holding a socket open.
//! - **Size limit** ([`CONTROL_READ_MAX_BYTES`], 64 KiB) prevents malicious or
//!   buggy clients from sending multi-gigabyte payloads and causing OOM.
//! - **Lock poisoning** is handled gracefully — a poisoned mutex logs a warning
//!   and returns an internal-error JSON response rather than panicking.
//! - **Accept errors** (e.g. `EMFILE`) trigger a 1 s backoff and retry rather
//!   than crashing the server.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::kv::SharedDiffStore;
use crate::server::StatusMap;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of bytes the control socket will read from a single client
/// request before rejecting it.  This prevents a malicious or buggy client
/// from sending a multi-gigabyte line and causing OOM.
const CONTROL_READ_MAX_BYTES: usize = 64 * 1024; // 64 KiB

// ---------------------------------------------------------------------------
// Control socket server
// ---------------------------------------------------------------------------

/// Spawn the TCP control socket accept loop as a background task.
///
/// Supported JSON commands:
/// - `{"cmd": "reload"}` – broadcast a reload message and return `{"status":"ok"}`.
/// - `{"cmd": "status"}` – return the current config status map and connection count.
/// - `{"cmd": "diff", "path": "<path>"}` – return the latest diff for a file.
/// - `{"cmd": "diffs"}` – list all tracked files with a summary.
/// - `{"cmd": "diff-clear"}` – clear all entries from the diff store.
pub fn spawn_control_server(
    listener: TcpListener,
    btx: broadcast::Sender<String>,
    statuses: StatusMap,
    cancel: CancellationToken,
    connection_count: Arc<AtomicUsize>,
    diff_store: Option<SharedDiffStore>,
    auth_token: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    max_connections: Option<u32>,
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
                                handle_control_client(socket, btx, statuses, cancel, counter, diff_store, None, None).await;
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "control socket accept error; retrying in 1s");
                            // Sleep briefly to avoid a tight error loop (e.g. EMFILE),
                            // then continue accepting instead of killing the server.
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Client handling
// ---------------------------------------------------------------------------

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
    auth_token: Option<String>,
    max_connections: Option<u32>,
) {
    let mut buf: Vec<u8> = Vec::with_capacity(256);

    // Read with a timeout to protect against slow/malicious clients.
    // Also cap the total bytes read to CONTROL_READ_MAX_BYTES to prevent OOM.
    let read_result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        // Wrap the socket in `take()` to enforce an upper bound on bytes read,
        // preventing a client from sending a multi-gigabyte line before '\n'.
        let limited = (&mut socket).take(CONTROL_READ_MAX_BYTES as u64);
        let mut reader = BufReader::new(limited);
        let n = reader.read_until(b'\n', &mut buf).await?;
        // If we hit the byte limit without finding '\n', reject the request.
        if n >= CONTROL_READ_MAX_BYTES && !buf.ends_with(b"\n") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request too large",
            ));
        }
        Ok(n)
    })
    .await;

    match read_result {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::InvalidData => {
            debug!("control socket request exceeded size limit");
            let _ = socket
                .write_all(b"{\"error\":\"request too large\"}\n")
                .await;
            return;
        }
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

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

/// Process a `reload` command: broadcast a reload message and update statuses.
fn handle_reload_cmd(btx: &broadcast::Sender<String>, statuses: &StatusMap) -> String {
    let _ = btx.send(serde_json::json!({"type": "reload"}).to_string());

    if let Ok(mut guard) = statuses.lock() {
        for value in guard.values_mut() {
            *value = "up to date".to_string();
        }
    } else {
        warn!("status map lock poisoned during reload; statuses not reset");
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
    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_to_end(&mut buf),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            eprintln!("warning: error reading response: {e}");
        }
        Err(_) => {
            eprintln!("warning: timed out reading response after 10s");
        }
    }
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
    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_to_end(&mut buf),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            eprintln!("warning: error reading response: {e}");
        }
        Err(_) => {
            eprintln!("warning: timed out reading response after 10s");
        }
    }
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
    use crate::server::{new_status_map, set_status, spawn_ws_server};
    use tokio::io::AsyncWriteExt;

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
    // handle_diff_cmd
    // =====================================================================

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
            guard.record_change("big.txt", &[b'X'; 20]);
        }
        let resp = handle_diff_cmd(&Some(store), Some("big.txt"));
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["truncated"], true);
        assert!(v["diff"].as_str().unwrap().contains("too large"));
    }

    // =====================================================================
    // handle_diffs_cmd
    // =====================================================================

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

    // =====================================================================
    // handle_diff_clear_cmd
    // =====================================================================

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

    // =====================================================================
    // Control socket integration (async tests)
    // =====================================================================

    /// Helper: bind a control server on a random port and return the address.
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
    ///
    /// Uses tolerant error handling on `shutdown` and `read_to_end` because
    /// on Windows the server may reset the connection before the client
    /// finishes reading, especially under high concurrency.
    async fn control_send(addr: &str, line: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(line.as_bytes()).await.unwrap();
        // Shut down write half so the server knows we're done.
        // Ignore errors — the server may have already closed the connection.
        let _ = stream.shutdown().await;

        let mut buf = Vec::new();
        // read_to_end may fail with ConnectionReset on Windows if the server
        // closes the socket before we finish reading.  That's fine — we just
        // use whatever bytes we managed to read.
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut buf),
        )
        .await;
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

        // Poll until the counter reaches 1 (connect is detected).
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if counter.load(Ordering::Relaxed) == 1 {
                break;
            }
        }

        // Now status should show 1 connection.
        let response = control_send(&ctrl_addr, "{\"cmd\":\"status\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["connections"], 1);

        drop(ws);

        // Poll until the counter drops back to 0 (disconnect is detected).
        // Use a generous timeout (up to 5 s) to avoid flakiness under load.
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if counter.load(Ordering::Relaxed) == 0 {
                break;
            }
        }

        // After disconnect, back to 0.
        let response = control_send(&ctrl_addr, "{\"cmd\":\"status\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert_eq!(parsed["connections"], 0);

        cancel.cancel();
    }

    // -- Control socket size limit -----------------------------------------

    #[tokio::test]
    async fn control_socket_rejects_oversized_request() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        // Send a request that exceeds CONTROL_READ_MAX_BYTES (64 KiB) without
        // a newline terminator.  The server should reject it rather than OOM.
        // We send in chunks to avoid OS send-buffer back-pressure issues.
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let chunk = vec![b'X'; 16 * 1024]; // 16 KiB per chunk
        for _ in 0..8 {
            // 8 × 16 KiB = 128 KiB total
            if stream.write_all(&chunk).await.is_err() {
                // Server may have closed the connection early — that's fine,
                // it means it rejected the oversized request.
                break;
            }
        }

        // Give the server time to process and respond.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Shut down write half so read_to_end completes.
        let _ = stream.shutdown().await;

        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_to_end(&mut buf),
        )
        .await;

        let response = String::from_utf8_lossy(&buf);
        // The server should respond with an error (either "request too large"
        // or "invalid json" if it read a partial chunk), OR the connection may
        // have been reset.  The key property is that the server does NOT hang,
        // crash, or consume unbounded memory.
        //
        // On some platforms the server closes the connection before we read,
        // yielding an empty response — that is also acceptable.
        if !response.is_empty() {
            assert!(
                response.contains("error"),
                "expected error response for oversized request, got: {response}"
            );
        }

        // Verify the server is still alive by sending a normal request.
        let followup = control_send(&addr, "{\"cmd\":\"status\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(followup.trim()).unwrap();
        assert!(
            parsed.get("configs").is_some(),
            "server should still be functional after rejecting oversized request"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn control_socket_accepts_request_under_limit() {
        let (addr, _btx, _statuses, cancel, _counter) = setup_control_server().await;

        // A normal-sized request should still work fine.
        let response = control_send(&addr, "{\"cmd\":\"status\"}\n").await;
        let parsed: serde_json::Value = serde_json::from_str(response.trim()).unwrap();
        assert!(parsed.get("configs").is_some());

        cancel.cancel();
    }
}
