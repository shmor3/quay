//! WebSocket server and TCP control socket for the `watchd` application.
//!
//! The WebSocket server accepts browser clients and forwards broadcast messages
//! (reload / inject-css) to all connected peers.
//!
//! The control socket provides a simple JSON-over-TCP protocol so that CLI
//! subcommands (`watchd reload`, `watchd status`) can interact with the running
//! server.

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
    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            debug!(error = %e, %addr, "WebSocket handshake failed");
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
                            tokio::spawn(async move {
                                handle_control_client(socket, btx, statuses, cancel, counter).await;
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

/// Send a reload request to the control socket and print the response.
pub async fn send_reload(control_addr: &str) -> std::result::Result<(), String> {
    send_control_command(control_addr, "reload").await
}

/// Send a status request to the control socket and print the response.
pub async fn send_status(control_addr: &str) -> std::result::Result<(), String> {
    send_control_command(control_addr, "status").await
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

    #[test]
    fn new_status_map_populates_entries() {
        let map = new_status_map(vec!["a".to_string(), "b".to_string()]);
        let guard = map.lock().unwrap();
        assert_eq!(guard.get("a").unwrap(), "up to date");
        assert_eq!(guard.get("b").unwrap(), "up to date");
        assert_eq!(guard.len(), 2);
    }

    #[test]
    fn set_status_updates_entry() {
        let map = new_status_map(vec!["cfg".to_string()]);
        set_status(&map, "cfg", "reloading");
        let guard = map.lock().unwrap();
        assert_eq!(guard.get("cfg").unwrap(), "reloading");
    }

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
}
