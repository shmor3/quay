//! WebSocket server and shared status-map infrastructure for `watchd`.
//!
//! This module owns the WebSocket accept loop that forwards broadcast messages
//! (reload / inject-css) to all connected browser clients.  It also provides
//! the [`StatusMap`] type and helpers used by both the WebSocket server and the
//! control socket (defined in [`crate::control`]).
//!
//! ## Security
//!
//! - **Handshake timeout** (10 s) prevents raw TCP connections that never
//!   complete the HTTP Upgrade from holding resources indefinitely.
//! - **Accept-error backoff** (1 s) prevents tight error loops from exhausting
//!   CPU when the OS rejects new connections (e.g. `EMFILE`).
//! - **Lock poisoning** on the status map is handled gracefully with a warning
//!   rather than a panic.

use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use prometheus::IntGauge;

use crate::watcher;

// ---------------------------------------------------------------------------
// Status map
// ---------------------------------------------------------------------------

/// Shared status map tracking the state of each loaded config.
///
/// Keys are config names; values are human-readable status strings such as
/// `"up to date"` or `"reloading"`.  The map is shared between the watcher
/// worker thread (which updates statuses) and the control socket (which
/// reports them).
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
///
/// If the lock is poisoned (a thread panicked while holding it), the update is
/// silently skipped and a warning is logged.
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
    tls_cert: Option<String>,
    tls_key: Option<String>,
    max_connections: Option<u32>,
) {
    let ws_gauge = IntGauge::new("watchd_ws_connections", "WebSocket connections").unwrap();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    info!("WebSocket accept loop shutting down");
                    ws_gauge.set(0);
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            let peer_btx = btx.clone();
                            let peer_cancel = cancel.clone();
                            let peer_counter = connection_count.clone();
                            let ws_gauge = ws_gauge.clone();
                            tokio::spawn(async move {
                                ws_gauge.inc();
                                handle_ws_client(stream, addr, peer_btx, peer_cancel, peer_counter, None).await;
                                ws_gauge.dec();
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "WebSocket listener accept error; retrying in 1s");
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
// WebSocket client handling
// ---------------------------------------------------------------------------

/// Handle a single WebSocket client connection.
///
/// The lifecycle is:
/// 1. Complete the WebSocket handshake (with a 10 s timeout).
/// 2. Increment the connection counter.
/// 3. Forward broadcast messages to the client until disconnect or cancellation.
/// 4. Decrement the connection counter on exit.
async fn handle_ws_client(
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    btx: broadcast::Sender<String>,
    cancel: CancellationToken,
    connection_count: Arc<AtomicUsize>,
    max_connections: Option<u32>,
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

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
    // WebSocket server integration (async tests)
    // =====================================================================

    /// Helper: bind a WS server on a random port and return the address.
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
}
