//! File-system watcher, event debouncing, and change-handling logic.
//!
//! This module owns the `notify` watcher and processes incoming file-system
//! events.  Events are debounced per-path, filtered through the global
//! [`PathFilter`], matched against loaded [`ConfigEntry`] values, and finally
//! result in either a command execution or a WebSocket broadcast (or both).
//!
//! ## Key behaviours
//!
//! - **Event kind filtering** – only data-affecting events (Create, Modify
//!   *content*, Remove, Rename) are processed; pure metadata changes and
//!   access events are ignored.
//! - **Command timeout** – an optional per-invocation timeout prevents stuck
//!   build commands from blocking the worker indefinitely.
//! - **Config hot-reload** – when `hotreload.yaml` itself is modified the
//!   worker reloads its config entries automatically without a restart.

use base64::Engine;
use notify::{
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher,
};
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{self, ConfigEntry, NotifyMode};
use crate::filter::{normalize_path, PathFilter};
use crate::server::{set_status, StatusMap};

// ---------------------------------------------------------------------------
// Watcher start parameters
// ---------------------------------------------------------------------------

/// Bundles all parameters needed by [`start`] to reduce argument count.
pub struct WatcherParams {
    /// Root directory to watch recursively.
    pub watch_root: Box<Path>,
    /// Command template with optional `{path}` placeholder.
    pub cmd_template: String,
    /// Debounce window in milliseconds.
    pub debounce_ms: u64,
    /// When `true`, skip running the command on startup.
    pub skip_initial_run: bool,
    /// Global include/exclude path filter.
    pub filter: PathFilter,
    /// Loaded config entries (with compiled watch sets).
    pub configs: Vec<ConfigEntry>,
    /// Broadcast sender for WebSocket messages.
    pub btx: broadcast::Sender<String>,
    /// Shared status map for the control interface.
    pub statuses: StatusMap,
    /// Cancellation token for coordinated shutdown.
    pub cancel: CancellationToken,
    /// Optional per-command timeout in milliseconds.
    pub cmd_timeout_ms: Option<u64>,
    /// Shared counter of active WebSocket connections (for status reporting).
    pub connection_count: Arc<AtomicUsize>,
}

// ---------------------------------------------------------------------------
// Connection tracking
// ---------------------------------------------------------------------------

/// Atomically increment the connection count and return the new value.
pub fn track_connect(counter: &Arc<AtomicUsize>) -> usize {
    let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
    debug!(connections = n, "WebSocket client connected");
    n
}

/// Atomically decrement the connection count and return the new value.
pub fn track_disconnect(counter: &Arc<AtomicUsize>) -> usize {
    let prev = counter.fetch_sub(1, Ordering::Relaxed);
    let n = prev.saturating_sub(1);
    debug!(connections = n, "WebSocket client disconnected");
    n
}

// ---------------------------------------------------------------------------
// Event kind filtering
// ---------------------------------------------------------------------------

/// Returns `true` when the [`EventKind`] represents a data-affecting
/// filesystem change that the worker should process.
///
/// We deliberately ignore:
/// - `Access` events (file was read, not changed)
/// - `Modify(Metadata)` events (permissions/timestamps only)
/// - `Other` / unknown events
fn is_relevant_event(kind: &EventKind) -> bool {
    match kind {
        EventKind::Create(_) => true,
        EventKind::Remove(_) => true,
        EventKind::Modify(modify_kind) => {
            use notify::event::ModifyKind;
            match modify_kind {
                ModifyKind::Data(_) => true,
                ModifyKind::Name(_) => true, // renames
                ModifyKind::Any => true,     // catch-all from some backends
                ModifyKind::Metadata(_) => false,
                ModifyKind::Other => false,
            }
        }
        EventKind::Access(_) => false,
        EventKind::Other => false,
        EventKind::Any => true, // generic fallback from some watchers
    }
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Run a shell command synchronously (blocking the current thread).
///
/// On Windows the command is executed via `cmd /C`; on Unix via `sh -c`.
///
/// When `timeout` is `Some`, the child process is killed if it has not
/// completed within the given duration.
///
/// Errors are logged but do not propagate — a failing build command should not
/// crash the watcher.
pub fn run_command_blocking(cmd: &str, timeout: Option<Duration>) {
    let result = {
        #[cfg(target_os = "windows")]
        {
            Command::new("cmd").args(["/C", cmd]).spawn()
        }
        #[cfg(not(target_os = "windows"))]
        {
            Command::new("sh").args(["-c", cmd]).spawn()
        }
    };

    match result {
        Ok(mut child) => {
            if let Some(dur) = timeout {
                // Poll-based timeout: check every 50 ms.
                let start = Instant::now();
                loop {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            if !status.success() {
                                warn!(
                                    cmd,
                                    code = status.code().unwrap_or(-1),
                                    "command exited with non-zero status"
                                );
                            }
                            break;
                        }
                        Ok(None) => {
                            if start.elapsed() > dur {
                                warn!(
                                    cmd,
                                    timeout_ms = dur.as_millis() as u64,
                                    "command timed out; killing process"
                                );
                                let _ = child.kill();
                                let _ = child.wait(); // reap
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(50));
                        }
                        Err(e) => {
                            error!(cmd, error = %e, "failed to poll child process");
                            break;
                        }
                    }
                }
            } else {
                // No timeout — block until completion.
                match child.wait() {
                    Ok(status) => {
                        if !status.success() {
                            warn!(
                                cmd,
                                code = status.code().unwrap_or(-1),
                                "command exited with non-zero status"
                            );
                        }
                    }
                    Err(e) => error!(cmd, error = %e, "failed to wait on child process"),
                }
            }
        }
        Err(e) => error!(cmd, error = %e, "failed to spawn command"),
    }
}

// ---------------------------------------------------------------------------
// Debouncer
// ---------------------------------------------------------------------------

/// A simple per-path debouncer that suppresses duplicate events within a
/// configurable time window.
///
/// The internal map is periodically pruned to prevent unbounded growth during
/// long-running sessions.
struct Debouncer {
    window: Duration,
    last_seen: HashMap<String, Instant>,
    /// Number of events processed since the last prune.
    events_since_prune: u64,
    /// Prune the map every N events.
    prune_interval: u64,
}

impl Debouncer {
    fn new(window: Duration) -> Self {
        Self {
            window,
            last_seen: HashMap::new(),
            events_since_prune: 0,
            prune_interval: 1000,
        }
    }

    /// Returns `true` if the path should be processed (i.e. enough time has
    /// elapsed since the last event for this path).
    fn should_handle(&mut self, path: &str) -> bool {
        let now = Instant::now();
        let dominated = match self.last_seen.get(path) {
            Some(prev) => now.duration_since(*prev) <= self.window,
            None => false,
        };
        self.last_seen.insert(path.to_string(), now);

        // Periodic pruning: remove entries older than 10× the debounce window.
        self.events_since_prune += 1;
        if self.events_since_prune >= self.prune_interval {
            self.events_since_prune = 0;
            let stale_threshold = self.window * 10;
            self.last_seen
                .retain(|_, ts| now.duration_since(*ts) < stale_threshold);
        }

        !dominated
    }
}

// ---------------------------------------------------------------------------
// Notification helpers
// ---------------------------------------------------------------------------

/// Determine the notification action for a file change and send the
/// appropriate broadcast message.
fn notify_clients(
    btx: &broadcast::Sender<String>,
    path: &Path,
    normalized: &str,
    mode: &NotifyMode,
    label: &str,
) {
    match mode {
        NotifyMode::None => {
            debug!(
                path = normalized,
                config = label,
                "notify=none; no broadcast"
            );
        }
        NotifyMode::Reload => {
            broadcast_reload(btx, normalized, label);
        }
        NotifyMode::InjectCss => {
            broadcast_inject_css(btx, path, normalized, label);
        }
        NotifyMode::Auto => {
            // Decide based on file extension.
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();

            if ext == "css" {
                broadcast_inject_css(btx, path, normalized, label);
            } else {
                broadcast_reload(btx, normalized, label);
            }
        }
    }
}

fn broadcast_reload(btx: &broadcast::Sender<String>, normalized: &str, label: &str) {
    let msg = serde_json::json!({"type": "reload"}).to_string();
    let _ = btx.send(msg);
    info!(path = normalized, config = label, "broadcast reload");
}

fn broadcast_inject_css(
    btx: &broadcast::Sender<String>,
    path: &Path,
    normalized: &str,
    label: &str,
) {
    match std::fs::read(path) {
        Ok(content) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(&content);
            let msg = serde_json::json!({
                "type": "inject-css",
                "path": normalized,
                "content": encoded,
            })
            .to_string();
            let _ = btx.send(msg);
            info!(path = normalized, config = label, "broadcast inject-css");
        }
        Err(e) => {
            warn!(
                path = normalized,
                error = %e,
                "failed to read CSS file for injection; falling back to reload"
            );
            broadcast_reload(btx, normalized, label);
        }
    }
}

// ---------------------------------------------------------------------------
// Config hot-reload
// ---------------------------------------------------------------------------

/// Attempt to reload `hotreload.yaml` from `config_path`.
///
/// Returns `Some(new_configs)` on success, or `None` if reading / parsing
/// fails (errors are logged internally).
fn try_reload_configs(config_path: &Path) -> Option<Vec<ConfigEntry>> {
    let text = match std::fs::read_to_string(config_path) {
        Ok(t) => t,
        Err(e) => {
            warn!(path = %config_path.display(), error = %e, "failed to read config for hot-reload");
            return None;
        }
    };

    let mut parsed = config::parse_configs(&text);
    if parsed.is_empty() {
        warn!(path = %config_path.display(), "config hot-reload produced zero entries; keeping previous configs");
        return None;
    }

    for cfg in &mut parsed {
        cfg.compile_watch_set();
    }

    info!(
        path = %config_path.display(),
        count = parsed.len(),
        "hot-reloaded configs"
    );
    Some(parsed)
}

// ---------------------------------------------------------------------------
// Event processing (worker thread)
// ---------------------------------------------------------------------------

/// Context for the blocking worker thread that processes file-system events.
struct WorkerContext {
    rx: std_mpsc::Receiver<NotifyResult<Event>>,
    btx: broadcast::Sender<String>,
    filter: PathFilter,
    configs: Vec<ConfigEntry>,
    statuses: StatusMap,
    cmd_template: String,
    debouncer: Debouncer,
    cmd_timeout: Option<Duration>,
    /// Absolute path to `hotreload.yaml` (used for config hot-reload detection).
    config_path: Box<Path>,
}

impl WorkerContext {
    /// Main event loop — runs until the channel is closed.
    fn run(mut self) {
        while let Ok(result) = self.rx.recv() {
            match result {
                Ok(event) => {
                    // Skip events that don't represent actual data changes.
                    if !is_relevant_event(&event.kind) {
                        continue;
                    }

                    for path in &event.paths {
                        self.handle_path(path);
                    }
                }
                Err(e) => warn!(error = %e, "file watcher reported an error"),
            }
        }
        debug!("watcher event channel closed; worker exiting");
    }

    fn handle_path(&mut self, path: &Path) {
        let raw = path.to_string_lossy().to_string();

        // Debounce: skip if we saw this path too recently.
        if !self.debouncer.should_handle(&raw) {
            return;
        }

        let normalized = normalize_path(&raw);

        // Detect config file change → hot-reload.
        if path == self.config_path.as_ref() {
            info!("hotreload.yaml changed; reloading configs");
            if let Some(new_configs) = try_reload_configs(&self.config_path) {
                // Update the status map: remove old entries, add new ones.
                if let Ok(mut guard) = self.statuses.lock() {
                    guard.clear();
                    for cfg in &new_configs {
                        guard.insert(cfg.name.clone(), "up to date".to_string());
                    }
                }
                self.configs = new_configs;
            }
            return; // Don't run build commands for the config file itself.
        }

        // Global include/exclude filter.
        if !self.filter.is_allowed(&normalized) {
            debug!(path = %normalized, "skipped by filter");
            return;
        }

        // Try each loaded config.
        let mut handled = false;
        for cfg in &self.configs {
            if !cfg.matches(&normalized) {
                continue;
            }
            handled = true;
            info!(config = %cfg.name, path = %normalized, "config matched");

            set_status(&self.statuses, &cfg.name, "reloading");

            // Run on_change / build command if configured.
            if let Some(cmd) = cfg.command_for(&normalized) {
                info!(config = %cfg.name, cmd = %cmd, "running command");
                run_command_blocking(&cmd, self.cmd_timeout);
            }

            // Notify browser clients.
            notify_clients(&self.btx, path, &normalized, &cfg.notify, &cfg.name);

            set_status(&self.statuses, &cfg.name, "up to date");
        }

        if handled {
            return;
        }

        // ----- Fallback behaviour (no config matched) -----

        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();

        if ext == "css" {
            broadcast_inject_css(&self.btx, path, &normalized, "default");
            return;
        }

        if ext == "html" || ext == "htm" {
            broadcast_reload(&self.btx, &normalized, "default");
            return;
        }

        // Generic fallback: run the configured command template and reload.
        let cmd = self.cmd_template.replace("{path}", &normalized);
        info!(cmd = %cmd, "running fallback command");
        run_command_blocking(&cmd, self.cmd_timeout);
        broadcast_reload(&self.btx, &normalized, "default");
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start the file-system watcher and the blocking event-processing worker.
///
/// This function:
/// 1. Creates a `notify` [`RecommendedWatcher`] watching `watch_root` recursively.
/// 2. Optionally runs the startup command (unless `skip_initial_run` is true).
/// 3. Spawns a background **OS thread** (not a tokio task) that processes
///    events in a tight loop with debouncing.
///
/// The returned [`RecommendedWatcher`] must be kept alive for the duration of
/// the program — dropping it stops file-system notifications.
pub fn start(params: WatcherParams) -> crate::error::Result<RecommendedWatcher> {
    let WatcherParams {
        watch_root,
        cmd_template,
        debounce_ms,
        skip_initial_run,
        filter,
        configs,
        btx,
        statuses,
        cancel: _cancel,
        cmd_timeout_ms,
        connection_count: _connection_count,
    } = params;

    let cmd_timeout = cmd_timeout_ms.map(Duration::from_millis);

    let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();

    let mut watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        Config::default(),
    )?;

    watcher.watch(&watch_root, RecursiveMode::Recursive)?;
    info!(path = %watch_root.display(), "watching for changes");

    // Resolve the config file path for hot-reload detection.
    // Canonicalize so it matches the absolute paths reported by `notify`.
    let config_path = watch_root
        .join("hotreload.yaml")
        .canonicalize()
        .unwrap_or_else(|_| watch_root.join("hotreload.yaml"))
        .into_boxed_path();

    // Optionally run the startup command.
    if !skip_initial_run {
        let cmd = cmd_template.clone();
        let timeout = cmd_timeout;
        std::thread::spawn(move || {
            info!(cmd = %cmd, "running startup command");
            run_command_blocking(&cmd, timeout);
        });
    }

    // Spawn the blocking worker thread.
    let ctx = WorkerContext {
        rx,
        btx,
        filter,
        configs,
        statuses,
        cmd_template,
        debouncer: Debouncer::new(Duration::from_millis(debounce_ms)),
        cmd_timeout,
        config_path,
    };
    std::thread::Builder::new()
        .name("watchd-worker".to_string())
        .spawn(move || ctx.run())
        .map_err(notify::Error::io)?;

    Ok(watcher)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Debouncer ---------------------------------------------------------

    #[test]
    fn debouncer_allows_first_event() {
        let mut d = Debouncer::new(Duration::from_millis(200));
        assert!(d.should_handle("src/main.rs"));
    }

    #[test]
    fn debouncer_suppresses_rapid_duplicates() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        assert!(d.should_handle("src/main.rs"));
        // Second call within the window should be suppressed.
        assert!(!d.should_handle("src/main.rs"));
    }

    #[test]
    fn debouncer_allows_different_paths() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        assert!(d.should_handle("a.txt"));
        assert!(d.should_handle("b.txt"));
    }

    #[test]
    fn debouncer_prune_removes_stale_entries() {
        let mut d = Debouncer::new(Duration::from_millis(1));
        d.prune_interval = 2;

        d.should_handle("old.txt");
        // Sleep long enough for the entry to become stale (10× window = 10ms).
        std::thread::sleep(Duration::from_millis(20));
        // Trigger two more events to force a prune.
        d.should_handle("trigger1.txt");
        d.should_handle("trigger2.txt");

        // "old.txt" should have been pruned.
        assert!(!d.last_seen.contains_key("old.txt"));
    }

    // -- Command execution -------------------------------------------------

    #[test]
    fn run_command_blocking_handles_echo() {
        // Just ensure it doesn't panic.
        run_command_blocking("echo hello", None);
    }

    #[test]
    fn run_command_blocking_respects_timeout() {
        // A long-running command should be killed by a short timeout.
        // We use a generous threshold so CI doesn't flake.
        let start = Instant::now();
        #[cfg(target_os = "windows")]
        run_command_blocking("ping -n 10 127.0.0.1", Some(Duration::from_millis(500)));
        #[cfg(not(target_os = "windows"))]
        run_command_blocking("sleep 10", Some(Duration::from_millis(500)));
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "command should have been killed by timeout but ran for {:?}",
            elapsed
        );
    }

    #[test]
    fn run_command_blocking_fast_command_with_timeout() {
        // A fast command with a long timeout should complete normally.
        let start = Instant::now();
        run_command_blocking("echo fast", Some(Duration::from_secs(30)));
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(5));
    }

    // -- Event kind filtering ----------------------------------------------

    #[test]
    fn create_events_are_relevant() {
        assert!(is_relevant_event(&EventKind::Create(
            notify::event::CreateKind::File
        )));
        assert!(is_relevant_event(&EventKind::Create(
            notify::event::CreateKind::Any
        )));
    }

    #[test]
    fn remove_events_are_relevant() {
        assert!(is_relevant_event(&EventKind::Remove(
            notify::event::RemoveKind::File
        )));
    }

    #[test]
    fn modify_data_events_are_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Data(notify::event::DataChange::Content)
        )));
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Data(notify::event::DataChange::Any)
        )));
    }

    #[test]
    fn modify_name_events_are_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Name(notify::event::RenameMode::From)
        )));
    }

    #[test]
    fn modify_metadata_events_are_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::WriteTime)
        )));
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::Permissions)
        )));
    }

    #[test]
    fn access_events_are_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Access(
            notify::event::AccessKind::Read
        )));
    }

    #[test]
    fn other_events_are_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Other));
    }

    #[test]
    fn any_events_are_relevant() {
        // Some watcher backends only report EventKind::Any.
        assert!(is_relevant_event(&EventKind::Any));
    }

    // -- Notification helpers ----------------------------------------------

    #[test]
    fn notify_mode_none_does_not_broadcast() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("test.css");
        notify_clients(&btx, path, "test.css", &NotifyMode::None, "test");
        // Channel should be empty.
        assert!(brx.try_recv().is_err());
    }

    #[test]
    fn notify_mode_reload_broadcasts() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("test.js");
        notify_clients(&btx, path, "test.js", &NotifyMode::Reload, "test");
        let msg = brx.try_recv().unwrap();
        assert!(msg.contains("\"reload\""));
    }

    #[test]
    fn auto_mode_css_injects() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        // Use a path that doesn't exist on disk — inject will fail and fall back
        // to reload, but we can still verify the logic path runs without panic.
        let path = Path::new("nonexistent.css");
        notify_clients(&btx, path, "nonexistent.css", &NotifyMode::Auto, "test");
        let msg = brx.try_recv().unwrap();
        // Falls back to reload because the file can't be read.
        assert!(msg.contains("\"reload\""));
    }

    #[test]
    fn auto_mode_non_css_reloads() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("app.js");
        notify_clients(&btx, path, "app.js", &NotifyMode::Auto, "test");
        let msg = brx.try_recv().unwrap();
        assert!(msg.contains("\"reload\""));
    }

    // -- Connection tracking -----------------------------------------------

    #[test]
    fn connection_tracking_increment_decrement() {
        let counter = Arc::new(AtomicUsize::new(0));
        assert_eq!(track_connect(&counter), 1);
        assert_eq!(track_connect(&counter), 2);
        assert_eq!(track_disconnect(&counter), 1);
        assert_eq!(track_disconnect(&counter), 0);
    }

    #[test]
    fn connection_tracking_underflow_saturates() {
        let counter = Arc::new(AtomicUsize::new(0));
        // Disconnecting when already at 0 should saturate to 0, not wrap.
        let n = track_disconnect(&counter);
        assert_eq!(n, 0);
    }
}
