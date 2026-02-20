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
//! - **Worker resilience** – the worker thread catches panics and logs them
//!   rather than crashing silently, and tracks event-processing statistics
//!   for observability.

use base64::Engine;
use notify::{
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher,
};
use std::collections::HashMap;
use std::panic;
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
///
/// Uses a compare-and-swap loop to avoid wrapping past zero — if the counter
/// is already `0` it stays at `0` instead of wrapping to `usize::MAX`.
pub fn track_disconnect(counter: &Arc<AtomicUsize>) -> usize {
    loop {
        let current = counter.load(Ordering::Relaxed);
        if current == 0 {
            debug!(
                connections = 0,
                "WebSocket client disconnected (counter already at zero)"
            );
            return 0;
        }
        let new_val = current - 1;
        match counter.compare_exchange_weak(current, new_val, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => {
                debug!(connections = new_val, "WebSocket client disconnected");
                return new_val;
            }
            Err(_) => {
                // Another thread changed the counter; retry.
                continue;
            }
        }
    }
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
    /// Create a new debouncer with the given window duration.
    ///
    /// A minimum window of 1 ms is enforced — a zero-duration window would
    /// effectively disable debouncing and could cause the worker to process
    /// the same event multiple times from backends that emit bursts.
    fn new(window: Duration) -> Self {
        let window = if window.is_zero() {
            warn!("debounce window is 0ms; clamping to 1ms minimum");
            Duration::from_millis(1)
        } else {
            window
        };
        Self {
            window,
            last_seen: HashMap::new(),
            events_since_prune: 0,
            prune_interval: 1000,
        }
    }

    /// Returns `true` if the path should be processed (i.e. enough time has
    /// elapsed since the last event for this path).
    ///
    /// Internally the map is pruned every [`prune_interval`] events to prevent
    /// unbounded growth during long-running sessions.  Entries older than 10×
    /// the debounce window are considered stale and removed.
    fn should_handle(&mut self, path: &str) -> bool {
        let now = Instant::now();
        let dominated = match self.last_seen.get(path) {
            Some(prev) => now.duration_since(*prev) <= self.window,
            None => false,
        };
        self.last_seen.insert(path.to_string(), now);

        // Periodic pruning: remove entries older than 10× the debounce window
        // so that the map does not grow without bound in sessions that touch
        // many distinct paths over hours/days.
        self.events_since_prune += 1;
        if self.events_since_prune >= self.prune_interval {
            self.events_since_prune = 0;
            let stale_threshold = self.window.saturating_mul(10);
            let before = self.last_seen.len();
            self.last_seen
                .retain(|_, ts| now.duration_since(*ts) < stale_threshold);
            let pruned = before.saturating_sub(self.last_seen.len());
            if pruned > 0 {
                debug!(
                    pruned,
                    remaining = self.last_seen.len(),
                    "debouncer pruned stale entries"
                );
            }
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
    ///
    /// Each incoming event is wrapped in [`panic::catch_unwind`] so that a bug
    /// in a single event handler (e.g. a malformed path triggering an
    /// unexpected panic) does not kill the entire worker thread.  The worker
    /// continues processing subsequent events after logging the panic.
    ///
    /// Statistics (total events received, events processed, errors, panics)
    /// are logged on exit for post-mortem observability.
    fn run(mut self) {
        let mut total_received: u64 = 0;
        let mut total_processed: u64 = 0;
        let mut total_errors: u64 = 0;
        let mut total_panics: u64 = 0;

        while let Ok(result) = self.rx.recv() {
            total_received += 1;

            match result {
                Ok(event) => {
                    // Skip events that don't represent actual data changes.
                    if !is_relevant_event(&event.kind) {
                        continue;
                    }

                    for path in event.paths.iter() {
                        // Catch panics so that one bad path doesn't kill the
                        // worker.  We use AssertUnwindSafe because our mutable
                        // borrow cannot cross the unwind boundary without it,
                        // and we accept the (tiny) risk that internal state is
                        // left slightly inconsistent — the debouncer and
                        // status map are tolerant of this.
                        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                            self.handle_path(path);
                        }));
                        match result {
                            Ok(()) => {
                                total_processed += 1;
                            }
                            Err(payload) => {
                                total_panics += 1;
                                let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                                    (*s).to_string()
                                } else if let Some(s) = payload.downcast_ref::<String>() {
                                    s.clone()
                                } else {
                                    "unknown panic payload".to_string()
                                };
                                error!(
                                    path = %path.display(),
                                    panic = %msg,
                                    "worker panic caught while handling event; continuing"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    total_errors += 1;
                    warn!(error = %e, "file watcher reported an error");
                }
            }
        }

        info!(
            total_received,
            total_processed,
            total_errors,
            total_panics,
            "watcher event channel closed; worker exiting"
        );
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
        // The actual atomic counter must also remain at 0, not wrap to usize::MAX.
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn connection_tracking_underflow_repeated() {
        let counter = Arc::new(AtomicUsize::new(0));
        // Multiple disconnects at zero should all return 0 and keep counter at 0.
        for _ in 0..10 {
            assert_eq!(track_disconnect(&counter), 0);
            assert_eq!(counter.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn connection_tracking_concurrent() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        // Spawn 50 connect threads.
        for _ in 0..50 {
            let c = counter.clone();
            handles.push(std::thread::spawn(move || {
                track_connect(&c);
            }));
        }
        for h in handles.drain(..) {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 50);

        // Spawn 50 disconnect threads.
        for _ in 0..50 {
            let c = counter.clone();
            handles.push(std::thread::spawn(move || {
                track_disconnect(&c);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn connection_tracking_interleaved() {
        let counter = Arc::new(AtomicUsize::new(0));
        assert_eq!(track_connect(&counter), 1);
        assert_eq!(track_connect(&counter), 2);
        assert_eq!(track_disconnect(&counter), 1);
        assert_eq!(track_connect(&counter), 2);
        assert_eq!(track_connect(&counter), 3);
        assert_eq!(track_disconnect(&counter), 2);
        assert_eq!(track_disconnect(&counter), 1);
        assert_eq!(track_disconnect(&counter), 0);
        assert_eq!(track_disconnect(&counter), 0); // underflow guard
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    // -- Debouncer additional tests ----------------------------------------

    #[test]
    fn debouncer_zero_window_clamped() {
        // A zero-duration window should be clamped to 1ms, not disable debouncing.
        let mut d = Debouncer::new(Duration::from_millis(0));
        assert!(d.window >= Duration::from_millis(1));
        // First event should still pass.
        assert!(d.should_handle("file.txt"));
        // Immediate second event should be suppressed (1ms window).
        assert!(!d.should_handle("file.txt"));
    }

    #[test]
    fn debouncer_very_large_window() {
        let mut d = Debouncer::new(Duration::from_secs(3600)); // 1 hour
        assert!(d.should_handle("a.txt"));
        assert!(!d.should_handle("a.txt")); // within window
        assert!(d.should_handle("b.txt")); // different path
    }

    #[test]
    fn debouncer_allows_after_window_expires() {
        let mut d = Debouncer::new(Duration::from_millis(10));
        assert!(d.should_handle("file.rs"));
        // Wait for the debounce window to expire.
        std::thread::sleep(Duration::from_millis(20));
        // Should be allowed again.
        assert!(d.should_handle("file.rs"));
    }

    #[test]
    fn debouncer_many_distinct_paths() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        for i in 0..1000 {
            let path = format!("path/{}/file_{}.rs", i / 10, i);
            assert!(d.should_handle(&path));
        }
        // All 1000 distinct paths should be in the map.
        assert_eq!(d.last_seen.len(), 1000);
    }

    #[test]
    fn debouncer_prune_actually_removes_stale() {
        let mut d = Debouncer::new(Duration::from_millis(1));
        d.prune_interval = 3; // prune after every 3 events

        // Insert a path.
        d.should_handle("old.txt");
        // Sleep so it becomes stale (10× window = 10ms).
        std::thread::sleep(Duration::from_millis(20));

        // Insert 3 more events to trigger prune.
        d.should_handle("a.txt");
        d.should_handle("b.txt");
        d.should_handle("c.txt");

        // "old.txt" should have been pruned.
        assert!(!d.last_seen.contains_key("old.txt"));
        // Recent entries should remain.
        assert!(d.last_seen.contains_key("a.txt"));
        assert!(d.last_seen.contains_key("b.txt"));
        assert!(d.last_seen.contains_key("c.txt"));
    }

    #[test]
    fn debouncer_prune_keeps_recent_entries() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        d.prune_interval = 2;

        d.should_handle("fresh.txt");
        d.should_handle("trigger1.txt");
        d.should_handle("trigger2.txt"); // triggers prune

        // All entries are fresh (within 10× 10s = 100s), so none should be pruned.
        assert!(d.last_seen.contains_key("fresh.txt"));
        assert!(d.last_seen.contains_key("trigger1.txt"));
        assert!(d.last_seen.contains_key("trigger2.txt"));
    }

    #[test]
    fn debouncer_prune_interval_default() {
        let d = Debouncer::new(Duration::from_millis(200));
        assert_eq!(d.prune_interval, 1000);
        assert_eq!(d.events_since_prune, 0);
    }

    // -- Event kind filtering exhaustive -----------------------------------

    #[test]
    fn create_file_is_relevant() {
        assert!(is_relevant_event(&EventKind::Create(
            notify::event::CreateKind::File
        )));
    }

    #[test]
    fn create_folder_is_relevant() {
        assert!(is_relevant_event(&EventKind::Create(
            notify::event::CreateKind::Folder
        )));
    }

    #[test]
    fn create_any_is_relevant() {
        assert!(is_relevant_event(&EventKind::Create(
            notify::event::CreateKind::Any
        )));
    }

    #[test]
    fn create_other_is_relevant() {
        assert!(is_relevant_event(&EventKind::Create(
            notify::event::CreateKind::Other
        )));
    }

    #[test]
    fn remove_file_is_relevant() {
        assert!(is_relevant_event(&EventKind::Remove(
            notify::event::RemoveKind::File
        )));
    }

    #[test]
    fn remove_folder_is_relevant() {
        assert!(is_relevant_event(&EventKind::Remove(
            notify::event::RemoveKind::Folder
        )));
    }

    #[test]
    fn remove_any_is_relevant() {
        assert!(is_relevant_event(&EventKind::Remove(
            notify::event::RemoveKind::Any
        )));
    }

    #[test]
    fn remove_other_is_relevant() {
        assert!(is_relevant_event(&EventKind::Remove(
            notify::event::RemoveKind::Other
        )));
    }

    #[test]
    fn modify_data_content_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Data(notify::event::DataChange::Content)
        )));
    }

    #[test]
    fn modify_data_size_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Data(notify::event::DataChange::Size)
        )));
    }

    #[test]
    fn modify_data_any_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Data(notify::event::DataChange::Any)
        )));
    }

    #[test]
    fn modify_data_other_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Data(notify::event::DataChange::Other)
        )));
    }

    #[test]
    fn modify_name_from_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Name(notify::event::RenameMode::From)
        )));
    }

    #[test]
    fn modify_name_to_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Name(notify::event::RenameMode::To)
        )));
    }

    #[test]
    fn modify_name_both_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Name(notify::event::RenameMode::Both)
        )));
    }

    #[test]
    fn modify_name_any_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Name(notify::event::RenameMode::Any)
        )));
    }

    #[test]
    fn modify_name_other_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Name(notify::event::RenameMode::Other)
        )));
    }

    #[test]
    fn modify_any_is_relevant() {
        assert!(is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Any
        )));
    }

    #[test]
    fn modify_other_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Other
        )));
    }

    #[test]
    fn modify_metadata_writetime_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::WriteTime)
        )));
    }

    #[test]
    fn modify_metadata_accesstime_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::AccessTime)
        )));
    }

    #[test]
    fn modify_metadata_permissions_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::Permissions)
        )));
    }

    #[test]
    fn modify_metadata_ownership_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::Ownership)
        )));
    }

    #[test]
    fn modify_metadata_extended_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::Extended)
        )));
    }

    #[test]
    fn modify_metadata_any_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::Any)
        )));
    }

    #[test]
    fn modify_metadata_other_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Metadata(notify::event::MetadataKind::Other)
        )));
    }

    #[test]
    fn access_read_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Access(
            notify::event::AccessKind::Read
        )));
    }

    #[test]
    fn access_open_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Access(
            notify::event::AccessKind::Open(notify::event::AccessMode::Read)
        )));
    }

    #[test]
    fn access_close_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Access(
            notify::event::AccessKind::Close(notify::event::AccessMode::Write)
        )));
    }

    #[test]
    fn access_any_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Access(
            notify::event::AccessKind::Any
        )));
    }

    #[test]
    fn access_other_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Access(
            notify::event::AccessKind::Other
        )));
    }

    #[test]
    fn event_kind_other_is_not_relevant() {
        assert!(!is_relevant_event(&EventKind::Other));
    }

    #[test]
    fn event_kind_any_is_relevant() {
        assert!(is_relevant_event(&EventKind::Any));
    }

    // -- broadcast_reload --------------------------------------------------

    #[test]
    fn broadcast_reload_sends_valid_json() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_reload(&btx, "src/main.rs", "test-cfg");
        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "reload");
    }

    #[test]
    fn broadcast_reload_no_receivers() {
        let (btx, _) = broadcast::channel::<String>(8);
        // No active receivers — send should not panic.
        broadcast_reload(&btx, "file.txt", "cfg");
    }

    #[test]
    fn broadcast_reload_multiple_receivers() {
        let (btx, _) = broadcast::channel::<String>(8);
        let mut rx1 = btx.subscribe();
        let mut rx2 = btx.subscribe();
        let mut rx3 = btx.subscribe();

        broadcast_reload(&btx, "app.js", "scripts");

        assert!(rx1.try_recv().unwrap().contains("reload"));
        assert!(rx2.try_recv().unwrap().contains("reload"));
        assert!(rx3.try_recv().unwrap().contains("reload"));
    }

    // -- broadcast_inject_css ----------------------------------------------

    #[test]
    fn broadcast_inject_css_with_real_file() {
        // Create a real temp CSS file and verify inject-css broadcast.
        let dir = std::env::temp_dir().join("watchd_test_inject_css");
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("style.css");
        std::fs::write(&css_path, "body { color: red; }").unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_inject_css(&btx, &css_path, "style.css", "test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");
        assert_eq!(parsed["path"], "style.css");

        // Verify the content is base64-encoded.
        let content = parsed["content"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(content)
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "body { color: red; }");

        // Cleanup.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn broadcast_inject_css_nonexistent_file_falls_back_to_reload() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("/nonexistent/does_not_exist.css");
        broadcast_inject_css(&btx, path, "missing.css", "test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        // Should fall back to reload since the file can't be read.
        assert_eq!(parsed["type"], "reload");
    }

    #[test]
    fn broadcast_inject_css_empty_file() {
        let dir = std::env::temp_dir().join("watchd_test_inject_empty");
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("empty.css");
        std::fs::write(&css_path, "").unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_inject_css(&btx, &css_path, "empty.css", "test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");

        let content = parsed["content"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(content)
            .unwrap();
        assert!(decoded.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn broadcast_inject_css_large_file() {
        let dir = std::env::temp_dir().join("watchd_test_inject_large");
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("large.css");
        // 100KB of CSS content.
        let content = "x".repeat(100_000);
        std::fs::write(&css_path, &content).unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_inject_css(&btx, &css_path, "large.css", "test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");

        let b64 = parsed["content"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(decoded.len(), 100_000);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn broadcast_inject_css_utf8_content() {
        let dir = std::env::temp_dir().join("watchd_test_inject_utf8");
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("unicode.css");
        let css = "/* 日本語コメント */ body { content: '🎨'; }";
        std::fs::write(&css_path, css).unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_inject_css(&btx, &css_path, "unicode.css", "test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let b64 = parsed["content"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), css);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- notify_clients additional tests -----------------------------------

    #[test]
    fn notify_clients_inject_css_mode_with_real_file() {
        let dir = std::env::temp_dir().join("watchd_test_notify_inject");
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("notify.css");
        std::fs::write(&css_path, ".x { color: blue; }").unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        notify_clients(&btx, &css_path, "notify.css", &NotifyMode::InjectCss, "t");

        let msg = brx.try_recv().unwrap();
        assert!(msg.contains("inject-css"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn notify_clients_inject_css_mode_missing_file_falls_back() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("totally_missing.css");
        notify_clients(
            &btx,
            path,
            "totally_missing.css",
            &NotifyMode::InjectCss,
            "t",
        );

        let msg = brx.try_recv().unwrap();
        // Missing file with InjectCss mode should fall back to reload.
        assert!(msg.contains("reload"));
    }

    #[test]
    fn notify_clients_auto_mode_html_reloads() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("index.html");
        notify_clients(&btx, path, "index.html", &NotifyMode::Auto, "t");
        let msg = brx.try_recv().unwrap();
        assert!(msg.contains("reload"));
    }

    #[test]
    fn notify_clients_auto_mode_no_extension_reloads() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("Makefile");
        notify_clients(&btx, path, "Makefile", &NotifyMode::Auto, "t");
        let msg = brx.try_recv().unwrap();
        assert!(msg.contains("reload"));
    }

    #[test]
    fn notify_clients_auto_mode_scss_reloads() {
        // Only .css files trigger inject-css in auto mode; .scss should reload.
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("styles.scss");
        notify_clients(&btx, path, "styles.scss", &NotifyMode::Auto, "t");
        let msg = brx.try_recv().unwrap();
        assert!(msg.contains("reload"));
    }

    #[test]
    fn notify_clients_auto_mode_css_uppercase() {
        // Extension comparison should be case-insensitive.
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("theme.CSS");
        notify_clients(&btx, path, "theme.CSS", &NotifyMode::Auto, "t");
        let msg = brx.try_recv().unwrap();
        // Auto mode uses to_ascii_lowercase, so .CSS should match css path.
        // But file doesn't exist on disk, so inject-css falls back to reload.
        assert!(msg.contains("reload"));
    }

    #[test]
    fn notify_clients_reload_mode_regardless_of_extension() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        // Even a CSS file with reload mode should produce a reload message.
        let path = Path::new("should_reload.css");
        notify_clients(&btx, path, "should_reload.css", &NotifyMode::Reload, "t");
        let msg = brx.try_recv().unwrap();
        assert!(msg.contains("reload"));
        assert!(!msg.contains("inject-css"));
    }

    #[test]
    fn notify_clients_none_mode_produces_no_message() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("ignored.txt");
        notify_clients(&btx, path, "ignored.txt", &NotifyMode::None, "t");
        // Channel should be empty.
        assert!(brx.try_recv().is_err());
    }

    #[test]
    fn notify_clients_none_mode_for_css_still_no_message() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let path = Path::new("silent.css");
        notify_clients(&btx, path, "silent.css", &NotifyMode::None, "t");
        assert!(brx.try_recv().is_err());
    }

    // -- try_reload_configs ------------------------------------------------

    #[test]
    fn try_reload_configs_valid_file() {
        let dir = std::env::temp_dir().join("watchd_test_reload_valid");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml");
        std::fs::write(
            &cfg_path,
            r#"
- name: styles
  watch: "**/*.css"
  notify: inject-css
- name: scripts
  watch: "**/*.js"
  notify: reload
"#,
        )
        .unwrap();

        let result = try_reload_configs(&cfg_path);
        assert!(result.is_some());
        let configs = result.unwrap();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].name, "styles");
        assert_eq!(configs[1].name, "scripts");
        // Verify watch sets were compiled.
        assert!(configs[0].watch_set.is_some());
        assert!(configs[1].watch_set.is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_reload_configs_nonexistent_file() {
        let path = Path::new("/nonexistent/hotreload.yaml");
        let result = try_reload_configs(path);
        assert!(result.is_none());
    }

    #[test]
    fn try_reload_configs_empty_file() {
        let dir = std::env::temp_dir().join("watchd_test_reload_empty");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml");
        std::fs::write(&cfg_path, "").unwrap();

        // Empty YAML produces zero configs → returns None (keeps previous).
        let result = try_reload_configs(&cfg_path);
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_reload_configs_invalid_yaml() {
        let dir = std::env::temp_dir().join("watchd_test_reload_invalid");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml");
        std::fs::write(&cfg_path, "{{{{not valid yaml at all}}}}").unwrap();

        let result = try_reload_configs(&cfg_path);
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_reload_configs_single_config() {
        let dir = std::env::temp_dir().join("watchd_test_reload_single");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml");
        std::fs::write(
            &cfg_path,
            "name: solo\nwatch: \"**/*.rs\"\non_change: \"cargo build\"\n",
        )
        .unwrap();

        let result = try_reload_configs(&cfg_path);
        assert!(result.is_some());
        let configs = result.unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].name, "solo");
        assert!(configs[0].watch_set.is_some());
        assert!(configs[0].matches("src/main.rs"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_reload_configs_scalar_yaml() {
        let dir = std::env::temp_dir().join("watchd_test_reload_scalar");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml");
        std::fs::write(&cfg_path, "42\n").unwrap();

        // Scalar YAML produces zero configs → None.
        let result = try_reload_configs(&cfg_path);
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- run_command_blocking edge cases ------------------------------------

    #[test]
    fn run_command_blocking_nonexistent_command() {
        // Should log an error but not panic.
        run_command_blocking("this_command_does_not_exist_12345", None);
    }

    #[test]
    fn run_command_blocking_empty_command() {
        // Empty command — the shell should handle it gracefully.
        // On some shells this is a no-op; on others it may fail.
        // The key property is no panic.
        run_command_blocking("", None);
    }

    #[test]
    fn run_command_blocking_failing_command() {
        // A command that exits with non-zero should log a warning but not panic.
        #[cfg(target_os = "windows")]
        run_command_blocking("cmd /C exit 1", None);
        #[cfg(not(target_os = "windows"))]
        run_command_blocking("false", None);
    }

    #[test]
    fn run_command_blocking_zero_timeout() {
        // A zero-ms timeout should still work (command likely killed immediately).
        run_command_blocking("echo quick", Some(Duration::from_millis(0)));
    }

    #[test]
    fn run_command_blocking_generous_timeout() {
        // A very generous timeout with a fast command should complete normally.
        let start = Instant::now();
        run_command_blocking("echo hello", Some(Duration::from_secs(60)));
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    // -- WorkerContext event processing ------------------------------------

    #[test]
    fn worker_context_exits_when_channel_closes() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, _) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_exit");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::with_defaults(&[]),
            configs: vec![],
            statuses,
            cmd_template: "echo {path}".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(200)),
            cmd_timeout: None,
            config_path: cfg_path,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Drop the sender to close the channel.
        drop(tx);

        // Worker should exit cleanly.
        handle.join().expect("worker thread should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_context_handles_error_events() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, _) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_errors");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::with_defaults(&[]),
            configs: vec![],
            statuses,
            cmd_template: "echo {path}".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(200)),
            cmd_timeout: None,
            config_path: cfg_path,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send error events — should be logged but not crash the worker.
        tx.send(Err(notify::Error::generic("test error 1")))
            .unwrap();
        tx.send(Err(notify::Error::generic("test error 2")))
            .unwrap();

        // Close channel to let worker exit.
        drop(tx);

        handle
            .join()
            .expect("worker should handle errors without crashing");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_context_skips_irrelevant_events() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_irrelevant");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::with_defaults(&[]),
            configs: vec![],
            statuses,
            cmd_template: "echo {path}".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send an access event (irrelevant) — should not produce any broadcast.
        let access_event = Event {
            kind: EventKind::Access(notify::event::AccessKind::Read),
            paths: vec![dir.join("file.txt")],
            attrs: Default::default(),
        };
        tx.send(Ok(access_event)).unwrap();

        // Give worker time to process.
        std::thread::sleep(Duration::from_millis(50));

        // No broadcast should have been sent.
        assert!(brx.try_recv().is_err());

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_context_processes_relevant_events() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_relevant");
        let _ = std::fs::create_dir_all(&dir);

        // Create a real HTML file so the handler produces a reload broadcast.
        let html_path = dir.join("page.html");
        std::fs::write(&html_path, "<html></html>").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo changed".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: Some(Duration::from_secs(5)),
            config_path: cfg_path,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send a modify-data event for an HTML file.
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![html_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        // Give worker time to process.
        std::thread::sleep(Duration::from_millis(200));

        // Should have broadcast a reload for the HTML file.
        let msg = brx.try_recv().expect("should receive a broadcast");
        assert!(msg.contains("reload"));

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_context_processes_css_with_config() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(vec!["styles".to_string()]);

        let dir = std::env::temp_dir().join("watchd_test_worker_css_cfg");
        let _ = std::fs::create_dir_all(&dir);

        let css_path = dir.join("app.css");
        std::fs::write(&css_path, "body { margin: 0; }").unwrap();

        // Create a config that matches CSS files with inject-css mode.
        let mut cfg = config::ConfigEntry {
            name: "styles".to_string(),
            watches: vec!["**/*.css".to_string()],
            watch_set: None,
            on_change: None,
            build: None,
            notify: NotifyMode::InjectCss,
            ignore: vec![],
        };
        cfg.compile_watch_set();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![cfg],
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send a create event for the CSS file.
        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![css_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        // Give worker time to process.
        std::thread::sleep(Duration::from_millis(200));

        let msg = brx.try_recv().expect("should receive a broadcast");
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_context_skips_filtered_paths() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_filtered");
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        // Filter that excludes everything in a "tmp" directory.
        let filter = PathFilter::new(&[], &["**/*.tmp".to_string()]);

        let ctx = WorkerContext {
            rx,
            btx,
            filter,
            configs: vec![],
            statuses,
            cmd_template: "echo {path}".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: Some(Duration::from_secs(5)),
            config_path: cfg_path,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send a modify event for a .tmp file — should be filtered out.
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![dir.join("cache.tmp")],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(100));

        // No broadcast should have been produced.
        assert!(brx.try_recv().is_err());

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
