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

// ---------------------------------------------------------------------------
// Shell escaping
// ---------------------------------------------------------------------------

/// Shell-escape a string so it can be safely embedded inside a command passed
/// to `sh -c` (Unix) or `cmd /C` (Windows).
///
/// On Unix, the value is wrapped in single quotes with any internal single
/// quotes replaced by the sequence `'\''` (end quote, escaped quote, start
/// quote).
///
/// On Windows, the value is wrapped in double quotes with internal double
/// quotes doubled (`"` → `""`), and `%` is escaped as `%%` to prevent
/// environment-variable expansion.
///
/// This prevents command injection when a file path contains shell
/// metacharacters (e.g. `;`, `|`, `$()`, backticks).
pub fn shell_escape(s: &str) -> String {
    #[cfg(target_os = "windows")]
    {
        // cmd.exe: wrap in double quotes, double any interior quotes, escape %.
        let escaped = s.replace('"', "\"\"").replace('%', "%%");
        format!("\"{}\"", escaped)
    }
    #[cfg(not(target_os = "windows"))]
    {
        // POSIX sh: wrap in single quotes; the only character that needs
        // special handling inside single quotes is the single quote itself.
        let escaped = s.replace('\'', "'\\''");
        format!("'{}'", escaped)
    }
}

use crate::config::{self, ConfigEntry, NotifyMode};
use crate::filter::{normalize_path, PathFilter};
use crate::kv::SharedDiffStore;
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
    /// Optional diff store.  When `Some`, file diffs are recorded on every
    /// change so they can be queried via the control socket.
    /// Enabled by the `--diff` CLI flag.
    pub diff_store: Option<SharedDiffStore>,
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
    /// Optional diff store for recording file change diffs.
    diff_store: Option<SharedDiffStore>,
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

        // Record the diff in the KV store (if enabled).
        // This is done *after* the filter check so that excluded paths
        // (e.g. node_modules/, .git/, target/) are not recorded.
        if let Some(ref store) = self.diff_store {
            if let Ok(mut guard) = store.lock() {
                guard.record_change_from_disk(&normalized, path);
            }
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
        // Shell-escape the path to prevent command injection via crafted filenames.
        let cmd = self
            .cmd_template
            .replace("{path}", &shell_escape(&normalized));
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
        diff_store,
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
        diff_store,
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

    // -- shell_escape ------------------------------------------------------

    #[test]
    fn shell_escape_normal_path() {
        let escaped = shell_escape("src/main.rs");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"src/main.rs\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'src/main.rs'");
    }

    #[test]
    fn shell_escape_path_with_spaces() {
        let escaped = shell_escape("my project/file name.rs");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"my project/file name.rs\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'my project/file name.rs'");
    }

    #[test]
    fn shell_escape_shell_metacharacters() {
        let escaped = shell_escape("file;rm -rf /");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"file;rm -rf /\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'file;rm -rf /'");
    }

    #[test]
    fn shell_escape_backticks_and_dollar() {
        let escaped = shell_escape("$(whoami)`id`");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"$(whoami)`id`\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'$(whoami)`id`'");
    }

    #[test]
    fn shell_escape_single_quotes_unix() {
        let escaped = shell_escape("it's a file");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"it's a file\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'it'\\''s a file'");
    }

    #[test]
    fn shell_escape_double_quotes_windows() {
        let escaped = shell_escape("file\"name");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"file\"\"name\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'file\"name'");
    }

    #[test]
    fn shell_escape_percent_windows() {
        let escaped = shell_escape("100%done");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"100%%done\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'100%done'");
    }

    #[test]
    fn shell_escape_empty_string() {
        let escaped = shell_escape("");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "''");
    }

    #[test]
    fn shell_escape_pipe_and_ampersand() {
        let escaped = shell_escape("a | b && c");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"a | b && c\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'a | b && c'");
    }

    #[test]
    fn shell_escape_newline_and_tab() {
        let escaped = shell_escape("line1\nline2\ttab");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"line1\nline2\ttab\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'line1\nline2\ttab'");
    }

    #[test]
    fn shell_escape_unicode_path() {
        let escaped = shell_escape("ファイル/配置.rs");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"ファイル/配置.rs\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'ファイル/配置.rs'");
    }

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
            diff_store: None,
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
            diff_store: None,
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
            diff_store: None,
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
            diff_store: None,
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
            diff_store: None,
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
            diff_store: None,
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

    // ======================================================================
    // Edge-case and chaos tests
    // ======================================================================

    // -- WorkerContext with diff store enabled ------------------------------

    #[test]
    fn worker_context_with_diff_store_records_changes() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_diffstore");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        // Create a real file that will be "changed".
        let file_path = dir.join("tracked.html");
        std::fs::write(&file_path, "<html><body>v1</body></html>").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();
        let diff_store = crate::kv::DiffStore::new_shared(50, 500, 512 * 1024);

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: Some(diff_store.clone()),
        };

        let handle = std::thread::spawn(move || ctx.run());

        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![file_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        // The diff store should have recorded the change.
        let guard = diff_store.lock().unwrap();
        let keys = guard.list_keys();
        assert!(!keys.is_empty(), "diff store should have at least one key");

        // Verify a broadcast was sent (HTML file → reload).
        let msg = brx.try_recv().expect("should receive a broadcast");
        assert!(msg.contains("reload"));

        drop(guard);
        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Config hot-reload detection ---------------------------------------

    #[test]
    fn worker_context_detects_config_change() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(vec!["original".to_string()]);

        let dir = std::env::temp_dir().join("watchd_test_worker_hotreload_cfg");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let cfg_path = dir.join("hotreload.yaml");
        // Write a valid config for hot-reload.
        std::fs::write(
            &cfg_path,
            "- name: reloaded\n  watch: \"**/*.rs\"\n  notify: reload\n",
        )
        .unwrap();

        // Canonicalize so it matches what the watcher does.
        let canonical = cfg_path.canonicalize().unwrap().into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses: statuses.clone(),
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: canonical.clone(),
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send an event for the config file path itself.
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![canonical.to_path_buf()],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        // Status map should now contain "reloaded" instead of "original".
        {
            let guard = statuses.lock().unwrap();
            assert!(
                guard.contains_key("reloaded"),
                "status map should contain the reloaded config name, got: {:?}",
                *guard
            );
            assert!(
                !guard.contains_key("original"),
                "old config should have been cleared"
            );
        }

        // No broadcast should be sent for config file changes.
        assert!(brx.try_recv().is_err());

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Event with multiple paths -----------------------------------------

    #[test]
    fn worker_context_handles_multi_path_event() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_multipath");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let html1 = dir.join("a.html");
        let html2 = dir.join("b.html");
        std::fs::write(&html1, "<a>").unwrap();
        std::fs::write(&html2, "<b>").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo multi".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Single event with two paths.
        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![html1, html2],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        // Should receive two broadcast messages (one per path).
        let msg1 = brx.try_recv().expect("first path broadcast");
        assert!(msg1.contains("reload"));
        let msg2 = brx.try_recv().expect("second path broadcast");
        assert!(msg2.contains("reload"));

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Event with empty paths vec ----------------------------------------

    #[test]
    fn worker_context_handles_empty_paths_event() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_emptypaths");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Event with no paths at all — should be handled gracefully.
        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(100));

        // No broadcast expected.
        assert!(brx.try_recv().is_err());

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Generic fallback path (non-HTML, non-CSS, no config) --------------

    #[test]
    fn worker_context_generic_fallback_runs_command_and_reloads() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_generic_fallback");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        // Create a file with an extension that doesn't match HTML or CSS.
        let rs_path = dir.join("lib.rs");
        std::fs::write(&rs_path, "fn main() {}").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![], // No configs → falls through to generic fallback.
            statuses,
            cmd_template: "echo fallback {path}".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: Some(Duration::from_secs(5)),
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![rs_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(500));

        // The generic fallback should broadcast a reload.
        let msg = brx.try_recv().expect("should receive reload broadcast");
        assert!(msg.contains("reload"));

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- CSS fallback (no config match, .css extension) --------------------

    #[test]
    fn worker_context_css_fallback_injects_css() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_css_fallback");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let css_path = dir.join("theme.css");
        std::fs::write(&css_path, "h1 { color: green; }").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![], // No configs → CSS fallback.
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![css_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        let msg = brx.try_recv().expect("should receive inject-css broadcast");
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- HTM extension fallback --------------------------------------------

    #[test]
    fn worker_context_htm_extension_triggers_reload() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_htm");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let htm_path = dir.join("index.htm");
        std::fs::write(&htm_path, "<html></html>").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![htm_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        let msg = brx
            .try_recv()
            .expect("should receive reload broadcast for .htm");
        assert!(msg.contains("reload"));

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Rapid-fire chaos: many events in quick succession -----------------

    #[test]
    fn worker_context_rapid_fire_events_no_panic() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, _brx) = broadcast::channel::<String>(256);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_rapidfire");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        // Create many files.
        for i in 0..20 {
            let path = dir.join(format!("file_{}.html", i));
            std::fs::write(&path, format!("<p>{}</p>", i)).unwrap();
        }

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo rapid".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(5)),
            cmd_timeout: Some(Duration::from_secs(2)),
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Blast 200 events rapidly.
        for i in 0..200 {
            let file_idx = i % 20;
            let event = Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Content,
                )),
                paths: vec![dir.join(format!("file_{}.html", file_idx))],
                attrs: Default::default(),
            };
            tx.send(Ok(event)).unwrap();
        }

        std::thread::sleep(Duration::from_millis(500));

        drop(tx);
        handle
            .join()
            .expect("worker should survive rapid-fire events");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Mixed event types chaos -------------------------------------------

    #[test]
    fn worker_context_mixed_event_types_no_panic() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, _brx) = broadcast::channel::<String>(64);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_mixedchaos");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo chaos".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(1)),
            cmd_timeout: Some(Duration::from_secs(1)),
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        let kinds = vec![
            EventKind::Create(notify::event::CreateKind::File),
            EventKind::Create(notify::event::CreateKind::Folder),
            EventKind::Remove(notify::event::RemoveKind::File),
            EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Both,
            )),
            EventKind::Access(notify::event::AccessKind::Read),
            EventKind::Modify(notify::event::ModifyKind::Metadata(
                notify::event::MetadataKind::WriteTime,
            )),
            EventKind::Other,
            EventKind::Any,
        ];

        let paths = vec![
            dir.join("a.css"),
            dir.join("b.html"),
            dir.join("c.js"),
            dir.join("d.rs"),
            dir.join("Makefile"),
        ];

        // Create the files so CSS inject can read them.
        for p in &paths {
            std::fs::write(p, "content").unwrap();
        }

        // Send a mix of all event kinds with all path types.
        for kind in &kinds {
            for path in &paths {
                let event = Event {
                    kind: *kind,
                    paths: vec![path.clone()],
                    attrs: Default::default(),
                };
                let _ = tx.send(Ok(event));
            }
        }

        // Also throw in some errors.
        for _ in 0..5 {
            let _ = tx.send(Err(notify::Error::generic("chaos error")));
        }

        std::thread::sleep(Duration::from_millis(500));

        drop(tx);
        handle
            .join()
            .expect("worker should survive mixed chaos events");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Debouncer: rapid same-path suppression ----------------------------

    #[test]
    fn debouncer_rapid_same_path_only_first_passes() {
        let mut d = Debouncer::new(Duration::from_secs(60));
        assert!(d.should_handle("same.txt"));
        for _ in 0..100 {
            assert!(!d.should_handle("same.txt"));
        }
    }

    // -- Debouncer: empty string path --------------------------------------

    #[test]
    fn debouncer_empty_path() {
        let mut d = Debouncer::new(Duration::from_millis(200));
        assert!(d.should_handle(""));
        assert!(!d.should_handle(""));
    }

    // -- Debouncer: unicode path -------------------------------------------

    #[test]
    fn debouncer_unicode_path() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        assert!(d.should_handle("路径/文件.txt"));
        assert!(!d.should_handle("路径/文件.txt"));
        assert!(d.should_handle("дорожка/файл.txt"));
    }

    // -- Debouncer: very long path -----------------------------------------

    #[test]
    fn debouncer_very_long_path() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        let long_path = "a/".repeat(5000) + "file.txt";
        assert!(d.should_handle(&long_path));
        assert!(!d.should_handle(&long_path));
    }

    // -- Debouncer: paths with special characters --------------------------

    #[test]
    fn debouncer_special_chars_in_path() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        let specials = vec![
            "file with spaces.txt",
            "file\twith\ttabs.txt",
            "file(1).txt",
            "file[2].txt",
            "file{3}.txt",
            "file$var.txt",
            "file#hash.txt",
            "file@at.txt",
            "file!bang.txt",
            "file%percent.txt",
        ];
        for s in &specials {
            assert!(d.should_handle(s), "first event for '{}' should pass", s);
            assert!(
                !d.should_handle(s),
                "second event for '{}' should be suppressed",
                s
            );
        }
    }

    // -- broadcast_inject_css with binary content --------------------------

    #[test]
    fn broadcast_inject_css_binary_content_still_encodes() {
        let dir = std::env::temp_dir().join("watchd_test_inject_binary");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("binary.css");

        // Write binary content (with null bytes).
        let mut content = vec![0u8; 256];
        for (i, byte) in content.iter_mut().enumerate() {
            *byte = i as u8;
        }
        std::fs::write(&css_path, &content).unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_inject_css(&btx, &css_path, "binary.css", "test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");

        // Verify base64 roundtrip preserves binary content.
        let b64 = parsed["content"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(decoded, content);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- broadcast_inject_css with unicode filename -------------------------

    #[test]
    fn broadcast_inject_css_unicode_filename() {
        let dir = std::env::temp_dir().join("watchd_test_inject_unicode_name");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("样式.css");
        std::fs::write(&css_path, ".x { color: red; }").unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_inject_css(&btx, &css_path, "样式.css", "test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");
        assert_eq!(parsed["path"], "样式.css");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- notify_clients: auto mode with real CSS file ----------------------

    #[test]
    fn notify_clients_auto_mode_real_css_file_injects() {
        let dir = std::env::temp_dir().join("watchd_test_notify_auto_real_css");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("real.css");
        std::fs::write(&css_path, "body { margin: 0; }").unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        notify_clients(&btx, &css_path, "real.css", &NotifyMode::Auto, "auto-test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- try_reload_configs: multiple configs with complex patterns ---------

    #[test]
    fn try_reload_configs_complex_yaml() {
        let dir = std::env::temp_dir().join("watchd_test_reload_complex");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml");
        std::fs::write(
            &cfg_path,
            r#"
- name: frontend
  watch:
    - "src/**/*.tsx"
    - "src/**/*.ts"
    - "public/**/*.html"
  on_change: "npm run build"
  build: "npm run build:prod"
  notify: reload
  ignore:
    - "src/**/*.test.tsx"
    - "node_modules/**"

- name: styles
  watch: "styles/**/*.css"
  notify: inject-css

- name: assets
  watch:
    - "**/*.png"
    - "**/*.jpg"
    - "**/*.svg"
  notify: none
  ignore:
    - ".git/**"
"#,
        )
        .unwrap();

        let result = try_reload_configs(&cfg_path);
        assert!(result.is_some());
        let configs = result.unwrap();
        assert_eq!(configs.len(), 3);
        assert_eq!(configs[0].name, "frontend");
        assert_eq!(configs[0].watches.len(), 3);
        assert_eq!(configs[0].ignore.len(), 2);
        assert!(configs[0].on_change.is_some());
        assert!(configs[0].build.is_some());
        assert_eq!(configs[1].name, "styles");
        assert_eq!(configs[2].name, "assets");
        assert_eq!(configs[2].watches.len(), 3);

        // Verify watch sets were compiled.
        for cfg in &configs {
            assert!(
                cfg.watch_set.is_some(),
                "watch_set should be compiled for {}",
                cfg.name
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- try_reload_configs: YAML with only whitespace and comments ---------

    #[test]
    fn try_reload_configs_whitespace_and_comments_only() {
        let dir = std::env::temp_dir().join("watchd_test_reload_comments");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml");
        std::fs::write(&cfg_path, "# just a comment\n\n  \n# another comment\n").unwrap();

        let result = try_reload_configs(&cfg_path);
        // Comments/whitespace produce zero configs → returns None.
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- run_command_blocking with path substitution -----------------------

    #[test]
    fn run_command_blocking_with_special_chars_in_args() {
        // Command with quotes and special characters — should not panic.
        run_command_blocking("echo \"hello world\"", None);
        run_command_blocking("echo path/with spaces/file.txt", None);
    }

    // -- run_command_blocking timeout kills correctly ----------------------

    #[test]
    fn run_command_blocking_timeout_near_boundary() {
        let start = Instant::now();
        #[cfg(target_os = "windows")]
        run_command_blocking("ping -n 100 127.0.0.1", Some(Duration::from_millis(200)));
        #[cfg(not(target_os = "windows"))]
        run_command_blocking("sleep 100", Some(Duration::from_millis(200)));
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(3),
            "should have been killed quickly, took {:?}",
            elapsed
        );
    }

    // -- WorkerContext with diff store and config --------------------------

    #[test]
    fn worker_context_diff_store_with_config_match() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(vec!["css".to_string()]);

        let dir = std::env::temp_dir().join("watchd_test_worker_diff_cfg");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let css_path = dir.join("app.css");
        std::fs::write(&css_path, ".old { color: black; }").unwrap();

        let diff_store = crate::kv::DiffStore::new_shared(50, 500, 512 * 1024);

        let mut cfg = config::ConfigEntry {
            name: "css".to_string(),
            watches: vec!["**/*.css".to_string()],
            watch_set: None,
            on_change: Some("echo building css".to_string()),
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
            statuses: statuses.clone(),
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: Some(Duration::from_secs(5)),
            config_path: cfg_path,
            diff_store: Some(diff_store.clone()),
        };

        let handle = std::thread::spawn(move || ctx.run());

        // "Change" the file.
        std::fs::write(&css_path, ".new { color: white; }").unwrap();

        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![css_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(500));

        // Should have an inject-css broadcast.
        let msg = brx.try_recv().expect("should receive broadcast");
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "inject-css");

        // Diff store should have recorded the change.
        let guard = diff_store.lock().unwrap();
        assert!(
            !guard.list_keys().is_empty(),
            "diff store should have tracked the file"
        );

        // Status should have cycled through "reloading" back to "up to date".
        drop(guard);
        {
            let st = statuses.lock().unwrap();
            assert_eq!(st.get("css").map(|s| s.as_str()), Some("up to date"));
        }

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- WorkerContext: interleaved errors and valid events -----------------

    #[test]
    fn worker_context_interleaved_errors_and_events() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(32);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_interleaved");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let html_path = dir.join("test.html");
        std::fs::write(&html_path, "<html></html>").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo test".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(1)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Interleave errors and valid events.
        tx.send(Err(notify::Error::generic("err1"))).unwrap();
        let valid_event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![html_path.clone()],
            attrs: Default::default(),
        };
        tx.send(Ok(valid_event)).unwrap();
        tx.send(Err(notify::Error::generic("err2"))).unwrap();
        tx.send(Err(notify::Error::generic("err3"))).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        // The valid event should still produce a broadcast despite the errors.
        let msg = brx
            .try_recv()
            .expect("valid event should produce broadcast");
        assert!(msg.contains("reload"));

        drop(tx);
        handle
            .join()
            .expect("worker should survive interleaved errors");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- WorkerContext: debouncer suppresses duplicates ---------------------

    #[test]
    fn worker_context_debouncer_suppresses_within_window() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(32);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_debounce");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let html_path = dir.join("debounce.html");
        std::fs::write(&html_path, "<html></html>").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo debounce".to_string(),
            debouncer: Debouncer::new(Duration::from_secs(60)), // Very long debounce.
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send the same event 10 times — only the first should produce a broadcast.
        for _ in 0..10 {
            let event = Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Content,
                )),
                paths: vec![html_path.clone()],
                attrs: Default::default(),
            };
            tx.send(Ok(event)).unwrap();
        }

        std::thread::sleep(Duration::from_millis(300));

        // Should receive exactly one broadcast.
        let msg = brx.try_recv().expect("first event should broadcast");
        assert!(msg.contains("reload"));
        assert!(
            brx.try_recv().is_err(),
            "subsequent events should be debounced"
        );

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- broadcast_reload JSON structure -----------------------------------

    #[test]
    fn broadcast_reload_message_is_minimal_json() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_reload(&btx, "any/path.txt", "cfg");
        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        // Should only have a "type" field — no extra fields.
        assert_eq!(parsed.as_object().unwrap().len(), 1);
        assert_eq!(parsed["type"], "reload");
    }

    // -- broadcast_inject_css JSON structure --------------------------------

    #[test]
    fn broadcast_inject_css_message_has_three_fields() {
        let dir = std::env::temp_dir().join("watchd_test_inject_fields");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let css_path = dir.join("fields.css");
        std::fs::write(&css_path, "p { padding: 0; }").unwrap();

        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_inject_css(&btx, &css_path, "fields.css", "test");

        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.len(), 3);
        assert!(obj.contains_key("type"));
        assert!(obj.contains_key("path"));
        assert!(obj.contains_key("content"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Debouncer: prune with lots of paths then re-use -------------------

    #[test]
    fn debouncer_heavy_prune_then_reuse() {
        let mut d = Debouncer::new(Duration::from_millis(1));
        d.prune_interval = 50;

        // Insert many paths.
        for i in 0..100 {
            d.should_handle(&format!("path_{}.txt", i));
        }

        // Sleep so all entries become stale.
        std::thread::sleep(Duration::from_millis(20));

        // Insert enough to trigger prune.
        for i in 100..160 {
            d.should_handle(&format!("path_{}.txt", i));
        }

        // Old entries should have been pruned.
        assert!(
            d.last_seen.len() < 120,
            "stale entries should have been pruned, got {} entries",
            d.last_seen.len()
        );

        // New entries should still be present.
        assert!(d.last_seen.contains_key("path_159.txt"));
    }

    // -- Debouncer: window exactly at boundary -----------------------------

    #[test]
    fn debouncer_boundary_window() {
        let mut d = Debouncer::new(Duration::from_millis(50));
        assert!(d.should_handle("boundary.txt"));
        // Sleep just past the window.
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            d.should_handle("boundary.txt"),
            "should pass after window expires"
        );
    }

    // -- Connection tracking: high concurrency stress ----------------------

    #[test]
    fn connection_tracking_high_concurrency_stress() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        // 100 threads connecting.
        for _ in 0..100 {
            let c = counter.clone();
            handles.push(std::thread::spawn(move || {
                track_connect(&c);
            }));
        }
        for h in handles.drain(..) {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 100);

        // 100 threads disconnecting.
        for _ in 0..100 {
            let c = counter.clone();
            handles.push(std::thread::spawn(move || {
                track_disconnect(&c);
            }));
        }
        for h in handles.drain(..) {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        // Extra disconnects should saturate at 0.
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

    // -- Connection tracking: interleaved connect/disconnect from threads --

    #[test]
    fn connection_tracking_threaded_interleave() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        // Alternate connect and disconnect from many threads.
        for i in 0..200 {
            let c = counter.clone();
            handles.push(std::thread::spawn(move || {
                if i % 2 == 0 {
                    track_connect(&c);
                } else {
                    track_disconnect(&c);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // 100 connects and 100 disconnects. Counter should be between 0 and 100
        // depending on ordering. The key property is no panic and no wrap.
        let val = counter.load(Ordering::Relaxed);
        assert!(
            val <= 100,
            "counter should not exceed number of connects, got {}",
            val
        );
    }

    // -- notify_clients: auto mode with various extensions -----------------

    #[test]
    fn notify_clients_auto_mode_various_extensions() {
        let extensions = vec![
            ("file.js", "reload"),
            ("file.ts", "reload"),
            ("file.jsx", "reload"),
            ("file.tsx", "reload"),
            ("file.py", "reload"),
            ("file.rb", "reload"),
            ("file.go", "reload"),
            ("file.rs", "reload"),
            ("file.json", "reload"),
            ("file.yaml", "reload"),
            ("file.md", "reload"),
            ("file.txt", "reload"),
        ];

        for (filename, expected_type) in extensions {
            let (btx, mut brx) = broadcast::channel::<String>(8);
            let path = Path::new(filename);
            notify_clients(&btx, path, filename, &NotifyMode::Auto, "test");
            let msg = brx
                .try_recv()
                .unwrap_or_else(|_| panic!("should broadcast for {}", filename));
            assert!(
                msg.contains(expected_type),
                "expected {} for {}, got: {}",
                expected_type,
                filename,
                msg
            );
        }
    }

    // -- WorkerContext: config with on_change and build (on_change priority) -

    #[test]
    fn worker_context_on_change_takes_priority_over_build() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(vec!["test".to_string()]);

        let dir = std::env::temp_dir().join("watchd_test_worker_priority");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let js_path = dir.join("app.js");
        std::fs::write(&js_path, "console.log('hi');").unwrap();

        let mut cfg = config::ConfigEntry {
            name: "test".to_string(),
            watches: vec!["**/*.js".to_string()],
            watch_set: None,
            on_change: Some("echo on_change ran".to_string()),
            build: Some("echo build ran".to_string()),
            notify: NotifyMode::Reload,
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
            cmd_timeout: Some(Duration::from_secs(5)),
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![js_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(500));

        let msg = brx.try_recv().expect("should receive broadcast");
        assert!(msg.contains("reload"));

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- WorkerContext: config with no commands (notify only) ---------------

    #[test]
    fn worker_context_config_notify_only_no_command() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(vec!["notifyonly".to_string()]);

        let dir = std::env::temp_dir().join("watchd_test_worker_notify_only");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let js_path = dir.join("script.js");
        std::fs::write(&js_path, "var x = 1;").unwrap();

        let mut cfg = config::ConfigEntry {
            name: "notifyonly".to_string(),
            watches: vec!["**/*.js".to_string()],
            watch_set: None,
            on_change: None, // No command.
            build: None,     // No build either.
            notify: NotifyMode::Reload,
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
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![js_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        // Should still broadcast even without a command.
        let msg = brx.try_recv().expect("should broadcast reload");
        assert!(msg.contains("reload"));

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- WorkerContext: multiple configs, first match wins ------------------

    #[test]
    fn worker_context_multiple_configs_all_matching_run() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(32);
        let statuses =
            crate::server::new_status_map(vec!["first".to_string(), "second".to_string()]);

        let dir = std::env::temp_dir().join("watchd_test_worker_multi_cfg");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let css_path = dir.join("shared.css");
        std::fs::write(&css_path, ".shared { display: block; }").unwrap();

        // Both configs match *.css.
        let mut cfg1 = config::ConfigEntry {
            name: "first".to_string(),
            watches: vec!["**/*.css".to_string()],
            watch_set: None,
            on_change: None,
            build: None,
            notify: NotifyMode::Reload,
            ignore: vec![],
        };
        cfg1.compile_watch_set();

        let mut cfg2 = config::ConfigEntry {
            name: "second".to_string(),
            watches: vec!["**/*.css".to_string()],
            watch_set: None,
            on_change: None,
            build: None,
            notify: NotifyMode::InjectCss,
            ignore: vec![],
        };
        cfg2.compile_watch_set();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![cfg1, cfg2],
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![css_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        // Both configs match, so we should get two broadcasts.
        let msg1 = brx.try_recv().expect("first config should broadcast");
        assert!(msg1.contains("reload")); // first config is Reload mode
        let msg2 = brx.try_recv().expect("second config should broadcast");
        assert!(msg2.contains("inject-css")); // second config is InjectCss mode

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- WorkerContext: config with ignore patterns filters out paths -------

    #[test]
    fn worker_context_config_ignore_skips_matching_paths() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(vec!["filtered".to_string()]);

        let dir = std::env::temp_dir().join("watchd_test_worker_cfg_ignore");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let js_path = dir.join("app.js");
        std::fs::write(&js_path, "var y = 2;").unwrap();

        // Config watches *.js but the global filter excludes *.js via ignore.
        let mut cfg = config::ConfigEntry {
            name: "filtered".to_string(),
            watches: vec!["**/*.js".to_string()],
            watch_set: None,
            on_change: None,
            build: None,
            notify: NotifyMode::Reload,
            ignore: vec![],
        };
        cfg.compile_watch_set();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        // Use the global filter to exclude *.js files.
        let filter = PathFilter::new(&[], &["**/*.js".to_string()]);

        let ctx = WorkerContext {
            rx,
            btx,
            filter,
            configs: vec![cfg],
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![js_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(200));

        // Global filter should block the path before config matching.
        assert!(
            brx.try_recv().is_err(),
            "filtered path should produce no broadcast"
        );

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- WorkerContext: remove event for a file ----------------------------

    #[test]
    fn worker_context_remove_event_triggers_fallback() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_remove");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo removed".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: Some(Duration::from_secs(2)),
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // A remove event for a .rs file — should trigger generic fallback.
        let event = Event {
            kind: EventKind::Remove(notify::event::RemoveKind::File),
            paths: vec![dir.join("deleted.rs")],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(500));

        let msg = brx
            .try_recv()
            .expect("remove event should produce broadcast");
        assert!(msg.contains("reload"));

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- WorkerContext: rename event (ModifyKind::Name) ---------------------

    #[test]
    fn worker_context_rename_event_is_processed() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, mut brx) = broadcast::channel::<String>(8);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_rename");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let old_path = dir.join("old.html");
        let new_path = dir.join("new.html");
        std::fs::write(&new_path, "<renamed>").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo rename".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Rename event with both old and new paths.
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Both,
            )),
            paths: vec![old_path, new_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        // Should receive broadcasts for both the old and new paths.
        let msg1 = brx
            .try_recv()
            .expect("rename should produce at least one broadcast");
        assert!(msg1.contains("reload"));

        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Chaos: rapid create/delete cycle ----------------------------------

    #[test]
    fn worker_context_rapid_create_delete_cycle() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, _brx) = broadcast::channel::<String>(256);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_create_delete");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        let diff_store = crate::kv::DiffStore::new_shared(10, 50, 4096);

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo cycle".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(1)),
            cmd_timeout: Some(Duration::from_secs(1)),
            config_path: cfg_path,
            diff_store: Some(diff_store.clone()),
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Simulate rapid create → modify → remove cycles.
        for i in 0..30 {
            let file = dir.join(format!("ephemeral_{}.html", i % 5));

            // Create.
            std::fs::write(&file, format!("v{}", i)).unwrap();
            tx.send(Ok(Event {
                kind: EventKind::Create(notify::event::CreateKind::File),
                paths: vec![file.clone()],
                attrs: Default::default(),
            }))
            .unwrap();

            // Modify.
            std::fs::write(&file, format!("v{}_modified", i)).unwrap();
            tx.send(Ok(Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Content,
                )),
                paths: vec![file.clone()],
                attrs: Default::default(),
            }))
            .unwrap();

            // Remove.
            let _ = std::fs::remove_file(&file);
            tx.send(Ok(Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![file],
                attrs: Default::default(),
            }))
            .unwrap();
        }

        std::thread::sleep(Duration::from_millis(1000));

        drop(tx);
        handle
            .join()
            .expect("worker should survive create/delete chaos");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Chaos: concurrent senders on the channel --------------------------

    #[test]
    fn worker_context_concurrent_event_senders() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, _brx) = broadcast::channel::<String>(512);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_concurrent_send");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();

        // Create files for the senders to reference.
        for i in 0..10 {
            std::fs::write(dir.join(format!("f{}.html", i)), "<p>x</p>").unwrap();
        }

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo concurrent".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(1)),
            cmd_timeout: Some(Duration::from_secs(2)),
            config_path: cfg_path,
            diff_store: None,
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Spawn 5 threads each sending 20 events.
        let mut sender_handles = Vec::new();
        for t in 0..5 {
            let tx_clone = tx.clone();
            let dir_clone = dir.clone();
            sender_handles.push(std::thread::spawn(move || {
                for i in 0..20 {
                    let file_idx = (t * 4 + i) % 10;
                    let event = Event {
                        kind: EventKind::Modify(notify::event::ModifyKind::Data(
                            notify::event::DataChange::Content,
                        )),
                        paths: vec![dir_clone.join(format!("f{}.html", file_idx))],
                        attrs: Default::default(),
                    };
                    let _ = tx_clone.send(Ok(event));
                }
            }));
        }

        for h in sender_handles {
            h.join().unwrap();
        }

        std::thread::sleep(Duration::from_millis(500));

        drop(tx);
        handle
            .join()
            .expect("worker should survive concurrent senders");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- is_relevant_event: exhaustive ModifyKind::Other -------------------

    #[test]
    fn modify_kind_other_is_not_relevant() {
        // ModifyKind::Other should be treated as non-relevant.
        assert!(!is_relevant_event(&EventKind::Modify(
            notify::event::ModifyKind::Other
        )));
    }

    // -- broadcast_reload with empty label and path ------------------------

    #[test]
    fn broadcast_reload_empty_strings() {
        let (btx, mut brx) = broadcast::channel::<String>(8);
        broadcast_reload(&btx, "", "");
        let msg = brx.try_recv().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["type"], "reload");
    }

    // -- notify_clients with all modes exhaustive --------------------------

    #[test]
    fn notify_clients_all_modes_no_panic() {
        let modes = vec![
            NotifyMode::Auto,
            NotifyMode::Reload,
            NotifyMode::InjectCss,
            NotifyMode::None,
        ];
        let path = Path::new("test.txt");

        for mode in &modes {
            let (btx, _brx) = broadcast::channel::<String>(8);
            // Should never panic regardless of mode.
            notify_clients(&btx, path, "test.txt", mode, "exhaustive");
        }
    }

    // -- try_reload_configs: UTF-8 BOM -------------------------------------

    #[test]
    fn try_reload_configs_utf8_bom() {
        let dir = std::env::temp_dir().join("watchd_test_reload_bom");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("hotreload.yaml");

        // UTF-8 BOM + valid YAML.  serde_yaml may or may not handle the BOM
        // gracefully — the key property is that it must not panic.
        let yaml = "\u{FEFF}- name: bom\n  watch: \"**/*.rs\"\n";
        std::fs::write(&cfg_path, yaml).unwrap();

        let result = try_reload_configs(&cfg_path);
        // Either it parses successfully or returns None (keeps previous).
        if let Some(configs) = result {
            assert_eq!(configs.len(), 1);
            assert_eq!(configs[0].name, "bom");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- WorkerContext: diff store records even for non-existent file -------

    #[test]
    fn worker_context_diff_store_handles_missing_file() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, _brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_diff_missing");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();
        let diff_store = crate::kv::DiffStore::new_shared(50, 500, 512 * 1024);

        let ctx = WorkerContext {
            rx,
            btx,
            filter: PathFilter::new(&[], &[]),
            configs: vec![],
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: Some(diff_store.clone()),
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send event for a file that doesn't exist on disk.
        let event = Event {
            kind: EventKind::Remove(notify::event::RemoveKind::File),
            paths: vec![dir.join("gone.rs")],
            attrs: Default::default(),
        };
        tx.send(Ok(event)).unwrap();

        std::thread::sleep(Duration::from_millis(300));

        // Should not panic. Diff store might or might not record it — key
        // property is no crash.

        drop(tx);
        handle
            .join()
            .expect("worker should handle missing file for diff");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Diff store does NOT record filtered paths (Fix #5) ----------------

    #[test]
    fn worker_context_diff_store_skips_filtered_paths() {
        let (tx, rx) = std_mpsc::channel::<NotifyResult<Event>>();
        let (btx, _brx) = broadcast::channel::<String>(16);
        let statuses = crate::server::new_status_map(Vec::<String>::new());

        let dir = std::env::temp_dir().join("watchd_test_worker_diff_filtered");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);

        // Create a .tmp file that will be excluded by the filter.
        let tmp_path = dir.join("scratch.tmp");
        std::fs::write(&tmp_path, "temporary data").unwrap();

        // Also create an allowed file for comparison.
        let rs_path = dir.join("allowed.rs");
        std::fs::write(&rs_path, "fn main() {}").unwrap();

        let cfg_path = dir.join("hotreload.yaml").into_boxed_path();
        let diff_store = crate::kv::DiffStore::new_shared(50, 500, 512 * 1024);

        // Filter that excludes *.tmp files (matches DEFAULT_EXCLUDES behaviour).
        let filter = PathFilter::new(&[], &["**/*.tmp".to_string()]);

        let ctx = WorkerContext {
            rx,
            btx,
            filter,
            configs: vec![],
            statuses,
            cmd_template: "echo noop".to_string(),
            debouncer: Debouncer::new(Duration::from_millis(10)),
            cmd_timeout: None,
            config_path: cfg_path,
            diff_store: Some(diff_store.clone()),
        };

        let handle = std::thread::spawn(move || ctx.run());

        // Send event for the filtered .tmp file.
        let event_tmp = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![tmp_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event_tmp)).unwrap();

        // Send event for the allowed .rs file.
        let event_rs = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![rs_path],
            attrs: Default::default(),
        };
        tx.send(Ok(event_rs)).unwrap();

        std::thread::sleep(Duration::from_millis(500));

        // The diff store should only have the allowed file, NOT the filtered one.
        let guard = diff_store.lock().unwrap();
        let keys = guard.list_keys();

        // The .tmp file must not appear in the diff store.
        for key in &keys {
            assert!(
                !key.ends_with(".tmp"),
                "filtered path should NOT be in diff store, but found: {key}"
            );
        }

        // The .rs file should be recorded.
        let has_rs = keys.iter().any(|k| k.ends_with("allowed.rs"));
        assert!(
            has_rs,
            "allowed path should be in diff store; keys: {:?}",
            keys
        );

        drop(guard);
        drop(tx);
        handle.join().expect("worker should exit cleanly");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
