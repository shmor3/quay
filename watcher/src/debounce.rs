//! Per-path event debouncer for the file watcher.
//!
//! The [`Debouncer`] suppresses duplicate file-system events for the same path
//! within a configurable time window.  This prevents redundant rebuilds when
//! editors emit multiple write events for a single save operation or when OS
//! backends deliver burst notifications.
//!
//! ## Memory safety
//!
//! The internal map is periodically pruned to prevent unbounded growth during
//! long-running sessions.  Entries older than 10× the debounce window are
//! considered stale and removed every [`prune_interval`](Debouncer::prune_interval)
//! events.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Debouncer
// ---------------------------------------------------------------------------

/// A simple per-path debouncer that suppresses duplicate events within a
/// configurable time window.
///
/// The internal map is periodically pruned to prevent unbounded growth during
/// long-running sessions.
pub(crate) struct Debouncer {
    /// Minimum interval between handling the same path.
    pub(crate) window: Duration,
    /// Last time each path was handled.
    pub(crate) last_seen: HashMap<String, Instant>,
    /// Number of events processed since the last prune.
    pub(crate) events_since_prune: u64,
    /// Prune the map every N events.
    pub(crate) prune_interval: u64,
}

impl Debouncer {
    /// Create a new debouncer with the given window duration.
    ///
    /// A minimum window of 1 ms is enforced — a zero-duration window would
    /// effectively disable debouncing and could cause the worker to process
    /// the same event multiple times from backends that emit bursts.
    pub(crate) fn new(window: Duration) -> Self {
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
    /// Internally the map is pruned every [`prune_interval`](Self::prune_interval)
    /// events to prevent unbounded growth during long-running sessions.
    /// Entries older than 10× the debounce window are considered stale and
    /// removed.
    pub(crate) fn should_handle(&mut self, path: &str) -> bool {
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Construction & defaults -------------------------------------------

    #[test]
    fn new_sets_default_prune_interval() {
        let d = Debouncer::new(Duration::from_millis(200));
        assert_eq!(d.prune_interval, 1000);
        assert_eq!(d.events_since_prune, 0);
    }

    #[test]
    fn new_stores_window() {
        let d = Debouncer::new(Duration::from_millis(500));
        assert_eq!(d.window, Duration::from_millis(500));
    }

    #[test]
    fn new_starts_with_empty_map() {
        let d = Debouncer::new(Duration::from_millis(200));
        assert!(d.last_seen.is_empty());
    }

    // -- Zero window clamping ----------------------------------------------

    #[test]
    fn zero_window_clamped_to_1ms() {
        let d = Debouncer::new(Duration::from_millis(0));
        assert!(d.window >= Duration::from_millis(1));
    }

    #[test]
    fn zero_window_still_debounces() {
        let mut d = Debouncer::new(Duration::from_millis(0));
        assert!(d.should_handle("file.txt"));
        // Immediate second event should be suppressed (1ms window).
        assert!(!d.should_handle("file.txt"));
    }

    // -- Basic behaviour ---------------------------------------------------

    #[test]
    fn allows_first_event() {
        let mut d = Debouncer::new(Duration::from_millis(200));
        assert!(d.should_handle("src/main.rs"));
    }

    #[test]
    fn suppresses_rapid_duplicates() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        assert!(d.should_handle("src/main.rs"));
        // Second call within the window should be suppressed.
        assert!(!d.should_handle("src/main.rs"));
    }

    #[test]
    fn allows_different_paths() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        assert!(d.should_handle("a.txt"));
        assert!(d.should_handle("b.txt"));
    }

    #[test]
    fn rapid_same_path_only_first_passes() {
        let mut d = Debouncer::new(Duration::from_secs(60));
        assert!(d.should_handle("same.txt"));
        for _ in 0..100 {
            assert!(!d.should_handle("same.txt"));
        }
    }

    // -- Window expiry -----------------------------------------------------

    #[test]
    fn allows_after_window_expires() {
        let mut d = Debouncer::new(Duration::from_millis(10));
        assert!(d.should_handle("file.rs"));
        // Wait for the debounce window to expire.
        std::thread::sleep(Duration::from_millis(20));
        // Should be allowed again.
        assert!(d.should_handle("file.rs"));
    }

    #[test]
    fn boundary_window() {
        let mut d = Debouncer::new(Duration::from_millis(50));
        assert!(d.should_handle("boundary.txt"));
        // Sleep just past the window.
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            d.should_handle("boundary.txt"),
            "should pass after window expires"
        );
    }

    // -- Large/edge window values ------------------------------------------

    #[test]
    fn very_large_window() {
        let mut d = Debouncer::new(Duration::from_secs(3600)); // 1 hour
        assert!(d.should_handle("a.txt"));
        assert!(!d.should_handle("a.txt")); // within window
        assert!(d.should_handle("b.txt")); // different path
    }

    // -- Many distinct paths -----------------------------------------------

    #[test]
    fn many_distinct_paths() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        for i in 0..1000 {
            let path = format!("path/{}/file_{}.rs", i / 10, i);
            assert!(d.should_handle(&path));
        }
        // All 1000 distinct paths should be in the map.
        assert_eq!(d.last_seen.len(), 1000);
    }

    // -- Pruning -----------------------------------------------------------

    #[test]
    fn prune_removes_stale_entries() {
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

    #[test]
    fn prune_keeps_recent_entries() {
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
    fn prune_actually_removes_stale() {
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
    fn heavy_prune_then_reuse() {
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

    // -- Special path values -----------------------------------------------

    #[test]
    fn empty_path() {
        let mut d = Debouncer::new(Duration::from_millis(200));
        assert!(d.should_handle(""));
        assert!(!d.should_handle(""));
    }

    #[test]
    fn unicode_path() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        assert!(d.should_handle("路径/文件.txt"));
        assert!(!d.should_handle("路径/文件.txt"));
        assert!(d.should_handle("дорожка/файл.txt"));
    }

    #[test]
    fn very_long_path() {
        let mut d = Debouncer::new(Duration::from_secs(10));
        let long_path = "a/".repeat(5000) + "file.txt";
        assert!(d.should_handle(&long_path));
        assert!(!d.should_handle(&long_path));
    }

    #[test]
    fn special_chars_in_path() {
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
}
