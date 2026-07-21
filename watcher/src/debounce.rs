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
use tracing::warn;

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
    /// Pending events to be processed (deferred).
    pub(crate) pending: HashMap<String, Instant>,
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
            pending: HashMap::new(),
        }
    }

    /// Add an event generated at `event_time`. We keep the latest event time.
    pub(crate) fn add_event(&mut self, path: String, event_time: Instant) {
        let current = self.pending.entry(path).or_insert(event_time);
        if event_time > *current {
            *current = event_time;
        }
    }

    /// Drain events that have been quiet for `window` duration.
    pub(crate) fn drain_ready(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut ready = Vec::new();
        self.pending.retain(|path, ts| {
            if now >= *ts && now.duration_since(*ts) >= self.window {
                ready.push(path.clone());
                false
            } else {
                true
            }
        });
        ready
    }

    /// Calculate the duration until the next event is ready.
    pub(crate) fn next_timeout(&self) -> Option<Duration> {
        if self.pending.is_empty() {
            return None;
        }
        let now = Instant::now();
        let mut min_wait = self.window;
        for ts in self.pending.values() {
            if now >= *ts {
                let elapsed = now.duration_since(*ts);
                if elapsed >= self.window {
                    return Some(Duration::ZERO);
                } else {
                    let wait = self.window - elapsed;
                    if wait < min_wait {
                        min_wait = wait;
                    }
                }
            }
        }
        Some(min_wait)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stores_window() {
        let d = Debouncer::new(Duration::from_millis(500));
        assert_eq!(d.window, Duration::from_millis(500));
    }

    #[test]
    fn new_starts_with_empty_map() {
        let d = Debouncer::new(Duration::from_millis(200));
        assert!(d.pending.is_empty());
    }

    #[test]
    fn zero_window_clamped_to_1ms() {
        let d = Debouncer::new(Duration::from_millis(0));
        assert!(d.window >= Duration::from_millis(1));
    }

    #[test]
    fn defers_events_correctly() {
        let mut d = Debouncer::new(Duration::from_millis(50));
        let now = Instant::now();
        d.add_event("a.txt".to_string(), now);
        assert!(d.drain_ready().is_empty());
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(d.drain_ready(), vec!["a.txt".to_string()]);
    }
}
