//! Worker thread health monitoring and self-healing.
//!
//! This module provides a lightweight watchdog that monitors the worker thread
//! spawned by [`crate::watcher::start`].  If the worker thread terminates
//! unexpectedly (panic that escapes `catch_unwind`, or any other fatal error),
//! the watchdog detects this and triggers a coordinated shutdown via the
//! [`CancellationToken`] so that the rest of the application (WebSocket server,
//! control socket) does not continue running in a degraded state with no file
//! watcher.
//!
//! ## Design
//!
//! The [`WorkerWatchdog`] wraps a [`std::thread::JoinHandle`] and is polled
//! periodically by a background Tokio task.  The poll interval is configurable
//! but defaults to 5 seconds — frequent enough to detect failures quickly
//! without adding measurable overhead.
//!
//! ## Recovery strategy
//!
//! The current strategy is **fail-fast**: if the worker dies, the entire
//! process shuts down gracefully.  This is intentional — a file watcher that
//! silently stops watching files is worse than a crash, because the user gets
//! no feedback that changes are no longer being detected.
//!
//! Automatic restart of the worker thread is intentionally *not* implemented
//! because:
//! 1. The `notify` crate's `RecommendedWatcher` holds OS-level file handles
//!    that cannot be transferred to a new thread.
//! 2. A restart would require re-creating the watcher, re-reading configs,
//!    and re-establishing the event channel — essentially restarting the
//!    application, which is better done by an external process supervisor.
//!
//! For production deployments, use a process supervisor (systemd, Docker
//! restart policy, etc.) to restart `quay` automatically on exit.

use std::thread::JoinHandle;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default interval between health checks of the worker thread.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Minimum poll interval to prevent busy-looping.
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// WorkerWatchdog
// ---------------------------------------------------------------------------

/// Monitors a worker thread and triggers shutdown if it terminates unexpectedly.
///
/// # Usage
///
/// ```ignore
/// let handle = std::thread::spawn(|| { /* worker loop */ });
/// let watchdog = WorkerWatchdog::new(handle, cancel.clone());
/// watchdog.spawn();
/// ```
pub struct WorkerWatchdog {
    /// Join handle for the worker thread.
    handle: Option<JoinHandle<()>>,
    /// Cancellation token to signal coordinated shutdown.
    cancel: CancellationToken,
    /// How often to check if the worker thread is still alive.
    poll_interval: Duration,
}

impl WorkerWatchdog {
    /// Create a new watchdog for the given worker thread.
    pub fn new(handle: JoinHandle<()>, cancel: CancellationToken) -> Self {
        Self {
            handle: Some(handle),
            cancel,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Override the default poll interval.
    ///
    /// Values below [`MIN_POLL_INTERVAL`] are clamped to prevent busy-looping.
    #[allow(dead_code)]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = if interval < MIN_POLL_INTERVAL {
            warn!(
                requested_ms = interval.as_millis() as u64,
                min_ms = MIN_POLL_INTERVAL.as_millis() as u64,
                "watchdog poll interval too low; clamping to minimum"
            );
            MIN_POLL_INTERVAL
        } else {
            interval
        };
        self
    }

    /// Spawn the watchdog as a background Tokio task.
    ///
    /// The task runs until:
    /// - The worker thread terminates (triggers shutdown), or
    /// - The cancellation token is cancelled (normal shutdown).
    ///
    /// Returns a [`tokio::task::JoinHandle`] for the watchdog task itself,
    /// which can be awaited if desired but is not required.
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        let cancel = self.cancel.clone();
        let poll_interval = self.poll_interval;
        let handle = self
            .handle
            .take()
            .expect("WorkerWatchdog::spawn called twice");

        tokio::spawn(async move {
            info!(
                poll_interval_ms = poll_interval.as_millis() as u64,
                "worker watchdog started"
            );

            // We cannot call `handle.join()` from an async context (it blocks),
            // and we cannot call `handle.is_finished()` without consuming the
            // handle.  Instead we use `is_finished()` in a poll loop.
            loop {
                tokio::select! {
                    () = cancel.cancelled() => {
                        info!("worker watchdog: shutdown signal received; stopping");
                        // Normal shutdown — try to join the worker thread to
                        // ensure clean exit.  Use a bounded wait so we don't
                        // hang forever if the worker is stuck.
                        let join_deadline = tokio::time::sleep(Duration::from_secs(5));
                        tokio::pin!(join_deadline);

                        loop {
                            if handle.is_finished() {
                                info!("worker thread joined cleanly");
                                break;
                            }
                            tokio::select! {
                                () = tokio::time::sleep(Duration::from_millis(50)) => {}
                                () = &mut join_deadline => {
                                    warn!("worker thread did not exit within 5s; abandoning join");
                                    break;
                                }
                            }
                        }

                        return;
                    }
                    () = tokio::time::sleep(poll_interval) => {
                        if handle.is_finished() {
                            error!(
                                "worker thread terminated unexpectedly; \
                                 initiating graceful shutdown"
                            );
                            cancel.cancel();

                            // Try to extract the panic message for diagnostics.
                            match handle.join() {
                                Ok(()) => {
                                    error!(
                                        "worker thread exited normally (channel closed?) \
                                         but was expected to run indefinitely"
                                    );
                                }
                                Err(panic_payload) => {
                                    let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                                        (*s).to_string()
                                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                                        s.clone()
                                    } else {
                                        "unknown panic payload".to_string()
                                    };
                                    error!(
                                        panic = %msg,
                                        "worker thread panic details"
                                    );
                                }
                            }

                            return;
                        }
                    }
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    // -- Construction ------------------------------------------------------

    #[test]
    fn new_sets_default_poll_interval() {
        let handle = std::thread::spawn(|| {});
        let cancel = CancellationToken::new();
        let wd = WorkerWatchdog::new(handle, cancel);
        assert_eq!(wd.poll_interval, DEFAULT_POLL_INTERVAL);
    }

    #[test]
    fn with_poll_interval_sets_custom() {
        let handle = std::thread::spawn(|| {});
        let cancel = CancellationToken::new();
        let wd = WorkerWatchdog::new(handle, cancel).with_poll_interval(Duration::from_secs(10));
        assert_eq!(wd.poll_interval, Duration::from_secs(10));
    }

    #[test]
    fn with_poll_interval_clamps_low_values() {
        let handle = std::thread::spawn(|| {});
        let cancel = CancellationToken::new();
        let wd = WorkerWatchdog::new(handle, cancel).with_poll_interval(Duration::from_millis(1));
        assert_eq!(wd.poll_interval, MIN_POLL_INTERVAL);
    }

    #[test]
    fn with_poll_interval_zero_is_clamped() {
        let handle = std::thread::spawn(|| {});
        let cancel = CancellationToken::new();
        let wd = WorkerWatchdog::new(handle, cancel).with_poll_interval(Duration::ZERO);
        assert_eq!(wd.poll_interval, MIN_POLL_INTERVAL);
    }

    #[test]
    fn with_poll_interval_exact_minimum_accepted() {
        let handle = std::thread::spawn(|| {});
        let cancel = CancellationToken::new();
        let wd = WorkerWatchdog::new(handle, cancel).with_poll_interval(MIN_POLL_INTERVAL);
        assert_eq!(wd.poll_interval, MIN_POLL_INTERVAL);
    }

    // -- Watchdog detects dead thread --------------------------------------

    #[tokio::test]
    async fn detects_thread_exit_and_cancels() {
        let cancel = CancellationToken::new();

        // Spawn a thread that exits immediately.
        let handle = std::thread::spawn(|| {
            // Exits right away — simulates unexpected termination.
        });

        let wd = WorkerWatchdog::new(handle, cancel.clone())
            .with_poll_interval(Duration::from_millis(500));

        wd.spawn();

        // Wait for the watchdog to detect the dead thread.
        tokio::time::timeout(Duration::from_secs(5), cancel.cancelled())
            .await
            .expect("watchdog should have cancelled within 5s");

        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn detects_panicked_thread_and_cancels() {
        let cancel = CancellationToken::new();

        // Spawn a thread that panics.
        let handle = std::thread::spawn(|| {
            panic!("simulated worker panic");
        });

        // Give the thread time to panic.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let wd = WorkerWatchdog::new(handle, cancel.clone())
            .with_poll_interval(Duration::from_millis(500));

        wd.spawn();

        // Wait for the watchdog to detect the panic.
        tokio::time::timeout(Duration::from_secs(5), cancel.cancelled())
            .await
            .expect("watchdog should have cancelled within 5s");

        assert!(cancel.is_cancelled());
    }

    // -- Normal shutdown ---------------------------------------------------

    #[tokio::test]
    async fn stops_on_cancellation() {
        let cancel = CancellationToken::new();
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        // Spawn a thread that runs until told to stop.
        let handle = std::thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(50));
            }
        });

        let wd = WorkerWatchdog::new(handle, cancel.clone())
            .with_poll_interval(Duration::from_millis(500));

        let watchdog_task = wd.spawn();

        // Give the watchdog a moment to start.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Trigger normal shutdown.
        running.store(false, Ordering::Relaxed);
        cancel.cancel();

        // Watchdog task should complete.
        tokio::time::timeout(Duration::from_secs(10), watchdog_task)
            .await
            .expect("watchdog task should complete within 10s")
            .expect("watchdog task should not panic");
    }

    // -- Long-running worker is not falsely flagged ------------------------

    #[tokio::test]
    async fn does_not_false_positive_on_live_thread() {
        let cancel = CancellationToken::new();
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        // Spawn a thread that keeps running.
        let handle = std::thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(50));
            }
        });

        let wd = WorkerWatchdog::new(handle, cancel.clone())
            .with_poll_interval(Duration::from_millis(500));

        wd.spawn();

        // Wait a few poll cycles — the cancel should NOT fire.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            !cancel.is_cancelled(),
            "watchdog should not cancel while worker is alive"
        );

        // Clean up.
        running.store(false, Ordering::Relaxed);
        cancel.cancel();

        // Give time for shutdown.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // -- Constants ---------------------------------------------------------

    #[test]
    fn default_poll_interval_is_5s() {
        assert_eq!(DEFAULT_POLL_INTERVAL, Duration::from_secs(5));
    }

    #[test]
    fn min_poll_interval_is_500ms() {
        assert_eq!(MIN_POLL_INTERVAL, Duration::from_millis(500));
    }
}
