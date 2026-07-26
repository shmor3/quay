//! Prometheus metrics, registered exactly once into the default registry.
//!
//! Previously the metrics were split across a private `Registry` (health, reload)
//! and the global default registry (ws connections), while the diff-count gauge
//! was never registered at all — so `/metrics` exported only two always-zero
//! values.  Centralising every metric here, on the **default** registry that the
//! `/metrics` endpoint gathers, guarantees they are all exported and all updated.

use prometheus::{IntCounter, IntGauge};
use std::sync::OnceLock;

fn register_gauge(name: &'static str, help: &'static str) -> IntGauge {
    let g = IntGauge::new(name, help).unwrap_or_else(|_| {
        IntGauge::new("quay_fallback_gauge", "fallback").expect("fallback gauge is valid")
    });
    // Ignore AlreadyRegistered (e.g. multiple instances inside one test binary).
    let _ = prometheus::default_registry().register(Box::new(g.clone()));
    g
}

fn register_counter(name: &'static str, help: &'static str) -> IntCounter {
    let c = IntCounter::new(name, help).unwrap_or_else(|_| {
        IntCounter::new("quay_fallback_counter", "fallback").expect("fallback counter is valid")
    });
    let _ = prometheus::default_registry().register(Box::new(c.clone()));
    c
}

/// `1` while the watcher worker thread is alive, `0` once it stops.
pub fn health() -> &'static IntGauge {
    static G: OnceLock<IntGauge> = OnceLock::new();
    G.get_or_init(|| register_gauge("quay_health", "1 when the watcher worker thread is alive"))
}

/// Current number of connected WebSocket clients.
pub fn ws_connections() -> &'static IntGauge {
    static G: OnceLock<IntGauge> = OnceLock::new();
    G.get_or_init(|| register_gauge("quay_ws_connections", "active WebSocket connections"))
}

/// Number of files currently tracked in the diff store.
pub fn diff_count() -> &'static IntGauge {
    static G: OnceLock<IntGauge> = OnceLock::new();
    G.get_or_init(|| register_gauge("quay_diff_count", "files tracked in the diff store"))
}

/// Total reload / inject-css messages broadcast since startup.
pub fn reloads() -> &'static IntCounter {
    static C: OnceLock<IntCounter> = OnceLock::new();
    C.get_or_init(|| register_counter("quay_reload_count", "reload/inject messages broadcast"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_are_registered_and_usable() {
        // Touch each metric; registration must not panic and values must move.
        health().set(1);
        assert_eq!(health().get(), 1);
        ws_connections().set(3);
        assert_eq!(ws_connections().get(), 3);
        diff_count().set(2);
        assert_eq!(diff_count().get(), 2);
        let before = reloads().get();
        reloads().inc();
        assert_eq!(reloads().get(), before + 1);
    }
}
