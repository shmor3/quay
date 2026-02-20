//! Centralized error types for the watchd application.

use std::path::PathBuf;

/// Top-level error type for the watchd application.
///
/// Some variants are not yet constructed but are defined for completeness and
/// future use by downstream consumers of this module.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum WatchdError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("file watcher error: {0}")]
    Notify(#[from] notify::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] Box<tokio_tungstenite::tungstenite::Error>),

    #[error("failed to parse config at {path}: {reason}")]
    ConfigParse { path: PathBuf, reason: String },

    #[error("invalid glob pattern '{pattern}': {reason}")]
    InvalidGlob { pattern: String, reason: String },

    #[error("failed to bind to {addr}: {source}")]
    Bind {
        addr: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to execute command '{cmd}': {reason}")]
    CommandExec { cmd: String, reason: String },

    #[error("failed to connect to control socket at {addr}: {source}")]
    ControlConnect {
        addr: String,
        #[source]
        source: std::io::Error,
    },
}

/// Convenience alias used throughout the application.
pub type Result<T> = std::result::Result<T, WatchdError>;
