//! Centralized error types for the quay application.

use std::path::PathBuf;

/// Top-level error type for the quay application.
///
/// Some variants are not yet constructed but are defined for completeness and
/// future use by downstream consumers of this module.

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum WatchdError {
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error, Option<String>),

    #[error("file watcher error: {0}")]
    Notify(#[source] notify::Error, Option<String>),

    #[error("WebSocket error: {0}")]
    WebSocket(
        #[source] Box<tokio_tungstenite::tungstenite::Error>,
        Option<String>,
    ),

    #[error("failed to parse config at {path}: {reason}")]
    ConfigParse {
        path: PathBuf,
        reason: String,
        user_message: Option<String>,
    },

    #[error("invalid glob pattern '{pattern}': {reason}")]
    InvalidGlob {
        pattern: String,
        reason: String,
        user_message: Option<String>,
    },

    #[error("failed to bind to {addr}: {source}")]
    Bind {
        addr: String,
        #[source]
        source: std::io::Error,
        user_message: Option<String>,
    },

    #[error("failed to execute command '{cmd}': {reason}")]
    CommandExec {
        cmd: String,
        reason: String,
        user_message: Option<String>,
    },

    #[error("failed to connect to control socket at {addr}: {source}")]
    ControlConnect {
        addr: String,
        #[source]
        source: std::io::Error,
        user_message: Option<String>,
    },
}

impl WatchdError {
    /// The optional user-facing, actionable message attached to this error.
    ///
    /// The `Display` impl (via `thiserror`) renders the technical cause; callers
    /// that want to also show the friendlier hint print `user_message()`.
    pub fn user_message(&self) -> Option<&str> {
        match self {
            WatchdError::Io(_, m) | WatchdError::Notify(_, m) | WatchdError::WebSocket(_, m) => {
                m.as_deref()
            }
            WatchdError::ConfigParse { user_message, .. }
            | WatchdError::InvalidGlob { user_message, .. }
            | WatchdError::Bind { user_message, .. }
            | WatchdError::CommandExec { user_message, .. }
            | WatchdError::ControlConnect { user_message, .. } => user_message.as_deref(),
        }
    }
}

impl From<std::io::Error> for WatchdError {
    fn from(e: std::io::Error) -> Self {
        WatchdError::Io(e, None)
    }
}

impl From<notify::Error> for WatchdError {
    fn from(e: notify::Error) -> Self {
        WatchdError::Notify(e, None)
    }
}

impl From<Box<tokio_tungstenite::tungstenite::Error>> for WatchdError {
    fn from(e: Box<tokio_tungstenite::tungstenite::Error>) -> Self {
        WatchdError::WebSocket(e, None)
    }
}

/// Convenience alias used throughout the application.
pub type Result<T> = std::result::Result<T, WatchdError>;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;

    // -- Display messages --------------------------------------------------

    #[test]
    fn io_error_display() {
        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "file gone");
        let err = WatchdError::Io(inner, None);
        let msg = err.to_string();
        assert!(msg.contains("I/O error"), "got: {msg}");
        assert!(msg.contains("file gone"), "got: {msg}");
    }

    #[test]
    fn notify_error_display() {
        let inner = notify::Error::generic("watcher exploded");
        let err = WatchdError::Notify(
            inner,
            Some("Check file watcher configuration and supported events.".to_string()),
        );
        let msg = err.to_string();
        assert!(msg.contains("file watcher error"), "got: {msg}");
        assert!(msg.contains("watcher exploded"), "got: {msg}");
    }

    #[test]
    fn websocket_error_display() {
        let inner = tokio_tungstenite::tungstenite::Error::ConnectionClosed;
        let err = WatchdError::WebSocket(
            Box::new(inner),
            Some("Check WebSocket server address and port.".to_string()),
        );
        let msg = err.to_string();
        assert!(msg.contains("WebSocket error"), "got: {msg}");
    }

    #[test]
    fn config_parse_display() {
        let err = WatchdError::ConfigParse {
            path: PathBuf::from("/tmp/quay.yaml"),
            reason: "invalid YAML syntax".to_string(),
            user_message: None,
        };
        let msg = err.to_string();
        assert!(msg.contains("failed to parse config"), "got: {msg}");
        assert!(msg.contains("quay.yaml"), "got: {msg}");
        assert!(msg.contains("invalid YAML syntax"), "got: {msg}");
    }

    #[test]
    fn invalid_glob_display() {
        let err = WatchdError::InvalidGlob {
            pattern: "[unclosed".to_string(),
            reason: "missing closing bracket".to_string(),
            user_message: None,
        };
        let msg = err.to_string();
        assert!(msg.contains("invalid glob pattern"), "got: {msg}");
        assert!(msg.contains("[unclosed"), "got: {msg}");
        assert!(msg.contains("missing closing bracket"), "got: {msg}");
    }

    #[test]
    fn bind_error_display() {
        let inner = std::io::Error::new(std::io::ErrorKind::AddrInUse, "port taken");
        let err = WatchdError::Bind {
            addr: "127.0.0.1:3012".to_string(),
            source: inner,
            user_message: None,
        };
        let msg = err.to_string();
        assert!(msg.contains("failed to bind"), "got: {msg}");
        assert!(msg.contains("127.0.0.1:3012"), "got: {msg}");
        assert!(msg.contains("port taken"), "got: {msg}");
    }

    #[test]
    fn command_exec_display() {
        let err = WatchdError::CommandExec {
            cmd: "npm run build".to_string(),
            reason: "process exited with code 1".to_string(),
            user_message: None,
        };
        let msg = err.to_string();
        assert!(msg.contains("failed to execute command"), "got: {msg}");
        assert!(msg.contains("npm run build"), "got: {msg}");
        assert!(msg.contains("process exited with code 1"), "got: {msg}");
    }

    #[test]
    fn control_connect_display() {
        let inner =
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let err = WatchdError::ControlConnect {
            addr: "127.0.0.1:3013".to_string(),
            source: inner,
            user_message: None,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("failed to connect to control socket"),
            "got: {msg}"
        );
        assert!(msg.contains("127.0.0.1:3013"), "got: {msg}");
        assert!(msg.contains("connection refused"), "got: {msg}");
    }

    // -- From impls --------------------------------------------------------

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: WatchdError = io_err.into();
        assert!(matches!(err, WatchdError::Io(_, _)));
    }

    #[test]
    fn from_notify_error() {
        let notify_err = notify::Error::generic("boom");
        let err: WatchdError = notify_err.into();
        assert!(matches!(err, WatchdError::Notify(_, _)));
    }

    #[test]
    fn from_boxed_websocket_error() {
        let ws_err = Box::new(tokio_tungstenite::tungstenite::Error::ConnectionClosed);
        let err: WatchdError = ws_err.into();
        assert!(matches!(err, WatchdError::WebSocket(_, _)));
    }

    // -- Source chains -----------------------------------------------------

    #[test]
    fn bind_error_has_source() {
        let inner = std::io::Error::new(std::io::ErrorKind::AddrInUse, "in use");
        let err = WatchdError::Bind {
            addr: "0.0.0.0:80".to_string(),
            source: inner,
            user_message: None,
        };
        let source = err.source().expect("Bind variant should have a source");
        assert!(source.to_string().contains("in use"));
    }

    #[test]
    fn control_connect_error_has_source() {
        let inner = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err = WatchdError::ControlConnect {
            addr: "localhost:3013".to_string(),
            source: inner,
            user_message: None,
        };
        let source = err
            .source()
            .expect("ControlConnect variant should have a source");
        assert!(source.to_string().contains("refused"));
    }

    #[test]
    fn io_error_has_source() {
        let inner = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let err = WatchdError::Io(inner, None);
        // thiserror transparent-wraps #[from] so source is the inner error.
        let source = err.source().expect("Io variant should have a source");
        assert!(source.to_string().contains("pipe broken"));
    }

    #[test]
    fn config_parse_has_no_source() {
        let err = WatchdError::ConfigParse {
            path: PathBuf::from("x.yaml"),
            reason: "bad".to_string(),
            user_message: None,
        };
        assert!(
            err.source().is_none(),
            "ConfigParse should not have a source"
        );
    }

    #[test]
    fn invalid_glob_has_no_source() {
        let err = WatchdError::InvalidGlob {
            pattern: "*".to_string(),
            reason: "bad".to_string(),
            user_message: None,
        };
        assert!(
            err.source().is_none(),
            "InvalidGlob should not have a source"
        );
    }

    #[test]
    fn command_exec_has_no_source() {
        let err = WatchdError::CommandExec {
            cmd: "make".to_string(),
            reason: "failed".to_string(),
            user_message: None,
        };
        assert!(
            err.source().is_none(),
            "CommandExec should not have a source"
        );
    }

    // -- Result alias ------------------------------------------------------

    #[test]
    fn result_alias_ok() {
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }
        let r = returns_ok();
        assert_eq!(r.unwrap(), 42);
    }

    #[test]
    fn result_alias_err() {
        let r: Result<i32> = Err(WatchdError::CommandExec {
            cmd: "test".to_string(),
            reason: "fail".to_string(),
            user_message: None,
        });
        assert!(r.is_err());
    }

    // -- Debug impl -------------------------------------------------------

    #[test]
    fn all_variants_implement_debug() {
        // Ensures Debug is derived and does not panic for any variant.
        let variants: Vec<WatchdError> = vec![
            WatchdError::Io(std::io::Error::other("x"), None),
            WatchdError::Notify(notify::Error::generic("x"), None),
            WatchdError::WebSocket(
                Box::new(tokio_tungstenite::tungstenite::Error::ConnectionClosed),
                None,
            ),
            WatchdError::ConfigParse {
                path: PathBuf::from("a"),
                reason: "b".into(),
                user_message: None,
            },
            WatchdError::InvalidGlob {
                pattern: "c".into(),
                reason: "d".into(),
                user_message: None,
            },
            WatchdError::Bind {
                addr: "e".into(),
                source: std::io::Error::other("f"),
                user_message: None,
            },
            WatchdError::CommandExec {
                cmd: "g".into(),
                reason: "h".into(),
                user_message: None,
            },
            WatchdError::ControlConnect {
                addr: "i".into(),
                source: std::io::Error::other("j"),
                user_message: None,
            },
        ];
        for v in &variants {
            let dbg = format!("{:?}", v);
            assert!(!dbg.is_empty());
        }
    }

    // -- Send + Sync bounds ------------------------------------------------

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        // This will fail to compile if WatchdError is not Send + Sync.
        assert_send_sync::<WatchdError>();
    }
}
