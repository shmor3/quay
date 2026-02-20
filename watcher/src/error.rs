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
        let err = WatchdError::Io(inner);
        let msg = err.to_string();
        assert!(msg.contains("I/O error"), "got: {msg}");
        assert!(msg.contains("file gone"), "got: {msg}");
    }

    #[test]
    fn notify_error_display() {
        let inner = notify::Error::generic("watcher exploded");
        let err = WatchdError::Notify(inner);
        let msg = err.to_string();
        assert!(msg.contains("file watcher error"), "got: {msg}");
        assert!(msg.contains("watcher exploded"), "got: {msg}");
    }

    #[test]
    fn websocket_error_display() {
        let inner = tokio_tungstenite::tungstenite::Error::ConnectionClosed;
        let err = WatchdError::WebSocket(Box::new(inner));
        let msg = err.to_string();
        assert!(msg.contains("WebSocket error"), "got: {msg}");
    }

    #[test]
    fn config_parse_display() {
        let err = WatchdError::ConfigParse {
            path: PathBuf::from("/tmp/hotreload.yaml"),
            reason: "invalid YAML syntax".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("failed to parse config"), "got: {msg}");
        assert!(msg.contains("hotreload.yaml"), "got: {msg}");
        assert!(msg.contains("invalid YAML syntax"), "got: {msg}");
    }

    #[test]
    fn invalid_glob_display() {
        let err = WatchdError::InvalidGlob {
            pattern: "[unclosed".to_string(),
            reason: "missing closing bracket".to_string(),
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
        assert!(matches!(err, WatchdError::Io(_)));
    }

    #[test]
    fn from_notify_error() {
        let notify_err = notify::Error::generic("boom");
        let err: WatchdError = notify_err.into();
        assert!(matches!(err, WatchdError::Notify(_)));
    }

    #[test]
    fn from_boxed_websocket_error() {
        let ws_err = Box::new(tokio_tungstenite::tungstenite::Error::ConnectionClosed);
        let err: WatchdError = ws_err.into();
        assert!(matches!(err, WatchdError::WebSocket(_)));
    }

    // -- Source chains -----------------------------------------------------

    #[test]
    fn bind_error_has_source() {
        let inner = std::io::Error::new(std::io::ErrorKind::AddrInUse, "in use");
        let err = WatchdError::Bind {
            addr: "0.0.0.0:80".to_string(),
            source: inner,
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
        };
        let source = err
            .source()
            .expect("ControlConnect variant should have a source");
        assert!(source.to_string().contains("refused"));
    }

    #[test]
    fn io_error_has_source() {
        let inner = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let err = WatchdError::Io(inner);
        // thiserror transparent-wraps #[from] so source is the inner error.
        let source = err.source().expect("Io variant should have a source");
        assert!(source.to_string().contains("pipe broken"));
    }

    #[test]
    fn config_parse_has_no_source() {
        let err = WatchdError::ConfigParse {
            path: PathBuf::from("x.yaml"),
            reason: "bad".to_string(),
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
        });
        assert!(r.is_err());
    }

    // -- Debug impl -------------------------------------------------------

    #[test]
    fn all_variants_implement_debug() {
        // Ensures Debug is derived and does not panic for any variant.
        let variants: Vec<WatchdError> = vec![
            WatchdError::Io(std::io::Error::other("x")),
            WatchdError::Notify(notify::Error::generic("x")),
            WatchdError::WebSocket(Box::new(
                tokio_tungstenite::tungstenite::Error::ConnectionClosed,
            )),
            WatchdError::ConfigParse {
                path: PathBuf::from("a"),
                reason: "b".into(),
            },
            WatchdError::InvalidGlob {
                pattern: "c".into(),
                reason: "d".into(),
            },
            WatchdError::Bind {
                addr: "e".into(),
                source: std::io::Error::other("f"),
            },
            WatchdError::CommandExec {
                cmd: "g".into(),
                reason: "h".into(),
            },
            WatchdError::ControlConnect {
                addr: "i".into(),
                source: std::io::Error::other("j"),
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
