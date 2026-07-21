//! Shell escaping and command execution utilities.
//!
//! This module provides a single, canonical implementation of shell escaping
//! and blocking command execution used throughout the application.  Having one
//! implementation avoids duplication between the `config` and `watcher` modules
//! and ensures that security-sensitive escaping logic is maintained in exactly
//! one place.
//!
//! ## Security
//!
//! [`shell_escape`] prevents command injection when file paths containing shell
//! metacharacters (`;`, `|`, `$()`, backticks, etc.) are interpolated into
//! command templates via the `{path}` placeholder.
//!
//! [`run_command_blocking`] spawns commands through the platform shell (`sh -c`
//! on Unix, `cmd /C` on Windows) and optionally enforces a timeout so that
//! stuck builds cannot block the worker thread indefinitely.

use std::process::Command;
use std::time::{Duration, Instant};
use tracing::{error, warn};

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
///
/// # Examples
///
/// ```
/// # // Platform-dependent examples shown for illustration.
/// # #[cfg(not(target_os = "windows"))]
/// # {
/// # use quay_watcher::command::shell_escape;
/// assert_eq!(shell_escape("src/main.rs"), "'src/main.rs'");
/// assert_eq!(shell_escape("it's a file"), "'it'\\''s a file'");
/// # }
/// ```
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

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Run a shell command synchronously (blocking the current thread).
///
/// On Windows the command is executed via `cmd /C`; on Unix via `sh -c`.
///
/// When `timeout` is `Some`, the child process is killed if it has not
/// completed within the given duration.  The poll interval is 50 ms.
///
/// # Error handling
///
/// Errors are logged but do **not** propagate — a failing build command should
/// not crash the watcher.  Non-zero exit codes are logged at `warn` level;
/// spawn/wait failures are logged at `error` level.
pub fn run_command_blocking(
    cmd: &str,
    timeout: Option<Duration>,
    _max_memory_mb: Option<u32>,
    _max_cpu_seconds: Option<u32>,
) {
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

    #[test]
    fn shell_escape_semicolon_injection() {
        let escaped = shell_escape("file.txt; rm -rf /");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"file.txt; rm -rf /\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'file.txt; rm -rf /'");
    }

    #[test]
    fn shell_escape_glob_characters() {
        let escaped = shell_escape("path/*/file?.rs");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"path/*/file?.rs\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'path/*/file?.rs'");
    }

    #[test]
    fn shell_escape_parentheses_and_braces() {
        let escaped = shell_escape("dir/{a,b}/(c).rs");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"dir/{a,b}/(c).rs\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'dir/{a,b}/(c).rs'");
    }

    #[test]
    fn shell_escape_backslash() {
        let escaped = shell_escape("path\\to\\file");
        #[cfg(target_os = "windows")]
        assert_eq!(escaped, "\"path\\to\\file\"");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(escaped, "'path\\to\\file'");
    }

    // -- run_command_blocking ----------------------------------------------

    #[test]
    fn run_command_blocking_handles_echo() {
        // Just ensure it doesn't panic.
        run_command_blocking("echo hello", None, None, None);
    }

    #[test]
    fn run_command_blocking_respects_timeout() {
        // A long-running command should be killed by a short timeout.
        // We use a generous threshold so CI doesn't flake.
        let start = Instant::now();
        #[cfg(target_os = "windows")]
        run_command_blocking(
            "ping -n 10 127.0.0.1",
            Some(Duration::from_millis(500)),
            None,
            None,
        );
        #[cfg(not(target_os = "windows"))]
        run_command_blocking("sleep 10", Some(Duration::from_millis(500)), None, None);
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
        run_command_blocking("echo fast", Some(Duration::from_secs(30)), None, None);
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(5));
    }

    #[test]
    fn run_command_blocking_nonexistent_command() {
        // Should log an error but not panic.
        run_command_blocking("this_command_does_not_exist_12345", None, None, None);
    }

    #[test]
    fn run_command_blocking_empty_command() {
        // Empty command — the shell should handle it gracefully.
        // On some shells this is a no-op; on others it may fail.
        // The key property is no panic.
        run_command_blocking("", None, None, None);
    }

    #[test]
    fn run_command_blocking_failing_command() {
        // A command that exits with non-zero should log a warning but not panic.
        #[cfg(target_os = "windows")]
        run_command_blocking("cmd /C exit 1", None, None, None);
        #[cfg(not(target_os = "windows"))]
        run_command_blocking("false", None, None, None);
    }

    #[test]
    fn run_command_blocking_zero_timeout() {
        // A zero-ms timeout should still work (command likely killed immediately).
        run_command_blocking("echo quick", Some(Duration::from_millis(0)), None, None);
    }

    #[test]
    fn run_command_blocking_generous_timeout() {
        // A very generous timeout with a fast command should complete normally.
        let start = Instant::now();
        run_command_blocking("echo hello", Some(Duration::from_secs(60)), None, None);
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn run_command_blocking_with_special_chars_in_args() {
        // Command with quotes and special characters — should not panic.
        run_command_blocking("echo \"hello world\"", None, None, None);
        run_command_blocking("echo path/with spaces/file.txt", None, None, None);
    }

    #[test]
    fn run_command_blocking_timeout_near_boundary() {
        let start = Instant::now();
        #[cfg(target_os = "windows")]
        run_command_blocking(
            "ping -n 100 127.0.0.1",
            Some(Duration::from_millis(200)),
            None,
            None,
        );
        #[cfg(not(target_os = "windows"))]
        run_command_blocking("sleep 100", Some(Duration::from_millis(200)), None, None);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(3),
            "should have been killed quickly, took {:?}",
            elapsed
        );
    }
}
