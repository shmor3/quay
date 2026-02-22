//! Input validation for CLI arguments and configuration values.
//!
//! This module provides early validation of user-supplied inputs that go beyond
//! what `clap`'s type system can enforce.  Catching invalid values at startup
//! with clear error messages is preferable to mysterious failures later during
//! execution.
//!
//! ## Design philosophy
//!
//! Each validator returns a [`Result<(), String>`] with a human-readable error
//! message on failure.  The caller (typically `main.rs`) can log the error and
//! exit with a non-zero status code.
//!
//! Validators are intentionally lenient where possible — they warn on
//! questionable values but only reject values that are certain to cause
//! failures or undefined behaviour.

use tracing::warn;

// ---------------------------------------------------------------------------
// Bind address validation
// ---------------------------------------------------------------------------

/// Validate that the bind address is a plausible IP address or hostname.
///
/// This does **not** attempt DNS resolution — it only checks syntactic
/// validity.  The actual bind will fail with a clear OS error if the address
/// is unreachable or in use.
///
/// # Errors
///
/// Returns an error string if the address is empty or contains characters
/// that are clearly invalid for an IP address or hostname.
pub fn validate_bind_addr(addr: &str, expose_network: bool) -> Result<(), String> {
    if addr.is_empty() {
        return Err("bind address must not be empty".to_string());
    }

    // Reject addresses that contain whitespace — a common copy-paste mistake.
    if addr.chars().any(char::is_whitespace) {
        return Err(format!(
            "bind address '{}' contains whitespace; expected an IP address or hostname",
            addr
        ));
    }

    // Reject addresses that contain shell metacharacters — these would never
    // be valid and likely indicate a quoting mistake.
    const INVALID_CHARS: &[char] = &[';', '|', '&', '$', '`', '(', ')', '{', '}', '<', '>', '!'];
    for ch in INVALID_CHARS {
        if addr.contains(*ch) {
            return Err(format!(
                "bind address '{}' contains invalid character '{}'",
                addr, ch
            ));
        }
    }

    // Warn (but allow) binding to all interfaces — this exposes the server
    // to the local network which may not be intended.
    if (addr == "0.0.0.0" || addr == "::") && !expose_network {
        return Err("binding to all interfaces is not allowed unless expose_network=true".to_string());
    } else if addr == "0.0.0.0" || addr == "::" {
        warn!(
            addr,
            "binding to all interfaces; the WebSocket and control servers will \
             be accessible from other machines on the network. Use 127.0.0.1 \
             to restrict to localhost only."
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Port validation
// ---------------------------------------------------------------------------

/// Validate that the port number allows a control socket on port + 1.
///
/// The control socket always binds to `port + 1`.  Port 65535 would cause
/// an overflow, so `main.rs` handles this separately with `checked_add`.
/// This validator catches other edge cases and emits warnings for
/// commonly-reserved port ranges.
///
/// # Errors
///
/// This function does not return errors — it only emits warnings for
/// potentially problematic port values.
pub fn warn_port_issues(port: u16) {
    if port == 0 {
        warn!(
            "port 0 requests an OS-assigned ephemeral port; the control socket \
             will bind to port 1, which is a reserved port and likely to fail"
        );
    }

    if port < 1024 && port > 0 {
        warn!(
            port,
            "ports below 1024 are privileged on most systems and may require \
             elevated permissions (sudo / administrator)"
        );
    }

    if port == 65535 {
        // This is a fatal condition handled in main.rs via checked_add.
        // We include a warning here as a belt-and-suspenders measure.
        warn!(
            "port 65535 cannot be used because the control socket requires \
             port + 1 (65536), which overflows u16"
        );
    }
}

// ---------------------------------------------------------------------------
// Debounce validation
// ---------------------------------------------------------------------------

/// Validate the debounce delay and warn about extreme values.
///
/// Very low debounce values can cause excessive rebuilds from editors that
/// emit multiple write events per save.  Very high values can make the
/// watcher feel unresponsive.
///
/// # Errors
///
/// Returns an error string if the debounce value is zero (which would disable
/// debouncing entirely — the [`Debouncer`](crate::debounce::Debouncer) clamps
/// to 1 ms, but it's better to tell the user up front).
pub fn validate_debounce_ms(ms: u64) -> Result<(), String> {
    if ms == 0 {
        return Err(
            "debounce delay of 0ms is not supported; the minimum effective \
             value is 1ms. Use --debounce-ms 1 for the fastest response."
                .to_string(),
        );
    }

    if ms < 50 {
        warn!(
            debounce_ms = ms,
            "debounce delay below 50ms may cause redundant rebuilds from editors \
             that emit multiple write events per save operation"
        );
    }

    if ms > 10_000 {
        warn!(
            debounce_ms = ms,
            "debounce delay above 10s will make the watcher feel very unresponsive; \
             typical values are 100-500ms"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Command timeout validation
// ---------------------------------------------------------------------------

/// Validate the command timeout and warn about extreme values.
pub fn validate_cmd_timeout_ms(ms: Option<u64>) -> Result<(), String> {
    if let Some(timeout) = ms {
        if timeout == 0 {
            warn!(
                "command timeout of 0ms will kill commands immediately; \
                 this is almost certainly not what you want"
            );
        }

        if timeout > 0 && timeout < 100 {
            warn!(
                cmd_timeout_ms = timeout,
                "command timeout below 100ms is very aggressive and may kill \
                 commands before they can start"
            );
        }

        if timeout > 600_000 {
            warn!(
                cmd_timeout_ms = timeout,
                "command timeout above 10 minutes; consider whether this build \
                 step could benefit from optimization"
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Diff store validation
// ---------------------------------------------------------------------------

/// Validate diff-related flags for consistency.
///
/// Warns if `--diff-max-file-size` is set but `--diff` is not enabled,
/// since the max-file-size setting has no effect without the diff store.
pub fn validate_diff_flags(diff_enabled: bool, diff_max_file_size: usize) {
    if !diff_enabled && diff_max_file_size != 512 * 1024 {
        warn!(
            diff_max_file_size,
            "--diff-max-file-size has no effect without --diff; \
             add --diff to enable the diff store"
        );
    }

    if diff_enabled && diff_max_file_size < 256 {
        warn!(
            diff_max_file_size,
            "diff max file size below 256 bytes will cause most source files \
             to be recorded with placeholder diffs instead of real content"
        );
    }
}

pub fn validate_auth_token(token: &Option<String>) -> Result<(), String> {
    if token.is_none() {
        return Err("auth_token is required for secure operation".to_string());
    }
    Ok(())
}

pub fn validate_tls(cert: &Option<String>, key: &Option<String>) -> Result<(), String> {
    if cert.is_none() || key.is_none() {
        return Err("TLS certificate and key are required for secure operation".to_string());
    }
    Ok(())
}

pub fn validate_max_connections(max: Option<u32>) -> Result<(), String> {
    if let Some(m) = max {
        if m == 0 {
            return Err("max_connections must be greater than zero".to_string());
        }
    }
    Ok(())
}
// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // =====================================================================
    // validate_bind_addr
    // =====================================================================

    #[test]
    fn valid_localhost() {
        assert!(validate_bind_addr("127.0.0.1").is_ok());
    }

    #[test]
    fn valid_all_interfaces() {
        assert!(validate_bind_addr("0.0.0.0").is_ok());
    }

    #[test]
    fn valid_ipv6_loopback() {
        assert!(validate_bind_addr("::1").is_ok());
    }

    #[test]
    fn valid_ipv6_all() {
        assert!(validate_bind_addr("::").is_ok());
    }

    #[test]
    fn valid_hostname() {
        assert!(validate_bind_addr("my-server.local").is_ok());
    }

    #[test]
    fn valid_ip_with_numbers() {
        assert!(validate_bind_addr("192.168.1.100").is_ok());
    }

    #[test]
    fn empty_address_rejected() {
        let result = validate_bind_addr("");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must not be empty"));
    }

    #[test]
    fn address_with_spaces_rejected() {
        let result = validate_bind_addr("127.0.0.1 ");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("whitespace"));
    }

    #[test]
    fn address_with_tab_rejected() {
        let result = validate_bind_addr("127.0.0.1\t");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("whitespace"));
    }

    #[test]
    fn address_with_semicolon_rejected() {
        // No spaces — hits the invalid character check.
        let result = validate_bind_addr("127.0.0.1;rm");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid character"));
    }

    #[test]
    fn address_with_pipe_rejected() {
        // No spaces — hits the invalid character check.
        let result = validate_bind_addr("127.0.0.1|cat");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid character"));
    }

    #[test]
    fn address_with_dollar_rejected() {
        let result = validate_bind_addr("$HOME");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid character"));
    }

    #[test]
    fn address_with_backtick_rejected() {
        let result = validate_bind_addr("`whoami`");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid character"));
    }

    #[test]
    fn address_with_ampersand_rejected() {
        // No spaces — hits the invalid character check.
        let result = validate_bind_addr("127.0.0.1&&echo");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid character"));
    }

    #[test]
    fn address_with_spaces_and_metachar_hits_whitespace_first() {
        // When both whitespace and metacharacters are present,
        // the whitespace check fires first.
        let result = validate_bind_addr("127.0.0.1; echo pwned");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("whitespace"));
    }

    // =====================================================================
    // warn_port_issues
    // =====================================================================

    #[test]
    fn port_normal_range_no_panic() {
        // These should not panic — warnings are logged but no error returned.
        warn_port_issues(3012);
        warn_port_issues(8080);
        warn_port_issues(1024);
    }

    #[test]
    fn port_zero_no_panic() {
        warn_port_issues(0);
    }

    #[test]
    fn port_privileged_no_panic() {
        warn_port_issues(80);
        warn_port_issues(443);
        warn_port_issues(1);
    }

    #[test]
    fn port_max_no_panic() {
        warn_port_issues(65535);
        warn_port_issues(65534);
    }

    // =====================================================================
    // validate_debounce_ms
    // =====================================================================

    #[test]
    fn debounce_normal_value() {
        assert!(validate_debounce_ms(200).is_ok());
    }

    #[test]
    fn debounce_minimum_valid() {
        assert!(validate_debounce_ms(1).is_ok());
    }

    #[test]
    fn debounce_zero_rejected() {
        let result = validate_debounce_ms(0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("0ms"));
    }

    #[test]
    fn debounce_low_value_accepted_with_warning() {
        // Should succeed (just warns).
        assert!(validate_debounce_ms(10).is_ok());
    }

    #[test]
    fn debounce_high_value_accepted_with_warning() {
        // Should succeed (just warns).
        assert!(validate_debounce_ms(30_000).is_ok());
    }

    #[test]
    fn debounce_50ms_no_warning() {
        assert!(validate_debounce_ms(50).is_ok());
    }

    #[test]
    fn debounce_10000ms_no_warning() {
        assert!(validate_debounce_ms(10_000).is_ok());
    }

    // =====================================================================
    // validate_cmd_timeout_ms
    // =====================================================================

    #[test]
    fn cmd_timeout_none_accepted() {
        assert!(validate_cmd_timeout_ms(None).is_ok());
    }

    #[test]
    fn cmd_timeout_normal_value() {
        assert!(validate_cmd_timeout_ms(Some(30_000)).is_ok());
    }

    #[test]
    fn cmd_timeout_zero_accepted_with_warning() {
        assert!(validate_cmd_timeout_ms(Some(0)).is_ok());
    }

    #[test]
    fn cmd_timeout_very_low_accepted_with_warning() {
        assert!(validate_cmd_timeout_ms(Some(50)).is_ok());
    }

    #[test]
    fn cmd_timeout_very_high_accepted_with_warning() {
        assert!(validate_cmd_timeout_ms(Some(1_000_000)).is_ok());
    }

    #[test]
    fn cmd_timeout_100ms_no_warning() {
        assert!(validate_cmd_timeout_ms(Some(100)).is_ok());
    }

    // =====================================================================
    // validate_diff_flags
    // =====================================================================

    #[test]
    fn diff_disabled_default_size_no_panic() {
        validate_diff_flags(false, 512 * 1024);
    }

    #[test]
    fn diff_enabled_default_size_no_panic() {
        validate_diff_flags(true, 512 * 1024);
    }

    #[test]
    fn diff_disabled_custom_size_warns_no_panic() {
        // Should warn but not panic.
        validate_diff_flags(false, 1024);
    }

    #[test]
    fn diff_enabled_tiny_size_warns_no_panic() {
        // Should warn but not panic.
        validate_diff_flags(true, 100);
    }

    #[test]
    fn diff_enabled_256_no_warning() {
        validate_diff_flags(true, 256);
    }

    #[test]
    fn diff_enabled_large_size_no_panic() {
        validate_diff_flags(true, 10 * 1024 * 1024);
    }
}
