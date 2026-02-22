# Test Plan for hotreload (watchd)

## Areas to Cover
- Watcher event handling (file create, modify, remove, rename)
- Reload and inject-css WebSocket messages
- Diff store functionality
- Control socket commands (status, reload, diff, diffs, diff-clear)

## Example Rust Test (watcher/src/watcher.rs)

```
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_filtering() {
        // Simulate file events and assert only data-affecting events are processed
    }
}
```

## Integration Test Example

```
#[cfg(test)]
mod integration {
    #[test]
    fn test_control_socket_status() {
        // Start watchd, connect to control socket, send status command, assert response
    }
}
```

## End-to-End Test Example

---

## Practical Guidance

- Use `cargo test` to run all tests.
- For integration tests, start the watcher in a subprocess and connect via control socket or WebSocket.
- Use temporary directories for file event tests to avoid polluting the workspace.
- Set `RUST_LOG=debug` for verbose output during test runs.
- Test with both valid and invalid configs to ensure error handling.
- Simulate common errors (invalid YAML, port in use, permission denied) and assert correct error messages.
- Test diff store limits by creating large files and many changes.
- Test graceful shutdown by sending Ctrl-C or killing the process.

---

## Troubleshooting & Common Errors

- If tests fail, check for:
  - Invalid config: fix YAML syntax.
  - Port conflicts: use unique ports for each test.
  - Permission issues: run tests with appropriate access.
  - Stuck commands: use `--cmd-timeout-ms`.

---

## FAQ

**Q: How do I run only integration tests?**
A: Use `cargo test --test integration`.

**Q: How do I debug test failures?**
A: Set `RUST_LOG=debug` and check logs.

**Q: How do I simulate file changes?**
A: Use Rust's `std::fs` to create, modify, and delete files in test directories.

**Q: How do I test WebSocket messages?**
A: Use a WebSocket client library in your test to connect and assert received messages.

**Q: How do I test diff store limits?**
A: Create many files/changes and check eviction behavior.

**Q: How do I test error handling?**
A: Provide invalid configs, trigger permission errors, and assert error responses.

---

```
#[cfg(test)]
mod e2e {
    #[test]
    fn test_websocket_reload() {
        // Start watchd, connect WebSocket client, trigger file change, assert reload message
    }
}
```

Add similar tests in relevant modules. Use cargo test to run.
