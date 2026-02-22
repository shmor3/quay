# Error Handling Improvements for hotreload

## Recommendations
- Audit all modules for error handling.
- Use centralised error type (`WatchdError`) via `thiserror`.
- Avoid bare unwrap()/expect() in production code.
- Log errors using `tracing` macros.
- Propagate errors with Result types and handle recoverable failures gracefully.
- Add tests for error scenarios.

## Example
```
// Instead of unwrap()
let result = some_operation().map_err(|e| WatchdError::from(e))?;
```

## Action
- Review and refactor all modules to follow these patterns.
