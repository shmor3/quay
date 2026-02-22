# Linting and Formatting for hotreload

## Linting
- Use Clippy for Rust linting:
  - `cargo clippy --manifest-path watcher/Cargo.toml -- -D warnings`

## Formatting
- Use rustfmt for formatting:
  - `cargo fmt -- --check`

## Integration
- Both are run in CI (see .github/workflows/ci.yml)
- Run locally before committing to ensure code quality
