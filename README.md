# hotreload

A minimal Rust project containing a small hot-reload-style file watcher that can run a command when files change.

Watcher (in repository `watcher/`):

Build the watcher:

```bash
cargo build --manifest-path watcher/Cargo.toml
```

Run the watcher (example runs `npm run build` on changes):

```bash
cargo run --manifest-path watcher/Cargo.toml -- "npm run build"
# or run the built binary directly:
./watcher/target/debug/hotreload-watcher "npm run build"
```

By default (no args) the watcher runs `echo files changed` on startup and when files change.
/* css-trigger */
/* css-trigger */
// md-trigger
