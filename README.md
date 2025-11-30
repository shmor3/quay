# watchd

A minimal, language-agnostic file watcher that runs commands when files change and broadcasts reload or CSS-inject messages to browser clients.

Build the watcher binary and run:

```bash
cargo build --manifest-path watcher/Cargo.toml
cargo run --manifest-path watcher/Cargo.toml -- --path .
```

Or run the produced binary directly (built at `watcher/target/debug/watchd`):

```bash
./watcher/target/debug/watchd --path .
```

Configuration is read from `hotreload.yaml` located at the watch root. See `watcher/README.md` for full documentation and examples.

CLI

The `watchd` program supports a few flags and subcommands. You can run the watcher server (default) or invoke the control subcommands which contact the running watchd control socket on port+1.

Flags (when running the server):

- `-p, --path <path>` : path to watch and where to look for `hotreload.yaml` (default `.`)
- `--port <port>` : websocket port (default `3012`)
- `--debounce-ms <ms>` : debounce delay in milliseconds (default `200`)
- `--no-run-on-start` : do not run configured commands on startup

Subcommands (client mode):

- `status` : query the running watchd for status of loaded configs
- `reload` : request an immediate reload; will run configured build/on_change commands and broadcast a `reload` message

Examples

```bash
# query status via cargo-run (uses the control socket)
cargo run --manifest-path watcher/Cargo.toml -- status

# trigger reload via cargo-run
cargo run --manifest-path watcher/Cargo.toml -- reload

# run the server and watch a directory
cargo run --manifest-path watcher/Cargo.toml -- --path /path/to/project --port 3012

# run the produced binary directly (status / reload are client-mode subcommands)
./watcher/target/debug/watchd status
./watcher/target/debug/watchd reload
```
