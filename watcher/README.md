# hotreload-watcher

A small, language-agnostic file watcher that runs commands incrementally and broadcasts changes to browser clients via WebSocket.

## Features

- Watch the current directory recursively
- Run a command template on changes (use `{path}` to substitute the changed path)
- Broadcast messages to connected browsers to inject CSS (`inject-css`) or request a full reload (`reload`)
- Include/exclude pattern file support with `+` (include) and `-` (exclude) prefixes
- Sensible default excludes: `target/**`, `.git/**`, `node_modules/**`, etc.
- Configurable debounce, websocket port, and option to skip initial run

## Quick start

Build:

```bash
cargo build --manifest-path d:/GitHub/hotreload/watcher/Cargo.toml
```

Run (example):

```bash
cargo run --manifest-path d:/GitHub/hotreload/watcher/Cargo.toml -- "echo built {path}"
```

Flags:

- `--patterns-file <path>` : path to patterns file (default `.hotreloadignore`)
- `--debounce-ms <ms>` : debounce delay in milliseconds (default `200`)
- `--port <port>` : websocket port (default `3012`)
- `--no-run-on-start` : do not run command on startup

## Patterns file (.hotreloadignore)

Create a `.hotreloadignore` file in the repo root or pass a different file with `--patterns-file`.

Syntax:

- Lines starting with `+` are include patterns (glob syntax)
- Lines starting with `-` are exclude patterns
- Lines starting with `#` are comments
- Blank lines are ignored
- Lines without a prefix default to include

Example:

```
# include source and static assets
+ src/**
+ static/**/*.css

# exclude build artifacts
- target/**
- .git/**
- node_modules/**
```

Note: exclude patterns are always honored; if include patterns exist, a file must match at least one include to be considered.

## Client

Include `hotreload-client.js` in your HTML pages to receive live updates:

```html
<script src="/hotreload-client.js"></script>
```

The client connects to `ws://<host>:3012` by default and will:

- Inject CSS sent as `inject-css` messages by replacing/creating a `<style data-hotreload="...">` element
- Reload the page when a `reload` message is broadcast

## Extensibility

This tool is intentionally language-agnostic: it doesn't parse or compile language-specific files. Use the `cmd_template` to invoke any build step you need (e.g., `npm run build -- {path}` or `cargo build --manifest-path project/Cargo.toml`).

If you'd like, I can add:

- .gitignore-like order/negation semantics for patterns
- Default merge behavior that allows user overrides
- Tests for Windows path normalization and UNC paths

## Adapters

You can add adapters — small, human-readable pseudo-language files placed in `watcher/adapters/` — to provide language-specific build or run logic. The watcher will load any adapter files it finds at startup and use them when a matching file changes.

Adapter syntax (simple key: value per line):

- `name: <identifier>` — adapter name
- `watch: <glob>` — glob pattern to match changed files (can appear multiple times)
- `on_change: <cmd>` — command template to run when a matching file changes (use `{path}`)
- `build: <cmd>` — alternative build command
- `notify: <auto|reload|inject-css|none>` — how to notify clients; `auto` uses extension heuristics

Example `watcher/adapters/typescript.adapter`:

```
name: typescript
watch: **/*.ts
on_change: npm run build -- {path}
notify: reload
```

Behavior:
- If any adapter matches a changed file, the watcher runs the adapter's `on_change` (or `build`) command and follows its `notify` behavior. Multiple adapters may match and will all be executed.
- If no adapter matches, the watcher falls back to its default heuristics (inject CSS for `.css`, reload for `.html`, or run the generic `cmd_template`).

Adapters are intentionally simple and stored as text so you can author them quickly without changing the Rust code. If you want a richer adapter language, I can extend the parser/interpreter to support variables, conditional steps, and named actions.

---

If you'd like me to make any of the above improvements (or implement a config file format), tell me which and I'll continue.
