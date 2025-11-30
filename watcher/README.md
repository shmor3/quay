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

## Config files

You can add config files — small YAML files placed in `watcher/configs/` (or `configs/`) — to provide project- or language-specific build/run logic. The watcher will load any config files it finds at startup and use them when a matching file changes.

Config syntax (YAML):

- `name: <identifier>` — config name
- `watch:` — a glob or sequence of globs to match changed files (e.g., `"**/*.ts"`)
- `on_change: <cmd>` — command template to run when a matching file changes (use `{path}`)
- `build: <cmd>` — alternative build command
- `notify: <auto|reload|inject-css|none>` — how to notify clients; `auto` uses extension heuristics
- `ignore:` — optional list of exclude globs that will be merged into the watcher's exclude set (used when no `--patterns-file` is supplied)

Example `watcher/configs/typescript.yaml`:

```yaml
name: typescript
watch:
	- "**/*.ts"
on_change: "npm run build -- {path}"
notify: "reload"
```

Behavior:
- If any config matches a changed file, the watcher runs the config's `on_change` (or `build`) command and follows its `notify` behavior. Multiple config files may match and will all be executed.
- If no config matches, the watcher falls back to its default heuristics (inject CSS for `.css`, reload for `.html`, or run the generic `cmd_template`).

Config files are intentionally simple and stored as YAML so you can author them quickly without changing the Rust code. They can also declare `ignore:` patterns that are merged into the watcher's exclude set when you don't supply a `--patterns-file`.

---

If you'd like me to make any of the above improvements (or implement a config file format), tell me which and I'll continue.
