# Python Hot-Reload Client

A WebSocket client that connects to a running `quay` server and reacts to
file-change notifications. Supports both **async** (`websockets`) and
**synchronous** (`websocket-client`) modes.

## Requirements

Install one of the supported WebSocket libraries:

```bash
# Async client (recommended)
pip install websockets

# Synchronous client (thread-based alternative)
pip install websocket-client
```

## Quick Start

```bash
# Default: connect to ws://localhost:3012
python quay_client.py

# Custom host and port
python quay_client.py --host 192.168.1.10 --port 4000

# Use the synchronous client instead of asyncio
python quay_client.py --sync
```

## Usage as a Library

### Async client

```python
from quay_client import HotReloadClient

def on_reload():
    print("Files changed — restarting server...")

def on_css(path, content):
    with open(f"public/{path}", "w") as f:
        f.write(content)

client = HotReloadClient(
    host="localhost",
    port=3012,
    on_reload=on_reload,
    on_inject_css=on_css,
)

# Blocking — runs the asyncio event loop internally
client.run()
```

### Async client with `asyncio`

```python
import asyncio
from quay_client import HotReloadClient

async def main():
    client = HotReloadClient(
        port=3012,
        on_reload=lambda: print("reload!"),
    )
    await client.run_async()

asyncio.run(main())
```

### Synchronous client

```python
from quay_client import HotReloadClientSync

client = HotReloadClientSync(
    port=3012,
    on_reload=lambda: print("reload!"),
    on_inject_css=lambda path, css: print(f"CSS: {path}"),
)
client.run()
```

## Features

- **Automatic reconnection** with exponential backoff (1 s → 30 s cap)
- **Two client modes**: async (`websockets`) and sync (`websocket-client`)
- **Configurable callbacks** for `reload` and `inject-css` events
- **Base64 decoding** of CSS content from `inject-css` messages
- **Clean shutdown** on `Ctrl-C` / `SIGINT`
- **Structured logging** with timestamps

## API Reference

### `HotReloadClient(host, port, on_reload, on_inject_css, on_connect, on_disconnect)`

Async WebSocket client using the `websockets` library.

| Parameter       | Type                          | Default       | Description                          |
|-----------------|-------------------------------|---------------|--------------------------------------|
| `host`          | `str`                         | `"localhost"` | Server hostname                      |
| `port`          | `int`                         | `3012`        | Server WebSocket port                |
| `on_reload`     | `() -> None`                  | `None`        | Called on `reload` messages          |
| `on_inject_css` | `(path: str, css: str) -> None` | `None`      | Called on `inject-css` messages      |
| `on_connect`    | `() -> None`                  | `None`        | Called when connected                |
| `on_disconnect` | `() -> None`                  | `None`        | Called when disconnected             |

**Methods:**

- `run()` — run synchronously (blocks the calling thread)
- `run_async()` — run as an awaitable coroutine
- `stop()` — signal the client to disconnect and stop reconnecting

### `HotReloadClientSync(...)`

Same constructor signature as `HotReloadClient`, but uses the synchronous
`websocket-client` library instead of `asyncio`.

**Methods:**

- `run()` — run synchronously (blocks the calling thread)
- `stop()` — signal the client to disconnect and stop reconnecting

## Protocol

See the [examples README](../README.md) for full protocol documentation.