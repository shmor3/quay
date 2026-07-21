# Node.js Hot-Reload Client

A server-side Node.js WebSocket client that connects to a running `quay` server and reacts to file-change notifications. Useful for triggering rebuilds, restarting processes, clearing caches, or any custom logic when files change.

## Requirements

- Node.js 14+
- [`ws`](https://www.npmjs.com/package/ws) package

## Quick Start

```bash
# Install dependencies
npm install

# Run with defaults (connects to ws://localhost:3012)
npm start

# Or run directly with a custom port
node quay-client.js 4000

# Custom host and port
node quay-client.js 3012 192.168.1.10
```

## Usage as a Library

You can import `HotReloadClient` into your own Node.js application:

```js
const { HotReloadClient } = require("./quay-client");

const client = new HotReloadClient({
  host: "localhost",
  port: 3012,
  autoReconnect: true,
});

// React to full-reload messages
client.on("reload", () => {
  console.log("Files changed — restarting server...");
  // Restart your app, clear require cache, etc.
});

// React to CSS injection messages
client.on("inject-css", ({ path, content }) => {
  console.log(`CSS updated: ${path} (${content.length} chars)`);
  // Write to disk, push to a template engine, etc.
});

// Lifecycle events
client.on("connected", () => console.log("Connected to quay"));
client.on("disconnected", () => console.log("Lost connection"));
client.on("error", (err) => console.error("Error:", err.message));

client.connect();

// Later, to disconnect:
// client.close();
```

## API

### `new HotReloadClient(options?)`

| Option          | Type    | Default       | Description                                        |
|-----------------|---------|---------------|----------------------------------------------------|
| `host`          | string  | `"localhost"` | WebSocket server hostname                          |
| `port`          | number  | `3012`        | WebSocket server port                              |
| `initialDelay`  | number  | `1000`        | Initial reconnect delay in ms                      |
| `maxDelay`      | number  | `30000`       | Maximum reconnect delay in ms (backoff cap)        |
| `autoReconnect` | boolean | `true`        | Whether to automatically reconnect on disconnect   |
| `cssOutputDir`  | string  | `null`        | If set, write received CSS files to this directory |

### Events

| Event          | Payload                          | Description                            |
|----------------|----------------------------------|----------------------------------------|
| `reload`       | —                                | Server requested a full reload         |
| `inject-css`   | `{ path: string, content: string }` | CSS file changed; content is decoded |
| `message`      | `object`                         | Any message with an unknown type       |
| `connected`    | —                                | WebSocket connection established       |
| `disconnected` | `code, reason`                   | WebSocket connection closed            |
| `error`        | `Error`                          | WebSocket error occurred               |

### Methods

| Method      | Description                              |
|-------------|------------------------------------------|
| `connect()` | Open the WebSocket connection            |
| `close()`   | Disconnect and stop reconnecting         |

## Features

- **Automatic reconnection** with exponential backoff (1s → 30s cap)
- **Event-driven API** built on Node.js `EventEmitter`
- **Base64 decoding** of CSS content from `inject-css` messages
- **Optional CSS file writing** to disk via `cssOutputDir`
- **Graceful shutdown** on `SIGINT` / `SIGTERM`
- **Zero dependencies** beyond the `ws` package

## Protocol

See the [examples README](../README.md) for the full WebSocket protocol specification.