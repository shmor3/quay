# Example Clients

This directory contains example hot-reload clients for various languages and
platforms. Each client connects to the `watchd` WebSocket server and reacts to
file-change notifications.

The server is **language-agnostic** — any WebSocket client that understands the
simple JSON protocol can integrate with it.

---

## WebSocket Protocol

### Connection

Connect to the watchd WebSocket server at:

```
ws://<host>:<port>
```

The default port is **3012**. The host is typically `127.0.0.1` or `localhost`
during development.

### Messages (server → client)

All messages are JSON objects with a `type` field:

#### `reload`

Indicates that a watched file has changed and the client should perform a full
reload (refresh the page, restart the process, re-execute a script, etc.).

```json
{
  "type": "reload"
}
```

#### `inject-css`

Indicates that a CSS file has changed. The new content is included as a
base64-encoded string so the client can apply it without a full reload.

```json
{
  "type": "inject-css",
  "path": "src/styles/main.css",
  "content": "Ym9keSB7IGJhY2tncm91bmQ6IHJlZDsgfQ=="
}
```

| Field     | Type   | Description                              |
|-----------|--------|------------------------------------------|
| `type`    | string | Always `"inject-css"`                    |
| `path`    | string | Relative path of the changed CSS file    |
| `content` | string | Base64-encoded CSS content               |

### Messages (client → server)

The server does not currently expect any messages from clients. Client frames
are silently ignored. Future protocol versions may add client-to-server
commands.

---

## Available Clients

| Directory                        | Language / Platform        | Description                                          |
|----------------------------------|----------------------------|------------------------------------------------------|
| [`javascript/`](javascript/)    | JavaScript (Browser)       | Drop-in browser `<script>` with auto-reconnect       |
| [`html/`](html/)                | HTML                       | Minimal demo page using the browser client           |
| [`nodejs/`](nodejs/)            | Node.js                    | Server-side Node.js client using the `ws` package    |
| [`python/`](python/)            | Python 3                   | Async client using `websockets`                      |
| [`go/`](go/)                    | Go                         | Client using `gorilla/websocket`                     |
| [`rust/`](rust/)                | Rust                       | Client using `tungstenite`                           |
| [`ruby/`](ruby/)                | Ruby                       | Client using `websocket-client-simple`               |
| [`csharp/`](csharp/)            | C# / .NET                  | Client using `System.Net.WebSockets`                 |

---

## Quick Start

1. **Start the watchd server** in your project directory:

   ```bash
   watchd --path /path/to/project --port 3012
   ```

2. **Run any example client** — pick the language you prefer. Each directory
   has its own README with specific instructions.

3. **Edit a watched file** and observe the client reacting to the change.

---

## Writing Your Own Client

Implementing a watchd client in any language is straightforward:

1. Open a WebSocket connection to `ws://localhost:3012`.
2. Listen for incoming text messages.
3. Parse each message as JSON.
4. Inspect `msg.type`:
   - `"reload"` → perform your reload action.
   - `"inject-css"` → base64-decode `msg.content` and apply it.
   - Unknown types → log and ignore (forward compatibility).
5. On disconnect, reconnect with exponential backoff.

That's it. No handshake, no auth, no subscription — just connect and listen.