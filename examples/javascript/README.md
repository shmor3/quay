# JavaScript (Browser) Client

A drop-in browser `<script>` that connects to the quay WebSocket server and
handles live-reload and CSS hot-injection automatically.

## Features

- **Auto-reconnect** with exponential backoff (1 s → 30 s cap)
- **Full page reload** on `reload` messages
- **CSS hot-injection** on `inject-css` messages — injects/updates `<style data-quay="...">` elements
- **Cache-busting** of `<link rel="stylesheet">` elements whose `href` matches the changed path
- **Console logging** with coloured `[quay]` prefix
- Zero external dependencies — pure vanilla JS

## Usage

### Option 1 — External script tag

Copy `quay-client.js` into your project's static assets directory and
include it in your HTML:

```html
<script src="/quay-client.js"></script>
```

### Option 2 — Override the port

If the quay server runs on a non-default port, use the `data-port` attribute:

```html
<script src="/quay-client.js" data-port="4000"></script>
```

### Option 3 — Inline snippet

Use the quay `--print-snippet` flag to generate a self-contained inline
`<script>` tag that requires no extra HTTP request:

```bash
quay --port 3012 --print-snippet
```

Paste the output directly into your HTML `<head>`.

## How It Works

1. On page load (or `DOMContentLoaded`), the client opens a WebSocket
   connection to `ws://<current-hostname>:<port>`.
2. When the server sends `{"type": "reload"}`, the client calls
   `location.reload()`.
3. When the server sends `{"type": "inject-css", "path": "...", "content": "..."}`,
   the client base64-decodes the content and:
   - Updates an existing `<style data-quay="<path>">` element, or creates
     a new one.
   - Appends a cache-busting query parameter to any `<link rel="stylesheet">`
     whose `href` contains the changed path.
4. On disconnect, the client schedules a reconnect with exponential backoff
   (1 s → 2 s → 4 s → … → 30 s cap). The delay resets on successful
   reconnection.

## Configuration

| Attribute    | Default | Description                        |
|--------------|---------|------------------------------------|
| `data-port`  | `3012`  | WebSocket port to connect to       |

The hostname is automatically derived from `window.location.hostname` (falls
back to `localhost`).

## Browser Support

Works in all modern browsers that support `WebSocket` and
`document.currentScript` (Chrome, Firefox, Safari, Edge).