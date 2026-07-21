# quay Hot-Reload Client — Go

A standalone WebSocket client that connects to a running `quay` server and reacts to file-change notifications. Useful for triggering rebuilds, restarting services, or executing arbitrary callbacks in Go applications.

## Requirements

- Go 1.21+
- [`gorilla/websocket`](https://github.com/gorilla/websocket)

## Quick Start

```bash
# Install dependencies
go mod tidy

# Run with defaults (connects to ws://127.0.0.1:3012)
go run main.go

# Connect to a custom address
go run main.go -addr ws://192.168.1.10:4000
```

## Build

```bash
go build -o quay-client .
./quay-client -addr ws://127.0.0.1:3012
```

## How It Works

1. Connects to the `quay` WebSocket server.
2. Listens for JSON messages:
   - `{"type": "reload"}` — calls `onReload()`.
   - `{"type": "inject-css", "path": "...", "content": "..."}` — base64-decodes the content and calls `onInjectCSS()`.
3. On disconnect, reconnects automatically with exponential backoff (1 s → 30 s cap).
4. Shuts down cleanly on `SIGINT` / `SIGTERM`.

## Customisation

Edit the handler functions in `main.go` to implement your own reload logic:

```go
func onReload() {
    // Restart your server, trigger a rebuild, clear caches, etc.
    cmd := exec.Command("go", "build", "./...")
    cmd.Run()
}

func onInjectCSS(path, encodedContent string) {
    // Write updated CSS to disk, push to a template engine, etc.
    css, _ := base64.StdEncoding.DecodeString(encodedContent)
    os.WriteFile(path, css, 0644)
}
```

## Protocol Reference

See the [examples README](../README.md) for full protocol documentation.

## License

MIT