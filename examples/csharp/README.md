# watchd Hot-Reload Client — C# / .NET

A WebSocket client that connects to a running `watchd` server and reacts to
`reload` and `inject-css` messages. Built on `System.Net.WebSockets` with zero
external dependencies beyond the .NET BCL.

## Features

- **Event-driven API** — subscribe to `OnReload`, `OnInjectCss`, `OnMessage`,
  `OnConnected`, `OnDisconnected`, and `OnError` events
- **Automatic reconnection** with exponential backoff (1 s → 30 s cap)
- **Base64 decoding** of CSS content for `inject-css` messages
- **Cancellation-aware** — pass a `CancellationToken` for clean shutdown
- **Zero external dependencies** — uses only `System.Net.WebSockets` and
  `System.Text.Json`

## Requirements

- [.NET 6.0 SDK](https://dotnet.microsoft.com/download) or later

## Quick Start

```bash
# Run with default settings (ws://localhost:3012)
dotnet run

# Connect to a custom host and port
dotnet run -- localhost 4000
```

## Usage as a Library

Copy `HotreloadClient.cs` into your project and use the `HotreloadClient` class
directly:

```csharp
using Hotreload;

using var cts = new CancellationTokenSource();
using var client = new HotreloadClient("localhost", 3012);

client.OnReload += (_, e) =>
{
    Console.WriteLine("Files changed — restarting service...");
    // Trigger your rebuild or restart logic here.
};

client.OnInjectCss += (_, e) =>
{
    Console.WriteLine($"CSS updated: {e.Path} ({e.Content?.Length} chars)");
    // Write to disk, push to a UI framework, etc.
};

await client.RunAsync(cts.Token);
```

## API Reference

### `HotreloadClient(string host, int port)`

Create a new client targeting the specified watchd server.

| Parameter | Default       | Description                       |
|-----------|---------------|-----------------------------------|
| `host`    | `"localhost"` | Hostname or IP of the watchd server |
| `port`    | `3012`        | WebSocket port                    |

### Events

| Event            | Args                  | Description                                  |
|------------------|-----------------------|----------------------------------------------|
| `OnReload`       | `HotreloadEventArgs`  | Fired on `reload` messages                   |
| `OnInjectCss`    | `HotreloadEventArgs`  | Fired on `inject-css` messages               |
| `OnMessage`      | `HotreloadEventArgs`  | Fired on any message (including unknown types)|
| `OnConnected`    | `EventArgs`           | Fired when the WebSocket connection opens    |
| `OnDisconnected` | `EventArgs`           | Fired when the connection is lost            |
| `OnError`        | `Exception`           | Fired when a connection error occurs         |

### `HotreloadEventArgs`

| Property     | Type      | Description                                    |
|--------------|-----------|------------------------------------------------|
| `Type`       | `string`  | Message type (`"reload"`, `"inject-css"`, etc.) |
| `Path`       | `string?` | File path (for `inject-css` messages)          |
| `Content`    | `string?` | Decoded CSS content (for `inject-css`)         |
| `RawMessage` | `string`  | The original JSON string from the server       |

## Integration Ideas

- **ASP.NET / Kestrel** — trigger a middleware reload or clear response caches
- **Blazor Server** — push CSS updates to connected browsers via SignalR
- **WPF / WinForms** — refresh UI resources when stylesheets change
- **Background services** — restart `IHostedService` instances on file changes
- **Build pipelines** — trigger `dotnet build` or `dotnet test` on reload

## License

MIT