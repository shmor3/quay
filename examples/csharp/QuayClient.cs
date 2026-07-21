// quay hot-reload client for C# / .NET
//
// A standalone console application that connects to the quay WebSocket
// server and reacts to reload and inject-css messages. Useful for triggering
// rebuilds, restarting services, or any custom action in .NET applications.
//
// Usage:
//   dotnet run [-- [host] [port]]
//   dotnet run -- localhost 3012
//
// Requirements:
//   .NET 6.0+ (uses System.Net.WebSockets which is built-in)
//
// License: MIT

using System;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Quay;

/// <summary>
/// Event arguments passed to hot-reload event handlers.
/// </summary>
public class QuayEventArgs : EventArgs
{
    /// <summary>The message type received from the server ("reload", "inject-css", etc.).</summary>
    public string Type { get; init; } = "";

    /// <summary>The file path associated with the event (for inject-css messages).</summary>
    public string? Path { get; init; }

    /// <summary>Decoded CSS content (for inject-css messages). Null for other message types.</summary>
    public string? Content { get; init; }

    /// <summary>The raw JSON message received from the server.</summary>
    public string RawMessage { get; init; } = "";
}

/// <summary>
/// A reusable hot-reload WebSocket client that connects to a quay server
/// and raises events when reload or inject-css messages are received.
///
/// Features:
///   - Automatic reconnection with exponential backoff (1s → 30s cap)
///   - Event-driven API via C# events
///   - Thread-safe and cancellation-aware
///   - Zero external dependencies beyond .NET BCL
/// </summary>
public class QuayClient : IDisposable
{
    private const int InitialReconnectDelayMs = 1000;
    private const int MaxReconnectDelayMs = 30000;
    private const int ReceiveBufferSize = 8192;

    private readonly string _host;
    private readonly int _port;
    private int _reconnectDelayMs = InitialReconnectDelayMs;
    private bool _disposed;

    /// <summary>Raised when a "reload" message is received.</summary>
    public event EventHandler<QuayEventArgs>? OnReload;

    /// <summary>Raised when an "inject-css" message is received.</summary>
    public event EventHandler<QuayEventArgs>? OnInjectCss;

    /// <summary>Raised when any message is received (including unknown types).</summary>
    public event EventHandler<QuayEventArgs>? OnMessage;

    /// <summary>Raised when the client successfully connects to the server.</summary>
    public event EventHandler? OnConnected;

    /// <summary>Raised when the client disconnects from the server.</summary>
    public event EventHandler? OnDisconnected;

    /// <summary>Raised when an error occurs.</summary>
    public event EventHandler<Exception>? OnError;

    /// <summary>
    /// Creates a new QuayClient targeting the specified quay server.
    /// </summary>
    /// <param name="host">Hostname or IP address (default: "localhost").</param>
    /// <param name="port">WebSocket port (default: 3012).</param>
    public QuayClient(string host = "localhost", int port = 3012)
    {
        _host = host;
        _port = port;
    }

    /// <summary>
    /// Connect to the quay server and listen for messages indefinitely.
    /// Automatically reconnects on disconnection with exponential backoff.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the connection loop.</param>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAndListenAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                OnError?.Invoke(this, ex);
            }

            OnDisconnected?.Invoke(this, EventArgs.Empty);

            if (cancellationToken.IsCancellationRequested)
                break;

            Log($"reconnecting in {_reconnectDelayMs / 1000.0:F1}s");

            try
            {
                await Task.Delay(_reconnectDelayMs, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            // Exponential backoff with cap.
            _reconnectDelayMs = Math.Min(_reconnectDelayMs * 2, MaxReconnectDelayMs);
        }
    }

    private async Task ConnectAndListenAsync(CancellationToken ct)
    {
        using var ws = new ClientWebSocket();
        var uri = new Uri($"ws://{_host}:{_port}");
        Log($"connecting to {uri}");

        await ws.ConnectAsync(uri, ct);
        Log("connected");
        _reconnectDelayMs = InitialReconnectDelayMs; // Reset backoff on success.
        OnConnected?.Invoke(this, EventArgs.Empty);

        var buffer = new byte[ReceiveBufferSize];

        while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            using var ms = new MemoryStream();
            WebSocketReceiveResult result;

            do
            {
                result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), ct);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    Log($"disconnected (code {(int)(result.CloseStatus ?? WebSocketCloseStatus.Empty)})");
                    return;
                }

                ms.Write(buffer, 0, result.Count);
            }
            while (!result.EndOfMessage);

            if (result.MessageType == WebSocketMessageType.Text)
            {
                var text = Encoding.UTF8.GetString(ms.ToArray());
                HandleMessage(text);
            }
        }
    }

    private void HandleMessage(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;

            var type = root.TryGetProperty("type", out var typeProp)
                ? typeProp.GetString() ?? ""
                : "";

            var path = root.TryGetProperty("path", out var pathProp)
                ? pathProp.GetString()
                : null;

            // Decode base64 content for inject-css messages.
            string? decodedContent = null;
            if (root.TryGetProperty("content", out var contentProp))
            {
                var b64 = contentProp.GetString();
                if (b64 != null)
                {
                    try
                    {
                        decodedContent = Encoding.UTF8.GetString(Convert.FromBase64String(b64));
                    }
                    catch (FormatException)
                    {
                        Warn($"failed to decode base64 content for {path}");
                    }
                }
            }

            var args = new QuayEventArgs
            {
                Type = type,
                Path = path,
                Content = decodedContent,
                RawMessage = raw
            };

            OnMessage?.Invoke(this, args);

            switch (type)
            {
                case "reload":
                    Log("reload requested");
                    OnReload?.Invoke(this, args);
                    break;

                case "inject-css":
                    if (path != null && decodedContent != null)
                    {
                        Log($"CSS update for {path} ({decodedContent.Length} chars)");
                        OnInjectCss?.Invoke(this, args);
                    }
                    else
                    {
                        Warn("inject-css message missing path or content");
                    }
                    break;

                default:
                    Warn($"unknown message type: {type}");
                    break;
            }
        }
        catch (JsonException ex)
        {
            Warn($"failed to parse message: {ex.Message}");
        }
    }

    private static void Log(string msg)
    {
        var ts = DateTime.Now.ToString("HH:mm:ss");
        Console.WriteLine($"\u001b[1;31m[quay]\u001b[0m [{ts}] {msg}");
    }

    private static void Warn(string msg)
    {
        var ts = DateTime.Now.ToString("HH:mm:ss");
        Console.Error.WriteLine($"\u001b[1;33m[quay]\u001b[0m [{ts}] WARNING: {msg}");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Example console application demonstrating the QuayClient.
/// </summary>
public static class Program
{
    public static async Task Main(string[] args)
    {
        var host = args.Length > 0 ? args[0] : "localhost";
        var port = args.Length > 1 && int.TryParse(args[1], out var p) ? p : 3012;

        Console.WriteLine($"quay hot-reload client for C# / .NET");
        Console.WriteLine($"Connecting to ws://{host}:{port}");
        Console.WriteLine("Press Ctrl+C to exit.\n");

        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        using var client = new QuayClient(host, port);

        // ── Register event handlers ─────────────────────────────────────

        client.OnReload += (_, e) =>
        {
            Console.WriteLine("  → Action: would restart application / trigger rebuild");
            // TODO: Add your reload logic here.
            // Examples:
            //   Process.Start("dotnet", "build");
            //   Application.Restart();
            //   serviceProvider.GetService<IHostLifetime>()?.StopAsync(ct);
        };

        client.OnInjectCss += (_, e) =>
        {
            Console.WriteLine($"  → CSS path: {e.Path}");
            Console.WriteLine($"  → CSS length: {e.Content?.Length ?? 0} chars");
            // TODO: Add your CSS handling logic here.
            // In a Blazor or desktop app, you could update styles dynamically.
        };

        client.OnConnected += (_, _) =>
        {
            Console.WriteLine("  Ready to receive file change notifications.\n");
        };

        client.OnError += (_, ex) =>
        {
            Console.Error.WriteLine($"  Connection error: {ex.Message}");
        };

        // ── Run the client ──────────────────────────────────────────────

        try
        {
            await client.RunAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected on Ctrl+C.
        }

        Console.WriteLine("\nGoodbye.");
    }
}
