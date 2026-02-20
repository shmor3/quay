/**
 * watchd hot-reload client for Node.js
 *
 * Connects to a running watchd WebSocket server and executes callbacks
 * when reload or CSS-inject messages are received. Designed for server-side
 * Node.js applications that need to react to file changes broadcast by watchd.
 *
 * Usage:
 *
 *   npm install ws
 *   node hotreload-client.js
 *
 * Or import as a module:
 *
 *   const { HotReloadClient } = require('./hotreload-client');
 *   const client = new HotReloadClient({ port: 3012 });
 *   client.on('reload', () => { console.log('Reload triggered!'); });
 *   client.connect();
 *
 * Features:
 *   - Automatic reconnection with exponential backoff (1s → 30s cap)
 *   - Event-based API for reload and inject-css messages
 *   - Configurable host, port, and callbacks
 *   - Graceful shutdown on SIGINT/SIGTERM
 *   - Base64 content decoding for inject-css messages
 *   - Zero dependencies beyond the `ws` package
 *
 * @license MIT
 */

"use strict";

const WebSocket = require("ws");
const { EventEmitter } = require("events");
const path = require("path");
const fs = require("fs");

// ---------------------------------------------------------------------------
// HotReloadClient
// ---------------------------------------------------------------------------

class HotReloadClient extends EventEmitter {
  /**
   * @param {Object} [options]
   * @param {string} [options.host="localhost"]    - WebSocket server hostname
   * @param {number} [options.port=3012]           - WebSocket server port
   * @param {number} [options.initialDelay=1000]   - Initial reconnect delay (ms)
   * @param {number} [options.maxDelay=30000]      - Maximum reconnect delay (ms)
   * @param {boolean} [options.autoReconnect=true] - Whether to reconnect on disconnect
   * @param {string|null} [options.cssOutputDir]   - Directory to write CSS files to on inject-css
   */
  constructor(options = {}) {
    super();
    this.host = options.host || "localhost";
    this.port = options.port || 3012;
    this.initialDelay = options.initialDelay || 1000;
    this.maxDelay = options.maxDelay || 30000;
    this.autoReconnect = options.autoReconnect !== false;
    this.cssOutputDir = options.cssOutputDir || null;

    this._reconnectDelay = this.initialDelay;
    this._reconnectTimer = null;
    this._ws = null;
    this._closed = false;
  }

  /**
   * The WebSocket URL to connect to.
   * @returns {string}
   */
  get url() {
    return `ws://${this.host}:${this.port}`;
  }

  /**
   * Open the WebSocket connection to the watchd server.
   */
  connect() {
    if (this._closed) return;

    this._log(`connecting to ${this.url}`);

    let ws;
    try {
      ws = new WebSocket(this.url);
    } catch (err) {
      this._warn(`WebSocket constructor failed: ${err.message}`);
      this._scheduleReconnect();
      return;
    }

    this._ws = ws;

    ws.on("open", () => {
      this._log("connected");
      this._reconnectDelay = this.initialDelay;
      this.emit("connected");
    });

    ws.on("message", (data) => {
      this._handleMessage(data);
    });

    ws.on("close", (code, reason) => {
      this._log(
        `disconnected (code ${code}${reason ? ", " + reason : ""})`
      );
      this._ws = null;
      this.emit("disconnected", code, reason);
      this._scheduleReconnect();
    });

    ws.on("error", (err) => {
      // The 'close' event will fire after 'error', so reconnection is
      // handled there. We just emit the error for callers to observe.
      this._warn(`WebSocket error: ${err.message}`);
      this.emit("error", err);
    });
  }

  /**
   * Gracefully close the connection and stop reconnecting.
   */
  close() {
    this._closed = true;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._ws) {
      this._ws.close();
      this._ws = null;
    }
    this._log("client closed");
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * Parse and dispatch an incoming WebSocket message.
   * @param {Buffer|string} raw
   */
  _handleMessage(raw) {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      this._warn(`failed to parse message: ${raw}`);
      return;
    }

    switch (msg.type) {
      case "reload":
        this._log("reload message received");
        this.emit("reload");
        break;

      case "inject-css":
        if (msg.path && msg.content) {
          const decoded = Buffer.from(msg.content, "base64").toString("utf-8");
          this._log(`inject-css for ${msg.path} (${decoded.length} bytes)`);
          this.emit("inject-css", { path: msg.path, content: decoded });

          // Optionally write the CSS to disk.
          if (this.cssOutputDir) {
            this._writeCSSFile(msg.path, decoded);
          }
        } else {
          this._warn("inject-css message missing path or content");
        }
        break;

      default:
        this._log(`unknown message type: ${msg.type}`);
        this.emit("message", msg);
    }
  }

  /**
   * Write decoded CSS content to the configured output directory.
   * @param {string} filePath - Relative path from the watchd root
   * @param {string} css      - Decoded CSS content
   */
  _writeCSSFile(filePath, css) {
    try {
      const outPath = path.join(this.cssOutputDir, path.basename(filePath));
      fs.mkdirSync(path.dirname(outPath), { recursive: true });
      fs.writeFileSync(outPath, css, "utf-8");
      this._log(`wrote CSS to ${outPath}`);
    } catch (err) {
      this._warn(`failed to write CSS file: ${err.message}`);
    }
  }

  /**
   * Schedule a reconnection attempt with exponential backoff.
   */
  _scheduleReconnect() {
    if (this._closed || !this.autoReconnect) return;
    if (this._reconnectTimer) return;

    this._log(`reconnecting in ${this._reconnectDelay / 1000}s`);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this.connect();
    }, this._reconnectDelay);

    // Exponential backoff with cap.
    this._reconnectDelay = Math.min(this._reconnectDelay * 2, this.maxDelay);
  }

  _log(msg) {
    console.log(`\x1b[35m[hotreload]\x1b[0m ${msg}`);
  }

  _warn(msg) {
    console.warn(`\x1b[33m[hotreload]\x1b[0m ${msg}`);
  }
}

// ---------------------------------------------------------------------------
// Standalone execution
// ---------------------------------------------------------------------------

if (require.main === module) {
  const port = parseInt(process.argv[2], 10) || 3012;
  const host = process.argv[3] || "localhost";

  console.log(`watchd Node.js hot-reload client`);
  console.log(`Connecting to ws://${host}:${port}\n`);

  const client = new HotReloadClient({ host, port });

  client.on("reload", () => {
    console.log("→ Reload triggered! Implement your restart logic here.");
    // Example: you could restart a child process, clear require cache, etc.
  });

  client.on("inject-css", ({ path: filePath, content }) => {
    console.log(`→ CSS update for ${filePath} (${content.length} chars)`);
  });

  client.connect();

  // Graceful shutdown on signals.
  const shutdown = () => {
    console.log("\nShutting down...");
    client.close();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = { HotReloadClient };
