/**
 * watchd hot-reload client
 *
 * Drop this script into any HTML page to enable automatic reloading and
 * CSS hot-injection when the watchd server detects file changes.
 *
 * Usage:
 *
 *   <script src="/hotreload-client.js"></script>
 *
 * To override the default WebSocket port (3012), add a data-port attribute:
 *
 *   <script src="/hotreload-client.js" data-port="4000"></script>
 *
 * Features:
 *   - Automatic reconnection with exponential backoff (1s → 30s cap)
 *   - Full page reload on "reload" messages
 *   - In-place CSS injection on "inject-css" messages (no page reload)
 *   - Console logging with [hotreload] prefix
 *   - Zero dependencies — pure vanilla JS, works in all modern browsers
 *
 * @license MIT
 */
(function () {
  "use strict";

  var DEFAULT_PORT = 3012;

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------

  // Try to read a custom port from the <script> tag's data-port attribute.
  var scriptEl = document.currentScript;
  var port =
    scriptEl && scriptEl.getAttribute("data-port")
      ? parseInt(scriptEl.getAttribute("data-port"), 10)
      : DEFAULT_PORT;

  if (isNaN(port) || port < 1 || port > 65535) {
    console.warn(
      "[hotreload] invalid data-port value; falling back to " + DEFAULT_PORT
    );
    port = DEFAULT_PORT;
  }

  // ---------------------------------------------------------------------------
  // Reconnection state
  // ---------------------------------------------------------------------------

  var INITIAL_DELAY = 1000;
  var MAX_DELAY = 30000;
  var reconnectDelay = INITIAL_DELAY;
  var reconnectTimer = null;

  // ---------------------------------------------------------------------------
  // Logging helpers
  // ---------------------------------------------------------------------------

  function log(msg) {
    console.log(
      "%c[hotreload]%c " + msg,
      "color:#e06c75;font-weight:bold",
      "color:inherit"
    );
  }

  function warn(msg) {
    console.warn("[hotreload] " + msg);
  }

  // ---------------------------------------------------------------------------
  // Base64 decoding
  // ---------------------------------------------------------------------------

  /**
   * Decode a base64 string to its original text content.
   * Falls back gracefully if decoding fails.
   */
  function b64decode(str) {
    try {
      return atob(str);
    } catch (e) {
      warn("failed to decode base64 content: " + e);
      return "";
    }
  }

  // ---------------------------------------------------------------------------
  // CSS injection
  // ---------------------------------------------------------------------------

  /**
   * Inject or update a <style> element identified by a data-hotreload attribute
   * matching the given path. If no matching element exists, a new one is
   * appended to <head>.
   */
  function injectCSS(path, encodedContent) {
    var css = b64decode(encodedContent);
    if (!css) return;

    // Build a selector-safe attribute value. CSS.escape may not be available
    // in very old browsers, so we fall back to a simple replacement.
    var escapedPath =
      typeof CSS !== "undefined" && typeof CSS.escape === "function"
        ? CSS.escape(path)
        : path.replace(/"/g, '\\"');

    var existing = document.querySelector(
      'style[data-hotreload="' + escapedPath + '"]'
    );

    if (existing) {
      existing.textContent = css;
      log("injected CSS update for " + path);
    } else {
      var style = document.createElement("style");
      style.setAttribute("data-hotreload", path);
      style.textContent = css;
      document.head.appendChild(style);
      log("injected new CSS for " + path);
    }

    // Also attempt to update any <link> stylesheet that references the same
    // path by appending a cache-busting query parameter, which forces the
    // browser to re-fetch the resource.
    var links = document.querySelectorAll('link[rel="stylesheet"]');
    for (var i = 0; i < links.length; i++) {
      var href = links[i].getAttribute("href");
      if (href && href.indexOf(path) !== -1) {
        var separator = href.indexOf("?") === -1 ? "?" : "&";
        links[i].setAttribute(
          "href",
          href.split("?")[0] + separator + "_hr=" + Date.now()
        );
        log("cache-busted linked stylesheet: " + href);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // WebSocket connection
  // ---------------------------------------------------------------------------

  function connect() {
    var host = location.hostname || "localhost";
    var url = "ws://" + host + ":" + port;
    log("connecting to " + url);

    var ws;
    try {
      ws = new WebSocket(url);
    } catch (e) {
      warn("WebSocket constructor failed: " + e);
      scheduleReconnect();
      return;
    }

    ws.onopen = function () {
      log("connected");
      reconnectDelay = INITIAL_DELAY; // reset backoff on successful connection
    };

    ws.onmessage = function (event) {
      var msg;
      try {
        msg = JSON.parse(event.data);
      } catch (e) {
        warn("failed to parse message: " + event.data);
        return;
      }

      switch (msg.type) {
        case "reload":
          log("reloading page");
          location.reload();
          break;

        case "inject-css":
          if (msg.path && msg.content) {
            injectCSS(msg.path, msg.content);
          } else {
            warn("inject-css message missing path or content");
          }
          break;

        default:
          log("unknown message type: " + msg.type);
      }
    };

    ws.onclose = function (event) {
      log("disconnected (code " + event.code + ")");
      scheduleReconnect();
    };

    ws.onerror = function () {
      // The onclose handler will fire after onerror, so reconnection is
      // handled there. We intentionally do nothing here to avoid double
      // scheduling.
    };
  }

  // ---------------------------------------------------------------------------
  // Reconnection
  // ---------------------------------------------------------------------------

  function scheduleReconnect() {
    if (reconnectTimer) return;
    log("reconnecting in " + (reconnectDelay / 1000) + "s");
    reconnectTimer = setTimeout(function () {
      reconnectTimer = null;
      connect();
    }, reconnectDelay);
    // Exponential backoff with cap.
    reconnectDelay = Math.min(reconnectDelay * 2, MAX_DELAY);
  }

  // ---------------------------------------------------------------------------
  // Bootstrap
  // ---------------------------------------------------------------------------

  // Start on DOMContentLoaded if the document isn't ready yet, otherwise
  // connect immediately (covers dynamically injected scripts).
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", connect);
  } else {
    connect();
  }
})();
