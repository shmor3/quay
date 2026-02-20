//! Embedded browser client for `watchd`.
//!
//! Contains a self-contained JavaScript snippet that connects to the watchd
//! WebSocket server and handles `reload` and `inject-css` messages.  The
//! snippet is designed to be dropped into any HTML page via a `<script>` tag.
//!
//! # Features
//!
//! - Auto-detects the WebSocket URL from `window.location.hostname`
//! - Reconnects automatically with exponential backoff (1 s → 30 s cap)
//! - Handles `reload` messages by calling `location.reload()`
//! - Handles `inject-css` messages by decoding base64 content and
//!   inserting/updating a `<style data-hotreload="<path>">` element
//! - Logs connection lifecycle events to the browser console
//! - Zero external dependencies — pure vanilla JS

/// The default WebSocket port used by watchd.
pub const DEFAULT_WS_PORT: u16 = 3012;

/// Minified JavaScript client that can be embedded directly in a `<script>`
/// tag or served as a standalone `.js` file.
///
/// The client reads an optional `data-port` attribute from its own `<script>`
/// element to allow overriding the default port:
///
/// ```html
/// <script src="/hotreload-client.js" data-port="4000"></script>
/// ```
pub const CLIENT_JS: &str = r#"(function(){
  "use strict";

  var DEFAULT_PORT = 3012;

  // Try to read a custom port from the <script> tag's data-port attribute.
  var scriptEl = document.currentScript;
  var port = (scriptEl && scriptEl.getAttribute("data-port"))
    ? parseInt(scriptEl.getAttribute("data-port"), 10)
    : DEFAULT_PORT;

  var reconnectDelay = 1000;
  var maxReconnectDelay = 30000;
  var reconnectTimer = null;

  function log(msg) {
    console.log("%c[hotreload]%c " + msg, "color:#e06c75;font-weight:bold", "color:inherit");
  }

  function warn(msg) {
    console.warn("[hotreload] " + msg);
  }

  // Base64 decode that works in all browsers.
  function b64decode(str) {
    try {
      return atob(str);
    } catch (e) {
      warn("failed to decode base64 content: " + e);
      return "";
    }
  }

  function injectCSS(path, encodedContent) {
    var css = b64decode(encodedContent);
    var id = "hotreload:" + path;
    var existing = document.querySelector('style[data-hotreload="' + CSS.escape(path) + '"]');
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
  }

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

    ws.onopen = function() {
      log("connected");
      reconnectDelay = 1000; // reset backoff on successful connection
    };

    ws.onmessage = function(event) {
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

    ws.onclose = function(event) {
      log("disconnected (code " + event.code + ")");
      scheduleReconnect();
    };

    ws.onerror = function() {
      // onclose will fire after onerror, so reconnection is handled there.
    };
  }

  function scheduleReconnect() {
    if (reconnectTimer) return;
    log("reconnecting in " + (reconnectDelay / 1000) + "s");
    reconnectTimer = setTimeout(function() {
      reconnectTimer = null;
      connect();
    }, reconnectDelay);
    // Exponential backoff with cap.
    reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
  }

  // Start on DOMContentLoaded if the document isn't ready yet, otherwise
  // connect immediately (covers dynamically injected scripts).
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", connect);
  } else {
    connect();
  }
})();
"#;

/// Return a complete `<script>` tag that embeds the hot-reload client inline.
///
/// This is useful for injecting directly into an HTML response without needing
/// to serve a separate `.js` file.
pub fn inline_script_tag(port: u16) -> String {
    // Replace the hardcoded DEFAULT_PORT in the JS with the actual port.
    let js = if port != DEFAULT_WS_PORT {
        CLIENT_JS.replace(
            &format!("var DEFAULT_PORT = {};", DEFAULT_WS_PORT),
            &format!("var DEFAULT_PORT = {};", port),
        )
    } else {
        CLIENT_JS.to_string()
    };

    format!("<script>{}</script>", js)
}

/// Return a `<script src="...">` tag pointing at an external URL.
///
/// Optionally includes a `data-port` attribute when the port differs from the
/// default.
pub fn external_script_tag(src: &str, port: u16) -> String {
    if port != DEFAULT_WS_PORT {
        format!(r#"<script src="{}" data-port="{}"></script>"#, src, port)
    } else {
        format!(r#"<script src="{}"></script>"#, src)
    }
}

/// Return a user-friendly help string showing how to add the client to a page.
pub fn snippet_help(port: u16) -> String {
    let mut help =
        String::from("Add one of the following to your HTML pages to enable hot-reloading:\n\n");

    help.push_str("Option 1 — inline script (no extra requests):\n\n");
    help.push_str("  ");
    help.push_str(&inline_script_tag(port));
    help.push_str("\n\n");

    help.push_str("Option 2 — external script tag (if you serve the JS file yourself):\n\n");
    help.push_str("  ");
    help.push_str(&external_script_tag("/hotreload-client.js", port));
    help.push('\n');

    help
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_js_is_valid_string() {
        // Ensure the embedded JS is non-empty and starts with an IIFE.
        assert!(!CLIENT_JS.is_empty());
        assert!(CLIENT_JS.starts_with("(function()"));
        assert!(CLIENT_JS.trim_end().ends_with("})();"));
    }

    #[test]
    fn client_js_contains_key_handlers() {
        assert!(CLIENT_JS.contains("\"reload\""));
        assert!(CLIENT_JS.contains("\"inject-css\""));
        assert!(CLIENT_JS.contains("location.reload()"));
        assert!(CLIENT_JS.contains("data-hotreload"));
        assert!(CLIENT_JS.contains("WebSocket"));
    }

    #[test]
    fn inline_script_tag_default_port() {
        let tag = inline_script_tag(DEFAULT_WS_PORT);
        assert!(tag.starts_with("<script>"));
        assert!(tag.ends_with("</script>"));
        assert!(tag.contains("var DEFAULT_PORT = 3012;"));
    }

    #[test]
    fn inline_script_tag_custom_port() {
        let tag = inline_script_tag(4000);
        assert!(tag.contains("var DEFAULT_PORT = 4000;"));
        assert!(!tag.contains("var DEFAULT_PORT = 3012;"));
    }

    #[test]
    fn external_script_tag_default_port() {
        let tag = external_script_tag("/hotreload-client.js", DEFAULT_WS_PORT);
        assert_eq!(tag, r#"<script src="/hotreload-client.js"></script>"#);
    }

    #[test]
    fn external_script_tag_custom_port() {
        let tag = external_script_tag("/hotreload-client.js", 5000);
        assert_eq!(
            tag,
            r#"<script src="/hotreload-client.js" data-port="5000"></script>"#
        );
    }

    #[test]
    fn snippet_help_contains_both_options() {
        let help = snippet_help(3012);
        assert!(help.contains("Option 1"));
        assert!(help.contains("Option 2"));
        assert!(help.contains("<script>"));
        assert!(help.contains("</script>"));
    }

    #[test]
    fn client_js_has_reconnect_logic() {
        assert!(CLIENT_JS.contains("reconnectDelay"));
        assert!(CLIENT_JS.contains("maxReconnectDelay"));
        assert!(CLIENT_JS.contains("scheduleReconnect"));
    }

    #[test]
    fn client_js_supports_data_port_attribute() {
        assert!(CLIENT_JS.contains("data-port"));
        assert!(CLIENT_JS.contains("getAttribute"));
    }
}
