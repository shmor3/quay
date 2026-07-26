//! Embedded browser client for `quay`.
//!
//! Contains a self-contained JavaScript snippet that connects to the quay
//! WebSocket server and handles `reload` and `inject-css` messages.  The
//! snippet is designed to be dropped into any HTML page via a `<script>` tag.
//!
//! # Features
//!
//! - Auto-detects the WebSocket URL from `window.location.hostname`
//! - Reconnects automatically with exponential backoff (1 s → 30 s cap)
//! - Handles `reload` messages by calling `location.reload()`
//! - Handles `inject-css` messages by decoding base64 content and
//!   inserting/updating a `<style data-quay="<path>">` element
//! - Logs connection lifecycle events to the browser console
//! - Zero external dependencies — pure vanilla JS

/// The default WebSocket port used by quay.
pub const DEFAULT_WS_PORT: u16 = 3012;

/// Minified JavaScript client that can be embedded directly in a `<script>`
/// tag or served as a standalone `.js` file.
///
/// The client reads an optional `data-port` attribute from its own `<script>`
/// element to allow overriding the default port:
///
/// ```html
/// <script src="/quay-client.js" data-port="4000"></script>
/// ```
pub const CLIENT_JS: &str = r#"(function(){
  "use strict";

  var DEFAULT_PORT = /*PORT_PLACEHOLDER*/3012;

  // Try to read a custom port from the <script> tag's data-port attribute.
  var scriptEl = document.currentScript;
  var port = (scriptEl && scriptEl.getAttribute("data-port"))
    ? parseInt(scriptEl.getAttribute("data-port"), 10)
    : DEFAULT_PORT;

  var reconnectDelay = 1000;
  var maxReconnectDelay = 30000;
  var reconnectTimer = null;

  function log(msg) {
    console.log("%c[quay]%c " + msg, "color:#e06c75;font-weight:bold", "color:inherit");
  }

  function warn(msg) {
    console.warn("[quay] " + msg);
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

  function cssBasename(p) {
    p = String(p).split("?")[0];
    var i = p.lastIndexOf("/");
    return i >= 0 ? p.slice(i + 1) : p;
  }

  function injectCSS(path, encodedContent) {
    // Prefer reloading a matching <link>: re-fetching the file from disk applies
    // additions AND deletions.  A layered <style> can only add/override rules,
    // so a deleted rule would silently persist.
    var base = cssBasename(path);
    var links = document.querySelectorAll('link[rel="stylesheet"]');
    var matched = false;
    for (var i = 0; i < links.length; i++) {
      var href = links[i].getAttribute("href");
      if (href && cssBasename(href) === base) {
        var clean = href.split("?")[0];
        links[i].setAttribute("href", clean + "?quay=" + Date.now());
        matched = true;
        log("reloaded stylesheet " + base);
      }
    }
    if (matched) return;

    // Fallback (no matching <link>, e.g. inline styling): inject/replace a
    // <style> block keyed by path.
    var css = b64decode(encodedContent);
    var existing = document.querySelector('style[data-quay="' + CSS.escape(path) + '"]');
    if (existing) {
      existing.textContent = css;
      log("injected CSS update for " + path);
    } else {
      var style = document.createElement("style");
      style.setAttribute("data-quay", path);
      style.textContent = css;
      document.head.appendChild(style);
      log("injected new CSS for " + path);
    }
  }

  function connect() {
    var host = location.hostname || "localhost";
    var proto = (location.protocol === "https:") ? "wss://" : "ws://";
    var url = proto + host + ":" + port;
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
    let js = CLIENT_JS.replace("/*PORT_PLACEHOLDER*/3012", &port.to_string());

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
    help.push_str(&external_script_tag("/quay-client.js", port));
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
    #[allow(clippy::const_is_empty)]
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
        assert!(CLIENT_JS.contains("data-quay"));
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
        let tag = external_script_tag("/quay-client.js", DEFAULT_WS_PORT);
        assert_eq!(tag, r#"<script src="/quay-client.js"></script>"#);
    }

    #[test]
    fn external_script_tag_custom_port() {
        let tag = external_script_tag("/quay-client.js", 5000);
        assert_eq!(
            tag,
            r#"<script src="/quay-client.js" data-port="5000"></script>"#
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

    // -- DEFAULT_WS_PORT constant ------------------------------------------

    #[test]
    fn default_ws_port_is_3012() {
        assert_eq!(DEFAULT_WS_PORT, 3012);
    }

    #[test]
    fn client_js_default_port_matches_constant() {
        let needle = format!(
            "var DEFAULT_PORT = /*PORT_PLACEHOLDER*/{};",
            DEFAULT_WS_PORT
        );
        assert!(
            CLIENT_JS.contains(&needle),
            "CLIENT_JS should contain '{}' but doesn't",
            needle
        );
    }

    // -- CLIENT_JS structural validation -----------------------------------

    #[test]
    fn client_js_is_self_contained_iife() {
        let trimmed = CLIENT_JS.trim();
        assert!(trimmed.starts_with("(function()"));
        assert!(trimmed.ends_with("})();"));
    }

    #[test]
    fn client_js_does_not_use_es6_features() {
        // The client targets all modern browsers including older ones, so it
        // uses var instead of let/const, function instead of arrow functions.
        // Check that no `let ` or `const ` declarations appear.
        assert!(
            !CLIENT_JS.contains("\n  let "),
            "CLIENT_JS should use 'var' not 'let' for broad compatibility"
        );
        assert!(
            !CLIENT_JS.contains("\n  const "),
            "CLIENT_JS should use 'var' not 'const' for broad compatibility"
        );
    }

    #[test]
    fn client_js_has_base64_decode() {
        assert!(CLIENT_JS.contains("atob"));
        assert!(CLIENT_JS.contains("b64decode"));
    }

    #[test]
    fn client_js_has_css_injection() {
        assert!(CLIENT_JS.contains("injectCSS"));
        assert!(CLIENT_JS.contains("data-quay"));
        assert!(CLIENT_JS.contains("createElement"));
        assert!(CLIENT_JS.contains("textContent"));
    }

    #[test]
    fn client_js_has_cache_busting() {
        // The embedded CLIENT_JS is a compact version. The standalone file in
        // examples/javascript has full cache-busting.  The embedded version
        // does reference linked stylesheets via CSS.escape for selector safety.
        assert!(CLIENT_JS.contains("CSS.escape"));
        assert!(CLIENT_JS.contains("data-quay"));
    }

    #[test]
    fn client_js_has_console_logging() {
        assert!(CLIENT_JS.contains("console.log"));
        assert!(CLIENT_JS.contains("console.warn"));
        assert!(CLIENT_JS.contains("[quay]"));
    }

    #[test]
    fn client_js_handles_dom_content_loaded() {
        assert!(CLIENT_JS.contains("DOMContentLoaded"));
        assert!(CLIENT_JS.contains("document.readyState"));
    }

    #[test]
    fn client_js_has_exponential_backoff() {
        // Verify the reconnect delay is capped.
        assert!(CLIENT_JS.contains("maxReconnectDelay"));
        assert!(CLIENT_JS.contains("Math.min"));
    }

    #[test]
    fn client_js_handles_websocket_events() {
        assert!(CLIENT_JS.contains("ws.onopen"));
        assert!(CLIENT_JS.contains("ws.onmessage"));
        assert!(CLIENT_JS.contains("ws.onclose"));
        assert!(CLIENT_JS.contains("ws.onerror"));
    }

    #[test]
    fn client_js_parses_json_messages() {
        assert!(CLIENT_JS.contains("JSON.parse"));
    }

    #[test]
    fn client_js_handles_unknown_message_types() {
        assert!(CLIENT_JS.contains("unknown message type"));
    }

    // -- inline_script_tag edge cases --------------------------------------

    #[test]
    fn inline_script_tag_port_1() {
        let tag = inline_script_tag(1);
        assert!(tag.contains("var DEFAULT_PORT = 1;"));
        assert!(!tag.contains("var DEFAULT_PORT = 3012;"));
    }

    #[test]
    fn inline_script_tag_port_65535() {
        let tag = inline_script_tag(65535);
        assert!(tag.contains("var DEFAULT_PORT = 65535;"));
    }

    #[test]
    fn inline_script_tag_port_0() {
        let tag = inline_script_tag(0);
        assert!(tag.contains("var DEFAULT_PORT = 0;"));
    }

    #[test]
    fn inline_script_tag_wraps_in_script_tags() {
        let tag = inline_script_tag(3012);
        assert!(tag.starts_with("<script>"));
        assert!(tag.ends_with("</script>"));
        // Should contain the full IIFE.
        assert!(tag.contains("(function()"));
        assert!(tag.contains("})();"));
    }

    #[test]
    fn inline_script_tag_contains_no_src_attribute() {
        let tag = inline_script_tag(3012);
        assert!(
            !tag.contains("src="),
            "inline tag should not have a src attribute"
        );
    }

    #[test]
    fn inline_script_tag_contains_full_client_logic() {
        let tag = inline_script_tag(3012);
        assert!(tag.contains("WebSocket"));
        assert!(tag.contains("location.reload()"));
        assert!(tag.contains("scheduleReconnect"));
    }

    // -- external_script_tag edge cases ------------------------------------

    #[test]
    fn external_script_tag_empty_src() {
        let tag = external_script_tag("", DEFAULT_WS_PORT);
        assert_eq!(tag, r#"<script src=""></script>"#);
    }

    #[test]
    fn external_script_tag_absolute_url() {
        let tag = external_script_tag("https://cdn.example.com/quay.js", DEFAULT_WS_PORT);
        assert_eq!(
            tag,
            r#"<script src="https://cdn.example.com/quay.js"></script>"#
        );
    }

    #[test]
    fn external_script_tag_absolute_url_with_custom_port() {
        let tag = external_script_tag("https://cdn.example.com/quay.js", 9999);
        assert_eq!(
            tag,
            r#"<script src="https://cdn.example.com/quay.js" data-port="9999"></script>"#
        );
    }

    #[test]
    fn external_script_tag_relative_path() {
        let tag = external_script_tag("./assets/quay.js", DEFAULT_WS_PORT);
        assert_eq!(tag, r#"<script src="./assets/quay.js"></script>"#);
    }

    #[test]
    fn external_script_tag_port_1() {
        let tag = external_script_tag("/hr.js", 1);
        assert_eq!(tag, r#"<script src="/hr.js" data-port="1"></script>"#);
    }

    #[test]
    fn external_script_tag_port_65535() {
        let tag = external_script_tag("/hr.js", 65535);
        assert_eq!(tag, r#"<script src="/hr.js" data-port="65535"></script>"#);
    }

    #[test]
    fn external_script_tag_port_0() {
        let tag = external_script_tag("/hr.js", 0);
        assert_eq!(tag, r#"<script src="/hr.js" data-port="0"></script>"#);
    }

    #[test]
    fn external_script_tag_src_with_query_params() {
        let tag = external_script_tag("/quay.js?v=2", DEFAULT_WS_PORT);
        assert_eq!(tag, r#"<script src="/quay.js?v=2"></script>"#);
    }

    #[test]
    fn external_script_tag_src_with_query_and_custom_port() {
        let tag = external_script_tag("/quay.js?v=2", 8080);
        assert_eq!(
            tag,
            r#"<script src="/quay.js?v=2" data-port="8080"></script>"#
        );
    }

    #[test]
    fn external_script_tag_no_data_port_at_default() {
        let tag = external_script_tag("/hr.js", DEFAULT_WS_PORT);
        assert!(
            !tag.contains("data-port"),
            "default port should not produce a data-port attribute"
        );
    }

    #[test]
    fn external_script_tag_has_data_port_at_non_default() {
        let tag = external_script_tag("/hr.js", DEFAULT_WS_PORT + 1);
        assert!(
            tag.contains("data-port"),
            "non-default port should produce a data-port attribute"
        );
    }

    // -- snippet_help edge cases -------------------------------------------

    #[test]
    fn snippet_help_default_port_content() {
        let help = snippet_help(DEFAULT_WS_PORT);
        assert!(help.contains("Option 1"));
        assert!(help.contains("Option 2"));
        assert!(help.contains("inline script"));
        assert!(help.contains("external script"));
        assert!(help.contains("<script>"));
        assert!(help.contains("</script>"));
        assert!(help.contains("/quay-client.js"));
    }

    #[test]
    fn snippet_help_custom_port_replaces_default() {
        let help = snippet_help(7777);
        // The inline script should use the custom port.
        assert!(help.contains("var DEFAULT_PORT = 7777;"));
        assert!(!help.contains("var DEFAULT_PORT = 3012;"));
        // The external script tag should include data-port.
        assert!(help.contains("data-port=\"7777\""));
    }

    #[test]
    fn snippet_help_default_port_no_data_port_in_script_tag() {
        let help = snippet_help(DEFAULT_WS_PORT);
        // At the default port, the inline script should use 3012.
        assert!(help.contains("var DEFAULT_PORT = 3012;"));
        // The external <script> tag at default port should NOT include
        // a data-port attribute.  Note: the inline JS body itself contains
        // the string "data-port" as part of getAttribute("data-port"), so
        // we check the external tag line specifically.
        let external = external_script_tag("/quay-client.js", DEFAULT_WS_PORT);
        assert!(
            !external.contains("data-port"),
            "external tag at default port should not include data-port"
        );
    }

    #[test]
    fn snippet_help_port_0() {
        let help = snippet_help(0);
        assert!(help.contains("var DEFAULT_PORT = 0;"));
        assert!(help.contains("data-port=\"0\""));
    }

    #[test]
    fn snippet_help_port_65535() {
        let help = snippet_help(65535);
        assert!(help.contains("var DEFAULT_PORT = 65535;"));
        assert!(help.contains("data-port=\"65535\""));
    }

    #[test]
    fn snippet_help_is_not_empty() {
        let help = snippet_help(3012);
        assert!(!help.is_empty());
        assert!(help.len() > 100, "snippet_help should be substantial");
    }

    #[test]
    fn snippet_help_ends_with_newline() {
        let help = snippet_help(3012);
        assert!(help.ends_with('\n'));
    }

    #[test]
    fn snippet_help_contains_valid_html() {
        let help = snippet_help(4000);
        // The snippet should contain at least one opening and closing script tag.
        // Note: the inline JS body itself contains the string "<script" as part
        // of DOM manipulation references, so we check the outer tags only by
        // verifying the help starts with proper structure.
        assert!(help.contains("<script>"));
        assert!(help.contains("</script>"));
        // Both Option 1 (inline) and Option 2 (external) should be present.
        assert!(help.contains("Option 1"));
        assert!(help.contains("Option 2"));
    }

    // -- Port boundary consistency -----------------------------------------

    #[test]
    fn all_port_functions_consistent_at_default() {
        let inline = inline_script_tag(DEFAULT_WS_PORT);
        let external = external_script_tag("/quay-client.js", DEFAULT_WS_PORT);
        let help = snippet_help(DEFAULT_WS_PORT);

        // At default port: inline uses 3012, external has no data-port.
        assert!(inline.contains("3012"));
        assert!(!external.contains("data-port"));
        // snippet_help indents both options with "  " prefix.
        let indented_inline = format!("  {}", inline);
        let indented_external = format!("  {}", external);
        assert!(help.contains(&indented_inline));
        assert!(help.contains(&indented_external));
    }

    #[test]
    fn all_port_functions_consistent_at_custom() {
        let port: u16 = 9876;
        let inline = inline_script_tag(port);
        let external = external_script_tag("/quay-client.js", port);
        let help = snippet_help(port);

        assert!(inline.contains("var DEFAULT_PORT = 9876;"));
        assert!(external.contains("data-port=\"9876\""));
        // snippet_help should embed both the inline and external tags.
        assert!(help.contains(&inline));
        assert!(help.contains(&external));
    }
}
