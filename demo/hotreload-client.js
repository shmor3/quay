// hotreload-client.js (copied for demo)
(function () {
  const port = 3012;
  const url =
    (location.protocol === "https:" ? "wss" : "ws") +
    "://" +
    (location.hostname || "127.0.0.1") +
    ":" +
    port;

  let ws;
  let reconnectDelay = 1000;

  function connect() {
    ws = new WebSocket(url);
    ws.addEventListener("open", () => {
      console.log("[hotreload] connected to", url);
      reconnectDelay = 1000;
    });

    ws.addEventListener("message", (ev) => {
      try {
        const data = JSON.parse(ev.data);
        if (data.type === "inject-css" && data.content) {
          const css = atob(data.content);
          const id = "hotreload-css-" + btoa(data.path).replace(/=/g, "");
          let el = document.querySelector('style[data-hotreload="' + id + '"]');
          if (!el) {
            el = document.createElement("style");
            el.setAttribute("data-hotreload", id);
            document.head.appendChild(el);
          }
          el.textContent = css;
          console.log("[hotreload] injected css for", data.path);
        } else if (data.type === "reload") {
          console.log("[hotreload] reload requested");
          window.location.reload();
        } else {
          console.log("[hotreload] unknown message", data);
        }
      } catch (e) {
        console.error("[hotreload] failed to parse message", e);
      }
    });

    ws.addEventListener("close", () => {
      console.warn(
        "[hotreload] disconnected, will retry in",
        reconnectDelay,
        "ms"
      );
      setTimeout(() => {
        reconnectDelay = Math.min(10000, reconnectDelay * 1.5);
        connect();
      }, reconnectDelay);
    });

    ws.addEventListener("error", (e) => {
      console.error("[hotreload] websocket error", e);
      ws.close();
    });
  }

  // start
  connect();
})();
