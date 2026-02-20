#!/usr/bin/env python3
"""
watchd hot-reload client for Python

Connects to a running watchd WebSocket server and executes callbacks
when reload or CSS-inject messages are received.

Features:
  - Automatic reconnection with exponential backoff (1s → 30s cap)
  - Configurable callbacks for reload and CSS injection events
  - Base64 content decoding for inject-css messages
  - Clean shutdown on Ctrl-C
  - Zero hard dependencies beyond the standard library (uses the
    popular `websockets` library if available, otherwise falls back
    to `websocket-client`)

Usage:

    # Basic usage — prints events to stdout
    python hotreload_client.py

    # Custom port
    python hotreload_client.py --port 4000

    # Custom host
    python hotreload_client.py --host 192.168.1.10 --port 3012

    # As a library
    from hotreload_client import HotReloadClient

    def on_reload():
        print("Reloading!")

    def on_css(path, content):
        print(f"CSS changed: {path}")

    client = HotReloadClient(
        port=3012,
        on_reload=on_reload,
        on_inject_css=on_css,
    )
    client.run()

Requirements:

    pip install websockets

License: MIT
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import signal
import sys
import time
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [hotreload] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("hotreload")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 3012
INITIAL_RECONNECT_DELAY = 1.0  # seconds
MAX_RECONNECT_DELAY = 30.0  # seconds

# ---------------------------------------------------------------------------
# Client (async, using `websockets` library)
# ---------------------------------------------------------------------------


class HotReloadClient:
    """Async WebSocket client that listens for watchd messages.

    Parameters
    ----------
    host:
        Hostname or IP where watchd is running.
    port:
        WebSocket port of the watchd server (default ``3012``).
    on_reload:
        Callback invoked when a ``reload`` message is received.
        Signature: ``() -> None``.
    on_inject_css:
        Callback invoked when an ``inject-css`` message is received.
        Signature: ``(path: str, decoded_css: str) -> None``.
    on_connect:
        Callback invoked when the WebSocket connection is established.
    on_disconnect:
        Callback invoked when the WebSocket connection is lost.
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        on_reload: Optional[Callable[[], None]] = None,
        on_inject_css: Optional[Callable[[str, str], None]] = None,
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.on_reload = on_reload
        self.on_inject_css = on_inject_css
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._running = True

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}"

    # -- public API --------------------------------------------------------

    def run(self) -> None:
        """Run the client synchronously (blocks until interrupted)."""
        try:
            asyncio.run(self._run_forever())
        except KeyboardInterrupt:
            log.info("interrupted — exiting")

    async def run_async(self) -> None:
        """Run the client as an awaitable coroutine."""
        await self._run_forever()

    def stop(self) -> None:
        """Signal the client to disconnect and stop reconnecting."""
        self._running = False

    # -- internal ----------------------------------------------------------

    async def _run_forever(self) -> None:
        """Connect, listen, reconnect loop."""
        try:
            import websockets  # type: ignore[import-untyped]
        except ImportError:
            log.error(
                "The 'websockets' package is required. Install it with:\n"
                "  pip install websockets"
            )
            sys.exit(1)

        while self._running:
            try:
                log.info("connecting to %s", self.url)
                async with websockets.connect(self.url) as ws:
                    log.info("connected")
                    self._reconnect_delay = INITIAL_RECONNECT_DELAY
                    if self.on_connect:
                        self.on_connect()

                    await self._listen(ws)

            except (
                ConnectionRefusedError,
                ConnectionResetError,
                OSError,
            ) as exc:
                log.warning("connection failed: %s", exc)
            except Exception as exc:
                log.warning("unexpected error: %s", exc)
            finally:
                if self.on_disconnect:
                    self.on_disconnect()

            if not self._running:
                break

            log.info("reconnecting in %.1fs", self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, MAX_RECONNECT_DELAY)

    async def _listen(self, ws) -> None:  # noqa: ANN001 (websockets type)
        """Read messages from the WebSocket and dispatch them."""
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("failed to parse message: %s", raw)
                continue

            msg_type = msg.get("type")

            if msg_type == "reload":
                log.info("received reload")
                if self.on_reload:
                    self.on_reload()

            elif msg_type == "inject-css":
                path = msg.get("path", "")
                content_b64 = msg.get("content", "")
                try:
                    decoded = base64.b64decode(content_b64).decode("utf-8")
                except Exception as exc:
                    log.warning("failed to decode CSS content: %s", exc)
                    decoded = ""
                log.info("received inject-css for %s (%d bytes)", path, len(decoded))
                if self.on_inject_css:
                    self.on_inject_css(path, decoded)

            else:
                log.info("unknown message type: %s", msg_type)


# ---------------------------------------------------------------------------
# Synchronous fallback using websocket-client
# ---------------------------------------------------------------------------


class HotReloadClientSync:
    """Synchronous WebSocket client using the ``websocket-client`` package.

    Use this if you prefer a thread-based approach or cannot use ``asyncio``.
    The API mirrors :class:`HotReloadClient` but blocks the calling thread.

    Requirements::

        pip install websocket-client
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        on_reload: Optional[Callable[[], None]] = None,
        on_inject_css: Optional[Callable[[str, str], None]] = None,
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.on_reload = on_reload
        self.on_inject_css = on_inject_css
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._running = True

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}"

    def run(self) -> None:
        """Run the client (blocks until interrupted or stopped)."""
        try:
            import websocket  # type: ignore[import-untyped]
        except ImportError:
            log.error(
                "The 'websocket-client' package is required. Install it with:\n"
                "  pip install websocket-client"
            )
            sys.exit(1)

        while self._running:
            try:
                log.info("connecting to %s", self.url)
                ws = websocket.create_connection(self.url)
                log.info("connected")
                self._reconnect_delay = INITIAL_RECONNECT_DELAY
                if self.on_connect:
                    self.on_connect()

                try:
                    while self._running:
                        raw = ws.recv()
                        if not raw:
                            break
                        self._dispatch(raw)
                finally:
                    ws.close()

            except (
                ConnectionRefusedError,
                ConnectionResetError,
                OSError,
            ) as exc:
                log.warning("connection failed: %s", exc)
            except Exception as exc:
                log.warning("unexpected error: %s", exc)
            finally:
                if self.on_disconnect:
                    self.on_disconnect()

            if not self._running:
                break

            log.info("reconnecting in %.1fs", self._reconnect_delay)
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, MAX_RECONNECT_DELAY)

    def stop(self) -> None:
        self._running = False

    def _dispatch(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("failed to parse message: %s", raw)
            return

        msg_type = msg.get("type")

        if msg_type == "reload":
            log.info("received reload")
            if self.on_reload:
                self.on_reload()

        elif msg_type == "inject-css":
            path = msg.get("path", "")
            content_b64 = msg.get("content", "")
            try:
                decoded = base64.b64decode(content_b64).decode("utf-8")
            except Exception as exc:
                log.warning("failed to decode CSS content: %s", exc)
                decoded = ""
            log.info("received inject-css for %s (%d bytes)", path, len(decoded))
            if self.on_inject_css:
                self.on_inject_css(path, decoded)

        else:
            log.info("unknown message type: %s", msg_type)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _default_on_reload() -> None:
    log.info("→ reload triggered (override on_reload for custom behaviour)")


def _default_on_css(path: str, content: str) -> None:
    preview = content[:120].replace("\n", " ")
    if len(content) > 120:
        preview += "…"
    log.info("→ CSS [%s]: %s", path, preview)


def main() -> None:
    parser = argparse.ArgumentParser(description="watchd hot-reload client for Python")
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"watchd server hostname (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"watchd WebSocket port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="use the synchronous client (websocket-client) instead of asyncio",
    )
    args = parser.parse_args()

    if args.sync:
        client = HotReloadClientSync(
            host=args.host,
            port=args.port,
            on_reload=_default_on_reload,
            on_inject_css=_default_on_css,
        )
    else:
        client = HotReloadClient(
            host=args.host,
            port=args.port,
            on_reload=_default_on_reload,
            on_inject_css=_default_on_css,
        )

    # Handle Ctrl-C gracefully.
    def _sigint(sig, frame):  # noqa: ANN001
        client.stop()

    signal.signal(signal.SIGINT, _sigint)

    client.run()


if __name__ == "__main__":
    main()
