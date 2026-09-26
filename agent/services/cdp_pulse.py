"""Trusted-input activity pulse via Chrome DevTools Protocol.

reCAPTCHA Enterprise scores the *session*, not just the token request. A tab
that sits idle for hours mints low-score tokens; the same session after a few
seconds of real pointer activity mints noticeably better ones. CDP
``Input.dispatch*`` events are trusted (isTrusted=true) — the browser cannot
tell them from hardware input, unlike synthetic JS events.

Enabled per nick by the account ``cdp_debug`` flag, which makes
``_launch_args`` add ``--remote-debugging-port=0``. Chrome writes the chosen
port to ``DevToolsActivePort`` inside the user-data dir, so no port registry
is needed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import urllib.request
from pathlib import Path
from typing import Optional

from agent.services.chrome_nicks import chrome_data_dir

logger = logging.getLogger(__name__)

_ID = 0


def _devtools_port(nick_id: str) -> Optional[int]:
    f = chrome_data_dir(nick_id) / "DevToolsActivePort"
    try:
        return int(f.read_text().splitlines()[0].strip())
    except Exception:
        return None


def _targets(port: int) -> list[dict]:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/json", timeout=5) as r:
        return json.loads(r.read())


def _flow_target_ws(nick_id: str) -> Optional[str]:
    port = _devtools_port(nick_id)
    if not port:
        return None
    try:
        pages = [t for t in _targets(port) if t.get("type") == "page"]
    except Exception:
        return None
    for t in pages:
        if "flow.google.com" in (t.get("url") or ""):
            return t.get("webSocketDebuggerUrl")
    return (pages[0] or {}).get("webSocketDebuggerUrl") if pages else None


async def _cmd(ws, method: str, params: dict | None = None):
    global _ID
    _ID += 1
    mid = _ID
    await ws.send(json.dumps({"id": mid, "method": method, "params": params or {}}))
    while True:
        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        if msg.get("id") == mid:
            return msg.get("result") or msg


async def pulse(nick_id: str, seconds: float = 8.0, click: bool = False) -> dict:
    """Move/scroll the Flow tab like a person for ~`seconds`, then leave it.

    Optionally ends with a click (left mouse press+release) at the cursor's
    final position — useful right before a mint so the click is fresh in
    reCAPTCHA's signal window.
    """
    import websockets

    ws_url = _flow_target_ws(nick_id)
    if not ws_url:
        return {"ok": False, "error": "NO_CDP_TARGET", "nick_id": nick_id}

    moves = 0
    try:
        async with websockets.connect(ws_url, max_size=4 * 1024 * 1024,
                                      open_timeout=10, close_timeout=3) as ws:
            await _cmd(ws, "Page.bringToFront")
            # Get viewport so movement stays inside the window
            layout = await _cmd(ws, "Page.getLayoutMetrics")
            vw = int((layout.get("visualViewport") or {}).get("clientWidth") or 1280)
            vh = int((layout.get("visualViewport") or {}).get("clientHeight") or 720)

            x, y = random.uniform(vw * 0.2, vw * 0.5), random.uniform(vh * 0.3, vh * 0.6)
            deadline = asyncio.get_event_loop().time() + max(1.0, seconds)
            while asyncio.get_event_loop().time() < deadline:
                # wander to a nearby point in 3-6 stepped moves — like a hand
                tx = min(max(x + random.uniform(-300, 300), 10), vw - 10)
                ty = min(max(y + random.uniform(-200, 200), 10), vh - 10)
                steps = random.randint(3, 6)
                for i in range(1, steps + 1):
                    px = x + (tx - x) * i / steps + random.uniform(-2, 2)
                    py = y + (ty - y) * i / steps + random.uniform(-2, 2)
                    await _cmd(ws, "Input.dispatchMouseEvent", {
                        "type": "mouseMoved", "x": px, "y": py,
                    })
                    moves += 1
                    await asyncio.sleep(random.uniform(0.015, 0.05))
                x, y = tx, ty
                if random.random() < 0.4:
                    await _cmd(ws, "Input.dispatchMouseEvent", {
                        "type": "mouseWheel", "x": x, "y": y,
                        "deltaX": 0, "deltaY": random.choice([-240, -180, 160, 220]),
                    })
                await asyncio.sleep(random.uniform(0.15, 0.5))

            if click:
                await _cmd(ws, "Input.dispatchMouseEvent", {
                    "type": "mousePressed", "x": x, "y": y,
                    "button": "left", "clickCount": 1,
                })
                await asyncio.sleep(0.05)
                await _cmd(ws, "Input.dispatchMouseEvent", {
                    "type": "mouseReleased", "x": x, "y": y,
                    "button": "left", "clickCount": 1,
                })
        return {"ok": True, "nick_id": nick_id, "moves": moves, "clicked": click}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "nick_id": nick_id, "moves": moves}
