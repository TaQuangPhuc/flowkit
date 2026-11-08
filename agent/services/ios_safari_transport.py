"""iOS Safari WebKit Remote Inspector Transport for FlowKit.

Connects to `ios-webkit-debug-proxy` (WebKit Remote Inspector over usbmuxd / USB),
discovers the open `flow.google.com` Safari tab on an attached iPhone, and provides
the full extension RPC surface (`batch_rpc`, `flow_tab_health`, `reload_flow_tab`).

Enables an iPhone to serve Flow generate requests with genuine mobile TLS/JA3,
mobile sensor fingerprints, and real Safari execution context.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import re
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Optional

import websockets

logger = logging.getLogger("ios_safari_transport")

RECAPTCHA_SITE_KEY = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"


class IOSSafariTransport:
    """WebKit Remote Inspector transport driving iOS Safari over USB."""

    def __init__(
        self,
        proxy_host: str = "127.0.0.1",
        proxy_port: int = 9322,
        profile_id: str = "iphone-1",
    ):
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.profile_id = profile_id
        self._ws: Any = None
        self._page_target_id: Optional[str] = None
        self._tab_info: dict = {}
        self._injected: bool = False
        self._msg_id: int = 0
        self._lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        if self._ws is None:
            return False
        try:
            return getattr(self._ws, "close_code", None) is None
        except Exception:
            return True

    def find_tab(self) -> Optional[dict]:
        """Query debug proxy /json endpoint for the flow.google.com tab."""
        url = f"http://{self.proxy_host}:{self.proxy_port}/json"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                tabs = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            logger.error("Failed to query debug proxy at %s: %s", url, e)
            return None

        flow_tab = None
        for t in tabs:
            if "flow.google.com" in t.get("url", ""):
                flow_tab = t
                break
        if not flow_tab and tabs:
            flow_tab = tabs[0]
        return flow_tab

    async def connect(self, timeout: float = 15.0) -> bool:
        """Connect to Safari tab WebKit WebSocket and discover page Target."""
        tab = self.find_tab()
        if not tab:
            logger.error("No Safari tabs found on %s:%s", self.proxy_host, self.proxy_port)
            return False

        self._tab_info = tab
        ws_url = tab.get("webSocketDebuggerUrl")
        if not ws_url:
            logger.error("No webSocketDebuggerUrl in tab info: %s", tab)
            return False

        logger.info("Connecting to Safari Inspector at %s (title=%s)", ws_url, tab.get("title"))
        self._ws = await websockets.connect(ws_url, open_timeout=timeout)
        self._page_target_id = None

        # Read initial Target.targetCreated messages from WebKit
        if self._ws is not None:
            try:
                start = time.time()
                while time.time() - start < 3.0:
                    msg_raw = await asyncio.wait_for(self._ws.recv(), timeout=0.6)
                    data = json.loads(msg_raw)
                    if data.get("method") == "Target.targetCreated":
                        tinfo = data.get("params", {}).get("targetInfo", {})
                        if tinfo.get("type") == "page":
                            self._page_target_id = tinfo.get("targetId")
                            logger.info("Bound to WebKit page targetId: %s", self._page_target_id)
            except (asyncio.TimeoutError, Exception):
                pass

        if not self._page_target_id:
            self._page_target_id = "page-42"
            logger.warning("Using fallback page targetId: %s", self._page_target_id)

        # Inject extension/injected.js script
        await self.inject_scripts()
        return True

    async def close(self):
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def evaluate(self, expression: str, timeout: float = 30.0) -> dict:
        """Evaluate synchronous JS in the page context via Target.sendMessageToTarget."""
        async with self._lock:
            if not self.is_connected or self._ws is None:
                raise RuntimeError("iOS Safari transport is not connected")

            self._msg_id += 1
            cmd_id = self._msg_id
            inner_cmd = {
                "id": cmd_id,
                "method": "Runtime.evaluate",
                "params": {
                    "expression": expression,
                    "returnByValue": True,
                },
            }
            outer_payload = {
                "id": cmd_id + 50000,
                "method": "Target.sendMessageToTarget",
                "params": {
                    "targetId": self._page_target_id,
                    "message": json.dumps(inner_cmd),
                },
            }
            await self._ws.send(json.dumps(outer_payload))

            start_t = time.time()
            while time.time() - start_t < timeout:
                try:
                    msg_raw = await asyncio.wait_for(self._ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    logger.error("Evaluate timeout (%ss) - possible iOS background tab suspension!", timeout)
                    return {"success": False, "error": f"Evaluation timed out ({timeout}s) - tab may be suspended"}

                data = json.loads(msg_raw)
                if data.get("method") == "Target.dispatchMessageFromTarget":
                    inner_msg = json.loads(data.get("params", {}).get("message", "{}"))
                    if inner_msg.get("id") == cmd_id:
                        res = inner_msg.get("result", {})
                        if "result" in res:
                            val = res["result"].get("value")
                            was_thrown = res.get("wasThrown", False)
                            return {"success": not was_thrown, "value": val, "raw": res}
                        if "error" in inner_msg:
                            return {"success": False, "error": inner_msg["error"]}
                        return {"success": True, "raw": inner_msg}
            return {"success": False, "error": f"Evaluation timed out ({timeout}s)"}

    async def evaluate_async(self, expression: str, timeout: float = 45.0) -> dict:
        """Evaluate async JS (Promise) by resolving into a global bucket and polling."""
        async_id = f"async_{int(time.time()*1000)}_{random.randint(1000, 9999)}"
        init_code = f"""
        (() => {{
            window.__flow_async_results = window.__flow_async_results || {{}};
            window.__flow_async_results['{async_id}'] = {{ done: false }};
            (async () => {{
                try {{
                    const _val = await ({expression});
                    window.__flow_async_results['{async_id}'] = {{ done: true, ok: true, result: _val }};
                }} catch (err) {{
                    window.__flow_async_results['{async_id}'] = {{ done: true, ok: false, error: (err && err.message) ? err.message : String(err) }};
                }}
            }})();
        }})()
        """
        await self.evaluate(init_code)

        start_t = time.time()
        while time.time() - start_t < timeout:
            await asyncio.sleep(0.15)
            check = await self.evaluate(f"window.__flow_async_results && window.__flow_async_results['{async_id}']")
            val = check.get("value")
            if isinstance(val, dict) and val.get("done"):
                await self.evaluate(f"delete window.__flow_async_results['{async_id}']")
                if val.get("ok"):
                    return {"success": True, "value": val.get("result")}
                return {"success": False, "error": val.get("error")}
        return {"success": False, "error": f"Async evaluation timed out ({timeout}s)"}

    async def inject_scripts(self) -> bool:
        """Inject extension/injected.js into the Safari page context."""
        injected_path = Path(__file__).parent.parent.parent / "extension" / "injected.js"
        if not injected_path.exists():
            injected_path = Path("extension/injected.js").resolve()

        if not injected_path.exists():
            logger.error("injected.js not found at %s", injected_path)
            return False

        with open(injected_path, "r", encoding="utf-8") as f:
            code = f.read()

        await self.evaluate(code)
        check = await self.evaluate("typeof window.__flowExecuteBatchOnce")
        if check.get("value") == "function":
            self._injected = True
            logger.info("Successfully injected extension/injected.js into Safari tab")
            return True

        logger.warning("__flowExecuteBatchOnce not found after injection, defining fallback")
        return False

    async def flow_tab_health(self) -> dict:
        """Probe the live page state using classifyFlowPage()."""
        res = await self.evaluate("typeof classifyFlowPage === 'function' ? classifyFlowPage() : null")
        val = res.get("value")
        if isinstance(val, dict):
            return {
                "alive": True,
                "page_state": val.get("page_state", "unknown"),
                "ready": val.get("ready", False),
                "url": val.get("url", self._tab_info.get("url", "")),
                "title": val.get("title", self._tab_info.get("title", "")),
                "documentId": val.get("documentId"),
            }
        return {
            "alive": False,
            "page_state": "not_injected",
            "ready": False,
            "url": self._tab_info.get("url", ""),
            "title": self._tab_info.get("title", ""),
        }

    async def reload_flow_tab(self, wait_ready: bool = True, timeout: float = 30.0) -> dict:
        """Reload the Safari tab and re-inject scripts once app_ready."""
        await self.evaluate("location.reload()")
        await asyncio.sleep(2.0)
        self._injected = False

        start = time.time()
        while time.time() - start < timeout:
            await self.inject_scripts()
            health = await self.flow_tab_health()
            if health.get("ready") and health.get("page_state") == "app_ready":
                return {"ok": True, "health": health}
            await asyncio.sleep(1.0)
        return {"ok": False, "error": "Tab reload wait timed out"}

    async def mint_recaptcha(self, action: str = "IMAGE_GENERATION", timeout: float = 25.0) -> str:
        """Mint in-page reCAPTCHA Enterprise token."""
        mint_code = f"""
        (async () => {{
            if (typeof waitForGrecaptcha === 'function') {{
                await waitForGrecaptcha();
            }}
            return await grecaptcha.enterprise.execute('{RECAPTCHA_SITE_KEY}', {{ action: '{action}' }});
        }})()
        """
        res = await self.evaluate_async(mint_code, timeout=timeout)
        if res.get("success") and res.get("value"):
            return str(res["value"])
        raise RuntimeError(f"reCAPTCHA mint failed: {res.get('error')}")

    async def batch_rpc(self, params: dict, timeout: float = 60.0) -> dict:
        """Execute a batchexecute RPC call inside the Safari tab."""
        rpcid = params.get("rpcid")
        freq = params.get("freq")
        max_text = params.get("maxText")
        match = params.get("match")
        custom_path = params.get("customPath")
        captcha_action = params.get("captchaAction")
        captcha_token = params.get("captchaToken")

        # Mint in-page captcha if requested and replace __CAPTCHA__ placeholder
        if isinstance(freq, str) and "__CAPTCHA__" in freq:
            if captcha_token:
                token = captcha_token
            else:
                action = captcha_action or "IMAGE_GENERATION"
                token = await self.mint_recaptcha(action)
            freq = freq.replace("__CAPTCHA__", token)

        payload_obj = {
            "rpcid": rpcid,
            "freq": freq,
            "maxText": max_text,
            "match": match,
            "path": custom_path,
            "documentId": None,
            "expiresAt": int(time.time() * 1000) + int(timeout * 1000),
        }

        rpc_expr = f"""window.__flowExecuteBatchOnce({json.dumps(payload_obj)})"""
        res = await self.evaluate_async(rpc_expr, timeout=timeout)
        if res.get("success") and isinstance(res.get("value"), dict):
            return res["value"]
        if not res.get("success"):
            return {"error": res.get("error", "BATCH_RPC_FAILED")}
        return {"error": "INVALID_BATCH_RESPONSE"}


FLOW_PROJECT_RE = re.compile(
    r"flow\.google\.com/project/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.IGNORECASE,
)


def flow_project_from_url(url: str | None) -> str | None:
    if not url:
        return None
    m = FLOW_PROJECT_RE.search(str(url))
    return m.group(1) if m else None


async def run_ios_safari_bridge(
    agent_ws_url: str = "ws://127.0.0.1:9222",
    proxy_host: str = "127.0.0.1",
    proxy_port: int = 9322,
    profile_id: str = "iphone-1",
):
    """Continuous bridge loop: connects to Agent WS as an extension worker, routes RPCs to Safari."""
    while True:
        try:
            transport = IOSSafariTransport(proxy_host=proxy_host, proxy_port=proxy_port, profile_id=profile_id)
            connected = await transport.connect()
            if not connected:
                logger.warning("Could not connect to Safari at %s:%s. Retrying in 5s...", proxy_host, proxy_port)
                await asyncio.sleep(5.0)
                continue

            logger.info("Safari transport ready. Connecting to FlowKit Agent WS at %s...", agent_ws_url)
            async with websockets.connect(agent_ws_url) as agent_ws:
                health = await transport.flow_tab_health()
                proj_id = flow_project_from_url(health.get("url") or transport._tab_info.get("url"))
                await agent_ws.send(json.dumps({
                    "type": "extension_ready",
                    "flowGuardVersion": "2026-09-21.2",
                    "flowKeyPresent": True,
                    "profileId": profile_id,
                    "flowProjectId": proj_id,
                    "transport": "ios_safari",
                }))
                await agent_ws.send(json.dumps({
                    "type": "token_captured",
                    "flowKey": "batch_rpc_active",
                }))
                logger.info("Handshake with Agent WS complete as '%s' (transport=ios_safari)", profile_id)

                async for raw_msg in agent_ws:
                    try:
                        msg = json.loads(raw_msg)
                        mid = msg.get("id")
                        method = msg.get("method")
                        params = msg.get("params", {})

                        if method in ("batch_rpc", "flow_batch_rpc"):
                            result = await transport.batch_rpc(params)
                            if "error" in result and not result.get("text"):
                                await agent_ws.send(json.dumps({
                                    "id": mid,
                                    "status": result.get("status", 502),
                                    "error": result["error"],
                                }))
                            else:
                                await agent_ws.send(json.dumps({
                                    "id": mid,
                                    "status": result.get("status", 200),
                                    "data": result.get("text", ""),
                                }))
                        elif method == "solve_captcha":
                            try:
                                action = params.get("captchaAction") or "IMAGE_GENERATION"
                                token = await transport.mint_recaptcha(action)
                                await agent_ws.send(json.dumps({"id": mid, "result": {"token": token}}))
                            except Exception as e:
                                await agent_ws.send(json.dumps({"id": mid, "result": {"error": str(e)}}))
                        elif method == "flow_tab_health":
                            result = await transport.flow_tab_health()
                            await agent_ws.send(json.dumps({"id": mid, "result": result}))
                        elif method == "reload_flow_tab":
                            result = await transport.reload_flow_tab()
                            await agent_ws.send(json.dumps({"id": mid, "result": result}))
                        elif method == "get_status":
                            health = await transport.flow_tab_health()
                            proj_id = flow_project_from_url(health.get("url") or transport._tab_info.get("url"))
                            await agent_ws.send(json.dumps({
                                "id": mid,
                                "result": {
                                    "state": "idle",
                                    "profileId": profile_id,
                                    "flowProjectId": proj_id,
                                    "transport": "ios_safari",
                                    "health": health,
                                },
                            }))
                        else:
                            await agent_ws.send(json.dumps({"id": mid, "result": {"ok": True}}))
                    except Exception as e:
                        logger.error("Error processing agent message: %s", e)
        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
            logger.warning("Connection lost (%s). Reconnecting in 3s...", e)
            await asyncio.sleep(3.0)
        except Exception as e:
            logger.error("Unexpected bridge error: %s. Reconnecting in 5s...", e)
            await asyncio.sleep(5.0)


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="FlowKit iOS Safari Transport Bridge")
    parser.add_argument("--agent-ws", default="ws://127.0.0.1:9222", help="FlowKit Agent WS URL")
    parser.add_argument("--proxy-host", default="127.0.0.1", help="ios-webkit-debug-proxy host")
    parser.add_argument("--proxy-port", type=int, default=9322, help="ios-webkit-debug-proxy port")
    parser.add_argument("--profile-id", default="iphone-1", help="Nick profile ID to register")
    args = parser.parse_args()

    try:
        asyncio.run(run_ios_safari_bridge(
            agent_ws_url=args.agent_ws,
            proxy_host=args.proxy_host,
            proxy_port=args.proxy_port,
            profile_id=args.profile_id,
        ))
    except KeyboardInterrupt:
        logger.info("iOS Safari Bridge stopped by user.")


if __name__ == "__main__":
    main()
