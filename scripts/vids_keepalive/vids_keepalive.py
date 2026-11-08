#!/usr/bin/env python3
"""Vids session keepalive — headless Chromium per nick.

The Vids/Omni cookie jar dies ~1-3h after a Chrome dump because the
__Secure-*PSIDTS/SIDCC rotation chain only advances inside a real page
(accounts.google.com/RotateCookiesPage iframe runs JS). Plain HTTP cannot
replicate it, so we drive headless Chromium over CDP:

  per nick: launch chromium (--proxy-server) -> import vids_cookies.json
    -> open docs.google.com/videos/d/<doc>/edit (RotateCookies fires)
    -> ~75s -> dump cookies back to vids_cookies.json
    -> signin redirect => touch _vids_dead

Cycles all nicks sequentially (one browser at a time, ~300MB), then sleeps.
"""
import itertools
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
import websocket

NICKS_DIR = Path("/opt/flowkit/nicks")
PORT = 19377
WAIT_S = 75          # time for RotateCookiesPage iframes to fire
CYCLE_GAP_S = 1200   # re-visit each nick every ~20 min
LOG = "/opt/flowkit/.scratch/vids-keepalive.log"


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    try:
        Path(LOG).parent.mkdir(parents=True, exist_ok=True)
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def proxy_parts(nick: str) -> tuple[str, str, str]:
    """Return (host:port, user, pass) from nicks/<nick>/proxy.txt."""
    raw = (NICKS_DIR / nick / "proxy.txt").read_text().strip()
    if "://" not in raw:
        raw = "http://" + raw
    u = urlparse(raw)
    return f"{u.hostname}:{u.port}", u.username or "", u.password or ""


class CDP:
    def __init__(self, ws_url: str):
        self.ws = websocket.create_connection(ws_url, timeout=60)
        self._id = itertools.count(1)

    def cmd(self, method: str, params: dict = None, session: str = None):
        mid = next(self._id)
        msg = {"id": mid, "method": method, "params": params or {}}
        if session:
            msg["sessionId"] = session
        self.ws.send(json.dumps(msg))
        while True:
            r = json.loads(self.ws.recv())
            if r.get("id") == mid:
                if "error" in r:
                    raise RuntimeError(f"{method}: {r['error']}")
                return r.get("result", {})
            self.handle_event(r)

    def handle_event(self, r: dict) -> None:
        pass

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass


def keepalive_nick(nick: str) -> str:
    d = NICKS_DIR / nick
    cookies_f = d / "vids_cookies.json"
    doc_f = d / "vids_doc.txt"
    marker = d / "_vids_dead"
    if not cookies_f.exists() or not doc_f.exists():
        return "skip(no artifacts)"
    host_port, user, pw = proxy_parts(nick)
    doc = doc_f.read_text().strip()
    rows = json.loads(cookies_f.read_text())
    prof = tempfile.mkdtemp(prefix=f"vk-{nick}-")
    proc = subprocess.Popen(
        ["chromium", "--headless=new", "--no-sandbox", "--disable-gpu",
         "--disable-dev-shm-usage", f"--remote-debugging-port={PORT}",
         "--remote-allow-origins=*",
         f"--user-data-dir={prof}",
         f"--proxy-server=http://{host_port}",
         "about:blank"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                tabs = requests.get(
                    f"http://127.0.0.1:{PORT}/json", timeout=2).json()
                page = next(t for t in tabs if t["type"] == "page")
                break
            except Exception:
                time.sleep(0.4)
        else:
            return "fail(chromium no ws)"
        cdp = CDP(page["webSocketDebuggerUrl"])
        auth = {"done": False}

        def on_auth(evt):
            if evt.get("method") == "Fetch.authRequired":
                cdp.ws.send(json.dumps({
                    "id": next(cdp._id), "method": "Fetch.continueWithAuth",
                    "params": {"requestId": evt["params"]["requestId"],
                               "authChallengeResponse": {
                                   "response": "ProvideCredentials",
                                   "username": user, "password": pw}}}))
            elif evt.get("method") == "Fetch.requestPaused":
                cdp.ws.send(json.dumps({
                    "id": next(cdp._id), "method": "Fetch.continueRequest",
                    "params": {"requestId": evt["params"]["requestId"]}}))

        cdp.handle_event = on_auth
        if user:
            cdp.cmd("Fetch.enable", {
                "handleAuthRequests": True,
                "patterns": [{"urlPattern": "*"}]})
        cdp.cmd("Network.enable")
        cs = []
        for c in rows:
            e = {"name": c["name"], "value": c["value"],
                 "domain": c.get("domain", ".google.com"),
                 "path": c.get("path", "/"),
                 "secure": bool(c.get("secure", True)),
                 "httpOnly": bool(c.get("httpOnly", False))}
            if c.get("expires") and isinstance(c["expires"], (int, float)):
                e["expires"] = c["expires"]
            cs.append(e)
        cdp.cmd("Network.setCookies", {"cookies": cs})
        cdp.cmd("Page.enable")
        cdp.cmd("Page.navigate",
                {"url": f"https://docs.google.com/videos/d/{doc}/edit"})
        deadline = time.time() + WAIT_S
        while time.time() < deadline:
            try:
                cdp.handle_event(json.loads(cdp.ws.recv()))
            except websocket.WebSocketTimeoutException:
                pass
        url = cdp.cmd("Runtime.evaluate",
                      {"expression": "location.href",
                       "returnByValue": True})["result"].get("value", "")
        html = cdp.cmd("Runtime.evaluate",
                       {"expression": "document.documentElement.innerHTML",
                        "returnByValue": True})["result"].get("value", "")
        dead = ("ServiceLogin" in url or "signin" in url
                or "CAMS" not in html)
        if dead:
            marker.touch()
            cdp.close()
            return "DEAD(no CAMS in page)"
        all_ck = cdp.cmd("Network.getAllCookies")["cookies"]
        keep = [c for c in all_ck
                if "google" in c.get("domain", "")
                or "docs.google" in c.get("domain", "")]
        # Same shape as cookie-sync output (subset of CDP cookie fields).
        out = [{"name": c["name"], "value": c["value"], "domain": c["domain"],
                "path": c.get("path", "/"), "secure": c.get("secure", False),
                "httpOnly": c.get("httpOnly", False),
                **({"expires": c["expires"]} if c.get("expires") else {})}
               for c in keep]
        if len(out) >= len(rows) * 0.7:
            cookies_f.write_text(json.dumps(out, indent=1))
        cdp.close()
        marker.unlink(missing_ok=True)
        return f"ok({len(out)} cookies)"
    except Exception as e:  # noqa: BLE001
        return f"fail({e!r})"
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
        subprocess.run(["rm", "-rf", prof], check=False)


def main() -> None:
    log("vids-keepalive start")
    while True:
        nicks = sorted(
            p.name for p in NICKS_DIR.iterdir()
            if p.is_dir() and not p.name.startswith("_")
            and (p / "vids_cookies.json").exists())
        for nick in nicks:
            log(f"{nick}: {keepalive_nick(nick)}")
            time.sleep(5)
        log(f"cycle done — sleep {CYCLE_GAP_S}s")
        time.sleep(CYCLE_GAP_S)


if __name__ == "__main__":
    sys.exit(main())
