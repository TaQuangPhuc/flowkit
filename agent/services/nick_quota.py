"""Per-nick Google Flow quota ledger via web session minted from the
nick's OAuthLogin refresh token.

Chain (verified live 2026-09-29):

    nick refresh_token (OAuthLogin RT, client 936475272427)
      -> POST oauth2.googleapis.com/token          -> ya29 scope=OAuthLogin
      -> GET  accounts.google.com/accounts/OAuthLogin?issueuberauth=1
      -> GET  accounts.google.com/MergeSession?uberauth=...
      -> full web cookie jar (SID/HSID/__Secure-3PSID...)
      -> GET  one.google.com/u/0/ai/activity?utm_source=flow&g1_landing_page=0
      -> POST DrWK4 batchexecute                  -> full credit ledger

Each nick gets its own session; all requests egress through the nick's
proxy so the IP association matches its gRPC traffic. The ledger is the
accepted-request ground truth Google itself uses for usage limiting.
"""

from __future__ import annotations

import http.cookiejar
import json
import logging
import re
import time
import urllib.parse
import urllib.request
from bisect import bisect_left
from collections import Counter
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

SIGNIN_CLIENT_ID = "936475272427.apps.googleusercontent.com"
OAUTHLOGIN_SCOPE = "https://www.google.com/accounts/OAuthLogin"
TOKEN_URL = "https://oauth2.googleapis.com/token"
UBERAUTH_URL = ("https://accounts.google.com/accounts/OAuthLogin"
                "?source=flowkit&issueuberauth=1")
MERGE_URL = "https://accounts.google.com/MergeSession"
ACTIVITY_URL = ("https://one.google.com/u/0/ai/activity"
                "?utm_source=flow&g1_landing_page=0")
DRWK4_URL = ("https://one.google.com/u/0/_/SubscriptionsManagementUi"
             "/data/batchexecute")
WEB_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

# non-generation operations that appear in the ledger
UPSCALE_MODEL = "veo_3_1_upsampler_1080p"


class QuotaError(Exception):
    pass


class _Session:
    """Minted web cookie jar for one nick; refreshed on demand."""

    def __init__(self, nick):
        self.nick = nick
        self.jar: http.cookiejar.CookieJar | None = None
        self.expires = 0.0

    # -- minting ------------------------------------------------------

    def _oauthlogin_token(self) -> str:
        r = requests.post(
            TOKEN_URL,
            data={"client_id": SIGNIN_CLIENT_ID,
                  "refresh_token": self.nick.refresh_token,
                  "grant_type": "refresh_token"},
            proxies=self.nick.proxies, timeout=30)
        r.raise_for_status()
        return r.json()["access_token"]

    def mint(self) -> http.cookiejar.CookieJar:
        ya = self._oauthlogin_token()
        jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(jar),
            self._proxy_handler())

        # uberauth
        r = opener.open(urllib.request.Request(
            UBERAUTH_URL,
            headers={"Authorization": "Bearer " + ya,
                     "User-Agent": WEB_UA}), timeout=30)
        uberauth = r.read().decode().strip()

        # mergesession -> SID cookies land in jar
        class _NoRedir(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None
        op2 = urllib.request.build_opener(
            _NoRedir, urllib.request.HTTPCookieProcessor(jar),
            self._proxy_handler())
        try:
            op2.open(urllib.request.Request(
                MERGE_URL + "?" + urllib.parse.urlencode(
                    {"uberauth": uberauth,
                     "continue": "https://one.google.com/"}),
                headers={"User-Agent": WEB_UA}), timeout=30)
        except urllib.error.HTTPError as e:
            if e.code not in (301, 302, 303, 307, 308):
                raise
        if not any(c.name == "SID" for c in jar):
            raise QuotaError(f"{self.nick.name}: MergeSession gave no SID")
        self.jar = jar
        self.expires = time.time() + 1500  # re-mint every ~25min max
        return jar

    def _proxy_handler(self):
        if not self.nick.proxy:
            return urllib.request.ProxyHandler({})
        return urllib.request.ProxyHandler(
            {"http": self.nick.proxy, "https": self.nick.proxy})

    def cookie_header(self) -> str:
        if not self.jar or time.time() > self.expires:
            self.mint()
        return "; ".join(f"{c.name}={c.value}" for c in self.jar)


_sessions: dict[str, _Session] = {}


def _session_for(nick) -> _Session:
    s = _sessions.get(nick.name)
    if not s:
        s = _Session(nick)
        _sessions[nick.name] = s
    return s


def _fetch_ledger(nick, cookie: str, depth: int = 0) -> dict:
    """Activity page -> DrWK4 ledger. Retries once on session expiry."""
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler(
            {"https": nick.proxy, "http": nick.proxy} if nick.proxy else {}))
    html = opener.open(urllib.request.Request(
        ACTIVITY_URL,
        headers={"Cookie": cookie, "User-Agent": WEB_UA}),
        timeout=30).read().decode("utf-8", "replace")

    if "ServiceLogin" in html[:2000] or "signin" in (html[:500].lower()):
        if depth == 0:
            _session_for(nick).expires = 0
            return _fetch_ledger(nick, _session_for(nick).cookie_header(), 1)
        raise QuotaError(f"{nick.name}: session rejected (login page)")

    email = next(iter(re.findall(r"[\w.+-]+@[\w-]+\.[\w.]+", html)), "")

    at = re.search(r'"SNlM0e":"([^"]+)"', html)
    if not at:
        return {"nick": nick.name, "email": email, "credits": None,
                "ledger_rows": 0, "entries": [], "reset_ts": None}
    fsid = re.search(r"f\.sid=(-?\d+)", html)

    be = (DRWK4_URL + "?rpcids=DrWK4"
          "&source-path=%2Fu%2F0%2Fai%2Factivity"
          f"&f.sid={fsid.group(1) if fsid else 0}"
          "&bl=boq_subscriptionsuiserver_20260921.03_p2"
          "&hl=en&soc-app=727&soc-platform=1&soc-device=1"
          "&_reqid=300001&rt=c")
    body = ("f.req=" + urllib.parse.quote(
        json.dumps([[["DrWK4", '[null,["flow"]]', None, "generic"]]]))
        + "&at=" + urllib.parse.quote(at.group(1)) + "&")
    txt = opener.open(urllib.request.Request(
        be, data=body.encode(), method="POST",
        headers={"Cookie": cookie, "User-Agent": WEB_UA,
                 "Content-Type": "application/x-www-form-urlencoded"
                                 ";charset=UTF-8",
                 "Referer": "https://one.google.com/u/0/ai/activity",
                 "Origin": "https://one.google.com"}),
        timeout=60).read().decode("utf-8", "replace")

    inner = json.loads(json.loads(max(txt.splitlines(), key=len))[0][2])
    hdr, entries = inner[0], inner[1]

    credits = reset_ts = None
    total_credits = 0
    best_block_credits = -1
    for block in (hdr or []):
        if (isinstance(block, list) and len(block) >= 3
                and isinstance(block[0], int) and block[0] < 10_000_000
                and isinstance(block[2], list) and block[2]
                and isinstance(block[2][0], (int, float))
                and 1_700_000_000 < block[2][0] < 2_000_000_000):
            total_credits += block[0]
            if block[0] > best_block_credits:
                best_block_credits = block[0]
                reset_ts = block[2][0]
            credits = total_credits

    ts = sorted(e[1][0] for e in entries if e[1])
    gen_ts = sorted(e[1][0] for e in entries
                    if e[1] and (len(e) < 7 or e[6] != UPSCALE_MODEL))
    models = Counter(e[6] for e in entries if len(e) > 6 and e[6])
    now = time.time()
    today = datetime.now().strftime("%Y-%m-%d")
    return {
        "nick": nick.name,
        "email": email,
        "credits": credits,
        "reset_ts": reset_ts,
        "reset_at": (datetime.fromtimestamp(reset_ts)
                     .strftime("%m-%d %H:%M") if reset_ts else None),
        "ledger_rows": len(entries),
        "accepts_1h": len(ts) - bisect_left(ts, now - 3600),
        "accepts_4h": len(ts) - bisect_left(ts, now - 14400),
        "accepts_24h": len(ts) - bisect_left(ts, now - 86400),
        "accepts_today": sum(
            1 for s in ts if datetime.fromtimestamp(s).strftime("%Y-%m-%d")
            == today),
        "gen_1h": len(gen_ts) - bisect_left(gen_ts, now - 3600),
        "gen_24h": len(gen_ts) - bisect_left(gen_ts, now - 86400),
        "newest": (datetime.fromtimestamp(ts[-1]).strftime("%m-%d %H:%M:%S")
                   if ts else None),
        "by_model": dict(models.most_common(8)),
    }


def get_nick_quota(name: str) -> dict:
    import types
    from agent.services.flow_grpc import get_flow_grpc, NICKS_DIR
    from agent.services import vids_client
    t = get_flow_grpc()
    nick = t._nicks.get(name)
    d = NICKS_DIR / name
    if not nick and d.is_dir():
        px = (d / 'proxy.txt').read_text().strip() if (d / 'proxy.txt').exists() else ''
        nick = types.SimpleNamespace(name=name, proxy=px, proxies={'http': px, 'https': px} if px else None, refresh_token='')
    if not nick:
        raise QuotaError(f"unknown nick {name}")
    vq = vids_client.load_vids_quota(name)
    vc = d / "vids_cookies.json"
    if vc.exists():
        try:
            ck = json.loads(vc.read_text(encoding="utf-8"))
            seen_names: dict[str, str] = {}
            for c in ck:
                if (isinstance(c, dict)
                        and c.get("domain") in (".google.com", "one.google.com")
                        and c.get("name") and c.get("value")):
                    seen_names[c["name"]] = c["value"]
            chdr = "; ".join(f"{k}={v}" for k, v in seen_names.items())
            if chdr:
                res = _fetch_ledger(nick, chdr, depth=1)
                if vq:
                    res["vids_credits_left"] = vq.get("credits_left")
                    res["vids_credits_total"] = vq.get("credits_total")
                    if res.get("credits") is None:
                        res["credits"] = vq.get("credits_left")
                return res
        except Exception as e:
            if vq:
                return {
                    "nick": name,
                    "credits": vq.get("credits_left"),
                    "credits_total": vq.get("credits_total"),
                    "credits_used": vq.get("credits_used"),
                    "updated_at": vq.get("updated_at"),
                    "source": "vids_genai",
                }
            raise
    return _fetch_ledger(nick, _session_for(nick).cookie_header())


def get_all_quota() -> dict:
    from agent.services.flow_grpc import get_flow_grpc, NICKS_DIR
    t = get_flow_grpc()
    names = set(t.nicks())
    if NICKS_DIR.exists():
        for d in NICKS_DIR.iterdir():
            if d.is_dir() and not d.name.startswith("_") and (d / "vids_cookies.json").exists():
                names.add(d.name)
    out = {}
    for name in sorted(names):
        try:
            out[name] = get_nick_quota(name)
        except Exception as e:
            out[name] = {"nick": name, "error": str(e)[:200]}
    return out
