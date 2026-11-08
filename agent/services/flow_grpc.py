"""Direct PC-side gRPC transport for Google Flow (iOS app protocol).

Replaces the Chrome-extension batchexecute path with plain gRPC to
``aisandbox-pa.googleapis.com`` using per-account credential bundles under
``nicks/<name>/`` (see .omc/research/FLOW_IOS_API.md for the wire research):

    refresh_token.txt   1//... -> ya29 via oauthaccountmanager issuetoken
    aas_et.txt          aas_et/... Android master token -> ya29 via
                        android.googleapis.com/auth (alternative credential;
                        takes precedence over refresh_token when present)
    mrr_req.bin         recaptcha/api3/mrr template -> 0cAFcWe… token
    gen_img_req.bin     captured BatchGenerateImages payload (prompt slots)
    meta.json           device_id / client_id / project_uuid / user_agent
                        / platform ("iphone"|"android") / video_tier
    proxy.txt           http://user:pass@host:port (optional egress pin)

Shared call templates captured once live in ``nicks/_tpl/`` (t2v/i2v/r2v).
Their top-level ``f2`` metadata block is swapped per call for the calling
nick's own block (extracted from ``gen_img_req.bin``), so client-id /
project-uuid / captcha always match the account that owns the refresh token.

Per-nick egress IP: gRPC only honours the process-wide ``https_proxy`` env,
so each proxied nick gets a ``LocalProxyBridge`` on a daemon asyncio thread
and the env is pointed at the bridge while its channel is created
(serialised by a lock). requests-based mints use ``proxies=`` directly.
"""
from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
import logging
import os
import random
import re
import threading
import time
import uuid as _uuid_mod
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from agent.config import (VIDEO_STUCK_TIMEOUT, VIDEO_RESUBMIT_S,
                          FLOW_SUBMIT_RETRY_S, UPSCALE_MODELS)

import grpc
import requests

from agent.services.flow_proto import (
    _looks_msg, decode, encode, fld_msg, fld_str, fld_varint,
    replace_fields, set_leaf_bytes, set_varint, walk_leaves,
)
from agent.services import flow_stats
from agent.services.flow_trace import (emit as _trace_emit,
                                       job_id as _trace_job,
                                       request_gone as _request_gone)

logger = logging.getLogger(__name__)

NICKS_DIR = Path(__file__).resolve().parents[2] / "nicks"
TPL_DIR = NICKS_DIR / "_tpl"
ROUTES_FILE = NICKS_DIR / "_routes.json"
# Manual per-nick submit park, persisted so backend flips/restarts keep
# it. {nick: epoch_until} — merged into _throttle_until at load.
PARK_FILE = NICKS_DIR / "_parked.json"
QUOTA_FILE = NICKS_DIR / "_quota_dead.json"
UPLOAD_MAP_FILE = NICKS_DIR / "_uploaded.json"
UPLOAD_BYTES_DIR = NICKS_DIR / "_upload_cache"
# Nicks whose THROTTLED cooldown is disabled — they stay schedulable
# even when upstream flags the egress IP. Persisted [nick, ...].
NOCOOLDOWN_FILE = NICKS_DIR / "_no_cooldown.json"
# Both agent processes share _routes.json; merge+write must be atomic
# across processes or the last writer drops entries the sibling added
# between its own merge and rename.
ROUTES_LOCK_FILE = NICKS_DIR / "_routes.lock"
# ya29 tokens minted on-device and pushed by the phone — the phone owns the
# aas_et, so minting off-device gets BadAuthentication'd within a day.
# {nick: {"token": ..., "exp": epoch}} — sibling agents adopt via
# _refresh_flags, and the file survives restarts.
YA29_FILE = NICKS_DIR / "_ya29.json"

if os.name == "nt":
    import msvcrt

    def _flock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_LOCK, 1)

    def _funlock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _flock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX)

    def _funlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)

GRPC_HOST = "aisandbox-pa.googleapis.com:443"
ISSUETOKEN_URL = "https://oauthaccountmanager.googleapis.com/v1/issuetoken"
ANDROID_AUTH_URL = "https://android.googleapis.com/auth"
MRR_URL = "https://www.recaptcha.net/recaptcha/api3/mrr"

SVC = "google.internal.labs.aisandbox.proto"
M_IMAGE = f"/{SVC}.flow.v1.FlowService/BatchGenerateImages"
M_T2V = f"/{SVC}.videofx.v1.VideoFxService/BatchAsyncGenerateVideoText"
M_I2V = f"/{SVC}.videofx.v1.VideoFxService/BatchAsyncGenerateVideoStartImage"
M_R2V = f"/{SVC}.videofx.v1.VideoFxService/BatchAsyncGenerateVideoReferenceImages"
M_CHECK = f"/{SVC}.videofx.v1.VideoFxService/BatchCheckAsyncVideoGenerationStatus"
M_UPSAMPLE = f"/{SVC}.videofx.v1.VideoFxService/BatchAsyncGenerateVideoUpsampleVideo"
M_GET_MEDIA = f"/{SVC}.flow.v1.FlowService/BatchGetMedia"
M_PROJECT = f"/{SVC}.flow.v1.FlowService/GetProjectContents"
M_UPLOAD = f"/{SVC}.flow.v1.FlowService/UploadImage"

UA_GRPC = ("Flow App/1.566.3 (Build 1.566.3; ios {ios}; {hw}) "
           "Dart/RPCClient")
UA_GRPC_OMNI = ("Flow App/2.0.0 (Build 2.0.0; ios {ios}; {hw}) "
                "Dart/RPCClient")
UA_MINT = "com.google.whisk/1.566.3 iSL/3.4 iPhone/{ios} hw/{hw} (gzip)"
UA_CFN = ("Flow/1.566.3 CFNetwork/3826.400.120 Darwin/24.3.0")

UUID_RE = re.compile(rb"^[0-9a-f]{8}-[0-9a-f-]{27}$")
# Upscaled clips are media whose id is the source uuid + "_upsampled" or "_4k_upsampled".
MEDIA_ID_RE = re.compile(rb"^[0-9a-f]{8}-[0-9a-f-]{27}(?:_(?:4k_)?upsampled)?$")
CAPTCHA_RE = re.compile(rb"[03][0-9a-zA-Z]AFcWe[0-9A-Za-z_.\-]{100,}")
URL_RE = re.compile(rb"https://[^\s\"'\x00-\x20]+")
# Leaf strings that are never the prompt when auto-discovering prompt slots.
NON_PROMPT_RE = re.compile(
    rb"^([0-9a-f]{8}-|veo_|abra_|macaron_cookie_|omni_|mobile;|projects/|NARWHAL|[03][0-9a-zA-Z]AFcWe|"
    rb"[A-Z]{2,4}$|http)")

_channel_lock = threading.Lock()


class FlowGrpcError(RuntimeError):
    def __init__(self, message: str, code=None):
        super().__init__(message)
        self.code = code


# Reads. A dropped proxy can be retried: the server did not start a render.
_READ_RPCS = {M_CHECK, M_GET_MEDIA, M_PROJECT}
# Connection never left the machine. Safe to resend a generate too.
_PRECONNECT_MARKERS = (
    "failed to connect",
    "connection refused",
    "name resolution",
    "dns resolution",
    "failed to create subchannel",
    "connectex",
    "http2 header with status: 502",
    "http2 header with status: 503",
    "http2 header with status: 504",
)


def _transport_detail(detail: str) -> bool:
    text = (detail or "").lower()
    return any(s in text for s in (
        "no status received", "stream removed", "connection reset",
        "failed to connect", "connection refused", "goaway",
        "broken pipe", "socket closed", "channel closed",
    ))


def _rpc_retryable(method: str, code, detail: str) -> bool:
    """True when repeating this RPC cannot start a second render.

    'No status received' / 'Stream removed' means the proxy died after the
    bytes may already have reached Google. Polls and media lookups are safe
    to repeat. A generate is only repeated when the TCP connect itself failed.
    """
    text = (detail or "").lower()
    transport = code in (
        getattr(grpc.StatusCode, "UNAVAILABLE", None),
        getattr(grpc.StatusCode, "INTERNAL", None),
        getattr(grpc.StatusCode, "CANCELLED", None),
    ) or any(s in text for s in (
        "no status received", "stream removed", "connection reset",
        "goaway", "broken pipe", "socket closed", "transport",
        "channel closed",
    ))
    if not transport:
        return False
    if method in _READ_RPCS or method == M_UPLOAD:
        return True
    return any(s in text for s in _PRECONNECT_MARKERS) or "channel closed" in text


# --------------------------------------------------------------------------
# per-nick proxy bridges on a daemon asyncio loop
# --------------------------------------------------------------------------

class _BridgePool:
    """Hosts LocalProxyBridge instances on a private loop thread so proxied
    nicks work from both sync scripts and the agent's event loop."""

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True,
                         name="flow-grpc-bridges").start()
        self._ports: dict[str, int] = {}
        self._lock = threading.Lock()

    def port_for(self, nick: str, upstream_url: str) -> int:
        with self._lock:
            if nick in self._ports:
                return self._ports[nick]
            from agent.services.proxy_forward import LocalProxyBridge
            from agent.services.proxy_url import parse_proxy_url
            upstream = parse_proxy_url(upstream_url)

            async def _start():
                bridge = LocalProxyBridge(upstream)
                return await bridge.start()

            port = asyncio.run_coroutine_threadsafe(_start(), self._loop).result()
            self._ports[nick] = port
            return port


_bridges: Optional[_BridgePool] = None


def _bridge_pool() -> _BridgePool:
    global _bridges
    if _bridges is None:
        _bridges = _BridgePool()
    return _bridges


# --------------------------------------------------------------------------
# nick bundle
# --------------------------------------------------------------------------

class GrpcNick:
    """One account bundle under nicks/<name>/."""

    def __init__(self, dirpath: Path):
        self.dir = dirpath
        self.name = dirpath.name
        rt = dirpath / "refresh_token.txt"
        self.refresh_token = rt.read_text().strip() if rt.exists() else ""
        aas = dirpath / "aas_et.txt"
        self.aas_et = aas.read_text().strip() if aas.exists() else ""
        lc = dirpath / "labs_cookie.txt"
        self.labs_cookie = lc.read_text().strip() if lc.exists() else ""
        self.meta = json.loads((dirpath / "meta.json").read_text())
        proxy_file = dirpath / "proxy.txt"
        self.proxy = (proxy_file.read_text().strip() or None) \
            if proxy_file.exists() else None
        self.proxies = ({"http": self.proxy, "https": self.proxy}
                        if self.proxy else None)
        self.gen_template = (dirpath / "gen_img_req.bin").read_bytes()
        self._mrr_req = (dirpath / "mrr_req.bin").read_bytes() \
            if (dirpath / "mrr_req.bin").exists() else None
        # Action-specific mrr templates captured from the phone, e.g.
        # mrr_image_upload.bin — preferred over patching the gen template.
        self._mrr_by_action: dict[str, bytes] = {}
        for p in dirpath.glob("mrr_*.bin"):
            if p.name == "mrr_req.bin":
                continue
            self._mrr_by_action[p.stem[4:].upper()] = p.read_bytes()
        self._ya29: Optional[str] = None
        self._ya29_exp = 0.0
        self._captchas: dict[str, tuple[bytes, float]] = {}
        self._grpc_proxy_url: Optional[str] = None
        self._channel: Optional[grpc.Channel] = None

    def _reload_credentials(self) -> None:
        """Cookie-sync pushes land on disk while the agent is running —
        re-read rotated credentials so mint never keeps using a dead JWE
        for the whole ya29 cache window. Partial scp writes are ignored:
        a labs JWE always starts with 'eyJ' and carries two dots."""
        changed = False
        for fname, attr in (("labs_cookie.txt", "labs_cookie"),
                            ("refresh_token.txt", "refresh_token"),
                            ("aas_et.txt", "aas_et")):
            p = self.dir / fname
            try:
                val = p.read_text().strip() if p.exists() else ""
            except OSError:
                continue
            if fname == "labs_cookie.txt" and val:
                # Adopt only a structurally complete JWE (5 base64url
                # segments, empty 2nd for dir enc) — a truncated scp
                # write would mint 401s and park the nick for nothing.
                segs = val.split("session-token=", 1)[-1].split(".")
                if len(segs) != 5 or any(
                        not s or not re.fullmatch(r"[A-Za-z0-9_-]+", s)
                        for i, s in enumerate(segs) if i != 1):
                    continue
            if val and val != getattr(self, attr):
                setattr(self, attr, val)
                changed = True
        if changed:
            self._ya29 = None
            self._ya29_exp = 0.0
            _trace_emit("grpc.nick.creds_reload", nick=self.name)

    # -- identity -----------------------------------------------------------

    @property
    def project_uuid(self) -> str:
        return self.meta.get("project_uuid", "")

    def _ua_parts(self) -> dict:
        m = re.search(r"iPhone/(\S+) hw/(\S+)", self.meta.get("user_agent", ""))
        ios, hw = (m.group(1), m.group(2).replace("_", ",")) if m \
            else ("18.3.2", "iPhone11,8")
        return {"ios": ios, "hw": hw}

    def meta_block(self, project_id: str | None = None,
                   fresh_captcha: bool = False,
                   captcha_force: bool = False,
                   captcha_action: str = "") -> bytes:
        """f4 metadata msg of the captured gen payload; project uuid patched
        when the caller's project differs from the captured one."""
        fields = decode(self.gen_template)
        meta = next(v for f, w, v in fields if f == 4 and w == 2)
        target = (project_id or self.project_uuid).encode()
        if self.project_uuid and target != self.project_uuid.encode():
            meta = set_leaf_bytes(meta, [6], target)
        if fresh_captcha:
            cap = self.mint_captcha(force=bool(captcha_force),
                                    action=captcha_action)
            inner = encode([(1, 2, cap), (2, 0, 3)])
            meta = replace_fields(meta, [], 11, [inner])
        return meta

    def video_model_for(self, tpl: str, key: str,
                        aspect_ratio: str = "") -> str | None:
        """Resolve a friendly video-model key ('fast', 'omni_8s'…) to this
        account's captured model string; None if the nick can't serve it.
        Value may be a plain t2v string (i2v/r2v derive by swapping the
        _t2v_ tag — right for abra_*, WRONG for veo fast/quality whose i2v
        names differ) or a dict keyed by 't2v'/'i2v'/'r2v' with optional
        '_landscape' variants (landscape drops the _portrait marker)."""
        model = (self.meta.get("video_models") or {}).get(key)
        if isinstance(model, dict):
            # Explicit per-template map — a missing tpl means the model
            # doesn't exist for it (e.g. no r2v quality), never send the
            # t2v string into an i2v/r2v call.
            if "LANDSCAPE" in aspect_ratio:
                return model.get(f"{tpl}_landscape") or model.get(tpl)
            return model.get(tpl)
        if model and tpl != "t2v" and "_t2v_" in model:
            model = model.replace("_t2v_", f"_{tpl}_")
        return model

    # -- tokens -------------------------------------------------------------

    def mint_access_token(self, force: bool = False) -> str:
        if not force and self._ya29 and time.time() < self._ya29_exp:
            return self._ya29
        self._reload_credentials()
        if self.labs_cookie or (self.dir / "vids_cookies.json").exists():
            return self._mint_labs()
        if self.aas_et:
            return self._mint_android()
        ua = self._ua_parts()
        r = requests.post(
            self._relay_url(ISSUETOKEN_URL),
            headers={
                "authorization": f"Bearer {self.refresh_token}",
                "x-oauth-client-id": self.meta["client_id"],
                "user-agent": UA_MINT.format(**ua),
            },
            data={
                "app_id": "com.google.whisk",
                "client_id": self.meta["client_id"],
                "device_id": self.meta["device_id"],
                "hl": self.meta.get("hl") or "vi-VN",
                "lib_ver": "3.4",
                "response_type": "token",
                "scope": self.meta["scope"],
            },
            proxies=None if self._relay_port() else self.proxies,
            timeout=30,
        )
        if r.status_code >= 400:
            # requests.HTTPError is not FlowGrpcError — it used to escape the
            # facade handlers into http.exception as a bare 500.
            raise FlowGrpcError(
                f"{self.name}: mint token HTTP {r.status_code}")
        body = r.json()
        self._ya29 = body["token"]
        self._ya29_exp = time.time() + int(body.get("expiresIn", 3599)) - 120
        return self._ya29

    def _mint_android(self) -> str:
        """Android master token (aas_et/…) -> ya29 via the on-device auth
        endpoint. Extracted from the phone's GoogleAccountDataStore.pb; the
        response is urlencoded key=value lines, ``Auth`` is the token."""
        r = requests.post(
            self._relay_url(ANDROID_AUTH_URL),
            data={
                "app": self.meta.get("android_app",
                                     "com.google.android.apps.labs.whisk"),
                "client_sig": self.meta.get(
                    "client_sig",
                    "d27c106448af3ab8b6c5497271356f14024718aa"),
                "service": "oauth2:" + (self.meta.get("scope") or
                                        "openid https://www.googleapis.com/"
                                        "auth/aisandbox"),
                "Token": self.aas_et,
                "ACCESS_TOKEN": "1",
            },
            proxies=None if self._relay_port() else self.proxies,
            timeout=30,
        )
        if r.status_code >= 400:
            raise FlowGrpcError(
                f"{self.name}: mint token HTTP {r.status_code}")
        body = dict(l.split("=", 1) for l in r.text.splitlines() if "=" in l)
        if "Auth" not in body:
            raise FlowGrpcError(
                f"{self.name}: android mint failed: {r.text[:120]}")
        self._ya29 = body["Auth"]
        self._ya29_exp = time.time() + int(
            body.get("ExpiresInDurationSec", 3599)) - 120
        return self._ya29

    LABS_SESSION_URL = "https://labs.google/fx/"
    IFRAMERPC_URL = "https://accounts.google.com/o/oauth2/iframerpc"
    LABS_OAUTH_CLIENT_ID = (
        "365941595420-v51i7q8ik0crmaoltkhuo5ogb1kekl6f.apps.googleusercontent.com"
    )

    @staticmethod
    def _parse_labs_session(r) -> dict:
        ctype = (r.headers.get("content-type") or "").lower()
        if "application/json" in ctype:
            try:
                return r.json() or {}
            except Exception:
                return {}
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            r.text or "",
        )
        if m:
            try:
                nd = json.loads(m.group(1))
                return ((nd.get("props") or {}).get("pageProps") or {}).get("session") or {}
            except Exception:
                return {}
        return {}

    def _google_cookie_header(self) -> str:
        vc = self.dir / "vids_cookies.json"
        if vc.exists():
            try:
                cookies = json.loads(vc.read_text(encoding="utf-8"))
                parts = [
                    f"{c['name']}={c['value']}"
                    for c in cookies
                    if isinstance(c, dict)
                    and c.get("domain") in (".google.com", "accounts.google.com", "google.com")
                    and c.get("name") and c.get("value")
                ]
                if parts:
                    return "; ".join(parts)
            except Exception:
                pass
        if self.labs_cookie and any(
            k in self.labs_cookie for k in ("SID=", "__Secure-1PSID=", "__Secure-3PSID=")
        ):
            return self.labs_cookie
        return ""

    def _persist_rotated_cookies(self, jar) -> str:
        if not jar:
            return ""
        updates = {c.name: c.value for c in jar if c.value}
        if not updates:
            return ""
        vc = self.dir / "vids_cookies.json"
        if vc.exists():
            try:
                cookies = json.loads(vc.read_text(encoding="utf-8"))
                seen = set()
                for c in cookies:
                    if isinstance(c, dict) and c.get("name") in updates and c.get("domain") in (".google.com", "accounts.google.com", "google.com"):
                        c["value"] = updates[c["name"]]
                        seen.add(c["name"])
                for k, v in updates.items():
                    if k not in seen:
                        cookies.append({"name": k, "value": v, "domain": ".google.com", "path": "/"})
                tmp = self.dir / "vids_cookies.json.tmp"
                tmp.write_text(json.dumps(cookies), encoding="utf-8")
                os.replace(tmp, vc)
            except Exception:
                pass
        return self._google_cookie_header()

    def _rotate_google_cookies(self, cookie_hdr: str) -> str:
        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        )
        page_url = "https://accounts.google.com/RotateCookiesPage?og_pid=1&rot=3&origin=https%3A%2F%2Fwww.google.com&exp_id=0"
        rot_url = "https://accounts.google.com/RotateCookies"
        try:
            r_page = requests.get(
                page_url,
                headers={"cookie": cookie_hdr, "user-agent": ua},
                proxies=None,
                timeout=15,
            )
            m = re.search(r"init\('([^']+)'", r_page.text or "")
            if not m:
                return cookie_hdr
            r_rot = requests.post(
                rot_url,
                headers={
                    "cookie": cookie_hdr,
                    "content-type": "application/json",
                    "user-agent": ua,
                    "origin": "https://accounts.google.com",
                    "referer": page_url,
                },
                data=json.dumps([1, m.group(1)], separators=(",", ":")),
                proxies=None,
                timeout=15,
            )
            updated = self._persist_rotated_cookies(r_rot.cookies)
            return updated or cookie_hdr
        except Exception:
            return cookie_hdr

    def _mint_iframerpc(self, cookie_hdr: str) -> str:
        cookie_hdr = self._rotate_google_cookies(cookie_hdr)
        headers = {
            "cookie": cookie_hdr,
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            "referer": "https://accounts.google.com/o/oauth2/iframe",
            "x-requested-with": "XmlHttpRequest",
        }
        params_base = {
            "client_id": self.LABS_OAUTH_CLIENT_ID,
            "origin": "https://labs.google",
            "scope": "openid email profile https://www.googleapis.com/auth/aisandbox",
            "ss_domain": "https://labs.google",
        }
        def _get_iframerpc(params: dict):
            if self.proxies:
                try:
                    r = requests.get(
                        self.IFRAMERPC_URL,
                        params=params,
                        headers=headers,
                        proxies=self.proxies,
                        timeout=5,
                    )
                    if r.status_code < 500:
                        return r
                except requests.RequestException:
                    pass
            return requests.get(
                self.IFRAMERPC_URL,
                params=params,
                headers=headers,
                proxies=None,
                timeout=20,
            )

        r1 = _get_iframerpc({"action": "listSessions", **params_base})
        if r1.status_code >= 400:
            raise FlowGrpcError(f"{self.name}: iframerpc listSessions HTTP {r1.status_code}")
        sessions = (r1.json() or {}).get("sessions") or []
        if not sessions or not sessions[0].get("login_hint"):
            raise FlowGrpcError(f"{self.name}: google session expired (no active session in cookies)")
        r2 = _get_iframerpc({
            "action": "issueToken",
            "response_type": "token id_token",
            "login_hint": sessions[0]["login_hint"],
            **params_base,
        })
        if r2.status_code >= 400:
            raise FlowGrpcError(f"{self.name}: iframerpc issueToken HTTP {r2.status_code}")
        self._persist_rotated_cookies(r2.cookies)
        body = r2.json() or {}
        token = body.get("access_token") or ""
        if not token.startswith("ya29."):
            raise FlowGrpcError(
                f"{self.name}: iframerpc token failed ({body.get('error') or 'no ya29'})"
            )
        self._ya29 = token
        self._ya29_exp = time.time() + min(int(body.get("expires_in", 3599)) - 120, 3300)
        return self._ya29

    def _mint_labs(self) -> str:
        """Mint ya29 (scope aisandbox) via Google OAuth iframerpc (vids_cookies.json
        or Google session cookies), falling back to labs.google/fx/ SSR JWE."""
        cookie_hdr = self._google_cookie_header()
        if cookie_hdr:
            try:
                return self._mint_iframerpc(cookie_hdr)
            except Exception as e:
                if not self.labs_cookie or "session-token" not in self.labs_cookie:
                    raise
                logger.warning("%s: iframerpc mint failed (%s), falling back to labs SSR", self.name, e)
        r = requests.get(
            self.LABS_SESSION_URL,
            headers={
                "cookie": self.labs_cookie,
                "referer": "https://labs.google/fx/",
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/136.0.0.0 Safari/537.36"
                ),
            },
            proxies=self.proxies,
            timeout=30,
        )
        if r.status_code >= 400:
            raise FlowGrpcError(
                f"{self.name}: labs mint HTTP {r.status_code}")
        body = self._parse_labs_session(r)
        if body.get("error"):
            raise FlowGrpcError(
                f"{self.name}: labs session dead ({body.get('error')}) ? re-login + refresh cookie")
        token = body.get("access_token") or ""
        if not token.startswith("ya29."):
            raise FlowGrpcError(
                f"{self.name}: labs session dead ? re-login + refresh cookie")
        new_jwe = r.cookies.get(JWE_COOKIE_NAME)
        if new_jwe and len(new_jwe.split(".")) == 5:
            cookie_str = f"{JWE_COOKIE_NAME}={new_jwe}"
            self.labs_cookie = cookie_str
            try:
                (NICKS_DIR / self.name / "labs_cookie.txt").write_text(cookie_str)
            except Exception:
                pass
        self._ya29 = token
        self._ya29_exp = time.time() + 3300  # conservative 55min cache
        return self._ya29

    def mint_captcha(self, force: bool = False, max_age_s: int = 7200,
                     action: str = "") -> bytes:
        """Replay the mrr template -> fresh 0cAFcWe… token. reCAPTCHA
        tokens are action-scoped: the phone mints IMAGE_GENERATION for
        gen calls and IMAGE_UPLOAD for UploadImage — f4 of the mrr body
        carries the action string."""
        if not force:
            hit = self._captchas.get(action)
            if hit and time.time() - hit[1] < max_age_s:
                return hit[0]
        if not self._mrr_req:
            raise FlowGrpcError(f"{self.name}: no mrr_req.bin captured")
        tpl = self._mrr_by_action.get(action.upper()) if action else None
        body = tpl or self._mrr_req
        if action and not tpl:
            body = encode([
                (f, w, action.encode() if f == 4 else v)
                for f, w, v in decode(self._mrr_req)])
        r = requests.post(
            self._relay_url(MRR_URL),
            headers={"content-type": "application/x-protobuffer",
                     "user-agent": UA_CFN.format(**self._ua_parts())},
            data=body,
            proxies=None if self._relay_port() else self.proxies,
            timeout=30,
        )
        if r.status_code >= 400:
            raise FlowGrpcError(
                f"{self.name}: captcha mint HTTP {r.status_code}")
        m = CAPTCHA_RE.search(r.content)
        if not m:
            raise FlowGrpcError(
                f"{self.name}: no captcha token in mrr response")
        self._captchas[action] = (m.group(0), time.time())
        return m.group(0)

    # -- grpc ---------------------------------------------------------------

    def _relay_port(self) -> int:
        """tlsrelay sidecar h2c port for this nick — when the Go relay is
        running it owns the upstream proxy + iOS-fingerprinted TLS, so the
        Python channel just speaks cleartext h2c to loopback."""
        try:
            ports = json.loads((NICKS_DIR / "_relay.json").read_text())
            return int(ports.get(self.name) or 0)
        except Exception:
            return 0

    def _relay_url(self, url: str) -> str:
        """Route a plain HTTPS call through the relay's /to/<host>/ path so
        token minting / captcha / media downloads carry the same iOS TLS
        fingerprint + nick proxy as gRPC — otherwise Python's OpenSSL
        handshake betrays the client on every non-gRPC call."""
        port = self._relay_port()
        m = re.match(r"https://([^/]+)(/.*)$", url)
        if not port or not m:
            return url
        return f"http://127.0.0.1:{port}/to/{m.group(1)}{m.group(2)}"

    def _grpc_ua(self) -> str:
        ua = self._ua_parts()
        tpl = UA_GRPC_OMNI if self.name in OMNI_ALLOWED_NICKS else UA_GRPC
        return tpl.format(**ua)

    def channel(self) -> grpc.Channel:
        if self._channel is not None:
            return self._channel
        relay_port = self._relay_port()
        if relay_port:
            self._channel = grpc.insecure_channel(
                f"127.0.0.1:{relay_port}",
                options=[("grpc.default_authority", GRPC_HOST),
                         ("grpc.primary_user_agent",
                          self._grpc_ua() + " dart-grpc/2.0.0")],
            )
            return self._channel
        if self.proxy and self._grpc_proxy_url is None:
            port = _bridge_pool().port_for(self.name, self.proxy)
            self._grpc_proxy_url = f"http://127.0.0.1:{port}"
        prev = os.environ.get("https_proxy")
        # grpc reads https_proxy at channel creation; serialise so a
        # concurrent channel for another nick cannot pick up this proxy.
        with _channel_lock:
            try:
                if self._grpc_proxy_url:
                    os.environ["https_proxy"] = self._grpc_proxy_url
                elif "https_proxy" in os.environ:
                    del os.environ["https_proxy"]
                self._channel = grpc.secure_channel(
                    GRPC_HOST,
                    grpc.ssl_channel_credentials(),
                    options=[("grpc.primary_user_agent",
                              self._grpc_ua() + " dart-grpc/2.0.0")],
                )
            finally:
                if prev is not None:
                    os.environ["https_proxy"] = prev
                else:
                    os.environ.pop("https_proxy", None)
        return self._channel

    def _drop_channel(self, expected_ch: Optional[grpc.Channel] = None) -> None:
        with _channel_lock:
            if expected_ch is not None and self._channel is not expected_ch:
                return
            self._channel = None

    def call(self, method: str, payload: bytes, timeout: int = 90) -> bytes:
        token = self.mint_access_token()
        md = [
            ("authorization", f"Bearer {token}"),
            ("x-goog-authuser", "0"),
            ("x-custom-user-agent", self._grpc_ua()),
        ]
        rpc = method.rsplit("/", 1)[-1]
        t0 = time.monotonic()
        _trace_emit("grpc.rpc.start", nick=self.name, rpc=rpc,
                    req_bytes=len(payload), proxy=bool(self.proxy))
        # One resend for a connect that never happened; two for polls.
        budget = 2 if method in _READ_RPCS else 1
        last: grpc.RpcError | None = None
        for attempt in range(budget + 1):
            ch = self.channel()
            try:
                unary = ch.unary_unary(
                    method,
                    request_serializer=lambda x: x,
                    response_deserializer=lambda x: x,
                )
                resp, _ctx = unary.with_call(
                    payload, metadata=md, timeout=timeout)
                elapsed_ms = round((time.monotonic() - t0) * 1000)
                _trace_emit("grpc.rpc.end", nick=self.name, rpc=rpc,
                            elapsed_ms=elapsed_ms,
                            resp_bytes=len(resp or b""),
                            attempt=attempt)
                from agent.services.flow_debug_log import note_grpc
                note_grpc(self.name, rpc, elapsed_ms, resp=resp or b"")
                return resp or b""
            except ValueError as ve:
                self._drop_channel(ch)
                _trace_emit("grpc.rpc.closed_channel_reopen", nick=self.name,
                            rpc=rpc, attempt=attempt, detail=str(ve)[:200])
                if attempt < budget:
                    continue
                ch2 = self.channel()
                try:
                    unary2 = ch2.unary_unary(
                        method,
                        request_serializer=lambda x: x,
                        response_deserializer=lambda x: x,
                    )
                    resp, _ctx = unary2.with_call(
                        payload, metadata=md, timeout=timeout)
                    elapsed_ms = round((time.monotonic() - t0) * 1000)
                    _trace_emit("grpc.rpc.end", nick=self.name, rpc=rpc,
                                elapsed_ms=elapsed_ms,
                                resp_bytes=len(resp or b""),
                                attempt=attempt + 1)
                    from agent.services.flow_debug_log import note_grpc
                    note_grpc(self.name, rpc, elapsed_ms, resp=resp or b"")
                    return resp or b""
                except grpc.RpcError as e2:
                    self._drop_channel(ch2)
                    raise FlowGrpcError(str(e2.details() or ""), e2.code())
                except Exception as e2:
                    self._drop_channel(ch2)
                    raise FlowGrpcError(str(e2), grpc.StatusCode.UNAVAILABLE)
            except grpc.RpcError as e:
                last = e
                if e.code() == grpc.StatusCode.UNAUTHENTICATED and attempt == 0:
                    _trace_emit("grpc.rpc.reauth", nick=self.name, rpc=rpc)
                    md[0] = ("authorization",
                             f"Bearer {self.mint_access_token(force=True)}")
                    continue
                detail = str(e.details() or "")
                if (attempt < budget
                        and _rpc_retryable(method, e.code(), detail)):
                    self._drop_channel(ch)
                    _trace_emit("grpc.rpc.retry", nick=self.name, rpc=rpc,
                                attempt=attempt + 1,
                                grpc_code=str(e.code()),
                                detail=detail[:200])
                    time.sleep(0.4 * (attempt + 1))
                    continue
                if _rpc_retryable(method, e.code(), detail) or any(
                        s in detail.lower() for s in (
                            "no status received", "stream removed",
                            "connection reset", "broken pipe")):
                    self._drop_channel(ch)
                elapsed_ms = round((time.monotonic() - t0) * 1000)
                _trace_emit("grpc.rpc.error", nick=self.name, rpc=rpc,
                            elapsed_ms=elapsed_ms,
                            grpc_code=str(e.code()),
                            detail=detail[:300])
                from agent.services.flow_debug_log import note_grpc
                note_grpc(self.name, rpc, elapsed_ms, error=detail,
                          grpc_code=str(e.code()))
                raise FlowGrpcError(detail, e.code())
        detail = str(last.details() or "") if last is not None else "rpc failed"
        code = last.code() if last is not None else None
        raise FlowGrpcError(detail, code)


# --------------------------------------------------------------------------
# payload builders / response readers
# --------------------------------------------------------------------------

def _deepest_text_leaf(buf: bytes, prefix: list[int]) -> list[int] | None:
    """Deepest utf-8 text leaf under `prefix` that is not a uuid/model/token.
    Image prompts may sit at the slot itself ([2,2]) or nested inside a
    structured segment ([2,2,15]) — pick whichever leaf actually holds text.

    Descent is gated on `_looks_msg` (strict parse): decode() is lenient, so
    plain ASCII prompts like 'monkey image' fake-parse into phantom fields
    and patching a fragment (e.g. f15=' image') leaves the head bytes
    ('monke') in the payload — the captured prompt bleeds into every request
    on that nick. Real nested containers strict-parse, so their prompts are
    still found (e.g. video prompt at [1,1,3,1,1])."""
    def _is_text(v) -> bool:
        if not isinstance(v, bytes) or NON_PROMPT_RE.match(v):
            return False
        try:
            s = v.decode("utf-8")
        except UnicodeDecodeError:
            return False
        return bool(s) and all(c.isprintable() or c in "\n\t\r" for c in s)

    v = None
    cur = decode(buf)
    for fn in prefix:
        v = next((x for f, w, x in cur if f == fn and w == 2), None)
        if v is None:
            return None
        cur = decode(v) if _looks_msg(v) else []

    best: list[int] | None = None

    def rec(val, path):
        nonlocal best
        if isinstance(val, bytes) and _looks_msg(val):
            for f, w, sv in decode(val):
                if w == 2 and isinstance(sv, bytes):
                    rec(sv, path + [f])
            return
        if _is_text(val) and (best is None or len(path) > len(best)):
            best = path

    rec(v, prefix)
    return best


def _patch_prompt_slots(buf: bytes, prompt: str,
                        slots: list[list[int]]) -> bytes:
    """Patch the text leaf under each prompt container path. A slot that has
    no eligible leaf is left untouched (stale prompt survives rather than
    corrupting an unknown field)."""
    for slot in slots:
        leaf = _deepest_text_leaf(buf, slot)
        if leaf:
            buf = set_leaf_bytes(buf, leaf, prompt.encode())
        else:
            logger.warning("no prompt leaf under %s — payload keeps captured text", slot)
    return buf


def _url_in(v) -> str:
    """fifeUrl may be embedded mid-leaf behind a binary prefix — regex, not
    startswith."""
    if not isinstance(v, bytes):
        return ""
    m = URL_RE.search(v)
    return m.group(0).decode() if m else ""


def _block_media_id(block: bytes, leaves) -> Optional[bytes]:
    """Media blocks put their own uuid at f1 (BatchGenerateImages) or
    f1.f1 (BatchGetMedia). Read it by position; the first-uuid scan is
    only a fallback."""
    try:
        f1 = next((v for f, w, v in decode(block) if f == 1 and w == 2), None)
        if isinstance(f1, bytes):
            if MEDIA_ID_RE.match(f1):
                return f1
            f1_1 = next((v for f, w, v in decode(f1) if f == 1 and w == 2), None)
            if isinstance(f1_1, bytes) and MEDIA_ID_RE.match(f1_1):
                return f1_1
    except Exception:
        pass
    return next((lv for _, lv in leaves
                 if isinstance(lv, bytes) and MEDIA_ID_RE.match(lv)), None)


def _read_media_records(resp: bytes) -> list[dict]:
    """BatchGenerateImages response -> [{media_id, url}]."""
    out = []
    for f, w, v in decode(resp):
        if f != 1 or w != 2:
            continue
        leaves = walk_leaves(v)
        uuid = _block_media_id(v, leaves)
        url = next((u for u in (_url_in(lv) for _, lv in leaves) if u), "")
        if uuid:
            out.append({"media_id": uuid.decode(), "url": url})
    return out


def _scan_text_leaves(resp: bytes, max_n: int = 6) -> list[str]:
    """Collect short printable strings from a response — for diagnosing a
    gen call that returned media ids but no URL. Skips UUIDs and URLs."""
    out = []
    for _, v in walk_leaves(resp):
        if not isinstance(v, bytes):
            continue
        if UUID_RE.match(v) or URL_RE.search(v):
            continue
        try:
            s = v.decode("utf-8").strip()
        except UnicodeDecodeError:
            continue
        if len(s) < 4 or len(s) > 160 or not s.isprintable():
            continue
        out.append(s[:160])
        if len(out) >= max_n:
            break
    return out


def _uuid(v) -> str:
    return v.decode() if isinstance(v, bytes) and UUID_RE.match(v) else ""


def _leaf_at(fields, path):
    cur = fields
    for i, fn in enumerate(path):
        nxt = next((v for f, w, v in cur if f == fn and w == 2), None)
        if nxt is None:
            return None
        cur = decode(nxt) if i < len(path) - 1 else None
        if i == len(path) - 1:
            return nxt
    return None


def _read_operation(resp: bytes) -> dict:
    """Video submit response -> {op_id, media_id, project_id}.

    iOS shape (friend i2v, 2026-09-29): f3.f1 is the operation uuid,
    f3.f4.f5 is the media uuid the app BatchGetMedia's until `/video/`
    appears on that same id, f3.f4.f6 is a side id the app never polls,
    f3.f5 is the project. Treating f5 as the op and f6 as the media made
    every poll look at the poster and stay PENDING."""
    op_id = media_id = project_id = ""
    for f, w, v in decode(resp):
        if f != 3 or w != 2:
            continue
        sub = decode(v)
        op_id = _uuid(_leaf_at(sub, [1]) or b"") or op_id
        project_id = _uuid(_leaf_at(sub, [5]) or b"") or project_id
        inner = next((iv for fn, fw, iv in sub if fn == 4 and fw == 2), b"")
        if inner:
            ifs = decode(inner)
            media_id = _uuid(_leaf_at(ifs, [5]) or b"") or media_id
            if not media_id:
                media_id = _uuid(_leaf_at(ifs, [6]) or b"") or media_id
    if not op_id:
        for f, w, v in decode(resp):
            if f == 3 and w == 2:
                for _, lv in walk_leaves(v):
                    if _uuid(lv):
                        op_id = lv.decode()
                        break
    return {"op_id": op_id, "media_id": media_id, "project_id": project_id}


def _read_upsample(resp: bytes) -> dict:
    """BatchAsyncGenerateVideoUpsampleVideo submit response.

    Unlike the generate RPCs, the operation lands in top-level f4:
    f4.f1 = "<src_uuid>_upsampled" (or "_4k_upsampled") output media id,
    f4.f2 = project uuid, f4.f3 = the operation uuid.
    """
    op_id = project_id = media_id = ""
    for f, w, v in decode(resp):
        if f != 4 or w != 2:
            continue
        sub = decode(v)
        mleaf = _leaf_at(sub, [1]) or b""
        if isinstance(mleaf, bytes) and MEDIA_ID_RE.match(mleaf):
            media_id = mleaf.decode()
        op_id = _uuid(_leaf_at(sub, [3]) or b"") or op_id
        project_id = _uuid(_leaf_at(sub, [2]) or b"") or project_id
    return {"op_id": op_id, "project_id": project_id, "media_id": media_id}


def _terminal_video_complaint(text: str) -> bool:
    t = (text or "").lower()
    if not t or "not found" in t or "media not found" in t:
        return False
    return any(s in t for s in (
        "unsafe", "policy", "denied", "permission", "unusual_activity",
        "public_error", "failed",
    ))


def _read_check(resp: bytes) -> dict:
    """BatchCheckAsyncVideoGenerationStatus response.

    Pending ops return f3{ f1 op_uuid, f6{ f9{ complaint… } } } with no
    media uuid — the media id only arrives in the submit response. Resolved
    ops carry f3{ f1 op, f2 project, f3 media }."""
    op_id = media_id = complaint = ""
    for f, w, v in decode(resp):
        if f == 3 and w == 2:
            uuids = [lv.decode() for _, lv in walk_leaves(v)
                     if isinstance(lv, bytes) and UUID_RE.match(lv)]
            if uuids:
                op_id = uuids[0]
                media_id = uuids[-1] if len(uuids) > 2 else ""
            texts = [lv.decode("utf-8", errors="replace")
                     for _, lv in walk_leaves(v)
                     if isinstance(lv, bytes) and b" " in lv
                     and not UUID_RE.match(lv) and not lv.startswith(b"http")]
            complaint = next(
                (t for t in texts if t.isprintable() and re.search(
                    r"not found|error|fail|denied|policy|permission|unsafe",
                    t, re.I)), "")
    return {"op_id": op_id, "media_id": media_id, "complaint": complaint}


def _read_media_urls(resp: bytes) -> dict[str, dict]:
    """BatchGetMedia response -> {media_id: {image, video}}.

    A video media block carries both a poster url at f1.f6.f11, a thumbnail
    at f1.f8.f6.f1 / f1.f8.f6.f2.f2, and the clip url at f1.f8.f1.f9 (either
    flow-content.google/video/<uuid> or lh3.googleusercontent.com/asb/...).
    Never classify a URL containing '/image/' or thumbnail [1,8,6,*] as video."""
    out = {}
    for f, w, v in decode(resp):
        if f != 1 or w != 2:
            continue
        leaves = walk_leaves(v)
        uuid = _block_media_id(v, leaves)
        if not uuid:
            continue
        rec = {}
        for path, lv in leaves:
            u = _url_in(lv)
            if not u:
                continue
            if "/image/" in u:
                from agent.services.cdn import bump_google_image_url
                rec["image"] = bump_google_image_url(u)
            elif "/video/" in u or (len(path) >= 4 and path[:4] == [1, 8, 1, 9]):
                rec["video"] = u
            else:
                from agent.services.cdn import bump_google_image_url
                rec["image"] = bump_google_image_url(u)
        out[uuid.decode()] = rec
    return out


_MISSING_RE = re.compile(
    rb"Media not found: ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    rb"[0-9a-f]{4}-[0-9a-f]{12}(?:_(?:4k_)?upsampled)?)")


def _read_media_missing(resp: bytes) -> set[str]:
    """BatchGetMedia per-item NOT_FOUND: f1{ f2{ f1: 5, f2: 'Media not found: <id>' } }."""
    out = set()
    for f, w, v in decode(resp or b""):
        if f != 1 or w != 2:
            continue
        for sf, sw, sv in decode(v):
            if sf == 2 and sw == 2:
                m = _MISSING_RE.search(sv)
                if m:
                    out.add(m.group(1).decode())
    return out


def _read_project_media(resp: bytes) -> dict[str, str]:
    """GetProjectContents response -> {uuid_inside_op_entry: media_uuid}.

    Listing entries (f2): f1 = entry id (sometimes the op uuid itself).
    Media items (f3): f1 media_uuid, f3 parent == the f2 entry id. A uuid
    appearing anywhere inside an f2 entry (op uuid, placeholder…) resolves
    to the media joined through that entry's f1."""
    entry_uuids = {}   # entry_id -> set of uuids inside the entry
    parent_media = {}  # f3 parent entry id -> media uuid
    for f, w, v in decode(resp):
        if w != 2:
            continue
        sub = decode(v)
        if f == 2:
            eid = _uuid(_leaf_at(sub, [1]) or b"")
            uuids = {lv.decode() for _, lv in walk_leaves(v)
                     if isinstance(lv, bytes) and UUID_RE.match(lv)}
            if eid:
                entry_uuids[eid] = uuids
        elif f == 3:
            media = _uuid(_leaf_at(sub, [1]) or b"")
            parent = _uuid(_leaf_at(sub, [3]) or b"")
            if media and parent:
                parent_media[parent] = media
    out = {}
    for eid, uuids in entry_uuids.items():
        media = parent_media.get(eid)
        if media:
            for u in uuids:
                out.setdefault(u, media)
    return out


def _build_project_contents(project_id: str) -> bytes:
    return fld_str(1, f"projects/{project_id}")


def _build_check(op_id: str) -> bytes:
    return fld_msg(3, fld_str(1, op_id))


def _build_get_media(media_id: str) -> bytes:
    return fld_str(1, media_id)


def _ensure_jpeg(image: bytes) -> bytes:
    """UploadImage accepts JPEG bytes in f2. Nova often sends PNG/WebP."""
    if len(image) >= 3 and image[:3] == b"\xff\xd8\xff":
        return image
    try:
        from io import BytesIO
        from PIL import Image
        im = Image.open(BytesIO(image))
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        buf = BytesIO()
        im.save(buf, format="JPEG", quality=92)
        return buf.getvalue()
    except Exception:
        return image


def _build_upload(meta_block: bytes, image: bytes) -> bytes:
    # f3 = mime type — sits after the multi-MB image field in the phone's
    # payload so captures truncated at 200KB never showed it. Without it
    # UploadImage returns INVALID_ARGUMENT.
    return (fld_msg(1, meta_block) + fld_msg(2, _ensure_jpeg(image))
            + fld_str(3, "image/jpeg"))


# Aspect enums — confirmed by capture diffs on landon:
#   video: t2v portrait tpl f3=1, 16:9 capture f3=2 (i2v same slot, r2v f4)
#   image: portrait tpl f2.f5=2, landscape capture f2.f5=3
_VIDEO_ASPECT_FIELD = {"t2v": 3, "i2v": 3, "r2v": 4}
# Model string leaf path per tpl — t2v/i2v f1.f2, r2v f1.f3.
_VIDEO_MODEL_FIELD = {"t2v": [1, 2], "i2v": [1, 2], "r2v": [1, 3]}
# Clip length is a suffix on the model string. The captured t2v/i2v/r2v
# templates have no seconds field (f8/f10/f12 is an aspect-config varint).
VEO_DURATIONS = (4, 6, 8)
MACARON_DURATIONS = tuple(range(3, 16))
ABRA_DURATIONS = (4, 6, 8, 10)
_VEO_DURATION_TAG = re.compile(r"_(?:[3-9]|1[0-5])s(?=_|$)")
_VIDEO_ASPECT_VALUE = {"portrait": 1, "9:16": 1, "vertical": 1, "1:1": 1,
                       "landscape": 2, "16:9": 2, "horizontal": 2}
_IMAGE_ASPECT_VALUE = {
    "portrait_four_three": 4, "portrait_4_3": 4, "3:4": 4, "3x4": 4, "3/4": 4,
    "landscape_four_three": 5, "landscape_4_3": 5, "4:3": 5, "4x3": 5, "4/3": 5,
    "square": 1, "1:1": 1, "1x1": 1, "1/1": 1,
    "portrait": 2, "9:16": 2, "9x16": 2, "9/16": 2, "vertical": 2,
    "landscape": 3, "16:9": 3, "16x9": 3, "16/9": 3, "horizontal": 3,
}

# Flow-app display name -> request codename (iPhone mitm 2026-10-07,
# lucas account; gen order NB2.1, NB2 Lite, NB Pro -> BELUGA,
# HARBOR_SEAL, GEM_PIX_2). NARWHAL (old default) retired upstream.
_IMAGE_MODEL_ALIASES = {
    "nano banana 2.1": "BELUGA",
    "nano banana 2 lite": "HARBOR_SEAL",
    "nano banana pro": "GEM_PIX_2",
    "nb2.1": "BELUGA", "nb2 lite": "HARBOR_SEAL", "nb pro": "GEM_PIX_2",
    # gate aliases Nova/lightvisual gửi (chưa từng tới upstream — field
    # này bị drop tới 2026-10-07, giờ forward thật nên phải map đúng)
    "nano_banana_pro": "GEM_PIX_2", "gem_pix": "GEM_PIX_2",
    "nano_banana_2": "BELUGA", "nano banana 2": "BELUGA",
}


def _image_model_codename(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    return _IMAGE_MODEL_ALIASES.get(n.lower(), n.upper())


def _aspect_varint(aspect_ratio: str, table: dict) -> int | None:
    a = (aspect_ratio or "").strip().lower().replace(" ", "")
    for key, val in table.items():
        if key in a:
            return val
    m = re.match(r"^(\d+)[x?*](\d+)$", a)
    if m and table is _IMAGE_ASPECT_VALUE:
        w, h = int(m.group(1)), int(m.group(2))
        if w > 0 and h > 0:
            if w == h:
                return 1
            if w > h:
                return 5 if abs(w * 3 - h * 4) * 10 <= h * 4 else 3
            return 4 if abs(w * 4 - h * 3) * 10 <= w * 4 else 2
    return None


def coerce_veo_duration(duration_s, tpl: str) -> int:
    """Veo t2v/i2v clips are 4, 6, or 8 seconds. Ingredients (r2v) is 8 only."""
    try:
        dur = int(duration_s)
    except (TypeError, ValueError):
        dur = None
    if dur not in VEO_DURATIONS:
        raise FlowGrpcError(
            f"Veo duration must be 4, 6, or 8 seconds (got {duration_s!r})")
    if tpl == "r2v" and dur != 8:
        raise FlowGrpcError("Veo reference-to-video is 8 seconds only")
    return dur


def veo_model_for_duration(model: str, duration_s: int, tpl: str) -> str:
    """Encode clip length in the model key. 8s keeps the captured string.

    Text-to-video inserts ``_{N}s`` before ``_low_priority``:
    ``veo_3_1_t2v_lite_low_priority`` → ``veo_3_1_t2v_lite_4s_low_priority``.
    Image-to-video lite uses the ``_s_`` form. Phone submits on 2026-10-22
    returned HTTP 200 for ``veo_3_1_i2v_s_lite_4s_low_priority`` and
    ``veo_3_1_i2v_s_lite_6s_low_priority``. The no-``_s_`` form
    ``veo_3_1_i2v_lite_4s_low_priority`` was NOT_FOUND. A free-tier key
    without ``_low_priority`` stays ``veo_3_1_i2v_s_lite_{N}s``. A key that
    already names a duration is unchanged.
    """
    dur = coerce_veo_duration(duration_s, tpl)
    if dur == 8 or not model or _VEO_DURATION_TAG.search(model):
        return model
    if tpl == "i2v" and "i2v_lite" in model:
        key = f"veo_3_1_i2v_s_lite_{dur}s"
        if "_low_priority" in model:
            return key + "_low_priority"
        return key
    tag = f"_{dur}s"
    marker = "_low_priority"
    at = model.find(marker)
    if at != -1:
        return model[:at] + tag + model[at:]
    return model + tag


_OMNI_12_ALIASES = {
    "omni_1_2", "omni_1_2_flash", "omni_1.2",
    "omni_1.2_flash", "macaron_cookie", "omni_360p", "omni_1_2_flash_360p",
    "omni_1.2_flash_360p", "macaron_cookie_360p",
}
_OMNI_11_ALIASES = {
    "omni", "omni_flash", "omni_1_1", "omni_1_1_flash", "omni_1.1",
    "omni_1.1_flash", "abra", "omni_8s", "omni_10s",
}
OMNI_ALLOWED_NICKS = ("vids01",)


def _nearest_abra_duration(dur: int) -> int:
    return min(ABRA_DURATIONS, key=lambda d: abs(d - dur))


def _resolve_omni_model(video_model: str, tpl: str,
                        duration_s: int = 8) -> tuple[str, int] | None:
    """Resolve an Omni 1.2 Flash (`macaron_cookie_*`) or Omni 1.1 Flash (`abra_*`)
    model string and validated duration_s, or return None if not an Omni model."""
    raw = (video_model or "").strip().lower()
    if not raw:
        return None
    norm = re.sub(r"[\s\-]+", "_", raw)
    try:
        dur = int(duration_s)
    except (TypeError, ValueError):
        dur = 8

    if norm.startswith("macaron_cookie_"):
        m = re.sub(r"_(?:t2v|i2v|r2v)_", f"_{tpl}_", norm, count=1)
        m_dur = re.search(r"_(\d+)s(?=_|$)", m)
        if m_dur:
            d = int(m_dur.group(1))
            if d not in MACARON_DURATIONS:
                raise FlowGrpcError(
                    f"Omni 1.2 Flash duration must be 3..15s (got {d}s)")
            return m, d
        if dur not in MACARON_DURATIONS:
            raise FlowGrpcError(
                f"Omni 1.2 Flash duration must be 3..15s (got {duration_s!r})")
        suffix = "_360p" if m.endswith("_360p") else ""
        return f"macaron_cookie_{tpl}_{dur}s{suffix}", dur

    if norm.startswith("abra_"):
        m = re.sub(r"_(?:t2v|i2v|r2v)_", f"_{tpl}_", norm, count=1)
        m_dur = re.search(r"_(\d+)s(?=_|$)", m)
        if m_dur:
            d = int(m_dur.group(1))
            if d not in ABRA_DURATIONS:
                raise FlowGrpcError(
                    f"Omni 1.1 Flash duration must be 4, 6, 8, or 10s (got {d}s)")
            return m, d
        if dur not in ABRA_DURATIONS:
            raise FlowGrpcError(
                f"Omni 1.1 Flash duration must be 4, 6, 8, or 10s (got {duration_s!r})")
        return f"abra_{tpl}_{dur}s", dur

    if norm in _OMNI_12_ALIASES:
        if dur not in MACARON_DURATIONS:
            raise FlowGrpcError(
                f"Omni 1.2 Flash duration must be 3..15s (got {duration_s!r})")
        suffix = "_360p" if norm.endswith("_360p") else ""
        return f"macaron_cookie_{tpl}_{dur}s{suffix}", dur

    if norm in _OMNI_11_ALIASES:
        if dur not in ABRA_DURATIONS:
            raise FlowGrpcError(
                f"Omni 1.1 Flash duration must be 4, 6, 8, or 10s (got {duration_s!r})")
        return f"abra_{tpl}_{dur}s", dur

    return None


def _build_video(tpl_name: str, prompt: str, meta_block: bytes,
                 start_uuid: str | None = None,
                 ref_uuids: list[str] | None = None,
                 aspect_ratio: str = "") -> bytes:
    tpl = (TPL_DIR / f"{tpl_name}.bin").read_bytes()
    buf = _patch_prompt_slots(tpl, prompt, [[1]])
    if ref_uuids:
        entry = [fld_str(2, u) + fld_varint(3, 1) for u in ref_uuids]
        buf = replace_fields(buf, [1], 2, entry)
    if start_uuid:
        buf = set_leaf_bytes(buf, [1, 5, 2], start_uuid.encode())
    av = _aspect_varint(aspect_ratio, _VIDEO_ASPECT_VALUE)
    if av is not None:
        buf = set_varint(buf, [1, _VIDEO_ASPECT_FIELD[tpl_name]], av)
    return set_leaf_bytes(buf, [2], meta_block)


def _build_upscale(source_uuid: str, meta_block: bytes,
                   model: str = "", aspect_ratio: str = "") -> bytes:
    """BatchAsyncGenerateVideoUpsampleVideo (captured 2026-09-29, friend).

    f1{ f1{f2 source media uuid}, f3 aspect, f5{f2 fresh op uuid},
        f7 resolution_enum (2=1080p, 3=4k),
        f32 'veo_3_1_upsampler_1080p'|'veo_3_1_upsampler_4k' } + f2 meta block.
    f3 is the same video aspect enum as t2v/i2v f3 (1=portrait, 2=landscape)
    — the capture came from a landscape source so the template carries 2,
    and leaving it there stretches 9:16 sources into 16:9.
    Output media id is deterministic: <source>_upsampled (1080p) or
    <source>_4k_upsampled (4K) — the app polls it with plain BatchGetMedia."""
    buf = (TPL_DIR / "upscale.bin").read_bytes()
    buf = set_leaf_bytes(buf, [1, 1, 2], source_uuid.encode())
    buf = set_leaf_bytes(buf, [1, 5, 2], str(_uuid_mod.uuid4()).encode())
    av = _aspect_varint(aspect_ratio, _VIDEO_ASPECT_VALUE)
    if av is not None:
        buf = set_varint(buf, [1, 3], av)
    res_enum = 3 if "4k" in (model or "").lower() else 2
    buf = set_varint(buf, [1, 7], res_enum)
    if model:
        buf = set_leaf_bytes(buf, [1, 32], model.encode())
    return set_leaf_bytes(buf, [2], meta_block)


# --------------------------------------------------------------------------
# transport
# --------------------------------------------------------------------------

# Per-nick scheduling: max in-flight gen calls and minimum spacing between
# submits (the iOS app itself fires serially; bursts beyond this drew
# UNUSUAL_ACTIVITY on web paths).
MAX_CONCURRENT_PER_NICK = int(os.environ.get("MAX_CONCURRENT_PER_NICK", "12"))
# USER_QUOTA_REACHED = account credits exhausted → park this long.
QUOTA_REACHED_PARK_S = int(os.environ.get("QUOTA_REACHED_PARK_S", "21600"))
# UploadImage RESOURCE_EXHAUSTED → no uploads / i2v / r2v on that account.
# Quota refills gradually ("hồi nhỏ giọt"), so a dead account is probed on
# an escalating schedule: park starts at PROBE_BASE and doubles per failed
# probe, capped at PROBE_MAX — instead of a flat 24h park.
UPLOAD_DEAD_PARK_S = int(os.environ.get("UPLOAD_DEAD_PARK_S", "86400"))
UPLOAD_PROBE_BASE_S = int(os.environ.get("UPLOAD_PROBE_BASE_S", "900"))
UPLOAD_PROBE_MAX_S = int(os.environ.get("UPLOAD_PROBE_MAX_S", "3600"))
PACE_MIN_S = 2.0
PACE_MAX_S = 3.0

# _routes.json retention. Nova stops polling an op within the hour; 2 hours
# covers late /media/{op} downloads. Media owners are refreshed on every use
# as a reference, so a reused character image never ages out.
ROUTES_OP_TTL_S = 2 * 3600
ROUTES_MEDIA_TTL_S = 6 * 3600
# Route saves coalesce into one flush per this window — handled entirely
# in the background writer thread so caller request threads are never blocked.
ROUTES_FLUSH_S = 3.0

# Poll result reuse. Nova polls each op every few seconds; ops Google never
# finishes (stuck) were ~60% of all BatchGetMedia traffic. Serve a recent
# answer instead of asking Google again, backing off as the op ages.
POLL_DONE_TTL_S = 600

# labs JWE (~30d) + Google session cookies both rotate via Set-Cookie on
# plain HTTPS responses — verified: GET /fx/api/auth/session returns a
# fresh session-token cookie, GET myaccount.google.com rotates SID family.
# A periodic HTTP sweep replaces the PC-side Chrome cookie-sync entirely.
SESSION_REFRESH_S = int(os.environ.get("SESSION_REFRESH_S", "480"))  # 8m (< 600s Google RotateCookies window)
JWE_COOKIE_NAME = "__Secure-next-auth.session-token"
_MYACCOUNT_URL = "https://myaccount.google.com/"

# Session-refresh hits browser-shaped endpoints — carry a real Chrome TLS
# fingerprint (JA3) so the traffic does not stand out next to the account's
# own browser history. requests stays as the no-cffi fallback.
try:
    from curl_cffi import requests as _creq
    _HAS_CFFI = True
except ImportError:
    _creq = requests
    _HAS_CFFI = False

# Google sometimes deletes a render's output media (and its project entry)
# right when it would finish — ~5% of videos, never recovered (checked hours
# later). Only the owning nick's answer counts; a wrong nick also says
# not-found. Confirm across polls before failing.
REMOVED_MIN_AGE_S = 20
REMOVED_CONFIRM_S = 15

# Submit-time content refusals (PUBLIC_ERROR_UNSAFE_GENERATION and friends
# arrive as INVALID_ARGUMENT / FAILED_PRECONDITION).
_POLICY_RE = re.compile(
    r"unsafe|polic(?:y|ies)|prominent[_ ]people|celebrit"
    r"|safety[_ ]?(?:filter|block)|safetyblocked"
    r"|responsible[_ ]ai|minor_|\bminors?\b|child_|sexual|violen",
    re.IGNORECASE)


def _poll_ttl(elapsed: float) -> float:
    if elapsed < 90:
        return 3.0
    if elapsed < VIDEO_STUCK_TIMEOUT:
        return 8.0
    return 12.0


class FlowGrpcTransport:
    """Drop-in gRPC equivalent of the FlowClient generation surface."""

    def __init__(self):
        self._nicks: dict[str, GrpcNick] = {}
        self._order: list[str] = []
        self._rr_idx = 0
        self._inflight: dict[str, int] = {}
        self._next_slot: dict[str, float] = {}
        self._pace_locks: dict[str, asyncio.Lock] = {}
        self._waiters = 0
        self._waiters_hi = 0
        self._waiters_ops: dict[str, int] = {"image": 0, "video": 0}
        self._throttle_until: dict[str, float] = {}
        self._throttle_streak: dict[str, int] = {}
        # Account-keyed upload-quota death (project_uuid -> dead-until).
        # RESOURCE_EXHAUSTED on UploadImage is per-account, not per-nick —
        # clone siblings share the project and fail together. Kept separate
        # from _throttle_until so a dead upload quota does not also stop
        # the account's (still-working) generation traffic.
        self._quota_dead_until: dict[str, float] = {}
        # Consecutive quota-dead probes per account — each failure doubles
        # the park so a rolling "uploading too fast" window actually gets
        # time to clear instead of being re-poked every 2 minutes.
        self._quota_streak: dict[str, int] = {}
        # Consecutive UNAUTHENTICATED per nick — cooldown escalates per
        # failure and >=5 parks long-term so dead creds stop erroring
        # customer requests until a cookie refresh lands.
        self._unauth_streak: dict[str, int] = {}
        self._no_cooldown: set[str] = set()
        # Manual/auth parks (long-horizon, _parked.json) — the empty-pool
        # fallback must never probe these; they're parked on purpose.
        self._parked: set[str] = set()
        self._sched_lock: Optional[asyncio.Lock] = None
        self._media_owner: dict[str, str] = {}
        self._op_owner: dict[str, str] = {}
        self._op_media: dict[str, str] = {}
        self._op_project: dict[str, str] = {}
        self._rebase_cache: dict[str, str] = {}
        self._op_started: dict[str, float] = {}
        self._op_start_image: dict[str, str] = {}
        self._media_seen: dict[str, float] = {}
        self._routes_lock = threading.Lock()
        self._poll_cache: dict[str, tuple[float, dict]] = {}
        self._op_missing_since: dict[str, float] = {}
        self._op_retry: dict[str, dict] = {}
        self._op_alias: dict[str, str] = {}
        self._op_req: dict[str, str] = {}
        self._op_checked: dict[str, float] = {}
        # sha256(raw bytes) -> {media_id, nick, ts}: a repeated upload of the
        # same file reuses the media_id instead of burning upload quota.
        self._uploaded: dict[str, dict] = {}
        self._resubmit_lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._last_disk_merge = 0.0
        self._flag_mtimes: dict = {}
        self._routes_sig: tuple | None = None
        self._routes_dirty = False
        self._routes_last_flush = 0.0
        self._routes_last_prune = 0.0
        self._load_routes()
        self._load_park()
        self._load_nocooldown()
        self._load_quota()
        self._load_uploaded()
        self._load_pushed_tokens()
        threading.Thread(target=self._routes_flush_loop,
                         daemon=True).start()
        threading.Thread(target=self._session_refresh_loop,
                         daemon=True).start()

    # -- persisted park ------------------------------------------------------

    def _load_park(self) -> None:
        try:
            data = json.loads(PARK_FILE.read_text())
        except Exception:
            return
        now = time.time()
        self._parked = {name for name, until in data.items()
                        if isinstance(until, (int, float)) and until > now
                        and name in self._nicks}
        for name in self._parked:
            self._throttle_until[name] = max(
                self._throttle_until.get(name, 0),
                float(data[name]))

    def set_park(self, name: str, until: float) -> None:
        """Park/unpark a nick for submits, persisted across restarts.
        Polls/downloads still run; pinned refs rebase off the nick."""
        now = time.time()
        try:
            data = json.loads(PARK_FILE.read_text())
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        if until > now:
            self._throttle_until[name] = max(
                self._throttle_until.get(name, 0), until)
            self._parked.add(name)
            data[name] = until
        else:
            was_parked = name in self._parked or name in data
            self._parked.discard(name)
            data.pop(name, None)
            if was_parked:
                self._throttle_until.pop(name, None)
                self._throttle_streak.pop(name, None)
        try:
            PARK_FILE.write_text(json.dumps(data))
        except Exception:
            pass

    def _load_nocooldown(self) -> None:
        try:
            data = json.loads(NOCOOLDOWN_FILE.read_text())
            self._no_cooldown = {n for n in data
                                 if isinstance(n, str)}
        except Exception:
            self._no_cooldown = set()

    def _load_quota(self) -> None:
        """A quota death the sibling saw applies here too — the account is
        exhausted regardless of which process caught RESOURCE_EXHAUSTED."""
        try:
            data = json.loads(QUOTA_FILE.read_text())
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        now = time.time()
        live = set()
        for acct, rec in data.items():
            try:
                until = float((rec or {}).get("until", 0))
            except (TypeError, ValueError):
                continue
            if until <= now:
                continue
            live.add(acct)
            if until > self._quota_dead_until.get(acct, 0):
                self._quota_dead_until[acct] = until
                self._quota_streak[acct] = max(
                    self._quota_streak.get(acct, 0),
                    int((rec or {}).get("streak", 1) or 1))
        # quota_ok on the sibling deletes the entry — mirror the clear.
        for acct in list(self._quota_dead_until):
            if acct not in live:
                self._quota_dead_until.pop(acct, None)
                self._quota_streak.pop(acct, None)

    def _save_quota(self) -> None:
        try:
            try:
                data = json.loads(QUOTA_FILE.read_text())
            except Exception:
                data = {}
            if not isinstance(data, dict):
                data = {}
            for acct, until in self._quota_dead_until.items():
                if until > time.time():
                    data[acct] = {"until": until,
                                  "streak": self._quota_streak.get(acct, 1)}
            for acct in list(data):
                if acct not in self._quota_dead_until:
                    data.pop(acct, None)
            tmp = QUOTA_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(data))
            os.chmod(tmp, 0o600)
            os.replace(tmp, QUOTA_FILE)
        except Exception:
            pass

    def _load_uploaded(self) -> None:
        try:
            data = json.loads(UPLOAD_MAP_FILE.read_text())
            if isinstance(data, dict):
                self._uploaded = {h: r for h, r in data.items()
                                  if isinstance(r, dict) and r.get("media_id")}
                self._uploaded_owner = {
                    r["media_id"]: r["nick"]
                    for r in self._uploaded.values()
                    if r.get("media_id") and r.get("nick")
                }
        except Exception:
            pass

    def _save_uploaded(self) -> None:
        try:
            tmp = UPLOAD_MAP_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._uploaded))
            os.chmod(tmp, 0o600)
            os.replace(tmp, UPLOAD_MAP_FILE)
        except Exception:
            pass

    def _save_pushed_token(self, name: str, token: str, exp: float) -> None:
        try:
            try:
                data = json.loads(YA29_FILE.read_text())
            except Exception:
                data = {}
            if not isinstance(data, dict):
                data = {}
            now = time.time()
            data[name] = {"token": token, "exp": exp}
            for n, rec in list(data.items()):
                if float((rec or {}).get("exp", 0)) <= now:
                    data.pop(n, None)
            tmp = YA29_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(data))
            os.chmod(tmp, 0o600)
            os.replace(tmp, YA29_FILE)
        except Exception:
            pass

    def _load_pushed_tokens(self) -> None:
        """Adopt phone-pushed ya29 tokens — the owning device minted them,
        so they survive far longer than a VPS-side aas_et mint."""
        try:
            data = json.loads(YA29_FILE.read_text())
        except Exception:
            return
        if not isinstance(data, dict):
            return
        now = time.time()
        for name, rec in data.items():
            nick = self._nicks.get(name)
            try:
                exp = float((rec or {}).get("exp", 0))
                tok = (rec or {}).get("token", "")
            except (TypeError, ValueError):
                continue
            if nick and tok and exp > now and exp > nick._ya29_exp:
                nick._ya29 = tok
                nick._ya29_exp = exp

    def _refresh_flags(self) -> None:
        """Both agents serve traffic now — an admin toggle written through
        the other process must take effect here too. Stat the flag files
        once per acquire (µs) and reload when mtime moved."""
        for path in (PARK_FILE, NOCOOLDOWN_FILE, QUOTA_FILE, YA29_FILE):
            try:
                mt = path.stat().st_mtime
            except OSError:
                mt = 0.0
            if self._flag_mtimes.get(path) == mt:
                continue
            self._flag_mtimes[path] = mt
            if path is NOCOOLDOWN_FILE:
                self._load_nocooldown()
                continue
            if path is QUOTA_FILE:
                self._load_quota()
                continue
            if path is YA29_FILE:
                self._load_pushed_tokens()
                continue
            self._load_park()
            # _load_park only ever raises throttle_until — propagate manual
            # unparks too: entries past the 120s horizon came from set_park
            # (THROTTLED cooldowns are <=20s), so a nick missing from the
            # file was unparked by the other agent.
            try:
                data = json.loads(path.read_text()) or {}
                parked = {k for k, v in data.items()
                          if isinstance(v, (int, float)) and v > time.time()}
                horizon = time.time() + 120
                for n, until in list(self._throttle_until.items()):
                    if n not in parked and until > horizon:
                        self._throttle_until.pop(n, None)
                self._parked.intersection_update(parked)
            except Exception:
                pass

    def set_no_cooldown(self, name: str, off: bool) -> None:
        """Disable/enable THROTTLED cooldown for a nick, persisted."""
        if off:
            self._no_cooldown.add(name)
        else:
            self._no_cooldown.discard(name)
        try:
            NOCOOLDOWN_FILE.write_text(
                json.dumps(sorted(self._no_cooldown)))
        except Exception:
            pass

    # -- nick registry -------------------------------------------------------

    def nicks(self) -> list[str]:
        try:
            nmt = NICKS_DIR.stat().st_mtime
        except OSError:
            nmt = 0.0
        if self._flag_mtimes.get(NICKS_DIR) != nmt:
            self.reload()
        return sorted(self._nicks)

    def reload(self) -> None:
        try:
            self._flag_mtimes[NICKS_DIR] = NICKS_DIR.stat().st_mtime
        except OSError:
            self._flag_mtimes[NICKS_DIR] = 0.0
        old_nicks = getattr(self, "_nicks", {}) or {}
        new_nicks: dict[str, GrpcNick] = {}
        if NICKS_DIR.exists():
            for d in NICKS_DIR.iterdir():
                if not d.is_dir() or d.name.startswith("_"):
                    continue
                if not ((d / "refresh_token.txt").exists()
                        or (d / "aas_et.txt").exists()
                        or (d / "labs_cookie.txt").exists()):
                    continue
                nj = GrpcNick(d)
                prev = old_nicks.get(d.name)
                if prev is not None and prev.proxy == nj.proxy:
                    nj._channel = prev._channel
                    nj._channel_relay_port = getattr(prev, "_channel_relay_port", None)
                    nj._grpc_proxy_url = prev._grpc_proxy_url
                    nj._captchas = prev._captchas
                    if (prev.labs_cookie == nj.labs_cookie
                            and prev.refresh_token == nj.refresh_token
                            and prev.aas_et == nj.aas_et):
                        nj._ya29 = prev._ya29
                        nj._ya29_exp = prev._ya29_exp
                elif prev is not None:
                    prev._drop_channel()
                new_nicks[d.name] = nj
        for dead_name, dead_nick in old_nicks.items():
            if dead_name not in new_nicks:
                dead_nick._drop_channel()
        self._nicks = new_nicks
        self._order = sorted(self._nicks)
        for name in self._order:
            self._inflight.setdefault(name, 0)
        for name in list(self._inflight):
            if name not in self._nicks:
                self._inflight.pop(name, None)
        self._load_park()
        self._load_nocooldown()
        self._load_pushed_tokens()

    def _route_dicts(self) -> list[tuple[str, dict]]:
        return [("media", self._media_owner), ("ops", self._op_owner),
                ("op_media", self._op_media),
                ("op_project", self._op_project),
                ("rebase", self._rebase_cache),
                ("op_started", self._op_started),
                ("op_start_image", self._op_start_image),
                ("op_alias", self._op_alias),
                ("op_retry", self._op_retry),
                ("op_req", self._op_req),
                ("media_seen", self._media_seen)]

    def _load_routes(self) -> None:
        self._merge_disk_routes()
        # A resubmit flag written by the process that just died would
        # otherwise block auto-retry forever.
        for s in self._op_retry.values():
            s.pop("resubmitting", None)

    def _merge_disk_routes(self) -> None:
        """Adopt entries another process wrote. During a zero-downtime swap
        the draining agent still lands submits the new one must poll on the
        right nick. Sig-gated on (mtime, size): the file is tens of MB, so
        re-parsing when nothing changed is a measurable stall — callers on
        the event loop must stay cheap."""
        try:
            st = ROUTES_FILE.stat()
        except OSError:
            return
        if getattr(self, "_routes_sig", None) == (st.st_mtime_ns,
                                                  st.st_size):
            return
        try:
            routes = json.loads(ROUTES_FILE.read_text())
        except Exception:
            return
        self._routes_sig = (st.st_mtime_ns, st.st_size)
        for key, d in self._route_dicts():
            for k, v in (routes.get(key) or {}).items():
                d.setdefault(k, v)

    def _locked_merge(self) -> None:
        with self._routes_lock:
            self._merge_disk_routes()

    def _prune_routes(self, now: float) -> None:
        # Entries saved before timestamps existed start their clock now.
        for op in list(self._op_owner):
            self._op_started.setdefault(op, now)
        for m in list(self._media_owner):
            self._media_seen.setdefault(m, now)
        old_ops = {k for k, t in list(self._op_started.items())
                   if now - t > ROUTES_OP_TTL_S}
        old_media = {k for k, t in list(self._media_seen.items())
                     if now - t > ROUTES_MEDIA_TTL_S}
        for d in (self._op_owner, self._op_media, self._op_project,
                  self._op_started, self._op_start_image,
                  self._op_alias, self._op_retry, self._op_checked,
                  self._op_missing_since, self._op_req):
            for k in old_ops & d.keys():
                d.pop(k, None)
        for d in (self._media_owner, self._media_seen):
            for k in old_media & d.keys():
                d.pop(k, None)
        old_rebase = {k for k in self._rebase_cache
                      if k.split(":")[0] in old_media}
        for k in old_rebase:
            self._rebase_cache.pop(k, None)

    def _touch_media(self, media_ids) -> None:
        now = time.time()
        for m in media_ids or ():
            if m in self._media_owner:
                self._media_seen[m] = now

    def _save_routes(self, sync: bool = False) -> None:
        """Mark routes dirty; writer thread coalesces bursts into flushes."""
        self._routes_dirty = True
        if sync or time.time() - getattr(self, "_routes_last_flush", 0.0) \
                >= ROUTES_FLUSH_S:
            self._flush_routes()

    def _routes_flush_loop(self) -> None:
        while True:
            time.sleep(ROUTES_FLUSH_S)
            if self._routes_dirty:
                try:
                    self._flush_routes()
                except Exception:
                    logger.exception("routes flush failed")

    def _flush_routes(self) -> None:
        # Serialize per process, then across processes via the lock file:
        # merge+write must be atomic or the last writer drops entries the
        # sibling committed between its own merge and rename. dict() copy
        # is atomic under the GIL; tmp+rename keeps the file consistent on
        # crash.
        try:
            with self._routes_lock:
                self._routes_dirty = False
                self._routes_last_flush = time.time()
                fd = os.open(str(ROUTES_LOCK_FILE), os.O_CREAT | os.O_RDWR)
                try:
                    _flock(fd)
                    try:
                        self._merge_disk_routes()
                        now = time.time()
                        if now - getattr(self, "_routes_last_prune", 0.0) \
                                >= 60:
                            self._routes_last_prune = now
                            self._prune_routes(now)
                        snap = {k: dict(v)
                                for k, v in self._route_dicts()}
                        tmp = ROUTES_FILE.with_suffix(f".{os.getpid()}.tmp")
                        tmp.write_text(json.dumps(snap,
                                                  separators=(",", ":")))
                        os.chmod(tmp, 0o600)
                        os.replace(tmp, ROUTES_FILE)
                        try:
                            st = ROUTES_FILE.stat()
                            self._routes_sig = (st.st_mtime_ns,
                                                st.st_size)
                        except OSError:
                            pass
                    finally:
                        _funlock(fd)
                finally:
                    os.close(fd)
        except Exception:
            logger.exception("save routes failed")

    # -- session refresh (replaces PC-side Chrome cookie-sync) ---------------

    def _session_refresh_loop(self) -> None:
        time.sleep(30)  # let the fleet settle before the first sweep
        while True:
            seen_names = set()
            for nick in list(self._nicks.values()):
                seen_names.add(nick.name)
                try:
                    out = self._refresh_nick_session(nick)
                    if out:
                        _trace_emit("session.refresh", nick=nick.name, **out)
                except Exception:
                    logger.exception("session refresh %s", nick.name)
                time.sleep(5)  # stagger
            # Also keep Google cookies rotated for vids_only nicks on VPS
            if NICKS_DIR.exists():
                for d in sorted(NICKS_DIR.iterdir()):
                    if not d.is_dir() or d.name.startswith("_") or d.name in seen_names:
                        continue
                    if (d / "vids_cookies.json").exists():
                        try:
                            vn = GrpcNick(d)
                            hdr = vn._google_cookie_header()
                            if hdr:
                                vn._rotate_google_cookies(hdr)
                        except Exception:
                            pass
                        time.sleep(3)
            time.sleep(SESSION_REFRESH_S)

    def _refresh_nick_session(self, nick) -> dict:
        """Rotate JWE + Google session cookies over plain HTTPS through the
        nick's own proxy. Verified 2026-10: both labs session-token and the
        SID family arrive as Set-Cookie on ordinary authenticated GETs — no
        browser needed. A 401/403 means the Google session truly died →
        leave a trace marker so ops can see it needs a real relogin."""
        out: dict = {}
        d = NICKS_DIR / nick.name
        cookie_hdr = nick._google_cookie_header()
        if cookie_hdr:
            try:
                tok = nick._mint_iframerpc(cookie_hdr)
                if tok and tok.startswith("ya29."):
                    out["jwe"] = "iframerpc_ok"
                    return out
            except Exception as e:
                out["jwe"] = f"iframerpc_err:{str(e)[:80]}"
        kw = {"impersonate": "chrome"} if _HAS_CFFI else {}
        jwe_f = d / "labs_cookie.txt"
        if jwe_f.exists() and nick.labs_cookie:
            try:
                r = _creq.get(
                    GrpcNick.LABS_SESSION_URL,
                    headers={"cookie": nick.labs_cookie,
                             "referer": "https://labs.google/fx/"},
                    proxies=nick.proxies, timeout=30, **kw)
                body = GrpcNick._parse_labs_session(r) if r.status_code == 200 else {}
                err = (body or {}).get("error")
                tok = (body or {}).get("access_token") or ""
                new = r.cookies.get(JWE_COOKIE_NAME)
                if err:
                    out["jwe"] = f"err:{err}"
                elif new and len(new.split(".")) == 5 and tok.startswith("ya29."):
                    jwe_f.write_text(f"{JWE_COOKIE_NAME}={new}")
                    nick.labs_cookie = f"{JWE_COOKIE_NAME}={new}"
                    nick._ya29 = tok
                    nick._ya29_exp = time.time() + 3300
                    out["jwe"] = "rotated"
                elif r.status_code == 200 and tok.startswith("ya29."):
                    nick._ya29 = tok
                    nick._ya29_exp = time.time() + 3300
                    out["jwe"] = "ok"
                else:
                    out["jwe"] = f"http{r.status_code}"
            except Exception as e:
                out["jwe"] = f"err:{str(e)[:60]}"
        # vids_cookies.json is deliberately NOT rotated here. The Vids/Omni
        # web session is anchored to the PC Chrome that created it: any
        # docs.google.com/myaccount hit from the VPS proxy IP is a
        # session-hijack signal — on 2026-10-13 loading the doc page from
        # headless Chromium on this VPS got all 3 Google sessions killed
        # globally (PC Chrome forced back to accountchooser, Labs OAuth
        # grants revoked). Fresh cookies arrive from the PC-side
        # cookie-sync every ~30min; death is reported by vids_client
        # itself via the _vids_dead marker when a real call 401s.
        return out

    # -- request scheduling ---------------------------------------------------
    #
    # Spread evenly across nicks (least in-flight wins, round-robin on ties),
    # cap 12 concurrent calls per nick, and pace submits 2-3s apart per nick.
    # Pinned calls (media/operation owner) must land on their nick and only
    # wait for that nick's slot.

    def _sched(self) -> asyncio.Lock:
        if self._sched_lock is None:
            self._sched_lock = asyncio.Lock()
        return self._sched_lock

    def _candidate(self, profile_id: str | None,
                   media_ids: list[str] | None) -> str | None:
        if profile_id and profile_id in self._nicks:
            return profile_id
        for mid in media_ids or []:
            owner = self._media_owner.get(mid)
            if owner and owner in self._nicks:
                return owner
        return None

    def _upload_dead(self, name: str, now: float | None = None) -> bool:
        """True if the nick's account has an exhausted upload quota."""
        now = now or time.time()
        acct = getattr(self._nicks.get(name), "project_uuid", "") or name
        return self._quota_dead_until.get(acct, 0) > now

    def _mark_upload_dead(self, nick: "GrpcNick") -> None:
        """UploadImage RESOURCE_EXHAUSTED: park the account's uploads and
        i2v/r2v for UPLOAD_DEAD_PARK_S (quota refills on Google's side)."""
        acct = getattr(nick, "project_uuid", "") or nick.name
        streak = self._quota_streak.get(acct, 0) + 1
        self._quota_streak[acct] = streak
        park_s = min(UPLOAD_PROBE_BASE_S * streak, UPLOAD_PROBE_MAX_S)
        until = time.time() + park_s
        self._quota_dead_until[acct] = until
        self._save_quota()
        _trace_emit("grpc.account.quota_dead", nick=nick.name,
                    account=acct[:8], streak=streak,
                    dead_s=round(until - time.time(), 1))

    def _recent_owned(self, window_s: float = 3 * 3600) -> Counter:
        """Per-nick count of media seen in the last `window_s` (cached 5s).

        All-time ownership counts are dominated by stale history and pile
        new media onto whichever nick is historically least-owned.
        """
        now = time.time()
        cache = getattr(self, "_recent_owned_cache", None)
        if cache and now - cache[0] < 5:
            return cache[1]
        cutoff = now - window_s
        cnt: Counter = Counter()
        for m, t in list(self._media_seen.items()):
            if t >= cutoff:
                o = self._media_owner.get(m)
                if o:
                    cnt[o] += 1
        self._recent_owned_cache = (now, cnt)
        return cnt

    def _anchor_nick_name(self, profile_id: str | None,
                           media_ids: list[str] | None,
                           exclude: set | None = None) -> str | None:
        now = time.time()
        if profile_id and profile_id in self._nicks:
            if (profile_id not in self._parked
                    and not self._upload_dead(profile_id, now)):
                return profile_id
        owners = [self._media_owner[m] for m in media_ids or []
                  if self._media_owner.get(m) in self._nicks]
        # `exclude` = nicks that just failed this request (e.g. THROTTLED on
        # a _no_cooldown nick, which never gets a cooldown stamp). Skip them
        # so the refs are copied elsewhere instead of re-pinning to them.
        # Upload-quota-dead accounts are parked for i2v/r2v as well.
        # Manually parked nicks (_parked.json) have dead creds on purpose —
        # never rebase media onto them either.
        for name, _ in Counter(owners).most_common():
            if (self._throttle_until.get(name, 0) <= now
                    and name not in self._parked
                    and not (exclude and name in exclude)
                    and not self._upload_dead(name, now)):
                return name
        # Every known owner is cooling off: anchor on the least-loaded
        # healthy nick so rebase copies the refs there instead of pinning
        # the op to a parked account. Rebase = UploadImage, so skip accounts
        # whose upload quota is exhausted (else 429 loop on a dead account).
        pool = [n for n in self._order
                if self._throttle_until.get(n, 0) <= now
                and n not in self._parked
                and not (exclude and n in exclude)]
        # Premium nicks (video_models map, paid quota) are only ever chosen
        # explicitly (profile pin / gated model key) — never as a generic
        # rebase target, or ordinary traffic drains their credits.
        regular = [n for n in pool
                   if not (getattr(self._nicks[n], "meta", None)
                           or {}).get("video_models")]
        if regular:
            pool = regular
        live = [n for n in pool if not self._upload_dead(n, now)]
        if live:
            recent = self._recent_owned()
            return min(live, key=lambda n: (self._inflight.get(n, 0),
                                            recent.get(n, 0)))
        # Rebase requires UploadImage, so never pick an upload-dead nick.
        # If all candidates in pool are cooling off or upload-dead, pick
        # any regular nick that can still upload:
        any_live = [n for n in self._order
                    if not (getattr(self._nicks[n], "meta", None) or {}).get("video_models")
                    and n not in self._parked
                    and not self._upload_dead(n, now)]
        if any_live:
            return min(any_live, key=lambda n: self._inflight.get(n, 0))
        return None

    def _copy_media_onto(self, media_id: str, dest_name: str) -> str:
        """Download an image from its owning nick and upload it on dest."""
        cache_key = f"{media_id}:{dest_name}"
        hit = self._rebase_cache.get(cache_key)
        if hit and self._media_owner.get(hit) == dest_name:
            return hit
        src_name = self._media_owner.get(media_id)
        src = self._nicks.get(src_name) if src_name else None
        dest = self._nicks.get(dest_name)
        if dest is None:
            raise FlowGrpcError(f"rebase dest nick '{dest_name}' is missing")
        if src is None:
            src = dest
        cached_jpg = UPLOAD_BYTES_DIR / f"{media_id}.jpg"
        content = b""
        if cached_jpg.exists():
            try:
                content = cached_jpg.read_bytes()
            except OSError:
                content = b""
        if not content:
            rec = self._get_media_sync(src, media_id)
            url = ((rec.get("data") or {}).get("image") or {}).get("fifeUrl") or ""
            if not url:
                raise FlowGrpcError(
                    f"rebase {media_id[:8]}: no image url on nick {src.name}")
            headers = {
                "Authorization": f"Bearer {src.mint_access_token()}",
                "User-Agent": UA_CFN,
            }
            r = requests.get(
                src._relay_url(url), headers=headers,
                proxies=None if src._relay_port() else src.proxies,
                timeout=60)
            r.raise_for_status()
            content = r.content
        if not content:
            raise FlowGrpcError(f"rebase {media_id[:8]}: empty download")
        uploaded = self._upload_sync(dest, content, dest.project_uuid)
        new_mid = uploaded.get("_mediaId") or ""
        if not new_mid:
            raise FlowGrpcError(f"rebase {media_id[:8]}: upload returned no id")
        self._rebase_cache[cache_key] = new_mid
        self._save_routes()
        _trace_emit("grpc.rebase", src=src.name, dest=dest_name,
                    from_id=media_id[:8], to_id=new_mid[:8],
                    bytes=len(content))
        return new_mid

    def _rebase_media_ids(self, media_ids: list[str] | None,
                          profile_id: str | None = None,
                          exclude: set | None = None) -> list[str]:
        """Make every ref belong to one live nick.

        Google media lives inside the account that created it. A scene that
        mixes nicks used to 400 MEDIA_PROFILE_MISMATCH. Copy the foreign
        images onto the majority (or pinned) nick and return the new ids.
        """
        mids = [m for m in (media_ids or []) if m]
        if not mids:
            return list(media_ids or [])
        if not self._nicks:
            self.reload()
        if (any(m not in self._media_owner for m in mids)
                and time.time() - self._last_disk_merge > 0.5):
            # The upload may have been served by the sibling agent — adopt
            # its routes before treating these refs as ownerless.
            self._last_disk_merge = time.time()
            self._locked_merge()
        for m in mids:
            if m not in self._media_owner:
                uo = getattr(self, "_uploaded_owner", {}).get(m)
                if uo:
                    self._media_owner[m] = uo
                    self._media_seen[m] = time.time()
        skip = set(exclude or ())
        for attempt in range(3):
            dest = self._anchor_nick_name(profile_id, mids, skip)
            if not dest:
                return mids
            owners = {m: self._media_owner.get(m) for m in mids}
            known = {o for o in owners.values() if o}
            if len(known) <= 1 and (not known or dest in known):
                return mids
            out, copied = [], 0
            try:
                for mid in mids:
                    owner = owners.get(mid)
                    if owner == dest or owner is None:
                        out.append(mid)
                        continue
                    out.append(self._copy_media_onto(mid, dest))
                    copied += 1
            except FlowGrpcError as e:
                # Dest account's upload quota is gone: park it (uploads +
                # i2v/r2v) and pick another dest within this same request.
                if ("resource has been exhausted" not in str(e).lower()
                        or profile_id or attempt == 2):
                    raise
                self._mark_upload_dead(self._nicks[dest])
                skip.add(dest)
                continue
            if copied:
                logger.info("Rebased %d refs onto %s (owners were %s)",
                            copied, dest, sorted(known))
            return out
        return mids

    def _disabled_owner_error(self,
                              media_ids: list[str] | None) -> dict | None:
        """If a referenced media is owned by a nick whose bundle is disabled
        or missing, fail fast instead of submitting a doomed call."""
        for mid in media_ids or []:
            owner = self._media_owner.get(mid)
            if owner and owner not in self._nicks:
                return {"status": 409,
                        "error_code": "media_owner_disabled",
                        "retryable": False,
                        "error": f"media {mid[:8]}… belongs to nick "
                                 f"'{owner}' which is currently disabled"}
        return None

    def _model_available(self, name: str, key: str) -> bool:
        nick = self._nicks.get(name)
        # "lite" is the default tier — every account serves it even when
        # its video_models map is empty (map only declares premium keys).
        return bool(nick) and \
            (key == "lite"
             or key in (nick.meta.get("video_models") or {}))

    def _model_is_key(self, model: str) -> bool:
        """True iff `model` is meant as a friendly key (lite, fast,
        quality, omni_8s…) → capability-gated to nicks that declare it.
        Raw Google model ids (legacy callers like Nova google/flow-veo
        passing veo_3_1_i2v_lite_low_priority verbatim) bypass the gate —
        gating them regressed ~1000 jobs to model_unavailable 400s.
        An unknown friendly-looking key still fails fast, not verbatim."""
        m = model.lower().strip()
        norm = re.sub(r"[\s\-]+", "_", m)
        if norm in _OMNI_12_ALIASES or norm in _OMNI_11_ALIASES:
            return False
        return not (m.startswith(("veo_", "abra_", "macaron_cookie_", "gem_", "narwhal"))
                    or "_t2v_" in m or "_i2v_" in m or "_r2v_" in m)

    async def _acquire(self, profile_id: str | None = None,
                       media_ids: list[str] | None = None,
                       exclude: set | None = None,
                       op: str = "", deadline: float | None = None,
                       model: str | None = None,
                       priority: str = "") -> GrpcNick:
        if not self._nicks:
            self.reload()
        if not self._nicks:
            raise FlowGrpcError("no nick bundles under nicks/")
        self._refresh_flags()
        self._loop = asyncio.get_running_loop()
        self._touch_media(media_ids)
        pinned = self._candidate(profile_id, media_ids)
        # Capability gate: a friendly video_model KEY can only run on nicks
        # whose captured video_models map contains it — anything else would
        # die at submit with PERMISSION_DENIED anyway. Raw model strings
        # bypass the gate entirely (verbatim passthrough, legacy behavior).
        gated = bool(model) and self._model_is_key(model)
        if gated and pinned and not self._model_available(pinned, model):
            raise FlowGrpcError(
                f"FLOWKIT_MODEL_UNAVAILABLE: {pinned} lacks "
                f"video_model={model}")
        order = self._order
        if gated and pinned is None:
            order = [n for n in order if self._model_available(n, model)]
            if not order:
                raise FlowGrpcError(
                    f"FLOWKIT_MODEL_UNAVAILABLE: no nick serves "
                    f"video_model={model}")
        elif pinned is None:
            # Premium nicks (video_models map) are reserved for friendly-key
            # traffic — unpinned ordinary work must not burn premium quota.
            # Ownership/profile pins still land there (media lives on the
            # account, the op must run on it).
            regular = [n for n in order
                       if not self._nicks[n].meta.get("video_models")]
            if regular:
                order = regular
        queued_at = 0.0
        waited_n = 0
        gone = _request_gone.get()
        # priority="high" waiters cut the submit queue: ordinary queued
        # requests keep yielding while any high-priority waiter exists.
        hi = priority == "high"
        while True:
            if gone is not None and await gone():
                # Caller already hung up — don't even take a slot.
                raise FlowGrpcError("FLOWKIT_CLIENT_GONE: caller disconnected")
            async with self._sched():
                name = pinned
                now = time.time()
                if name and op == "upload" and self._upload_dead(name, now):
                    name = None
                if name is None and not hi and self._waiters_hi:
                    # A high-priority waiter is queued behind us — leave the
                    # slot for it and keep waiting ourselves.
                    name = None
                elif name is None:
                    # least in-flight among non-throttled nicks; round-robin on
                    # ties. If every nick is cooling off, ignore the cooldown
                    # and pick the one that recovers soonest.
                    now = time.time()
                    pool = [n for n in order
                            if self._throttle_until.get(n, 0) <= now
                            and not (exclude and n in exclude)]
                    if not pool:
                        # Every nick is cooling off — pick whoever recovers
                        # soonest, but never a manually/auth-parked nick:
                        # those are parked on purpose (dead creds, admin
                        # hold) and probing them just spams mint calls.
                        pool = sorted(
                            [n for n in order
                             if not (exclude and n in exclude)
                             and n not in self._parked],
                            key=lambda n: self._throttle_until.get(n, 0))
                    if not pool:
                        raise FlowGrpcError(
                            "FLOWKIT_ALL_PARKED: every nick is parked")
                    if op == "upload":
                        # Prefer accounts whose upload quota hasn't died.
                        # When every account is dead, keep rotating probes
                        # into the longest-dead ones — quota refills
                        # gradually, so probing recovers the moment it does.
                        live = [
                            n for n in pool
                            if self._quota_dead_until.get(
                                getattr(self._nicks.get(n), "project_uuid",
                                        "") or n, 0) <= now]
                        if live:
                            pool = live
                        else:
                            pool = sorted(
                                pool,
                                key=lambda n: self._quota_dead_until.get(
                                    getattr(self._nicks.get(n),
                                            "project_uuid", "") or n, 0))[:3]
                    elif op == "video" and media_ids:
                        # i2v/r2v park on upload-exhausted accounts: streak
                        # persists past the probe window and clears only on a
                        # successful upload, so burned accounts stay out of
                        # ref-media traffic while t2v still flows. Media-bound
                        # ops pinned by ownership still land on the owner.
                        live = [
                            n for n in pool
                            if not self._quota_streak.get(
                                getattr(self._nicks.get(n), "project_uuid",
                                        "") or n, 0)]
                        if live:
                            pool = live
                    lo = min(self._inflight.get(n, 0) for n in pool)
                    if op in ("image", "upload"):
                        # Deprioritize accounts that have hit model/account
                        # quota so uploads and image gens don't get steered to
                        # a quota-exhausted free nick just because it owns 0
                        # recent media.
                        live_q = [
                            n for n in pool
                            if not self._quota_streak.get(n, 0)
                            and not self._quota_streak.get(
                                getattr(self._nicks.get(n), "project_uuid",
                                        "") or n, 0)]
                        if live_q:
                            pool = live_q
                        # Ownership-aware steering: every created media drags
                        # its future i2v/poll demand onto the owning nick, so
                        # biasing creation to the least-owned healthy nick
                        # rebalances fleet demand over time (plain round-robin
                        # let hot accounts keep compounding their share).
                        owned = self._recent_owned()
                        cands = sorted(
                            pool, key=lambda n: (self._inflight.get(n, 0),
                                                 owned.get(n, 0)))
                        for cand in cands:
                            if self._inflight.get(cand, 0) \
                                    < MAX_CONCURRENT_PER_NICK:
                                name = cand
                                break
                    else:
                        for i in range(len(order)):
                            cand = order[
                                (self._rr_idx + i) % len(order)]
                            if cand not in pool:
                                continue
                            if self._inflight.get(cand, 0) == lo:
                                name = cand
                                self._rr_idx = (self._rr_idx + i + 1) \
                                    % len(order)
                                break
                if name and self._inflight.get(name, 0) < MAX_CONCURRENT_PER_NICK:
                    self._inflight[name] = self._inflight.get(name, 0) + 1
                    break
            if not queued_at:
                queued_at = time.time()
                _trace_emit("grpc.queued", pinned=pinned,
                            waiters=self._waiters + 1, hi=hi,
                            inflight={n: self._inflight.get(n, 0)
                                      for n in self._order})
                self._waiters += 1
                if hi:
                    self._waiters_hi += 1
                if op in self._waiters_ops:
                    self._waiters_ops[op] += 1
            else:
                waited_n += 1
                if waited_n % 10 == 0:  # ~every 5s while queued
                    _trace_emit("grpc.sched.wait", pinned=pinned,
                                queued_s=round(time.time() - queued_at, 1),
                                inflight={n: self._inflight.get(n, 0)
                                          for n in self._order})
            await asyncio.sleep(0.5)
            if gone is not None and await gone():
                # Caller already hung up (Nova submit timeout) — creating the
                # op now would orphan it: Google still renders, nobody polls.
                if queued_at:
                    self._waiters = max(0, self._waiters - 1)
                    if hi:
                        self._waiters_hi = max(0, self._waiters_hi - 1)
                    if op in self._waiters_ops:
                        self._waiters_ops[op] = max(
                            0, self._waiters_ops[op] - 1)
                _trace_emit("grpc.client_gone", op=op, pinned=pinned,
                            queued_s=round(time.time() - queued_at, 1)
                            if queued_at else 0)
                raise FlowGrpcError("FLOWKIT_CLIENT_GONE: caller disconnected")
            if deadline and time.time() > deadline:
                # Give the slot-waiter its counter back, then surface a
                # retryable busy instead of holding Nova past its own
                # submit timeout and orphaning the op.
                if queued_at:
                    self._waiters = max(0, self._waiters - 1)
                    if hi:
                        self._waiters_hi = max(0, self._waiters_hi - 1)
                    if op in self._waiters_ops:
                        self._waiters_ops[op] = max(
                            0, self._waiters_ops[op] - 1)
                raise FlowGrpcError(
                    f"FLOWKIT_SUBMIT_BUSY: submit queue exceeded "
                    f"{FLOW_SUBMIT_RETRY_S}s budget")
        if queued_at:
            self._waiters = max(0, self._waiters - 1)
            if hi:
                self._waiters_hi = max(0, self._waiters_hi - 1)
            if op in self._waiters_ops:
                self._waiters_ops[op] = max(
                    0, self._waiters_ops[op] - 1)
        # per-nick submit pacing — serialise the wait on the nick's own lock
        lock = self._pace_locks.setdefault(name, asyncio.Lock())
        async with lock:
            delay = self._next_slot.get(name, 0.0) - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_slot[name] = time.time() + random.uniform(
                PACE_MIN_S, PACE_MAX_S)
        _trace_emit("grpc.acquire", nick=name, pinned=bool(pinned),
                    inflight=self._inflight.get(name, 0),
                    queued_s=round(time.time() - queued_at, 1) if queued_at else 0,
                    paced_s=round(delay, 2) if delay > 0 else 0)
        return self._nicks[name]

    def _acquire_fail(self, e: "FlowGrpcError", res: dict | None) -> dict:
        """Surface an _acquire() failure. ALL_PARKED/UPLOAD_DEAD carry
        distinct codes so Nova sees *why* nothing could be scheduled;
        everything else collapses to the retryable busy it already knows."""
        if res is not None:
            return res
        err = str(e)
        if "FLOWKIT_ALL_PARKED" in err:
            return {"status": 503, "error_code": "all_workers_parked",
                    "retryable": True, "retry_after_s": 60,
                    "error": err[:200]}
        if "FLOWKIT_UPLOAD_DEAD" in err:
            return {"status": 429, "error_code": "resource_exhausted",
                    "retryable": True, "retry_after_s": 60,
                    "error": err[:200]}
        if "FLOWKIT_MODEL_UNAVAILABLE" in err:
            # Caller asked for a premium model no healthy nick can serve —
            # retrying lands on the same non-capable pool.
            return {"status": 400, "error_code": "model_unavailable",
                    "retryable": False,
                    "error": err[:200]}
        return {"status": 429, "error_code": "submit_queue_busy",
                "retryable": True, "retry_after_s": 5,
                "error": err[:200]}

    def _facade_error(self, op: str, nick: "GrpcNick", e: "FlowGrpcError") -> dict:
        """Classify a failed gRPC call. PUBLIC_ERROR_USER_REQUESTS_THROTTLED
        parks the nick for a cooldown so the scheduler spreads load to other
        accounts, and returns a retryable 429-style payload."""
        err = str(e)
        _trace_emit("grpc.facade.error", op=op, nick=nick.name,
                    error=err[:300])
        low0 = err.lower()
        if "mint" in low0 and ("http" in low0 or "failed" in low0):
            # Token/captcha mint endpoint answered non-2xx (used to escape
            # as bare 500). Retryable so Nova lands on a healthy nick; a
            # 4xx means the credential itself is dead — park the nick so
            # the fleet stops spending mint calls on it until recaptured.
            m = re.search(r"http (\d+)", low0)
            sc = int(m.group(1)) if m else 0
            if sc in (400, 401, 403):
                self.set_park(nick.name, time.time() + 1800)
            return {"status": 429,
                    "error_code": "auth_mint",
                    "retryable": True,
                    "retry_after_s": 30,
                    "error": f"grpc[{nick.name}] {err}"}
        if "FLOWKIT_CLIENT_GONE" in err:
            # The HTTP client disconnected mid-queue; nobody is listening,
            # so retryable/no-op hints are moot — log only.
            return {"status": 499,
                    "error_code": "client_gone",
                    "retryable": False,
                    "error": f"grpc[{nick.name}] {err}"}
        if (e.code == grpc.StatusCode.UNAUTHENTICATED
                or "invalid authentication credentials" in low0
                or "expected oauth 2 access token" in low0
                or "labs session dead" in low0
                or "google session expired" in low0
                or "iframerpc" in low0):
            # Upstream rejected the ya29 — usually a Chrome session rotated
            # and the fresh JWE hasn't synced onto this box yet. Drop the
            # dead token so the next call re-mints off whatever cookie is
            # on disk, cool the nick so the scheduler fails over instead
            # of hammering it, and tell Nova to resubmit on a healthy one.
            nick._ya29 = None
            nick._ya29_exp = 0.0
            streak = self._unauth_streak.get(nick.name, 0) + 1
            self._unauth_streak[nick.name] = streak
            cool = 1800 if ("labs session dead" in low0 or "google session expired" in low0) else min(120 * streak, 1800)
            self._throttle_until[nick.name] = max(
                self._throttle_until.get(nick.name, 0), time.time() + cool)
            if streak >= 5:
                # Re-mint kept failing: the cookie/JWE itself is dead, not
                # a transient rotation. Park long so the fleet stops
                # burning customer requests on it; a successful auth or a
                # cookie refresh + reload unparks.
                self.set_park(nick.name, time.time() + 6 * 3600)
                _trace_emit("grpc.nick.unauth_park", nick=nick.name,
                            streak=streak)
            _trace_emit("grpc.nick.unauth_cool", nick=nick.name, op=op,
                        streak=streak, cooldown_s=cool)
            return {"status": 429,
                    "error_code": "auth_unauthenticated",
                    "retryable": True,
                    "retry_after_s": 15,
                    "error": f"grpc[{nick.name}] {err}"}
        if "FLOWKIT_SUBMIT_BUSY" in err:
            return {"status": 429, "error_code": "submit_queue_busy",
                    "retryable": True, "retry_after_s": 5,
                    "error": f"grpc[{nick.name}] {err}"}
        if "FLOWKIT_ALL_PARKED" in err:
            return {"status": 503, "error_code": "all_workers_parked",
                    "retryable": True, "retry_after_s": 60,
                    "error": f"grpc[{nick.name}] {err}"}
        if "FLOWKIT_UPLOAD_DEAD" in err:
            return {"status": 429, "error_code": "resource_exhausted",
                    "retryable": True, "retry_after_s": 60,
                    "error": f"grpc[{nick.name}] {err}"}
        if _POLICY_RE.search(err):
            # Google refused the prompt/image. Resending the same request
            # fails the same way, so tell Nova right away instead of the
            # retryable INVALID_ARGUMENT path below.
            flow_stats.bump(nick.name, policy=1)
            return {"status": 400,
                    "error_code": "content_policy_violation",
                    "retryable": False,
                    "error": f"grpc[{nick.name}] {err}"}
        if "INVALID_ARGUMENT" in err.upper() or "invalid argument" in err.lower():
            if op == "upscale":
                # Upscale args are template-fixed; INVALID_ARGUMENT means a
                # real rejection (bad model/resolution/source state), not a
                # transient throttle — retrying storms the same failure.
                return {"status": 400,
                        "error_code": "invalid_argument",
                        "retryable": False,
                        "error": f"grpc[{nick.name}] {err}"}
            return {"status": 429,
                    "error_code": "user_requests_throttled",
                    "retryable": True,
                    "retry_after_s": 8,
                    "error": f"grpc[{nick.name}] {err}"}
        if ("USER_QUOTA_REACHED" in err.upper()
                or "QUOTA_REACHED" in err.upper()
                or "PER_MODEL_DAILY_QUOTA" in err.upper()
                or "QUOTA EXCEEDED" in err.upper()
                or e.code == grpc.StatusCode.PERMISSION_DENIED
                or "the caller does not have permission" in low0
                or "model_access_denied" in low0):
            # Account credits are exhausted or account lacks permission for
            # the requested video tier. Cool the nick on an escalating window
            # and let the failover loop rebase any refs onto a healthy nick.
            streak = self._quota_streak.get(nick.name, 0) + 1
            self._quota_streak[nick.name] = streak
            acct = getattr(nick, "project_uuid", "") or nick.name
            self._quota_streak[acct] = max(self._quota_streak.get(acct, 0), streak)
            cooldown = min(300 * streak, 1800)
            self._throttle_until[nick.name] = max(
                self._throttle_until.get(nick.name, 0),
                time.time() + cooldown)
            logger.warning("Nick %s hit %s on %s — cooled %.0fs",
                           nick.name, e.code or "USER_QUOTA_REACHED", op, cooldown)
            _trace_emit("grpc.nick.quota_reached", nick=nick.name, op=op,
                        park_s=QUOTA_REACHED_PARK_S, detail=err[:120])
            flow_stats.bump(nick.name, quota_reached=1)
            return {"status": 429,
                    "error_code": "user_quota_reached",
                    "retryable": True,
                    "retry_after_s": 15,
                    "error": f"grpc[{nick.name}] {err}"}
        if "resource has been exhausted" in err.lower():
            # Per-account media quota (seen on UploadImage) — distinct from
            # the rolling USER_REQUESTS_THROTTLED rate window. Clone nicks
            # share the account's project, so dead-park the whole account —
            # uploads + i2v/r2v only: t2v/image gen keeps running there.
            self._mark_upload_dead(nick)
            flow_stats.bump(nick.name, throttled=1,
                            first_throttled_ts=datetime.now(
                                timezone.utc).isoformat())
            return {"status": 429,
                    "error_code": "resource_exhausted",
                    "retryable": True,
                    "retry_after_s": 60,
                    "error": f"grpc[{nick.name}] {err}"}
        if "THROTTLED" in err.upper() or e.code == grpc.StatusCode.RESOURCE_EXHAUSTED:
            # Rolling quota — a hot egress IP only needs a short nap; the
            # failover loop moves the request to a healthy nick meanwhile.
            # Streak is still counted for observability.
            streak = self._throttle_streak.get(nick.name, 0) + 1
            self._throttle_streak[nick.name] = streak
            # Android nicks never sit out: their quota is per-account
            # rolling, so a THROTTLED hit is retried on them freely.
            # _no_cooldown nicks (admin toggle) get the same treatment.
            if ((nick.meta.get("platform") or "iphone") == "android"
                    or nick.name in self._no_cooldown):
                cooldown = 0.0
            else:
                cooldown = random.uniform(10, 20)
            # Never shorten a longer existing cooldown (e.g. a parked nick
            # probed by a pinned call must stay parked). cooldown=0 leaves
            # the nick fully schedulable (android nicks above).
            if cooldown:
                self._throttle_until[nick.name] = max(
                    self._throttle_until.get(nick.name, 0),
                    time.time() + cooldown)
            _trace_emit("grpc.nick.cooldown", nick=nick.name,
                        cooldown_s=cooldown, streak=streak)
            flow_stats.bump(nick.name, throttled=1,
                            first_throttled_ts=datetime.now(
                                timezone.utc).isoformat())
            return {"status": 429,
                    "error_code": "user_requests_throttled",
                    "retryable": True,
                    "retry_after_s": min(int(cooldown), 60),
                    "error": f"grpc[{nick.name}] {err}"}
        if _transport_detail(err):
            # Proxy cut the stream. The worker should send the job again
            # soon. A generate is not repeated inside call() — repeating it
            # here would start a second render when the first one landed.
            return {"status": 429,
                    "error_code": "grpc_transport",
                    "retryable": True,
                    "retry_after_s": 5,
                    "error": f"grpc[{nick.name}] {err}"}
        low = err.lower()
        if "entity was not found" in low:
            # Start/reference media is invisible to this account. Ownership
            # may just be stale across agents (failover re-merges routes and
            # tries the true owner) or the media died server-side.
            return {"status": 502,
                    "error_code": "media_not_found",
                    "error": f"grpc[{nick.name}] {err}"}
        # Google returned no operation. Nova only silent-retries 429+retryable;
        # a bare 502 became generation_failed with nothing to poll.
        if (
            "internal error encountered" in low
            or "http2 header with status: 502" in low
            or "returned no media" in low
        ):
            return {"status": 429,
                    "error_code": "google_transient",
                    "retryable": True,
                    "retry_after_s": 5,
                    "error": f"grpc[{nick.name}] {err}"}
        return {"status": 502, "error": f"grpc[{nick.name}] {err}"}

    def _release(self, name: str) -> None:
        self._inflight[name] = max(0, self._inflight.get(name, 1) - 1)

    # -- sync cores ----------------------------------------------------------

    def _gen_images_sync(self, nick: GrpcNick, prompt: str,
                         project_id: str | None,
                         ref_media_ids: list[str] | None = None,
                         aspect_ratio: str = "",
                         model: str = "") -> dict:
        tpl = nick.gen_template
        foreign = False
        code = _image_model_codename(model)
        if code:
            # multi-model templates bắt từ iPhone mitm (2026-10-07, acc
            # lucas): nicks/_tpl/gen_img_<CODENAME>.bin — BELUGA /
            # HARBOR_SEAL / GEM_PIX_2. Bin giữ nguyên identity của account
            # capture (session, captcha, project 833779d7) — server không
            # check khớp auth, verbatim replay OK trên 7 acc; swap meta
            # blocks → INVALID_ARGUMENT upstream.
            alt = TPL_DIR / f"gen_img_{code}.bin"
            if alt.exists():
                tpl, foreign = alt.read_bytes(), True
            else:
                # codename chưa capture: patch field [2,6] trên template nick
                tpl = set_leaf_bytes(tpl, [2, 6], code.encode())
        buf = _patch_prompt_slots(tpl, prompt,
                                  [[2, 2], [2, 9]])
        av = _aspect_varint(aspect_ratio, _IMAGE_ASPECT_VALUE)
        if av is not None:
            buf = set_varint(buf, [2, 5], av)
        if ref_media_ids:
            # captured shape (landon, ingredient gen): f2.f3 repeated,
            # each {f1: media_uuid, f5: slot}. Slots are tray positions
            # descending from 2 — n=3 must emit 2,1,0 (NOT 3,2,1, which
            # upstream rejects with INTERNAL). Max tray size is 3.
            n = len(ref_media_ids)
            payloads = [encode([(1, 2, mid.encode()),
                                (5, 0, (2 if n >= 3 else n) - i)])
                        for i, mid in enumerate(ref_media_ids)]
            buf = replace_fields(buf, [2], 3, payloads)
        pid = project_id or (None if foreign else nick.project_uuid)
        if pid and buf.find(pid.encode()) < 0:
            # explicit project override, or seed bin copied from another
            # account (watson/michael/s08 carry friend's old 340f6e3a bin)
            # — stamp the serving account's project fields. Foreign _tpl
            # bins skip this: their captured project is the live one.
            buf = set_leaf_bytes(buf, [1], f"projects/{pid}".encode())
            buf = set_leaf_bytes(buf, [2, 8, 6], pid.encode())
            buf = set_leaf_bytes(buf, [4, 6], pid.encode())
        resp = nick.call(M_IMAGE, buf, timeout=120)
        media = _read_media_records(resp)
        if not media:
            raise FlowGrpcError(
                "BatchGenerateImages returned no media; "
                f"response_leaves={_scan_text_leaves(resp)}")
        missing = [m["media_id"] for m in media if not m["url"]]
        if missing:
            raise FlowGrpcError(
                f"BatchGenerateImages media missing URL: {missing}; "
                f"response_leaves={_scan_text_leaves(resp)}")
        for m in media:
            self._media_owner[m["media_id"]] = nick.name
        self._save_routes()
        return {"status": 200, "data": {"media": [
            {"name": m["media_id"],
             "image": {"generatedImage": {"mediaId": m["media_id"],
                                          "fifeUrl": m["url"]}}}
            for m in media]}}

    def _gen_video_sync(self, nick: GrpcNick, tpl: str, prompt: str,
                        project_id: str | None, aspect_ratio: str = "",
                        video_model: str = "", duration_s: int = 8,
                        **kw) -> dict:
        # Model pool is per-account: free tier serves veo_3_1_*_lite, paid
        # accounts run *_lite_low_priority. Wrong pool = PERMISSION_DENIED
        # or QUOTA_REACHED at submit. An explicit video_model key overrides —
        # resolved through the nick's captured video_models map (premium
        # tiers: fast/quality/omni_*), capability-gated at _acquire time,
        # or via built-in Omni 1.2 Flash (macaron_cookie_*) / Omni 1.1 (abra_*).
        model = ""
        is_omni = False
        if video_model:
            if video_model in (nick.meta.get("video_models") or {}):
                model = nick.video_model_for(tpl, video_model,
                                             aspect_ratio=aspect_ratio)
                if not model:
                    raise FlowGrpcError(
                        f"FLOWKIT_MODEL_UNAVAILABLE: video_model "
                        f"{video_model!r} has no {tpl} variant on "
                        f"{nick.name}")
                omni_res = _resolve_omni_model(model, tpl, duration_s)
                if omni_res is not None:
                    model, duration_s = omni_res
                    is_omni = True
            else:
                omni_res = _resolve_omni_model(video_model, tpl, duration_s)
                if omni_res is not None:
                    res_model, res_dur = omni_res
                    if res_model.startswith("macaron_cookie_") and nick.name not in OMNI_ALLOWED_NICKS:
                        raise FlowGrpcError(
                            f"FLOWKIT_MODEL_UNAVAILABLE: Omni 1.2 Flash is "
                            f"restricted to {OMNI_ALLOWED_NICKS} (got {nick.name})")
                    model, duration_s = res_model, res_dur
                    is_omni = True
                else:
                    # Raw Google model ids from legacy callers are dropped —
                    # free-tier nicks can only serve *_lite_low_priority and
                    # premium-only strings need the captured dict anyway. This
                    # restores pre-video_model behavior (field was ignored).
                    model = ""
        auto_tier = not model
        if not is_omni:
            # Reject a bad length before the captcha mint inside _build_video.
            duration_s = coerce_veo_duration(duration_s, tpl)
        buf = _build_video(tpl, prompt,
                           nick.meta_block(project_id, fresh_captcha=True),
                           aspect_ratio=aspect_ratio, **kw)
        if auto_tier:
            tier = (nick.meta.get("video_tier") or "low_priority")
            model = (f"veo_3_1_{tpl}_lite"
                     + ("_low_priority" if tier == "low_priority" else ""))
        if not is_omni:
            model = veo_model_for_duration(model, duration_s, tpl)
        buf = set_leaf_bytes(buf, _VIDEO_MODEL_FIELD[tpl], model.encode())
        _trace_emit("grpc.video.model", nick=nick.name, tpl=tpl,
                    model=model, key=video_model or None,
                    duration_s=duration_s)
        rpc_method = {"t2v": M_T2V, "i2v": M_I2V, "r2v": M_R2V}[tpl]
        try:
            resp = nick.call(rpc_method, buf, timeout=180)
        except FlowGrpcError as e:
            if (model.startswith("macaron_cookie_")
                    and (e.code in (grpc.StatusCode.NOT_FOUND,
                                    grpc.StatusCode.PERMISSION_DENIED)
                         or "not found" in str(e).lower())):
                alt_dur = _nearest_abra_duration(duration_s)
                alt_model = f"abra_{tpl}_{alt_dur}s"
                buf = _build_video(tpl, prompt,
                                   nick.meta_block(project_id, fresh_captcha=True),
                                   aspect_ratio=aspect_ratio, **kw)
                buf = set_leaf_bytes(buf, _VIDEO_MODEL_FIELD[tpl], alt_model.encode())
                _trace_emit("grpc.video.model_fallback", nick=nick.name, tpl=tpl,
                            from_model=model, to_model=alt_model, tier="abra_fallback")
                resp = nick.call(rpc_method, buf, timeout=180)
            # Tier discovery is for the captured 8s key only. A guessed 4s/6s
            # key must not flip video_tier on disk when Google denies it.
            elif (auto_tier and duration_s == 8
                    and (e.code == grpc.StatusCode.PERMISSION_DENIED
                         or "permission" in str(e).lower())):
                alt_tier = "free" if model.endswith("_low_priority") else "low_priority"
                alt_model = veo_model_for_duration(
                    f"veo_3_1_{tpl}_lite"
                    + ("_low_priority" if alt_tier == "low_priority" else ""),
                    duration_s, tpl)
                nick.meta["video_tier"] = alt_tier
                try:
                    meta_path = nick.dir / "meta.json"
                    disk_meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
                    disk_meta["video_tier"] = alt_tier
                    meta_path.write_text(json.dumps(disk_meta, indent=2))
                except Exception:
                    pass
                buf = _build_video(tpl, prompt,
                                   nick.meta_block(project_id, fresh_captcha=True),
                                   aspect_ratio=aspect_ratio, **kw)
                buf = set_leaf_bytes(buf, _VIDEO_MODEL_FIELD[tpl], alt_model.encode())
                _trace_emit("grpc.video.model_fallback", nick=nick.name, tpl=tpl,
                            from_model=model, to_model=alt_model, tier=alt_tier)
                resp = nick.call(rpc_method, buf, timeout=180)
            else:
                raise
        op = _read_operation(resp)
        if not op["op_id"]:
            raise FlowGrpcError(f"{tpl} returned no operation")
        self._op_owner[op["op_id"]] = nick.name
        self._op_project[op["op_id"]] = project_id or nick.project_uuid
        if op["media_id"]:
            self._op_media[op["op_id"]] = op["media_id"]
            self._media_owner[op["media_id"]] = nick.name
        start_uuid = kw.get("start_uuid") or ""
        if start_uuid:
            self._op_start_image[op["op_id"]] = start_uuid
        self._op_retry[op["op_id"]] = {
            "kind": tpl, "prompt": prompt, "aspect_ratio": aspect_ratio,
            "project_id": project_id or "",
            "start_uuid": kw.get("start_uuid") or "",
            "ref_uuids": list(kw.get("ref_uuids") or []),
            "video_model": video_model or "",
            "duration_s": duration_s,
            "attempts": 0}
        jid = _trace_job.get()
        if jid:
            self._op_req[op["op_id"]] = jid
        self._op_started[op["op_id"]] = time.time()
        self._save_routes()
        return {"status": 200, "data": {"operations": [{
            "operation": {"name": op["op_id"]},
            "status": "MEDIA_GENERATION_STATUS_PENDING"}]}}

    def _upscale_sync(self, nick: GrpcNick, media_id: str,
                      project_id: str | None, model: str,
                      aspect_ratio: str = "") -> dict:
        media_id = re.sub(r"_(?:4k_)?upsampled$", "", (media_id or "").strip())
        out_suffix = "_4k_upsampled" if "4k" in (model or "").lower() else "_upsampled"
        # Media record is the ground truth for orientation — callers pass
        # request-body aspect which may not match the source video (gateway
        # defaults to portrait). Video block [1,1,8,1,17]: 1=portrait,
        # 2=landscape, same enum as request f3.
        try:
            mresp = nick.call(M_GET_MEDIA, _build_get_media(media_id),
                              timeout=30)
            node = _leaf_at(decode(mresp), [1, 1, 8, 1])
            av = next((v for f, w, v in decode(node)
                       if f == 17 and w == 0), None)
            if av == 1:
                aspect_ratio = "portrait"
            elif av == 2:
                aspect_ratio = "landscape"
        except Exception:
            pass
        buf = _build_upscale(
            media_id, nick.meta_block(project_id, fresh_captcha=True), model,
            aspect_ratio)
        try:
            resp = nick.call(M_UPSAMPLE, buf, timeout=180)
        except FlowGrpcError as e:
            if "already exists" not in str(e).lower():
                raise
            # Idempotent retry: an upsample op for this source already
            # exists on the account, but its real op id is unknowable.
            # The output media id is deterministic (<src>_upsampled or
            # <src>_4k_upsampled), so hand back a pseudo-op whose poll
            # checks that media directly.
            out_media = f"{media_id}{out_suffix}"
            self._op_owner[out_media] = nick.name
            self._op_media[out_media] = out_media
            self._media_owner[out_media] = nick.name
            self._op_started.setdefault(out_media, time.time())
            jid = _trace_job.get()
            if jid:
                self._op_req.setdefault(out_media, jid)
            self._save_routes()
            _trace_emit("grpc.facade.end", op="upscale", nick=nick.name,
                        ops=[out_media[:12]], existing=True)
            return {"status": 200, "data": {"operations": [{
                "operation": {"name": out_media},
                "status": "MEDIA_GENERATION_STATUS_PENDING"}]}}
        op = _read_upsample(resp)
        if not op["op_id"]:
            raise FlowGrpcError("upsample returned no operation")
        # The submit response carries the output media id at f4.f1 and
        # f3.f4.f5 (<source>_upsampled for 1080p, <source>_4k_upsampled for 4K).
        out_media = op.get("media_id") or f"{media_id}{out_suffix}"
        self._op_owner[op["op_id"]] = nick.name
        self._op_project[op["op_id"]] = project_id or nick.project_uuid
        self._op_media[op["op_id"]] = out_media
        self._media_owner[out_media] = nick.name
        self._op_retry.setdefault(op["op_id"], {
            "kind": "upscale", "media_id": media_id, "model": model,
            "aspect_ratio": aspect_ratio,
            "project_id": project_id or "", "attempts": 0})
        jid = _trace_job.get()
        if jid:
            self._op_req.setdefault(op["op_id"], jid)
        self._op_started.setdefault(op["op_id"], time.time())
        self._save_routes()
        return {"status": 200, "data": {"operations": [{
            "operation": {"name": op["op_id"]},
            "status": "MEDIA_GENERATION_STATUS_PENDING"}]}}

    def _resolve_op(self, op_id: str) -> str:
        clean = op_id.removeprefix("operations/")
        seen = set()
        while clean in self._op_alias and clean not in seen:
            seen.add(clean)
            clean = self._op_alias[clean]
        return clean

    def _maybe_resubmit(self, clean: str, nick: GrpcNick,
                        elapsed: float, dead_hint: str = "") -> None:
        spec = self._op_retry.get(clean)
        if (not spec or spec.get("resubmitting")
                or spec.get("attempts", 0) >= 1):
            return
        if time.time() - spec.get("last_try", 0) < 120:
            return
        dead = dead_hint
        if (not dead
                and time.time() - self._op_checked.get(clean, 0) >= 60):
            self._op_checked[clean] = time.time()
            try:
                dead = (_read_check(nick.call(
                    M_CHECK, _build_check(clean), timeout=30))
                    .get("complaint") or "")
            except FlowGrpcError:
                pass
        if dead and "media not found" in dead.lower():
            # A pending op — especially upscale — reports "Media not
            # found." until its output media lands. That is noise, not
            # death: only hard complaints (removed/denied/policy/fail)
            # justify an instant resubmit; otherwise wait out
            # VIDEO_RESUBMIT_S starvation.
            dead = ""
        if not (dead or elapsed >= VIDEO_RESUBMIT_S):
            return
        with self._resubmit_lock:
            if spec.get("resubmitting"):
                return
            spec["resubmitting"] = True
            spec["last_try"] = time.time()
        _trace_emit("grpc.poll.resubmit", nick=nick.name, op=clean[:8],
                    complaint=dead or None, elapsed_s=int(elapsed),
                    job=self._op_req.get(clean) or _trace_job.get() or None)
        threading.Thread(target=self._resubmit_op,
                         args=(clean,), daemon=True).start()

    def _resubmit_op(self, old: str) -> None:
        spec = self._op_retry.get(old)
        if not spec:
            return
        loop = self._loop
        if loop is None or not loop.is_running():
            spec["resubmitting"] = False
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._resubmit_async(old, spec), loop).result(timeout=300)
        except Exception as e:
            spec["resubmitting"] = False
            _trace_emit("grpc.poll.resubmit", op=old[:8],
                        error=str(e)[:160])

    async def _resubmit_async(self, old: str, spec: dict) -> None:
        kind = spec["kind"]
        vmodel = spec.get("video_model", "")
        dur = spec.get("duration_s") or 8
        # A nick whose render just starved does not get the retry — pin the
        # resubmit to a different healthy account. Media-bearing kinds also
        # need upload quota alive for the rebase copy onto the new nick.
        old_owner = self._op_owner.get(old)
        pin = None
        if old_owner:
            media_bound = kind in ("i2v", "r2v")
            now = time.time()
            pool = [
                n for n in self._order
                if n != old_owner and n not in self._parked
                and self._throttle_until.get(n, 0) <= now
                and (not vmodel or self._model_available(n, vmodel))
                and (not media_bound or not self._quota_streak.get(
                    getattr(self._nicks[n], "project_uuid", "") or n))]
            if pool:
                start = self._rr_idx
                self._rr_idx = (start + 1) % max(len(self._order), 1)
                order_rank = {
                    self._order[(start + i) % len(self._order)]: i
                    for i in range(len(self._order))
                }
                pin = min(pool, key=lambda n: (
                    self._inflight.get(n, 0), order_rank.get(n, 0)))
        if kind == "r2v":
            r = await self.generate_video_from_references(
                list(spec.get("ref_uuids") or []), spec.get("prompt", ""),
                project_id=spec.get("project_id") or "",
                aspect_ratio=spec.get("aspect_ratio") or "",
                profile_id=pin, video_model=vmodel, duration_s=dur)
        elif kind == "upscale":
            mid = spec["media_id"]
            nick = await self._acquire(
                None, [mid], op="video",
                deadline=time.time() + FLOW_SUBMIT_RETRY_S)
            try:
                r = await asyncio.to_thread(
                    self._upscale_sync, nick, mid,
                    spec.get("project_id") or None, spec["model"],
                    spec.get("aspect_ratio", ""))
            finally:
                self._release(nick.name)
        else:
            r = await self.generate_video(
                start_image_media_id=spec.get("start_uuid") or None,
                prompt=spec.get("prompt", ""),
                project_id=spec.get("project_id") or "",
                aspect_ratio=spec.get("aspect_ratio") or "",
                profile_id=pin, video_model=vmodel, duration_s=dur)
        ops = [(o.get("operation") or {}).get("name")
               for o in (r.get("data") or {}).get("operations", [])]
        new_id = (ops[0] or "").removeprefix("operations/") if ops else ""
        if not new_id or r.get("status") != 200:
            raise FlowGrpcError(
                f"resubmit failed: {str(r.get('error'))[:120]}")
        if new_id == old:
            # Flow dedup returned the very op we meant to replace (upscale
            # on the same media). Count the attempt so we don't retry in an
            # infinite loop every 120s.
            spec["attempts"] = spec.get("attempts", 0) + 1
            spec["resubmitting"] = False
            _trace_emit("grpc.poll.resubmit", op=old[:8], kind=kind,
                        dedup=True, job=self._op_req.get(old) or None)
            self._save_routes()
            return
        self._op_alias[old] = new_id
        if self._op_req.get(old):
            self._op_req[new_id] = self._op_req[old]
        # The resubmit recorded a fresh spec with attempts=0 — carry the
        # budget so a second dead render does not loop forever.
        nxt = self._op_retry.get(new_id)
        if nxt is not None:
            nxt["attempts"] = spec.get("attempts", 0) + 1
        _trace_emit("grpc.poll.resubmit", op=old[:8], new_op=new_id[:8],
                    kind=kind, pin=pin,
                    job=self._op_req.get(old) or None)
        self._save_routes()

    def _poll_op_sync(self, nick: GrpcNick, op_id: str) -> dict:
        asked = op_id.removeprefix("operations/")
        clean = self._resolve_op(op_id)
        now = time.time()
        hit = self._poll_cache.get(clean)
        if hit and now < hit[0]:
            entry = copy.deepcopy(hit[1])
            entry["operation"]["name"] = asked
            return entry
        entry = self._poll_op_live(nick, clean)
        entry["operation"]["name"] = asked  # caller keeps its op id
        if entry.get("status") in ("MEDIA_GENERATION_STATUS_SUCCESSFUL",
                                   "MEDIA_GENERATION_STATUS_FAILED"):
            ttl = POLL_DONE_TTL_S
        else:
            ttl = _poll_ttl(now - float(self._op_started.get(clean) or now))
        if len(self._poll_cache) > 5000:
            for k, (exp, _) in list(self._poll_cache.items()):
                if exp <= now:
                    self._poll_cache.pop(k, None)
        self._poll_cache[clean] = (time.time() + ttl, copy.deepcopy(entry))
        return entry

    def _poll_op_live(self, nick: GrpcNick, op_id: str) -> dict:
        """BatchGetMedia the output media id until it grows a `/video/` URL.

        The iOS app does not use BatchCheck or GetProjectContents to notice
        a finished clip. The submit media id returns a poster first, then
        the same id gains `/video/` about a minute later. Image-only is
        still PENDING. Project listing is not on this path — it held the
        nick for up to 180s per poll and Nova timed out first.
        """
        clean = op_id.removeprefix("operations/")
        job_kw = ({"job": j} if (j := self._op_req.get(clean)
                                 or _trace_job.get()) else {})
        entry = {"operation": {"name": clean},
                 "status": "MEDIA_GENERATION_STATUS_PENDING"}
        self._op_started.setdefault(clean, time.time())
        stored = self._op_media.get(clean, "")
        start_img = self._op_start_image.get(clean, "")
        ids: list[str] = []
        for mid in (stored, clean):
            if not mid or mid in ids or not MEDIA_ID_RE.match(mid.encode()):
                continue
            # Skip the input frame only when we already have a different
            # output id. The clip url lands on the output id, which can
            # briefly look like an image.
            if mid == start_img and stored and stored != mid:
                continue
            ids.append(mid)

        urls: dict = {}
        missing: set[str] = set()
        for mid in ids:
            try:
                resp = nick.call(M_GET_MEDIA, _build_get_media(mid))
            except FlowGrpcError:
                continue
            urls.update(_read_media_urls(resp))
            missing |= _read_media_missing(resp)

        media_id = ""
        video_url = ""
        for mid in ids:
            rec = urls.get(mid) or {}
            if rec.get("video"):
                media_id, video_url = mid, rec["video"]
                break
            if rec.get("image") and not media_id:
                media_id = mid

        if media_id:
            self._media_owner.setdefault(media_id, nick.name)
            self._op_media.setdefault(clean, media_id)
            entry["operation"]["metadata"] = {"video": {"mediaId": media_id}}
            if video_url:
                entry["operation"]["metadata"]["video"]["fifeUrl"] = video_url
                entry["status"] = "MEDIA_GENERATION_STATUS_SUCCESSFUL"
                elapsed = time.time() - float(self._op_started.get(clean) or time.time())
                _trace_emit("grpc.poll", nick=nick.name, op=clean[:8],
                            status="SUCCESSFUL", has_video=True,
                            elapsed_s=int(elapsed), **job_kw)
                return entry

        now = time.time()
        elapsed = now - float(self._op_started.get(clean) or now)
        if stored and stored in missing:
            if (self._op_owner.get(clean) == nick.name
                    and elapsed >= REMOVED_MIN_AGE_S):
                first = self._op_missing_since.setdefault(clean, now)
                if now - first >= REMOVED_CONFIRM_S:
                    self._op_missing_since.pop(clean, None)
                    spec = self._op_retry.get(clean)
                    if (spec and spec.get("attempts", 0) < 1
                            and not spec.get("resubmitting")):
                        # Google dropped the render mid-flight — auto-resubmit
                        # once and stay PENDING so the caller never sees the
                        # transient rejection as a failed job.
                        self._maybe_resubmit(clean, nick, elapsed,
                                             dead_hint="media_removed")
                        return entry
                    _trace_emit("grpc.poll.rejected", nick=nick.name,
                                op=clean[:8], media=stored[:8],
                                elapsed_s=int(elapsed), **job_kw)
                    flow_stats.bump(nick.name, rejected=1)
                    return {"operation": {"name": clean},
                            "status": "MEDIA_GENERATION_STATUS_FAILED",
                            "error_code": "upstream_rejected",
                            "error": "Google removed the output during "
                                     f"rendering (media not found: {stored})"}
        else:
            self._op_missing_since.pop(clean, None)
        _trace_emit("grpc.poll", nick=nick.name, op=clean[:8],
                    status="PENDING", has_video=False, has_media=bool(media_id),
                    elapsed_s=int(elapsed), **job_kw)
        if elapsed >= VIDEO_STUCK_TIMEOUT:
            spec = self._op_retry.get(clean)
            if (spec and spec.get("attempts", 0) >= 1
                    and not spec.get("resubmitting")
                    and elapsed >= VIDEO_RESUBMIT_S):
                _trace_emit("grpc.poll.rejected", nick=nick.name,
                            op=clean[:8], media=(stored or clean)[:8],
                            elapsed_s=int(elapsed), **job_kw)
                flow_stats.bump(nick.name, rejected=1)
                return {"operation": {"name": clean},
                        "status": "MEDIA_GENERATION_STATUS_FAILED",
                        "error_code": "upstream_rejected",
                        "error": f"Resubmitted video stalled after {int(elapsed)}s"}
            # Stay PENDING. Returning FAILED made Nova mark the customer job
            # generation_failed (busy copy, no credits) even though retryable.
            _trace_emit("grpc.poll.stuck", nick=nick.name, op=clean[:8],
                        elapsed_s=int(elapsed), **job_kw)
            entry["complaint"] = f"still rendering after {int(elapsed)}s"
            self._maybe_resubmit(clean, nick, elapsed)
        return entry

    def _get_media_sync(self, nick: GrpcNick, media_id: str) -> dict:
        urls = _read_media_urls(
            nick.call(M_GET_MEDIA, _build_get_media(media_id), timeout=45))
        rec = urls.get(media_id) or {}
        if not rec:
            _trace_emit("grpc.media", nick=nick.name, id=media_id[:8],
                        status=404, has_video=False, has_image=False)
            return {"status": 404, "error": f"No urls for media {media_id}"}
        data = {}
        if rec.get("video"):
            data["video"] = {"fifeUrl": rec["video"]}
        if rec.get("image"):
            data["image"] = {"fifeUrl": rec["image"]}
        _trace_emit("grpc.media", nick=nick.name, id=media_id[:8],
                    status=200, has_video=bool(rec.get("video")),
                    has_image=bool(rec.get("image")))
        return {"status": 200, "data": data}

    def _upload_sync(self, nick: GrpcNick, image: bytes,
                     project_id: str | None,
                     captcha_force: bool = False) -> dict:
        jpeg = _ensure_jpeg(image)
        _trace_emit("grpc.upload", nick=nick.name, bytes=len(jpeg),
                    jpeg=jpeg[:3] == b"\xff\xd8\xff",
                    converted=len(jpeg) != len(image),
                    captcha_force=captcha_force)
        buf = _build_upload(
            nick.meta_block(project_id, fresh_captcha=True,
                            captcha_force=captcha_force,
                            captcha_action="IMAGE_UPLOAD"), jpeg)
        resp = nick.call(M_UPLOAD, buf, timeout=180)
        # response f1: {f1 media_uuid, f2 project_uuid, ...} — take f1.f1,
        # not just the first uuid in the block (project uuid sorts earlier).
        media_id = ""
        for f, w, v in decode(resp):
            if f == 1 and w == 2:
                for sf, sw, sv in decode(v):
                    if sf == 1 and sw == 2 and UUID_RE.match(sv):
                        media_id = sv.decode()
                        break
                if media_id:
                    break
        if not media_id:
            raise FlowGrpcError("UploadImage returned no media id")
        acct = getattr(nick, "project_uuid", "") or nick.name
        if acct in self._quota_dead_until or self._quota_streak.get(acct):
            self._quota_dead_until.pop(acct, None)
            self._quota_streak.pop(acct, None)
            self._save_quota()
            _trace_emit("grpc.account.quota_ok", nick=nick.name,
                        account=acct[:8])
        self._media_owner[media_id] = nick.name
        self._save_routes()
        try:
            UPLOAD_BYTES_DIR.mkdir(parents=True, exist_ok=True)
            (UPLOAD_BYTES_DIR / f"{media_id}.jpg").write_bytes(jpeg)
            files = list(UPLOAD_BYTES_DIR.glob("*.jpg"))
            if len(files) > 300:
                cutoff = time.time() - 7200
                for fp in files:
                    try:
                        if fp.stat().st_mtime < cutoff:
                            fp.unlink(missing_ok=True)
                    except OSError:
                        pass
        except OSError:
            pass
        self._uploaded[hashlib.sha256(image).hexdigest()] = {
            "media_id": media_id, "nick": nick.name, "ts": time.time()}
        if hasattr(self, "_uploaded_owner"):
            self._uploaded_owner[media_id] = nick.name
        self._save_uploaded()
        return {"status": 200, "data": {"media": {"name": media_id}},
                "_mediaId": media_id}

    # -- async facade (same shapes as FlowClient) -----------------------------

    async def generate_images(self, prompt: str, project_id: str = "",
                              aspect_ratio: str = "", profile_id=None,
                              character_media_ids: list[str] | None = None,
                              image_model: str = "",
                              **_kw) -> dict:
        if character_media_ids and len(character_media_ids) > 3:
            # BatchGenerateImages ref tray has 3 slots (f5 descends 2..0).
            # n>3 cannot be encoded — fail fast, not retryable.
            return {"status": 400, "error_code": "invalid_argument",
                    "retryable": False,
                    "error": "image gen supports at most 3 "
                             "character_media_ids (ref tray has 3 slots)"}
        try:
            character_media_ids = await asyncio.to_thread(
                self._rebase_media_ids,
                list(character_media_ids or []), profile_id)
        except (FlowGrpcError, requests.RequestException) as e:
            _trace_emit("grpc.facade.error", op="generate_images",
                        stage="rebase", error=str(e)[:300])
            return {"status": 502, "error_code": "media_rebase_failed",
                    "retryable": True, "error": f"rebase refs: {e}"}
        ids = list(character_media_ids or [])
        disabled = self._disabled_owner_error(ids)
        if disabled:
            return disabled
        res: dict | None = None
        # Failover loop: a THROTTLED nick cools off and the request retries
        # on the next healthy nick inside this same call — quota is rolling,
        # so whichever account can serve the request should serve it. With
        # cd-off nicks the whole pool reopens instantly, so loop until the
        # submit budget ends rather than giving up after one try per nick.
        tried: set = set()
        nf = 0
        bad_ia = 0
        deadline = time.time() + FLOW_SUBMIT_RETRY_S
        limit = 1 if profile_id else max(1, len(self._order))
        while True:
            if len(tried) >= limit:
                if time.time() >= deadline:
                    return res
                tried.clear()
                await asyncio.sleep(1.0)
            try:
                nick = await self._acquire(profile_id, ids, exclude=tried,
                                           op="image", deadline=deadline)
            except FlowGrpcError as e:
                return self._acquire_fail(e, res)
            _trace_emit("grpc.facade", op="generate_images", nick=nick.name,
                        refs=len(ids),
                        ref_ids={m[:8]: self._media_owner.get(m, "?")
                                 for m in ids} or None,
                        aspect=aspect_ratio or "default",
                        plen=len(prompt or ""),
                        project=project_id or nick.project_uuid)
            try:
                r = await asyncio.to_thread(
                    self._gen_images_sync, nick, prompt, project_id or None,
                    list(ids), aspect_ratio, image_model or "")
                media = [m["name"]
                         for m in (r.get("data") or {}).get("media", [])]
                _trace_emit("grpc.facade.end", op="generate_images",
                            nick=nick.name, media=media)
                flow_stats.bump(nick.name, images=len(media) or 1)
                self._throttle_streak.pop(nick.name, None)
                self._unauth_streak.pop(nick.name, None)
                self._quota_streak.pop(nick.name, None)
                return r
            except FlowGrpcError as e:
                res = self._facade_error("generate_images", nick, e)
                code = res.get("error_code")
                if code == "media_not_found":
                    # Ownership may live only in the sibling agent's routes
                    # file — merge before deciding the media is gone. The
                    # parse is tens of MB — keep it off the event loop.
                    self._last_disk_merge = time.time()
                    await asyncio.to_thread(self._locked_merge)
                    if nf >= 4 or (ids and all(
                            self._media_owner.get(m) == nick.name
                            for m in ids)):
                        # The owning nick itself cannot see the media —
                        # gone server-side, no failover will resurrect it.
                        return res
                    nf += 1
                elif code not in ("user_requests_throttled",
                                  "grpc_transport", "submit_queue_busy",
                                  "auth_mint", "auth_unauthenticated",
                                  "user_quota_reached"):
                    return res
                if code == "user_requests_throttled" \
                        and ("invalid argument" in str(e).lower()
                             or "invalid_argument" in str(e).lower()):
                    bad_ia += 1
                    if bad_ia >= 2:
                        # Same bare INVALID_ARGUMENT on 2+ different
                        # accounts — the request itself is malformed; no
                        # nick can fix it. Surface a real 400 so Nova
                        # stops retrying instead of storming the fleet.
                        return {"status": 400, "error_code": "invalid_argument",
                                "retryable": False,
                                "error": res.get("error", "")}
                tried.add(nick.name)
                _trace_emit("grpc.failover", op="generate_images",
                            nick=nick.name)
                try:
                    ids = await asyncio.to_thread(
                        self._rebase_media_ids, list(ids), None, set(tried))
                except (FlowGrpcError, requests.RequestException):
                    return res
            finally:
                self._release(nick.name)
        return res

    async def generate_video(self, start_image_media_id: str | None = None,
                             prompt: str = "", project_id: str = "",
                             aspect_ratio: str = "", profile_id=None,
                             video_model: str = "", priority: str = "",
                             duration_s: int = 8, **_kw) -> dict:
        tpl_pre = "i2v" if start_image_media_id else "t2v"
        try:
            omni_pre = _resolve_omni_model(video_model, tpl_pre, duration_s)
            if omni_pre is not None:
                res_m, duration_s = omni_pre
                if res_m.startswith("macaron_cookie_"):
                    if profile_id and profile_id not in OMNI_ALLOWED_NICKS:
                        return {"status": 400, "retryable": False,
                                "error_code": "model_unavailable",
                                "error": f"Omni 1.2 Flash is restricted to {OMNI_ALLOWED_NICKS} (got {profile_id!r})"}
                    profile_id = OMNI_ALLOWED_NICKS[0]
            else:
                duration_s = coerce_veo_duration(duration_s, tpl_pre)
        except FlowGrpcError as e:
            return {"status": 400, "retryable": False,
                    "error_code": "invalid_argument", "error": str(e)}
        if start_image_media_id:
            anchor = profile_id
            gated = bool(video_model) and self._model_is_key(video_model)
            owner = self._media_owner.get(start_image_media_id)
            now = time.time()
            if not profile_id and gated:
                # The start image's owner may lack this model key (e.g. only
                # premium nicks serve `lite`) — copy the image onto a capable
                # nick rather than failing with model_unavailable.
                if owner and not self._model_available(owner, video_model):
                    capable = [n for n in self._order
                               if self._model_available(n, video_model)
                               and self._throttle_until.get(n, 0) <= now]
                    if capable:
                        anchor = min(capable,
                                     key=lambda n: self._inflight.get(n, 0))
            elif (not profile_id and owner in self._nicks
                  and self._nicks[owner].meta.get("video_models")):
                # Ordinary model but the image sits on a premium nick: move
                # it to a regular nick so paid premium quota is not drained.
                regular = [n for n in self._order
                           if not self._nicks[n].meta.get("video_models")
                           and self._throttle_until.get(n, 0) <= now]
                if regular:
                    anchor = min(regular,
                                 key=lambda n: self._inflight.get(n, 0))
            try:
                start_image_media_id = (await asyncio.to_thread(
                    self._rebase_media_ids,
                    [start_image_media_id], anchor))[0]
            except (FlowGrpcError, requests.RequestException) as e:
                _trace_emit("grpc.facade.error", op="generate_video",
                            stage="rebase", error=str(e)[:300])
                return {"status": 502, "error_code": "media_rebase_failed",
                        "retryable": True, "error": f"rebase start image: {e}"}
        media = [start_image_media_id] if start_image_media_id else []
        disabled = self._disabled_owner_error(media)
        if disabled:
            return disabled
        res: dict | None = None
        tried: set = set()
        nf = 0
        bad_ia = 0
        deadline = time.time() + FLOW_SUBMIT_RETRY_S
        limit = 1 if profile_id else max(1, len(self._order))
        while True:
            if len(tried) >= limit:
                if time.time() >= deadline:
                    return res
                tried.clear()
                await asyncio.sleep(1.0)
            try:
                nick = await self._acquire(profile_id, media, exclude=tried,
                                           op="video", deadline=deadline,
                                           model=video_model or None,
                                           priority=priority)
            except FlowGrpcError as e:
                return self._acquire_fail(e, res)
            kind = "i2v" if media else "t2v"
            _trace_emit("grpc.facade", op=kind, nick=nick.name,
                        start=media[0][:8] if media else None,
                        start_owner=self._media_owner.get(
                            media[0], "?") if media else None,
                        aspect=aspect_ratio or "default",
                        vmodel=video_model or None,
                        duration_s=duration_s,
                        project=project_id or nick.project_uuid)
            try:
                if media:
                    r = await asyncio.to_thread(
                        self._gen_video_sync, nick, "i2v", prompt,
                        project_id or None, aspect_ratio=aspect_ratio,
                        video_model=video_model, duration_s=duration_s,
                        start_uuid=media[0])
                else:
                    r = await asyncio.to_thread(
                        self._gen_video_sync, nick, "t2v", prompt,
                        project_id or None, aspect_ratio=aspect_ratio,
                        video_model=video_model, duration_s=duration_s)
                ops = [(o.get("operation") or {}).get("name")
                       for o in (r.get("data") or {}).get("operations", [])]
                _trace_emit("grpc.facade.end", op=kind, nick=nick.name,
                            ops=ops)
                flow_stats.bump(nick.name, videos=len(ops) or 1)
                self._throttle_streak.pop(nick.name, None)
                self._unauth_streak.pop(nick.name, None)
                return r
            except FlowGrpcError as e:
                res = self._facade_error(kind, nick, e)
                code = res.get("error_code")
                if code == "media_not_found":
                    self._last_disk_merge = time.time()
                    await asyncio.to_thread(self._locked_merge)
                    if nf >= 4 or (media and all(
                            self._media_owner.get(m) == nick.name
                            for m in media)):
                        # Owning nick cannot see it either — media is gone
                        # server-side, no failover will resurrect it.
                        return res
                    nf += 1
                elif code not in ("user_requests_throttled",
                                  "grpc_transport", "submit_queue_busy",
                                  "auth_mint", "auth_unauthenticated",
                                  "user_quota_reached"):
                    return res
                if code == "user_requests_throttled" \
                        and ("invalid argument" in str(e).lower()
                             or "invalid_argument" in str(e).lower()):
                    bad_ia += 1
                    if bad_ia >= 2:
                        return {"status": 400, "error_code": "invalid_argument",
                                "retryable": False,
                                "error": res.get("error", "")}
                tried.add(nick.name)
                _trace_emit("grpc.failover", op=kind, nick=nick.name)
                try:
                    media = await asyncio.to_thread(
                        self._rebase_media_ids, list(media), None, set(tried))
                except (FlowGrpcError, requests.RequestException):
                    return res
            finally:
                self._release(nick.name)
        return res

    async def generate_video_from_references(
            self, reference_media_ids: list[str], prompt: str,
            project_id: str = "", aspect_ratio: str = "", profile_id=None,
            video_model: str = "", priority: str = "",
            duration_s: int = 8, **_kw) -> dict:
        try:
            omni_pre = _resolve_omni_model(video_model, "r2v", duration_s)
            if omni_pre is not None:
                res_m, duration_s = omni_pre
                if res_m.startswith("macaron_cookie_"):
                    if profile_id and profile_id not in OMNI_ALLOWED_NICKS:
                        return {"status": 400, "retryable": False,
                                "error_code": "model_unavailable",
                                "error": f"Omni 1.2 Flash is restricted to {OMNI_ALLOWED_NICKS} (got {profile_id!r})"}
                    profile_id = OMNI_ALLOWED_NICKS[0]
            else:
                duration_s = coerce_veo_duration(duration_s, "r2v")
        except FlowGrpcError as e:
            return {"status": 400, "retryable": False,
                    "error_code": "invalid_argument", "error": str(e)}
        if not reference_media_ids:
            return {"status": 400, "error": "no reference media_ids"}
        try:
            reference_media_ids = await asyncio.to_thread(
                self._rebase_media_ids,
                list(reference_media_ids), profile_id)
        except (FlowGrpcError, requests.RequestException) as e:
            _trace_emit("grpc.facade.error", op="r2v",
                        stage="rebase", error=str(e)[:300])
            return {"status": 502, "error_code": "media_rebase_failed",
                    "retryable": True, "error": f"rebase refs: {e}"}
        disabled = self._disabled_owner_error(reference_media_ids)
        if disabled:
            return disabled
        res: dict | None = None
        refs = list(reference_media_ids)
        tried: set = set()
        nf = 0
        bad_ia = 0
        deadline = time.time() + FLOW_SUBMIT_RETRY_S
        limit = 1 if profile_id else max(1, len(self._order))
        while True:
            if len(tried) >= limit:
                if time.time() >= deadline:
                    return res
                tried.clear()
                await asyncio.sleep(1.0)
            try:
                nick = await self._acquire(profile_id, refs, exclude=tried,
                                           op="video", deadline=deadline,
                                           model=video_model or None,
                                           priority=priority)
            except FlowGrpcError as e:
                return self._acquire_fail(e, res)
            _trace_emit("grpc.facade", op="r2v", nick=nick.name,
                        refs=len(refs),
                        ref_ids={m[:8]: self._media_owner.get(m, "?")
                                 for m in refs},
                        aspect=aspect_ratio or "default",
                        vmodel=video_model or None,
                        duration_s=duration_s,
                        project=project_id or nick.project_uuid)
            try:
                r = await asyncio.to_thread(
                    self._gen_video_sync, nick, "r2v", prompt,
                    project_id or None, aspect_ratio=aspect_ratio,
                    video_model=video_model, duration_s=duration_s,
                    ref_uuids=list(refs))
                ops = [(o.get("operation") or {}).get("name")
                       for o in (r.get("data") or {}).get("operations", [])]
                _trace_emit("grpc.facade.end", op="r2v", nick=nick.name,
                            ops=ops)
                flow_stats.bump(nick.name, videos=len(ops) or 1)
                self._throttle_streak.pop(nick.name, None)
                self._unauth_streak.pop(nick.name, None)
                return r
            except FlowGrpcError as e:
                res = self._facade_error("r2v", nick, e)
                code = res.get("error_code")
                if code == "media_not_found":
                    self._last_disk_merge = time.time()
                    await asyncio.to_thread(self._locked_merge)
                    if nf >= 4 or (refs and all(
                            self._media_owner.get(m) == nick.name
                            for m in refs)):
                        # Owning nick cannot see it either — media is gone
                        # server-side, no failover will resurrect it.
                        return res
                    nf += 1
                elif code not in ("user_requests_throttled",
                                  "grpc_transport", "submit_queue_busy",
                                  "auth_mint", "auth_unauthenticated",
                                  "user_quota_reached"):
                    return res
                if code == "user_requests_throttled" \
                        and ("invalid argument" in str(e).lower()
                             or "invalid_argument" in str(e).lower()):
                    bad_ia += 1
                    if bad_ia >= 2:
                        return {"status": 400, "error_code": "invalid_argument",
                                "retryable": False,
                                "error": res.get("error", "")}
                tried.add(nick.name)
                _trace_emit("grpc.failover", op="r2v", nick=nick.name)
                try:
                    refs = await asyncio.to_thread(
                        self._rebase_media_ids, list(refs), None, set(tried))
                except (FlowGrpcError, requests.RequestException):
                    return res
            finally:
                self._release(nick.name)
        return res

    async def upscale_video(self, media_id: str, scene_id: str = "",
                            aspect_ratio: str = "",
                            resolution: str = "VIDEO_RESOLUTION_1080P",
                            profile_id=None, **_kw) -> dict:
        """Video upscale (720p→1080p/4K) — captured iOS RPC 2026-09-29."""
        if not media_id:
            return {"status": 400, "error": "no media_id"}
        raw_media_id = media_id.strip()
        media_id = re.sub(r"_(?:4k_)?upsampled$", "", raw_media_id)
        if media_id != raw_media_id and raw_media_id in self._media_owner:
            self._media_owner.setdefault(
                media_id, self._media_owner[raw_media_id])
        res_norm = (resolution or "VIDEO_RESOLUTION_1080P").strip().upper()
        if res_norm in ("4K", "VIDEO_RESOLUTION_4K", "2160P", "UHD"):
            model = UPSCALE_MODELS.get(
                "VIDEO_RESOLUTION_4K", "veo_3_1_upsampler_4k")
        elif res_norm in ("1080P", "VIDEO_RESOLUTION_1080P", "1K", "FHD"):
            model = UPSCALE_MODELS.get(
                "VIDEO_RESOLUTION_1080P", "veo_3_1_upsampler_1080p")
        else:
            model = UPSCALE_MODELS.get(resolution, "veo_3_1_upsampler_1080p")
        if model not in ("veo_3_1_upsampler_1080p", "veo_3_1_upsampler_4k"):
            return {"status": 400,
                    "error": f"unsupported upscale resolution {resolution!r}: "
                             f"model {model!r} is unverified — use "
                             f"VIDEO_RESOLUTION_1080P or VIDEO_RESOLUTION_4K"}
        try:
            nick = await self._acquire(
                profile_id, [media_id],
                deadline=time.time() + FLOW_SUBMIT_RETRY_S)
        except FlowGrpcError as e:
            return {"status": 429, "error_code": "submit_queue_busy",
                    "retryable": True, "retry_after_s": 5,
                    "error": str(e)[:200]}
        _trace_emit("grpc.facade", op="upscale", nick=nick.name,
                    media=media_id[:8],
                    media_owner=self._media_owner.get(media_id, "?") or None,
                    model=model)
        try:
            r = await asyncio.to_thread(
                self._upscale_sync, nick, media_id, None, model, aspect_ratio)
            ops = [(o.get("operation") or {}).get("name")
                   for o in (r.get("data") or {}).get("operations", [])]
            _trace_emit("grpc.facade.end", op="upscale", nick=nick.name,
                        ops=ops)
            flow_stats.bump(nick.name, videos=1)
            self._throttle_streak.pop(nick.name, None)
            return r
        except FlowGrpcError as e:
            return self._facade_error("upscale", nick, e)
        finally:
            self._release(nick.name)

    async def check_video_status(self, operations: list[dict]) -> dict:
        out = []
        for entry in operations or []:
            op_id = ((entry.get("operation") or {}).get("name")
                     or entry.get("name") or "").removeprefix("operations/")
            if not op_id:
                out.append({"operation": {}, "error": "no operation name",
                            "status": "MEDIA_GENERATION_STATUS_FAILED"})
                continue
            resolved = self._resolve_op(op_id)
            if (resolved not in self._op_owner
                    and time.time() - self._last_disk_merge > 0.5):
                # Maybe submitted by the other agent during a swap.
                self._last_disk_merge = time.time()
                await asyncio.to_thread(self._locked_merge)
                resolved = self._resolve_op(op_id)
            nick = self._nicks.get(self._op_owner.get(resolved, ""))
            if nick is None:
                nick = self._nicks[self._order[0]]
            try:
                out.append(await asyncio.to_thread(
                    self._poll_op_sync, nick, op_id))
            except FlowGrpcError as e:
                out.append({"operation": {"name": op_id},
                            "status": "MEDIA_GENERATION_STATUS_PENDING",
                            "complaint": str(e)})
        return {"status": 200, "data": {"operations": out}}

    def _owner_for_media(self, media_id: str) -> str:
        owner = self._media_owner.get(media_id, "")
        if not owner and media_id and media_id.endswith("_upsampled"):
            base_id = re.sub(r"_(?:4k_)?upsampled$", "", media_id)
            owner = (self._media_owner.get(base_id, "")
                     or self._media_owner.get(f"{base_id}_upsampled", ""))
            if owner:
                self._media_owner[media_id] = owner
        return owner

    async def get_media(self, media_id: str, profile_id=None, **_kw) -> dict:
        # Nova often GETs /media/{operation_id}. Resolve the submit-time
        # placeholder so a finished clip is not 404 under the hop id.
        resolved = (self._op_media.get(self._resolve_op(media_id))
                    or self._op_media.get(media_id) or media_id)
        if (not self._owner_for_media(resolved)
                and not self._owner_for_media(media_id)
                and self._resolve_op(media_id) not in self._op_owner
                and time.time() - self._last_disk_merge > 0.5):
            # The submit that owns this media may have been served by the
            # sibling agent — adopt its routes before guessing a nick.
            self._last_disk_merge = time.time()
            await asyncio.to_thread(self._locked_merge)
        nick = self._nicks.get(
            self._owner_for_media(resolved)
            or self._owner_for_media(media_id)
            or self._op_owner.get(self._resolve_op(media_id), ""))
        if nick is None:
            cand = self._candidate(profile_id, None)
            nick = self._nicks.get(cand) or self._nicks[self._order[0]]
        try:
            rec = await asyncio.to_thread(
                self._get_media_sync, nick, resolved)
            if rec.get("status") == 404 and resolved != media_id:
                rec = await asyncio.to_thread(
                    self._get_media_sync, nick, media_id)
            return rec
        except FlowGrpcError as e:
            return {"status": 502, "error": f"grpc[{nick.name}] {e}"}

    def media_auth_headers(self, media_id: str) -> dict:
        """BatchGetMedia returns lh3.googleusercontent URLs for images —
        those only download with the owning account's ya29 bearer token."""
        if not self._nicks:
            self.reload()
        if (not self._owner_for_media(media_id)
                and time.time() - self._last_disk_merge > 0.5):
            self._last_disk_merge = time.time()
            self._locked_merge()
        nick = self._nicks.get(self._owner_for_media(media_id))
        if nick is None and self._order:
            nick = self._nicks[self._order[0]]
        if nick is None:
            return {}
        try:
            return {"Authorization": f"Bearer {nick.mint_access_token()}",
                    "User-Agent": UA_CFN}
        except Exception:
            return {}

    def media_url_via_relay(self, url: str, media_id: str = "") -> str:
        """Rewrite a Google CDN url onto the owning nick's TLS relay so the
        download carries the iOS fingerprint + the same egress proxy —
        a direct fetch would leave Python's TLS + home IP."""
        if (not self._owner_for_media(media_id)
                and time.time() - self._last_disk_merge > 0.5):
            self._last_disk_merge = time.time()
            self._locked_merge()
        nick = self._nicks.get(self._owner_for_media(media_id))
        if nick is None and self._order:
            nick = self._nicks[self._order[0]]
        return nick._relay_url(url) if nick else url

    async def upload_image(self, image_base64: str,
                           mime_type: str = "image/jpeg",
                           project_id: str = "", profile_id=None,
                           reference_media_id: str | None = None,
                           **_kw) -> dict:
        # reference_media_id chains uploads onto the anchor's owner nick so a
        # multi-ref r2v/image set stays on one account — otherwise each upload
        # lands on the least-busy nick and generation sees refs it lacks.
        anchor = [reference_media_id] if reference_media_id else None
        raw = base64.b64decode(image_base64)
        # Dedup: same bytes already uploaded -> reuse the media_id on the
        # account that owns it. Verify with BatchGetMedia first — media dies
        # with its project, so a stale hit falls through to a real upload.
        if not anchor:
            rec = self._uploaded.get(hashlib.sha256(raw).hexdigest())
            owner = self._nicks.get((rec or {}).get("nick", ""))
            mid = (rec or {}).get("media_id", "")
            if (owner and mid
                    and owner.name not in self._parked
                    and self._throttle_until.get(owner.name, 0) <= time.time()
                    and not self._upload_dead(owner.name)):
                try:
                    info = await asyncio.to_thread(
                        self._get_media_sync, owner, mid)
                    live = (info.get("data", {}).get("image")
                            or info.get("data", {}).get("video"))
                    if live:
                        self._media_owner[mid] = owner.name
                        self._media_seen[mid] = time.time()
                        self._save_routes()
                        _trace_emit("grpc.upload.dedup", nick=owner.name,
                                    media_id=mid)
                        return {"status": 200,
                                "data": {"media": {"name": mid}},
                                "_mediaId": mid, "deduped": True}
                except Exception:
                    pass
                self._uploaded.pop(hashlib.sha256(raw).hexdigest(), None)
                self._save_uploaded()
        last: FlowGrpcError | None = None
        # Least-busy nick (pinned by profile_id / anchor owner). Uploads used
        # to always land on order[0], which pinned every i2v start image to
        # that nick — one account rendered ~20x the fleet and ops timed out.
        # Transient network / channel failures retry once on another nick.
        budget_nicks = 4 if not anchor else 1
        tried: set = set()
        res: dict | None = None
        for nick_attempt in range(budget_nicks):
            try:
                nick = await self._acquire(
                    profile_id, anchor, exclude=tried, op="upload",
                    deadline=time.time() + FLOW_SUBMIT_RETRY_S)
            except FlowGrpcError as e:
                return res or {"status": 429, "error_code": "submit_queue_busy",
                               "retryable": True, "retry_after_s": 5,
                               "error": str(e)[:200]}
            _trace_emit("grpc.facade", op="upload_image", nick=nick.name,
                        anchored=bool(anchor))
            try:
                try:
                    return await asyncio.to_thread(
                        self._upload_sync, nick, raw, project_id or None)
                except FlowGrpcError as e:
                    if "invalid argument" not in str(e).lower():
                        raise
                    _trace_emit("grpc.upload.recaptcha", nick=nick.name)
                    return await asyncio.to_thread(
                        self._upload_sync, nick, raw, project_id or None, True)
            except FlowGrpcError as e:
                last = e
                err_str = str(e).lower()
                if "invalid argument" not in err_str:
                    if (_transport_detail(str(e)) or "channel closed" in err_str):
                        nick._drop_channel()
                    res = self._facade_error("upload_image", nick, e)
                    if (nick_attempt < budget_nicks - 1
                            and res.get("error_code") in (
                                "auth_unauthenticated", "auth_mint",
                                "grpc_transport", "user_requests_throttled",
                                "resource_exhausted")):
                        tried.add(nick.name)
                        _trace_emit("grpc.failover", op="upload_image",
                                    nick=nick.name)
                        continue
                    return res
                break
            finally:
                self._release(nick.name)
        # Not a throttle. A 429 here made Nova retry the same bytes for minutes.
        return {
            "status": 400,
            "error_code": "upload_invalid_argument",
            "retryable": False,
            "error": str(last or "upload failed"),
        }


# --------------------------------------------------------------------------

_transport: Optional[FlowGrpcTransport] = None


def get_flow_grpc() -> FlowGrpcTransport:
    global _transport
    if _transport is None:
        _transport = FlowGrpcTransport()
        _transport.reload()
    return _transport


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    t = get_flow_grpc()
    print("nicks:", t.nicks())
    result = asyncio.run(t.generate_images(
        sys.argv[1] if len(sys.argv) > 1 else "smoke test",
        profile_id=sys.argv[2] if len(sys.argv) > 2 else None))
    print(json.dumps(result, indent=2)[:800])
