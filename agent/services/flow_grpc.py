"""Direct PC-side gRPC transport for Google Flow (iOS app protocol).

Replaces the Chrome-extension batchexecute path with plain gRPC to
``aisandbox-pa.googleapis.com`` using per-account credential bundles under
``nicks/<name>/`` (see .omc/research/FLOW_IOS_API.md for the wire research):

    refresh_token.txt   1//... -> ya29 via oauthaccountmanager issuetoken
    mrr_req.bin         recaptcha/api3/mrr template -> 0cAFcWe… token
    gen_img_req.bin     captured BatchGenerateImages payload (prompt slots)
    meta.json           device_id / client_id / project_uuid / user_agent
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
import json
import logging
import os
import random
import re
import threading
import time
from pathlib import Path
from typing import Optional

import grpc
import requests

from agent.services.flow_proto import (
    decode, encode, fld_msg, fld_str, fld_varint,
    replace_fields, set_leaf_bytes, set_varint, walk_leaves,
)
from agent.services.flow_trace import emit as _trace_emit

logger = logging.getLogger(__name__)

NICKS_DIR = Path(__file__).resolve().parents[2] / "nicks"
TPL_DIR = NICKS_DIR / "_tpl"
ROUTES_FILE = NICKS_DIR / "_routes.json"

GRPC_HOST = "aisandbox-pa.googleapis.com:443"
ISSUETOKEN_URL = "https://oauthaccountmanager.googleapis.com/v1/issuetoken"
MRR_URL = "https://www.recaptcha.net/recaptcha/api3/mrr"

SVC = "google.internal.labs.aisandbox.proto"
M_IMAGE = f"/{SVC}.flow.v1.FlowService/BatchGenerateImages"
M_T2V = f"/{SVC}.videofx.v1.VideoFxService/BatchAsyncGenerateVideoText"
M_I2V = f"/{SVC}.videofx.v1.VideoFxService/BatchAsyncGenerateVideoStartImage"
M_R2V = f"/{SVC}.videofx.v1.VideoFxService/BatchAsyncGenerateVideoReferenceImages"
M_CHECK = f"/{SVC}.videofx.v1.VideoFxService/BatchCheckAsyncVideoGenerationStatus"
M_GET_MEDIA = f"/{SVC}.flow.v1.FlowService/BatchGetMedia"
M_PROJECT = f"/{SVC}.flow.v1.FlowService/GetProjectContents"
M_UPLOAD = f"/{SVC}.flow.v1.FlowService/UploadImage"

UA_GRPC = ("Flow App/1.566.3 (Build 1.566.3; ios {ios}; {hw}) "
           "Dart/RPCClient")
UA_MINT = "com.google.whisk/1.566.3 iSL/3.4 iPhone/{ios} hw/{hw} (gzip)"

UUID_RE = re.compile(rb"^[0-9a-f]{8}-[0-9a-f-]{27}$")
CAPTCHA_RE = re.compile(rb"[03][0-9a-zA-Z]AFcWe[0-9A-Za-z_.\-]{100,}")
URL_RE = re.compile(rb"https://[^\s\"'\x00-\x20]+")
# Leaf strings that are never the prompt when auto-discovering prompt slots.
NON_PROMPT_RE = re.compile(
    rb"^([0-9a-f]{8}-|veo_|mobile;|projects/|NARWHAL|[03][0-9a-zA-Z]AFcWe|"
    rb"[A-Z]{2,4}$|http)")

_channel_lock = threading.Lock()


class FlowGrpcError(RuntimeError):
    def __init__(self, message: str, code=None):
        super().__init__(message)
        self.code = code


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
        self.refresh_token = (dirpath / "refresh_token.txt").read_text().strip()
        self.meta = json.loads((dirpath / "meta.json").read_text())
        proxy_file = dirpath / "proxy.txt"
        self.proxy = (proxy_file.read_text().strip() or None) \
            if proxy_file.exists() else None
        self.proxies = ({"http": self.proxy, "https": self.proxy}
                        if self.proxy else None)
        self.gen_template = (dirpath / "gen_img_req.bin").read_bytes()
        self._mrr_req = (dirpath / "mrr_req.bin").read_bytes() \
            if (dirpath / "mrr_req.bin").exists() else None
        self._ya29: Optional[str] = None
        self._ya29_exp = 0.0
        self._captcha: Optional[bytes] = None
        self._captcha_at = 0.0
        self._grpc_proxy_url: Optional[str] = None
        self._channel: Optional[grpc.Channel] = None

    # -- identity -----------------------------------------------------------

    @property
    def project_uuid(self) -> str:
        return self.meta.get("project_uuid", "")

    def _ua_parts(self) -> dict:
        m = re.search(r"iPhone/(\S+) hw/(\S+)", self.meta.get("user_agent", ""))
        ios, hw = (m.group(1), m.group(2).replace("_", ",")) if m \
            else ("18.3.2", "iPhone11,8")
        return {"ios": ios, "hw": hw}

    def meta_block(self, project_id: str | None = None) -> bytes:
        """f4 metadata msg of the captured gen payload; project uuid patched
        when the caller's project differs from the captured one."""
        fields = decode(self.gen_template)
        meta = next(v for f, w, v in fields if f == 4 and w == 2)
        target = (project_id or self.project_uuid).encode()
        if self.project_uuid and target != self.project_uuid.encode():
            meta = set_leaf_bytes(meta, [6], target)
        return meta

    # -- tokens -------------------------------------------------------------

    def mint_access_token(self, force: bool = False) -> str:
        if not force and self._ya29 and time.time() < self._ya29_exp:
            return self._ya29
        ua = self._ua_parts()
        r = requests.post(
            ISSUETOKEN_URL,
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
            proxies=self.proxies,
            timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        self._ya29 = body["token"]
        self._ya29_exp = time.time() + int(body.get("expiresIn", 3599)) - 120
        return self._ya29

    def mint_captcha(self, force: bool = False, max_age_s: int = 7200) -> bytes:
        """Replay the mrr template -> fresh 0cAFcWe… token. The captured
        token embedded in gen templates is accepted for image gen; freshly
        minted ones are reserved for calls that reject stale tokens."""
        if (not force and self._captcha
                and time.time() - self._captcha_at < max_age_s):
            return self._captcha
        if not self._mrr_req:
            raise FlowGrpcError(f"{self.name}: no mrr_req.bin captured")
        r = requests.post(
            MRR_URL,
            headers={"content-type": "application/x-protobuffer"},
            data=self._mrr_req,
            proxies=self.proxies,
            timeout=30,
        )
        r.raise_for_status()
        m = CAPTCHA_RE.search(r.content)
        if not m:
            raise FlowGrpcError(
                f"{self.name}: no captcha token in mrr response")
        self._captcha = m.group(0)
        self._captcha_at = time.time()
        return self._captcha

    # -- grpc ---------------------------------------------------------------

    def channel(self) -> grpc.Channel:
        if self._channel is not None:
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
                ua = self._ua_parts()
                self._channel = grpc.secure_channel(
                    GRPC_HOST,
                    grpc.ssl_channel_credentials(),
                    options=[("grpc.primary_user_agent",
                              UA_GRPC.format(**ua) + " dart-grpc/2.0.0")],
                )
            finally:
                if prev is not None:
                    os.environ["https_proxy"] = prev
                else:
                    os.environ.pop("https_proxy", None)
        return self._channel

    def call(self, method: str, payload: bytes, timeout: int = 90) -> bytes:
        unary = self.channel().unary_unary(
            method,
            request_serializer=lambda x: x,
            response_deserializer=lambda x: x,
        )
        ua = self._ua_parts()
        token = self.mint_access_token()
        md = [
            ("authorization", f"Bearer {token}"),
            ("x-goog-authuser", "0"),
            ("x-custom-user-agent", UA_GRPC.format(**ua)),
        ]
        rpc = method.rsplit("/", 1)[-1]
        t0 = time.monotonic()
        _trace_emit("grpc.rpc.start", nick=self.name, rpc=rpc,
                    req_bytes=len(payload), proxy=bool(self.proxy))
        try:
            resp, _ctx = unary.with_call(payload, metadata=md, timeout=timeout)
            _trace_emit("grpc.rpc.end", nick=self.name, rpc=rpc,
                        elapsed_ms=round((time.monotonic() - t0) * 1000),
                        resp_bytes=len(resp))
            return resp
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAUTHENTICATED:
                _trace_emit("grpc.rpc.reauth", nick=self.name, rpc=rpc)
                md[0] = ("authorization",
                         f"Bearer {self.mint_access_token(force=True)}")
                try:
                    resp, _ctx = unary.with_call(payload, metadata=md,
                                                 timeout=timeout)
                    _trace_emit("grpc.rpc.end", nick=self.name, rpc=rpc,
                                elapsed_ms=round((time.monotonic() - t0) * 1000),
                                resp_bytes=len(resp), reauth=True)
                    return resp
                except grpc.RpcError as e2:
                    _trace_emit("grpc.rpc.error", nick=self.name, rpc=rpc,
                                elapsed_ms=round((time.monotonic() - t0) * 1000),
                                grpc_code=str(e2.code()),
                                detail=str(e2.details())[:300])
                    raise FlowGrpcError(str(e2.details()), e2.code())
            _trace_emit("grpc.rpc.error", nick=self.name, rpc=rpc,
                        elapsed_ms=round((time.monotonic() - t0) * 1000),
                        grpc_code=str(e.code()),
                        detail=str(e.details())[:300])
            raise FlowGrpcError(str(e.details()), e.code())


# --------------------------------------------------------------------------
# payload builders / response readers
# --------------------------------------------------------------------------

def _deepest_text_leaf(buf: bytes, prefix: list[int]) -> list[int] | None:
    """Deepest utf-8 text leaf under `prefix` that is not a uuid/model/token.
    Image prompts may sit at the slot itself ([2,2]) or nested inside a
    structured segment ([2,2,15]) — pick whichever leaf actually holds text."""
    best = None
    for p, v in walk_leaves(buf):
        if p[: len(prefix)] != prefix or not isinstance(v, bytes):
            continue
        if NON_PROMPT_RE.match(v):
            continue
        try:
            v.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if best is None or len(p) > len(best) or (
                len(p) == len(best) and len(v) > 0):
            best = p
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


def _read_media_records(resp: bytes) -> list[dict]:
    """BatchGenerateImages response -> [{media_id, url}]."""
    out = []
    for f, w, v in decode(resp):
        if f != 1 or w != 2:
            continue
        leaves = walk_leaves(v)
        uuid = next((lv for _, lv in leaves if UUID_RE.match(lv)
                     if isinstance(lv, bytes)), None)
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

    Shape: f3{ f4{ f5 op_uuid, f6 media_uuid }, f5 project_uuid } + f4 full
    status msg."""
    op_id = media_id = project_id = ""
    for f, w, v in decode(resp):
        if f != 3 or w != 2:
            continue
        sub = decode(v)
        inner = next((iv for fn, fw, iv in sub if fn == 4 and fw == 2), b"")
        if inner:
            ifs = decode(inner)
            op_id = _uuid(_leaf_at(ifs, [5]) or b"") or op_id
            media_id = _uuid(_leaf_at(ifs, [6]) or b"") or media_id
        project_id = _uuid(_leaf_at(sub, [5]) or b"") or project_id
    if not op_id:  # fallback: first uuid leaf inside f3
        for f, w, v in decode(resp):
            if f == 3 and w == 2:
                for _, lv in walk_leaves(v):
                    if _uuid(lv):
                        op_id = lv.decode()
                        break
    return {"op_id": op_id, "media_id": media_id, "project_id": project_id}


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

    A video media block carries both a poster `/image/<uuid>` url and the
    clip `/video/<uuid>` url — collect every url leaf and classify."""
    out = {}
    for f, w, v in decode(resp):
        if f != 1 or w != 2:
            continue
        leaves = walk_leaves(v)
        uuid = next((lv for _, lv in leaves
                     if isinstance(lv, bytes) and UUID_RE.match(lv)), None)
        urls = [u for u in (_url_in(lv) for _, lv in leaves) if u]
        if not uuid:
            continue
        rec = {}
        for u in urls:
            rec["video" if "/video/" in u else "image"] = u
        out[uuid.decode()] = rec
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


def _build_upload(meta_block: bytes, image: bytes) -> bytes:
    return fld_msg(1, meta_block) + fld_msg(2, image)


# Aspect enums — confirmed by capture diffs on landon:
#   video: t2v portrait tpl f3=1, 16:9 capture f3=2 (i2v same slot, r2v f4)
#   image: portrait tpl f2.f5=2, landscape capture f2.f5=3
_VIDEO_ASPECT_FIELD = {"t2v": 3, "i2v": 3, "r2v": 4}
_VIDEO_ASPECT_VALUE = {"portrait": 1, "9:16": 1, "vertical": 1, "1:1": 1,
                       "landscape": 2, "16:9": 2, "horizontal": 2}
_IMAGE_ASPECT_VALUE = {"portrait": 2, "9:16": 2, "vertical": 2,
                       "landscape": 3, "16:9": 3, "horizontal": 3,
                       "square": 1, "1:1": 1}


def _aspect_varint(aspect_ratio: str, table: dict) -> int | None:
    a = (aspect_ratio or "").lower()
    for key, val in table.items():
        if key in a:
            return val
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


# --------------------------------------------------------------------------
# transport
# --------------------------------------------------------------------------

# Per-nick scheduling: max in-flight gen calls and minimum spacing between
# submits (the iOS app itself fires serially; bursts beyond this drew
# UNUSUAL_ACTIVITY on web paths).
MAX_CONCURRENT_PER_NICK = 12
PACE_MIN_S = 2.0
PACE_MAX_S = 3.0


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
        self._sched_lock: Optional[asyncio.Lock] = None
        self._media_owner: dict[str, str] = {}
        self._op_owner: dict[str, str] = {}
        self._op_media: dict[str, str] = {}
        self._op_project: dict[str, str] = {}
        self._load_routes()

    # -- nick registry -------------------------------------------------------

    def nicks(self) -> list[str]:
        if not self._nicks:
            self.reload()
        return sorted(self._nicks)

    def reload(self) -> None:
        self._nicks = {
            d.name: GrpcNick(d)
            for d in NICKS_DIR.iterdir()
            if d.is_dir() and not d.name.startswith("_")
            and (d / "refresh_token.txt").exists()
        }
        self._order = self.nicks()
        for name in self._order:
            self._inflight.setdefault(name, 0)

    def _load_routes(self) -> None:
        try:
            routes = json.loads(ROUTES_FILE.read_text())
            self._media_owner = routes.get("media", {})
            self._op_owner = routes.get("ops", {})
            self._op_media = routes.get("op_media", {})
            self._op_project = routes.get("op_project", {})
        except Exception:
            pass

    def _save_routes(self) -> None:
        try:
            ROUTES_FILE.write_text(json.dumps(
                {"media": self._media_owner, "ops": self._op_owner,
                 "op_media": self._op_media,
                 "op_project": self._op_project}))
        except Exception:
            pass

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

    async def _acquire(self, profile_id: str | None = None,
                       media_ids: list[str] | None = None) -> GrpcNick:
        if not self._nicks:
            self.reload()
        if not self._nicks:
            raise FlowGrpcError("no nick bundles under nicks/")
        pinned = self._candidate(profile_id, media_ids)
        queued_at = 0.0
        waited_n = 0
        while True:
            async with self._sched():
                name = pinned
                if name is None:
                    # least in-flight; rotate starting point for ties
                    lo = min(self._inflight.get(n, 0) for n in self._order)
                    for i in range(len(self._order)):
                        cand = self._order[(self._rr_idx + i) % len(self._order)]
                        if self._inflight.get(cand, 0) == lo:
                            name = cand
                            self._rr_idx = (self._rr_idx + i + 1) % len(self._order)
                            break
                if name and self._inflight.get(name, 0) < MAX_CONCURRENT_PER_NICK:
                    self._inflight[name] = self._inflight.get(name, 0) + 1
                    break
            if not queued_at:
                queued_at = time.time()
                _trace_emit("grpc.queued", pinned=pinned,
                            waiters=self._waiters + 1,
                            inflight={n: self._inflight.get(n, 0)
                                      for n in self._order})
                self._waiters += 1
            else:
                waited_n += 1
                if waited_n % 10 == 0:  # ~every 5s while queued
                    _trace_emit("grpc.sched.wait", pinned=pinned,
                                queued_s=round(time.time() - queued_at, 1),
                                inflight={n: self._inflight.get(n, 0)
                                          for n in self._order})
            await asyncio.sleep(0.5)
        if queued_at:
            self._waiters = max(0, self._waiters - 1)
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

    def _release(self, name: str) -> None:
        self._inflight[name] = max(0, self._inflight.get(name, 1) - 1)

    # -- sync cores ----------------------------------------------------------

    def _gen_images_sync(self, nick: GrpcNick, prompt: str,
                         project_id: str | None,
                         ref_media_ids: list[str] | None = None,
                         aspect_ratio: str = "") -> dict:
        buf = _patch_prompt_slots(nick.gen_template, prompt,
                                  [[2, 2], [2, 9]])
        av = _aspect_varint(aspect_ratio, _IMAGE_ASPECT_VALUE)
        if av is not None:
            buf = set_varint(buf, [2, 5], av)
        if ref_media_ids:
            # captured shape (landon, ingredient gen): f2.f3 repeated,
            # each {f1: media_uuid, f5: slot} with slot counting down N..1
            n = len(ref_media_ids)
            payloads = [encode([(1, 2, mid.encode()),
                                (5, 0, n - i)])
                        for i, mid in enumerate(ref_media_ids)]
            buf = replace_fields(buf, [2], 3, payloads)
        if project_id and project_id != nick.project_uuid:
            buf = set_leaf_bytes(buf, [1], f"projects/{project_id}".encode())
            buf = set_leaf_bytes(buf, [2, 8, 6], project_id.encode())
            buf = set_leaf_bytes(buf, [4, 6], project_id.encode())
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
                        **kw) -> dict:
        buf = _build_video(tpl, prompt,
                           nick.meta_block(project_id),
                           aspect_ratio=aspect_ratio, **kw)
        resp = nick.call({
            "t2v": M_T2V, "i2v": M_I2V, "r2v": M_R2V}[tpl],
            buf, timeout=180)
        op = _read_operation(resp)
        if not op["op_id"]:
            raise FlowGrpcError(f"{tpl} returned no operation")
        self._op_owner[op["op_id"]] = nick.name
        self._op_project[op["op_id"]] = project_id or nick.project_uuid
        if op["media_id"]:
            self._op_media[op["op_id"]] = op["media_id"]
            self._media_owner[op["media_id"]] = nick.name
        self._save_routes()
        return {"status": 200, "data": {"operations": [{
            "operation": {"name": op["op_id"]},
            "status": "MEDIA_GENERATION_STATUS_PENDING"}]}}

    def _poll_op_sync(self, nick: GrpcNick, op_id: str) -> dict:
        """Listing is the authority (same as the web path): the op's
        fetchable media uuid only appears in GetProjectContents once the
        render resolves — the submit-time media id is a placeholder."""
        clean = op_id.removeprefix("operations/")
        entry = {"operation": {"name": clean},
                 "status": "MEDIA_GENERATION_STATUS_PENDING"}
        complaint = None
        media_id = ""
        try:
            check = _read_check(nick.call(M_CHECK, _build_check(clean)))
            complaint = check.get("complaint")
        except FlowGrpcError as e:
            complaint = str(e)
        urls = _read_media_urls(
            nick.call(M_GET_MEDIA, _build_get_media(clean)))
        if (urls.get(clean) or {}).get("video"):
            media_id = clean  # video media keyed under the op uuid itself
        else:
            try:
                project = self._op_project.get(clean) or nick.project_uuid
                listing = nick.call(
                    M_PROJECT, _build_project_contents(project))
                media_id = _read_project_media(listing).get(clean, "")
            except FlowGrpcError as e:
                complaint = complaint or str(e)
        if complaint:
            entry["complaint"] = complaint
        if media_id:
            self._media_owner.setdefault(media_id, nick.name)
            entry["operation"]["metadata"] = {"video": {"mediaId": media_id}}
            if media_id != clean:
                urls = _read_media_urls(
                    nick.call(M_GET_MEDIA, _build_get_media(media_id)))
            url = (urls.get(media_id) or {}).get("video", "")
            if url:
                entry["operation"]["metadata"]["video"]["fifeUrl"] = url
                entry["status"] = "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        return entry

    def _get_media_sync(self, nick: GrpcNick, media_id: str) -> dict:
        urls = _read_media_urls(
            nick.call(M_GET_MEDIA, _build_get_media(media_id), timeout=45))
        rec = urls.get(media_id) or {}
        if not rec:
            return {"status": 404, "error": f"No urls for media {media_id}"}
        data = {}
        if rec.get("video"):
            data["video"] = {"fifeUrl": rec["video"]}
        if rec.get("image"):
            data["image"] = {"fifeUrl": rec["image"]}
        return {"status": 200, "data": data}

    def _upload_sync(self, nick: GrpcNick, image: bytes,
                     project_id: str | None) -> dict:
        buf = _build_upload(nick.meta_block(project_id), image)
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
        self._media_owner[media_id] = nick.name
        self._save_routes()
        return {"status": 200, "data": {"media": {"name": media_id}},
                "_mediaId": media_id}

    # -- async facade (same shapes as FlowClient) -----------------------------

    async def generate_images(self, prompt: str, project_id: str = "",
                              aspect_ratio: str = "", profile_id=None,
                              character_media_ids: list[str] | None = None,
                              **_kw) -> dict:
        owners = {self._media_owner[m].casefold()
                  for m in (character_media_ids or []) if self._media_owner.get(m)}
        if len(owners) > 1:
            return {"status": 400, "error_code": "media_profile_mismatch",
                    "retryable": False,
                    "error": "MEDIA_PROFILE_MISMATCH: character_media_ids span "
                             "multiple nicks; upload all references through one "
                             "anchor chain"}
        nick = await self._acquire(profile_id,
                                   list(character_media_ids or []))
        _trace_emit("grpc.facade", op="generate_images", nick=nick.name,
                    refs=len(character_media_ids or []),
                    ref_ids={m[:8]: self._media_owner.get(m, "?")
                             for m in (character_media_ids or [])} or None,
                    aspect=aspect_ratio or "default",
                    project=project_id or nick.project_uuid)
        try:
            r = await asyncio.to_thread(
                self._gen_images_sync, nick, prompt, project_id or None,
                list(character_media_ids or []), aspect_ratio)
            _trace_emit("grpc.facade.end", op="generate_images",
                        nick=nick.name,
                        media=[m["name"] for m in
                               (r.get("data") or {}).get("media", [])])
            return r
        except FlowGrpcError as e:
            _trace_emit("grpc.facade.error", op="generate_images",
                        nick=nick.name, error=str(e)[:300])
            return {"status": 502, "error": f"grpc[{nick.name}] {e}"}
        finally:
            self._release(nick.name)

    async def generate_video(self, start_image_media_id: str | None = None,
                             prompt: str = "", project_id: str = "",
                             aspect_ratio: str = "", profile_id=None,
                             **_kw) -> dict:
        media = [start_image_media_id] if start_image_media_id else []
        nick = await self._acquire(profile_id, media)
        kind = "i2v" if start_image_media_id else "t2v"
        _trace_emit("grpc.facade", op=kind, nick=nick.name,
                    start=start_image_media_id[:8] if start_image_media_id else None,
                    start_owner=self._media_owner.get(
                        start_image_media_id or "", "?") or None,
                    aspect=aspect_ratio or "default",
                    project=project_id or nick.project_uuid)
        try:
            if start_image_media_id:
                r = await asyncio.to_thread(
                    self._gen_video_sync, nick, "i2v", prompt,
                    project_id or None, aspect_ratio=aspect_ratio,
                    start_uuid=start_image_media_id)
            else:
                r = await asyncio.to_thread(
                    self._gen_video_sync, nick, "t2v", prompt,
                    project_id or None, aspect_ratio=aspect_ratio)
            ops = [(o.get("operation") or {}).get("name")
                   for o in (r.get("data") or {}).get("operations", [])]
            _trace_emit("grpc.facade.end", op=kind, nick=nick.name, ops=ops)
            return r
        except FlowGrpcError as e:
            _trace_emit("grpc.facade.error", op=kind, nick=nick.name,
                        error=str(e)[:300])
            return {"status": 502, "error": f"grpc[{nick.name}] {e}"}
        finally:
            self._release(nick.name)

    async def generate_video_from_references(
            self, reference_media_ids: list[str], prompt: str,
            project_id: str = "", aspect_ratio: str = "", profile_id=None,
            **_kw) -> dict:
        if not reference_media_ids:
            return {"status": 400, "error": "no reference media_ids"}
        owners = {self._media_owner[m].casefold()
                  for m in reference_media_ids if self._media_owner.get(m)}
        if len(owners) > 1:
            return {"status": 400, "error_code": "media_profile_mismatch",
                    "retryable": False,
                    "error": "MEDIA_PROFILE_MISMATCH: reference media_ids span "
                             "multiple nicks; upload all references through one "
                             "anchor chain"}
        nick = await self._acquire(profile_id, reference_media_ids)
        _trace_emit("grpc.facade", op="r2v", nick=nick.name,
                    refs=len(reference_media_ids),
                    ref_ids={m[:8]: self._media_owner.get(m, "?")
                             for m in reference_media_ids},
                    aspect=aspect_ratio or "default",
                    project=project_id or nick.project_uuid)
        try:
            r = await asyncio.to_thread(
                self._gen_video_sync, nick, "r2v", prompt,
                project_id or None, aspect_ratio=aspect_ratio,
                ref_uuids=list(reference_media_ids))
            ops = [(o.get("operation") or {}).get("name")
                   for o in (r.get("data") or {}).get("operations", [])]
            _trace_emit("grpc.facade.end", op="r2v", nick=nick.name, ops=ops)
            return r
        except FlowGrpcError as e:
            _trace_emit("grpc.facade.error", op="r2v", nick=nick.name,
                        error=str(e)[:300])
            return {"status": 502, "error": f"grpc[{nick.name}] {e}"}
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
            nick = self._nicks.get(self._op_owner.get(op_id, ""))
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

    async def get_media(self, media_id: str, profile_id=None, **_kw) -> dict:
        nick = self._nicks.get(self._media_owner.get(media_id, ""))
        if nick is None:
            cand = self._candidate(profile_id, None)
            nick = self._nicks.get(cand) or self._nicks[self._order[0]]
        try:
            return await asyncio.to_thread(
                self._get_media_sync, nick, media_id)
        except FlowGrpcError as e:
            return {"status": 502, "error": f"grpc[{nick.name}] {e}"}

    def media_auth_headers(self, media_id: str) -> dict:
        """BatchGetMedia returns lh3.googleusercontent URLs for images —
        those only download with the owning account's ya29 bearer token."""
        if not self._nicks:
            self.reload()
        nick = self._nicks.get(self._media_owner.get(media_id, ""))
        if nick is None and self._order:
            nick = self._nicks[self._order[0]]
        if nick is None:
            return {}
        try:
            return {"Authorization": f"Bearer {nick.mint_access_token()}"}
        except Exception:
            return {}

    async def upload_image(self, image_base64: str,
                           mime_type: str = "image/jpeg",
                           project_id: str = "", profile_id=None,
                           reference_media_id: str | None = None,
                           **_kw) -> dict:
        # reference_media_id chains uploads onto the anchor's owner nick so a
        # multi-ref r2v/image set stays on one account — otherwise each upload
        # lands on the least-busy nick and generation sees refs it lacks.
        anchor = [reference_media_id] if reference_media_id else None
        nick = await self._acquire(profile_id, anchor)
        try:
            raw = base64.b64decode(image_base64)
            return await asyncio.to_thread(
                self._upload_sync, nick, raw, project_id or None)
        except FlowGrpcError as e:
            return {"status": 502, "error": f"grpc[{nick.name}] {e}"}
        finally:
            self._release(nick.name)


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
