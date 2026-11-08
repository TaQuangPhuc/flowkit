"""Google Vids API client (docs.google.com/videos → appsgenaiserver).

Replay của traffic Vids bắt qua mitm (chromemgr/captures/):
  POST https://appsgenaiserver-pa.clients6.google.com/v1/genai/generate?key=<K>
  auth: SAPISIDHASH (sha1 ts+SAPISID+origin) + cookie google + x-server-token
  body: json+protobuf nested array (templates từ chromemgr/vids_templates.json)

Per-nick artifacts (nicks/<nick>/):
  vids_cookies.json  — cookie set (chromemgr cookie-sync dump + push)
  vids_doc.txt       — Vids doc id (flix document id)

Upload ảnh external → AVL token qua resumable protocol:
  POST docs.google.com/upload/temporaryblob/videos (start) → x-goog-upload-url
  PUT  <upload_url> bytes → ["AVL_..."]
"""
from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import subprocess as _sp
import time
import uuid as _uuid
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

NICKS_DIR = Path(__file__).resolve().parent.parent.parent / "nicks"
TEMPLATES_PATH = Path(__file__).resolve().parent.parent.parent / "chromemgr" / "vids_templates.json"

API_KEY = "AIzaSyA-njTXslyyMKk1VOogRfVP59F6fNGeNW8"
GENAI_BASE = "https://appsgenaiserver-pa.clients6.google.com/v1/genai"
UPLOAD_URL = "https://docs.google.com/upload/temporaryblob/videos?authuser=0"
ORIGIN = "https://docs.google.com"

ENV = {"t2v": "374", "i2v": "375", "r2v": "376", "edit": "377", "extend": "378",
       "upscale": "279"}
AR = {"landscape": 1, "portrait": 2, "square": 3}  # square chưa verify

# Placeholder values gốc trong template (sẽ bị thay)
_T_DOC = "1ambUe7XZuSYkWr4eYOe56vgW84-tULU_TaKsASb2F7g"
_T_ASSET_UUIDS = {"375": "0FC6C0F2-93D0-4C33-B581-4CA4E0B2CE78",
                  "376a": "633DCA76-3ECB-48B9-A2DD-D4D2F59606E0",
                  "376b": "E2FEC24D-E1D9-475C-97E3-B9DB758B9504",
                  "377": "11833CBD-28CE-428D-A139-069CD740A06D",
                  "378": "11833CBD-28CE-428D-A139-069CD740A06D"}
_T_AVL = {"375": "AVL_0qhGbjcWnDdEXHYpGD3G1DY8FPzbX3pukvQ18XEqFzI40qZIRO8ikbtJhiiykZWY5lIkNKPaNrL8FvWpHee8OztDZGiMqSYmhBM5efywgOnwAW3kUxXE1B8vbzTve_1a8bCyBQYC3MVw5rdo64qZP5FGCAjTJI20dF5_eZKgF4fpiDwTfeM",
          "376a": "AVL_0qgT9SNXjMrfugsQ1i1ufif2D9Ncnem3U48vg8YcC1S7Q6qhMFZFdyfpcWmsd4f2Kf9iREfKZyPAFRG-Ttep8MwyOHxS28LoLQDuvybeA5M1KpuRkNZaCqDbSNm5OO2tyPyF0wiKSSlepWZzpfGRlOzmpgaOmsO6S9v7kzsYUiGsTuPscis",
          "376b": "AVL_0qivdLIFaSa7TDXhjrlfwRuqY4mPZH7nWnVl8N4mV3qK0Yf7G1rU4W0lVnR5sT0pC8kS9nW7rV4iH9uD6rN0jK9vX3mH8oZ9xY3uS1sJ6oV6pH1vD0oU2nK4pW8fL7qQ0rS8tV9wX2yZ5aB3cD6eF7gH8iJ9kL0mN1oP2qR3sT4uV5wX6yZ7",
          "377": None, "378": None}
_T_TEMPID = "33c1038630a6958500065d3651cab9a1088ea0dd0923f642"
_T_TEMPID_279 = "d10574b547b4a1f100065d9b5a38db4906ad3b8d2201afaf"
# Doc id trong capture edit-video khác với doc của 374-378
_T_DOC2 = "1MqbYyK0HkFBKdONkQk038LBe4yJGO126wa82pfqHTIY"
_T_PROMPTS = {"374": "con cho chay tren bai bien",
              "375": "Image1 quay phim cận cảnh con mèo đang chạy",
              "377": "doi mau nen thanh mau xanh",
              "378": "doi mau nen thanh mau xanhcon meo chay ve phia camera",
              "379": "Hãy thay đổi khung cảnh của người trong video ",
              "380": "Hãy thay người trong video bằng người trong  "}
# Edit video upload (AVL) — capture user submit trên video vừa up
_T_VAVL = {"379": "AVL_0qhh6iEqYoPDtN1T-AhFD9g8MX7GoGlFrSiz7_6OQ9w2ZWYU_"
                  "vvp3lvSIHXatvtty84YpHnGV_JDWpTzosPVeRgEisBFzfj5yCnJSob"
                  "Rh-8ardn_9LsxKa7gg1TIhUWXqXFwSQezAo8IEDeVIh07xdUTcIfa-JmR"
                  "-Gb8D40kbBjg8T4cjtM",
           "380": "AVL_0qgLPLP2M9k9e2s4h6IxcZEApXiMja4T_z8ABSWv0vjckrz3Gt-"
                  "koWuGhqY3nUXoeEnlXt_7oj8p7qXVY11tPhKyv1W8CV2G6_TGInxCMW-"
                  "VRC9yTPaJHMJOZQYkwX8cNltoE-ZGGJrtg6lwKh1__udKfGQReaCyPA62"
                  "1wXO6zWEcmvqWH0"}
_T_ASSET_UUIDS.update({"379": "23A01E52-80F1-4245-B776-3B8B9178352D",
                       "380v": "5E763BE8-D1E3-498C-A4FD-F6AEB857F2DF",
                       "380i": "6D7AA19A-6671-4BD0-8697-7376BF4EA2CA"})
_T_IMGAVL_380 = ("AVL_0qh5CDPbLgpvxb6tkJCiurnq3zLniWwC9qdQp-sMXhmfK69aFWo28IA"
                 "wBxf3h6kteuU-Pt5GYK7GkuFFZq6yUk97PCvUM4v3tR49qZZBuZkwkQlfkJ"
                 "u-hfV_0_WsyZtIOJNlCBUMftFs24GaFwHBzTo4I1VkIltL4LuYY7jZqwrk5"
                 "jzRoDs")
_T_IMGNAME_380 = "Hình ảnh1"  # capture: i+0300 + U+1EA3

_templates: dict | None = None
_server_tokens: dict[str, tuple[str, float]] = {}  # nick → (token, ts)


def _templates_load() -> dict:
    global _templates
    if _templates is None:
        _templates = json.loads(TEMPLATES_PATH.read_text(encoding="utf-8"))
    return _templates


class VidsError(Exception):
    pass


class VidsPolicyError(VidsError):
    """Upstream từ chối prompt/ảnh vì policy (genai trả REQUEST_REFUSED)."""
    pass


class VidsQuotaError(VidsError):
    """Nick hết quota Google Vids (HTTP 429 USER_GENAI_QUOTA_EXHAUSTED)."""
    pass


QUOTA_DEAD_TTL_S = 6 * 3600

def save_vids_quota(nick: str, credits_total: int | None, credits_left: int | None) -> None:
    if credits_total is None and credits_left is None:
        return
    try:
        qf = NICKS_DIR / nick / "vids_quota.json"
        data = {
            "credits_total": credits_total,
            "credits_left": credits_left,
            "credits_used": (credits_total - credits_left) if (credits_total is not None and credits_left is not None) else None,
            "updated_at": time.time(),
        }
        qf.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


def load_vids_quota(nick: str) -> dict:
    qf = NICKS_DIR / nick / "vids_quota.json"
    if qf.exists():
        try:
            return json.loads(qf.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}



def is_nick_dead(nick: str) -> bool:
    d = NICKS_DIR / nick
    if not (d / "vids_cookies.json").exists() or not (d / "vids_doc.txt").exists():
        return True
    if (d / "_vids_dead").exists():
        return True
    qf = d / "_vids_quota_dead"
    if qf.exists():
        try:
            if time.time() - qf.stat().st_mtime < QUOTA_DEAD_TTL_S:
                return True
            qf.unlink(missing_ok=True)
        except OSError:
            pass
    return False


def live_nicks(exclude: set[str] | None = None) -> list[str]:
    out = []
    if not NICKS_DIR.exists():
        return out
    excl = exclude or set()
    for n in sorted(NICKS_DIR.iterdir()):
        if not n.is_dir() or n.name.startswith("_") or n.name in excl:
            continue
        if (n / "vids_cookies.json").exists() and (n / "vids_doc.txt").exists():
            if not is_nick_dead(n.name):
                out.append(n.name)
    return out


_REFUSED_RE = re.compile(r'REQUEST_REFUSED|UNSAFE_GENERATION|content_policy',
                         re.IGNORECASE)


def _check_refused(raw: str, status: int = 200) -> None:
    """Google Vids refusal: HTTP 400 `[3,"invalid argument", ErrorInfo
    ["REQUEST_REFUSED","espresso-pa.googleapis.com"]]`. Bắt cả 200-thiếu-url."""
    if _REFUSED_RE.search(raw or ""):
        raise VidsPolicyError(f"generate refused {status}: {raw[:200]}")


class VidsNick:
    """Per-nick cookie + doc context."""

    def __init__(self, nick: str):
        d = NICKS_DIR / nick
        self.nick = nick
        ck = json.loads((d / "vids_cookies.json").read_text(encoding="utf-8"))
        # cookiejar theo domain thật — SID/OSID có nhiều giá trị
        # khác nhau theo domain (.google.com vs .google.com.vn vs
        # .docs.google.com), để requests tự match per-host.
        self.session = requests.Session()
        for c in ck:
            self.session.cookies.set(
                c["name"], c["value"],
                domain=c.get("domain", ".google.com"),
                path=c.get("path", "/"))
        self.sapisid = next(
            (c["value"] for c in ck
             if c["name"] == "SAPISID" and c.get("domain") == ".google.com"),
            None)
        if not self.sapisid:
            raise VidsError(f"{nick}: thiếu SAPISID trong vids_cookies.json")
        self.doc = (d / "vids_doc.txt").read_text().strip()
        proxy_f = d / "proxy.txt"
        if proxy_f.exists():
            p = proxy_f.read_text().strip()
            if p:
                self.session.proxies = {"http": p, "https": p}
        self.session.headers.update({
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/154.0.0.0 Safari/537.36"})

    def sapisid_hash(self) -> str:
        ts = int(time.time())
        h = hashlib.sha1(f"{ts} {self.sapisid} {ORIGIN}".encode()).hexdigest()
        return f"{ts}_{h}"

    def server_token(self, force: bool = False) -> str:
        tok, ts = _server_tokens.get(self.nick, (None, 0))
        if tok and not force and time.time() - ts < 3600:
            return tok
        # token CAMS... nhúng trong HTML của doc page
        last_err = None
        r = None
        for _ in range(3):
            try:
                r = self.session.get(
                    f"https://docs.google.com/videos/d/{self.doc}/edit",
                    timeout=(8, 15))
                break
            except requests.exceptions.RequestException as e:
                last_err = e
                _rotate_proxy(self)
        if r is None:
            raise VidsError(f"{self.nick}: server_token proxy/network fail: "
                            f"{str(last_err)[:200]}")
        m = re.search(r'"(CAMS[\w\-=]{20,})"', r.text) or re.search(
            r"\b(CAMS[\w\-=]{20,})", r.text)
        if not m:
            if r.status_code in (401, 403) or "signin" in str(r.url):
                # Marker drives /api/vids/nicks session_dead — cleared when
                # the PC cookie-sync pushes a fresh jar.
                (NICKS_DIR / self.nick / "_vids_dead").touch()
                raise VidsError(
                    f"{self.nick}: Google session dead — cần relogin nick "
                    "(vids_cookies hết hạn)")
            raise VidsError(f"{self.nick}: không thấy CAMS token trong doc page "
                            f"(status {r.status_code})")
        _server_tokens[self.nick] = (m.group(1), time.time())
        return m.group(1)

    def headers(self, token: str) -> dict:
        return {
            "authorization": f"SAPISIDHASH {self.sapisid_hash()}",
            "x-server-token": token,
            "content-type": "application/json+protobuf",
            "origin": ORIGIN,
            "referer": "https://docs.google.com/",
            "x-goog-authuser": "0",
        }


def get_nick(nick: str) -> VidsNick:
    return VidsNick(nick)


# ---------------------------------------------------------------------------
# Proxy rotation cho đường genai — exit proxy flap ngẫu nhiên (CONNECT bị
# đóng). Pool = mọi proxy.txt của fleet (residential, đã trust Google); mỗi
# nick tự chạy trước, lỗi thì xoay. proxy_pool.txt trong dir nick (nếu có)
# override pool chung.
# ---------------------------------------------------------------------------
def _proxy_pool(nick: str) -> list[str]:
    f = NICKS_DIR / nick / "proxy_pool.txt"
    if f.exists():
        return [l.strip() for l in f.read_text().splitlines() if l.strip()]
    out, seen = [], set()
    for d in sorted(NICKS_DIR.iterdir()):
        if not d.is_dir() or d.name.startswith("_"):
            continue
        pf = d / "proxy.txt"
        if not pf.exists():
            continue
        p = pf.read_text().strip()
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _rotate_proxy(nk: VidsNick) -> None:
    """Đổi session.proxies sang proxy kế trong pool (xoay vòng)."""
    pool = _proxy_pool(nk.nick)
    if len(pool) < 2:
        return
    cur = (nk.session.proxies or {}).get("https")
    try:
        nxt = pool[(pool.index(cur) + 1) % len(pool)]
    except ValueError:
        nxt = pool[0]
    nk.session.proxies = {"http": nxt, "https": nxt}


_UPLOAD_CHUNK = 6 << 20  # Chrome chia 6MB/chunk cho video lớn


def upload_image(nk: VidsNick, data: bytes) -> str:
    """Resumable upload → AVL token. Cùng endpoint cho ảnh lẫn video
    (temporaryblob/videos); >6MB upload theo chunk như Chrome."""
    last_err = None
    for attempt in range(3):
        try:
            r = nk.session.post(
                UPLOAD_URL,
                headers={
                    "origin": ORIGIN,
                    "referer": f"https://docs.google.com/videos/d/{nk.doc}/edit",
                    "x-goog-upload-command": "start",
                    "x-goog-upload-protocol": "resumable",
                    "x-goog-upload-content-length": str(len(data)),
                    "x-goog-upload-file-name": "temporary_blob",
                    "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
                }, timeout=(10, 30))
            up = r.headers.get("x-goog-upload-url")
            if r.status_code != 200 or not up:
                raise VidsError(f"upload init fail {r.status_code} {r.text[:200]}")
            hdrs = {
                "origin": ORIGIN,
                "referer": f"https://docs.google.com/videos/d/{nk.doc}/edit",
                "content-type": "application/x-www-form-urlencoded;charset=utf-8",
            }
            off, last = 0, None
            while True:
                chunk = data[off:off + _UPLOAD_CHUNK]
                fin = off + len(chunk) >= len(data)
                r2 = nk.session.put(
                    up, data=chunk,
                    headers={**hdrs,
                             "x-goog-upload-command":
                                 "upload, finalize" if fin else "upload",
                             "x-goog-upload-offset": str(off)},
                    timeout=(10, 120))
                if r2.status_code != 200:
                    raise VidsError(f"upload chunk @{off} fail {r2.status_code} "
                                    f"{r2.text[:200]}")
                off += len(chunk)
                if fin:
                    last = r2
                    break
            break
        except (requests.exceptions.RequestException, VidsError) as e:
            last_err = e
            _rotate_proxy(nk)
            if attempt == 2:
                if isinstance(e, VidsError):
                    raise
                raise VidsError(f"upload proxy/network fail: {str(last_err)[:200]}")
    try:
        avl = json.loads(last.text)[0]
    except Exception:
        raise VidsError(f"upload finalize fail {last.status_code} "
                        f"{last.text[:200]}")
    # Google Scotty temporaryblob returns a 137-byte Tink ciphertext encoded as
    # ~183-char base64url. Depending on active Google key ID, prefix is "AVL_..."
    # (0x0152ffd2a8) or "AYGX..." (0x018197a630) or another "A..." Tink header.
    if not isinstance(avl, str) or len(avl) < 40 or not re.fullmatch(r"[A-Za-z0-9_\-=]+", avl):
        raise VidsError(f"upload resp lạ: {last.text[:200]}")
    return avl


def _walk_patch(node, repl: dict):
    """Replace string values in nested list theo bảng repl (exact match)."""
    if isinstance(node, list):
        for i, v in enumerate(node):
            if isinstance(v, str) and v in repl:
                node[i] = repl[v]
            else:
                _walk_patch(v, repl)


def _prompt_segments(body, placeholder: str):
    """Tìm list chứa segment text `[null,null,<placeholder>]` trong prompt."""
    def rec(n):
        if isinstance(n, list):
            if any(isinstance(e, list) and len(e) > 2 and e[2] == placeholder
                   for e in n):
                return n
            for v in n:
                r = rec(v)
                if r is not None:
                    return r
        return None
    return rec(body)


def _asset_list(body):
    """Container list của asset entries `[[null×7, [uuid,...]], ...]`."""
    def rec(n):
        if isinstance(n, list):
            try:
                if (n and isinstance(n[0], list) and n[0]
                        and isinstance(n[0][0], list)
                        and isinstance(n[0][0][7], list)
                        and isinstance(n[0][0][7][0], str)
                        and "-" in n[0][0][7][0]):
                    return n
            except (IndexError, TypeError):
                pass
            for v in n:
                r = rec(v)
                if r is not None:
                    return r
        return None
    return rec(body)


def _img_entry(avl: str, uid: str, name: str):
    """Asset entry ảnh — clone từ template 376 (r2v)."""
    body = json.loads(_templates_load()["376"])
    ent = json.loads(json.dumps(_asset_list(body)[0]))
    _walk_patch(ent, {_T_ASSET_UUIDS["376a"]: uid,
                      _T_AVL["376a"]: avl, "Image1": name})
    return ent


def _vid_entry(avl: str, uid: str):
    """Asset entry video upload — clone từ template 379."""
    body = json.loads(_templates_load()["379"])
    ent = json.loads(json.dumps(_asset_list(body)[0]))
    _walk_patch(ent, {_T_ASSET_UUIDS["379"]: uid, _T_VAVL["379"]: avl})
    return ent


def _mention_node(name: str, uid: str):
    """Mention node trong prompt — clone từ mention đầu của template 376."""
    body = json.loads(_templates_load()["376"])
    segs = _prompt_segments(body, "Người này đứng lên và nhảy với cái váy của")
    node = json.loads(json.dumps(segs[0]))
    _walk_patch(node, {"Image1": name, _T_ASSET_UUIDS["376a"]: uid})
    return node


_IMG_MARK = re.compile(r"\{img(\d*)\}")


def _build_prompt(prompt: str, names_uuids: list) -> list:
    """Prompt text + markers `{img}`/`{img1}`..`{imgN}` → segment list
    xen kẽ text + mention. Ảnh không được mention trong prompt thì append
    mention ở cuối (Google cần link uuid để bind asset)."""
    prompt = prompt.replace("{img}", "{img1}")
    parts = _IMG_MARK.split(prompt)
    segs, seen = [], set()
    for i, p in enumerate(parts):
        if i % 2 == 0:
            if p:
                segs.append([None, None, p])
        else:
            idx = (int(p) if p else 1) - 1
            if 0 <= idx < len(names_uuids) and idx not in seen:
                segs.append(_mention_node(*names_uuids[idx]))
                seen.add(idx)
    for j, nu in enumerate(names_uuids):
        if j not in seen:
            if segs and segs[-1][2:3] != [" "]:
                segs.append([None, None, " "])
            segs.append(_mention_node(*nu))
    return segs or [[None, None, prompt]]


def _set_aspect(body, ar: int):
    """Tìm sublist [0,12,null,0,X,...] → set X = ar."""
    def rec(n):
        if isinstance(n, list):
            if (len(n) >= 5 and n[0] == 0 and n[1] == 12
                    and n[3] == 0 and isinstance(n[4], int)):
                n[4] = ar
                return True
            return any(rec(v) for v in n)
        return False
    rec(body)


def _dup_img_asset(template_body, avl: str, uid: str):
    """Clone asset sub-array của i2v template cho 1 ảnh mới."""
    tpl = _templates_load()["375"]
    body = json.loads(tpl)
    target = _T_ASSET_UUIDS["375"]
    found = []

    def rec(n):
        if isinstance(n, list) and n and n[0] == target:
            found.append(n)
        if isinstance(n, list):
            for v in n:
                rec(v)

    rec(body)
    asset = json.loads(json.dumps(found[0]))  # deep copy
    _walk_patch(asset, {_T_ASSET_UUIDS["375"]: uid, _T_AVL["375"]: avl})
    return asset


def _set_duration(body, seconds: int, source_us: int | None = None):
    """[4][15] = output duration block: [8]=seconds, [7]=[seconds,srcµs?].
    Capture 380: [6, 42000000] cho clip nguồn 42s — f7[1] là duration
    video nguồn tính µs."""
    try:
        f15 = body[4][15]
    except (IndexError, TypeError):
        return
    if not isinstance(f15, list) or len(f15) < 9:
        return
    f15[8] = seconds
    if isinstance(f15[7], list) and f15[7]:
        f15[7][0] = seconds
        if len(f15[7]) > 1:
            f15[7][1] = source_us if source_us is not None \
                else seconds * 1_000_000


def build_body(mode: str, doc: str, prompt: str,
               images: list[str] | None = None,
               temp_data_id: str | None = None,
               aspect: str = "landscape",
               duration: int = 0,
               source_us: int | None = None,
               video_avl: str | None = None,
               image_name: str = "Image1",
               resolution: int = 720) -> str:
    """Build request body cho mode từ captured template."""
    res1080 = resolution >= 1080 and mode != "upscale"
    env = ENV[mode]
    if mode == "edit" and video_avl:
        env = "380" if images else "379"
    body = json.loads(_templates_load()[env])
    repl = {_T_DOC: doc, _T_DOC2: doc}

    # prompt placeholder
    if env in ("374", "377", "378", "375", "379"):
        old = _T_PROMPTS.get(env)
        if old:
            repl[old] = prompt

    if env == "375":  # i2v — 1 ảnh
        if not images:
            raise VidsError("i2v cần 1 image AVL token")
        repl[_T_AVL["375"]] = images[0]
        repl[_T_ASSET_UUIDS["375"]] = str(_uuid.uuid4()).upper()
    elif env == "376":  # r2v — N ảnh; prompt dùng {img1}..{imgN}
        if not images or len(images) < 2:
            raise VidsError("r2v cần >=2 image AVL tokens")
        assets = _asset_list(body)
        entries, names = [], []
        for i, avl in enumerate(images):
            uid = str(_uuid.uuid4()).upper()
            nm = f"Image{i + 1}"
            entries.append(_img_entry(avl, uid, nm))
            names.append((nm, uid))
        assets[:] = entries
        segs = _prompt_segments(
            body, "Người này đứng lên và nhảy với cái váy của")
        if segs is None:
            raise VidsError("template 376: không tìm thấy prompt segments")
        segs[:] = _build_prompt(prompt, names)
    elif env in ("377", "378"):  # edit/extend — video nguồn
        if video_avl and env == "378":  # extend video upload ngoài (capture: AVL layout như 379)
            assets = _asset_list(body)
            assets[:] = [_vid_entry(video_avl, str(_uuid.uuid4()).upper())]
        elif temp_data_id:
            repl[_T_TEMPID] = temp_data_id
            repl[_T_ASSET_UUIDS[env]] = str(_uuid.uuid4()).upper()
        else:
            raise VidsError(f"{mode} cần temp_data_id hoặc video_avl của video nguồn")
    elif env == "379":  # edit video upload ngoài (AVL)
        repl[_T_VAVL["379"]] = video_avl
        repl[_T_ASSET_UUIDS["379"]] = str(_uuid.uuid4()).upper()
    elif env == "380":  # edit video upload + N ảnh reference, {imgN} markers
        if not images:
            raise VidsError("edit+image cần >=1 image AVL token")
        assets = _asset_list(body)
        entries = [_vid_entry(video_avl, str(_uuid.uuid4()).upper())]
        names = []
        base = image_name.rstrip("0123456789 ") or "Image"
        for i, avl in enumerate(images):
            uid = str(_uuid.uuid4()).upper()
            nm = image_name if len(images) == 1 else f"{base}{i + 1}"
            entries.append(_img_entry(avl, uid, nm))
            names.append((nm, uid))
        assets[:] = entries
        segs = _prompt_segments(body, _T_PROMPTS["380"])
        if segs is None:
            raise VidsError("template 380: không tìm thấy prompt segments")
        segs[:] = _build_prompt(prompt, names)
    elif env == "279":  # upscale clip Omni (temp_data_id), không prompt
        if not temp_data_id:
            raise VidsError("upscale cần temp_data_id của clip đã gen")
        repl[_T_TEMPID_279] = temp_data_id

    _walk_patch(body, repl)
    # fresh request id
    def regoog(n):
        if isinstance(n, list):
            for i, v in enumerate(n):
                if isinstance(v, str) and v.startswith("goog_"):
                    n[i] = f"goog_{random.randint(10**8, 10**9 - 1)}"
                else:
                    regoog(v)
    regoog(body)
    if env == "279":
        pass  # [4][15][4]=5 là op-type upscale, ko phải aspect/res
    elif res1080:
        # 1080p: enum encode cả res+aspect — 5=landscape, 6=portrait.
        # Square ko có 1080p (7+ invalid) → giữ 720p square.
        if aspect == "square":
            _set_aspect(body, AR["square"])
        else:
            try:
                body[4][15][4] = 6 if aspect == "portrait" else 5
            except (IndexError, TypeError):
                raise VidsError("template không có output-spec field")
    else:
        _set_aspect(body, AR.get(aspect, 1))
    if duration and env != "279":
        _set_duration(body, duration, source_us)
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False)


def generate(nick: str, prompt: str, mode: str = "t2v",
             images: list[str] | None = None,
             temp_data_id: str | None = None,
             aspect: str = "landscape",
             timeout: int = 0,
             video_avl: str | None = None,
             image_name: str = "Image1",
             duration: int = 0,
             source_us: int | None = None,
             delogo: bool = True,
             resolution: int = 720) -> dict:
    """Gọi genai/generate → {model, video_url, temp_data_id, raw}.

    mode="edit" nhận video nguồn qua temp_data_id (clip Omni đã gen)
    hoặc video_avl (video upload ngoài — upload_bytes trước). Kèm
    images=[AVL] để edit video theo ảnh reference; đặt "{img}" trong
    prompt ở vị trí muốn mention ảnh. resolution=1080 → output 1080p
    trực tiếp (field [4][15][4]=6), rẻ hơn gen 720 + upscale."""
    if timeout <= 0:
        # t2v/i2v/r2v bình thường 40-90s; để 95s để nếu socket treo thì ngắt sớm
        # trước OmniGenTimeout (170s) của gateway, kịp sang nick kế tiếp.
        timeout = 158 if mode in ("edit", "extend") else 95
    nk = get_nick(nick)
    body = build_body(mode, nk.doc, prompt, images, temp_data_id, aspect,
                      duration=duration, source_us=source_us,
                      video_avl=video_avl, image_name=image_name,
                      resolution=resolution)
    pool = _proxy_pool(nick)
    max_attempts = min(max(len(pool), 2), 6)
    last_err = None
    r = None
    need_fresh = False
    for attempt in range(max_attempts):
        t0 = time.monotonic()
        try:
            tok = nk.server_token(force=need_fresh)
            r = nk.session.post(
                f"{GENAI_BASE}/generate?key={API_KEY}",
                headers=nk.headers(tok), data=body.encode("utf-8"),
                timeout=(10, timeout))  # connect 10s — proxy chết fail nhanh
        except (requests.exceptions.ProxyError,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.SSLError) as e:
            last_err = e
            r = None
            _rotate_proxy(nk)
            continue
        except requests.exceptions.ReadTimeout as e:
            last_err = e
            r = None
            # urllib3 _prepare_proxy -> _tunnel dùng connect timeout (10s) nhưng
            # ném ReadTimeout khi proxy giữ TCP mà không trả 200 Connection established.
            if time.monotonic() - t0 < 25:
                _rotate_proxy(nk)
                continue
            raise VidsError(f"{nick}: generate read timeout: {str(e)[:200]}")
        except requests.exceptions.RequestException as e:
            last_err = e
            r = None
            _rotate_proxy(nk)
            continue
        if r.status_code == 401 and not need_fresh:
            last_err = r.text
            r = None
            need_fresh = True
            continue  # retry với token mới
        break
    if r is None:
        raise VidsError(f"generate proxy fail sau {max_attempts} lần: "
                        f"{str(last_err)[:200]}")
    if r.status_code != 200:
        if r.status_code == 401 and "SESSION_COOKIE_INVALID" in (r.text or ""):
            (NICKS_DIR / nick / "_vids_dead").touch()
        if r.status_code == 429 or "USER_GENAI_QUOTA_EXHAUSTED" in (r.text or ""):
            cr429 = re.search(r'\["(\d{3,})",null,"(\d+)"', r.text or "")
            if cr429:
                save_vids_quota(nick, int(cr429.group(1)), int(cr429.group(2)))
            (NICKS_DIR / nick / "_vids_quota_dead").touch()
            try:
                from agent.services.flow_trace import emit as _trace_emit
                _trace_emit("vids.generate.error", nick=nick, op=f"vids_{mode}", error=f"HTTP 429 quota exhausted")
            except Exception:
                pass
            raise VidsQuotaError(f"generate {r.status_code}: {r.text[:300]}")
        _check_refused(r.text, r.status_code)
        raise VidsError(f"generate {r.status_code}: {r.text[:300]}")

    raw = r.text
    model = re.search(r"(/flix/\w+/\w+)", raw)
    url = re.search(r"https://contribution-rt\.usercontent\.google\.com/"
                    r"download\?[^\s\"\\]+", raw)
    if not url:
        _check_refused(raw)
    temp = re.search(r"lookup_temp_data[^a-z0-9]+([a-z0-9]{30,})", raw)
    # response kèm credit: ["<tổng>",null,"<còn lại>",1,...]
    cr = re.search(r'\["(\d{3,})",null,"(\d+)"', raw)
    c_tot = int(cr.group(1)) if cr else None
    c_left = int(cr.group(2)) if cr else None
    save_vids_quota(nick, c_tot, c_left)
    try:
        from agent.services import flow_stats
        from agent.services.flow_trace import emit as _trace_emit
        flow_stats.bump(nick, videos=1)
        _trace_emit("grpc.facade.end", nick=nick, op=f"vids_{mode}", credits_left=c_left, credits_total=c_tot)
    except Exception:
        pass
    video_file = None
    if url:
        video_file = _download_video(nk, url.group(0), delogo=delogo)
    return {
        "model": model.group(1) if model else None,
        "video_url": url.group(0) if url else None,
        "video_file": video_file,
        "temp_data_id": temp.group(1) if temp else None,
        "elapsed_hint": None,
        "raw_len": len(raw),
        "credits_total": c_tot,
        "credits_left": c_left,
    }


OUT_DIR = Path(__file__).resolve().parent.parent.parent / "media" / "vids"
OUT_KEEP_S = 24 * 3600


_ALPHA_PATH = Path(__file__).resolve().parent / "vids_star_alpha.npy"
_ALPHA_1080_PATH = Path(__file__).resolve().parent / "vids_star_alpha_1080.npy"
_alpha_cache = {}


def _load_alpha(key: str, p: Path, thr: float, size: int = 0):
    """Trả (a, m, den, rim): alpha map, feather mask, denominator 1-a, rim mask."""
    ent = _alpha_cache.get(key)
    if ent is None:
        import cv2
        import numpy as np
        A = np.load(p)
        if size and A.shape[0] != size:
            A = np.clip(cv2.resize(A, (size, size), interpolation=cv2.INTER_CUBIC),
                        0.0, 1.0)
        m = cv2.GaussianBlur(
            (A > thr).astype(np.float32), (0, 0), 1.2)[..., None]
        rim = None
        if size >= 270:
            inner = cv2.erode((A > 0.25).astype(np.uint8),
                              np.ones((3, 3), np.uint8))
            outer = cv2.dilate((A > 0.025).astype(np.uint8),
                               np.ones((3, 3), np.uint8))
            rim = ((outer - inner) > 0).astype(np.uint8) * 255
        ent = (A[:, :, None], m, np.clip(1 - A[:, :, None], 0.05, 1), rim)
        _alpha_cache[key] = ent
    return ent


def _find_star_1080(cap, W: int, H: int, A):
    """Watermark upscale 1080p KHÔNG neo góc — trôi theo video.
    Temporal median của ~40 frame → star tĩnh nổi trên nền động;
    detect blob sáng ở nửa dưới-phải. Trả (cx, cy) hoặc None."""
    import cv2
    import numpy as np
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) or 0
    idx_set = set(np.linspace(0, max(total - 1, 0), min(32, max(total, 1)),
                              dtype=int).tolist()) if total else None
    x1, y1 = int(W * 0.55), int(H * 0.5)
    frs, i = [], 0
    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
    while True:
        if idx_set is not None and i not in idx_set:
            if not cap.grab():
                break
            i += 1
            continue
        ok, f = cap.read()
        if not ok:
            break
        frs.append(cv2.cvtColor(f[y1:, x1:], cv2.COLOR_BGR2GRAY))
        i += 1
    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
    if not frs:
        return None
    roi = np.median(np.stack(frs), axis=0).astype(np.uint8)
    bg = cv2.medianBlur(roi, 75).astype(np.int16)
    d = roi.astype(np.int16) - bg
    n, _, st, _ = cv2.connectedComponentsWithStats((d > 18).astype(np.uint8), 8)
    best = None
    for j in range(1, n):
        x, y, w, h, ar = st[j]
        if 500 <= ar <= 4000 and 40 <= w <= 140 and 40 <= h <= 140:
            if best is None or ar > best[0]:
                best = (ar, x + w // 2 + x1, y + h // 2 + y1)
    if best is None:
        return None
    # fine-tune: matchTemplate hình star (alpha map) quanh blob ±20px
    half = A.shape[0] // 2
    cx, cy = best[1], best[2]
    sx0, sy0 = max(cx - half - 20, x1), max(cy - half - 20, y1)
    sx1, sy1 = min(cx + half + 20, W), min(cy + half + 20, H)
    win = roi[sy0 - y1:sy1 - y1, sx0 - x1:sx1 - x1]
    if win.shape[0] >= A.shape[0] and win.shape[1] >= A.shape[1]:
        res = cv2.matchTemplate(win, (A[:, :] * 255).astype(np.uint8),
                                cv2.TM_CCOEFF_NORMED)
        _, max_val, _, mx = cv2.minMaxLoc(res)
        if max_val < 0.22:
            return None
        cx, cy = sx0 + mx[0] + half, sy0 + mx[1] + half
    return (cx, cy)


def _delogo_unblend(path: Path) -> None:
    """Xóa watermark ✦ góc dưới-phải.
    720p: alpha un-blend, star 48px neo góc W-120/H-120
    (vids_star_alpha.npy 180x180).
    1080p (ngang 1920x1080 & dọc 1080x1920): alpha un-blend 1.5x
    (270x270 neo góc W-270/H-270, tâm sao W-180/H-180) + khử viền
    sharpen 2px bằng Telea inpaint.
    No-op nếu thiếu cv2/numpy/ffmpeg."""
    try:
        import cv2
        import numpy as np
    except ImportError:
        return
    try:
        cap = cv2.VideoCapture(str(path))
        if not cap.isOpened():
            return
        W = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        H = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS) or 24
        hd = max(W, H) >= 1600
        if hd:
            a, m, den, rim = _load_alpha("A_270", _ALPHA_PATH, 0.004, size=270)
        else:
            a, m, den, rim = _load_alpha("A", _ALPHA_PATH, 0.004)
        x0, y0 = W - a.shape[1], H - a.shape[0]
        box = a.shape[0]
        tmp = path.with_suffix(".delogo.mp4")
        out = cv2.VideoWriter(str(tmp), cv2.VideoWriter_fourcc(*"mp4v"),
                              fps, (W, H))
        while True:
            ok, f = cap.read()
            if not ok:
                break
            reg = f[y0:y0 + box, x0:x0 + box]
            if reg.shape[:2] != (box, box):
                out.write(f)
                continue
            reg_f = reg.astype(np.float32)
            reg2 = np.clip((reg_f - a * 255.0) / den, 0, 255)
            u = (reg_f * (1 - m) + reg2 * m).astype(np.uint8)
            if rim is not None:
                u = cv2.inpaint(u, rim, 3, cv2.INPAINT_TELEA)
            f[y0:y0 + box, x0:x0 + box] = u
            out.write(f)
        cap.release()
        out.release()
        # mux lại audio gốc + encode h264
        done = path.with_suffix(".mux.mp4")
        r = _sp.run(
            ["ffmpeg", "-y", "-v", "error", "-threads", "2",
             "-i", str(tmp), "-i", str(path),
             "-map", "0:v", "-map", "1:a?", "-c:v", "libx264", "-crf", "18",
             "-preset", "ultrafast", "-c:a", "copy", str(done)],
            capture_output=True, timeout=300)
        if r.returncode == 0 and done.exists() and done.stat().st_size > 0:
            done.replace(path)
        tmp.unlink(missing_ok=True)
        done.unlink(missing_ok=True)
    except Exception:
        try:
            path.with_suffix(".delogo.mp4").unlink(missing_ok=True)
            path.with_suffix(".mux.mp4").unlink(missing_ok=True)
        except Exception:
            pass


def _download_video(nk: VidsNick, url: str, delogo: bool = True) -> str:
    """URL contribution-rt yêu cầu cookie google — tải qua session của nick,
    lưu media/vids/<uuid>.mp4 để API re-host (caller ngoài không fetch được
    link gốc vì nó redirect sang ServiceLogin)."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    name = f"{_uuid.uuid4()}.mp4"
    path = OUT_DIR / name
    last_err = None
    for attempt in range(3):
        try:
            r = nk.session.get(url, timeout=(10, 120), stream=True)
            ct = (r.headers.get("content-type") or "").lower()
            if r.status_code != 200 or ("video" not in ct and "octet-stream" not in ct):
                raise VidsError(
                    f"download {r.status_code} ct={ct} — link google cần auth, "
                    f"session của {nk.nick} có thể đã stale")
            with open(path, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
            break
        except requests.exceptions.RequestException as e:
            last_err = e
            path.unlink(missing_ok=True)
            _rotate_proxy(nk)
            if attempt == 2:
                raise VidsError(f"download proxy/network fail: {str(last_err)[:200]}")
    size = path.stat().st_size if path.exists() else 0
    if size < 1024:
        path.unlink(missing_ok=True)
        raise VidsError(f"download rỗng ({size}B)")
    if delogo:
        _delogo_unblend(path)
    _gc_out()
    return name


def _gc_out() -> None:
    try:
        now = time.time()
        for p in OUT_DIR.glob("*.mp4"):
            if now - p.stat().st_mtime > OUT_KEEP_S:
                p.unlink(missing_ok=True)
    except Exception:
        pass


def video_path(name: str) -> Path:
    """Path file đã tải; validate tên chống traversal."""
    if not re.fullmatch(r"[a-f0-9\-]{36}\.mp4", name or ""):
        raise VidsError("bad file name")
    p = OUT_DIR / name
    if not p.exists():
        raise VidsError("file không còn (đã dọn sau 24h)")
    return p


def upload_bytes(nick: str, data: bytes) -> str:
    return upload_image(get_nick(nick), data)


FLOW_ABRA_NICKS = ("vids01", "everly", "kailyn", "landon", "madyson", "michael")
FLOW_MACARON_NICKS = ("vids01",)


def _clean_abra_prompt(prompt: str) -> str:
    """Clean Vids `{img1}`..`{imgN}` / `<IMAGE_N>` markers for Flow abra_*."""
    p = (prompt or "").strip()
    if not p:
        return p
    m_tail = re.search(r"^(.*?)(?:\s+\{img\d*\})+\s*$", p, flags=re.S)
    if m_tail:
        prefix = m_tail.group(1).strip()
        if prefix and not re.search(
            r"\b(?:trong|của|với|từ|như|bằng|cho|cạnh|giống|in|from|of|with|as|to|by|like|near)\s*$",
            prefix, flags=re.I
        ):
            p = prefix
    p = re.sub(r"\{img(\d+)\}", lambda m: f"image {m.group(1)}", p)
    p = re.sub(r"\{img\}", "image 1", p)
    p = re.sub(r"(?i)<IMAGE_(\d+)>", lambda m: f"image {int(m.group(1)) + 1}", p)
    return p.strip() or (prompt or "").strip()


def _upload_flow_img(t, fk_nick, img_bytes: bytes) -> str:
    h = hashlib.sha256(img_bytes).hexdigest()
    cached = t._uploaded.get(h)
    if (cached and cached.get("nick") == fk_nick.name
            and time.time() - float(cached.get("ts") or 0) < 3600
            and cached.get("media_id")):
        return cached["media_id"]
    up = t._upload_sync(fk_nick, img_bytes, fk_nick.project_uuid)
    return up["_mediaId"]


def _upscale_1080_ffmpeg(path: Path) -> None:
    """Scale 720p MP4 to 1080p using 2-thread ffmpeg lanczos when caller requested 1080p."""
    done = path.with_suffix(".1080.mp4")
    try:
        r = _sp.run(
            ["ffmpeg", "-y", "-v", "error", "-threads", "2",
             "-i", str(path),
             "-vf", "scale='if(gt(iw,ih),1920,1080)':'if(gt(iw,ih),1080,1920)':flags=lanczos",
             "-map", "0:v", "-map", "0:a?",
             "-c:v", "libx264", "-crf", "18", "-preset", "ultrafast",
             "-c:a", "copy", str(done)],
            capture_output=True, timeout=120)
        if r.returncode == 0 and done.exists() and done.stat().st_size > 1024:
            done.replace(path)
    except Exception:
        pass
    finally:
        done.unlink(missing_ok=True)


def _download_flow_abra_video(t, fk_nick, media_id: str, raw_url: str, resolution: int = 720) -> str:
    """Download finished Flow abra_* video (=m15 / =m22) into OUT_DIR."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    name = f"{_uuid.uuid4()}.mp4"
    path = OUT_DIR / name
    auth = {}
    try:
        auth = t.media_auth_headers(media_id) or {}
    except Exception:
        try:
            auth = {"Authorization": f"Bearer {fk_nick.mint_access_token()}", "User-Agent": "Mozilla/5.0"}
        except Exception:
            auth = {}
    if "googleusercontent.com" in raw_url:
        base_u = re.sub(r"=[a-z0-9_-]+$", "", raw_url.split("?", 1)[0], flags=re.I)
        qs = ("?" + raw_url.split("?", 1)[1]) if "?" in raw_url else ""
        cands = [f"{base_u}=m15{qs}", f"{base_u}=m22{qs}"]
    else:
        cands = [raw_url]
    wrapped = []
    for u in cands:
        try:
            wrapped.append(t.media_url_via_relay(u, media_id) or u)
        except Exception:
            wrapped.append(u)
    raw_bytes = b""
    last_err = None
    for attempt in range(10):
        for u in wrapped:
            try:
                r = requests.get(u, headers=auth, timeout=(10, 60), verify=False)
                if r.status_code == 200 and len(r.content) > 1024 and b"ftyp" in r.content[:32]:
                    raw_bytes = r.content
                    break
            except Exception as e:
                last_err = e
        if raw_bytes:
            break
        time.sleep(2.0)
    if not raw_bytes:
        raise VidsError(f"abra download failed for {media_id[:8]}: {last_err}")
    path.write_bytes(raw_bytes)
    if resolution >= 1080:
        _upscale_1080_ffmpeg(path)
    _gc_out()
    return name


def generate_flow_abra(
    nick: str,
    prompt: str,
    mode: str = "t2v",
    raw_imgs: list[bytes] | None = None,
    aspect: str = "landscape",
    duration: int = 0,
    resolution: int = 720,
    model: str = "",
) -> dict:
    """Fallback Omni generation via Google Flow gRPC (`macaron_cookie_*` Omni 1.2 Flash
    with automatic fallback to `abra_*` Omni 1.1 Flash) using the 25,000 Flow AI Credits pool."""
    from agent.services.flow_grpc import (
        get_flow_grpc, FlowGrpcError, _build_video, set_leaf_bytes,
        _VIDEO_MODEL_FIELD, M_T2V, M_I2V, M_R2V, M_GET_MEDIA, M_CHECK,
        _build_get_media, _build_check, _read_operation, _read_media_urls,
        _read_media_missing, _read_check, _POLICY_RE,
        MACARON_DURATIONS, ABRA_DURATIONS, _nearest_abra_duration,
    )
    from agent.services import flow_stats
    from agent.services.flow_trace import emit as _trace_emit

    t = get_flow_grpc()
    if not t._nicks:
        t.reload()

    imgs = list(raw_imgs or [])
    if mode == "t2v" or not imgs:
        tpl = "t2v"
    elif len(imgs) == 1:
        tpl = "i2v"
    else:
        tpl = "r2v"

    m_low = (model or "").strip().lower()
    use_macaron = any(k in m_low for k in ("macaron", "1.2", "1_2"))
    if use_macaron:
        dur = duration if duration in MACARON_DURATIONS else 10
        suffix = "_360p" if (resolution == 360 or "360p" in m_low) else ""
        model_str = f"macaron_cookie_{tpl}_{dur}s{suffix}"
        allowed_pool = FLOW_MACARON_NICKS
    else:
        dur = duration if duration in ABRA_DURATIONS else 10
        model_str = f"abra_{tpl}_{dur}s"
        allowed_pool = FLOW_ABRA_NICKS
    rpc_method = {"t2v": M_T2V, "i2v": M_I2V, "r2v": M_R2V}[tpl]
    clean_prompt = _clean_abra_prompt(prompt)

    candidates = []
    if nick in allowed_pool and nick in t._nicks and nick not in t._parked:
        candidates.append(nick)
    for cand in allowed_pool:
        if cand not in candidates and cand in t._nicks and cand not in t._parked:
            candidates.append(cand)
    if not candidates:
        raise VidsError(f"No Flow gRPC nicks available for {model_str}")

    last_err = None
    fk_nick = None
    op_id = ""
    media_id = ""

    for cand_name in candidates[:3]:
        fk_nick = t._nicks[cand_name]
        try:
            media_ids = []
            if len(imgs) == 1:
                media_ids = [_upload_flow_img(t, fk_nick, imgs[0])]
            elif len(imgs) > 1:
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=min(len(imgs), 4)) as ex:
                    futs = [ex.submit(_upload_flow_img, t, fk_nick, b) for b in imgs]
                    media_ids = [f.result() for f in futs]

            buf = _build_video(
                tpl, clean_prompt,
                fk_nick.meta_block(fk_nick.project_uuid, fresh_captcha=True),
                start_uuid=media_ids[0] if tpl == "i2v" else None,
                ref_uuids=media_ids if tpl == "r2v" else None,
                aspect_ratio=aspect,
            )
            buf = set_leaf_bytes(buf, _VIDEO_MODEL_FIELD[tpl], model_str.encode())
            _trace_emit("grpc.facade", op=f"vids_abra_{tpl}", nick=fk_nick.name,
                        model=model_str, aspect=aspect, refs=len(media_ids))
            try:
                resp = fk_nick.call(rpc_method, buf, timeout=90)
            except FlowGrpcError as sub_e:
                sub_s = str(sub_e).lower()
                if model_str.startswith("macaron_cookie_") and (
                    "not found" in sub_s or "permission" in sub_s
                ):
                    alt_dur = _nearest_abra_duration(dur)
                    alt_model = f"abra_{tpl}_{alt_dur}s"
                    logger.warning(
                        "generate_flow_abra %s failed on %s (%s), falling back to %s",
                        model_str, fk_nick.name, str(sub_e)[:120], alt_model,
                    )
                    buf = _build_video(
                        tpl, clean_prompt,
                        fk_nick.meta_block(fk_nick.project_uuid, fresh_captcha=True),
                        start_uuid=media_ids[0] if tpl == "i2v" else None,
                        ref_uuids=media_ids if tpl == "r2v" else None,
                        aspect_ratio=aspect,
                    )
                    buf = set_leaf_bytes(buf, _VIDEO_MODEL_FIELD[tpl], alt_model.encode())
                    resp = fk_nick.call(rpc_method, buf, timeout=90)
                    model_str = alt_model
                else:
                    raise
            op = _read_operation(resp)
            op_id = (op.get("op_id") or "").removeprefix("operations/")
            media_id = op.get("media_id") or ""
            if not op_id:
                raise VidsError(f"{fk_nick.name}: {model_str} returned no operation")
            break
        except FlowGrpcError as e:
            err_s = str(e)
            if _POLICY_RE.search(err_s):
                raise VidsPolicyError(f"abra refused on {fk_nick.name}: {err_s[:240]}")
            last_err = e
            logger.warning("generate_flow_abra submit failed on %s (%s): %s", fk_nick.name, model_str, err_s[:200])
            continue
        except Exception as e:
            last_err = e
            logger.warning("generate_flow_abra error on %s (%s): %s", fk_nick.name, model_str, str(e)[:200])
            continue

    if not op_id or fk_nick is None:
        raise VidsError(f"abra submit failed: {str(last_err)[:240]}")

    t._op_owner[op_id] = fk_nick.name
    t._op_project[op_id] = fk_nick.project_uuid
    if media_id:
        t._op_media[op_id] = media_id
        t._media_owner[media_id] = fk_nick.name
    t._op_started[op_id] = time.time()
    t._save_routes()

    t_poll0 = time.monotonic()
    missing_since = None
    video_url = ""
    resolved_mid = media_id or op_id
    while time.monotonic() - t_poll0 < 125:
        time.sleep(3.0)
        ids_to_check = [m for m in (media_id, op_id) if m]
        urls = {}
        missing = set()
        for mid in ids_to_check:
            try:
                mresp = fk_nick.call(M_GET_MEDIA, _build_get_media(mid), timeout=30)
                urls.update(_read_media_urls(mresp))
                missing |= _read_media_missing(mresp)
            except FlowGrpcError:
                continue
        for mid in ids_to_check:
            rec = urls.get(mid) or {}
            if rec.get("video"):
                resolved_mid = mid
                video_url = rec["video"]
                break
        if video_url:
            break
        elapsed = time.monotonic() - t_poll0
        if media_id and media_id in missing and elapsed >= 20:
            if missing_since is None:
                missing_since = time.monotonic()
            elif time.monotonic() - missing_since >= 12:
                try:
                    chk = _read_check(fk_nick.call(M_CHECK, _build_check(op_id), timeout=20))
                    comp = chk.get("complaint") or ""
                    if comp and _POLICY_RE.search(comp):
                        raise VidsPolicyError(f"abra policy refused: {comp[:200]}")
                except VidsPolicyError:
                    raise
                except Exception:
                    pass
                raise VidsPolicyError(
                    f"Google rejected video during rendering (media {media_id[:8]})"
                )
        else:
            missing_since = None

    if not video_url:
        raise VidsError(f"{fk_nick.name}: {model_str} poll timeout after 125s (op={op_id[:8]})")

    t._media_owner[resolved_mid] = fk_nick.name
    video_file = _download_flow_abra_video(t, fk_nick, resolved_mid, video_url, resolution=resolution)
    try:
        flow_stats.bump(fk_nick.name, videos=1)
        _trace_emit("grpc.facade.end", nick=fk_nick.name, op=f"vids_abra_{tpl}",
                    model=model_str, media_id=resolved_mid[:8])
    except Exception:
        pass
    return {
        "model": model_str,
        "video_url": video_url,
        "video_file": video_file,
        "temp_data_id": resolved_mid,
        "elapsed_hint": round(time.monotonic() - t_poll0, 1),
        "raw_len": 0,
        "credits_total": 25000,
        "credits_left": None,
        "nick_used": fk_nick.name,
    }
