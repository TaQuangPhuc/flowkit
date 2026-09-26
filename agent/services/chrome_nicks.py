"""Launch vanilla Chrome per nick, with a local auth-injecting proxy bridge."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Optional

import aiohttp

from agent.config import BASE_DIR
from agent.services.accounts import get_account
from agent.services.proxy_forward import LocalProxyBridge
from agent.services.proxy_url import ParsedProxy, ProxyURLError, parse_proxy_url, resolve_known_proxy


logger = logging.getLogger(__name__)

_bridges: dict[str, LocalProxyBridge] = {}
_procs: dict[str, subprocess.Popen] = {}

# ManifestLocation::kUnpacked. Chrome 137+ (Google Chrome 152 here) hard-ignores
# --load-extension; persisting an unpacked install in Preferences is how a
# Load unpacked on chrome://extensions survives restarts.
_UNPACKED_LOCATION = 4
_CREATION_FLAGS = 38  # REQUIRE_MODERN_MANIFEST_VERSION | ALLOW_FILE_ACCESS | FOLLOW_SYMLINKS
_WINDOWS_EPOCH_US = 11_644_473_600_000_000


def extension_dir() -> Path:
    return BASE_DIR / "extension"


def nick_extension_dir(user_data_dir: Path | str) -> Path:
    return Path(user_data_dir) / "FlowKitExtension"


def _strip_dnr_from_copy(dst: Path) -> None:
    """Drop static DNR from a nick copy.

    Chrome 152 indexes `declarative_net_request` into `_metadata` only when
    the user clicks Load unpacked. A seeded copy never gets that file, so
    chrome://extensions warns: "failed to load properly… intercept network
    requests." Batch transport signs RPCs in the Flow tab and uses
    webRequest; the DNR ruleset only rewrites legacy aisandbox-pa headers.
    """
    manifest_path = dst / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    changed = False
    if "declarative_net_request" in manifest:
        manifest.pop("declarative_net_request", None)
        changed = True
    perms = [p for p in (manifest.get("permissions") or []) if p != "declarativeNetRequest"]
    if perms != list(manifest.get("permissions") or []):
        manifest["permissions"] = perms
        changed = True
    if changed:
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    rules = dst / "rules.json"
    if rules.exists():
        rules.unlink()


def sync_extension_copy(
    user_data_dir: Path | str, profile_id: str | None = None,
) -> Path:
    """Per-nick copy so two Chromes do not share unpacked `_metadata`.

    Profile 1 and nick-a both loading `/home/pc/flowkit/extension` makes
    Chrome fail DNR indexing: "failed to load properly… intercept network
    requests."
    """
    src = extension_dir().resolve()
    dst = nick_extension_dir(user_data_dir)
    if dst.exists():
        shutil.rmtree(dst)

    def ignore(directory: str, names: list[str]) -> list[str]:
        return [n for n in names if n in {"_metadata", "__pycache__", ".git"}]

    shutil.copytree(src, dst, ignore=ignore)
    _strip_dnr_from_copy(dst)
    _patch_manifest_for_browser(dst, profile_id)
    if profile_id:
        (dst / "profile.json").write_text(
            json.dumps({"profileId": profile_id}) + "\n", encoding="utf-8",
        )
        bg = dst / "background.js"
        if bg.exists():
            baked = json.dumps(profile_id)
            text = bg.read_text(encoding="utf-8")
            updated = text.replace(
                "const BAKED_PROFILE_ID = null;",
                f"const BAKED_PROFILE_ID = {baked};",
                1,
            )
            if updated != text:
                bg.write_text(updated, encoding="utf-8")
    _bump_copy_version(dst)
    return dst.resolve()


def _patch_manifest_for_browser(dst: Path, profile_id: str | None) -> None:
    """Norton Neo crashes ~20-50s after injected.js wraps window.fetch /
    XMLHttpRequest on flow.google.com — its anti-fingerprint patching
    collides with ours. The wraps only OBSERVE traffic (chat-session sniff,
    legacy TRPC urls); the batch RPC path uses its own XHR, so for Neo we
    strip them and also move the content script to document_idle for a
    safety margin. Other browsers keep the full injected.js."""
    try:
        acc = get_account(profile_id) if profile_id else None
    except Exception:
        acc = None
    if str((acc or {}).get("browser") or "").strip().lower() != "neo":
        return
    manifest_path = dst / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        changed = False
        for cs in manifest.get("content_scripts") or []:
            if cs.get("run_at") != "document_idle":
                cs["run_at"] = "document_idle"
                changed = True
        if changed:
            manifest_path.write_text(
                json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    inj = dst / "injected.js"
    if inj.exists():
        text = inj.read_text(encoding="utf-8")
        start = text.find("const _originalFetch")
        end = text.find("window.__flowRunBatch")
        if start != -1 and end > start:
            inj.write_text(text[:start] + text[end:], encoding="utf-8")


def _bump_copy_version(dst: Path) -> None:
    """Change unpacked version so Chrome drops a stale service-worker cache.

    Same path + same manifest version keeps the old background.js in
    ScriptCache even after we overwrite the files on disk.
    """
    manifest_path = dst / "manifest.json"
    if not manifest_path.exists():
        return
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload = b""
    for name in ("background.js", "injected.js", "content.js", "profile.json"):
        path = dst / name
        if path.exists():
            payload += path.read_bytes()
    n = int(hashlib.sha256(payload or b"0").hexdigest()[:7], 16) % 100000
    base = str(manifest.get("version") or "0.3.0").split(".")
    major, minor = (base + ["0", "0"])[:2]
    manifest["version"] = f"{major}.{minor}.{n}"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def unpacked_extension_id(path: Path | str) -> str:
    """Chrome's path-based ID for an unpacked extension with no manifest key."""
    digest = hashlib.sha256(str(path).encode("utf-8")).digest()[:16]
    return "".join(chr(ord("a") + (b >> 4)) + chr(ord("a") + (b & 0xF)) for b in digest)


def _chrome_now() -> str:
    return str(int(time.time() * 1_000_000) + _WINDOWS_EPOCH_US)


def _permissions_from_manifest(manifest: dict) -> dict:
    scriptable = []
    for cs in manifest.get("content_scripts") or []:
        scriptable.extend(cs.get("matches") or [])
    return {
        "api": list(manifest.get("permissions") or []),
        "explicit_host": list(manifest.get("host_permissions") or []),
        "manifest_permissions": [],
        "scriptable_host": scriptable,
    }


def seed_unpacked_extension(user_data_dir: Path | str, ext_dir: Path | str | None = None) -> str:
    """Write an unpacked Flow Kit install into a Chrome user-data-dir.

    Google Chrome 152 prints `--load-extension is not allowed` and skips the
    flag. The same unpacked entry Profile 1 already has (location=4 + path)
    is what actually loads.
    """
    root = Path(user_data_dir)
    ext = Path(ext_dir) if ext_dir is not None else extension_dir()
    ext = ext.resolve()
    manifest_path = ext / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    ext_id = unpacked_extension_id(ext)
    perms = _permissions_from_manifest(manifest)
    now = _chrome_now()
    prefs_path = root / "Default" / "Preferences"
    prefs_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        prefs = json.loads(prefs_path.read_text(encoding="utf-8")) if prefs_path.exists() else {}
    except json.JSONDecodeError:
        prefs = {}
    if not isinstance(prefs, dict):
        prefs = {}
    extensions = prefs.setdefault("extensions", {})
    if not isinstance(extensions, dict):
        extensions = {}
        prefs["extensions"] = extensions
    ui = extensions.setdefault("ui", {})
    if not isinstance(ui, dict):
        ui = {}
        extensions["ui"] = ui
    ui["developer_mode"] = True
    settings = extensions.setdefault("settings", {})
    if not isinstance(settings, dict):
        settings = {}
        extensions["settings"] = settings
    shared = str(extension_dir().resolve())
    for eid, row in list(settings.items()):
        if eid == ext_id or not isinstance(row, dict):
            continue
        path = str(row.get("path") or "")
        # Drop the shared repo path AND foreign per-nick copies. A cloned
        # profile inherits the parent's seeded entries, so without this its
        # Chrome loads several FlowKit service workers at once — each claims
        # a different profileId on the agent WS (the "ghost nick" bug).
        if (path == shared
                or path.rstrip("/").endswith("/flowkit/extension")
                or path.rstrip("/").endswith("/FlowKitExtension")):
            settings.pop(eid, None)
    existing = settings.get(ext_id) if isinstance(settings.get(ext_id), dict) else {}
    if "declarativeNetRequest" not in (perms.get("api") or []):
        existing.pop("dnr_static_ruleset", None)
    existing.pop("service_worker_registration_info", None)
    settings[ext_id] = {
        **existing,
        "active_permissions": perms,
        "granted_permissions": perms,
        "creation_flags": _CREATION_FLAGS,
        "from_webstore": False,
        "location": _UNPACKED_LOCATION,
        "path": str(ext),
        "was_installed_by_default": False,
        "was_installed_by_oem": False,
        "was_pinned_by_default": False,
        "withholding_permissions": False,
        "newAllowFileAccess": True,
        "first_install_time": existing.get("first_install_time") or now,
        "last_update_time": now,
        "commands": {"_execute_action": {"was_assigned": True}},
        "service_worker_registration_info": {
            "version": str(manifest.get("version") or "0.3.0"),
        },
    }
    _drop_extension_sw_cache(root)
    tmp = prefs_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(prefs) + "\n", encoding="utf-8")
    tmp.replace(prefs_path)
    logger.info("seeded unpacked extension id=%s path=%s profile=%s", ext_id, ext, root)
    return ext_id


def _drop_extension_sw_cache(user_data_dir: Path | str) -> None:
    """Drop compiled SW so a rewritten background.js actually runs.

    ScriptCache without Database leaves Chrome at DidStartWorkerFail : 5
    (kErrorExists). Wipe both.
    """
    sw = Path(user_data_dir) / "Default" / "Service Worker"
    if sw.exists():
        shutil.rmtree(sw, ignore_errors=True)


# Alternate browser binaries selectable per nick via the account `browser`
# field. Each entry: candidate paths plus extra CLI args that browser needs
# (the unpacked Cốc Cốc build has no setuid sandbox, and on this Wayland-only
# box it must be told to use ozone/wayland explicitly).
_BROWSER_REGISTRY: dict[str, dict] = {
    "coccoc": {
        "bins": [
            str(Path.home() / ".flowkit" / "coccoc-browser" / "browser"),
            "/opt/coccoc/browser/browser",
            "coccoc-browser",
        ],
        "extra_args": ["--no-sandbox", "--ozone-platform=wayland"],
    },
    # Chromium variants — same extension, different UA-brand/device profile.
    # Sessions logged in on these register as different devices to Google
    # (distinct from the Chrome-clone pattern), so a fresh login here is a
    # genuinely new session, not a copied one. Note: cookie encryption is
    # per-vendor — a Chrome profile moved to Edge reads as signed-out; these
    # entries are for fresh logins, not profile migration.
    "edge": {
        "bins": [
            "microsoft-edge-stable",
            "microsoft-edge",
            "/opt/microsoft/msedge/msedge",
        ],
        "extra_args": [],
    },
    "brave": {
        "bins": [
            "brave-browser",
            "brave",
            "/opt/brave.com/brave/brave",
        ],
        "extra_args": [],
    },
    "vivaldi": {
        "bins": [
            "vivaldi-stable",
            "vivaldi",
            "/opt/vivaldi/vivaldi",
        ],
        "extra_args": [],
    },
    # Norton Neo — Chromium-based privacy browser; unpacked .deb lives under
    # ~/.flowkit/neo-browser (no setuid sandbox → --no-sandbox, ozone/wayland
    # on this box like coccoc).
    "neo": {
        "bins": [
            str(Path.home() / ".flowkit" / "neo-browser" / "opt" / "neo" / "neo" / "neo-browser"),
            str(Path.home() / ".flowkit" / "neo-browser" / "opt" / "neo" / "neo" / "chrome"),
            "neo-browser-stable",
        ],
        # Neo GPU process crashes on wayland+vulkan — force software raster.
        "extra_args": ["--no-sandbox", "--ozone-platform=wayland", "--disable-gpu"],
    },
}


def _chrome_bin(nick_id: str | None = None) -> str:
    browser = ""
    if nick_id:
        try:
            from agent.services.accounts import get_account
            acc = get_account(nick_id)
            browser = str((acc or {}).get("browser") or "").strip().lower()
        except Exception:
            browser = ""
    if browser:
        spec = _BROWSER_REGISTRY.get(browser)
        if spec is None:
            raise RuntimeError(f"unknown browser {browser!r} for nick {nick_id}")
        for cand in spec["bins"]:
            p = Path(cand)
            if p.is_file() and os.access(p, os.X_OK):
                return str(p)
            found = shutil.which(cand)
            if found:
                return found
        raise RuntimeError(f"browser {browser!r} configured but no binary found")
    for name in (
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        "google-chrome-unstable",
    ):
        found = shutil.which(name)
        if found:
            return found
    raise RuntimeError("no Chrome/Chromium binary on PATH")


def _browser_extra_args(nick_id: str | None = None) -> list[str]:
    if not nick_id:
        return []
    try:
        from agent.services.accounts import get_account
        acc = get_account(nick_id)
        browser = str((acc or {}).get("browser") or "").strip().lower()
    except Exception:
        return []
    spec = _BROWSER_REGISTRY.get(browser)
    return list(spec["extra_args"]) if spec else []


def chrome_data_dir(nick_id: str) -> Path:
    root = Path(os.environ.get("FLOW_CHROME_DIR", Path.home() / ".flowkit" / "chrome"))
    return root / nick_id


_PS_CACHE: dict[str, object] = {"ts": 0.0, "lines": []}
# The dashboard polls every 4s and asked launch_status() per nick, i.e. one
# pgrep fork per nick per poll. Chrome does not start or stop between two nicks
# in the same poll, so one snapshot serves them all.
_PS_TTL_S = 2.0


def _chrome_ps(force: bool = False) -> list[str]:
    """Cached `pgrep -af chrome` lines."""
    now = time.monotonic()
    if not force and now - float(_PS_CACHE["ts"] or 0) < _PS_TTL_S:
        return list(_PS_CACHE["lines"])  # type: ignore[arg-type]
    try:
        lines = subprocess.check_output(["pgrep", "-af", "chrome"], text=True).splitlines()
    except Exception:
        lines = []
    _PS_CACHE["ts"] = now
    _PS_CACHE["lines"] = lines
    return list(lines)


def invalidate_chrome_ps() -> None:
    """Drop the snapshot after anything that starts or kills a Chrome."""
    _PS_CACHE["ts"] = 0.0


def _is_gologin(account: Optional[dict]) -> bool:
    return str((account or {}).get("browser") or "").strip().lower() == "gologin"


def _nick_dir_pattern(nick_id: str) -> "re.Pattern":
    """Match this nick's browser processes without prefix collisions —
    'nick-a' must not match 'nick-a-dual'. Gologin nicks match BOTH the
    --gologin-profile marker and the legacy chrome dir, so a browser flip
    doesn't orphan the previous Chrome."""
    pats = [re.escape(str(chrome_data_dir(nick_id)))]
    try:
        if _is_gologin(get_account(nick_id)):
            from agent.services.gologin_service import gologin_ps_marker
            pats.append(re.escape(gologin_ps_marker(nick_id)))
    except Exception:
        pass
    return re.compile(r"(?:" + "|".join(pats) + r")(?![\w-])")


def _nick_lock_dir(nick_id: str) -> Path:
    """Profile dir holding the singleton lock — the gologin profile dir for
    orbita nicks, the Chrome user-data-dir otherwise."""
    try:
        acc = get_account(nick_id)
        if _is_gologin(acc):
            from agent.services.gologin_service import nick_profile_id, profile_dir
            pid = nick_profile_id(acc or {})
            if pid:
                return profile_dir(pid)
    except Exception:
        pass
    return chrome_data_dir(nick_id)


def find_running_chrome_pid(nick_id: str) -> Optional[int]:
    """Find OS PID of an already running Chrome browser for this nick."""
    pat = _nick_dir_pattern(nick_id)
    try:
        for line in _chrome_ps():
            # Main browser process matches user-data-dir and is not a child worker/renderer
            if pat.search(line) and "--type=" not in line:
                parts = line.strip().split()
                if parts and parts[0].isdigit():
                    return int(parts[0])
    except Exception:
        pass
    return None


def chrome_running(nick_id: str) -> bool:
    proc = _procs.get(nick_id)
    if proc is not None:
        code = proc.poll()
        if code is None:
            return True
        _procs.pop(nick_id, None)

    # If server was restarted, check if Chrome process is still running on OS
    pid = find_running_chrome_pid(nick_id)
    return pid is not None


def running_chrome_proxy_port(nick_id: str) -> Optional[int]:
    pat = _nick_dir_pattern(nick_id)
    try:
        for line in _chrome_ps():
            if pat.search(line) and "--proxy-server=http://127.0.0.1:" in line:
                m = re.search(r"--proxy-server=http://127\.0\.0\.1:(\d+)", line)
                if m:
                    return int(m.group(1))
    except Exception:
        pass
    return None


async def ensure_bridge(nick_id: str, parsed: ParsedProxy, port: int = 0) -> LocalProxyBridge:
    existing = _bridges.get(nick_id)
    if existing and existing.upstream == parsed and existing.port:
        return existing
    if existing:
        await existing.stop()
        _bridges.pop(nick_id, None)
    if not port:
        port = running_chrome_proxy_port(nick_id) or 0
    bridge = LocalProxyBridge(parsed, port=port)
    await bridge.start()
    _bridges[nick_id] = bridge
    return bridge


async def stop_bridge(nick_id: str) -> None:
    bridge = _bridges.pop(nick_id, None)
    if bridge:
        await bridge.stop()



def get_bridge(nick_id: str) -> Optional[LocalProxyBridge]:
    return _bridges.get(nick_id)


async def restore_nick_bridge(nick_id: str) -> Optional[LocalProxyBridge]:
    """Ensure the local proxy bridge is listening on the exact port expected by Chrome.
    
    Prevents ERR_PROXY_CONNECTION_FAILED and error pages when FlowKit restarts.
    """
    from agent.services.accounts import get_account
    account = get_account(nick_id)
    if not account or not account.get("proxy_url"):
        return None

    parsed = parse_proxy_url(account["proxy_url"])
    if not parsed.has_auth:
        return None

    # Determine what port Chrome was started with (if running)
    expected_port = running_chrome_proxy_port(nick_id) or 0
    bridge = await ensure_bridge(nick_id, parsed, port=expected_port)
    logger.info("Proxy bridge restored/verified for %s on port %d -> %s", nick_id, bridge.port, parsed.redacted)
    return bridge


async def restore_all_bridges() -> dict[str, int]:
    """Ensure proxy bridges are actively listening for ALL accounts with proxy_url on startup."""
    from agent.services.accounts import load_accounts
    restored = {}
    for acc in load_accounts():
        if acc.get("proxy_url"):
            try:
                bridge = await restore_nick_bridge(acc["id"])
                if bridge and bridge.port:
                    restored[acc["id"]] = bridge.port
            except Exception as exc:
                logger.warning("Could not restore bridge for %s: %s", acc["id"], exc)
    return restored


async def check_proxy(proxy_url: str) -> dict:
    resolved = resolve_known_proxy(proxy_url)
    parsed = parse_proxy_url(resolved)
    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("https://api.ipify.org", proxy=parsed.raw.rstrip("/")) as resp:
                body = (await resp.text()).strip()
                if resp.status != 200 or not body:
                    return {
                        "ok": False,
                        "proxy": parsed.redacted,
                        "error": f"ip lookup HTTP {resp.status}: {body[:200]}",
                    }
                return {"ok": True, "proxy": parsed.redacted, "egress_ip": body}
    except Exception as exc:
        return {"ok": False, "proxy": parsed.redacted, "error": str(exc)}


def _launch_args(
    nick_id: str,
    proxy_server: Optional[str],
    *,
    local_bridge: bool = False,
    extension: Optional[str] = None,
) -> list[str]:
    data = chrome_data_dir(nick_id)
    data.mkdir(parents=True, exist_ok=True)
    extension = extension or str(extension_dir().resolve())
    args = [
        _chrome_bin(nick_id),
        f"--user-data-dir={data}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-sync",
        "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
        # Harmless on Google Chrome 152 (it logs "not allowed" and ignores).
        # Still the right flag for Chromium builds.
        "--enable-unsafe-extension-debugging",
        "--disable-features=DisableLoadExtensionCommandLineSwitch",
        f"--load-extension={extension}",
        "https://flow.google.com/",
    ]
    # Wayland-only session: Chrome auto-detects X11 via a stale/dead DISPLAY
    # and dies with "Missing X server or $DISPLAY". Force ozone/wayland when
    # the wayland socket exists (harmless for Cốc Cốc which sets it anyway).
    if os.path.exists(f"/run/user/{os.getuid()}/wayland-0") and "--ozone-platform=wayland" not in args:
        args.insert(1, "--ozone-platform=wayland")
    args[1:1] = _browser_extra_args(nick_id)
    try:
        from agent.services.accounts import get_account
        _acc = get_account(nick_id) or {}
    except Exception:
        _acc = {}
    if _acc.get("cdp_debug"):
        # Port 0: Chrome picks a free port and writes it to DevToolsActivePort
        # in the user-data dir — no per-nick port bookkeeping needed.
        args.insert(1, "--remote-debugging-port=0")
        args.insert(2, "--remote-debugging-address=127.0.0.1")
    if proxy_server:
        extra = [f"--proxy-server={proxy_server}"]
        # Do NOT add <-loopback>. Chrome already bypasses 127.0.0.1, so the
        # extension can POST :8100 / WS :9222 in origin-form. Forcing those
        # through the bridge made uvicorn see `POST http://127.0.0.1:8100/...`
        # and 404 the netlog that pins r2v.
        args[1:1] = extra
    return args


async def launch_nick(nick_id: str) -> dict:
    account = get_account(nick_id)
    if account is None:
        raise KeyError(nick_id)
    if chrome_running(nick_id):
        existing_pid = _procs[nick_id].pid if nick_id in _procs else find_running_chrome_pid(nick_id)
        if account.get("proxy_url") and not _is_gologin(account):
            try:
                parsed = parse_proxy_url(account["proxy_url"])
                if parsed.has_auth:
                    await ensure_bridge(nick_id, parsed)
            except Exception as e:
                logger.warning("Could not re-ensure bridge for %s: %s", nick_id, e)
        return {
            "ok": True,
            "id": nick_id,
            "already_running": True,
            "pid": existing_pid,
            "data_dir": str(chrome_data_dir(nick_id)),
        }

    if _is_gologin(account):
        from agent.services import gologin_service
        return await gologin_service.launch(nick_id, account)

    proxy_server = None
    proxy_display = ""
    local_bridge = False
    if not account.get("proxy_url"):
        try:
            from agent.services.proxy_pool import get_verified_proxy_for_nick
            auto_proxy = await asyncio.to_thread(get_verified_proxy_for_nick, nick_id)
            if auto_proxy:
                account["proxy_url"] = auto_proxy
                from agent.services.accounts import upsert_account
                upsert_account(account)
                logger.info("Auto-assigned proxy on launch for %s: %s", nick_id, auto_proxy)
        except Exception as exc:
            logger.warning("Could not auto-assign proxy on launch for %s: %s", nick_id, exc)

    if account.get("proxy_url"):
        parsed = parse_proxy_url(account["proxy_url"])
        proxy_display = parsed.redacted
        if parsed.has_auth:
            if parsed.scheme.startswith("socks"):
                raise ProxyURLError(
                    "Chrome cannot send SOCKS5 user:pass; use an HTTP proxy "
                    "like http://user:pass@host:port"
                )
            bridge = await ensure_bridge(nick_id, parsed)
            proxy_server = bridge.listen_url
            local_bridge = True
        else:
            proxy_server = parsed.chrome_server()

    data = chrome_data_dir(nick_id)
    drop_stale_chrome_lock(data)
    ext_copy = sync_extension_copy(data, profile_id=nick_id)
    seed_unpacked_extension(data, ext_copy)
    args = _launch_args(
        nick_id,
        proxy_server,
        local_bridge=local_bridge,
        extension=str(ext_copy),
    )
    env = os.environ.copy()
    wayland_socket = f"/run/user/{os.getuid()}/wayland-0"
    if os.path.exists(wayland_socket):
        env["WAYLAND_DISPLAY"] = "wayland-0"
        env["XDG_RUNTIME_DIR"] = f"/run/user/{os.getuid()}"
        # A bogus DISPLAY makes Chrome pick X11 and die on a Wayland-only box.
        env.pop("DISPLAY", None)
    else:
        if not env.get("DISPLAY"):
            env["DISPLAY"] = ":0"
        if not env.get("XDG_RUNTIME_DIR"):
            env["XDG_RUNTIME_DIR"] = f"/run/user/{os.getuid()}"
    err_path = chrome_data_dir(nick_id) / "chrome.stderr.log"
    err_f = open(err_path, "ab")
    try:
        proc = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=err_f,
            start_new_session=True,
            env=env,
        )
    finally:
        err_f.close()
    _procs[nick_id] = proc
    logger.info(
        "launched Chrome nick=%s pid=%s proxy=%s",
        nick_id, proc.pid, proxy_display or "none",
    )
    await asyncio.sleep(0.4)
    if proc.poll() is not None:
        raise RuntimeError(
            f"Chrome exited immediately (code {proc.returncode}). "
            "Is a display available?"
        )
    invalidate_chrome_ps()
    return {
        "ok": True,
        "id": nick_id,
        "already_running": False,
        "pid": proc.pid,
        "data_dir": str(chrome_data_dir(nick_id)),
        "proxy": proxy_display,
        "chrome_proxy": proxy_server,
    }


def _pid_is_live(pid: int) -> bool:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except (FileNotFoundError, ProcessLookupError, OSError):
        return False
    comm_end = stat.rfind(")")
    state = stat[comm_end + 2] if comm_end != -1 and comm_end + 2 < len(stat) else ""
    return state not in {"", "Z"}


def get_all_chrome_pids_for_nick(nick_id: str) -> list[int]:
    """Find ALL Chrome process PIDs (main, renderers, utility, crashpad) for this nick."""
    pat = _nick_dir_pattern(nick_id)
    pids = []
    try:
        # Kill paths must see the live process list, never a 2s-old snapshot.
        for line in _chrome_ps(force=True):
            if pat.search(line):
                parts = line.strip().split()
                if parts and parts[0].isdigit():
                    pids.append(int(parts[0]))
    except Exception:
        pass
    return pids


def drop_stale_chrome_lock(user_data_dir: Path | str, force: bool = False) -> bool:
    """Remove SingletonLock if its pid is gone or a zombie, or unconditionally if force=True."""
    data = Path(user_data_dir)
    lock = data / "SingletonLock"
    if not lock.exists() and not lock.is_symlink():
        return False
    if not force:
        live = False
        try:
            target = os.readlink(lock)
            pid = int(target.rsplit("-", 1)[-1])
            live = _pid_is_live(pid)
        except (OSError, ValueError):
            live = False
        if live:
            return False

    for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        try:
            (data / name).unlink()
        except FileNotFoundError:
            pass
        except OSError:
            logger.warning("could not remove %s", data / name)
    return True


def cleanup_orphaned_chrome(nick_id: str, force: bool = False) -> int:
    """Detect and clean up orphaned or stuck Chrome processes for this nick."""
    pids = get_all_chrome_pids_for_nick(nick_id)
    if not pids:
        drop_stale_chrome_lock(_nick_lock_dir(nick_id), force=force)
        return 0

    killed = 0
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            killed += 1
        except (ProcessLookupError, PermissionError, OSError):
            pass

    time.sleep(0.3)
    for pid in pids:
        if _pid_is_live(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass

    _procs.pop(nick_id, None)
    drop_stale_chrome_lock(_nick_lock_dir(nick_id), force=True)
    logger.info("Cleaned up %d orphaned Chrome processes for nick=%s", killed, nick_id)
    return killed


async def stop_nick(nick_id: str) -> bool:
    """Stop Chrome and all child processes for this nick cleanly."""
    pids = get_all_chrome_pids_for_nick(nick_id)
    proc = _procs.pop(nick_id, None)
    if proc is not None and proc.pid not in pids:
        pids.append(proc.pid)

    stopped = False
    if pids:
        stopped = True
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                pass

        for _ in range(25):
            remaining = [p for p in pids if _pid_is_live(p)]
            if not remaining:
                break
            await asyncio.sleep(0.1)

        for pid in pids:
            if _pid_is_live(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError, OSError):
                    pass

    drop_stale_chrome_lock(_nick_lock_dir(nick_id), force=True)
    await stop_bridge(nick_id)
    invalidate_chrome_ps()
    return stopped


async def ensure_nick_active(nick_id: str) -> dict:
    """Ensure nick has active Chrome and its proxy bridge is listening on startup.
    
    Prevents orphaned processes and eliminates ERR_PROXY_CONNECTION_FAILED permanently.
    """
    from agent.services.accounts import get_account
    account = get_account(nick_id)
    if not account:
        raise KeyError(nick_id)

    # 1. Restore/ensure bridge first so Chrome never encounters ERR_PROXY_CONNECTION_FAILED
    #    (gologin nicks carry their proxy on the orbita profile — no bridge)
    if account.get("proxy_url") and not _is_gologin(account):
        try:
            parsed = parse_proxy_url(account["proxy_url"])
            if parsed.has_auth:
                expected_port = running_chrome_proxy_port(nick_id) or 0
                await ensure_bridge(nick_id, parsed, port=expected_port)
        except Exception as e:
            logger.warning("ensure_nick_active: bridge setup error for %s: %s", nick_id, e)

    # 2. Check if Chrome is already alive
    if chrome_running(nick_id):
        existing_pid = _procs[nick_id].pid if nick_id in _procs else find_running_chrome_pid(nick_id)
        logger.info("ensure_nick_active: Chrome already alive for %s (PID %s)", nick_id, existing_pid)
        return {
            "ok": True,
            "id": nick_id,
            "already_running": True,
            "pid": existing_pid,
            "data_dir": str(chrome_data_dir(nick_id)),
        }

    # 3. Clean any orphaned lockfiles and launch Chrome
    drop_stale_chrome_lock(_nick_lock_dir(nick_id), force=True)
    return await launch_nick(nick_id)


def launch_status(nick_id: str) -> dict:
    proc = _procs.get(nick_id)
    running = chrome_running(nick_id)
    bridge = _bridges.get(nick_id)
    return {
        "chrome_running": running,
        "pid": proc.pid if running and proc is not None else None,
        "data_dir": str(chrome_data_dir(nick_id)),
        "bridge_port": bridge.port if bridge else None,
    }
