"""Stable nick ownership and two-phase routing for the local VPN gateway."""
from __future__ import annotations

import hashlib
import ipaddress
import urllib.request
from urllib.parse import quote, unquote, urlsplit, urlunsplit


def is_surfshark_url(url: str) -> bool:
    try:
        parts = urlsplit(url)
        return (
            parts.hostname in {"127.0.0.1", "localhost", "::1"}
            and parts.port in {18888, 11080}
            and unquote(parts.username or "").split("__", 1)[0] == "surf"
        )
    except ValueError:
        return False


def owner_id(nick_id: str) -> str:
    # Case-sensitive IDs must not collide after punctuation sanitization.
    return "nick_" + hashlib.sha256(nick_id.encode()).hexdigest()[:32]


def bind_nick_proxy(url: str, nick_id: str, *, session: str | None = None,
                    prepare: bool = False) -> str:
    if not is_surfshark_url(url):
        return url
    parts = urlsplit(url)
    user, _, tail = unquote(parts.username or "").partition("__")
    params = dict(item.split(".", 1) for item in tail.split(";") if "." in item)
    params["owner"] = owner_id(nick_id)
    params["sessid"] = session or params.get("sessid") or owner_id(nick_id)
    params["sessttl"] = params.pop("ttl", params.get("sessttl", "60"))
    params.pop("prepare", None)
    if prepare:
        params["prepare"] = "1"
    username = user + "__" + ";".join(f"{k}.{v}" for k, v in sorted(params.items()))
    credentials = quote(username, safe=";._-") + ":" + quote(unquote(parts.password or ""), safe="")
    host = f"[{parts.hostname}]" if ":" in (parts.hostname or "") else parts.hostname
    return urlunsplit(parts._replace(netloc=f"{credentials}@{host}:{parts.port}"))


def probe_egress(proxy_url: str, timeout: int = 60) -> str:
    """Require a public IPv4 result; never silently accept a missing IP."""
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
    )
    with opener.open("https://api.ipify.org", timeout=timeout) as response:
        value = response.read(128).decode("ascii").strip()
    ip = ipaddress.ip_address(value)
    if ip.version != 4 or not ip.is_global:
        raise ValueError("EGRESS_IP_UNVERIFIED")
    return str(ip)


def monitored_proxy_urls(pool: list[str], accounts: list[dict]) -> set[str]:
    # Historical sessions are aliases, not spare proxies. Probing them creates
    # stale health rows and previously triggered accidental rotations.
    return {p.strip() for p in pool if p.strip() and not is_surfshark_url(p.strip())} | {
        a["proxy_url"].strip() for a in accounts if a.get("proxy_url")
    }
