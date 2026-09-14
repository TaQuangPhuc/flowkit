"""Parse and redact HTTP/SOCKS proxy URLs, including user:pass in the URL."""
from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlparse


class ProxyURLError(ValueError):
    pass


_SCHEMES = {"http", "https", "socks5", "socks5h"}


@dataclass(frozen=True)
class ParsedProxy:
    scheme: str
    username: str
    password: str
    host: str
    port: int
    raw: str

    @property
    def has_auth(self) -> bool:
        return bool(self.username or self.password)

    @property
    def origin(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    @property
    def redacted(self) -> str:
        if self.has_auth:
            user = self.username or "user"
            return f"{self.scheme}://{user}:***@{self.host}:{self.port}"
        return self.origin

    def chrome_server(self) -> str:
        """Value Chrome's --proxy-server flag accepts (no userinfo)."""
        if self.scheme.startswith("socks"):
            return f"socks5://{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"


def parse_proxy_url(raw: str) -> ParsedProxy:
    text = (raw or "").strip()
    if not text:
        raise ProxyURLError("empty proxy url")
    parsed = urlparse(text)
    scheme = (parsed.scheme or "").lower()
    if scheme not in _SCHEMES:
        raise ProxyURLError(
            f"unsupported proxy scheme {scheme or '(none)'}; "
            "use http://user:pass@host:port"
        )
    host = parsed.hostname
    if not host:
        raise ProxyURLError("proxy url missing host")
    default_port = 443 if scheme == "https" else 80
    if scheme.startswith("socks"):
        default_port = 1080
    port = parsed.port or default_port
    return ParsedProxy(
        scheme=scheme,
        username=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        host=host,
        port=int(port),
        raw=text,
    )


def redact_proxy_url(raw: str) -> str:
    if not (raw or "").strip():
        return ""
    try:
        return parse_proxy_url(raw).redacted
    except ProxyURLError:
        return "(invalid proxy url)"


def resolve_known_proxy(raw: str) -> str:
    """If raw has redacted password '***', find matching proxy in pool or accounts."""
    text = (raw or "").strip()
    if not text or ":***@" not in text:
        return text
    try:
        target = parse_proxy_url(text)
    except Exception:
        return text

    # Search in pool
    try:
        from agent.services.proxy_pool import load_proxy_pool
        for p in load_proxy_pool().get("proxies", []):
            try:
                candidate = parse_proxy_url(p)
                if candidate.host == target.host and candidate.port == target.port:
                    if not target.username or candidate.username == target.username:
                        return candidate.raw
            except Exception:
                continue
    except Exception:
        pass

    # Search in accounts
    try:
        from agent.services.accounts import load_accounts
        for acc in load_accounts():
            p = acc.get("proxy_url")
            if p:
                try:
                    candidate = parse_proxy_url(p)
                    if candidate.host == target.host and candidate.port == target.port:
                        if not target.username or candidate.username == target.username:
                            return candidate.raw
                except Exception:
                    continue
    except Exception:
        pass

    return text

