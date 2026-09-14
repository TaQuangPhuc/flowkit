"""Local HTTP proxy that injects upstream auth Chrome cannot put on --proxy-server.

Chrome's --proxy-server flag strips user:pass. Each nick therefore talks to a
127.0.0.1 forwarder; the forwarder adds Proxy-Authorization and sends the
request to the sticky residential proxy. Destinations on loopback go direct so
the extension can still reach :8100 / :9222.
"""
from __future__ import annotations

import asyncio
import base64
import logging
from typing import Optional

from agent.services.proxy_url import ParsedProxy

logger = logging.getLogger(__name__)

_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1", "0.0.0.0"}


def _is_local_host(host: str) -> bool:
    h = (host or "").strip("[]").lower()
    return h in _LOCAL_HOSTS or h.endswith(".localhost")


class LocalProxyBridge:
    """HTTP CONNECT (and plain HTTP) forwarder bound to 127.0.0.1."""

    def __init__(self, upstream: ParsedProxy, port: int = 0):
        self.upstream = upstream
        self._requested_port = port
        self.port: Optional[int] = None
        self._server: Optional[asyncio.AbstractServer] = None
        token = f"{upstream.username}:{upstream.password}".encode()
        self._auth = "Basic " + base64.b64encode(token).decode()

    @property
    def listen_url(self) -> str:
        if not self.port:
            raise RuntimeError("bridge is not listening")
        return f"http://127.0.0.1:{self.port}"

    def switch_upstream(self, new_upstream: ParsedProxy) -> None:
        self.upstream = new_upstream
        token = f"{new_upstream.username}:{new_upstream.password}".encode()
        self._auth = "Basic " + base64.b64encode(token).decode()
        logger.info(
            "proxy bridge 127.0.0.1:%d switched upstream to %s",
            self.port, self.upstream.redacted,
        )

    async def start(self) -> int:
        if self._server is not None:
            return int(self.port or 0)
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", self._requested_port)
        self.port = int(self._server.sockets[0].getsockname()[1])
        logger.info(
            "proxy bridge 127.0.0.1:%d -> %s",
            self.port, self.upstream.redacted,
        )
        return self.port

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        self.port = None

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            head = await _read_http_head(reader)
            if not head:
                return
            first, rest = _split_request(head)
            method, target, version = first
            if method == "CONNECT":
                host, port = _host_port(target, 443)
                await self._connect(reader, writer, host, port, version)
            else:
                await self._http(reader, writer, head, method, target)
        except Exception as exc:
            logger.debug("proxy bridge client error: %s", exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _connect(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        host: str,
        port: int,
        version: str,
    ) -> None:
        if _is_local_host(host):
            remote_r, remote_w = await asyncio.open_connection(host, port)
        else:
            remote_r, remote_w = await asyncio.open_connection(
                self.upstream.host, self.upstream.port
            )
            req = (
                f"CONNECT {host}:{port} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                f"Proxy-Authorization: {self._auth}\r\n"
                f"Proxy-Connection: Keep-Alive\r\n"
                f"\r\n"
            ).encode()
            remote_w.write(req)
            await remote_w.drain()
            reply = await _read_http_head(remote_r)
            status = _status_line(reply)
            if not status.startswith("HTTP/") or " 200 " not in status:
                writer.write(reply or b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await writer.drain()
                remote_w.close()
                return
        writer.write(f"{version} 200 Connection Established\r\n\r\n".encode())
        await writer.drain()
        await _pipe(reader, writer, remote_r, remote_w)

    async def _http(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        head: bytes,
        method: str,
        target: str,
    ) -> None:
        # Absolute-form URL from an HTTP proxy client.
        host, port = _origin_from_target(target)
        if _is_local_host(host):
            remote_r, remote_w = await asyncio.open_connection(host, port)
            # Chrome sends absolute-form (`POST http://127.0.0.1:8100/path`)
            # to the proxy. Origin servers (uvicorn) treat that as the path
            # (`POST http%3A//...`) and 404. Rewrite to origin-form.
            remote_w.write(_origin_form_head(head, target))
            await remote_w.drain()
        else:
            remote_r, remote_w = await asyncio.open_connection(
                self.upstream.host, self.upstream.port
            )
            remote_w.write(_inject_proxy_auth(head, self._auth))
            await remote_w.drain()
        await _pipe(reader, writer, remote_r, remote_w)


async def _read_http_head(reader: asyncio.StreamReader) -> bytes:
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = await asyncio.wait_for(reader.read(4096), timeout=30)
        if not chunk:
            break
        data += chunk
        if len(data) > 65536:
            raise ValueError("HTTP headers too large")
    return data


def _split_request(head: bytes) -> tuple[tuple[str, str, str], bytes]:
    text = head.decode("iso-8859-1", errors="replace")
    line, _, rest = text.partition("\r\n")
    parts = line.split(" ")
    if len(parts) < 3:
        raise ValueError("malformed request line")
    method, target, version = parts[0], parts[1], parts[2]
    return (method.upper(), target, version), rest.encode("iso-8859-1")


def _status_line(head: bytes) -> str:
    if not head:
        return ""
    return head.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="replace")


def _host_port(target: str, default_port: int) -> tuple[str, int]:
    if target.startswith("[") and "]" in target:
        host, _, port = target[1:].partition("]")
        if port.startswith(":"):
            return host, int(port[1:])
        return host, default_port
    if ":" in target:
        host, port = target.rsplit(":", 1)
        if port.isdigit():
            return host, int(port)
    return target, default_port


def _origin_from_target(target: str) -> tuple[str, int]:
    if target.startswith("http://") or target.startswith("https://"):
        from urllib.parse import urlparse
        parsed = urlparse(target)
        host = parsed.hostname or ""
        default = 443 if parsed.scheme == "https" else 80
        return host, int(parsed.port or default)
    return _host_port(target, 80)


def _origin_form_head(head: bytes, target: str) -> bytes:
    """Turn an HTTP-proxy absolute-form request into origin-form.

    `POST http://127.0.0.1:8100/api/ext/netlog HTTP/1.1` →
    `POST /api/ext/netlog HTTP/1.1`. Leaves origin-form and CONNECT
    targets unchanged.
    """
    if not (target.startswith("http://") or target.startswith("https://")):
        return head
    from urllib.parse import urlparse
    parsed = urlparse(target)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    text = head.decode("iso-8859-1", errors="replace")
    line, sep, rest = text.partition("\r\n")
    parts = line.split(" ")
    if len(parts) < 3:
        return head
    method, version = parts[0], parts[2]
    return f"{method} {path} {version}{sep}{rest}".encode("iso-8859-1")


def _inject_proxy_auth(head: bytes, auth: str) -> bytes:
    text = head.decode("iso-8859-1", errors="replace")
    if "Proxy-Authorization:" in text:
        return head
    line, sep, rest = text.partition("\r\n")
    if not sep:
        return head
    return f"{line}\r\nProxy-Authorization: {auth}\r\n{rest}".encode("iso-8859-1")


async def _pipe(
    client_r: asyncio.StreamReader,
    client_w: asyncio.StreamWriter,
    remote_r: asyncio.StreamReader,
    remote_w: asyncio.StreamWriter,
) -> None:
    async def one(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
        try:
            while True:
                chunk = await src.read(65536)
                if not chunk:
                    break
                dst.write(chunk)
                await dst.drain()
        except Exception:
            pass
        finally:
            try:
                dst.close()
            except Exception:
                pass

    await asyncio.gather(one(client_r, remote_w), one(remote_r, client_w))
