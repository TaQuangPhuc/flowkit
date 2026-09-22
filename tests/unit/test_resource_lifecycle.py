"""Regressions for sqlite and proxy sockets surviving their request lifetime."""
import asyncio
import gc
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.services import flow_failover, proxy_forward
from agent.services.incident_manager import IncidentManager
from agent.services.proxy_url import parse_proxy_url


def test_sqlite_connections_close_without_garbage_collection(tmp_path, monkeypatch):
    monkeypatch.setattr(flow_failover, "DB_PATH", tmp_path / "replay.db")
    manager = IncidentManager(tmp_path / "incidents.db")
    gc.collect()
    before = len(list(Path('/proc/self/fd').iterdir()))
    enabled = gc.isenabled()
    gc.disable()
    held = []
    try:
        for _ in range(300):
            for factory in (flow_failover._get_conn, manager._get_connection):
                with factory() as conn:
                    assert conn.execute("SELECT 1").fetchone()[0] == 1
                    held.append(conn)
        assert len(list(Path('/proc/self/fd').iterdir())) <= before + 2
        for conn in held:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                conn.execute("SELECT 1")
        for factory in (flow_failover._get_conn, manager._get_connection):
            with pytest.raises(RuntimeError):
                with factory() as conn:
                    raise RuntimeError("request failed")
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                conn.execute("SELECT 1")
    finally:
        if enabled:
            gc.enable()


async def test_failed_proxy_handshake_closes_upstream_socket(monkeypatch):
    disconnected = asyncio.Event()
    async def upstream(reader, writer):
        try:
            await reader.read()
            disconnected.set()
        finally:
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_server(upstream, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    bridge = proxy_forward.LocalProxyBridge(parse_proxy_url(f"http://127.0.0.1:{port}"))
    writer = MagicMock()
    writer.drain = AsyncMock()
    monkeypatch.setattr(proxy_forward, "_read_http_head", AsyncMock(side_effect=OSError("handshake failed")))
    try:
        with pytest.raises(OSError, match="handshake failed"):
            await bridge._connect(asyncio.StreamReader(), writer, "example.com", 443, "HTTP/1.1")
        await asyncio.wait_for(disconnected.wait(), timeout=2)
        assert not bridge._active_writers
    finally:
        server.close()
        await server.wait_closed()


async def test_restart_does_not_wait_for_its_own_process(monkeypatch):
    from agent.api import system
    shield = MagicMock()
    shield.wait_until_idle = AsyncMock()
    controller = MagicMock()
    controller.drain = AsyncMock()
    monkeypatch.setattr(system, "get_request_shield", lambda: shield)
    monkeypatch.setattr("agent.worker.processor.get_worker_controller", lambda: controller)
    process = MagicMock()
    process.wait = AsyncMock(return_value=0)
    spawn = AsyncMock(return_value=process)
    monkeypatch.setattr(system.asyncio, "create_subprocess_exec", spawn)
    await system._background_graceful_restart()
    spawn.assert_awaited_once_with("systemctl", "--user", "--no-block", "restart", "flowkit")
