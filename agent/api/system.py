"""System administration, request shielding, hot-reloading and graceful reload endpoints."""
from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from agent.services.request_shield import get_request_shield

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/system", tags=["system"])

# Process start time for tracking modified files
_PROCESS_START_TIME = time.time()


class ModuleReloadRequest(BaseModel):
    modules: Optional[List[str]] = None


class ModuleReloadResponse(BaseModel):
    ok: bool
    reloaded_modules: List[str]
    errors: List[str]
    elapsed_ms: float
    message: str


@router.get("/shield-status")
async def get_shield_status():
    """Get real-time status of the Client Request Shield."""
    shield = get_request_shield()
    return shield.get_status()


@router.post("/prepare-reload")
async def prepare_reload():
    """Put the server into graceful draining mode before a restart or code update."""
    shield = get_request_shield()
    shield.set_draining(True)
    return {
        "ok": True,
        "message": "Server set to draining mode. New heavy requests will be paused.",
        "shield": shield.get_status(),
    }


@router.post("/cancel-reload")
async def cancel_reload():
    """Cancel graceful draining mode and resume normal operations."""
    shield = get_request_shield()
    shield.set_draining(False)
    return {
        "ok": True,
        "message": "Draining mode cancelled. Server accepting all requests.",
        "shield": shield.get_status(),
    }


@router.post("/reload-modules", response_model=ModuleReloadResponse)
async def reload_modules(body: Optional[ModuleReloadRequest] = None):
    """Hot-reload modified Python modules in memory without restarting the server.
    
    Zero downtime, zero connection drops, zero interrupted client requests!
    """
    t0 = time.monotonic()
    modules_to_reload = body.modules if body and body.modules else []
    
    # Auto-detect modified modules under agent/ if not explicitly passed
    if not modules_to_reload:
        agent_dir = Path(__file__).resolve().parent.parent
        for py_file in agent_dir.rglob("*.py"):
            if py_file.name.startswith("."):
                continue
            try:
                mtime = py_file.stat().st_mtime
                if mtime > _PROCESS_START_TIME:
                    rel = py_file.relative_to(agent_dir.parent)
                    mod_name = str(rel.with_suffix("")).replace("/", ".")
                    if mod_name in sys.modules:
                        modules_to_reload.append(mod_name)
            except Exception:
                pass

    reloaded = []
    errors = []

    for mod_name in modules_to_reload:
        # Don't reload main or config directly to preserve running event loop state
        if mod_name in {"agent.main", "agent.config"}:
            continue
        if mod_name in sys.modules:
            try:
                mod = sys.modules[mod_name]
                importlib.reload(mod)
                reloaded.append(mod_name)
                logger.info("Hot-reloaded module: %s", mod_name)
            except Exception as e:
                err_msg = f"{mod_name}: {e}"
                errors.append(err_msg)
                logger.warning("Failed to hot-reload %s: %s", mod_name, e)

    elapsed = round((time.monotonic() - t0) * 1000, 2)
    msg = f"Đã hot-reload thành công {len(reloaded)} module(s) trong {elapsed}ms (Zero downtime)"
    if errors:
        msg += f", {len(errors)} lỗi."

    return ModuleReloadResponse(
        ok=len(errors) == 0,
        reloaded_modules=reloaded,
        errors=errors,
        elapsed_ms=elapsed,
        message=msg,
    )


async def _background_graceful_restart():
    """Background task to wait for active requests and then trigger restart."""
    shield = get_request_shield()
    shield.set_draining(True)
    logger.info("Graceful restart triggered. Waiting for client requests to drain...")
    await shield.wait_until_idle(timeout=45.0)
    
    # Wait for worker tasks
    try:
        from agent.worker.processor import get_worker_controller
        controller = get_worker_controller()
        controller.request_shutdown()
        await controller.drain(timeout=60.0)
    except Exception:
        pass

    logger.info("All client requests drained. Executing systemctl restart...")
    os.system("systemctl --user restart flowkit")


@router.post("/graceful-restart")
async def trigger_graceful_restart(background_tasks: BackgroundTasks):
    """Trigger a graceful restart of the FlowKit service in the background.
    
    Waits for all active in-flight client requests and worker tasks to finish cleanly first!
    """
    background_tasks.add_task(_background_graceful_restart)
    return {
        "ok": True,
        "message": "Graceful restart initiated. Draining client requests cleanly before restart.",
    }
