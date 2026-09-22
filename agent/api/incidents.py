"""Incident & Observability REST & SSE API Router."""

import asyncio
import json
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from agent.services.central_watchdog import CentralWatchdog
from agent.services.incident_manager import get_incident_manager

router = APIRouter(prefix="/api/system/incidents", tags=["System Incidents & Observability"])


@router.get("")
async def list_incidents(
    module: Optional[str] = Query(None, description="Filter by module (lookbook, tvc, batch, worker, proxy)"),
    status: Optional[str] = Query(None, description="Filter by status (OPEN, AUTO_HEALING, RESOLVED, FAILED)"),
    severity: Optional[str] = Query(None, description="Filter by severity (INFO, WARNING, CRITICAL, HEALED)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """Retrieve paginated incidents with multi-attribute filtering."""
    mgr = get_incident_manager()
    incidents = mgr.get_incidents(module=module, status=status, severity=severity, limit=limit, offset=offset)
    return {
        "ok": True,
        "total": len(incidents),
        "limit": limit,
        "offset": offset,
        "incidents": incidents
    }


@router.get("/summary")
async def get_incident_summary():
    """Retrieve high-level system health score, incident counts, and module breakdowns."""
    mgr = get_incident_manager()
    return mgr.get_summary()


@router.post("/sweep")
async def trigger_watchdog_sweep():
    """Trigger an immediate sweep and self-healing pass across all 5 domains."""
    watchdog = CentralWatchdog()
    # run_sweep() is blocking (proxy probes + revival cycle hit the network),
    # so it must not run on the event loop or it stalls the whole API.
    sweep_results = await asyncio.to_thread(watchdog.run_sweep)
    mgr = get_incident_manager()
    summary = mgr.get_summary()
    return {
        "ok": True,
        "message": "Watchdog sweep executed successfully",
        "sweep_results": sweep_results,
        "summary": summary
    }


@router.post("/{incident_id}/resolve")
async def resolve_incident(incident_id: str, payload: dict = None):
    """Mark an incident as manually or automatically resolved."""
    mgr = get_incident_manager()
    action = (payload or {}).get("action_taken", "MANUALLY_RESOLVED")
    severity = (payload or {}).get("severity", "HEALED")
    success = mgr.resolve_incident(incident_id, action_taken=action, severity=severity)
    if not success:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")
    return {"ok": True, "incident_id": incident_id, "status": "RESOLVED"}


@router.get("/stream")
async def stream_incidents(request: Request):
    """Real-time Server-Sent Events (SSE) stream for live incident notifications and self-healing events."""
    mgr = get_incident_manager()
    queue = mgr.register_listener()

    async def event_generator():
        try:
            # Yield initial connect ping
            init_data = json.dumps({"event": "connected", "summary": mgr.get_summary()})
            yield f"data: {init_data}\n\n"

            while True:
                if await request.is_disconnected():
                    break
                try:
                    # Wait for next event or send keepalive every 15s
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    # Keepalive ping
                    yield f": keepalive {time.time()}\n\n"
        finally:
            mgr.unregister_listener(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
