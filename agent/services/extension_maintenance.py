"""Drain a nick's browser requests before changing its network route."""
import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

# Longest the tab's own pause lease can hold (flow_guard leaseMs=120s) plus
# slack; the mark is cleared in `finally` on the normal path anyway.
_MARK_S = 180.0


def _mark_unavailable(client, nick_id: str) -> float:
    """Bench the nick in routing while its gate is paused for maintenance.

    Returns the mark timestamp so the caller only clears a mark nobody else
    touched — a park raised by another error during maintenance must stand.
    """
    mark = time.time() + _MARK_S
    for session in client._extensions.values():
        if session.get("profile_id") == nick_id:
            session["unavailable_until"] = max(
                float(session.get("unavailable_until") or 0), mark)
    return mark


def _clear_unavailable(client, nick_id: str, mark: float) -> None:
    for session in client._extensions.values():
        if (session.get("profile_id") == nick_id
                and float(session.get("unavailable_until") or 0) <= mark):
            session["unavailable_until"] = 0


@asynccontextmanager
async def proxy_maintenance(nick_id: str):
    from agent.services.flow_client import get_flow_client
    from agent.services.chrome_nicks import get_bridge
    client = get_flow_client()
    connected = any(s.get("profile_id") == nick_id for s in client._extensions.values())
    state = {"changed": False, "reloaded": False}
    if not connected:
        if get_bridge(nick_id):
            raise RuntimeError("EXTENSION_NOT_CONNECTED: cannot drain live proxy bridge")
        yield state
        return
    lease_id = uuid.uuid4().hex
    # Bench the nick before pausing its gate — anything routed to it during
    # the drain would queue, hit the deadline and bounce FLOW_BUSY.
    mark = _mark_unavailable(client, nick_id)
    try:
        ready = await client.profile_control(nick_id, "prepare_proxy_rotation", {"leaseId": lease_id})
        if not ready.get("ok"):
            raise RuntimeError(ready.get("error") or "EXTENSION_DRAIN_FAILED")
        yield state
    finally:
        # Also release a pause whose acknowledgement was lost or cancelled.
        async def release():
            result = await client.profile_control(nick_id, "finish_proxy_rotation",
                {"leaseId": lease_id, "reload": state["changed"]}, timeout=30)
            state["reloaded"] = bool(state["changed"] and result.get("ok"))
            if not result.get("ok"):
                logger.warning("Extension maintenance release for %s: %s", nick_id, result.get("error"))
        try:
            await asyncio.shield(release())
        finally:
            # Only unbench after the gate has actually resumed, so a newly
            # routed request never lands in a still-paused queue.
            _clear_unavailable(client, nick_id, mark)
