"""Drain a nick's browser requests before changing its network route."""
import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


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
        await asyncio.shield(release())
