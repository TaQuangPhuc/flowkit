"""Bounded structural snapshots; no free text, prompts, cookies or signed URLs."""
from collections import OrderedDict
import re
from agent.services.flow_trace import emit, fingerprint, identifier

_states = OrderedDict()
_UUID = re.compile(r'^[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$')


def shape(value, depth=0, budget=None):
    budget = [160] if budget is None else budget
    budget[0] -= 1
    if budget[0] < 0 or depth > 7:
        return {'omitted': True}
    if value is None or type(value) in (bool, int, float):
        return value
    if isinstance(value, str):
        if _UUID.fullmatch(value):
            return {'uuid': value}
        if value in {'MEDIA_GENERATION_STATUS_PENDING', 'MEDIA_GENERATION_STATUS_FAILED',
                     'MEDIA_GENERATION_STATUS_SUCCESSFUL', 'CAE'}:
            return {'enum': value}
        return {'string_length': len(value)}
    if isinstance(value, list):
        return {'length': len(value), 'items': [shape(x, depth+1, budget) for x in value[:24]]}
    if isinstance(value, dict):
        # Unknown keys can themselves contain customer input.
        allowed = {'operation', 'name', 'status', 'error', 'error_code', 'mediaId', 'projectId', 'done', 'metadata', 'video'}
        return {k: shape(v, depth+1, budget) for k,v in value.items() if k in allowed}
    return {'type': type(value).__name__}


def transition(kind, key, state, *, payload=None, **fields):
    cache_key = (kind, key)
    previous = _states.get(cache_key)
    if previous == state:
        return
    _states[cache_key] = state
    _states.move_to_end(cache_key)
    while len(_states) > 2048:
        _states.popitem(last=False)
    emit('video.evidence', kind=kind, key=identifier(key), previous=previous,
         state=state, structure=shape(payload), **fields)
