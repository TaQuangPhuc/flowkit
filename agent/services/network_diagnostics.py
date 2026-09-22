"""Allowlisted network-error metadata; never persist request bodies or URL queries."""
import json
import math
import re
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from urllib.parse import urlsplit

_LOCK = Lock()
MAX_BYTES = 2 * 1024 * 1024


def sanitize_failure(data):
    def token(key, pattern, limit):
        value = data.get(key)
        return value if isinstance(value, str) and len(value) <= limit and re.fullmatch(pattern, value) else None
    try:
        url = urlsplit(str(data.get('url') or ''))
        origin = f'{url.scheme}://{url.hostname}'
        if origin not in {'https://flow.google.com', 'https://labs.google'} or url.port not in (None, 443):
            return None
    except ValueError:
        return None
    # Only known RPC endpoint paths; do not persist arbitrary path content.
    path = url.path if re.fullmatch(r'/_/[A-Za-z0-9_./-]{1,200}', url.path) else '/_/[redacted]'
    elapsed = data.get('elapsedMs')
    elapsed = round(max(0, min(elapsed, 3600000))) if type(elapsed) in (int, float) and math.isfinite(elapsed) else None
    ts = token('ts', r'[0-9TZ:.+\-]+', 40)
    return {
        'event': 'network_error', 'diagnosticVersion': 1,
        'receivedAt': datetime.now(timezone.utc).isoformat(), 'ts': ts,
        'requestId': token('requestId', r'[A-Za-z0-9_.-]+', 128),
        'profileId': token('profileId', r'[\w@.+ -]+', 128),
        'rpcid': token('rpcid', r'[A-Za-z0-9_,]+', 80),
        'url': origin + path,
        'networkError': token('networkError', r'net::ERR_[A-Z0-9_]+', 100) or 'UNKNOWN_NETWORK_ERROR',
        'elapsedMs': elapsed,
    }


def append_failure(path: Path, record):
    with _LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.stat().st_size >= MAX_BYTES:
            path.replace(path.with_suffix('.previous.jsonl'))
        with path.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + '\n')


def tail_failures(path: Path, limit=40):
    with _LOCK:
        if not path.exists():
            return []
        with path.open(encoding='utf-8') as handle:
            lines = deque(handle, maxlen=max(1, min(limit, 200)))
    return [json.loads(line) for line in lines]
