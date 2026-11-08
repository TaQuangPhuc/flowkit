"""Per-nick daily generation counters that survive trace rotation and restarts.

The trace jsonl rotates every ~30 min under load, so counting "today" from it
undercounts. Counters live in one small JSON keyed by VN-local day. Two agent
processes overlap during a zero-downtime swap, so every bump is a locked
read-modify-write of the file, not an in-memory tally.
"""
from __future__ import annotations

import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import fcntl
except ImportError:  # Windows dev box
    fcntl = None

from agent.config import BASE_DIR

VN = timezone(timedelta(hours=7))
STATS_FILE = Path(BASE_DIR) / ".scratch" / "flow-daily-stats.json"
KEEP_DAYS = 14
_lock = threading.Lock()


def today() -> str:
    return datetime.now(VN).strftime("%Y-%m-%d")


@contextmanager
def _file_lock():
    STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if fcntl is None:
        yield
        return
    with open(STATS_FILE.with_suffix(".lock"), "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _read() -> dict:
    try:
        return json.loads(STATS_FILE.read_text())
    except (OSError, ValueError):
        return {}


def bump(nick: str, **inc) -> None:
    """Add counts for `nick` today. `*_ts` keys keep their first value."""
    if not nick:
        return
    try:
        with _lock, _file_lock():
            data = _read()
            row = data.setdefault(today(), {}).setdefault(nick, {})
            for k, v in inc.items():
                if k.endswith("_ts"):
                    row.setdefault(k, v)
                else:
                    row[k] = row.get(k, 0) + v
            for day in sorted(data)[:-KEEP_DAYS]:
                del data[day]
            tmp = STATS_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(data))
            os.replace(tmp, STATS_FILE)
    except Exception:
        pass  # counters must never fail generation


def day_counts(day: str | None = None) -> dict[str, dict]:
    return _read().get(day or today(), {})
