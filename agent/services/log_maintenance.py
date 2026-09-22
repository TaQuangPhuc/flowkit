"""Append-only log trimming — nothing rotated these files before.

server.log reached 618MB and .scratch/ext-netlog.jsonl 129MB. server.log is
opened by systemd with `StandardOutput=append:`, i.e. O_APPEND, so truncating
in place is safe: the next write lands at the new end of file. Trimming keeps
the tail rather than deleting the file, so a post-mortem still has the recent
history, and it never renames the file out from under a writer's open fd.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)


def read_tail(path: Path, max_bytes: int) -> bytes:
    """Read at most max_bytes from the end of a file, starting at a line break."""
    size = path.stat().st_size
    with path.open("rb") as fh:
        if size > max_bytes:
            fh.seek(size - max_bytes)
            chunk = fh.read()
            nl = chunk.find(b"\n")
            return chunk[nl + 1:] if nl != -1 else chunk
        return fh.read()


def trim_log(path: Path, max_bytes: int, keep_bytes: int) -> Dict[str, Any]:
    """Trim path to its last keep_bytes once it grows past max_bytes."""
    res: Dict[str, Any] = {"path": str(path), "trimmed": False, "before": 0, "after": 0}
    try:
        if not path.exists():
            return res
        before = path.stat().st_size
        res["before"] = before
        res["after"] = before
        if before <= max_bytes:
            return res

        tail = read_tail(path, keep_bytes)
        marker = (
            f"--- log trimmed {time.strftime('%Y-%m-%dT%H:%M:%S')} "
            f"({before} -> ~{len(tail)} bytes) ---\n"
        ).encode("utf-8")
        with path.open("r+b") as fh:
            fh.seek(0)
            fh.write(marker + tail)
            fh.truncate()
        res["trimmed"] = True
        res["after"] = path.stat().st_size
        logger.info("Trimmed %s: %d -> %d bytes", path.name, before, res["after"])
    except Exception as exc:
        logger.warning("Could not trim %s: %s", path, exc)
        res["error"] = str(exc)
    return res


def trim_logs(paths: Iterable[Path], max_bytes: int, keep_bytes: int) -> List[Dict[str, Any]]:
    return [trim_log(Path(p), max_bytes, keep_bytes) for p in paths]
