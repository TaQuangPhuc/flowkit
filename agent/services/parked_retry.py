"""Client-side backoff for the `all_workers_parked` 503.

The studios retried a 503 four times, 2.5s apart, then failed the item. A park
lasts 30 minutes, so every job that hit a fully-parked fleet died in ~10s even
though nothing had been submitted upstream and a nick was coming back.

Nothing reached Flow on this response, so a retry cannot double-render — the
one thing the studios' max_retries=1 on video endpoints exists to prevent.
"""

from __future__ import annotations

import os
import time
from typing import Any

PARKED_ERROR_CODE = "all_workers_parked"
# Total wall clock a single call may spend waiting for the fleet to come back.
PARKED_RETRY_BUDGET_S = float(os.environ.get("PARKED_RETRY_BUDGET_S", "900"))
PARKED_DEFAULT_DELAY_S = 60.0


def retry_after_seconds(err: Any, default: float) -> float:
    """Read a Retry-After header, clamped to something sane."""
    try:
        raw = err.headers.get("Retry-After") if getattr(err, "headers", None) else None
    except Exception:
        raw = None
    if not raw:
        return default
    try:
        return min(max(float(str(raw).strip()), 1.0), 300.0)
    except (TypeError, ValueError):
        return default


def is_parked(err: Any, err_body: str) -> bool:
    return getattr(err, "code", None) == 503 and PARKED_ERROR_CODE in (err_body or "")


class ParkedBackoff:
    """One instance per API call: waits out a park within a fixed budget."""

    def __init__(self, budget_s: float = PARKED_RETRY_BUDGET_S):
        self.budget_s = budget_s
        self.deadline: float | None = None
        self.waits = 0

    def wait(self, err: Any, err_body: str) -> bool:
        """Sleep and return True if the caller should re-issue the request."""
        if not is_parked(err, err_body):
            return False
        now = time.time()
        if self.deadline is None:
            self.deadline = now + self.budget_s
        remaining = self.deadline - now
        if remaining <= 0:
            return False
        time.sleep(min(retry_after_seconds(err, PARKED_DEFAULT_DELAY_S), remaining))
        self.waits += 1
        return True
