"""External reCAPTCHA V3 Enterprise token solver.

Experiment path: Flow mints the token inside the signed-in tab, which binds the
score to the nick's IP/session. When that score fails (CAPTCHA_EVALUATION_FAILED
/ UNUSUAL_ACTIVITY), a solver-minted token is the A/B alternative — the solver
mints in their own environment, so pass-rate tells us how much of the score is
IP/session-bound versus token-content.

Each token is single-use; every solve costs balance. Wire-up: the agent asks
for a token in ``batch_rpc`` when the mode allows and passes it as
``captchaToken`` in the WS message; the extension substitutes it for
``__CAPTCHA__`` verbatim instead of minting in-page.

Two provider APIs are supported, selected by CAPTCHA_SOLVER_PROVIDER:
  ``anticaptcha`` — anticaptcha.top in.php/res.php (key/method/googlekey)
  ``2captcha``    — api.2captcha.com createTask/getTaskResult
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
import urllib.request

from agent import config as _config

logger = logging.getLogger(__name__)

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/152.0.7977.75 Safari/537.36"
)
PAGE_URL = "https://flow.google.com/"


class CaptchaSolverError(Exception):
    pass


def _post_json(url: str, payload: dict, timeout: float) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _get_json(url: str, timeout: float) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _solve_anticaptcha(action: str, *, user_agent: str | None,
                       timeout_s: float, poll_s: float) -> str:
    key = _config.CAPTCHA_SOLVER_API_KEY
    base = _config.CAPTCHA_SOLVER_BASE.rstrip("/")
    resp = _post_json(f"{base}/in.php", {
        "key": key,
        "method": "userrecaptcha",
        "version": "v3",
        "googlekey": _config.RECAPTCHA_SITE_KEY,
        "enterprise": 1,
        "pageurl": PAGE_URL,
        "action": action,
        "userAgent": user_agent or DEFAULT_UA,
        "json": 1,
    }, timeout=15)
    if resp.get("status") != 1:
        raise CaptchaSolverError(f"createTask failed: {resp.get('request')}")
    task_id = resp["request"]

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(poll_s)
        res = _get_json(
            f"{base}/res.php?key={urllib.parse.quote(key)}&id={urllib.parse.quote(str(task_id))}&json=1",
            timeout=15,
        )
        if res.get("status") == 1:
            return res["request"]
        if "CAPCHA_NOT_READY" not in str(res.get("request")):
            raise CaptchaSolverError(f"getTaskResult failed: {res.get('request')}")
    raise CaptchaSolverError(f"solve timeout after {timeout_s}s")


def _solve_2captcha(action: str, *, user_agent: str | None,
                    timeout_s: float, poll_s: float) -> str:
    key = _config.CAPTCHA_SOLVER_API_KEY
    resp = _post_json("https://api.2captcha.com/createTask", {
        "clientKey": key,
        "task": {
            "type": "RecaptchaV3TaskProxyless",
            "websiteURL": PAGE_URL,
            "websiteKey": _config.RECAPTCHA_SITE_KEY,
            "minScore": 0.7,
            "pageAction": action,
            "isEnterprise": True,
            "apiDomain": "www.google.com",
            "userAgent": user_agent or DEFAULT_UA,
        },
    }, timeout=15)
    if resp.get("errorId"):
        raise CaptchaSolverError(
            f"createTask failed: {resp.get('errorCode')} {resp.get('errorDescription')}")
    task_id = resp["taskId"]

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(poll_s)
        res = _post_json("https://api.2captcha.com/getTaskResult",
                         {"clientKey": key, "taskId": task_id}, timeout=15)
        if res.get("status") == "ready":
            token = (res.get("solution") or {}).get("token") \
                or (res.get("solution") or {}).get("gRecaptchaResponse")
            if not token:
                raise CaptchaSolverError("ready but no token in solution")
            return token
        if res.get("errorId"):
            raise CaptchaSolverError(
                f"getTaskResult failed: {res.get('errorCode')} {res.get('errorDescription')}")
    raise CaptchaSolverError(f"solve timeout after {timeout_s}s")


def _solve_sync(action: str, *, user_agent: str | None = None,
                timeout_s: float = 45, poll_s: float = 3.0) -> str:
    if not _config.CAPTCHA_SOLVER_API_KEY:
        raise CaptchaSolverError("CAPTCHA_SOLVER_API_KEY not set")
    provider = _config.CAPTCHA_SOLVER_PROVIDER
    if provider == "2captcha":
        return _solve_2captcha(action, user_agent=user_agent,
                               timeout_s=timeout_s, poll_s=poll_s)
    if provider == "anticaptcha":
        return _solve_anticaptcha(action, user_agent=user_agent,
                                  timeout_s=timeout_s, poll_s=poll_s)
    raise CaptchaSolverError(f"unknown CAPTCHA_SOLVER_PROVIDER {provider!r}")


async def solve_recaptcha_v3(action: str, *, user_agent: str | None = None,
                             timeout_s: float | None = None) -> str:
    """Mint one Enterprise token for ``action`` on flow.google.com via solver."""
    return await asyncio.to_thread(
        _solve_sync, action,
        user_agent=user_agent,
        timeout_s=timeout_s or _config.CAPTCHA_SOLVER_TIMEOUT_S,
    )
