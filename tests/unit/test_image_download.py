"""Keyframe image fetch must survive a stuck nick relay."""
import pytest

from agent.services.watermark_remover import (
    _download_retryable,
    _fetch_image_bytes,
    _image_download_order,
    _unwrap_relay_url,
)


def test_unwrap_relay_url():
    url = "http://127.0.0.1:51901/to/lh3.googleusercontent.com/abc=s0?Expires=1"
    assert _unwrap_relay_url(url) == "https://lh3.googleusercontent.com/abc=s0?Expires=1"
    assert _unwrap_relay_url("https://lh3.googleusercontent.com/abc") == ""


def test_download_order_tries_signed_url_between_relay_attempts():
    relay = "http://127.0.0.1:51901/to/lh3.googleusercontent.com/abc=s0"
    order = _image_download_order([relay])
    assert order == [
        relay,
        "https://lh3.googleusercontent.com/abc=s0",
        relay,
    ]


def test_download_order_leaves_direct_urls_alone():
    direct = "https://flow-content.google/image/abc"
    assert _image_download_order([direct, direct]) == [direct]


def test_timeout_and_relay_502_are_retryable():
    assert _download_retryable(TimeoutError("timed out"))
    assert _download_retryable(RuntimeError("Failed to download image from CDN: HTTP 502"))
    assert not _download_retryable(RuntimeError("Failed to download image from CDN: HTTP 403"))


def test_fetch_retries_transient_then_returns_bytes():
    calls = {"n": 0}

    def get(url, headers, timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("read timed out")
        return 200, b"\xff\xd8jpeg"

    body = _fetch_image_bytes(get, "https://cdn.example/a", {}, 30, 2)
    assert body.startswith(b"\xff\xd8")
    assert calls["n"] == 2


def test_fetch_does_not_retry_forbidden():
    def get(url, headers, timeout):
        return 403, b"no"

    with pytest.raises(RuntimeError, match="HTTP 403"):
        _fetch_image_bytes(get, "https://cdn.example/a", {}, 30, 3)
