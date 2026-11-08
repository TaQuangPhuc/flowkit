"""Google CDN image URLs often default to a 512px thumb (=s512 / =w512-h286)."""
from __future__ import annotations

import re

_S_SIZE = re.compile(r"=s\d+((?:-[a-z0-9]+)*)", re.I)
_WH_SIZE = re.compile(r"=w\d+-h\d+((?:-[a-z0-9]+)*)", re.I)
# Bare lh3/googleusercontent URLs have no size suffix and serve a small
# thumbnail — they need an explicit =s0. flow-content.google/image URLs are
# already full-size and signed: any mutation (even =s0) returns 403.
_LH3_BARE = re.compile(
    r"^(https://[a-z0-9-]*\.googleusercontent\.com/[^=?]+)(\?.*)?$", re.I)


def bump_google_image_url(url: str) -> str:
    """Ask the CDN for the original (s0), not the 512px preview."""
    if not url or not url.startswith("http"):
        return url
    bumped = _S_SIZE.sub(r"=s0\1", url, count=1)
    bumped = _WH_SIZE.sub(r"=s0\1", bumped, count=1)
    if bumped != url:
        return bumped
    m = _LH3_BARE.match(url)
    if m:
        return m.group(1) + "=s0" + (m.group(2) or "")
    return url
