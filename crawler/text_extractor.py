"""
Text Extractor for FlowKit Crawler Engine.
Extracts concise Unique Selling Proposition (USP) highlight chip tags from product descriptions and titles.
"""

import re
from typing import List

FEATURE_KEYWORDS = [
    "chất liệu", "công dụng", "đặc điểm", "ưu điểm", "tính năng",
    "thiết kế", "phù hợp", "kèm", "bảo hành", "chuẩn",
    "chống", "siêu", "cao cấp", "bền", "tiện lợi", "an toàn",
    "dễ dàng", "đàn hồi", "co giãn", "thoáng khí", "chính hãng"
]

IGNORED_PATTERNS = [
    r"^hotline\b", r"^địa chỉ\b", r"^inbox\b", r"^liên hệ\b",
    r"^lưu ý\b", r"^cam kết\b", r"^chú ý\b", r"^hướng dẫn\b",
    r"^quý khách\b", r"^chúc quý khách\b", r"^đổi trả\b",
    r"^shopee\b", r"^tiktok\b", r"^shop\b", r"^sỉ\b", r"^tuyển sỉ\b"
]


def clean_line(text: str) -> str:
    """Strip leading bullets, numbers, emojis, and trim whitespace."""
    # Remove leading bullets, numbers, emoji markers
    cleaned = re.sub(r"^[\s\-\*\+•▪▫◦►▶✔✓✅🔥👉⭐✨🌟\d\.\)]+\s*", "", text).strip()
    # Remove trailing punctuation
    cleaned = re.sub(r"[\s\.,;:\-]+$", "", cleaned).strip()
    return cleaned


def extract_usp_highlights(description: str, title: str = "", max_highlights: int = 5) -> List[str]:
    """
    Extract concise USP highlights chips from description and title.
    Returns 3-5 punchy strings suitable for UI chip badges and script generation.
    """
    highlights: List[str] = []
    seen = set()

    def add_candidate(cand: str):
        c = clean_line(cand)
        if len(c) < 8 or len(c) > 75:
            return
        c_lower = c.lower()
        # Check against ignored patterns
        for ign in IGNORED_PATTERNS:
            if re.search(ign, c_lower):
                return
        # Deduplication check
        if c_lower in seen:
            return
        # Avoid near-duplicates
        for existing in seen:
            if c_lower in existing or existing in c_lower:
                return
        seen.add(c_lower)
        highlights.append(c)

    # 1. First extract title USP brackets e.g. "[Kèm Keo 3M]", "(Hàng Chính Hãng)"
    if title:
        bracket_matches = re.findall(r"\[(.*?)\]|\((.*?)\)", title)
        for b1, b2 in bracket_matches:
            cand = b1 or b2
            if cand and not any(skip in cand.lower() for skip in ["freeship", "voucher", "mã", "rẻ"]):
                add_candidate(cand)

    # 2. Extract structured bullet lines from description
    if description:
        lines = description.splitlines()
        bullet_candidates = []
        keyword_candidates = []

        in_feature_section = False

        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue

            line_lower = line.lower()

            # Detect feature section headers
            if any(sec in line_lower for sec in ["đặc điểm nổi bật", "ưu điểm", "tính năng", "thông tin chi tiết", "công dụng"]):
                in_feature_section = True
                continue

            # Detect section end
            if any(end in line_lower for end in ["hướng dẫn sử dụng", "bảo quản", "chính sách", "quy định"]):
                in_feature_section = False

            # Check if line looks like a bullet or numbered list
            is_bullet = bool(re.match(r"^[\-\*\+•▪▫◦►▶✔✓✅🔥👉\d\.]+\s+", line))
            has_keyword = any(k in line_lower for k in FEATURE_KEYWORDS)

            if is_bullet or in_feature_section:
                cleaned = clean_line(line)
                if len(cleaned) >= 8 and len(cleaned) <= 75:
                    bullet_candidates.append(cleaned)
            elif has_keyword:
                cleaned = clean_line(line)
                if len(cleaned) >= 8 and len(cleaned) <= 75:
                    keyword_candidates.append(cleaned)

        for cand in bullet_candidates:
            if len(highlights) >= max_highlights:
                break
            add_candidate(cand)

        for cand in keyword_candidates:
            if len(highlights) >= max_highlights:
                break
            add_candidate(cand)

    # 3. Fallback: if we still have fewer than 2 highlights, extract phrases from title
    if len(highlights) < 2 and title:
        parts = re.split(r"[,\-|/]", title)
        for part in parts:
            if len(highlights) >= max_highlights:
                break
            add_candidate(part)

    # Clean capitalisation
    result = []
    for h in highlights[:max_highlights]:
        if h and h[0].islower():
            h = h[0].upper() + h[1:]
        result.append(h)

    return result
