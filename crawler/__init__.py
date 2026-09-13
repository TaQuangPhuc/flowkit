"""
FlowKit E-Commerce Product Crawler Package.
Provides unified self-hosted scraping for Shopee and TikTok Shop products.
"""

import re
import ipaddress
import urllib.parse
from typing import Optional

from .normalizer import NormalizedProduct
from .shopee_crawler import parse_shopee_product, SHOPEE_DOMAINS
from .tiktok_crawler import parse_tiktok_product, TIKTOK_DOMAINS
from .text_extractor import extract_usp_highlights

__all__ = [
    "NormalizedProduct",
    "scrape_product",
    "scrape_product_url",
    "extract_usp_highlights",
    "validate_and_sanitize_url",
]

# Whitelisted host patterns for SSRF prevention
ALLOWED_DOMAINS = [
    r"(^|\.)shopee\.vn$",
    r"(^|\.)shp\.ee$",
    r"(^|\.)tiktok\.com$",
]

BLOCKED_IP_PREFIXES = [
    "127.", "10.", "192.168.", "172.16.", "172.17.", "172.18.",
    "172.19.", "172.20.", "172.21.", "172.22.", "172.23.",
    "172.24.", "172.25.", "172.26.", "172.27.", "172.28.",
    "172.29.", "172.30.", "172.31.", "0.0.0.0", "localhost"
]


def validate_and_sanitize_url(raw_input: str) -> str:
    """
    Extract first HTTP(S) URL from user input, validate against SSRF,
    and verify it belongs to Shopee or TikTok.
    """
    if not raw_input or not isinstance(raw_input, str):
        raise ValueError("URL không được để trống")

    # 1. Extract first URL if user pasted mobile message containing extraneous copy
    url_match = re.search(r"https?://[^\s<>\"'`]+", raw_input.strip())
    if not url_match:
        # Check if it was pasted without scheme
        raw_trimmed = raw_input.strip()
        if any(d in raw_trimmed.lower() for d in ["shopee.vn", "shp.ee", "tiktok.com"]):
            raw_trimmed = "https://" + raw_trimmed
            url_match = re.search(r"https?://[^\s<>\"'`]+", raw_trimmed)

    if not url_match:
        raise ValueError("Không tìm thấy đường link hợp lệ trong nội dung đã dán")

    clean_url = url_match.group(0)

    # 2. Parse URL and check scheme
    parsed = urllib.parse.urlparse(clean_url)
    if parsed.scheme.lower() not in ["http", "https"]:
        raise ValueError("Chỉ hỗ trợ giao thức HTTP và HTTPS")

    hostname = (parsed.hostname or "").lower()
    if not hostname:
        raise ValueError("Đường link không chứa tên miền hợp lệ")

    # 3. SSRF Protection: Check loopback and private IPs
    for blocked in BLOCKED_IP_PREFIXES:
        if hostname == blocked or hostname.startswith(blocked):
            raise ValueError("Không được phép truy cập địa chỉ mạng nội bộ")

    try:
        ip = ipaddress.ip_address(hostname)
        if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local:
            raise ValueError("Không được phép truy cập địa chỉ IP riêng tư")
    except ValueError:
        pass  # It's a standard domain name, proceed

    # 4. Whitelist domain check
    is_allowed = any(re.search(pat, hostname) for pat in ALLOWED_DOMAINS)
    if not is_allowed:
        raise ValueError("Chỉ hỗ trợ liên kết sản phẩm từ Shopee (*.shopee.vn, *.shp.ee) hoặc TikTok (*.tiktok.com)")

    return clean_url


def scrape_product(url: str) -> NormalizedProduct:
    """
    Scrape product details from a Shopee or TikTok URL.
    Returns NormalizedProduct instance.
    No caching: Performs fresh live scrape on every invocation.
    """
    clean_url = validate_and_sanitize_url(url)
    parsed = urllib.parse.urlparse(clean_url)
    hostname = (parsed.hostname or "").lower()

    if any(domain in hostname for domain in ["shopee.vn", "shp.ee"]):
        return parse_shopee_product(clean_url)
    elif "tiktok.com" in hostname:
        return parse_tiktok_product(clean_url)
    else:
        raise ValueError("Nền tảng thương mại điện tử chưa được hỗ trợ")


# Alias for backward and forward compatibility
scrape_product_url = scrape_product
