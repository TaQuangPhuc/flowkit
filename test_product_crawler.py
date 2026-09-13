#!/usr/bin/env python3
"""
Comprehensive Automated Test Suite for FlowKit E-Commerce Product Crawler.
Verifies Shopee, TikTok, URL Sanitization, SSRF Protection, Normalization, and USP Highlights Extraction.

Run using:
    /home/pc/flowkit/venv/bin/python /home/pc/flowkit/test_product_crawler.py
"""

import sys
import unittest
from pathlib import Path

# Add /home/pc/flowkit to sys.path
FLOWKIT_DIR = Path(__file__).resolve().parent
if str(FLOWKIT_DIR) not in sys.path:
    sys.path.insert(0, str(FLOWKIT_DIR))

from crawler import (
    scrape_product,
    validate_and_sanitize_url,
    extract_usp_highlights,
    NormalizedProduct
)
from crawler.shopee_crawler import (
    expand_shopee_shortlink,
    extract_shop_and_item_id,
    parse_shopee_product
)
from crawler.tiktok_crawler import (
    expand_tiktok_shortlink,
    extract_tiktok_id,
    fetch_tiktok_oembed,
    parse_tiktok_product
)


class TestUrlSanitizationAndSSRF(unittest.TestCase):
    """Test URL cleaning, mobile message extraction, and SSRF domain protection."""

    def test_clean_shopee_shortlink(self):
        url = "https://s.shopee.vn/4VU2IjQjPF"
        cleaned = validate_and_sanitize_url(url)
        self.assertEqual(cleaned, url)

    def test_mobile_share_string_extraction(self):
        raw_share = "Mua ngay tại Shopee nha cả nhà ơi: https://s.shopee.vn/4VU2IjQjPF giảm giá sốc!"
        cleaned = validate_and_sanitize_url(raw_share)
        self.assertEqual(cleaned, "https://s.shopee.vn/4VU2IjQjPF")

    def test_tiktok_shortlink_extraction(self):
        raw_share = "Xem video này nè https://vt.tiktok.com/ZSjR8nQ7U/ đỉnh quá"
        cleaned = validate_and_sanitize_url(raw_share)
        self.assertEqual(cleaned, "https://vt.tiktok.com/ZSjR8nQ7U/")

    def test_ssrf_blocked_loopback_and_private_ips(self):
        forbidden_urls = [
            "http://127.0.0.1:8080/secret",
            "http://localhost:8089/api/system/concurrency",
            "http://192.168.1.1/admin",
            "http://10.0.0.1/status",
            "http://172.17.0.1:8089/test",
            "http://0.0.0.0:8089/",
        ]
        for bad_url in forbidden_urls:
            with self.assertRaises(ValueError, msg=f"Should reject private IP/host: {bad_url}"):
                validate_and_sanitize_url(bad_url)

    def test_reject_unsupported_domains(self):
        unsupported = [
            "https://google.com/search?q=shoes",
            "https://amazon.com/dp/B08N5WRWNW",
            "https://evil-shopee.vn.attacker.com/steal",
        ]
        for bad_url in unsupported:
            with self.assertRaises(ValueError, msg=f"Should reject unsupported domain: {bad_url}"):
                validate_and_sanitize_url(bad_url)

    def test_reject_empty_or_malformed(self):
        with self.assertRaises(ValueError):
            validate_and_sanitize_url("")
        with self.assertRaises(ValueError):
            validate_and_sanitize_url("không có đường dẫn nào ở đây")


class TestTextExtractor(unittest.TestCase):
    """Test USP highlights chip tag extraction from descriptions."""

    def test_extract_highlights_from_bullets(self):
        desc = """
        Thông tin chi tiết:
        - Chất liệu silicone cao cấp dẻo dai đàn hồi tốt
        - Kèm sẵn keo dán 3M chuyên dụng siêu chắc
        - Phù hợp mọi loại nón bảo hiểm xe máy
        Hotline hỗ trợ: 0901234567
        Địa chỉ shop: 123 Đường ABC
        """
        highlights = extract_usp_highlights(desc, title="[Kèm Keo 3M] Phụ Kiện Tai Mèo")
        self.assertGreaterEqual(len(highlights), 2)
        # Should contain bracketed USP or bullet USPs
        all_text = " ".join(highlights).lower()
        self.assertIn("keo 3m", all_text)
        # Should not include hotline or address
        self.assertNotIn("0901234567", all_text)
        self.assertNotIn("địa chỉ", all_text)

    def test_fallback_to_title_phrases(self):
        title = "Áo Thun Nam Cổ Tròn Cotton 100% Thoáng Khí Cao Cấp"
        highlights = extract_usp_highlights("", title=title)
        self.assertGreaterEqual(len(highlights), 1)


class TestShopeeCrawler(unittest.TestCase):
    """Test live Shopee product crawling, short-link expansion, and SSR extraction."""

    def test_extract_shop_and_item_id(self):
        urls = [
            ("https://shopee.vn/product/281960897/28504498734", "281960897", "28504498734"),
            ("https://shopee.vn/opaanlp/281960897/28504498734?__mobile__=1", "281960897", "28504498734"),
            ("https://shopee.vn/Tai-Meo-Gan-Mu-Bao-Hiem-i.281960897.28504498734", "281960897", "28504498734"),
            ("https://shopee.vn/item?itemid=28504498734&shopid=281960897", "281960897", "28504498734"),
        ]
        for url, expected_shop, expected_item in urls:
            shop_id, item_id = extract_shop_and_item_id(url)
            self.assertEqual(shop_id, expected_shop)
            self.assertEqual(item_id, expected_item)

    def test_expand_shopee_shortlink(self):
        short_url = "https://s.shopee.vn/4VU2IjQjPF"
        expanded = expand_shopee_shortlink(short_url)
        self.assertTrue(expanded.startswith("https://shopee.vn/"))
        self.assertIn("28504498734", expanded)

    def test_live_shopee_scrape(self):
        short_url = "https://s.shopee.vn/4VU2IjQjPF"
        product = parse_shopee_product(short_url)

        self.assertIsInstance(product, NormalizedProduct)
        self.assertEqual(product.platform, "shopee")
        self.assertEqual(product.product_id, "28504498734")
        self.assertEqual(product.shop_id, "281960897")
        self.assertIn("VARO Helmets", product.shop_name)
        self.assertIn("Mũ Bảo Hiểm", product.title)
        self.assertGreaterEqual(len(product.images), 5)
        self.assertTrue(product.primary_image.startswith("https://down-vn.img.susercontent.com/file/"))
        self.assertIsNotNone(product.video_url)
        self.assertTrue(product.video_url.endswith(".mp4") or "mms.vod.susercontent.com" in product.video_url)
        self.assertTrue(product.is_price_estimated)
        self.assertGreaterEqual(len(product.highlights), 2)

        # Test to_dict contract
        d = product.to_dict()
        self.assertEqual(d["platform"], "shopee")
        self.assertEqual(d["product_id"], "28504498734")
        self.assertIn("price", d)
        self.assertIn("images", d)
        self.assertIn("primary_image", d)
        self.assertIn("video_url", d)


class TestTikTokCrawler(unittest.TestCase):
    """Test live TikTok product and video showcase extraction."""

    def test_extract_tiktok_id(self):
        url = "https://www.tiktok.com/@scout2015/video/6718335390845095173"
        vid = extract_tiktok_id(url)
        self.assertEqual(vid, "6718335390845095173")

    def test_expand_tiktok_shortlink(self):
        short_url = "https://vt.tiktok.com/ZSjR8nQ7U/"
        expanded = expand_tiktok_shortlink(short_url)
        self.assertIn("tiktok.com", expanded)
        self.assertIn("7428497611499359496", expanded)

    def test_tiktok_oembed(self):
        canonical = "https://www.tiktok.com/@scout2015/video/6718335390845095173"
        oembed = fetch_tiktok_oembed(canonical)
        self.assertIsNotNone(oembed)
        self.assertEqual(oembed.get("author_unique_id"), "scout2015")
        self.assertTrue(oembed.get("thumbnail_url").startswith("http"))

    def test_live_tiktok_scrape(self):
        short_url = "https://vt.tiktok.com/ZSjR8nQ7U/"
        product = parse_tiktok_product(short_url)

        self.assertIsInstance(product, NormalizedProduct)
        self.assertEqual(product.platform, "tiktok")
        self.assertEqual(product.product_id, "7428497611499359496")
        self.assertTrue(len(product.title) > 0)
        self.assertGreaterEqual(len(product.images), 1)
        self.assertTrue(product.primary_image.startswith("http"))

        d = product.to_dict()
        self.assertEqual(d["platform"], "tiktok")
        self.assertIn("formatted_price", d)


class TestUnifiedScraper(unittest.TestCase):
    """Test unified scrape_product function matching PROJECT.md interface contract."""

    def test_scrape_product_shopee(self):
        product = scrape_product("https://s.shopee.vn/4VU2IjQjPF")
        self.assertEqual(product.platform, "shopee")
        self.assertTrue(len(product.images) >= 1)

    def test_scrape_product_tiktok(self):
        product = scrape_product("https://vt.tiktok.com/ZSjR8nQ7U/")
        self.assertEqual(product.platform, "tiktok")
        self.assertTrue(len(product.images) >= 1)


if __name__ == "__main__":
    print("=" * 70)
    print("Running FlowKit E-Commerce Product Crawler Automated Test Suite")
    print("Python interpreter:", sys.executable)
    print("=" * 70)
    unittest.main(verbosity=2)
