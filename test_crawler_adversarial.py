#!/usr/bin/env python3
"""
Adversarial Challenge & Stress Test Suite for FlowKit Product Crawler Engine.
Tests:
1. SSRF & IP Bypasses (Loopback, Cloud Metadata, Octal/Hex/Decimal IPs, DNS Spoofing, Scheme Manipulation)
2. Malformed & Exploit URLs (Null, Non-string types, 100k-char DoS, Unicode homographs, CRLF injection, Corrupted links)
3. Real-Time Crawl Verification (No stale cache, monotonically advancing crawled_at)
4. Concurrency Stress (5+ concurrent requests, mixed valid/adversarial workloads, latency & throughput analysis)
5. Live Endpoint Contract Validation (Shopee & TikTok via POST /api/product/scrape on port 8089)

Execute using:
    /home/pc/flowkit/venv/bin/python /home/pc/flowkit/test_crawler_adversarial.py
"""

import sys
import time
import json
import urllib.request
import urllib.error
import concurrent.futures
from pathlib import Path
from typing import Dict, Any, Tuple, List

FLOWKIT_DIR = Path(__file__).resolve().parent
if str(FLOWKIT_DIR) not in sys.path:
    sys.path.insert(0, str(FLOWKIT_DIR))

from crawler import (
    validate_and_sanitize_url,
    scrape_product,
    NormalizedProduct
)

LIVE_ENDPOINT = "http://127.0.0.1:8089/api/product/scrape"
VALID_SHOPEE_URL = "https://s.shopee.vn/4VU2IjQjPF"
VALID_TIKTOK_URL = "https://vt.tiktok.com/ZSjR8nQ7U/"


class AdversarialTestRunner:
    def __init__(self):
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = 0
        self.findings: List[Dict[str, Any]] = []

    def log(self, msg: str):
        print(msg)

    def record_finding(self, severity: str, category: str, summary: str, details: str):
        finding = {
            "severity": severity,
            "category": category,
            "summary": summary,
            "details": details
        }
        self.findings.append(finding)
        self.log(f"  [!] FINDING ({severity}): {summary}")

    def run_unit_check(self, name: str, fn) -> bool:
        self.total_tests += 1
        try:
            fn()
            self.passed_tests += 1
            self.log(f"  [PASS] {name}")
            return True
        except AssertionError as ae:
            self.failed_tests += 1
            self.log(f"  [FAIL] {name}: {ae}")
            return False
        except Exception as e:
            self.failed_tests += 1
            self.log(f"  [ERROR] {name}: {e}")
            return False

    def send_scrape_request(self, payload: Any, timeout: float = 30.0) -> Tuple[int, Dict[str, Any], float]:
        t0 = time.time()
        if isinstance(payload, bytes):
            data = payload
        elif isinstance(payload, str):
            data = payload.encode("utf-8")
        else:
            data = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(
            LIVE_ENDPOINT,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                elapsed = time.time() - t0
                body = resp.read().decode("utf-8")
                return resp.status, json.loads(body), elapsed
        except urllib.error.HTTPError as he:
            elapsed = time.time() - t0
            body = he.read().decode("utf-8")
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = {"raw": body}
            return he.code, parsed, elapsed
        except Exception as e:
            elapsed = time.time() - t0
            return 0, {"error": str(e), "exception_type": type(e).__name__}, elapsed

    # =========================================================================
    # SECTION 1: SSRF & IP BYPASS TESTS
    # =========================================================================
    def test_ssrf_and_ip_bypasses(self):
        self.log("\n" + "=" * 70)
        self.log("SECTION 1: SSRF & IP Bypass Attack Vectors")
        self.log("=" * 70)

        attack_vectors = [
            ("http://127.0.0.1:8089/api/system/concurrency", "Direct IPv4 loopback"),
            ("http://localhost:8089/api/product/scrape", "Localhost hostname"),
            ("http://0.0.0.0:8089/", "Wildcard 0.0.0.0 address"),
            ("http://169.254.169.254/latest/meta-data/", "AWS/GCP Cloud instance metadata"),
            ("http://[::1]:8089/secret", "IPv6 loopback"),
            ("http://0177.0.0.1:8089", "Octal encoded loopback IP"),
            ("http://2130706433:8089", "Decimal encoded loopback IP (2130706433)"),
            ("http://0x7f000001:8089", "Hexadecimal encoded loopback IP"),
            ("http://127.1", "Short notation loopback (127.1)"),
            ("http://127.0.1", "Short notation loopback (127.0.1)"),
            ("http://shopee.vn.127.0.0.1.nip.io", "DNS wildcard resolution to loopback"),
            ("http://127.0.0.1.nip.io", "nip.io loopback service"),
            ("https://tiktok.com@127.0.0.1:8089", "Userinfo host override"),
            ("https://shopee.vn:8089@127.0.0.1", "Userinfo host override with port"),
            ("https://shopee.vn.evil.com/product/1", "Subdomain suffix spoofing"),
            ("https://evil-shopee.vn/item", "Hyphenated prefix domain spoofing"),
            ("https://google.com/search?q=tvc", "Arbitrary non-whitelisted external domain"),
            ("file:///etc/passwd", "Local file scheme disclosure"),
            ("gopher://127.0.0.1:8089/_POST%20/", "Gopher SSRF exploitation protocol"),
            ("ftp://shopee.vn/file.txt", "FTP scheme manipulation"),
            ("javascript:alert(document.domain)", "JavaScript pseudo-scheme"),
            ("data:text/html,<h1>XSS</h1>", "Data URI scheme"),
        ]

        for payload, description in attack_vectors:
            def check(url=payload, desc=description):
                try:
                    res = validate_and_sanitize_url(url)
                    raise AssertionError(f"VULNERABILITY: Allowed dangerous SSRF/Bypass payload '{url}' -> {res}")
                except ValueError:
                    pass  # Correctly rejected
            self.run_unit_check(f"SSRF rejection: {description} ({payload})", check)

        # Test live endpoint rejection for SSRF
        live_ssrf_payloads = [
            {"url": "http://127.0.0.1:8089/api/system/concurrency"},
            {"url": "http://localhost:8089/"},
            {"url": "http://169.254.169.254/latest/meta-data/"},
            {"url": "https://google.com"},
            {"url": "file:///etc/passwd"},
        ]
        for p in live_ssrf_payloads:
            def check_live(payload_dict=p):
                status, data, _ = self.send_scrape_request(payload_dict)
                assert status == 400, f"Expected HTTP 400 rejection, got {status} ({data})"
                assert data.get("success") is False, f"Expected success: False, got {data}"
            self.run_unit_check(f"Live Endpoint SSRF Rejection: {p['url']}", check_live)

    # =========================================================================
    # SECTION 2: MALFORMED URLS & EXPLOIT INPUTS
    # =========================================================================
    def test_malformed_and_exploit_inputs(self):
        self.log("\n" + "=" * 70)
        self.log("SECTION 2: Malformed URLs & Exploit Inputs")
        self.log("=" * 70)

        # 1. Null / Non-string input handling at unit level
        def test_unit_none():
            try:
                validate_and_sanitize_url(None)
                assert False, "Should reject None"
            except ValueError:
                pass
        self.run_unit_check("validate_and_sanitize_url handles None gracefully", test_unit_none)

        def test_unit_empty():
            try:
                validate_and_sanitize_url("")
                assert False, "Should reject empty string"
            except ValueError:
                pass
        self.run_unit_check("validate_and_sanitize_url handles empty string", test_unit_empty)

        # 2. Unicode exploit characters
        unicode_attacks = [
            ("https://sh\x00opee.vn/item", "Null byte injection in hostname"),
            ("https://sh\u200bopee.vn/item", "Zero-width space injection"),
            ("https://sh\u202eopee.vn/item", "Right-to-Left Override (RTLO) spoofing"),
            ("https://sh\u043Epee.vn/item", "Cyrillic homograph spoofing (shоpee with U+043E)"),
            ("https://\uff53\uff48\uff4f\uff50\uff45\uff45\uff0e\uff56\uff4e", "Fullwidth ASCII unicode evasion"),
            ("https://shopee.vn\r\nHost: evil.com", "CRLF HTTP header injection"),
        ]
        for payload, desc in unicode_attacks:
            def check(url=payload, d=desc):
                try:
                    cleaned = validate_and_sanitize_url(url)
                    # If allowed, ensure it did not retain the malicious host or CRLF
                    assert "evil.com" not in cleaned
                    assert "\r" not in cleaned and "\n" not in cleaned
                except ValueError:
                    pass  # Correctly rejected
            self.run_unit_check(f"Unicode defense: {desc}", check)

        # 3. Buffer & DoS Stress (10,000 & 100,000 characters)
        def test_10k_char_url():
            huge_url = "https://s.shopee.vn/" + "a" * 10000
            t0 = time.time()
            validate_and_sanitize_url(huge_url)
            elapsed = time.time() - t0
            assert elapsed < 0.1, f"Regex DoS vulnerability: took {elapsed:.2f}s"
        self.run_unit_check("Extreme Length URL (10,000 chars) Regex ReDoS resilience", test_10k_char_url)

        def test_100k_char_url():
            massive_url = "https://s.shopee.vn/" + "x" * 100000
            t0 = time.time()
            validate_and_sanitize_url(massive_url)
            elapsed = time.time() - t0
            assert elapsed < 0.5, f"Regex DoS vulnerability: took {elapsed:.2f}s"
        self.run_unit_check("Extreme Length URL (100,000 chars) Regex ReDoS resilience", test_100k_char_url)

        # 4. Live Endpoint Server Crash Testing with Type Mismatches
        self.log("\n  --> Testing Live Server Endpoint crash resilience with malformed JSON payloads...")

        def test_server_none_url():
            status, data, _ = self.send_scrape_request({"url": None})
            if status == 0:
                self.record_finding(
                    "HIGH",
                    "Input Validation / Crash",
                    "Endpoint drops connection on {'url': null} due to unhandled AttributeError",
                    "In auto_tvc_server.py:3101, raw_url = req_data.get('url', '').strip() executes None.strip() "
                    "outside a try-except block, causing an uncaught AttributeError and immediate socket termination."
                )
                assert False, f"Server socket dropped / crashed on {{'url': null}} (Error: {data.get('error')})"
            assert status == 400, f"Expected 400, got {status}"
        self.run_unit_check("Live Endpoint resilience: {'url': null}", test_server_none_url)

        def test_server_integer_url():
            status, data, _ = self.send_scrape_request({"url": 99999})
            if status == 0:
                self.record_finding(
                    "HIGH",
                    "Input Validation / Crash",
                    "Endpoint drops connection on {'url': 99999} due to unhandled AttributeError",
                    "In auto_tvc_server.py:3101, non-string integer value lacks .strip(), triggering uncaught crash."
                )
                assert False, f"Server socket dropped / crashed on {{'url': 99999}} (Error: {data.get('error')})"
            assert status == 400, f"Expected 400, got {status}"
        self.run_unit_check("Live Endpoint resilience: {'url': 99999}", test_server_integer_url)

        def test_server_corrupted_json():
            status, data, _ = self.send_scrape_request(b"{invalid_json_payload", timeout=5.0)
            assert status == 400, f"Expected 400 for corrupted JSON, got {status}"
            assert "Invalid JSON" in data.get("error", "")
        self.run_unit_check("Live Endpoint resilience: Malformed JSON body", test_server_corrupted_json)

        # 5. Nonexistent / 404 Shortlink Graceful Failure Verification
        def test_corrupted_shopee_shortlink():
            try:
                prod = scrape_product("https://s.shopee.vn/invalid_nonexistent_xyz9999")
                # Critic observation: Check if it fabricated dummy data
                if prod.product_id == "unknown" and len(prod.images) == 0:
                    self.record_finding(
                        "MEDIUM",
                        "Data Integrity / Fail-Open",
                        "Shopee crawler fails open on 404/broken shortlinks by returning dummy price & empty images",
                        "When s.shopee.vn redirects to an error page, parse_shopee_product fabricates a 25.000đ dummy product "
                        "with empty images instead of raising ValueError or returning an error status."
                    )
                    assert False, "Shopee crawler returned dummy product with empty images on 404 link"
            except ValueError:
                pass  # Correctly raised ValueError when product / images not found
        self.run_unit_check("Shopee Crawler handling of nonexistent shortlink", test_corrupted_shopee_shortlink)

        def test_corrupted_tiktok_shortlink():
            try:
                prod = scrape_product("https://vt.tiktok.com/invalid_nonexistent_xyz9999")
                if prod.product_id == "unknown" and len(prod.images) == 0:
                    self.record_finding(
                        "MEDIUM",
                        "Data Integrity / Fail-Open",
                        "TikTok crawler fails open on nonexistent shortlinks by returning dummy price & empty images",
                        "When vt.tiktok.com redirects to TikTok home page, parse_tiktok_product fabricates a 49.000đ dummy product "
                        "with empty images instead of raising an error."
                    )
                    assert False, "TikTok crawler returned dummy product with empty images on nonexistent link"
            except ValueError:
                pass  # Correctly raised ValueError when product / images not found
        self.run_unit_check("TikTok Crawler handling of nonexistent shortlink", test_corrupted_tiktok_shortlink)

    # =========================================================================
    # SECTION 3: REAL-TIME CRAWL VERIFICATION (NO CACHING)
    # =========================================================================
    def test_real_time_crawl_no_caching(self):
        self.log("\n" + "=" * 70)
        self.log("SECTION 3: Real-Time Crawl Verification (No Caching)")
        self.log("=" * 70)

        def verify_no_caching():
            self.log("  Fetching 3 sequential requests for the same Shopee URL to test cache freshness...")
            timestamps = []
            for i in range(3):
                t0 = time.time()
                status, data, elapsed = self.send_scrape_request({"url": VALID_SHOPEE_URL})
                assert status == 200, f"Request {i+1} failed with status {status}"
                prod = data.get("product", {})
                crawled_at = prod.get("crawled_at")
                assert crawled_at is not None, f"crawled_at missing from product in request {i+1}"
                timestamps.append((crawled_at, elapsed))
                self.log(f"    Req {i+1}: crawled_at={crawled_at:.3f}, server_elapsed={elapsed:.2f}s")
                time.sleep(1.0)

            # Check strictly monotonic advance of crawled_at
            t1, t2, t3 = timestamps[0][0], timestamps[1][0], timestamps[2][0]
            assert t2 > t1, f"Cached response detected! t2 ({t2}) <= t1 ({t1})"
            assert t3 > t2, f"Cached response detected! t3 ({t3}) <= t2 ({t2})"

            diff1 = t2 - t1
            diff2 = t3 - t2
            assert diff1 >= 1.0, f"Timestamp difference too small ({diff1:.3f}s), possible cache hit"
            assert diff2 >= 1.0, f"Timestamp difference too small ({diff2:.3f}s), possible cache hit"
            self.log(f"  [OK] Caching check confirmed: Fresh crawl on every invocation (delta: {diff1:.2f}s, {diff2:.2f}s)")

        self.run_unit_check("Real-Time crawl verification: Zero caching between subsequent scrapes", verify_no_caching)

    # =========================================================================
    # SECTION 4: CONCURRENCY STRESS TESTING
    # =========================================================================
    def test_concurrency_stress(self):
        self.log("\n" + "=" * 70)
        self.log("SECTION 4: Concurrency Stress Testing (5+ Concurrent Workers)")
        self.log("=" * 70)

        def stress_5_concurrent_valid():
            concurrency_count = 5
            self.log(f"  Firing {concurrency_count} concurrent requests to POST /api/product/scrape...")

            start_wall = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_count) as executor:
                futures = [
                    executor.submit(self.send_scrape_request, {"url": VALID_SHOPEE_URL})
                    for _ in range(concurrency_count)
                ]
                results = [f.result() for f in futures]
            wall_elapsed = time.time() - start_wall

            statuses = [r[0] for r in results]
            latencies = [r[2] for r in results]
            successes = [r[1].get("success", False) for r in results]

            self.log(f"  Wall time: {wall_elapsed:.2f}s | Latencies: min={min(latencies):.2f}s, max={max(latencies):.2f}s, avg={sum(latencies)/len(latencies):.2f}s")
            self.log(f"  Status codes: {statuses}")

            assert all(s == 200 for s in statuses), f"Non-200 responses under concurrency: {statuses}"
            assert all(suc is True for suc in successes), "One or more concurrent requests failed"

            # Check if execution was serialized due to single-threaded HTTPServer
            # If serialized, max latency ~ wall_time and sum(latencies) ~ wall_time * (N+1)/2
            if wall_elapsed > 8.0:
                self.record_finding(
                    "LOW",
                    "Performance / Architecture",
                    "Single-threaded HTTPServer serializes concurrent scrape requests",
                    f"In auto_tvc_server.py:5985, HTTPServer is used instead of ThreadingHTTPServer. "
                    f"Under 5 concurrent requests, wall time stretched to {wall_elapsed:.2f}s because each "
                    f"2-second crawler network request blocks the event loop for subsequent requests."
                )

        self.run_unit_check("Concurrency Stress: 5 simultaneous valid product scrapes", stress_5_concurrent_valid)

        def stress_mixed_workload():
            self.log("  Firing mixed concurrent workload (3 valid + 3 adversarial/malformed requests)...")
            mixed_payloads = [
                {"url": VALID_SHOPEE_URL},
                {"url": "http://127.0.0.1:8089/admin"},     # SSRF attempt
                {"url": VALID_TIKTOK_URL},
                {"url": "https://google.com/search"},       # Disallowed domain
                {"url": "https://s.shopee.vn/" + "z" * 1000}, # Corrupted path
                {"url": ""},                                # Empty
            ]

            start_wall = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(mixed_payloads)) as executor:
                futures = [executor.submit(self.send_scrape_request, p) for p in mixed_payloads]
                results = [f.result() for f in futures]
            wall_elapsed = time.time() - start_wall

            statuses = [r[0] for r in results]
            self.log(f"  Mixed workload finished in {wall_elapsed:.2f}s | Statuses: {statuses}")

            # Verify server survived without socket crashes (code 0)
            assert 0 not in statuses, f"Server crashed on mixed workload! Statuses: {statuses}"
            self.log("  [OK] Server maintained stability under mixed adversarial load.")

        self.run_unit_check("Concurrency Stress: Mixed valid and adversarial requests", stress_mixed_workload)

    # =========================================================================
    # SECTION 5: LIVE ENDPOINT CONTRACT VALIDATION
    # =========================================================================
    def test_live_endpoint_contract(self):
        self.log("\n" + "=" * 70)
        self.log("SECTION 5: Live Endpoint Contract Validation (Shopee & TikTok)")
        self.log("=" * 70)

        def check_shopee_contract():
            status, data, elapsed = self.send_scrape_request({"url": VALID_SHOPEE_URL})
            assert status == 200, f"Shopee scrape returned HTTP {status}"
            assert data.get("success") is True, "Expected success: True"
            prod = data.get("product", {})

            # Check PROJECT.md schema contract
            required_keys = [
                "platform", "product_id", "title", "price", "original_price",
                "images", "primary_image", "video_url", "highlights"
            ]
            for k in required_keys:
                assert k in prod, f"Missing required contract key '{k}' in product response"

            assert prod["platform"] == "shopee"
            assert len(prod["images"]) >= 1, "Shopee product must contain at least 1 HD image"
            assert prod["primary_image"].startswith("http"), "primary_image must be a valid HTTP URL"
            self.log(f"  Shopee verified in {elapsed:.2f}s: ID={prod['product_id']}, Images={len(prod['images'])}, Title={prod['title'][:40]}...")

        self.run_unit_check("Live Endpoint Contract: Shopee scraper (s.shopee.vn)", check_shopee_contract)

        def check_tiktok_contract():
            status, data, elapsed = self.send_scrape_request({"url": VALID_TIKTOK_URL})
            assert status == 200, f"TikTok scrape returned HTTP {status}"
            assert data.get("success") is True, "Expected success: True"
            prod = data.get("product", {})

            required_keys = ["platform", "product_id", "title", "images", "primary_image"]
            for k in required_keys:
                assert k in prod, f"Missing required contract key '{k}' in product response"

            assert prod["platform"] == "tiktok"
            assert len(prod["images"]) >= 1, "TikTok product must contain at least 1 image"
            self.log(f"  TikTok verified in {elapsed:.2f}s: ID={prod['product_id']}, Images={len(prod['images'])}, Title={prod['title'][:40]}...")

        self.run_unit_check("Live Endpoint Contract: TikTok scraper (vt.tiktok.com)", check_tiktok_contract)

    # =========================================================================
    # SUMMARY & VERDICT GENERATION
    # =========================================================================
    def report(self) -> Dict[str, Any]:
        self.log("\n" + "=" * 70)
        self.log(f"ADVERSARIAL STRESS TEST SUMMARY: Total: {self.total_tests} | Passed: {self.passed_tests} | Failed: {self.failed_tests}")
        self.log("=" * 70)

        high_findings = [f for f in self.findings if f["severity"] == "HIGH"]
        med_findings = [f for f in self.findings if f["severity"] == "MEDIUM"]
        low_findings = [f for f in self.findings if f["severity"] == "LOW"]

        self.log(f"Findings Breakdown: HIGH={len(high_findings)}, MEDIUM={len(med_findings)}, LOW={len(low_findings)}")
        for f in self.findings:
            self.log(f" - [{f['severity']}] {f['summary']}")

        verdict = "REQUEST_CHANGES" if (len(high_findings) > 0 or self.failed_tests > 0) else "APPROVE"
        self.log(f"\nFINAL VERDICT: {verdict}\n")
        return {
            "total_tests": self.total_tests,
            "passed": self.passed_tests,
            "failed": self.failed_tests,
            "findings": self.findings,
            "verdict": verdict
        }


def main():
    runner = AdversarialTestRunner()
    runner.test_ssrf_and_ip_bypasses()
    runner.test_malformed_and_exploit_inputs()
    runner.test_real_time_crawl_no_caching()
    runner.test_concurrency_stress()
    runner.test_live_endpoint_contract()
    summary = runner.report()
    return 0 if summary["verdict"] == "APPROVE" else 1


if __name__ == "__main__":
    sys.exit(main())
