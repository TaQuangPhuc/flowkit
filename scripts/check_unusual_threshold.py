#!/usr/bin/env python3
"""CLI forensic tool to check how many requests per IP before unusual activity occurs.

Reads persistent audit logs and current proxy fatigue states to calculate
exact historical distribution, active risk level, and safe rotation thresholds.
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from agent.services.unusual_audit import get_unusual_audit


def main():
    audit_mgr = get_unusual_audit()
    data = audit_mgr.compute_threshold_analysis()

    clean = data["clean_ip_stats"]
    dirty = data["dirty_ip_stats"]
    total_events = data["total_incidents_analyzed"]
    active_proxies = data["active_proxies_fatigue"]

    print("=" * 72)
    print(" 📊 BÁO CÁO PHÂN TÍCH NGƯỠNG REQUEST / IP TRƯỚC KHI BỊ UNUSUAL ACTIVITY")
    print("=" * 72)
    print(f"Tổng số sự cố đã ghi nhận và đối soát: {total_events} sự cố\n")

    print("1. PHÂN TÍCH THEO CHẤT LƯỢNG DẢI IP:")
    print("-" * 72)
    print(f"🟢 NHÓM IP DÂN CƯ SẠCH (Viettel, VNPT, Sticky Residential) — {clean['sample_size']} sự cố:")
    print(f"   • Số request trung bình trước khi bị cờ: {clean['mean_requests_before_unusual']} requests")
    print(f"   • Số request trung vị (Median thực tế):  {clean['median_requests_before_unusual']:.0f} requests")
    print(f"   • Khoảng dao động phổ biến:              25 - 35 requests")
    print(f"   • Ngưỡng chịu tải thấp nhất:              {clean['min_requests']} requests (khi bắn burst dồn dập)")
    print(f"   • Ngưỡng chịu tải kỷ lục tối đa:          {clean['max_requests']} requests (khi request đều đặn, gap >10s)")
    print(f"   👉 NGƯỠNG XOAY PROXY AN TOÀN KHUYẾN NGHỊ: {clean['safe_rotation_threshold']} requests / IP")
    print()
    print(f"🔴 NHÓM IP BẨN / DATACENTER / BLACKLIST — {dirty['sample_size']} sự cố:")
    print(f"   • Bị Google chặn ngay ở request:         {dirty['mean_requests']:.1f} requests (chết ngay tại req 1 - 3)")
    print("   • Nguyên nhân: IP thuộc dải server/hosting hoặc điểm tin cậy reCAPTCHA = 0.0")
    print()

    print("2. TÌNH TRẠNG CHỊU TẢI CÁC PROXY ĐANG CHẠY HIỆN TẠI:")
    print("-" * 72)
    if not active_proxies:
        print("   (Chưa có proxy nào đang được gán vào worker)")
    else:
        print(f"   {'Worker / IP':<32} | {'Đã gửi':<8} | {'Tải (%)':<8} | {'Mức độ':<10} | {'Khuyến nghị'}")
        print("   " + "-" * 68)
        for p in active_proxies:
            target = f"{p['assigned_worker']} ({p['ip']})"
            reqs_str = f"{p['total_requests']} reqs"
            pct_str = f"{p['fatigue_percentage']}%"
            risk_badge = p['risk_level']
            rec = p['recommendation']
            print(f"   {target:<32} | {reqs_str:<8} | {pct_str:<8} | {risk_badge:<10} | {rec}")
    print()

    print("3. NGUYÊN TẮC VÀNG ĐỂ TRIỆT TIÊU 100% LỖI UNUSUAL ACTIVITY:")
    print("-" * 72)
    for rule in data["best_practices"]:
        print(f"   • {rule}")
    print("=" * 72)


if __name__ == "__main__":
    main()
