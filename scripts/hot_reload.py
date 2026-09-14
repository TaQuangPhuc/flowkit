#!/usr/bin/env python3
"""Hot Reload Tool for FlowKit Modules.

Dynamically reloads modified Python modules in memory WITHOUT restarting the server.
Zero downtime, zero connection drops, zero interrupted requests.
"""
import json
import sys
import urllib.request
import urllib.error

API_BASE = "http://127.0.0.1:8100"


def main():
    modules = sys.argv[1:]
    payload = {"modules": modules} if modules else {}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        f"{API_BASE}/api/system/reload-modules",
        data=data,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            res = json.loads(resp.read().decode("utf-8"))
            print("========================================================================")
            print(" ⚡ FLOWKIT IN-MEMORY HOT RELOAD (ZERO DOWNTIME)")
            print("========================================================================")
            print(f"Trạng thái: {'✅ THÀNH CÔNG' if res.get('ok') else '⚠️ CÓ CẢNH BÁO'}")
            print(f"Thông điệp: {res.get('message')}")
            if res.get("reloaded_modules"):
                print("Các module đã reload:")
                for m in res["reloaded_modules"]:
                    print(f"   • {m}")
            if res.get("errors"):
                print("Lỗi:")
                for err in res["errors"]:
                    print(f"   ❌ {err}")
            print("========================================================================")
    except Exception as exc:
        print(f"❌ Không thể kết nối tới FlowKit API Server: {exc}")
        print("Nếu server đang tắt, hãy dùng: python3 scripts/safe_restart.py")
        sys.exit(1)


if __name__ == "__main__":
    main()
