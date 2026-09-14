# FlowKit — Project Handoff & Technical Guide

Tài liệu này dành cho các Agent (Codex, Claude, Gemini, Grok, v.v.) tiếp quản hệ thống **FlowKit**, nắm bắt kiến trúc, các lỗi đã giải quyết, cơ chế xoay proxy tự động và quy trình vận hành.

---

## 1. Tổng quan hệ thống (System Overview)

FlowKit là hệ thống tự động hóa điều khiển Google Flow (`flow.google.com`) để sinh ảnh và video chất lượng cao (Veo 3.1 & Imagen 3):

```
                                      ┌────────────────────────────────────────┐
                                      │              MÁY NỘI BỘ (PC)           │
Nova VPS (Proxy-Gate-Way)             │                                        │
  │                                   │   FastAPI Server (127.0.0.1:8100)      │
  │ private tunnel (ssh -R)           │     ├── /api/flow/* (Nova gateway)     │
  └──────────────────────────────────►│     ├── /api/accounts/* (Quản lý nick) │
                                      │     └── WebSocket (:9222)              │
                                      │           ▲                            │
                                      │           │ ws://127.0.0.1:9222        │
                                      │           ▼                            │
                                      │   Chrome Extension (FlowKitExtension)  │
                                      │           ▲                            │
                                      │   Google Chrome (nick-a)               │
                                      │     │ --proxy-server=127.0.0.1:32913   │
                                      │     ▼                                  │
                                      │   LocalProxyBridge (127.0.0.1:32913)   │
                                      │     │ Inject Proxy-Authorization       │
                                      │     ▼                                  │
                                      │   Upstream Sticky Proxy (Proxymart VN) │
                                      └─────┼──────────────────────────────────┘
                                            │
                                            ▼
                                     Google Flow CDN
```

### Thành phần chính:
- **Server FastAPI** (`127.0.0.1:8100`): Cung cấp REST API cho Nova và dashboard cục bộ.
- **WebSocket Server** (`127.0.0.1:9222`): Nhận kết nối từ Chrome Extension, dispatch các RPC request và nhận kết quả.
- **Chrome Extension** (`extension/`): Chạy ngầm trong Google Chrome đã đăng nhập Google Flow, thực thi RPC `batchexecute` trực tiếp trong trang Flow tab.
- **LocalProxyBridge** (`agent/services/proxy_forward.py`): Forwarder cục bộ trên `127.0.0.1:{port}` giúp inject `Proxy-Authorization` upstream mà không làm mất phiên duyệt web hay rò rỉ credential.
- **Proxy Pool Service** (`agent/services/proxy_pool.py`): Quản lý danh sách proxy dân cư sạch và điều phối xoay IP tự động.

---

## 2. Vấn đề cốt lõi & Cách đã khắc phục (Root Cause & Fix)

### Vấn đề: `PUBLIC_ERROR_UNUSUAL_ACTIVITY`
- **Nguyên nhân gốc**: Khi điều hướng request qua các proxy Datacenter (ví dụ: `192.227.201.229:29229` - ColoCrossing US), Google Flow phát hiện dải IP máy chủ dữ liệu và lập tức chặn với lỗi `PUBLIC_ERROR_UNUSUAL_ACTIVITY`.
- **Giải pháp**:
  1. Chỉ sử dụng **Residential Proxy (IP Dân cư)** sạch tại Việt Nam. Đã kiểm chứng: trên IP dân cư VN, FlowKit sinh 22 video liên tục (kèm burst 6 video đồng thời) thành công 100%, không hề bị lỗi.
  2. Xây dựng **Proxy Pool** chứa các proxy dự phòng để xoay vòng (round-robin) khi có sự cố.
  3. Cơ chế **Live Hot-Swap Upstream**: Chrome luôn kết nối cố định tới `127.0.0.1:{bridge_port}`. Khi xoay proxy, `LocalProxyBridge.switch_upstream()` lập tức đổi đích upstream mà **không cần khởi động lại Chrome hay ngắt kết nối WebSocket**.

---

## 3. Kiến trúc Proxy Pool & Cơ chế Xoay IP (Proxy Rotation)

### File cấu hình: `agent/proxy_pool.json`
Chứa danh sách proxy dân cư sạch và vị trí index xoay hiện tại:
```json
{
  "proxies": [
    "http://proxymart29419:bsHaRyBI@163.61.71.7:29419",
    "http://proxymart29194:MPeUTDTG@163.223.7.194:29194",
    ...
  ],
  "current_index": 5
}
```

### Các modules liên quan:
- **`agent/services/proxy_forward.py`**:
  - Lớp `LocalProxyBridge(upstream, port=0)`:
  - Phương thức `switch_upstream(new_upstream)`: Cập nhật auth header và upstream IP ngay trong runtime.
- **`agent/services/chrome_nicks.py`**:
  - `running_chrome_proxy_port(nick_id)`: Tự động trích xuất port bridge mà Chrome đang kết nối từ command line để bind lại nếu server khởi động lại.
  - `ensure_bridge(nick_id, parsed, port=0)`: Khởi động hoặc khôi phục bridge gắn với nick.
  - `get_bridge(nick_id)`: Trả về instance bridge đang hoạt động.
- **`agent/services/proxy_pool.py`**:
  - `load_proxy_pool()`, `save_proxy_pool()`: Đọc/ghi cấu hình pool.
  - `get_next_proxy()`: Lấy proxy kế tiếp theo cơ chế Round-Robin và tăng index.
  - `rotate_nick_proxy(nick_id)`: Xoay proxy cho nick, cập nhật profile và hot-swap bridge.

### API quản trị Proxy:
| Method | Endpoint | Mục đích |
|---|---|---|
| `GET` | `/api/accounts/proxy-pool` | Xem danh sách proxy (passwords đã ẩn) & index hiện tại |
| `POST` | `/api/accounts/proxy-pool` | Thêm danh sách proxy mới vào pool |
| `POST` | `/api/accounts/{nick_id}/rotate-proxy` | Kích hoạt xoay proxy thủ công cho nick |
| `POST` | `/api/accounts/{nick_id}/check-proxy` | Kiểm tra IP egress thực tế hiện tại của nick |

---

## 4. Cơ chế Auto-Rotate & Retry khi dính lỗi Unusual Activity

Khi một request sinh ảnh/video hoặc upload gặp lỗi `PUBLIC_ERROR_UNUSUAL_ACTIVITY`:

1. **Tự động xoay & Auto-retry nội bộ (Lớp 1)**:
   - Trong [agent/services/flow_client.py](file:///home/pc/flowkit/agent/services/flow_client.py#L475-L525), hệ thống bắt mã lỗi, đồng bộ gọi `await rotate_nick_proxy(prof_id)`.
   - Ngay sau khi bridge chuyển sang IP mới, FlowKit **tự động retry lại request đó 1 lần ngay lập tức**.
   - Nếu lần retry này thành công, client (Nova) nhận kết quả HTTP 200 mà không hề bị ngắt quãng.
2. **Thông báo chuẩn HTTP 429 Retryable cho Nova (Lớp 2)**:
   - Nếu lỗi vẫn tồn tại hoặc cần báo về client, [agent/api/flow.py](file:///home/pc/flowkit/agent/api/flow.py) trả về **`HTTP 429 Too Many Requests`** kèm header `Retry-After: 1`:
   ```json
   {
     "ok": false,
     "error": "PUBLIC_ERROR_UNUSUAL_ACTIVITY",
     "message": "UNUSUAL_ACTIVITY: Proxy đã được đổi thành công sang IP mới. Vui lòng retry lại ngay.",
     "detail": "UNUSUAL_ACTIVITY: Proxy đã được đổi thành công sang IP mới. Vui lòng retry lại ngay.",
     "proxy_rotated": true,
     "new_proxy": "http://proxymart...:***@163.223.7.194:29194",
     "retryable": true,
     "retry_after_s": 1
   }
   ```
   - Phía Nova chỉ cần kiểm tra: nếu `StatusCode == 429` hoặc `proxy_rotated == true` thì sleep 1s rồi retry submit (tối đa 2 lần).

---

## 5. Danh mục Models & Materials

Quản lý tại [agent/models.json](file:///home/pc/flowkit/agent/models.json):

### Image Models:
- **`NANO_BANANA_PRO`** (Mã: `GEM_PIX_2`): Model mặc định (`default_image_model`), chất lượng cao nhất của Imagen 3 / Gemini Pixel.
- **`NANO_BANANA_2`** (Mã: `NARWHAL`): Model Imagen thế hệ trước, xử lý nhanh.

### Video Models:
- **`veo_3_1_i2v_lite_low_priority`** (Mặc định): Veo 3.1 Lite tối ưu, clip 8s, 0-credit Google.
- **`veo_3_1_i2v_lite`**: Veo 3.1 Lite bản standard.
- **`veo_3_1_i2v_s_fast_ultra`**: Veo 3.1 Ultra (chất lượng hình ảnh và chuyển động cao nhất).
- **`veo_3_1_t2v_lite_low_priority`**: Text-to-Video.
- **`veo_3_1_lite_low_priority`**: Reference-to-Video (Ingredients panel).
- **`abra_*`**: Dòng Omni Flash hỗ trợ các độ dài 4s, 6s, 8s, 10s.
- **Upscaler**: `veo_3_1_upsampler_1080p` và `veo_3_1_upsampler_4k`.

---

## 6. Vận hành & Lệnh quan trọng (Operational Commands)

### Môi trường thực thi:
- **Thư mục workspace**: `/home/pc/flowkit`
- **Python Virtualenv**: `/home/pc/flowkit/venv/bin/python` (Bắt buộc dùng python này, không dùng `/usr/bin/python3`).

### Lệnh kiểm tra sức khỏe (Health Check):
```bash
curl -s http://127.0.0.1:8100/health
# Bắt buộc trả về: {"status": "ok", "extension_connected": true, ...}
```

### Khởi động lại Server khi sửa code:
```bash
# Tìm và dừng tiến trình server cũ
kill -9 $(lsof -t -i :8100)

# Chạy server nền
/home/pc/flowkit/venv/bin/python -m agent.main
```

### Kiểm tra proxy của nick hiện tại:
```bash
curl -s -X POST http://127.0.0.1:8100/api/accounts/nick-a/check-proxy
```

### Xoay thủ công sang IP kế tiếp:
```bash
curl -s -X POST http://127.0.0.1:8100/api/accounts/nick-a/rotate-proxy
```

---

## 7. Các điểm cấm kỵ (Rules & Constraints)

1. **KHÔNG BAO GIỜ dùng IP Datacenter** cho Chrome / nick Flow. Chỉ dùng proxy Residential (IP dân cư sạch).
2. **Media ID luôn là UUID** (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`). Tuyệt đối không dùng chuỗi dạng `CAMS...` / base64.
3. **Không bind port 8100 ra public internet**. Giao tiếp với Nova VPS hoàn toàn qua reverse SSH tunnel (`ssh -R 127.0.0.1:8100:127.0.0.1:8100`).
4. **Không viết throwaway script loop request**. Mọi batch generate dùng `POST /api/requests/batch`, server tự throttle concurrency max 5 + cooldown 10s.
