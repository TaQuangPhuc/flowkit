# Bảo trì iPhone nick (App Store / Zalo)

Gen khách chạy trên **VPS** (`tlsrelay` + `proxy.txt`). Tunnel WireGuard trên điện thoại **chỉ** để capture / mint captcha. Tắt WG trên một máy **không** cắt Nova.

Mitm VPS **đã tắt** (UDP nhà cung cấp không vào được). Capture `friend` chạy lại trên PC: `mitmdump` UDP `51820`, Endpoint `192.168.1.88:51820`. QR: `nicks/friend/wg_qr_split_apple.png` và `nicks/wg_qr_scan.html`.

## Lúc này (cài Zalo trên `friend`)

Trên iPhone nick **friend**:

1. Mở app **WireGuard**.
2. Tắt tunnel của nick đó (switch Off).
3. Cài Zalo / App Store.
4. Bật lại tunnel khi xong.

Không restart VPS, không `vps_deploy`, không đụng `nicks/friend` trên server.

## Lần sau — đỡ phải tắt hẳn

Hai mức, chọn cái nhẹ hơn:

| Việc | Cách | Ảnh hưởng khách |
|---|---|---|
| App Store / iCloud / APNs | Profile **split-apple**: WG vẫn on, trừ dải Apple `17.0.0.0/8` | Không |
| Zalo, Facebook, app không-Apple | Tắt tunnel nick đó 1–2 phút | Không |
| Capture Flow / mint captcha | Bật lại full tunnel hoặc split-apple | Không |

Tạo QR split-apple (không đổi key, không restart mitm):

```powershell
cd C:\Users\Khanh\OneDrive\Desktop\flowkit\.omc\research
python wg_qr.py friend 51820 --split-apple
```

QR: `nicks/friend/wg_qr_split_apple.png`. Trên điện thoại: WireGuard → Edit tunnel → thay AllowedIPs, hoặc import QR mới (cùng key).

Full tunnel lại: `python wg_qr.py friend 51820` → `nicks/friend/wg_qr.png`.

## Không làm

- Không stop `flowkit-relay` / agent trên VPS.
- Không xóa `nicks/friend/` hay `proxy.txt`.
- Không bật WG full `0.0.0.0/0` rồi mong App Store chạy — Apple đi nhầm qua mitm thì store không load.
