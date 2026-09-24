# FlowKit Standby — Cold Backup trên máy nhà (Windows 11 + WSL2)

Dự phòng cho trường hợp máy chính mất điện/rớt mạng. Máy nhà giữ **bản clone đồng bộ định kỳ** của toàn bộ fleet: code, Chrome profiles (session login), DB, config. Khi máy chính chết → bấm `failover.sh` → fleet chạy tiếp từ nhà, caller không đổi gì (vẫn đi qua Nova VPS tunnel).

## Kiến trúc

```
                 ┌──────────── caller (SaaS) ────────────┐
                 │        Nova VPS 172.17.0.1:8100        │
                 └───────────────▲────────────────────────┘
                       -R tunnel │ (chỉ 1 đầu bind được)
        ┌────────────────────────┴───────────┐
        │                                    │
  MÁY CHÍNH (active)              MÁY NHÀ (cold standby)
  flowkit + gateway-netns         flowkit + gateway (no netns)
        │                                    ▲
        └────── rsync pull mỗi 30p ──────────┘
           repo + profiles + db + units
```

- **Cold standby**: mọi service flowkit trên máy nhà `disabled` — chỉ `backup-sync.timer` chạy. Tunnel `-R` tự chặn double-fleet (main còn sống → máy nhà bind fail).
- **Bắt buộc WSL2**: Chrome profile cookies mã hoá `v11` Linux OSCrypt — Windows Chrome (DPAPI) không giải mã được → session chết → phải relogin. WSL2 giữ nguyên Linux semantics → session sống.
- **Gateway bỏ netns**: netns chỉ là cách ly, OVPN vẫn tunnel qua mạng nhà ra đúng Surfshark egress. User unit, không cần root.

## Setup 1 lần trên máy nhà

### 1. WSL2 + user `pc`

```powershell
# PowerShell admin
wsl --install Ubuntu-24.04
# trong Ubuntu: tạo user `pc` thuộc sudo (path /home/pc giống hệt máy chính)
```

```bash
# trong Ubuntu — bật systemd
echo -e "[boot]\nsystemd=true" | sudo tee /etc/wsl.conf
# PowerShell: wsl --shutdown → mở lại Ubuntu
```

### 2. Deps

```bash
sudo apt update
sudo apt install -y rsync sqlite3 openssh-client curl ffmpeg python3 python3-venv git

# Google Chrome (GUI chạy qua WSLg — hiện cửa sổ lên desktop Windows)
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install -y ./google-chrome-stable_current_amd64.deb
```

### 3. Tailscale (đường sync)

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up        # join cùng tailnet với máy chính
tailscale status         # note IP/tên của máy chính
```

### 4. SSH tới máy chính

```bash
ssh-keygen -t ed25519
ssh-copy-id pc@<ip-máy-chính-trên-tailnet>
# thêm vào ~/.ssh/config:
#   Host flowkit-main
#       HostName <ip-hoặc-tên-tailnet-máy-chính>
#       User pc
ssh flowkit-main true    # test
```

### 5. Pull repo + chạy setup

```bash
git clone https://github.com/TaQuangPhuc/flowkit.git ~/flowkit
# hoặc: rsync -a pc@flowkit-main:/home/pc/flowkit/ ~/flowkit/
bash ~/flowkit/scripts/standby/standby-setup.sh
```

Setup script sẽ: verify deps/WSLg/ssh → cài `flowkit-gateway.service` + `backup-sync.{service,timer}` → **disable mọi flowkit service** → chạy first sync (~9.6G profiles lần đầu, qua tailscale).

### 6. Linger (services sống khi logout/WSL boot)

```bash
sudo loginctl enable-linger pc
```

## Vận hành

### Sync tự động

`backup-sync.timer` chạy mỗi 30p. Xem lần chạy cuối:

```bash
systemctl --user list-timers backup-sync.timer
journalctl --user -u backup-sync.service -n 50
```

Sync gì:
| Nguồn (main) | Đích (standby) | Exclude |
|---|---|---|
| `~/flowkit` | `~/flowkit` | node_modules, *.db (riêng), logs |
| 4 sqlite db | snapshot `.backup` → repo | — |
| `~/.flowkit/chrome` | profiles | Cache dirs, `FlowKitExtension/`, `SingletonLock` |
| `~/.flowkit` còn lại | coccoc-browser, misc | `chrome/` (đã sync trên) |
| `~/.ssh` config+keys | `~/.ssh` | — |
| `~/.config/systemd/user/flowkit*` | units | — |

### Failover (máy chính chết)

```bash
bash ~/flowkit/scripts/standby/failover.sh
```

Start: gateway :18888 → api :8100 + tvc + dashboard → tunnel → timers. Nick auto-launch, warm-up gate giữ ~1-2 phút rồi fleet full capacity.

Verify:

```bash
curl http://127.0.0.1:8100/health          # extension_connected: true
curl http://127.0.0.1:8100/api/accounts/diagnose   # fleet state
```

### Failback (máy chính sống lại)

```bash
bash ~/flowkit/scripts/standby/failback.sh   # trên máy nhà — đẩy state ngược
# rồi trên máy chính: start lại services như bình thường
```

### Sync tay

```bash
bash ~/flowkit/scripts/standby/backup-sync.sh
# KHÔNG chạy khi standby fleet đang live (script tự refuse)
```

## Lưu ý

- **Không bao giờ chạy 2 fleet cùng lúc** — cùng account Google trên 2 máy → flag. Tunnel `-R` chặn tự nhiên, đừng bypass.
- Test `failover.sh` 1 lần khi rảnh — DR chưa test = chưa có DR.
- `FlowKitExtension/` bị exclude khỏi sync **cố ý** — nó mang baked `profileId` của máy gốc (ghost-nick bug); `launch_nick` tự regen đúng.
- Chrome hiện cửa sổ qua WSLg trên desktop Windows — vẫn mở tay relogin được nếu cần.
- UPS nhỏ cho máy chính xử lý 90% ca mất điện ngắn — standby dành cho outage dài.
