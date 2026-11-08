# FlowKit generation VPS

Tài liệu này dành cho agent tiếp quản deploy. Điện thoại iPhone **ở máy local**. Agent + `tlsrelay` + Auto-TVC chạy trên VPS. Nova là **một VPS khác** và gọi hai cổng public.

## Máy

| | |
|---|---|
| Host | `180.93.43.217` |
| OS | Ubuntu 18.04, kernel 4.15 |
| Login | `root` |
| SSH key (máy Khanh) | `C:\Users\Khanh\.ssh\flowkit_vps` |
| Public key comment | `flowkit-vps-deploy` |
| Code trên VPS | `/opt/flowkit` |
| Symlink path cứng trong studio | `/home/pc/flowkit` → `/opt/flowkit` |
| Python | `/opt/python311/bin/python3.11` (venv `/opt/flowkit/venv`) |
| Go | `/usr/local/go/bin/go` |
| ffmpeg | system package (`apt install ffmpeg`) |
| edge_tts | `/opt/flowkit/venv` (`pip install edge_tts`) |

Không ghi mật khẩu VPS vào git. Đăng nhập:

```powershell
powershell -File scripts/vps_ssh.ps1
# hoặc
ssh -i $env:USERPROFILE\.ssh\flowkit_vps -o IdentitiesOnly=yes root@180.93.43.217
```

## Nova trỏ vào đây

Nova **không** chạy trên máy này. Gán đúng hai URL:

| Việc | URL |
|---|---|
| FlowKit core (health, gen, poll, media) | `http://180.93.43.217:8100` |
| Auto-TVC / batch / lookbook | `http://180.93.43.217:8089` |

`8089` từ ngoài cần header `X-TVC-Key` (cùng `TVC_API_KEY` trong `secrets.env`). Loopback trên VPS không cần khóa. `8100` hiện không khóa.

## Cổng

Public (Nova): **8100**, **8089**.

Chỉ localhost: **8101** (agent), **8102** (agent idle), **9222** (WS extension), **51900–51904** (h2c từng nick).

`tlsrelay` listen `GW_LISTEN=0.0.0.0:8100` và reverse-proxy sang agent `127.0.0.1:8101`. Traffic Google **không** đi ra IP VPS (trừ nick không có `proxy.txt`): mỗi nick egress qua proxy dân cư trong `nicks/<name>/proxy.txt`.

## systemd

| Unit | Process |
|---|---|
| `flowkit-agent@.service` | `python -m agent.main` `API_PORT=%i` (8101 hoặc 8102) |
| `flowkit-relay.service` | `/opt/flowkit/tlsrelay /opt/flowkit` `GW_LISTEN=0.0.0.0:8100` |
| `flowkit-tvc.service` | `python auto_tvc_server.py` `EnvironmentFile=/etc/flowkit/tvc.env` |

```bash
systemctl status flowkit-agent flowkit-relay flowkit-tvc
journalctl -u flowkit-agent -n 80 --no-pager
journalctl -u flowkit-relay -n 40 --no-pager
journalctl -u flowkit-tvc -n 40 --no-pager
curl -sS http://127.0.0.1:8100/health
curl -sS http://127.0.0.1:8100/__gw/status
```

Restart nhanh từ máy Khanh: `python scripts/vps_deploy.py --restart-only`

## Nick

Năm nick gRPC: `friend`, `gemma`, `kailyn`, `landon`, `madyson`. Mỗi nick cần `meta.json`, `refresh_token.txt`, `gen_img_req.bin`, `mrr_req.bin`. Template `nicks/_tpl/{t2v,i2v,r2v}.bin`.

`landon` **không** có `proxy.txt` — request nick đó egress IP datacenter của VPS. Bốn nick kia có proxy dân cư.

Mint captcha / refresh token khi bị thu hồi: làm trên **điện thoại local**, rồi `python scripts/vps_deploy.py` để đẩy `nicks/`.

`nicks/` và `secrets.env` gitignored. Deploy script copy chúng lên VPS.

## Deploy từ máy Windows này

```powershell
cd C:\Users\Khanh\OneDrive\Desktop\flowkit
python scripts/vps_deploy.py            # zero-downtime: new agent on idle 8101/8102, flip gateway
python scripts/vps_deploy.py --health
python scripts/vps_deploy.py --restart-only
python scripts/vps_deploy.py --hard     # restart all units; brief blip on 8100/8089
```

Deploy mặc định **không tắt :8100**. Agent mới lên cổng idle (8101 hoặc 8102), `tlsrelay` đọc `.scratch/gw_target` rồi chuyển trong ~1.5s, drain job cũ, tắt process cũ. `flowkit-relay` chỉ restart khi binary Go đổi. `flowkit-tvc` (:8089) chỉ restart khi `auto_tvc_server.py` đổi.

Script pack: `agent/`, `nicks/`, `tools/tlsrelay` (source Go), `crawler/`, `assets/bgm`, `extension/`, `scripts/`, các file studio ở root, `flow_agent.db`, `secrets.env`. Build `tlsrelay` trên VPS bằng Go 1.22+. Không copy `venv/`, `__pycache__`, `*.exe`.

Khóa SSH mới (chỉ khi mất key cũ):

```powershell
ssh-keygen -t ed25519 -f $env:USERPROFILE\.ssh\flowkit_vps -C flowkit-vps-deploy
Get-Content $env:USERPROFILE\.ssh\flowkit_vps.pub
# append pubkey vào /root/.ssh/authorized_keys trên VPS
```

## iPhone / WireGuard / mitm capture

Mitm VPS **đang tắt** (`systemctl disable --now flowkit-mitm@*`). Capture `friend` ở PC local. Unit vẫn còn trên đĩa nếu bật lại sau khi mở UDP. Trước đó đã cài:

| Unit | UDP | Upstream |
|---|---|---|
| `flowkit-mitm@friend` | 51820 | `nicks/friend/proxy.txt` |
| `flowkit-mitm@gemma` | 51822 | `nicks/gemma/proxy.txt` |
| `flowkit-mitm@kailyn` | 51823 | `nicks/kailyn/proxy.txt` |
| `flowkit-mitm@madyson` | 51824 | `nicks/madyson/proxy.txt` |
| `flowkit-mitm@landon` | 51825 | không (egress IP VPS) |

HTTP proxy của mitm bind `127.0.0.1:2802x` — không public. CA copy từ PC (`~/.mitmproxy`) nên điện thoại không trust lại. Key WG copy từ `.omc/research/wg/<nick>.key`.

```powershell
python scripts/vps_mitm_setup.py
python scripts/vps_mitm_setup.py --status
```

Điện thoại `friend` hiện trỏ PC `192.168.1.88:51820`, không trỏ VPS. Tắt WG trên một nick để cài App Store/Zalo: [docs/PHONE_MAINTENANCE.md](PHONE_MAINTENANCE.md). Production gen **không** đi tunnel điện thoại.

`vps_deploy.py` không đụng unit mitm.

## Debug exchange

Mỗi response `/api/flow` và mỗi gRPC (thành công hoặc lỗi) ghi một dòng đã redact vào `/opt/flowkit/.scratch/flow-exchange-api.jsonl` (8 MB × 3). Không có token, prompt, ảnh, hay URL có chữ ký. Tắt: `FLOW_DEBUG_EXCHANGE=0`. Xem: `GET /api/admin/exchange?trace_id=nova_req_…`.

## Việc không làm trên VPS này

- Không mở **8101 / 51900–51904 / 9222** ra internet. UDP **51820–51825** là WG capture (key-auth); HTTP mitm không public.
- Không dùng IP VPS làm egress Google cho nick có proxy.
- Không copy `nicks/` hay `secrets.env` vào repo Nova.
- Chrome nick `iphone-1` trên VPS không có binary Chrome — đường live là **gRPC + tlsrelay**, không phải extension.

## Health mong đợi

`GET http://180.93.43.217:8100/health` → `status: ok`, `transport: grpc`, `grpc_nicks` đủ năm nick.

Clip Veo thành công: **52–162s** (trung vị ~93s). `VIDEO_STUCK_TIMEOUT` 240s chỉ **log** ghost; `check-status` vẫn `PENDING` (không `FAILED`) — Nova `FAILED` trên poll đã biến thành `generation_failed` trên UI khách. Ghost: Nova `poll_timeout`. Upload Google `INVALID_ARGUMENT` thử nick khác rồi 429 `retry_after_s=8`. `GET /api/flow/media/{operation_id}` resolve sang media id lúc nộp.

`GET http://180.93.43.217:8089/api/system/concurrency` kèm `X-TVC-Key` → HTTP 200.
