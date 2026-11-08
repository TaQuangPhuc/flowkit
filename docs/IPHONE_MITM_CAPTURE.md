# iPhone MITM Capture (WireGuard mode)

Bắt credential (`refresh_token` `1//0e…`) từ app Flow trên iPhone để build
`nicks/<nick>/` bundle chạy gRPC trực tiếp trên VPS/PC — không cần browser,
không cần sign-in lại nếu acc đã login sẵn trong app.

Đã chứng minh: capture thành công `premium1` (2026-10-02) và 4 acc cùng lúc
bằng một lần chuyển nick (2026-10-03).

## Topology

```
iPhone ──WG tunnel (UDP 28026)──▶ mitmdump trên PC
                                      │ server_conn.via = HTTP CONNECT
                                      ▼
                              upstream proxy (residential)
                                      ▼
                                Google APIs
```

- mitmdump chạy **WireGuard mode**: iPhone tunnel toàn bộ traffic vào PC.
- `mitm_logger.py` set `flow.server_conn.via` → mọi flow đi tiếp qua upstream
  proxy → egress IP = IP residential (không phải IP nhà/PC).
- UDP :443 (QUIC) bị kill trong logger — buộc fallback TCP h2 qua được proxy.
- Mọi request/response ghi vào `.scratch/mitm/mitm_captured.jsonl`.

## Files

| file | vai trò |
|---|---|
| `.omc/research/wg/premium1.key` | WG server keypair (JSON `server_key`/`client_key`) |
| `nicks/<nick>/wg_lan.conf` | client conf cho iPhone — `Endpoint = <IP-LAN-PC>:28026` |
| `nicks/<nick>/wg_lan_qr.png` | QR sinh từ `wg_lan.conf` — quét bằng WireGuard app |
| `.omc/research/mitm_logger.py` | addon: `MITM_UPSTREAM`, `MITM_NICK`, `MITM_LOG_DIR`; ghi jsonl |
| `.omc/research/tls_hello_dump.py` | addon: dump TLS ClientHello → `tls_hello_*.jsonl` |
| `.scratch/mitm-ca/` | mitmproxy CA (đã trust sẵn trên iPhone) |
| `nicks/<nick>/proxy.txt` | upstream proxy của nick — dùng làm `MITM_UPSTREAM` |

## Bật mitm

```bash
cd /c/Users/Khanh/OneDrive/Desktop/flowkit

# IP LAN hiện tại của PC (WiFi adapter) — QR phải trỏ IP này
ipconfig | grep "IPv4 Address"

# Lấy upstream proxy từ nick định route traffic qua
UP=$(cat nicks/premium1/proxy.txt)   # vd http://v94r:v94r@103.166.184.92:33834

MITM_NICK="<nick>" \
MITM_LOG_DIR="$(pwd -W)/.scratch/mitm" \
MITM_UPSTREAM="$UP" \
python -c "import sys; from mitmproxy.tools.main import mitmdump; \
sys.argv=['mitmdump', \
  '--mode','wireguard:.omc/research/wg/premium1.key@0.0.0.0:28026', \
  '--set','confdir=$(pwd -W)/.scratch/mitm-ca', \
  '--set','ssl_insecure=true', \
  '--set','connection_strategy=lazy', \
  '--set','http3=false', \
  '--set','upstream_auth=v94r:v94r', \
  '-s','.omc/research/mitm_logger.py', \
  '-s','.omc/research/tls_hello_dump.py']; \
mitmdump()" > .scratch/mitm/mitm_wg_live.log 2>&1 &
```

Verify: `netstat -an | grep 28026` → phải có `UDP 0.0.0.0:28026`.

**Bẫy đã gặp (đừng lặp lại):**

1. `MITM_UPSTREAM` phải có user/pass trong URL **và** thêm `--set upstream_auth=user:pass`
   — `mitm_logger.py` chỉ lấy `hostname:port` làm `via`, auth đi qua option riêng.
   Thiếu → upstream trả `407 Proxy Authentication Required` → Safari báo 502.
2. QR/conf cũ trỏ `Endpoint` theo IP LAN **tại thời điểm sinh** — IP PC đổi
   (DHCP) là handshake fail "sending handshake didn't succeed". Fix: sửa
   `Endpoint` trong `wg_lan.conf` rồi regen QR (xem dưới).
3. Instance cũ chiếm UDP 28026 → instance mới bind không được (log trống,
   chạy im lìm). `taskkill //PID <pid>` trước khi start lại.

## QR cho iPhone

Conf client `nicks/premium1/wg_lan.conf`:

```ini
[Interface]
PrivateKey = <client_key>
Address = 10.0.0.1/32
DNS = 10.0.0.53            # DNS giả của mitmproxy — resolve qua tunnel

[Peer]
PublicKey = <pubkey của server_key>
AllowedIPs = 0.0.0.0/0
Endpoint = <IP-LAN-PC>:28026
```

Regen QR sau khi sửa Endpoint:

```bash
python -c "import qrcode; qrcode.make(open('nicks/premium1/wg_lan.conf').read())\
.save(r'$(pwd -W)/nicks/premium1/wg_lan_qr.png')"
start "" nicks\\premium1\\wg_lan_qr.png
```

Trên iPhone:

```
WireGuard app → "+" → Create from QR code → quét → đặt tên → bật tunnel
→ TẮT cellular → WiFi proxy OFF (không cần, WG bắt tầng packet)
```

Verify tunnel: Safari → ipinfo.io → ra IP egress của upstream proxy
(không phải IP nhà). Log `mitm_wg_live.log` thấy `client connect 10.0.0.1:...`
và `server connect <upstream>:<port>` là đúng.

## Bắt credential của từng nick

Nguyên tắc: RT chỉ xuất hiện trên wire khi app **mint token mới**. Hai nguồn:

- `POST oauth2.googleapis.com/token` body `grant_type=refresh_token&refresh_token=1//0e…`
- `POST oauthaccountmanager.googleapis.com/v1/issuetoken` header `Authorization: Bearer 1//0e…` (đây chính là RT)

Không cần sign-in lại. Acc nào đang login sẵn trong app Flow:

```
1. Bật mitm + tunnel như trên
2. Mở app Flow → vào màn hình chọn account (hoặc chỉ mở app)
3. Chuyển qua lại giữa các nick → mỗi acc trigger issuetoken + /token refresh
   → RT của TẤT CẢ acc login sẵn bay ra cùng lúc (đã chứng minh, ~30 calls)
4. Muốn đủ hơn: gen thử 1 ảnh trên từng nick → bắt thêm mrr + gen_img template
```

Extract RT → email → project_uuid:

```python
import sys, json, re, grpc, urllib.parse, urllib.request
sys.path.insert(0, '.omc/research')
import oauth_bootstrap as ob, proto_lib as pl

DEVICE_ID = "<device_id từ capture>"  # vd B22CAE37-... (issuetoken body)
UA = "Flow App/1.566.3 (Build 1.566.3; ios 18.3.2; iPhone11,8) Dart/RPCClient"

# 1) unique RT từ /token bodies (hoặc Bearer trên issuetoken)
rts = set()
for line in open('.scratch/mitm/mitm_captured.jsonl', encoding='utf-8'):
    d = json.loads(line)
    if d.get('type') != 'request': continue
    b = (d.get('body') or {}).get('data', '')
    if 'refresh_token=' in b:
        rts.add(urllib.parse.parse_qs(b)['refresh_token'][0])
    if 'issuetoken' in d.get('url', ''):
        rts.add(d['headers']['authorization'].split(' ',1)[1])

# 2) mint ya29 -> email (GetProjects -> project uuid)
ch = grpc.secure_channel("aisandbox-pa.googleapis.com:443",
    grpc.ssl_channel_credentials())
call = ch.unary_unary(
    "/google.internal.labs.aisandbox.proto.flow.v1.FlowService/GetProjects",
    request_serializer=lambda x: x, response_deserializer=lambda x: x)
gp_msg = open('.scratch/mitm/getprojects_msg.bin','rb').read()  # msg gRPC đã gunzip
for rt in rts:
    ya = ob.mint_ya29(rt, DEVICE_ID)['token']
    info = json.loads(urllib.request.urlopen(
        'https://oauth2.googleapis.com/tokeninfo?access_token='+ya).read())
    resp, _ = call.with_call(gp_msg, metadata=[
        ("authorization", f"Bearer {ya}"), ("x-goog-authuser","0"),
        ("x-custom-user-agent", UA)], timeout=30)
    projs = [f[2][2:38].decode() for f in pl.decode(resp)
             if f[0]==1 and len(f[2])>38 and f[2][:2]==b'\n$']
    print(info['email'], projs)   # uuid đầu mỗi sub-message = project id
```

Lưu ý:

- `GetProjects` response có nhiều uuid — **chỉ uuid ở đầu mỗi project
  sub-message là project id** (uuid lồng trong là media cover). List sort theo
  ngày mới nhất trước — chọn cái đầu làm `project_uuid`.
- RT `1//0e…` nào mint ya29 bị `401` → RT đã revoked/expired, phải sign-in
  lại acc đó trong app để phát RT mới.
- Cùng acc capture lại → RT thường giống hệt (RT không rotate khi dùng).

## Đóng gói `nicks/<nick>/`

| file | nguồn |
|---|---|
| `refresh_token.txt` | RT `1//0e…` bắt được |
| `meta.json` | `project_uuid` từ GetProjects, `device_id`/`client_id`/`user_agent` từ capture (`com.google.whisk/1.566.3 iSL/3.4 iPhone/18.3.2 hw/iPhone11_8 (gzip)`), `scope: "openid https://www.googleapis.com/auth/aisandbox"`, `hl`, `captured`, `notes` |
| `gen_img_req.bin`, `mrr_req.bin`, `mrr_image_upload.bin` | copy từ nick capture trên **cùng thiết bị** (vd premium1 — cùng `device_id`); lý tưởng nhất là capture riêng bằng cách gen ảnh trên chính nick đó |
| `proxy.txt` | một proxy từ `nicks/_proxy_pool.txt` — **không trùng** proxy nick khác nếu được (throttle tính theo egress IP) |

Deploy lên VPS: `scp -r nicks/<nick> root@180.93.43.217:/opt/flowkit/nicks/` →
agent tự nhận (nick_watcher), kiểm tra `curl 127.0.0.1:8100/health`.

## Web OAuth path — KHÔNG dùng được

Link `accounts.google.com/v3/signin/identifier?...` (kể cả bản relay
`:8433/signin` kèm `auth_extension`+`kdi`+`epd`) chỉ sống trong SafariVC của
app thật — params session-bound, Google verify ngữ cảnh →
`/v3/signin/rejected`. Đã thử: Safari iPhone qua WG+upstream, Safari+Google
session trước, link trần — đều bị chặn. Đừng mất thời gian với đường này.
