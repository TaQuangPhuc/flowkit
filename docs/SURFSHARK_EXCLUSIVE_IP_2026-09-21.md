# Surfshark: IP riêng cho từng nick

Áp dụng lúc 12:04 ngày 21/09/2026 (Asia/Bangkok).

## Nguyên lý và giới hạn

Surfshark dùng IP chia sẻ, hữu hạn. Khác session, máy chủ hoặc quốc gia không
thay thế được việc đo IP công khai. Số nick phục vụ đồng thời phụ thuộc số IP
thật đã xác minh, chất lượng đường truyền và giới hạn Google; không suy ra từ
số file cấu hình VPN. IP riêng trong hệ thống FlowKit vẫn có thể được khách
hàng Surfshark bên ngoài sử dụng.

## Luồng cấp và xoay IP

1. Mã nick được băm thành owner cố định; đổi session không đổi owner.
2. Gateway dựng tunnel, đo IPv4 công khai qua `tun0`, rồi mới cho dùng.
3. Một IP chỉ được một owner giữ. Khóa cấp phát kiểm tra lại khi nhiều nick
   xin IP đồng thời; áp dụng cả HTTP và SOCKS5.
4. Khi xoay, gateway giữ riêng một IP ứng viên. FlowKit kiểm tra IP và truy
   cập Google/Labs trước khi chuyển đường hiện tại, lưu tài khoản và đổi bridge.
5. Kết nối cũ đang đóng vẫn giữ quyền sở hữu IP cũ. IP chỉ được cấp cho nick
   khác sau khi tunnel cũ biến mất và qua khoảng đệm 30 giây.
6. Hết IP khác nhau: từ chối cấp mới. Xoay thất bại trước commit giữ nguyên
   đường hiện tại. Không tự chuyển sang proxy chưa kiểm tra hoặc cấp trùng IP.
   Đây là cơ chế từ chối an toàn; chưa có hàng đợi chờ IP riêng.

URL session cũ đi theo đường hiện tại của owner, không tự xoay ngược. Bộ kiểm
tra sức khỏe bỏ qua Surfshark session lịch sử; watchdog chỉ báo đã chữa khi
API xoay trả `ok=true`. Lưu tài khoản dùng khóa và thay file nguyên tử để tránh
hai nick xoay đồng thời ghi đè cấu hình của nhau trong tiến trình API.

OpenVPN kết nối lại phải tạo worker mới và đo lại IP. Socket dữ liệu chỉ đi
qua `tun0`/IPv4. Gateway ghim đúng phiên bản worker khi mở socket để tránh nhầm
tunnel khi số thứ tự namespace được tái sử dụng. Việc đo IP phản ánh egress
tại lúc xác minh; không phải cam kết IP dành riêng từ nhà cung cấp VPN.

## Cấu hình triển khai

- Nguồn gateway: `scratch/surfshark-gateway` (repository lồng riêng).
- Binary chạy: `gateway/gateway`; service: `surfshark-gateway.service`.
- `OVPN_DIR=/home/pc/flowkit/gateway/ovpn-expanded`.
- 284 file TCP/UDP thuộc 100 mã quốc gia; đây không phải 284 IP khác nhau.
- `REQUIRE_PROXY_OWNER=true`; `MIN_POOL_SIZE=8` tunnel.
- Ưu tiên VN, SG, MY, TH, TW, JP, KR, HK; dùng danh mục rộng hơn khi cần.
- Tuổi tunnel 60 phút có jitter; tạo đường thay thế trước khi đóng đường cũ.
- Gateway hiện có trần 255 namespace. Không phải hệ thống IP vô hạn; mở rộng
  nhiều gateway cần kho khóa IP dùng chung trước khi khẳng định không trùng.
- Trạng thái lease: `GET /__flowkit/leases` tại cổng HTTP gateway, bắt buộc
  `Proxy-Authorization` hiện có. Không ghi mật khẩu vào tài liệu/log kiểm tra.
- Không thay đổi giới hạn video đồng thời hay policy chỉ dùng model low priority.
- Không đổi hợp đồng API với Nova; thay đổi này không cần triển khai lại Nova.

## Kiểm chứng

- FlowKit: 461 unit test qua; 1 cảnh báo deprecation Starlette có sẵn.
- Gateway: `go test -race ./...` và `go vet ./...` qua.
- Test: IP trùng, hết IP, prepare/commit, URL cũ, worker hết hạn/đổi identity,
  IP chưa xác minh, giữ claim khi draining, 20 owner cấp và xoay đồng thời.
- Sau triển khai, cả 3 nick kết nối lại và qua kiểm tra kết nối Google/Labs.
- Xoay thực tế `nick-a` và `Nick-b` đồng thời: cả hai `ok=true`,
  `switched_live=true`; IP mới vẫn khác nhau và khác nick thứ ba.

| Nick | IP trước thử xoay | IP sau thử xoay |
|---|---|---|
| nick-a | 151.240.33.25 | 202.176.4.55 |
| Nick-b | 149.88.106.171 | 149.88.23.77 |
| Nick thứ ba | 83.97.112.35 | 83.97.112.35 |

Kiểm chứng chỉ dùng kết nối/IP, không gửi yêu cầu tạo video. Kết quả CLEAN
không bảo đảm Google chấp nhận mọi yêu cầu tạo video; chưa đo tải video dài hạn
trên bản này. Tự thay tunnel được kiểm tra bằng unit test, chưa đợi hết chu kỳ
60 phút trên hệ thống thật.

## Khôi phục

Bản sao binary, cấu hình gateway và accounts trước triển khai nằm tại
`gateway/backups/exclusive-ip-20260921-120345/`. Dừng nhận việc mới, chờ shield
báo rảnh rồi mới thay binary/cấu hình và khởi động lại các service. Bản cũ
không có đảm bảo IP riêng, nên chỉ khôi phục khi cần xử lý sự cố triển khai.
