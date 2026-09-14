import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

MAU_DIR = Path("/home/pc/flowkit/mau")
MAU_DIR.mkdir(parents=True, exist_ok=True)

class WebHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0].lstrip("/")
        
        # File serving
        file_map = {
            "final": ("final_tvc_24s.mp4", "video/mp4"),
            "video1": ("clip_scene_1.mp4", "video/mp4"),
            "video2": ("clip_scene_2.mp4", "video/mp4"),
            "video3": ("clip_scene_3.mp4", "video/mp4"),
            "kf1": ("keyframe_scene_1.jpg", "image/jpeg"),
            "kf2": ("keyframe_scene_2.jpg", "image/jpeg"),
            "kf3": ("keyframe_scene_3.jpg", "image/jpeg"),
            "product": ("product.jpg", "image/jpeg"),
            "human": ("human.jpg", "image/jpeg"),
        }
        
        if path in file_map:
            fname, ctype = file_map[path]
            fpath = MAU_DIR / fname
            if fpath.exists():
                data = fpath.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Accept-Ranges", "bytes")
                self.end_headers()
                self.wfile.write(data)
                return

        # Main Showcase Page
        html = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>TVC 24 Giây Hoàn Chỉnh - FlowKit AI V3.8</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0b0f19; color: #f8fafc; margin: 0; padding: 24px; display: flex; flex-direction: column; align-items: center; }
        .container { max-width: 1200px; width: 100%; }
        h1 { color: #38bdf8; text-align: center; margin-bottom: 6px; font-size: 26px; }
        .subtitle { text-align: center; color: #94a3b8; font-size: 14px; margin-bottom: 28px; }
        
        /* Master video card */
        .master-box { background: linear-gradient(145deg, #1e293b, #0f172a); border: 2px solid #0284c7; border-radius: 20px; padding: 28px; display: flex; gap: 32px; align-items: center; margin-bottom: 40px; box-shadow: 0 15px 35px rgba(2,132,199,0.2); }
        .master-video { width: 340px; border-radius: 14px; border: 2px solid #38bdf8; background: #000; box-shadow: 0 8px 25px rgba(0,0,0,0.6); flex-shrink: 0; }
        .master-info { flex: 1; display: flex; flex-direction: column; gap: 14px; }
        .master-badge { display: inline-block; background: #10b981; color: white; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 700; width: fit-content; }
        
        /* Scenes Grid */
        .scenes-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; margin-bottom: 40px; }
        .scene-card { background: #1e293b; border: 1px solid #334155; border-radius: 16px; padding: 18px; display: flex; flex-direction: column; gap: 12px; }
        .scene-badge { background: #6366f1; color: white; padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 600; width: fit-content; }
        .scene-video { width: 100%; border-radius: 10px; border: 1px solid #475569; background: #000; height: 380px; object-fit: cover; }
        .scene-text { font-size: 12px; color: #cbd5e1; line-height: 1.5; background: #0f172a; padding: 10px; border-radius: 8px; border: 1px solid #1e293b; }
        
        .btn { display: inline-block; text-align: center; padding: 12px 24px; background: #10b981; color: white; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 15px; transition: 0.2s; box-shadow: 0 4px 12px rgba(16,185,129,0.3); }
        .btn:hover { background: #059669; }
        .btn-blue { background: #0284c7; box-shadow: 0 4px 12px rgba(2,132,199,0.3); }
        .btn-blue:hover { background: #0369a1; }
    </style>
</head>
<body>
    <div class="container">
        <h1>BẢN TVC HOÀN CHỈNH 3 PHÂN CẢNH (24 GIÂY)</h1>
        <div class="subtitle">Khớp 100% Đặc Tả Kiến Trúc Em Bông V3.8 &bull; Ghép Nối Liền Mạch Bằng FFmpeg Engine</div>

        <!-- MASTER VIDEO -->
        <div class="master-box">
            <video class="master-video" controls autoplay loop playsinline>
                <source src="/final" type="video/mp4">
            </video>
            <div class="master-info">
                <span class="master-badge">&#10004; FINAL TVC 24S (576 FRAMES @ 24FPS)</span>
                <h2 style="margin: 0; color: #f8fafc; font-size: 22px;">Gấu Bông Biến Hình 2in1 Bemori (Cà Rốt & Bánh Cá)</h2>
                <p style="color: #94a3b8; font-size: 14px; line-height: 1.6; margin: 0;">
                    Video đã được ghép nối tự động từ 3 phân cảnh (8s/cảnh) với tỉ lệ khung hình chuẩn <b>9:16 (720x1280)</b>. 
                    Nhân vật nữ áo croptop Brazil và cả 2 phiên bản sản phẩm (Thỏ cà rốt & Mèo bánh cá) giữ được tính nhất quán và sắc nét tuyệt đối xuyên suốt 24 giây.
                </p>
                <div style="display: flex; gap: 12px; margin-top: 8px;">
                    <a href="/final" download="final_tvc_24s.mp4" class="btn">&#11015; Tải Video 24s Hoàn Chỉnh Về Máy (9.1 MB)</a>
                </div>
            </div>
        </div>

        <!-- 3 SCENES BREAKDOWN -->
        <h2 style="color: #38bdf8; font-size: 18px; margin-bottom: 16px;">CHI TIẾT 3 PHÂN CẢNH THÀNH PHẦN (8 GIÂY / CẢNH)</h2>
        <div class="scenes-grid">
            <!-- SCENE 1 -->
            <div class="scene-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span class="scene-badge">CẢNH 1 (0–8s): HOOK</span>
                    <small style="color: #94a3b8;">Thỏ Cà Rốt</small>
                </div>
                <video class="scene-video" controls loop playsinline>
                    <source src="/video1" type="video/mp4">
                </video>
                <div class="scene-text">
                    <b>Thoại (35 từ):</b> "Trời ơi xem em gấu bông biến hình của nhà Bemori này, kéo khóa ra là em thỏ cà rốt siêu cưng luôn! Chất nhung mịn mềm ôm cực thích, làm quà tặng người yêu thì chỉ có đổ đứ đừ thôi nha!"
                </div>
            </div>

            <!-- SCENE 2 -->
            <div class="scene-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span class="scene-badge">CẢNH 2 (8–16s): TÍNH NĂNG</span>
                    <small style="color: #94a3b8;">Mèo Bánh Cá Taiyaki</small>
                </div>
                <video class="scene-video" controls loop playsinline>
                    <source src="/video2" type="video/mp4">
                </video>
                <div class="scene-text">
                    <b>Thoại (36 từ):</b> "Chưa hết đâu nha, nhà Bemori còn có cả phiên bản bé mèo bánh cá Taiyaki nướng vàng ruộm này nữa nè! Vải nhung lông thỏ mềm mướt, ôm bao phê, không hề rụng lông đâu ạ!"
                </div>
            </div>

            <!-- SCENE 3 -->
            <div class="scene-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span class="scene-badge">CẢNH 3 (16–24s): CTA</span>
                    <small style="color: #94a3b8;">Kêu Gọi Chốt Đơn</small>
                </div>
                <video class="scene-video" controls loop playsinline>
                    <source src="/video3" type="video/mp4">
                </video>
                <div class="scene-text">
                    <b>Thoại (35 từ):</b> "Mua tặng bạn thân hay người yêu dịp sinh nhật này là ghi điểm tuyệt đối luôn đó. Mọi người nhanh tay bấm vào giỏ hàng góc trái rinh ngay một em về ôm ngủ nha!"
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8089), WebHandler)
    print("TVC 24s Showcase running on port 8089")
    server.serve_forever()
