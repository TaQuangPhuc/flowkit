"""Nova sends image_base64; local tools may still send file_path."""
from pathlib import Path

import pytest

from agent.api.flow import UploadImageRequest, decode_upload_image, optimize_image_for_upload


JPEG_B64 = "aGVsbG8="  # "hello" — decoder does not inspect pixels


class TestDecodeUploadImage:
    def test_raw_base64(self):
        b64, mime, name = decode_upload_image(UploadImageRequest(
            image_base64=JPEG_B64, mime_type="image/jpeg",
        ))
        assert b64 == JPEG_B64
        assert mime == "image/jpeg"
        assert name == "image.jpg"

    def test_data_url(self):
        b64, mime, name = decode_upload_image(UploadImageRequest(
            image_base64=f"data:image/png;base64,{JPEG_B64}",
        ))
        assert b64 == JPEG_B64
        assert mime == "image/png"
        assert name == "image.png"

    def test_data_url_whitespace(self):
        b64, mime, _ = decode_upload_image(UploadImageRequest(
            image_base64=f"data:image/jpeg;base64,{JPEG_B64[:3]}\n{JPEG_B64[3:]}",
        ))
        assert b64 == JPEG_B64
        assert mime == "image/jpeg"

    def test_file_path(self, tmp_path: Path):
        p = tmp_path / "cup.jpg"
        p.write_bytes(b"\xff\xd8\xff")
        b64, mime, name = decode_upload_image(UploadImageRequest(file_path=str(p)))
        assert mime == "image/jpeg"
        assert name == "image.png"
        assert b64  # non-empty

    def test_missing_both(self):
        with pytest.raises(ValueError, match="image_base64 or file_path"):
            decode_upload_image(UploadImageRequest())

    def test_empty_data_url(self):
        with pytest.raises(ValueError, match="empty"):
            decode_upload_image(UploadImageRequest(image_base64="data:image/jpeg;base64,"))

    def test_missing_file(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            decode_upload_image(UploadImageRequest(file_path=str(tmp_path / "nope.png")))

    def test_request_allows_empty_file_path(self):
        body = UploadImageRequest(image_base64=JPEG_B64)
        assert body.file_path == ""


class TestOptimizeImageForUpload:
    def test_fallback_on_invalid_image(self):
        b64, mime, name = optimize_image_for_upload(JPEG_B64, "image/jpeg", "image.jpg")
        assert b64 == JPEG_B64
        assert mime == "image/jpeg"
        assert name == "image.jpg"

    def test_small_image_untouched(self):
        import io, base64
        from PIL import Image
        img = Image.new("RGB", (200, 200), (10, 20, 30))
        buf = io.BytesIO()
        img.save(buf, "JPEG")
        orig_b64 = base64.b64encode(buf.getvalue()).decode()
        b64, mime, name = optimize_image_for_upload(orig_b64, "image/jpeg", "sample.jpg")
        assert b64 == orig_b64
        assert mime == "image/jpeg"
        assert name == "sample.jpg"

    def test_large_image_resized_and_compressed(self):
        import io, base64
        from PIL import Image
        img = Image.new("RGB", (3000, 2000), (100, 150, 200))
        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=95)
        large_b64 = base64.b64encode(buf.getvalue()).decode()
        b64, mime, name = optimize_image_for_upload(large_b64, "image/png", "raw_photo.png")
        assert mime == "image/jpeg"
        assert name == "raw_photo.jpg"
        out_img = Image.open(io.BytesIO(base64.b64decode(b64)))
        assert max(out_img.size) == 1536
        assert out_img.size == (1536, 1024)

