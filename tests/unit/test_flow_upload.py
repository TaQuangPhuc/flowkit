"""Nova sends image_base64; local tools may still send file_path."""
from pathlib import Path

import pytest

from agent.api.flow import UploadImageRequest, decode_upload_image


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
