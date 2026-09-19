"""Unit tests for Master Flow Fashion Lookbook Studio."""

import pytest
from fastapi.testclient import TestClient
import fashion_lookbook_studio as fls
from agent.main import app

client = TestClient(app)


def test_presets_catalog():
    """Verify Master Flow presets contain all 15 poses, styles, and models."""
    presets = fls.get_all_lookbook_presets()
    assert len(presets["poses"]) == 15
    assert "Toàn thân, nhìn thẳng" in presets["poses"]
    assert "Cận cảnh chi tiết chất liệu vải" in presets["poses"]
    assert "Tư thế đang xoay người" in presets["poses"]
    assert len(presets["styles"]) == 5
    assert len(presets["lightings"]) == 4
    assert len(presets["backgrounds"]) == 5
    assert len(presets["motions"]) == 5
    assert len(presets["cameras"]) == 4
    assert "Omni Flash" in presets["video_models"]


def test_presets_api_endpoint():
    """Verify GET /api/fashion-lookbook/presets returns 200 with presets."""
    resp = client.get("/api/fashion-lookbook/presets")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert len(data["presets"]["poses"]) == 15


def test_prompt_builders():
    """Verify prompt builder output conforms to Master Flow specification."""
    p_img = fls.build_image_prompt(
        pose="Toàn thân, nhìn thẳng",
        aspect_ratio="3:4",
        quality="4K",
        style="Luxury boutique fashion",
        lighting="soft studio lighting",
        background_preset="pure white studio",
        has_background_ref=False
    )
    assert "Toàn thân, nhìn thẳng" in p_img
    assert "Sử dụng bối cảnh: pure white studio." in p_img
    assert "tỷ lệ 3:4" in p_img
    assert "chất lượng 4K" in p_img

    p_bg_ref = fls.build_image_prompt(
        pose="Góc ba phần tư",
        aspect_ratio="9:16",
        quality="2K",
        style="Clean ecommerce catalog",
        lighting="natural daylight",
        background_preset="",
        has_background_ref=True
    )
    assert "SỬ DỤNG CHÍNH XÁC phông nền từ hình ảnh tham chiếu" in p_bg_ref

    p_vid = fls.build_video_prompt(
        motion_preset="Elegant Turnaround",
        camera_movement="Cinematic Push-in",
        duration=8
    )
    assert "Người mẫu thực hiện: Elegant Turnaround." in p_vid
    assert "Camera thực hiện: Cinematic Push-in." in p_vid
    assert "dài 8 giây" in p_vid


def test_select_default_poses():
    """Verify select_default_poses returns the correct count and valid poses."""
    p1 = fls.select_default_poses(1)
    assert len(p1) == 1
    assert p1[0] in fls.POSES

    p4 = fls.select_default_poses(4)
    assert len(p4) == 4
    for p in p4:
        assert p in fls.POSES

    p8 = fls.select_default_poses(8)
    assert len(p8) == 8
    for p in p8:
        assert p in fls.POSES


def test_stage1_validation_no_products():
    """Verify Stage 1 rejects request without products."""
    resp = client.post("/api/fashion-lookbook/stage1/generate-images", json={})
    assert resp.status_code == 400
    assert "ảnh sản phẩm" in resp.json()["detail"]


def test_stage2_validation_invalid_job():
    """Verify Stage 2 rejects request with invalid job ID."""
    resp = client.post(
        "/api/fashion-lookbook/stage2/generate-videos",
        json={"job_id": "non_existent_job_12345", "selected_image_ids": [1]}
    )
    assert resp.status_code == 400


def test_save_lookbook_job_signatures():
    """Verify save_lookbook_job accepts positional, keyword, and unpacked dicts without collision."""
    j1 = fls.save_lookbook_job("sig_1", job_id="sig_1", status="QUEUED")
    assert j1["job_id"] == "sig_1"
    assert j1["status"] == "QUEUED"

    # Dict unpacking with job_id included in the dict
    sample_dict = {"job_id": "sig_2", "status": "ANALYZING", "progress_percent": 10}
    j2 = fls.save_lookbook_job("sig_2", **sample_dict)
    assert j2["job_id"] == "sig_2"
    assert j2["status"] == "ANALYZING"

    # Keyword-only
    j3 = fls.save_lookbook_job(job_id="sig_3", status="COMPLETED")
    assert j3["job_id"] == "sig_3"
    assert j3["status"] == "COMPLETED"

