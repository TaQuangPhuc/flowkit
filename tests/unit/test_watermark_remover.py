import pytest
from pathlib import Path
from PIL import Image
import numpy as np

from agent.services.watermark_remover import (
    remove_watermark_from_image,
    remove_watermark_from_bytes,
    _load_template,
)

SAMPLE_IMAGE = Path("/home/pc/.gemini/antigravity-ide/brain/78379e3b-58f1-485d-812f-f438485cb42a/fresh_generated_image.jpg")


def test_template_load():
    tmpl48 = _load_template(48)
    assert tmpl48.shape == (48, 48)
    assert 0.0 <= tmpl48.min()
    assert tmpl48.max() == 1.0


def test_clean_sample_image():
    if not SAMPLE_IMAGE.exists():
        pytest.skip("Sample image not found")
    with Image.open(SAMPLE_IMAGE) as img:
        cleaned = remove_watermark_from_image(img)
        assert cleaned.size == img.size
        # Verify watermark was removed by checking the center area
        # At (898+24, 898+24) = (922, 922)
        orig_arr = np.array(img, dtype=np.float32)
        clean_arr = np.array(cleaned, dtype=np.float32)
        # Original watermark had bright white added, so cleaned pixels should be darker
        # than original watermarked pixels at center
        patch_orig = orig_arr[898:898+48, 898:898+48]
        patch_clean = clean_arr[898:898+48, 898:898+48]
        assert patch_clean.mean() < patch_orig.mean()


def test_no_watermark_passthrough():
    # A plain solid color image should have no watermark detected and be unchanged
    img = Image.new("RGB", (512, 512), (100, 150, 200))
    cleaned = remove_watermark_from_image(img)
    arr_orig = np.array(img)
    arr_clean = np.array(cleaned)
    assert np.array_equal(arr_orig, arr_clean)


def test_clean_bytes():
    if not SAMPLE_IMAGE.exists():
        pytest.skip("Sample image not found")
    data = SAMPLE_IMAGE.read_bytes()
    cleaned_data = remove_watermark_from_bytes(data)
    assert len(cleaned_data) > 0
    assert cleaned_data != data


def test_clean_landscape_image():
    raw_path = Path.home() / ".flowkit" / "media_cache" / "d78822b0-8847-4de8-b269-37ae9e31008e_raw.jpg"
    if not raw_path.exists():
        pytest.skip("Landscape sample image not found")
    with Image.open(raw_path) as img:
        cleaned = remove_watermark_from_image(img)
        assert cleaned.size == img.size
        orig_arr = np.array(img, dtype=np.float32)
        clean_arr = np.array(cleaned, dtype=np.float32)
        # At (1255, 647), watermark was removed
        patch_orig = orig_arr[647:647+48, 1255:1255+48]
        patch_clean = clean_arr[647:647+48, 1255:1255+48]
        assert patch_clean.mean() < patch_orig.mean()
