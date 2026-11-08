from auto_tvc_server import apply_dual_ref_lock, apply_photo_look


def test_ugc_two_refs_blocks_species_blend():
    out = apply_dual_ref_lock("CREATOR REFERENCE LOCK: a woman. PRODUCT: cat plush.", 2, "ugc")
    assert "SPECIES AND ROLE LOCK" in out
    assert "Never replace the creator" in out
    assert "Do not copy the background" in out
    assert "product-only" in out
    assert "Never drop the person" in out


def test_unboxing_two_refs_is_product_plus_bg_not_hybrid_lock():
    prompt = "PRODUCT REFERENCE LOCK: a bottle."
    assert apply_dual_ref_lock(prompt, 2, "unboxing") == prompt


def test_single_ref_unchanged():
    prompt = "a woman holding a pillow"
    assert apply_dual_ref_lock(prompt, 1, "ugc") == prompt


def test_photo_look_strips_8k_cgi():
    out = apply_photo_look("a woman. Photorealistic 8k vertical 9:16")
    assert "photorealistic 8k vertical" not in out.lower()
    assert "CAMERA LOOK" in out
    assert "plastic skin" in out
