from auto_tvc_server import (
    VOICE_PROFILES, lock_scene_voice, normalize_vietnamese_dialogue,
)


def test_fixes_common_typos():
    s = normalize_vietnamese_dialogue("sản phẩn này khuyễn mãi, bấm giỏ hang nhận ưu đải", "female_north")
    assert "sản phẩm" in s
    assert "khuyến mãi" in s
    assert "giỏ hàng" in s
    assert "ưu đãi" in s


def test_north_drops_southern_particles():
    s = normalize_vietnamese_dialogue("mua nghen thiệt sự luôn á", "female_north")
    assert "nghen" not in s.lower()


def test_lock_scene_voice_is_identical_across_scenes():
    v = VOICE_PROFILES["female_north"]
    a = lock_scene_voice({"audio_dialogue": "Chị em ơi nhé", "video_motion_prompt": "Creator waves. Say: \"old\" in a squeaky voice."}, v)
    b = lock_scene_voice({"audio_dialogue": "Bấm giỏ hang nhé", "video_motion_prompt": "Creator smiles."}, v)
    assert "SPEAKER_ID CHAR_VN_FEMALE_NORTH_01" in a["video_motion_prompt"]
    assert "SPEAKER_ID CHAR_VN_FEMALE_NORTH_01" in b["video_motion_prompt"]
    assert a["video_motion_prompt"].split("SPEAKER_ID")[1][:40] == b["video_motion_prompt"].split("SPEAKER_ID")[1][:40]
    assert "giỏ hàng" in b["audio_dialogue"]
    assert "squeaky" not in a["video_motion_prompt"]
