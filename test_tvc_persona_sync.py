#!/usr/bin/env python3
"""
Unit tests for TVC Persona Demographics & Audio Synchronization Suite.
Covers:
1. determine_product_persona across gender/regional demographic matrices and unisex products.
2. Semantic replacements for forbidden openers and dialect slang without nonsensical repetition.
3. sync_dialogue_to_motion_prompt lip-synchronization lock between video motion prompt and audio dialogue.
4. mux_tts_to_video timing buffer, lead-in delay, and tempo scaling.
5. smart_concat_videos duration enforcement across 8.0s, 6.0s, etc.
"""

import os
import sys
import unittest
import subprocess
import shutil
import re
from pathlib import Path

# Add flowkit to path
FLOWKIT_DIR = Path("/home/pc/flowkit")
sys.path.insert(0, str(FLOWKIT_DIR))

from auto_tvc_server import (
    determine_product_persona,
    apply_persona_replacements,
    sync_dialogue_to_motion_prompt,
    get_media_duration,
    mux_tts_to_video,
    smart_concat_videos,
    VOICE_PROFILES,
    BGM_PROFILES
)


class TestTVCPersonaDemographics(unittest.TestCase):
    """Test persona identification and demographic opener selection."""

    def test_male_grooming_female_north_voice(self):
        """User's bug report case: Shaving razor reviewed by female northern voice."""
        res = determine_product_persona(
            product_name="Dao Cạo Râu Cổ Điển Inox 304 Kèm Hộp Gương GEARSHOP",
            product_category="Personal Grooming",
            voice_key="female_north",
            model_gender="female"
        )
        self.assertTrue(res["is_male_product"])
        self.assertFalse(res["is_female_product"])
        self.assertFalse(res["is_unisex_product"])
        self.assertEqual(res["target_persona"], "female_speaking_male_product")
        
        # 'mấy bà ơi' MUST be strictly forbidden
        self.assertIn("mấy bà ơi", res["forbidden_words"])
        
        # Openers must be neutral or male/gift oriented, NOT peer female slang
        for opener in res["recommended_openers"]:
            self.assertNotIn("mấy bà", opener.lower())
        self.assertIn("Mọi người ơi", res["recommended_openers"])
        self.assertIn("Anh em ơi", res["recommended_openers"])

    def test_male_grooming_male_voice(self):
        """Male speaker reviewing men's product."""
        res = determine_product_persona(
            product_name="Máy cạo râu và tông đơ cắt tóc nam",
            voice_key="male_north",
            model_gender="male"
        )
        self.assertEqual(res["target_persona"], "male_speaker")
        self.assertIn("mấy bà ơi", res["forbidden_words"])
        self.assertIn("chị em ơi", res["forbidden_words"])
        self.assertIn("mê xỉu", res["forbidden_words"])
        self.assertIn("Anh em ơi", res["recommended_openers"])

    def test_male_voice_general_product(self):
        """Male speaker reviewing unisex product - must still never use female slang."""
        res = determine_product_persona(
            product_name="Bình giữ nhiệt inox 316 cao cấp 1000ml",
            voice_key="male_south",
            model_gender="male"
        )
        self.assertEqual(res["target_persona"], "male_speaker")
        self.assertIn("mấy bà ơi", res["forbidden_words"])
        self.assertIn("Anh em ơi", res["recommended_openers"])

    def test_female_cosmetics_female_north(self):
        """Female northern voice reviewing women's lipstick."""
        res = determine_product_persona(
            product_name="Son môi kem lì dưỡng ẩm kháng nước",
            voice_key="female_north",
            model_gender="female"
        )
        self.assertTrue(res["is_female_product"])
        self.assertFalse(res["is_male_product"])
        self.assertIn("mấy bà ơi", res["forbidden_words"]) # Northern speakers don't say mấy bà ơi
        self.assertIn("Chị em ơi", res["recommended_openers"])
        self.assertIn("Mọi người ơi", res["recommended_openers"])

    def test_female_cosmetics_female_south(self):
        """Female southern voice reviewing women's lipstick."""
        res = determine_product_persona(
            product_name="Son dưỡng môi hồng tự nhiên dành cho nữ",
            voice_key="female_south",
            model_gender="female"
        )
        self.assertTrue(res["is_female_product"])
        self.assertIn("Chị em ơi", res["recommended_openers"])

    def test_unisex_product_detection(self):
        """Unisex product with 'cho nam nữ' must NOT be forced into male-only persona."""
        res = determine_product_persona(
            product_name="Áo Thun Unisex Cho Nam Nữ Phong Cách Hàn Quốc",
            voice_key="female_north"
        )
        self.assertTrue(res["is_unisex_product"])
        self.assertFalse(res["is_male_product"])
        self.assertFalse(res["is_female_product"])
        self.assertEqual(res["target_persona"], "general_product")
        self.assertIn("Mọi người ơi", res["recommended_openers"])
        self.assertIn("mấy bà ơi", res["forbidden_words"])

    def test_strictly_male_grooming_with_nam_nu_in_title(self):
        """A razor titled 'Dao Cạo Râu... Cho Nam Nữ' is still strictly male grooming."""
        res = determine_product_persona(
            product_name="Dao Cạo Râu Cổ Điển GEARSHOP | Bàn Cạo Râu Vintage Cho Nam Nữ",
            voice_key="female_north"
        )
        self.assertTrue(res["is_male_product"])
        self.assertEqual(res["target_persona"], "female_speaking_male_product")
        self.assertIn("mấy bà ơi", res["forbidden_words"])


class TestSemanticSafeguardSanitization(unittest.TestCase):
    """Test semantic keyword replacement avoiding nonsensical repetition."""

    def test_semantic_replacements_preserve_grammar(self):
        """Ensure slang like 'mê xỉu' or 'nghen' is replaced by natural expressions, NOT by opener."""
        persona = determine_product_persona(
            product_name="Dao Cạo Râu Inox GEARSHOP",
            voice_key="female_north"
        )
        raw_text = "Mấy bà ơi chiếc dao cạo này sáng bóng mê xỉu luôn nghen!"
        sanitized = apply_persona_replacements(raw_text, persona["replacements"])

        self.assertNotIn("mấy bà ơi", sanitized.lower())
        self.assertNotIn("mê xỉu", sanitized.lower())
        self.assertNotIn("nghen", sanitized.lower())
        # Must NOT produce repetitive "Mọi người ơi chiếc dao cạo này Mọi người ơi..."
        self.assertEqual(sanitized.count("Mọi người ơi"), 1)
        self.assertIn("thích mê", sanitized)
        self.assertIn("nhé", sanitized)
        self.assertEqual(sanitized, "Mọi người ơi chiếc dao cạo này sáng bóng thích mê luôn nhé!")

    def test_male_speaker_semantic_replacements(self):
        """Male speaker replacing female slang with masculine terms."""
        persona = determine_product_persona(
            product_name="Bình giữ nhiệt",
            voice_key="male_north"
        )
        raw_text = "Mấy bà ơi cái bình này cưng xỉu luôn á!"
        sanitized = apply_persona_replacements(raw_text, persona["replacements"])

        self.assertNotIn("mấy bà ơi", sanitized.lower())
        self.assertNotIn("cưng xỉu", sanitized.lower())
        self.assertTrue(sanitized.startswith("Anh em ơi"))
        self.assertIn("quá ưng", sanitized)


class TestMotionPromptDialogueLipSync(unittest.TestCase):
    """Test lip-sync locking between video_motion_prompt and audio_dialogue."""

    def test_sync_replaces_escaped_say_clause(self):
        motion = 'CREATOR LOCK — HIGHEST PRIORITY: Creator looks at camera. Say: \\"Mấy bà ơi chân ái đây rồi, mê xỉu luôn nhé!\\" in natural voice'
        dialogue = "Mọi người ơi chân ái cạo râu đây rồi, dao cạo inox siêu êm nhé!"
        synced = sync_dialogue_to_motion_prompt(motion, dialogue)

        self.assertIn('Say: \\"Mọi người ơi chân ái cạo râu đây rồi, dao cạo inox siêu êm nhé!\\"', synced)
        self.assertNotIn("Mấy bà ơi", synced)
        self.assertTrue(synced.endswith("in natural voice"))

    def test_sync_replaces_unescaped_say_clause(self):
        motion = 'CREATOR LOCK: Creator smiles. Say: "old text here" in sweet tone.'
        dialogue = "Chào cả nhà, đây là sản phẩm tuyệt vời nhé!"
        synced = sync_dialogue_to_motion_prompt(motion, dialogue)

        self.assertIn('Say: \\"Chào cả nhà, đây là sản phẩm tuyệt vời nhé!\\"', synced)
        self.assertNotIn("old text here", synced)

    def test_sync_appends_when_say_missing(self):
        motion = "CREATOR LOCK: Creator holds product in front of camera."
        dialogue = "Mọi người ơi xem ngay nhé!"
        synced = sync_dialogue_to_motion_prompt(motion, dialogue, veo_voice_prompt="natural female Northern accent")

        self.assertIn('Say: \\"Mọi người ơi xem ngay nhé!\\"', synced)
        self.assertIn("in natural female Northern accent", synced)


class TestTVCAudioSyncAndMux(unittest.TestCase):
    """Test audio synchronization, tempo scaling, and stream muxing."""

    @classmethod
    def setUpClass(cls):
        cls.test_dir = Path("/tmp/test_tvc_sync_suite")
        cls.test_dir.mkdir(parents=True, exist_ok=True)

        # Create a synthetic 8.0s test video with internal dummy audio
        cls.dummy_video = cls.test_dir / "dummy_video_with_audio.mp4"
        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "color=c=navy:s=720x1280:d=8.0:r=24",
            "-f", "lavfi", "-i", "sine=f=440:d=8.0",
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k",
            str(cls.dummy_video)
        ]
        subprocess.run(cmd, check=True, capture_output=True)

        # Create a synthetic 10.0s test TTS audio (longer than 8.0s video)
        cls.long_tts = cls.test_dir / "dummy_long_tts.mp3"
        cmd_long = [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "sine=f=880:d=10.0",
            "-c:a", "libmp3lame", "-b:a", "48k", "-ar", "24000",
            str(cls.long_tts)
        ]
        subprocess.run(cmd_long, check=True, capture_output=True)

        # Create a synthetic 5.0s test TTS audio (shorter than 8.0s video)
        cls.short_tts = cls.test_dir / "dummy_short_tts.mp3"
        cmd_short = [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "sine=f=880:d=5.0",
            "-c:a", "libmp3lame", "-b:a", "48k", "-ar", "24000",
            str(cls.short_tts)
        ]
        subprocess.run(cmd_short, check=True, capture_output=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir, ignore_errors=True)

    def test_media_duration_probe(self):
        """Verify accurate duration probing."""
        dur = get_media_duration(self.dummy_video)
        self.assertAlmostEqual(dur, 8.0, delta=0.05)
        
        tts_dur = get_media_duration(self.long_tts)
        self.assertAlmostEqual(tts_dur, 10.0, delta=0.05)

    def test_mux_overwrites_original_video_audio(self):
        """Muxing must discard the video's original audio stream and use TTS."""
        out_vid = self.test_dir / "out_muxed.mp4"
        shutil.copy(self.dummy_video, out_vid)

        success = mux_tts_to_video(out_vid, self.long_tts, target_duration=8.0)
        self.assertTrue(success)

        # Check duration is exactly 8.0s
        dur = get_media_duration(out_vid)
        self.assertAlmostEqual(dur, 8.0, delta=0.05)

        # Check that audio stream has 24000 Hz (from the TTS track)
        probe = subprocess.check_output([
            "ffprobe", "-v", "error", "-select_streams", "a",
            "-show_entries", "stream=sample_rate",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(out_vid)
        ]).decode().strip()
        self.assertEqual(probe, "24000")

    def test_mux_short_tts_does_not_truncate_video(self):
        """When TTS is shorter (5s), video must NOT be truncated to 5s!"""
        out_vid = self.test_dir / "out_short_muxed.mp4"
        shutil.copy(self.dummy_video, out_vid)

        success = mux_tts_to_video(out_vid, self.short_tts, target_duration=8.0)
        self.assertTrue(success)

        # Video must remain 8.0s
        dur = get_media_duration(out_vid)
        self.assertAlmostEqual(dur, 8.0, delta=0.05)


class TestSmartConcatDurations(unittest.TestCase):
    """Test smart_concat_videos duration handling."""

    @classmethod
    def setUpClass(cls):
        cls.test_dir = Path("/tmp/test_tvc_concat_suite")
        cls.test_dir.mkdir(parents=True, exist_ok=True)

        # Create two 8.0s clips
        cls.clips = []
        for i in range(2):
            c_p = cls.test_dir / f"clip_{i}.mp4"
            cmd = [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", f"color=c={'blue' if i==0 else 'green'}:s=720x1280:d=8.0:r=24",
                "-f", "lavfi", "-i", "sine=f=440:d=8.0",
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "128k",
                str(c_p)
            ]
            subprocess.run(cmd, check=True, capture_output=True)
            cls.clips.append(c_p)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir, ignore_errors=True)

    def test_smart_concat_enforces_8s_target_duration(self):
        """When target_duration=8.0 is passed, smart_concat seamlessly handles clips."""
        out_mp4 = self.test_dir / "final_concat.mp4"
        smart_concat_videos(self.clips, out_mp4, target_duration=8.0)
        dur = get_media_duration(out_mp4)
        # 2 clips of 8.0s with 0.15s end trim on clip 0 and 0.35s crossfade = 7.85 + 8.0 - 0.35 = 15.5s
        self.assertAlmostEqual(dur, 15.5, delta=0.1)


if __name__ == "__main__":
    unittest.main()
