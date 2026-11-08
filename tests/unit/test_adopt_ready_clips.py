from pathlib import Path
import auto_tvc_server as tvc


def test_disk_clip_flips_rendering_to_completed(tmp_path, monkeypatch):
    monkeypatch.setattr(tvc, "WORK_DIR", tmp_path)
    monkeypatch.setattr(tvc, "JOBS", {})
    jdir = tmp_path / "abc123"
    jdir.mkdir()
    (jdir / "clip_1.mp4").write_bytes(b"x" * 60000)
    tvc.JOBS["abc123"] = {
        "job_id": "abc123",
        "scenes": [
            {"scene_id": 1, "status": "RENDERING_VIDEO", "video_url": None},
            {"scene_id": 2, "status": "RENDERING_VIDEO", "video_url": None},
        ],
    }
    n = tvc.adopt_ready_clips("abc123")
    assert n == 1
    sc = tvc.JOBS["abc123"]["scenes"][0]
    assert sc["status"] == "COMPLETED"
    assert sc["video_url"] == "/job/abc123/clip/1"
    assert tvc.JOBS["abc123"]["scenes"][1]["status"] == "RENDERING_VIDEO"
