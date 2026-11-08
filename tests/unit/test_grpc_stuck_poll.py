from agent.services.flow_grpc import _terminal_video_complaint


def test_media_not_found_is_still_pending():
    assert not _terminal_video_complaint("Media not found.")
    assert not _terminal_video_complaint("")


def test_unsafe_is_terminal():
    assert _terminal_video_complaint("PUBLIC_ERROR_UNSAFE_GENERATION")
    assert _terminal_video_complaint("permission denied")
