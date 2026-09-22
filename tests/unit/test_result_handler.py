"""Tests for agent.sdk.services.result_handler."""

import pytest
from unittest.mock import AsyncMock, patch

from agent.sdk.services.result_handler import parse_result, apply_scene_result, apply_character_result
from agent.sdk.models.media import GenerationResult


@pytest.fixture(autouse=True)
def scene_lookup():
    # These tests isolate field updates; do not open the shared live DB.
    with patch("agent.sdk.services.result_handler.crud.get_scene", new_callable=AsyncMock, return_value=None):
        yield


# ---------------------------------------------------------------------------
# parse_result tests
# ---------------------------------------------------------------------------


def test_parse_result_image_success_extracts_media_id_and_url(sample_image_success, sample_uuid):
    result = parse_result(sample_image_success, "GENERATE_IMAGE")

    assert result.success is True
    assert result.media_id == sample_uuid
    assert sample_uuid in result.url
    assert result.error is None


def test_parse_result_video_success_extracts_media_id_and_url(sample_video_success, sample_uuid):
    result = parse_result(sample_video_success, "GENERATE_VIDEO")

    assert result.success is True
    assert result.media_id == sample_uuid
    assert result.url is not None
    assert result.error is None


def test_parse_result_top_level_error_returns_failure(sample_error_response):
    result = parse_result(sample_error_response, "GENERATE_IMAGE")

    assert result.success is False
    assert result.error == "Internal error encountered"
    assert result.media_id is None


def test_parse_result_nested_error_returns_failure(sample_nested_error):
    result = parse_result(sample_nested_error, "GENERATE_IMAGE")

    assert result.success is False
    assert "permission" in result.error
    assert result.media_id is None


def test_parse_result_raw_is_attached(sample_image_success):
    result = parse_result(sample_image_success, "GENERATE_IMAGE")

    assert result.raw is sample_image_success


# ---------------------------------------------------------------------------
# apply_scene_result tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_scene_result_generate_image_sets_fields_and_cascades(sample_uuid, mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_scene", new_callable=AsyncMock)
    result = GenerationResult(success=True, media_id=sample_uuid, url="https://example.com/img.jpg")

    await apply_scene_result("scene-001", "GENERATE_IMAGE", "VERTICAL", result)

    mock_update.assert_awaited_once()
    _, kwargs = mock_update.call_args
    assert kwargs["vertical_image_media_id"] == sample_uuid
    assert kwargs["vertical_image_status"] == "COMPLETED"
    # Cascade: video and upscale reset to PENDING
    assert kwargs["vertical_video_status"] == "PENDING"
    assert kwargs["vertical_video_media_id"] is None
    assert kwargs["vertical_upscale_status"] == "PENDING"
    assert kwargs["vertical_upscale_media_id"] is None


@pytest.mark.asyncio
async def test_apply_scene_result_edit_image_same_cascade_as_generate(sample_uuid, mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_scene", new_callable=AsyncMock)
    result = GenerationResult(success=True, media_id=sample_uuid, url="https://example.com/img.jpg")

    await apply_scene_result("scene-001", "EDIT_IMAGE", "VERTICAL", result)

    mock_update.assert_awaited_once()
    _, kwargs = mock_update.call_args
    assert kwargs["vertical_image_status"] == "COMPLETED"
    assert kwargs["vertical_video_status"] == "PENDING"
    assert kwargs["vertical_upscale_status"] == "PENDING"


@pytest.mark.asyncio
async def test_apply_scene_result_generate_video_sets_fields_and_cascades_upscale(sample_uuid, mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_scene", new_callable=AsyncMock)
    result = GenerationResult(success=True, media_id=sample_uuid, url="https://storage.googleapis.com/vid.mp4")

    await apply_scene_result("scene-001", "GENERATE_VIDEO", "VERTICAL", result)

    mock_update.assert_awaited_once()
    _, kwargs = mock_update.call_args
    assert kwargs["vertical_video_media_id"] == sample_uuid
    assert kwargs["vertical_video_status"] == "COMPLETED"
    # Cascade: upscale reset to PENDING
    assert kwargs["vertical_upscale_status"] == "PENDING"
    assert kwargs["vertical_upscale_media_id"] is None
    # No image keys touched
    assert "vertical_image_status" not in kwargs


@pytest.mark.asyncio
async def test_apply_scene_result_upscale_video_sets_fields_no_cascade(sample_uuid, mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_scene", new_callable=AsyncMock)
    result = GenerationResult(success=True, media_id=sample_uuid, url="https://storage.googleapis.com/upscale.mp4")

    await apply_scene_result("scene-001", "UPSCALE_VIDEO", "VERTICAL", result)

    mock_update.assert_awaited_once()
    _, kwargs = mock_update.call_args
    assert kwargs["vertical_upscale_media_id"] == sample_uuid
    assert kwargs["vertical_upscale_status"] == "COMPLETED"
    # No image or video keys touched
    assert "vertical_image_status" not in kwargs
    assert "vertical_video_status" not in kwargs


@pytest.mark.asyncio
async def test_apply_scene_result_skips_when_scene_id_is_none(sample_uuid, mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_scene", new_callable=AsyncMock)
    result = GenerationResult(success=True, media_id=sample_uuid, url="https://example.com/img.jpg")

    await apply_scene_result(None, "GENERATE_IMAGE", "VERTICAL", result)

    mock_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_scene_result_skips_when_result_failed(mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_scene", new_callable=AsyncMock)
    result = GenerationResult(success=False, error="API error")

    await apply_scene_result("scene-001", "GENERATE_IMAGE", "VERTICAL", result)

    mock_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_scene_result_horizontal_orientation_uses_correct_prefix(sample_uuid, mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_scene", new_callable=AsyncMock)
    result = GenerationResult(success=True, media_id=sample_uuid, url="https://example.com/img.jpg")

    await apply_scene_result("scene-001", "GENERATE_IMAGE", "HORIZONTAL", result)

    mock_update.assert_awaited_once()
    _, kwargs = mock_update.call_args
    assert kwargs["horizontal_image_media_id"] == sample_uuid
    assert kwargs["horizontal_image_status"] == "COMPLETED"
    assert "vertical_image_status" not in kwargs


# ---------------------------------------------------------------------------
# apply_character_result tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_character_result_sets_media_id_and_url(sample_uuid, mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_character", new_callable=AsyncMock)
    result = GenerationResult(
        success=True,
        media_id=sample_uuid,
        url=f"https://example.com/ref/{sample_uuid}",
    )

    await apply_character_result("char-001", result)

    mock_update.assert_awaited_once_with("char-001", media_id=sample_uuid, reference_image_url=result.url)


@pytest.mark.asyncio
async def test_apply_character_result_skips_when_result_failed(mocker):
    mock_update = mocker.patch("agent.sdk.services.result_handler.crud.update_character", new_callable=AsyncMock)
    result = GenerationResult(success=False, error="generation failed")

    await apply_character_result("char-001", result)

    mock_update.assert_not_awaited()
