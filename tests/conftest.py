"""Shared pytest fixtures for Flow Kit tests."""

import pytest


@pytest.fixture(scope="session", autouse=True)
def isolate_database(tmp_path_factory):
    """Point every sqlite consumer at a throwaway DB for the whole test session.

    Declared first so it is set up before any other autouse fixture and torn
    down last. Without it the suite wrote straight into the production
    flow_agent.db: replay rows, failover mappings and TEST_TIMEOUT incidents
    from fixtures all landed in live tables.

    DB_PATH is imported into each module at import time, but every consumer
    reads the module global at call time, so rebinding the attribute is enough.
    IncidentManager binds it as a default argument, so its singleton is rebuilt.
    """
    import asyncio

    import agent.config as config
    import agent.db.schema as schema
    import agent.services.central_watchdog as central_watchdog
    import agent.services.flow_failover as flow_failover
    import agent.services.incident_manager as incident_manager

    tmp_db = tmp_path_factory.mktemp("flowdb") / "flow_agent_test.db"
    targets = (config, schema, central_watchdog, flow_failover, incident_manager)
    originals = [(m, m.DB_PATH) for m in targets]
    prev_incident_instance = incident_manager.IncidentManager._instance

    for module, _ in originals:
        module.DB_PATH = tmp_db
    incident_manager.IncidentManager._instance = incident_manager.IncidentManager(db_path=tmp_db)

    asyncio.run(schema.init_db())

    yield tmp_db

    for module, original in originals:
        module.DB_PATH = original
    incident_manager.IncidentManager._instance = prev_incident_instance


@pytest.fixture(scope="session", autouse=True)
def close_shared_database():
    yield
    import asyncio
    from agent.db.schema import close_db
    async def close_with_deadline():
        await asyncio.wait_for(close_db(), timeout=5)
    asyncio.run(close_with_deadline())


@pytest.fixture
def sample_uuid():
    return "550e8400-e29b-41d4-a716-446655440000"


@pytest.fixture
def sample_cams_id():
    """A CAMS... base64 mediaGenerationId — NOT a valid UUID."""
    return "CAMSJDkxMTYwNzM4LTRlMjYtNDVkZi05OTMz"


@pytest.fixture
def sample_image_success(sample_uuid):
    """Successful image generation response from Google Flow API."""
    return {
        "data": {
            "media": [{
                "name": sample_uuid,
                "image": {
                    "generatedImage": {
                        "mediaId": sample_uuid,
                        "fifeUrl": f"https://lh3.googleusercontent.com/image/{sample_uuid}?sqp=params",
                    }
                }
            }]
        }
    }


@pytest.fixture
def sample_image_success_no_uuid():
    """Image response where media[0].name is NOT a UUID (CAMS format)."""
    return {
        "data": {
            "media": [{
                "name": "CAMSJDkxMTYwNzM4LTRlMjYtNDVkZi05OTMz",
                "image": {
                    "generatedImage": {
                        "mediaId": "CAMSJDkxMTYwNzM4",
                        "fifeUrl": "https://lh3.googleusercontent.com/image/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee?sqp=params",
                    }
                }
            }]
        }
    }


@pytest.fixture
def sample_video_success(sample_uuid):
    """Successful video generation response."""
    return {
        "data": {
            "operations": [{
                "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL",
                "operation": {
                    "name": "operations/video-123",
                    "metadata": {
                        "video": {
                            "mediaId": sample_uuid,
                            "fifeUrl": f"https://storage.googleapis.com/video/{sample_uuid}",
                        }
                    }
                }
            }]
        }
    }


@pytest.fixture
def sample_error_response():
    """Error response from Google Flow API."""
    return {"error": "Internal error encountered"}


@pytest.fixture
def sample_nested_error():
    """Error nested inside data.error."""
    return {
        "data": {
            "error": {
                "code": 403,
                "message": "caller does not have permission",
            }
        }
    }


@pytest.fixture
def sample_scene_row(sample_uuid):
    """A flat DB row for a scene with completed vertical image."""
    return {
        "id": "scene-001",
        "video_id": "video-001",
        "display_order": 0,
        "prompt": "Hero walks into the castle courtyard at dawn",
        "image_prompt": None,
        "video_prompt": "0-3s: Hero pushes open gate. 3-6s: Looks up. 6-8s: Zoom on sword.",
        "character_names": '["Hero", "Castle"]',
        "parent_scene_id": None,
        "chain_type": "ROOT",
        "vertical_image_media_id": sample_uuid,
        "vertical_image_url": f"https://example.com/image/{sample_uuid}",
        "vertical_image_status": "COMPLETED",
        "vertical_video_media_id": None,
        "vertical_video_url": None,
        "vertical_video_status": "PENDING",
        "vertical_upscale_media_id": None,
        "vertical_upscale_url": None,
        "vertical_upscale_status": "PENDING",
        "vertical_end_scene_media_id": None,
        "horizontal_image_media_id": None,
        "horizontal_image_url": None,
        "horizontal_image_status": "PENDING",
        "horizontal_video_media_id": None,
        "horizontal_video_url": None,
        "horizontal_video_status": "PENDING",
        "horizontal_upscale_media_id": None,
        "horizontal_upscale_url": None,
        "horizontal_upscale_status": "PENDING",
        "horizontal_end_scene_media_id": None,
        "trim_start": None,
        "trim_end": None,
        "duration": None,
        "created_at": "2026-04-01T00:00:00",
        "updated_at": "2026-04-01T00:00:00",
    }


@pytest.fixture
def sample_character_row(sample_uuid):
    """A flat DB row for a character entity."""
    return {
        "id": "char-001",
        "name": "Hero",
        "entity_type": "character",
        "description": "A brave warrior with golden armor",
        "image_prompt": "Full body portrait of a warrior in golden armor, front-facing, neutral background",
        "voice_description": "Deep calm heroic voice",
        "reference_image_url": f"https://example.com/ref/{sample_uuid}",
        "media_id": sample_uuid,
        "created_at": "2026-04-01T00:00:00",
        "updated_at": "2026-04-01T00:00:00",
    }


@pytest.fixture
def mocker():
    """Lightweight fixture providing a patch helper compatible with pytest-mock."""
    from unittest.mock import patch
    class Mocker:
        def __init__(self):
            self._patches = []
        def patch(self, *args, **kwargs):
            p = patch(*args, **kwargs)
            mock = p.start()
            self._patches.append(p)
            return mock
        def stopall(self):
            for p in reversed(self._patches):
                p.stop()
    m = Mocker()
    yield m
    m.stopall()
