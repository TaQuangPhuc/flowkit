import json

from agent.services.flow_debug_log import note_grpc, note_http, redact, slim


def test_redact_strips_tokens_and_blobs():
    text = "Bearer ya29.abc_DEF-123 " + ("A" * 90) + " tail"
    out = redact(text, 200)
    assert "ya29" not in out
    assert "AAAA" not in out
    assert "[redacted]" in out
    assert "[blob]" in out


def test_slim_keeps_error_and_collapses_signed_url():
    body = slim({
        "error": "GOOGLE_TRANSIENT",
        "error_code": "google_transient",
        "retryable": True,
        "prompt": "a long customer prompt",
        "operations": [{
            "status": "MEDIA_GENERATION_STATUS_PENDING",
            "operation": {"name": "9b51fb0f-52b3-4f9d-90e2-e18a003b2dd3"},
            "metadata": {"video": {
                "fifeUrl": "https://flow-content.google/video/abc?Signature=SECRET",
            }},
        }],
    })
    assert body["prompt"] == "[redacted]"
    assert body["retryable"] is True
    assert body["operations"][0]["metadata"]["video"]["fifeUrl"] == "video"
    assert "SECRET" not in json.dumps(body)


def test_note_http_and_grpc_write_redacted_lines(tmp_path, monkeypatch):
    import agent.services.flow_debug_log as dbg
    monkeypatch.setattr(dbg, "_ENABLED", True)
    path = tmp_path / "flow-exchange-api.jsonl"
    handler_path = path

    class _H:
        def __init__(self):
            self.lines = []

        def info(self, line):
            self.lines.append(line)

    fake = _H()
    monkeypatch.setattr(dbg._logger, "info", fake.info)
    monkeypatch.setattr(dbg._logger, "handlers", [object()])

    note_http("POST", "/api/flow/generate-image", 429, 40,
              json.dumps({
                  "error": "GOOGLE_TRANSIENT",
                  "detail": "grpc[friend] Internal error encountered. Bearer ya29.secret",
                  "retryable": True,
              }).encode())
    note_grpc(
        "friend", "BatchGetMedia", 80,
        resp=b"https://flow-content.google/image/82efb500-13a2-4247-84e1-79089952e7c6",
    )
    note_grpc("friend", "BatchGenerateImages", 12, error="Internal error encountered.")
    assert len(fake.lines) == 3
    http = json.loads(fake.lines[0])
    assert http["status"] == 429
    assert http["body"]["retryable"] is True
    assert "ya29" not in fake.lines[0]
    media = json.loads(fake.lines[1])
    assert media["has_image"] is True
    assert media["has_video"] is False
    assert media["uuids"] == ["82efb500-13a2-4247-84e1-79089952e7c6"]
    err = json.loads(fake.lines[2])
    assert "Internal error" in err["google_text"][0]
    assert handler_path  # path reserved for the on-disk handler
