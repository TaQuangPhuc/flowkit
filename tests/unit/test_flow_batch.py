"""The batchexecute codec, builders and readers.

These lock down the slots that cost hours to find — see docs/CAPTURE.md and the
comments in flow_batch.py. A payload Flow accepts and then ignores looks exactly
like a payload that worked, so the assertions here are about position, not shape.
"""
import json

import pytest

from agent.services import flow_batch as fb


def envelope(rpcid: str, payload) -> str:
    """A response body as batchexecute serves it: sentinel, then chunks."""
    chunk = json.dumps([["wrb.fr", rpcid, json.dumps(payload)]])
    return f")]}}'\n{len(chunk)}\n{chunk}"


def inner(freq: str):
    """The inner payload back out of an f.req envelope."""
    return json.loads(json.loads(freq)[0][0][1])


class TestEnvelopeCodec:
    def test_build_wraps_inner_as_a_json_string(self):
        freq = fb.build_envelope("rpc1", [1, "two"])
        assert json.loads(freq) == [[["rpc1", '[1,"two"]', None, "generic"]]]

    def test_parse_reads_a_payload_back(self):
        results = fb.parse_envelope(envelope("rpc1", {"a": 1}))
        assert [(r.rpcid, r.data) for r in results] == [("rpc1", {"a": 1})]

    def test_parse_survives_a_truncated_tail(self):
        """A response cut mid-chunk must not cost us the envelopes before it."""
        body = envelope("rpc1", {"a": 1}) + '\n50\n[["wrb.fr","rpc2","[1,2'
        results = fb.parse_envelope(body)
        assert [r.rpcid for r in results] == ["rpc1"]

    def test_parse_tolerates_a_missing_sentinel(self):
        chunk = json.dumps([["wrb.fr", "rpc1", '{"a":1}']])
        assert fb.parse_envelope(chunk)[0].data == {"a": 1}

    def test_empty_body_is_no_results_not_a_crash(self):
        assert fb.parse_envelope("") == []

    def test_error_slot_becomes_an_error_result(self):
        chunk = json.dumps([["wrb.fr", "rpc1", None, None, None, [8]]])
        result = fb.parse_envelope(f")]}}'\n{len(chunk)}\n{chunk}")[0]
        assert not result.ok and result.error == [8]

    def test_first_payload_raises_on_the_error_slot(self):
        chunk = json.dumps([["wrb.fr", "rpc1", None, None, None, [8]]])
        with pytest.raises(fb.RpcError):
            fb.first_payload(f")]}}'\n{len(chunk)}\n{chunk}", "rpc1")

    def test_first_payload_raises_when_the_rpc_is_absent(self):
        with pytest.raises(fb.FlowBatchError):
            fb.first_payload(envelope("other", [1]), "rpc1")

    def test_payloads_returns_every_ok_envelope(self):
        body = envelope("rpc1", {"a": 1}) + envelope("rpc1", {"b": 2})
        assert fb.payloads(body, "rpc1") == [{"a": 1}, {"b": 2}]

    def test_payloads_skips_an_error_chunk_when_a_later_one_is_ok(self):
        """StreamChat can ack with an error-shaped chunk, then the media row."""
        err = json.dumps([["wrb.fr", "rpc1", None, None, None, [5]]])
        body = f")]}}'\n{len(err)}\n{err}" + envelope("rpc1", {"ok": True})
        assert fb.payloads(body, "rpc1") == [{"ok": True}]

    def test_first_payload_is_still_the_first_ok_envelope(self):
        body = envelope("rpc1", {"a": 1}) + envelope("rpc1", {"b": 2})
        assert fb.first_payload(body, "rpc1") == {"a": 1}


class TestImageRequest:
    PID = "11111111-2222-3333-4444-555555555555"

    def test_aspect_lands_in_slot_4_not_a_variant_count(self):
        """Slot 4 is the aspect ratio. `count=1` only looked right because
        1 means square."""
        item = inner(fb.image_request("a cat", self.PID,
                                      aspect="IMAGE_ASPECT_RATIO_LANDSCAPE"))[1][0]
        assert item[4] == fb.ASPECT_LANDSCAPE

    def test_count_repeats_the_item_under_fresh_seeds(self):
        items = inner(fb.image_request("a cat", self.PID, count=3, seed=100))[1]
        assert len(items) == 3
        assert [i[3] for i in items] == [100, 100 + 9973, 100 + 2 * 9973]

    def test_prompts_give_each_variant_its_own_text(self):
        items = inner(fb.image_request("fallback", self.PID, count=3,
                                       prompts=["one", "two"]))[1]
        assert [i[8][0][0][0] for i in items] == ["one", "two", "fallback"]

    def test_reference_puts_the_media_id_first_and_the_type_flag_fourth(self):
        """The arrangement probing never found: wrong ones are accepted and
        then quietly ignored."""
        item = inner(fb.image_request("a cat", self.PID, ref_media_ids=["mid-1"]))[1][0]
        assert item[2] == [["mid-1", None, None, None, fb.REF_TYPE_IMAGE]]

    def test_no_references_leaves_the_slot_null_rather_than_empty(self):
        assert inner(fb.image_request("a cat", self.PID))[1][0][2] is None

    def test_the_captcha_placeholder_is_present_for_the_extension_to_replace(self):
        assert fb.CAPTCHA_SLOT in fb.image_request("a cat", self.PID)

    def test_the_project_id_rides_in_the_context(self):
        assert inner(fb.image_request("a cat", self.PID))[1][0][7][5] == self.PID

    def test_the_model_is_named_in_slot_5(self):
        item = inner(fb.image_request("a cat", self.PID, model="NARWHAL"))[1][0]
        assert item[5] == "NARWHAL"


class TestVideoRequest:
    PID = "11111111-2222-3333-4444-555555555555"

    def test_video_aspect_does_not_share_the_image_encoding(self):
        """1 is portrait here; for an image 1 is square."""
        payload = inner(fb.video_request("go", self.PID, "mid",
                                         aspect="VIDEO_ASPECT_RATIO_PORTRAIT"))
        assert payload[0][0][2] == fb.VIDEO_ASPECT_PORTRAIT

    def test_an_image_aspect_is_refused_rather_than_rendered_wrong(self):
        with pytest.raises(ValueError):
            fb.video_request("go", self.PID, "mid", aspect=fb.ASPECT_LANDSCAPE)

    def test_the_source_media_id_and_a_full_frame_crop_travel_together(self):
        block = inner(fb.video_request("go", self.PID, "mid-9"))[0][0][4]
        assert block[1] == "mid-9"
        assert block[5] == fb.FULL_FRAME_CROP

    def test_a_hand_reframed_crop_overrides_the_default(self):
        crop = [None, 0.1, 1, 0.9]
        assert inner(fb.video_request("go", self.PID, "mid", crop=crop))[0][0][4][5] == crop


class TestT2VRequest:
    PID = "11111111-2222-3333-4444-555555555555"

    def test_uses_the_captured_rpcid_not_i2v(self):
        freq = json.loads(fb.t2v_request("go", self.PID))
        assert freq[0][0][0] == fb.RPC_GEN_T2V
        assert freq[0][0][0] != fb.RPC_GEN_VIDEO

    def test_omits_the_start_image_slot_entirely(self):
        """i2v puts media+crop in variant slot 4; t2v has client uuids there."""
        variant = inner(fb.t2v_request("go", self.PID, count=1))[0][0]
        assert len(variant) == 5
        assert variant[4][1] is None
        assert json.dumps(fb.FULL_FRAME_CROP) not in json.dumps(variant)

    def test_the_captured_model_is_not_an_i2v_name(self):
        variant = inner(fb.t2v_request("go", self.PID, count=1))[0][0]
        assert variant[1] == fb.VIDEO_T2V_MODEL
        assert "i2v" not in variant[1]

    def test_default_count_matches_the_ui_two_variants(self):
        variants = inner(fb.t2v_request("a cat fights a dog", self.PID))[0]
        assert len(variants) == 2
        assert variants[0][0][2][0][0][0] == "a cat fights a dog"
        assert variants[1][0][2][0][0][0] == "a cat fights a dog"
        assert variants[0][4][4] != variants[1][4][4]

    def test_count_one_is_a_single_variant(self):
        assert len(inner(fb.t2v_request("go", self.PID, count=1))[0]) == 1

    def test_video_aspect_is_its_own_encoding(self):
        payload = inner(fb.t2v_request("go", self.PID,
                                       aspect="VIDEO_ASPECT_RATIO_PORTRAIT"))
        assert payload[0][0][2] == fb.VIDEO_ASPECT_PORTRAIT

    def test_the_captcha_placeholder_is_present(self):
        assert fb.CAPTCHA_SLOT in fb.t2v_request("go", self.PID)

    def test_the_project_id_rides_in_the_context(self):
        assert inner(fb.t2v_request("go", self.PID))[1][5] == self.PID

    def test_read_operations_returns_every_variant(self):
        op_a = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        op_b = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
        ops = fb.read_operations([None, 50, [
            [op_a, self.PID, "scene", None],
            [op_b, self.PID, "scene", None],
        ]])
        assert [o.operation_id for o in ops] == [op_a, op_b]


class TestStreamChatRequest:
    PID = "11111111-2222-3333-4444-555555555555"

    def _inner(self, freq):
        outer = json.loads(freq)
        assert outer[0] is None
        return json.loads(outer[1])

    def test_prompt_and_refs_land_in_the_captured_slots(self):
        payload = self._inner(fb.stream_chat_request(
            "man picks flowers", self.PID, ["mid-a", "mid-b"]))
        assert payload[1][0][0][0][0] == "man picks flowers"
        assert payload[1][1] == [["mid-a"], ["mid-b"]]
        assert payload[2][0] == f"projects/{self.PID}"
        assert payload[2][2] == [fb.CAPTCHA_SLOT, 1]
        assert payload[2][5] == fb.STREAM_CHAT_VIDEO_MODE
        assert payload[0] == fb.CHAT_SESSION_SLOT

    def test_empty_refs_are_refused(self):
        with pytest.raises(ValueError):
            fb.stream_chat_request("go", self.PID, [])

    def test_envelope_is_not_the_batchexecute_generic_wrapper(self):
        freq = fb.stream_chat_request("go", self.PID, ["mid-a"])
        parsed = json.loads(freq)
        assert parsed[0] is None
        assert isinstance(parsed[1], str)


class TestVideoDefaults:
    PID = "11111111-2222-3333-4444-555555555555"

    def test_matches_the_captured_low_priority_row(self):
        payload = inner(fb.set_video_defaults_request(self.PID))
        assert payload == [
            f"projects/{self.PID}",
            [None, None, [None, [
                fb.VIDEO_R2V_MODEL,
                fb.VIDEO_ASPECT_LANDSCAPE,
                fb.VIDEO_DEFAULTS_TRAILING,
            ]]],
            [["default_generation_settings.video_defaults"]],
        ]
        assert json.loads(fb.set_video_defaults_request(self.PID))[0][0][0] == (
            fb.RPC_PROJECT_SETTINGS
        )
        assert fb.VIDEO_R2V_MODEL == "veo_3_1_lite_low_priority"

    def test_portrait_aspect_lands_in_the_captured_int_slot(self):
        payload = inner(fb.set_video_defaults_request(
            self.PID, aspect="VIDEO_ASPECT_RATIO_PORTRAIT"))
        assert payload[1][2][1][1] == fb.VIDEO_ASPECT_PORTRAIT


class TestChatSessionBind:
    PID = "11111111-2222-3333-4444-555555555555"
    SID = "da9d4724-5d1c-4b19-bf13-c24ea499e41f"
    CLIENT = "E6D766FA-23B1-4F39-A90B-682AD9F6DCD2"

    def test_list_sessions_sends_the_project_id(self):
        assert inner(fb.list_sessions_request(self.PID)) == [self.PID]
        assert json.loads(fb.list_sessions_request(self.PID))[0][0][0] == fb.RPC_LIST_SESSIONS

    def test_create_session_matches_the_captured_slots(self):
        payload = inner(fb.create_session_request(self.PID, self.CLIENT))
        assert payload == [self.PID, None, self.CLIENT]
        assert json.loads(fb.create_session_request(self.PID, self.CLIENT))[0][0][0] == (
            fb.RPC_CREATE_SESSION
        )

    def test_get_session_wraps_the_conversation_id(self):
        assert inner(fb.get_session_request(self.SID)) == [self.SID]

    def test_reader_skips_project_and_client_ids(self):
        payload = [self.PID, self.CLIENT, self.SID, "Untitled"]
        assert fb.read_session_ids(payload, exclude={self.PID, self.CLIENT}) == [self.SID]

    def test_reader_prefers_lowercase_over_client_uuid(self):
        payload = [self.CLIENT, self.SID]
        assert fb.read_session_ids(payload) == [self.SID, self.CLIENT]

    def test_reader_prefers_the_cae_record_over_an_earlier_chat_uuid(self):
        chat = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        media = "12345678-1234-1234-1234-1234567890ab"
        project = self.PID
        scene = "99999999-9999-9999-9999-999999999999"
        payload = [
            ["chat-event", chat, "hello"],
            [media, project, scene, "CAE"],
        ]
        op = fb.read_stream_chat_operation(payload)
        assert op.operation_id == media
        assert op.status == "CAE"

    def test_reader_looks_across_streamed_envelopes(self):
        """The first wrb.fr is a chat-message uuid; the clip is a later chunk."""
        chat = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        media = "12345678-1234-1234-1234-1234567890ab"
        scene = "99999999-9999-9999-9999-999999999999"
        chunks = [
            [["chat-event", chat, "hello"]],
            [[media, self.PID, scene, "CAE"]],
        ]
        op = fb.read_stream_chat_operation(chunks)
        assert op.operation_id == media

    def test_reader_unwraps_a_json_string_slot(self):
        media = "12345678-1234-1234-1234-1234567890ab"
        scene = "99999999-9999-9999-9999-999999999999"
        payload = [None, json.dumps([media, self.PID, scene, "CAE"])]
        op = fb.read_stream_chat_operation(payload)
        assert op.operation_id == media

    def test_find_stream_chat_media_ids_skips_excluded_and_prefers_cae(self):
        chat = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        pending = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        media = "12345678-1234-1234-1234-1234567890ab"
        scene = "99999999-9999-9999-9999-999999999999"
        payload = [
            [chat, self.PID, scene, "MSG"],
            [pending, self.PID, scene, "PEND"],
            [media, self.PID, scene, "CAE"],
        ]
        assert fb.find_stream_chat_media_ids(payload, exclude={chat}) == [media, pending]

    def test_find_video_media_ids_reads_cdn_paths(self):
        media = "12345678-1234-1234-1234-1234567890ab"
        url = f"https://{fb.MEDIA_HOST}/video/{media}?sig=x"
        assert fb.find_video_media_ids([url, "noise"], exclude={self.SID}) == [media]

    def test_find_video_media_ids_unwraps_dicts_and_escaped_slashes(self):
        media = "12345678-1234-1234-1234-1234567890ab"
        url = f"https://{fb.MEDIA_HOST}\\/video\\/{media}?sig=x"
        assert fb.find_video_media_ids({"clip": url}, exclude={self.SID}) == [media]

    def test_collect_uuids_keeps_the_last_occurrence_last(self):
        older = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        newer = "12345678-1234-1234-1234-1234567890ab"
        assert fb.collect_uuids(
            [older, self.SID, newer, older], exclude={self.SID}
        ) == [newer, older]

    def test_find_r2v_session_media_ids_reads_queued_tool_result(self):
        media = "12345678-1234-1234-1234-1234567890ab"
        payload = [
            ["media_id", [None, None, media]],
            ["status", [None, None, "queued"]],
            ["placeholder_frontend_id", [None, None, self.SID]],
        ]
        assert fb.find_r2v_session_media_ids(payload, exclude={self.SID}) == [media]

    def test_find_r2v_session_media_ids_ignores_other_keys(self):
        other = "12345678-1234-1234-1234-1234567890ab"
        payload = [
            ["placeholder_frontend_id", [None, None, other]],
            ["batch_id", [None, None, other]],
            ["workflow_id", [None, None, other]],
        ]
        assert fb.find_r2v_session_media_ids(payload) == []

    def test_find_r2v_session_media_ids_reads_escaped_getsession_text(self):
        media = "1a0286cd-f182-491b-8730-57527244da1f"
        raw = (
            r'["0de5575f-d5e5-43a9-b030-b8c11d94b875","generate_video_with_references",'
            rf'[[["media_id",[null,null,"{media}"]],'
            r'["status",[null,null,"queued"]]]]]'
        )
        assert fb.find_r2v_session_media_ids(raw) == [media]

    def test_find_r2v_media_ids_in_text_reads_cae_rows(self):
        media = "12345678-1234-1234-1234-1234567890ab"
        op = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        other = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        text = (
            f'["{other}","{self.PID}","{other}","CAE"],'
            f'["{media}","{self.PID}","{op}","CAE"]'
        )
        assert fb.find_r2v_media_ids_in_text(
            text, self.PID, operation_id=op, exclude={self.SID}
        ) == [media]
        assert fb.find_r2v_media_ids_in_text(
            text, self.PID, operation_id=other
        ) == [other]

    def test_a_null_operation_slot_is_not_treated_as_an_id(self):
        """StreamChat sometimes acks ``[null, 50, [[null, …]]]``; that is not a handle."""
        with pytest.raises(fb.FlowBatchError):
            fb.read_stream_chat_operation(
                [None, 50, [[None, self.PID, "scene", None]]]
            )


class TestReaders:
    OP = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    MID = "12345678-1234-1234-1234-1234567890ab"

    def test_images_are_read_out_of_the_url_path(self):
        url = f"https://{fb.MEDIA_HOST}/image/{self.MID}?sig=x"
        images = fb.read_images(["noise", [url, "more"]])
        assert images == [fb.GeneratedImage(media_id=self.MID, url=url)]

    def test_a_repeated_url_is_not_a_second_variant(self):
        url = f"https://{fb.MEDIA_HOST}/image/{self.MID}?sig=x"
        assert len(fb.read_images([url, url])) == 1

    def test_operation_reads_the_id_and_status(self):
        op = fb.read_operation([None, 50, [[self.OP, "proj", "scene", "CAE"]]])
        assert (op.operation_id, op.status, op.done) == (self.OP, "CAE", True)

    def test_the_third_uuid_is_the_scene_and_is_never_taken_as_media(self):
        """Feeding it to the media rpc answers NOT_FOUND forever."""
        record = [self.OP, "proj", "scene-uuid", "CAE"]
        op = fb.read_operation([None, 50, [record]])
        assert "scene-uuid" not in (op.operation_id, op.project_id, op.status)

    def test_a_complaint_is_carried_but_is_not_a_terminal_status(self):
        detail = [None] * 8 + [[fb.OUTCOME_COMPLAINT, [None, "Media not found."]]]
        op = fb.read_operation([None, 50, [[self.OP, "proj", "scene", None, None, detail]]])
        assert op.complained and op.error == "Media not found."
        assert not op.done

    def test_a_healthy_outcome_carries_no_complaint(self):
        detail = [None] * 8 + [[fb.OUTCOME_OK]]
        op = fb.read_operation([None, 50, [[self.OP, "p", "s", "CAE", None, detail]]])
        assert op.error is None

    def test_an_empty_operation_payload_raises(self):
        with pytest.raises(fb.FlowBatchError):
            fb.read_operation([None, 50, []])

    def test_the_media_id_is_found_in_an_unparsable_listing(self):
        """The listing outgrows any response cap; a truncated tail still holds
        the entry we came for."""
        text = f'["{self.OP}",null,null,["title",1,2,null,null,"{self.MID}"],"proj' 
        assert fb.find_media_id_in_text(text, self.OP) == self.MID

    def test_an_absent_operation_reads_as_not_there_yet(self):
        assert fb.find_media_id_in_text("nothing here", self.OP) is None

    def test_the_media_id_is_found_in_a_decoded_listing_too(self):
        payload = [[self.OP, None, None, ["t", 1, None, None, self.MID], "proj"]]
        assert fb.find_media_id(payload, self.OP) == self.MID

    def test_stream_chat_listing_row_uses_slot_zero_as_media(self):
        """r2v rows are [mediaId, projectId, sceneId, CAE]; as29s on slot 0 works."""
        project = "11111111-2222-3333-4444-555555555555"
        scene = "99999999-9999-9999-9999-999999999999"
        payload = [[self.MID, project, scene, "CAE", None, ["title"]]]
        assert fb.find_media_id(payload, self.MID) == self.MID

    def test_stream_chat_listing_text_treats_cae_key_as_media(self):
        text = (
            f'["{self.MID}","11111111-2222-3333-4444-555555555555",'
            f'"99999999-9999-9999-9999-999999999999","CAE",null,'
        )
        assert fb.find_media_id_in_text(text, self.MID) == self.MID

    def test_urls_are_split_by_kind(self):
        video = f"https://{fb.MEDIA_HOST}/video/{self.MID}?s=1"
        image = f"https://{fb.MEDIA_HOST}/image/{self.MID}?s=1"
        urls = fb.read_media_urls([image, video], self.MID)
        assert (urls.video, urls.image) == (video, image)

    def test_a_poster_only_record_has_no_video_yet(self):
        """A media id arrives before the clip is fetchable; downloading on the
        id alone saves a still picture."""
        image = f"https://{fb.MEDIA_HOST}/image/{self.MID}?s=1"
        assert fb.read_media_urls([image], self.MID).video is None

    def test_uploaded_media_id_is_the_first_slot(self):
        assert fb.read_uploaded_media_id([[self.MID, "proj", "op", "CAE"]]) == self.MID

    def test_an_upload_with_no_id_raises(self):
        with pytest.raises(fb.FlowBatchError):
            fb.read_uploaded_media_id([[]])


class TestResolvers:
    def test_rest_era_aspect_names_still_work(self):
        assert fb.resolve_aspect("IMAGE_ASPECT_RATIO_PORTRAIT") == fb.ASPECT_PORTRAIT
        assert fb.resolve_aspect("9:16") == fb.ASPECT_PORTRAIT
        assert fb.resolve_aspect("16:9") == fb.ASPECT_LANDSCAPE
        assert fb.resolve_aspect("1:1") == fb.ASPECT_SQUARE

    def test_an_unknown_aspect_name_raises_rather_than_defaulting(self):
        with pytest.raises(ValueError):
            fb.resolve_aspect("IMAGE_ASPECT_RATIO_CINEMA")

    def test_nicknames_resolve_to_wire_names(self):
        assert fb.resolve_image_model("NANO_BANANA_PRO") == "GEM_PIX_2"
        assert fb.resolve_image_model("Nano Banana Pro") == "GEM_PIX_2"
        assert fb.resolve_image_model("NANO_BANANA_2") == "NARWHAL"

    def test_a_wire_name_passes_through(self):
        assert fb.resolve_image_model("NARWHAL") == "NARWHAL"

    def test_an_unknown_image_model_coerces_to_the_default(self):
        assert fb.resolve_image_model("SOMETHING_ELSE") == fb.IMAGE_MODEL

    @pytest.mark.parametrize("legacy,expected", [
        ("veo_3_1_i2v_s_fast_ultra_relaxed", fb.VIDEO_MODEL),
        ("veo_3_1_i2v_s_fast_portrait", fb.VIDEO_MODEL),
        ("veo_3_1_i2v_s_fast_fl", fb.VIDEO_MODEL),
        ("veo_3_1_r2v_fast_landscape_ultra_relaxed", fb.VIDEO_MODEL),
        ("veo_3_1_i2v_lite", fb.VIDEO_MODEL),
        ("veo_3_1_t2v_lite_low_priority", fb.VIDEO_T2V_MODEL),
        ("veo_3_1_t2v_s_fast_ultra", fb.VIDEO_T2V_MODEL),
        (None, fb.VIDEO_MODEL),
    ])
    def test_rest_era_video_keys_fold_onto_accepted_names(self, legacy, expected):
        """Aspect and chaining are their own slots now; the suffixed names are
        rejected outright, so only the tier/quality intent survives."""
        assert fb.resolve_video_model(legacy) == expected

    def test_every_resolved_video_model_is_one_flow_accepts(self):
        for tier in ("PAYGATE_TIER_ONE", "PAYGATE_TIER_TWO"):
            for gen in ("frame_2_video", "start_end_frame_2_video", "reference_frame_2_video"):
                for aspect in ("VIDEO_ASPECT_RATIO_PORTRAIT", "VIDEO_ASPECT_RATIO_LANDSCAPE"):
                    from agent.config import VIDEO_MODELS
                    key = VIDEO_MODELS.get(tier, {}).get(gen, {}).get(aspect)
                    assert fb.resolve_video_model(key) in fb.VIDEO_MODELS


class TestVisionAnalyze:
    def test_vision_analyze_request_shape(self):
        freq = fb.vision_analyze_request(
            prompt="Phân tích sản phẩm",
            images=["data:image/jpeg;base64,AAAA"],
            system_instruction="Vai trò biên kịch",
            session_uuid="test-uuid",
        )
        parsed = json.loads(freq)
        assert parsed[0][0][0] == "agJzFb"
        inner = json.loads(parsed[0][0][1])
        assert inner[0] == "gemini-3-flash-preview"
        assert inner[9][0][1] == "user"
        assert inner[9][0][0][0] == ["Phân tích sản phẩm"]
        assert inner[9][0][0][1] == [None, ["image/jpeg", "AAAA"]]
        assert inner[11] == [[["Vai trò biên kịch"]]]
        assert inner[14][2][3] == "test-uuid"

    def test_read_vision_analysis_from_code_fence(self):
        sample_json = {"product_name": "Bemori", "pain_points": "bad sleep"}
        inner_content = f"Dưới đây là kịch bản:\n```json\n{json.dumps(sample_json)}\n```\nChúc bạn thành công!"
        inner_payload = [
            None, None, None,
            [[0, [[[inner_content]]]]]
        ]
        resp_text = envelope("agJzFb", inner_payload)
        result = fb.read_vision_analysis(resp_text)
        assert result["product_name"] == "Bemori"
        assert result["pain_points"] == "bad sleep"

    def test_read_vision_analysis_fallback_to_raw_text(self):
        inner_content = "Đây là văn bản thuần túy không có JSON"
        inner_payload = [
            None, None, None,
            [[0, [[[inner_content]]]]]
        ]
        resp_text = envelope("agJzFb", inner_payload)
        result = fb.read_vision_analysis(resp_text)
        assert result["raw_text"] == inner_content


@pytest.mark.parametrize("model", ["veo_3_1_i2v_lite", "veo_3_1_i2v_s_fast_ultra", "abra_r2v_8s"])
def test_paid_models_cannot_be_encoded(model):
    with pytest.raises(ValueError, match="LOW_PRIORITY_ONLY"):
        fb.video_request("go", "pid", "mid", model=model)
    with pytest.raises(ValueError, match="LOW_PRIORITY_ONLY"):
        fb.t2v_request("go", "pid", model=model)
    with pytest.raises(ValueError, match="LOW_PRIORITY_ONLY"):
        fb.set_video_defaults_request("pid", model=model)
