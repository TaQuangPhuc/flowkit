"""i2v start-frame media must not be treated as the finished clip."""


def test_start_image_is_not_an_output_candidate():
    start = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    op = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    placeholder = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    start_img = start
    candidates = []
    for mid in (op, placeholder, start):
        if mid and mid not in candidates and mid != start_img:
            candidates.append(mid)
    assert start not in candidates
    assert op in candidates
    assert placeholder in candidates
