from agent.services.cdn import bump_google_image_url


def test_replaces_s512_thumb():
    u = "https://lh3.googleusercontent.com/gg/abc=s512-c-k-no"
    assert "=s0" in bump_google_image_url(u)
    assert "s512" not in bump_google_image_url(u)


def test_replaces_wh_thumb():
    u = "https://lh3.googleusercontent.com/gg/abc=w512-h286-p-k-no"
    out = bump_google_image_url(u)
    assert "=s0" in out
    assert "w512" not in out


def test_bare_lh3_gets_s0():
    u = "https://lh3.googleusercontent.com/asb/ANqvLOZ6bdVWyNUBg9UiL2s"
    assert bump_google_image_url(u) == u + "=s0"


def test_flow_content_signed_url_untouched():
    # signed flow-content URLs are already full-size; any mutation is a 403
    u = "https://flow-content.google/image/d35bedca-d035-4105-98da-705923e50582?Expires=1&Signature=x"
    assert bump_google_image_url(u) == u
