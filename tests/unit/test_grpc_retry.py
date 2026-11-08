"""Which gRPC failures may be sent again without starting a second render."""
import grpc

from agent.services.flow_grpc import (
    M_CHECK, M_GET_MEDIA, M_I2V, M_IMAGE, M_PROJECT,
    _rpc_retryable, _transport_detail,
)


def test_poll_retries_when_proxy_drops_after_send():
    assert _rpc_retryable(M_CHECK, grpc.StatusCode.UNAVAILABLE, "No status received")
    assert _rpc_retryable(M_GET_MEDIA, grpc.StatusCode.UNAVAILABLE, "Stream removed")
    assert _rpc_retryable(M_PROJECT, grpc.StatusCode.UNAVAILABLE, "Connection reset by peer")


def test_generate_is_not_repeated_after_a_lost_trailer():
    assert not _rpc_retryable(
        M_I2V, grpc.StatusCode.UNAVAILABLE, "No status received")
    assert not _rpc_retryable(
        M_IMAGE, grpc.StatusCode.UNAVAILABLE, "Stream removed")


def test_proxy_drop_is_a_transport_blip():
    assert _transport_detail("No status received")
    assert _transport_detail("Stream removed")
    assert not _transport_detail("PUBLIC_ERROR_UNSAFE_GENERATION")


def test_generate_is_repeated_only_if_connect_never_happened():
    assert _rpc_retryable(
        M_I2V, grpc.StatusCode.UNAVAILABLE, "failed to connect to all addresses")
    assert not _rpc_retryable(
        M_I2V, grpc.StatusCode.INVALID_ARGUMENT, "bad prompt")
