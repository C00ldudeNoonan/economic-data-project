"""Regression coverage for the default no-network test contract."""

import os
import socket

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_NETWORK_TESTS") == "1",
    reason="Network isolation is intentionally disabled for opt-in network tests",
)


def test_default_suite_blocks_socket_connections() -> None:
    with pytest.raises(RuntimeError, match="Network access is disabled"):
        socket.create_connection(("127.0.0.1", 9))
