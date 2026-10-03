"""Client IP resolution: X-Forwarded-For is trusted only from the local proxy."""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ws_server  # noqa: E402


@pytest.mark.parametrize("proxy", ["127.0.0.1", "::1"])
def test_forwarded_for_honoured_from_loopback(proxy):
    assert ws_server.client_ip_for((proxy, 50000), "203.0.113.7, 10.0.0.1") == "203.0.113.7"


def test_forwarded_for_ignored_from_non_loopback_peer():
    assert ws_server.client_ip_for(("198.51.100.9", 50000), "203.0.113.7") == "198.51.100.9"


def test_no_forwarded_for_uses_remote_address():
    assert ws_server.client_ip_for(("198.51.100.9", 50000), "") == "198.51.100.9"
    assert ws_server.client_ip_for(("127.0.0.1", 50000), "") == "127.0.0.1"


def test_empty_first_forwarded_entry_falls_back_to_peer():
    assert ws_server.client_ip_for(("127.0.0.1", 50000), " , 203.0.113.7") == "127.0.0.1"


def test_missing_remote_address():
    assert ws_server.client_ip_for(None, "203.0.113.7") is None


class _Headers(dict):
    pass


class _Conn:
    def __init__(self, remote_address):
        self.remote_address = remote_address


class _Req:
    def __init__(self, xff, path="/ws"):
        self.headers = _Headers({"X-Forwarded-For": xff} if xff else {})
        self.path = path


@pytest.mark.parametrize("peer,expected", [
    ("127.0.0.1", "203.0.113.7"),
    ("198.51.100.9", "198.51.100.9"),
])
def test_process_request_stores_resolved_ip(peer, expected):
    conn = _Conn((peer, 50000))
    try:
        asyncio.run(ws_server.process_request(conn, _Req("203.0.113.7")))
        assert ws_server.client_real_ips[id(conn)] == expected
    finally:
        ws_server.client_real_ips.pop(id(conn), None)
        ws_server.client_priority.pop(id(conn), None)


def test_default_bind_host_is_loopback():
    # The module reads the env var at import; check the shipped default.
    if "FREENET_DASHBOARD_WS_HOST" in os.environ:
        pytest.skip("FREENET_DASHBOARD_WS_HOST is set in this environment")
    assert ws_server.WS_HOST == "127.0.0.1"
