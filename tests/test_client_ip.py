"""Client IP resolution and bind host.

X-Forwarded-For is trusted only from a loopback peer (the local reverse proxy),
and the server binds to loopback unless explicitly configured otherwise.
"""
import asyncio
import os
import sys

import pytest
from websockets.datastructures import Headers

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ws_server  # noqa: E402

CLIENT = "203.0.113.7"
REMOTE = "198.51.100.9"


# --- client_ip_for -----------------------------------------------------------

@pytest.mark.parametrize("proxy", ["127.0.0.1", "::1"])
def test_forwarded_for_honoured_from_loopback(proxy):
    assert ws_server.client_ip_for((proxy, 50000), [CLIENT]) == CLIENT


def test_forwarded_for_ignored_from_non_loopback_peer():
    assert ws_server.client_ip_for((REMOTE, 50000), [CLIENT]) == REMOTE


def test_ipv4_mapped_loopback_is_not_trusted():
    # Deliberately fail closed: only the exact loopback addresses are trusted.
    peer = "::ffff:127.0.0.1"
    assert ws_server.client_ip_for((peer, 50000), [CLIENT]) == peer


def test_rightmost_entry_is_used():
    assert ws_server.client_ip_for(("127.0.0.1", 50000), ["192.0.2.1, " + CLIENT]) == CLIENT


def test_multiple_header_lines_use_last_entry():
    assert ws_server.client_ip_for(("127.0.0.1", 50000), ["192.0.2.1", CLIENT]) == CLIENT


def test_no_forwarded_for_uses_remote_address():
    assert ws_server.client_ip_for((REMOTE, 50000), []) == REMOTE
    assert ws_server.client_ip_for(("127.0.0.1", 50000), []) == "127.0.0.1"


@pytest.mark.parametrize("value", [
    "", " , ", "not-an-ip", "203.0.113.7:443", "999.1.1.1", "203.0.113.7 junk",
    CLIENT + ", garbage",
])
def test_malformed_forwarded_for_falls_back_to_peer(value):
    assert ws_server.client_ip_for(("127.0.0.1", 50000), [value]) == "127.0.0.1"


def test_missing_remote_address():
    assert ws_server.client_ip_for(None, [CLIENT]) is None


# --- process_request (real case-insensitive Headers) -------------------------

class _Conn:
    def __init__(self, remote_address):
        self.remote_address = remote_address


class _Req:
    def __init__(self, headers, path="/ws"):
        self.headers = headers
        self.path = path


def _run_process_request(peer, headers):
    conn = _Conn((peer, 50000))
    try:
        asyncio.run(ws_server.process_request(conn, _Req(headers)))
        return ws_server.client_real_ips.get(id(conn))
    finally:
        ws_server.client_real_ips.pop(id(conn), None)
        ws_server.client_priority.pop(id(conn), None)


@pytest.mark.parametrize("peer,expected", [
    ("127.0.0.1", CLIENT),
    (REMOTE, REMOTE),
])
def test_process_request_stores_resolved_ip(peer, expected):
    h = Headers()
    h["x-forwarded-for"] = CLIENT  # lower case: header lookup is case-insensitive
    assert _run_process_request(peer, h) == expected


def test_process_request_handles_multiple_header_lines():
    h = Headers()
    h["X-Forwarded-For"] = "192.0.2.1"
    h["X-Forwarded-For"] = CLIENT
    assert _run_process_request("127.0.0.1", h) == CLIENT


# --- handle_client capacity rejection ---------------------------------------

class _ClosingWs:
    remote_address = ("127.0.0.1", 50000)

    def __init__(self):
        self.closed = None

    async def close(self, code, reason):
        self.closed = code


@pytest.mark.parametrize("priority", [True, False])
def test_capacity_reject_does_not_leak_client_ip(monkeypatch, priority):
    ws = _ClosingWs()
    # priority=True hits the absolute-capacity branch, False the general one.
    monkeypatch.setattr(ws_server, "MAX_CLIENTS", 0 if priority else 10)
    monkeypatch.setattr(ws_server, "PRIORITY_RESERVED", 10)
    ws_server.client_real_ips[id(ws)] = CLIENT
    ws_server.client_priority[id(ws)] = priority
    try:
        asyncio.run(ws_server.handle_client(ws))
        assert ws.closed == 1013
        assert id(ws) not in ws_server.client_real_ips
    finally:
        ws_server.client_real_ips.pop(id(ws), None)
        ws_server.client_priority.pop(id(ws), None)


# --- bind host ---------------------------------------------------------------

def _serve_host(monkeypatch):
    calls = []
    monkeypatch.setattr(ws_server.websockets, "serve",
                        lambda *a, **kw: calls.append((a, kw)))
    ws_server.serve_websocket()
    (args, kwargs), = calls
    return args[1]


def test_serve_binds_loopback_by_default(monkeypatch):
    monkeypatch.delenv("FREENET_DASHBOARD_WS_HOST", raising=False)
    assert _serve_host(monkeypatch) == "127.0.0.1"


@pytest.mark.parametrize("value", ["", "   "])
def test_serve_empty_host_env_falls_back_to_loopback(monkeypatch, value):
    monkeypatch.setenv("FREENET_DASHBOARD_WS_HOST", value)
    assert _serve_host(monkeypatch) == "127.0.0.1"


def test_serve_host_env_override(monkeypatch):
    monkeypatch.setenv("FREENET_DASHBOARD_WS_HOST", "0.0.0.0")
    assert _serve_host(monkeypatch) == "0.0.0.0"
