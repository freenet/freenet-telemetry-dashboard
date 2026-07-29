"""The server and the browser client must classify transactions identically.

ws_server.py and js/events.js each carry their own copy of the start/terminal
tables. They are the two halves of the same fix, and a change to one that
misses the other silently reintroduces the drift issue #15 is about.
"""
import ast
import os
import re

import pytest

import ws_server

JS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                  "js", "events.js")


def js_source():
    with open(JS, encoding="utf-8") as f:
        return f.read()


def parse_js_object(name):
    """Pull a `const NAME = { ... };` literal out of js/events.js."""
    src = js_source()
    m = re.search(r"const\s+%s\s*=\s*\{(.*?)\n\};" % re.escape(name), src, re.S)
    assert m, f"{name} not found in js/events.js"
    body = m.group(1)
    out = {}
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("//"):
            continue
        km = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+?),?$", line)
        if not km:
            continue
        raw = km.group(2).rstrip(",")
        if raw.startswith("["):
            out[km.group(1)] = tuple(ast.literal_eval(raw.replace("'", '"')))
        else:
            out[km.group(1)] = ast.literal_eval(raw.replace("'", '"'))
    return out


def test_start_events_match():
    assert parse_js_object("TX_START_EVENTS") == ws_server.TX_START_EVENTS


def test_terminal_events_match():
    js = parse_js_object("TX_TERMINAL_EVENTS")
    py = {k: tuple(v) for k, v in ws_server.TX_TERMINAL_EVENTS.items()}
    assert js == py


@pytest.mark.parametrize("event_type", [
    "get_request", "get_success", "get_not_found", "get_terminal",
    "subscribe_request", "subscribe_success", "subscribe_not_found",
    "subscribe_timeout", "subscribed",
    "put_request", "put_success", "update_request", "update_success",
    "update_broadcast_received", "connect_connected", "disconnect",
])
def test_classification_agrees_for_every_event_the_dashboard_sees(event_type):
    """Cross-check the Python classifier against the JS tables."""
    op, role = ws_server.classify_tx_event(event_type)
    js_start = parse_js_object("TX_START_EVENTS")
    js_terminal = parse_js_object("TX_TERMINAL_EVENTS")
    if event_type in js_start:
        assert (op, role) == (js_start[event_type], "start")
    elif event_type in js_terminal:
        assert (op, role) == (js_terminal[event_type][0], "terminal")
    elif event_type == "get_terminal":
        assert (op, role) == ("get", "terminal")
    else:
        assert role is None


class TestTheClientNoLongerCarriesTheOldSemantics:
    def test_no_complete_fallback_in_the_client(self):
        src = js_source()
        assert "status || 'complete'" not in src
        assert "tx.status" not in src, "client still writes the misleading field"

    def test_client_settles_on_the_real_subscribe_terminals(self):
        js = parse_js_object("TX_TERMINAL_EVENTS")
        assert js["subscribe_success"] == ("subscribe", "success")
        assert js["subscribe_not_found"] == ("subscribe", "not_found")
        assert js["subscribe_timeout"] == ("subscribe", "timeout")

    def test_client_does_not_settle_on_per_hop_get_events(self):
        js = parse_js_object("TX_TERMINAL_EVENTS")
        assert "get_success" not in js
        assert "get_not_found" not in js
