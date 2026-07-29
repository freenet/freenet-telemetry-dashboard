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


def js_classify(event_type):
    """Evaluate js/events.js's classifyTxEvent fallback chain in Python.

    Parsed out of the source rather than reimplemented, so the two cannot drift
    without this failing.
    """
    js_start = parse_js_object("TX_START_EVENTS")
    js_terminal = parse_js_object("TX_TERMINAL_EVENTS")
    if event_type in js_start:
        return js_start[event_type], "start"
    if event_type in js_terminal:
        return js_terminal[event_type][0], "terminal"
    src = js_source()
    body = re.search(r"export function classifyTxEvent\(eventType\) \{(.*?)\n\}",
                     src, re.S).group(1)
    for line in body.splitlines():
        m = re.search(
            r"if \((.*?)\) return \{ op: '([a-z]+)', role: (null|'[a-z]+') \};", line)
        if not m:
            continue
        cond, op, role = m.group(1), m.group(2), m.group(3)
        role = None if role == "null" else role.strip("'")
        for pat in re.findall(r"eventType\.startsWith\('([^']+)'\)", cond):
            if event_type.startswith(pat):
                return op, role
        for pat in re.findall(r"eventType\.includes\('([^']+)'\)", cond):
            if pat in event_type:
                return op, role
        for lit in re.findall(r"eventType === '([^']+)'", cond):
            if event_type == lit:
                return op, role
    return event_type.split("_")[0] or "other", None


@pytest.mark.parametrize("event_type", [
    "get_request", "get_success", "get_not_found", "get_terminal",
    "subscribe_request", "subscribe_success", "subscribe_not_found",
    "subscribe_timeout", "subscribed", "unsubscribed",
    "put_request", "put_success", "put_failure",
    "update_request", "update_success", "update_failure",
    "update_broadcast_received", "update_broadcast_applied", "broadcast_emitted",
    "connect_connected", "connect_rejected", "connect_request_sent", "disconnect",
    "seeding_started", "peer_startup", "transfer_completed", "hosting_started",
])
def test_classification_agrees_for_every_event_the_dashboard_sees(event_type):
    """Compare BOTH op and role.

    Asserting only `role is None` for anything outside the two tables left the
    test structurally blind to op-level drift, and a fix that touched only the
    Python side went unnoticed because of it: `unsubscribed` classified as
    'unsubscribed' on the server and 'subscribe' in the client, which decides
    whether the event creates a transaction at all.
    """
    assert ws_server.classify_tx_event(event_type) == js_classify(event_type)


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
