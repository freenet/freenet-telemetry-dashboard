"""The server and the browser client must classify transactions identically.

ws_server.py and js/events.js each carry their own copy of the start/terminal
tables. They are the two halves of the same fix, and a change to one that
misses the other silently reintroduces the drift issue #15 is about.
"""
import ast
import json
import os
import re
import shutil
import subprocess

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


# ── The resolution rule, not just the classification tables ────────────────
#
# The agreement test above compares how each side CLASSIFIES an event. It
# cannot see how each side RESOLVES two terminals landing on the same
# transaction, which is a separate rule that must also match — the server
# gained precedence while the client was still last-write-wins, and nothing
# here could tell.
#
# These drive the REAL js/events.js through node rather than reimplementing or
# parsing it, so the comparison is against shipped behaviour.

NODE = shutil.which("node")

OUTCOMES = ["success", "not_found", "timeout_exhausted", "timeout",
            "rejected", "failure", "disconnected", "something_unknown", None]
EVENTS = ["get_terminal", "subscribe_success", "subscribe_not_found",
          "subscribe_timeout", "put_success", "connect_rejected", "disconnect"]


def js_outcome_wins_matrix(cases):
    """Evaluate outcomeWins() in js/events.js over `cases` via node."""
    script = os.path.join(os.path.dirname(JS), "..",
                          "_agreement_probe.mjs")
    script = os.path.abspath(script)
    payload = json.dumps(cases)
    with open(script, "w", encoding="utf-8") as f:
        f.write(
            f"import {{ outcomeWins }} from {json.dumps(JS)};\n"
            f"const cases = {payload};\n"
            "console.log(JSON.stringify(cases.map("
            "c => outcomeWins(c[0], c[1], c[2], c[3]))));\n"
        )
    try:
        out = subprocess.run([NODE, script], capture_output=True, text=True,
                             check=True, timeout=60).stdout
    finally:
        os.unlink(script)
    return json.loads(out)


@pytest.mark.skipif(NODE is None, reason="node not available")
def test_resolution_rule_agrees_across_every_combination():
    cases = [[no, ne, oo, oe]
             for no in OUTCOMES for ne in EVENTS
             for oo in OUTCOMES for oe in EVENTS]
    js = js_outcome_wins_matrix(cases)
    mismatches = [
        (c, j) for c, j in zip(cases, js)
        if ws_server.outcome_wins(c[0], c[1], c[2], c[3]) is not j
    ]
    assert not mismatches, (
        f"{len(mismatches)} of {len(cases)} disagree, e.g. {mismatches[:3]}"
    )


@pytest.mark.skipif(NODE is None, reason="node not available")
def test_the_matrix_actually_exercises_both_answers():
    """Guards the test above from passing because everything returned True."""
    cases = [[no, ne, oo, oe]
             for no in OUTCOMES for ne in EVENTS
             for oo in OUTCOMES for oe in EVENTS]
    js = js_outcome_wins_matrix(cases)
    assert {True, False} <= set(js), "the matrix is one-sided and proves nothing"


def test_precedence_tables_match():
    js = parse_js_object("TX_OUTCOME_PRECEDENCE")
    assert js == ws_server.TX_OUTCOME_PRECEDENCE


def js_track(sequences):
    """Drive the REAL trackTransactionFromEvent in js/events.js via node.

    Testing outcomeWins() alone is not enough: the client can agree on the rule
    and still not apply it at the call site, which is exactly how it stayed
    last-write-wins while the server had precedence.
    """
    script = os.path.abspath(os.path.join(os.path.dirname(JS), "..",
                                          "_track_probe.mjs"))
    state_js = json.dumps(os.path.join(os.path.dirname(JS), "state.js"))
    with open(script, "w", encoding="utf-8") as f:
        f.write(
            f"import {{ state }} from {state_js};\n"
            f"import {{ trackTransactionFromEvent }} from {json.dumps(JS)};\n"
            f"const seqs = {json.dumps(sequences)};\n"
            "const out = seqs.map(seq => {\n"
            "  state.allTransactions = []; state.transactionMap = new Map();\n"
            "  seq.forEach(([t, ts, outcome], i) => trackTransactionFromEvent(\n"
            "    { tx_id: 'tx', event_type: t, timestamp: ts, peer_id: 'p',\n"
            "      ...(outcome ? { outcome } : {}) }));\n"
            "  const tx = state.allTransactions[0];\n"
            "  return tx ? { tx_shape: tx.tx_shape, outcome: tx.outcome } : null;\n"
            "});\n"
            "console.log(JSON.stringify(out));\n"
        )
    try:
        out = subprocess.run([NODE, script], capture_output=True, text=True,
                             check=True, timeout=60).stdout
    finally:
        os.unlink(script)
    return json.loads(out)


def py_track(srv_mod, seq):
    srv_mod.transactions.clear()
    srv_mod.transaction_order.clear()
    for event_type, ts, outcome in seq:
        srv_mod.track_transaction("tx", event_type, ts, "p",
                                  terminal_outcome=outcome)
    tx = srv_mod.transactions.get("tx")
    return {"tx_shape": tx["tx_shape"], "outcome": tx["outcome"]} if tx else None


TRACK_SEQUENCES = [
    # Contradictory hop terminals, both arrival orders.
    [["subscribe_request", 100, None], ["subscribe_not_found", 200, None],
     ["subscribe_success", 300, None]],
    [["subscribe_request", 100, None], ["subscribe_success", 200, None],
     ["subscribe_not_found", 300, None]],
    # A client-facing terminal that ranks LOWER than the hop terminal already
    # recorded: only the authority rule overturns it.
    [["subscribe_request", 100, None], ["subscribe_success", 200, None],
     ["get_terminal", 300, "not_found"]],
    # ...and a hop terminal that ranks higher arriving after a client one.
    [["get_request", 100, None], ["get_terminal", 200, "not_found"],
     ["subscribe_success", 300, None]],
    # A late per-hop start must not reopen a settled transaction.
    [["get_request", 100, None], ["get_terminal", 200, "success"],
     ["get_request", 300, None]],
    # Repeated identical hop terminals stay stable.
    [["subscribe_request", 100, None], ["subscribe_success", 200, None],
     ["subscribe_success", 300, None], ["subscribe_success", 400, None]],
    # Propagation only: partial, no fabricated outcome.
    [["update_broadcast_received", 100, None]],
]


@pytest.mark.skipif(NODE is None, reason="node not available")
def test_the_client_applies_the_resolution_rule_not_just_defines_it():
    js = js_track(TRACK_SEQUENCES)
    py = [py_track(ws_server, seq) for seq in TRACK_SEQUENCES]
    mismatches = [(s, j, p) for s, j, p in zip(TRACK_SEQUENCES, js, py) if j != p]
    assert not mismatches, (
        "client and server disagree on the settled state of the same event "
        f"stream: {mismatches}"
    )


@pytest.mark.skipif(NODE is None, reason="node not available")
def test_the_sequences_actually_discriminate():
    """Guards the test above: the fixtures must produce more than one answer,
    or agreement would be trivial."""
    js = js_track(TRACK_SEQUENCES)
    assert len({(r or {}).get("outcome") for r in js}) > 1
    assert len({(r or {}).get("tx_shape") for r in js}) > 1
