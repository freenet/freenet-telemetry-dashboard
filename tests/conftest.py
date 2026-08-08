"""Shared fixtures.

The dashboard keeps its counters in module-level globals, so every test has to
start from a clean slate and against a throwaway database.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def srv(tmp_path):
    """ws_server with all mutable global state reset and a temp DB attached."""
    import ws_server
    from telemetry_db import TelemetryDB

    ws_server.transactions.clear()
    ws_server.transaction_order.clear()
    ws_server.metrics_buckets.clear()
    ws_server._current_bucket = None
    ws_server.pending_ops.clear()
    ws_server.event_history.clear()
    ws_server.peers.clear()

    ws_server.op_stats["put"].update(requests=0, successes=0, latencies=[])
    ws_server.op_stats["get"].update(
        hop_requests=0, hop_not_found=0,
        term_direct_net={"success": 0, "not_found": 0,
                         "timeout_exhausted": 0, "other": 0},
        term_direct_loc={"success": 0, "not_found": 0,
                         "timeout_exhausted": 0, "other": 0},
        term_direct_unk={"success": 0, "not_found": 0,
                         "timeout_exhausted": 0, "other": 0},
        term_sub_op={"success": 0, "not_found": 0,
                     "timeout_exhausted": 0, "other": 0},
        latencies=[],
    )
    ws_server.op_stats["update"].update(requests=0, successes=0, broadcasts=0, latencies=[])
    ws_server.op_stats["subscribe"].update(requests=0, successes=0, not_found=0, timeouts=0)

    old_db = ws_server.db
    db = TelemetryDB(str(tmp_path / "t.db"))
    db.open()
    ws_server.db = db
    try:
        yield ws_server
    finally:
        db.close()
        ws_server.db = old_db


def make_record(event_type, ts, tx_id=None, peer="8.8.8.8", **body_fields):
    """Build an OTLP-shaped record the way a real peer reports one."""
    import orjson

    body = {"type": event_type, "this_peer": f"pk@{peer}:31337 (@0.5)"}
    if tx_id:
        body["id"] = tx_id
    body.update(body_fields)
    return {
        "timeUnixNano": str(ts),
        "attributes": [{"key": "event_type", "value": {"stringValue": event_type}}],
        "body": {"stringValue": orjson.dumps(body).decode()},
    }
