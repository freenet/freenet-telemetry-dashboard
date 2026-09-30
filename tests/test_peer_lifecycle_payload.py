"""What the server stores from a startup report and sends on connect.

`peer_startup` fields come verbatim from a peer and reach every dashboard
client, so their size is bounded at ingest. The lifecycle list sent on connect
is every ring peer plus a bounded top-up of off-ring peers.
"""
import orjson

import ws_server


def startup_record(peer_id, **body_fields):
    body = {"type": "peer_startup", "version": "0.2.90"}
    body.update(body_fields)
    return {
        "timeUnixNano": "1000",
        "attributes": [
            {"key": "event_type", "value": {"stringValue": "peer_startup"}},
            {"key": "peer_id", "value": {"stringValue": peer_id}},
        ],
        "body": {"stringValue": orjson.dumps(body).decode()},
    }


def stored(srv, **body_fields):
    srv.process_record(startup_record("peerA", **body_fields), store_history=False)
    (record,) = srv.peer_lifecycle.values()
    return record


def test_ordinary_startup_fields_are_stored_unchanged(srv):
    record = stored(srv, os="linux", arch="x86_64",
                    os_version="Debian GNU/Linux 13 (trixie)")
    assert record["os"] == "linux"
    assert record["arch"] == "x86_64"
    assert record["os_version"] == "Debian GNU/Linux 13 (trixie)"


def test_oversized_startup_fields_are_truncated(srv):
    huge = " " * 100_000 + "x"
    record = stored(srv, os=huge, arch=huge, os_version=huge)
    for field in ("os", "arch", "os_version"):
        assert len(record[field]) == ws_server.MAX_PEER_FIELD_LEN, field


def test_missing_startup_fields_keep_their_defaults(srv):
    record = stored(srv)
    assert record["os"] == "unknown"
    assert record["arch"] == "unknown"
    assert record["os_version"] is None


def test_non_string_startup_fields_become_bounded_strings(srv, monkeypatch):
    # record_version keeps what it is given in module globals.
    monkeypatch.setattr(srv, "_seen_versions", set())
    monkeypatch.setattr(srv, "version_markers", [])
    record = stored(srv, version=["0.2.90"], os=["linux"] * 1000, arch=64,
                    os_version={"a": 1})
    assert record["version"] == "['0.2.90']"
    assert record["arch"] == "64"
    assert record["os_version"] == "{'a': 1}"
    assert record["os"] == str(["linux"] * 1000)[:ws_server.MAX_PEER_FIELD_LEN]
    # The version tracker gets the same bounded string.
    assert srv.version_markers == [(1000, "['0.2.90']")]


def test_oversized_version_is_truncated(srv, monkeypatch):
    monkeypatch.setattr(srv, "_seen_versions", set())
    monkeypatch.setattr(srv, "version_markers", [])
    record = stored(srv, version="9" * 10_000)
    assert record["version"] == "9" * ws_server.MAX_PEER_FIELD_LEN


def lifecycle(n, prefix):
    return {f"{prefix}{i}": {"os": "linux"} for i in range(n)}


def test_off_ring_peers_fill_only_the_room_left():
    ring = lifecycle(30, "ring")
    off = lifecycle(40, "off")
    topo, other = ws_server.lifecycle_peers_for_clients({**ring, **off}, set(ring))
    assert len(topo) == 30
    assert len(other) == ws_server.LIFECYCLE_FILL_TARGET - 30


def test_no_off_ring_peers_when_the_ring_is_exactly_the_target():
    ring = lifecycle(ws_server.LIFECYCLE_FILL_TARGET, "ring")
    off = lifecycle(40, "off")
    topo, other = ws_server.lifecycle_peers_for_clients({**ring, **off}, set(ring))
    assert len(topo) == ws_server.LIFECYCLE_FILL_TARGET
    assert other == []


def test_no_off_ring_peers_once_the_ring_passes_the_target():
    # The regression: 50 - 250 is -200, and [:-200] is "all but the last 200",
    # so 300 of these 500 off-ring peers used to be sent.
    ring = lifecycle(250, "ring")
    off = lifecycle(500, "off")
    topo, other = ws_server.lifecycle_peers_for_clients({**ring, **off}, set(ring))
    assert len(topo) == 250
    assert other == []


def test_ring_peers_without_a_startup_report_are_left_out():
    ring = lifecycle(3, "ring")
    topo, other = ws_server.lifecycle_peers_for_clients(
        ring, {"ring0", "ring1", "never-reported"})
    assert sorted(p["peer_id"] for p in topo) == ["ring0", "ring1"]
    assert [p["peer_id"] for p in other] == ["ring2"]
