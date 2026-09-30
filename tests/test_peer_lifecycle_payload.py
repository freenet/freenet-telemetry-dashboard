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


# ── Surviving a restart ──

DAY_NS = 24 * 60 * 60 * 1_000_000_000
NOW = 100 * DAY_NS


def started(srv, pid, ip=None, **fields):
    srv.peer_lifecycle[pid] = {"version": "0.2.90", "os": "linux",
                               "startup_time": NOW - DAY_NS,
                               "shutdown_time": None, **fields}
    if ip:
        srv.attrs_peer_id_to_ip[pid] = ip


def restart(srv, snapshot, now=NOW):
    """What a restart does: memory is gone, the stored snapshot is loaded."""
    stored = orjson.loads(orjson.dumps(snapshot))
    srv.peer_lifecycle.clear()
    srv.attrs_peer_id_to_ip.clear()
    srv._lifecycle_confirmed_ns.clear()
    return srv.restore_lifecycle(stored, now)


def test_a_production_peer_survives_a_restart(srv):
    started(srv, "prod", ip="8.8.8.8", os_version="Debian GNU/Linux 13 (trixie)")
    before = dict(srv.peer_lifecycle["prod"])

    assert restart(srv, srv.lifecycle_snapshot(NOW)) == 1
    assert srv.peer_lifecycle == {"prod": before}


def test_peers_never_seen_on_a_public_ip_are_not_snapshotted(srv):
    started(srv, "prod", ip="8.8.8.8")
    started(srv, "docker", ip="127.0.0.1")
    started(srv, "no-ip-yet")
    assert set(srv.lifecycle_snapshot(NOW)) == {"prod"}


def test_a_peer_that_shut_down_is_not_snapshotted(srv):
    started(srv, "gone", ip="8.8.8.8", shutdown_time=NOW - 5)
    assert srv.lifecycle_snapshot(NOW) == {}


def test_a_restored_peer_is_kept_until_it_is_seen_again(srv):
    # Right after a restart no peer has been seen yet. The first snapshot
    # must not replace the stored one with an empty one.
    started(srv, "prod", ip="8.8.8.8")
    restart(srv, srv.lifecycle_snapshot(NOW))
    assert set(srv.lifecycle_snapshot(NOW + 60)) == {"prod"}


def test_a_restored_peer_that_never_returns_ages_out(srv):
    started(srv, "prod", ip="8.8.8.8")
    restart(srv, srv.lifecycle_snapshot(NOW))
    later = NOW + srv.LIFECYCLE_SNAPSHOT_MAX_AGE_NS + 1
    assert srv.lifecycle_snapshot(later) == {}
    # ...while one that is seen again is confirmed afresh and stays.
    started(srv, "back", ip="8.8.4.4")
    assert set(srv.lifecycle_snapshot(later)) == {"back"}
    assert set(srv.lifecycle_snapshot(later + DAY_NS - 1)) == {"back"}


def test_a_snapshot_too_old_to_trust_is_not_restored(srv):
    started(srv, "prod", ip="8.8.8.8")
    snapshot = srv.lifecycle_snapshot(NOW)
    late = NOW + srv.LIFECYCLE_SNAPSHOT_MAX_AGE_NS + 1
    assert restart(srv, snapshot, now=late) == 0
    assert srv.peer_lifecycle == {}


def test_a_startup_replayed_from_the_log_beats_the_snapshot(srv):
    started(srv, "prod", ip="8.8.8.8", version="0.2.89")
    snapshot = orjson.loads(orjson.dumps(srv.lifecycle_snapshot(NOW)))
    srv.peer_lifecycle.clear()
    started(srv, "prod", version="0.2.90")  # the peer restarted meanwhile

    assert srv.restore_lifecycle(snapshot, NOW) == 0
    assert srv.peer_lifecycle["prod"]["version"] == "0.2.90"


def test_a_peer_dropped_as_stale_leaves_the_snapshot(srv):
    started(srv, "prod", ip="8.8.8.8")
    assert set(srv.lifecycle_snapshot(NOW)) == {"prod"}
    srv.peer_lifecycle.pop("prod")
    srv.attrs_peer_id_to_ip.pop("prod")
    assert srv.lifecycle_snapshot(NOW + 1) == {}
    assert srv._lifecycle_confirmed_ns == {}
