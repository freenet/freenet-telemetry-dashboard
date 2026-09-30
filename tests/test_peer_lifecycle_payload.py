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
HOUR_NS = DAY_NS // 24
NOW = 100 * DAY_NS


def started(srv, pid, ip=None, on_ring=True, **fields):
    srv.peer_lifecycle[pid] = {"version": "0.2.90", "os": "linux",
                               "startup_time": NOW - DAY_NS,
                               "shutdown_time": None, **fields}
    if ip:
        seen(srv, pid, ip, on_ring=on_ring)


def seen(srv, pid, ip, on_ring=True, last_seen=0):
    """The peer shows up in the event stream at this IP."""
    srv.attrs_peer_id_to_ip[pid] = ip
    if on_ring:
        srv.peers[ip] = {"id": f"anon-{pid}", "peer_id": pid,
                         "last_seen": last_seen, "connections": set()}


def restart(srv, snapshot, now=NOW):
    """What a restart does: memory is gone, the stored snapshot is loaded."""
    stored = orjson.loads(orjson.dumps(snapshot))
    srv.peer_lifecycle.clear()
    srv.attrs_peer_id_to_ip.clear()
    srv.peers.clear()
    srv._lifecycle_confirmed_ns.clear()
    srv._lifecycle_awaiting_return.clear()
    return srv.restore_lifecycle(stored, now)


def shutdown_record(peer_id):
    return {
        "timeUnixNano": "2000",
        "attributes": [
            {"key": "event_type", "value": {"stringValue": "peer_shutdown"}},
            {"key": "peer_id", "value": {"stringValue": peer_id}},
        ],
        "body": {"stringValue": orjson.dumps(
            {"type": "peer_shutdown", "graceful": True}).decode()},
    }


def test_a_production_peer_survives_a_restart(srv):
    started(srv, "prod", ip="8.8.8.8", os_version="Debian GNU/Linux 13 (trixie)")
    before = dict(srv.peer_lifecycle["prod"])

    assert restart(srv, srv.lifecycle_snapshot(NOW)) == 1
    assert srv.peer_lifecycle == {"prod": before}


def test_only_peers_on_the_ring_at_a_public_ip_are_snapshotted(srv):
    started(srv, "prod", ip="8.8.8.8")
    started(srv, "docker", ip="127.0.0.1")
    started(srv, "mapped-but-not-on-ring", ip="8.8.4.4", on_ring=False)
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
    # Nothing else would ever remove it, so it leaves memory too.
    assert srv.peer_lifecycle == {}
    assert srv._lifecycle_awaiting_return == set()


def test_restarts_do_not_extend_the_life_of_a_peer_that_never_returns(srv):
    # The age is measured from when the peer was last confirmed, not from the
    # last restart. Otherwise restarts less than a day apart keep it forever.
    started(srv, "prod", ip="8.8.8.8")
    restart(srv, srv.lifecycle_snapshot(NOW))
    at_23h = NOW + 23 * HOUR_NS
    assert restart(srv, srv.lifecycle_snapshot(at_23h), now=at_23h) == 1
    assert srv.lifecycle_snapshot(NOW + 25 * HOUR_NS) == {}


def test_a_restored_peer_seen_again_is_confirmed_afresh(srv):
    started(srv, "prod", ip="8.8.8.8")
    restart(srv, srv.lifecycle_snapshot(NOW))
    at_23h = NOW + 23 * HOUR_NS
    seen(srv, "prod", "8.8.8.8")
    assert srv.lifecycle_snapshot(at_23h)["prod"]["confirmed_ns"] == at_23h
    assert srv._lifecycle_awaiting_return == set()


def test_a_snapshot_too_old_to_trust_is_not_restored(srv):
    started(srv, "prod", ip="8.8.8.8")
    snapshot = srv.lifecycle_snapshot(NOW)
    late = NOW + srv.LIFECYCLE_SNAPSHOT_MAX_AGE_NS + 1
    assert restart(srv, snapshot, now=late) == 0
    assert srv.peer_lifecycle == {}


def test_a_shutdown_missed_while_down_is_applied_to_the_restored_peer(srv):
    # Restore runs before the log replay. Were it the other way round, the
    # replayed shutdown would find no record, be ignored, and the peer would
    # come back as still running.
    started(srv, "prod", ip="8.8.8.8")
    restart(srv, srv.lifecycle_snapshot(NOW))

    srv.process_record(shutdown_record("prod"), store_history=False)
    seen(srv, "prod", "8.8.8.8")

    assert srv.peer_lifecycle["prod"]["shutdown_time"] == 2000
    assert srv.lifecycle_snapshot(NOW + 60) == {}


def test_a_startup_replayed_after_restore_replaces_the_restored_record(srv):
    started(srv, "peerA", ip="8.8.8.8", version="0.2.89")
    restart(srv, srv.lifecycle_snapshot(NOW))

    srv.process_record(startup_record("peerA"), store_history=False)

    assert srv.peer_lifecycle["peerA"]["version"] == "0.2.90"
    assert srv.peer_lifecycle["peerA"]["startup_time"] == 1000


def test_a_record_already_present_is_not_overwritten_by_restore(srv):
    started(srv, "prod", ip="8.8.8.8", version="0.2.89")
    snapshot = orjson.loads(orjson.dumps(srv.lifecycle_snapshot(NOW)))
    srv.peer_lifecycle["prod"]["version"] = "0.2.90"

    assert srv.restore_lifecycle(snapshot, NOW) == 0
    assert srv.peer_lifecycle["prod"]["version"] == "0.2.90"


def test_the_stale_sweep_spares_a_restored_peer_until_it_is_seen(srv):
    # After a long outage the replayed log carries old timestamps, so the
    # first sweep finds every replayed IP stale.
    started(srv, "prod", ip="8.8.8.8")
    restart(srv, srv.lifecycle_snapshot(NOW))
    seen(srv, "prod", "8.8.8.8", last_seen=0)

    srv.cleanup_stale_peers()
    assert "8.8.8.8" not in srv.peers
    assert "prod" in srv.peer_lifecycle

    # Once it has been seen and confirmed, the sweep treats it as any other.
    seen(srv, "prod", "8.8.8.8", last_seen=0)
    srv.lifecycle_snapshot(NOW + 60)
    srv.cleanup_stale_peers()
    assert "prod" not in srv.peer_lifecycle


def test_a_peer_dropped_as_stale_leaves_the_snapshot(srv):
    started(srv, "prod", ip="8.8.8.8")
    assert set(srv.lifecycle_snapshot(NOW)) == {"prod"}
    srv.peer_lifecycle.pop("prod")
    srv.attrs_peer_id_to_ip.pop("prod")
    assert srv.lifecycle_snapshot(NOW + 1) == {}
    assert srv._lifecycle_confirmed_ns == {}


def test_a_malformed_stored_snapshot_restores_what_it_can(srv):
    good = {"version": "0.2.90", "shutdown_time": None, "confirmed_ns": NOW}
    stored = {
        "good": good,
        "not-a-record": "oops",
        "no-confirmation": {"version": "0.2.90"},
        "bad-confirmation": {"version": "0.2.90", "confirmed_ns": "yesterday"},
    }
    assert srv.restore_lifecycle(stored, NOW) == 1
    assert set(srv.peer_lifecycle) == {"good"}
    assert srv.restore_lifecycle(["not", "a", "dict"], NOW) == 0
