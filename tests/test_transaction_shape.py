"""Regression tests for issue #15 — transaction shape vs measured outcome.

`transactions.status` used to conflate "we saw a terminal event" with "we saw
any event at all": its 'complete' value was a fallback for any transaction
whose first observed event was not a start. Propagation events dominate that
case, so success rates read off the column were confidently wrong.
"""
import time

import pytest


def track(srv, event_type, tx_id, ts=None, **kw):
    srv.track_transaction(tx_id, event_type, ts or time.time_ns(), "peer-a", **kw)
    return srv.transactions[tx_id]


class TestPropagationEventsDoNotMintCompletions:
    """Defect 1: 'complete' was a fallback, not a result."""

    @pytest.mark.parametrize("event_type", [
        "update_broadcast_received",
        "update_broadcast_applied",
        "update_broadcast_emitted",
    ])
    def test_propagation_event_alone_is_partial_with_no_outcome(self, srv, event_type):
        # Before the fix this produced status='complete' — ~29M synthetic
        # completions over a 48h window.
        tx = track(srv, event_type, "tx-prop")
        assert tx["tx_shape"] == "partial"
        assert tx["outcome"] is None

    def test_a_start_alone_is_open_not_settled(self, srv):
        tx = track(srv, "get_request", "tx-open")
        assert tx["tx_shape"] == "open"
        assert tx["outcome"] is None

    def test_late_start_upgrades_partial_to_open(self, srv):
        track(srv, "update_broadcast_received", "tx-late", ts=100)
        tx = track(srv, "update_request", "tx-late", ts=200)
        assert tx["tx_shape"] == "open"


class TestSubscribeReachesATerminal:
    """Defect 2: the terminal handler keyed on an event core never emits."""

    def test_subscribed_is_not_the_name_core_emits(self, srv):
        # Guards the root cause: `subscribed` had zero occurrences in the entire
        # live database, while subscribe_success had 594,480.
        assert "subscribe_success" in srv.TX_TERMINAL_EVENTS
        assert "subscribe_not_found" in srv.TX_TERMINAL_EVENTS
        assert "subscribe_timeout" in srv.TX_TERMINAL_EVENTS

    @pytest.mark.parametrize("event_type,expected", [
        ("subscribe_success", "success"),
        ("subscribe_not_found", "not_found"),
        ("subscribe_timeout", "timeout"),
        ("subscribed", "success"),  # legacy alias must still settle
    ])
    def test_subscribe_settles_with_its_outcome(self, srv, event_type, expected):
        track(srv, "subscribe_request", "tx-sub", ts=100)
        tx = track(srv, event_type, "tx-sub", ts=200)
        assert tx["tx_shape"] == "settled"
        assert tx["outcome"] == expected

    def test_subscribe_does_not_stay_open_forever(self, srv):
        # 1,555,103 permanently-pending subscribes were measured on main.
        track(srv, "subscribe_request", "tx-sub", ts=100)
        assert srv.transactions["tx-sub"]["tx_shape"] == "open"
        track(srv, "subscribe_success", "tx-sub", ts=200)
        assert srv.transactions["tx-sub"]["tx_shape"] == "settled"


class TestGetOutcomeComesFromTheClientTerminal:
    """get_success / get_not_found are per-HOP and must not settle a tx."""

    @pytest.mark.parametrize("hop_event", ["get_success", "get_not_found"])
    def test_hop_events_do_not_settle_or_claim_an_outcome(self, srv, hop_event):
        track(srv, "get_request", "tx-get", ts=100)
        tx = track(srv, hop_event, "tx-get", ts=200)
        assert tx["tx_shape"] == "open", (
            "a per-hop event reported a relay's local view as the client's result"
        )
        assert tx["outcome"] is None

    @pytest.mark.parametrize("outcome", ["success", "not_found", "timeout_exhausted"])
    def test_get_terminal_settles_with_the_client_facing_outcome(self, srv, outcome):
        track(srv, "get_request", "tx-get", ts=100)
        tx = track(srv, "get_terminal", "tx-get", ts=200, terminal_outcome=outcome)
        assert tx["tx_shape"] == "settled"
        assert tx["outcome"] == outcome

    def test_get_terminal_without_an_outcome_measures_nothing(self, srv):
        track(srv, "get_request", "tx-get", ts=100)
        tx = track(srv, "get_terminal", "tx-get", ts=200, terminal_outcome=None)
        assert tx["tx_shape"] == "open"
        assert tx["outcome"] is None

    def test_client_terminal_wins_over_a_contradicting_hop(self, srv):
        """A GET that 404s at one relay and succeeds at the client is a success."""
        track(srv, "get_request", "tx-get", ts=100)
        track(srv, "get_not_found", "tx-get", ts=150)
        tx = track(srv, "get_terminal", "tx-get", ts=200, terminal_outcome="success")
        assert tx["outcome"] == "success"


class TestNoPhantomTransactionGrowth:
    """The issue is partly about phantom rows, so classification must not start
    tracking event types that were previously ignored."""

    @pytest.mark.parametrize("event_type", [
        "unsubscribed", "seeding_started", "seeding_stopped",
        "peer_startup", "peer_shutdown", "transfer_completed",
    ])
    def test_untracked_events_do_not_create_transactions(self, srv, event_type):
        srv.track_transaction("tx-new", event_type, 100, "peer-a")
        assert "tx-new" not in srv.transactions

    def test_untracked_events_still_attach_to_an_existing_transaction(self, srv):
        track(srv, "subscribe_request", "tx-x", ts=100)
        srv.track_transaction("tx-x", "unsubscribed", 200, "peer-a")
        assert len(srv.transactions["tx-x"]["events"]) == 2


class TestInvariants:
    def test_outcome_is_set_only_when_settled(self, srv):
        """The whole point of the split: a result exists only when measured."""
        stream = [
            ("get_request", "a"), ("get_not_found", "a"), ("get_success", "a"),
            ("update_broadcast_received", "b"), ("update_broadcast_applied", "b"),
            ("subscribe_request", "c"), ("subscribe_not_found", "c"),
            ("put_request", "d"),
            ("connect_request_sent", "e"), ("connect_connected", "e"),
        ]
        for i, (et, tx_id) in enumerate(stream):
            track(srv, et, tx_id, ts=100 + i)
        for tx_id, tx in srv.transactions.items():
            assert tx["tx_shape"] in ("open", "settled", "partial")
            if tx["outcome"] is not None:
                assert tx["tx_shape"] == "settled", f"{tx_id} claims an unmeasured outcome"
            if tx["tx_shape"] != "settled":
                assert tx["outcome"] is None, f"{tx_id} has an outcome without a terminal"

    def test_complete_is_no_longer_a_reachable_value(self, srv):
        """'complete' was the fallback that started all this."""
        for i, et in enumerate([
            "update_broadcast_received", "get_request", "get_not_found",
            "subscribe_request", "subscribe_success",
            "put_request", "put_success", "seeding_started",
        ]):
            # seeding_started is not a tracked op, so it may be skipped —
            # that is fine, this asserts over whatever was recorded.
            srv.track_transaction(f"tx-{i}", et, 100 + i, "peer-a")
        assert srv.transactions, "expected some transactions to be tracked"
        shapes = {tx["tx_shape"] for tx in srv.transactions.values()}
        assert "complete" not in shapes
        assert "pending" not in shapes


class TestLateHopEventsDoNotDemoteASettledTransaction:
    """`get_request` is per-HOP, so a relay's request can legitimately arrive
    after the originator's terminal. Dropping the `tx_shape == "partial"` guard
    survived the whole suite, yet it lets a late start flip a settled
    transaction back to 'open' while leaving its outcome in place — which
    violates the invariant that an outcome implies settled."""

    def test_a_late_start_does_not_reopen_a_settled_transaction(self, srv):
        track(srv, "get_request", "tx-1", ts=100)
        track(srv, "get_terminal", "tx-1", ts=200, terminal_outcome="success")
        tx = track(srv, "get_request", "tx-1", ts=300)
        assert tx["tx_shape"] == "settled", "a late per-hop request reopened a settled tx"
        assert tx["outcome"] == "success"

    def test_the_invariant_holds_under_reordering(self, srv):
        for i, et in enumerate(["get_terminal", "get_request", "get_request"]):
            srv.track_transaction("tx-2", et, 100 + i, "peer-a",
                                  terminal_outcome="not_found" if et == "get_terminal" else None)
        tx = srv.transactions["tx-2"]
        assert tx["outcome"] is None or tx["tx_shape"] == "settled"

    @pytest.mark.parametrize("start_event", ["subscribe_request", "put_request",
                                             "update_request"])
    def test_holds_for_every_op(self, srv, start_event):
        op = start_event.split("_")[0]
        terminal = {"subscribe": "subscribe_success", "put": "put_success",
                    "update": "update_success"}[op]
        track(srv, start_event, "tx-3", ts=100)
        track(srv, terminal, "tx-3", ts=200)
        tx = track(srv, start_event, "tx-3", ts=300)
        assert tx["tx_shape"] == "settled"


class TestContradictoryTerminalsAreResolvedDeterministically:
    """SUBSCRIBE/PUT/UPDATE terminals are minted per hop, and one client
    subscribe can emit both subscribe_not_found and subscribe_success. Under
    last-write-wins the stored outcome depended on collector ingest order."""

    def test_success_wins_regardless_of_arrival_order(self, srv):
        track(srv, "subscribe_request", "tx-a", ts=100)
        track(srv, "subscribe_not_found", "tx-a", ts=200)
        tx = track(srv, "subscribe_success", "tx-a", ts=300)
        assert tx["outcome"] == "success"

        track(srv, "subscribe_request", "tx-b", ts=100)
        track(srv, "subscribe_success", "tx-b", ts=200)
        tx = track(srv, "subscribe_not_found", "tx-b", ts=300)
        assert tx["outcome"] == "success", "outcome depended on ingest order"

    def test_repeated_hop_terminals_are_stable(self, srv):
        track(srv, "subscribe_request", "tx-c", ts=100)
        for i in range(4):  # a 4-hop subscribe emits four
            track(srv, "subscribe_success", "tx-c", ts=200 + i)
        assert srv.transactions["tx-c"]["outcome"] == "success"

    def test_a_client_terminal_is_not_overridden_by_a_hop_terminal(self, srv):
        """GET's client-facing verdict is authoritative."""
        track(srv, "get_request", "tx-d", ts=100)
        track(srv, "get_terminal", "tx-d", ts=200, terminal_outcome="timeout_exhausted")
        tx = track(srv, "put_success", "tx-d", ts=300)
        assert tx["outcome"] == "timeout_exhausted", (
            "a hop-observed terminal overwrote the client-facing outcome"
        )

    def test_a_client_terminal_overrides_an_earlier_hop_terminal(self, srv):
        track(srv, "subscribe_request", "tx-e", ts=100)
        track(srv, "subscribe_not_found", "tx-e", ts=200)
        tx = track(srv, "get_terminal", "tx-e", ts=300, terminal_outcome="success")
        assert tx["outcome"] == "success"
