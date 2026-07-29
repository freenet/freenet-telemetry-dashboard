"""Regression tests for issue #15 — GET health must come from get_terminal.

`get_request` and `get_not_found` are emitted once per HOP, so their ratio
tracks route length rather than user-visible success. Deriving the Performance
chart's GET line from them understated success by roughly 600x during the
2026-07-26 growth surge: the chart read 0.05-0.24% while the client-facing rate
measured from get_terminal was 86.8-92.2%.
"""
import time

from conftest import make_record


def feed(srv, event_type, ts, tx_id=None, **body):
    srv.process_record(make_record(event_type, ts, tx_id=tx_id, **body))


def series_of(srv):
    s = srv.get_metrics_timeseries()["series"]
    assert s, "expected at least one metrics bucket"
    return s[-1]


class TestGetSuccessRateIgnoresPerHopEvents:
    def test_a_storm_of_hop_404s_does_not_sink_the_success_rate(self, srv):
        """The exact production shape: ~1000 hop 404s alongside 20 real successes."""
        t = time.time_ns()
        for i in range(1000):
            feed(srv, "get_not_found", t + i, tx_id=f"hop-{i}")
        for i in range(20):
            feed(srv, "get_terminal", t + 2000 + i, tx_id=f"cli-{i}",
                 outcome="success", is_sub_op=False, elapsed_ms=12)

        point = series_of(srv)
        assert point["get_rate"] == 100.0, (
            "GET success rate was contaminated by per-hop routing events "
            f"(got {point['get_rate']}%)"
        )
        assert point["get_n"] == 20
        # The hop volume is still reported, just not as a success signal.
        assert point["get_hops_n"] == 1000

    def test_rate_tracks_the_terminals_not_the_hops(self, srv):
        t = time.time_ns()
        for i in range(500):
            feed(srv, "get_not_found", t + i, tx_id=f"hop-{i}")
        for i in range(8):
            feed(srv, "get_terminal", t + 1000 + i, tx_id=f"ok-{i}",
                 outcome="success", is_sub_op=False)
        for i in range(2):
            feed(srv, "get_terminal", t + 2000 + i, tx_id=f"bad-{i}",
                 outcome="not_found", is_sub_op=False)

        point = series_of(srv)
        assert point["get_rate"] == 80.0
        assert point["get_n"] == 10

    def test_hop_success_events_do_not_inflate_the_denominator(self, srv):
        t = time.time_ns()
        for i in range(6):
            feed(srv, "get_terminal", t + i, tx_id=f"cli-{i}",
                 outcome="success", is_sub_op=False)
        for i in range(50):
            feed(srv, "get_success", t + 1000 + i, tx_id=f"hop-{i}")
        assert series_of(srv)["get_n"] == 6


class TestSubOperationGetsAreVisible:
    """The regression that the old signal hid: sub-op GETs timing out."""

    def test_sub_op_outcomes_are_reported_separately(self, srv):
        t = time.time_ns()
        for i in range(10):
            feed(srv, "get_terminal", t + i, tx_id=f"d-{i}",
                 outcome="success", is_sub_op=False)
        for i in range(8):
            feed(srv, "get_terminal", t + 100 + i, tx_id=f"s-{i}",
                 outcome="timeout_exhausted", is_sub_op=True)
        for i in range(2):
            feed(srv, "get_terminal", t + 200 + i, tx_id=f"sok-{i}",
                 outcome="success", is_sub_op=True)

        point = series_of(srv)
        assert point["get_rate"] == 100.0, "direct GETs were healthy"
        assert point["get_sub_rate"] == 20.0, "sub-op failures must stay visible"
        assert point["get_n"] == 10
        assert point["get_sub_n"] == 10

    def test_sub_op_failures_do_not_drag_down_the_direct_rate(self, srv):
        t = time.time_ns()
        for i in range(6):
            feed(srv, "get_terminal", t + i, tx_id=f"d-{i}",
                 outcome="success", is_sub_op=False)
        for i in range(6):
            feed(srv, "get_terminal", t + 100 + i, tx_id=f"s-{i}",
                 outcome="timeout_exhausted", is_sub_op=True)
        assert series_of(srv)["get_rate"] == 100.0


class TestOperationStatsPanel:
    def test_get_success_rate_is_measured_from_terminals(self, srv):
        t = time.time_ns()
        for i in range(200):
            feed(srv, "get_not_found", t + i, tx_id=f"hop-{i}")
        for i in range(9):
            feed(srv, "get_terminal", t + 1000 + i, tx_id=f"ok-{i}",
                 outcome="success", is_sub_op=False)
        feed(srv, "get_terminal", t + 2000, tx_id="bad", outcome="not_found",
             is_sub_op=False)

        stats = srv.get_operation_stats()["get"]
        assert stats["success_rate"] == 90.0
        assert stats["total"] == 10
        assert stats["not_found"] == 1
        # Hop counters survive under names that cannot be read as an outcome.
        assert stats["hop_not_found"] == 200
        assert "requests" not in stats

    def test_sub_op_rate_is_exposed(self, srv):
        t = time.time_ns()
        for i in range(4):
            feed(srv, "get_terminal", t + i, tx_id=f"s-{i}",
                 outcome="timeout_exhausted", is_sub_op=True)
        feed(srv, "get_terminal", t + 100, tx_id="sok", outcome="success",
             is_sub_op=True)
        stats = srv.get_operation_stats()["get"]
        assert stats["sub_op_total"] == 5
        assert stats["sub_op_success_rate"] == 20.0

    def test_unknown_outcome_counts_without_inflating_success(self, srv):
        t = time.time_ns()
        for i in range(5):
            feed(srv, "get_terminal", t + i, tx_id=f"w-{i}",
                 outcome="something_new", is_sub_op=False)
        stats = srv.get_operation_stats()["get"]
        assert stats["total"] == 5
        assert stats["success_rate"] == 0.0

    def test_latency_still_measured_from_request_to_success(self, srv):
        """get_terminal reports elapsed_ms=0 on 99.5% of successful direct GETs,
        so it cannot be the latency source even though it is the outcome source.
        Latency stays on the request->success delta."""
        t = time.time_ns()
        for i in range(5):
            feed(srv, "get_request", t + i * 1_000_000_000, tx_id=f"l-{i}")
            feed(srv, "get_success", t + i * 1_000_000_000 + 40_000_000, tx_id=f"l-{i}")
        assert srv.get_operation_stats()["get"]["latency"]["p50"] == 40

    def test_zero_elapsed_terminals_do_not_flatten_the_latency_series(self, srv):
        """Regression guard for the trap above: a flood of elapsed_ms=0
        terminals must not drag the reported GET latency to zero."""
        t = time.time_ns()
        for i in range(5):
            feed(srv, "get_request", t + i * 1_000_000_000, tx_id=f"l-{i}")
            feed(srv, "get_success", t + i * 1_000_000_000 + 40_000_000, tx_id=f"l-{i}")
        for i in range(500):
            feed(srv, "get_terminal", t + 10_000_000_000 + i, tx_id=f"z-{i}",
                 outcome="success", is_sub_op=False, elapsed_ms=0)

        assert srv.get_operation_stats()["get"]["latency"]["p50"] == 40
        assert series_of(srv)["lat_get"] == 40
        # ...while the success rate is still measured from those terminals.
        assert series_of(srv)["get_rate"] == 100.0


class TestSubscribeStatsAreNoLongerPinnedAtZero:
    def test_subscribe_success_is_counted(self, srv):
        """op_stats and sub_ok both keyed on `subscribed`, which is never
        emitted, so every subscribe counter read zero."""
        t = time.time_ns()
        for i in range(8):
            feed(srv, "subscribe_request", t + i, tx_id=f"s-{i}")
        for i in range(6):
            feed(srv, "subscribe_success", t + 100 + i, tx_id=f"s-{i}")
        for i in range(2):
            feed(srv, "subscribe_not_found", t + 200 + i, tx_id=f"s-{6 + i}")

        stats = srv.get_operation_stats()["subscribe"]
        assert stats["total"] == 8
        assert stats["success_rate"] == 75.0
        assert stats["not_found"] == 2
        assert series_of(srv)["sub_n"] == 8
        assert series_of(srv)["sub_rate"] == 75.0

    def test_subscribe_timeout_counts_as_a_failure(self, srv):
        t = time.time_ns()
        for i in range(5):
            feed(srv, "subscribe_success", t + i, tx_id=f"s-{i}")
        for i in range(5):
            feed(srv, "subscribe_timeout", t + 100 + i, tx_id=f"t-{i}")
        stats = srv.get_operation_stats()["subscribe"]
        assert stats["timeouts"] == 5
        assert stats["success_rate"] == 50.0
