"""Regression tests for the two ways the GET success rate has been wrong.

issue #15 — GET health must come from get_terminal.
    `get_request` and `get_not_found` are emitted once per HOP, so their ratio
    tracks route length rather than user-visible success. Deriving the
    Performance chart's GET line from them understated success by roughly 600x
    during the 2026-07-26 growth surge: the chart read 0.05-0.24% while the
    client-facing rate measured from get_terminal was 86.8-92.2%.

2026-08-08 — the rate must cover only GETs that actually ROUTED.
    get_terminal carries `attempts`, and attempts == 0 means the GET was served
    from the local store without ever contacting a peer. Those cannot fail:
    213,787 of them in 24h, 100.00% success, 0 ms at p50/p90/p99. They were 95%
    of the denominator, so the published rate read 95.4% while network-routed
    GET success was 8.46% (n=11,190; not_found 74.2%, timeout_exhausted 17.3%).
    Confirmed independently against 26 GB of raw OTLP log: 8.35% routed
    (271/3,247), 100.00% local (57,756/57,756), 95.12% blended.

    A metric that reads ~95% whether or not the network can serve a request is
    not a network metric. The classes are counted and published separately.
"""
import time

from conftest import make_record


def feed(srv, event_type, ts, tx_id=None, **body):
    """Feed one event.

    get_terminal defaults to `attempts=1` — a GET that routed to exactly one
    peer — because every test below is about network-visible GET behaviour.
    Tests covering local hits pass attempts=0 explicitly; tests covering the
    unclassifiable case pass attempts=None. The default is deliberate rather
    than incidental: an omitted `attempts` is a distinct third case, and a
    fixture that silently produced it would leave the routed rate at None.
    """
    if event_type == "get_terminal" and "attempts" not in body:
        body["attempts"] = 1
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
        assert point["get_routed_rate"] == 100.0, (
            "GET success rate was contaminated by per-hop routing events "
            f"(got {point['get_routed_rate']}%)"
        )
        assert point["get_routed_n"] == 20
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
        assert point["get_routed_rate"] == 80.0
        assert point["get_routed_n"] == 10

    def test_hop_success_events_do_not_inflate_the_denominator(self, srv):
        t = time.time_ns()
        for i in range(6):
            feed(srv, "get_terminal", t + i, tx_id=f"cli-{i}",
                 outcome="success", is_sub_op=False)
        for i in range(50):
            feed(srv, "get_success", t + 1000 + i, tx_id=f"hop-{i}")
        assert series_of(srv)["get_routed_n"] == 6


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
        assert point["get_routed_rate"] == 100.0, "direct GETs were healthy"
        assert point["get_sub_rate"] == 20.0, "sub-op failures must stay visible"
        assert point["get_routed_n"] == 10
        assert point["get_sub_n"] == 10

    def test_sub_op_failures_do_not_drag_down_the_direct_rate(self, srv):
        t = time.time_ns()
        for i in range(6):
            feed(srv, "get_terminal", t + i, tx_id=f"d-{i}",
                 outcome="success", is_sub_op=False)
        for i in range(6):
            feed(srv, "get_terminal", t + 100 + i, tx_id=f"s-{i}",
                 outcome="timeout_exhausted", is_sub_op=True)
        assert series_of(srv)["get_routed_rate"] == 100.0


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
        assert stats["routed_success_rate"] == 90.0
        assert stats["routed_total"] == 10
        assert stats["routed_not_found"] == 1
        # Hop counters survive under names that cannot be read as an outcome.
        assert stats["hop_not_found"] == 200
        assert "requests" not in stats

    def test_no_key_carries_a_rate_without_naming_its_population(self, srv):
        """A bare `success_rate`/`total` is how the local-hit blend shipped.

        Any consumer reading an unqualified key gets nothing rather than a
        number whose scope it has guessed wrong.
        """
        t = time.time_ns()
        feed(srv, "get_terminal", t, tx_id="ok", outcome="success")
        stats = srv.get_operation_stats()["get"]
        for bare in ("success_rate", "total", "not_found"):
            assert bare not in stats, (
                f"{bare!r} does not say which GETs it covers"
            )

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

    def test_a_terminal_with_no_outcome_measures_nothing(self, srv):
        """Stats and transaction tracking must agree: an event carrying no
        outcome has measured nothing and must not count as a failure."""
        t = time.time_ns()
        for i in range(6):
            feed(srv, "get_terminal", t + i, tx_id=f"ok-{i}",
                 outcome="success", is_sub_op=False)
        for i in range(20):
            # No `outcome` key at all.
            feed(srv, "get_terminal", t + 1000 + i, tx_id=f"none-{i}", is_sub_op=False)

        stats = srv.get_operation_stats()["get"]
        assert stats["routed_total"] == 6, "outcome-less terminals must not be counted"
        assert stats["routed_success_rate"] == 100.0
        assert series_of(srv)["get_routed_rate"] == 100.0

    def test_unknown_outcome_counts_without_inflating_success(self, srv):
        t = time.time_ns()
        for i in range(5):
            feed(srv, "get_terminal", t + i, tx_id=f"w-{i}",
                 outcome="something_new", is_sub_op=False)
        stats = srv.get_operation_stats()["get"]
        assert stats["routed_total"] == 5
        assert stats["routed_success_rate"] == 0.0

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
        assert series_of(srv)["get_routed_rate"] == 100.0


class TestLocalHitsDoNotDiluteTheRoutedRate:
    """The 2026-08-08 defect, in production proportions.

    Every test here fails if the attempts split is removed, and the first one
    fails with the exact number the dashboard was publishing.
    """

    def feed_production_shape(self, srv):
        """24h of real traffic, scaled 1:20.

        Routed success lands on 8.4% here against 8.46% in the 24h to
        2026-08-08; blending the local hits back in gives 95.4%, which is what
        the dashboard was publishing.
        """
        t = time.time_ns()
        n = 0

        def add(count, **body):
            nonlocal n
            for _ in range(count):
                feed(srv, "get_terminal", t + n, tx_id=f"g-{n}", **body)
                n += 1

        add(10689, outcome="success", attempts=0)            # local hits
        add(47, outcome="success", attempts=3)               # routed successes
        add(415, outcome="not_found", attempts=6)
        add(97, outcome="timeout_exhausted", attempts=9)
        return series_of(srv)

    def test_routed_rate_is_not_propped_up_by_local_hits(self, srv):
        point = self.feed_production_shape(srv)
        assert point["get_routed_rate"] == 8.4, (
            "local-store hits cannot fail, so including them in the denominator "
            "publishes cache-hit share as network health — this read 95.4%"
        )
        assert point["get_routed_n"] == 559

    def test_the_blended_rate_is_not_published_at_all(self, srv):
        """Guards against a 'fix' that renames the blend instead of splitting."""
        point = self.feed_production_shape(srv)
        blended = round(10736 / 11248 * 100, 1)   # 95.4
        assert blended not in [
            v for k, v in point.items()
            if k.endswith("_rate") and v is not None
        ], "some published rate is still the local+routed blend"

    def test_local_hits_are_still_reported_as_what_they_are(self, srv):
        point = self.feed_production_shape(srv)
        assert point["get_local_n"] == 10689
        assert point["get_local_ok"] == 10689

    def test_routed_failures_are_split_by_cause(self, srv):
        """not_found means findability; timeout means routing/transport. A
        single failure count cannot tell an operator which one to chase."""
        point = self.feed_production_shape(srv)
        assert point["get_routed_nf_n"] == 415
        assert point["get_routed_timeout_n"] == 97

    def test_a_healthy_cache_cannot_mask_a_dead_network(self, srv):
        """The scenario the old metric was structurally unable to show."""
        t = time.time_ns()
        for i in range(5000):
            feed(srv, "get_terminal", t + i, tx_id=f"loc-{i}",
                 outcome="success", attempts=0)
        for i in range(100):
            feed(srv, "get_terminal", t + 10000 + i, tx_id=f"net-{i}",
                 outcome="not_found", attempts=4)

        point = series_of(srv)
        assert point["get_routed_rate"] == 0.0, "every routed GET failed"
        assert point["get_local_n"] == 5000

    def test_a_local_only_window_reports_no_routed_rate_rather_than_100(self, srv):
        """No routed GETs means the network was not measured. Reporting 100%
        off the back of cache hits is the failure mode in miniature."""
        t = time.time_ns()
        for i in range(400):
            feed(srv, "get_terminal", t + i, tx_id=f"loc-{i}",
                 outcome="success", attempts=0)
        point = series_of(srv)
        assert point["get_routed_rate"] is None
        assert point["get_routed_n"] == 0
        assert point["get_local_n"] == 400

    def test_attempts_of_one_counts_as_routed(self, srv):
        """attempts=1 means the first peer answered — it still left the machine.
        Only 0 is local; an off-by-one here would discard the majority of the
        routed population."""
        t = time.time_ns()
        for i in range(10):
            feed(srv, "get_terminal", t + i, tx_id=f"g-{i}",
                 outcome="success", attempts=1)
        assert series_of(srv)["get_routed_n"] == 10
        assert series_of(srv)["get_local_n"] == 0

    def test_missing_attempts_is_counted_apart_from_both(self, srv):
        """Peers that stop reporting `attempts` must show up as unclassified,
        not get silently folded into either population."""
        t = time.time_ns()
        for i in range(7):
            feed(srv, "get_terminal", t + i, tx_id=f"u-{i}",
                 outcome="success", attempts=None)
        for i in range(6):
            feed(srv, "get_terminal", t + 100 + i, tx_id=f"n-{i}",
                 outcome="not_found", attempts=2)

        point = series_of(srv)
        assert point["get_unknown_n"] == 7
        assert point["get_routed_n"] == 6
        assert point["get_routed_rate"] == 0.0, "unclassified must not count as success"
        assert point["get_local_n"] == 0

    def test_the_split_survives_a_restart(self, srv):
        """The rebuild classifies in SQL and the live path in Python. If they
        drift, the dashboard changes its story when the server restarts."""
        t = time.time_ns()
        samples = ([("success", 0)] * 300 + [("success", 2)] * 4
                   + [("not_found", 5)] * 40 + [("timeout_exhausted", 9)] * 6
                   + [("success", None)] * 3)
        for i, (outcome, attempts) in enumerate(samples):
            feed(srv, "get_terminal", t + i, tx_id=f"g-{i}",
                 outcome=outcome, attempts=attempts)
        live = series_of(srv)

        srv.db.flush()
        srv.metrics_buckets.clear()
        srv._current_bucket = None
        srv.precompute_metrics_from_db()
        rebuilt = series_of(srv)

        for key in ("get_routed_rate", "get_routed_n", "get_local_n",
                    "get_unknown_n", "get_routed_nf_n", "get_routed_timeout_n"):
            assert live[key] == rebuilt[key], f"{key} changed across a restart"
        assert live["get_routed_n"] == 50
        assert live["get_local_n"] == 300


class TestTheHeadlineSumsCountsNotRates:
    """The 24h headline aggregates buckets, and a sparse bucket publishes
    `get_routed_rate: None` (below METRICS_MIN_SAMPLES) while its volume is
    still real. Reconstructing successes as `n * rate/100` dropped those
    successes and kept their n, understating the headline — a true 100% read
    as 87%, worst during exactly the quiet periods this metric should catch.
    The server therefore publishes the exact numerator.
    """

    def test_the_exact_routed_success_count_is_published(self, srv):
        t = time.time_ns()
        for i in range(3):
            feed(srv, "get_terminal", t + i, tx_id=f"ok-{i}",
                 outcome="success", attempts=2)
        feed(srv, "get_terminal", t + 100, tx_id="nf", outcome="not_found",
             attempts=2)
        point = series_of(srv)
        assert point["get_routed_ok_n"] == 3
        assert point["get_routed_n"] == 4

    def test_a_sparse_bucket_publishes_counts_even_with_no_rate(self, srv):
        """Two routed GETs is under METRICS_MIN_SAMPLES, so no rate — but the
        counts must survive, or the headline loses them."""
        t = time.time_ns()
        for i in range(2):
            feed(srv, "get_terminal", t + i, tx_id=f"ok-{i}",
                 outcome="success", attempts=2)
        point = series_of(srv)
        assert point["get_routed_rate"] is None, "too few samples for a rate"
        assert point["get_routed_ok_n"] == 2, "successes vanished with the rate"
        assert point["get_routed_n"] == 2

    def test_the_client_does_not_invert_the_rate_to_get_successes(self):
        """js/metrics.js must sum get_routed_ok_n, not n * rate / 100."""
        import os
        js = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "js", "metrics.js")
        with open(js, encoding="utf-8") as f:
            src = f.read()
        head, _, body = src.partition("function recentGetTotals")
        assert body, "recentGetTotals moved; this pin no longer bounds anything"
        body = body[:body.index("\n}")]
        assert "p.get_routed_ok_n" in body
        # Match the property ACCESS, not the bare word: the body explains in a
        # comment why the rate must not be used, and a needle that its own
        # explanation satisfies would fire on the fixed code.
        assert "p.get_routed_rate" not in body, (
            "the headline is reconstructing successes from a rounded, "
            "sometimes-null rate again"
        )


class TestTheSplitterChoiceIsRecordedAndDefensible:
    """freenet-core #4852 P2 switched its OWN split from `attempts` to
    `hop_count`, because a loopback LocalCompletion bumps `requests_sent`
    without a network round-trip. The `attempts` doc comment core exports was
    never updated and still recommends the split core abandoned.

    We still split on `attempts`, because hop_count is not populated in this
    telemetry (224,736 NULL / 242 zero / 2 non-zero over ~24h) and because the
    loopback case is empirically absent — the populations separate by latency
    with an empty band between them. That is a real judgement against a real
    counter-argument, so it is written down where the next person will look.
    """

    def test_the_hop_count_rationale_is_recorded_next_to_the_classifier(self):
        import inspect

        import telemetry_db
        src = inspect.getsource(telemetry_db)
        head, _, tail = src.partition("ROUTE_CLASS_SQL = (")
        assert tail, "ROUTE_CLASS_SQL moved; this pin no longer bounds anything"
        # Bound the search to the comment block immediately above the constant.
        block = head[head.rindex("ROUTE_LOCAL"):]
        for needle in ("hop_count", "4852", "LocalCompletion"):
            assert needle in block, (
                f"the reason we split on attempts rather than hop_count "
                f"({needle!r}) is no longer recorded beside the classifier"
            )

    def test_classification_follows_attempts_and_ignores_latency(self, srv):
        """Latency is the EVIDENCE for the choice, never the rule applied.

        A reader of the rationale might reasonably think elapsed_ms is part of
        the classifier. It is not, deliberately: latency is what we measured to
        show the populations are clean, and baking a threshold into the code
        would invent a second, unvalidated rule. So a 0 ms attempts=1 terminal
        counts as ROUTED (this is the loopback shape core's #4852 warns about —
        if production ever produces it, revisit the classifier) and a slow
        attempts=0 terminal counts as LOCAL.
        """
        t = time.time_ns()
        for i in range(6):
            feed(srv, "get_terminal", t + i, tx_id=f"fast-{i}",
                 outcome="success", attempts=1, elapsed_ms=0)
        for i in range(5):
            feed(srv, "get_terminal", t + 100 + i, tx_id=f"slow-{i}",
                 outcome="success", attempts=0, elapsed_ms=9000)

        point = series_of(srv)
        assert point["get_routed_n"] == 6, "0 ms did not make it local"
        assert point["get_local_n"] == 5, "9 s did not make it routed"


class TestOperationStatsSplitsLocalFromRouted:
    def test_local_hits_are_not_in_the_routed_rate(self, srv):
        t = time.time_ns()
        for i in range(200):
            feed(srv, "get_terminal", t + i, tx_id=f"loc-{i}",
                 outcome="success", attempts=0)
        for i in range(8):
            feed(srv, "get_terminal", t + 1000 + i, tx_id=f"nf-{i}",
                 outcome="not_found", attempts=3)
        feed(srv, "get_terminal", t + 2000, tx_id="ok", outcome="success", attempts=3)

        stats = srv.get_operation_stats()["get"]
        assert stats["routed_total"] == 9
        assert stats["routed_success_rate"] == 11.1
        assert stats["local_hit_total"] == 200
        assert stats["local_hit_success"] == 200

    def test_unclassified_terminals_do_not_leak_into_the_routed_rate(self, srv):
        """Mutating _DIRECT_STAT_BUCKET to file unknown-attempts terminals as
        routed left the whole suite green, so the stats path had no guard."""
        t = time.time_ns()
        for i in range(9):
            feed(srv, "get_terminal", t + i, tx_id=f"u-{i}",
                 outcome="success", attempts=None)
        feed(srv, "get_terminal", t + 100, tx_id="nf", outcome="not_found",
             attempts=2)

        stats = srv.get_operation_stats()["get"]
        assert stats["unclassified_total"] == 9
        assert stats["routed_total"] == 1, "unclassified leaked into routed"
        assert stats["routed_success_rate"] == 0.0
        assert stats["local_hit_total"] == 0

    def test_timeouts_are_counted_separately_from_not_found(self, srv):
        t = time.time_ns()
        for i in range(4):
            feed(srv, "get_terminal", t + i, tx_id=f"to-{i}",
                 outcome="timeout_exhausted", attempts=9)
        for i in range(3):
            feed(srv, "get_terminal", t + 100 + i, tx_id=f"nf-{i}",
                 outcome="not_found", attempts=9)
        stats = srv.get_operation_stats()["get"]
        assert stats["routed_timeout_exhausted"] == 4
        assert stats["routed_not_found"] == 3


class TestPutAndUpdateAreVolumeNotSuccess:
    """PUT/UPDATE terminals are minted per HOP, so no client-visible rate can be
    derived from them — the reasoning SUBSCRIBE has always had applied.

    Their published ratios were inverted, not merely noisy. UPDATE read 0.0-0.1%
    (update_request fires ~1,553x per transaction) while updates propagated
    fine. PUT computed 148.6% / 147.5% / 134.2% in 6 of 8 buckets, drawn against
    a hardcoded max of 100, so it rendered pinned flat at the top — a broken
    measurement displaying as perfect health, which is worse than an obviously
    wrong number. Fixing properly needs freenet-core#5250.
    """

    def test_no_put_or_update_rate_is_published(self, srv):
        t = time.time_ns()
        for i in range(40):
            feed(srv, "put_request", t + i, tx_id=f"p-{i}")
        for i in range(60):  # more successes than requests: the >100% shape
            feed(srv, "put_success", t + 100 + i, tx_id=f"p-{i % 40}")
        for i in range(30):
            feed(srv, "update_request", t + 200 + i, tx_id=f"u-{i}")
        feed(srv, "update_success", t + 300, tx_id="u-0")

        point = series_of(srv)
        for gone in ("put_rate", "upd_rate", "put_n", "upd_n"):
            assert gone not in point, (
                f"{gone!r} is a per-hop ratio presented as an outcome"
            )

    def test_the_impossible_put_ratio_is_not_published_anywhere(self, srv):
        """60 successes over 40 requests is 150%. No published value may be it."""
        t = time.time_ns()
        for i in range(40):
            feed(srv, "put_request", t + i, tx_id=f"p-{i}")
        for i in range(60):
            feed(srv, "put_success", t + 100 + i, tx_id=f"p-{i % 40}")

        point = series_of(srv)
        rates = [v for k, v in point.items() if k.endswith("_rate") and v is not None]
        assert not any(v > 100 for v in rates), f"a published rate exceeds 100%: {rates}"
        assert 150.0 not in rates

    def test_volume_is_still_reported_under_hop_names(self, srv):
        t = time.time_ns()
        for i in range(40):
            feed(srv, "put_request", t + i, tx_id=f"p-{i}")
        for i in range(60):
            feed(srv, "put_success", t + 100 + i, tx_id=f"p-{i % 40}")
        for i in range(30):
            feed(srv, "update_request", t + 200 + i, tx_id=f"u-{i}")

        point = series_of(srv)
        assert point["put_hops_n"] == 40
        assert point["put_hops_ok"] == 60
        assert point["upd_hops_n"] == 30
        # Every PUT/UPDATE key exposed must carry the hop caveat in its name.
        for k in point:
            if k.startswith(("put", "upd")):
                assert "hops" in k, f"{k!r} does not say it is a per-hop count"

    def test_the_chart_draws_them_on_a_separate_uncapped_axis(self):
        """A count on a 0-100 axis is how 148.6% rendered as flat healthy."""
        import os
        js = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "js", "metrics.js")
        with open(js, encoding="utf-8") as f:
            src = f.read()
        assert "'PUT hops'" in src and "'UPDATE hops'" in src
        # Assert the DATASETS are bound to the volume axis, not merely that the
        # axis is declared somewhere. A first version of this pin checked only
        # `"yVol" in src`, and passed when every dataset was pointed back at the
        # 0-100 success axis — the exact regression it exists to catch.
        for label in ("PUT hops", "UPDATE hops"):
            block = src[src.index(f"label: '{label}'"):]
            block = block[:block.index("},")]
            assert "yAxisID: 'yVol'" in block, (
                f"the {label} dataset is drawn on the success-% axis"
            )
        # The volume axis must not be capped. Match the actual property
        # (`max: <number>`), not the bare word — the block's own comment
        # explains that there is no max, and a needle its explanation
        # satisfies would fire on the correct code.
        import re
        vol = src[src.index("yVol: {"):]
        vol = vol[:vol.index("\n                y: {")]
        assert not re.search(r"^\s*max:\s*\d", vol, re.M), (
            "capping a count reproduces the original defect"
        )


class TestSubscribeHasNoSuccessRate:
    """issue #15 follow-up: SUBSCRIBE has no client-facing terminal event.

    The core emits GetEvent::ClientTerminal at the client boundary, but there is
    no equivalent on SubscribeEvent. subscribe_success is minted at every peer
    on the response path (register.rs, on SubscribeMsg::Response, with the
    comment noting hop_count is "preserved by relays bubbling up"), so a 4-hop
    subscribe emits four. Their ratio therefore weights by hop count — the same
    defect that made GET read 0.05% against a real 87%. No arithmetic fixes it,
    so the counts ship without a rate.
    """

    def test_no_subscribe_success_rate_is_published(self, srv):
        t = time.time_ns()
        for i in range(8):
            feed(srv, "subscribe_request", t + i, tx_id=f"s-{i}")
        for i in range(6):
            feed(srv, "subscribe_success", t + 100 + i, tx_id=f"s-{i}")
        for i in range(2):
            feed(srv, "subscribe_not_found", t + 200 + i, tx_id=f"s-{6 + i}")

        stats = srv.get_operation_stats()["subscribe"]
        assert "success_rate" not in stats, (
            "a rate over per-hop counters is issue #15 with a new name"
        )
        assert "sub_rate" not in series_of(srv)

    def test_counters_are_named_so_they_cannot_be_read_as_outcomes(self, srv):
        t = time.time_ns()
        for i in range(8):
            feed(srv, "subscribe_request", t + i, tx_id=f"s-{i}")
        for i in range(6):
            feed(srv, "subscribe_success", t + 100 + i, tx_id=f"s-{i}")
        for i in range(2):
            feed(srv, "subscribe_not_found", t + 200 + i, tx_id=f"s-{6 + i}")
        feed(srv, "subscribe_timeout", t + 300, tx_id="s-9")

        stats = srv.get_operation_stats()["subscribe"]
        assert stats == {
            "hop_requests": 8, "hop_successes": 6,
            "hop_not_found": 2, "hop_timeouts": 1,
        }
        # Every exposed key must carry the hop caveat in its name.
        assert all(k.startswith("hop_") for k in stats)

    def test_hop_counts_are_still_reported_in_the_series(self, srv):
        t = time.time_ns()
        for i in range(6):
            feed(srv, "subscribe_success", t + i, tx_id=f"s-{i}")
        for i in range(2):
            feed(srv, "subscribe_not_found", t + 100 + i, tx_id=f"n-{i}")
        point = series_of(srv)
        assert point["sub_hops_ok"] == 6
        assert point["sub_hops_bad"] == 2

    def test_subscribe_events_are_still_counted_at_all(self, srv):
        """The original defect was counters pinned at zero because they keyed
        on `subscribed`, which the core never emits. Dropping the rate must not
        reintroduce that."""
        t = time.time_ns()
        for i in range(6):
            feed(srv, "subscribe_success", t + i, tx_id=f"s-{i}")
        assert srv.get_operation_stats()["subscribe"]["hop_successes"] == 6


class TestPrecomputeRebuildsEveryOutcome:
    """The post-restart path rebuilds what the dashboard shows most of the time.

    Mutating `ok = outcome == "success"` to `ok = True` in
    precompute_metrics_from_db survived the whole suite, because every existing
    test fed it only outcome='success' at is_sub_op=0.
    """

    def feed_and_rebuild(self, srv, samples):
        t = time.time_ns()
        for i, (outcome, sub) in enumerate(samples):
            feed(srv, "get_terminal", t + i, tx_id=f"g-{i}",
                 outcome=outcome, is_sub_op=sub)
        srv.db.flush()
        srv.metrics_buckets.clear()
        srv._current_bucket = None
        srv.precompute_metrics_from_db()
        return srv.get_metrics_timeseries()["series"][-1]

    def test_rebuild_distinguishes_success_from_failure(self, srv):
        point = self.feed_and_rebuild(srv, (
            [("success", False)] * 7
            + [("not_found", False)] * 2
            + [("timeout_exhausted", False)] * 1
        ))
        assert point["get_routed_rate"] == 70.0, "non-success outcomes were counted as success"
        assert point["get_routed_n"] == 10

    def test_rebuild_keeps_sub_ops_separate(self, srv):
        point = self.feed_and_rebuild(srv, (
            [("success", False)] * 6
            + [("timeout_exhausted", True)] * 8
            + [("success", True)] * 2
        ))
        assert point["get_routed_rate"] == 100.0, "direct GETs were healthy"
        assert point["get_sub_rate"] == 20.0, "sub-op failures must survive a restart"
        assert point["get_routed_n"] == 6
        assert point["get_sub_n"] == 10

    def test_rebuild_matches_the_live_path(self, srv):
        """Whatever the restart shows must equal what was shown before it."""
        samples = ([("success", False)] * 5 + [("not_found", False)] * 3
                   + [("success", True)] * 4 + [("timeout_exhausted", True)] * 6)
        t = time.time_ns()
        for i, (outcome, sub) in enumerate(samples):
            feed(srv, "get_terminal", t + i, tx_id=f"g-{i}",
                 outcome=outcome, is_sub_op=sub)
        live = srv.get_metrics_timeseries()["series"][-1]

        srv.db.flush()
        srv.metrics_buckets.clear()
        srv._current_bucket = None
        srv.precompute_metrics_from_db()
        rebuilt = srv.get_metrics_timeseries()["series"][-1]

        for key in ("get_routed_rate", "get_sub_rate", "get_routed_n", "get_sub_n"):
            assert live[key] == rebuilt[key], f"{key} changed across a restart"
