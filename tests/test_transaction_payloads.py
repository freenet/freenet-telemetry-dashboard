"""The three writers of transaction shape must agree.

The dashboard reaches the same `transactions` table by three independent
paths — the live in-memory tracker, the SQLite history query, and the bulk
backfill script. The client cannot tell them apart, so a field name or a
classification that differs between them is a silent break.
"""
import time

import pytest

import backfill_db
import ws_server
from conftest import make_record


class TestBackfillSharesTheServersDefinitions:
    """backfill_db.py writes the same tables by an independent path.

    It used to keep its own transcription of the classification tables and
    drifted twice: keying subscribes off `subscribed` (never emitted) and
    recording get_not_found as "success". It now imports them, so drift is
    unrepresentable rather than merely tested for.
    """

    def test_tables_are_the_servers_own(self):
        assert backfill_db.TX_START_EVENTS == set(ws_server.TX_START_EVENTS)
        assert backfill_db.TX_TERMINAL_EVENTS == \
            {k: v[1] for k, v in ws_server.TX_TERMINAL_EVENTS.items()}
        assert backfill_db.outcome_wins is ws_server.outcome_wins

    def test_schema_is_the_servers_own(self):
        """The hand-copied DDL silently omitted get_terminals, so a rebuilt
        database produced a chart with both GET lines blank."""
        import inspect
        import telemetry_db
        src = inspect.getsource(backfill_db.main)
        assert "executescript(SCHEMA_TABLES)" in src
        assert "CREATE TABLE events" not in src, "schema was re-transcribed"
        assert "get_terminals" in telemetry_db.SCHEMA_TABLES

    def test_backfill_does_not_settle_on_per_hop_get_events(self):
        assert "get_success" not in backfill_db.TX_TERMINAL_EVENTS
        assert "get_not_found" not in backfill_db.TX_TERMINAL_EVENTS

    def test_backfill_stores_the_client_facing_get_event(self):
        assert "get_terminal" in backfill_db.HISTORY_EVENT_TYPES
        assert "subscribe_timeout" in backfill_db.HISTORY_EVENT_TYPES

    def test_backfill_populates_get_terminals(self):
        """Without this the documented recovery path restores PUT and UPDATE
        and leaves both GET lines blank — the metric this work exists to fix."""
        import inspect
        src = inspect.getsource(backfill_db.main)
        assert "INSERT INTO get_terminals" in src
        assert "getterm_buf.append" in src


class TestLiveAndHistoryPayloadsAgree:
    def test_live_list_reports_shape_and_outcome(self, srv):
        srv.track_transaction("tx1", "get_request", 100, "peer-a")
        srv.track_transaction("tx1", "get_terminal", 200, "peer-a",
                              terminal_outcome="success")
        [tx] = srv.get_transactions_list()
        assert tx["tx_shape"] == "settled"
        assert tx["outcome"] == "success"
        assert "status" not in tx

    def test_live_and_db_paths_expose_the_same_fields(self, srv):
        srv.track_transaction("tx1", "subscribe_request", 100, "peer-a")
        srv.track_transaction("tx1", "subscribe_not_found", 200, "peer-a")
        live = srv.get_transactions_list()[0]
        srv.db.flush()
        stored = srv.db.get_recent_transactions(limit=10)[0]

        shared = {"tx_id", "op", "start_ns", "end_ns", "duration_ms",
                  "tx_shape", "outcome", "event_count", "events"}
        assert shared <= set(live)
        assert shared <= set(stored)
        assert live["tx_shape"] == stored["tx_shape"] == "settled"
        assert live["outcome"] == stored["outcome"] == "not_found"

    def test_unsettled_transaction_carries_no_outcome_through_either_path(self, srv):
        srv.track_transaction("tx1", "update_broadcast_received", 100, "peer-a")
        live = srv.get_transactions_list()[0]
        srv.db.flush()
        stored = srv.db.get_recent_transactions(limit=10)[0]
        assert live["tx_shape"] == stored["tx_shape"] == "partial"
        assert live["outcome"] is None and stored["outcome"] is None


class TestGetTerminalsPersistEndToEnd:
    def test_process_record_projects_the_outcome_into_its_table(self, srv):
        t = time.time_ns()
        srv.process_record(make_record("get_terminal", t, tx_id="tx1",
                                       outcome="timeout_exhausted", is_sub_op=True,
                                       attempts=2, hop_count=4, elapsed_ms=310))
        srv.db.flush()
        rows = srv.db.conn.execute(
            "SELECT tx_id, outcome, is_sub_op, attempts, hop_count, elapsed_ms "
            "FROM get_terminals").fetchall()
        assert rows == [("tx1", "timeout_exhausted", 1, 2, 4, 310)]

    def test_the_metrics_precompute_reads_them_back(self, srv):
        t = time.time_ns()
        for i in range(6):
            srv.process_record(make_record("get_terminal", t + i, tx_id=f"g-{i}",
                                           outcome="success", is_sub_op=False))
        srv.db.flush()
        # Wipe the in-memory buckets: this is the post-restart path.
        srv.metrics_buckets.clear()
        srv._current_bucket = None
        srv.precompute_metrics_from_db()
        assert srv.get_metrics_timeseries()["series"][-1]["get_rate"] == 100.0

    @pytest.mark.parametrize("outcome", ["success", "not_found", "timeout_exhausted"])
    def test_no_row_is_written_without_an_outcome(self, srv, outcome):
        t = time.time_ns()
        srv.process_record(make_record("get_terminal", t, tx_id="good", outcome=outcome))
        srv.process_record(make_record("get_terminal", t + 1, tx_id="bad"))
        srv.db.flush()
        stored = [r[0] for r in srv.db.conn.execute("SELECT tx_id FROM get_terminals")]
        assert stored == ["good"]
