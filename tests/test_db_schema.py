"""Schema migration and get_terminals persistence (issue #15)."""
import sqlite3
import time

from telemetry_db import TelemetryDB


def columns(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


class TestAdditiveOnlyMigration:
    """issue #15 follow-up: the migration is purely additive.

    `status` is left in place and untouched, the two new columns are appended
    beside it, and no row is rewritten. That keeps a rollback to pre-#16 code
    working, which matters because flush() batches every table into one
    transaction and _try_flush disables the DB permanently on error — a missing
    column would silently stop ALL ingest, not just transactions.
    """

    LEGACY_ROWS = [
        ("t1", "update", "complete"),    # phantom, minted by a propagation event
        ("t2", "subscribe", "pending"),  # never settled: wrong terminal name
        ("t3", "get", "success"),        # per-HOP verdict, not a client outcome
        ("t4", "get", "not_found"),      # per-HOP verdict, not a client outcome
    ]

    def make_legacy_db(self, path):
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE transactions (
                tx_id TEXT PRIMARY KEY, op TEXT NOT NULL, contract_key TEXT,
                contract_short TEXT, start_ns INTEGER NOT NULL, end_ns INTEGER,
                status TEXT DEFAULT 'pending', duration_ms REAL,
                event_count INTEGER DEFAULT 0
            );
        """)
        conn.executemany(
            "INSERT INTO transactions (tx_id, op, start_ns, status) VALUES (?, ?, 1, ?)",
            self.LEGACY_ROWS)
        conn.commit()
        conn.close()

    def test_status_survives_so_a_rollback_still_works(self, tmp_path):
        p = str(tmp_path / "legacy.db")
        self.make_legacy_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            cols = columns(db.conn, "transactions")
            assert "status" in cols, "dropping `status` breaks rollback to pre-#16 code"
            assert {"tx_shape", "outcome"} <= cols
            # An old writer's INSERT names `status` explicitly; it must still work.
            db.conn.execute(
                "INSERT INTO transactions (tx_id, op, contract_key, contract_short, "
                "start_ns, end_ns, status, duration_ms, event_count) "
                "VALUES ('old', 'get', NULL, NULL, 1, 2, 'pending', 1.0, 1)")
        finally:
            db.close()

    def test_no_legacy_row_is_rewritten(self, tmp_path):
        p = str(tmp_path / "legacy.db")
        self.make_legacy_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            got = dict(db.conn.execute("SELECT tx_id, status FROM transactions"))
        finally:
            db.close()
        assert got == {t: s for t, _, s in self.LEGACY_ROWS}, \
            "legacy values must be preserved, not reinterpreted in place"

    def test_legacy_rows_report_unclassified_not_a_fabricated_outcome(self, tmp_path):
        """`partial` honestly means "we have not classified this row".

        t3/t4 matter most: their legacy `status` came from get_success /
        get_not_found, which are per-HOP. Promoting those into `outcome` would
        put a relay's local verdict into a column documented as a client-facing
        result — the very confusion issue #15 is about.
        """
        p = str(tmp_path / "legacy.db")
        self.make_legacy_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            got = dict(
                (r[0], (r[1], r[2]))
                for r in db.conn.execute(
                    "SELECT tx_id, tx_shape, outcome FROM transactions")
            )
        finally:
            db.close()
        for tx_id in ("t1", "t2", "t3", "t4"):
            assert got[tx_id] == ("partial", None), tx_id

    def test_migration_is_idempotent(self, tmp_path):
        p = str(tmp_path / "legacy.db")
        self.make_legacy_db(p)
        for _ in range(3):
            db = TelemetryDB(p)
            db.open()
            db.close()
        db = TelemetryDB(p)
        db.open()
        try:
            assert columns(db.conn, "transactions") >= {"status", "tx_shape", "outcome"}
            assert dict(db.conn.execute("SELECT tx_id, status FROM transactions")) == \
                {t: s for t, _, s in self.LEGACY_ROWS}
        finally:
            db.close()

    def test_new_writes_use_the_new_columns_only(self, tmp_path):
        """Nothing writes `status` any more; it is inert, not dual-written."""
        p = str(tmp_path / "legacy.db")
        self.make_legacy_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            db.upsert_transaction("new", "get", None, None, 1, 2,
                                  "settled", "success", 1.0, 2)
            db.flush()
            row = db.conn.execute(
                "SELECT status, tx_shape, outcome FROM transactions WHERE tx_id='new'"
            ).fetchone()
            assert row == ("pending", "settled", "success")
        finally:
            db.close()

    def test_fresh_database_has_every_table_and_column(self, tmp_path):
        db = TelemetryDB(str(tmp_path / "fresh.db"))
        db.open()
        try:
            assert {"status", "tx_shape", "outcome"} <= columns(db.conn, "transactions")
            tables = {r[0] for r in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            assert "get_terminals" in tables
        finally:
            db.close()


class TestGetTerminalsTable:
    def test_roundtrip_and_bucket_aggregation(self, tmp_path):
        db = TelemetryDB(str(tmp_path / "t.db"))
        db.open()
        try:
            bucket = 4 * 3600 * 1_000_000_000
            base = (time.time_ns() // bucket) * bucket + 1_000_000
            for i in range(7):
                db.insert_get_terminal(base + i, f"tx{i}", "peer-a", "ck",
                                       "success", False, 0, 3, 12.0)
            for i in range(3):
                db.insert_get_terminal(base + 100 + i, f"sx{i}", "peer-a", "ck",
                                       "timeout_exhausted", True, 1, 5, 900.0)
            db.flush()

            rows = db.get_terminal_buckets(base - bucket, bucket)
            agg = {(r[1], r[2]): r[3] for r in rows}
            assert agg[("success", 0)] == 7
            assert agg[("timeout_exhausted", 1)] == 3
            assert len({r[0] for r in rows}) == 1, "all samples share one bucket"
        finally:
            db.close()

    def test_pruning_removes_expired_rows(self, tmp_path):
        db = TelemetryDB(str(tmp_path / "t.db"))
        db.open()
        try:
            now = time.time_ns()
            old = now - 48 * 3600 * 1_000_000_000
            db.insert_get_terminal(old, "old", "p", "c", "success", False, 0, 1, 1.0)
            db.insert_get_terminal(now, "new", "p", "c", "success", False, 0, 1, 1.0)
            db.flush()
            db.prune(retention_ns=24 * 3600 * 1_000_000_000)
            left = [r[0] for r in db.conn.execute("SELECT tx_id FROM get_terminals")]
            assert left == ["new"], "get_terminals must not grow without bound"
        finally:
            db.close()


class TestTransactionRoundTrip:
    def test_shape_and_outcome_survive_a_write_and_read(self, tmp_path):
        db = TelemetryDB(str(tmp_path / "t.db"))
        db.open()
        try:
            db.upsert_transaction("tx1", "get", "ck", "ck...", 1, 2,
                                  "settled", "success", 1.0, 2)
            db.upsert_transaction("tx2", "update", None, None, 1, 2,
                                  "partial", None, None, 1)
            db.flush()
            got = {t["tx_id"]: t for t in db.get_recent_transactions(limit=10)}
            assert got["tx1"]["tx_shape"] == "settled"
            assert got["tx1"]["outcome"] == "success"
            assert got["tx2"]["tx_shape"] == "partial"
            assert got["tx2"]["outcome"] is None
            assert "status" not in got["tx1"]
        finally:
            db.close()


class TestGetTerminalsNeedsNoIndex:
    """A deliberate decision, pinned structurally rather than by a benchmark.

    get_terminal_buckets is called with since_ns = METRICS_MAX_AGE_NS (8 days)
    against a 24h retention, so its WHERE matches every row by construction,
    and it GROUPs BY a computed bucket expression no timestamp_ns index can
    serve. So an index cannot help the only query this table exists for, at any
    row count — which a timing run alone would not establish.
    """

    def test_no_index_is_declared_for_get_terminals(self):
        from telemetry_db import SCHEMA_INDEXES
        assert not any("get_terminals" in stmt for stmt in SCHEMA_INDEXES), (
            "an index cannot serve this table's only query and adds an "
            "operator step; see the note in SCHEMA_INDEXES"
        )

    def test_the_metrics_window_is_wider_than_retention(self, tmp_path):
        """Why the WHERE filters nothing: the lookback exceeds what is kept."""
        import ws_server
        from telemetry_db import DEFAULT_RETENTION_NS
        assert ws_server.METRICS_MAX_AGE_NS > DEFAULT_RETENTION_NS

        db = TelemetryDB(str(tmp_path / "t.db"))
        db.open()
        try:
            now = time.time_ns()
            for i in range(20):
                db.insert_get_terminal(now - i * 3_600_000_000_000, f"t{i}", "p",
                                       "c", "success", False, 0, 1, 1.0)
            db.flush()
            cut = now - ws_server.METRICS_MAX_AGE_NS
            matched = db.conn.execute(
                "SELECT COUNT(*) FROM get_terminals WHERE timestamp_ns > ?",
                (cut,)).fetchone()[0]
            total = db.conn.execute("SELECT COUNT(*) FROM get_terminals").fetchone()[0]
            assert matched == total == 20, "the WHERE is not a filter in practice"
        finally:
            db.close()

    def test_an_index_cannot_serve_the_group_by(self, tmp_path):
        """The temp B-tree survives the index, so the index buys nothing."""
        db = TelemetryDB(str(tmp_path / "t.db"))
        db.open()
        try:
            db.conn.execute(
                "CREATE INDEX idx_probe ON get_terminals(timestamp_ns)")
            bucket = 4 * 3600 * 1_000_000_000
            plan = [r[-1] for r in db.conn.execute(
                "EXPLAIN QUERY PLAN "
                f"SELECT (timestamp_ns/{bucket})*{bucket} AS b, outcome, is_sub_op, "
                "COUNT(*), AVG(elapsed_ms) FROM get_terminals "
                "WHERE timestamp_ns > ? GROUP BY b, outcome, is_sub_op", (0,))]
            assert any("TEMP B-TREE" in step for step in plan), (
                f"expected the GROUP BY to still need a temp B-tree, got {plan}"
            )
        finally:
            db.close()

    def test_the_aggregate_is_correct_without_an_index(self, tmp_path):
        db = TelemetryDB(str(tmp_path / "t.db"))
        db.open()
        try:
            bucket = 4 * 3600 * 1_000_000_000
            base = (time.time_ns() // bucket) * bucket + 1_000_000
            for i in range(50):
                db.insert_get_terminal(base + i, f"tx{i}", "p", "c",
                                       "success" if i % 2 else "not_found",
                                       i % 3 == 0, 0, 2, 5.0)
            db.flush()
            rows = db.get_terminal_buckets(base - bucket, bucket)
            assert sum(r[3] for r in rows) == 50
            assert {r[1] for r in rows} == {"success", "not_found"}
        finally:
            db.close()


class TestMigrationRepairsADatabaseThatRanTheRenamingVersion:
    """The superseded migration renamed `status` away. CREATE TABLE IF NOT
    EXISTS cannot bring a renamed column back, so without an explicit repair a
    rollback to pre-#16 code fails with "no such column: status" — and
    _try_flush swallows that by disabling writes permanently, so the process
    keeps running while persisting nothing.
    """

    def make_renamed_db(self, path):
        """A database in the state #16's migration would have left it."""
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE transactions (
                tx_id TEXT PRIMARY KEY, op TEXT NOT NULL, contract_key TEXT,
                contract_short TEXT, start_ns INTEGER NOT NULL, end_ns INTEGER,
                tx_shape TEXT DEFAULT 'partial', outcome TEXT, duration_ms REAL,
                event_count INTEGER DEFAULT 0
            );
        """)
        conn.execute("INSERT INTO transactions (tx_id, op, start_ns, tx_shape) "
                     "VALUES ('t1', 'get', 1, 'settled')")
        conn.commit()
        conn.close()

    def test_status_is_restored(self, tmp_path):
        p = str(tmp_path / "renamed.db")
        self.make_renamed_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            assert "status" in columns(db.conn, "transactions")
        finally:
            db.close()

    def test_a_pre_16_writer_can_insert_again(self, tmp_path):
        p = str(tmp_path / "renamed.db")
        self.make_renamed_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            db.conn.execute(
                "INSERT INTO transactions (tx_id, op, contract_key, contract_short, "
                "start_ns, end_ns, status, duration_ms, event_count) "
                "VALUES ('old', 'get', NULL, NULL, 1, 2, 'pending', 1.0, 1)")
        finally:
            db.close()

    def test_existing_new_style_rows_are_untouched(self, tmp_path):
        p = str(tmp_path / "renamed.db")
        self.make_renamed_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            assert db.conn.execute(
                "SELECT tx_shape FROM transactions WHERE tx_id='t1'"
            ).fetchone()[0] == "settled"
        finally:
            db.close()
