"""Schema migration and get_terminals persistence (issue #15)."""
import sqlite3
import time

from telemetry_db import TelemetryDB


def columns(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


class TestMigrationFromTheOldStatusColumn:
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
        rows = [
            ("t1", "update", "complete"),   # phantom: propagation event
            ("t2", "subscribe", "pending"),  # never settled, wrong terminal name
            ("t3", "get", "success"),        # genuinely measured
            ("t4", "get", "not_found"),      # genuinely measured
        ]
        conn.executemany(
            "INSERT INTO transactions (tx_id, op, start_ns, status) VALUES (?, ?, 1, ?)",
            rows)
        conn.commit()
        conn.close()

    def test_status_is_renamed_and_outcome_added(self, tmp_path):
        p = str(tmp_path / "legacy.db")
        self.make_legacy_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            cols = columns(db.conn, "transactions")
            assert "status" not in cols, "the misleading column name must be gone"
            assert {"tx_shape", "outcome"} <= cols
        finally:
            db.close()

    def test_legacy_values_are_reinterpreted_honestly(self, tmp_path):
        p = str(tmp_path / "legacy.db")
        self.make_legacy_db(p)
        db = TelemetryDB(p)
        db.open()
        try:
            got = dict(
                (r[0], (r[1], r[2]))
                for r in db.conn.execute("SELECT tx_id, tx_shape, outcome FROM transactions")
            )
        finally:
            db.close()
        # 'complete' was never evidence of a terminal, so it degrades to partial
        # rather than being promoted to a settled result.
        assert got["t1"] == ("partial", None)
        assert got["t2"] == ("open", None)
        assert got["t3"] == ("settled", "success")
        assert got["t4"] == ("settled", "not_found")

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
            assert columns(db.conn, "transactions") >= {"tx_shape", "outcome"}
            n = db.conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE tx_shape IN "
                "('pending','complete','success','not_found')").fetchone()[0]
            assert n == 0
        finally:
            db.close()

    def test_fresh_database_has_the_new_schema(self, tmp_path):
        db = TelemetryDB(str(tmp_path / "fresh.db"))
        db.open()
        try:
            cols = columns(db.conn, "transactions")
            assert "status" not in cols
            assert {"tx_shape", "outcome"} <= cols
            assert "get_terminals" in {
                r[0] for r in db.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
            }
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
