"""
SQLite-backed storage for Freenet telemetry events, transactions, and flows.

Replaces the in-memory event_history deque and transactions dict with persistent
indexed storage. Enables instant startup (no 4.6GB JSONL parsing), deeper history
(days instead of minutes), and server-side flow queries for replay animation.

Note: The contract filter on flows is approximate — it matches flows whose peers
appear in any transaction for the contract, which may include false positives.
Duplicate events may occur if the server crashes mid-ingest before storing the
byte offset; this is benign for visualization purposes.
"""

import sqlite3
import time

import orjson

# Default DB path alongside ws_server.py
DEFAULT_DB_PATH = "/var/www/freenet-dashboard/telemetry.db"

# Keep 7 days of data by default
DEFAULT_RETENTION_NS = 7 * 24 * 60 * 60 * 1_000_000_000

SCHEMA = """
-- Events: replaces event_history deque
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ns INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    peer_id TEXT,
    tx_id TEXT,
    contract_key TEXT,
    data TEXT NOT NULL  -- full event dict as JSON
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_events_tx ON events(tx_id) WHERE tx_id IS NOT NULL;

-- Transactions: replaces transactions dict
CREATE TABLE IF NOT EXISTS transactions (
    tx_id TEXT PRIMARY KEY,
    op TEXT NOT NULL,
    contract_key TEXT,
    contract_short TEXT,
    start_ns INTEGER NOT NULL,
    end_ns INTEGER,
    status TEXT DEFAULT 'pending',
    duration_ms REAL,
    event_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_tx_start ON transactions(start_ns);

-- Transaction events: individual events within a transaction
CREATE TABLE IF NOT EXISTS tx_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    peer_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_txe_txid ON tx_events(tx_id);

-- Pre-computed flows: peer-to-peer message hops
-- tx_id stored for contract filtering via JOIN
CREATE TABLE IF NOT EXISTS flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ns INTEGER NOT NULL,
    from_peer TEXT NOT NULL,
    to_peer TEXT NOT NULL,
    event_type TEXT NOT NULL,
    tx_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_flows_ts ON flows(timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_flows_tx ON flows(tx_id) WHERE tx_id IS NOT NULL;

-- Metadata for tracking ingest position
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class TelemetryDB:
    def __init__(self, db_path=DEFAULT_DB_PATH):
        self.db_path = db_path
        self.conn = None
        self._event_buf = []
        self._tx_buf = {}  # tx_id -> tx tuple (batched upserts)
        self._txe_buf = []  # (tx_id, timestamp_ns, event_type, peer_id)
        self._flow_buf = []
        self._FLUSH_SIZE = 200
        self._enabled = True  # set to False on persistent errors to degrade gracefully

    def open(self):
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,  # autocommit; we manage transactions manually
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")  # 64MB
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.executescript(SCHEMA)

    def close(self):
        if self.conn:
            try:
                self.flush()
            except Exception:
                pass
            self.conn.close()
            self.conn = None

    # ---- Write path ----

    def insert_event(self, event):
        """Buffer an event for batch insert."""
        if not self._enabled:
            return
        self._event_buf.append((
            event.get("timestamp", 0),
            event.get("event_type", ""),
            event.get("peer_id"),
            event.get("tx_id"),
            event.get("contract_full"),
            orjson.dumps(event).decode(),
        ))
        if len(self._event_buf) >= self._FLUSH_SIZE:
            self._try_flush()

    def upsert_transaction(self, tx_id, op, contract_key, contract_short,
                           start_ns, end_ns, status, duration_ms, event_count):
        """Buffer a transaction upsert."""
        if not self._enabled:
            return
        self._tx_buf[tx_id] = (
            tx_id, op, contract_key, contract_short,
            start_ns, end_ns, status, duration_ms, event_count
        )

    def insert_tx_event(self, tx_id, timestamp_ns, event_type, peer_id):
        """Buffer a transaction event."""
        if not self._enabled:
            return
        self._txe_buf.append((tx_id, timestamp_ns, event_type, peer_id))

    def compute_flows_for_tx(self, tx_id):
        """Compute peer-to-peer flows from a completed transaction's events.
        Uses events already in DB (flushed) or in the buffer."""
        if not self._enabled:
            return
        try:
            # Get events from DB
            cur = self.conn.execute(
                "SELECT timestamp_ns, event_type, peer_id FROM tx_events "
                "WHERE tx_id = ? ORDER BY timestamp_ns",
                (tx_id,)
            )
            events = list(cur.fetchall())

            # Also check buffer for unflushed events
            for txe in self._txe_buf:
                if txe[0] == tx_id:
                    events.append((txe[1], txe[2], txe[3]))
            events.sort(key=lambda e: e[0])

            if len(events) < 2:
                return

            # Find consecutive events on different peers
            for j in range(1, len(events)):
                ts_prev, _et_prev, pid_prev = events[j - 1]
                ts_curr, et_curr, pid_curr = events[j]
                if pid_prev and pid_curr and pid_prev != pid_curr:
                    mid_ts = (ts_prev + ts_curr) // 2
                    self._flow_buf.append((mid_ts, pid_prev, pid_curr, et_curr, tx_id))
        except Exception as e:
            print(f"[db] compute_flows_for_tx error: {e}")

    def _try_flush(self):
        """Flush with error handling — disables DB on persistent failures."""
        try:
            self.flush()
        except Exception as e:
            print(f"[db] flush error (disabling DB writes): {e}")
            self._enabled = False
            # Clear buffers to prevent memory buildup
            self._event_buf.clear()
            self._tx_buf.clear()
            self._txe_buf.clear()
            self._flow_buf.clear()

    def flush(self):
        """Flush all buffered writes to DB in a single transaction."""
        if not self._event_buf and not self._tx_buf and not self._txe_buf and not self._flow_buf:
            return

        self.conn.execute("BEGIN")
        try:
            if self._event_buf:
                self.conn.executemany(
                    "INSERT INTO events (timestamp_ns, event_type, peer_id, tx_id, contract_key, data) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    self._event_buf,
                )
                self._event_buf.clear()

            if self._tx_buf:
                self.conn.executemany(
                    "INSERT OR REPLACE INTO transactions "
                    "(tx_id, op, contract_key, contract_short, start_ns, end_ns, status, duration_ms, event_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    list(self._tx_buf.values()),
                )
                self._tx_buf.clear()

            if self._txe_buf:
                self.conn.executemany(
                    "INSERT INTO tx_events (tx_id, timestamp_ns, event_type, peer_id) "
                    "VALUES (?, ?, ?, ?)",
                    self._txe_buf,
                )
                self._txe_buf.clear()

            if self._flow_buf:
                self.conn.executemany(
                    "INSERT INTO flows (timestamp_ns, from_peer, to_peer, event_type, tx_id) "
                    "VALUES (?, ?, ?, ?, ?)",
                    self._flow_buf,
                )
                self._flow_buf.clear()

            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    # ---- Read path ----

    def get_recent_events(self, limit=20000):
        """Get the most recent events as dicts."""
        cur = self.conn.execute(
            "SELECT data FROM events ORDER BY timestamp_ns DESC LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
        # Reverse so oldest-first (clients expect chronological order)
        return [orjson.loads(row[0]) for row in reversed(rows)]

    def get_time_range(self):
        """Get (min_timestamp, max_timestamp) from events table.
        Uses indexed MIN/MAX which are O(1) on the timestamp_ns index."""
        cur = self.conn.execute(
            "SELECT MIN(timestamp_ns), MAX(timestamp_ns) FROM events"
        )
        row = cur.fetchone()
        return (row[0] or 0, row[1] or 0)

    def get_recent_transactions(self, limit=2000, ops=None):
        """Get recent transactions with their events.
        Uses a single JOIN query instead of N+1 individual queries."""
        if ops:
            placeholders = ",".join("?" for _ in ops)
            cur = self.conn.execute(
                f"SELECT t.tx_id, t.op, t.contract_key, t.contract_short, t.start_ns, "
                f"t.end_ns, t.status, t.duration_ms, t.event_count "
                f"FROM transactions t WHERE t.op IN ({placeholders}) "
                f"ORDER BY t.start_ns DESC LIMIT ?",
                (*ops, limit),
            )
        else:
            cur = self.conn.execute(
                "SELECT tx_id, op, contract_key, contract_short, start_ns, end_ns, "
                "status, duration_ms, event_count "
                "FROM transactions ORDER BY start_ns DESC LIMIT ?",
                (limit,),
            )
        tx_rows = cur.fetchall()
        if not tx_rows:
            return []

        # Collect all tx_ids and fetch their events in one query
        tx_ids = [row[0] for row in tx_rows]
        placeholders = ",".join("?" for _ in tx_ids)
        ecur = self.conn.execute(
            f"SELECT tx_id, timestamp_ns, event_type, peer_id FROM tx_events "
            f"WHERE tx_id IN ({placeholders}) ORDER BY timestamp_ns",
            tx_ids,
        )
        # Group events by tx_id
        tx_events = {}
        for e in ecur.fetchall():
            tx_events.setdefault(e[0], []).append(
                {"timestamp": e[1], "event_type": e[2], "peer_id": e[3]}
            )

        result = []
        for row in reversed(tx_rows):  # oldest-first
            tx_id = row[0]
            events = tx_events.get(tx_id, [])
            result.append({
                "tx_id": tx_id,
                "op": row[1],
                "contract": row[3],  # short form
                "contract_full": row[2],
                "start_ns": row[4],
                "end_ns": row[5] or row[4],
                "duration_ms": row[7],
                "status": row[6],
                "event_count": len(events),
                "events": events,
            })
        return result

    def get_flows_for_range(self, start_ns, end_ns, contract_key=None, peer_id=None):
        """Get pre-computed flows for a time range.
        Contract filtering uses the tx_id stored on each flow to JOIN
        to the transactions table."""
        if contract_key:
            # Filter via tx_id -> transactions.contract_key
            sql = (
                "SELECT f.timestamp_ns, f.from_peer, f.to_peer, f.event_type "
                "FROM flows f "
                "JOIN transactions t ON f.tx_id = t.tx_id "
                "WHERE f.timestamp_ns BETWEEN ? AND ? AND t.contract_key = ?"
            )
            params = [start_ns, end_ns, contract_key]
            if peer_id:
                sql += " AND (f.from_peer = ? OR f.to_peer = ?)"
                params.extend([peer_id, peer_id])
        else:
            sql = "SELECT timestamp_ns, from_peer, to_peer, event_type FROM flows WHERE timestamp_ns BETWEEN ? AND ?"
            params = [start_ns, end_ns]
            if peer_id:
                sql += " AND (from_peer = ? OR to_peer = ?)"
                params.extend([peer_id, peer_id])

        sql += " ORDER BY timestamp_ns LIMIT 500"

        cur = self.conn.execute(sql, params)
        return [
            {
                "timestamp_ns": row[0],
                "fromPeer": row[1],
                "toPeer": row[2],
                "eventType": row[3],
                "offsetMs": (row[0] - start_ns) / 1_000_000,
            }
            for row in cur.fetchall()
        ]

    # ---- Metadata ----

    def get_meta(self, key, default=None):
        cur = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else default

    def set_meta(self, key, value):
        """Write metadata. Uses its own transaction to avoid interfering
        with any buffered write transaction."""
        self.conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            (key, str(value)),
        )

    # ---- Maintenance ----

    def prune(self, retention_ns=DEFAULT_RETENTION_NS):
        """Remove data older than retention period."""
        if not self._enabled:
            return
        cutoff = int(time.time() * 1_000_000_000) - retention_ns
        try:
            self.conn.execute("BEGIN")
            self.conn.execute("DELETE FROM events WHERE timestamp_ns < ?", (cutoff,))
            self.conn.execute("DELETE FROM flows WHERE timestamp_ns < ?", (cutoff,))
            # Delete tx_events for old transactions first, then transactions
            self.conn.execute(
                "DELETE FROM tx_events WHERE tx_id IN "
                "(SELECT tx_id FROM transactions WHERE start_ns < ?)",
                (cutoff,),
            )
            self.conn.execute("DELETE FROM transactions WHERE start_ns < ?", (cutoff,))
            self.conn.execute("COMMIT")
        except Exception as e:
            try:
                self.conn.execute("ROLLBACK")
            except Exception:
                pass
            print(f"[db] prune error: {e}")

    def optimize(self):
        """Run PRAGMA optimize for query planner."""
        try:
            self.conn.execute("PRAGMA optimize")
        except Exception:
            pass

    def event_count(self):
        """Approximate event count using SQLite's internal page stats.
        Falls back to exact count for small tables."""
        try:
            # Use max rowid as approximation (fast, O(1) on index)
            cur = self.conn.execute("SELECT MAX(id) FROM events")
            row = cur.fetchone()
            return row[0] or 0
        except Exception:
            return 0

    def flow_count(self):
        try:
            cur = self.conn.execute("SELECT MAX(id) FROM flows")
            row = cur.fetchone()
            return row[0] or 0
        except Exception:
            return 0
