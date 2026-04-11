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
CREATE INDEX IF NOT EXISTS idx_tx_contract ON transactions(contract_key) WHERE contract_key IS NOT NULL;

-- Transaction events: individual events within a transaction
CREATE TABLE IF NOT EXISTS tx_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    peer_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_txe_txid ON tx_events(tx_id);
CREATE INDEX IF NOT EXISTS idx_txe_type_ts ON tx_events(event_type, timestamp_ns);

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
CREATE INDEX IF NOT EXISTS idx_flows_type_ts ON flows(event_type, timestamp_ns);

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

            # Find consecutive events on different peers, capped to avoid
            # explosion from large broadcast transactions (100+ peers)
            MAX_FLOWS_PER_TX = 5
            flow_count = 0
            for j in range(1, len(events)):
                ts_prev, _et_prev, pid_prev = events[j - 1]
                ts_curr, et_curr, pid_curr = events[j]
                if pid_prev and pid_curr and pid_prev != pid_curr:
                    mid_ts = (ts_prev + ts_curr) // 2
                    self._flow_buf.append((mid_ts, pid_prev, pid_curr, et_curr, tx_id))
                    flow_count += 1
                    if flow_count >= MAX_FLOWS_PER_TX:
                        break
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

    def get_sampled_events(self, limit=10000):
        """Get events sampled evenly across the full time range.

        Uses jittered time-grid seek sampling: generate `limit` evenly-spaced
        target timestamps, then index-seek to the nearest event for each.
        Two-phase for the large events table: collect rowids from the index
        first, then batch-fetch full rows by sorted primary key.
        """
        start_ns, end_ns = self.get_time_range()
        if not start_ns or not end_ns or start_ns >= end_ns:
            return self.get_recent_events(limit)

        # Cap end_ns to now + 1 hour to exclude bogus future-dated events
        import time as _time
        now_ns = int(_time.time() * 1_000_000_000)
        end_ns = min(end_ns, now_ns + 3600 * 1_000_000_000)

        total = self.event_count()
        if total <= limit:
            cur = self.conn.execute(
                "SELECT data FROM events ORDER BY timestamp_ns"
            )
            return [orjson.loads(row[0]) for row in cur.fetchall()]

        # Phase 1: Generate evenly-spaced target timestamps and collect rowids
        # via index-only seeks (no table access for the heavy data column).
        range_ns = end_ns - start_ns
        step_ns = range_ns // limit
        rowids = []
        seen = set()
        for i in range(limit):
            target_ns = start_ns + i * step_ns + step_ns // 2  # midpoint of slot
            row = self.conn.execute(
                "SELECT id FROM events WHERE timestamp_ns >= ? ORDER BY timestamp_ns LIMIT 1",
                (target_ns,),
            ).fetchone()
            if row and row[0] not in seen:
                rowids.append(row[0])
                seen.add(row[0])

        if not rowids:
            return []

        # Phase 2: Batch-fetch full rows by sorted primary key for sequential I/O.
        rowids.sort()
        results = []
        for i in range(0, len(rowids), 500):
            chunk = rowids[i:i + 500]
            ph = ",".join("?" * len(chunk))
            cur = self.conn.execute(
                f"SELECT data FROM events WHERE id IN ({ph}) ORDER BY timestamp_ns",
                chunk,
            )
            for row in cur.fetchall():
                results.append(orjson.loads(row[0]))
        return results

    def get_time_range(self):
        """Get (min_timestamp, max_timestamp) from events table.
        Uses two ORDER BY LIMIT 1 queries that leverage idx_events_ts index."""
        row = self.conn.execute(
            "SELECT timestamp_ns FROM events ORDER BY timestamp_ns ASC LIMIT 1"
        ).fetchone()
        min_ts = row[0] if row else 0
        row = self.conn.execute(
            "SELECT timestamp_ns FROM events ORDER BY timestamp_ns DESC LIMIT 1"
        ).fetchone()
        max_ts = row[0] if row else 0
        return (min_ts, max_ts)

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

    def _seek_sample_tx_events(self, types, budget, start_ns, end_ns,
                               contract_key=None, peer_id=None):
        """Sample tx_events using time-grid seek: one index seek per sample point.

        Generates `budget` evenly-spaced target timestamps and seeks to the
        nearest matching event for each. Uses idx_txe_type_ts for O(log n) seeks.
        Returns list of (timestamp_ns, event_type, peer_id, tx_id, None, None).
        """
        range_ns = end_ns - start_ns
        step_ns = max(1, range_ns // budget)
        results = []
        seen_keys = set()  # deduplicate by (timestamp_ns, peer_id)

        ph = ",".join("?" * len(types))

        if contract_key:
            sql = (f"SELECT te.timestamp_ns, te.event_type, te.peer_id, te.tx_id "
                   f"FROM tx_events te JOIN transactions t ON te.tx_id = t.tx_id "
                   f"WHERE te.event_type IN ({ph}) AND te.timestamp_ns >= ? "
                   f"AND t.contract_key = ? ORDER BY te.timestamp_ns LIMIT 1")
            for i in range(budget):
                target = start_ns + i * step_ns + step_ns // 2
                row = self.conn.execute(sql, (*types, target, contract_key)).fetchone()
                if row and row[0] <= end_ns:
                    key = (row[0], row[2])
                    if key not in seen_keys:
                        seen_keys.add(key)
                        results.append((row[0], row[1], row[2], row[3], None, None))
        elif peer_id:
            sql = (f"SELECT timestamp_ns, event_type, peer_id, tx_id "
                   f"FROM tx_events WHERE event_type IN ({ph}) "
                   f"AND timestamp_ns >= ? AND peer_id = ? "
                   f"ORDER BY timestamp_ns LIMIT 1")
            for i in range(budget):
                target = start_ns + i * step_ns + step_ns // 2
                row = self.conn.execute(sql, (*types, target, peer_id)).fetchone()
                if row and row[0] <= end_ns:
                    key = (row[0], row[2])
                    if key not in seen_keys:
                        seen_keys.add(key)
                        results.append((row[0], row[1], row[2], row[3], None, None))
        else:
            sql = (f"SELECT timestamp_ns, event_type, peer_id, tx_id "
                   f"FROM tx_events WHERE event_type IN ({ph}) "
                   f"AND timestamp_ns >= ? ORDER BY timestamp_ns LIMIT 1")
            for i in range(budget):
                target = start_ns + i * step_ns + step_ns // 2
                row = self.conn.execute(sql, (*types, target)).fetchone()
                if row and row[0] <= end_ns:
                    key = (row[0], row[2])
                    if key not in seen_keys:
                        seen_keys.add(key)
                        results.append((row[0], row[1], row[2], row[3], None, None))

        return results

    def _seek_sample_broadcast_events(self, types, budget, start_ns, end_ns,
                                      contract_key=None, peer_id=None):
        """Sample broadcast events from the events table using time-grid seeks.

        Similar to _seek_sample_tx_events but reads from the events table
        to extract from_peer/to_peer from the data JSON column.
        """
        range_ns = end_ns - start_ns
        step_ns = max(1, range_ns // budget)
        results = []
        seen_keys = set()

        ph = ",".join("?" * len(types))
        base_sql = (f"SELECT timestamp_ns, event_type, peer_id, tx_id, data "
                    f"FROM events WHERE event_type IN ({ph}) AND timestamp_ns >= ?")
        extra_params = []
        if contract_key:
            base_sql += " AND contract_key = ?"
            extra_params.append(contract_key)
        elif peer_id:
            base_sql += " AND peer_id = ?"
            extra_params.append(peer_id)
        base_sql += " ORDER BY timestamp_ns LIMIT 1"

        for i in range(budget):
            target = start_ns + i * step_ns + step_ns // 2
            row = self.conn.execute(base_sql, (*types, target, *extra_params)).fetchone()
            if not row or row[0] > end_ns:
                continue
            key = (row[0], row[2])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            from_peer, to_peer = None, None
            if row[4]:
                try:
                    d = orjson.loads(row[4])
                    fp = d.get("from_peer")
                    tp = d.get("to_peer")
                    pid = d.get("peer_id")
                    if fp and tp:
                        from_peer, to_peer = fp, tp
                    elif tp and pid and tp != pid:
                        from_peer, to_peer = tp, pid
                except Exception:
                    pass
            results.append((row[0], row[1], row[2], row[3], from_peer, to_peer))

        return results

    def get_events_for_range(self, start_ns, end_ns, contract_key=None, peer_id=None):
        """Get events for particle animation, with per-type budgets.

        Uses time-grid seek sampling: for each event type group, generates
        `budget` evenly-spaced target timestamps and seeks to the nearest
        matching event. This gives truly uniform timeline coverage with no
        clustering at bucket boundaries.

        Returns a mix of 'hop' particles (peer-to-peer travel) and 'pulse'
        particles (single-peer glow).
        """
        if not self.conn:
            return []

        range_ns = end_ns - start_ns
        if range_ns <= 0:
            return []

        # Per-type budgets — proportional to visual importance, not raw count
        TYPE_BUDGETS = {
            'connect': {
                'types': ('connected',),
                'limit': 2000,
            },
            'get': {
                'types': ('get_request', 'get_success', 'get_not_found', 'get_failure'),
                'limit': 5000,
            },
            'subscribe': {
                'types': ('subscribe_request', 'subscribe_success', 'subscribe_not_found'),
                'limit': 5000,
            },
            'update': {
                'types': ('update_request', 'update_success', 'update_failure'),
                'limit': 5000,
            },
            'broadcast': {
                'types': ('update_broadcast_received', 'update_broadcast_applied',
                          'broadcast_emitted', 'update_broadcast_emitted', 'broadcast_applied'),
                'limit': 3000,
            },
            'put': {
                'types': ('put_request', 'put_success'),
                'limit': 2000,
            },
        }

        all_events = []

        for group_name, group in TYPE_BUDGETS.items():
            if group_name == 'connect' and contract_key:
                continue

            types = group['types']
            budget = group['limit']

            if group_name == 'broadcast':
                events = self._seek_sample_broadcast_events(
                    types, budget, start_ns, end_ns, contract_key, peer_id)
            else:
                events = self._seek_sample_tx_events(
                    types, budget, start_ns, end_ns, contract_key, peer_id)
            all_events.extend(events)

        if not all_events:
            return []

        # Batch lookup: tx_id → contract_key
        tx_ids = set(e[3] for e in all_events if e[3])
        tx_to_contract = {}
        if tx_ids:
            # Query in chunks to avoid too-large IN clause
            tx_list = list(tx_ids)
            for i in range(0, len(tx_list), 500):
                chunk = tx_list[i:i+500]
                ph_chunk = ",".join("?" * len(chunk))
                cur = self.conn.execute(
                    f"SELECT tx_id, contract_key FROM transactions WHERE tx_id IN ({ph_chunk}) AND contract_key IS NOT NULL",
                    chunk)
                for row in cur.fetchall():
                    tx_to_contract[row[0]] = row[1]

        # Group by tx_id for hop reconstruction
        by_tx = {}
        no_tx = []
        for event_tuple in all_events:
            ts, et, pid, txid = event_tuple[0], event_tuple[1], event_tuple[2], event_tuple[3]
            from_peer = event_tuple[4] if len(event_tuple) > 4 else None
            to_peer = event_tuple[5] if len(event_tuple) > 5 else None

            # If we have explicit from/to peers (broadcast events), emit hop directly
            if from_peer and to_peer and from_peer != to_peer:
                no_tx.append((ts, et, pid, txid, from_peer, to_peer))
            elif txid:
                if txid not in by_tx:
                    by_tx[txid] = []
                by_tx[txid].append((ts, et, pid))
            else:
                no_tx.append((ts, et, pid, txid, None, None))

        particles = []

        # Reconstruct hops from tx groups (consecutive events on different peers)
        MAX_HOPS_PER_TX = 8
        for txid, events in by_tx.items():
            events.sort(key=lambda e: e[0])
            hops_emitted = 0
            prev_pulse_peer = None
            ck = tx_to_contract.get(txid)

            for j in range(len(events)):
                ts, et, pid = events[j]
                if j > 0 and hops_emitted < MAX_HOPS_PER_TX:
                    ts_prev, _et_prev, pid_prev = events[j - 1]
                    if pid and pid_prev and pid != pid_prev:
                        p = {
                            "type": "hop",
                            "timestamp_ns": (ts_prev + ts) // 2,
                            "fromPeer": pid_prev,
                            "toPeer": pid,
                            "eventType": et,
                            "txId": txid,
                            "offsetMs": ((ts_prev + ts) // 2 - start_ns) / 1_000_000,
                        }
                        if ck:
                            p["contractKey"] = ck
                        particles.append(p)
                        hops_emitted += 1
                        prev_pulse_peer = pid
                        continue

                # Single-peer event or no hop detected — emit pulse
                if pid and pid != prev_pulse_peer:
                    p = {
                        "type": "pulse",
                        "timestamp_ns": ts,
                        "peer": pid,
                        "eventType": et,
                        "txId": txid,
                        "offsetMs": (ts - start_ns) / 1_000_000,
                    }
                    if ck:
                        p["contractKey"] = ck
                    particles.append(p)
                    prev_pulse_peer = pid

        # Events with explicit from/to or without tx_id
        for event_tuple in no_tx:
            ts, et, pid, txid = event_tuple[0], event_tuple[1], event_tuple[2], event_tuple[3]
            from_peer = event_tuple[4] if len(event_tuple) > 4 else None
            to_peer = event_tuple[5] if len(event_tuple) > 5 else None
            ck = tx_to_contract.get(txid) if txid else None

            if from_peer and to_peer and from_peer != to_peer:
                p = {
                    "type": "hop",
                    "timestamp_ns": ts,
                    "fromPeer": from_peer,
                    "toPeer": to_peer,
                    "eventType": et,
                    "txId": txid,
                    "offsetMs": (ts - start_ns) / 1_000_000,
                }
                if ck:
                    p["contractKey"] = ck
                particles.append(p)
            elif pid:
                p = {
                    "type": "pulse",
                    "timestamp_ns": ts,
                    "peer": pid,
                    "eventType": et,
                    "offsetMs": (ts - start_ns) / 1_000_000,
                }
                if ck:
                    p["contractKey"] = ck
                particles.append(p)

        return particles

    def get_flows_for_range(self, start_ns, end_ns, contract_key=None, peer_id=None, limit=None):
        """Get pre-computed flows for a time range.
        When filtered by contract or peer, returns all flows via single query.
        When unfiltered, samples across time buckets to limit volume."""
        is_filtered = bool(contract_key or peer_id)
        if limit is None:
            limit = 50000 if is_filtered else 10000

        range_ns = end_ns - start_ns
        if range_ns <= 0:
            return []

        # Only include interesting event types (positive filter uses idx_flows_type_ts efficiently)
        INTERESTING_TYPES = (
            'get_request', 'get_success', 'get_not_found', 'get_failure',
            'put_request', 'put_success',
            'subscribe_request', 'subscribe_success', 'subscribe_not_found',
            'update_request', 'update_success', 'update_failure',
            'update_broadcast_received', 'update_broadcast_applied',
            'broadcast_emitted', 'update_broadcast_emitted', 'broadcast_applied',
        )
        # Connect events get a separate small budget so they don't overwhelm
        CONNECT_TYPES = ('connected',)
        CONNECT_LIMIT = 500  # sparse sample of connect events
        interesting_filter = " AND event_type IN ({})".format(",".join("?" * len(INTERESTING_TYPES)))

        where = "timestamp_ns BETWEEN ? AND ?"
        params = [start_ns, end_ns]
        table = "flows"
        select_cols = "timestamp_ns, from_peer, to_peer, event_type, tx_id"

        if contract_key:
            table = "flows f JOIN transactions t ON f.tx_id = t.tx_id"
            select_cols = "f.timestamp_ns, f.from_peer, f.to_peer, f.event_type, f.tx_id"
            where = "f.timestamp_ns BETWEEN ? AND ? AND t.contract_key = ?"
            params = [start_ns, end_ns, contract_key]
            if peer_id:
                where += " AND (f.from_peer = ? OR f.to_peer = ?)"
                params.extend([peer_id, peer_id])
        elif peer_id:
            where += " AND (from_peer = ? OR to_peer = ?)"
            params.extend([peer_id, peer_id])

        def row_to_flow(row):
            return {
                "timestamp_ns": row[0],
                "fromPeer": row[1],
                "toPeer": row[2],
                "eventType": row[3],
                "txId": row[4],
                "offsetMs": (row[0] - start_ns) / 1_000_000,
            }

        if is_filtered:
            # Single query — relies on idx_tx_contract and idx_flows_tx indexes
            sql = f"SELECT {select_cols} FROM {table} WHERE {where} ORDER BY timestamp_ns LIMIT {limit}"
            cur = self.conn.execute(sql, params)
            return [row_to_flow(row) for row in cur.fetchall()]

        # Send ALL non-connect interesting flows (~53k, ~6MB) — no sampling.
        # Connect events get a sparse sample (500) since there are 30M+ of them.
        NON_CONNECT_TYPES = (
            'get_request', 'get_success', 'get_not_found', 'get_failure',
            'put_request', 'put_success',
            'subscribe_request', 'subscribe_success', 'subscribe_not_found',
            'update_request', 'update_success', 'update_failure',
            'update_broadcast_received', 'update_broadcast_applied',
            'broadcast_emitted', 'update_broadcast_emitted', 'broadcast_applied',
        )
        nc_filter = " AND event_type IN ({})".format(",".join("?" * len(NON_CONNECT_TYPES)))
        sql = f"SELECT {select_cols} FROM {table} WHERE {where}{nc_filter} ORDER BY timestamp_ns"
        cur = self.conn.execute(sql, params + list(NON_CONNECT_TYPES))
        all_flows = [row_to_flow(row) for row in cur.fetchall()]

        # Add sparse sample of connect events
        CONNECT_LIMIT = 500
        conn_filter = " AND event_type = 'connected'"
        num_buckets = 50
        bucket_ns = range_ns // num_buckets
        conn_per_bucket = max(1, CONNECT_LIMIT // num_buckets)
        conn_count = 0
        for b in range(num_buckets):
            bs = start_ns + b * bucket_ns
            be = bs + bucket_ns
            bp = list(params)
            bp[0] = bs
            bp[1] = be
            sql = f"SELECT {select_cols} FROM {table} WHERE {where}{conn_filter} ORDER BY timestamp_ns LIMIT {conn_per_bucket}"
            cur = self.conn.execute(sql, bp)
            for row in cur.fetchall():
                all_flows.append(row_to_flow(row))
                conn_count += 1
            if conn_count >= CONNECT_LIMIT:
                break

        return all_flows

    # ---- Contract reconstruction ----

    def get_active_contracts(self, since_ns=None):
        """Get contracts with recent activity for rebuilding in-memory state.
        Returns {contract_key: {subscribers: set(peer_id), peer_count: int}}
        from transactions and tx_events tables."""
        if not self.conn:
            return {}

        if since_ns is None:
            # Default: last 7 days
            row = self.conn.execute(
                "SELECT timestamp_ns FROM events ORDER BY timestamp_ns DESC LIMIT 1"
            ).fetchone()
            if not row:
                return {}
            since_ns = row[0] - 7 * 24 * 3600 * 1_000_000_000

        # Find contracts with recent transactions
        cur = self.conn.execute(
            "SELECT DISTINCT contract_key FROM transactions "
            "WHERE contract_key IS NOT NULL AND start_ns > ?",
            (since_ns,)
        )
        contract_keys = [row[0] for row in cur.fetchall()]
        if not contract_keys:
            return {}

        result = {}
        for ck in contract_keys:
            # Get distinct peer_ids involved with this contract
            cur = self.conn.execute(
                "SELECT DISTINCT te.peer_id FROM tx_events te "
                "JOIN transactions t ON te.tx_id = t.tx_id "
                "WHERE t.contract_key = ? AND te.timestamp_ns > ? "
                "AND te.peer_id IS NOT NULL",
                (ck, since_ns)
            )
            peers = set(row[0] for row in cur.fetchall() if row[0])
            if peers:
                result[ck] = {"subscribers": peers, "peer_count": len(peers)}

        return result

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
