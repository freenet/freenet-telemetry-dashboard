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

import os
import sqlite3
import time

import orjson

# Overridable to run against a scratch database in local development.
DEFAULT_DB_PATH = os.environ.get(
    "FREENET_DASHBOARD_DB", "/var/www/freenet-dashboard/telemetry.db")

# Keep 24 hours of data by default. The telemetry firehose scales with the peer
# count, and the network doubled in two days (2026-07-26), which put the DB at
# 157 GB on a volume with 168 GB free — a 48h window leaves no room for another
# doubling. A 7-day window previously grew the DB past 300 GB and filled the
# root disk (2026-05-22). 24h keeps a full day of lookback at roughly half the
# footprint; the raw JSONL retains a comparable window if deeper lookback is
# needed. Tune this constant to taste — lowering it takes effect on the next
# prune() and drains gradually, since prune() works in bounded batches.
DEFAULT_RETENTION_NS = 24 * 60 * 60 * 1_000_000_000

# prune() batching. A retention cut (or a long outage) leaves a backlog far
# larger than one cycle's worth of data; deleting it in a single transaction
# balloons the WAL and blocks ingest for the whole delete. A runaway WAL grew a
# 329 GB husk and filled the disk (2026-05-22). Bounded batches keep each
# transaction small and let a backlog drain over many cycles instead.
PRUNE_ROW_BATCH = 20_000  # rows per DELETE for events/flows
PRUNE_TX_BATCH = 2_000  # transactions (with their tx_events) per DELETE
# ws_server is single-threaded asyncio, so prune() blocks the event loop (and
# therefore the log tailer and every WebSocket client) for as long as it runs.
# Keep the budget short: in steady state prune returns early once the ~60s of
# newly-expired rows are gone, and a backlog just takes more cycles to drain.
PRUNE_TIME_BUDGET_S = 5.0  # max wall-clock spent pruning per call

SCHEMA_TABLES = """
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

-- Transactions: replaces transactions dict
--
-- tx_shape describes the SHAPE of what we observed, never the result:
--   'open'    -- a genuine start event was seen, no terminal yet
--   'settled' -- a genuine terminal event was seen; `outcome` says which
--   'partial' -- events seen, but neither a start nor a terminal. Propagation
--                events (update_broadcast_received and friends) land here.
--
-- `outcome` is NULL unless tx_shape='settled', so a rate computed over it
-- returns no rows rather than a confident wrong number when nothing was
-- measured. What it means depends on the op, and the difference matters:
--
--   GET  -- CLIENT-FACING. Sourced from get_terminal, the only event the core
--           emits at the client boundary. Safe to compute a success rate from,
--           and the get_terminals table exists for exactly that.
--   everything else -- HOP-OBSERVED. The core has no ClientTerminal analogue
--           for SUBSCRIBE/PUT/UPDATE (see event_kind.rs: ClientTerminal is a
--           GetEvent variant only), so their terminals are minted at each peer
--           on the response path. One transaction can therefore produce several,
--           and this column records the winner under TX_OUTCOME_PRECEDENCE in
--           ws_server.py. It says "a terminal of this kind was observed
--           somewhere on the path", NOT "this is what the client saw", and it
--           MUST NOT be aggregated into a success rate — doing so weights by
--           hop count, which is the exact defect issue #15 was filed about.
--
-- `status` is the superseded column, kept in place and untouched. Nothing
-- writes it any more; it survives so a rollback to pre-#16 code still finds
-- the column it expects. Read tx_shape/outcome instead. It can be dropped once
-- rolling back that far is no longer a concern.
CREATE TABLE IF NOT EXISTS transactions (
    tx_id TEXT PRIMARY KEY,
    op TEXT NOT NULL,
    contract_key TEXT,
    contract_short TEXT,
    start_ns INTEGER NOT NULL,
    end_ns INTEGER,
    status TEXT DEFAULT 'pending',
    tx_shape TEXT DEFAULT 'partial',
    outcome TEXT,
    duration_ms REAL,
    event_count INTEGER DEFAULT 0
);

-- Client-facing GET outcomes, projected out of the `get_terminal` event.
--
-- This is the ONLY event that reports the outcome a GET client actually saw.
-- get_request/get_success/get_not_found are emitted per HOP, so their ratio
-- tracks route length, not user-visible success. Kept as its own small table
-- (~200k rows/day) so GET health can be aggregated without either scanning the
-- multi-hundred-GB events table or adding an event_type index to it.
--
-- `attempts` is load-bearing and must be split on before any rate is computed:
--   attempts = 0   -- LOCAL store hit. The GET never left the machine, so it
--                     cannot fail. 100.00% success over 213,787 samples in the
--                     24h to 2026-08-08, and 0 ms at p50/p90/p99.
--   attempts >= 1  -- NETWORK-ROUTED. This is the only population that measures
--                     whether the network can serve a request.
-- Local hits are ~95% of direct GETs, so a rate over the union is ~95% no
-- matter how badly the network is doing — it read 95.4% on 2026-08-08 while
-- routed GET success was 8.5% (not_found 74.2%, timeout_exhausted 17.3%).
-- Never aggregate across the two. See TelemetryDB.route_class.
CREATE TABLE IF NOT EXISTS get_terminals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ns INTEGER NOT NULL,
    tx_id TEXT,
    peer_id TEXT,
    contract_key TEXT,
    outcome TEXT NOT NULL,       -- success | not_found | timeout_exhausted | ...
    is_sub_op INTEGER NOT NULL,  -- 0 = client-issued GET, 1 = sub-operation
    attempts INTEGER,
    hop_count INTEGER,
    elapsed_ms REAL
);

-- Transaction events: individual events within a transaction
CREATE TABLE IF NOT EXISTS tx_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    peer_id TEXT
);

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

-- Metadata for tracking ingest position
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Synthetic network checks (freenet-core #4665). Separate tables because the
-- value here is the multi-week trend: the 48h prune that the telemetry
-- firehose needs would delete a 7-day retention series before it could be
-- plotted. prune() must never touch these.
-- Keyed by (scenario, run_id): run ids are the nightly's timestamp, so two
-- checks running the same night would otherwise overwrite each other.
CREATE TABLE IF NOT EXISTS check_runs (
    run_id           TEXT    NOT NULL,
    scenario         TEXT    NOT NULL,   -- "put_get", "update_propagation", ...
    vantage          TEXT    NOT NULL,   -- where it ran from: "nova", "ci", ...
    timestamp_ns     INTEGER NOT NULL,
    duration_ms      INTEGER,
    verdict          TEXT    NOT NULL,   -- pass | degraded | fail | error
    ops_total        INTEGER,
    ops_failed       INTEGER,
    software_version TEXT,
    conditions       TEXT,              -- JSON blob, UI-opaque
    PRIMARY KEY (scenario, run_id)
);

CREATE TABLE IF NOT EXISTS check_ops (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         TEXT    NOT NULL,
    scenario       TEXT    NOT NULL,
    vantage        TEXT    NOT NULL,
    timestamp_ns   INTEGER NOT NULL,
    op             TEXT    NOT NULL,     -- put | get | update | subscribe | ...
    dimension      TEXT,                 -- "0h", "24h", "7d", "join", ...
    dimension_secs INTEGER,              -- numeric form for ordering; NULL if N/A
    contract_key   TEXT,                 -- join key into events/transactions/flows
    ok             INTEGER NOT NULL,
    latency_ms     REAL,
    bytes          INTEGER,
    error          TEXT,
    extra          TEXT                  -- JSON blob, UI-opaque
);
"""

# Indexes are created separately so they can be deferred on large DBs.
# On a 128GB DB, CREATE INDEX IF NOT EXISTS still checks the full table
# for new indexes, which can take minutes. We create them in a background
# thread after the server is already accepting connections.
SCHEMA_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp_ns)",
    "CREATE INDEX IF NOT EXISTS idx_events_tx ON events(tx_id) WHERE tx_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_events_peer_ts ON events(peer_id, event_type, timestamp_ns) WHERE peer_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_tx_start ON transactions(start_ns)",
    "CREATE INDEX IF NOT EXISTS idx_tx_contract ON transactions(contract_key) WHERE contract_key IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_txe_txid ON tx_events(tx_id)",
    "CREATE INDEX IF NOT EXISTS idx_txe_type_ts ON tx_events(event_type, timestamp_ns)",
    "CREATE INDEX IF NOT EXISTS idx_txe_peer_ts ON tx_events(peer_id, event_type, timestamp_ns)",
    # get_terminals deliberately has NO index on timestamp_ns, for a structural
    # reason rather than a benchmarked one. get_terminal_buckets is called with
    # since_ns = METRICS_MAX_AGE_NS (8 days) against a 24h retention, so its
    # WHERE matches every row by construction, and it GROUPs BY a computed
    # bucket expression that no timestamp_ns index can serve — the query plan
    # keeps its temp B-tree either way. An index therefore cannot help the only
    # query this table exists for, at any row count. It would also make
    # correctness depend on an operator running create_indexes.py, since
    # nothing creates indexes at startup.
    #
    # The cost that DOES grow is prune's probe, which scans. The trigger is GET
    # volume rising at fixed retention, not retention changing. Measured:
    # 250k rows 382ms aggregate / 14ms probe; 500k 977/51; 1M 2142/167. prune
    # blocks the asyncio event loop and probes at least twice per cycle, so 1M
    # rows is roughly a 330ms stall per cycle with nothing alarming on it.
    # Cheap now and linear from here — but this module's own retention comment
    # records the network doubling in two days, so revisit when get_terminals
    # approaches ~1M rows in a window.
    "CREATE INDEX IF NOT EXISTS idx_flows_ts ON flows(timestamp_ns)",
    "CREATE INDEX IF NOT EXISTS idx_flows_tx ON flows(tx_id) WHERE tx_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_flows_type_ts ON flows(event_type, timestamp_ns)",
    "CREATE INDEX IF NOT EXISTS idx_check_runs_scenario_ts ON check_runs(scenario, timestamp_ns)",
    "CREATE INDEX IF NOT EXISTS idx_check_ops_scenario_ts ON check_ops(scenario, timestamp_ns)",
    "CREATE INDEX IF NOT EXISTS idx_check_ops_run ON check_ops(scenario, run_id)",
    "CREATE INDEX IF NOT EXISTS idx_check_ops_contract ON check_ops(contract_key) WHERE contract_key IS NOT NULL",
]


class TelemetryDB:
    def __init__(self, db_path=DEFAULT_DB_PATH):
        self.db_path = db_path
        self.conn = None
        self._event_buf = []
        self._tx_buf = {}  # tx_id -> tx tuple (batched upserts)
        self._txe_buf = []  # (tx_id, timestamp_ns, event_type, peer_id)
        self._flow_buf = []
        self._getterm_buf = []  # client-facing GET outcomes
        self._FLUSH_SIZE = 200
        self._enabled = True  # set to False on persistent errors to degrade gracefully

    def open(self):
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,  # autocommit; we manage transactions manually
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        # Cap the WAL file: without a size limit the WAL never shrinks once it
        # balloons. A runaway WAL grew a 329 GB husk and filled the disk
        # (2026-05-22). 512 MB is truncated back after each checkpoint.
        self.conn.execute("PRAGMA journal_size_limit=536870912")  # 512 MB
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")  # 64MB
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        # Only create tables synchronously — indexes are deferred
        self.conn.executescript(SCHEMA_TABLES)
        self._migrate()

    def _migrate(self):
        """Bring an existing database up to the current schema.

        Purely additive: `status` is left in place and untouched, and the two
        new columns are appended beside it. Nothing is renamed and no row is
        rewritten, which has three consequences worth stating.

        It is effectively free. ADD COLUMN with a constant DEFAULT is
        metadata-only in SQLite, so existing rows report the default without a
        rewrite — measured at 0.011s against 2,931,393 rows, versus 7.1s for the
        in-place rewrite this replaced.

        It is reversible. Rolling back to pre-#16 code finds `status` exactly as
        it left it. That matters more than it first appears: flush() batches
        events, transactions, tx_events, flows and get_terminals into a single
        transaction, and TelemetryDB._try_flush sets `_enabled = False`
        permanently on error — so a missing column would abort the whole batch
        and silently stop ALL dashboard ingest while the process looked healthy.

        It loses nothing. Legacy rows keep their real terminal in `status` and
        simply report tx_shape='partial' ("we have not classified this row"),
        which is honest rather than lossy. Deriving tx_shape from `status` at
        read time was considered and rejected: it would make a legacy row answer
        'partial' to direct SQL and 'settled' to the dashboard API, and ad-hoc
        SQL against this database is precisely how the #15 misdiagnosis
        happened. It also avoids promoting per-hop GET verdicts — legacy
        status='success' on a GET came from get_success, a per-hop event — into
        a column documented as an outcome.

        There is deliberately no PRAGMA user_version gate. The previous version
        needed one to keep an expensive rewrite from re-running every restart;
        with nothing expensive left, ADD COLUMN's own IF-absent check is the
        cheaper and more honest guard, and a version counter that no migration
        consults is a claim about ordering we would not be keeping.
        """
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(transactions)")}
        if not cols:
            return  # table absent entirely; executescript above will have made it
        # `status` is listed so it is RESTORED on a database that already ran
        # the superseded renaming migration. CREATE TABLE IF NOT EXISTS cannot
        # bring back a column that was renamed away, and without it a rollback
        # to pre-#16 code fails with "no such column: status" — which
        # _try_flush swallows by setting _enabled = False permanently, so the
        # process keeps running and silently stops persisting everything. The
        # rollback safety net this whole migration exists to provide would be
        # missing on exactly the databases that most need it.
        for name, ddl in (("status", "TEXT DEFAULT 'pending'"),
                          ("tx_shape", "TEXT DEFAULT 'partial'"),
                          ("outcome", "TEXT")):
            if name not in cols:
                self.conn.execute(
                    f"ALTER TABLE transactions ADD COLUMN {name} {ddl}")

    # There is deliberately no ensure_indexes() here. One existed with zero
    # callers, which read as "indexes are created for you" — they are not, and
    # calling it would have taken an exclusive write lock for 30+ minutes per
    # new index on a 128 GB database, stalling live ingest. Indexes are created
    # offline by create_indexes.py while the server is stopped; that script is
    # the only sanctioned path. See the note in ws_server.main().

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
                           start_ns, end_ns, tx_shape, outcome, duration_ms,
                           event_count):
        """Buffer a transaction upsert.

        `tx_shape` is structural ('open'/'settled'/'partial'); `outcome` is the
        measured result and must be None unless tx_shape == 'settled'.
        """
        if not self._enabled:
            return
        self._tx_buf[tx_id] = (
            tx_id, op, contract_key, contract_short,
            start_ns, end_ns, tx_shape, outcome, duration_ms, event_count
        )

    def insert_get_terminal(self, timestamp_ns, tx_id, peer_id, contract_key,
                            outcome, is_sub_op, attempts, hop_count, elapsed_ms):
        """Buffer a client-facing GET outcome for the get_terminals table."""
        if not self._enabled:
            return
        self._getterm_buf.append((
            timestamp_ns, tx_id, peer_id, contract_key, outcome,
            1 if is_sub_op else 0, attempts, hop_count, elapsed_ms,
        ))
        if len(self._getterm_buf) >= self._FLUSH_SIZE:
            self._try_flush()

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
            self._getterm_buf.clear()

    def flush(self):
        """Flush all buffered writes to DB in a single transaction."""
        if (not self._event_buf and not self._tx_buf and not self._txe_buf
                and not self._flow_buf and not self._getterm_buf):
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
                    "(tx_id, op, contract_key, contract_short, start_ns, end_ns, "
                    "tx_shape, outcome, duration_ms, event_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    list(self._tx_buf.values()),
                )
                self._tx_buf.clear()

            if self._getterm_buf:
                self.conn.executemany(
                    "INSERT INTO get_terminals (timestamp_ns, tx_id, peer_id, "
                    "contract_key, outcome, is_sub_op, attempts, hop_count, elapsed_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    self._getterm_buf,
                )
                self._getterm_buf.clear()

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
        Returns up to `limit` events spread across all available history,
        ensuring good timeline coverage rather than just the last few seconds."""
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

        # Time-grid seek sampling (two-phase for the large events table):
        # Phase 1: for each of `limit` evenly-spaced target timestamps, index-seek
        # to the nearest event and collect rowids (index-only scan, no data column).
        # Phase 2: batch-fetch full rows by sorted primary key for sequential I/O.
        # Previous bucketing (LIMIT N per bucket) clustered events at bucket starts.
        range_ns = end_ns - start_ns
        step_ns = max(1, range_ns // limit)
        rowids = []
        seen = set()
        for i in range(limit):
            target_ns = start_ns + i * step_ns + step_ns // 2
            row = self.conn.execute(
                "SELECT id FROM events WHERE timestamp_ns >= ? "
                "ORDER BY timestamp_ns LIMIT 1",
                (target_ns,),
            ).fetchone()
            if row and row[0] not in seen:
                rowids.append(row[0])
                seen.add(row[0])

        if not rowids:
            return []

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
                f"t.end_ns, t.tx_shape, t.duration_ms, t.event_count, t.outcome "
                f"FROM transactions t WHERE t.op IN ({placeholders}) "
                f"ORDER BY t.start_ns DESC LIMIT ?",
                (*ops, limit),
            )
        else:
            cur = self.conn.execute(
                "SELECT tx_id, op, contract_key, contract_short, start_ns, end_ns, "
                "tx_shape, duration_ms, event_count, outcome "
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
                "tx_shape": row[6],
                "outcome": row[9],
                "event_count": len(events),
                "events": events,
            })
        return result

    # `attempts` classification, shared by the SQL below and by ws_server's live
    # path so a restart cannot reclassify history. See ROUTE_CLASS_SQL.
    ROUTE_LOCAL = "local"      # attempts == 0: answered from the local store
    ROUTE_NETWORK = "network"  # attempts >= 1: at least one GET was sent
    ROUTE_UNKNOWN = "unknown"  # attempts absent: which one is unmeasured

    # attempts is the core's own split (GetEvent::ClientTerminal): "`1` means the
    # first peer answered; `0` is the convention for a LOCAL-cache hit that never
    # routed to the network ... letting analysts split 'all client GET successes'
    # (attempts >= 0) from 'network GET findability' (attempts >= 1)".
    #
    # WHY NOT hop_count. freenet-core itself stopped splitting on `attempts` in
    # #4852 P2: `summarize_client_get_outcomes` (core `tracing.rs`) now splits on
    # `hop_count >= 1`, because a loopback `LocalCompletion` bumps `requests_sent`
    # (so attempts >= 1) with no network round-trip and would be over-counted as a
    # network success. The doc comment quoted above predates that and was never
    # updated, so it still recommends the split core abandoned. Both facts are
    # real. We still use `attempts` here, for two measured reasons:
    #
    #   1. hop_count is not populated in this telemetry. Over ~24h of direct
    #      GETs: 224,736 NULL, 242 zero, 2 non-zero. Splitting on `hop_count >= 1`
    #      would report 2 network GETs out of 225k. It is unusable as a splitter,
    #      whatever its semantics.
    #   2. The loopback contamination that motivated core's switch is absent from
    #      this data, and latency proves it. A LocalCompletion has no network
    #      round-trip, so it must be ~0 ms. The populations separate with an EMPTY
    #      band between them: attempts=0 tops out at 15 ms (213,461 of 213,744
    #      under 1 ms), and attempts>=1 successes start at 60 ms — zero rows below
    #      50 ms, zero rows in between. If loopbacks were landing in attempts>=1
    #      they would show up as ~0 ms successes there. There are none.
    #
    # That same separation also rules out a version-skew reading: a release that
    # emitted attempts=0 for a GET that really routed would appear as a slow
    # attempts=0 row, and there are none.
    #
    # If hop_count ever starts being populated, prefer it — it is the more direct
    # signal and core's reasoning is sound. Re-check the latency separation first.
    ROUTE_CLASS_SQL = (
        "CASE WHEN attempts IS NULL THEN 'unknown' "
        "WHEN attempts = 0 THEN 'local' ELSE 'network' END"
    )

    @staticmethod
    def route_class(attempts):
        """Classify one terminal's `attempts` the way ROUTE_CLASS_SQL does.

        The live counter path and the post-restart rebuild MUST agree, so both
        go through this rather than each writing its own comparison.
        """
        if attempts is None:
            return TelemetryDB.ROUTE_UNKNOWN
        return TelemetryDB.ROUTE_LOCAL if attempts == 0 else TelemetryDB.ROUTE_NETWORK

    def get_terminal_buckets(self, since_ns, bucket_ns, until_ns=None):
        """Aggregate client-facing GET outcomes into time buckets.

        Returns rows of
        (bucket_ts, outcome, is_sub_op, route_class, count, mean elapsed_ms).

        `route_class` splits local-store hits from network-routed GETs and is
        NOT optional detail: a local hit never leaves the machine and therefore
        cannot fail, so blending the two produces a success rate whose
        denominator is ~95% cases with no failure mode. Through 2026-08-08 that
        blend read ~95% while network-routed GET success was 8.5%.

        The mean elapsed_ms is not currently consumed — get_terminal reports
        elapsed_ms=0 on ~99.5% of successful direct GETs, so latency comes from
        the get_request -> get_success delta instead.
        Reads the small get_terminals projection rather than the events table,
        which has no event_type index and is hundreds of GB.

        `until_ns`, when given, excludes rows with a bogus future timestamp
        (e.g. sim/CI telemetry hitting this same prod endpoint) — one such
        row is enough to stretch the metrics chart's time axis across months.
        """
        select = (
            f"SELECT (timestamp_ns / {int(bucket_ns)}) * {int(bucket_ns)} AS bucket, "
            f"outcome, is_sub_op, {self.ROUTE_CLASS_SQL} AS route_class, "
            "COUNT(*), AVG(elapsed_ms) FROM get_terminals "
        )
        group = " GROUP BY bucket, outcome, is_sub_op, route_class ORDER BY bucket"
        if until_ns is None:
            return self.conn.execute(
                select + "WHERE timestamp_ns > ?" + group, (since_ns,),
            ).fetchall()
        return self.conn.execute(
            select + "WHERE timestamp_ns > ? AND timestamp_ns <= ?" + group,
            (since_ns, until_ns),
        ).fetchall()

    def get_events_for_range(self, start_ns, end_ns, contract_key=None, peer_id=None):
        """Get events for particle animation, with per-type budgets.
        Returns a mix of 'hop' particles (peer-to-peer travel) and 'pulse'
        particles (single-peer glow). Uses tx_events for hop reconstruction."""
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

        # Collect events from tx_events with per-type budgets
        # Format: (timestamp_ns, event_type, peer_id, tx_id, from_peer_or_None, to_peer_or_None)
        all_events = []

        for group_name, group in TYPE_BUDGETS.items():
            # Skip connect when filtering by contract — connect events don't have contract keys
            if group_name == 'connect' and contract_key:
                continue

            types = group['types']
            budget = group['limit']
            ph = ",".join("?" * len(types))

            # For broadcast events, query events table to get from_peer/to_peer from data JSON
            use_events_table = (group_name == 'broadcast')
            # When filtering by contract/peer, skip bucketing — result set is naturally
            # small, so one query per event-type group is faster than 50 bucketed queries
            # (which each scan the type-ts index even for buckets with no matches).
            filtered = bool(contract_key or peer_id)

            if use_events_table:
                # Query events table with data JSON for broadcast src/dest
                base_sql = (f"SELECT timestamp_ns, event_type, peer_id, tx_id, data "
                            f"FROM events WHERE event_type IN ({ph}) "
                            f"AND timestamp_ns BETWEEN ? AND ?")
                base_params = list(types) + [start_ns, end_ns]
                if contract_key:
                    base_sql += " AND contract_key = ?"
                    base_params.append(contract_key)
                elif peer_id:
                    base_sql += " AND peer_id = ?"
                    base_params.append(peer_id)

                def _append_broadcast_row(row):
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
                    all_events.append((row[0], row[1], row[2], row[3], from_peer, to_peer))

                def _collect_broadcast(cur):
                    for row in cur.fetchall():
                        _append_broadcast_row(row)

                if filtered:
                    # Fetch up to 4x budget then sample uniformly across time
                    # so particles span the full range instead of clustering.
                    hard_cap = budget * 4
                    sql = base_sql + f" ORDER BY timestamp_ns LIMIT {hard_cap}"
                    rows = self.conn.execute(sql, base_params).fetchall()
                    if len(rows) > budget:
                        step = len(rows) / budget
                        rows = [rows[int(i * step)] for i in range(budget)]
                    for row in rows:
                        _append_broadcast_row(row)
                else:
                    # Time-grid seek sampling: one index-seek per target timestamp.
                    # Gives truly uniform distribution (no bucket-start clustering).
                    seek_sql = (f"SELECT timestamp_ns, event_type, peer_id, tx_id, data "
                                f"FROM events WHERE event_type IN ({ph}) "
                                f"AND timestamp_ns >= ? ORDER BY timestamp_ns LIMIT 1")
                    step_ns = max(1, range_ns // budget)
                    seen_keys = set()
                    for i in range(budget):
                        target = start_ns + i * step_ns + step_ns // 2
                        row = self.conn.execute(
                            seek_sql, (*types, target)
                        ).fetchone()
                        if not row or row[0] > end_ns:
                            continue
                        key = (row[0], row[2])
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)
                        _append_broadcast_row(row)
                continue

            if filtered:
                # Single query per event-type group over the full range.
                # Fetch with a hard cap (4x budget) then sample uniformly across
                # time so particles are distributed across the timeline instead
                # of clustering at the start. The indexes make the full fetch
                # cheap even when we don't need all of it.
                hard_cap = budget * 4
                if contract_key:
                    sql = (f"SELECT te.timestamp_ns, te.event_type, te.peer_id, te.tx_id "
                           f"FROM tx_events te JOIN transactions t ON te.tx_id = t.tx_id "
                           f"WHERE te.event_type IN ({ph}) AND te.timestamp_ns BETWEEN ? AND ? "
                           f"AND t.contract_key = ? ORDER BY te.timestamp_ns LIMIT {hard_cap}")
                    params = list(types) + [start_ns, end_ns, contract_key]
                else:  # peer_id
                    sql = (f"SELECT timestamp_ns, event_type, peer_id, tx_id "
                           f"FROM tx_events WHERE event_type IN ({ph}) "
                           f"AND timestamp_ns BETWEEN ? AND ? AND peer_id = ? "
                           f"ORDER BY timestamp_ns LIMIT {hard_cap}")
                    params = list(types) + [start_ns, end_ns, peer_id]
                rows = self.conn.execute(sql, params).fetchall()
                # Uniform sample: if we got more than budget rows, pick every
                # Nth row so the remaining events span the full time range.
                if len(rows) > budget:
                    step = len(rows) / budget
                    rows = [rows[int(i * step)] for i in range(budget)]
                for row in rows:
                    all_events.append((row[0], row[1], row[2], row[3], None, None))
                continue

            # Unfiltered: time-grid seek sampling for uniform distribution.
            # One index-seek per target timestamp using idx_txe_type_ts.
            seek_sql = (f"SELECT timestamp_ns, event_type, peer_id, tx_id "
                        f"FROM tx_events WHERE event_type IN ({ph}) "
                        f"AND timestamp_ns >= ? ORDER BY timestamp_ns LIMIT 1")
            step_ns = max(1, range_ns // budget)
            seen_keys = set()
            for i in range(budget):
                target = start_ns + i * step_ns + step_ns // 2
                row = self.conn.execute(seek_sql, (*types, target)).fetchone()
                if not row or row[0] > end_ns:
                    continue
                key = (row[0], row[2])
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                all_events.append((row[0], row[1], row[2], row[3], None, None))

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

    # ---- Network checks (freenet-core #4665) ----
    #
    # Unbuffered: a few rows a night, so batching would only add a window in
    # which a restart loses a night's verdict.

    def insert_check_run(self, body):
        if not self._enabled:
            return
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO check_runs (run_id, scenario, vantage, "
                "timestamp_ns, duration_ms, verdict, ops_total, ops_failed, "
                "software_version, conditions) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    body.get("run_id"),
                    body.get("scenario", "unknown"),
                    body.get("vantage", "unknown"),
                    int(body.get("timestamp") or 0),
                    body.get("duration_ms"),
                    body.get("verdict", "unknown"),
                    body.get("ops_total"),
                    body.get("ops_failed"),
                    body.get("software_version"),
                    orjson.dumps(body.get("conditions") or {}).decode(),
                ),
            )
        except Exception as e:
            print(f"[db] insert_check_run error: {e}")

    def insert_check_op(self, body):
        if not self._enabled:
            return
        try:
            self.conn.execute(
                "INSERT INTO check_ops (run_id, scenario, vantage, timestamp_ns, op, "
                "dimension, dimension_secs, contract_key, ok, latency_ms, bytes, "
                "error, extra) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    body.get("run_id"),
                    body.get("scenario", "unknown"),
                    body.get("vantage", "unknown"),
                    int(body.get("timestamp") or 0),
                    body.get("op", "unknown"),
                    body.get("dimension"),
                    body.get("dimension_secs"),
                    body.get("contract_key"),
                    1 if body.get("ok") else 0,
                    body.get("latency_ms"),
                    body.get("bytes"),
                    body.get("error"),
                    orjson.dumps(body.get("extra")).decode() if body.get("extra") else None,
                ),
            )
        except Exception as e:
            print(f"[db] insert_check_op error: {e}")

    def get_check_state(self, run_limit=60):
        """Everything the checks panel needs, in one round trip.

        Scenarios and dimensions are discovered from the data, never enumerated:
        a new scenario must light up the panel with no dashboard change.
        """
        if not self._enabled or not self.conn:
            return {"runs": [], "ops": [], "scenarios": []}
        try:
            run_cols = ("run_id", "scenario", "vantage", "timestamp_ns", "duration_ms",
                        "verdict", "ops_total", "ops_failed", "software_version")
            runs = [
                dict(zip(run_cols, r))
                for r in self.conn.execute(
                    f"SELECT {', '.join(run_cols)} FROM check_runs "
                    "ORDER BY timestamp_ns DESC LIMIT ?", (run_limit,)
                )
            ]
            op_cols = ("run_id", "scenario", "vantage", "timestamp_ns", "op", "dimension",
                       "dimension_secs", "contract_key", "ok", "latency_ms", "bytes", "error")
            if runs:
                # Match on (scenario, run_id): run ids repeat across scenarios.
                pairs = [(r["scenario"], r["run_id"]) for r in runs]
                clause = " OR ".join(["(scenario = ? AND run_id = ?)"] * len(pairs))
                params = [v for pair in pairs for v in pair]
                ops = [
                    dict(zip(op_cols, r))
                    for r in self.conn.execute(
                        f"SELECT {', '.join(op_cols)} FROM check_ops "
                        f"WHERE {clause} ORDER BY timestamp_ns", params
                    )
                ]
            else:
                ops = []
            scenarios = [r[0] for r in self.conn.execute(
                "SELECT DISTINCT scenario FROM check_runs ORDER BY scenario")]
            return {"runs": runs, "ops": ops, "scenarios": scenarios}
        except Exception as e:
            print(f"[db] get_check_state error: {e}")
            return {"runs": [], "ops": [], "scenarios": []}

    # ---- Maintenance ----

    def prune(self, retention_ns=DEFAULT_RETENTION_NS,
              time_budget_s=PRUNE_TIME_BUDGET_S):
        """Remove data older than the retention period, in bounded batches.

        Returns the number of rows deleted. Each batch is its own transaction,
        and the total work is capped by ``time_budget_s`` — whatever is left
        over is picked up by the next call. A large backlog (after cutting
        retention, or after an outage) therefore drains over many cycles
        instead of stalling ingest inside one multi-GB DELETE.
        """
        if not self._enabled:
            return 0
        cutoff = int(time.time() * 1_000_000_000) - retention_ns
        deadline = time.monotonic() + time_budget_s
        total = 0
        hit_budget = False
        try:
            # check_runs/check_ops are deliberately absent: their retention is
            # measured in weeks, not hours (see SCHEMA_TABLES).
            while True:
                if time.monotonic() >= deadline:
                    hit_budget = True
                    break
                deleted = 0
                self.conn.execute("BEGIN")
                try:
                    # events and flows carry an index on timestamp_ns, so their
                    # subquery seeks straight to the old rows and the outer
                    # DELETE removes them by primary key. get_terminals has no
                    # such index (see SCHEMA_INDEXES) so its probe scans; that
                    # is affordable only while retention keeps the table small,
                    # and it is the cost that grows if GET volume rises.
                    for table in ("events", "flows", "get_terminals"):
                        cur = self.conn.execute(
                            f"DELETE FROM {table} WHERE id IN "
                            f"(SELECT id FROM {table} WHERE timestamp_ns < ? LIMIT ?)",
                            (cutoff, PRUNE_ROW_BATCH),
                        )
                        deleted += cur.rowcount
                    # tx_events has no index on timestamp_ns alone (only
                    # composites led by event_type/peer_id), so pruning it by
                    # its own timestamp would table-scan. Select the expired
                    # transactions via idx_tx_start instead and delete their
                    # tx_events by tx_id via idx_txe_txid.
                    tx_ids = [
                        row[0]
                        for row in self.conn.execute(
                            "SELECT tx_id FROM transactions WHERE start_ns < ? LIMIT ?",
                            (cutoff, PRUNE_TX_BATCH),
                        )
                    ]
                    if tx_ids:
                        placeholders = ",".join("?" * len(tx_ids))
                        cur = self.conn.execute(
                            f"DELETE FROM tx_events WHERE tx_id IN ({placeholders})",
                            tx_ids,
                        )
                        deleted += cur.rowcount
                        cur = self.conn.execute(
                            f"DELETE FROM transactions WHERE tx_id IN ({placeholders})",
                            tx_ids,
                        )
                        deleted += cur.rowcount
                    self.conn.execute("COMMIT")
                except Exception:
                    try:
                        self.conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise
                total += deleted
                if deleted == 0:
                    break  # nothing older than the cutoff remains
            # Checkpoint and truncate the WAL so the pages freed by the DELETEs
            # above are reclaimed and the WAL file cannot grow without bound.
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception as e:
            print(f"[db] prune error: {e}")
        if hit_budget:
            print(f"[db] prune: deleted {total:,} rows, backlog remains "
                  f"(hit {time_budget_s:.0f}s budget)", flush=True)
        return total

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
