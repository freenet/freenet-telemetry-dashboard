#!/usr/bin/env python3
"""
Fast bulk import of telemetry JSONL into SQLite.

WARNING: This script DELETES and recreates the database from scratch.
Only use it for initial setup or disaster recovery. Normal operations
should never need this — the ws_server writes events to SQLite in
real-time and resumes from stored offset on restart.

Usage: ./venv/bin/python3 backfill_db.py
"""

import re
import sqlite3
import sys
import time
from pathlib import Path

import orjson

import ws_server
from telemetry_db import SCHEMA_TABLES

TELEMETRY_LOG = Path("/mnt/media/freenet-telemetry/logs.jsonl")
DB_PATH = "/var/www/freenet-dashboard/telemetry.db"

# Only keep events from the last N hours
HISTORY_HOURS = 48

# Event types worth storing (matches HISTORY_EVENT_TYPES in ws_server.py)
HISTORY_EVENT_TYPES = {
    "put_request", "put_success",
    "get_request", "get_success", "get_not_found", "get_failure",
    "get_terminal",
    "update_request", "update_success", "update_failure",
    "subscribe_request", "subscribe_success", "subscribe_not_found",
    "subscribe_timeout",
    "update_broadcast_received", "update_broadcast_applied",
    "update_broadcast_emitted", "broadcast_emitted",
    "update_broadcast_delivery_summary",
    "connect_connected", "connect_rejected",
    "peer_startup", "peer_shutdown",
    "seeding_started", "seeding_stopped",
    "subscribed",
}

TRACKED_TX_OPS = {"put", "get", "update", "broadcast", "connect", "subscribe"}

# Imported rather than copied. This script previously kept its own transcription
# of the classification tables and drifted from the server twice: it keyed
# subscribes off `subscribed` (an event the core never emits) and recorded
# get_not_found as "success". Sharing the definitions makes that class of bug
# unrepresentable. get_success/get_not_found are per-HOP and so are not
# terminals; get_terminal is the client-facing GET outcome and is handled
# inline, because its result lives in the event body.
TX_START_EVENTS = set(ws_server.TX_START_EVENTS)
TX_TERMINAL_EVENTS = {k: v[1] for k, v in ws_server.TX_TERMINAL_EVENTS.items()}
outcome_wins = ws_server.outcome_wins

PEER_PATTERN = re.compile(r'(\w+)@(\d+\.\d+\.\d+\.\d+):(\d+)\s*\(@\s*([\d.]+)\)')


# Reused rather than duplicated so a backfill rebuild produces the SAME
# peer-IDs as live ws_server output — a local copy would drift the moment
# ws_server's salted hashing changed (see PEER_ID_SALT in ws_server.py).
anonymize_ip = ws_server.anonymize_ip
ip_hash = ws_server.ip_hash


def is_public_ip(ip):
    parts = ip.split(".")
    if len(parts) != 4:
        return False
    first = int(parts[0])
    second = int(parts[1])
    if first == 10:
        return False
    if first == 172 and 16 <= second <= 31:
        return False
    if first == 192 and second == 168:
        return False
    if first == 127:
        return False
    return True


def parse_peer_string(peer_str):
    if not peer_str:
        return None, None, None
    match = PEER_PATTERN.search(peer_str)
    if match:
        return match.group(1), match.group(2), float(match.group(4))
    return None, None, None


def classify_op(event_type):
    if event_type.startswith("put_"):
        return "put"
    if event_type.startswith("get_"):
        return "get"
    if event_type.startswith("update_"):
        return "update"
    if event_type.startswith("subscribe") or event_type == "subscribed":
        return "subscribe"
    if event_type.startswith("connect"):
        return "connect"
    if "broadcast" in event_type:
        return "broadcast"
    return None


def main():
    if not TELEMETRY_LOG.exists():
        print(f"Telemetry log not found: {TELEMETRY_LOG}")
        sys.exit(1)

    file_size = TELEMETRY_LOG.stat().st_size
    print(f"Backfilling from {TELEMETRY_LOG} ({file_size / 1e9:.1f} GB)")

    now_ns = int(time.time() * 1_000_000_000)
    cutoff = now_ns - HISTORY_HOURS * 60 * 60 * 1_000_000_000
    print(f"Keeping events from last {HISTORY_HOURS} hours (cutoff: {time.ctime(cutoff / 1e9)})")

    # Delete existing DB and start fresh
    db_path = Path(DB_PATH)
    if db_path.exists():
        db_path.unlink()
        # Also remove WAL/SHM files
        for ext in ("-wal", "-shm"):
            p = Path(DB_PATH + ext)
            if p.exists():
                p.unlink()

    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")  # Fast bulk import, not crash-safe
    conn.execute("PRAGMA cache_size=-256000")  # 256MB cache
    conn.execute("PRAGMA temp_store=MEMORY")

    # Tables come from the server's own schema rather than a transcription of
    # it. The previous hand-copy silently omitted get_terminals, which is where
    # GET success rates are sourced from — so a database rebuilt by this script
    # produced a Performance chart with PUT and UPDATE restored and both GET
    # lines blank, the exact metric this work exists to fix. SCHEMA_TABLES
    # carries no indexes; those are still added after the bulk insert.
    conn.executescript(SCHEMA_TABLES)

    event_buf = []
    getterm_buf = []
    tx_data = {}  # tx_id -> {op, contract, start_ns, end_ns, tx_shape, outcome, events}
    count = 0
    stored = 0
    skipped = 0
    BATCH = 5000

    start_time = time.time()

    with open(TELEMETRY_LOG, "r") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                batch = orjson.loads(line)
            except Exception:
                continue

            for rl in batch.get("resourceLogs", []):
                for sl in rl.get("scopeLogs", []):
                    for record in sl.get("logRecords", []):
                        count += 1

                        ts_raw = record.get("timeUnixNano", "0")
                        ts = int(ts_raw) if isinstance(ts_raw, str) else ts_raw

                        if ts < cutoff:
                            skipped += 1
                            continue

                        attrs = {a["key"]: a["value"].get("stringValue") or a["value"].get("doubleValue")
                                 for a in record.get("attributes", [])}
                        body_str = record.get("body", {}).get("stringValue", "")
                        body = {}
                        if body_str:
                            try:
                                body = orjson.loads(body_str)
                            except Exception:
                                body = {}

                        # Handle kvlistValue bodies
                        if not body:
                            kv = record.get("body", {}).get("kvlistValue", {}).get("values", [])
                            if kv:
                                body = {}
                                for item in kv:
                                    k = item.get("key", "")
                                    v = item.get("value", {})
                                    body[k] = v.get("stringValue") or v.get("doubleValue") or v.get("intValue")

                        event_type = attrs.get("event_type") or body.get("type", "")
                        if not event_type:
                            continue

                        # Skip non-history event types
                        body_type = body.get("type", "")
                        display_type = body_type if (event_type == "connect" and body_type) else event_type
                        if display_type not in HISTORY_EVENT_TYPES:
                            continue

                        # Extract peer info
                        _, this_ip, this_loc = parse_peer_string(body.get("this_peer", ""))
                        other_ip = None
                        for field in ["connected_peer", "target", "requester", "subscriber", "upstream"]:
                            if field in body:
                                _, other_ip, _ = parse_peer_string(body[field])
                                if other_ip:
                                    break

                        # Determine display peer
                        display_ip = this_ip or other_ip
                        if not display_ip:
                            # Try addr fields
                            for af in ["this_peer_addr", "from_peer_addr", "peer_addr"]:
                                addr = body.get(af, "")
                                if addr and ":" in addr:
                                    ip = addr.split(":")[0]
                                    if is_public_ip(ip):
                                        display_ip = ip
                                        break

                        if not display_ip:
                            continue

                        peer_id = anonymize_ip(display_ip)
                        tx_id = body.get("id") or attrs.get("transaction_id") or None
                        if tx_id == "00000000000000000000000000":
                            tx_id = None
                        contract_key = body.get("key") or body.get("contract_key") or None

                        # Build event dict
                        event = {
                            "type": "event",
                            "timestamp": ts,
                            "event_type": display_type,
                            "peer_id": peer_id,
                            "peer_ip_hash": ip_hash(display_ip),
                        }
                        if this_ip and is_public_ip(this_ip):
                            event["from_peer"] = anonymize_ip(this_ip)
                        if other_ip and is_public_ip(other_ip):
                            event["to_peer"] = anonymize_ip(other_ip)
                        if contract_key:
                            event["contract"] = contract_key[:12] + "..."
                            event["contract_full"] = contract_key
                        if tx_id:
                            event["tx_id"] = tx_id

                        event_buf.append((
                            ts, display_type, peer_id, tx_id, contract_key,
                            orjson.dumps(event).decode()
                        ))

                        # Track transaction
                        if tx_id:
                            op = classify_op(display_type)
                            if op and op in TRACKED_TX_OPS:
                                if tx_id not in tx_data:
                                    tx_data[tx_id] = {
                                        "op": op, "contract": contract_key,
                                        "start_ns": ts, "end_ns": ts,
                                        "tx_shape": "partial", "outcome": None,
                                        "outcome_src": None,
                                        "events": [],
                                    }
                                tx = tx_data[tx_id]
                                tx["events"].append((ts, display_type, peer_id))
                                if ts < tx["start_ns"]:
                                    tx["start_ns"] = ts
                                if ts > tx["end_ns"]:
                                    tx["end_ns"] = ts
                                # Shape/outcome must match ws_server.py. The old
                                # rule here marked get_not_found as "success"
                                # and keyed subscribes off the never-emitted
                                # `subscribed` (issue #15).
                                if display_type in TX_START_EVENTS:
                                    if tx["tx_shape"] == "partial":
                                        tx["tx_shape"] = "open"
                                else:
                                    if display_type in TX_TERMINAL_EVENTS:
                                        outcome = TX_TERMINAL_EVENTS[display_type]
                                    elif display_type == "get_terminal":
                                        outcome = body.get("outcome")
                                    else:
                                        outcome = None
                                    if outcome:
                                        tx["tx_shape"] = "settled"
                                        # Same precedence as the server: several
                                        # hop-observed terminals can land on one
                                        # transaction and can contradict.
                                        if outcome_wins(outcome, display_type,
                                                        tx["outcome"], tx["outcome_src"]):
                                            tx["outcome"] = outcome
                                            tx["outcome_src"] = display_type

                        # Project the client-facing GET outcome, the source the
                        # server computes GET success rates from.
                        if display_type == "get_terminal" and body.get("outcome"):
                            getterm_buf.append((
                                ts, tx_id, peer_id, contract_key,
                                body["outcome"], 1 if body.get("is_sub_op") else 0,
                                body.get("attempts"), body.get("hop_count"),
                                body.get("elapsed_ms"),
                            ))

                        stored += 1

                        # Batch insert events
                        if len(event_buf) >= BATCH:
                            conn.execute("BEGIN")
                            conn.executemany(
                                "INSERT INTO events (timestamp_ns, event_type, peer_id, tx_id, contract_key, data) "
                                "VALUES (?, ?, ?, ?, ?, ?)",
                                event_buf,
                            )
                            conn.execute("COMMIT")
                            event_buf.clear()
                            elapsed = time.time() - start_time
                            rate = count / elapsed if elapsed > 0 else 0
                            print(f"\r  {count:,} records ({skipped:,} skipped, {stored:,} stored) "
                                  f"[{rate:,.0f}/sec]", end="", flush=True)

        # Final byte offset
        final_offset = f.tell()

    # Flush remaining events
    if event_buf:
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT INTO events (timestamp_ns, event_type, peer_id, tx_id, contract_key, data) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            event_buf,
        )
        conn.execute("COMMIT")

    if getterm_buf:
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT INTO get_terminals (timestamp_ns, tx_id, peer_id, contract_key, "
            "outcome, is_sub_op, attempts, hop_count, elapsed_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            getterm_buf,
        )
        conn.execute("COMMIT")

    print(f"\n  Events: {stored:,} stored")
    print(f"  GET terminals: {len(getterm_buf):,} stored")

    # Insert transactions and tx_events
    print("  Writing transactions and computing flows...")
    tx_rows = []
    txe_rows = []
    flow_rows = []

    for tx_id, tx in tx_data.items():
        contract_short = tx["contract"][:12] + "..." if tx["contract"] else None
        duration_ms = (tx["end_ns"] - tx["start_ns"]) / 1_000_000 if tx["end_ns"] else None
        tx_rows.append((
            tx_id, tx["op"], tx["contract"], contract_short,
            tx["start_ns"], tx["end_ns"], tx["tx_shape"], tx["outcome"],
            duration_ms, len(tx["events"])
        ))

        for ts, et, pid in tx["events"]:
            txe_rows.append((tx_id, ts, et, pid))

        # Compute flows (capped per transaction to avoid explosion from broadcasts)
        events = sorted(tx["events"], key=lambda e: e[0])
        if len(events) >= 2:
            peers = set(e[2] for e in events if e[2])
            if len(peers) >= 2:
                tx_flow_count = 0
                for j in range(1, len(events)):
                    if events[j][2] and events[j - 1][2] and events[j][2] != events[j - 1][2]:
                        mid_ts = (events[j - 1][0] + events[j][0]) // 2
                        flow_rows.append((mid_ts, events[j - 1][2], events[j][2], events[j][1], tx_id))
                        tx_flow_count += 1
                        if tx_flow_count >= 5:
                            break

    conn.execute("BEGIN")
    conn.executemany(
        "INSERT OR REPLACE INTO transactions "
        "(tx_id, op, contract_key, contract_short, start_ns, end_ns, tx_shape, "
        "outcome, duration_ms, event_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        tx_rows,
    )
    conn.executemany(
        "INSERT INTO tx_events (tx_id, timestamp_ns, event_type, peer_id) VALUES (?, ?, ?, ?)",
        txe_rows,
    )
    conn.executemany(
        "INSERT INTO flows (timestamp_ns, from_peer, to_peer, event_type, tx_id) VALUES (?, ?, ?, ?, ?)",
        flow_rows,
    )
    conn.execute("COMMIT")

    print(f"  Transactions: {len(tx_rows):,}")
    print(f"  Tx events: {len(txe_rows):,}")
    print(f"  Flows: {len(flow_rows):,}")

    # Create indexes
    print("  Creating indexes...")
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp_ns);
        CREATE INDEX IF NOT EXISTS idx_events_tx ON events(tx_id) WHERE tx_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_tx_start ON transactions(start_ns);
        CREATE INDEX IF NOT EXISTS idx_txe_txid ON tx_events(tx_id);
        CREATE INDEX IF NOT EXISTS idx_flows_ts ON flows(timestamp_ns);
        CREATE INDEX IF NOT EXISTS idx_flows_tx ON flows(tx_id) WHERE tx_id IS NOT NULL;
    """)

    # Store offset
    conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)", ("ingest_offset", str(final_offset)))
    conn.execute("PRAGMA synchronous=NORMAL")  # Restore safe mode
    conn.execute("PRAGMA optimize")
    conn.close()

    db_size = Path(DB_PATH).stat().st_size
    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f}s. DB size: {db_size / 1e6:.1f} MB")
    print(f"Total records: {count:,}, skipped: {skipped:,}, stored: {stored:,}")


if __name__ == "__main__":
    main()
