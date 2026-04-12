#!/usr/bin/env python3
"""Create all DB indexes defined in SCHEMA_INDEXES.

Run this OFFLINE (while the ws_server is stopped) because CREATE INDEX
acquires an exclusive write lock on SQLite, which would block live
telemetry ingestion. On a 128GB DB, new index creation can take 30+
minutes per index.

Usage:
    sudo systemctl stop freenet-dashboard-ws
    cd /var/www/freenet-dashboard
    sudo ./venv/bin/python3 create_indexes.py
    sudo systemctl start freenet-dashboard-ws
"""
import sys
import time
import sqlite3

from telemetry_db import DEFAULT_DB_PATH, SCHEMA_INDEXES


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    print(f"Opening {db_path}")
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-1048576")  # 1GB cache for index build
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA busy_timeout=60000")

    # Show existing indexes
    existing = set()
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall():
        existing.add(row[0])
    print(f"Existing indexes: {sorted(existing)}")
    print()

    for stmt in SCHEMA_INDEXES:
        # Parse out the index name for logging
        idx_name = stmt.split("IF NOT EXISTS ")[1].split(" ON ")[0]
        if idx_name in existing:
            print(f"[skip] {idx_name} already exists")
            continue
        print(f"[create] {idx_name} ... ", end="", flush=True)
        t0 = time.time()
        try:
            conn.execute(stmt)
            dt = time.time() - t0
            print(f"done in {dt:.1f}s")
        except Exception as e:
            dt = time.time() - t0
            print(f"FAILED after {dt:.1f}s: {e}")
            return 1

    print()
    print("Running ANALYZE to update query planner stats...")
    t0 = time.time()
    conn.execute("ANALYZE")
    print(f"ANALYZE done in {time.time() - t0:.1f}s")
    conn.close()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
