#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb"]
# ///
"""Migrate JSON cache files to a single DuckDB database.

Reads data/cache/*.json (the old hex+stringified-json format) and inserts
into data/cache.duckdb with one row per aircraft-day.
"""

import json
import sys
from pathlib import Path

import duckdb

CACHE_DIR = Path("data/cache")
DB_PATH = Path("data/cache.duckdb")


def main():
    json_files = sorted(CACHE_DIR.glob("????-??-??.json"))
    if not json_files:
        print("No JSON cache files found.")
        return

    print(f"Found {len(json_files)} cache files to migrate")

    db = duckdb.connect(str(DB_PATH))
    db.execute("""
        CREATE TABLE IF NOT EXISTS traces (
            date DATE,
            icao_hex VARCHAR,
            registration VARCHAR,
            icao_type VARCHAR,
            description VARCHAR,
            timestamp DOUBLE,
            trace JSON
        )
    """)

    existing = set(
        r[0].strftime("%Y-%m-%d")
        for r in db.execute("SELECT DISTINCT date FROM traces").fetchall()
    )

    total_rows = 0
    for f in json_files:
        day = f.stem
        if day in existing:
            print(f"  {day}: already migrated, skipping")
            continue

        data = json.loads(f.read_text())
        if not data:
            print(f"  {day}: empty")
            continue

        rows = []
        for hex_code, raw_json_str in data:
            parsed = json.loads(raw_json_str)
            trace = parsed.get("trace", [])
            rows.append((
                day,
                hex_code,
                parsed.get("r", ""),
                parsed.get("t", ""),
                parsed.get("desc", ""),
                parsed.get("timestamp"),
                json.dumps(trace),
            ))

        db.executemany(
            "INSERT INTO traces VALUES ($1::DATE, $2, $3, $4, $5, $6, $7::JSON)",
            rows,
        )
        total_rows += len(rows)
        print(f"  {day}: {len(rows)} aircraft")

    db.execute("CHECKPOINT")

    count = db.execute("SELECT count(*) FROM traces").fetchone()[0]
    days = db.execute("SELECT count(DISTINCT date) FROM traces").fetchone()[0]
    db.close()

    size_mb = DB_PATH.stat().st_size / 1024 / 1024
    print(f"\nDone: {count} total rows across {days} days -> {DB_PATH} ({size_mb:.1f} MB)")
    print(f"  ({total_rows} rows inserted this run)")


if __name__ == "__main__":
    main()
