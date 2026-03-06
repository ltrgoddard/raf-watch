#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx", "duckdb"]
# ///
"""Export UK military flight data as GeoParquet for analysis.

Fetches any missing days into the DuckDB cache, then builds a GeoParquet
file with one row per flight-day containing a LineString track and summary
metadata.

Usage:
  uv run scripts/export_flights.py                        # last 6 weeks
  uv run scripts/export_flights.py --weeks 12             # last 12 weeks
  uv run scripts/export_flights.py --from 2026-01-01 --to 2026-03-01
  uv run scripts/export_flights.py --no-fetch             # cache only, skip API
  uv run scripts/export_flights.py --refetch              # re-fetch all days
"""

import sys
sys.stdout.reconfigure(line_buffering=True)

import argparse
import asyncio
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import httpx

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Referer": "https://globe.adsbexchange.com/",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "DNT": "1",
}

DB_PATH = Path("data/cache.duckdb")
OUT_DIR = Path("data/export")
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 10, 30]

# Trace column indices from ADSB Exchange format
T_SECONDS = 0
T_LAT = 1
T_LON = 2
T_ALT_BARO = 3
T_GROUND_SPEED = 4
T_HEADING = 5
T_FLAGS = 6
T_VERT_RATE = 7
T_METADATA = 8


def parse_args():
    p = argparse.ArgumentParser(description="Export flight data as GeoParquet")
    p.add_argument("--weeks", type=int, default=6)
    p.add_argument("--from", dest="from_date", type=str, default=None)
    p.add_argument("--to", dest="to_date", type=str, default=None)
    p.add_argument("--rate", type=float, default=30.0, help="max requests/sec for fetching")
    p.add_argument("--no-fetch", action="store_true", help="only export cached data, don't fetch")
    p.add_argument("--refetch", action="store_true", help="re-fetch all days, ignoring cache")
    return p.parse_args()


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def init_db() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
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
    return db


def cached_dates(db: duckdb.DuckDBPyConnection) -> set[date]:
    return set(
        r[0] for r in db.execute("SELECT DISTINCT date FROM traces").fetchall()
    )


# ── Fetching ─────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, rate: float):
        self.interval = 1.0 / rate
        self.lock = asyncio.Lock()
        self.last = 0.0

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            wait = self.last + self.interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self.last = time.monotonic()


async def fetch_one_with_retry(client, url, rate_limiter):
    for attempt in range(MAX_RETRIES):
        await rate_limiter.acquire()
        try:
            resp = await client.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                if b'"trace"' in resp.content:
                    data = resp.json()
                    if data.get("trace"):
                        return data, None
                return None, None
            elif resp.status_code == 404:
                return None, None
            elif resp.status_code == 429:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(wait)
                    continue
                return None, "429_exhausted"
            else:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])
                    continue
                return None, f"http_{resp.status_code}"
        except httpx.TimeoutException:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])
                continue
            return None, "timeout"
        except httpx.HTTPError as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])
                continue
            return None, f"network_{type(e).__name__}"

    return None, "max_retries"


async def fetch_day(client, hex_codes, target_date, rate_limiter, db):
    """Fetch traces for all hex codes for a single day into DuckDB."""
    date_path = target_date.strftime("%Y/%m/%d")
    results = []
    errors = {}
    sem = asyncio.Semaphore(20)
    done = 0

    async def fetch_one(hex_code):
        nonlocal done
        async with sem:
            last2 = hex_code[-2:]
            url = f"https://globe.adsbexchange.com/globe_history/{date_path}/traces/{last2}/trace_full_{hex_code}.json"
            data, err = await fetch_one_with_retry(client, url, rate_limiter)
            if data is not None:
                results.append((
                    str(target_date),
                    hex_code,
                    data.get("r", ""),
                    data.get("t", ""),
                    data.get("desc", ""),
                    data.get("timestamp"),
                    json.dumps(data["trace"]),
                ))
            if err is not None:
                errors[hex_code] = err
            done += 1
            if done % 500 == 0:
                print(f"    {done}/{len(hex_codes)} checked ({len(results)} found, {len(errors)} errors)")

    tasks = [fetch_one(h) for h in hex_codes]
    await asyncio.gather(*tasks)

    if results:
        db.executemany(
            "INSERT INTO traces VALUES ($1::DATE, $2, $3, $4, $5, $6, $7::JSON)",
            results,
        )

    status = f"{len(results)} aircraft"
    if errors:
        by_type = {}
        for reason in errors.values():
            by_type[reason] = by_type.get(reason, 0) + 1
        err_summary = ", ".join(f"{v}x {k}" for k, v in sorted(by_type.items()))
        status += f", {len(errors)} ERRORS ({err_summary})"
    print(f"  {target_date}: {status}")

    return results, len(errors)


# ── Parsing ──────────────────────────────────────────────────────────

def load_aircraft_meta():
    aircraft_file = Path("data/aircraft.json")
    if not aircraft_file.exists():
        print("Warning: data/aircraft.json not found, metadata will be limited")
        return {}
    aircraft = json.loads(aircraft_file.read_text())
    return {a["hex"]: a for a in aircraft}


def seconds_to_utc(day_date, seconds_offset):
    day_epoch = datetime(day_date.year, day_date.month, day_date.day, tzinfo=timezone.utc)
    return day_epoch + timedelta(seconds=seconds_offset)


def parse_traces_from_db(db, start, end, aircraft_meta):
    """Parse cached traces from DuckDB into rows for GeoParquet export."""
    rows_db = db.execute(
        "SELECT date, icao_hex, trace::VARCHAR FROM traces WHERE date BETWEEN $1 AND $2",
        [start, end],
    ).fetchall()

    rows = []
    for day_date, hex_code, trace_json in rows_db:
        trace = json.loads(trace_json)
        if not trace:
            continue

        meta = aircraft_meta.get(hex_code, {})
        reg = meta.get("reg", "")
        aircraft_type = meta.get("type", "")
        icao_type = meta.get("icao_type", "")
        unit = meta.get("unit", "")

        callsigns = set()
        squawks = set()
        for pt in trace:
            if len(pt) > T_METADATA and isinstance(pt[T_METADATA], dict):
                cs = pt[T_METADATA].get("flight", "").strip()
                if cs:
                    callsigns.add(cs)
                sq = pt[T_METADATA].get("squawk")
                if sq:
                    squawks.add(sq)

        callsign_str = "; ".join(sorted(callsigns))
        squawk_str = "; ".join(sorted(squawks))

        for i, pt in enumerate(trace):
            ts = seconds_to_utc(day_date, pt[T_SECONDS])
            alt = pt[T_ALT_BARO]
            if alt == "ground":
                alt = 0

            rows.append((
                str(day_date),
                hex_code,
                reg,
                aircraft_type,
                icao_type,
                unit,
                callsign_str,
                squawk_str,
                ts.strftime("%Y-%m-%d %H:%M:%S"),
                i,
                pt[T_LON],
                pt[T_LAT],
                alt,
                pt[T_GROUND_SPEED],
                pt[T_HEADING],
                pt[T_VERT_RATE],
            ))

    return rows


# ── GeoParquet export via DuckDB ─────────────────────────────────────

def build_geoparquet(all_rows, out_path):
    import csv
    import os
    import tempfile

    export_db = duckdb.connect()
    export_db.execute("INSTALL spatial; LOAD spatial;")

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="")
    try:
        w = csv.writer(tmp)
        w.writerow([
            "date", "icao_hex", "registration", "aircraft_type", "icao_type",
            "unit", "callsigns", "squawks", "timestamp_utc", "point_idx",
            "lon", "lat", "alt_baro_ft", "speed_kts", "heading", "vert_rate_fpm",
        ])
        w.writerows(all_rows)
        tmp.close()

        export_db.execute(f"""
            CREATE TABLE traces AS
            SELECT * FROM read_csv('{tmp.name}', auto_detect=true)
        """)
    finally:
        os.unlink(tmp.name)

    print(f"  {export_db.execute('SELECT count(*) FROM traces').fetchone()[0]} trace points loaded")

    export_db.execute(f"""
        COPY (
            SELECT
                date,
                icao_hex,
                registration,
                aircraft_type,
                icao_type,
                unit,
                callsigns,
                squawks,
                min(timestamp_utc)  AS first_seen_utc,
                max(timestamp_utc)  AS last_seen_utc,
                round((epoch(max(timestamp_utc)::TIMESTAMP) -
                       epoch(min(timestamp_utc)::TIMESTAMP)) / 60, 1)
                                    AS duration_minutes,
                count(*)            AS trace_points,
                min(alt_baro_ft)    AS min_alt_ft,
                max(alt_baro_ft)    AS max_alt_ft,
                round(max(speed_kts), 1) AS max_speed_kts,
                CASE WHEN count(*) >= 2
                     THEN ST_MakeLine(list(ST_Point(lon, lat) ORDER BY point_idx))
                     ELSE ST_Point(min(lon), min(lat))
                END                 AS geometry
            FROM traces
            WHERE lon IS NOT NULL AND lat IS NOT NULL
            GROUP BY date, icao_hex, registration, aircraft_type,
                     icao_type, unit, callsigns, squawks
            ORDER BY date, icao_hex
        ) TO '{out_path}'
        WITH (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    count = export_db.execute(f"SELECT count(*) FROM '{out_path}'").fetchone()[0]
    export_db.close()
    return count


async def main():
    args = parse_args()

    if args.from_date and args.to_date:
        start = date.fromisoformat(args.from_date)
        end = date.fromisoformat(args.to_date)
    else:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(weeks=args.weeks)

    dates = list(date_range(start, end))
    aircraft_meta = load_aircraft_meta()
    hex_codes = list(aircraft_meta.keys())

    db = init_db()

    # Fetch
    if not args.no_fetch:
        already = cached_dates(db)

        if args.refetch:
            # Delete existing rows for these dates so we can re-fetch
            for d in dates:
                db.execute("DELETE FROM traces WHERE date = $1", [d])
            to_fetch = list(reversed(dates))
        else:
            to_fetch = [d for d in reversed(dates) if d not in already]

        if to_fetch:
            print(f"Fetching {len(to_fetch)} days (newest first: {to_fetch[0]} -> {to_fetch[-1]}), {len(hex_codes)} hex codes")
            print(f"Rate limit: {args.rate} req/s, retries: {MAX_RETRIES} per request\n")
            rate_limiter = RateLimiter(args.rate)
            total_errors = 0
            async with httpx.AsyncClient() as client:
                for d in to_fetch:
                    _, errs = await fetch_day(client, hex_codes, d, rate_limiter, db)
                    total_errors += errs

            if total_errors > 0:
                print(f"\n  {total_errors} total fetch errors. Re-run with --refetch to retry.\n")
        else:
            print(f"All {len(dates)} days cached.\n")

    # Parse all cached days into trace rows
    print(f"Parsing {len(dates)} days ({start} to {end})...")
    all_rows = parse_traces_from_db(db, start, end, aircraft_meta)
    db.close()

    if not all_rows:
        print("No data found.")
        return

    # Build GeoParquet
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"flights_{start}_{end}.parquet"

    print(f"Building GeoParquet...")
    count = build_geoparquet(all_rows, str(out_path))

    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"\n  {out_path}")
    print(f"  {count} flight tracks, {size_mb:.1f} MB")


asyncio.run(main())
