#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx", "duckdb"]
# ///
"""Fetch flight traces from globe.adsbexchange.com for UK military aircraft.

Caches results in data/cache.duckdb (one row per aircraft-day). Outputs
per-day .bin files and a manifest.json for the frontend.

Usage:
  uv run scripts/fetch_adsb.py               # fetch last 14 days
  uv run scripts/fetch_adsb.py --days 7      # fetch last 7 days
  uv run scripts/fetch_adsb.py --from 2026-02-20 --to 2026-03-02
"""

import sys
sys.stdout.reconfigure(line_buffering=True)

import argparse
import asyncio
import json
import struct
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


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--from", dest="from_date", type=str, default=None)
    p.add_argument("--to", dest="to_date", type=str, default=None)
    p.add_argument("--keep-days", type=int, default=14, help="days of .bin files to keep")
    p.add_argument("--rate", type=float, default=10.0, help="max requests/sec")
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
    db.execute("""
        CREATE TABLE IF NOT EXISTS fetched_dates (
            date DATE PRIMARY KEY
        )
    """)
    return db


def cached_dates(db: duckdb.DuckDBPyConnection) -> set[date]:
    return set(
        r[0] for r in db.execute("SELECT DISTINCT date FROM fetched_dates").fetchall()
    )


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


async def fetch_day(
    client: httpx.AsyncClient,
    hex_codes: list[str],
    target_date: date,
    rate_limiter: RateLimiter,
    db: duckdb.DuckDBPyConnection,
) -> int:
    """Fetch traces for all hex codes for a single day. Returns count of aircraft found."""

    date_path = target_date.strftime("%Y/%m/%d")
    results: list[tuple] = []
    sem = asyncio.Semaphore(15)
    done = 0
    errors_429 = 0

    async def fetch_one(hex_code: str):
        nonlocal done, errors_429
        async with sem:
            await rate_limiter.acquire()
            last2 = hex_code[-2:]
            url = f"https://globe.adsbexchange.com/globe_history/{date_path}/traces/{last2}/trace_full_{hex_code}.json"
            try:
                resp = await client.get(url, headers=HEADERS, timeout=15)
                if resp.status_code == 200 and b'"trace"' in resp.content:
                    data = resp.json()
                    trace = data.get("trace")
                    if trace:
                        results.append((
                            str(target_date),
                            hex_code,
                            data.get("r", ""),
                            data.get("t", ""),
                            data.get("desc", ""),
                            data.get("timestamp"),
                            json.dumps(trace),
                        ))
                elif resp.status_code == 429:
                    errors_429 += 1
                    if errors_429 <= 3:
                        print(f"    429 rate limit hit, backing off...")
                        await asyncio.sleep(30)
                        resp = await client.get(url, headers=HEADERS, timeout=15)
                        if resp.status_code == 200 and b'"trace"' in resp.content:
                            data = resp.json()
                            trace = data.get("trace")
                            if trace:
                                results.append((
                                    str(target_date),
                                    hex_code,
                                    data.get("r", ""),
                                    data.get("t", ""),
                                    data.get("desc", ""),
                                    data.get("timestamp"),
                                    json.dumps(trace),
                                ))
            except Exception:
                pass
            done += 1
            if done % 500 == 0:
                print(f"    {done}/{len(hex_codes)} checked ({len(results)} found)")

    tasks = [fetch_one(h) for h in hex_codes]
    await asyncio.gather(*tasks)

    # Write to DuckDB
    if results:
        db.executemany(
            "INSERT INTO traces VALUES ($1::DATE, $2, $3, $4, $5, $6, $7::JSON)",
            results,
        )
        print(f"  {target_date}: {len(results)} aircraft found")
    else:
        print(f"  {target_date}: no data")

    # Mark date as fetched so we don't re-fetch it
    db.execute(
        "INSERT OR IGNORE INTO fetched_dates VALUES ($1::DATE)",
        [str(target_date)],
    )

    return len(results)


def encode_uvarint(value: int) -> bytes:
    """Encode unsigned integer as LEB128 varint."""
    buf = bytearray()
    while value > 0x7F:
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf.append(value & 0x7F)
    return bytes(buf)


def encode_svarint(value: int) -> bytes:
    """Encode signed integer as ZigZag + LEB128."""
    return encode_uvarint((value << 1) ^ (value >> 63))


def build_binary_from_db(db: duckdb.DuckDBPyConnection, target_date: date, out_path: Path):
    """Build a RAF1 .bin file for one day from the DuckDB cache."""
    rows = db.execute(
        "SELECT icao_hex, trace::VARCHAR FROM traces WHERE date = $1",
        [target_date],
    ).fetchall()

    if not rows:
        return 0

    # Day epoch: UTC midnight as unix timestamp
    day_epoch = int(datetime(target_date.year, target_date.month, target_date.day,
                             tzinfo=timezone.utc).timestamp())

    with open(out_path, "wb") as f:
        # File header: magic + day_epoch + flight_count
        f.write(b"RAF1")
        f.write(struct.pack("<IH", day_epoch, len(rows)))

        for hex_code, trace_json in rows:
            trace = json.loads(trace_json)
            if not trace:
                continue

            hex_int = int(hex_code, 16)
            # uint24 LE
            f.write(struct.pack("<I", hex_int)[:3])
            f.write(encode_uvarint(len(trace)))

            # Delta-encode trace points; initial state = 0
            prev_time = 0
            prev_lat = 0
            prev_lon = 0
            prev_alt = 0
            prev_speed = 0

            for pt in trace:
                # Quantise
                time_s = int(pt[0])  # whole seconds
                lat_q = round(pt[1] * 1e5)
                lon_q = round(pt[2] * 1e5)
                alt_raw = pt[3]
                alt_q = -1 if alt_raw == "ground" or alt_raw is None else int(alt_raw) // 25
                speed_q = round(pt[4]) if pt[4] is not None else 0
                track_raw = pt[5] if len(pt) > 5 and pt[5] is not None else 0
                track_byte = round(track_raw * 256 / 360) & 0xFF

                # Deltas
                f.write(encode_uvarint(time_s - prev_time))
                f.write(encode_svarint(lat_q - prev_lat))
                f.write(encode_svarint(lon_q - prev_lon))
                f.write(encode_svarint(alt_q - prev_alt))
                f.write(encode_svarint(speed_q - prev_speed))
                f.write(bytes([track_byte]))

                prev_time = time_s
                prev_lat = lat_q
                prev_lon = lon_q
                prev_alt = alt_q
                prev_speed = speed_q

    return len(rows)


async def main():
    args = parse_args()

    aircraft_file = Path("data/aircraft.json")
    if not aircraft_file.exists():
        print("Run scripts/scrape_hex.py first")
        sys.exit(1)

    aircraft = json.loads(aircraft_file.read_text())
    hex_lookup = {a["hex"]: a for a in aircraft}
    hex_codes = list(hex_lookup.keys())

    if args.from_date and args.to_date:
        start = date.fromisoformat(args.from_date)
        end = date.fromisoformat(args.to_date)
    else:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=args.days - 1)

    dates = list(date_range(start, end))
    db = init_db()

    # Backfill fetched_dates from existing traces data (one-time migration)
    db.execute("""
        INSERT OR IGNORE INTO fetched_dates
        SELECT DISTINCT date FROM traces
        WHERE date NOT IN (SELECT date FROM fetched_dates)
    """)

    already_cached = cached_dates(db)
    to_fetch = [d for d in dates if d not in already_cached]

    print(f"Date range: {start} to {end} ({len(dates)} days)")
    print(f"Already cached: {len(dates) - len(to_fetch)}, to fetch: {len(to_fetch)}")
    print(f"Hex codes: {len(hex_codes)}, rate limit: {args.rate} req/s\n")

    if to_fetch:
        rate_limiter = RateLimiter(args.rate)
        async with httpx.AsyncClient() as client:
            for d in to_fetch:
                await fetch_day(client, hex_codes, d, rate_limiter, db)

    # Build .bin files for the keep window (for the web frontend)
    bin_end = date.today() - timedelta(days=1)
    bin_start = bin_end - timedelta(days=args.keep_days - 1)
    bin_dates = list(date_range(bin_start, bin_end))

    fetched_set = set(to_fetch) if to_fetch else set()
    for d in bin_dates:
        day_bin = Path(f"data/{d}.bin")
        if day_bin.exists() and d not in fetched_set:
            continue  # keep existing .bin; DuckDB may not have this day cached
        count = build_binary_from_db(db, d, day_bin)
        if count == 0 and day_bin.exists():
            day_bin.unlink()

    # Remove .bin files outside the 14-day window
    for bin_file in sorted(Path("data").glob("????-??-??.bin")):
        file_date = date.fromisoformat(bin_file.stem)
        if file_date < bin_start:
            print(f"  Pruning {bin_file.name}")
            bin_file.unlink()

    # Build manifest from all existing .bin files (with hours flown)
    manifest_data = {}
    for f in sorted(Path("data").glob("????-??-??.bin")):
        with open(f, "rb") as bf:
            magic = bf.read(4)
            if magic != b"RAF1":
                continue
            _day_epoch, flight_count = struct.unpack("<IH", bf.read(6))
            total_hours = 0.0

            for _ in range(flight_count):
                # Skip uint24 hex
                bf.read(3)
                # Read point count (uvarint)
                point_count = 0
                shift = 0
                while True:
                    b = bf.read(1)[0]
                    point_count |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7

                # Decode trace to get first and last time
                time_acc = 0
                first_time = None
                for j in range(point_count):
                    # Read uvarint dt
                    dt = 0
                    shift = 0
                    while True:
                        b = bf.read(1)[0]
                        dt |= (b & 0x7F) << shift
                        if not (b & 0x80):
                            break
                        shift += 7
                    time_acc += dt
                    if first_time is None:
                        first_time = time_acc

                    # Skip dlat, dlon, dalt, dspeed (all svarints) + track (1 byte)
                    for _ in range(4):  # 4 svarints
                        while bf.read(1)[0] & 0x80:
                            pass
                    bf.read(1)  # track byte

                if first_time is not None and point_count >= 2:
                    total_hours += (time_acc - first_time) / 3600

            manifest_data[f.stem] = round(total_hours, 1)

    manifest = Path("data/manifest.json")
    manifest.write_text(json.dumps(manifest_data))

    # Write aircraft metadata
    meta_out = Path("data/aircraft_meta.json")
    meta_out.write_text(json.dumps(hex_lookup))

    db.close()
    print(f"\nDone: {len(manifest_data)} days with data -> data/*.bin + manifest.json")


asyncio.run(main())
