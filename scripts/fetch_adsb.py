#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx", "duckdb"]
# ///
"""Fetch flight traces from globe.adsbexchange.com for military aircraft.

Caches results in DuckDB (one row per aircraft-day). Outputs per-day .bin
files and a manifest.json for the frontend.

Usage:
  uv run scripts/fetch_adsb.py                         # UK, last 14 days
  uv run scripts/fetch_adsb.py --days 7                # UK, last 7 days
  uv run scripts/fetch_adsb.py --from 2026-02-20 --to 2026-03-02

  # US aircraft with adaptive skipping (skip after 3 consecutive misses)
  uv run scripts/fetch_adsb.py --aircraft data/us/aircraft.json \\
      --data-dir data/us --db data/us/cache.duckdb --adaptive 3
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


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--from", dest="from_date", type=str, default=None)
    p.add_argument("--to", dest="to_date", type=str, default=None)
    p.add_argument("--keep-days", type=int, default=14, help="days of .bin files to keep")
    p.add_argument("--rate", type=float, default=10.0, help="max requests/sec")
    p.add_argument("--aircraft", type=str, default="data/aircraft.json", help="path to aircraft.json")
    p.add_argument("--data-dir", type=str, default="data", help="output directory for .bin files")
    p.add_argument("--db", type=str, default=None, help="DuckDB path (default: <data-dir>/cache.duckdb)")
    p.add_argument("--adaptive", type=int, default=0,
                   help="adaptive skip: stop checking hex after N consecutive days with no data (0=disabled)")
    return p.parse_args()


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def init_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(db_path))
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
) -> tuple[int, set[str]]:
    """Fetch traces for all hex codes for a single day.
    Returns (count_found, set_of_hex_codes_with_data)."""

    date_path = target_date.strftime("%Y/%m/%d")
    results: list[tuple] = []
    found_hexes: set[str] = set()
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
                        found_hexes.add(hex_code)
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
                                found_hexes.add(hex_code)
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

    return len(results), found_hexes


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


def build_manifest(data_dir: Path) -> dict:
    """Build manifest from all existing .bin files (with hours flown)."""
    manifest_data = {}
    for f in sorted(data_dir.glob("????-??-??.bin")):
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

    return manifest_data


async def main():
    args = parse_args()
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db) if args.db else data_dir / "cache.duckdb"

    aircraft_file = Path(args.aircraft)
    if not aircraft_file.exists():
        print(f"Aircraft file not found: {aircraft_file}")
        print("Run scripts/scrape_hex.py first")
        sys.exit(1)

    aircraft = json.loads(aircraft_file.read_text())
    hex_lookup = {a["hex"]: a for a in aircraft}
    all_hex_codes = list(hex_lookup.keys())

    if args.from_date and args.to_date:
        start = date.fromisoformat(args.from_date)
        end = date.fromisoformat(args.to_date)
    else:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=args.days - 1)

    dates = list(date_range(start, end))
    db = init_db(db_path)

    # Backfill fetched_dates from existing traces data (one-time migration)
    db.execute("""
        INSERT OR IGNORE INTO fetched_dates
        SELECT DISTINCT date FROM traces
        WHERE date NOT IN (SELECT date FROM fetched_dates)
    """)

    already_cached = cached_dates(db)
    to_fetch = [d for d in dates if d not in already_cached]

    adaptive = args.adaptive
    print(f"Date range: {start} to {end} ({len(dates)} days)")
    print(f"Already cached: {len(dates) - len(to_fetch)}, to fetch: {len(to_fetch)}")
    print(f"Hex codes: {len(all_hex_codes)}, rate limit: {args.rate} req/s")
    if adaptive:
        print(f"Adaptive skipping: enabled (threshold={adaptive} consecutive misses)")
    print()

    if to_fetch:
        rate_limiter = RateLimiter(args.rate)

        # Adaptive skipping state: miss_streak per hex code
        miss_streak: dict[str, int] = {h: 0 for h in all_hex_codes}
        total_skipped = 0

        async with httpx.AsyncClient() as client:
            for i, d in enumerate(to_fetch):
                # Determine which hex codes to check this day
                if adaptive and i > 0:
                    active_hexes = [h for h in all_hex_codes if miss_streak[h] < adaptive]
                    skipped = len(all_hex_codes) - len(active_hexes)
                    total_skipped += skipped
                    if skipped > 0:
                        print(f"  Adaptive: checking {len(active_hexes)}/{len(all_hex_codes)} "
                              f"(skipping {skipped} inactive)")
                else:
                    active_hexes = all_hex_codes

                _count, found_hexes = await fetch_day(client, active_hexes, d, rate_limiter, db)

                # Update miss streaks
                if adaptive:
                    for h in all_hex_codes:
                        if h in found_hexes:
                            miss_streak[h] = 0
                        elif h in active_hexes:  # only increment if we actually checked
                            miss_streak[h] += 1

        if adaptive and total_skipped > 0:
            ever_seen = sum(1 for h in all_hex_codes if miss_streak[h] < len(to_fetch))
            never_seen = len(all_hex_codes) - ever_seen
            print(f"\nAdaptive summary: {total_skipped} total lookups skipped")
            print(f"  {ever_seen} aircraft seen at least once, {never_seen} never seen")

    # Build .bin files for the keep window
    bin_end = date.today() - timedelta(days=1)
    bin_start = bin_end - timedelta(days=args.keep_days - 1)
    bin_dates = list(date_range(bin_start, bin_end))

    fetched_set = set(to_fetch) if to_fetch else set()
    for d in bin_dates:
        day_bin = data_dir / f"{d}.bin"
        if day_bin.exists() and d not in fetched_set:
            continue
        count = build_binary_from_db(db, d, day_bin)
        if count == 0 and day_bin.exists():
            day_bin.unlink()

    # Remove .bin files outside the keep window
    for bin_file in sorted(data_dir.glob("????-??-??.bin")):
        file_date = date.fromisoformat(bin_file.stem)
        if file_date < bin_start:
            print(f"  Pruning {bin_file.name}")
            bin_file.unlink()

    # Build manifest
    manifest_data = build_manifest(data_dir)
    manifest = data_dir / "manifest.json"
    manifest.write_text(json.dumps(manifest_data))

    # Write aircraft metadata
    meta_out = data_dir / "aircraft_meta.json"
    meta_out.write_text(json.dumps(hex_lookup))

    db.close()
    print(f"\nDone: {len(manifest_data)} days with data -> {data_dir}/*.bin + manifest.json")


asyncio.run(main())
