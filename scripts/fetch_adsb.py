#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx"]
# ///
"""Fetch flight traces from globe.adsbexchange.com for UK military aircraft.

Caches per-day results so it can resume across runs. Outputs per-day .bin
files and a manifest.json for the frontend.

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
from datetime import date, timedelta
from pathlib import Path

import httpx

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Referer": "https://globe.adsbexchange.com/",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "DNT": "1",
}

CACHE_DIR = Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--from", dest="from_date", type=str, default=None)
    p.add_argument("--to", dest="to_date", type=str, default=None)
    p.add_argument("--rate", type=float, default=10.0, help="max requests/sec")
    p.add_argument("--keep-days", type=int, default=0, help="prune .bin files older than N days (0=no pruning)")
    return p.parse_args()


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


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
) -> list[tuple[str, bytes]]:
    """Fetch traces for all hex codes for a single day. Returns list of (hex, raw_json_bytes)."""

    cache_file = CACHE_DIR / f"{target_date}.json"
    if cache_file.exists():
        cached = json.loads(cache_file.read_text())
        if cached:
            print(f"  {target_date}: {len(cached)} aircraft (cached)")
        return [(h, raw.encode()) for h, raw in cached]

    date_path = target_date.strftime("%Y/%m/%d")
    results: list[tuple[str, bytes]] = []
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
                    if data.get("trace"):
                        results.append((hex_code, resp.content))
                elif resp.status_code == 429:
                    errors_429 += 1
                    if errors_429 <= 3:
                        print(f"    429 rate limit hit, backing off...")
                        await asyncio.sleep(30)
                        # retry once
                        resp = await client.get(url, headers=HEADERS, timeout=15)
                        if resp.status_code == 200 and b'"trace"' in resp.content:
                            data = resp.json()
                            if data.get("trace"):
                                results.append((hex_code, resp.content))
            except Exception:
                pass
            done += 1
            if done % 500 == 0:
                print(f"    {done}/{len(hex_codes)} checked ({len(results)} found)")

    tasks = [fetch_one(h) for h in hex_codes]
    await asyncio.gather(*tasks)

    # Cache results
    cache_data = [(h, raw.decode()) for h, raw in results]
    cache_file.write_text(json.dumps(cache_data))

    if results:
        print(f"  {target_date}: {len(results)} aircraft found")
    else:
        print(f"  {target_date}: no data")

    return results


def build_binary(all_results: list[tuple[str, bytes]], out_path: Path):
    """Pack all results into binary container."""
    with open(out_path, "wb") as f:
        f.write(struct.pack("<I", len(all_results)))
        for hex_code, raw_json in all_results:
            hex_int = int(hex_code, 16)
            f.write(struct.pack("<II", hex_int, len(raw_json)))
            f.write(raw_json)


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
    print(f"Fetching {len(dates)} days ({start} to {end}), {len(hex_codes)} hex codes")
    print(f"Rate limit: {args.rate} req/s\n")

    rate_limiter = RateLimiter(args.rate)

    async with httpx.AsyncClient() as client:
        for d in dates:
            day_results = await fetch_day(client, hex_codes, d, rate_limiter)
            if day_results:
                # Write per-day .bin file
                day_bin = Path(f"data/{d}.bin")
                build_binary(day_results, day_bin)

    # Prune old .bin files if --keep-days is set
    if args.keep_days > 0:
        cutoff = date.today() - timedelta(days=args.keep_days)
        for bin_file in sorted(Path("data").glob("????-??-??.bin")):
            file_date = date.fromisoformat(bin_file.stem)
            if file_date < cutoff:
                print(f"  Pruning {bin_file.name}")
                bin_file.unlink()

    # Build manifest from all existing .bin files (with flight counts)
    manifest_data = {}
    for f in sorted(Path("data").glob("????-??-??.bin")):
        with open(f, "rb") as bf:
            count = struct.unpack("<I", bf.read(4))[0]
        manifest_data[f.stem] = count

    manifest = Path("data/manifest.json")
    manifest.write_text(json.dumps(manifest_data))

    # Write aircraft metadata
    meta_out = Path("data/aircraft_meta.json")
    meta_out.write_text(json.dumps(hex_lookup))

    print(f"\nDone: {len(manifest_data)} days with data -> data/*.bin + manifest.json")


asyncio.run(main())
