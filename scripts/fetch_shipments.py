#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb", "httpx", "python-dotenv", "pyjwt"]
# ///
"""Extract military fuel shipments + AIS tracks from Kpler.

1. Queries the Kpler backup DuckDB for military trades (products, installations, players)
2. Fetches vessel AIS positions from the Kpler terminal API for each shipment
3. Packs everything into a binary container for the raf-watch map

Binary format (same as flights):
  [uint32 LE]  shipment count
  per shipment:
    [uint32 LE]  vessel IMO
    [uint32 LE]  JSON payload length
    [N bytes]    compact JSON with embedded trace array

Usage:
  uv run scripts/fetch_shipments.py
  uv run scripts/fetch_shipments.py --db /path/to/backup.duckdb
  uv run scripts/fetch_shipments.py --skip-positions  # DB only, no API calls
"""

import sys
sys.stdout.reconfigure(line_buffering=True)

import argparse
import asyncio
import json
import os
import struct
import time
from datetime import datetime
from pathlib import Path

import duckdb
import httpx
import jwt
from dotenv import load_dotenv

KPLER_DIR = Path.home() / "Tools/kpler-backup"
DB_PATH = KPLER_DIR / "data/kpler_backup.duckdb"
OUT_DIR = Path("data")
CACHE_DIR = Path("data/cache/shipments")

# ── Kpler auth (reuses kpler-backup tokens) ──────────────────────────

AUTH_URL = "https://auth.kpler.com/oauth/token"
CLIENT_ID = "0LglhXfJvfepANl3HqVT9i1U0OwV0gSP"
AUDIENCE = "https://terminal.kpler.com"
AUTH0_CLIENT = "eyJuYW1lIjoiYXV0aDAtc3BhLWpzIiwidmVyc2lvbiI6IjIuMS4zIn0="
BASE_URL = "https://terminal.kpler.com/api"
WEB_VERSION = "v21.2161.1"
HEADERS_BASE = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://terminal.kpler.com",
    "referer": "https://terminal.kpler.com/",
    "use-access-token": "true",
    "x-web-application-version": WEB_VERSION,
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


class KplerAuth:
    """Minimal auth manager — reuses tokens from kpler-backup."""

    def __init__(self, token_dir: Path):
        self.token_file = token_dir / ".kpler_token"
        self.refresh_file = token_dir / ".kpler_refresh_token"
        self.info_file = token_dir / ".kpler_token_info"
        self._http: httpx.AsyncClient | None = None

    def set_client(self, client: httpx.AsyncClient):
        self._http = client

    def _load(self):
        return {
            "access_token": self.token_file.read_text().strip(),
            "refresh_token": self.refresh_file.read_text().strip(),
            "expires_at": int(self.info_file.read_text().strip()),
        }

    def _save(self, access_token, refresh_token):
        self.token_file.write_text(access_token)
        self.refresh_file.write_text(refresh_token)
        exp = jwt.decode(access_token, options={"verify_signature": False}).get("exp", 0)
        self.info_file.write_text(str(exp))

    async def _refresh(self):
        tokens = self._load()
        resp = await self._http.post(AUTH_URL, headers={
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://terminal.kpler.com",
            "auth0-client": AUTH0_CLIENT,
        }, data={
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "refresh_token": tokens["refresh_token"],
            "audience": AUDIENCE,
        })
        resp.raise_for_status()
        r = resp.json()
        self._save(r["access_token"], r.get("refresh_token", tokens["refresh_token"]))

    async def _login(self):
        load_dotenv(KPLER_DIR / ".env")
        u, p = os.getenv("KPLER_USERNAME"), os.getenv("KPLER_PASSWORD")
        if not u or not p:
            raise RuntimeError("No tokens and no KPLER_USERNAME/KPLER_PASSWORD")
        resp = await self._http.post(AUTH_URL, headers={
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://terminal.kpler.com",
            "auth0-client": AUTH0_CLIENT,
        }, data={
            "grant_type": "password", "username": u, "password": p,
            "client_id": CLIENT_ID, "audience": AUDIENCE,
            "scope": "openid profile email offline_access",
        })
        resp.raise_for_status()
        r = resp.json()
        self._save(r["access_token"], r["refresh_token"])

    async def get_token(self) -> str:
        if not self.token_file.exists():
            await self._login()
        tokens = self._load()
        if int(time.time()) > (tokens["expires_at"] - 60):
            await self._refresh()
            tokens = self._load()
        return tokens["access_token"]


# ── Military criteria ────────────────────────────────────────────────

MILITARY_PRODUCTS = {"F-76 Military Diesel", "Jet JP5", "Jet JP8", "Jet JP1"}

MILITARY_INSTALLATIONS = {
    3025, 3036, 3035, 3214, 11006, 12024,  # US Navy Pacific
    3201, 3190, 9761, 11357, 11709, 7195,  # US Navy Atlantic/IO
    3090, 3236,                              # NATO
    10954, 11723, 3282,                      # UK MoD
}

MILITARY_PLAYERS = ["U.S. Navy", "Military Sealift", "DLA Energy", "Government of UK"]

PRODUCT_SHORT = {
    "F-76 Military Diesel": "F-76", "Jet JP5": "JP-5",
    "Jet JP8": "JP-8", "Jet JP1": "JP-1",
}


# ── Trade extraction ─────────────────────────────────────────────────

def load_installation_coords(db):
    rows = db.sql("""
        SELECT installation_id, name,
               json_extract_string(raw, '$.position.latitude')::DOUBLE,
               json_extract_string(raw, '$.position.longitude')::DOUBLE,
               json_extract_string(raw, '$.zone.country.name')
        FROM installations
        WHERE json_extract_string(raw, '$.position.latitude') IS NOT NULL
    """).fetchall()
    return {r[0]: {"name": r[1], "lat": r[2], "lon": r[3], "country": r[4]} for r in rows}


def extract_shipment(raw, raw_str, inst_coords):
    vessels = raw.get("vessels", [])
    if not vessels or not vessels[0].get("imo"):
        return None

    v = vessels[0]
    fq = raw.get("flowQuantities", [])
    product = fq[0]["name"] if fq else None

    origin_pc = raw.get("portCallOrigin") or raw.get("forecastPortCallOrigin") or {}
    dest_pc = raw.get("portCallDestination") or raw.get("forecastPortCallDestination") or {}
    origin_inst = origin_pc.get("installation") or {}
    dest_inst = dest_pc.get("installation") or {}
    oc = inst_coords.get(origin_inst.get("id"), {})
    dc = inst_coords.get(dest_inst.get("id"), {})
    if not oc.get("lat") or not dc.get("lat"):
        return None

    buyer = seller = None
    seqs = raw.get("orgSpecificInfo", {}).get("default", {}).get("bestTradeLinkSequences", [])
    if seqs:
        for link in seqs[0].get("tradeLinks", []):
            if link.get("buyer") and not buyer:
                buyer = link["buyer"]["name"]
            if link.get("seller") and not seller:
                seller = link["seller"]["name"]

    vol = mass = None
    if fq and fq[0].get("flowQuantity"):
        q = fq[0]["flowQuantity"]
        vol, mass = q.get("volume"), q.get("mass")

    flags = []
    if product in MILITARY_PRODUCTS:
        flags.append("mil_product")
    if origin_inst.get("id") in MILITARY_INSTALLATIONS:
        flags.append("mil_origin")
    if dest_inst.get("id") in MILITARY_INSTALLATIONS:
        flags.append("mil_dest")
    if any(n in raw_str for n in MILITARY_PLAYERS):
        flags.append("mil_player")

    return {
        "id": raw["id"],
        "imo": int(v["imo"]),
        "vesselId": int(v["id"]),
        "vessel": v.get("name"),
        "vesselType": v.get("vesselType") or v.get("vesselTypeClass"),
        "product": PRODUCT_SHORT.get(product, product),
        "status": raw.get("status"),
        "start": raw.get("start"),
        "end": raw.get("end"),
        "origin": [round(oc["lon"], 5), round(oc["lat"], 5), origin_inst.get("name") or "", oc.get("country") or ""],
        "dest": [round(dc["lon"], 5), round(dc["lat"], 5), dest_inst.get("name") or "", dc.get("country") or ""],
        "vol": vol,
        "mass": mass,
        "buyer": buyer,
        "seller": seller,
        "flags": ",".join(flags),
    }


def query_trades(db, inst_coords):
    ids = ",".join(str(i) for i in MILITARY_INSTALLATIONS)
    prods = ",".join(f"'{p}'" for p in MILITARY_PRODUCTS)

    passes = [
        ("products", f"SELECT trade_id, raw FROM trades WHERE json_extract_string(raw, '$.flowQuantities[0].name') IN ({prods})"),
        ("destinations", f"""SELECT trade_id, raw FROM trades
            WHERE TRY_CAST(json_extract_string(raw, '$.portCallDestination.installation.id') AS INTEGER) IN ({ids})
               OR TRY_CAST(json_extract_string(raw, '$.forecastPortCallDestination.installation.id') AS INTEGER) IN ({ids})"""),
        ("origins", f"""SELECT trade_id, raw FROM trades
            WHERE TRY_CAST(json_extract_string(raw, '$.portCallOrigin.installation.id') AS INTEGER) IN ({ids})
               OR TRY_CAST(json_extract_string(raw, '$.forecastPortCallOrigin.installation.id') AS INTEGER) IN ({ids})"""),
    ]
    for needle in MILITARY_PLAYERS:
        passes.append((needle, f"SELECT trade_id, raw FROM trades WHERE raw::VARCHAR LIKE '%{needle}%'"))

    shipments = []
    seen = set()

    for label, sql in passes:
        print(f"  {label}...", end=" ", flush=True)
        n = 0
        result = db.sql(sql)
        while batch := result.fetchmany(500):
            for tid, raw_str in batch:
                if tid in seen:
                    continue
                seen.add(tid)
                raw = json.loads(raw_str) if isinstance(raw_str, str) else raw_str
                raw_s = raw_str if isinstance(raw_str, str) else json.dumps(raw_str)
                s = extract_shipment(raw, raw_s, inst_coords)
                if s:
                    shipments.append(s)
                    n += 1
        print(n)

    return shipments


# ── AIS position fetching ────────────────────────────────────────────

async def fetch_positions(shipments: list[dict], auth: KplerAuth, concurrency: int = 5):
    """Fetch AIS positions for each shipment. Adds 'trace' array in-place."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(concurrency)
    done = [0]
    total = len(shipments)
    cached = [0]

    async def fetch_one(client: httpx.AsyncClient, s: dict):
        vid = s["vesselId"]
        start = s["start"][:10] if s.get("start") else None
        end = s["end"][:10] if s.get("end") else None
        if not start:
            s["trace"] = []
            return

        # Use end date or today
        if not end:
            end = datetime.now().strftime("%Y-%m-%d")

        # Cache key
        cache_key = f"{vid}_{start}_{end}"
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            s["trace"] = json.loads(cache_file.read_text())
            cached[0] += 1
            done[0] += 1
            return

        async with sem:
            token = await auth.get_token()
            headers = {**HEADERS_BASE, "x-access-token": token}
            url = f"{BASE_URL}/vessels/{vid}/positions"
            params = {"after": start, "before": end, "limit": 5000}

            for attempt in range(3):
                try:
                    resp = await client.get(url, headers=headers, params=params, timeout=30)
                    if resp.status_code == 401:
                        token = await auth.get_token()
                        headers["x-access-token"] = token
                        continue
                    if resp.status_code in (429, 502, 503, 504):
                        await asyncio.sleep(2 ** attempt)
                        continue
                    resp.raise_for_status()

                    data = resp.json()
                    positions = data.get("data", data) if isinstance(data, dict) else data

                    # Compact trace: [unix_ts, lat, lon, speed, course]
                    trace = []
                    for p in positions:
                        t = p.get("receivedTime")
                        geo = p.get("geo", {})
                        if not t or not geo.get("lat"):
                            continue
                        ts = int(datetime.fromisoformat(t.replace("Z", "+00:00").replace("+00:00", "")).timestamp())
                        trace.append([
                            ts,
                            round(geo["lat"], 5),
                            round(geo["lon"], 5),
                            round(p.get("speed", 0), 1),
                            p.get("course", 0),
                        ])

                    trace.sort(key=lambda x: x[0])
                    s["trace"] = trace
                    cache_file.write_text(json.dumps(trace, separators=(",", ":")))
                    break

                except (httpx.ReadTimeout, httpx.ConnectTimeout):
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        s["trace"] = []
                except Exception:
                    s["trace"] = []
                    break

        done[0] += 1
        if done[0] % 50 == 0:
            print(f"  {done[0]}/{total} ({cached[0]} cached)")

    async with httpx.AsyncClient() as client:
        auth.set_client(client)
        tasks = [fetch_one(client, s) for s in shipments]
        await asyncio.gather(*tasks)

    print(f"  {done[0]}/{total} done ({cached[0]} cached)")

    # Stats
    with_trace = sum(1 for s in shipments if s.get("trace"))
    total_pts = sum(len(s.get("trace", [])) for s in shipments)
    print(f"  {with_trace} shipments with tracks, {total_pts:,} total positions")


# ── Binary output ────────────────────────────────────────────────────

def build_binary(shipments, out_path):
    """[uint32 count] + per entry: [uint32 imo, uint32 json_len, json_bytes]"""
    with open(out_path, "wb") as f:
        f.write(struct.pack("<I", len(shipments)))
        for s in shipments:
            # Strip vesselId from output (internal only)
            out = {k: v for k, v in s.items() if k != "vesselId"}
            payload = json.dumps(out, separators=(",", ":")).encode()
            f.write(struct.pack("<II", s["imo"], len(payload)))
            f.write(payload)


# ── Main ─────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=str, default=str(DB_PATH))
    p.add_argument("--skip-positions", action="store_true", help="skip AIS position fetching")
    p.add_argument("--concurrency", type=int, default=5)
    return p.parse_args()


async def async_main():
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(db_path), read_only=True)
    db.sql("SET threads=4; SET preserve_insertion_order=false")

    # Phase 1: Extract trades
    print("Loading installations...")
    inst_coords = load_installation_coords(db)
    print(f"  {len(inst_coords)} with coordinates")

    print("Scanning trades...")
    shipments = query_trades(db, inst_coords)
    shipments.sort(key=lambda s: s.get("start") or "")
    print(f"  {len(shipments)} military shipments")
    db.close()

    # Phase 2: Fetch AIS positions
    if not args.skip_positions:
        print("\nFetching AIS positions...")
        auth = KplerAuth(KPLER_DIR)
        await fetch_positions(shipments, auth, args.concurrency)

    # Phase 3: Write binary
    bin_path = OUT_DIR / "shipments.bin"
    build_binary(shipments, bin_path)
    print(f"\n  -> {bin_path} ({bin_path.stat().st_size:,} bytes)")

    # Metadata
    products, flags = {}, {}
    for s in shipments:
        products[s["product"]] = products.get(s["product"], 0) + 1
        for f in s["flags"].split(","):
            if f:
                flags[f] = flags.get(f, 0) + 1

    meta = {"count": len(shipments), "products": products, "flags": flags}
    meta_path = OUT_DIR / "shipments_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    print(f"  -> {meta_path}")


asyncio.run(async_main())
