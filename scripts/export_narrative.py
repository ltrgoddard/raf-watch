#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb"]
# ///
"""Export flight data as human-readable narratives for LLM analysis.

Produces a text file with one entry per flight-day, reverse-geocoding
start/end positions to known military airfields and describing the
flight character (training sortie, deployment, patrol, etc).
"""

import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

DB_PATH = Path("data/cache.duckdb")
OUT_PATH = Path("data/export/flights_narrative.md")

# Known military airfields and key locations.
# (lat, lon, radius_km, name)
BASES = [
    # UK RAF / Army / Navy
    (52.798, -2.668, 5, "RAF Shawbury"),
    (51.750, -1.590, 5, "RAF Brize Norton"),
    (51.009, -2.638, 5, "RNAS Yeovilton"),
    (53.093, -0.166, 8, "RAF Coningsby"),
    (53.030, -0.508, 8, "RAF Waddington"),
    (53.168, -0.521, 8, "RAF Scampton"),
    (53.248, -4.535, 10, "RAF Valley"),
    (54.310, -1.530, 15, "RAF Leeming"),
    (57.712, -3.340, 15, "RAF Lossiemouth"),
    (51.151, -1.747, 5, "Boscombe Down"),
    (51.150, -1.564, 5, "Middle Wallop"),
    (51.200, -1.100, 10, "RAF Benson / Dalton Barracks area"),
    (50.850, -1.200, 8, "Southampton / Solent area"),
    (52.127, 0.956, 5, "RAF Lakenheath"),
    (52.362, 0.773, 5, "RAF Mildenhall"),
    (52.342, -0.648, 5, "RAF Wittering"),
    (50.086, -5.257, 15, "RNAS Culdrose"),
    (52.960, -0.565, 15, "RAF Cranwell"),
    (50.796, -1.814, 5, "Bournemouth / Hurn"),
    (51.234, -0.943, 5, "RAF Odiham"),
    (53.183, -2.964, 5, "Ternhill"),
    (53.582, -3.050, 5, "RAF Woodvale"),
    (51.302, -0.092, 5, "Biggin Hill"),
    (51.289, -1.779, 5, "Salisbury Plain (Keevil/Upavon)"),
    (55.877, -3.390, 8, "Edinburgh area"),
    (53.700, -2.900, 8, "Warton / BAE Systems"),
    (50.400, -5.000, 8, "Newquay / St Mawgan"),
    (51.900, -1.700, 8, "Enstone / Upper Heyford area"),
    (41.800, 9.600, 15, "Corsica, France"),
    (51.200, -2.300, 8, "Bath / Colerne area"),
    (55.514, -4.586, 5, "Prestwick"),
    (54.843, -5.150, 5, "Belfast area"),
    (51.470, -0.461, 10, "London Heathrow area"),
    (52.460, -1.748, 5, "Birmingham area"),
    (51.886, -0.365, 5, "RAF Henlow / Luton area"),

    # Overseas — Cyprus, Middle East, etc.
    (34.590, 32.988, 15, "RAF Akrotiri, Cyprus"),
    (34.100, 33.500, 15, "Limassol area, Cyprus"),
    (34.880, 33.630, 10, "Larnaca, Cyprus"),
    (25.613, 56.324, 15, "Al Minhad, UAE"),
    (24.428, 54.651, 15, "Al Dhafra, UAE"),
    (23.600, 58.284, 15, "Muscat area, Oman"),
    (26.270, 50.633, 15, "Bahrain"),
    (26.299, 50.122, 10, "Bahrain (offshore)"),
    (29.219, 47.969, 15, "Kuwait"),
    (25.255, 55.365, 15, "Dubai, UAE"),
    (36.156, 37.213, 15, "Aleppo area, Syria"),
    (33.511, 36.277, 15, "Damascus area, Syria"),

    # Africa
    (-1.329, 36.922, 10, "Nairobi, Kenya"),
    (12.041, -17.804, 15, "Dakar area, Senegal"),

    # US bases (some UK aircraft train there)
    (36.236, -115.034, 10, "Nellis AFB, Nevada"),
    (32.085, -106.420, 10, "Holloman AFB, New Mexico"),
    (34.583, -117.383, 10, "Edwards AFB, California"),
    (35.236, -120.637, 10, "Vandenberg SFB, California"),
    (28.234, -80.608, 10, "Patrick SFB, Florida"),
    (43.064, -70.812, 10, "Pease ANGB, New Hampshire"),
    (32.909, -80.045, 10, "Charleston area, South Carolina"),

    # European
    (47.258, 11.354, 10, "Innsbruck area, Austria"),
    (64.13, -21.94, 10, "Keflavik, Iceland"),
    (69.312, 16.144, 10, "Andøya, Norway"),
    (58.109, 7.137, 10, "Kjevik, Norway"),
    (60.121, 11.100, 10, "Rygge / Oslo area, Norway"),
    (48.689, 2.073, 10, "Paris area, France"),
    (49.017, 2.547, 10, "CDG / Paris, France"),
    (43.435, 5.227, 10, "Istres / Marseille, France"),
    (35.857, 14.477, 10, "Malta"),
    (40.652, 22.972, 10, "Thessaloniki, Greece"),
    (37.894, 23.726, 10, "Athens, Greece"),
    (38.175, 35.429, 15, "Central Turkey"),
    (37.002, 35.425, 10, "Incirlik, Turkey"),
    (50.030, 8.570, 10, "Ramstein area, Germany"),
    (40.301, -3.712, 10, "Madrid area, Spain"),

    # Sea areas (large radius catch-alls for patrol flights)
    (58.0, 1.0, 80, "North Sea"),
    (48.5, -8.0, 80, "Bay of Biscay / Western Approaches"),
    (60.0, -10.0, 100, "North Atlantic / GIUK Gap"),
    (36.0, 18.0, 120, "Central Mediterranean"),
    (35.0, 25.0, 80, "Eastern Mediterranean"),
    (27.0, 57.0, 120, "Strait of Hormuz / Gulf of Oman"),
    (26.0, 52.0, 100, "Persian Gulf"),
]


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def geocode(lat, lon):
    best = None
    best_dist = float('inf')
    for blat, blon, radius, name in BASES:
        d = haversine_km(lat, lon, blat, blon)
        if d < radius and d < best_dist:
            best = name
            best_dist = d
    return best


def classify_flight(duration_min, max_alt, max_speed, distance_km, start_name, end_name, icao_type):
    if distance_km > 1500:
        return "long-range deployment/transit"
    if distance_km > 500:
        return "medium-range transit"

    heli_types = {"H135", "H145", "AS65", "LYNX", "WILD", "A109", "MLIN", "PUMA", "CH47", "AW59", "AW10"}
    if icao_type in heli_types:
        if duration_min < 60 and distance_km < 50:
            return "local helicopter training"
        return "helicopter sortie"

    patrol_types = {"P8", "C30J", "A400", "RC135", "SENT", "SHDW"}
    if any(t in icao_type for t in patrol_types) and duration_min > 180:
        return "long-endurance patrol/ISR"

    if start_name and end_name and start_name == end_name:
        if duration_min < 90:
            return "local training sortie"
        return "training/exercise sortie"

    if "Sea" in (start_name or "") or "Sea" in (end_name or "") or "Atlantic" in (start_name or "") or "Atlantic" in (end_name or ""):
        return "maritime patrol"

    if duration_min < 45 and distance_km < 80:
        return "short local sortie"

    return "sortie"


T_SECONDS = 0
T_LAT = 1
T_LON = 2
T_ALT_BARO = 3
T_GROUND_SPEED = 4
T_HEADING = 5
T_METADATA = 8


def main():
    db = duckdb.connect(str(DB_PATH), read_only=True)
    aircraft_meta = {}
    af = Path("data/aircraft.json")
    if af.exists():
        aircraft_meta = {a["hex"]: a for a in json.loads(af.read_text())}

    rows = db.execute("""
        SELECT date, icao_hex, registration, icao_type, description,
               trace::VARCHAR
        FROM traces
        WHERE json_array_length(trace) > 5
        ORDER BY date, icao_hex
    """).fetchall()
    db.close()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out = open(OUT_PATH, "w")

    out.write("# UK Military Flight Data — Narrative Export\n\n")
    out.write(f"Exported: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
    out.write(f"Total flights: {len(rows)}\n\n")
    out.write("Each entry is one aircraft observed on one day. Positions are reverse-geocoded\n")
    out.write("to known military airfields where possible. Coordinates included for verification.\n\n")
    out.write("---\n\n")

    current_date = None
    date_count = 0

    for date_val, icao_hex, reg, icao_type, desc, trace_raw in rows:
        trace = json.loads(trace_raw)
        if not trace:
            continue

        meta = aircraft_meta.get(icao_hex, {})
        unit = meta.get("unit", "")
        reg = reg or meta.get("reg", "")
        icao_type = icao_type or meta.get("icao_type", "")
        desc = desc or meta.get("type", "")

        # Start/end positions
        start_lat, start_lon = trace[0][T_LAT], trace[0][T_LON]
        end_lat, end_lon = trace[-1][T_LAT], trace[-1][T_LON]
        start_name = geocode(start_lat, start_lon)
        end_name = geocode(end_lat, end_lon)

        # Timing
        day_epoch = datetime(date_val.year, date_val.month, date_val.day, tzinfo=timezone.utc)
        first_seen = day_epoch + timedelta(seconds=trace[0][T_SECONDS])
        last_seen = day_epoch + timedelta(seconds=trace[-1][T_SECONDS])
        duration_min = (trace[-1][T_SECONDS] - trace[0][T_SECONDS]) / 60

        # Altitude / speed
        alts = []
        speeds = []
        for pt in trace:
            a = pt[T_ALT_BARO]
            if a != "ground" and a is not None:
                alts.append(a)
            if pt[T_GROUND_SPEED] is not None:
                speeds.append(pt[T_GROUND_SPEED])

        max_alt = max(alts) if alts else 0
        max_speed = max(speeds) if speeds else 0

        # Distance
        distance_km = haversine_km(start_lat, start_lon, end_lat, end_lon)

        # Callsigns / squawks
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

        # Regions transited (sample every ~20th point)
        transited = set()
        step = max(1, len(trace) // 30)
        for i in range(0, len(trace), step):
            pt = trace[i]
            name = geocode(pt[T_LAT], pt[T_LON])
            if name and name != start_name and name != end_name:
                transited.add(name)

        flight_type = classify_flight(duration_min, max_alt, max_speed, distance_km, start_name, end_name, icao_type)

        # Date header
        date_str = str(date_val)
        if date_str != current_date:
            if current_date is not None:
                out.write("\n")
            weekday = date_val.strftime("%A")
            out.write(f"## {date_str} ({weekday})\n\n")
            current_date = date_str
            date_count = 0

        date_count += 1

        # Write entry
        aircraft_str = f"{reg} — {desc}" if desc else reg
        if unit:
            aircraft_str += f" [{unit}]"

        out.write(f"**{aircraft_str}**\n")
        out.write(f"- Type: {flight_type}\n")
        out.write(f"- Callsign: {', '.join(sorted(callsigns)) or '—'}")
        if squawks:
            out.write(f" | Squawk: {', '.join(sorted(squawks))}")
        out.write("\n")

        start_str = start_name or "unknown"
        end_str = end_name or "unknown"
        out.write(f"- From: {start_str} ({start_lat:.3f}, {start_lon:.3f})\n")
        out.write(f"- To: {end_str} ({end_lat:.3f}, {end_lon:.3f})\n")

        if transited:
            out.write(f"- Via: {', '.join(sorted(transited))}\n")

        out.write(f"- Time: {first_seen.strftime('%H:%M')}–{last_seen.strftime('%H:%M')} UTC ({duration_min:.0f} min)\n")
        out.write(f"- Max alt: {max_alt:,} ft, max speed: {max_speed:.0f} kts, {len(trace)} trace points\n")
        out.write("\n")

    out.close()

    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"{len(rows)} flights across {len(set(r[0] for r in rows))} days -> {OUT_PATH} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
