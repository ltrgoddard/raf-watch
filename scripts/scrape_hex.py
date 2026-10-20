#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["requests", "beautifulsoup4", "lxml"]
# ///
"""Scrape military aircraft hex codes from live-mobile-mode-s.eu

Usage:
  uv run scripts/scrape_hex.py                  # UK (default)
  uv run scripts/scrape_hex.py --country US     # US military
  uv run scripts/scrape_hex.py --country US --output data/us/aircraft.json
"""

import argparse
import json
from pathlib import Path

import requests
from bs4 import BeautifulSoup


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--country", default="UK", help="DGCountry code (UK, US, FR, DE, ...)")
    p.add_argument("--output", default=None, help="output path (default: data/aircraft.json or data/{country}/aircraft.json)")
    return p.parse_args()


args = parse_args()
country = args.country.upper()
URL = f"https://live-mobile-mode-s.eu/overviewall.php?DGCountry={country}"

print(f"Scraping {country} aircraft from {URL}...")
resp = requests.get(URL, timeout=120)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "lxml")

aircraft = []
for row in soup.select("table tr"):
    cells = row.find_all("td")
    if len(cells) < 6:
        continue
    hex_code = cells[0].get_text(strip=True)
    if len(hex_code) != 6 or not all(c in "0123456789ABCDEFabcdef" for c in hex_code):
        continue
    aircraft.append({
        "hex": hex_code.lower(),
        "reg": cells[1].get_text(strip=True).strip("()").strip(),
        "type": cells[3].get_text(strip=True),
        "icao_type": cells[4].get_text(strip=True),
        "unit": cells[5].get_text(strip=True),
    })

if len(aircraft) < 100:
    raise SystemExit(f"only {len(aircraft)} aircraft parsed; keeping old list")

if args.output:
    out = Path(args.output)
else:
    out = Path("data/aircraft.json") if country == "UK" else Path(f"data/{country.lower()}/aircraft.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(aircraft, indent=2))
print(f"Scraped {len(aircraft)} aircraft → {out}")
