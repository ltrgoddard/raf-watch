#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["requests", "beautifulsoup4", "lxml"]
# ///
"""Scrape UK military aircraft hex codes from live-mobile-mode-s.eu"""

import json
from pathlib import Path

import requests
from bs4 import BeautifulSoup

URL = "https://live-mobile-mode-s.eu/overviewall.php?DGCountry=UK"

resp = requests.get(URL, timeout=30)
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

out = Path("data/aircraft.json")
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(aircraft, indent=2))
print(f"Scraped {len(aircraft)} aircraft → {out}")
