#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""write data/YYYY-MM-DD.bin (RAF1) per UTC day from adsb.exposed.

  uv run scripts/fetch.py                        last 7 days, missing or empty
  uv run scripts/fetch.py 2025-01-01 2025-12-31  given range, overwrite
"""
import json, re, struct, sys, time, urllib.parse, urllib.request
from datetime import date, datetime, timedelta, UTC
from pathlib import Path

D, SQL = Path('data'), (Path(__file__).parent / 'days.sql').read_text()
URL = 'https://kvzqttvc2n.eu-west-1.aws.clickhouse-staging.com/?'
def clean(s): return re.sub(r'\s*\|\s*', ' · ', s).strip(' ·-?')


def uv(n):
    b = bytearray()
    while n > 127: b.append(n & 127 | 128); n >>= 7
    return bytes(b + bytes([n]))


def sv(n): return uv(n << 1 ^ n >> 63)


def query(a, b, hexes):
    q = urllib.parse.urlencode({'user': 'website', 'param_a': a,
                                'param_b': b, 'param_h': str(hexes)})
    for i in range(6):
        try:
            with urllib.request.urlopen(URL + q, SQL.encode(), 300) as r:
                return [json.loads(l) for l in r.read().splitlines()]
        except Exception as e:
            print(f'  {a}..{b}: {e}, retry {i + 1}', file=sys.stderr)
            time.sleep(30 * 2 ** i)
    raise SystemExit(f'{a}..{b}: query failed')


def encode(d, rows):
    t0 = int(datetime.fromisoformat(d).replace(tzinfo=UTC).timestamp())
    b, h = bytearray(b'RAF1' + struct.pack('<IH', t0, len(rows))), 0
    for r in rows:
        p = r['p']
        b += int(r['icao'], 16).to_bytes(3, 'little') + uv(len(p))
        q = (0,) * 5
        for *v, tr in p:
            b += uv(v[0] - q[0]) + b''.join(sv(x - y) for x, y in
                                            zip(v[1:], q[1:]))
            b.append(round(tr * 256 / 360) & 255)
            q = v
        h += sum(y[0] - x[0] for x, y in zip(p, p[1:])
                 if y[0] - x[0] <= 600 and min(x[3], y[3]) >= 0)
    (D / f'{d}.bin').write_bytes(b)
    return round(h / 3600, 1)


ac = {a['hex']: a for a in json.loads((D / 'aircraft.json').read_text())}
mf, mm = D / 'manifest.json', D / 'aircraft_meta.json'
man = json.loads(mf.read_text()) if mf.exists() else {}
meta = json.loads(mm.read_text()) if mm.exists() else {}
y = date.today() - timedelta(1)
x = [date.fromisoformat(v) for v in sys.argv[1:3]] or [y - timedelta(6), y]
days = [x[0] + timedelta(i) for i in range((x[-1] - x[0]).days + 1)]
if len(sys.argv) < 2: days = [d for d in days if not man.get(str(d))]
runs = []  # each query scans the whole table, so batch up to 7 days
for d in days:
    if runs and d - runs[-1][-1] == timedelta(1) and len(runs[-1]) < 7:
        runs[-1].append(d)
    else: runs.append([d])
for run in runs:
    rows = query(run[0], run[-1], list(ac))
    for d in map(str, run):
        day = [r for r in rows if r['d'] == d]
        man[d] = encode(d, day)
        for r in day:
            s = ac.get(r['icao'], {})
            meta[r['icao']] = {
                'reg': clean(s.get('reg', '')) or r['r'],
                'type': clean(s.get('type', '')) or r['desc'],
                'icao_type': clean(s.get('icao_type', '')) or r['t'],
                'unit': clean(s.get('unit', '')), 'heli': r['heli']}
        print(f'{d}: {len(day)} aircraft, {man[d]}h', flush=True)
    mf.write_text(json.dumps(dict(sorted(man.items())), separators=(',', ':')))
    mm.write_text(json.dumps(meta, separators=(',', ':')))
