const T = { TIME: 0, LAT: 1, LON: 2, ALT: 3, SPEED: 4, TRACK: 5 };

// Color palette for aircraft type groups
const TYPE_COLORS = {
  fighter:    '#e05555',
  transport:  '#5588cc',
  helicopter: '#44aa77',
  tanker:     '#cc9944',
  patrol:     '#9977bb',
  trainer:    '#55aabb',
  other:      '#778899',
};

const TYPE_MAP = {
  EUFI: 'fighter', F35: 'fighter', HURR: 'fighter', SPIT: 'fighter',
  C130: 'transport', A400: 'transport', C17: 'transport', A332: 'transport',
  GLF6: 'transport', B737: 'transport', BAE146: 'transport', HS25: 'transport',
  H25A: 'transport', BALL: 'transport',
  H47: 'helicopter', WILD: 'helicopter', LYNX: 'helicopter', MLIN: 'helicopter',
  PUMA: 'helicopter', EC35: 'helicopter', A139: 'helicopter', AS32: 'helicopter',
  EC75: 'helicopter', S61: 'helicopter', HDJF: 'helicopter',
  A330: 'tanker', KC3: 'tanker', MRTT: 'tanker', VC10: 'tanker',
  P8: 'patrol', NIM: 'patrol', SENT: 'patrol', SHAD: 'patrol',
  HAR: 'fighter', HAWK: 'trainer', PHOE: 'trainer', TEXT: 'trainer',
  PC21: 'trainer', T6: 'trainer', TUCA: 'trainer', GR4: 'fighter',
  CNBR: 'patrol', RC12: 'patrol',
};

function classifyType(icaoType) {
  return TYPE_MAP[icaoType] || 'other';
}

function typeColor(icaoType) {
  return TYPE_COLORS[classifyType(icaoType)] || TYPE_COLORS.other;
}


// ── RAF1 binary decoder ─────────────────────────────────────────────

function parseRAF1(buf) {
  const bytes = new Uint8Array(buf);
  const view = new DataView(buf);

  // Header
  const magic = String.fromCharCode(bytes[0], bytes[1], bytes[2], bytes[3]);
  if (magic !== 'RAF1') throw new Error('Not a RAF1 file');
  const dayEpoch = view.getUint32(4, true);
  const flightCount = view.getUint16(8, true);
  let pos = 10;

  function readUvarint() {
    let value = 0, shift = 0;
    while (true) {
      const b = bytes[pos++];
      value |= (b & 0x7F) << shift;
      if (!(b & 0x80)) return value;
      shift += 7;
    }
  }

  function readSvarint() {
    const n = readUvarint();
    return (n >>> 1) ^ -(n & 1);
  }

  const flights = [];

  for (let i = 0; i < flightCount; i++) {
    // uint24 LE
    const hex = bytes[pos] | (bytes[pos + 1] << 8) | (bytes[pos + 2] << 16);
    pos += 3;
    const pointCount = readUvarint();
    const hexStr = hex.toString(16).padStart(6, '0');

    const trace = [];
    let time = 0, lat = 0, lon = 0, alt = 0, speed = 0;

    for (let j = 0; j < pointCount; j++) {
      time += readUvarint();
      lat += readSvarint();
      lon += readSvarint();
      alt += readSvarint();
      speed += readSvarint();
      const track = bytes[pos++];

      trace.push([
        time,              // T=0: seconds-of-day
        lat / 1e5,         // T=1: latitude
        lon / 1e5,         // T=2: longitude
        alt * 25,          // T=3: altitude in feet (-25 = ground)
        speed,             // T=4: speed in knots
        track * 360 / 256, // T=5: heading in degrees
      ]);
    }

    flights.push({
      _hexStr: hexStr,
      timestamp: dayEpoch,
      trace,
    });
  }

  return flights;
}

async function loadMeta() {
  try {
    const resp = await fetch('data/aircraft_meta.json');
    return resp.ok ? await resp.json() : {};
  } catch { return {}; }
}


// ── Day cache — lazy-load per-day .bin files ────────────────────────

const dayCache = new Map();

async function loadDay(date) {
  if (dayCache.has(date)) return dayCache.get(date);

  const resp = await fetch(`data/${date}.bin`);
  if (!resp.ok) return [];

  const flights = parseRAF1(await resp.arrayBuffer());
  dayCache.set(date, flights);
  return flights;
}


// ── Build GeoJSON from trace data ───────────────────────────────────

// Max seconds between consecutive ADS-B reports before we consider it a gap
const GAP_THRESHOLD = 180;

function traceToGeoJSON(flights, meta) {
  const features = [];

  for (const flight of flights) {
    const trace = flight.trace;
    if (!trace || trace.length < 2) continue;

    const hex = flight._hexStr;
    const m = meta[hex] || {};
    let maxAlt = 0;
    let totalSpeed = 0;
    let pointCount = 0;

    // Collect valid points with their timestamps
    const points = [];
    for (const pt of trace) {
      const lat = pt[T.LAT];
      const lon = pt[T.LON];
      const alt = pt[T.ALT] || 0;
      if (lat == null || lon == null) continue;
      points.push({ lon, lat, time: pt[T.TIME], speed: pt[T.SPEED] || 0, alt });
      if (alt > maxAlt) maxAlt = alt;
      totalSpeed += pt[T.SPEED] || 0;
      pointCount++;
    }

    if (points.length < 2) continue;

    const flightDate = flight.timestamp
      ? new Date(flight.timestamp * 1000).toISOString().slice(0, 10)
      : '';

    const baseProps = {
      hex,
      reg: m.reg || '',
      type: m.type || '',
      icao_type: m.icao_type || '',
      unit: m.unit || '',
      date: flightDate,
      maxAlt,
      avgSpeed: pointCount ? Math.round(totalSpeed / pointCount) : 0,
      points: pointCount,
      color: typeColor(m.icao_type || ''),
      typeGroup: classifyType(m.icao_type || ''),
    };

    const baseTime = flight.timestamp || 0;

    function emitSegment(seg, gap) {
      features.push({
        type: 'Feature',
        geometry: { type: 'LineString', coordinates: seg.map(p => [p.lon, p.lat]) },
        properties: { ...baseProps, gap, times: JSON.stringify(seg.map(p => baseTime + p.time)) },
      });
    }

    // Split into solid and gap segments based on time between points
    let segment = [points[0]];
    for (let i = 1; i < points.length; i++) {
      const dt = points[i].time - points[i - 1].time;
      if (dt > GAP_THRESHOLD) {
        if (segment.length >= 2) emitSegment(segment, false);
        emitSegment([segment[segment.length - 1], points[i]], true);
        segment = [points[i]];
      } else {
        segment.push(points[i]);
      }
    }
    if (segment.length >= 2) emitSegment(segment, false);
  }

  return { type: 'FeatureCollection', features };
}

function lastPositions(flights, meta) {
  const features = [];
  for (const flight of flights) {
    const trace = flight.trace;
    if (!trace || trace.length === 0) continue;

    const hex = flight._hexStr;
    const m = meta[hex] || {};

    for (let i = trace.length - 1; i >= 0; i--) {
      const pt = trace[i];
      if (pt[T.LAT] != null && pt[T.LON] != null) {
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [pt[T.LON], pt[T.LAT]] },
          properties: {
            hex,
            reg: m.reg || '',
            type: m.type || '',
            icao_type: m.icao_type || '',
            unit: m.unit || '',
            alt: pt[T.ALT] || 0,
            speed: pt[T.SPEED] || 0,
            track: pt[T.TRACK] || 0,
            color: typeColor(m.icao_type || ''),
          },
        });
        break;
      }
    }
  }
  return { type: 'FeatureCollection', features };
}


// ── Hash helpers (date param alongside MapLibre's built-in map hash) ─

function getHashParam(key) {
  const m = location.hash.match(new RegExp(`(?:^#|&)${key}=([^&]*)`));
  return m ? decodeURIComponent(m[1]) : null;
}

function setHashParam(key, value) {
  const hash = location.hash.slice(1);
  const re = new RegExp(`((?:^|&)${key}=)[^&]*`);
  if (re.test(hash)) {
    location.hash = hash.replace(re, `$1${encodeURIComponent(value)}`);
  } else {
    location.hash = hash + (hash ? '&' : '') + `${key}=${encodeURIComponent(value)}`;
  }
}

// ── Map setup ───────────────────────────────────────────────────────

const mapStyle = {
  version: 8,
  glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
  projection: { type: 'globe' },
  sky: { 'atmosphere-blend': 0 },
  sources: {
    carto: {
      type: 'raster',
      tiles: ['https://a.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}@2x.png'],
      tileSize: 256,
      attribution: '&copy; <a href="https://carto.com">CARTO</a> &copy; <a href="https://openstreetmap.org">OSM</a>',
    },
    labels: {
      type: 'vector',
      url: 'https://tiles.openfreemap.org/planet',
    },
  },
  layers: [
    {
      id: 'basemap', type: 'raster', source: 'carto',
    },
    {
      id: 'country-borders', type: 'line', source: 'labels',
      'source-layer': 'boundary',
      filter: ['==', ['get', 'admin_level'], 2],
      paint: {
        'line-color': 'rgba(255, 255, 255, 0.15)',
        'line-width': ['interpolate', ['linear'], ['zoom'], 1, 0.5, 6, 1.5],
      },
    },
    {
      id: 'country-labels', type: 'symbol', source: 'labels',
      'source-layer': 'place',
      filter: ['==', ['get', 'class'], 'country'],
      minzoom: 2,
      layout: {
        'symbol-sort-key': ['get', 'rank'],
        'text-field': ['coalesce', ['get', 'name:en'], ['get', 'name']],
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 2, 10, 6, 14],
        'text-transform': 'uppercase',
        'text-letter-spacing': 0.15,
        'text-max-width': 8,
      },
      paint: {
        'text-color': 'rgba(255, 255, 255, 0.85)',
        'text-halo-color': 'rgba(0, 0, 0, 0.6)',
        'text-halo-width': 1.5,
      },
    },
    {
      id: 'state-labels', type: 'symbol', source: 'labels',
      'source-layer': 'place',
      filter: ['==', ['get', 'class'], 'state'],
      minzoom: 4,
      layout: {
        'symbol-sort-key': ['get', 'rank'],
        'text-field': ['coalesce', ['get', 'name:en'], ['get', 'name']],
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 4, 9, 8, 12],
        'text-letter-spacing': 0.1,
        'text-max-width': 8,
      },
      paint: {
        'text-color': 'rgba(255, 255, 255, 0.6)',
        'text-halo-color': 'rgba(0, 0, 0, 0.5)',
        'text-halo-width': 1,
      },
    },
    {
      id: 'city-labels', type: 'symbol', source: 'labels',
      'source-layer': 'place',
      filter: ['in', ['get', 'class'], ['literal', ['city', 'town']]],
      minzoom: 4,
      layout: {
        'symbol-sort-key': ['get', 'rank'],
        'text-field': ['coalesce', ['get', 'name:en'], ['get', 'name']],
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 4, 10, 10, 14, 14, 18],
        'text-max-width': 8,
      },
      paint: {
        'text-color': 'rgba(255, 255, 255, 0.9)',
        'text-halo-color': 'rgba(0, 0, 0, 0.6)',
        'text-halo-width': 1.5,
      },
    },
    {
      id: 'village-labels', type: 'symbol', source: 'labels',
      'source-layer': 'place',
      filter: ['in', ['get', 'class'], ['literal', ['village', 'suburb', 'neighbourhood']]],
      minzoom: 10,
      layout: {
        'symbol-sort-key': ['get', 'rank'],
        'text-field': ['coalesce', ['get', 'name:en'], ['get', 'name']],
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 10, 10, 14, 14],
        'text-max-width': 8,
      },
      paint: {
        'text-color': 'rgba(255, 255, 255, 0.7)',
        'text-halo-color': 'rgba(0, 0, 0, 0.5)',
        'text-halo-width': 1,
      },
    },
  ],
};

let map;

map = new maplibregl.Map({
  container: 'map',
  style: mapStyle,
  center: [-2.5, 54.5],
  zoom: 4,
  minZoom: 2,
  hash: 'map',
  attributionControl: false,
});

map.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');
initApp();

// ── Tooltip ─────────────────────────────────────────────────────────

const tooltip = document.getElementById('tooltip');

function nearestTime(e, feature) {
  const times = feature.properties.times;
  if (!times) return '';
  const coords = feature.geometry.coordinates;
  const parsed = JSON.parse(times);
  if (!coords || !parsed.length) return '';
  const lngLat = e.lngLat;
  let best = 0, bestDist = Infinity;
  for (let i = 0; i < coords.length; i++) {
    const dx = coords[i][0] - lngLat.lng;
    const dy = coords[i][1] - lngLat.lat;
    const d = dx * dx + dy * dy;
    if (d < bestDist) { bestDist = d; best = i; }
  }
  return new Date(parsed[best] * 1000).toISOString().slice(11, 19) + ' UTC';
}

function showTooltip(e, props, feature) {
  const time = feature ? nearestTime(e, feature) : '';
  tooltip.innerHTML = `
    <div class="tt-hex">${props.hex}</div>
    <div class="tt-label">Aircraft</div>
    <div class="tt-value">${props.type || props.icao_type || 'Unknown'}</div>
    ${props.reg ? `<div class="tt-label">Registration</div><div class="tt-value">${props.reg}</div>` : ''}
    ${props.unit ? `<div class="tt-label">Unit</div><div class="tt-value">${props.unit}</div>` : ''}
    ${props.date ? `<div class="tt-label">Date</div><div class="tt-value">${props.date}${time ? ' · ' + time : ''}</div>` : ''}
    ${props.maxAlt != null ? `<div class="tt-label">Max altitude</div><div class="tt-value">${props.maxAlt.toLocaleString()} ft</div>` : ''}
    ${props.avgSpeed ? `<div class="tt-label">Avg speed</div><div class="tt-value">${props.avgSpeed} kts</div>` : ''}
    ${props.alt != null && props.speed != null ? `<div class="tt-label">Last position</div><div class="tt-value">${props.alt.toLocaleString()} ft · ${props.speed} kts</div>` : ''}
  `;
  tooltip.style.display = 'block';
  tooltip.style.left = e.point.x + 16 + 'px';
  tooltip.style.top = e.point.y + 'px';
}

function hideTooltip() {
  tooltip.style.display = 'none';
}


// ── Legend with filtering ────────────────────────────────────────────

const hiddenTypes = new Set();
let currentFlights = [];
let currentMeta = {};
let currentDate = '';

function updateMapData() {
  const filtered = currentFlights.filter(f => {
    const hex = f._hexStr;
    const m = currentMeta[hex] || {};
    const group = classifyType(m.icao_type || '');
    return !hiddenTypes.has(group);
  });
  const tracks = traceToGeoJSON(filtered, currentMeta);
  const dots = lastPositions(filtered, currentMeta);
  map.getSource('tracks').setData(tracks);
  map.getSource('dots').setData(dots);
}

function buildLegend() {
  const el = document.getElementById('legend');
  el.classList.add('active');
}

function updateLegendCounts() {
  const el = document.getElementById('legend-items');
  const counts = {};
  for (const f of currentFlights) {
    const hex = f._hexStr;
    const m = currentMeta[hex] || {};
    const group = classifyType(m.icao_type || '');
    counts[group] = (counts[group] || 0) + 1;
  }

  const sorted = Object.entries(TYPE_COLORS)
    .filter(([name]) => counts[name])
    .sort((a, b) => a[0].localeCompare(b[0]));

  el.innerHTML = sorted.map(([name, color]) => {
    const active = !hiddenTypes.has(name);
    return `<div class="legend-item${active ? '' : ' dimmed'}" data-type="${name}">
      <span class="legend-circle" style="background:${active ? color : 'transparent'};border:1.5px solid ${color}"></span>
      <span class="legend-name">${name.charAt(0).toUpperCase() + name.slice(1)}</span>
      <span class="legend-cnt">${counts[name] || 0}</span>
    </div>`;
  }).join('');

  // Bind click handlers
  for (const item of el.querySelectorAll('.legend-item')) {
    item.addEventListener('click', () => {
      const type = item.dataset.type;
      if (hiddenTypes.has(type)) hiddenTypes.delete(type);
      else hiddenTypes.add(type);
      updateLegendCounts();
      updateMapData();
    });
  }
}


// ── Date slider with lazy loading ───────────────────────────────────

function setupDateSlider(dates, meta, flightCounts) {
  const slider = document.getElementById('slider');
  const labels = document.getElementById('slider-labels');
  const container = document.getElementById('date-slider');
  const histogram = document.getElementById('slider-histogram');

  if (dates.length === 0) return;

  slider.min = 0;
  slider.max = dates.length - 1;
  slider.value = dates.length - 1; // default to latest
  container.classList.add('active');

  const ticks = document.getElementById('slider-ticks');
  ticks.innerHTML = dates.map((_, i) => {
    const pct = dates.length > 1 ? (i / (dates.length - 1)) * 100 : 50;
    return `<span class="tick" style="left:calc(5px + ${pct} * (100% - 10px) / 100)"></span>`;
  }).join('');

  // Build histogram bars positioned to match tick marks
  histogram.innerHTML = dates.map((_, i) => {
    const pct = dates.length > 1 ? (i / (dates.length - 1)) * 100 : 50;
    return `<div class="histo-bar" data-idx="${i}" style="left:calc(5px + ${pct} * (100% - 10px) / 100)"></div>`;
  }).join('');

  function updateHistogramActive() {
    const idx = parseInt(slider.value);
    for (const bar of histogram.querySelectorAll('.histo-bar')) {
      bar.classList.toggle('active', parseInt(bar.dataset.idx) === idx);
    }
  }

  // Set histogram bar heights from manifest counts
  if (flightCounts.length) {
    const max = Math.max(...flightCounts, 1);
    const bars = histogram.querySelectorAll('.histo-bar');
    bars.forEach((bar, i) => {
      const pct = Math.max((flightCounts[i] / max) * 100, 4);
      bar.style.height = `${pct}%`;
    });
  }
  updateHistogramActive();
  updateLabel(dates[parseInt(slider.value)]);

  let loading = false;

  function updateLabel(date) {
    const idx = dates.indexOf(date);
    const count = flightCounts[idx];
    const current = count !== undefined ? `${date} · ${Math.round(count)}h flown` : date;
    labels.innerHTML = `
      <span>${dates[0]}</span>
      <span class="current">${current}</span>
      <span>${dates[dates.length - 1]}</span>
    `;
  }

  async function showDate() {
    if (loading) return;
    const idx = parseInt(slider.value);
    const date = dates[idx];

    loading = true;
    updateLabel(date);
    updateHistogramActive();

    currentFlights = await loadDay(date);
    currentMeta = meta;
    currentDate = date;
    updateLegendCounts();
    updateMapData();
    setHashParam('date', date);

    loading = false;
  }

  slider.addEventListener('input', showDate);

  // Click on histogram bar jumps slider to that date
  histogram.addEventListener('click', (e) => {
    const bar = e.target.closest('.histo-bar');
    if (!bar) return;
    slider.value = bar.dataset.idx;
    showDate();
  });

  // return loader for initial date
  return showDate;
}


// ── Shipments mode ──────────────────────────────────────────────────

const PRODUCT_COLORS = {
  'JP-5':  '#e05555',
  'JP-8':  '#cc9944',
  'JP-1':  '#55aabb',
  'F-76':  '#5588cc',
  'Jet A-1': '#9977bb',
  'Other': '#778899',
};

const PRODUCT_ORDER = ['JP-5', 'JP-8', 'JP-1', 'F-76', 'Jet A-1', 'Other'];

function shipmentProductColor(product) {
  return PRODUCT_COLORS[product] || PRODUCT_COLORS['Other'];
}

function shipmentProductGroup(product) {
  return PRODUCT_COLORS[product] ? product : 'Other';
}

function parseShipmentsBin(buf) {
  const view = new DataView(buf);
  const count = view.getUint32(0, true);
  let offset = 4;
  const shipments = [];

  for (let i = 0; i < count; i++) {
    const imo = view.getUint32(offset, true);
    const len = view.getUint32(offset + 4, true);
    offset += 8;
    const bytes = new Uint8Array(buf, offset, len);
    const text = new TextDecoder().decode(bytes);
    const data = JSON.parse(text);
    data._imo = imo;
    shipments.push(data);
    offset += len;
  }

  return shipments;
}

let shipmentsCache = null;
const hiddenProducts = new Set();

async function loadShipments() {
  if (shipmentsCache) return shipmentsCache;
  const resp = await fetch('data/shipments.bin');
  if (!resp.ok) return [];
  shipmentsCache = parseShipmentsBin(await resp.arrayBuffer());
  return shipmentsCache;
}

function shipmentsToGeoJSON(shipments) {
  const features = [];

  for (const s of shipments) {
    const group = shipmentProductGroup(s.product);
    if (hiddenProducts.has(group)) continue;
    const color = shipmentProductColor(s.product);

    const props = {
      id: s.id,
      vessel: s.vessel || `IMO ${s._imo}`,
      product: s.product,
      status: s.status,
      start: s.start,
      end: s.end,
      origin: s.origin ? s.origin[2] : '',
      originCountry: s.origin ? s.origin[3] : '',
      dest: s.dest ? s.dest[2] : '',
      destCountry: s.dest ? s.dest[3] : '',
      mass: s.mass,
      vol: s.vol,
      buyer: s.buyer || '',
      seller: s.seller || '',
      color,
      group,
    };

    // Vessel AIS trace
    if (s.trace && s.trace.length >= 2) {
      const coords = s.trace.map(pt => [pt[2], pt[1]]); // [lon, lat]
      features.push({
        type: 'Feature',
        geometry: { type: 'LineString', coordinates: coords },
        properties: { ...props, layer: 'trace' },
      });
    }

    // Origin → Destination arc (when no trace or as fallback)
    if (s.origin && s.dest) {
      features.push({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [s.origin[0], s.origin[1]] },
        properties: { ...props, layer: 'port', portType: 'origin', portName: s.origin[2] },
      });
      features.push({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [s.dest[0], s.dest[1]] },
        properties: { ...props, layer: 'port', portType: 'dest', portName: s.dest[2] },
      });
    }
  }

  return { type: 'FeatureCollection', features };
}

function shipmentPorts(shipments) {
  const features = [];
  for (const s of shipments) {
    const group = shipmentProductGroup(s.product);
    if (hiddenProducts.has(group)) continue;
    const color = shipmentProductColor(s.product);
    const props = { vessel: s.vessel, product: s.product, color, group };

    if (s.origin) {
      features.push({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [s.origin[0], s.origin[1]] },
        properties: { ...props, portType: 'origin', portName: s.origin[2], country: s.origin[3] },
      });
    }
    if (s.dest) {
      features.push({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [s.dest[0], s.dest[1]] },
        properties: { ...props, portType: 'dest', portName: s.dest[2], country: s.dest[3] },
      });
    }
  }
  return { type: 'FeatureCollection', features };
}


// ── Main ────────────────────────────────────────────────────────────

let currentMode = 'flights';
const EMPTY_FC = { type: 'FeatureCollection', features: [] };

function initApp() {
  map.on('load', async () => {
    try {
      const [manifestResp, meta] = await Promise.all([
        fetch('data/manifest.json'),
        loadMeta(),
      ]);

      if (!manifestResp.ok) throw new Error('No manifest');
      const manifest = await manifestResp.json();
      const dates = Array.isArray(manifest) ? manifest : Object.keys(manifest).sort();
      const flightCounts = Array.isArray(manifest) ? [] : dates.map(d => manifest[d]);

      map.addSource('tracks', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'tracks-glow', type: 'line', source: 'tracks',
        filter: ['!=', ['get', 'gap'], true],
        paint: { 'line-color': ['get', 'color'], 'line-width': 4, 'line-opacity': 0.15, 'line-blur': 4 },
      });
      map.addLayer({
        id: 'tracks-line', type: 'line', source: 'tracks',
        filter: ['!=', ['get', 'gap'], true],
        paint: { 'line-color': ['get', 'color'], 'line-width': 1.5, 'line-opacity': 0.7 },
      });
      map.addLayer({
        id: 'tracks-gap', type: 'line', source: 'tracks',
        filter: ['==', ['get', 'gap'], true],
        paint: { 'line-color': ['get', 'color'], 'line-width': 1.5, 'line-opacity': 0.35, 'line-dasharray': [2, 4] },
      });
      // Wide invisible layer for easier mouse/touch interaction
      map.addLayer({
        id: 'tracks-hit', type: 'line', source: 'tracks',
        paint: { 'line-color': '#000', 'line-width': 14, 'line-opacity': 0.01 },
      });

      // Chevron icon for direction
      const sz = 16, cv = document.createElement('canvas');
      cv.width = sz; cv.height = sz;
      const cx = cv.getContext('2d');
      cx.strokeStyle = '#fff';
      cx.lineWidth = 2.5;
      cx.lineCap = 'round';
      cx.lineJoin = 'round';
      cx.beginPath();
      cx.moveTo(2, 12);
      cx.lineTo(8, 6);
      cx.lineTo(14, 12);
      cx.stroke();
      map.addImage('chevron', { width: sz, height: sz, data: cx.getImageData(0, 0, sz, sz).data }, { sdf: true });

      map.addLayer({
        id: 'tracks-arrows', type: 'symbol', source: 'tracks',
        layout: {
          'symbol-placement': 'line',
          'symbol-spacing': 80,
          'icon-image': 'chevron',
          'icon-size': 1,
          'icon-rotate': 90,
          'icon-rotation-alignment': 'map',
          'icon-allow-overlap': true,
          'icon-ignore-placement': true,
        },
        paint: {
          'icon-color': ['get', 'color'],
          'icon-opacity': 0.5,
        },
      });

      map.addSource('dots', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'dots-glow', type: 'circle', source: 'dots',
        paint: { 'circle-radius': 6, 'circle-color': ['get', 'color'], 'circle-opacity': 0.25, 'circle-blur': 1 },
      });
      map.addLayer({
        id: 'dots', type: 'circle', source: 'dots',
        paint: { 'circle-radius': 3, 'circle-color': ['get', 'color'], 'circle-opacity': 0.9 },
      });

      // RAF bases
      const basesFC = {
        type: 'FeatureCollection',
        features: RAF_BASES.map(b => ({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: b.coords },
          properties: { name: b.name, icao: b.icao || '' },
        })),
      };
      map.addSource('bases', { type: 'geojson', data: basesFC });
      map.addLayer({
        id: 'bases', type: 'circle', source: 'bases',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 2, 3, 8, 5],
          'circle-color': 'transparent',
          'circle-stroke-color': 'rgba(255, 255, 255, 0.5)',
          'circle-stroke-width': 1.5,
        },
      });
      map.addLayer({
        id: 'bases-labels', type: 'symbol', source: 'bases',
        minzoom: 6,
        layout: {
          'text-field': ['get', 'name'],
          'text-font': ['Noto Sans Regular'],
          'text-size': 10,
          'text-offset': [0, 1.2],
          'text-anchor': 'top',
        },
        paint: {
          'text-color': 'rgba(255, 255, 255, 0.6)',
          'text-halo-color': 'rgba(0, 0, 0, 0.6)',
          'text-halo-width': 1,
        },
      });

      // ── Shipment layers ──
      map.addSource('ship-tracks', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'ship-tracks-glow', type: 'line', source: 'ship-tracks',
        paint: { 'line-color': ['get', 'color'], 'line-width': 3, 'line-opacity': 0.15, 'line-blur': 3 },
      });
      map.addLayer({
        id: 'ship-tracks-line', type: 'line', source: 'ship-tracks',
        paint: { 'line-color': ['get', 'color'], 'line-width': 1.2, 'line-opacity': 0.6 },
      });
      map.addLayer({
        id: 'ship-tracks-hit', type: 'line', source: 'ship-tracks',
        paint: { 'line-color': '#000', 'line-width': 14, 'line-opacity': 0.01 },
      });

      map.addSource('ship-ports', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'ship-ports-glow', type: 'circle', source: 'ship-ports',
        paint: { 'circle-radius': 5, 'circle-color': ['get', 'color'], 'circle-opacity': 0.2, 'circle-blur': 1 },
      });
      map.addLayer({
        id: 'ship-ports', type: 'circle', source: 'ship-ports',
        paint: {
          'circle-radius': ['case', ['==', ['get', 'portType'], 'dest'], 3.5, 2.5],
          'circle-color': ['get', 'color'],
          'circle-opacity': 0.8,
        },
      });

      setShipmentLayersVisible(false);

      // ── Mode switching ──
      function setFlightLayersVisible(v) {
        const vis = v ? 'visible' : 'none';
        for (const id of ['tracks-glow','tracks-line','tracks-gap','tracks-hit','tracks-arrows','dots-glow','dots','bases','bases-labels']) {
          map.setLayoutProperty(id, 'visibility', vis);
        }
      }
      function setShipmentLayersVisible(v) {
        const vis = v ? 'visible' : 'none';
        for (const id of ['ship-tracks-glow','ship-tracks-line','ship-tracks-hit','ship-ports-glow','ship-ports']) {
          map.setLayoutProperty(id, 'visibility', vis);
        }
      }

      async function switchMode(mode) {
        currentMode = mode;
        document.querySelectorAll('.mode-btn').forEach(b => b.classList.toggle('active', b.dataset.mode === mode));

        if (mode === 'flights') {
          setShipmentLayersVisible(false);
          setFlightLayersVisible(true);
          document.getElementById('date-slider').classList.add('active');
          document.getElementById('legend').querySelector('h4').textContent = 'Aircraft type';
          updateLegendCounts();
          updateMapData();
        } else {
          setFlightLayersVisible(false);
          setShipmentLayersVisible(true);
          document.getElementById('date-slider').classList.remove('active');

          const shipments = await loadShipments();
          const traces = shipmentsToGeoJSON(shipments);
          const traceLines = { type: 'FeatureCollection', features: traces.features.filter(f => f.geometry.type === 'LineString') };
          const ports = shipmentPorts(shipments);
          map.getSource('ship-tracks').setData(traceLines);
          map.getSource('ship-ports').setData(ports);

          updateShipmentLegend(shipments);
        }
      }

      function updateShipmentLegend(shipments) {
        const el = document.getElementById('legend-items');
        const legend = document.getElementById('legend');
        legend.querySelector('h4').textContent = 'Fuel type';

        const counts = {};
        for (const s of shipments) {
          const g = shipmentProductGroup(s.product);
          counts[g] = (counts[g] || 0) + 1;
        }

        el.innerHTML = PRODUCT_ORDER
          .filter(name => counts[name])
          .map(name => {
            const color = PRODUCT_COLORS[name];
            const active = !hiddenProducts.has(name);
            return `<div class="legend-item${active ? '' : ' dimmed'}" data-type="${name}">
              <span class="legend-circle" style="background:${active ? color : 'transparent'};border:1.5px solid ${color}"></span>
              <span class="legend-name">${name}</span>
              <span class="legend-cnt">${counts[name] || 0}</span>
            </div>`;
          }).join('');

        for (const item of el.querySelectorAll('.legend-item')) {
          item.addEventListener('click', async () => {
            const type = item.dataset.type;
            if (hiddenProducts.has(type)) hiddenProducts.delete(type);
            else hiddenProducts.add(type);
            const s = await loadShipments();
            const traces = shipmentsToGeoJSON(s);
            const traceLines = { type: 'FeatureCollection', features: traces.features.filter(f => f.geometry.type === 'LineString') };
            map.getSource('ship-tracks').setData(traceLines);
            map.getSource('ship-ports').setData(shipmentPorts(s));
            updateShipmentLegend(s);
          });
        }
      }

      for (const btn of document.querySelectorAll('.mode-btn')) {
        btn.addEventListener('click', () => switchMode(btn.dataset.mode));
      }

      buildLegend();
      const showDate = setupDateSlider(dates, meta, flightCounts);

      // If hash specifies a date, jump the slider to it
      const hashDate = getHashParam('date');
      if (hashDate && dates.includes(hashDate)) {
        document.getElementById('slider').value = dates.indexOf(hashDate);
      }

      await showDate();

      // Shipment tooltip
      function showShipmentTooltip(e, props) {
        const mass = props.mass ? `${Math.round(props.mass).toLocaleString()} t` : '';
        tooltip.innerHTML = `
          <div class="tt-hex">${props.vessel}</div>
          <div class="tt-label">Product</div>
          <div class="tt-value">${props.product}</div>
          ${props.origin ? `<div class="tt-label">Origin</div><div class="tt-value">${props.origin}${props.originCountry ? ', ' + props.originCountry : ''}</div>` : ''}
          ${props.dest ? `<div class="tt-label">Destination</div><div class="tt-value">${props.dest}${props.destCountry ? ', ' + props.destCountry : ''}</div>` : ''}
          ${mass ? `<div class="tt-label">Mass</div><div class="tt-value">${mass}</div>` : ''}
          ${props.buyer ? `<div class="tt-label">Buyer</div><div class="tt-value">${props.buyer}</div>` : ''}
          <div class="tt-label">Period</div>
          <div class="tt-value">${props.start || '?'} → ${props.end || '?'}</div>
          <div class="tt-label">Status</div>
          <div class="tt-value">${props.status || '?'}</div>
        `;
        tooltip.style.display = 'block';
        tooltip.style.left = e.point.x + 16 + 'px';
        tooltip.style.top = e.point.y + 'px';
      }

      for (const layer of ['ship-tracks-hit', 'ship-ports']) {
        map.on('mouseenter', layer, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', layer, () => {
          map.getCanvas().style.cursor = '';
          hideTooltip();
          map.setPaintProperty('ship-tracks-line', 'line-opacity', 0.6);
          map.setPaintProperty('ship-tracks-line', 'line-width', 1.2);
        });
        map.on('mousemove', layer, (e) => {
          const props = e.features[0].properties;
          if (layer === 'ship-ports') {
            tooltip.innerHTML = `
              <div class="tt-hex">${props.portName}</div>
              <div class="tt-label">${props.portType === 'origin' ? 'Origin' : 'Destination'}</div>
              <div class="tt-value">${props.country || ''}</div>
            `;
            tooltip.style.display = 'block';
            tooltip.style.left = e.point.x + 16 + 'px';
            tooltip.style.top = e.point.y + 'px';
          } else {
            showShipmentTooltip(e, props);
            map.setPaintProperty('ship-tracks-line', 'line-opacity', [
              'case', ['==', ['get', 'id'], props.id], 1, 0.15,
            ]);
            map.setPaintProperty('ship-tracks-line', 'line-width', [
              'case', ['==', ['get', 'id'], props.id], 2.5, 0.8,
            ]);
          }
        });
      }

      // Base hover
      map.on('mouseenter', 'bases', () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', 'bases', () => { map.getCanvas().style.cursor = ''; hideTooltip(); });
      map.on('mousemove', 'bases', (e) => {
        const p = e.features[0].properties;
        tooltip.innerHTML = `<div class="tt-hex">${p.name}</div>${p.icao ? `<div class="tt-label">ICAO</div><div class="tt-value">${p.icao}</div>` : ''}`;
        tooltip.style.display = 'block';
        tooltip.style.left = e.point.x + 16 + 'px';
        tooltip.style.top = e.point.y + 'px';
      });

      for (const layer of ['tracks-hit', 'dots']) {
        map.on('mouseenter', layer, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', layer, () => {
          map.getCanvas().style.cursor = '';
          hideTooltip();
          map.setPaintProperty('tracks-line', 'line-opacity', 0.7);
          map.setPaintProperty('tracks-line', 'line-width', 1.5);
          map.setPaintProperty('tracks-gap', 'line-opacity', 0.35);
        });
        map.on('mousemove', layer, (e) => {
          const feature = e.features[0];
          const props = feature.properties;
          showTooltip(e, props, feature);
          map.setPaintProperty('tracks-line', 'line-opacity', [
            'case', ['==', ['get', 'hex'], props.hex], 1, 0.25,
          ]);
          map.setPaintProperty('tracks-line', 'line-width', [
            'case', ['==', ['get', 'hex'], props.hex], 2.5, 1,
          ]);
          map.setPaintProperty('tracks-gap', 'line-opacity', [
            'case', ['==', ['get', 'hex'], props.hex], 0.6, 0.15,
          ]);
        });
        map.on('click', layer, (e) => {
          const props = e.features[0].properties;
          const { lng, lat } = e.lngLat;
          const zoom = map.getZoom().toFixed(1);
          window.open(`https://globe.adsbexchange.com/?icao=${props.hex}&lat=${lat.toFixed(3)}&lon=${lng.toFixed(3)}&zoom=${zoom}&showTrace=${currentDate}&trackLabels`, '_blank');
        });
      }
    } catch (err) {
      console.error('Failed to load flight data:', err);
    }
  });
}
