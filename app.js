// Trace array field indices (tar1090 / adsbexchange format)
const T = {
  TIME: 0, LAT: 1, LON: 2, ALT: 3, SPEED: 4,
  TRACK: 5, FLAG: 6, VERT_RATE: 7, EXT: 8, SIG_TYPE: 9,
};

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


// ── Parse binary container ──────────────────────────────────────────

function parseBin(buf) {
  const view = new DataView(buf);
  const count = view.getUint32(0, true);
  let offset = 4;
  const flights = [];

  for (let i = 0; i < count; i++) {
    const hexInt = view.getUint32(offset, true);
    const len = view.getUint32(offset + 4, true);
    offset += 8;
    const bytes = new Uint8Array(buf, offset, len);
    const text = new TextDecoder().decode(bytes);
    const data = JSON.parse(text);
    data._hexStr = hexInt.toString(16).padStart(6, '0');
    flights.push(data);
    offset += len;
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

  const flights = parseBin(await resp.arrayBuffer());
  dayCache.set(date, flights);
  return flights;
}


// ── Build GeoJSON from trace data ───────────────────────────────────

function traceToGeoJSON(flights, meta) {
  const features = [];

  for (const flight of flights) {
    const trace = flight.trace;
    if (!trace || trace.length < 2) continue;

    const hex = flight._hexStr || flight.icao;
    const m = meta[hex] || {};
    const coords = [];
    let maxAlt = 0;
    let totalSpeed = 0;
    let pointCount = 0;

    for (const pt of trace) {
      const lat = pt[T.LAT];
      const lon = pt[T.LON];
      const alt = pt[T.ALT] || 0;
      if (lat == null || lon == null) continue;
      coords.push([lon, lat]);
      if (alt > maxAlt) maxAlt = alt;
      totalSpeed += pt[T.SPEED] || 0;
      pointCount++;
    }

    if (coords.length < 2) continue;

    const flightDate = flight.timestamp
      ? new Date(flight.timestamp * 1000).toISOString().slice(0, 10)
      : '';

    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: coords },
      properties: {
        hex,
        reg: m.reg || flight.r || '',
        type: m.type || flight.desc || '',
        icao_type: m.icao_type || flight.t || '',
        unit: m.unit || '',
        date: flightDate,
        maxAlt,
        avgSpeed: pointCount ? Math.round(totalSpeed / pointCount) : 0,
        points: pointCount,
        color: typeColor(m.icao_type || flight.t || ''),
        typeGroup: classifyType(m.icao_type || flight.t || ''),
      },
    });
  }

  return { type: 'FeatureCollection', features };
}

function lastPositions(flights, meta) {
  const features = [];
  for (const flight of flights) {
    const trace = flight.trace;
    if (!trace || trace.length === 0) continue;

    const hex = flight._hexStr || flight.icao;
    const m = meta[hex] || {};

    for (let i = trace.length - 1; i >= 0; i--) {
      const pt = trace[i];
      if (pt[T.LAT] != null && pt[T.LON] != null) {
        features.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [pt[T.LON], pt[T.LAT]] },
          properties: {
            hex,
            reg: m.reg || flight.r || '',
            type: m.type || flight.desc || '',
            icao_type: m.icao_type || flight.t || '',
            unit: m.unit || '',
            alt: pt[T.ALT] || 0,
            speed: pt[T.SPEED] || 0,
            track: pt[T.TRACK] || 0,
            color: typeColor(m.icao_type || flight.t || ''),
          },
        });
        break;
      }
    }
  }
  return { type: 'FeatureCollection', features };
}


// ── Map setup ───────────────────────────────────────────────────────

const mapStyle = {
  version: 8,
  glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
  projection: { type: 'globe' },
  sky: { 'atmosphere-blend': ['interpolate', ['linear'], ['zoom'], 0, 1, 5, 1, 7, 0] },
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
  attributionControl: false,
});

map.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');
initApp();

// ── Tooltip ─────────────────────────────────────────────────────────

const tooltip = document.getElementById('tooltip');

function showTooltip(e, props) {
  tooltip.innerHTML = `
    <div class="tt-hex">${props.hex}</div>
    <div class="tt-label">Aircraft</div>
    <div class="tt-value">${props.type || props.icao_type || 'Unknown'}</div>
    ${props.reg ? `<div class="tt-label">Registration</div><div class="tt-value">${props.reg}</div>` : ''}
    ${props.unit ? `<div class="tt-label">Unit</div><div class="tt-value">${props.unit}</div>` : ''}
    ${props.date ? `<div class="tt-label">Date</div><div class="tt-value">${props.date}</div>` : ''}
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
    const hex = f._hexStr || f.icao;
    const m = currentMeta[hex] || {};
    const group = classifyType(m.icao_type || f.t || '');
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
    const hex = f._hexStr || f.icao;
    const m = currentMeta[hex] || {};
    const group = classifyType(m.icao_type || f.t || '');
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

function setupDateSlider(dates, meta) {
  const slider = document.getElementById('slider');
  const labels = document.getElementById('slider-labels');
  const container = document.getElementById('date-slider');

  if (dates.length === 0) return;

  slider.min = 0;
  slider.max = dates.length - 1;
  slider.value = dates.length - 1; // default to latest
  container.classList.add('active');

  let loading = false;

  function updateLabel(date) {
    labels.innerHTML = `
      <span>${dates[0]}</span>
      <span class="current">${date}</span>
      <span>${dates[dates.length - 1]}</span>
    `;
  }

  async function showDate() {
    if (loading) return;
    const idx = parseInt(slider.value);
    const date = dates[idx];

    loading = true;
    updateLabel(date);

    currentFlights = await loadDay(date);
    currentMeta = meta;
    currentDate = date;
    updateLegendCounts();
    updateMapData();

    loading = false;
  }

  slider.addEventListener('input', showDate);

  // return loader for initial date
  return showDate;
}


// ── Main ────────────────────────────────────────────────────────────

const EMPTY_FC = { type: 'FeatureCollection', features: [] };

function initApp() {
  map.on('load', async () => {
    try {
      const [manifestResp, meta] = await Promise.all([
        fetch('data/manifest.json'),
        loadMeta(),
      ]);

      if (!manifestResp.ok) throw new Error('No manifest');
      const dates = await manifestResp.json();

      map.addSource('tracks', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'tracks-glow', type: 'line', source: 'tracks',
        paint: { 'line-color': ['get', 'color'], 'line-width': 4, 'line-opacity': 0.15, 'line-blur': 4 },
      });
      map.addLayer({
        id: 'tracks-line', type: 'line', source: 'tracks',
        paint: { 'line-color': ['get', 'color'], 'line-width': 1.5, 'line-opacity': 0.7 },
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

      buildLegend();
      const showDate = setupDateSlider(dates, meta);
      await showDate();

      for (const layer of ['tracks-line', 'dots']) {
        map.on('mouseenter', layer, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', layer, () => {
          map.getCanvas().style.cursor = '';
          hideTooltip();
          map.setPaintProperty('tracks-line', 'line-opacity', 0.7);
          map.setPaintProperty('tracks-line', 'line-width', 1.5);
        });
        map.on('mousemove', layer, (e) => {
          const props = e.features[0].properties;
          showTooltip(e, props);
          map.setPaintProperty('tracks-line', 'line-opacity', [
            'case', ['==', ['get', 'hex'], props.hex], 1, 0.25,
          ]);
          map.setPaintProperty('tracks-line', 'line-width', [
            'case', ['==', ['get', 'hex'], props.hex], 2.5, 1,
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
