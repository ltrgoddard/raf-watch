import * as maplibregl from './vendor/maplibre-gl.mjs'
import style from './basemap.js'
import { RAF_BASES } from './bases.js'

const $ = s => document.querySelector(s)
const GAP = 180, MAXDAYS = 31

// ── aircraft classification ──────────────────────────────────────────

const GROUPS = {
  fighter: ['#e05555', 'EUFI F35 TOR GR4 HAR'],
  transport: ['#5588cc', 'A400 C17 C130 B737 GLF6 B463 BAE146 HS25 H25A C208'],
  tanker: ['#cc9944', 'A332 A330 KC3 MRTT VC10'],
  'ISR & patrol': ['#9977bb', 'P8 R135 B350 E737 SENT SHAD NIM FFLO Q9 RC12'],
  helicopter: ['#44aa77', 'H47 LYNX WILD EH10 MLIN PUMA S61 EC45 EC35 H64 '
    + 'AH64 A109 A139 AS65 AS32 EC75'],
  trainer: ['#55aabb', 'HAWK G12T TEX2 T6 E50P PC21 TUCA PHOE GLID DA42'],
  heritage: ['#c8b88a', 'SPIT SASP HURR LANC DHC1 F156 HUNT DAK DC3'],
  other: ['#778899', ''],
}
const GROUP = Object.fromEntries(Object.entries(GROUPS)
  .flatMap(([g, [, s]]) => s.split(' ').map(t => [t, g])))
const color = g => GROUPS[g][0]

// ── RAF1 decoder (see README) ────────────────────────────────────────

function parse(buf) {
  const b = new Uint8Array(buf), v = new DataView(buf), t0 = v.getUint32(4, 1)
  const date = new Date(t0 * 1e3).toISOString().slice(0, 10)
  let i = 10
  const uv = () => {
    let x = 0, s = 1, c
    do { c = b[i++]; x += (c & 127) * s; s *= 128 } while (c & 128)
    return x
  }
  const sv = () => { const n = uv(); return n % 2 ? -(n + 1) / 2 : n / 2 }
  return Array.from({ length: v.getUint16(8, 1) }, () => {
    const hex = (b[i] | b[i + 1] << 8 | b[i + 2] << 16)
      .toString(16).padStart(6, '0')
    i += 3
    const n = uv(), f = { hex, date, t0, t: [], c: [], alt: [], gs: [] }
    let t = 0, la = 0, lo = 0, al = 0, gs = 0, off = 0, px
    for (let j = 0; j < n; j++, i++) {
      t += uv(); la += sv(); lo += sv(); al += sv(); gs += sv()
      let x = lo / 1e5 + off   // unwrap across the antimeridian
      if (j && x - px > 180) off -= 360, x -= 360
      else if (j && px - x > 180) off += 360, x += 360
      f.t.push(t); f.c.push([px = x, la / 1e5]); f.alt.push(al * 25)
      f.gs.push(gs)
    }
    return f
  })
}

const cache = new Map()
const load = d => cache.get(d) || cache.set(d, fetch(`data/${d}.bin`)
  .then(r => r.ok ? r.arrayBuffer().then(parse) : [])).get(d)

// ── hash state (shares the hash with maplibre's map= param) ─────────

const hget = k => decodeURIComponent(
  location.hash.match(`[#&]${k}=([^&]*)`)?.[1] || '')
function hset(k, v) {
  const h = location.hash.slice(1).split('&')
    .filter(s => s && !s.startsWith(k + '='))
  if (v) h.push(`${k}=${encodeURIComponent(v)}`)
  history.replaceState(null, '', '#' + h.join('&'))
}

// ── state ────────────────────────────────────────────────────────────

let dates = [], hours = [], meta = {}, flights = [], segs = [], sel = [0, 0]
const hidden = new Set()
let query = hget('q')
const m = h => meta[h] || {}
const group = h => GROUP[m(h).icao_type?.toUpperCase()]
  || (m(h).heli ? 'helicopter' : 'other')
const words = h => Object.values({ hex: h, ...m(h) }).join(' ').toLowerCase()
const match = f => query.toLowerCase().split(/\s+/)
  .every(w => words(f.hex).includes(w))
const shown = f => !hidden.has(group(f.hex)) && match(f)

// ── map ──────────────────────────────────────────────────────────────

const map = new maplibregl.Map({
  container: 'map', style, center: [-2.5, 54.5], zoom: 4, minZoom: 1.5,
  hash: 'map', attributionControl: { compact: true },
})
const lyr = (id, type, source, paint, x = {}) =>
  map.addLayer({ id, type, source, paint, ...x })
const solid = ['!=', ['get', 'gap'], true]

map.on('load', async () => {
  const [man, mt] = await Promise.all(['manifest', 'aircraft_meta']
    .map(f => fetch(`data/${f}.json`).then(r => r.json()).catch(() => ({}))))
  dates = Object.keys(man).sort(); hours = dates.map(d => man[d]); meta = mt
  if (!dates.length) return $('#tl-cur').textContent = 'no data'

  const fc = { type: 'FeatureCollection', features: [] }
  map.addSource('tracks', { type: 'geojson', data: fc })
  map.addSource('dots', { type: 'geojson', data: fc })
  map.addSource('bases', { type: 'geojson', data: { ...fc,
    features: RAF_BASES.map(b => ({ type: 'Feature', properties: b,
      geometry: { type: 'Point', coordinates: b.coords } })) } })
  const c = ['get', 'color']
  lyr('bases', 'circle', 'bases', { 'circle-radius': 4,
    'circle-color': 'transparent', 'circle-stroke-width': 1.5,
    'circle-stroke-color': 'rgba(255,255,255,.45)' })
  lyr('bases-labels', 'symbol', 'bases', { 'text-color': 'rgba(255,255,255,.6)',
    'text-halo-color': 'rgba(0,0,0,.6)', 'text-halo-width': 1 }, { minzoom: 6,
    layout: { 'text-field': ['get', 'name'], 'text-font': ['Noto Sans Regular'],
      'text-size': 10, 'text-offset': [0, 1.2], 'text-anchor': 'top' } })
  lyr('tracks-glow', 'line', 'tracks', { 'line-color': c, 'line-width': 4,
    'line-opacity': .15, 'line-blur': 4 }, { filter: solid })
  lyr('tracks-line', 'line', 'tracks', { 'line-color': c, 'line-width': 1.5,
    'line-opacity': .7 }, { filter: solid })
  lyr('tracks-gap', 'line', 'tracks', { 'line-color': c, 'line-width': 1.5,
    'line-opacity': .35, 'line-dasharray': [2, 4] }, { filter: ['!', solid] })
  lyr('tracks-hit', 'line', 'tracks', { 'line-width': 14, 'line-opacity': .01 })
  const cv = new OffscreenCanvas(16, 16), x = cv.getContext('2d')
  Object.assign(x, { strokeStyle: '#fff', lineWidth: 2.5, lineCap: 'round' })
  x.moveTo(2, 12); x.lineTo(8, 6); x.lineTo(14, 12); x.stroke()
  map.addImage('chevron', x.getImageData(0, 0, 16, 16), { sdf: true })
  lyr('tracks-arrows', 'symbol', 'tracks', { 'icon-color': c,
    'icon-opacity': .5 }, { layout: { 'symbol-placement': 'line',
    'symbol-spacing': 80, 'icon-image': 'chevron', 'icon-rotate': 90,
    'icon-rotation-alignment': 'map', 'icon-allow-overlap': true,
    'icon-ignore-placement': true } })
  lyr('dots-glow', 'circle', 'dots', { 'circle-radius': 6, 'circle-color': c,
    'circle-opacity': .25, 'circle-blur': 1 })
  lyr('dots', 'circle', 'dots', { 'circle-radius': 3, 'circle-color': c })

  for (const l of ['tracks-hit', 'dots']) {
    map.on('mousemove', l, hover)
    map.on('mouseleave', l, unhover)
    map.on('click', l, e => {   // touch: show the tooltip instead
      const f = segs[e.features[0].id]?.f, { lng, lat } = e.lngLat
      if (!f || e.originalEvent.pointerType == 'touch') return hover(e)
      open(`https://globe.adsbexchange.com/?icao=${f.hex}&lat=${lat}`
        + `&lon=${lng}&zoom=${map.getZoom()}&showTrace=${f.date}`)
    })
  }
  map.on('mousemove', 'bases', e => tip(e, `<div class="tt-hex">${
    e.features[0].properties.name}</div>${e.features[0].properties.icao}`))
  map.on('mouseleave', 'bases', unhover)

  timeline()
  const [a, b = a] = hget('date').split('..').map(d => dates.indexOf(d))
  select(a >= 0 && b >= a ? [a, b] : [dates.length - 1, dates.length - 1])
  $('#q').value = query
  $('#q').oninput = e => { query = e.target.value; hset('q', query); draw() }
})

// ── tracks ───────────────────────────────────────────────────────────

function draw() {
  segs = []
  const lines = [], dots = [], feat = (id, properties, type, coordinates) =>
    ({ type: 'Feature', id, properties, geometry: { type, coordinates } })
  for (const f of flights.filter(shown)) {
    const props = { hex: f.hex, color: color(group(f.hex)) }, n = f.t.length
    const emit = (a, b, gap) => {
      if (b > a) lines.push(feat(segs.push({ f, a, b }) - 1, { ...props, gap },
        'LineString', f.c.slice(a, b + 1)))
    }
    // skip taxiing: keep airborne points and the ground points either side
    const air = j => f.alt[j] >= 0 || f.alt[j - 1] >= 0 || f.alt[j + 1] >= 0
    let a = null
    for (let j = 0; j < n; j++) {
      if (!air(j)) a != null && emit(a, j - 1, false), a = null
      else if (a == null) a = j
      else if (f.t[j] - f.t[j - 1] > GAP)
        emit(a, j - 1, false), emit(j - 1, j, true), a = j
    }
    if (a != null) emit(a, n - 1, false)
    dots.push(feat(segs.push({ f, a: n - 1, b: n - 1 }) - 1, props, 'Point',
      f.c[n - 1]))
  }
  const fc = features => ({ type: 'FeatureCollection', features })
  map.getSource('tracks').setData(fc(lines))
  map.getSource('dots').setData(fc(dots))
  legend()
}

function legend() {
  const n = {}
  for (const h of new Set(flights.filter(match).map(f => f.hex)))
    n[group(h)] = -~n[group(h)]
  $('#legend-items').innerHTML = Object.keys(GROUPS).filter(g => n[g])
    .map(g => `<div class="legend-item${hidden.has(g) ? ' dimmed' : ''}"
      data-g="${g}"><span class="legend-circle" style="--c:${color(g)}"></span>
      <span class="legend-name">${g}</span><span class="legend-cnt">${n[g]}
      </span></div>`).join('')
}
$('#legend-items').onclick = e => {
  const g = e.target.closest('[data-g]')?.dataset.g
  if (g) hidden.delete(g) || hidden.add(g), draw()
}

// ── tooltip and highlight ────────────────────────────────────────────

const esc = s => String(s ?? '').replace(/[&<>"]/g, c => `&#${c.charCodeAt()};`)
function tip(e, html) {
  const t = $('#tooltip')
  t.innerHTML = html
  t.style.display = 'block'
  t.style.left = Math.min(e.point.x + 16, innerWidth - t.offsetWidth - 8) + 'px'
  t.style.top = Math.min(e.point.y, innerHeight - t.offsetHeight - 8) + 'px'
  map.getCanvas().style.cursor = 'pointer'
}

function hover(e) {
  const s = segs[e.features[0].id], { lng, lat } = e.lngLat
  if (!s) return
  const { f, a, b } = s
  let k = a, best = Infinity
  for (let j = a; j <= b; j++) {
    const d = (f.c[j][0] - lng) ** 2 + (f.c[j][1] - lat) ** 2
    if (d < best) best = d, k = j
  }
  const { reg, type, icao_type, unit } = m(f.hex)
  const time = new Date((f.t0 + f.t[k]) * 1e3).toISOString().slice(11, 19)
  const row = (l, v) => v ? `<div class="tt-label">${l}</div>`
    + `<div class="tt-value">${esc(v)}</div>` : ''
  tip(e, `<div class="tt-hex">${esc(reg || f.hex)}</div>`
    + row('Aircraft', type || icao_type || 'Unknown') + row('Unit', unit)
    + row('Time', `${f.date} ${time} UTC`)
    + row('Altitude · speed', `${f.alt[k] < 0 ? 'ground' : f.alt[k]
      .toLocaleString() + ' ft'} · ${f.gs[k]} kts`)
    + row('ICAO hex', f.hex))
  const on = (y, n) => ['case', ['==', ['get', 'hex'], f.hex], y, n]
  map.setPaintProperty('tracks-line', 'line-opacity', on(1, .2))
  map.setPaintProperty('tracks-line', 'line-width', on(2.5, 1))
  map.setPaintProperty('tracks-gap', 'line-opacity', on(.6, .15))
}

function unhover() {
  $('#tooltip').style.display = 'none'
  map.getCanvas().style.cursor = ''
  map.setPaintProperty('tracks-line', 'line-opacity', .7)
  map.setPaintProperty('tracks-line', 'line-width', 1.5)
  map.setPaintProperty('tracks-gap', 'line-opacity', .35)
}

// ── timeline: click a day, drag a range, arrow keys step ─────────────

function timeline() {
  const h = $('#histo'), max = Math.max(...hours, 1)
  h.innerHTML = dates.map((d, i) => `<i style="height:${Math.max(2, hours[i]
    / max * 100)}%"${d.endsWith('-01') ? ` class="${d.endsWith('01-01')
    ? 'yr' : 'mo'}" data-y="${d.slice(0, 4)}"` : ''}></i>`).join('')
  $('#tl-first').textContent = dates[0]
  $('#tl-last').textContent = dates.at(-1)
  const at = e => {
    const r = h.getBoundingClientRect()
    return Math.max(0, Math.min(dates.length - 1,
      Math.floor((e.clientX - r.left) / r.width * dates.length)))
  }
  let a0 = null
  h.onpointerdown = e => { h.setPointerCapture(e.pointerId); a0 = at(e)
    mark([a0, a0]) }
  h.onpointermove = e => {
    const i = at(e)
    if (a0 == null) return label([i, i])
    mark([Math.max(Math.min(a0, i), a0 - MAXDAYS + 1),
      Math.min(Math.max(a0, i), a0 + MAXDAYS - 1)])
  }
  h.onpointerup = () => { a0 = null; select(sel) }
  h.onpointerleave = () => a0 == null && label(sel)
  onkeydown = e => {
    if (e.target.tagName == 'INPUT' || !/^Arrow[LR]/.test(e.key)) return
    const d = e.key == 'ArrowLeft' ? -1 : 1, n = dates.length - 1
    const c = x => Math.max(0, Math.min(n, x))
    select(e.shiftKey ? [sel[0], c(sel[1] + d)].sort((x, y) => x - y)
      : [c(sel[0] + d), c(sel[1] + d)])
  }
}

function label([a, b]) {
  const h = hours.slice(a, b + 1).reduce((x, y) => x + y, 0)
  $('#tl-cur').textContent = (a == b ? dates[a] : `${dates[a]} – ${dates[b]}`)
    + ` · ${Math.round(h).toLocaleString()}h flown`
}

function mark(s) {
  sel = s
  label(s)
  $('#histo').querySelectorAll('i').forEach((el, i) =>
    el.classList.toggle('on', i >= s[0] && i <= s[1]))
}

let token = 0
async function select([a, b]) {
  const s = [a, Math.min(b, a + MAXDAYS - 1)]
  mark(s)
  const t = ++token, ds = dates.slice(s[0], s[1] + 1)
  hset('date', ds.length > 1 ? `${ds[0]}..${ds.at(-1)}` : ds[0])
  $('#date-slider').classList.add('loading')
  const fs = (await Promise.all(ds.map(load))).flat()
  if (t != token) return
  $('#date-slider').classList.remove('loading')
  flights = fs
  draw()
}
