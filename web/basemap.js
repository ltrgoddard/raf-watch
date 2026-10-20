// dark vector basemap from OpenFreeMap (OpenMapTiles schema), no API key
const name = ['coalesce', ['get', 'name:en'], ['get', 'name']]
const font = ['Noto Sans Regular']
const halo = { 'text-halo-color': 'rgba(0,0,0,.6)', 'text-halo-width': 1.2 }
const z = (...s) => ['interpolate', ['linear'], ['zoom'], ...s]
const label = (id, filter, minzoom, size, color, layout = {}) => ({
  id, type: 'symbol', source: 'omt', 'source-layer': 'place', minzoom,
  filter: ['in', ['get', 'class'], ['literal', filter]],
  layout: { 'symbol-sort-key': ['get', 'rank'], 'text-field': name,
    'text-font': font, 'text-size': size, 'text-max-width': 8, ...layout },
  paint: { 'text-color': color, ...halo },
})

export default {
  version: 8,
  projection: { type: 'globe' },
  sky: { 'atmosphere-blend': 0 },
  glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
  sources: {
    omt: { type: 'vector', url: 'https://tiles.openfreemap.org/planet',
      attribution: '<a href="https://openfreemap.org">OpenFreeMap</a> '
        + '© <a href="https://openstreetmap.org/copyright">OSM</a>' },
  },
  layers: [
    { id: 'land', type: 'background',
      paint: { 'background-color': '#1b1c1f' } },
    { id: 'water', type: 'fill', source: 'omt', 'source-layer': 'water',
      paint: { 'fill-color': '#0c0d0f' } },
    { id: 'aerodrome', type: 'fill', source: 'omt', 'source-layer': 'aeroway',
      minzoom: 9, filter: ['==', ['get', 'class'], 'aerodrome'],
      paint: { 'fill-color': '#25262a' } },
    { id: 'roads', type: 'line', source: 'omt',
      'source-layer': 'transportation', minzoom: 6,
      filter: ['in', ['get', 'class'], ['literal', ['motorway', 'trunk']]],
      paint: { 'line-color': '#2c2d31', 'line-width': z(6, .5, 12, 2) } },
    { id: 'runways', type: 'line', source: 'omt', 'source-layer': 'aeroway',
      minzoom: 9, filter: ['in', ['get', 'class'], ['literal', ['runway']]],
      paint: { 'line-color': '#4a4c52', 'line-width': z(9, 1, 14, 12) } },
    { id: 'states', type: 'line', source: 'omt', 'source-layer': 'boundary',
      minzoom: 4, filter: ['all', ['==', ['get', 'admin_level'], 4],
        ['!=', ['get', 'maritime'], 1]],
      paint: { 'line-color': 'rgba(255,255,255,.08)',
        'line-dasharray': [2, 2] } },
    { id: 'borders', type: 'line', source: 'omt', 'source-layer': 'boundary',
      filter: ['all', ['==', ['get', 'admin_level'], 2],
        ['!=', ['get', 'maritime'], 1]],
      paint: { 'line-color': 'rgba(255,255,255,.18)',
        'line-width': z(1, .5, 6, 1.5) } },
    label('countries', ['country'], 2, z(2, 10, 6, 14), 'rgba(255,255,255,.75)',
      { 'text-transform': 'uppercase', 'text-letter-spacing': .15 }),
    label('regions', ['state'], 5, z(5, 9, 8, 12), 'rgba(255,255,255,.45)'),
    label('cities', ['city', 'town'], 4, z(4, 10, 10, 14),
      'rgba(255,255,255,.8)'),
    label('villages', ['village', 'suburb'], 10, z(10, 10, 14, 13),
      'rgba(255,255,255,.6)'),
    { id: 'airfields', type: 'symbol', source: 'omt',
      'source-layer': 'aerodrome_label', minzoom: 8,
      layout: { 'text-field': ['concat', name, '\n',
        ['coalesce', ['get', 'icao'], '']],
        'text-font': font, 'text-size': 10, 'text-max-width': 10 },
      paint: { 'text-color': 'rgba(160,190,255,.6)', ...halo } },
  ],
}
