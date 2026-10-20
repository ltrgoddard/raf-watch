-- UTC days a..b of traces for the listed hexes plus the UK military block,
-- deduplicated to one point per second and thinned: every 30s in straight
-- flight, 5s while turning, 60s on the ground, and both ends of any gap.
-- the table is not partitioned by date, so each query scans it: batch days.
SELECT toString(d) d, icao, any(r) r, any(t) t, any(desc) desc,
  max(heli) heli, arraySort(groupArray((s, la, lo, al, gs, tr))) p
FROM (
  SELECT d, icao, s, r, t, desc, heli,
    toInt32(round(v.1 * 1e5)) la, toInt32(round(v.2 * 1e5)) lo,
    if(v.3 = 0, -1, intDiv(v.3, 25)) al, toInt32(round(v.4)) gs, v.5 tr,
    lagInFrame(s, 1, -999) OVER w ps, leadInFrame(s, 1, 99999) OVER w ns,
    lagInFrame(tr, 1, tr) OVER w pt, count() OVER w n,
    if(al < 0, 60, if(abs((tr - pt + 540) % 360 - 180) > 1, 5, 30)) b
  FROM (
    SELECT date d, icao, toInt32(toUnixTimestamp(time)
        - toUnixTimestamp(toDateTime(date, 'UTC'))) s,
      any(r) r, any(t) t, any(desc) desc,
      max(CAST(aircraft_category, 'Int8') = 8) heli,
      any((lat, lon, altitude, ground_speed, track_degrees)) v
    FROM planes_mercator
    WHERE date BETWEEN {a:Date} AND {b:Date} AND (lat != 0 OR lon != 0)
      AND (icao IN {h:Array(String)} OR startsWith(icao, '43c'))
    GROUP BY d, icao, s
  )
  WINDOW w AS (PARTITION BY d, icao ORDER BY s
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
WHERE n >= 60 AND (s - ps > 60 OR ns - s > 60 OR intDiv(s, b) != intDiv(ps, b))
GROUP BY d, icao ORDER BY d, icao
FORMAT JSONEachRow
