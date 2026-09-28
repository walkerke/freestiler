SELECT ST_Point(lon, lat) AS geom, group_id
FROM (VALUES
  (-97.3, 32.7, 1), (-97.3, 32.7, 1), (-97.3, 32.7, 2),
  (-122.4, 37.7, 1), (-122.4, 37.7, 2),
  (179.9, 0.0, 1), (179.9, 0.0, NULL), (179.9, 0.0, 9)
) AS points(lon, lat, group_id)
