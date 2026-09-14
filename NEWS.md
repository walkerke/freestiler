# freestiler 0.2.0

* Tile generation now uses dramatically less memory on large inputs. Encoded
  tiles are compressed and spooled to a temporary file as they are produced
  instead of being accumulated in RAM across all zoom levels, and the PMTiles
  archive is assembled by streaming from that spool. Together with allocation
  fixes in the GeoParquet reader and MVT encoder, peak memory for
  multi-million-feature polygon inputs drops to roughly the cost of the
  decoded features themselves. Note the tradeoff: tile data is staged on disk,
  so a build transiently needs about twice the final archive size in free
  space.
* Output archives are now written atomically: tiles are assembled in a
  temporary sibling file that is renamed over the destination only on
  success. A failed run no longer leaves a partial archive, and an existing
  output file is preserved until its replacement is complete. Relatedly,
  `overwrite = TRUE` is now respected through the DuckDB and CRS-reprojection
  fallback paths instead of being reset to `FALSE` internally.
* Identical tiles are no longer deduplicated within the archive (a
  micro-optimization measured at ~0.01% of archive size on real data); the
  PMTiles `clustered` header flag is now computed from the actual tile data
  layout rather than always claimed.
* Thin (sub-pixel-width) polygons no longer flicker in and out across zoom
  levels (#13). A polygon that collapses on a tile's integer pixel grid is
  now replaced by a one-pixel square at its centroid instead of being
  dropped, so narrow features stay continuously visible. Ring winding is
  also normalized to the MVT specification (exterior rings positive area,
  interior rings negative), zero-area degenerate rings are dropped rather
  than emitted as invalid polygons, and quantization-induced spikes
  (out-and-back needle vertices) are removed.
* `freestile_h3()` is a new function for dynamic hexagonal binning. It
  aggregates points into H3 hexagons at zoom-appropriate resolutions via
  DuckDB's H3 community extension and writes a multi-layer `.pmtiles`
  archive where low zooms show coarse hexes, intermediate zooms show
  progressively finer hexes, and zooms at or above `base_zoom` show the raw
  points. Aggregation rules are user-defined SQL expressions
  (e.g. `c(n = "COUNT(*)", avg_pop = "AVG(pop)")`). Opt-in `fade = TRUE`
  produces overlapping zoom windows so adjacent hex resolutions can
  cross-fade visually.
* `view_h3_tiles()` is a companion viewer that auto-styles a `freestile_h3()`
  archive in `mapgl`, detecting clean-break vs cross-fade mode from the
  PMTiles metadata.
* Hexagons that cross the antimeridian are split at +/-180 degrees rather
  than rendering as world-spanning slivers.
* See `vignette("h3-hexagonal-binning")` for a walkthrough.

# freestiler 0.1.7

* Updated the CRAN Rust build path to use a dependency graph compatible with
  rustc/cargo 1.77.2.

# freestiler 0.1.0

Initial release.

## Tile generation

* `freestile()` creates PMTiles archives from sf data frames with zero external
  dependencies (no tippecanoe, no Java, no Go).
* Supports **MapLibre Tiles (MLT)** and **Mapbox Vector Tiles (MVT)** output
  formats.
* Multi-layer output via named lists or `freestile_layer()` per-layer zoom
  control.

## Geometry types

* POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON.
* Automatic CRS transformation to WGS84.
* Z/M dimension handling (dropped automatically).

## Performance features

* Parallel tile encoding with rayon (across tiles and within tiles).
* Tile-pixel grid snapping for zoom-adaptive simplification without slivers.
* Buffered tile assignment and clipping for seamless tile boundaries.

## Feature management

* `drop_rate` exponential feature thinning with Morton-curve spatial ordering
  for points and area-based ordering for polygons/lines.
* `base_zoom` control for ensuring all features present at higher zooms.
* `cluster_distance` point clustering with `point_count` attribute.
* `coalesce` line merging and polygon grouping.

## MLT encoder

* Spec-compliant MapLibre Tile encoder with varint, delta, RLE, and dictionary
  encoding.
* Validated against mlt-core 0.1.2 reference decoder.
