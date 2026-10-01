# freestiler 0.3.0

* Requests for native features omitted from the CRAN build now explain how to
  install from R-Universe, including the platform limits of Rust DuckDB and
  streaming and the R DuckDB fallback.
* Experimental ordered categorical clustering is available through
  `freestile_file()` in R and Python (`category`, `category_values`,
  `cluster_min_points`). It uses the pinned Supercluster 8.0.1/KDBush 4.1.0
  port, with 512-tile pixel radius, typed category counts and expansion zoom.
  This increment requires a POINT GeoParquet file, `cluster_maxzoom = max_zoom`,
  and no thinning/coalescing; use a separate dot source above the cutoff.
  The global index is compact but resident, not out-of-core. Existing
  noncategorical clustering is unchanged pending the broader API migration.
  Output is spooled one tile at a time, exact singleton originals live on disk,
  and `FREESTILER_CLUSTER_TILE_BUDGET_MB` (default 256) guards decoded tile buffers
  and the tile-coordinate index. Numeric/string-union abbreviation fields are
  omitted from TileJSON's single-type field map.
* `freestile_h3()` now supports aggregate-only archives with
  `include_points = FALSE`, covering the complete requested zoom range without
  fetching a raw-point frame. Optional `category` and `category_values` add
  `point_count`, `<category>:<value>` counts, an `_other` count for missing or
  unlisted values, and modal category/count/share/tie fields. Ties have no
  selected category. R and Python share these semantics. All finalized hex
  layers are still held in memory; bounded polygon output is separate work.
* H3 integer aggregates are checked before host conversion and encoded as
  integers, including values above R's 32-bit integer range (supported exact
  range: +/- (2^53 - 1)). Antimeridian repair now touches only crossing cells.
* PMTiles field metadata now uses TileJSON `Number`, `String`, and `Boolean`
  types, with a `field_order` extension preserving requested property order.
  `view_h3_tiles()` defaults to `point_count` for categorical output and skips
  H3 bookkeeping fields when choosing a numeric aggregate.

# freestiler 0.2.0

* The streaming point pipeline (`freestile_query()`) now partitions the query
  result on disk and tiles each partition independently instead of sorting
  the whole dataset at once. This removes the global sort that previously
  spilled tens of gigabytes on very large inputs and makes billion-point
  tiling feasible with bounded memory. Runs that would exhaust a resource now
  stop with a clear error (naming the tile or region and suggesting
  `drop_rate` or a higher `min_zoom`) rather than degrading, and a failed run
  never disturbs an existing output. Optional environment variables:
  `FREESTILER_TEMP_DIR` (where bulk temporary data goes),
  `FREESTILER_DUCKDB_MEMORY`, and `FREESTILER_STREAM_WORKERS`.
* With `drop_rate`, streaming point thinning is now computed per partition:
  the per-zoom density is unchanged, but the exact set of retained points can
  differ from earlier releases (the sampling phase restarts per partition, so
  membership shifts are not limited to partition boundaries). Without
  `drop_rate` output is unchanged.

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
# freestiler 0.2.1

* macOS builds now use Rust's baseline deployment target for Rust and dependency
  C objects instead of the host OS version. This prevents warnings when R links
  for an older macOS version. Explicit user deployment targets are preserved.

# freestiler 0.2.0

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
