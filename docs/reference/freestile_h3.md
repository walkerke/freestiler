# Create vector tiles with dynamic H3 hexagonal binning

Aggregates points into H3 hexagons at zoom-appropriate resolutions and
writes a PMTiles archive in which low zooms show coarse hexagons,
intermediate zooms show progressively finer hexagons, and zooms at or
above `base_zoom` show individual points. Aggregations (count, sum,
mean, etc.) are computed in DuckDB via the H3 community extension; the
function then assembles the per-resolution hex layers and the raw-point
layer via
[`freestile()`](https://walker-data.com/freestiler/reference/freestile.md).
Set `include_points = FALSE` to build only hexagons, for example when an
existing point archive supplies the close-up view. Aggregation happens
in DuckDB, but all resulting hex layers are still materialized for
tiling; this is not a bounded-memory polygon pipeline.

## Usage

``` r
freestile_h3(
  input,
  output,
  agg = "count",
  hex_layer_prefix = "h3",
  point_layer_name = "points",
  min_zoom = 0L,
  max_zoom = 14L,
  base_zoom = NULL,
  h3_resolutions = NULL,
  source_crs = NULL,
  db_path = NULL,
  tile_format = "mvt",
  fade = FALSE,
  fade_overlap = 1L,
  overwrite = TRUE,
  quiet = FALSE,
  include_points = TRUE,
  category = NULL,
  category_values = NULL
)
```

## Arguments

- input:

  An `sf` data frame of POINT geometry, or a character SQL query that
  returns a geometry column when executed against DuckDB. (DuckDB's
  spatial functions such as `ST_Read()` and `read_parquet()` are
  available.)

- output:

  Character. Path for the output `.pmtiles` file.

- agg:

  Aggregation specification:

  - `"count"` (default): single `count = COUNT(*)` property.

  - Named character vector of SQL aggregation expressions, e.g.
    `c(n = "COUNT(*)", avg_pop = "AVG(pop)", total = "SUM(pop)")`.

  - Named list of `prop_name = c(fn, column)` for callers who don't want
    to write SQL, e.g.
    `list(n = c("count", "*"), avg_pop = c("mean", "pop"))`. Supported
    `fn` values: `"count"`, `"sum"`, `"mean"`/`"avg"`, `"min"`, `"max"`,
    `"median"`.

- hex_layer_prefix:

  Character. Prefix for the per-resolution hex MVT layer names. Default
  `"h3"` produces `"h3_r01"`, `"h3_r02"`, etc.

- point_layer_name:

  Character. MVT layer name for raw points (default `"points"`).

- min_zoom, max_zoom:

  Integer. Global zoom range (default 0–14).

- base_zoom:

  Integer or NULL. Zoom level at and above which raw points take over
  from hex aggregations. Default `NULL` resolves to `max_zoom - 2L`,
  clamped so `min_zoom <= base_zoom <= max_zoom`. If
  `base_zoom == min_zoom`, no hex layers are created.

- h3_resolutions:

  Optional override of the zoom -\> H3 resolution mapping. Accepts
  `NULL` (use built-in defaults), an unnamed integer vector with
  `length(min_zoom:(base_zoom - 1L))` entries mapped positionally
  (`length(min_zoom:max_zoom)` for aggregate-only output), or a named
  integer vector with names that parse to integer zoom levels (sparse
  overrides; defaults fill the rest). All resolutions must be integers
  in `0:15`. The same resolution appearing in non-contiguous zoom runs
  (e.g. zooms 4–5 and 8) is rejected.

- source_crs:

  Character or NULL. CRS of geometry returned by SQL input, e.g.
  `"EPSG:4326"` (default for `sf` input, since `sf` is auto-transformed
  to WGS84) or `"EPSG:3857"`. Ignored for `sf` input. For SQL input, if
  `NULL`, a warning is emitted and the geometry is assumed to be
  `"EPSG:4326"`.

- db_path:

  Character or NULL. Path to an existing DuckDB database file, or
  `NULL`/`""` (default) for an in-memory database.

- tile_format:

  Character. `"mvt"` (default) or `"mlt"`.

- fade:

  Logical. If `TRUE`, adjacent hex layer zoom windows overlap by
  `fade_overlap` zooms so
  [`view_h3_tiles()`](https://walker-data.com/freestiler/reference/view_h3_tiles.md)
  can cross-fade between resolutions. Default `FALSE` (disjoint windows,
  clean zoom breaks).

- fade_overlap:

  Integer. Zooms of overlap on each side (only used when `fade = TRUE`,
  default `1L`).

- overwrite:

  Logical. Whether to overwrite an existing output file (default
  `TRUE`).

- quiet:

  Logical. Suppress progress messages (default `FALSE`).

- include_points:

  Logical. Include the raw-point layer (default `TRUE`). Set `FALSE` for
  aggregate-only output over the complete `min_zoom:max_zoom` range;
  `base_zoom` must then be `NULL`, and positional `h3_resolutions` must
  cover that complete range. Hex geometries are still held in memory.

- category:

  Optional categorical column to summarize. Adds `point_count`,
  per-category counts named `"<category>:<value>"`, and
  `modal_category`, `modal_count`, `modal_share`, and `modal_tie`. Ties
  have no selected category.

- category_values:

  Optional character or whole-number category dictionary (at most 64
  values). If `NULL`, infer it from the input. NULL and unlisted values
  contribute to `<category>:_other`, including in the mode. `_other` and
  `_total` are reserved. Counts are checked for exact integer transport
  up to 2^53 - 1; these are counts of input records, not weighted
  totals. Category labels must be unique ignoring case. Modal labels are
  strings, even for numeric categories; tied labels are missing in the
  encoded tile.

## Value

The output file path (invisibly).

## Details

Each distinct H3 resolution becomes its own MVT source-layer (named
`"<hex_layer_prefix>_r<resolution>"`, e.g. `"h3_r05"`); raw points are
emitted as a separate source-layer (`point_layer_name`). With the
default `fade = FALSE`, per-layer zoom windows are disjoint so the
rendered map swaps between resolutions cleanly. With `fade = TRUE`,
adjacent windows overlap by `fade_overlap` zooms so the companion
[`view_h3_tiles()`](https://walker-data.com/freestiler/reference/view_h3_tiles.md)
helper can cross-fade between resolutions.

DuckDB and the H3 community extension are required. With `sf` input the
data is written to DuckDB via a temporary Parquet (or `dbWriteTable`)
roundtrip; with character SQL input, the user query is wrapped in a
temporary view inside DuckDB.

## Point volume at `base_zoom`

The raw-point layer is encoded without thinning: every input point is
written to every tile that contains it from `base_zoom` up (and from
`base_zoom - fade_overlap` when `fade = TRUE`). For very large inputs
the tiles at the first point zoom can get heavy. Raising `base_zoom`
defers points to higher zooms, where each tile covers less area and so
holds fewer points. Per-layer feature dropping for the points layer is
on the roadmap.

## Antimeridian and polar cells

Hexagons that cross the antimeridian are split at +/-180 degrees so they
render correctly on both sides of the dateline instead of as
world-spanning slivers. The rare cells that contain a pole receive the
same split and render approximately (the polygon edge follows the cell
boundary vertices, so the polar cap itself is not filled).

## See also

[`freestile()`](https://walker-data.com/freestiler/reference/freestile.md),
[`view_h3_tiles()`](https://walker-data.com/freestiler/reference/view_h3_tiles.md)

## Examples

``` r
if (FALSE) { # \dontrun{
library(sf)
pts <- st_as_sf(data.frame(
  x = runif(50000, -100, -80),
  y = runif(50000, 30, 45),
  w = rnorm(50000, 100, 10)
), coords = c("x", "y"), crs = 4326)

freestile_h3(pts, "wind.pmtiles",
  agg = c(n = "COUNT(*)", avg_w = "AVG(w)"),
  min_zoom = 2, max_zoom = 12, base_zoom = 10)

view_h3_tiles("wind.pmtiles", agg_column = "n")

# Cross-fade between resolutions
freestile_h3(pts, "wind_fade.pmtiles",
  agg = "count",
  min_zoom = 2, max_zoom = 12, base_zoom = 10,
  fade = TRUE)

# Categorical hexes only; all category counts remain available for styling.
pts$group <- sample(c("a", "b", "c"), nrow(pts), replace = TRUE)
freestile_h3(pts, "groups.pmtiles", include_points = FALSE,
  category = "group", category_values = c("a", "b", "c"),
  min_zoom = 3, max_zoom = 8)
} # }
```
