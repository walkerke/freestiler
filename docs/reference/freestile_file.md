# Create vector tiles from a spatial file

Reads a GeoParquet, GeoPackage, Shapefile, or other spatial file
directly into the tiling engine. Input data in any coordinate reference
system is automatically reprojected to WGS84 (EPSG:4326) before tiling.

## Usage

``` r
freestile_file(
  input,
  output,
  layer_name = NULL,
  tile_format = "mvt",
  min_zoom = 0L,
  max_zoom = 14L,
  base_zoom = NULL,
  drop_rate = NULL,
  cluster_distance = NULL,
  cluster_maxzoom = NULL,
  coalesce = FALSE,
  simplification = TRUE,
  overwrite = TRUE,
  quiet = FALSE,
  engine = "geoparquet"
)
```

## Arguments

- input:

  Character. Path to the input spatial file.

- output:

  Character. Path for the output .pmtiles file.

- layer_name:

  Character. Name for the tile layer. If NULL, derived from the output
  filename.

- tile_format:

  Character. `"mvt"` (default) or `"mlt"`.

- min_zoom:

  Integer. Minimum zoom level (default 0).

- max_zoom:

  Integer. Maximum zoom level (default 14).

- base_zoom:

  Integer. Zoom level at and above which all features are present. NULL
  (default) uses max_zoom.

- drop_rate:

  Numeric. Exponential drop rate. NULL (default) disables.

- cluster_distance:

  Numeric. Pixel distance for clustering. NULL disables.

- cluster_maxzoom:

  Integer. Max zoom for clustering. Default max_zoom - 1.

- coalesce:

  Logical. Whether to merge features with identical attributes (default
  FALSE).

- simplification:

  Logical. Whether to snap geometries to the tile pixel grid (default
  TRUE).

- overwrite:

  Logical. Whether to overwrite existing output (default TRUE).

- quiet:

  Logical. Whether to suppress progress (default FALSE).

- engine:

  Character. Backend engine: `"geoparquet"` (default, for GeoParquet
  files) or `"duckdb"` (for any file format DuckDB supports).

## Value

The output file path (invisibly).

## Details

The GeoParquet engine requires compilation with
`FREESTILER_GEOPARQUET=true`. The DuckDB engine uses the Rust DuckDB
backend when included in the build (enabled by default for native
builds), or falls back to the R `duckdb` package. Control backend
selection with `options(freestiler.duckdb_backend = "auto"|"rust"|"r")`.

## Examples

``` r
if (FALSE) { # \dontrun{
freestile_file("data.parquet", "output.pmtiles")
freestile_file("data.gpkg", "output.pmtiles", engine = "duckdb")
} # }
```
