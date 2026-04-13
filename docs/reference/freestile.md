# Create vector tiles from spatial data

Creates a PMTiles archive containing vector tiles from one or more sf
data frames. Supports both Mapbox Vector Tile (MVT) and MapLibre Tile
(MLT) formats, multi-layer output, feature dropping, point clustering,
and feature coalescing.

## Usage

``` r
freestile(
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
  generate_ids = TRUE,
  overwrite = TRUE,
  quiet = FALSE
)
```

## Arguments

- input:

  An sf data frame, or a named list of sf/freestile_layer objects for
  multi-layer output.

- output:

  Character. Path for the output .pmtiles file.

- layer_name:

  Character. Name for the tile layer. If NULL, derived from the output
  filename. Only used for single-layer input.

- tile_format:

  Character. Tile encoding format: `"mvt"` (default) for Mapbox Vector
  Tiles or `"mlt"` for MapLibre Tiles.

- min_zoom:

  Integer. Minimum zoom level (default 0).

- max_zoom:

  Integer. Maximum zoom level (default 14).

- base_zoom:

  Integer. Zoom level at and above which all features are present (no
  dropping). NULL (default) uses each layer's own max_zoom. The
  drop-rate curve is also computed relative to base_zoom, so lowering it
  produces gentler thinning at low zooms. Inspired by tippecanoe's `-B`
  / `--base-zoom`.

- drop_rate:

  Numeric. Exponential drop rate for feature thinning (e.g. 2.5). At
  each zoom level below base_zoom, features are retained at a rate of
  1/drop_rate^(base_zoom - zoom). Points are thinned using spatial
  ordering; polygons/lines are thinned by area. NULL (default) disables
  drop-rate thinning.

- cluster_distance:

  Numeric. Pixel distance for point clustering. Points within this
  radius are merged into cluster features with a `point_count`
  attribute. NULL (default) disables clustering.

- cluster_maxzoom:

  Integer. Maximum zoom level for clustering. Above this zoom,
  individual points are shown. Default is max_zoom - 1.

- coalesce:

  Logical. Whether to merge features with identical attributes within
  each tile (default FALSE). Lines sharing endpoints are merged;
  polygons are grouped into MultiPolygons.

- simplification:

  Logical. Whether to snap geometries to the tile pixel grid at each
  zoom level (default TRUE). This provides zoom-adaptive simplification
  and prevents slivers between adjacent polygons.

- generate_ids:

  Logical. Whether to assign sequential feature IDs (default TRUE).

- overwrite:

  Logical. Whether to overwrite existing output file (default TRUE).

- quiet:

  Logical. Whether to suppress progress messages (default FALSE).

## Value

The output file path (invisibly).

## Details

Input data in any coordinate reference system (CRS) is automatically
reprojected to WGS84 (EPSG:4326) before tiling.

## Examples

``` r
if (FALSE) { # \dontrun{
library(sf)
nc <- st_read(system.file("shape/nc.shp", package = "sf"))

# Single layer
freestile(nc, "nc.pmtiles", layer_name = "counties")

# Multi-layer
pts <- st_centroid(nc)
freestile(
  list(counties = nc, centroids = pts),
  "nc_layers.pmtiles"
)

# With dropping and coalescing
freestile(nc, "nc_drop.pmtiles", drop_rate = 2.5, coalesce = TRUE)

# With point clustering
freestile(pts, "pts.pmtiles", cluster_distance = 50, cluster_maxzoom = 8)
} # }
```
