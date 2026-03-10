# freestiler

**freestiler** creates [PMTiles](https://github.com/protomaps/PMTiles)
vector tilesets from R and Python. Give it an sf object, a file on disk,
or a DuckDB SQL query, and it writes a single `.pmtiles` file you can
serve from anywhere. The tiling engine is written in Rust and runs
in-process, so there’s nothing else to install.

## Installation

### R

Install from [r-universe](https://walkerke.r-universe.dev):

``` r
install.packages(
  "freestiler",
  repos = c("https://walkerke.r-universe.dev", "https://cloud.r-project.org")
)
```

Or install from GitHub:

``` r
# install.packages("devtools")
devtools::install_github("walkerke/freestiler")
```

### Python

``` bash
pip install freestiler
```

Published PyPI wheels currently target Python 3.9 through 3.14.

See the [Python
Setup](https://walker-data.com/freestiler/articles/python.html) article
for more details.

## Quick start

The main function is
[`freestile()`](https://walker-data.com/freestiler/reference/freestile.md).
Let’s tile the North Carolina counties dataset that ships with sf:

``` r
library(sf)
library(freestiler)

nc <- st_read(system.file("shape/nc.shp", package = "sf"))

freestile(nc, "nc_counties.pmtiles", layer_name = "counties")
```

That’s useful for checking your installation, but the same API handles
much bigger data. Here we tile all 242,000 US block groups from
[tigris](https://github.com/walkerke/tigris):

``` r
library(tigris)
options(tigris_use_cache = TRUE)

bgs <- block_groups(cb = TRUE)

freestile(
  bgs,
  "us_bgs.pmtiles",
  layer_name = "bgs",
  min_zoom = 4,
  max_zoom = 12
)
```

## Viewing tiles

The quickest way to view a tileset is
[`view_tiles()`](https://walker-data.com/freestiler/reference/view_tiles.md),
which starts a local server and opens an interactive map:

``` r
view_tiles("us_bgs.pmtiles")
```

For more control, use
[`serve_tiles()`](https://walker-data.com/freestiler/reference/serve_tiles.md)
to start a local server and build your map with
[mapgl](https://walker-data.com/mapgl/):

``` r
library(mapgl)

serve_tiles("us_bgs.pmtiles")

maplibre(hash = TRUE) |>
  add_pmtiles_source(
    id = "bgs-src",
    url = "http://localhost:8080/us_bgs.pmtiles",
    promote_id = "GEOID"
  ) |>
  add_fill_layer(
    id = "bgs-fill",
    source = "bgs-src",
    source_layer = "bgs",
    fill_color = "navy",
    fill_opacity = 0.5,
    hover_options = list(
      fill_color = "#ffffcc",
      fill_opacity = 0.9
    )
  )
```

The built-in server handles CORS and range requests automatically. For
tilesets larger than ~1 GB, use an external server like
`npx http-server /path --cors -c-1` for better performance. See the
[Mapping with
mapgl](https://walker-data.com/freestiler/articles/mapping.html) article
for a full walkthrough.

## DuckDB queries

If your data lives in DuckDB,
[`freestile_query()`](https://walker-data.com/freestiler/reference/freestile_query.md)
lets you filter, join, and transform with SQL before tiling:

``` r
freestile_query(
  query = "SELECT * FROM read_parquet('blocks.parquet') WHERE state = 'NC'",
  output = "nc_blocks.pmtiles",
  layer_name = "blocks"
)
```

For very large point datasets, the streaming pipeline avoids loading the
full result into memory. On a recent run,
[`freestile_query()`](https://walker-data.com/freestiler/reference/freestile_query.md)
streamed 146 million US job points from DuckDB into a 2.3 GB PMTiles
archive in about 12 minutes:

``` r
freestile_query(
  query = "SELECT naics, state, ST_Point(lon, lat) AS geometry FROM jobs_dots",
  output = "us_jobs_dots.pmtiles",
  db_path = db_path,
  layer_name = "jobs",
  tile_format = "mvt",
  min_zoom = 4,
  max_zoom = 14,
  base_zoom = 14,
  drop_rate = 2.5,
  source_crs = "EPSG:4326",
  streaming = "always",
  overwrite = TRUE
)
```

## Direct file input

You can tile spatial files without loading them into R first:

``` r
# GeoParquet
freestile_file("census_blocks.parquet", "blocks.pmtiles")

# GeoPackage, Shapefile, or other formats via DuckDB
freestile_file("counties.gpkg", "counties.pmtiles", engine = "duckdb")
```

## Multi-layer tilesets

``` r
pts <- st_centroid(nc)

freestile(
  list(
    counties = freestile_layer(nc, min_zoom = 0, max_zoom = 10),
    centroids = freestile_layer(pts, min_zoom = 6, max_zoom = 14)
  ),
  "nc_layers.pmtiles"
)
```

## Tile formats

freestiler defaults to [MapLibre Tiles
(MLT)](https://github.com/maplibre/maplibre-tile-spec), a columnar
encoding that produces smaller files for polygon and line data. Use
`tile_format = "mvt"` when you need the widest viewer compatibility.

## Learn more

- [Getting
  Started](https://walker-data.com/freestiler/articles/getting-started.html) -
  full tutorial
- [Mapping with
  mapgl](https://walker-data.com/freestiler/articles/mapping.html) -
  viewing and styling tiles with mapgl
- [MapLibre Tiles
  (MLT)](https://walker-data.com/freestiler/articles/maplibre-tiles.html) -
  MLT vs MVT and when to use each
- [Python
  Setup](https://walker-data.com/freestiler/articles/python.html) -
  Python installation and usage
