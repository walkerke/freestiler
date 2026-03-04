# freestiler

<!-- badges: start -->
<!-- badges: end -->

**freestiler** is a Rust-powered vector tile engine for R. It takes sf data frames and produces [PMTiles](https://github.com/protomaps/PMTiles) archives with zero external dependencies --- no tippecanoe, no Java, no Go.

## Features

- **Two tile formats**: MapLibre Tiles (MLT) and Mapbox Vector Tiles (MVT)
- **Fast**: parallel Rust backend via extendr
- **Multiple input paths**: sf objects, spatial files (GeoParquet, GeoPackage, Shapefile), or DuckDB SQL queries
- **Multi-layer**: combine multiple sf layers into a single tileset
- **Feature management**: exponential drop rate, point clustering, line/polygon coalescing
- **Simplification**: tile-pixel grid snapping prevents slivers between adjacent polygons
- **Self-contained**: everything runs in-memory, no temp files or external processes

## Installation

Install from [r-universe](https://walkerke.r-universe.dev):

```r
install.packages("freestiler", repos = "https://walkerke.r-universe.dev")
```

Or install the development version from GitHub:

```r
# install.packages("devtools")
devtools::install_github("walkerke/freestiler")
```

## Quick start

```r
library(sf)
library(freestiler)

nc <- st_read(system.file("shape/nc.shp", package = "sf"))

# Create an MLT tileset
freestile(nc, "nc_counties.pmtiles", layer_name = "counties")

# Or use MVT format
freestile(nc, "nc_mvt.pmtiles", layer_name = "counties", tile_format = "mvt")
```

View with [mapgl](https://walker-data.com/mapgl/):

```r
library(mapgl)

maplibre() |>
  add_vector_source(
    id = "counties",
    url = paste0("pmtiles://", normalizePath("nc_counties.pmtiles"))
  ) |>
  add_fill_layer(
    id = "county-fill",
    source = "counties",
    source_layer = "counties",
    fill_color = "#00897b",
    fill_opacity = 0.5
  )
```

## Direct file input

Tile spatial files directly without loading them into R first. Supports GeoParquet, GeoPackage, Shapefile, and any other format readable by DuckDB's spatial extension.

```r
# GeoParquet (uses the geoparquet engine)
freestile_file("census_blocks.parquet", "blocks.pmtiles")

# GeoPackage, Shapefile, or other formats (uses the DuckDB engine)
freestile_file("counties.gpkg", "counties.pmtiles", engine = "duckdb")
```

## SQL queries with DuckDB

Run a SQL query through DuckDB's spatial extension and pipe the results directly into the tiling engine. Filter, join, and transform your data with SQL before tiling.

```r
# Filter features with SQL
freestile_query(
  "SELECT * FROM ST_Read('counties.shp') WHERE pop > 50000",
  "large_counties.pmtiles"
)

# Query a GeoParquet file
freestile_query(
  "SELECT * FROM read_parquet('blocks.parquet') WHERE state = 'NC'",
  "nc_blocks.pmtiles"
)

# Query an existing DuckDB database
freestile_query(
  "SELECT * FROM census_tracts WHERE median_income > 75000",
  "high_income.pmtiles",
  db_path = "census.duckdb"
)
```

The DuckDB backend auto-detects geometry columns and reprojects to WGS84. If the Rust DuckDB feature is not compiled, freestiler falls back to the R `duckdb` package automatically. Control this with `options(freestiler.duckdb_backend = "auto"|"rust"|"r")`.

## Multi-layer tiles

```r
pts <- st_centroid(nc)

freestile(
  list(
    counties = freestile_layer(nc, min_zoom = 0, max_zoom = 10),
    centroids = freestile_layer(pts, min_zoom = 6, max_zoom = 14)
  ),
  "nc_layers.pmtiles"
)
```

## Feature management

```r
# Exponential drop rate: thin features at low zoom levels
freestile(nc, "nc.pmtiles", drop_rate = 2.5, base_zoom = 10)

# Point clustering
freestile(pts, "pts.pmtiles", cluster_distance = 50, cluster_maxzoom = 8)

# Coalesce features with identical attributes
freestile(nc, "nc.pmtiles", coalesce = TRUE)
```

## Learn more

- [Getting Started](https://walker-data.com/freestiler/articles/getting-started.html) --- installation, usage, and all features
- [MapLibre Tiles (MLT)](https://walker-data.com/freestiler/articles/maplibre-tiles.html) --- the MLT format and how freestiler encodes it
- [Python Companion](https://walker-data.com/freestiler/articles/python.html) --- using freestiler from Python

## Python

freestiler also has a Python companion package sharing the same Rust engine. See the [Python article](https://walker-data.com/freestiler/articles/python.html) for details.

```python
from freestiler import freestile
import geopandas as gpd

gdf = gpd.read_file("nc.shp")
freestile(gdf, "nc_counties.pmtiles", layer_name="counties")
```
