# freestiler <a href="https://walker-data.com/freestiler/"><img src="man/figures/logo.png" align="right" height="139" alt="freestiler logo" /></a>

<!-- badges: start -->
<!-- badges: end -->

**freestiler** creates [PMTiles](https://github.com/protomaps/PMTiles)
vector tilesets from R and Python. Pass it an sf object or GeoDataFrame, a file
on disk, or a DuckDB SQL query and it writes a single `.pmtiles` file you can
serve from anywhere. The tiling engine is written in Rust and runs entirely
in-process, so there's nothing else to install.

Tile format options are [MapLibre Tiles (MLT)](https://github.com/maplibre/maplibre-tile-spec)
and Mapbox Vector Tiles (MVT). Other features include multi-layer tilesets,
point clustering, feature coalescing, and exponential drop rates for large
datasets.

## Why freestiler

- **R and Python API, one Rust engine**. The same tiler powers both packages.
- **Direct DuckDB tiling**. Filter, join, and transform data in SQL before
  writing tiles.
- **PMTiles output**. Write a single portable archive instead of a tile
  directory tree.
- **Serious point workloads**. Large DuckDB point queries can stream directly
  to PMTiles without loading the full result into memory.

## Serious workloads

This is not just a small-data convenience wrapper around vector tile encoding.
On a recent local run, `freestile_query()` streamed `146,575,672` US job points
from DuckDB to an MVT PMTiles archive in about `12 minutes`, producing a
`2.3 GB` tileset with `978,589` tiles.

```r
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

<!-- Screenshot target: nationwide jobs dots at low zoom -->
<!-- Screenshot target: metro-area zoom-in from the same tileset -->

If you want maximum viewer compatibility today, use `tile_format = "mvt"` for
large public-facing point tilesets.

## Installation

### R

Install from [r-universe](https://walkerke.r-universe.dev):

```r
install.packages('freestiler', repos = c('https://walkerke.r-universe.dev', 'https://cloud.r-project.org'))
```

The `r-universe` build is the recommended install for serious DuckDB-backed
workloads. Native macOS and Linux builds include the Rust DuckDB backend by
default, including the streaming point pipeline used by `freestile_query()`.
Windows currently uses the R `duckdb` fallback by default while the GNU DuckDB
toolchain issue is sorted out.

Or install the development version from GitHub:

```r
# install.packages("devtools")
devtools::install_github("walkerke/freestiler")
```

### Python

Install from PyPI:

```bash
pip install freestiler
```

PyPI wheels ship the native feature set, including GeoPandas input, direct
GeoParquet file input, DuckDB-backed file input, and SQL query support.

See the [Python Setup](https://walker-data.com/freestiler/articles/python.html) article for venv setup and optional features.

## Capability summary

| Capability | R | Python |
|---|---|---|
| `freestile()` from in-memory objects | Yes | Yes |
| Direct file tiling | Yes | Yes |
| DuckDB SQL tiling | Yes | Yes |
| Streaming point pipeline for large DuckDB queries | Yes | Yes |
| PMTiles output | Yes | Yes |
| Recommended format for widest viewer compatibility | MVT | MVT |

## Quick start

**R**

```r
library(sf)
library(freestiler)

nc <- st_read(system.file("shape/nc.shp", package = "sf"))

freestile(nc, "nc_counties.pmtiles", layer_name = "counties")
```

**Python**

```python
import geopandas as gpd
from freestiler import freestile

gdf = gpd.read_file("nc.shp")

freestile(gdf, "nc_counties.pmtiles", layer_name="counties")
```

North Carolina counties are a good smoke test, but the big advantage of
`freestiler` is that the same API scales up to much larger jobs.

## Viewing tiles

**R** -- [mapgl](https://walker-data.com/mapgl/) supports both MLT and MVT. PMTiles need HTTP range requests, so start a local server first (e.g. `npx http-server /tmp -p 8082 --cors -c-1`):

```r
library(mapgl)

maplibre() |>
  add_pmtiles_source(
    id = "counties-src",
    url = "http://localhost:8082/nc_counties.pmtiles"
  ) |>
  add_fill_layer(
    id = "county-fill",
    source = "counties-src",
    source_layer = "counties",
    fill_color = "#00897b",
    fill_opacity = 0.5
  )
```

**Python** -- Python viewer stacks are still catching up on MLT, so use
`tile_format="mvt"` for now unless you are viewing in a browser with current
MapLibre GL JS.

```python
# Create MVT tiles for Python viewer compatibility
freestile(gdf, "nc_counties.pmtiles", layer_name="counties", tile_format="mvt")
```

You can also view any PMTiles file (MLT or MVT) in the browser with
[MapLibre GL JS](https://maplibre.org/) 5.17+ and the
[PMTiles protocol](https://docs.protomaps.com/pmtiles/).

## DuckDB queries

If your data already lives in DuckDB, `freestile_query()` is the serious-work
path. This is where streaming matters.

**R**

```r
freestile_query(
  query = "SELECT * FROM read_parquet('blocks.parquet') WHERE state = 'NC'",
  output = "nc_blocks.pmtiles",
  layer_name = "blocks"
)
```

**Python**

```python
from freestiler import freestile_query

freestile_query(
    query="SELECT * FROM read_parquet('blocks.parquet') WHERE state = 'NC'",
    output="nc_blocks.pmtiles",
    layer_name="blocks",
)
```

## Direct file input

You can also tile files on disk without reading them into memory first.

**R** -- supports GeoParquet, GeoPackage, Shapefile, and other formats via DuckDB:

```r
freestile_file("census_blocks.parquet", "blocks.pmtiles")

freestile_file("counties.gpkg", "counties.pmtiles", engine = "duckdb")
```

**Python** -- published wheels include the DuckDB engine, so GeoPackage,
Shapefile, and other DuckDB-backed formats work out of the box:

```python
from freestiler import freestile_file

freestile_file("census_blocks.parquet", "blocks.pmtiles")

freestile_file("counties.gpkg", "counties.pmtiles", engine="duckdb")
```

## Multi-layer tiles

**R**

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

**Python**

```python
from freestiler import freestile, freestile_layer

centroids = gdf.copy()
centroids.geometry = gdf.geometry.centroid

freestile(
    {
        "counties": freestile_layer(gdf, min_zoom=0, max_zoom=10),
        "centroids": freestile_layer(centroids, min_zoom=6, max_zoom=14),
    },
    "nc_layers.pmtiles"
)
```

## Feature management

**R**

```r
# Exponential drop rate: thin features at low zoom levels
freestile(nc, "nc.pmtiles", drop_rate = 2.5, base_zoom = 10)

# Point clustering
freestile(pts, "pts.pmtiles", cluster_distance = 50, cluster_maxzoom = 8)

# Coalesce features with identical attributes
freestile(nc, "nc.pmtiles", coalesce = TRUE)
```

**Python**

```python
freestile(gdf, "nc.pmtiles", drop_rate=2.5, base_zoom=10)

freestile(centroids, "pts.pmtiles", cluster_distance=50, cluster_maxzoom=8)

freestile(gdf, "nc.pmtiles", coalesce=True)
```

## Learn more

- [Getting Started](https://walker-data.com/freestiler/articles/getting-started.html) -- full tutorial with R and Python examples
- [MapLibre Tiles (MLT)](https://walker-data.com/freestiler/articles/maplibre-tiles.html) -- how freestiler encodes the MLT format
- [Python Setup](https://walker-data.com/freestiler/articles/python.html) -- building from source, optional features, and viewer details
