# freestiler <a href="https://walker-data.com/freestiler/"><img src="man/figures/logo.png" align="right" height="139" alt="freestiler logo" /></a>

<!-- badges: start -->
<!-- badges: end -->

**freestiler** creates [PMTiles](https://github.com/protomaps/PMTiles) vector tilesets from R and Python. Pass it an sf object, a GeoDataFrame, a file on disk (GeoParquet, GeoPackage, Shapefile), or a DuckDB SQL query and it writes a single `.pmtiles` file you can serve from anywhere. The tiling engine is written in Rust and runs entirely in-process, so there's nothing else to install.

Tile format options are [MapLibre Tiles (MLT)](https://github.com/maplibre/maplibre-tile-spec) and Mapbox Vector Tiles (MVT). Other features include multi-layer tilesets, point clustering, feature coalescing, and exponential drop rates for large datasets.

## Installation

### R

Install from [r-universe](https://walkerke.r-universe.dev):

```r
install.packages("freestiler", repos = "https://walkerke.r-universe.dev")
```

Or install the development version from GitHub:

```r
# install.packages("devtools")
devtools::install_github("walkerke/freestiler")
```

### Python

Install from source (requires Rust toolchain):

```bash
cd python
pip install -e .
```

See the [Python Setup](https://walker-data.com/freestiler/articles/python.html) article for venv setup and optional features.

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

## Viewing tiles

**R** -- [mapgl](https://walker-data.com/mapgl/) supports both MLT and MVT. PMTiles need to be served over HTTP, so start a local server first (e.g. `npx serve /tmp -l 8082 --cors` or `cd /tmp && python3 -m http.server 8082`):

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

**Python** -- py-maplibregl and leafmap currently bundle MapLibre GL JS < 5.17, so use `tile_format="mvt"` for now. MLT viewing will work once they upgrade to GL JS 5.17+.

```python
# Create MVT tiles for Python viewer compatibility
freestile(gdf, "nc_counties.pmtiles", layer_name="counties", tile_format="mvt")
```

You can also view any PMTiles file (MLT or MVT) in the browser with [MapLibre GL JS](https://maplibre.org/) 5.17+ and the [PMTiles protocol](https://docs.protomaps.com/pmtiles/).

## Direct file input

You can also tile files on disk without reading them into memory first.

**R**

```r
freestile_file("census_blocks.parquet", "blocks.pmtiles")

freestile_file("counties.gpkg", "counties.pmtiles", engine = "duckdb")
```

**Python**

```python
from freestiler import freestile_file

freestile_file("census_blocks.parquet", "blocks.pmtiles")
```

## SQL queries with DuckDB

If your data lives in a DuckDB database or you want to filter/join before tiling, pass a SQL query directly.

**R**

```r
freestile_query(
  "SELECT * FROM ST_Read('counties.shp') WHERE pop > 50000",
  "large_counties.pmtiles"
)
```

**Python**

```python
from freestiler import freestile_query

freestile_query(
    "SELECT * FROM ST_Read('counties.shp') WHERE pop > 50000",
    "large_counties.pmtiles"
)
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
from shapely import Point

centroids = gdf.copy()
centroids.geometry = gdf.geometry.centroid

freestile(
    {"counties": gdf, "centroids": centroids},
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
