# Python Setup

freestiler’s Python package shares the same Rust engine as the R
package. This article covers Python-specific installation and usage. For
the full walkthrough of tiling concepts (zoom levels, drop rates,
multi-layer, clustering, DuckDB queries), see the [Getting
Started](https://walker-data.com/freestiler/articles/getting-started.md)
article - the API is nearly identical between R and Python.

### Installation

Install from PyPI:

``` bash
pip install freestiler
```

The 0.3.0 wheels include GeoPandas input, native GeoParquet and DuckDB
readers, streaming point queries, and categorical file clustering.
Wheels are available for Python 3.9 through 3.14 on Linux x86-64,
Windows x86-64, and macOS Apple Silicon. Other platforms need a source
build.

For H3 binning, also install the Python DuckDB dependency:

``` bash
pip install 'freestiler[h3]'
```

### Building from source

You only need a Rust toolchain if a wheel isn’t available for your
platform or you want an editable local build:

``` bash
git clone https://github.com/walkerke/freestiler.git
cd freestiler/python
python3 -m venv .venv
source .venv/bin/activate
pip install maturin
python3 -m maturin develop --release
```

### Basic usage

The Python API mirrors R closely. Here’s the equivalent of the North
Carolina example from the Getting Started article:

``` python
import geopandas as gpd
from freestiler import freestile

url = "https://raw.githubusercontent.com/r-spatial/sf/main/inst/gpkg/nc.gpkg"
gdf = gpd.read_file(url)

freestile(gdf, "nc_counties.pmtiles", layer_name="counties")
```

Multi-layer tilesets use a dictionary instead of R’s named list:

``` python
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

File input and DuckDB queries work the same way:

``` python
from freestiler import freestile_file, freestile_query

freestile_file("census_blocks.parquet", "blocks.pmtiles")

freestile_query(
    query="SELECT * FROM read_parquet('blocks.parquet') WHERE state = 'NC'",
    output="nc_blocks.pmtiles",
    layer_name="blocks"
)
```

### Large point datasets

Use `streaming="always"` to process a POINT query in disk-backed
partitions:

``` python
freestile_query(
    "SELECT kind, ST_Point(lon, lat) AS geometry FROM read_parquet('places.parquet')",
    "places.pmtiles",
    layer_name="places",
    min_zoom=3,
    max_zoom=14,
    base_zoom=14,
    drop_rate=2.5,
    streaming="always",
)
```

This path does not support lines, polygons, or clustering. See [Planning
a large
job](https://walker-data.com/freestiler/articles/getting-started.html#planning-a-large-job)
for memory and temporary disk settings.

For clusters with category counts, see [Point
clustering](https://walker-data.com/freestiler/articles/point-clustering.md).
For hexagon-only output, category shares, and tie handling, see
[Hexagonal binning with
H3](https://walker-data.com/freestiler/articles/h3-hexagonal-binning.html#hex-only-and-categorical-maps).

### Performance note

`freestile(gdf, ...)` is the most convenient Python entry point, but it
does more preprocessing in GeoPandas before Rust starts tiling. For
large datasets,
[`freestile_file()`](https://walker-data.com/freestiler/reference/freestile_file.md)
and
[`freestile_query()`](https://walker-data.com/freestiler/reference/freestile_query.md)
are usually the faster path.

In practice, the most expensive part of the GeoDataFrame path is often
reprojection to WGS84 plus geometry serialization before the Rust tiler
takes over. If your data is already on disk or in DuckDB, prefer those
paths for serious workloads.

### Viewing tiles

I’d recommend creating tiles with `tile_format="mvt"` for Python-facing
work for now. Python viewer stacks are still catching up on MLT support,
and MVT works everywhere.

One important caveat: Python’s built-in `http.server` does not support
byte-range requests, so it won’t work as a PMTiles server. You’ll want
to use a real static file server instead:

``` bash
npx http-server /path/to/tiles -p 8082 --cors -c-1
```

You can view any PMTiles file (MLT or MVT) in the browser with [MapLibre
GL JS](https://maplibre.org/) 5.17+ and the [PMTiles
protocol](https://docs.protomaps.com/pmtiles/). If you’re also an R
user, the [mapgl](https://walker-data.com/mapgl/) package is the most
reliable local viewing path right now.

My recommendation for Python users: try the Positron IDE, which supports
both Python and R simultaneously. You’ll be able to do your tiling in
Python then easily move over to R for mapping with the mapgl package.
Read the [mapping
vignette](https://walker-data.com/freestiler/articles/mapping.md) to
learn more.

### R vs Python API

| Feature | R | Python |
|----|----|----|
| Input type | sf data frame | GeoDataFrame |
| Multi-layer | Named list | Dict |
| Default format | `"mvt"` | `"mvt"` |
| Zoom range | `min_zoom = 0, max_zoom = 14` | `min_zoom=0, max_zoom=14` |
| Feature dropping | `drop_rate = 2.5` | `drop_rate=2.5` |
| Point clustering | `cluster_distance = 50` | `cluster_distance=50` |
| Feature coalescing | `coalesce = TRUE` | `coalesce=True` |
| File input | [`freestile_file()`](https://walker-data.com/freestiler/reference/freestile_file.md) | [`freestile_file()`](https://walker-data.com/freestiler/reference/freestile_file.md) |
| DuckDB queries | [`freestile_query()`](https://walker-data.com/freestiler/reference/freestile_query.md) | [`freestile_query()`](https://walker-data.com/freestiler/reference/freestile_query.md) |
