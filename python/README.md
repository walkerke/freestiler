# freestiler for Python

`freestiler` builds PMTiles vector tile archives from GeoPandas data,
GeoParquet files, and DuckDB spatial queries using a Rust tiling engine.

Features:

- MapLibre Tiles (`mlt`) and Mapbox Vector Tiles (`mvt`)
- Multi-layer tilesets
- Point clustering
- Feature coalescing
- Exponential feature dropping for low zoom levels

## Why this package exists

- Python-native API backed by the same Rust tiler as the R package
- PMTiles output instead of tile directory trees
- Direct DuckDB SQL tiling
- Streaming point tiling for large DuckDB query results

## Installation

Install from PyPI:

```bash
pip install freestiler
```

Published PyPI wheels ship the native feature set:

- GeoPandas input
- Multi-layer tiling and feature management
- Direct GeoParquet file input
- DuckDB-backed file input
- DuckDB SQL query support

If a wheel is not available for your platform, `pip` will build from source and
requires a Rust toolchain.

## Quick Start

```python
import geopandas as gpd
from freestiler import freestile

gdf = gpd.read_file("counties.shp")

freestile(gdf, "counties.pmtiles", layer_name="counties")
```

That example is intentionally small. The more interesting path is tiling
directly from DuckDB:

```python
from freestiler import freestile_query

freestile_query(
    query="SELECT * FROM read_parquet('blocks.parquet') WHERE state = 'NC'",
    output="nc_blocks.pmtiles",
    layer_name="blocks",
)
```

For very large point tables, use `streaming="always"` and prefer
`tile_format="mvt"` for maximum viewer compatibility.

## Source Builds

Published wheels include GeoParquet and DuckDB support by default. To build
from a local checkout:

```bash
git clone https://github.com/walkerke/freestiler.git
cd freestiler/python
python3 -m venv .venv
source .venv/bin/activate
pip install maturin
python3 -m maturin develop --release
```

To build an installable wheel instead of using an editable install:

```bash
python3 -m maturin build --release --out dist
pip install dist/freestiler-*.whl
```

## Links

- Documentation: https://walker-data.com/freestiler/articles/python.html
- Source: https://github.com/walkerke/freestiler
- Issues: https://github.com/walkerke/freestiler/issues
