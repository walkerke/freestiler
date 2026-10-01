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

Published PyPI wheels ship the native feature set for Python 3.9 through 3.14:

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

### Hexagonal binning

`freestile_h3()` aggregates points into H3 hexagons at zoom-appropriate
resolutions, writing a multi-layer archive where low zooms show coarse
hexagons, intermediate zooms show finer ones, and `base_zoom` and above show
the raw points. It needs the H3 extra (`pip install 'freestiler[h3]'`) for
DuckDB and the DuckDB H3 community extension.

```python
from freestiler import freestile_h3

freestile_h3(
    points,
    "wind.pmtiles",
    agg={"n": "COUNT(*)", "avg_mw": "AVG(capacity_mw)"},
    min_zoom=2, max_zoom=12, base_zoom=10,
)
```

`agg` also accepts `(fn, column)` tuples (e.g. `{"n": ("count", "*")}`) if you
would rather not write SQL.

For a categorical, hex-only archive alongside existing point tiles:

```python
freestile_h3(
    "SELECT ST_Point(lon, lat) AS geom, group_id FROM read_parquet('dots/*.parquet')",
    "groups.pmtiles", include_points=False,
    category="group_id", category_values=list(range(1, 9)),
    source_crs="EPSG:4326", min_zoom=3, max_zoom=11,
    h3_resolutions=[4, 4, 5, 5, 6, 6, 7, 7, 8], fade=True,
)
```

The output includes `point_count`, category counts (`group_id:1`, etc., plus
`group_id:_other` for null/unlisted values), `modal_category`, `modal_count`,
`modal_share`, and `modal_tie`. Ties have no selected category. This avoids a
raw-point GeoDataFrame, but all resulting hex layers still reside in memory;
it is not bounded polygon streaming.

Performance note:

- `freestile(gdf, ...)` is convenient for GeoDataFrames that already fit comfortably in memory.
- For larger datasets, `freestile_file()` and `freestile_query()` are usually faster because they avoid a heavier GeoPandas-to-Rust handoff.
- If your GeoDataFrame still needs `to_crs(4326)` before tiling, that reprojection step can dominate startup time on large layers.

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
