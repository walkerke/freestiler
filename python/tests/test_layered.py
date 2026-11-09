"""Multi-layer tiling with one layer in memory at a time (freestile_layers)."""

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Point, box

from freestiler import freestile, freestile_layer, freestile_layers


def _mesh(n=25, offset=0.0):
    cells = [
        box(-78 + i * 0.01, 40 + j * 0.01, -78 + (i + 1) * 0.01, 40 + (j + 1) * 0.01)
        for i in range(n)
        for j in range(n)
    ]
    return gpd.GeoDataFrame(
        {
            "cell_id": range(len(cells)),
            "value": [None if k % 9 == 0 else k * 0.5 + offset for k in range(len(cells))],
            "wet": [k % 2 == 0 for k in range(len(cells))],
        },
        geometry=cells,
        crs="EPSG:4326",
    )


def _layers():
    lines = gpd.GeoDataFrame(
        {"name": ["a", "b"]},
        geometry=[LineString([(-78, 40), (-77.8, 40.2)]), LineString([(-78, 40.2), (-77.8, 40)])],
        crs="EPSG:4326",
    )
    pts = gpd.GeoDataFrame({"label": ["p1"]}, geometry=[Point(-77.9, 40.1)], crs="EPSG:4326")
    return {
        "cells": _mesh(),
        "lines": lines,
        "depth": freestile_layer(_mesh(offset=1.0), min_zoom=8),
        "points": pts,
    }


@pytest.mark.parametrize("tile_format", ["mvt", "mlt"])
def test_layered_matches_in_memory_bytes(tmp_path, tile_format):
    layers = _layers()
    memory = tmp_path / "memory.pmtiles"
    layered = tmp_path / "layered.pmtiles"
    # freestile() with several layers also tiles one layer at a time, so
    # compare against the all-in-memory engine call explicitly.
    from freestiler import _preprocess_layer
    from freestiler._freestiler import _freestile

    rust_layers = []
    for name, value in layers.items():
        gdf = value.input if hasattr(value, "input") else value
        lo = getattr(value, "min_zoom", None) or 0
        rust_layers.append(_preprocess_layer(gdf, name, lo, 12, True))
    _freestile(rust_layers, str(memory), tile_format, 0, 12, -1, True, True, True,
               -1.0, -1.0, -1, False)
    freestile_layers(layers, layered, tile_format=tile_format, max_zoom=12, quiet=True)
    assert memory.read_bytes() == layered.read_bytes()
    via_freestile = tmp_path / "freestile.pmtiles"
    freestile(layers, via_freestile, tile_format=tile_format, max_zoom=12, quiet=True)
    assert via_freestile.read_bytes() == memory.read_bytes()


def test_layered_accepts_paths_callables_and_generators(tmp_path):
    cells = _mesh()
    path = tmp_path / "cells.parquet"
    cells.to_parquet(path)
    expected = tmp_path / "expected.pmtiles"
    freestile({"cells": cells, "depth": _mesh(offset=1.0)}, expected, max_zoom=10, quiet=True)

    def pairs():
        yield "cells", str(path)
        yield "depth", lambda: _mesh(offset=1.0)

    out = tmp_path / "lazy.pmtiles"
    freestile_layers(pairs(), out, max_zoom=10, quiet=True)
    assert out.read_bytes() == expected.read_bytes()


def test_layered_rejects_duplicate_and_missing_layers(tmp_path):
    with pytest.raises(ValueError, match="duplicate"):
        freestile_layers([("a", _mesh()), ("a", _mesh())], tmp_path / "dup.pmtiles", quiet=True)
    with pytest.raises(ValueError, match="at least one"):
        freestile_layers([], tmp_path / "none.pmtiles", quiet=True)
    assert not (tmp_path / "dup.pmtiles").exists()
