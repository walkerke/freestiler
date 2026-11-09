"""gzip_level changes tile compression only."""

import gzip

import geopandas as gpd
import pytest
from shapely.geometry import box

from freestiler import freestile


def _tiles(path):
    from pmtiles.reader import MmapSource, all_tiles

    with open(path, "rb") as f:
        return {key: gzip.decompress(data) for key, data in all_tiles(MmapSource(f))}


def _mesh():
    cells = [box(-78 + i * 0.002, 40 + j * 0.002, -78 + (i + 1) * 0.002, 40 + (j + 1) * 0.002)
             for i in range(40) for j in range(40)]
    return gpd.GeoDataFrame({"cell_id": range(len(cells))}, geometry=cells, crs="EPSG:4326")


def test_gzip_level_shrinks_without_changing_tiles(tmp_path):
    pytest.importorskip("pmtiles")
    fast, best = tmp_path / "fast.pmtiles", tmp_path / "best.pmtiles"
    freestile(_mesh(), fast, layer_name="cells", min_zoom=8, max_zoom=14, quiet=True)
    freestile(_mesh(), best, layer_name="cells", min_zoom=8, max_zoom=14, quiet=True, gzip_level=9)
    assert best.stat().st_size < fast.stat().st_size
    assert _tiles(fast) == _tiles(best)


@pytest.mark.parametrize("level", [-1, 10, 2.5, True, "6"])
def test_gzip_level_rejects_out_of_range(tmp_path, level):
    with pytest.raises(ValueError, match="gzip_level"):
        freestile(_mesh(), tmp_path / "x.pmtiles", max_zoom=10, quiet=True, gzip_level=level)


def test_gzip_level_refused_with_categorical_clustering(tmp_path):
    from shapely.geometry import Point

    pts = gpd.GeoDataFrame({"kind": ["a", "b"]},
                           geometry=[Point(-78, 40), Point(-77.9, 40.1)], crs="EPSG:4326")
    with pytest.raises(ValueError, match="gzip_level"):
        freestile(pts, tmp_path / "x.pmtiles", max_zoom=6, quiet=True, cluster_distance=40,
                  category="kind", category_values=["a", "b"], gzip_level=6)


def _layers():
    from freestiler import freestile_layer

    shifted = _mesh()
    shifted["depth"] = [k * 0.25 for k in range(len(shifted))]
    return {"cells": _mesh(), "depth": freestile_layer(shifted, min_zoom=10)}


@pytest.mark.parametrize("tile_format", ["mvt", "mlt"])
def test_gzip_level_layered_matches_in_memory(tmp_path, tile_format):
    pytest.importorskip("pmtiles")
    from freestiler import _preprocess_layer, freestile_layers
    from freestiler._freestiler import _freestile

    layers = _layers()
    rust_layers = [
        _preprocess_layer(getattr(v, "input", v), name, getattr(v, "min_zoom", None) or 8, 14, True)
        for name, v in layers.items()
    ]
    memory = tmp_path / "memory6.pmtiles"
    _freestile(rust_layers, str(memory), tile_format, 8, 14, -1, True, True, True,
               -1.0, -1.0, -1, False, gzip_level=6)
    layered = freestile_layers(_layers(), tmp_path / "layered6.pmtiles", tile_format=tile_format,
                               min_zoom=8, max_zoom=14, quiet=True, gzip_level=6)
    routed = freestile(_layers(), tmp_path / "routed6.pmtiles", tile_format=tile_format,
                       min_zoom=8, max_zoom=14, quiet=True, gzip_level=6)
    default = freestile_layers(_layers(), tmp_path / "layered1.pmtiles", tile_format=tile_format,
                               min_zoom=8, max_zoom=14, quiet=True)
    # Same compressor on the same raw tiles: identical archives.
    assert memory.read_bytes() == layered.read_bytes() == routed.read_bytes()
    # Only the compression differs from the default level.
    assert layered.stat().st_size < default.stat().st_size
    assert _tiles(layered) == _tiles(default) == _tiles(memory)


def test_gzip_level_single_layer_matches_layered(tmp_path):
    pytest.importorskip("pmtiles")
    from freestiler import freestile_layers

    single = freestile(_mesh(), tmp_path / "single6.pmtiles", layer_name="cells",
                       min_zoom=8, max_zoom=14, quiet=True, gzip_level=6)
    layered = freestile_layers({"cells": _mesh()}, tmp_path / "layered6.pmtiles",
                               min_zoom=8, max_zoom=14, quiet=True, gzip_level=6)
    assert single.read_bytes() == layered.read_bytes()
    assert len(_tiles(single)) > 10


@pytest.mark.parametrize("level", [-1, 10, 2.5, True, "6"])
def test_gzip_level_layered_rejects_bad_levels(tmp_path, level):
    from freestiler import freestile_layers

    out = tmp_path / "x.pmtiles"
    with pytest.raises(ValueError, match="gzip_level"):
        freestile_layers(_layers(), out, max_zoom=10, quiet=True, gzip_level=level)
    with pytest.raises(ValueError, match="gzip_level"):
        freestile(_layers(), out, max_zoom=10, quiet=True, gzip_level=level)
    assert not out.exists()


def test_gzip_level_rust_layered_tiler_refuses_bad_level():
    from freestiler._freestiler import _LayeredTiler

    with pytest.raises(RuntimeError, match="gzip_level"):
        _LayeredTiler("mvt", 0, 10, -1, True, True, True, -1.0, -1.0, -1, False, 12)
