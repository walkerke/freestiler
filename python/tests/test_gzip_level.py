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
