import gzip
from collections import defaultdict
import geopandas as gpd
import pytest
from shapely.geometry import Point
from freestiler import freestile_file


def test_ordered_categories_conserve_decoded_counts(tmp_path):
    pytest.importorskip("pyarrow")
    decoder = pytest.importorskip("mapbox_vector_tile")
    pmtiles = pytest.importorskip("pmtiles.reader")
    frame = gpd.GeoDataFrame({"group_id": [1, 2, 1, 2], "label": ["a", "b", "c", "d"]},
        geometry=[Point(-97, 32), Point(-97.00001, 32.00001), Point(179.999, 0), Point(-179.999, 0)], crs=4326)
    source = tmp_path / "source.parquet"
    frame.to_parquet(source)
    output = tmp_path / "clusters.pmtiles"
    freestile_file(source, output, layer_name="clusters", min_zoom=0, max_zoom=3,
        cluster_distance=60, cluster_maxzoom=3, category="group_id", category_values=[1, 2], quiet=True)
    seen = defaultdict(dict)
    with output.open("rb") as f:
        for (z, x, y), data in pmtiles.all_tiles(pmtiles.MmapSource(f)):
            for feature in decoder.decode(gzip.decompress(data))["clusters"]["features"]:
                p = feature["properties"]
                counts = (p["group_id:1"], p["group_id:2"], p["group_id:_other"])
                if sum(counts) > 1:
                    assert "group_id" not in p and "label" not in p
                    assert p["cluster_id"] == feature["id"]
                else:
                    assert "point_count" not in p and p["label"] in ["a", "b", "c", "d"]
                seen[z][feature["id"]] = counts
    assert set(seen) == {0, 1, 2, 3}
    for level in seen.values():
        assert tuple(sum(c[i] for c in level.values()) for i in range(3)) == (2, 2, 0)


def test_reject_unsupported_cluster_combinations_without_overwrite(tmp_path):
    pytest.importorskip("pyarrow")
    source = tmp_path / "source.parquet"
    gpd.GeoDataFrame({"group_id": [1]}, geometry=[Point(0, 0)], crs=4326).to_parquet(source)
    output = tmp_path / "keep.pmtiles"
    output.write_bytes(b"previous")
    opts = dict(cluster_distance=60, max_zoom=3, cluster_maxzoom=3,
                category="group_id", category_values=[1, 2], quiet=True)
    for changes, message in [({"base_zoom": 3}, "base_zoom = None"),
                             ({"simplification": False}, "simplification = True"),
                             ({"drop_rate": 2}, "conserve"),
                             ({"cluster_maxzoom": 2}, "separate dot source"),
                             ({"category_values": [1, 1]}, "distinct"),
                             ({"cluster_distance": float("nan")}, "positive")]:
        with pytest.raises(ValueError, match=message):
            freestile_file(source, output, **(opts | changes))
        assert output.read_bytes() == b"previous"
