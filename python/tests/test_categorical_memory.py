import gzip
from collections import defaultdict

import geopandas as gpd
import pytest
from shapely.geometry import Point

from freestiler import freestile


def points():
    return gpd.GeoDataFrame(
        {"group": [1.0, 2.0, None, 99.0], "label": ["a", "b", "c", "d"]},
        geometry=[Point(-97, 32), Point(-97.00001, 32.00001),
                  Point(120, -30), Point(120.00001, -30.00001)], crs=4326,
    )


@pytest.mark.parametrize("generate_ids", [True, False])
def test_memory_categories_and_raw_points(tmp_path, generate_ids):
    decoder = pytest.importorskip("mapbox_vector_tile")
    pmtiles = pytest.importorskip("pmtiles.reader")
    output = tmp_path / "plants.pmtiles"
    freestile(points().to_crs(3857), output, layer_name="plants", max_zoom=3,
              cluster_maxzoom=1, cluster_distance=60, category="group",
              category_values=[1, 2], generate_ids=generate_ids, quiet=True)
    seen = defaultdict(dict)
    with output.open("rb") as f:
        for (z, x, y), data in pmtiles.all_tiles(pmtiles.MmapSource(f)):
            for feature in decoder.decode(gzip.decompress(data))["plants"]["features"]:
                props = feature["properties"]
                counts = tuple(props[k] for k in ("group:1", "group:2", "group:_other"))
                if z > 1:
                    assert "cluster" not in props and "point_count" not in props
                    assert sum(counts) == 1
                    assert props["label"] in "abcd"
                elif "cluster" in props:
                    assert props["point_count"] == sum(counts)
                    assert props["cluster_expansion_zoom"] == 2
                key = props.get("cluster_id", props.get("label"))
                seen[z][key] = counts
    assert set(seen) == {0, 1, 2, 3}
    assert all(len(seen[z]) == 4 for z in (2, 3))
    assert any(len(seen[z]) < 4 for z in (0, 1))
    for level in seen.values():
        assert tuple(sum(c[i] for c in level.values()) for i in range(3)) == (1, 1, 2)


def test_invalid_memory_requests_preserve_output(tmp_path):
    output = tmp_path / "keep.pmtiles"
    output.write_bytes(b"previous")
    opts = dict(max_zoom=3, cluster_distance=60, category="group", category_values=[1, 2], quiet=True)
    for change, message in [({"cluster_maxzoom": 4}, "cluster_maxzoom"),
                            ({"category_values": [1, 1]}, "distinct"),
                            ({"drop_rate": 2}, "conserves"),
                            ({"category": "absent"}, "not found"),
                            ({"overwrite": False}, "already exists")]:
        with pytest.raises((ValueError, FileExistsError), match=message):
            freestile(points(), output, **(opts | change))
        assert output.read_bytes() == b"previous"
    invalid = points()
    invalid.loc[0, "geometry"] = Point()
    with pytest.raises(ValueError, match="non-empty POINT"):
        freestile(invalid, output, **opts)
