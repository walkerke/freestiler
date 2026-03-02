"""Tests for direct file input (GeoParquet -> PMTiles)."""

import pytest
import geopandas as gpd
from shapely.geometry import Point, box

from freestiler import freestile_file


@pytest.fixture
def parquet_file(tmp_path):
    """Create a small GeoParquet fixture."""
    gdf = gpd.GeoDataFrame(
        {
            "name": ["a", "b", "c"],
            "value": [1.0, 2.0, 3.0],
            "count": [10, 20, 30],
        },
        geometry=[
            box(-80, 35, -78, 37),
            box(-82, 34, -79, 36),
            box(-84, 33, -81, 35),
        ],
        crs="EPSG:4326",
    )
    path = tmp_path / "test.parquet"
    gdf.to_parquet(path)
    return path


@pytest.fixture
def point_parquet(tmp_path):
    """Create a small point GeoParquet fixture."""
    gdf = gpd.GeoDataFrame(
        {"label": ["p1", "p2", "p3"], "score": [10.5, 20.3, 30.1]},
        geometry=[Point(-78.6, 35.8), Point(-80.2, 36.1), Point(-82.5, 34.2)],
        crs="EPSG:4326",
    )
    path = tmp_path / "points.parquet"
    gdf.to_parquet(path)
    return path


def test_parquet_to_mvt(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    result = freestile_file(parquet_file, output, tile_format="mvt", max_zoom=6, quiet=True)
    assert result.exists()
    assert result.stat().st_size > 0


def test_parquet_to_mlt(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    result = freestile_file(parquet_file, output, tile_format="mlt", max_zoom=6, quiet=True)
    assert result.exists()
    assert result.stat().st_size > 0


def test_parquet_points(tmp_path, point_parquet):
    output = tmp_path / "pts.pmtiles"
    result = freestile_file(point_parquet, output, max_zoom=6, quiet=True)
    assert result.exists()
    assert result.stat().st_size > 0


def test_parquet_layer_name(tmp_path, parquet_file):
    output = tmp_path / "custom.pmtiles"
    result = freestile_file(
        parquet_file, output, layer_name="my_layer", max_zoom=4, quiet=True
    )
    assert result.exists()


def test_parquet_overwrite(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    freestile_file(parquet_file, output, max_zoom=4, quiet=True)
    freestile_file(parquet_file, output, max_zoom=4, quiet=True, overwrite=True)
    assert output.exists()


def test_parquet_no_overwrite(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    freestile_file(parquet_file, output, max_zoom=4, quiet=True)
    with pytest.raises(FileExistsError):
        freestile_file(parquet_file, output, max_zoom=4, quiet=True, overwrite=False)


def test_parquet_missing_file(tmp_path):
    output = tmp_path / "out.pmtiles"
    with pytest.raises(FileNotFoundError):
        freestile_file(tmp_path / "nonexistent.parquet", output, quiet=True)


def test_parquet_invalid_format(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    with pytest.raises(ValueError, match="tile_format"):
        freestile_file(parquet_file, output, tile_format="xyz", quiet=True)
