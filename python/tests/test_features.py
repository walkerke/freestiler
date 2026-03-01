"""Tests for feature manipulation options."""

import pytest
import geopandas as gpd
from shapely.geometry import Point, box

from freestiler import freestile


def test_drop_rate(tmp_path):
    """drop_rate parameter should work without error."""
    polys = gpd.GeoDataFrame(
        {"name": [f"p{i}" for i in range(20)]},
        geometry=[box(-80 + i * 0.5, 35, -79.5 + i * 0.5, 35.5) for i in range(20)],
        crs="EPSG:4326",
    )
    output = tmp_path / "test.pmtiles"
    freestile(
        polys, output, drop_rate=2.5, tile_format="mvt", max_zoom=8, quiet=True
    )
    assert output.exists()
    assert output.stat().st_size > 0


def test_cluster_distance(tmp_path):
    """cluster_distance parameter should work for point data."""
    pts = gpd.GeoDataFrame(
        {"name": [f"p{i}" for i in range(50)]},
        geometry=[Point(-80 + i * 0.1, 35 + (i % 5) * 0.1) for i in range(50)],
        crs="EPSG:4326",
    )
    output = tmp_path / "test.pmtiles"
    freestile(
        pts,
        output,
        cluster_distance=50,
        cluster_maxzoom=8,
        tile_format="mvt",
        max_zoom=10,
        quiet=True,
    )
    assert output.exists()
    assert output.stat().st_size > 0


def test_coalesce(tmp_path):
    """coalesce parameter should work without error."""
    polys = gpd.GeoDataFrame(
        {"name": ["same", "same", "other"]},
        geometry=[
            box(-80, 35, -79, 36),
            box(-79, 35, -78, 36),
            box(-82, 34, -81, 35),
        ],
        crs="EPSG:4326",
    )
    output = tmp_path / "test.pmtiles"
    freestile(
        polys, output, coalesce=True, tile_format="mvt", max_zoom=6, quiet=True
    )
    assert output.exists()
    assert output.stat().st_size > 0


def test_no_simplification(tmp_path):
    """simplification=False should work."""
    polys = gpd.GeoDataFrame(
        {"name": ["a"]},
        geometry=[box(-80, 35, -78, 37)],
        crs="EPSG:4326",
    )
    output = tmp_path / "test.pmtiles"
    freestile(
        polys,
        output,
        simplification=False,
        tile_format="mvt",
        max_zoom=4,
        quiet=True,
    )
    assert output.exists()
    assert output.stat().st_size > 0


def test_base_zoom(tmp_path):
    """base_zoom should control when feature dropping stops."""
    polys = gpd.GeoDataFrame(
        {"name": [f"p{i}" for i in range(30)]},
        geometry=[box(-80 + i * 0.3, 35, -79.7 + i * 0.3, 35.3) for i in range(30)],
        crs="EPSG:4326",
    )
    # With base_zoom=6, all features should be kept at zoom 6+
    output = tmp_path / "test_bz.pmtiles"
    freestile(
        polys,
        output,
        drop_rate=2.5,
        base_zoom=6,
        tile_format="mvt",
        max_zoom=8,
        quiet=True,
    )
    assert output.exists()
    assert output.stat().st_size > 0


def test_no_ids(tmp_path):
    """generate_ids=False should work."""
    polys = gpd.GeoDataFrame(
        {"name": ["a"]},
        geometry=[box(-80, 35, -78, 37)],
        crs="EPSG:4326",
    )
    output = tmp_path / "test.pmtiles"
    freestile(
        polys,
        output,
        generate_ids=False,
        tile_format="mvt",
        max_zoom=4,
        quiet=True,
    )
    assert output.exists()
    assert output.stat().st_size > 0
