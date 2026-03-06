"""Tests for DuckDB SQL query input (freestile_query)."""

import pytest
import geopandas as gpd
from shapely.geometry import Point, box

from freestiler import freestile_query

try:
    from freestiler._freestiler import _freestile_duckdb_query  # noqa: F401

    _HAS_DUCKDB = True
except ImportError:
    _HAS_DUCKDB = False

requires_duckdb = pytest.mark.skipif(
    not _HAS_DUCKDB, reason="DuckDB feature not compiled"
)


def _require_pyarrow() -> None:
    pytest.importorskip("pyarrow.parquet", reason="pyarrow is required for parquet fixtures")


@pytest.fixture
def parquet_file(tmp_path):
    """Create a small GeoParquet fixture."""
    _require_pyarrow()
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
    _require_pyarrow()
    gdf = gpd.GeoDataFrame(
        {"label": ["p1", "p2", "p3"], "score": [10.5, 20.3, 30.1]},
        geometry=[Point(-78.6, 35.8), Point(-80.2, 36.1), Point(-82.5, 34.2)],
        crs="EPSG:4326",
    )
    path = tmp_path / "points.parquet"
    gdf.to_parquet(path)
    return path


@requires_duckdb
def test_query_parquet_mlt(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    result = freestile_query(
        f"SELECT * FROM read_parquet('{parquet_file}')",
        output,
        tile_format="mlt",
        max_zoom=6,
        quiet=True,
    )
    assert result.exists()
    assert result.stat().st_size > 0


@requires_duckdb
def test_query_parquet_mvt(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    result = freestile_query(
        f"SELECT * FROM read_parquet('{parquet_file}')",
        output,
        tile_format="mvt",
        max_zoom=6,
        quiet=True,
    )
    assert result.exists()
    assert result.stat().st_size > 0


@requires_duckdb
def test_query_points(tmp_path, point_parquet):
    output = tmp_path / "pts.pmtiles"
    result = freestile_query(
        f"SELECT * FROM read_parquet('{point_parquet}')",
        output,
        max_zoom=6,
        quiet=True,
    )
    assert result.exists()
    assert result.stat().st_size > 0


@requires_duckdb
def test_query_points_streaming(tmp_path, point_parquet):
    output = tmp_path / "pts_stream.pmtiles"
    result = freestile_query(
        f"SELECT * FROM read_parquet('{point_parquet}')",
        output,
        max_zoom=6,
        quiet=True,
        streaming="always",
    )
    assert result.exists()
    assert result.stat().st_size > 0


@requires_duckdb
def test_query_streaming_rejects_non_points(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    with pytest.raises(RuntimeError, match="POINT geometries only"):
        freestile_query(
            f"SELECT * FROM read_parquet('{parquet_file}')",
            output,
            quiet=True,
            streaming="always",
        )


@requires_duckdb
def test_query_with_where_clause(tmp_path, parquet_file):
    output = tmp_path / "filtered.pmtiles"
    result = freestile_query(
        f"SELECT * FROM read_parquet('{parquet_file}') WHERE value > 1.5",
        output,
        max_zoom=6,
        quiet=True,
    )
    assert result.exists()
    assert result.stat().st_size > 0


@requires_duckdb
def test_query_layer_name(tmp_path, parquet_file):
    output = tmp_path / "custom.pmtiles"
    result = freestile_query(
        f"SELECT * FROM read_parquet('{parquet_file}')",
        output,
        layer_name="my_layer",
        max_zoom=4,
        quiet=True,
    )
    assert result.exists()


@requires_duckdb
def test_query_overwrite(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    query = f"SELECT * FROM read_parquet('{parquet_file}')"
    freestile_query(query, output, max_zoom=4, quiet=True)
    freestile_query(query, output, max_zoom=4, quiet=True, overwrite=True)
    assert output.exists()


@requires_duckdb
def test_query_no_overwrite(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    query = f"SELECT * FROM read_parquet('{parquet_file}')"
    freestile_query(query, output, max_zoom=4, quiet=True)
    with pytest.raises(FileExistsError):
        freestile_query(query, output, max_zoom=4, quiet=True, overwrite=False)


def test_query_invalid_format(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    with pytest.raises(ValueError, match="tile_format"):
        freestile_query(
            f"SELECT * FROM read_parquet('{parquet_file}')",
            output,
            tile_format="xyz",
            quiet=True,
        )


def test_query_invalid_streaming_mode(tmp_path, parquet_file):
    output = tmp_path / "out.pmtiles"
    with pytest.raises(ValueError, match="streaming"):
        freestile_query(
            f"SELECT * FROM read_parquet('{parquet_file}')",
            output,
            streaming="sometimes",
            quiet=True,
        )
