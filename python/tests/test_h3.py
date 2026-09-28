"""Tests for freestile_h3() — dynamic H3 hexagonal binning.

These mirror the R test-h3.R suite: GeoDataFrame or SQL input -> DuckDB H3
aggregation -> per-resolution hex layers + a points layer -> freestile().
"""

import math
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
from shapely.geometry import MultiPoint, Point, box

from freestiler import freestile_h3
from freestiler import h3 as h3mod

pmtiles_reader = pytest.importorskip("pmtiles.reader")


def test_category_arguments_and_aggregate_only_validation(tmp_path):
    for options, match in [
        (dict(include_points=None), "include_points"),
        (dict(include_points=False, base_zoom=4), "base_zoom"),
        (dict(category_values=[1]), "requires"),
    ]:
        with pytest.raises(ValueError, match=match):
            freestile_h3("invalid SQL", tmp_path / "x.pmtiles", **options)
    for values in ([1, 1], ["_other"], ["_Other"], [1.5], [2**53], list(range(65)), [1, "a"]):
        with pytest.raises(ValueError):
            h3mod._validate_category("g", values)
    with pytest.raises(ValueError, match="reserved"):
        h3mod._parse_agg({"h3_id": "COUNT(*)"})


def _category_sql():
    return (Path(__file__).resolve().parents[2] / "tests/fixtures/h3-categorical.sql").read_text()


def _h3_available() -> bool:
    try:
        import duckdb
    except ImportError:
        return False
    try:
        con = duckdb.connect(":memory:")
        con.execute("INSTALL h3 FROM community")
        con.execute("LOAD h3")
        con.execute("SELECT h3_latlng_to_cell(0, 0, 5)")
        con.close()
        return True
    except Exception:
        return False


requires_h3 = pytest.mark.skipif(
    not _h3_available(), reason="DuckDB H3 community extension not available"
)


@requires_h3
def test_categorical_conservation_ties_and_integer_range():
    import duckdb
    with duckdb.connect() as con:
        h3mod._open_input(con, _category_sql(), "EPSG:4326", True)
        agg = h3mod._parse_agg({"n": "COUNT(*)", "wide": "2147483648::HUGEINT", "avg": "AVG(2.0)"})
        spec = h3mod._category_spec(con, "group_id", [1, 2, 3], agg.names)
        for resolution in (2, 4):
            out = h3mod._aggregate_resolution(con, resolution, agg, spec)
            assert out.point_count.sum() == 8
            assert out[spec["keys"]].sum().tolist() == [4, 2, 0, 2]
            assert out.modal_tie.sum() == 1
            assert out.loc[out.modal_tie, "modal_category"].isna().all()
            assert out.loc[out.modal_category == "_other", "modal_count"].tolist() == [2]
            assert out.wide.tolist() == [2147483648] * len(out)
            assert str(out.wide.dtype) == "Int64"
            for geom in out.geometry:
                parts = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
                assert all(p.bounds[2] - p.bounds[0] <= 180 for p in parts)
        with pytest.raises(ValueError, match="collide"):
            h3mod._category_spec(con, "group_id", [1, 2], ["point_count"])
        con.execute("CREATE TEMP TABLE limits AS SELECT 9007199254740992::HUGEINT AS too_big")
        with pytest.raises(ValueError, match="exact range"):
            h3mod._check_integer_fields(con, "limits")


@requires_h3
def test_aggregate_only_decoded_tiles_and_no_raw_fetch(tmp_path, monkeypatch):
    import gzip
    import mapbox_vector_tile
    original = h3mod._query_to_gdf
    def guarded(con, sql):
        assert "EXCLUDE (geom)" not in sql, "raw points fetched"
        return original(con, sql)
    monkeypatch.setattr(h3mod, "_query_to_gdf", guarded)
    output = tmp_path / "category.pmtiles"
    freestile_h3(_category_sql(), output, min_zoom=3, max_zoom=5,
                 h3_resolutions=[2, 2, 3], include_points=False, fade=True,
                 source_crs="EPSG:4326", category="group_id", category_values=[1, 2, 3],
                 agg={"wide": "2147483648::HUGEINT", "avg": "AVG(2.0)"}, quiet=True)
    with open(output, "rb") as f:
        source = pmtiles_reader.MmapSource(f)
        layers = pmtiles_reader.Reader(source).metadata()["vector_layers"]
        assert {l["id"] for l in layers} == {"h3_r02", "h3_r03"}
        assert layers[0]["fields"]["point_count"] == "Number"
        assert layers[0]["fields"]["modal_tie"] == "Boolean"
        assert layers[0]["fields"]["modal_category"] == "String"
        by_zoom_layer = {}
        for (z, _, _), tile in pmtiles_reader.all_tiles(source):
            decoded = mapbox_vector_tile.decode(gzip.decompress(tile))
            for name, layer in decoded.items():
                cells = by_zoom_layer.setdefault((z, name), {})
                for feature in layer["features"]:
                    props = feature["properties"]
                    assert type(props["wide"]) is int and props["wide"] == 2147483648
                    assert type(props["avg"]) is float and props["avg"] == 2.0
                    assert type(props["modal_tie"]) is bool
                    if props["modal_tie"]:
                        assert "modal_category" not in props
                    previous = cells.setdefault(props["h3_id"], props)
                    assert previous == props
        for meta in layers:
            for z in range(meta["minzoom"], meta["maxzoom"] + 1):
                cells = by_zoom_layer[(z, meta["id"])].values()
                assert sum(p["point_count"] for p in cells) == 8
                assert [sum(p[f"group_id:{g}"] for p in cells) for g in (1,2,3,"_other")] == [4,2,0,2]


@requires_h3
def test_string_dictionary_inference_and_empty_dictionary():
    import duckdb
    con = duckdb.connect()
    try:
        sql = '''SELECT ST_Point(0,0) AS geom, g AS "a""b"
                 FROM (VALUES ('a''b'), ('x:y'), (NULL)) t(g)'''
        h3mod._open_input(con, sql, "EPSG:4326", quiet=True)
        agg = h3mod._parse_agg("count")
        spec = h3mod._category_spec(con, 'a"b', None, agg.names)
        out = h3mod._aggregate_resolution(con, 4, agg, spec)
        assert out[spec["keys"]].iloc[0].tolist() == [1, 1, 1]
        assert bool(out.modal_tie.iloc[0])
        empty = h3mod._category_spec(con, 'a"b', [], agg.names)
        out = h3mod._aggregate_resolution(con, 4, agg, empty)
        assert out.point_count.iloc[0] == 3
        assert out.modal_category.iloc[0] == "_other"
        assert not bool(out.modal_tie.iloc[0])
    finally:
        con.close()


def _points(n=2000, seed=1):
    rng = np.random.default_rng(seed)
    xs = rng.uniform(-100, -80, n)
    ys = rng.uniform(30, 45, n)
    return gpd.GeoDataFrame(
        {"w": rng.normal(100, 10, n), "cat": rng.choice(list("abc"), n)},
        geometry=[Point(x, y) for x, y in zip(xs, ys)],
        crs="EPSG:4326",
    )


def _layer_ids(path):
    meta = pmtiles_reader.Reader(pmtiles_reader.MmapSource(open(path, "rb"))).metadata()
    return meta["vector_layers"]


# ---------------------------------------------------------------------------
# Pure-Python helper tests (no DuckDB required)
# ---------------------------------------------------------------------------


def test_default_resolution_table():
    assert h3mod._default_resolution(0) == 1
    assert h3mod._default_resolution(8) == 6
    assert h3mod._default_resolution(14) == 10
    assert h3mod._default_resolution(-3) == 1
    assert h3mod._default_resolution(20) == 14  # round(20*0.7)


def test_resolve_base_zoom_default_and_bounds():
    assert h3mod._resolve_base_zoom(None, 0, 14) == 12
    assert h3mod._resolve_base_zoom(None, 10, 11) == 10  # clamped to min_zoom
    with pytest.raises(ValueError, match="base_zoom"):
        h3mod._resolve_base_zoom(20, 0, 14)


def test_parse_agg_forms():
    spec = h3mod._parse_agg("count")
    assert spec.names == ["count"]
    assert spec.select_clause == 'COUNT(*) AS "count"'

    spec = h3mod._parse_agg({"n": "COUNT(*)", "avg_w": "AVG(w)"})
    assert spec.names == ["n", "avg_w"]
    assert spec.outer_select == '"n", "avg_w"'

    spec = h3mod._parse_agg({"n": ("count", "*"), "m": ("mean", "pop")})
    assert 'COUNT(*) AS "n"' in spec.select_clause
    assert 'AVG("pop") AS "m"' in spec.select_clause


def test_parse_agg_escapes_quotes():
    spec = h3mod._parse_agg({'a"b': ("mean", 'po"p')})
    assert 'AVG("po""p") AS "a""b"' in spec.select_clause


def test_parse_agg_rejects_bad_input():
    with pytest.raises(ValueError):
        h3mod._parse_agg("sum")  # bare string other than "count"
    with pytest.raises(ValueError):
        h3mod._parse_agg({"n": ("nonsense", "x")})
    with pytest.raises(ValueError):
        h3mod._parse_agg({})


def test_zoom_windows_disjoint_default():
    wins = h3mod._zoom_windows(2, 6, 8, None, False, 1, "h3")
    spans = [(w.min_zoom, w.max_zoom) for w in wins]
    # windows must be contiguous and non-overlapping
    for (a_min, a_max), (b_min, b_max) in zip(spans, spans[1:]):
        assert a_max < b_min


def test_zoom_windows_fade_overlaps():
    plain = h3mod._zoom_windows(2, 10, 12, None, False, 1, "h3")
    faded = h3mod._zoom_windows(2, 10, 12, None, True, 1, "h3")
    if len(faded) >= 2:
        # adjacent faded windows touch or overlap
        assert any(
            faded[i].max_zoom >= faded[i + 1].min_zoom for i in range(len(faded) - 1)
        )


def test_h3_resolutions_list_length_mismatch():
    with pytest.raises(ValueError, match="length"):
        h3mod._zoom_windows(0, 7, 10, [4, 4, 4], False, 1, "h3")  # needs 7


def test_h3_resolutions_sparse_dict_honored():
    wins = h3mod._zoom_windows(0, 4, 10, {0: 2, 1: 2, 2: 2, 3: 2}, False, 1, "h3")
    assert len(wins) == 1
    assert wins[0].resolution == 2


def test_h3_resolutions_noncontiguous_rejected():
    with pytest.raises(ValueError, match="non-contiguous"):
        # res 4 at zooms 0-1 and again at 3 (res 6 between) -> collision
        h3mod._zoom_windows(0, 4, 10, [4, 4, 6, 4], False, 1, "h3")


def test_h3_resolutions_fractional_rejected():
    with pytest.raises(ValueError, match="whole numbers"):
        h3mod._zoom_windows(0, 3, 10, [4.5, 4, 4], False, 1, "h3")


def test_split_antimeridian_unit():
    crossing = gpd.GeoSeries.from_wkt(
        [
            "POLYGON ((179.866311 1.855493, 179.180049 0.659388, "
            "179.839797 -0.719798, -178.798236 -0.936164, "
            "-178.078803 0.254874, -178.754302 1.667852, 179.866311 1.855493))"
        ],
        crs="EPSG:4326",
    )
    normal = gpd.GeoSeries.from_wkt(
        ["POLYGON ((0 0, 1 0, 1.5 0.8, 1 1.6, 0 1.6, -0.5 0.8, 0 0))"],
        crs="EPSG:4326",
    )
    gdf = gpd.GeoDataFrame(
        {"h3_id": ["a", "b"], "n": [10, 20]},
        geometry=list(crossing) + list(normal),
        crs="EPSG:4326",
    )
    out = h3mod._split_antimeridian(gdf)

    assert list(out["h3_id"]) == ["a", "b"]
    assert out.geometry.iloc[0].geom_type == "MultiPolygon"
    # no surviving piece spans the globe
    for part in out.geometry.iloc[0].geoms:
        minx, _, maxx, _ = part.bounds
        assert maxx - minx < 180
    # the normal hex is unchanged
    assert out.geometry.iloc[1].equals(gdf.geometry.iloc[1])


def test_split_antimeridian_noop_away_from_dateline():
    gdf = gpd.GeoDataFrame(
        {"n": [5]},
        geometry=gpd.GeoSeries.from_wkt(
            ["POLYGON ((-100 40, -99 40, -98.5 40.8, -99 41.6, -100 41.6, -100.5 40.8, -100 40))"]
        ),
        crs="EPSG:4326",
    )
    out = h3mod._split_antimeridian(gdf)
    assert out.geometry.iloc[0].equals(gdf.geometry.iloc[0])


# ---------------------------------------------------------------------------
# End-to-end tests (require DuckDB + H3 extension)
# ---------------------------------------------------------------------------


@requires_h3
def test_default_agg_layer_set(tmp_path):
    out = tmp_path / "h3.pmtiles"
    freestile_h3(_points(), out, min_zoom=2, max_zoom=8, base_zoom=6, quiet=True)
    assert out.exists() and out.stat().st_size > 0

    layers = _layer_ids(out)
    ids = [l["id"] for l in layers]
    assert any(i.startswith("h3_r") for i in ids)
    assert "points" in ids

    # default fade=False -> disjoint hex windows
    hexes = sorted(
        (l for l in layers if l["id"].startswith("h3_r")), key=lambda l: l["minzoom"]
    )
    for a, b in zip(hexes, hexes[1:]):
        assert a["maxzoom"] < b["minzoom"]


@requires_h3
def test_custom_agg_becomes_properties(tmp_path):
    out = tmp_path / "h3.pmtiles"
    freestile_h3(
        _points(),
        out,
        agg={"n": "COUNT(*)", "avg_w": "AVG(w)"},
        min_zoom=2,
        max_zoom=7,
        base_zoom=5,
        quiet=True,
    )
    hexes = [l for l in _layer_ids(out) if l["id"].startswith("h3_r")]
    fields = hexes[0]["fields"]
    assert "n" in fields and "avg_w" in fields and "h3_id" in fields


@requires_h3
def test_agg_tuple_form(tmp_path):
    out = tmp_path / "h3.pmtiles"
    freestile_h3(
        _points(),
        out,
        agg={"n": ("count", "*"), "avg_w": ("mean", "w")},
        min_zoom=2,
        max_zoom=6,
        base_zoom=5,
        quiet=True,
    )
    fields = [l for l in _layer_ids(out) if l["id"].startswith("h3_r")][0]["fields"]
    assert "n" in fields and "avg_w" in fields


@requires_h3
def test_rejects_multipoint(tmp_path):
    gdf = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[MultiPoint([(0, 0), (1, 1)])],
        crs="EPSG:4326",
    )
    with pytest.raises(ValueError, match="MULTIPOINT"):
        freestile_h3(gdf, tmp_path / "x.pmtiles", quiet=True)


@requires_h3
def test_rejects_non_point(tmp_path):
    gdf = gpd.GeoDataFrame(
        {"id": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"
    )
    with pytest.raises(ValueError, match="POINT"):
        freestile_h3(gdf, tmp_path / "x.pmtiles", quiet=True)


@requires_h3
def test_base_zoom_equals_min_zoom_points_only(tmp_path):
    out = tmp_path / "h3.pmtiles"
    freestile_h3(_points(), out, min_zoom=4, max_zoom=8, base_zoom=4, quiet=True)
    ids = [l["id"] for l in _layer_ids(out)]
    assert ids == ["points"]


@requires_h3
def test_crs_reprojection(tmp_path):
    pts = _points().to_crs(3857)
    out = tmp_path / "h3.pmtiles"
    freestile_h3(pts, out, min_zoom=2, max_zoom=7, base_zoom=5, quiet=True)
    assert out.exists() and out.stat().st_size > 0


@requires_h3
def test_fade_produces_overlap(tmp_path):
    out = tmp_path / "h3.pmtiles"
    freestile_h3(
        _points(), out, min_zoom=2, max_zoom=12, base_zoom=10, fade=True, quiet=True
    )
    hexes = sorted(
        (l for l in _layer_ids(out) if l["id"].startswith("h3_r")),
        key=lambda l: l["minzoom"],
    )
    if len(hexes) >= 2:
        assert any(
            hexes[i]["maxzoom"] >= hexes[i + 1]["minzoom"] for i in range(len(hexes) - 1)
        )


@requires_h3
def test_geometry_only_input_succeeds(tmp_path):
    rng = np.random.default_rng(2)
    pts = gpd.GeoDataFrame(
        geometry=[
            Point(x, y)
            for x, y in zip(rng.uniform(-100, -80, 500), rng.uniform(30, 45, 500))
        ],
        crs="EPSG:4326",
    )
    out = tmp_path / "h3.pmtiles"
    freestile_h3(pts, out, min_zoom=2, max_zoom=6, base_zoom=5, quiet=True)
    assert out.exists() and out.stat().st_size > 0


@requires_h3
def test_sql_input(tmp_path, monkeypatch):
    pts = _points(500)
    parquet = tmp_path / "pts.parquet"
    pts.to_parquet(parquet)
    out = tmp_path / "h3.pmtiles"
    freestile_h3(
        f"SELECT geometry, w FROM read_parquet('{parquet.as_posix()}')",
        out,
        agg={"n": "COUNT(*)", "avg_w": "AVG(w)"},
        source_crs="EPSG:4326",
        min_zoom=2,
        max_zoom=6,
        base_zoom=5,
        quiet=True,
    )
    assert out.exists() and out.stat().st_size > 0


@requires_h3
def test_sql_input_without_source_crs_warns(tmp_path):
    pts = _points(300)
    parquet = tmp_path / "pts.parquet"
    pts.to_parquet(parquet)
    out = tmp_path / "h3.pmtiles"
    with pytest.warns(UserWarning, match="source_crs"):
        freestile_h3(
            f"SELECT geometry, w FROM read_parquet('{parquet.as_posix()}')",
            out,
            min_zoom=2,
            max_zoom=6,
            base_zoom=5,
            quiet=True,
        )


@requires_h3
def test_sql_input_epsg3857_axis_order(tmp_path):
    """ST_Transform must force lon/lat (x,y) order, not authority (lat,lon)."""
    import duckdb

    pts = _points(800).to_crs(3857)
    parquet = tmp_path / "pts.parquet"
    pts.to_parquet(parquet)

    con = duckdb.connect(":memory:")
    try:
        h3mod._open_input(
            con,
            f"SELECT geometry, w FROM read_parquet('{parquet.as_posix()}')",
            "EPSG:3857",
            True,
        )
        gdf = h3mod._aggregate_resolution(con, 4, h3mod._parse_agg("count"))
    finally:
        con.close()

    # Hex vertices should sit in the western-US lon/lat box, not swapped.
    minx, miny, maxx, maxy = gdf.total_bounds
    assert -101 < minx and maxx < -78
    assert 29 < miny and maxy < 47


@requires_h3
def test_antimeridian_end_to_end(tmp_path):
    rng = np.random.default_rng(42)
    xs = np.concatenate(
        [rng.uniform(179.2, 179.99, 100), rng.uniform(-179.99, -179.2, 100)]
    )
    ys = rng.uniform(-5, 5, 200)
    pts = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x, y in zip(xs, ys)], crs="EPSG:4326"
    )
    out = tmp_path / "h3.pmtiles"
    freestile_h3(pts, out, min_zoom=0, max_zoom=6, base_zoom=5, quiet=True)
    assert out.exists() and out.stat().st_size > 0


@requires_h3
def test_invalid_agg_raises(tmp_path):
    with pytest.raises(ValueError):
        freestile_h3(_points(100), tmp_path / "x.pmtiles", agg=42, quiet=True)
