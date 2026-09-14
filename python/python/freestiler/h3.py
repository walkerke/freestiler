"""Dynamic H3 hexagonal binning for freestiler.

``freestile_h3`` aggregates point data into H3 hexagons at zoom-appropriate
resolutions using DuckDB's H3 community extension, then assembles the
per-resolution hex layers and a raw-point layer into a multi-layer PMTiles
archive via :func:`freestiler.freestile`.

This is the Python counterpart of the R ``freestile_h3()`` function and writes
archives with the same layout: one MVT source-layer per H3 resolution (named
``"<hex_layer_prefix>_r<NN>"``) plus a separate points layer. With the default
``fade=False`` the per-layer zoom windows are disjoint (clean zoom breaks);
``fade=True`` overlaps adjacent windows so a viewer can cross-fade between
resolutions.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
import shapely

# H3 community-extension function names are stable across the DuckDB versions
# freestiler targets; see the R implementation for the canonical reference.

_RES_TABLE = {
    0: 1, 1: 1,
    2: 2, 3: 2,
    4: 3,
    5: 4,
    6: 5, 7: 5,
    8: 6,
    9: 7, 10: 7,
    11: 8,
    12: 9, 13: 9,
    14: 10,
}

_FN_MAP = {
    "count": "COUNT",
    "sum": "SUM",
    "mean": "AVG",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "median": "MEDIAN",
}


@dataclass
class _AggSpec:
    names: list
    select_clause: str
    outer_select: str


@dataclass
class _Window:
    resolution: int
    min_zoom: int
    max_zoom: int
    layer_name: str


def freestile_h3(
    input: Union[gpd.GeoDataFrame, str],
    output: Union[str, Path],
    *,
    agg: Union[str, dict] = "count",
    hex_layer_prefix: str = "h3",
    point_layer_name: str = "points",
    min_zoom: int = 0,
    max_zoom: int = 14,
    base_zoom: Optional[int] = None,
    h3_resolutions: Union[None, list, dict] = None,
    source_crs: Optional[str] = None,
    db_path: Optional[str] = None,
    tile_format: str = "mvt",
    fade: bool = False,
    fade_overlap: int = 1,
    overwrite: bool = True,
    quiet: bool = False,
) -> Path:
    """Create vector tiles with dynamic H3 hexagonal binning.

    Aggregates points into H3 hexagons at zoom-appropriate resolutions and
    writes a PMTiles archive in which low zooms show coarse hexagons,
    intermediate zooms show progressively finer hexagons, and zooms at or
    above ``base_zoom`` show individual points. Aggregations are computed in
    DuckDB via the H3 community extension.

    Each distinct H3 resolution becomes its own MVT source-layer (named
    ``"<hex_layer_prefix>_r<NN>"``, e.g. ``"h3_r05"``); raw points are emitted
    as a separate source-layer (``point_layer_name``).

    Parameters
    ----------
    input : GeoDataFrame or str
        A GeoDataFrame of POINT geometry, or a DuckDB SQL query string that
        returns a geometry column. DuckDB spatial functions such as
        ``ST_Read()`` and ``read_parquet()`` are available in SQL input.
    output : str or Path
        Path for the output ``.pmtiles`` file.
    agg : str or dict
        Aggregation specification:

        - ``"count"`` (default): a single ``count = COUNT(*)`` property.
        - A dict mapping property names to SQL aggregation expressions, e.g.
          ``{"n": "COUNT(*)", "avg_pop": "AVG(pop)"}``.
        - A dict mapping property names to ``(fn, column)`` tuples for callers
          who don't want to write SQL, e.g.
          ``{"n": ("count", "*"), "avg_pop": ("mean", "pop")}``. Supported
          ``fn`` values: ``"count"``, ``"sum"``, ``"mean"``/``"avg"``,
          ``"min"``, ``"max"``, ``"median"``.
    hex_layer_prefix : str
        Prefix for the per-resolution hex MVT layer names. Default ``"h3"``
        produces ``"h3_r01"``, ``"h3_r02"``, etc.
    point_layer_name : str
        MVT layer name for raw points (default ``"points"``).
    min_zoom, max_zoom : int
        Global zoom range (default 0--14).
    base_zoom : int, optional
        Zoom level at and above which raw points take over from hex
        aggregations. ``None`` (default) resolves to ``max_zoom - 2``, clamped
        so ``min_zoom <= base_zoom <= max_zoom``. If ``base_zoom == min_zoom``,
        no hex layers are created.
    h3_resolutions : None, list, or dict
        Optional override of the zoom -> H3 resolution mapping. ``None`` uses
        the built-in defaults; a list maps positionally to the hex zooms
        (``range(min_zoom, base_zoom)``, so its length must match); a dict
        keyed by integer zoom level applies sparse overrides with defaults
        filling the rest. All resolutions must be integers in 0..15. The same
        resolution appearing in non-contiguous zoom runs is rejected.
    source_crs : str, optional
        CRS of geometry returned by SQL input, e.g. ``"EPSG:4326"`` or
        ``"EPSG:3857"``. Ignored for GeoDataFrame input (auto-transformed to
        WGS84). For SQL input, if ``None``, a warning is emitted and the
        geometry is assumed to be ``"EPSG:4326"``.
    db_path : str, optional
        Path to an existing DuckDB database file, or ``None`` (default) for an
        in-memory database.
    tile_format : str
        ``"mvt"`` (default) or ``"mlt"``.
    fade : bool
        If ``True``, adjacent hex layer zoom windows overlap by
        ``fade_overlap`` zooms so a viewer can cross-fade between resolutions.
        Default ``False`` (disjoint windows, clean zoom breaks).
    fade_overlap : int
        Zooms of overlap on each side (only used when ``fade=True``, default 1).
    overwrite : bool
        Whether to overwrite an existing output file (default True).
    quiet : bool
        Suppress progress messages (default False).

    Returns
    -------
    Path
        The output file path.

    Notes
    -----
    Requires the ``duckdb`` package and the DuckDB H3 community extension
    (downloaded automatically on first use, which needs network access).

    Hexagons that cross the antimeridian are split at +/-180 degrees so they
    render correctly instead of as world-spanning slivers.

    The raw-point layer is encoded without thinning: every input point is
    written to every tile that contains it from ``base_zoom`` up (and from
    ``base_zoom - fade_overlap`` when ``fade=True``). For very large inputs,
    raising ``base_zoom`` defers points to higher zooms where each tile holds
    fewer of them.
    """
    if tile_format not in ("mvt", "mlt"):
        raise ValueError(f"tile_format must be 'mvt' or 'mlt', got '{tile_format}'")

    min_zoom = int(min_zoom)
    max_zoom = int(max_zoom)
    if min_zoom < 0 or max_zoom < min_zoom:
        raise ValueError("min_zoom and max_zoom must satisfy 0 <= min_zoom <= max_zoom.")

    base_zoom = _resolve_base_zoom(base_zoom, min_zoom, max_zoom)

    if not isinstance(fade, bool):
        raise ValueError("fade must be True or False.")
    fade_overlap = int(fade_overlap)
    if fade_overlap < 0:
        raise ValueError("fade_overlap must be a non-negative integer.")

    output = Path(output).resolve()
    if output.exists() and not overwrite:
        raise FileExistsError(
            f"Output file already exists: {output}. Set overwrite=True to replace."
        )

    agg_spec = _parse_agg(agg)

    # Compute zoom windows up front so invalid h3_resolutions errors early,
    # before opening a database connection.
    windows = _zoom_windows(
        min_zoom, base_zoom, max_zoom, h3_resolutions, fade, fade_overlap, hex_layer_prefix
    )

    if not quiet:
        n = len(windows)
        suffix = f", fade overlap = {fade_overlap}" if fade else ""
        print(
            f"Building H3 tiles (zoom {min_zoom}-{max_zoom}, base_zoom = {base_zoom}, "
            f"{n} hex layer{'' if n == 1 else 's'}{suffix})..."
        )

    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "freestile_h3() requires the 'duckdb' package.\n"
            "Install it with: pip install 'freestiler[h3]' or pip install duckdb."
        ) from exc

    # Imported here to avoid a circular import at module load.
    from freestiler import freestile, freestile_layer

    con = duckdb.connect(db_path if db_path else ":memory:")
    try:
        points_gdf = _open_input(con, input, source_crs, quiet)

        layers: dict = {}
        for window in windows:
            if not quiet:
                print(
                    f"  Aggregating H3 resolution {window.resolution} "
                    f"(zoom {window.min_zoom}-{window.max_zoom})..."
                )
            hex_gdf = _aggregate_resolution(con, window.resolution, agg_spec)
            if len(hex_gdf) == 0:
                if not quiet:
                    print(
                        f"    (no features at resolution {window.resolution}; "
                        "skipping layer)"
                    )
                continue
            layers[window.layer_name] = freestile_layer(
                hex_gdf, min_zoom=window.min_zoom, max_zoom=window.max_zoom
            )

        if points_gdf is None:
            points_gdf = _query_to_gdf(
                con, "SELECT ST_AsWKB(geom) AS __wkb, * EXCLUDE (geom) FROM __h3_input"
            )

        points_min_zoom = max(min_zoom, base_zoom - fade_overlap) if fade else base_zoom
        if points_min_zoom <= max_zoom:
            # The Rust feature encoder needs at least one attribute column; if
            # the points are geometry-only, attach a trivial sequential id.
            if len(points_gdf.columns) <= 1:
                points_gdf = points_gdf.copy()
                points_gdf["__id"] = range(1, len(points_gdf) + 1)
            layers[point_layer_name] = freestile_layer(
                points_gdf, min_zoom=points_min_zoom, max_zoom=max_zoom
            )

        if not layers:
            raise ValueError(
                "No layers were produced. Check base_zoom, min_zoom, max_zoom, "
                "and h3_resolutions."
            )

        return freestile(
            layers,
            output,
            tile_format=tile_format,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            overwrite=overwrite,
            quiet=quiet,
        )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _default_resolution(zoom: int) -> int:
    """Default H3 resolution for a tile zoom (matches the R lookup table)."""
    z = int(zoom)
    if z in _RES_TABLE:
        return _RES_TABLE[z]
    if z < 0:
        return 1
    # Beyond the table: extend linearly, capped at 15.
    return min(15, round(z * 0.7))


def _resolve_base_zoom(base_zoom, min_zoom: int, max_zoom: int) -> int:
    if base_zoom is None:
        return max(min_zoom, max_zoom - 2)
    bz = int(base_zoom)
    if bz < min_zoom or bz > max_zoom:
        raise ValueError(
            f"base_zoom ({bz}) must satisfy min_zoom ({min_zoom}) <= base_zoom "
            f"<= max_zoom ({max_zoom})."
        )
    return bz


def _quote_ident(x) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + str(x).replace('"', '""') + '"'


def _parse_agg(agg) -> _AggSpec:
    if agg == "count":
        names = ["count"]
        exprs = ["COUNT(*)"]
    elif isinstance(agg, dict):
        if not agg:
            raise ValueError("agg dict must be non-empty.")
        names = list(agg.keys())
        exprs = []
        for key, value in agg.items():
            if isinstance(value, str):
                exprs.append(value)
            elif isinstance(value, (tuple, list)) and len(value) == 2:
                fn, col = value
                fn_lower = str(fn).lower()
                sql_fn = _FN_MAP.get(fn_lower)
                if sql_fn is None:
                    raise ValueError(
                        f"Unsupported aggregation function '{fn}'. "
                        f"Use one of: {', '.join(_FN_MAP)}."
                    )
                if fn_lower == "count" and col == "*":
                    exprs.append("COUNT(*)")
                else:
                    exprs.append(f"{sql_fn}({_quote_ident(col)})")
            else:
                raise ValueError(
                    f"agg['{key}'] must be a SQL string or a (fn, column) tuple."
                )
    else:
        raise ValueError(
            "agg must be \"count\", a dict of SQL expressions, "
            "or a dict of (fn, column) tuples."
        )

    quoted = [_quote_ident(n) for n in names]
    select_clause = ", ".join(f"{e} AS {q}" for e, q in zip(exprs, quoted))
    outer_select = ", ".join(quoted)
    return _AggSpec(names=names, select_clause=select_clause, outer_select=outer_select)


def _validate_resolutions(h3_resolutions, hex_zooms: list) -> list:
    n = len(hex_zooms)
    if n == 0:
        return []

    res = [_default_resolution(z) for z in hex_zooms]
    if h3_resolutions is None:
        return res

    def _as_whole(values):
        out = []
        for v in values:
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError("h3_resolutions values must be integers in 0..15.")
            if float(v) != float(int(v)):
                raise ValueError(
                    f"h3_resolutions must be whole numbers in 0..15; "
                    f"got fractional value {v}."
                )
            out.append(int(v))
        return out

    if isinstance(h3_resolutions, dict):
        # Sparse override keyed by zoom level.
        for zoom_key, value in h3_resolutions.items():
            try:
                zk = int(zoom_key)
            except (TypeError, ValueError):
                raise ValueError(
                    f"h3_resolutions keys must be integer zoom levels; got '{zoom_key}'."
                )
            if zk not in hex_zooms:
                raise ValueError(
                    f"h3_resolutions keys must be zooms in "
                    f"[{min(hex_zooms)}, {max(hex_zooms)}]; offending: {zoom_key}."
                )
            res[hex_zooms.index(zk)] = _as_whole([value])[0]
    elif isinstance(h3_resolutions, (list, tuple)):
        if len(h3_resolutions) != n:
            raise ValueError(
                f"List h3_resolutions must have length {n} (one per hex zoom "
                f"{min(hex_zooms)}..{max(hex_zooms)}); got {len(h3_resolutions)}."
            )
        res = _as_whole(h3_resolutions)
    else:
        raise ValueError("h3_resolutions must be None, a list, or a dict.")

    if any(r < 0 or r > 15 for r in res):
        raise ValueError("H3 resolutions must be integers in 0..15.")
    return res


def _rle(values: list) -> list:
    """Run-length encode into a list of (value, length) tuples."""
    runs = []
    for v in values:
        if runs and runs[-1][0] == v:
            runs[-1][1] += 1
        else:
            runs.append([v, 1])
    return runs


def _zoom_windows(
    min_zoom, base_zoom, max_zoom, h3_resolutions, fade, fade_overlap, hex_layer_prefix
) -> list:
    hex_zooms = list(range(min_zoom, base_zoom)) if base_zoom > min_zoom else []
    if not hex_zooms:
        return []

    res = _validate_resolutions(h3_resolutions, hex_zooms)

    runs = _rle(res)
    run_values = [v for v, _ in runs]
    duplicates = {v for v in run_values if run_values.count(v) > 1}
    if duplicates:
        examples = "; ".join(
            f"res {r} at zooms "
            + ", ".join(str(z) for z, rr in zip(hex_zooms, res) if rr == r)
            for r in sorted(duplicates)
        )
        raise ValueError(
            "h3_resolutions produces the same H3 resolution in non-contiguous "
            "zoom runs, which would create colliding MVT layer names: "
            f"{examples}. Flatten the mapping or pick distinct resolutions."
        )

    windows = []
    start = 0
    for value, length in runs:
        run_min = hex_zooms[start]
        run_max = hex_zooms[start + length - 1]
        start += length
        if fade:
            run_min = max(min_zoom, run_min - fade_overlap)
            run_max = min(max_zoom, run_max + fade_overlap)
        windows.append(
            _Window(
                resolution=value,
                min_zoom=run_min,
                max_zoom=run_max,
                layer_name=f"{hex_layer_prefix}_r{value:02d}",
            )
        )
    return windows


def _open_input(con, input, source_crs, quiet) -> Optional[gpd.GeoDataFrame]:
    """Prepare a ``__h3_input`` view in DuckDB; return points GeoDataFrame or None."""
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    try:
        con.execute("INSTALL h3 FROM community")
        con.execute("LOAD h3")
    except Exception as exc:
        raise RuntimeError(
            "Could not load the DuckDB H3 community extension.\n"
            f"Original error: {exc}\n"
            "Make sure you have network access on first use (DuckDB downloads "
            "community extensions on demand) and that your DuckDB version "
            "supports community extensions."
        ) from exc

    if isinstance(input, gpd.GeoDataFrame):
        geom_types = set(input.geom_type.unique())
        if any("MultiPoint" == t for t in geom_types):
            raise ValueError(
                "MULTIPOINT input is not supported in this version of freestile_h3(). "
                "Use input.explode(index_parts=False) to split multi-points first."
            )
        if geom_types != {"Point"}:
            raise ValueError(
                "freestile_h3() requires POINT geometry. Got: "
                + ", ".join(sorted(geom_types))
                + "."
            )

        gdf = input
        if gdf.crs is None:
            warnings.warn(
                "Input GeoDataFrame has no CRS. Assuming WGS84 (EPSG:4326).",
                UserWarning,
                stacklevel=3,
            )
        elif gdf.crs.to_epsg() != 4326:
            if not quiet:
                print("  Transforming input to WGS84...")
            gdf = gdf.to_crs(4326)

        _write_gdf_to_duckdb(con, gdf)
        return gdf

    if isinstance(input, str) and input.strip():
        resolved_crs = source_crs
        if not resolved_crs:
            warnings.warn(
                "source_crs not provided for SQL input - assuming EPSG:4326. "
                "If your query returns geometry in another CRS, hex bins will be "
                "wrong; pass source_crs explicitly to silence this warning.",
                UserWarning,
                stacklevel=3,
            )
            resolved_crs = "EPSG:4326"

        statements = [s.strip() for s in input.split(";") if s.strip()]
        if not statements:
            raise ValueError("input SQL is empty.")
        for stmt in statements[:-1]:
            con.execute(stmt)
        final_sql = statements[-1]

        con.execute(f"CREATE TEMP VIEW __h3_raw AS ({final_sql})")
        desc = con.execute("DESCRIBE __h3_raw").fetchdf()
        geom_rows = desc[desc["column_type"].str.upper().str.startswith("GEOMETRY")]
        if len(geom_rows) == 0:
            raise ValueError(
                "SQL input does not return a geometry column. DuckDB DESCRIBE "
                "returned types: " + ", ".join(desc["column_type"].tolist())
            )
        geom_col = geom_rows["column_name"].iloc[0]

        if resolved_crs == "EPSG:4326":
            geom_select = f"{_quote_ident(geom_col)} AS geom"
        else:
            # always_xy=TRUE: force lon/lat axis order regardless of CRS metadata.
            geom_select = (
                f"ST_Transform({_quote_ident(geom_col)}, '{resolved_crs}', "
                "'EPSG:4326', TRUE) AS geom"
            )
        con.execute(
            f"CREATE TEMP VIEW __h3_input AS SELECT * EXCLUDE ({_quote_ident(geom_col)}), "
            f"{geom_select} FROM __h3_raw"
        )

        gtypes = con.execute(
            "SELECT DISTINCT ST_GeometryType(geom) AS gt FROM __h3_input LIMIT 10"
        ).fetchdf()
        if len(gtypes) == 0:
            raise ValueError("SQL input returned no rows.")
        clean = {t.upper().replace("ST_", "") for t in gtypes["gt"].tolist()}
        if "MULTIPOINT" in clean:
            raise ValueError(
                "MULTIPOINT input is not supported in this version of freestile_h3(). "
                "Cast to POINT in your SQL (e.g. ST_Centroid)."
            )
        if clean != {"POINT"}:
            raise ValueError(
                "freestile_h3() requires POINT geometry. Got: "
                + ", ".join(sorted(clean))
                + "."
            )
        return None

    raise TypeError("input must be a GeoDataFrame of points or a non-empty SQL string.")


def _write_gdf_to_duckdb(con, gdf: gpd.GeoDataFrame) -> None:
    """Materialize a points GeoDataFrame into a ``__h3_input`` view with a geom column."""
    geom_col = gdf.geometry.name
    attrs = gdf.drop(columns=[geom_col]).reset_index(drop=True)
    wkb = shapely.to_wkb(shapely.force_2d(gdf.geometry.values))

    df = attrs.copy()
    df["__geom_wkb"] = list(wkb)

    con.register("__h3_reg", df)
    con.execute("CREATE TEMP TABLE __h3_input_raw AS SELECT * FROM __h3_reg")
    con.unregister("__h3_reg")
    con.execute(
        'CREATE TEMP VIEW __h3_input AS SELECT * EXCLUDE ("__geom_wkb"), '
        'ST_GeomFromWKB("__geom_wkb") AS geom FROM __h3_input_raw'
    )


def _query_to_gdf(con, sql: str) -> gpd.GeoDataFrame:
    """Run a query whose first column is a WKB blob named ``__wkb``; return a GeoDataFrame."""
    df = con.execute(sql).fetchdf()
    if len(df) == 0:
        return gpd.GeoDataFrame(
            df.drop(columns=["__wkb"], errors="ignore"),
            geometry=gpd.GeoSeries([], crs="EPSG:4326"),
        )
    # DuckDB returns BLOB columns as bytearray; shapely.from_wkb wants bytes.
    wkb = [bytes(b) for b in df.pop("__wkb")]
    geom = shapely.from_wkb(wkb)
    return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries(geom, crs="EPSG:4326"))


def _aggregate_resolution(con, resolution: int, agg_spec: _AggSpec) -> gpd.GeoDataFrame:
    resolution = int(resolution)
    if resolution < 0 or resolution > 15:
        raise ValueError("H3 resolution must be an integer in 0..15.")

    sql = (
        "WITH cells AS (\n"
        f"  SELECT h3_latlng_to_cell(ST_Y(geom), ST_X(geom), {resolution}) AS h3, "
        f"{agg_spec.select_clause}\n"
        "  FROM __h3_input\n"
        "  GROUP BY h3\n"
        ")\n"
        "SELECT\n"
        "  ST_AsWKB(ST_GeomFromText(h3_cell_to_boundary_wkt(h3))) AS __wkb,\n"
        "  h3_h3_to_string(h3) AS h3_id,\n"
        f"  {agg_spec.outer_select}\n"
        "FROM cells"
    )
    return _split_antimeridian(_query_to_gdf(con, sql))


def _split_antimeridian(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Split hex polygons that cross the antimeridian into a MultiPolygon.

    ``h3_cell_to_boundary_wkt`` returns raw cell boundaries: a cell crossing
    the antimeridian has vertex longitudes jumping between ~+180 and ~-180,
    which planar tiling renders as a world-spanning sliver. Shift negative
    longitudes by +360 so the ring is contiguous, clip at the dateline, and
    shift the eastern piece back.
    """
    if len(gdf) == 0:
        return gdf

    from shapely import box
    from shapely.geometry import MultiPolygon

    geoms = list(gdf.geometry.values)
    spans = shapely.bounds(gdf.geometry.values)  # (n, 4): minx, miny, maxx, maxy
    changed = False

    def _shift_pos(coords):
        out = coords.copy()
        out[out[:, 0] < 0, 0] += 360.0
        return out

    def _shift_back(coords):
        out = coords.copy()
        out[:, 0] -= 360.0
        return out

    for i, geom in enumerate(geoms):
        minx, maxx = spans[i, 0], spans[i, 2]
        if maxx - minx <= 180:
            continue
        changed = True
        shifted = shapely.transform(geom, _shift_pos)
        west = shifted.intersection(box(-180.0, -90.0, 180.0, 90.0))
        east = shifted.intersection(box(180.0, -90.0, 540.0, 90.0))
        if not east.is_empty:
            east = shapely.transform(east, _shift_back)
        pieces = []
        for part in (west, east):
            if part.is_empty:
                continue
            if part.geom_type == "Polygon":
                pieces.append(part)
            elif part.geom_type == "MultiPolygon":
                pieces.extend(part.geoms)
        geoms[i] = MultiPolygon(pieces) if pieces else geom

    if not changed:
        return gdf

    out = gdf.copy()
    out.geometry = gpd.GeoSeries(geoms, crs=gdf.crs, index=gdf.index)
    return out
