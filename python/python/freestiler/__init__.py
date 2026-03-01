"""freestiler: Rust-powered MLT/MVT vector tile engine for Python."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Union

import geopandas as gpd
import numpy as np
import shapely

from freestiler._freestiler import _freestile


def freestile(
    input: Union[gpd.GeoDataFrame, dict[str, gpd.GeoDataFrame]],
    output: Union[str, Path],
    *,
    layer_name: str | None = None,
    tile_format: str = "mlt",
    min_zoom: int = 0,
    max_zoom: int = 14,
    base_zoom: int | None = None,
    drop_rate: float | None = None,
    cluster_distance: float | None = None,
    cluster_maxzoom: int | None = None,
    coalesce: bool = False,
    simplification: bool = True,
    generate_ids: bool = True,
    overwrite: bool = True,
    quiet: bool = False,
) -> Path:
    """Create a PMTiles archive from geospatial data.

    Parameters
    ----------
    input : GeoDataFrame or dict[str, GeoDataFrame]
        A single GeoDataFrame for single-layer output, or a dict mapping
        layer names to GeoDataFrames for multi-layer output.
    output : str or Path
        Output path for the .pmtiles file.
    layer_name : str, optional
        Name for the tile layer. If None, derived from the output filename.
        Only used for single-layer input.
    tile_format : str
        Tile encoding format: "mlt" (default) for MapLibre Tiles or "mvt"
        for Mapbox Vector Tiles.
    min_zoom : int
        Minimum zoom level (default 0).
    max_zoom : int
        Maximum zoom level (default 14).
    base_zoom : int, optional
        Zoom level at and above which ALL features are kept (no dropping).
        None defaults to each layer's max_zoom (like tippecanoe -B).
    drop_rate : float, optional
        Exponential drop rate for feature thinning. None disables.
    cluster_distance : float, optional
        Pixel distance for point clustering. None disables.
    cluster_maxzoom : int, optional
        Maximum zoom level for clustering. Default is max_zoom - 1.
    coalesce : bool
        Whether to merge features with identical attributes (default False).
    simplification : bool
        Whether to snap geometries to the tile pixel grid (default True).
    generate_ids : bool
        Whether to assign sequential feature IDs (default True).
    overwrite : bool
        Whether to overwrite existing output file (default True).
    quiet : bool
        Whether to suppress progress messages (default False).

    Returns
    -------
    Path
        The output file path.
    """
    if tile_format not in ("mlt", "mvt"):
        raise ValueError(f"tile_format must be 'mlt' or 'mvt', got '{tile_format}'")

    output = Path(output).resolve()

    if output.exists():
        if overwrite:
            output.unlink()
        else:
            raise FileExistsError(
                f"Output file already exists: {output}. Set overwrite=True to replace."
            )

    # Determine default layer_name
    if layer_name is None and isinstance(input, gpd.GeoDataFrame):
        layer_name = output.stem

    # Normalize to dict of layers
    if isinstance(input, gpd.GeoDataFrame):
        layers_dict = {layer_name or "default": input}
    elif isinstance(input, dict):
        layers_dict = input
    else:
        raise TypeError(
            "input must be a GeoDataFrame or dict[str, GeoDataFrame]"
        )

    # Count total features
    total_features = sum(len(gdf) for gdf in layers_dict.values())

    if not quiet:
        n_layers = len(layers_dict)
        print(
            f"Creating {tile_format.upper()} tiles (zoom {min_zoom}-{max_zoom}) "
            f"for {total_features} features across {n_layers} "
            f"layer{'s' if n_layers != 1 else ''}..."
        )

    # Preprocess each layer
    rust_layers = []
    for name, gdf in layers_dict.items():
        layer_data = _preprocess_layer(gdf, name, min_zoom, max_zoom, quiet)
        rust_layers.append(layer_data)

    # Call Rust
    result = _freestile(
        layers=rust_layers,
        output_path=str(output),
        tile_format=tile_format,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        base_zoom=base_zoom if base_zoom is not None else -1,
        do_simplify=simplification,
        generate_ids=generate_ids,
        quiet=quiet,
        drop_rate=drop_rate if drop_rate is not None else -1.0,
        cluster_distance=cluster_distance if cluster_distance is not None else -1.0,
        cluster_maxzoom=cluster_maxzoom if cluster_maxzoom is not None else -1,
        do_coalesce=coalesce,
    )

    if not quiet:
        size = output.stat().st_size
        print(f"Created {output} ({_format_size(size)})")

    return output


def _preprocess_layer(
    gdf: gpd.GeoDataFrame,
    name: str,
    min_zoom: int,
    max_zoom: int,
    quiet: bool,
) -> dict:
    """Preprocess a GeoDataFrame into a dict for the Rust tiling engine."""
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError(f"Layer '{name}' must be a GeoDataFrame")

    if len(gdf) == 0:
        raise ValueError(f"Layer '{name}' has no features")

    # CRS -> WGS84
    if gdf.crs is None:
        warnings.warn(
            f"Layer '{name}' has no CRS. Assuming WGS84 (EPSG:4326).",
            UserWarning,
            stacklevel=3,
        )
    elif not gdf.crs.is_geographic or gdf.crs.to_epsg() != 4326:
        if not quiet:
            print(f"  Transforming layer '{name}' to WGS84...")
        gdf = gdf.to_crs(4326)

    # Force 2D (drop Z/M)
    geom_array = gdf.geometry.values
    geom_2d = shapely.force_2d(geom_array)

    # Export WKB (vectorized)
    wkb_list = [bytes(shapely.to_wkb(g)) for g in geom_2d]

    # Geometry types
    type_ids = shapely.get_type_id(geom_2d)
    type_map = {
        0: "POINT",
        1: "LINESTRING",
        3: "POLYGON",
        4: "MULTIPOINT",
        5: "MULTILINESTRING",
        6: "MULTIPOLYGON",
    }
    geom_types = [type_map.get(int(t), "UNKNOWN") for t in type_ids]

    # Extract properties (column-oriented, typed)
    attrs = gdf.drop(columns=[gdf.geometry.name])

    prop_names = []
    prop_types = []
    string_cols = []
    int_cols = []
    float_cols = []
    bool_cols = []

    for col_name in attrs.columns:
        series = attrs[col_name]
        prop_names.append(str(col_name))

        import pandas as pd

        dtype = series.dtype
        dtype_name = getattr(dtype, "name", str(dtype)).lower()

        if dtype_name in ("bool", "boolean") or pd.api.types.is_bool_dtype(series):
            prop_types.append("boolean")
            bool_cols.append(
                [None if pd_isna(v) else bool(v) for v in series]
            )
        elif pd.api.types.is_integer_dtype(series):
            prop_types.append("integer")
            int_cols.append(
                [None if pd_isna(v) else int(v) for v in series]
            )
        elif pd.api.types.is_float_dtype(series):
            prop_types.append("double")
            float_cols.append(
                [None if pd_isna(v) else float(v) for v in series]
            )
        else:
            # Treat as string (includes object, category, StringDtype, etc.)
            prop_types.append("string")
            string_cols.append(
                [None if pd_isna(v) else str(v) for v in series]
            )

    return {
        "name": name,
        "wkb": wkb_list,
        "geom_types": geom_types,
        "prop_names": prop_names,
        "prop_types": prop_types,
        "string_columns": string_cols,
        "int_columns": int_cols,
        "float_columns": float_cols,
        "bool_columns": bool_cols,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
    }


def pd_isna(value) -> bool:
    """Check if a value is NA/NaN/None, handling various types."""
    if value is None:
        return True
    try:
        import pandas as pd
        return pd.isna(value)
    except (TypeError, ValueError):
        return False


def _format_size(size: int) -> str:
    """Format file size for display."""
    if size >= 1e6:
        return f"{size / 1e6:.1f} MB"
    elif size >= 1e3:
        return f"{size / 1e3:.1f} KB"
    else:
        return f"{size} bytes"


__all__ = ["freestile"]
