"""Type stubs for the freestiler Rust extension module."""

def _freestile(
    layers: list[dict],
    output_path: str,
    tile_format: str,
    min_zoom: int,
    max_zoom: int,
    base_zoom: int,
    do_simplify: bool,
    generate_ids: bool,
    quiet: bool,
    drop_rate: float,
    cluster_distance: float,
    cluster_maxzoom: int,
    do_coalesce: bool,
) -> str: ...

def _freestile_file(
    input_path: str,
    output_path: str,
    layer_name: str,
    tile_format: str,
    min_zoom: int,
    max_zoom: int,
    base_zoom: int,
    do_simplify: bool,
    quiet: bool,
    drop_rate: float,
    cluster_distance: float,
    cluster_maxzoom: int,
    do_coalesce: bool,
) -> str: ...

def _freestile_duckdb(
    input_path: str,
    output_path: str,
    layer_name: str,
    tile_format: str,
    min_zoom: int,
    max_zoom: int,
    base_zoom: int,
    do_simplify: bool,
    quiet: bool,
    drop_rate: float,
    cluster_distance: float,
    cluster_maxzoom: int,
    do_coalesce: bool,
) -> str: ...

def _freestile_duckdb_query(
    sql: str,
    db_path: str | None,
    output_path: str,
    layer_name: str,
    tile_format: str,
    min_zoom: int,
    max_zoom: int,
    base_zoom: int,
    do_simplify: bool,
    quiet: bool,
    drop_rate: float,
    cluster_distance: float,
    cluster_maxzoom: int,
    do_coalesce: bool,
    streaming_mode: str,
) -> str: ...
