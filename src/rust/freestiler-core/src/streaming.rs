#[cfg(feature = "duckdb")]
use duckdb::{params, Connection};
use pmtiles2::util::tile_id;
use pmtiles2::Entry;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::{ProgressReporter, TileConfig};
use crate::pmtiles_writer::{self, LayerMeta, TileFormat};
use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
use crate::{coalesce, mlt, mvt, tiler};

const STREAMING_AUTO_THRESHOLD: u64 = 1_000_000;

pub fn auto_threshold() -> u64 {
    STREAMING_AUTO_THRESHOLD
}

pub fn query_feature_count(db_path: Option<&str>, sql: &str) -> Result<u64, String> {
    let conn = open_connection(db_path)?;
    let count_sql = format!("SELECT COUNT(*) FROM ({}) AS __freestiler_count", sql);
    conn.query_row(&count_sql, params![], |row| row.get::<_, u64>(0))
        .map_err(|e| format!("Cannot count query rows: {}", e))
}

pub fn generate_pmtiles_from_duckdb_query(
    db_path: Option<&str>,
    sql: &str,
    output_path: &str,
    layer_name: &str,
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
) -> Result<u64, String> {
    if config.cluster_distance.map_or(false, |d| d > 0.0) {
        return Err("Streaming point mode does not support clustering yet.".to_string());
    }

    let conn = open_connection(db_path)?;
    let prepared = PreparedPointQuery::new(&conn, sql)?;
    let points_table = prepared.materialize_points_table(&conn)?;
    let stats = points_table.compute_stats(&conn)?;

    if !stats.only_points {
        return Err("Streaming point mode currently supports POINT geometries only.".to_string());
    }

    if stats.row_count == 0 {
        return Err("No valid features found".to_string());
    }

    if !config.coalesce && !config.simplification {
        // no-op, keep reporter output symmetric with the in-memory path
    }

    reporter.report(&format!("  Query returned {} features", stats.row_count));
    reporter.report("  Using streaming point pipeline");

    let layer_meta = LayerMeta {
        name: layer_name.to_string(),
        property_names: prepared.prop_names.clone(),
        min_zoom: config.min_zoom,
        max_zoom: config.max_zoom,
        geometry_type: Some("Point".to_string()),
    };

    let mut tile_spool = TileSpool::new()?;
    let bounds = (stats.min_lon, stats.min_lat, stats.max_lon, stats.max_lat);

    for zoom in config.min_zoom..=config.max_zoom {
        let retain_count = retain_count_for_zoom(
            stats.row_count,
            zoom,
            config.base_zoom.unwrap_or(config.max_zoom),
            config.drop_rate,
        );
        let zoom_sql =
            points_table.zoom_query_sql(zoom, &prepared.prop_names, stats.row_count, retain_count);
        let mut stmt = conn
            .prepare(&zoom_sql)
            .map_err(|e| format!("Cannot prepare zoom {} query: {}", zoom, e))?;
        let mut rows = stmt
            .query(params![])
            .map_err(|e| format!("Cannot execute zoom {} query: {}", zoom, e))?;

        let mut current_coord: Option<TileCoord> = None;
        let mut tile_features: Vec<Feature> = Vec::new();
        let mut encoded_tiles = 0u64;

        while let Some(row) = rows.next().map_err(|e| format!("Row error: {}", e))? {
            let x: u32 = row
                .get::<_, u32>(0)
                .map_err(|e| format!("Tile x read error: {}", e))?;
            let y: u32 = row
                .get::<_, u32>(1)
                .map_err(|e| format!("Tile y read error: {}", e))?;
            let lon: f64 = row
                .get::<_, f64>(2)
                .map_err(|e| format!("Longitude read error: {}", e))?;
            let lat: f64 = row
                .get::<_, f64>(3)
                .map_err(|e| format!("Latitude read error: {}", e))?;
            let row_id: i64 = row
                .get::<_, i64>(4)
                .map_err(|e| format!("Row id read error: {}", e))?;

            let coord = TileCoord { z: zoom, x, y };
            if current_coord != Some(coord) {
                if let Some(prev_coord) = current_coord {
                    write_tile(
                        &mut tile_spool,
                        prev_coord,
                        &layer_meta,
                        &prepared.prop_names,
                        &mut tile_features,
                        config,
                    )?;
                    encoded_tiles += 1;
                }
                current_coord = Some(coord);
            }

            let mut properties = Vec::with_capacity(prepared.prop_names.len());
            for (col_idx, kind) in prepared.prop_value_kinds.iter().enumerate() {
                properties.push(extract_value(row, 5 + col_idx, *kind));
            }

            tile_features.push(Feature {
                id: Some(row_id as u64),
                geometry: Geometry::Point(geo_types::Point::new(lon, lat)),
                properties,
            });
        }

        if let Some(coord) = current_coord {
            write_tile(
                &mut tile_spool,
                coord,
                &layer_meta,
                &prepared.prop_names,
                &mut tile_features,
                config,
            )?;
            encoded_tiles += 1;
        }

        reporter.report(&format!(
            "  Zoom {:>2}/{}: {:>6} encoded",
            zoom, config.max_zoom, encoded_tiles
        ));
    }

    let entries = std::mem::take(&mut tile_spool.entries);
    reporter.report(&format!(
        "  Writing PMTiles archive ({} tiles) ...",
        entries.len()
    ));
    pmtiles_writer::write_pmtiles_from_spool(
        output_path,
        &tile_spool.path,
        entries,
        config.tile_format,
        &[layer_meta],
        config.min_zoom,
        config.max_zoom,
        bounds,
    )?;

    Ok(stats.row_count)
}

struct PreparedPointQuery {
    sql: String,
    geom_expr: String,
    prop_names: Vec<String>,
    prop_value_kinds: Vec<DuckDbValueKind>,
}

impl PreparedPointQuery {
    fn new(conn: &Connection, sql: &str) -> Result<Self, String> {
        let discover_sql = format!("DESCRIBE ({})", sql);
        let mut discover_stmt = conn
            .prepare(&discover_sql)
            .map_err(|e| format!("Cannot describe query: {}", e))?;

        let mut all_columns: Vec<(String, String)> = Vec::new();
        let mut geom_col_name: Option<String> = None;

        {
            let rows = discover_stmt
                .query_map(params![], |row| {
                    let col_name: String = row.get(0)?;
                    let col_type: String = row.get(1)?;
                    Ok((col_name, col_type))
                })
                .map_err(|e| format!("Cannot describe query: {}", e))?;

            for row in rows {
                let (name, dtype) = row.map_err(|e| format!("Cannot read column info: {}", e))?;
                let dt = dtype.to_uppercase();
                if geom_col_name.is_none() && (dt == "GEOMETRY" || dt.starts_with("GEOMETRY")) {
                    geom_col_name = Some(name.clone());
                }
                all_columns.push((name, dtype));
            }
        }

        let geom_col_name = geom_col_name.ok_or_else(|| {
            "No geometry column found in query result. Ensure your query returns a GEOMETRY column."
                .to_string()
        })?;

        let geom_col_sql = quote_ident(&geom_col_name);
        let srid_sql = format!(
            "SELECT ST_SRID({}) AS __srid FROM ({}) AS __freestiler_src WHERE {} IS NOT NULL LIMIT 1",
            geom_col_sql, sql, geom_col_sql
        );
        let source_srid: Option<String> = conn
            .query_row(&srid_sql, params![], |row| row.get::<_, String>(0))
            .ok();

        let geom_expr = match source_srid.as_deref() {
            None | Some("EPSG:4326") | Some("") => geom_col_sql.clone(),
            Some(src_crs) => format!(
                "ST_Transform({}, {}, 'EPSG:4326')",
                geom_col_sql,
                quote_string(src_crs)
            ),
        };

        let mut prop_names = Vec::new();
        let mut prop_value_kinds = Vec::new();
        for (name, dtype) in all_columns {
            if name.eq_ignore_ascii_case(&geom_col_name) {
                continue;
            }
            prop_names.push(name);
            prop_value_kinds.push(duckdb_type_to_value_kind(&dtype));
        }

        Ok(Self {
            sql: sql.to_string(),
            geom_expr,
            prop_names,
            prop_value_kinds,
        })
    }

    fn materialize_points_table(&self, conn: &Connection) -> Result<PointsTable, String> {
        let table_name = format!("__freestiler_points_{}", unique_suffix());
        let prop_select = if self.prop_names.is_empty() {
            String::new()
        } else {
            format!(
                ", {}",
                self.prop_names
                    .iter()
                    .map(|name| quote_ident(name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let morton_expr = morton_sql_expr("__lon", "__lat");

        let create_sql = format!(
            "CREATE TEMP TABLE {} AS
             WITH __freestiler_src AS (
               SELECT {} AS __geom{} FROM ({}) AS __freestiler_query
             ),
             __freestiler_typed AS (
               SELECT
                 ROW_NUMBER() OVER () AS __src_rowid,
                 UPPER(CAST(ST_GeometryType(__geom) AS VARCHAR)) AS __geom_type,
                 CASE
                   WHEN UPPER(CAST(ST_GeometryType(__geom) AS VARCHAR)) = 'POINT'
                   THEN CAST(ST_X(__geom) AS DOUBLE)
                 END AS __lon,
                 CASE
                   WHEN UPPER(CAST(ST_GeometryType(__geom) AS VARCHAR)) = 'POINT'
                   THEN CAST(ST_Y(__geom) AS DOUBLE)
                 END AS __lat{}
               FROM __freestiler_src
               WHERE __geom IS NOT NULL
             ),
             __freestiler_mortonized AS (
               SELECT
                 __src_rowid,
                 __geom_type,
                 __lon,
                 __lat,
                 CASE
                   WHEN __geom_type = 'POINT' THEN {}
                 END AS __morton{}
               FROM __freestiler_typed
             )
             SELECT
               __src_rowid AS __rowid,
               __geom_type,
               __lon,
               __lat,
               __morton,
               CASE
                 WHEN __geom_type = 'POINT'
                 THEN ROW_NUMBER() OVER (PARTITION BY __geom_type ORDER BY __morton, __src_rowid)
               END AS __morton_rank{}
             FROM __freestiler_mortonized",
            quote_ident(&table_name),
            self.geom_expr,
            prop_select,
            self.sql,
            prop_select,
            morton_expr,
            prop_select,
            prop_select
        );

        conn.execute_batch(&create_sql)
            .map_err(|e| format!("Cannot materialize streaming points table: {}", e))?;

        Ok(PointsTable { name: table_name })
    }
}

struct PointsTable {
    name: String,
}

impl PointsTable {
    fn compute_stats(&self, conn: &Connection) -> Result<PointStats, String> {
        let table = quote_ident(&self.name);
        let stats_sql = format!(
            "SELECT
               SUM(CASE WHEN __geom_type = 'POINT' THEN 1 ELSE 0 END) AS point_count,
               SUM(CASE WHEN __geom_type <> 'POINT' THEN 1 ELSE 0 END) AS non_point_count,
               MIN(__lon) FILTER (WHERE __geom_type = 'POINT') AS min_lon,
               MIN(__lat) FILTER (WHERE __geom_type = 'POINT') AS min_lat,
               MAX(__lon) FILTER (WHERE __geom_type = 'POINT') AS max_lon,
               MAX(__lat) FILTER (WHERE __geom_type = 'POINT') AS max_lat
             FROM {}",
            table
        );

        let (row_count, non_point_count, min_lon, min_lat, max_lon, max_lat) = conn
            .query_row(&stats_sql, params![], |row| {
                Ok((
                    row.get::<_, Option<u64>>(0)?.unwrap_or(0),
                    row.get::<_, Option<u64>>(1)?.unwrap_or(0),
                    row.get::<_, Option<f64>>(2)?.unwrap_or(0.0),
                    row.get::<_, Option<f64>>(3)?.unwrap_or(0.0),
                    row.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
                    row.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
                ))
            })
            .map_err(|e| format!("Cannot compute streaming stats: {}", e))?;

        Ok(PointStats {
            row_count,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            only_points: non_point_count == 0,
        })
    }

    fn zoom_query_sql(
        &self,
        zoom: u8,
        prop_names: &[String],
        n_points: u64,
        retain_count: Option<u64>,
    ) -> String {
        let prop_select = if prop_names.is_empty() {
            String::new()
        } else {
            format!(
                ", {}",
                prop_names
                    .iter()
                    .map(|name| quote_ident(name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let table = quote_ident(&self.name);
        let n = 1u64 << zoom;
        let max_idx = n - 1;
        let clamped_lat = "LEAST(GREATEST(__lat, -85.05112878), 85.05112878)";
        let keep_predicate = retain_count.map_or_else(String::new, |retain| {
            format!(
                " AND (((CAST(__morton_rank AS UBIGINT) * {retain}) // {n_points}) > (((CAST(__morton_rank AS UBIGINT) - 1) * {retain}) // {n_points}))",
                retain = retain,
                n_points = n_points
            )
        });

        format!(
            "SELECT
               CAST(LEAST(GREATEST(FLOOR(((__lon + 180.0) / 360.0) * {n}), 0), {max_idx}) AS UINTEGER) AS __tile_x,
               CAST(LEAST(GREATEST(FLOOR(((1.0 - ASINH(TAN(RADIANS({clamped_lat}))) / PI()) / 2.0) * {n}), 0), {max_idx}) AS UINTEGER) AS __tile_y,
               __lon,
               __lat,
               __rowid{prop_select}
             FROM {table}
             WHERE __geom_type = 'POINT'{keep_predicate}
             ORDER BY __tile_x, __tile_y, __morton_rank",
            n = n,
            max_idx = max_idx,
            clamped_lat = clamped_lat,
            prop_select = prop_select,
            table = table,
            keep_predicate = keep_predicate,
        )
    }
}

struct PointStats {
    row_count: u64,
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
    only_points: bool,
}

struct TileSpool {
    path: PathBuf,
    file: File,
    offset: u64,
    entries: Vec<Entry>,
}

impl TileSpool {
    fn new() -> Result<Self, String> {
        let path = temp_file_path("tiles");
        let file = File::create(&path).map_err(|e| {
            format!(
                "Cannot create temporary tile spool {}: {}",
                path.display(),
                e
            )
        })?;
        Ok(Self {
            path,
            file,
            offset: 0,
            entries: Vec::new(),
        })
    }

    fn write_tile(&mut self, coord: TileCoord, bytes: &[u8]) -> Result<(), String> {
        let compressed = pmtiles_writer::gzip_compress(bytes)?;
        self.file
            .write_all(&compressed)
            .map_err(|e| format!("Cannot write tile spool: {}", e))?;

        self.entries.push(Entry {
            tile_id: tile_id(coord.z, coord.x as u64, coord.y as u64),
            offset: self.offset,
            length: compressed.len() as u32,
            run_length: 1,
        });
        self.offset += compressed.len() as u64;
        Ok(())
    }
}

impl Drop for TileSpool {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn write_tile(
    spool: &mut TileSpool,
    coord: TileCoord,
    layer_meta: &LayerMeta,
    prop_names: &[String],
    tile_features: &mut Vec<Feature>,
    config: &TileConfig,
) -> Result<(), String> {
    if tile_features.is_empty() {
        return Ok(());
    }

    if tile_features.len() > 1 {
        let bounds = tiler::tile_bounds(&coord);
        let west = bounds.min().x;
        let east = bounds.max().x;
        let south = bounds.min().y;
        let north = bounds.max().y;
        tile_features.sort_by(|a, b| {
            let key_a = tiler::tile_morton_key(&a.geometry, west, east, south, north);
            let key_b = tiler::tile_morton_key(&b.geometry, west, east, south, north);
            key_a.cmp(&key_b).then(a.id.cmp(&b.id))
        });
    }

    if config.coalesce {
        let coalesced = coalesce::coalesce_features(std::mem::take(tile_features), prop_names);
        *tile_features = coalesced;
    }

    if tile_features.is_empty() {
        return Ok(());
    }

    let layer_refs = [(
        layer_meta.name.as_str(),
        prop_names,
        tile_features.as_slice(),
    )];
    let tile_bytes = match config.tile_format {
        TileFormat::Mvt => mvt::encode_tile_multilayer(&coord, &layer_refs),
        TileFormat::Mlt => mlt::encode_tile_multilayer(&coord, &layer_refs),
    };

    tile_features.clear();

    if tile_bytes.is_empty() {
        return Ok(());
    }

    spool.write_tile(coord, &tile_bytes)
}

fn open_connection(db_path: Option<&str>) -> Result<Connection, String> {
    let conn = match db_path {
        Some(path) => Connection::open(path).map_err(|e| format!("Cannot open DB: {}", e))?,
        None => Connection::open_in_memory().map_err(|e| format!("Cannot open DB: {}", e))?,
    };

    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .map_err(|e| format!("Cannot load spatial extension: {}", e))?;
    Ok(conn)
}

fn retain_count_for_zoom(
    n_points: u64,
    zoom: u8,
    base_zoom: u8,
    drop_rate: Option<f64>,
) -> Option<u64> {
    let Some(rate) = drop_rate else {
        return None;
    };
    if rate <= 0.0 || zoom >= base_zoom || n_points == 0 {
        return None;
    }

    let zoom_diff = (base_zoom - zoom) as f64;
    let threshold = rate.powf(zoom_diff);
    let retain_count = ((n_points as f64) / threshold).ceil() as u64;
    Some(retain_count.clamp(1, n_points))
}

fn morton_sql_expr(lon_expr: &str, lat_expr: &str) -> String {
    let norm_x = format!(
        "CAST(LEAST(GREATEST(FLOOR((({lon} + 180.0) / 360.0) * 65536.0), 0), 65535) AS UBIGINT)",
        lon = lon_expr
    );
    let norm_y = format!(
        "CAST(LEAST(GREATEST(FLOOR((({lat} + 90.0) / 180.0) * 65536.0), 0), 65535) AS UBIGINT)",
        lat = lat_expr
    );
    let spread_x = spread_bits_sql(&norm_x);
    let spread_y = spread_bits_sql(&norm_y);
    format!(
        "({spread_x} | ({spread_y} << 1))",
        spread_x = spread_x,
        spread_y = spread_y
    )
}

fn spread_bits_sql(expr: &str) -> String {
    let step1 = format!(
        "((CAST({expr} AS UBIGINT) | (CAST({expr} AS UBIGINT) << 8)) & 16711935)",
        expr = expr
    );
    let step2 = format!("(({step1} | ({step1} << 4)) & 252645135)", step1 = step1);
    let step3 = format!("(({step2} | ({step2} << 2)) & 858993459)", step2 = step2);
    format!("(({step3} | ({step3} << 1)) & 1431655765)", step3 = step3)
}

#[derive(Clone, Copy)]
enum DuckDbValueKind {
    String,
    Int,
    Double,
    Bool,
}

fn extract_value(row: &duckdb::Row, col_idx: usize, kind: DuckDbValueKind) -> PropertyValue {
    match kind {
        DuckDbValueKind::String => row
            .get::<_, Option<String>>(col_idx)
            .ok()
            .flatten()
            .map(PropertyValue::String)
            .unwrap_or(PropertyValue::Null),
        DuckDbValueKind::Int => row
            .get::<_, Option<i64>>(col_idx)
            .ok()
            .flatten()
            .map(PropertyValue::Int)
            .unwrap_or(PropertyValue::Null),
        DuckDbValueKind::Double => row
            .get::<_, Option<f64>>(col_idx)
            .ok()
            .flatten()
            .map(|v| {
                if v.is_nan() {
                    PropertyValue::Null
                } else {
                    PropertyValue::Double(v)
                }
            })
            .unwrap_or(PropertyValue::Null),
        DuckDbValueKind::Bool => row
            .get::<_, Option<bool>>(col_idx)
            .ok()
            .flatten()
            .map(PropertyValue::Bool)
            .unwrap_or(PropertyValue::Null),
    }
}

fn duckdb_type_to_value_kind(dtype: &str) -> DuckDbValueKind {
    let dt = dtype.trim().to_uppercase();
    if matches!(dt.as_str(), "BOOLEAN" | "BOOL" | "LOGICAL") {
        DuckDbValueKind::Bool
    } else if matches!(
        dt.as_str(),
        "TINYINT"
            | "SMALLINT"
            | "INTEGER"
            | "INT"
            | "BIGINT"
            | "UTINYINT"
            | "USMALLINT"
            | "UINTEGER"
    ) {
        DuckDbValueKind::Int
    } else if matches!(dt.as_str(), "REAL" | "FLOAT" | "DOUBLE") || dt.starts_with("DECIMAL") {
        DuckDbValueKind::Double
    } else {
        DuckDbValueKind::String
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn temp_file_path(stem: &str) -> PathBuf {
    std::env::temp_dir().join(format!("freestiler_{}_{}.tmp", stem, unique_suffix()))
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}_{}", std::process::id(), nanos)
}
