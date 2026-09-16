//! Partitioned streaming point pipeline for DuckDB queries.
//!
//! The source query is materialized once into hive-partitioned Parquet keyed
//! by web-mercator tile cell (no global sort), oversized cells are refined by
//! measured occupancy, and each partition unit is tiled independently with
//! small in-memory sorts. Tiles above a unit's own zoom are routed through
//! disk-backed per-tile buckets. All bulk temporary data lives in a private
//! per-run directory that is removed when the run ends.
#[cfg(feature = "duckdb")]
use duckdb::{params, Connection};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::engine::{ProgressReporter, TileConfig};
use crate::pmtiles_writer::{self, unique_suffix, LayerMeta, TileFormat, TileSpool};
use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
use crate::{coalesce, mlt, mvt, tiler};

const STREAMING_AUTO_THRESHOLD: u64 = 1_000_000;

// M0-measured defaults (2026-09-15 spike; see plan file). Env-overridable.
const PARTITION_INITIAL_ZOOM: u8 = 5;
const PARTITION_STEP: u8 = 2;
const PARTITION_MAX_ZOOM: u8 = 13;
const DEFAULT_UNIT_BUDGET_ROWS: u64 = 8_000_000;
/// Decoded bytes ≈ 10-12x zstd parquet bytes (measured); guard vs ultra-wide rows.
const DECODED_PER_PARQUET_BYTE: u64 = 12;
const DEFAULT_UNIT_DECODED_BUDGET_MB: u64 = 6_144;
const DEFAULT_TILE_BUDGET_MB: u64 = 2_048;
const DEFAULT_BUCKET_BUFFER_MB: u64 = 256;

pub fn auto_threshold() -> u64 {
    STREAMING_AUTO_THRESHOLD
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

pub fn query_feature_count(db_path: Option<&str>, sql: &str) -> Result<u64, String> {
    let conn = open_connection(db_path)?;
    let (setup_stmts, query) = crate::file_input::split_sql_statements(sql);
    crate::file_input::run_setup_statements(&conn, &setup_stmts)?;
    let count_sql = format!("SELECT COUNT(*) FROM ({}) AS __freestiler_count", query);
    conn.query_row(&count_sql, params![], |row| row.get::<_, u64>(0))
        .map_err(|e| format!("Cannot count query rows: {}", e))
}

/// Private per-run directory holding partitions, buckets, spool, and DuckDB
/// spill. Removed recursively on drop; only this owned directory is touched.
struct RunDir {
    path: PathBuf,
}

impl RunDir {
    fn new() -> Result<Self, String> {
        let base = std::env::var("FREESTILER_TEMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let path = base.join(format!("freestiler_run_{}", unique_suffix()));
        fs::create_dir_all(&path)
            .map_err(|e| format!("Cannot create run directory {}: {}", path.display(), e))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o700));
        }
        Ok(Self { path })
    }
}

impl Drop for RunDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
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

    let run_dir = RunDir::new()?;

    let conn = open_connection(db_path)?;
    let duck_tmp = run_dir.path.join("duckdb_tmp");
    conn.execute_batch(&format!(
        "SET temp_directory = {}",
        quote_string(&duck_tmp.to_string_lossy())
    ))
    .map_err(|e| format!("Cannot set DuckDB temp directory: {}", e))?;
    if let Ok(mem) = std::env::var("FREESTILER_DUCKDB_MEMORY") {
        conn.execute_batch(&format!("SET memory_limit = {}", quote_string(&mem)))
            .map_err(|e| format!("Cannot set DuckDB memory limit: {}", e))?;
    }

    let (setup_stmts, query) = crate::file_input::split_sql_statements(sql);
    crate::file_input::run_setup_statements(&conn, &setup_stmts)?;
    let prepared = PreparedPointQuery::new(&conn, &query)?;

    // --- Phase A: one materialization pass into hive-partitioned parquet ---
    let parts_dir = run_dir.path.join("parts");
    reporter.report("  Partitioning query results ...");
    materialize_partitions(&conn, &prepared, &parts_dir)?;

    let stats = compute_stats(&conn, &parts_dir)?;
    if !stats.only_points {
        return Err("Streaming point mode currently supports POINT geometries only.".to_string());
    }
    if stats.row_count == 0 {
        return Err("No valid features found".to_string());
    }

    reporter.report(&format!("  Query returned {} features", stats.row_count));
    reporter.report("  Using streaming point pipeline");

    // --- Occupancy-based refinement of oversized cells ---
    let unit_budget_rows = env_u64("FREESTILER_UNIT_BUDGET_ROWS", DEFAULT_UNIT_BUDGET_ROWS);
    let unit_decoded_budget =
        env_u64("FREESTILER_UNIT_BUDGET_MB", DEFAULT_UNIT_DECODED_BUDGET_MB) * 1024 * 1024;
    let units = refine_partitions(&conn, &parts_dir, unit_budget_rows, unit_decoded_budget)?;
    reporter.report(&format!("  {} partition units", units.len()));

    let layer_meta = LayerMeta {
        name: layer_name.to_string(),
        property_names: prepared.prop_names.clone(),
        min_zoom: config.min_zoom,
        max_zoom: config.max_zoom,
        geometry_type: Some("Point".to_string()),
    };

    // --- Phase B/C: per-unit tiling into the shared spool + tile buckets ---
    let tile_budget = env_u64("FREESTILER_TILE_BUDGET_MB", DEFAULT_TILE_BUDGET_MB) * 1024 * 1024;
    let bucket_buffer =
        env_u64("FREESTILER_BUCKET_BUFFER_MB", DEFAULT_BUCKET_BUFFER_MB) * 1024 * 1024;

    let spool = Arc::new(Mutex::new(TileSpool::new_in(&run_dir.path)?));
    let buckets = Arc::new(Mutex::new(BucketStore::new(
        run_dir.path.join("buckets"),
        bucket_buffer,
    )?));
    let encoded_per_zoom = Arc::new(Mutex::new(HashMap::<u8, u64>::new()));

    let workers = env_u64("FREESTILER_STREAM_WORKERS", 1).max(1) as usize;
    let ctx = UnitContext {
        duck_tmp: duck_tmp.clone(),
        prepared: &prepared,
        layer_meta: &layer_meta,
        config,
        tile_budget,
        unit_decoded_budget,
    };

    if workers <= 1 {
        for (i, unit) in units.iter().enumerate() {
            process_unit(&conn, &ctx, unit, &spool, &buckets, &encoded_per_zoom)?;
            if (i + 1) % 50 == 0 {
                reporter.report(&format!("  Partitions: {}/{}", i + 1, units.len()));
            }
        }
    } else {
        process_units_parallel(
            &ctx,
            &units,
            workers,
            &spool,
            &buckets,
            &encoded_per_zoom,
            reporter,
        )?;
    }

    // --- Encode cross-unit bucket tiles ---
    reporter.report("  Assembling cross-partition tiles ...");
    {
        let mut store = buckets.lock().unwrap();
        let mut counts = encoded_per_zoom.lock().unwrap();
        store.finalize(
            &spool,
            &mut counts,
            &layer_meta,
            &prepared.prop_names,
            config,
            tile_budget,
        )?;
    }

    {
        let counts = encoded_per_zoom.lock().unwrap();
        let mut zooms: Vec<_> = counts.iter().collect();
        zooms.sort();
        for (z, n) in zooms {
            reporter.report(&format!(
                "  Zoom {:>2}/{}: {:>6} encoded",
                z, config.max_zoom, n
            ));
        }
    }

    let mut sp = Arc::try_unwrap(spool)
        .map_err(|_| "Internal error: spool still shared".to_string())?
        .into_inner()
        .unwrap();
    reporter.report(&format!(
        "  Writing PMTiles archive ({} tiles) ...",
        sp.len()
    ));
    let bounds = (stats.min_lon, stats.min_lat, stats.max_lon, stats.max_lat);
    pmtiles_writer::write_pmtiles_from_spool(
        output_path,
        &mut sp,
        config.tile_format,
        &[layer_meta],
        config.min_zoom,
        config.max_zoom,
        bounds,
    )?;

    Ok(stats.row_count)
}

// ---------------------------------------------------------------------------
// Materialization + refinement
// ---------------------------------------------------------------------------

fn partition_expr(zp: u8, lon: &str, lat: &str) -> String {
    let n = 1u64 << zp;
    let max_idx = n - 1;
    let clamped = format!("LEAST(GREATEST({lat}, -85.05112878), 85.05112878)");
    let tx = format!(
        "CAST(LEAST(GREATEST(FLOOR((({lon} + 180.0) / 360.0) * {n}), 0), {max_idx}) AS BIGINT)"
    );
    let ty = format!(
        "CAST(LEAST(GREATEST(FLOOR(((1.0 - ASINH(TAN(RADIANS({clamped}))) / PI()) / 2.0) * {n}), 0), {max_idx}) AS BIGINT)"
    );
    format!("({tx} * {n} + {ty})")
}

fn materialize_partitions(
    conn: &Connection,
    prepared: &PreparedPointQuery,
    parts_dir: &Path,
) -> Result<(), String> {
    let prop_select = prepared.prop_select();
    let morton = morton_sql_expr("__lon", "__lat");
    let part = partition_expr(PARTITION_INITIAL_ZOOM, "__lon", "__lat");
    let copy_sql = format!(
        "COPY (
           WITH __src AS (
             SELECT {} AS __geom{} FROM ({}) AS __freestiler_query
             WHERE {} IS NOT NULL
           ),
           __typed AS (
             SELECT
               ROW_NUMBER() OVER () AS __src_rowid,
               UPPER(CAST(ST_GeometryType(__geom) AS VARCHAR)) AS __geom_type,
               CASE WHEN UPPER(CAST(ST_GeometryType(__geom) AS VARCHAR)) = 'POINT'
                    THEN CAST(ST_X(__geom) AS DOUBLE) END AS __lon,
               CASE WHEN UPPER(CAST(ST_GeometryType(__geom) AS VARCHAR)) = 'POINT'
                    THEN CAST(ST_Y(__geom) AS DOUBLE) END AS __lat{}
             FROM __src
           )
           SELECT *,
             CASE WHEN __geom_type = 'POINT' THEN {} END AS __morton,
             CASE WHEN __geom_type = 'POINT' THEN {} ELSE -1 END AS __part
           FROM __typed
         ) TO {} (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (__part))",
        prepared.geom_expr,
        prop_select,
        prepared.sql,
        prepared.geom_expr,
        prop_select,
        morton,
        part,
        quote_string(&parts_dir.to_string_lossy()),
    );
    conn.execute_batch(&copy_sql)
        .map_err(|e| format!("Cannot materialize partitioned points: {}", e))
}

struct PointStats {
    row_count: u64,
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
    only_points: bool,
}

fn compute_stats(conn: &Connection, parts_dir: &Path) -> Result<PointStats, String> {
    let glob = parts_dir.join("**").join("*.parquet");
    let stats_sql = format!(
        "SELECT
           SUM(CASE WHEN __geom_type = 'POINT' THEN 1 ELSE 0 END),
           SUM(CASE WHEN __geom_type <> 'POINT' THEN 1 ELSE 0 END),
           MIN(__lon), MIN(__lat), MAX(__lon), MAX(__lat)
         FROM read_parquet({}, hive_partitioning = false)",
        quote_string(&glob.to_string_lossy())
    );
    let (points, non_points, min_lon, min_lat, max_lon, max_lat) = conn
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
        row_count: points,
        min_lon,
        min_lat,
        max_lon,
        max_lat,
        only_points: non_points == 0,
    })
}

#[derive(Clone, Debug)]
struct PartitionUnit {
    dir: PathBuf,
    z: u8,
    x: u32,
    y: u32,
    rows: u64,
}

fn leaf_partition_dirs(root: &Path, z: u8, out: &mut Vec<(PathBuf, u8, i64)>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(value) = name.strip_prefix("__part=") else {
            continue;
        };
        let Ok(part_val) = value.parse::<i64>() else {
            continue;
        };
        let has_parquet = fs::read_dir(&path).map_or(false, |it| {
            it.flatten()
                .any(|f| f.path().extension().map_or(false, |e| e == "parquet"))
        });
        if has_parquet {
            out.push((path.clone(), z, part_val));
        }
        leaf_partition_dirs(&path, z + PARTITION_STEP, out);
    }
}

fn unit_glob(dir: &Path) -> String {
    dir.join("*.parquet").to_string_lossy().into_owned()
}

fn count_rows(conn: &Connection, dir: &Path) -> Result<u64, String> {
    conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM read_parquet({}, hive_partitioning = false)",
            quote_string(&unit_glob(dir))
        ),
        params![],
        |row| row.get::<_, u64>(0),
    )
    .map_err(|e| format!("Cannot count partition rows in {}: {}", dir.display(), e))
}

fn dir_parquet_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|it| {
            it.flatten()
                .filter(|f| f.path().extension().map_or(false, |e| e == "parquet"))
                .filter_map(|f| f.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

fn refine_partitions(
    conn: &Connection,
    parts_dir: &Path,
    budget_rows: u64,
    decoded_budget_bytes: u64,
) -> Result<Vec<PartitionUnit>, String> {
    loop {
        let mut leaves = Vec::new();
        leaf_partition_dirs(parts_dir, PARTITION_INITIAL_ZOOM, &mut leaves);

        let mut oversized = Vec::new();
        let mut units = Vec::new();
        for (dir, z, part_val) in leaves {
            if part_val < 0 {
                continue; // non-point rows; already rejected by stats
            }
            let rows = count_rows(conn, &dir)?;
            let decoded_est = dir_parquet_bytes(&dir) * DECODED_PER_PARQUET_BYTE;
            if rows > budget_rows || decoded_est > decoded_budget_bytes {
                oversized.push((dir, z, rows));
            } else {
                let n = 1u64 << z;
                units.push(PartitionUnit {
                    dir,
                    z,
                    x: (part_val as u64 / n) as u32,
                    y: (part_val as u64 % n) as u32,
                    rows,
                });
            }
        }

        if oversized.is_empty() {
            units.sort_by_key(|u| (u.z, u.x, u.y));
            return Ok(units);
        }

        for (dir, z, rows) in oversized {
            let z_new = z + PARTITION_STEP;
            if z_new > PARTITION_MAX_ZOOM {
                return Err(format!(
                    "Partition cell at zoom {} ({}) still holds {} points at the refinement \
                     depth limit — likely a large number of coincident or near-coincident \
                     points. Reduce density with drop_rate, raise min_zoom, or thin the \
                     source query.",
                    z,
                    dir.display(),
                    rows
                ));
            }
            let staging = dir.with_extension("refine");
            // PARTITION_BY strips __part from the written files, so the
            // refined key is a fresh column, not a REPLACE.
            let copy_sql = format!(
                "COPY (SELECT *, {} AS __part
                       FROM read_parquet({}, hive_partitioning = false))
                 TO {} (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (__part))",
                partition_expr(z_new, "__lon", "__lat"),
                quote_string(&unit_glob(&dir)),
                quote_string(&staging.to_string_lossy()),
            );
            conn.execute_batch(&copy_sql)
                .map_err(|e| format!("Cannot refine partition {}: {}", dir.display(), e))?;
            fs::remove_dir_all(&dir)
                .map_err(|e| format!("Cannot replace partition {}: {}", dir.display(), e))?;
            fs::rename(&staging, &dir).map_err(|e| {
                format!("Cannot install refined partition {}: {}", dir.display(), e)
            })?;
        }
    }
}

// ---------------------------------------------------------------------------
// Per-unit tiling
// ---------------------------------------------------------------------------

struct UnitContext<'a> {
    duck_tmp: PathBuf,
    prepared: &'a PreparedPointQuery,
    layer_meta: &'a LayerMeta,
    config: &'a TileConfig,
    tile_budget: u64,
    unit_decoded_budget: u64,
}

/// One morton-ordered fetch per unit; row position IS the thinning rank, so
/// no window function and no per-zoom re-query or re-sort.
fn unit_fetch_query(prepared: &PreparedPointQuery, unit: &PartitionUnit) -> String {
    format!(
        "SELECT __lon, __lat, __src_rowid{} FROM read_parquet({}, hive_partitioning = false) \
         ORDER BY __morton, __src_rowid",
        prepared.prop_select(),
        quote_string(&unit_glob(&unit.dir)),
    )
}

/// Mirror of the SQL tile-coordinate math used at materialization time.
fn tile_coord_for(lon: f64, lat: f64, zoom: u8) -> (u32, u32) {
    let n = (1u64 << zoom) as f64;
    let max_idx = ((1u64 << zoom) - 1) as f64;
    let x = ((lon + 180.0) / 360.0 * n).floor().clamp(0.0, max_idx) as u32;
    let clamped = lat.clamp(-85.05112878, 85.05112878);
    let y = ((1.0 - clamped.to_radians().tan().asinh() / std::f64::consts::PI) / 2.0 * n)
        .floor()
        .clamp(0.0, max_idx) as u32;
    (x, y)
}

/// The Bresenham keep test used by the SQL thinning predicate, over the
/// 1-based morton rank. rank, retain <= unit rows (8M default): no overflow.
fn rank_kept(rank: u64, retain: u64, n: u64) -> bool {
    (rank * retain) / n > ((rank - 1) * retain) / n
}

struct UnitRow {
    lon: f64,
    lat: f64,
    id: u64,
    props: Vec<PropertyValue>,
}

fn process_unit(
    conn: &Connection,
    ctx: &UnitContext,
    unit: &PartitionUnit,
    spool: &Arc<Mutex<TileSpool>>,
    buckets: &Arc<Mutex<BucketStore>>,
    encoded_per_zoom: &Arc<Mutex<HashMap<u8, u64>>>,
) -> Result<(), String> {
    let config = ctx.config;
    let base_zoom = config.base_zoom.unwrap_or(config.max_zoom);

    // One morton-ordered fetch; every zoom is then processed in Rust.
    let sql = unit_fetch_query(ctx.prepared, unit);
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("Cannot prepare unit query: {}", e))?;
    let mut result = stmt
        .query(params![])
        .map_err(|e| format!("Cannot execute unit query: {}", e))?;

    let mut unit_rows: Vec<UnitRow> = Vec::new();
    let mut decoded_bytes: u64 = 0;
    while let Some(row) = result.next().map_err(|e| format!("Row error: {}", e))? {
        let lon: f64 = row
            .get(0)
            .map_err(|e| format!("Longitude read error: {}", e))?;
        let lat: f64 = row
            .get(1)
            .map_err(|e| format!("Latitude read error: {}", e))?;
        let id: i64 = row
            .get(2)
            .map_err(|e| format!("Row id read error: {}", e))?;
        let mut props = Vec::with_capacity(ctx.prepared.prop_names.len());
        for (col_idx, kind) in ctx.prepared.prop_value_kinds.iter().enumerate() {
            let value = extract_value(row, 3 + col_idx, *kind);
            decoded_bytes += 32
                + match &value {
                    PropertyValue::String(s) => s.len() as u64,
                    _ => 0,
                };
            props.push(value);
        }
        decoded_bytes += 32;
        if decoded_bytes > ctx.unit_decoded_budget {
            return Err(format!(
                "Partition unit z{}/{}/{} exceeds the decoded memory budget ({} MB). \
                 Raise FREESTILER_UNIT_BUDGET_MB or reduce property width in the query.",
                unit.z,
                unit.x,
                unit.y,
                ctx.unit_decoded_budget / 1_048_576
            ));
        }
        unit_rows.push(UnitRow {
            lon,
            lat,
            id: id as u64,
            props,
        });
    }
    drop(result);
    drop(stmt);

    let n = unit_rows.len() as u64;
    if n == 0 {
        return Ok(());
    }

    for zoom in config.min_zoom..=config.max_zoom {
        let retain = retain_count_for_zoom(n, zoom, base_zoom, config.drop_rate);
        let direct = zoom >= unit.z;

        if direct {
            // Tiles this unit emits must nest inside its cell; clamp guards
            // against last-ulp float disagreement with the SQL partition
            // math so the exactly-once tile invariant is structural.
            let shift = zoom - unit.z;
            let (x_min, y_min) = ((unit.x as u64) << shift, (unit.y as u64) << shift);
            let (x_max, y_max) = (
                (((unit.x as u64) + 1) << shift) - 1,
                (((unit.y as u64) + 1) << shift) - 1,
            );
            // (tile key, row index): index ascending == morton-rank order.
            let mut keyed: Vec<(u64, u32)> = Vec::new();
            for (i, r) in unit_rows.iter().enumerate() {
                let rank = i as u64 + 1;
                if let Some(retain) = retain {
                    if !rank_kept(rank, retain, n) {
                        continue;
                    }
                }
                let (x, y) = tile_coord_for(r.lon, r.lat, zoom);
                let x = (x as u64).clamp(x_min, x_max);
                let y = (y as u64).clamp(y_min, y_max);
                keyed.push(((x << 32) | y, i as u32));
            }
            keyed.sort_unstable();

            let mut current: Option<TileCoord> = None;
            let mut feats: Vec<Feature> = Vec::new();
            let mut feat_bytes: u64 = 0;
            let mut encoded = 0u64;
            for (key, idx) in keyed {
                let coord = TileCoord {
                    z: zoom,
                    x: (key >> 32) as u32,
                    y: (key & 0xFFFF_FFFF) as u32,
                };
                if current != Some(coord) {
                    if let Some(prev) = current {
                        // Encode and compress OUTSIDE the spool lock; only
                        // the short append is serialized across workers.
                        if let Some(compressed) = encode_tile_compressed(
                            prev,
                            ctx.layer_meta,
                            &ctx.prepared.prop_names,
                            &mut feats,
                            config,
                        )? {
                            spool
                                .lock()
                                .unwrap()
                                .write_compressed_tile(prev, &compressed)?;
                            encoded += 1;
                        }
                        feats.clear();
                    }
                    current = Some(coord);
                    feat_bytes = 0;
                }
                let r = &unit_rows[idx as usize];
                feat_bytes += 128
                    + r.props
                        .iter()
                        .map(|p| match p {
                            PropertyValue::String(s) => 32 + s.len() as u64,
                            _ => 32,
                        })
                        .sum::<u64>();
                if feat_bytes > ctx.tile_budget {
                    return Err(tile_budget_error(coord, feat_bytes, ctx.tile_budget));
                }
                feats.push(Feature {
                    id: Some(r.id),
                    geometry: Geometry::Point(geo_types::Point::new(r.lon, r.lat)),
                    properties: r.props.clone(),
                });
            }
            if let Some(prev) = current {
                if let Some(compressed) = encode_tile_compressed(
                    prev,
                    ctx.layer_meta,
                    &ctx.prepared.prop_names,
                    &mut feats,
                    config,
                )? {
                    spool
                        .lock()
                        .unwrap()
                        .write_compressed_tile(prev, &compressed)?;
                    encoded += 1;
                }
            }
            if encoded > 0 {
                *encoded_per_zoom.lock().unwrap().entry(zoom).or_insert(0) += encoded;
            }
        } else {
            // Bucket zoom: tiles wider than this unit's cell. The tile is
            // the unit cell's ancestor at this zoom — derived by bit shift,
            // not float math, so it can never land in a neighboring cell.
            // Records serialize straight from UnitRow (no Feature clone)
            // into a local buffer; the store lock is taken once per zoom.
            let shift = unit.z - zoom;
            let coord = TileCoord {
                z: zoom,
                x: unit.x >> shift,
                y: unit.y >> shift,
            };
            let mut buf = Vec::new();
            let mut decoded_sum: u64 = 0;
            for (i, r) in unit_rows.iter().enumerate() {
                let rank = i as u64 + 1;
                if let Some(retain) = retain {
                    if !rank_kept(rank, retain, n) {
                        continue;
                    }
                }
                encode_bucket_record_parts(&mut buf, r.id, r.lon, r.lat, &r.props);
                decoded_sum += 128
                    + r.props
                        .iter()
                        .map(|p| match p {
                            PropertyValue::String(s) => 32 + s.len() as u64,
                            _ => 32,
                        })
                        .sum::<u64>();
            }
            if !buf.is_empty() {
                buckets
                    .lock()
                    .unwrap()
                    .append_batch(coord, buf, decoded_sum)?;
            }
        }
    }

    Ok(())
}

fn process_units_parallel(
    ctx: &UnitContext,
    units: &[PartitionUnit],
    workers: usize,
    spool: &Arc<Mutex<TileSpool>>,
    buckets: &Arc<Mutex<BucketStore>>,
    encoded_per_zoom: &Arc<Mutex<HashMap<u8, u64>>>,
    reporter: &dyn ProgressReporter,
) -> Result<(), String> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let total = units.len();
    let done = AtomicUsize::new(0);
    let done = &done;
    let queue = Arc::new(Mutex::new(units.to_vec()));
    let error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let queue = Arc::clone(&queue);
            let error = Arc::clone(&error);
            let spool = Arc::clone(spool);
            let buckets = Arc::clone(buckets);
            let encoded = Arc::clone(encoded_per_zoom);
            scope.spawn(move || {
                // Unit queries are plain parquet + arithmetic: no spatial
                // extension needed, so workers use bare connections.
                let conn = match Connection::open_in_memory() {
                    Ok(c) => c,
                    Err(e) => {
                        *error.lock().unwrap() = Some(format!("Worker connection: {}", e));
                        return;
                    }
                };
                let _ = conn.execute_batch(&format!(
                    "SET temp_directory = {}",
                    quote_string(&ctx.duck_tmp.to_string_lossy())
                ));
                if let Ok(mem) = std::env::var("FREESTILER_DUCKDB_MEMORY") {
                    let _ =
                        conn.execute_batch(&format!("SET memory_limit = {}", quote_string(&mem)));
                }
                loop {
                    if error.lock().unwrap().is_some() {
                        return;
                    }
                    let unit = match queue.lock().unwrap().pop() {
                        Some(u) => u,
                        None => return,
                    };
                    if let Err(e) = process_unit(&conn, ctx, &unit, &spool, &buckets, &encoded) {
                        *error.lock().unwrap() = Some(e);
                        return;
                    }
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % 50 == 0 {
                        reporter.report(&format!("  Partitions: {}/{}", n, total));
                    }
                }
            });
        }
    });
    match Arc::try_unwrap(error).unwrap().into_inner().unwrap() {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn tile_budget_error(coord: TileCoord, bytes: u64, budget: u64) -> String {
    format!(
        "Tile z{}/{}/{} needs about {} MB of decoded features (limit {} MB). \
         Reduce point density with drop_rate, raise min_zoom, or raise \
         FREESTILER_TILE_BUDGET_MB if you have the memory.",
        coord.z,
        coord.x,
        coord.y,
        bytes / 1_048_576,
        budget / 1_048_576
    )
}

// ---------------------------------------------------------------------------
// Cross-unit tile buckets (disk-backed)
// ---------------------------------------------------------------------------

struct BucketStore {
    dir: PathBuf,
    buffers: HashMap<TileCoord, Vec<u8>>,
    buffered_bytes: usize,
    buffer_budget: usize,
    file_bytes: HashMap<TileCoord, u64>,
    /// Decoded (Feature) byte estimate per tile, accumulated at append time.
    decoded_est: HashMap<TileCoord, u64>,
}

impl BucketStore {
    fn new(dir: PathBuf, buffer_budget: u64) -> Result<Self, String> {
        fs::create_dir_all(&dir)
            .map_err(|e| format!("Cannot create bucket directory {}: {}", dir.display(), e))?;
        Ok(Self {
            dir,
            buffers: HashMap::new(),
            buffered_bytes: 0,
            buffer_budget: buffer_budget as usize,
            file_bytes: HashMap::new(),
            decoded_est: HashMap::new(),
        })
    }

    fn bucket_path(&self, coord: TileCoord) -> PathBuf {
        self.dir
            .join(format!("z{}_x{}_y{}.bin", coord.z, coord.x, coord.y))
    }

    fn append(&mut self, coord: TileCoord, feature: &Feature) -> Result<(), String> {
        let decoded: u64 = 128
            + feature
                .properties
                .iter()
                .map(|p| match p {
                    PropertyValue::String(s) => 32 + s.len() as u64,
                    _ => 32,
                })
                .sum::<u64>();
        let mut buf = Vec::new();
        encode_bucket_record(&mut buf, feature);
        self.append_batch(coord, buf, decoded)
    }

    /// Append pre-serialized records for one tile in a single lock hold.
    /// `decoded` is the DECODED feature cost of the batch (same accounting
    /// as the direct path), so finalize can enforce the tile budget on what
    /// the encoder will actually hold, not on the smaller serialized bytes.
    fn append_batch(
        &mut self,
        coord: TileCoord,
        data: Vec<u8>,
        decoded: u64,
    ) -> Result<(), String> {
        *self.decoded_est.entry(coord).or_insert(0) += decoded;
        let buf = self.buffers.entry(coord).or_default();
        if buf.is_empty() {
            let len = data.len();
            *buf = data;
            self.buffered_bytes += len;
        } else {
            buf.extend_from_slice(&data);
            self.buffered_bytes += data.len();
        }
        if self.buffered_bytes > self.buffer_budget {
            self.flush_largest()?;
        }
        Ok(())
    }

    fn flush_largest(&mut self) -> Result<(), String> {
        while self.buffered_bytes > self.buffer_budget / 2 {
            let Some((&coord, _)) = self.buffers.iter().max_by_key(|(_, b)| b.len()) else {
                return Ok(());
            };
            let buf = self.buffers.remove(&coord).unwrap();
            self.buffered_bytes -= buf.len();
            let path = self.bucket_path(coord);
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|e| format!("Cannot open bucket {}: {}", path.display(), e))?;
            file.write_all(&buf)
                .map_err(|e| format!("Cannot write bucket {}: {}", path.display(), e))?;
            *self.file_bytes.entry(coord).or_insert(0) += buf.len() as u64;
        }
        Ok(())
    }

    fn finalize(
        &mut self,
        spool: &Mutex<TileSpool>,
        encoded_per_zoom: &mut HashMap<u8, u64>,
        layer_meta: &LayerMeta,
        prop_names: &[String],
        config: &TileConfig,
        tile_budget: u64,
    ) -> Result<(), String> {
        use rayon::prelude::*;

        let mut coords: Vec<TileCoord> = self
            .buffers
            .keys()
            .chain(self.file_bytes.keys())
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        coords.sort_by_key(|c| (c.z, c.x, c.y));

        // Budget-check everything on decoded cost before any read-back.
        for coord in &coords {
            let decoded = self.decoded_est.get(coord).copied().unwrap_or(0);
            if decoded > tile_budget {
                return Err(tile_budget_error(*coord, decoded, tile_budget));
            }
        }

        // Decode + encode tiles in parallel, a bounded chunk at a time so
        // in-flight decoded tiles stay limited; spool appends are serial.
        let chunk_size = rayon::current_num_threads().max(4);
        for chunk in coords.chunks(chunk_size) {
            let inputs: Vec<(TileCoord, Vec<u8>)> = chunk
                .iter()
                .map(|&coord| {
                    let mem = self.buffers.remove(&coord).unwrap_or_default();
                    let file_len = self.file_bytes.remove(&coord).unwrap_or(0);
                    let mut data = Vec::with_capacity(mem.len() + file_len as usize);
                    if file_len > 0 {
                        let path = self.bucket_path(coord);
                        fs::File::open(&path)
                            .and_then(|mut f| f.read_to_end(&mut data))
                            .map_err(|e| format!("Cannot read bucket {}: {}", path.display(), e))?;
                        let _ = fs::remove_file(&path);
                    }
                    data.extend_from_slice(&mem);
                    Ok((coord, data))
                })
                .collect::<Result<_, String>>()?;

            let encoded: Vec<(TileCoord, Option<Vec<u8>>)> = inputs
                .into_par_iter()
                .map(|(coord, data)| {
                    let mut feats = decode_bucket_records(&data, coord)?;
                    drop(data);
                    if feats.is_empty() {
                        return Ok((coord, None));
                    }
                    let compressed =
                        encode_tile_compressed(coord, layer_meta, prop_names, &mut feats, config)?;
                    Ok((coord, compressed))
                })
                .collect::<Result<_, String>>()?;

            for (coord, compressed) in encoded {
                if let Some(compressed) = compressed {
                    spool
                        .lock()
                        .unwrap()
                        .write_compressed_tile(coord, &compressed)?;
                    *encoded_per_zoom.entry(coord.z).or_insert(0) += 1;
                }
            }
        }
        Ok(())
    }
}

fn encode_bucket_record(buf: &mut Vec<u8>, feature: &Feature) {
    let (lon, lat) = match &feature.geometry {
        Geometry::Point(p) => (p.x(), p.y()),
        _ => (0.0, 0.0),
    };
    encode_bucket_record_parts(buf, feature.id.unwrap_or(0), lon, lat, &feature.properties);
}

/// Serialize one bucket record straight from its parts — no Feature clone.
fn encode_bucket_record_parts(
    buf: &mut Vec<u8>,
    id: u64,
    lon: f64,
    lat: f64,
    properties: &[PropertyValue],
) {
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(&lon.to_le_bytes());
    buf.extend_from_slice(&lat.to_le_bytes());
    buf.extend_from_slice(&(properties.len() as u16).to_le_bytes());
    for prop in properties {
        match prop {
            PropertyValue::Null => buf.push(0),
            PropertyValue::String(s) => {
                buf.push(1);
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            PropertyValue::Int(i) => {
                buf.push(2);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            PropertyValue::Double(d) => {
                buf.push(3);
                buf.extend_from_slice(&d.to_le_bytes());
            }
            PropertyValue::Bool(b) => {
                buf.push(4);
                buf.push(*b as u8);
            }
        }
    }
}

fn decode_bucket_records(data: &[u8], coord: TileCoord) -> Result<Vec<Feature>, String> {
    let err = || {
        format!(
            "Corrupt bucket record for tile z{}/{}/{}",
            coord.z, coord.x, coord.y
        )
    };
    let mut feats = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        let take = |pos: &mut usize, n: usize| -> Result<&[u8], String> {
            let s = data.get(*pos..*pos + n).ok_or_else(err)?;
            *pos += n;
            Ok(s)
        };
        let id = u64::from_le_bytes(take(&mut pos, 8)?.try_into().unwrap());
        let lon = f64::from_le_bytes(take(&mut pos, 8)?.try_into().unwrap());
        let lat = f64::from_le_bytes(take(&mut pos, 8)?.try_into().unwrap());
        let nprops = u16::from_le_bytes(take(&mut pos, 2)?.try_into().unwrap()) as usize;
        let mut properties = Vec::with_capacity(nprops);
        for _ in 0..nprops {
            let tag = take(&mut pos, 1)?[0];
            properties.push(match tag {
                0 => PropertyValue::Null,
                1 => {
                    let len = u32::from_le_bytes(take(&mut pos, 4)?.try_into().unwrap()) as usize;
                    PropertyValue::String(
                        String::from_utf8(take(&mut pos, len)?.to_vec()).map_err(|_| err())?,
                    )
                }
                2 => PropertyValue::Int(i64::from_le_bytes(take(&mut pos, 8)?.try_into().unwrap())),
                3 => PropertyValue::Double(f64::from_le_bytes(
                    take(&mut pos, 8)?.try_into().unwrap(),
                )),
                4 => PropertyValue::Bool(take(&mut pos, 1)?[0] != 0),
                _ => return Err(err()),
            });
        }
        feats.push(Feature {
            id: Some(id),
            geometry: Geometry::Point(geo_types::Point::new(lon, lat)),
            properties,
        });
    }
    Ok(feats)
}

// ---------------------------------------------------------------------------
// Query preparation (unchanged behavior: DESCRIBE + SRID probe + prop typing)
// ---------------------------------------------------------------------------

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

    fn prop_select(&self) -> String {
        if self.prop_names.is_empty() {
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
        }
    }
}

// ---------------------------------------------------------------------------
// Tile encode + shared helpers (unchanged)
// ---------------------------------------------------------------------------

/// Sort, optionally coalesce, encode, and gzip one tile. Touches no shared
/// state, so it runs outside the spool lock and in parallel. Returns None
/// when the tile encodes to nothing.
fn encode_tile_compressed(
    coord: TileCoord,
    layer_meta: &LayerMeta,
    prop_names: &[String],
    tile_features: &mut Vec<Feature>,
    config: &TileConfig,
) -> Result<Option<Vec<u8>>, String> {
    if tile_features.is_empty() {
        return Ok(None);
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
        return Ok(None);
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
        return Ok(None);
    }

    Ok(Some(pmtiles_writer::gzip_compress(&tile_bytes)?))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::SilentReporter;

    #[test]
    fn bucket_record_roundtrip() {
        let feature = Feature {
            id: Some(42),
            geometry: Geometry::Point(geo_types::Point::new(-96.5, 32.75)),
            properties: vec![
                PropertyValue::String("alpha".to_string()),
                PropertyValue::Int(-7),
                PropertyValue::Double(3.5),
                PropertyValue::Bool(true),
                PropertyValue::Null,
            ],
        };
        let mut buf = Vec::new();
        encode_bucket_record(&mut buf, &feature);
        encode_bucket_record(&mut buf, &feature);
        let coord = TileCoord { z: 3, x: 1, y: 2 };
        let feats = decode_bucket_records(&buf, coord).unwrap();
        assert_eq!(feats.len(), 2);
        assert_eq!(feats[0].id, Some(42));
        match &feats[0].geometry {
            Geometry::Point(p) => {
                assert_eq!(p.x(), -96.5);
                assert_eq!(p.y(), 32.75);
            }
            _ => panic!("not a point"),
        }
        assert_eq!(feats[0].properties, feature.properties);
    }

    #[test]
    fn corrupt_bucket_record_errors() {
        let coord = TileCoord { z: 0, x: 0, y: 0 };
        assert!(decode_bucket_records(&[1, 2, 3], coord).is_err());
    }

    #[test]
    fn retain_count_semantics() {
        // no drop configured
        assert_eq!(retain_count_for_zoom(1000, 5, 10, None), None);
        // at/above base zoom everything is kept
        assert_eq!(retain_count_for_zoom(1000, 10, 10, Some(2.0)), None);
        // one zoom below base: n / rate
        assert_eq!(retain_count_for_zoom(1000, 9, 10, Some(2.0)), Some(500));
        // clamped to at least 1
        assert_eq!(retain_count_for_zoom(10, 0, 10, Some(2.0)), Some(1));
    }

    #[test]
    fn partition_value_decodes_to_cell_coords() {
        // partition_expr encodes x * 2^z + y; PartitionUnit decodes it back
        let z = PARTITION_INITIAL_ZOOM;
        let n = 1u64 << z;
        for (x, y) in [(0u64, 0u64), (5, 3), (n - 1, n - 1)] {
            let val = x * n + y;
            assert_eq!(val / n, x);
            assert_eq!(val % n, y);
        }
    }

    // Tests below read or set process-global env vars; serialize them.
    #[cfg(feature = "duckdb")]
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[cfg(feature = "duckdb")]
    fn tiny_query_sql(n: u64) -> String {
        // Points spanning several z5 cells so both direct and bucket paths run.
        format!(
            "SELECT ST_Point(-120.0 + (i % 100) * 0.6, 25.0 + (i // 100) * 0.2) AS geometry, \
             (i % 5)::INT AS grp, 'name_' || (i % 3)::VARCHAR AS label \
             FROM range({n}) t(i)"
        )
    }

    #[cfg(feature = "duckdb")]
    fn run_tiny(output: &std::path::Path, drop_rate: Option<f64>) -> u64 {
        let config = TileConfig {
            tile_format: TileFormat::Mvt,
            min_zoom: 0,
            max_zoom: 7,
            base_zoom: None,
            simplification: true,
            drop_rate,
            cluster_distance: None,
            cluster_maxzoom: None,
            coalesce: false,
        };
        generate_pmtiles_from_duckdb_query(
            None,
            &tiny_query_sql(20_000),
            output.to_str().unwrap(),
            "pts",
            &config,
            &SilentReporter,
        )
        .unwrap()
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn partitioned_streaming_is_deterministic() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!("freestiler_sm_test_{}", unique_suffix()));
        fs::create_dir_all(&dir).unwrap();
        let out_a = dir.join("a.pmtiles");
        let out_b = dir.join("b.pmtiles");

        let rows = run_tiny(&out_a, None);
        assert_eq!(rows, 20_000);
        run_tiny(&out_b, None);
        // Deterministic source (range) + preserved insertion order: the whole
        // archive must be byte-identical across runs.
        assert_eq!(fs::read(&out_a).unwrap(), fs::read(&out_b).unwrap());

        // With thinning on, runs must still be deterministic vs themselves.
        let out_c = dir.join("c.pmtiles");
        let out_d = dir.join("d.pmtiles");
        run_tiny(&out_c, Some(2.0));
        run_tiny(&out_d, Some(2.0));
        assert_eq!(fs::read(&out_c).unwrap(), fs::read(&out_d).unwrap());

        // Archive is valid and has tiles at bucket-assembled low zooms.
        let f = fs::File::open(&out_a).unwrap();
        let mut pm = pmtiles2::PMTiles::from_reader(f).unwrap();
        assert!(pm.num_tiles() > 0);
        assert!(
            pm.get_tile(0, 0, 0).unwrap().is_some(),
            "z0 bucket tile missing"
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn refinement_splits_oversized_partitions() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!("freestiler_sm_refine_{}", unique_suffix()));
        fs::create_dir_all(&dir).unwrap();
        let out_small = dir.join("small_units.pmtiles");
        let out_default = dir.join("default_units.pmtiles");

        // Force multi-pass refinement: 20K points, 500-row unit budget.
        std::env::set_var("FREESTILER_UNIT_BUDGET_ROWS", "500");
        let rows = run_tiny(&out_small, None);
        std::env::remove_var("FREESTILER_UNIT_BUDGET_ROWS");
        assert_eq!(rows, 20_000);

        // Refined output must be tile-for-tile identical to the unrefined
        // run (drop off ⇒ partitioning must not affect content at all).
        run_tiny(&out_default, None);
        let a = pmtiles2::PMTiles::from_reader(fs::File::open(&out_small).unwrap()).unwrap();
        let b = pmtiles2::PMTiles::from_reader(fs::File::open(&out_default).unwrap()).unwrap();
        assert_eq!(a.num_tiles(), b.num_tiles());
        let (mut a, mut b) = (a, b);
        for z in 0..=7u8 {
            for x in 0..(1u64 << z) {
                for y in 0..(1u64 << z) {
                    let ta = a.get_tile(x, y, z).unwrap();
                    let tb = b.get_tile(x, y, z).unwrap();
                    assert_eq!(ta, tb, "tile z{z}/{x}/{y} differs");
                }
            }
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn non_point_geometry_is_rejected() {
        let dir = std::env::temp_dir().join(format!("freestiler_sm_rej_{}", unique_suffix()));
        fs::create_dir_all(&dir).unwrap();
        let out = dir.join("rej.pmtiles");
        let config = TileConfig {
            tile_format: TileFormat::Mvt,
            min_zoom: 0,
            max_zoom: 4,
            base_zoom: None,
            simplification: true,
            drop_rate: None,
            cluster_distance: None,
            cluster_maxzoom: None,
            coalesce: false,
        };
        let err = generate_pmtiles_from_duckdb_query(
            None,
            "SELECT ST_Buffer(ST_Point(0, 0), 1.0) AS geometry FROM range(10)",
            out.to_str().unwrap(),
            "polys",
            &config,
            &SilentReporter,
        )
        .unwrap_err();
        assert!(
            err.contains("POINT geometries only"),
            "unexpected error: {err}"
        );
        assert!(!out.exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn tile_budget_exceeded_errors_and_preserves_output() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!("freestiler_sm_budget_{}", unique_suffix()));
        fs::create_dir_all(&dir).unwrap();
        let out = dir.join("budget.pmtiles");
        fs::write(&out, b"KEEP").unwrap();

        // 1 MB tile budget; 200K coincident-cell points blow it at max_zoom.
        std::env::set_var("FREESTILER_TILE_BUDGET_MB", "1");
        let config = TileConfig {
            tile_format: TileFormat::Mvt,
            min_zoom: 4,
            max_zoom: 6,
            base_zoom: None,
            simplification: true,
            drop_rate: None,
            cluster_distance: None,
            cluster_maxzoom: None,
            coalesce: false,
        };
        let result = generate_pmtiles_from_duckdb_query(
            None,
            "SELECT ST_Point(-96.5 + (i % 10) * 0.0001, 32.75) AS geometry, \
             'payload_' || i::VARCHAR AS label FROM range(200000) t(i)",
            out.to_str().unwrap(),
            "pts",
            &config,
            &SilentReporter,
        );
        std::env::remove_var("FREESTILER_TILE_BUDGET_MB");
        let err = result.unwrap_err();
        assert!(err.contains("drop_rate"), "unexpected error: {err}");
        assert_eq!(fs::read(&out).unwrap(), b"KEEP");
        fs::remove_dir_all(&dir).unwrap();
    }
}
