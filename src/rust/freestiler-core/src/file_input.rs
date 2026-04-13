//! Direct file input: read spatial files into LayerData without R/Python overhead.
//!
//! - `geoparquet` feature: read GeoParquet files via arrow-rs
//! - `duckdb` feature: read any spatial file via DuckDB's spatial extension

use crate::tiler::{Feature, Geometry, LayerData, PropertyValue};

// ---------------------------------------------------------------------------
// Multi-statement SQL support (shared by duckdb file_input and streaming)
// ---------------------------------------------------------------------------

/// Split multi-statement SQL into setup statements and a final query.
///
/// Statements like `LOAD h3;` or `CREATE TEMP TABLE ...;` are returned as
/// setup, and the last non-empty statement is the query to DESCRIBE/execute.
pub(crate) fn split_sql_statements(sql: &str) -> (Vec<String>, String) {
    let parts: Vec<&str> = sql.split(';').collect();
    let non_empty: Vec<String> = parts
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if non_empty.len() <= 1 {
        return (Vec::new(), sql.trim().trim_end_matches(';').to_string());
    }
    let setup = non_empty[..non_empty.len() - 1].to_vec();
    let query = non_empty.last().unwrap().clone();
    (setup, query)
}

/// Execute setup statements (LOAD, CREATE, etc.) on a DuckDB connection.
#[cfg(feature = "duckdb")]
pub(crate) fn run_setup_statements(
    conn: &duckdb::Connection,
    stmts: &[String],
) -> Result<(), String> {
    for stmt in stmts {
        conn.execute_batch(stmt)
            .map_err(|e| format!("Setup statement failed: {}", e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_single_statement() {
        let (setup, query) = split_sql_statements("SELECT * FROM foo");
        assert!(setup.is_empty());
        assert_eq!(query, "SELECT * FROM foo");
    }

    #[test]
    fn test_split_single_with_trailing_semicolon() {
        let (setup, query) = split_sql_statements("SELECT * FROM foo;");
        assert!(setup.is_empty());
        assert_eq!(query, "SELECT * FROM foo");
    }

    #[test]
    fn test_split_multi_statement() {
        let sql = "LOAD h3; SELECT * FROM foo";
        let (setup, query) = split_sql_statements(sql);
        assert_eq!(setup, vec!["LOAD h3"]);
        assert_eq!(query, "SELECT * FROM foo");
    }

    #[test]
    fn test_split_multi_statement_with_trailing_semicolon() {
        let sql = "LOAD spatial; LOAD h3; SELECT h3_cell FROM t;";
        let (setup, query) = split_sql_statements(sql);
        assert_eq!(setup, vec!["LOAD spatial", "LOAD h3"]);
        assert_eq!(query, "SELECT h3_cell FROM t");
    }
}

// ---------------------------------------------------------------------------
// Shared WKB → Geometry conversion (used by both geoparquet and duckdb)
// ---------------------------------------------------------------------------

#[cfg(any(feature = "geoparquet", feature = "duckdb"))]
fn wkb_to_geometry(wkb_bytes: &[u8]) -> Option<Geometry> {
    use geozero::wkb::Wkb;
    use geozero::ToGeo;

    let geo_geom = Wkb(wkb_bytes).to_geo().ok()?;
    match geo_geom {
        geo_types::Geometry::Point(p) => Some(Geometry::Point(p)),
        geo_types::Geometry::MultiPoint(mp) => Some(Geometry::MultiPoint(mp)),
        geo_types::Geometry::LineString(ls) => Some(Geometry::LineString(ls)),
        geo_types::Geometry::MultiLineString(mls) => Some(Geometry::MultiLineString(mls)),
        geo_types::Geometry::Polygon(p) => Some(Geometry::Polygon(p)),
        geo_types::Geometry::MultiPolygon(mp) => Some(Geometry::MultiPolygon(mp)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// GeoParquet input
// ---------------------------------------------------------------------------

#[cfg(feature = "geoparquet")]
mod geoparquet_impl {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    use arrow_schema::DataType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    pub fn parquet_to_layers(
        path: &str,
        layer_name: &str,
        min_zoom: u8,
        max_zoom: u8,
    ) -> Result<Vec<LayerData>, String> {
        let file = File::open(path).map_err(|e| format!("Cannot open {}: {}", path, e))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("Cannot read parquet: {}", e))?;

        let geom_col_name =
            find_geometry_column(builder.metadata()).unwrap_or_else(|| "geometry".to_string());

        check_crs_is_wgs84(builder.metadata(), &geom_col_name)?;

        let reader = builder
            .build()
            .map_err(|e| format!("Cannot build reader: {}", e))?;

        let mut features: Vec<Feature> = Vec::new();
        let mut prop_names: Vec<String> = Vec::new();
        let mut prop_types: Vec<String> = Vec::new();
        let mut first_batch = true;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| format!("Read error: {}", e))?;
            let schema = batch.schema();
            let n_rows = batch.num_rows();

            if first_batch {
                for field in schema.fields() {
                    let name = field.name();
                    if name == &geom_col_name {
                        continue;
                    }
                    prop_names.push(name.clone());
                    prop_types.push(arrow_type_to_string(field.data_type()));
                }
                first_batch = false;
            }

            let geom_idx = schema
                .index_of(&geom_col_name)
                .map_err(|_| format!("Geometry column '{}' not found", geom_col_name))?;
            let geom_col = batch.column(geom_idx);

            for row in 0..n_rows {
                let geometry = match extract_wkb_geometry(geom_col.as_ref(), row) {
                    Some(g) => g,
                    None => continue,
                };

                let mut properties = Vec::with_capacity(prop_names.len());
                let mut prop_col_idx = 0;
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    if field.name() == &geom_col_name {
                        continue;
                    }
                    if prop_col_idx >= prop_names.len() {
                        break;
                    }
                    let col = batch.column(col_idx);
                    properties.push(extract_property_value(col.as_ref(), row));
                    prop_col_idx += 1;
                }

                features.push(Feature {
                    id: Some((features.len() + 1) as u64),
                    geometry,
                    properties,
                });
            }
        }

        if features.is_empty() {
            return Err("No valid features found in parquet file".to_string());
        }

        Ok(vec![LayerData {
            name: layer_name.to_string(),
            features,
            prop_names,
            prop_types,
            min_zoom,
            max_zoom,
        }])
    }

    fn find_geometry_column(metadata: &parquet::file::metadata::ParquetMetaData) -> Option<String> {
        let kv = metadata.file_metadata().key_value_metadata()?;
        for entry in kv {
            if entry.key == "geo" {
                if let Some(ref value) = entry.value {
                    if let Ok(geo_meta) = serde_json::from_str::<serde_json::Value>(value) {
                        if let Some(col) = geo_meta.get("primary_column").and_then(|v| v.as_str()) {
                            return Some(col.to_string());
                        }
                    }
                }
            }
        }
        None
    }

    /// Check if GeoParquet CRS is WGS84 (EPSG:4326) or unspecified (OGC:CRS84).
    /// Returns Ok(()) if safe to tile, Err with message if reprojection needed.
    fn check_crs_is_wgs84(
        metadata: &parquet::file::metadata::ParquetMetaData,
        geom_col: &str,
    ) -> Result<(), String> {
        let kv = match metadata.file_metadata().key_value_metadata() {
            Some(kv) => kv,
            None => return Ok(()), // No metadata — assume WGS84
        };

        for entry in kv {
            if entry.key == "geo" {
                if let Some(ref value) = entry.value {
                    if let Ok(geo_meta) = serde_json::from_str::<serde_json::Value>(value) {
                        // Look up the column's CRS in "columns" → col_name → "crs"
                        if let Some(col_meta) =
                            geo_meta.get("columns").and_then(|c| c.get(geom_col))
                        {
                            // No CRS key means OGC:CRS84 (lon/lat) per GeoParquet spec
                            let crs = match col_meta.get("crs") {
                                Some(serde_json::Value::Null) => return Ok(()),
                                None => return Ok(()),
                                Some(crs_obj) => crs_obj,
                            };

                            // Check for EPSG:4326 / OGC:CRS84 in the PROJJSON id
                            if let Some(id) = crs.get("id") {
                                let authority =
                                    id.get("authority").and_then(|a| a.as_str()).unwrap_or("");
                                let code = id.get("code").and_then(|c| c.as_u64()).unwrap_or(0);

                                if (authority == "EPSG" && code == 4326)
                                    || (authority == "OGC" && code == 84)
                                {
                                    return Ok(());
                                }

                                return Err(format!(
                                    "GeoParquet file uses CRS {}:{}, but freestiler requires WGS84 (EPSG:4326). \
                                     Reproject before tiling, or use freestile_query() with DuckDB which auto-reprojects.",
                                    authority, code
                                ));
                            }

                            // Has CRS but no parseable id — check name as fallback
                            if let Some(name) = crs.get("name").and_then(|n| n.as_str()) {
                                let name_upper = name.to_uppercase();
                                if name_upper.contains("WGS 84")
                                    || name_upper.contains("WGS84")
                                    || name_upper.contains("CRS84")
                                {
                                    return Ok(());
                                }
                                return Err(format!(
                                    "GeoParquet file uses CRS '{}', but freestiler requires WGS84 (EPSG:4326). \
                                     Reproject before tiling, or use freestile_query() with DuckDB which auto-reprojects.",
                                    name
                                ));
                            }
                        }
                    }
                }
            }
        }

        Ok(()) // No geo metadata or no column entry — assume WGS84
    }

    fn arrow_type_to_string(dt: &DataType) -> String {
        match dt {
            DataType::Boolean => "logical".to_string(),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => "integer".to_string(),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => "numeric".to_string(),
            DataType::Utf8 | DataType::LargeUtf8 => "character".to_string(),
            _ => "character".to_string(),
        }
    }

    fn extract_wkb_geometry(col: &dyn Array, row: usize) -> Option<Geometry> {
        if col.is_null(row) {
            return None;
        }
        let wkb_bytes: Option<&[u8]> = match col.data_type() {
            DataType::Binary => Some(col.as_binary::<i32>().value(row)),
            DataType::LargeBinary => Some(col.as_binary::<i64>().value(row)),
            _ => None,
        };
        wkb_bytes.and_then(wkb_to_geometry)
    }

    fn extract_property_value(col: &dyn Array, row: usize) -> PropertyValue {
        if col.is_null(row) {
            return PropertyValue::Null;
        }
        match col.data_type() {
            DataType::Boolean => PropertyValue::Bool(col.as_boolean().value(row)),
            DataType::Int8 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::Int8Type>()
                    .value(row) as i64,
            ),
            DataType::Int16 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::Int16Type>()
                    .value(row) as i64,
            ),
            DataType::Int32 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::Int32Type>()
                    .value(row) as i64,
            ),
            DataType::Int64 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::Int64Type>()
                    .value(row),
            ),
            DataType::UInt8 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::UInt8Type>()
                    .value(row) as i64,
            ),
            DataType::UInt16 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::UInt16Type>()
                    .value(row) as i64,
            ),
            DataType::UInt32 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::UInt32Type>()
                    .value(row) as i64,
            ),
            DataType::UInt64 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::UInt64Type>()
                    .value(row) as i64,
            ),
            DataType::Float32 => {
                let v = col
                    .as_primitive::<arrow_array::types::Float32Type>()
                    .value(row) as f64;
                if v.is_nan() {
                    PropertyValue::Null
                } else {
                    PropertyValue::Double(v)
                }
            }
            DataType::Float64 => {
                let v = col
                    .as_primitive::<arrow_array::types::Float64Type>()
                    .value(row);
                if v.is_nan() {
                    PropertyValue::Null
                } else {
                    PropertyValue::Double(v)
                }
            }
            DataType::Utf8 => PropertyValue::String(col.as_string::<i32>().value(row).to_string()),
            DataType::LargeUtf8 => {
                PropertyValue::String(col.as_string::<i64>().value(row).to_string())
            }
            _ => PropertyValue::Null,
        }
    }
}

#[cfg(feature = "geoparquet")]
pub use geoparquet_impl::parquet_to_layers;

// ---------------------------------------------------------------------------
// DuckDB file input
// ---------------------------------------------------------------------------

#[cfg(feature = "duckdb")]
mod duckdb_impl {
    use super::*;

    #[derive(Clone, Copy)]
    enum DuckDbValueKind {
        String,
        Int,
        Double,
        Bool,
    }

    pub fn duckdb_file_to_layers(
        path: &str,
        layer_name: &str,
        min_zoom: u8,
        max_zoom: u8,
    ) -> Result<Vec<LayerData>, String> {
        let sql = format!(
            "SELECT * FROM ST_Read('{}', open_options=['FLATTEN_NESTED_ATTRIBUTES=YES'])",
            path.replace('\'', "''")
        );
        duckdb_query_to_layers(None, &sql, layer_name, min_zoom, max_zoom)
    }

    pub fn duckdb_query_to_layers(
        db_path: Option<&str>,
        sql: &str,
        layer_name: &str,
        min_zoom: u8,
        max_zoom: u8,
    ) -> Result<Vec<LayerData>, String> {
        use duckdb::{params, Connection};

        let conn = match db_path {
            Some(p) => Connection::open(p).map_err(|e| format!("Cannot open DB: {}", e))?,
            None => Connection::open_in_memory().map_err(|e| format!("Cannot open DB: {}", e))?,
        };

        conn.execute_batch("INSTALL spatial; LOAD spatial;")
            .map_err(|e| format!("Cannot load spatial extension: {}", e))?;

        // Split multi-statement SQL: run setup, keep final SELECT
        let (setup_stmts, sql) = split_sql_statements(sql);
        run_setup_statements(&conn, &setup_stmts)?;

        // Discover column names and geometry column via DESCRIBE
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
            "No geometry column found in query result. Ensure your query returns a GEOMETRY column.".to_string()
        })?;

        let wkb_col_idx = all_columns.len();

        let geom_col_lower = geom_col_name.to_lowercase();
        let skip_cols: Vec<String> = vec![geom_col_lower, "__wkb".into()];
        let mut prop_names: Vec<String> = Vec::new();
        let mut prop_col_indices: Vec<usize> = Vec::new();
        let mut prop_types: Vec<String> = Vec::new();
        let mut prop_value_kinds: Vec<DuckDbValueKind> = Vec::new();

        for (i, (name, dtype)) in all_columns.iter().enumerate() {
            let name_lower = name.to_lowercase();
            if skip_cols.contains(&name_lower) {
                continue;
            }
            prop_names.push(name.clone());
            prop_col_indices.push(i);
            prop_types.push(duckdb_type_to_property_type(dtype));
            prop_value_kinds.push(duckdb_type_to_value_kind(dtype));
        }

        // Detect source CRS via ST_SRID on the first non-null geometry
        let srid_sql = format!(
            "SELECT ST_SRID(\"{}\") AS __srid FROM ({}) AS __t WHERE \"{}\" IS NOT NULL LIMIT 1",
            geom_col_name, sql, geom_col_name
        );
        let source_srid: Option<String> = conn
            .query_row(&srid_sql, params![], |row| row.get::<_, String>(0))
            .ok();

        // Build geometry expression: reproject if not already WGS84
        let geom_expr = match source_srid.as_deref() {
            // Already WGS84 or unknown — use as-is
            None | Some("EPSG:4326") | Some("") => {
                format!("ST_AsWKB(\"{}\")", geom_col_name)
            }
            Some(src_crs) => {
                format!(
                    "ST_AsWKB(ST_Transform(\"{}\", '{}', 'EPSG:4326'))",
                    geom_col_name, src_crs
                )
            }
        };

        let wkb_sql = format!("SELECT *, {} AS __wkb FROM ({}) AS __t", geom_expr, sql);

        let mut stmt = conn
            .prepare(&wkb_sql)
            .map_err(|e| format!("Query error: {}", e))?;

        let mut features: Vec<Feature> = Vec::new();

        let mut rows = stmt
            .query(params![])
            .map_err(|e| format!("Query error: {}", e))?;

        while let Some(row) = rows.next().map_err(|e| format!("Row error: {}", e))? {
            let wkb_bytes: Vec<u8> = row
                .get(wkb_col_idx)
                .map_err(|e| format!("WKB read error: {}", e))?;

            let geometry = match wkb_to_geometry(&wkb_bytes) {
                Some(g) => g,
                None => continue,
            };

            let mut properties = Vec::with_capacity(prop_names.len());
            for (&col_idx, &kind) in prop_col_indices.iter().zip(prop_value_kinds.iter()) {
                properties.push(extract_value(row, col_idx, kind));
            }

            features.push(Feature {
                id: Some((features.len() + 1) as u64),
                geometry,
                properties,
            });
        }

        if features.is_empty() {
            return Err("No valid features found".to_string());
        }

        Ok(vec![LayerData {
            name: layer_name.to_string(),
            features,
            prop_names,
            prop_types,
            min_zoom,
            max_zoom,
        }])
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

    fn duckdb_type_to_property_type(dtype: &str) -> String {
        match duckdb_type_to_value_kind(dtype) {
            DuckDbValueKind::String => "character".to_string(),
            DuckDbValueKind::Int => "integer".to_string(),
            DuckDbValueKind::Double => "numeric".to_string(),
            DuckDbValueKind::Bool => "logical".to_string(),
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
}

#[cfg(feature = "duckdb")]
pub use duckdb_impl::{duckdb_file_to_layers, duckdb_query_to_layers};
