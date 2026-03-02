//! Direct file input: read spatial files into LayerData without R/Python overhead.
//!
//! - `geoparquet` feature: read GeoParquet files via arrow-rs
//! - `duckdb` feature: read any spatial file via DuckDB's spatial extension

use crate::tiler::{Feature, Geometry, LayerData, PropertyValue};

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

        let geom_col_name = find_geometry_column(builder.metadata())
            .unwrap_or_else(|| "geometry".to_string());

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

    fn find_geometry_column(
        metadata: &parquet::file::metadata::ParquetMetaData,
    ) -> Option<String> {
        let kv = metadata.file_metadata().key_value_metadata()?;
        for entry in kv {
            if entry.key == "geo" {
                if let Some(ref value) = entry.value {
                    if let Ok(geo_meta) = serde_json::from_str::<serde_json::Value>(value) {
                        if let Some(col) =
                            geo_meta.get("primary_column").and_then(|v| v.as_str())
                        {
                            return Some(col.to_string());
                        }
                    }
                }
            }
        }
        None
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
                col.as_primitive::<arrow_array::types::Int8Type>().value(row) as i64,
            ),
            DataType::Int16 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::Int16Type>().value(row) as i64,
            ),
            DataType::Int32 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::Int32Type>().value(row) as i64,
            ),
            DataType::Int64 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::Int64Type>().value(row),
            ),
            DataType::UInt8 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::UInt8Type>().value(row) as i64,
            ),
            DataType::UInt16 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::UInt16Type>().value(row) as i64,
            ),
            DataType::UInt32 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::UInt32Type>().value(row) as i64,
            ),
            DataType::UInt64 => PropertyValue::Int(
                col.as_primitive::<arrow_array::types::UInt64Type>().value(row) as i64,
            ),
            DataType::Float32 => {
                let v =
                    col.as_primitive::<arrow_array::types::Float32Type>().value(row) as f64;
                if v.is_nan() {
                    PropertyValue::Null
                } else {
                    PropertyValue::Double(v)
                }
            }
            DataType::Float64 => {
                let v = col.as_primitive::<arrow_array::types::Float64Type>().value(row);
                if v.is_nan() {
                    PropertyValue::Null
                } else {
                    PropertyValue::Double(v)
                }
            }
            DataType::Utf8 => PropertyValue::String(
                col.as_string::<i32>().value(row).to_string(),
            ),
            DataType::LargeUtf8 => PropertyValue::String(
                col.as_string::<i64>().value(row).to_string(),
            ),
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
            None => {
                Connection::open_in_memory().map_err(|e| format!("Cannot open DB: {}", e))?
            }
        };

        conn.execute_batch("INSTALL spatial; LOAD spatial;")
            .map_err(|e| format!("Cannot load spatial extension: {}", e))?;

        // Wrap query to export geometry as WKB
        let wkb_sql = format!(
            "SELECT *, ST_AsWKB(geom) AS __wkb FROM ({}) AS __t",
            sql
        );

        let mut stmt = conn
            .prepare(&wkb_sql)
            .map_err(|e| format!("Query error: {}", e))?;

        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).map_or("?".to_string(), |v| v.to_string()))
            .collect();

        let wkb_col_idx = column_names
            .iter()
            .position(|n| n == "__wkb")
            .ok_or("No __wkb column in result")?;

        let skip_cols: Vec<&str> = vec!["geom", "geometry", "__wkb"];
        let mut prop_names: Vec<String> = Vec::new();
        let mut prop_col_indices: Vec<usize> = Vec::new();

        for (i, name) in column_names.iter().enumerate() {
            if skip_cols.contains(&name.to_lowercase().as_str()) {
                continue;
            }
            prop_names.push(name.clone());
            prop_col_indices.push(i);
        }

        let mut features: Vec<Feature> = Vec::new();
        let mut prop_types: Vec<String> = Vec::new();
        let mut first_row = true;

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
            for &col_idx in &prop_col_indices {
                properties.push(extract_value(row, col_idx));
            }

            if first_row {
                prop_types = properties
                    .iter()
                    .map(|v| match v {
                        PropertyValue::String(_) => "character".to_string(),
                        PropertyValue::Int(_) => "integer".to_string(),
                        PropertyValue::Double(_) => "numeric".to_string(),
                        PropertyValue::Bool(_) => "logical".to_string(),
                        PropertyValue::Null => "character".to_string(),
                    })
                    .collect();
                first_row = false;
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

    fn extract_value(row: &duckdb::Row, col_idx: usize) -> PropertyValue {
        if let Ok(v) = row.get::<_, i64>(col_idx) {
            return PropertyValue::Int(v);
        }
        if let Ok(v) = row.get::<_, f64>(col_idx) {
            if v.is_nan() {
                return PropertyValue::Null;
            }
            return PropertyValue::Double(v);
        }
        if let Ok(v) = row.get::<_, bool>(col_idx) {
            return PropertyValue::Bool(v);
        }
        if let Ok(v) = row.get::<_, String>(col_idx) {
            return PropertyValue::String(v);
        }
        PropertyValue::Null
    }
}

#[cfg(feature = "duckdb")]
pub use duckdb_impl::{duckdb_file_to_layers, duckdb_query_to_layers};
