//! Regressions for temporal properties (#18) and CRS-tagged query geometry (#19).
#![cfg(any(feature = "duckdb", feature = "geoparquet"))]

use freestiler_core::{
    mvt,
    tiler::{LayerData, PropertyValue, TileCoord},
};
use prost::Message;
use std::{collections::BTreeMap, fs, path::PathBuf};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("freestiler_native_{}_{suffix}", std::process::id()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn decoded_properties(tile: mvt::Tile) -> Vec<BTreeMap<String, String>> {
    let layer = &tile.layers[0];
    layer
        .features
        .iter()
        .map(|feature| {
            feature
                .tags
                .chunks_exact(2)
                .map(|tag| {
                    let value = &layer.values[tag[1] as usize];
                    (
                        layer.keys[tag[0] as usize].clone(),
                        value.string_value.clone().unwrap(),
                    )
                })
                .collect()
        })
        .collect()
}

fn encode_decode(layer: &LayerData) -> Vec<BTreeMap<String, String>> {
    let bytes = mvt::encode_tile_multilayer(
        &TileCoord { z: 0, x: 0, y: 0 },
        &[(&layer.name, &layer.prop_names, &layer.features)],
    );
    decoded_properties(mvt::Tile::decode(bytes.as_slice()).unwrap())
}

#[cfg(feature = "geoparquet")]
#[test]
fn parquet_temporal_properties_survive_mvt_and_point_replay() {
    use arrow_array::{
        ArrayRef, BinaryArray, Date32Array, Date64Array, RecordBatch, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow_schema::{Field, Schema};
    use freestiler_core::{
        cluster_output::PointSource,
        file_input::{parquet_to_layers, ParquetPointSource},
    };
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let dir = Scratch::new();
    let path = dir.0.join("dates.parquet");
    // WKB point (-80,35); the null property row still has a valid geometry.
    let mut wkb = vec![1, 1, 0, 0, 0];
    wkb.extend_from_slice(&(-80.0f64).to_le_bytes());
    wkb.extend_from_slice(&35.0f64.to_le_bytes());
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "date",
            Arc::new(Date32Array::from(vec![Some(0), Some(-1), None])),
        ),
        (
            "date64",
            Arc::new(Date64Array::from(vec![Some(0), Some(-86_400_000), None])),
        ),
        (
            "seconds",
            Arc::new(TimestampSecondArray::from(vec![Some(0), Some(-1), None])),
        ),
        (
            "millis",
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(123),
                Some(-1),
                None,
            ])),
        ),
        (
            "micros",
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(123456),
                Some(-1),
                None,
            ])),
        ),
        (
            "nanos",
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(123456789),
                Some(-1),
                None,
            ])),
        ),
        (
            "utc",
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(0), Some(-1), None]).with_timezone("UTC"),
            ),
        ),
        (
            "geometry",
            Arc::new(BinaryArray::from_vec(vec![&wkb, &wkb, &wkb])),
        ),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let layers = parquet_to_layers(path.to_str().unwrap(), "dates", 0, 0).unwrap();
    let decoded = encode_decode(&layers[0]);
    assert_eq!(decoded[0]["date"], "1970-01-01");
    assert_eq!(decoded[1]["date"], "1969-12-31");
    assert_eq!(decoded[0]["date64"], "1970-01-01");
    assert_eq!(decoded[0]["seconds"], "1970-01-01T00:00:00");
    assert_eq!(decoded[0]["millis"], "1970-01-01T00:00:00.123");
    assert_eq!(decoded[0]["micros"], "1970-01-01T00:00:00.123456");
    assert_eq!(decoded[0]["nanos"], "1970-01-01T00:00:00.123456789");
    assert_eq!(decoded[0]["utc"], "1970-01-01T00:00:00Z");
    assert!(decoded[2].is_empty());
    let source = ParquetPointSource::open(path.to_str().unwrap()).unwrap();
    let mut replay = Vec::new();
    source
        .scan(&mut |f| {
            replay.push(f.properties);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        replay,
        layers[0]
            .features
            .iter()
            .map(|f| f.properties.clone())
            .collect::<Vec<_>>()
    );
}

#[cfg(feature = "duckdb")]
const TEMPORAL_QUERY: &str = "SELECT ST_Point(-80,35) AS geom, d AS \"survey date\", ts FROM (VALUES (DATE '2026-10-01', TIMESTAMP '2026-10-01 12:34:56.123456'), (NULL::DATE, NULL::TIMESTAMP)) t(d,ts)";

#[cfg(feature = "duckdb")]
#[test]
fn duckdb_temporal_properties_survive_mvt() {
    let layers =
        freestiler_core::file_input::duckdb_query_to_layers(None, TEMPORAL_QUERY, "dates", 0, 0)
            .unwrap();
    let decoded = encode_decode(&layers[0]);
    assert_eq!(decoded[0]["survey date"], "2026-10-01");
    assert_eq!(decoded[0]["ts"], "2026-10-01 12:34:56.123456");
    assert!(decoded[1].is_empty());
}

#[cfg(feature = "duckdb")]
#[test]
fn streaming_temporal_properties_survive_pmtiles() {
    use freestiler_core::{
        engine::{SilentReporter, TileConfig},
        pmtiles_writer::TileFormat,
    };
    let dir = Scratch::new();
    let output = dir.0.join("dates.pmtiles");
    let config = TileConfig {
        tile_format: TileFormat::Mvt,
        min_zoom: 0,
        max_zoom: 0,
        base_zoom: None,
        simplification: true,
        drop_rate: None,
        cluster_distance: None,
        cluster_maxzoom: None,
        coalesce: false,
    };
    freestiler_core::streaming::generate_pmtiles_from_duckdb_query(
        None,
        TEMPORAL_QUERY,
        output.to_str().unwrap(),
        "dates",
        &config,
        &SilentReporter,
    )
    .unwrap();
    let mut pm = pmtiles2::PMTiles::from_reader(fs::File::open(output).unwrap()).unwrap();
    let bytes = pm.get_tile(0, 0, 0).unwrap().unwrap();
    let mut uncompressed = Vec::new();
    use std::io::Read;
    flate2::read::GzDecoder::new(bytes.as_slice())
        .read_to_end(&mut uncompressed)
        .unwrap();
    let decoded = decoded_properties(mvt::Tile::decode(uncompressed.as_slice()).unwrap());
    assert_eq!(decoded.len(), 2);
    assert!(decoded.iter().any(
        |p| p.get("survey date").map(String::as_str) == Some("2026-10-01")
            && p.get("ts").map(String::as_str) == Some("2026-10-01 12:34:56.123456")
    ));
    assert!(decoded.iter().any(BTreeMap::is_empty));
}

#[cfg(feature = "duckdb")]
#[test]
fn duckdb_crs_geometry_is_reprojected_and_exported_only_as_wkb() {
    use freestiler_core::{file_input::duckdb_query_to_layers, tiler::Geometry};
    // A non-WGS84 CRS forces reprojection and catches longitude/latitude swaps.
    let sql = "SELECT 'kept' AS label, ST_SetCRS(ST_Point(-8905559.263461886, 4163881.144064293), 'EPSG:3857') AS geom, 42::INTEGER AS n";
    let layers = duckdb_query_to_layers(None, sql, "projected", 0, 0).unwrap();
    let feature = &layers[0].features[0];
    assert_eq!(
        feature.properties,
        vec![PropertyValue::String("kept".into()), PropertyValue::Int(42)]
    );
    match feature.geometry {
        Geometry::Point(p) => {
            assert!((p.x() + 80.0).abs() < 1e-8);
            assert!((p.y() - 35.0).abs() < 1e-8);
        }
        _ => panic!("expected point"),
    }
    // Also exercise geometry-only SELECTs after removing the original column.
    assert!(duckdb_query_to_layers(
        None,
        "SELECT ST_SetCRS(ST_Point(-80,35), 'EPSG:4326') AS geom",
        "point",
        0,
        0
    )
    .unwrap()[0]
        .features[0]
        .properties
        .is_empty());
}
