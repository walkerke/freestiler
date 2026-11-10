//! Standalone qualification transport; GeoJSON input, actual freestiler engine.
use freestiler_core::{
    engine,
    geo_types::{LineString, MultiPolygon, Polygon},
    pmtiles_writer::{self, LayerMeta, TileFormat, TileSpool},
    tiler::{Feature, Geometry, LayerData, PropertyValue},
};
use serde_json::Value;
use std::{env, fs, time::Instant};

fn polygon(v: &Value) -> Polygon<f64> {
    let rings: Vec<LineString<f64>> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            r.as_array()
                .unwrap()
                .iter()
                .map(|c| {
                    let c = c.as_array().unwrap();
                    (c[0].as_f64().unwrap(), c[1].as_f64().unwrap())
                })
                .collect()
        })
        .collect();
    Polygon::new(rings[0].clone(), rings[1..].to_vec())
}
fn main() {
    let a: Vec<String> = env::args().collect();
    let source: Value = serde_json::from_slice(&fs::read(&a[1]).unwrap()).unwrap();
    let zoom: u8 = a[3].parse().unwrap();
    let simplify = a.get(4).is_some_and(|x| x == "true");
    let format = if a.get(5).is_some_and(|x| x == "mlt") {
        TileFormat::Mlt
    } else {
        TileFormat::Mvt
    };
    let rows = source["features"].as_array().unwrap();
    let names: Vec<String> = rows[0]["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    let features = rows
        .iter()
        .map(|f| {
            let g = &f["geometry"];
            let geometry = match g["type"].as_str().unwrap() {
                "Polygon" => Geometry::Polygon(polygon(&g["coordinates"])),
                "MultiPolygon" => Geometry::MultiPolygon(MultiPolygon(
                    g["coordinates"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(polygon)
                        .collect(),
                )),
                _ => panic!("polygon corpus only"),
            };
            let properties = names
                .iter()
                .map(|k| match &f["properties"][k] {
                    Value::String(s) => PropertyValue::String(s.clone()),
                    Value::Bool(b) => PropertyValue::Bool(*b),
                    Value::Number(n) => n
                        .as_i64()
                        .map(PropertyValue::Int)
                        .unwrap_or(PropertyValue::Double(n.as_f64().unwrap())),
                    Value::Null => PropertyValue::Null,
                    _ => panic!("scalar properties only"),
                })
                .collect();
            Feature {
                id: Some(f["id"].as_str().unwrap().parse().unwrap()),
                geometry,
                properties,
            }
        })
        .collect();
    let layers = vec![LayerData {
        name: "features".into(),
        features,
        prop_names: names.clone(),
        prop_types: vec!["String".into(); names.len()],
        min_zoom: zoom,
        max_zoom: zoom,
    }];
    let config = engine::TileConfig {
        tile_format: format,
        min_zoom: zoom,
        max_zoom: zoom,
        base_zoom: None,
        simplification: simplify,
        drop_rate: None,
        cluster_distance: None,
        cluster_maxzoom: None,
        coalesce: false,
    };
    let start = Instant::now();
    let mut spool = TileSpool::new().unwrap();
    engine::generate_tiles(&layers, &config, &engine::SilentReporter, &mut spool).unwrap();
    let metadata = vec![LayerMeta {
        name: "features".into(),
        property_names: names.clone(),
        property_types: vec!["String".into(); names.len()],
        min_zoom: zoom,
        max_zoom: zoom,
        geometry_type: Some("Polygon".into()),
    }];
    pmtiles_writer::write_pmtiles_from_spool(
        &a[2],
        &mut spool,
        format,
        &metadata,
        zoom,
        zoom,
        engine::compute_all_bounds(&layers),
    )
    .unwrap();
    if format == TileFormat::Mlt {
        use std::io::Read;
        // pmtiles2 0.3.1 predates the MLT tile-type byte6. Only its private
        // in-memory directory-reader view uses1; archive/payloads are untouched.
        let mut container = fs::read(&a[2]).unwrap();
        assert_eq!(container[99], 6);
        container[99] = 1;
        let mut reader = pmtiles2::PMTiles::from_reader(std::io::Cursor::new(container)).unwrap();
        let ids: Vec<u64> = reader.tile_ids().into_iter().copied().collect();
        let mut decoded = Vec::new();
        for id in ids {
            let raw = reader.get_tile_by_id(id).unwrap().unwrap();
            let mut tile = Vec::new();
            flate2::read::GzDecoder::new(raw.as_slice())
                .read_to_end(&mut tile)
                .unwrap();
            let mut layers = mlt_core::parse_layers(&tile).unwrap();
            for layer in &mut layers {
                layer.decode_all().unwrap();
            }
            decoded.push(serde_json::json!({"address":pmtiles2::util::zxy(id).unwrap(),"collection":mlt_core::geojson::FeatureCollection::from_layers(&layers).unwrap()}));
        }
        fs::write(
            format!("{}.decoded.json", a[2]),
            serde_json::to_vec(&decoded).unwrap(),
        )
        .unwrap();
    }
    println!(
        "{{\"rows\":{},\"seconds\":{}}}",
        rows.len(),
        start.elapsed().as_secs_f64()
    );
}
