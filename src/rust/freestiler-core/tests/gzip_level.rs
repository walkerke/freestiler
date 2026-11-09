//! `TileConfig::gzip_level` changes only the tile compression, never tile
//! contents, and out-of-range levels refuse.

use freestiler_core::{
    engine::{generate_pmtiles, SilentReporter, TileConfig},
    geo_types::{LineString, Polygon},
    pmtiles_writer::{gzip_compress_level, TileFormat},
    tiler::{Feature, Geometry, LayerData, PropertyValue},
};

fn mesh() -> LayerData {
    let step = 0.002;
    let mut features = Vec::new();
    for i in 0..40 {
        for j in 0..40 {
            let (x, y) = (-77.9 + i as f64 * step, 40.9 + j as f64 * step);
            let ring = LineString::from(vec![
                (x, y),
                (x + step, y),
                (x + step, y + step),
                (x, y + step),
                (x, y),
            ]);
            let k = (i * 40 + j) as i64;
            features.push(Feature {
                id: Some(k as u64 + 1),
                geometry: Geometry::Polygon(Polygon::new(ring, vec![])),
                properties: vec![
                    PropertyValue::Int(k),
                    PropertyValue::Double(k as f64 * 0.01),
                ],
            });
        }
    }
    LayerData {
        name: "cells".into(),
        features,
        prop_names: vec!["cell_id".into(), "depth".into()],
        prop_types: vec!["integer".into(), "double".into()],
        min_zoom: 8,
        max_zoom: 14,
    }
}

fn config(gzip_level: Option<u32>) -> TileConfig {
    TileConfig {
        tile_format: TileFormat::Mvt,
        min_zoom: 8,
        max_zoom: 14,
        base_zoom: None,
        simplification: true,
        drop_rate: None,
        cluster_distance: None,
        cluster_maxzoom: None,
        coalesce: false,
        gzip_level,
    }
}

fn gunzip(data: &[u8]) -> Vec<u8> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(data)
        .read_to_end(&mut out)
        .unwrap();
    out
}

#[test]
fn levels_round_trip_and_bound() {
    let data = b"freestiler tile bytes ".repeat(500);
    let fast = gzip_compress_level(&data, None).unwrap();
    let best = gzip_compress_level(&data, Some(9)).unwrap();
    assert_eq!(gunzip(&fast), data);
    assert_eq!(gunzip(&best), data);
    assert!(gzip_compress_level(&data, Some(10)).is_err());
}

#[test]
fn higher_level_shrinks_archive_without_changing_it_otherwise() {
    let dir = std::env::temp_dir().join(format!("freestiler_gzip_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir(&dir).unwrap();
    let fast = dir.join("fast.pmtiles");
    let best = dir.join("best.pmtiles");
    generate_pmtiles(
        &[mesh()],
        fast.to_str().unwrap(),
        &config(None),
        &SilentReporter,
    )
    .unwrap();
    generate_pmtiles(
        &[mesh()],
        best.to_str().unwrap(),
        &config(Some(9)),
        &SilentReporter,
    )
    .unwrap();
    let (a, b) = (
        std::fs::metadata(&fast).unwrap().len(),
        std::fs::metadata(&best).unwrap().len(),
    );
    assert!(b < a, "level 9 {b} >= level 1 {a}");
    let tiles = |path: &std::path::Path| {
        let mut archive =
            pmtiles2::PMTiles::from_reader(std::fs::File::open(path).unwrap()).unwrap();
        let mut ids: Vec<u64> = archive.tile_ids().into_iter().copied().collect();
        ids.sort_unstable();
        ids.into_iter()
            .map(|id| (id, gunzip(&archive.get_tile_by_id(id).unwrap().unwrap())))
            .collect::<Vec<_>>()
    };
    let decoded = tiles(&fast);
    assert!(decoded.len() > 10);
    assert!(
        decoded == tiles(&best),
        "tile contents differ between gzip levels"
    );
    let refused = dir.join("refused.pmtiles");
    let error = generate_pmtiles(
        &[mesh()],
        refused.to_str().unwrap(),
        &config(Some(12)),
        &SilentReporter,
    )
    .unwrap_err();
    assert!(error.contains("gzip_level"), "{error}");
    assert!(!refused.exists());
    let _ = std::fs::remove_dir_all(&dir);
}
