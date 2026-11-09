//! `LayeredTiler` (one layer in memory at a time) must write the same archive
//! bytes as `generate_pmtiles` with every layer in memory.

use freestiler_core::{
    engine::{generate_pmtiles, LayeredTiler, SilentReporter, TileConfig},
    geo_types::{Coord, LineString, Point, Polygon},
    pmtiles_writer::{RawSpool, TileFormat},
    tiler::{Feature, Geometry, LayerData, PropertyValue},
};
use std::{fs, path::PathBuf};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("freestiler_layered_{}_{id}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, name: &str) -> String {
        self.0.join(name).to_string_lossy().into_owned()
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A grid of adjacent square cells sharing edges (a small "mesh").
fn mesh(name: &str, n: usize, value_offset: f64, min_zoom: u8, max_zoom: u8) -> LayerData {
    let (x0, y0, step) = (-77.9, 40.9, 0.004);
    let mut features = Vec::new();
    for i in 0..n {
        for j in 0..n {
            let (x, y) = (x0 + i as f64 * step, y0 + j as f64 * step);
            let ring = LineString::from(vec![
                (x, y),
                (x + step, y),
                (x + step, y + step),
                (x, y + step),
                (x, y),
            ]);
            let k = (i * n + j) as u64;
            features.push(Feature {
                id: Some(k + 1),
                geometry: Geometry::Polygon(Polygon::new(ring, vec![])),
                properties: vec![
                    PropertyValue::Int(k as i64),
                    if k % 7 == 0 {
                        PropertyValue::Null
                    } else {
                        PropertyValue::Double(k as f64 * 0.25 + value_offset)
                    },
                    PropertyValue::String(format!("c{}", k % 5)),
                ],
            });
        }
    }
    LayerData {
        name: name.into(),
        features,
        prop_names: vec!["cell_id".into(), "value".into(), "class".into()],
        prop_types: vec!["integer".into(), "double".into(), "string".into()],
        min_zoom,
        max_zoom,
    }
}

fn lines() -> LayerData {
    let features = (0..20)
        .map(|k| {
            let y = 40.9 + k as f64 * 0.003;
            Feature {
                id: Some(1000 + k),
                geometry: Geometry::LineString(LineString::new(
                    (0..30)
                        .map(|i| Coord {
                            x: -77.9 + i as f64 * 0.003,
                            y: y + ((i as f64) * 0.7).sin() * 0.0005,
                        })
                        .collect(),
                )),
                properties: vec![PropertyValue::Bool(k % 2 == 0)],
            }
        })
        .collect();
    LayerData {
        name: "lines".into(),
        features,
        prop_names: vec!["even".into()],
        prop_types: vec!["boolean".into()],
        min_zoom: 0,
        max_zoom: 14,
    }
}

fn points() -> LayerData {
    let features = (0..300)
        .map(|k| Feature {
            id: Some(5000 + k),
            geometry: Geometry::Point(Point::new(
                -77.9 + (k % 20) as f64 * 0.004,
                40.9 + (k / 20) as f64 * 0.004,
            )),
            properties: vec![PropertyValue::Int(k as i64)],
        })
        .collect();
    LayerData {
        name: "points".into(),
        features,
        prop_names: vec!["k".into()],
        prop_types: vec!["integer".into()],
        min_zoom: 0,
        max_zoom: 14,
    }
}

fn layers() -> Vec<LayerData> {
    vec![
        mesh("cells", 30, 0.0, 8, 14),
        lines(),
        mesh("depth", 30, 1.5, 10, 14),
        points(),
        // A layer whose zoom range starts above some others.
        mesh("velocity", 12, 3.0, 12, 13),
    ]
}

fn config(format: TileFormat, variant: u8) -> TileConfig {
    TileConfig {
        tile_format: format,
        min_zoom: 0,
        max_zoom: 14,
        base_zoom: (variant == 4).then_some(12),
        simplification: variant != 1,
        drop_rate: (variant == 4).then_some(2.5),
        cluster_distance: (variant == 2).then_some(40.0),
        cluster_maxzoom: (variant == 2).then_some(11),
        coalesce: variant == 3,
        gzip_level: (variant == 4).then_some(6),
    }
}

fn layered(path: &str, format: TileFormat, variant: u8) {
    let mut tiler = LayeredTiler::new(config(format, variant)).unwrap();
    // Exercise budget-limited assembly batches (one tile per batch at the
    // extreme); the archive must not depend on the batching.
    tiler.set_assembly_budget(if variant % 2 == 0 { 1 } else { 1 << 20 });
    for layer in layers() {
        tiler.add_layer(layer, &SilentReporter).unwrap();
    }
    assert_eq!(tiler.layer_count(), 5);
    tiler.finish(path, &SilentReporter).unwrap();
}

#[test]
fn layered_archive_is_byte_identical_to_in_memory() {
    let scratch = Scratch::new();
    for format in [TileFormat::Mvt, TileFormat::Mlt] {
        // 0: default snapping, 1: no snapping, 2: point clustering,
        // 3: coalesce, 4: feature dropping below base_zoom and gzip level 6
        for variant in 0..5 {
            let a = scratch.file(&format!("memory_{format:?}_{variant}.pmtiles"));
            let b = scratch.file(&format!("layered_{format:?}_{variant}.pmtiles"));
            generate_pmtiles(&layers(), &a, &config(format, variant), &SilentReporter).unwrap();
            layered(&b, format, variant);
            let (a, b) = (fs::read(a).unwrap(), fs::read(b).unwrap());
            assert!(a.len() > 1000);
            assert!(a == b, "{format:?} variant {variant}: archives differ");
        }
    }
}

#[test]
fn layered_without_tiles_fails_like_in_memory() {
    let scratch = Scratch::new();
    let out = scratch.file("empty.pmtiles");
    let empty = LayerData {
        name: "empty".into(),
        features: vec![],
        prop_names: vec![],
        prop_types: vec![],
        min_zoom: 0,
        max_zoom: 4,
    };
    let mut tiler = LayeredTiler::new(config(TileFormat::Mvt, 0)).unwrap();
    tiler.add_layer(empty, &SilentReporter).unwrap();
    let error = tiler.finish(&out, &SilentReporter).unwrap_err();
    assert!(error.contains("No tiles"), "{error}");
    assert!(!std::path::Path::new(&out).exists());
}

#[test]
fn raw_spool_appends_after_reads() {
    let mut spool = RawSpool::new().unwrap();
    let a = spool.append(b"first").unwrap();
    let b = spool.append(b"second").unwrap();
    let mut out = Vec::new();
    // Reading `a` leaves the file cursor inside the data; the next append
    // must still go to the end instead of overwriting `b`.
    spool.read_into(a, &mut out).unwrap();
    let c = spool.append(b"third").unwrap();
    assert_eq!((b, c), ((5, 6), (11, 5)));
    for extent in [c, b, a] {
        spool.read_into(extent, &mut out).unwrap();
    }
    assert_eq!(out, b"firstthirdsecondfirst");
    assert!(spool.read_into((12, 5), &mut out).is_err());
}

#[test]
fn layered_refuses_bad_gzip_level_before_output() {
    let mut bad = config(TileFormat::Mvt, 0);
    bad.gzip_level = Some(12);
    let error = LayeredTiler::new(bad).err().expect("level 12 must refuse");
    assert!(error.contains("gzip_level"), "{error}");
}
