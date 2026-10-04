//! Bounded tile output for the compact, ordered Supercluster kernel.
//!
//! The global index is resident (not an out-of-core clustering claim). Input
//! adapters are replayable, ordered streams. Only surviving original features
//! are recovered, to an indexed disk file; neither all Features nor all zoom
//! levels are retained. This module has no DuckDB/Arrow/host dependency.
use crate::engine::ProgressReporter;
use crate::pmtiles_writer::{self, LayerMeta, TileFormat, TileSpool};
use crate::supercluster::{self, Input, Level, Options};
use crate::tiler::{Feature, Geometry, LayerData, PropertyValue, TileCoord};
use crate::{mlt, mvt};
use geo_types::Point;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub trait PointSource {
    fn len(&self) -> usize;
    fn property_names(&self) -> &[String];
    fn property_types(&self) -> &[String];
    /// Visit every physical row, in the SAME order on every call. Reject null,
    /// invalid and non-POINT geometry rather than silently changing ordinals.
    fn scan(&self, visit: &mut dyn FnMut(Feature) -> Result<(), String>) -> Result<(), String>;
    fn check_unchanged(&self) -> Result<(), String>;
    fn provenance(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

impl PointSource for LayerData {
    fn len(&self) -> usize { self.features.len() }
    fn property_names(&self) -> &[String] { &self.prop_names }
    fn property_types(&self) -> &[String] { &self.prop_types }
    fn scan(&self, visit: &mut dyn FnMut(Feature) -> Result<(), String>) -> Result<(), String> {
        for feature in &self.features {
            visit(feature.clone())?;
        }
        Ok(())
    }
    fn check_unchanged(&self) -> Result<(), String> { Ok(()) }
}

#[derive(Clone, Default)]
pub struct Categories {
    pub column: Option<String>,
    pub values: Vec<PropertyValue>,
}
impl Categories {
    pub fn from_json(column: &str, json: &str) -> Result<Self, String> {
        if column.is_empty() && (json == "[]" || json.is_empty()) {
            return Ok(Self::default());
        }
        let raw: Vec<serde_json::Value> =
            serde_json::from_str(json).map_err(|e| format!("Invalid category_values: {e}"))?;
        let values = raw
            .into_iter()
            .map(|v| match v {
                serde_json::Value::String(s) => Ok(PropertyValue::String(s)),
                serde_json::Value::Number(n) => n
                    .as_i64()
                    .filter(|i| i.unsigned_abs() <= (1 << 53) - 1)
                    .map(PropertyValue::Int)
                    .ok_or_else(|| "Categories require JS-safe integers".to_string()),
                _ => Err("Categories require strings or integers".into()),
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            column: Some(column.into()),
            values,
        })
    }
    fn validate(&self, source: &dyn PointSource) -> Result<Option<usize>, String> {
        match &self.column {
            None if self.values.is_empty() => Ok(None),
            Some(column) => {
                crate::categorical_cluster::CategorySchema::new(column, self.values.clone())?;
                let strings = self
                    .values
                    .iter()
                    .all(|v| matches!(v, PropertyValue::String(_)));
                let integers = self
                    .values
                    .iter()
                    .all(|v| matches!(v, PropertyValue::Int(_)));
                if !strings && !integers {
                    return Err("Category values must have one type".into());
                }
                source
                    .property_names()
                    .iter()
                    .position(|s| s == column)
                    .map(Some)
                    .ok_or_else(|| format!("Category column '{column}' not found"))
            }
            _ => Err("category and category_values must be supplied together".into()),
        }
    }
    fn code(&self, value: &PropertyValue) -> u8 {
        self.values
            .iter()
            .position(|v| match (v, value) {
                (PropertyValue::Int(a), PropertyValue::Double(b)) => *a as f64 == *b,
                _ => v == value,
            })
            .unwrap_or(self.values.len()) as u8
    }
    fn fields(&self) -> Vec<String> {
        match &self.column {
            None => Vec::new(),
            Some(column) => self
                .values
                .iter()
                .map(|v| match v {
                    PropertyValue::String(s) => format!("{column}:{s}"),
                    PropertyValue::Int(i) => format!("{column}:{i}"),
                    _ => unreachable!(),
                })
                .chain(std::iter::once(format!("{column}:_other")))
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct LevelAudit {
    pub zoom: u8,
    pub representatives: usize,
    pub singletons: usize,
    pub people: u64,
    pub categories: Vec<u64>,
    pub tiles: usize,
}
#[derive(Debug, Serialize)]
pub struct BuildAudit {
    pub points: usize,
    pub categories: Vec<u64>,
    pub levels: Vec<LevelAudit>,
    pub peak_kernel_array_bytes: usize,
}

/// Private owned scratch directory. No caller-selected path is ever removed.
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Result<Self, String> {
        let base = std::env::var_os("FREESTILER_TEMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        for _ in 0..16 {
            let path = base.join(format!(
                "freestiler_clusters_{}",
                pmtiles_writer::unique_suffix()
            ));
            match builder.create(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(format!("Cannot create cluster scratch: {e}")),
            }
        }
        Err("Cannot create unique cluster scratch directory".into())
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// Exact f64 originals + typed properties, indexed ON DISK by original ordinal.
// The index has sorted fixed-width (u64 ordinal, u64 offset) records. Sparse
// input cannot turn recovery into an N-element HashMap or a host frame.
struct Originals {
    index: File,
    data: File,
    count: u64,
}
fn io<T>(r: std::io::Result<T>) -> Result<T, String> {
    r.map_err(|e| e.to_string())
}
fn u64_read(f: &mut File) -> Result<u64, String> {
    let mut b = [0; 8];
    io(f.read_exact(&mut b))?;
    Ok(u64::from_le_bytes(b))
}
fn write_feature(w: &mut impl Write, feature: &Feature) -> Result<(), String> {
    let Geometry::Point(p) = &feature.geometry else {
        return Err("Clustering requires POINT geometries only".into());
    };
    io(w.write_all(&feature.id.unwrap_or(u64::MAX).to_le_bytes()))?;
    io(w.write_all(&p.x().to_le_bytes()))?;
    io(w.write_all(&p.y().to_le_bytes()))?;
    io(w.write_all(&(feature.properties.len() as u64).to_le_bytes()))?;
    for v in &feature.properties {
        match v {
            PropertyValue::Null => io(w.write_all(&[0]))?,
            PropertyValue::Bool(b) => io(w.write_all(&[1, u8::from(*b)]))?,
            PropertyValue::Int(n) => {
                io(w.write_all(&[2]))?;
                io(w.write_all(&n.to_le_bytes()))?;
            }
            PropertyValue::Double(n) => {
                io(w.write_all(&[3]))?;
                io(w.write_all(&n.to_le_bytes()))?;
            }
            PropertyValue::String(s) => {
                io(w.write_all(&[4]))?;
                io(w.write_all(&(s.len() as u64).to_le_bytes()))?;
                io(w.write_all(s.as_bytes()))?;
            }
        }
    }
    Ok(())
}
impl Originals {
    fn recover(source: &dyn PointSource, bits: &[u8], dir: &Scratch) -> Result<Self, String> {
        let idx = dir.0.join("originals.idx");
        let dat = dir.0.join("originals.bin");
        let mut index = BufWriter::new(io(OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&idx))?);
        let mut data = BufWriter::new(io(OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&dat))?);
        let mut row = 0usize;
        let mut count = 0u64;
        source.scan(&mut |feature| {
            if row >= source.len() {
                return Err("Point source grew during singleton recovery".into());
            }
            if bits[row / 8] & (1 << (row % 8)) != 0 {
                io(index.write_all(&(row as u64).to_le_bytes()))?;
                let offset = io(data.stream_position())?;
                io(index.write_all(&offset.to_le_bytes()))?;
                write_feature(&mut data, &feature)?;
                count += 1;
            }
            row += 1;
            Ok(())
        })?;
        if row != source.len() {
            return Err("Point source shrank during singleton recovery".into());
        }
        source.check_unchanged()?;
        io(index.flush())?;
        io(data.flush())?;
        Ok(Self {
            index: io(File::open(idx))?,
            data: io(File::open(dat))?,
            count,
        })
    }
    fn get(&mut self, ordinal: u32) -> Result<Feature, String> {
        let mut lo = 0;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            io(self.index.seek(SeekFrom::Start(mid * 16)))?;
            let key = u64_read(&mut self.index)?;
            if key < ordinal as u64 {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == self.count {
            return Err("Missing singleton original".into());
        }
        io(self.index.seek(SeekFrom::Start(lo * 16)))?;
        if u64_read(&mut self.index)? != ordinal as u64 {
            return Err("Missing singleton original".into());
        }
        let offset = u64_read(&mut self.index)?;
        io(self.data.seek(SeekFrom::Start(offset)))?;
        let id = u64_read(&mut self.data)?;
        let lon = f64::from_bits(u64_read(&mut self.data)?);
        let lat = f64::from_bits(u64_read(&mut self.data)?);
        let n = u64_read(&mut self.data)? as usize;
        let mut properties = Vec::with_capacity(n);
        for _ in 0..n {
            let mut tag = [0];
            io(self.data.read_exact(&mut tag))?;
            properties.push(match tag[0] {
                0 => PropertyValue::Null,
                1 => {
                    let mut b = [0];
                    io(self.data.read_exact(&mut b))?;
                    PropertyValue::Bool(b[0] != 0)
                }
                2 => PropertyValue::Int(u64_read(&mut self.data)? as i64),
                3 => PropertyValue::Double(f64::from_bits(u64_read(&mut self.data)?)),
                4 => {
                    let len = u64_read(&mut self.data)? as usize;
                    let mut b = vec![0; len];
                    io(self.data.read_exact(&mut b))?;
                    PropertyValue::String(String::from_utf8(b).map_err(|e| e.to_string())?)
                }
                _ => return Err("Corrupt singleton store".into()),
            });
        }
        Ok(Feature {
            id: if id == u64::MAX { None } else { Some(id) },
            geometry: Geometry::Point(Point::new(lon, lat)),
            properties,
        })
    }
}

fn unproject(x: f64, y: f64) -> Point<f64> {
    Point::new(
        (x - 0.5) * 360.,
        (360.
            * ((180. - y * 360.) * std::f64::consts::PI / 180.)
                .exp()
                .atan()
            / std::f64::consts::PI)
            - 90.,
    )
}
fn abbrev(n: u64) -> PropertyValue {
    if n >= 10_000 {
        PropertyValue::String(format!("{}k", (n as f64 / 1000.).round() as u64))
    } else if n >= 1000 {
        PropertyValue::String(format!("{}k", (n as f64 / 100.).round() / 10.))
    } else {
        PropertyValue::Int(n as i64)
    }
}

/// Write clustered zooms only. The existing dot archive is a separate source
/// above the cutoff in the national maps; no points are thinned here.
pub fn write_pmtiles(
    source: &dyn PointSource,
    output: &str,
    name: &str,
    options: Options,
    categories: &Categories,
    format: TileFormat,
    reporter: &dyn ProgressReporter,
) -> Result<BuildAudit, String> {
    write_pmtiles_inner(source, output, name, options, options.max_zoom, categories,
        format, true, reporter)
}

/// In-memory categorical clusters, followed by original points above the cutoff.
pub fn write_layer(
    layer: &LayerData,
    output: &str,
    options: Options,
    max_zoom: u8,
    categories: &Categories,
    format: TileFormat,
    generate_ids: bool,
    reporter: &dyn ProgressReporter,
) -> Result<BuildAudit, String> {
    write_pmtiles_inner(layer, output, &layer.name, options, max_zoom, categories,
        format, generate_ids, reporter)
}

fn write_pmtiles_inner(
    source: &dyn PointSource,
    output: &str,
    name: &str,
    options: Options,
    max_zoom: u8,
    categories: &Categories,
    format: TileFormat,
    generate_ids: bool,
    reporter: &dyn ProgressReporter,
) -> Result<BuildAudit, String> {
    options.validate()?;
    if max_zoom < options.max_zoom || max_zoom > 30 {
        return Err("Output max_zoom must be between cluster_maxzoom and 30".into());
    }
    if source.len() == 0 || name.is_empty() {
        return Err("Empty cluster source/layer".into());
    }
    let category_index = categories.validate(source)?;
    let generated = [
        "cluster",
        "cluster_id",
        "point_count",
        "point_count_abbreviated",
        "cluster_expansion_zoom",
    ];
    let counts = categories.fields();
    if source
        .property_names()
        .iter()
        .any(|s| generated.contains(&s.as_str()) || counts.contains(s))
    {
        return Err("Source properties collide with generated cluster fields".into());
    }
    let mut names = source.property_names().to_vec();
    let original_width = names.len();
    names.extend(generated.iter().map(|s| s.to_string()));
    names.extend(counts);
    let mut types = source.property_types().to_vec();
    // Abbreviations are a Number/String union upstream; omit from typed
    // metadata (unknown), rather than falsely claiming all are strings.
    types.extend(
        ["logical", "integer", "integer", "mixed", "integer"]
            .iter()
            .map(|s| s.to_string()),
    );
    types.extend(categories.fields().iter().map(|_| "integer".into()));
    let mut input = Input::new(categories.values.len().max(1))?;
    input.reserve_exact(source.len())?;
    let mut expected = vec![0u64; categories.values.len().max(1) + 1];
    let mut bounds = (180f64, 90f64, -180f64, -90f64);
    reporter.report(&format!(
        "  Loading {} ordered points into the compact index ...",
        source.len()
    ));
    source.scan(&mut |feature| {
        let Geometry::Point(p) = feature.geometry else {
            return Err(
                "Clustering requires POINT geometries only (MultiPoint is not supported)".into(),
            );
        };
        if feature.properties.len() != original_width {
            return Err("Point source property width changed".into());
        }
        let code = category_index
            .map(|i| categories.code(&feature.properties[i]))
            .unwrap_or(0);
        input.push(p.x(), p.y(), code)?;
        expected[code as usize] += 1;
        bounds.0 = bounds.0.min(p.x());
        bounds.1 = bounds.1.min(p.y());
        bounds.2 = bounds.2.max(p.x());
        bounds.3 = bounds.3.max(p.y());
        Ok(())
    })?;
    if input.len() != source.len() {
        return Err("Point source row count changed".into());
    }
    source.check_unchanged()?;
    let scratch = Scratch::new()?;
    let mut originals = None;
    let mut spool = TileSpool::new_in(&scratch.0)?;
    let mut levels = Vec::new();
    let tile_budget = std::env::var("FREESTILER_CLUSTER_TILE_BUDGET_MB")
        .ok()
        .map(|v| v.parse::<usize>())
        .transpose()
        .map_err(|_| "Invalid FREESTILER_CLUSTER_TILE_BUDGET_MB")?
        .unwrap_or(256)
        .checked_mul(1024 * 1024)
        .ok_or("Tile budget overflow")?;
    if tile_budget == 0 {
        return Err("Cluster tile budget must be positive".into());
    }
    reporter.report("  Building the global Supercluster hierarchy ...");
    let stats = supercluster::build_with_points(&input, options, max_zoom, |zoom, level| {
        let mut actual = vec![0u64; expected.len()];
        let mut total = 0;
        let mut singles = 0;
        for i in 0..level.len() {
            let n = level.node(i);
            total += n.count;
            singles += usize::from(n.source.is_some());
            for (k, v) in actual.iter_mut().enumerate() {
                *v += level.category_count(i, k);
            }
        }
        if total != input.len() as u64 || actual != expected {
            return Err("Cluster conservation check failed".into());
        }
        if originals.is_none() {
            reporter.report(&format!(
                "  Recovering {singles} exact singleton originals to disk ..."
            ));
            originals = Some(Originals::recover(
                source,
                &level.singleton_bitset()?,
                &scratch,
            )?);
        }
        let before = spool.len();
        emit_level(
            zoom,
            level,
            options.radius,
            name,
            &names,
            original_width,
            categories.column.is_some(),
            generate_ids,
            originals.as_mut().unwrap(),
            format,
            tile_budget,
            &mut spool,
        )?;
        reporter.report(&format!(
            "  Zoom {zoom}: {} representatives ({singles} singletons), {} points, {} tiles",
            level.len(),
            total,
            spool.len() - before
        ));
        levels.push(LevelAudit {
            zoom,
            representatives: level.len(),
            singletons: singles,
            people: total,
            categories: actual,
            tiles: spool.len() - before,
        });
        Ok(())
    })?;
    source.check_unchanged()?;
    reporter.report(&format!("  Writing {} cluster tiles ...", spool.len()));
    let provenance = serde_json::json!({"algorithm":"Supercluster 8.0.1 / KDBush 4.1.0 compact Rust port",
        "radius":options.radius,"extent":512,"min_points":options.min_points,"cluster_maxzoom":options.max_zoom,"input":source.provenance(),
        "people":input.len(),"category":categories.column,"category_totals":expected,
        "levels":levels,"clustering":"global ordered raw points; no preaggregation or thinning"});
    pmtiles_writer::write_pmtiles_from_spool_metadata(
        output,
        &mut spool,
        format,
        &[LayerMeta {
            name: name.into(),
            property_names: names,
            property_types: types,
            min_zoom: options.min_zoom,
            max_zoom,
            geometry_type: Some("Point".into()),
        }],
        options.min_zoom,
        max_zoom,
        bounds,
        Some(&provenance),
    )?;
    Ok(BuildAudit {
        points: input.len(),
        categories: expected,
        levels,
        peak_kernel_array_bytes: stats.peak_accounted_bytes,
    })
}

fn emit_level(
    z: u8,
    level: &Level<'_>,
    radius: f64,
    name: &str,
    names: &[String],
    original_width: usize,
    categorical: bool,
    generate_ids: bool,
    originals: &mut Originals,
    format: TileFormat,
    budget: usize,
    spool: &mut TileSpool,
) -> Result<(), String> {
    let n = 1i64 << z;
    let nf = n as f64;
    let pad = radius / 512.;
    // Enumerate occupied padded tiles from Float32 KD coordinates. Original
    // singleton positions are deliberately NOT used for index selection.
    let mut coords = BTreeSet::new();
    for i in 0..level.len() {
        let p = level.node(i);
        let x = p.x as f32 as f64;
        let y = p.y as f32 as f64;
        for tx in
            (((x * nf - pad).ceil() as i64 - 1).max(-1))..=((x * nf + pad).floor() as i64).min(n)
        {
            for ty in ((y * nf - pad).ceil() as i64 - 1).max(0)
                ..=((y * nf + pad).floor() as i64).min(n - 1)
            {
                coords.insert((tx.rem_euclid(n) as u32, ty as u32));
                if coords.len().saturating_mul(64) > budget {
                    return Err("Cluster tile-coordinate index exceeds FREESTILER_CLUSTER_TILE_BUDGET_MB; reduce the zoom range or increase that budget".into());
                }
            }
        }
    }
    for (x, y) in coords {
        let coord = TileCoord { z, x, y };
        let mut features = Vec::new();
        let mut bytes = 0;
        let mut add = |i: u32, wrap: f64| -> Result<(), String> {
            let p = level.node(i as usize);
            let mut f = if let Some(row) = p.source {
                originals.get(row)?
            } else {
                Feature {
                    id: Some(p.id),
                    geometry: Geometry::Point(unproject(p.x, p.y)),
                    properties: vec![PropertyValue::Null; original_width],
                }
            };
            if wrap != 0. {
                if let Geometry::Point(point) = &mut f.geometry {
                    point.set_x(point.x() + wrap * 360.);
                }
            }
            if !generate_ids {
                f.id = None;
            }
            if p.source.is_some() {
                f.properties
                    .extend(std::iter::repeat(PropertyValue::Null).take(5));
            } else {
                f.properties.extend([
                    PropertyValue::Bool(true),
                    PropertyValue::Int(p.id as i64),
                    PropertyValue::Int(p.count as i64),
                    abbrev(p.count),
                    PropertyValue::Int(p.expansion_zoom.unwrap() as i64),
                ]);
            }
            if categorical {
                f.properties.extend(
                    (0..level.category_width())
                        .map(|k| PropertyValue::Int(level.category_count(i as usize, k) as i64)),
                );
            }
            bytes += std::mem::size_of::<Feature>()
                + f.properties.capacity() * std::mem::size_of::<PropertyValue>()
                + f.properties
                    .iter()
                    .map(|v| {
                        if let PropertyValue::String(s) = v {
                            s.len()
                        } else {
                            0
                        }
                    })
                    .sum::<usize>();
            if bytes > budget {
                return Err(format!("Cluster tile {z}/{x}/{y} exceeds FREESTILER_CLUSTER_TILE_BUDGET_MB before encoding"));
            }
            features.push(f);
            Ok(())
        };
        let mut error = None;
        let mut query = |bounds, wrap| {
            level.range(bounds, |i| {
                if error.is_none() {
                    if let Err(e) = add(i, wrap) {
                        error = Some(e);
                    }
                }
            });
        };
        query(
            [
                (x as f64 - pad) / nf,
                (y as f64 - pad) / nf,
                (x as f64 + 1. + pad) / nf,
                (y as f64 + 1. + pad) / nf,
            ],
            0.,
        );
        if x == 0 {
            query(
                [
                    (nf - pad) / nf,
                    (y as f64 - pad) / nf,
                    1.,
                    (y as f64 + 1. + pad) / nf,
                ],
                -1.,
            );
        }
        if x as i64 == n - 1 {
            query(
                [
                    0.,
                    (y as f64 - pad) / nf,
                    pad / nf,
                    (y as f64 + 1. + pad) / nf,
                ],
                1.,
            );
        }
        if let Some(e) = error {
            return Err(e);
        }
        if !features.is_empty() {
            let layers = [(name, names, features.as_slice())];
            let bytes = match format {
                TileFormat::Mvt => mvt::encode_tile_multilayer(&coord, &layers),
                TileFormat::Mlt => mlt::encode_tile_multilayer(&coord, &layers),
            };
            spool.write_tile(coord, &bytes)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::SilentReporter;
    use flate2::read::GzDecoder;
    use pmtiles2::PMTiles;
    use prost::Message;
    use std::collections::HashMap;
    struct Source {
        features: Vec<Feature>,
        names: Vec<String>,
        types: Vec<String>,
    }
    impl PointSource for Source {
        fn len(&self) -> usize {
            self.features.len()
        }
        fn property_names(&self) -> &[String] {
            &self.names
        }
        fn property_types(&self) -> &[String] {
            &self.types
        }
        fn scan(&self, visit: &mut dyn FnMut(Feature) -> Result<(), String>) -> Result<(), String> {
            for f in &self.features {
                visit(f.clone())?;
            }
            Ok(())
        }
        fn check_unchanged(&self) -> Result<(), String> {
            Ok(())
        }
    }
    fn source() -> Source {
        let features = [
            (-97., 32., 1),
            (-97.00001, 32.00001, 2),
            (179.999, 0., 1),
            (-179.999, 0., 2),
        ]
        .iter()
        .enumerate()
        .map(|(i, &(x, y, k))| Feature {
            id: Some(9000 + i as u64),
            geometry: Geometry::Point(Point::new(x, y)),
            properties: vec![
                PropertyValue::Int(k),
                PropertyValue::String(format!("original-{i}")),
            ],
        })
        .collect();
        Source {
            features,
            names: vec!["group".into(), "label".into()],
            types: vec!["integer".into(), "character".into()],
        }
    }
    fn categories() -> Categories {
        Categories {
            column: Some("group".into()),
            values: vec![PropertyValue::Int(1), PropertyValue::Int(2)],
        }
    }
    #[test]
    fn in_memory_clusters_and_raw_points_share_an_archive() {
        let mut s = source();
        // R numeric and pandas float columns must match integer dictionaries.
        s.features[0].properties[0] = PropertyValue::Double(1.0);
        s.features[1].properties[0] = PropertyValue::Double(2.0);
        s.features[2].properties[0] = PropertyValue::Null;
        s.features[3].properties[0] = PropertyValue::Int(99);
        let layer = LayerData {
            name: "plants".into(), features: s.features, prop_names: s.names,
            prop_types: s.types, min_zoom: 0, max_zoom: 3,
        };
        let dir = Scratch::new().unwrap();
        for generate_ids in [true, false] {
            let path = dir.0.join(format!("memory-{generate_ids}.pmtiles"));
            let audit = write_layer(&layer, path.to_str().unwrap(), Options {
                min_zoom: 0, max_zoom: 1, radius: 60., ..Default::default()
            }, 3, &categories(), TileFormat::Mvt, generate_ids, &SilentReporter).unwrap();
            assert_eq!(audit.categories, vec![1, 1, 2]);
            assert_eq!(audit.levels.len(), 4);
            assert!(audit.levels.iter().all(|l| l.people == 4 && l.categories == vec![1, 1, 2]));
            let mut archive = PMTiles::from_reader(File::open(path).unwrap()).unwrap();
            let ids: Vec<_> = archive.tile_ids().into_iter().copied().collect();
            let mut raw_labels = BTreeSet::new();
            let mut saw_cluster = false;
            for id in ids {
                let mut z = 0u8;
                while z < 30 && id >= ((1u64 << (2 * (z + 1))) - 1) / 3 { z += 1; }
                let compressed = archive.get_tile_by_id(id).unwrap().unwrap();
                let mut data = Vec::new();
                GzDecoder::new(&compressed[..]).read_to_end(&mut data).unwrap();
                let tile = mvt::Tile::decode(&data[..]).unwrap();
                let encoded = &tile.layers[0];
                for f in &encoded.features {
                    assert_eq!(f.id.is_some(), generate_ids);
                    let props: HashMap<_, _> = f.tags.chunks_exact(2).map(|t|
                        (encoded.keys[t[0] as usize].as_str(), &encoded.values[t[1] as usize])
                    ).collect();
                    let total: i64 = ["group:1", "group:2", "group:_other"].iter()
                        .map(|key| props[key].int_value.unwrap()).sum();
                    if z > 1 {
                        assert!(!props.contains_key("cluster"));
                        assert!(!props.contains_key("point_count"));
                        assert_eq!(total, 1);
                        raw_labels.insert((z, props["label"].string_value.clone().unwrap()));
                    } else if props.contains_key("cluster") {
                        saw_cluster = true;
                        assert_eq!(props["point_count"].int_value.unwrap(), total);
                        assert_eq!(props["cluster_expansion_zoom"].int_value.unwrap(), 2);
                    }
                }
            }
            assert!(saw_cluster);
            assert_eq!(raw_labels.len(), 8); // Four original plants at both raw zooms.
        }
    }

    #[test]
    fn scratch_directories_are_private_and_unique_for_parallel_callers() {
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    (0..32).map(|_| Scratch::new().unwrap()).collect::<Vec<_>>()
                })
            })
            .collect();
        let dirs: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let paths: BTreeSet<_> = dirs.iter().map(|d| d.0.clone()).collect();
        assert_eq!(paths.len(), 256);
        #[cfg(unix)]
        for path in &paths {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        drop(dirs);
        assert!(paths.iter().all(|p| !p.exists()));
    }
    #[test]
    fn disk_originals_preserve_exact_coordinates_ids_and_properties() {
        let s = source();
        let dir = Scratch::new().unwrap();
        let mut originals = Originals::recover(&s, &[0b1101], &dir).unwrap();
        for i in [0, 2, 3] {
            let f = originals.get(i).unwrap();
            assert_eq!(f.id, s.features[i as usize].id);
            assert_eq!(f.properties, s.features[i as usize].properties);
            let Geometry::Point(p) = f.geometry else {
                panic!()
            };
            let Geometry::Point(q) = s.features[i as usize].geometry else {
                panic!()
            };
            assert_eq!(p.x().to_bits(), q.x().to_bits());
            assert_eq!(p.y().to_bits(), q.y().to_bits());
        }
        assert!(originals.get(1).is_err());
    }
    #[test]
    fn decoded_tiles_conserve_counts_and_wrap_singletons_without_seed_properties() {
        let s = source();
        let dir = Scratch::new().unwrap();
        let path = dir.0.join("test.pmtiles");
        let audit = write_pmtiles(
            &s,
            path.to_str().unwrap(),
            "people",
            Options {
                min_zoom: 0,
                max_zoom: 3,
                radius: 60.,
                ..Default::default()
            },
            &categories(),
            TileFormat::Mvt,
            &SilentReporter,
        )
        .unwrap();
        assert_eq!(audit.points, 4);
        let mut archive = PMTiles::from_reader(File::open(path).unwrap()).unwrap();
        let ids: Vec<_> = archive.tile_ids().into_iter().copied().collect();
        let mut seen: HashMap<(u8, u64), Vec<u64>> = HashMap::new();
        let mut copies = HashMap::new();
        for tile_id in ids {
            let mut z = 0u8;
            while z < 30 && tile_id >= ((1u64 << (2 * (z + 1))) - 1) / 3 {
                z += 1;
            }
            let compressed = archive.get_tile_by_id(tile_id).unwrap().unwrap();
            let mut data = Vec::new();
            GzDecoder::new(&compressed[..])
                .read_to_end(&mut data)
                .unwrap();
            let tile = mvt::Tile::decode(&data[..]).unwrap();
            assert_eq!(tile.layers.len(), 1);
            let layer = &tile.layers[0];
            assert_eq!(layer.name, "people");
            for f in &layer.features {
                let props: HashMap<_, _> = f
                    .tags
                    .chunks_exact(2)
                    .map(|t| {
                        (
                            layer.keys[t[0] as usize].as_str(),
                            &layer.values[t[1] as usize],
                        )
                    })
                    .collect();
                let get = |key: &str| props.get(key).and_then(|v| v.int_value).unwrap() as u64;
                let counts = vec![get("group:1"), get("group:2"), get("group:_other")];
                let n: u64 = counts.iter().sum();
                if n > 1 {
                    assert!(!props.contains_key("group"));
                    assert!(!props.contains_key("label"));
                    assert_eq!(get("point_count"), n);
                    assert_eq!(get("cluster_id"), f.id.unwrap());
                    assert!(get("cluster_expansion_zoom") > z as u64);
                } else {
                    assert!(!props.contains_key("point_count"));
                    assert!(props.contains_key("label"));
                    assert!(props.contains_key("group"));
                }
                let key = (z, f.id.unwrap());
                if let Some(prior) = seen.insert(key, counts.clone()) {
                    assert_eq!(prior, counts);
                }
                *copies.entry(key).or_insert(0) += 1;
            }
        }
        for z in 0..=3 {
            let mut sums = vec![0; 3];
            for ((level, _), c) in &seen {
                if *level == z {
                    for (k, n) in c.iter().enumerate() {
                        sums[k] += n;
                    }
                }
            }
            assert_eq!(sums, vec![2, 2, 0]);
        }
        assert!(
            copies[&(3, 9002)] >= 2,
            "east singleton must wrap into west tile"
        );
        assert!(
            copies[&(3, 9003)] >= 2,
            "west singleton must wrap into east tile"
        );
    }
    #[test]
    fn invalid_category_and_multipoint_do_not_replace_existing_output() {
        let mut s = source();
        let dir = Scratch::new().unwrap();
        let path = dir.0.join("keep.pmtiles");
        fs::write(&path, b"existing").unwrap();
        s.features[0].geometry =
            Geometry::MultiPoint(geo_types::MultiPoint::new(vec![Point::new(0., 0.)]));
        let e = write_pmtiles(
            &s,
            path.to_str().unwrap(),
            "people",
            Options::default(),
            &categories(),
            TileFormat::Mvt,
            &SilentReporter,
        )
        .unwrap_err();
        assert!(e.contains("POINT geometries only"));
        assert_eq!(fs::read(path).unwrap(), b"existing");
        assert!(Categories::from_json("g", "[1.2]").is_err());
    }
}
