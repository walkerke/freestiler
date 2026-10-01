//! Experimental weighted categorical clustering for precomputed tiles.
//!
//! Radius is in pixels relative to a 512-pixel world tile at z0, as in
//! Supercluster/MapLibre. Coordinates are normalized spherical Mercator, not
//! lon/lat degrees or ground meters. This deterministic greedy implementation
//! is NOT a claim of identical Supercluster memberships. In particular, callers
//! may provide preaggregated cells, whose constituents then move together.
//!
//! Inputs must be in strictly increasing canonical key order. Category counts
//! are packed row-major, with an explicit final `other` slot: no Feature/string
//! allocation per source point. Only the current/next levels and spatial index
//! are held; a callback consumes each completed level. This is not yet an
//! out-of-core spatial index or an R/Python public API.

use std::collections::{HashMap, HashSet};

use crate::tiler::{Feature, Geometry, LayerData, PropertyValue};
use geo_types::Point;

const EXTENT: f64 = 512.0;
const MAX_SAFE_COUNT: u64 = (1 << 53) - 1;
const MAX_LAT: f64 = 85.0511287798066;

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub key: u64,
    /// Generated, JS-safe identity. A cluster that passes through unchanged
    /// retains its identity; a new merge gets a new identity.
    pub id: u64,
    pub x: f64,
    pub y: f64,
    pub point_count: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PackedPoints {
    nodes: Vec<Node>,
    counts: Vec<u64>,
    width: usize,
    total: u64,
    next_id: u64,
}

impl PackedPoints {
    /// Number of known categories (1..=64), plus a separate other slot.
    pub fn new(categories: usize) -> Result<Self, String> {
        if !(1..=64).contains(&categories) {
            return Err("Expected between 1 and 64 categories".into());
        }
        Ok(Self {
            nodes: Vec::new(),
            counts: Vec::new(),
            width: categories + 1,
            total: 0,
            next_id: 1,
        })
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }
    pub fn counts(&self, index: usize) -> &[u64] {
        &self.counts[index * self.width..(index + 1) * self.width]
    }
    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn category_totals(&self) -> Vec<u64> {
        let mut result = vec![0; self.width];
        for row in self.counts.chunks_exact(self.width) {
            for (total, value) in result.iter_mut().zip(row) {
                *total += value;
            }
        }
        result
    }

    pub fn push_lonlat(
        &mut self,
        key: u64,
        lon: f64,
        lat: f64,
        counts: &[u64],
    ) -> Result<(), String> {
        let (x, y) = project(lon, lat)?;
        self.push_projected(key, x, y, counts)
    }

    /// For preaggregated inputs, x/y must be the population-weighted mean of
    /// the original projected coordinates, NOT the H3 cell center.
    pub fn push_projected(
        &mut self,
        key: u64,
        x: f64,
        y: f64,
        counts: &[u64],
    ) -> Result<(), String> {
        if !x.is_finite()
            || !y.is_finite()
            || !(0.0..=1.0).contains(&x)
            || !(0.0..=1.0).contains(&y)
        {
            return Err("Projected coordinates must be finite and in [0,1]".into());
        }
        if self.nodes.last().map_or(false, |n| key <= n.key) {
            return Err("Input keys must be unique and strictly increasing".into());
        }
        if counts.len() != self.width {
            return Err("Wrong category-count width (include other)".into());
        }
        let count = counts
            .iter()
            .try_fold(0u64, |a, b| a.checked_add(*b))
            .ok_or("Category count overflow")?;
        let total = self.total.checked_add(count).ok_or("Population overflow")?;
        if count == 0 || total > MAX_SAFE_COUNT {
            return Err("Population must be positive and the total at most 2^53-1".into());
        }
        self.nodes.try_reserve(1).map_err(|e| e.to_string())?;
        self.counts
            .try_reserve(self.width)
            .map_err(|e| e.to_string())?;
        self.nodes.push(Node {
            key,
            id: self.next_id,
            x,
            y,
            point_count: count,
        });
        self.next_id += 1;
        self.counts.extend_from_slice(counts);
        self.total = total;
        Ok(())
    }
}

pub fn project(lon: f64, lat: f64) -> Result<(f64, f64), String> {
    if !lon.is_finite() || !lat.is_finite() || lon.abs() > 180.0 || lat.abs() > MAX_LAT {
        return Err("Longitude/latitude outside the finite Web Mercator domain".into());
    }
    Ok((
        (lon + 180.0) / 360.0,
        ((1.0 - lat.to_radians().tan().asinh() / std::f64::consts::PI) / 2.0).clamp(0.0, 1.0),
    ))
}

pub fn unproject(x: f64, y: f64) -> (f64, f64) {
    (
        x * 360.0 - 180.0,
        ((1.0 - 2.0 * y) * std::f64::consts::PI)
            .sinh()
            .atan()
            .to_degrees(),
    )
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub radius_pixels: f64,
    pub min_zoom: u8,
    /// Inclusive: clusters at this integer tile zoom, raw points above it.
    pub max_zoom: u8,
}

impl Config {
    fn validate(&self) -> Result<(), String> {
        let smallest = self.radius_pixels / (EXTENT * 2f64.powi(self.max_zoom.into()));
        if self.min_zoom > self.max_zoom
            || self.max_zoom > 30
            || !self.radius_pixels.is_finite()
            || self.radius_pixels <= 0.0
            || smallest < 2f64.powi(-52)
        {
            return Err("Invalid clustering radius/zoom range".into());
        }
        Ok(())
    }
}

/// Canonical seed order and fixed grid traversal make output independent of
/// upstream batch/worker boundaries once the canonical inputs are fixed.
/// Euclidean normalized-Mercator distance has the usual dateline cut; this
/// does not add a wraparound-neighbor rule absent from Supercluster's metric.
fn cluster_level(
    input: &PackedPoints,
    radius: f64,
    trace: bool,
) -> Result<(PackedPoints, Vec<usize>), String> {
    let mut grid: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
    let cell = |n: &Node| ((n.x / radius).floor() as i64, (n.y / radius).floor() as i64);
    for (i, n) in input.nodes.iter().enumerate() {
        grid.entry(cell(n)).or_default().push(i);
    }
    let mut visited = vec![false; input.nodes.len()];
    let mut out = PackedPoints::new(input.width - 1)?;
    out.next_id = input.next_id;
    let mut category_sums = vec![0u64; input.width];
    let mut parents = if trace { vec![usize::MAX; input.nodes.len()] } else { Vec::new() };
    for (i, seed) in input.nodes.iter().enumerate() {
        if visited[i] {
            continue;
        }
        visited[i] = true;
        if trace { parents[i] = out.nodes.len(); }
        category_sums.copy_from_slice(input.counts(i));
        let mut weight = seed.point_count;
        let mut sx = seed.x * weight as f64;
        let mut sy = seed.y * weight as f64;
        let mut merged = false;
        let (cx, cy) = cell(seed);
        for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(neighbors) = grid.get(&(cx + dx, cy + dy)) {
                    for &j in neighbors {
                        if visited[j] {
                            continue;
                        }
                        let n = &input.nodes[j];
                        let d2 = (n.x - seed.x).powi(2) + (n.y - seed.y).powi(2);
                        if d2 > radius * radius {
                            continue;
                        }
                        visited[j] = true;
                        if trace { parents[j] = out.nodes.len(); }
                        merged = true;
                        // Every constituent appears once; the validated global
                        // population bound also bounds all partial sums.
                        weight += n.point_count;
                        sx += n.x * n.point_count as f64;
                        sy += n.y * n.point_count as f64;
                        for (sum, n) in category_sums.iter_mut().zip(input.counts(j)) {
                            *sum += n;
                        }
                    }
                }
            }
        }
        let id = if merged {
            if out.next_id > MAX_SAFE_COUNT {
                return Err("Cluster identity overflow".into());
            }
            let id = out.next_id;
            out.next_id += 1;
            id
        } else {
            seed.id
        };
        out.nodes.push(Node {
            key: seed.key,
            id,
            x: if merged { sx / weight as f64 } else { seed.x },
            y: if merged { sy / weight as f64 } else { seed.y },
            point_count: weight,
        });
        out.counts.extend_from_slice(&category_sums);
        out.total += weight;
    }
    debug_assert_eq!(out.total, input.total);
    Ok((out, parents))
}

/// Visit finest to coarsest. Callback errors stop immediately. Do not retain
/// full Feature copies of every zoom when integrating a national tile sink.
pub fn cluster_levels(
    points: PackedPoints,
    config: Config,
    mut emit: impl FnMut(u8, &PackedPoints) -> Result<(), String>,
) -> Result<(), String> {
    cluster_levels_impl(points, config, false, |z, p, _| emit(z, p))
}

/// Diagnostic/hierarchy hook. `parents[i]` is the output node index owning
/// previous-level node i, including unchanged singletons. This allocates one
/// temporary usize per previous-level node; ordinary cluster_levels does not.
/// Callers can compose the maps to compare memberships without guessing from
/// nearest cluster centers. Indices are local to the callback, not public IDs.
pub fn cluster_levels_traced(
    points: PackedPoints,
    config: Config,
    emit: impl FnMut(u8, &PackedPoints, &[usize]) -> Result<(), String>,
) -> Result<(), String> {
    cluster_levels_impl(points, config, true, emit)
}

fn cluster_levels_impl(
    mut points: PackedPoints,
    config: Config,
    trace: bool,
    mut emit: impl FnMut(u8, &PackedPoints, &[usize]) -> Result<(), String>,
) -> Result<(), String> {
    config.validate()?;
    for zoom in (config.min_zoom..=config.max_zoom).rev() {
        let (next, parents) = cluster_level(
            &points,
            config.radius_pixels / (EXTENT * 2f64.powi(zoom.into())),
            trace,
        )?;
        drop(points);
        emit(zoom, &next, &parents)?;
        points = next;
    }
    Ok(())
}

/// Typed category values and existing mapgl field names. No palette or donut
/// rendering behavior belongs in the tiler. `_other`/`_total` are reserved.
pub struct CategorySchema {
    column: String,
    values: Vec<PropertyValue>,
    keys: Vec<String>,
}

impl CategorySchema {
    pub fn new(column: &str, values: Vec<PropertyValue>) -> Result<Self, String> {
        if column.is_empty()
            || [
                "cluster",
                "cluster_id",
                "point_count",
                "point_count_abbreviated",
            ]
            .contains(&column)
            || !(1..=64).contains(&values.len())
        {
            return Err("Invalid categorical column or category count".into());
        }
        let keys: Vec<String> = values
            .iter()
            .map(|v| match v {
                PropertyValue::String(s) => Ok(s.clone()),
                PropertyValue::Int(i) if i.unsigned_abs() <= MAX_SAFE_COUNT => Ok(i.to_string()),
                _ => Err("Categories must be strings or JS-safe integers".to_string()),
            })
            .collect::<Result<_, _>>()?;
        let unique: HashSet<_> = keys.iter().collect();
        if unique.len() != keys.len() || keys.iter().any(|k| k == "_other" || k == "_total") {
            return Err("Duplicate or reserved category value".into());
        }
        Ok(Self {
            column: column.into(),
            values,
            keys,
        })
    }

    /// Materialize ONE level for the existing encoder (suitable for fixtures
    /// and pilots). A bounded national integration must page tile output.
    pub fn layer(&self, points: &PackedPoints, name: &str, zoom: u8) -> Result<LayerData, String> {
        if points.width != self.values.len() + 1 || name.is_empty() || zoom > 30 {
            return Err("Category schema/level mismatch".into());
        }
        let mut names = vec![
            "cluster".into(),
            "cluster_id".into(),
            "point_count".into(),
            "point_count_abbreviated".into(),
            self.column.clone(),
        ];
        let value_type = if self
            .values
            .iter()
            .all(|v| matches!(v, PropertyValue::Int(_)))
        {
            "integer"
        } else {
            "string"
        };
        // Mixed string/integer dictionaries would make the raw category's
        // metadata ambiguous; reject rather than changing values silently.
        if value_type == "string"
            && self
                .values
                .iter()
                .any(|v| !matches!(v, PropertyValue::String(_)))
        {
            return Err("Category dictionary must have one value type".into());
        }
        let mut types = vec![
            "logical".into(),
            "integer".into(),
            "integer".into(),
            "string".into(),
            value_type.into(),
        ];
        names.extend(self.keys.iter().map(|k| format!("{}:{k}", self.column)));
        names.push(format!("{}:_other", self.column));
        types.extend((0..points.width).map(|_| "integer".into()));
        let features = points
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                let counts = points.counts(i);
                let mut props = if n.point_count > 1 {
                    vec![
                        PropertyValue::Bool(true),
                        PropertyValue::Int(n.id as i64),
                        PropertyValue::Int(n.point_count as i64),
                        PropertyValue::String(abbreviate(n.point_count)),
                        PropertyValue::Null,
                    ]
                } else {
                    // A genuine raw singleton must NOT have point_count, so
                    // standard MapLibre/Mapbox unclustered filters keep working.
                    let value = counts
                        .iter()
                        .take(self.values.len())
                        .position(|&v| v == 1)
                        .map(|k| self.values[k].clone())
                        .unwrap_or(PropertyValue::Null);
                    vec![
                        PropertyValue::Null,
                        PropertyValue::Null,
                        PropertyValue::Null,
                        PropertyValue::Null,
                        value,
                    ]
                };
                props.extend(counts.iter().map(|&n| PropertyValue::Int(n as i64)));
                let (lon, lat) = unproject(n.x, n.y);
                Feature {
                    id: Some(n.id),
                    geometry: Geometry::Point(Point::new(lon, lat)),
                    properties: props,
                }
            })
            .collect();
        Ok(LayerData {
            name: name.into(),
            features,
            prop_names: names,
            prop_types: types,
            min_zoom: zoom,
            max_zoom: zoom,
        })
    }
}

fn abbreviate(n: u64) -> String {
    if n >= 10_000 {
        format!("{}k", (n as f64 / 1000.0).round() as u64)
    } else if n >= 1_000 {
        format!("{}k", (n as f64 / 100.0).round() / 10.0)
    } else {
        n.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{mvt, tiler::TileCoord};
    use prost::Message;

    fn cfg(z: u8) -> Config {
        Config {
            radius_pixels: 50.0,
            min_zoom: z,
            max_zoom: z,
        }
    }
    fn one(points: PackedPoints, z: u8) -> PackedPoints {
        let mut out = None;
        cluster_levels(points, cfg(z), |_, p| {
            out = Some(p.clone());
            Ok(())
        })
        .unwrap();
        out.unwrap()
    }
    #[test]
    fn counts_centers_and_unmerged_weights_survive_every_level() {
        let mut p = PackedPoints::new(2).unwrap();
        p.push_projected(1, 0.40, 0.4, &[100, 0, 0]).unwrap();
        p.push_projected(2, 0.41, 0.4, &[0, 1, 0]).unwrap();
        p.push_projected(3, 0.9, 0.8, &[0, 0, 7]).unwrap();
        let expect = p.category_totals();
        let mut levels = 0;
        cluster_levels(
            p,
            Config {
                radius_pixels: 50.0,
                min_zoom: 0,
                max_zoom: 2,
            },
            |z, p| {
                assert_eq!(p.category_totals(), expect);
                assert_eq!(p.total(), 108);
                assert_eq!(p.nodes.len(), 2);
                assert!((p.nodes[0].x - (0.4 * 100. + 0.41) / 101.).abs() < 1e-14);
                assert_eq!(p.nodes[1].point_count, 7);
                assert_eq!(p.nodes[0].key, 1);
                assert_eq!(z, 2 - levels);
                levels += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(levels, 3);
    }
    #[test]
    fn radius_is_pixels_at_512_extent_not_degrees_or_ground_meters() {
        for lat in [0., 60.] {
            let (x, y) = project(10., lat).unwrap();
            let scale = 512. * 2f64.powi(8);
            let mut p = PackedPoints::new(1).unwrap();
            p.push_projected(1, x, y, &[1, 0]).unwrap();
            p.push_projected(2, x, y + 49. / scale, &[1, 0]).unwrap();
            p.push_projected(3, x + 51. / scale, y, &[1, 0]).unwrap();
            let out = one(p, 8);
            assert_eq!(out.nodes.len(), 2);
            assert_eq!(out.nodes[0].point_count, 2);
        }
    }
    #[test]
    fn hierarchy_is_deterministic_for_identical_canonical_input() {
        let mut p = PackedPoints::new(2).unwrap();
        for i in 0..1000 {
            p.push_projected(
                i,
                0.3 + (i % 50) as f64 * 0.0001,
                0.4 + (i / 50) as f64 * 0.0001,
                &[1, 2, 0],
            )
            .unwrap();
        }
        assert_eq!(one(p.clone(), 8), one(p, 8));
    }
    #[test]
    fn parent_maps_cover_every_input_and_do_not_change_clustering() {
        let mut p = PackedPoints::new(2).unwrap();
        for i in 0..100 {
            p.push_projected(i, 0.2 + (i % 10) as f64 * 0.002,
                0.4 + (i / 10) as f64 * 0.002, &[i + 1, 2, 0]).unwrap();
        }
        let config = Config { min_zoom: 0, max_zoom: 6, radius_pixels: 60.0 };
        let mut expected = Vec::new();
        cluster_levels(p.clone(), config, |z, level| {
            expected.push((z, level.clone())); Ok(())
        }).unwrap();
        let mut previous = p.clone();
        let mut original_to_current: Vec<usize> = (0..p.nodes.len()).collect();
        let mut index = 0;
        cluster_levels_traced(p.clone(), config, |z, level, parents| {
            assert_eq!(&(z, level.clone()), &expected[index]);
            assert_eq!(parents.len(), previous.nodes.len());
            let mut counts = vec![vec![0; 3]; level.nodes.len()];
            for (i, &parent) in parents.iter().enumerate() {
                assert!(parent < level.nodes.len());
                for (sum, value) in counts[parent].iter_mut().zip(previous.counts(i)) { *sum += value; }
            }
            for (i, row) in counts.iter().enumerate() { assert_eq!(row, level.counts(i)); }
            for owner in &mut original_to_current { *owner = parents[*owner]; }
            let mut weights = vec![0; level.nodes.len()];
            for (i, &owner) in original_to_current.iter().enumerate() { weights[owner] += p.nodes[i].point_count; }
            assert_eq!(weights, level.nodes.iter().map(|n| n.point_count).collect::<Vec<_>>());
            previous = level.clone(); index += 1; Ok(())
        }).unwrap();
        assert_eq!(index, 7);
    }
    #[test]
    fn greedy_radius_is_not_transitive_connected_components() {
        let radius = 50.0 / 512.0;
        let mut p = PackedPoints::new(1).unwrap();
        for (i, step) in [0.0, 0.8, 1.6].iter().enumerate() {
            p.push_projected(i as u64, 0.25 + step * radius, 0.4, &[1, 0])
                .unwrap();
        }
        let out = one(p, 0);
        assert_eq!(out.nodes.len(), 2);
        assert_eq!(out.nodes[0].point_count, 2);
        assert_eq!(out.nodes[1].point_count, 1);
    }
    #[test]
    fn neighbors_across_spatial_index_cells_merge() {
        let radius = 50.0 / 512.0;
        let mut p = PackedPoints::new(1).unwrap();
        p.push_projected(1, radius * 4.99, radius * 4.99, &[2, 0])
            .unwrap();
        p.push_projected(2, radius * 5.01, radius * 5.01, &[3, 0])
            .unwrap();
        let out = one(p, 0);
        assert_eq!(out.nodes.len(), 1);
        assert_eq!(out.total(), 5);
    }
    #[test]
    fn merged_ids_do_not_collide_with_inputs_and_persist_until_next_merge() {
        let mut p = PackedPoints::new(1).unwrap();
        p.push_projected(100, 0.3, 0.4, &[1, 0]).unwrap();
        p.push_projected(200, 0.301, 0.4, &[1, 0]).unwrap();
        p.push_projected(300, 0.9, 0.8, &[1, 0]).unwrap();
        let mut merged_id = None;
        cluster_levels(
            p,
            Config {
                radius_pixels: 50.,
                min_zoom: 0,
                max_zoom: 2,
            },
            |_, p| {
                assert_eq!(p.nodes.len(), 2);
                assert!(p.nodes[0].id > 3);
                assert_eq!(p.nodes[1].id, 3);
                if let Some(id) = merged_id {
                    assert_eq!(p.nodes[0].id, id);
                }
                merged_id = Some(p.nodes[0].id);
                Ok(())
            },
        )
        .unwrap();
    }
    #[test]
    fn rejects_bad_input_without_partial_insert() {
        let mut p = PackedPoints::new(1).unwrap();
        assert!(p.push_projected(1, f64::NAN, 0.5, &[1, 0]).is_err());
        assert!(p.push_lonlat(1, 0., 90., &[1, 0]).is_err());
        assert!(p.push_projected(1, 0.5, 0.5, &[0, 0]).is_err());
        assert!(p.push_projected(1, 0.5, 0.5, &[u64::MAX, 1]).is_err());
        assert!(p.push_projected(1, 0.5, 0.5, &[1]).is_err());
        assert_eq!(p.nodes.len(), 0);
        p.push_projected(1, 0.5, 0.5, &[MAX_SAFE_COUNT, 0]).unwrap();
        assert!(p.push_projected(1, 0.5, 0.5, &[1, 0]).is_err());
        assert!(p.push_projected(2, 0.5, 0.5, &[1, 0]).is_err());
        assert_eq!(p.total(), MAX_SAFE_COUNT);
    }
    #[test]
    fn zoom_radius_and_callback_errors_are_explicit() {
        for c in [
            Config {
                radius_pixels: f64::NAN,
                ..cfg(1)
            },
            Config {
                radius_pixels: 0.,
                ..cfg(1)
            },
            Config {
                min_zoom: 3,
                ..cfg(1)
            },
            cfg(31),
        ] {
            assert!(cluster_levels(PackedPoints::new(1).unwrap(), c, |_, _| Ok(())).is_err());
        }
        assert_eq!(
            cluster_levels(PackedPoints::new(1).unwrap(), cfg(1), |_, _| Err(
                "cancelled".into()
            )),
            Err("cancelled".into())
        );
    }
    #[test]
    fn retains_supercluster_dateline_cut_and_roundtrips_projection() {
        let mut p = PackedPoints::new(1).unwrap();
        p.push_lonlat(1, -179.999, 0., &[1, 0]).unwrap();
        p.push_lonlat(2, 179.999, 0., &[1, 0]).unwrap();
        assert_eq!(one(p, 0).nodes.len(), 2);
        for (lon, lat) in [(0., 0.), (-97., 32.), (179., -80.), (-180., MAX_LAT)] {
            let (x, y) = project(lon, lat).unwrap();
            let (a, b) = unproject(x, y);
            assert!((a - lon).abs() < 1e-10);
            assert!((b - lat).abs() < 1e-10);
        }
    }
    #[test]
    fn mapgl_schema_and_standard_singleton_filters_survive_mvt_encoding() {
        let mut p = PackedPoints::new(2).unwrap();
        p.push_lonlat(1, -97., 32., &[80, 20, 0]).unwrap();
        p.push_lonlat(2, -90., 30., &[0, 1, 0]).unwrap();
        let schema = CategorySchema::new(
            "group_id",
            vec![PropertyValue::Int(1), PropertyValue::Int(2)],
        )
        .unwrap();
        let layer = schema.layer(&p, "clusters", 0).unwrap();
        let tile = mvt::encode_tile(
            &TileCoord { z: 0, x: 0, y: 0 },
            &layer.features,
            "clusters",
            &layer.prop_names,
        );
        let decoded = mvt::Tile::decode(tile.as_slice()).unwrap();
        let l = &decoded.layers[0];
        let props = |i: usize| -> HashMap<&str, &mvt::Value> {
            l.features[i]
                .tags
                .chunks_exact(2)
                .map(|v| (l.keys[v[0] as usize].as_str(), &l.values[v[1] as usize]))
                .collect()
        };
        let cluster = props(0);
        assert_eq!(cluster["point_count"].int_value, Some(100));
        assert_eq!(cluster["group_id:1"].int_value, Some(80));
        assert_eq!(cluster["group_id:2"].int_value, Some(20));
        assert_eq!(cluster["cluster"].bool_value, Some(true));
        assert!(!cluster.contains_key("group_id"));
        let singleton = props(1);
        assert!(!singleton.contains_key("point_count"));
        assert!(!singleton.contains_key("cluster_id"));
        assert_eq!(singleton["group_id"].int_value, Some(2));
    }
    #[test]
    fn rejects_ambiguous_dictionaries_and_preserves_other() {
        assert!(CategorySchema::new("point_count", vec![PropertyValue::Int(1)]).is_err());
        assert!(CategorySchema::new("g", vec![PropertyValue::String("_other".into())]).is_err());
        assert!(CategorySchema::new(
            "g",
            vec![PropertyValue::Int(1), PropertyValue::String("1".into())]
        )
        .is_err());
        let mut p = PackedPoints::new(1).unwrap();
        p.push_lonlat(1, 0., 0., &[0, 4]).unwrap();
        let s = CategorySchema::new("g", vec![PropertyValue::Int(1)]).unwrap();
        let l = s.layer(&p, "c", 0).unwrap();
        assert_eq!(l.features[0].properties[2], PropertyValue::Int(4));
        assert_eq!(
            l.features[0].properties.last(),
            Some(&PropertyValue::Int(4))
        );
    }
}
