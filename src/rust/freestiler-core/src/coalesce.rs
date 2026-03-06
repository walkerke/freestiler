use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use geo_types::{Coord, LineString, MultiLineString, MultiPolygon, Polygon};

use crate::tiler::{Feature, Geometry, PropertyValue};

/// Coalesce features within a tile: merge lines sharing endpoints and properties,
/// group polygons with identical properties into MultiPolygons.
/// Points pass through unchanged.
pub fn coalesce_features(features: Vec<Feature>, _prop_names: &[String]) -> Vec<Feature> {
    if features.len() <= 1 {
        return features;
    }

    let mut points: Vec<Feature> = Vec::new();
    let mut lines: Vec<Feature> = Vec::new();
    let mut polygons: Vec<Feature> = Vec::new();

    for f in features {
        match &f.geometry {
            Geometry::Point(_) | Geometry::MultiPoint(_) => points.push(f),
            Geometry::LineString(_) | Geometry::MultiLineString(_) => lines.push(f),
            Geometry::Polygon(_) | Geometry::MultiPolygon(_) => polygons.push(f),
        }
    }

    let mut result = points;
    result.extend(merge_lines(lines));
    result.extend(group_polygons(polygons));
    result
}

/// Hash property values for grouping features with identical attributes.
fn property_hash(props: &[PropertyValue]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for p in props {
        match p {
            PropertyValue::String(s) => {
                0u8.hash(&mut hasher);
                s.hash(&mut hasher);
            }
            PropertyValue::Int(i) => {
                1u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            PropertyValue::Double(d) => {
                2u8.hash(&mut hasher);
                d.to_bits().hash(&mut hasher);
            }
            PropertyValue::Bool(b) => {
                3u8.hash(&mut hasher);
                b.hash(&mut hasher);
            }
            PropertyValue::Null => {
                4u8.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

/// Merge line features that share properties and endpoints.
/// Uses an endpoint adjacency graph to chain connected line segments.
fn merge_lines(lines: Vec<Feature>) -> Vec<Feature> {
    if lines.is_empty() {
        return Vec::new();
    }

    // Group by property hash
    let mut groups: HashMap<u64, Vec<Feature>> = HashMap::new();
    for f in lines {
        let key = property_hash(&f.properties);
        groups.entry(key).or_default().push(f);
    }

    let mut result = Vec::new();
    for (_, group) in groups {
        result.extend(merge_line_group(group));
    }
    result
}

/// Merge a group of line features with identical properties.
fn merge_line_group(features: Vec<Feature>) -> Vec<Feature> {
    if features.len() <= 1 {
        return features;
    }

    let props = features[0].properties.clone();
    let id = features[0].id;

    // Extract all linestrings from features (flatten multi-linestrings)
    let mut all_lines: Vec<Vec<Coord<f64>>> = Vec::new();
    for f in &features {
        match &f.geometry {
            Geometry::LineString(ls) => {
                if ls.0.len() >= 2 {
                    all_lines.push(ls.0.clone());
                }
            }
            Geometry::MultiLineString(mls) => {
                for ls in &mls.0 {
                    if ls.0.len() >= 2 {
                        all_lines.push(ls.0.clone());
                    }
                }
            }
            _ => {}
        }
    }

    if all_lines.is_empty() {
        return Vec::new();
    }

    // Quantize coordinate for endpoint matching (1e-8 precision)
    fn quantize(c: &Coord<f64>) -> (i64, i64) {
        ((c.x * 1e8).round() as i64, (c.y * 1e8).round() as i64)
    }

    // Build endpoint adjacency: endpoint → list of (line_index, is_start)
    let mut endpoint_map: HashMap<(i64, i64), Vec<(usize, bool)>> = HashMap::new();
    for (i, line) in all_lines.iter().enumerate() {
        let start = quantize(&line[0]);
        let end = quantize(line.last().unwrap());
        endpoint_map.entry(start).or_default().push((i, true));
        endpoint_map.entry(end).or_default().push((i, false));
    }

    // Greedy chaining
    let mut used = vec![false; all_lines.len()];
    let mut merged_lines: Vec<Vec<Coord<f64>>> = Vec::new();

    for start_idx in 0..all_lines.len() {
        if used[start_idx] {
            continue;
        }
        used[start_idx] = true;

        let mut chain = all_lines[start_idx].clone();

        // Extend forward
        loop {
            let end_q = quantize(chain.last().unwrap());
            let mut found = false;
            if let Some(candidates) = endpoint_map.get(&end_q) {
                for &(idx, is_start) in candidates {
                    if used[idx] {
                        continue;
                    }
                    used[idx] = true;
                    found = true;
                    if is_start {
                        // Append line (skip shared first point)
                        chain.extend_from_slice(&all_lines[idx][1..]);
                    } else {
                        // Append reversed line (skip shared last point)
                        let mut rev = all_lines[idx].clone();
                        rev.reverse();
                        chain.extend_from_slice(&rev[1..]);
                    }
                    break;
                }
            }
            if !found {
                break;
            }
        }

        // Extend backward
        loop {
            let start_q = quantize(&chain[0]);
            let mut found = false;
            if let Some(candidates) = endpoint_map.get(&start_q) {
                for &(idx, is_start) in candidates {
                    if used[idx] {
                        continue;
                    }
                    used[idx] = true;
                    found = true;
                    if is_start {
                        // Prepend reversed line
                        let mut rev = all_lines[idx].clone();
                        rev.reverse();
                        rev.pop(); // remove shared endpoint
                        rev.append(&mut chain);
                        chain = rev;
                    } else {
                        // Prepend line
                        let mut prefix = all_lines[idx].clone();
                        prefix.pop(); // remove shared endpoint
                        prefix.append(&mut chain);
                        chain = prefix;
                    }
                    break;
                }
            }
            if !found {
                break;
            }
        }

        if chain.len() >= 2 {
            merged_lines.push(chain);
        }
    }

    // Build result features
    if merged_lines.is_empty() {
        return Vec::new();
    }

    if merged_lines.len() == 1 {
        vec![Feature {
            id,
            geometry: Geometry::LineString(LineString(merged_lines.into_iter().next().unwrap())),
            properties: props,
        }]
    } else {
        let multi = MultiLineString(merged_lines.into_iter().map(LineString).collect());
        vec![Feature {
            id,
            geometry: Geometry::MultiLineString(multi),
            properties: props,
        }]
    }
}

/// Group polygon features with identical properties into MultiPolygons.
fn group_polygons(polygons: Vec<Feature>) -> Vec<Feature> {
    if polygons.is_empty() {
        return Vec::new();
    }

    // Group by property hash
    let mut groups: HashMap<u64, Vec<Feature>> = HashMap::new();
    for f in polygons {
        let key = property_hash(&f.properties);
        groups.entry(key).or_default().push(f);
    }

    let mut result = Vec::new();
    for (_, group) in groups {
        if group.len() == 1 {
            result.push(group.into_iter().next().unwrap());
        } else {
            let props = group[0].properties.clone();
            let id = group[0].id;

            // Collect all polygons
            let mut all_polys: Vec<Polygon<f64>> = Vec::new();
            for f in group {
                match f.geometry {
                    Geometry::Polygon(p) => all_polys.push(p),
                    Geometry::MultiPolygon(mp) => all_polys.extend(mp.0),
                    _ => {}
                }
            }

            if all_polys.is_empty() {
                continue;
            }

            if all_polys.len() == 1 {
                result.push(Feature {
                    id,
                    geometry: Geometry::Polygon(all_polys.into_iter().next().unwrap()),
                    properties: props,
                });
            } else {
                result.push(Feature {
                    id,
                    geometry: Geometry::MultiPolygon(MultiPolygon(all_polys)),
                    properties: props,
                });
            }
        }
    }

    result
}
