//! `coalesce_features()` must emit its property groups in a stable order.
//! Groups used to come out in `HashMap` iteration order, which is seeded per
//! map, so `coalesce = TRUE` archives differed from run to run.

use freestiler_core::{
    coalesce::coalesce_features,
    geo_types::{LineString, Polygon},
    tiler::{Feature, Geometry, PropertyValue},
};

fn square(x: f64, y: f64) -> Geometry {
    let ring = LineString::from(vec![
        (x, y),
        (x + 1.0, y),
        (x + 1.0, y + 1.0),
        (x, y + 1.0),
        (x, y),
    ]);
    Geometry::Polygon(Polygon::new(ring, vec![]))
}

fn segment(x: f64, y: f64) -> Geometry {
    Geometry::LineString(LineString::from(vec![(x, y), (x + 1.0, y)]))
}

/// Interleaved features from eight property groups, first seen in the order
/// class 0, 1, ..., 7 for polygons and lines alike.
fn features() -> Vec<Feature> {
    let mut out = Vec::new();
    for k in 0..48u64 {
        let class = (k % 8) as i64;
        let column = (k / 8) as f64;
        out.push(Feature {
            id: Some(k + 1),
            geometry: square(column * 2.0, class as f64 * 2.0),
            properties: vec![PropertyValue::Int(class)],
        });
        out.push(Feature {
            id: Some(1000 + k),
            // Segments of one class touch end to end, so they merge.
            geometry: segment(column, 100.0 + class as f64),
            properties: vec![PropertyValue::Int(class)],
        });
    }
    out
}

fn classes(features: &[Feature]) -> Vec<(bool, i64)> {
    features
        .iter()
        .map(|f| {
            let is_line = matches!(
                f.geometry,
                Geometry::LineString(_) | Geometry::MultiLineString(_)
            );
            match f.properties[0] {
                PropertyValue::Int(class) => (is_line, class),
                _ => unreachable!(),
            }
        })
        .collect()
}

#[test]
fn coalesce_groups_in_first_appearance_order() {
    let names = vec!["class".to_string()];
    let first = coalesce_features(features(), &names);

    // Lines first (one merged LineString per class), then one MultiPolygon
    // per class, each in order of first appearance.
    let expected: Vec<(bool, i64)> = (0..8)
        .map(|c| (true, c))
        .chain((0..8).map(|c| (false, c)))
        .collect();
    assert_eq!(classes(&first), expected);
    assert!(first[..8]
        .iter()
        .all(|f| matches!(f.geometry, Geometry::LineString(_))));
    assert!(first[8..]
        .iter()
        .all(|f| matches!(f.geometry, Geometry::MultiPolygon(_))));

    // Each HashMap gets a fresh random seed, so repeated calls in one process
    // were enough to reorder the groups before the fix.
    for _ in 0..32 {
        let again = coalesce_features(features(), &names);
        assert_eq!(format!("{first:?}"), format!("{again:?}"));
    }
}
