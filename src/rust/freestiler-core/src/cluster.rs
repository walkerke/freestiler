use std::collections::HashMap;

use geo_types::Point;

use crate::tiler::{Feature, Geometry, PropertyValue};

/// Configuration for point clustering
pub struct ClusterConfig {
    /// Clustering radius in display pixels
    pub distance: f64,
    /// Maximum zoom level for clustering (clusters dissolve above this)
    pub max_zoom: u8,
}

/// Pre-compute clustered point features for each zoom level.
/// Returns a map from zoom → Vec<Feature> of clustered/unclustered points.
///
/// Algorithm: hierarchical greedy (supercluster-style)
/// - Start at max_zoom with individual points
/// - At each zoom, grid-based spatial index with cell size = 2 × radius_deg
/// - Greedy: pick unvisited point, find neighbors within radius, form cluster
/// - Cluster = centroid geometry + point_count property + seed point properties
/// - Output becomes input for next lower zoom
pub fn cluster_points(
    features: &[Feature],
    config: &ClusterConfig,
    min_zoom: u8,
    n_original_props: usize,
) -> HashMap<u8, Vec<Feature>> {
    let mut results: HashMap<u8, Vec<Feature>> = HashMap::new();

    // Extract point features only
    let point_features: Vec<&Feature> = features
        .iter()
        .filter(|f| matches!(&f.geometry, Geometry::Point(_) | Geometry::MultiPoint(_)))
        .collect();

    if point_features.is_empty() {
        return results;
    }

    // Start at max_zoom with individual points (extended with point_count=Null)
    let mut current_features: Vec<Feature> = point_features
        .iter()
        .map(|f| {
            let mut props = f.properties.clone();
            // Extend properties to include point_count slot (Null for individual points)
            while props.len() < n_original_props {
                props.push(PropertyValue::Null);
            }
            props.push(PropertyValue::Null); // point_count slot
            Feature {
                id: f.id,
                geometry: f.geometry.clone(),
                properties: props,
            }
        })
        .collect();

    // Process from max_zoom down to min_zoom
    let start_zoom = config.max_zoom;
    for zoom in (min_zoom..=start_zoom).rev() {
        let radius_deg = pixel_radius_to_deg(config.distance, zoom);
        let cell_size = radius_deg * 2.0;

        let clustered = cluster_at_zoom(&current_features, radius_deg, cell_size, n_original_props);
        results.insert(zoom, clustered.clone());
        current_features = clustered;
    }

    results
}

/// Cluster features at a single zoom level using grid-based spatial index.
fn cluster_at_zoom(
    features: &[Feature],
    radius_deg: f64,
    cell_size: f64,
    n_original_props: usize,
) -> Vec<Feature> {
    if features.is_empty() {
        return Vec::new();
    }

    let radius_sq = radius_deg * radius_deg;

    // Build grid-based spatial index
    let mut grid: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
    let coords: Vec<(f64, f64)> = features.iter().map(|f| point_coords(&f.geometry)).collect();

    for (i, &(lon, lat)) in coords.iter().enumerate() {
        let cx = (lon / cell_size).floor() as i64;
        let cy = (lat / cell_size).floor() as i64;
        grid.entry((cx, cy)).or_default().push(i);
    }

    let mut visited = vec![false; features.len()];
    let mut result: Vec<Feature> = Vec::new();

    for i in 0..features.len() {
        if visited[i] {
            continue;
        }
        visited[i] = true;

        let (lon_i, lat_i) = coords[i];
        let cx = (lon_i / cell_size).floor() as i64;
        let cy = (lat_i / cell_size).floor() as i64;

        // Find neighbors within radius (check 3×3 grid cells)
        let mut neighbor_indices: Vec<usize> = vec![i];
        for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(cell_indices) = grid.get(&(cx + dx, cy + dy)) {
                    for &j in cell_indices {
                        if j == i || visited[j] {
                            continue;
                        }
                        let (lon_j, lat_j) = coords[j];
                        let dlat = lat_j - lat_i;
                        let dlon = lon_j - lon_i;
                        if dlon * dlon + dlat * dlat <= radius_sq {
                            visited[j] = true;
                            neighbor_indices.push(j);
                        }
                    }
                }
            }
        }

        if neighbor_indices.len() == 1 {
            // Single point, pass through
            result.push(features[i].clone());
        } else {
            // Form cluster: compute centroid and total point_count
            let mut sum_lon = 0.0_f64;
            let mut sum_lat = 0.0_f64;
            let mut total_count: i64 = 0;

            for &idx in &neighbor_indices {
                let (lon, lat) = coords[idx];
                // Get existing point_count (from prior zoom's clustering), or 1
                let count = get_point_count(&features[idx], n_original_props);
                sum_lon += lon * count as f64;
                sum_lat += lat * count as f64;
                total_count += count;
            }

            let centroid_lon = sum_lon / total_count as f64;
            let centroid_lat = sum_lat / total_count as f64;

            // Use seed point's properties, but set point_count
            let mut props = features[neighbor_indices[0]].properties.clone();
            // The last property is point_count
            if let Some(last) = props.last_mut() {
                *last = PropertyValue::Int(total_count);
            }

            result.push(Feature {
                id: features[neighbor_indices[0]].id,
                geometry: Geometry::Point(Point::new(centroid_lon, centroid_lat)),
                properties: props,
            });
        }
    }

    result
}

/// Get the point coordinates from a geometry (for clustering)
fn point_coords(geom: &Geometry) -> (f64, f64) {
    match geom {
        Geometry::Point(p) => (p.x(), p.y()),
        Geometry::MultiPoint(mp) => {
            if mp.0.is_empty() {
                (0.0, 0.0)
            } else {
                (mp.0[0].x(), mp.0[0].y())
            }
        }
        _ => (0.0, 0.0),
    }
}

/// Get the point_count from a feature's properties.
/// Returns 1 if not set (individual point).
fn get_point_count(feature: &Feature, n_original_props: usize) -> i64 {
    if feature.properties.len() > n_original_props {
        match &feature.properties[n_original_props] {
            PropertyValue::Int(count) => *count,
            _ => 1,
        }
    } else {
        1
    }
}

/// Convert a pixel radius to degrees at a given zoom level.
/// At zoom z, each tile covers 360/2^z degrees of longitude.
/// Each tile is 256 display pixels, so 1 pixel = 360/(256 × 2^z) degrees.
fn pixel_radius_to_deg(radius_px: f64, zoom: u8) -> f64 {
    radius_px * 360.0 / (256.0 * (1u64 << zoom) as f64)
}
