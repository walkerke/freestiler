use crate::tiler::{Feature, Geometry, geometry_bbox, morton_code};

/// Compute Morton-code spatial indices for point features.
/// Returns (original_feature_index, morton_code) pairs sorted by morton code.
/// Non-point features are excluded.
pub fn compute_spatial_indices(features: &[Feature]) -> Vec<(usize, u64)> {
    let mut indexed: Vec<(usize, u64)> = features
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            let (lon, lat) = match &f.geometry {
                Geometry::Point(p) => (p.x(), p.y()),
                Geometry::MultiPoint(mp) => {
                    if mp.0.is_empty() {
                        return None;
                    }
                    // Use centroid of first point for ordering
                    (mp.0[0].x(), mp.0[0].y())
                }
                _ => return None,
            };
            // Normalize lon/lat to [0, 65535]
            let x = ((lon + 180.0) / 360.0 * 65536.0).clamp(0.0, 65535.0) as u32;
            let y = ((lat + 90.0) / 180.0 * 65536.0).clamp(0.0, 65535.0) as u32;
            Some((i, morton_code(x, y)))
        })
        .collect();
    indexed.sort_by_key(|&(_, h)| h);
    indexed
}

/// Compute a drop mask for features at a given zoom level.
/// Returns a Vec<bool> where `true` means "keep" and `false` means "drop".
///
/// - Polygons/lines: drop if bbox area < pixel_deg² × drop_rate^(max_zoom - zoom)
/// - Points: retain 1/drop_rate^(max_zoom - zoom) fraction, evenly spaced in Morton order
pub fn compute_drop_mask(
    features: &[Feature],
    spatial_indices: &[(usize, u64)],
    zoom: u8,
    max_zoom: u8,
    drop_rate: f64,
    pixel_deg: f64,
) -> Vec<bool> {
    let n = features.len();
    let mut mask = vec![true; n];

    if zoom >= max_zoom {
        return mask; // keep everything at max zoom
    }

    let zoom_diff = (max_zoom - zoom) as f64;
    let threshold = drop_rate.powf(zoom_diff);

    // Area-based dropping for polygons and lines
    let area_threshold = pixel_deg * pixel_deg * threshold;
    for (i, f) in features.iter().enumerate() {
        match &f.geometry {
            Geometry::Point(_) | Geometry::MultiPoint(_) => {} // handled below
            _ => {
                let bbox = geometry_bbox(&f.geometry);
                let w = bbox.max().x - bbox.min().x;
                let h = bbox.max().y - bbox.min().y;
                if w * h < area_threshold {
                    mask[i] = false;
                }
            }
        }
    }

    // Spatial-order thinning for points
    if !spatial_indices.is_empty() {
        let n_points = spatial_indices.len();
        let retain_count = ((n_points as f64) / threshold).ceil() as usize;
        let retain_count = retain_count.clamp(1, n_points);

        // First mark all points as dropped
        for &(idx, _) in spatial_indices {
            mask[idx] = false;
        }

        // Then retain evenly spaced points from the Morton-sorted order
        if retain_count >= n_points {
            for &(idx, _) in spatial_indices {
                mask[idx] = true;
            }
        } else {
            let step = n_points as f64 / retain_count as f64;
            for j in 0..retain_count {
                let pos = (j as f64 * step).floor() as usize;
                let pos = pos.min(n_points - 1);
                mask[spatial_indices[pos].0] = true;
            }
        }
    }

    mask
}
