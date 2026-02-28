use geo::SimplifyVw;
use geo_types::{Coord, LineString, MultiLineString, MultiPolygon, Point, Polygon};

use crate::tiler::{Geometry, TileCoord, tile_bounds};

/// Tile coordinate extent (pixels per tile side)
const EXTENT: f64 = 4096.0;

/// Simplify a geometry for a specific tile by snapping coordinates to the tile
/// pixel grid and removing consecutive duplicate vertices.
///
/// This approach:
/// - Prevents slivers between adjacent features (shared vertices snap to the same pixel)
/// - Provides zoom-adaptive simplification (coarser grid at lower zoom levels naturally
///   collapses more vertices)
/// - Is extremely fast (just rounding + deduplication)
pub fn simplify_geometry(geom: &Geometry, coord: &TileCoord) -> Geometry {
    let bounds = tile_bounds(coord);
    let west = bounds.min().x;
    let south = bounds.min().y;
    let east = bounds.max().x;
    let north = bounds.max().y;

    snap_geometry(geom, west, south, east, north)
}

fn snap_geometry(
    geom: &Geometry,
    west: f64,
    south: f64,
    east: f64,
    north: f64,
) -> Geometry {
    // Precompute Mercator Y values for the tile bounds
    let south_merc = south.to_radians().tan().asinh();
    let north_merc = north.to_radians().tan().asinh();

    match geom {
        Geometry::Point(p) => {
            Geometry::Point(Point(snap_coord(&p.0, west, south_merc, east, north_merc)))
        }
        Geometry::MultiPoint(mp) => {
            let points = mp
                .0
                .iter()
                .map(|p| Point(snap_coord(&p.0, west, south_merc, east, north_merc)))
                .collect();
            Geometry::MultiPoint(geo_types::MultiPoint(points))
        }
        Geometry::LineString(ls) => {
            let snapped = snap_linestring(ls, west, south_merc, east, north_merc);
            if snapped.0.len() >= 2 {
                Geometry::LineString(snapped)
            } else {
                geom.clone()
            }
        }
        Geometry::MultiLineString(mls) => {
            let lines: Vec<LineString<f64>> = mls
                .0
                .iter()
                .map(|ls| snap_linestring(ls, west, south_merc, east, north_merc))
                .filter(|ls| ls.0.len() >= 2)
                .collect();
            if lines.is_empty() {
                geom.clone()
            } else {
                Geometry::MultiLineString(MultiLineString(lines))
            }
        }
        Geometry::Polygon(poly) => {
            let snapped = snap_polygon(poly, west, south_merc, east, north_merc);
            if is_valid_polygon(&snapped) {
                Geometry::Polygon(snapped)
            } else {
                geom.clone()
            }
        }
        Geometry::MultiPolygon(mp) => {
            let polys: Vec<Polygon<f64>> = mp
                .0
                .iter()
                .map(|p| snap_polygon(p, west, south_merc, east, north_merc))
                .filter(|p| is_valid_polygon(p))
                .collect();
            if polys.is_empty() {
                geom.clone()
            } else {
                Geometry::MultiPolygon(MultiPolygon(polys))
            }
        }
    }
}

/// Snap a coordinate to the nearest tile pixel position in Mercator space.
#[inline]
fn snap_coord(c: &Coord<f64>, west: f64, south_merc: f64, east: f64, north_merc: f64) -> Coord<f64> {
    let px = ((c.x - west) / (east - west) * EXTENT).round();
    let lat_merc = c.y.to_radians().tan().asinh();
    let py = ((north_merc - lat_merc) / (north_merc - south_merc) * EXTENT).round();
    Coord {
        x: west + px / EXTENT * (east - west),
        y: (north_merc - py / EXTENT * (north_merc - south_merc)).sinh().atan().to_degrees(),
    }
}

/// Snap linestring coordinates to the tile pixel grid (Mercator-aware),
/// removing consecutive duplicate vertices (points that map to the same pixel).
fn snap_linestring(
    ls: &LineString<f64>,
    west: f64,
    south_merc: f64,
    east: f64,
    north_merc: f64,
) -> LineString<f64> {
    let merc_range = north_merc - south_merc;
    let mut coords = Vec::with_capacity(ls.0.len());
    let mut prev_px = i32::MIN;
    let mut prev_py = i32::MIN;

    for c in &ls.0 {
        let px = ((c.x - west) / (east - west) * EXTENT).round() as i32;
        let lat_merc = c.y.to_radians().tan().asinh();
        let py = ((north_merc - lat_merc) / merc_range * EXTENT).round() as i32;

        if px == prev_px && py == prev_py {
            continue;
        }
        prev_px = px;
        prev_py = py;
        coords.push(Coord {
            x: west + px as f64 / EXTENT * (east - west),
            y: (north_merc - py as f64 / EXTENT * merc_range).sinh().atan().to_degrees(),
        });
    }

    LineString(coords)
}

/// Snap a polygon's rings to the tile pixel grid (Mercator-aware).
fn snap_polygon(
    poly: &Polygon<f64>,
    west: f64,
    south_merc: f64,
    east: f64,
    north_merc: f64,
) -> Polygon<f64> {
    let exterior = snap_ring(poly.exterior(), west, south_merc, east, north_merc);
    let interiors: Vec<LineString<f64>> = poly
        .interiors()
        .iter()
        .map(|ring| snap_ring(ring, west, south_merc, east, north_merc))
        .filter(|ring| ring.0.len() >= 4) // 3 unique vertices + closing point
        .collect();
    Polygon::new(exterior, interiors)
}

/// Snap a ring (closed linestring) to the tile pixel grid (Mercator-aware),
/// removing consecutive duplicates and preserving ring closure.
fn snap_ring(
    ring: &LineString<f64>,
    west: f64,
    south_merc: f64,
    east: f64,
    north_merc: f64,
) -> LineString<f64> {
    let merc_range = north_merc - south_merc;

    // Separate the closing point (rings have first == last)
    let source = if ring.0.len() >= 2 && ring.0.first() == ring.0.last() {
        &ring.0[..ring.0.len() - 1]
    } else {
        &ring.0[..]
    };

    let mut coords = Vec::with_capacity(source.len());
    let mut prev_px = i32::MIN;
    let mut prev_py = i32::MIN;

    for c in source {
        let px = ((c.x - west) / (east - west) * EXTENT).round() as i32;
        let lat_merc = c.y.to_radians().tan().asinh();
        let py = ((north_merc - lat_merc) / merc_range * EXTENT).round() as i32;

        if px == prev_px && py == prev_py {
            continue;
        }
        prev_px = px;
        prev_py = py;
        coords.push(Coord {
            x: west + px as f64 / EXTENT * (east - west),
            y: (north_merc - py as f64 / EXTENT * merc_range).sinh().atan().to_degrees(),
        });
    }

    // Check if last unique point duplicates first after snapping
    if coords.len() >= 2 {
        let first = &coords[0];
        let first_px = ((first.x - west) / (east - west) * EXTENT).round() as i32;
        let first_lat_merc = first.y.to_radians().tan().asinh();
        let first_py = ((north_merc - first_lat_merc) / merc_range * EXTENT).round() as i32;
        if first_px == prev_px && first_py == prev_py {
            coords.pop();
        }
    }

    // Close the ring
    if !coords.is_empty() {
        coords.push(coords[0]);
    }

    LineString(coords)
}

fn is_valid_polygon(poly: &Polygon<f64>) -> bool {
    poly.exterior().0.len() >= 4 // 3 unique vertices + closing point
}

// ---------------------------------------------------------------------------
// Visvalingam-Whyatt pre-simplification for line geometries
// ---------------------------------------------------------------------------

/// Compute VW area-based tolerance for a given zoom level.
/// This is ~8 pixel areas at the tile's resolution.
pub fn vw_tolerance_for_zoom(zoom: u8) -> f64 {
    let pixel_deg = 360.0 / ((1u64 << zoom) as f64 * 4096.0);
    pixel_deg * pixel_deg * 8.0
}

/// Pre-simplify line geometries using Visvalingam-Whyatt.
/// Only simplifies LineString and MultiLineString; returns clone for other types.
pub fn presimplify_line_vw(geom: &Geometry, tolerance: f64) -> Geometry {
    match geom {
        Geometry::LineString(ls) => {
            let simplified = ls.simplify_vw(&tolerance);
            if simplified.0.len() >= 2 {
                Geometry::LineString(simplified)
            } else {
                geom.clone()
            }
        }
        Geometry::MultiLineString(mls) => {
            let simplified = mls.simplify_vw(&tolerance);
            if simplified.0.is_empty() {
                geom.clone()
            } else {
                Geometry::MultiLineString(simplified)
            }
        }
        _ => geom.clone(),
    }
}
