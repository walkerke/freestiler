use geo_types::{
    Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, Rect,
};

use crate::tiler::{Geometry, TileCoord, tile_bounds};

/// Buffer factor as a fraction of tile extent (5% on each side)
const BUFFER_FRACTION: f64 = 0.05;

/// Clip a geometry to tile boundaries with a small buffer
pub fn clip_geometry_to_tile(geom: &Geometry, coord: &TileCoord) -> Option<Geometry> {
    let bounds = tile_bounds(coord);

    // Add a small buffer to avoid edge artifacts
    let dx = (bounds.max().x - bounds.min().x) * BUFFER_FRACTION;
    let dy = (bounds.max().y - bounds.min().y) * BUFFER_FRACTION;
    let buffered = Rect::new(
        Coord {
            x: bounds.min().x - dx,
            y: bounds.min().y - dy,
        },
        Coord {
            x: bounds.max().x + dx,
            y: bounds.max().y + dy,
        },
    );

    match geom {
        Geometry::Point(p) => {
            if point_in_rect(&p.0, &buffered) {
                Some(Geometry::Point(*p))
            } else {
                None
            }
        }
        Geometry::MultiPoint(mp) => {
            let points: Vec<Point<f64>> = mp
                .0
                .iter()
                .filter(|p| point_in_rect(&p.0, &buffered))
                .cloned()
                .collect();
            if points.is_empty() {
                None
            } else {
                Some(Geometry::MultiPoint(MultiPoint(points)))
            }
        }
        Geometry::LineString(ls) => {
            clip_linestring(ls, &buffered).map(|mls| {
                if mls.0.len() == 1 {
                    Geometry::LineString(mls.0.into_iter().next().unwrap())
                } else {
                    Geometry::MultiLineString(mls)
                }
            })
        }
        Geometry::MultiLineString(mls) => {
            let mut all_lines = Vec::new();
            for ls in &mls.0 {
                if let Some(MultiLineString(lines)) = clip_linestring(ls, &buffered) {
                    all_lines.extend(lines);
                }
            }
            if all_lines.is_empty() {
                None
            } else {
                Some(Geometry::MultiLineString(MultiLineString(all_lines)))
            }
        }
        Geometry::Polygon(poly) => {
            clip_polygon(poly, &buffered).map(|mp| {
                if mp.0.len() == 1 {
                    Geometry::Polygon(mp.0.into_iter().next().unwrap())
                } else {
                    Geometry::MultiPolygon(mp)
                }
            })
        }
        Geometry::MultiPolygon(mp) => {
            let mut all_polys = Vec::new();
            for poly in &mp.0 {
                if let Some(MultiPolygon(polys)) = clip_polygon(poly, &buffered) {
                    all_polys.extend(polys);
                }
            }
            if all_polys.is_empty() {
                None
            } else {
                Some(Geometry::MultiPolygon(MultiPolygon(all_polys)))
            }
        }
    }
}

fn point_in_rect(c: &Coord<f64>, rect: &Rect<f64>) -> bool {
    c.x >= rect.min().x && c.x <= rect.max().x && c.y >= rect.min().y && c.y <= rect.max().y
}

// ---------------------------------------------------------------------------
// Linestring clipping (Cohen-Sutherland)
// ---------------------------------------------------------------------------

/// Clip a linestring to a rectangle using Cohen-Sutherland line clipping
fn clip_linestring(ls: &LineString<f64>, rect: &Rect<f64>) -> Option<MultiLineString<f64>> {
    if ls.0.len() < 2 {
        return None;
    }

    let mut clipped_segments: Vec<LineString<f64>> = Vec::new();
    for window in ls.0.windows(2) {
        let (p0, p1) = (window[0], window[1]);
        if let Some((c0, c1)) = cohen_sutherland_clip(p0, p1, rect) {
            // Try to extend the last segment
            if let Some(last) = clipped_segments.last_mut() {
                let last_coord = *last.0.last().unwrap();
                if (last_coord.x - c0.x).abs() < 1e-10
                    && (last_coord.y - c0.y).abs() < 1e-10
                {
                    last.0.push(c1);
                    continue;
                }
            }
            clipped_segments.push(LineString(vec![c0, c1]));
        }
    }

    if clipped_segments.is_empty() {
        None
    } else {
        Some(MultiLineString(clipped_segments))
    }
}

/// Cohen-Sutherland outcodes
const INSIDE: u8 = 0;
const LEFT: u8 = 1;
const RIGHT: u8 = 2;
const BOTTOM: u8 = 4;
const TOP: u8 = 8;

fn outcode(p: Coord<f64>, rect: &Rect<f64>) -> u8 {
    let mut code = INSIDE;
    if p.x < rect.min().x {
        code |= LEFT;
    } else if p.x > rect.max().x {
        code |= RIGHT;
    }
    if p.y < rect.min().y {
        code |= BOTTOM;
    } else if p.y > rect.max().y {
        code |= TOP;
    }
    code
}

fn cohen_sutherland_clip(
    mut p0: Coord<f64>,
    mut p1: Coord<f64>,
    rect: &Rect<f64>,
) -> Option<(Coord<f64>, Coord<f64>)> {
    let mut code0 = outcode(p0, rect);
    let mut code1 = outcode(p1, rect);

    loop {
        if (code0 | code1) == 0 {
            return Some((p0, p1));
        }
        if (code0 & code1) != 0 {
            return None;
        }

        let code_out = if code0 != 0 { code0 } else { code1 };
        let dx = p1.x - p0.x;
        let dy = p1.y - p0.y;

        let p = if code_out & TOP != 0 {
            Coord {
                x: p0.x + dx * (rect.max().y - p0.y) / dy,
                y: rect.max().y,
            }
        } else if code_out & BOTTOM != 0 {
            Coord {
                x: p0.x + dx * (rect.min().y - p0.y) / dy,
                y: rect.min().y,
            }
        } else if code_out & RIGHT != 0 {
            Coord {
                x: rect.max().x,
                y: p0.y + dy * (rect.max().x - p0.x) / dx,
            }
        } else {
            Coord {
                x: rect.min().x,
                y: p0.y + dy * (rect.min().x - p0.x) / dx,
            }
        };

        if code_out == code0 {
            p0 = p;
            code0 = outcode(p0, rect);
        } else {
            p1 = p;
            code1 = outcode(p1, rect);
        }
    }
}

// ---------------------------------------------------------------------------
// Polygon clipping (geo::BooleanOps — topology-safe intersection)
// ---------------------------------------------------------------------------

/// Clip a polygon to a rectangle using geo::BooleanOps::intersection.
/// Correctly handles concave polygons that split into multiple pieces,
/// and holes that cross the clip boundary.
fn clip_polygon(poly: &Polygon<f64>, rect: &Rect<f64>) -> Option<MultiPolygon<f64>> {
    use geo::BooleanOps;
    use geo::BoundingRect;

    // Fast path: entirely within the clip rect
    if let Some(bbox) = poly.bounding_rect() {
        if bbox.min().x >= rect.min().x
            && bbox.max().x <= rect.max().x
            && bbox.min().y >= rect.min().y
            && bbox.max().y <= rect.max().y
        {
            return Some(MultiPolygon(vec![poly.clone()]));
        }
        // Fast reject: entirely outside the clip rect
        if bbox.max().x < rect.min().x
            || bbox.min().x > rect.max().x
            || bbox.max().y < rect.min().y
            || bbox.min().y > rect.max().y
        {
            return None;
        }
    }

    let clip_poly = rect.to_polygon();
    let result = poly.intersection(&clip_poly);

    if result.0.is_empty() {
        None
    } else {
        Some(result)
    }
}
