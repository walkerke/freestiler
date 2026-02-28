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
            clip_polygon_sh(poly, &buffered).map(|mp| {
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
                if let Some(MultiPolygon(polys)) = clip_polygon_sh(poly, &buffered) {
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
// Linestring clipping (Cohen-Sutherland, unchanged)
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
// Polygon clipping (Sutherland-Hodgman — O(4n) rectangle clipping)
// ---------------------------------------------------------------------------

/// Clip a polygon to a rectangle using Sutherland-Hodgman.
/// Much faster than BooleanOps for rectangle clipping: O(4n) vs O((n+k)log n).
fn clip_polygon_sh(poly: &Polygon<f64>, rect: &Rect<f64>) -> Option<MultiPolygon<f64>> {
    // Fast path: entirely within the clip rect
    use geo::BoundingRect;
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

    // Clip exterior ring
    let ext_verts = ring_vertices(poly.exterior());
    let clipped_ext = sutherland_hodgman(ext_verts, rect);
    if clipped_ext.len() < 3 {
        return None;
    }

    // Close the exterior ring
    let mut ext_ring = clipped_ext;
    ext_ring.push(ext_ring[0]);

    // Clip interior rings (holes)
    let mut interiors = Vec::new();
    for hole in poly.interiors() {
        let hole_verts = ring_vertices(hole);
        let clipped_hole = sutherland_hodgman(hole_verts, rect);
        if clipped_hole.len() >= 3 {
            let mut ring = clipped_hole;
            ring.push(ring[0]);
            interiors.push(LineString(ring));
        }
    }

    Some(MultiPolygon(vec![Polygon::new(
        LineString(ext_ring),
        interiors,
    )]))
}

/// Extract ring vertices without the closing point (last == first)
#[inline]
fn ring_vertices(ring: &LineString<f64>) -> &[Coord<f64>] {
    if ring.0.len() >= 2 && ring.0.first() == ring.0.last() {
        &ring.0[..ring.0.len() - 1]
    } else {
        &ring.0
    }
}

/// Sutherland-Hodgman polygon clipping against a rectangle.
/// Input: polygon vertices (implicitly closed — last connects to first).
/// Output: clipped polygon vertices (implicitly closed).
fn sutherland_hodgman(vertices: &[Coord<f64>], rect: &Rect<f64>) -> Vec<Coord<f64>> {
    if vertices.is_empty() {
        return Vec::new();
    }

    let mut output = vertices.to_vec();

    // Clip against left edge
    output = clip_edge(&output, |p| p.x >= rect.min().x, |a, b| {
        let t = (rect.min().x - a.x) / (b.x - a.x);
        Coord {
            x: rect.min().x,
            y: a.y + t * (b.y - a.y),
        }
    });
    if output.is_empty() {
        return output;
    }

    // Clip against right edge
    output = clip_edge(&output, |p| p.x <= rect.max().x, |a, b| {
        let t = (rect.max().x - a.x) / (b.x - a.x);
        Coord {
            x: rect.max().x,
            y: a.y + t * (b.y - a.y),
        }
    });
    if output.is_empty() {
        return output;
    }

    // Clip against bottom edge
    output = clip_edge(&output, |p| p.y >= rect.min().y, |a, b| {
        let t = (rect.min().y - a.y) / (b.y - a.y);
        Coord {
            x: a.x + t * (b.x - a.x),
            y: rect.min().y,
        }
    });
    if output.is_empty() {
        return output;
    }

    // Clip against top edge
    output = clip_edge(&output, |p| p.y <= rect.max().y, |a, b| {
        let t = (rect.max().y - a.y) / (b.y - a.y);
        Coord {
            x: a.x + t * (b.x - a.x),
            y: rect.max().y,
        }
    });

    output
}

/// Clip polygon vertices against a single edge of the clip rectangle.
#[inline]
fn clip_edge(
    input: &[Coord<f64>],
    inside: impl Fn(&Coord<f64>) -> bool,
    intersect: impl Fn(&Coord<f64>, &Coord<f64>) -> Coord<f64>,
) -> Vec<Coord<f64>> {
    if input.is_empty() {
        return Vec::new();
    }

    let mut output = Vec::with_capacity(input.len() + 4);
    let n = input.len();

    for i in 0..n {
        let current = &input[i];
        let next = &input[(i + 1) % n];
        let c_in = inside(current);
        let n_in = inside(next);

        if c_in {
            output.push(*current);
            if !n_in {
                output.push(intersect(current, next));
            }
        } else if n_in {
            output.push(intersect(current, next));
        }
    }

    output
}
