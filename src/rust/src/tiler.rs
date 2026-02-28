use geo_types::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, Rect};
use std::collections::HashMap;

/// A feature with geometry and properties, ready for tiling
#[derive(Clone, Debug)]
pub struct Feature {
    pub id: Option<u64>,
    pub geometry: Geometry,
    pub properties: Vec<PropertyValue>,
}

/// Supported geometry types
#[derive(Clone, Debug)]
pub enum Geometry {
    Point(Point<f64>),
    MultiPoint(MultiPoint<f64>),
    LineString(LineString<f64>),
    MultiLineString(MultiLineString<f64>),
    Polygon(Polygon<f64>),
    MultiPolygon(MultiPolygon<f64>),
}

/// Property value types
#[derive(Clone, Debug)]
pub enum PropertyValue {
    String(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Null,
}

/// A tile coordinate
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct TileCoord {
    pub z: u8,
    pub x: u32,
    pub y: u32,
}

/// Convert longitude to tile X at a given zoom level
pub fn lon_to_tile_x(lon: f64, zoom: u8) -> u32 {
    let n = (1u64 << zoom) as f64;
    let x = ((lon + 180.0) / 360.0 * n).floor() as i64;
    x.clamp(0, (1i64 << zoom) - 1) as u32
}

/// Convert latitude to tile Y at a given zoom level
pub fn lat_to_tile_y(lat: f64, zoom: u8) -> u32 {
    let n = (1u64 << zoom) as f64;
    let lat_rad = lat.to_radians();
    let y = ((1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n).floor() as i64;
    y.clamp(0, (1i64 << zoom) - 1) as u32
}

/// Convert tile X to longitude (west edge)
pub fn tile_x_to_lon(x: u32, zoom: u8) -> f64 {
    x as f64 / (1u64 << zoom) as f64 * 360.0 - 180.0
}

/// Convert tile Y to latitude (north edge)
pub fn tile_y_to_lat(y: u32, zoom: u8) -> f64 {
    let n = std::f64::consts::PI - 2.0 * std::f64::consts::PI * y as f64 / (1u64 << zoom) as f64;
    (0.5 * (n.exp() - (-n).exp())).atan().to_degrees()
}

/// Get the bounding box of a tile in lon/lat
pub fn tile_bounds(coord: &TileCoord) -> Rect<f64> {
    let west = tile_x_to_lon(coord.x, coord.z);
    let east = tile_x_to_lon(coord.x + 1, coord.z);
    let north = tile_y_to_lat(coord.y, coord.z);
    let south = tile_y_to_lat(coord.y + 1, coord.z);
    Rect::new(Coord { x: west, y: south }, Coord { x: east, y: north })
}

/// Get the bounding box of a geometry
pub fn geometry_bbox(geom: &Geometry) -> Rect<f64> {
    use geo::BoundingRect;
    match geom {
        Geometry::Point(p) => Rect::new(p.0, p.0),
        Geometry::MultiPoint(mp) => mp.bounding_rect().unwrap_or_else(|| Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 0.0, y: 0.0 })),
        Geometry::LineString(ls) => ls.bounding_rect().unwrap_or_else(|| Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 0.0, y: 0.0 })),
        Geometry::MultiLineString(mls) => mls.bounding_rect().unwrap_or_else(|| Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 0.0, y: 0.0 })),
        Geometry::Polygon(p) => p.bounding_rect().unwrap_or_else(|| Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 0.0, y: 0.0 })),
        Geometry::MultiPolygon(mp) => mp.bounding_rect().unwrap_or_else(|| Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 0.0, y: 0.0 })),
    }
}

/// Assign features to tiles using optional geometry overrides for bbox calculation.
/// When `geom_overrides[i]` is `Some(geom)`, uses that geometry's bbox instead of
/// the feature's original geometry. This allows using pre-simplified geometries
/// (e.g. VW-simplified lines) for tighter tile assignment.
pub fn assign_features_to_tiles_with_geoms(
    features: &[Feature],
    geom_overrides: &[Option<Geometry>],
    zoom: u8,
) -> HashMap<TileCoord, Vec<usize>> {
    let mut tile_map: HashMap<TileCoord, Vec<usize>> = HashMap::new();

    for (idx, feature) in features.iter().enumerate() {
        let bbox = match &geom_overrides[idx] {
            Some(g) => geometry_bbox(g),
            None => geometry_bbox(&feature.geometry),
        };

        let min_x = lon_to_tile_x(bbox.min().x, zoom);
        let max_x = lon_to_tile_x(bbox.max().x, zoom);
        let min_y = lat_to_tile_y(bbox.max().y, zoom); // lat/y inverted
        let max_y = lat_to_tile_y(bbox.min().y, zoom);

        for x in min_x..=max_x {
            for y in min_y..=max_y {
                tile_map
                    .entry(TileCoord { z: zoom, x, y })
                    .or_default()
                    .push(idx);
            }
        }
    }

    tile_map
}
