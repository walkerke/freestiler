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

/// A layer with its features and metadata
pub struct LayerData {
    pub name: String,
    pub features: Vec<Feature>,
    pub prop_names: Vec<String>,
    pub prop_types: Vec<String>,
    pub min_zoom: u8,
    pub max_zoom: u8,
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

/// Compute Morton code (Z-order curve) for spatial ordering.
/// Maps (x, y) in [0, 65535] to a 1D index with good spatial locality.
pub fn morton_code(x: u32, y: u32) -> u64 {
    fn spread_bits(v: u32) -> u64 {
        let mut v = v as u64;
        v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
        v = (v | (v << 8)) & 0x00FF00FF00FF00FF;
        v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F;
        v = (v | (v << 2)) & 0x3333333333333333;
        v = (v | (v << 1)) & 0x5555555555555555;
        v
    }
    spread_bits(x) | (spread_bits(y) << 1)
}

/// Compute a Morton key for a geometry within a tile's coordinate space.
/// Uses the centroid of the geometry's bounding box, normalized to the tile bounds.
/// Y is projected to Mercator space to match the tile encoder's coordinate transform.
pub fn tile_morton_key(geom: &Geometry, west: f64, east: f64, south: f64, north: f64) -> u64 {
    let bbox = geometry_bbox(geom);
    let cx = ((bbox.min().x + bbox.max().x) / 2.0 - west) / (east - west);
    // Normalize Y in Mercator space (matches lat_to_tile_coord in mlt.rs / mvt.rs)
    let lat = (bbox.min().y + bbox.max().y) / 2.0;
    let lat_merc = lat.to_radians().tan().asinh();
    let south_merc = south.to_radians().tan().asinh();
    let north_merc = north.to_radians().tan().asinh();
    // Note: in tile space, north is y=0 and south is y=extent, so invert
    let cy = (north_merc - lat_merc) / (north_merc - south_merc);
    let ix = (cx * 65535.0).clamp(0.0, 65535.0) as u32;
    let iy = (cy * 65535.0).clamp(0.0, 65535.0) as u32;
    morton_code(ix, iy)
}

/// Buffer factor as a fraction of tile extent for tile assignment.
/// Must match the BUFFER_FRACTION in clip.rs so features in the
/// clip buffer zone are assigned to the tile.
const ASSIGN_BUFFER_FRACTION: f64 = 0.05;

/// Assign features to tiles using optional geometry overrides for bbox calculation.
/// When `geom_overrides[i]` is `Some(geom)`, uses that geometry's bbox instead of
/// the feature's original geometry. This allows using pre-simplified geometries
/// (e.g. VW-simplified lines) for tighter tile assignment.
///
/// Features are assigned using buffered tile bounds so that features in the
/// clip buffer zone are included — preventing seams at tile boundaries.
pub fn assign_features_to_tiles_with_geoms(
    features: &[Feature],
    geom_overrides: &[Option<Geometry>],
    zoom: u8,
) -> HashMap<TileCoord, Vec<usize>> {
    let mut tile_map: HashMap<TileCoord, Vec<usize>> = HashMap::new();
    let max_tiles = (1u64 << zoom) as u32;

    // Precompute the buffer in degrees of longitude and latitude
    // at this zoom level (one tile's worth × buffer fraction)
    let tile_width_deg = 360.0 / (1u64 << zoom) as f64;
    let buf_x = tile_width_deg * ASSIGN_BUFFER_FRACTION;
    // For latitude, use an approximate buffer based on mid-latitude tile height
    // (conservative: use equatorial tile height which is the largest)
    let buf_y = tile_width_deg * ASSIGN_BUFFER_FRACTION;

    for (idx, feature) in features.iter().enumerate() {
        let bbox = match &geom_overrides[idx] {
            Some(g) => geometry_bbox(g),
            None => geometry_bbox(&feature.geometry),
        };

        // Expand the feature bbox by the buffer amount, then find tiles
        let min_x = lon_to_tile_x((bbox.min().x - buf_x).max(-180.0), zoom);
        let max_x = lon_to_tile_x((bbox.max().x + buf_x).min(180.0), zoom);
        let min_y = lat_to_tile_y((bbox.max().y + buf_y).min(85.051), zoom); // lat/y inverted
        let max_y = lat_to_tile_y((bbox.min().y - buf_y).max(-85.051), zoom);

        for x in min_x..=max_x.min(max_tiles - 1) {
            for y in min_y..=max_y.min(max_tiles - 1) {
                tile_map
                    .entry(TileCoord { z: zoom, x, y })
                    .or_default()
                    .push(idx);
            }
        }
    }

    tile_map
}
