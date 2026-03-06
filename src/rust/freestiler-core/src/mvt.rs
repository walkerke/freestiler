use prost::Message;
use std::collections::HashMap;

use crate::tiler::{tile_bounds, Feature, Geometry, PropertyValue, TileCoord};

/// MVT tile extent (coordinate space)
const EXTENT: u32 = 4096;

// --- Protobuf structs matching vector_tile.proto ---

#[derive(Clone, PartialEq, Message)]
pub struct Tile {
    #[prost(message, repeated, tag = "3")]
    pub layers: Vec<Layer>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Layer {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, repeated, tag = "2")]
    pub features: Vec<TileFeature>,
    #[prost(string, repeated, tag = "3")]
    pub keys: Vec<String>,
    #[prost(message, repeated, tag = "4")]
    pub values: Vec<Value>,
    #[prost(uint32, optional, tag = "5")]
    pub extent: Option<u32>,
    #[prost(uint32, tag = "15")]
    pub version: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct TileFeature {
    #[prost(uint64, optional, tag = "1")]
    pub id: Option<u64>,
    #[prost(uint32, repeated, packed = "true", tag = "2")]
    pub tags: Vec<u32>,
    #[prost(enumeration = "GeomType", optional, tag = "3")]
    pub r#type: Option<i32>,
    #[prost(uint32, repeated, packed = "true", tag = "4")]
    pub geometry: Vec<u32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Value {
    #[prost(string, optional, tag = "1")]
    pub string_value: Option<String>,
    #[prost(float, optional, tag = "2")]
    pub float_value: Option<f32>,
    #[prost(double, optional, tag = "3")]
    pub double_value: Option<f64>,
    #[prost(int64, optional, tag = "4")]
    pub int_value: Option<i64>,
    #[prost(uint64, optional, tag = "5")]
    pub uint_value: Option<u64>,
    #[prost(sint64, optional, tag = "6")]
    pub sint_value: Option<i64>,
    #[prost(bool, optional, tag = "7")]
    pub bool_value: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum GeomType {
    Unknown = 0,
    Point = 1,
    Linestring = 2,
    Polygon = 3,
}

// --- Encoding functions ---

/// Encode features from multiple layers into a single MVT tile
pub fn encode_tile_multilayer(
    coord: &TileCoord,
    layer_data: &[(&str, &[String], &[Feature])],
) -> Vec<u8> {
    let bounds = tile_bounds(coord);
    let west = bounds.min().x;
    let south = bounds.min().y;
    let east = bounds.max().x;
    let north = bounds.max().y;

    let mut layers = Vec::new();

    for &(layer_name, property_names, features) in layer_data {
        if features.is_empty() {
            continue;
        }

        let mut keys: Vec<String> = Vec::new();
        let mut key_map: HashMap<String, u32> = HashMap::new();
        let mut values: Vec<Value> = Vec::new();
        let mut value_map: HashMap<String, u32> = HashMap::new();
        let mut tile_features: Vec<TileFeature> = Vec::new();

        for feature in features {
            let geom_type = match &feature.geometry {
                Geometry::Point(_) | Geometry::MultiPoint(_) => GeomType::Point,
                Geometry::LineString(_) | Geometry::MultiLineString(_) => GeomType::Linestring,
                Geometry::Polygon(_) | Geometry::MultiPolygon(_) => GeomType::Polygon,
            };

            let geometry = encode_geometry(&feature.geometry, west, south, east, north);
            if geometry.is_empty() {
                continue;
            }

            let mut tags = Vec::new();
            for (i, prop) in feature.properties.iter().enumerate() {
                if matches!(prop, PropertyValue::Null) {
                    continue;
                }
                if i >= property_names.len() {
                    continue;
                }

                let key_name = &property_names[i];
                let key_idx = *key_map.entry(key_name.clone()).or_insert_with(|| {
                    let idx = keys.len() as u32;
                    keys.push(key_name.clone());
                    idx
                });

                let value_key = property_value_key(prop);
                let value_idx = *value_map.entry(value_key).or_insert_with(|| {
                    let idx = values.len() as u32;
                    values.push(property_to_value(prop));
                    idx
                });

                tags.push(key_idx);
                tags.push(value_idx);
            }

            tile_features.push(TileFeature {
                id: feature.id,
                tags,
                r#type: Some(geom_type as i32),
                geometry,
            });
        }

        if !tile_features.is_empty() {
            layers.push(Layer {
                version: 2,
                name: layer_name.to_string(),
                features: tile_features,
                keys,
                values,
                extent: Some(EXTENT),
            });
        }
    }

    if layers.is_empty() {
        return Vec::new();
    }

    let tile = Tile { layers };
    let mut buf = Vec::with_capacity(tile.encoded_len());
    tile.encode(&mut buf).expect("MVT encoding failed");
    buf
}

/// Encode features into an MVT tile (single layer — convenience wrapper)
pub fn encode_tile(
    coord: &TileCoord,
    features: &[Feature],
    layer_name: &str,
    property_names: &[String],
) -> Vec<u8> {
    encode_tile_multilayer(coord, &[(layer_name, property_names, features)])
}

/// Convert a longitude to tile-local X coordinate (0..4096)
fn lon_to_tile_coord(lon: f64, west: f64, east: f64) -> i32 {
    ((lon - west) / (east - west) * EXTENT as f64).round() as i32
}

/// Convert a latitude to tile-local Y coordinate (0..4096, Y-down)
fn lat_to_tile_coord(lat: f64, south: f64, north: f64) -> i32 {
    // Interpolate in Mercator Y space (not linear latitude) for correct projection
    let lat_merc = lat.to_radians().tan().asinh();
    let south_merc = south.to_radians().tan().asinh();
    let north_merc = north.to_radians().tan().asinh();
    ((north_merc - lat_merc) / (north_merc - south_merc) * EXTENT as f64).round() as i32
}

/// Zigzag encode a signed integer
fn zigzag(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

/// Create a command integer
fn command(id: u32, count: u32) -> u32 {
    (id & 0x7) | (count << 3)
}

const CMD_MOVE_TO: u32 = 1;
const CMD_LINE_TO: u32 = 2;
const CMD_CLOSE_PATH: u32 = 7;

/// Encode a geometry into MVT command sequence
fn encode_geometry(geom: &Geometry, west: f64, south: f64, east: f64, north: f64) -> Vec<u32> {
    match geom {
        Geometry::Point(p) => {
            let x = lon_to_tile_coord(p.x(), west, east);
            let y = lat_to_tile_coord(p.y(), south, north);
            vec![command(CMD_MOVE_TO, 1), zigzag(x), zigzag(y)]
        }
        Geometry::MultiPoint(mp) => {
            let mut cmds = Vec::new();
            let mut cx = 0i32;
            let mut cy = 0i32;
            cmds.push(command(CMD_MOVE_TO, mp.0.len() as u32));
            for p in &mp.0 {
                let x = lon_to_tile_coord(p.x(), west, east);
                let y = lat_to_tile_coord(p.y(), south, north);
                cmds.push(zigzag(x - cx));
                cmds.push(zigzag(y - cy));
                cx = x;
                cy = y;
            }
            cmds
        }
        Geometry::LineString(ls) => {
            encode_linestring_cmds(ls, west, south, east, north, &mut 0, &mut 0)
        }
        Geometry::MultiLineString(mls) => {
            let mut cmds = Vec::new();
            let mut cx = 0i32;
            let mut cy = 0i32;
            for ls in &mls.0 {
                cmds.extend(encode_linestring_cmds(
                    ls, west, south, east, north, &mut cx, &mut cy,
                ));
            }
            cmds
        }
        Geometry::Polygon(poly) => {
            encode_polygon_cmds(poly, west, south, east, north, &mut 0, &mut 0)
        }
        Geometry::MultiPolygon(mp) => {
            let mut cmds = Vec::new();
            let mut cx = 0i32;
            let mut cy = 0i32;
            for poly in &mp.0 {
                cmds.extend(encode_polygon_cmds(
                    poly, west, south, east, north, &mut cx, &mut cy,
                ));
            }
            cmds
        }
    }
}

fn encode_linestring_cmds(
    ls: &geo_types::LineString<f64>,
    west: f64,
    south: f64,
    east: f64,
    north: f64,
    cx: &mut i32,
    cy: &mut i32,
) -> Vec<u32> {
    if ls.0.len() < 2 {
        return Vec::new();
    }

    let mut cmds = Vec::new();

    // MoveTo first point
    let x = lon_to_tile_coord(ls.0[0].x, west, east);
    let y = lat_to_tile_coord(ls.0[0].y, south, north);
    cmds.push(command(CMD_MOVE_TO, 1));
    cmds.push(zigzag(x - *cx));
    cmds.push(zigzag(y - *cy));
    *cx = x;
    *cy = y;

    // LineTo remaining points, filtering zero-length segments
    let mut line_to_params: Vec<u32> = Vec::new();
    let mut count = 0u32;
    for coord in &ls.0[1..] {
        let x = lon_to_tile_coord(coord.x, west, east);
        let y = lat_to_tile_coord(coord.y, south, north);
        let dx = x - *cx;
        let dy = y - *cy;
        if dx == 0 && dy == 0 {
            continue;
        }
        line_to_params.push(zigzag(dx));
        line_to_params.push(zigzag(dy));
        *cx = x;
        *cy = y;
        count += 1;
    }

    if count == 0 {
        return Vec::new();
    }

    cmds.push(command(CMD_LINE_TO, count));
    cmds.extend(line_to_params);
    cmds
}

fn encode_polygon_cmds(
    poly: &geo_types::Polygon<f64>,
    west: f64,
    south: f64,
    east: f64,
    north: f64,
    cx: &mut i32,
    cy: &mut i32,
) -> Vec<u32> {
    let mut cmds = Vec::new();

    // Encode exterior ring
    let ext_cmds = encode_ring_cmds(poly.exterior(), west, south, east, north, cx, cy);
    if ext_cmds.is_empty() {
        return Vec::new();
    }
    cmds.extend(ext_cmds);

    // Encode interior rings (holes)
    for interior in poly.interiors() {
        let ring_cmds = encode_ring_cmds(interior, west, south, east, north, cx, cy);
        if !ring_cmds.is_empty() {
            cmds.extend(ring_cmds);
        }
    }

    cmds
}

fn encode_ring_cmds(
    ring: &geo_types::LineString<f64>,
    west: f64,
    south: f64,
    east: f64,
    north: f64,
    cx: &mut i32,
    cy: &mut i32,
) -> Vec<u32> {
    // A ring should have at least 4 coords (3 unique + close) but the last == first
    // We encode all but the last (ClosePath closes it)
    let coords: Vec<_> = if ring.0.len() >= 2 && ring.0.first() == ring.0.last() {
        ring.0[..ring.0.len() - 1].to_vec()
    } else {
        ring.0.clone()
    };

    if coords.len() < 3 {
        return Vec::new();
    }

    let mut cmds = Vec::new();

    // MoveTo first point
    let x = lon_to_tile_coord(coords[0].x, west, east);
    let y = lat_to_tile_coord(coords[0].y, south, north);
    cmds.push(command(CMD_MOVE_TO, 1));
    cmds.push(zigzag(x - *cx));
    cmds.push(zigzag(y - *cy));
    *cx = x;
    *cy = y;

    // LineTo remaining points
    let mut line_to_params: Vec<u32> = Vec::new();
    let mut count = 0u32;
    for coord in &coords[1..] {
        let x = lon_to_tile_coord(coord.x, west, east);
        let y = lat_to_tile_coord(coord.y, south, north);
        let dx = x - *cx;
        let dy = y - *cy;
        if dx == 0 && dy == 0 {
            continue;
        }
        line_to_params.push(zigzag(dx));
        line_to_params.push(zigzag(dy));
        *cx = x;
        *cy = y;
        count += 1;
    }

    if count < 2 {
        return Vec::new();
    }

    cmds.push(command(CMD_LINE_TO, count));
    cmds.extend(line_to_params);
    cmds.push(command(CMD_CLOSE_PATH, 1));

    cmds
}

/// Create a unique string key for a property value (for deduplication)
fn property_value_key(prop: &PropertyValue) -> String {
    match prop {
        PropertyValue::String(s) => format!("s:{}", s),
        PropertyValue::Int(i) => format!("i:{}", i),
        PropertyValue::Double(d) => format!("d:{}", d),
        PropertyValue::Bool(b) => format!("b:{}", b),
        PropertyValue::Null => "null".to_string(),
    }
}

/// Convert a PropertyValue to an MVT Value
fn property_to_value(prop: &PropertyValue) -> Value {
    match prop {
        PropertyValue::String(s) => Value {
            string_value: Some(s.clone()),
            ..Default::default()
        },
        PropertyValue::Int(i) => Value {
            int_value: Some(*i),
            ..Default::default()
        },
        PropertyValue::Double(d) => Value {
            double_value: Some(*d),
            ..Default::default()
        },
        PropertyValue::Bool(b) => Value {
            bool_value: Some(*b),
            ..Default::default()
        },
        PropertyValue::Null => Value::default(),
    }
}
