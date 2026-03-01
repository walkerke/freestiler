use integer_encoding::VarInt;
use std::collections::HashMap;

use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord, tile_bounds};

/// MLT tile extent
const EXTENT: u32 = 4096;

/// MLT layer tag for v01 format
const TAG_V01: u8 = 0x01;

// Column types
const COL_ID: u8 = 2; // LongId — 64-bit unsigned IDs (reference: ColumnType::LongId)
const COL_GEOMETRY: u8 = 4;
const COL_I64: u8 = 20;
const COL_OPT_I64: u8 = 21;
const COL_F64: u8 = 26;
const COL_OPT_F64: u8 = 27;
const COL_STR: u8 = 28;
const COL_OPT_STR: u8 = 29;
const COL_BOOL: u8 = 10;
const COL_OPT_BOOL: u8 = 11;

// Geometry types
const GEOM_POINT: u8 = 0;
const GEOM_LINESTRING: u8 = 1;
const GEOM_POLYGON: u8 = 2;
const GEOM_MULTI_POINT: u8 = 3;
const GEOM_MULTI_LINESTRING: u8 = 4;
const GEOM_MULTI_POLYGON: u8 = 5;

// PhysicalStreamType ordinals (upper nibble of byte 0)
// Enum order: PRESENT=0, DATA=1, OFFSET=2, LENGTH=3
const STREAM_PRESENT: u8 = 0; // ordinal 0
const STREAM_DATA: u8 = 1;    // ordinal 1
const STREAM_OFFSET: u8 = 2;  // ordinal 2
const STREAM_LENGTH: u8 = 3;  // ordinal 3

// DictionaryType ordinals (lower nibble of byte 0, when stream type = DATA)
// Enum order: NONE=0, SINGLE=1, SHARED=2, VERTEX=3, MORTON=4, FSST=5
const DATA_NONE: u8 = 0;   // DictionaryType.NONE
const DATA_SINGLE: u8 = 1; // DictionaryType.SINGLE
const DATA_VERTEX: u8 = 3; // DictionaryType.VERTEX

// OffsetType ordinals (lower nibble of byte 0, when stream type = OFFSET)
// Enum order: VERTEX=0, INDEX=1, STRING=2, KEY=3
const OFFSET_STRING: u8 = 2;

// LengthType ordinals (lower nibble of byte 0, when stream type = LENGTH)
// Enum order: VAR_BINARY=0, GEOMETRIES=1, PARTS=2, RINGS=3, TRIANGLES=4, SYMBOL=5, DICTIONARY=6
const LENGTH_VAR_BINARY: u8 = 0;
const LENGTH_GEOMETRIES: u8 = 1;
const LENGTH_PARTS: u8 = 2;
const LENGTH_RINGS: u8 = 3;
const LENGTH_DICTIONARY: u8 = 6;

// LogicalLevelTechnique ordinals (3 bits each in byte 1)
// Enum order: NONE=0, DELTA=1, COMPONENTWISE_DELTA=2, RLE=3, MORTON=4, PDE=5
const LOG_NONE: u8 = 0;
const LOG_DELTA: u8 = 1;
const LOG_COMPONENTWISE_DELTA: u8 = 2;
const LOG_RLE: u8 = 3;

// PhysicalLevelTechnique ordinals (2 bits in byte 1)
// Enum order: NONE=0, FAST_PFOR=1, VARINT=2, ALP=3
const PHYS_NONE: u8 = 0;
const PHYS_VARINT: u8 = 2;

/// Count the number of consecutive runs in a slice.
fn count_runs<T: PartialEq>(values: &[T]) -> usize {
    if values.is_empty() {
        return 0;
    }
    let mut runs = 1;
    for i in 1..values.len() {
        if values[i] != values[i - 1] {
            runs += 1;
        }
    }
    runs
}

/// ORC-style byte-RLE encoding.
/// Control byte 0x00-0x7F: run of (control + 3) copies of the next byte.
/// Control byte 0x80-0xFF: (256 - control) literal bytes follow.
/// Ref: ORC byte-RLE, used by MLT for geometry-type streams.
fn byte_rle_encode(values: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if values.is_empty() {
        return out;
    }
    let n = values.len();
    let mut i = 0;
    while i < n {
        // Check for a run of at least 3 identical bytes
        let val = values[i];
        let mut run_len = 1usize;
        while i + run_len < n && values[i + run_len] == val && run_len < 130 {
            run_len += 1;
        }
        if run_len >= 3 {
            // Emit run: control = (run_len - 3), then the byte value
            out.push((run_len - 3) as u8);
            out.push(val);
            i += run_len;
        } else {
            // Collect literals (up to 128)
            let start = i;
            let mut lit_len = 0usize;
            while i + lit_len < n && lit_len < 128 {
                // Check if a run of 3+ starts here — if so, stop collecting literals
                let v = values[i + lit_len];
                let mut ahead = 1usize;
                while i + lit_len + ahead < n && values[i + lit_len + ahead] == v && ahead < 3 {
                    ahead += 1;
                }
                if ahead >= 3 && lit_len > 0 {
                    break;
                }
                lit_len += 1;
            }
            // Emit literals: control = (256 - lit_len) as u8, then the literal bytes
            out.push((256 - lit_len) as u8);
            out.extend_from_slice(&values[start..start + lit_len]);
            i += lit_len;
        }
    }
    out
}

/// Integer RLE: two-buffer format for MLT.
/// Returns (encoded_bytes, num_runs, num_rle_values).
/// Buffer layout: [run_length_1, ..., run_length_N, value_1, ..., value_N] as varints.
/// Stream metadata must include extra varints: numRuns and numRleValues.
fn integer_rle_encode_u32(values: &[u32]) -> (Vec<u8>, usize, usize) {
    if values.is_empty() {
        return (Vec::new(), 0, 0);
    }
    let mut run_lengths: Vec<u32> = Vec::new();
    let mut run_values: Vec<u32> = Vec::new();
    let mut i = 0;
    while i < values.len() {
        let val = values[i];
        let mut count = 1u32;
        while i + (count as usize) < values.len() && values[i + count as usize] == val {
            count += 1;
        }
        run_lengths.push(count);
        run_values.push(val);
        i += count as usize;
    }
    let num_runs = run_lengths.len();
    let num_rle_values = values.len();
    // Encode: run_lengths first, then run_values, all as varints
    let mut out = Vec::new();
    for &rl in &run_lengths {
        let mut buf = [0u8; 5];
        let n = rl.encode_var(&mut buf);
        out.extend_from_slice(&buf[..n]);
    }
    for &rv in &run_values {
        let mut buf = [0u8; 5];
        let n = rv.encode_var(&mut buf);
        out.extend_from_slice(&buf[..n]);
    }
    (out, num_runs, num_rle_values)
}

/// Write MLT stream metadata header with extra RLE fields.
/// Appends varint(numRuns) and varint(numRleValues) after the standard header.
fn write_stream_meta_rle(
    out: &mut Vec<u8>,
    physical_stream_type: u8,
    logical_subtype: u8,
    logical_technique1: u8,
    logical_technique2: u8,
    physical_technique: u8,
    num_values: usize,
    byte_length: usize,
    num_runs: usize,
    num_rle_values: usize,
) {
    let byte0 = (physical_stream_type << 4) | logical_subtype;
    let byte1 = (logical_technique1 << 5) | (logical_technique2 << 2) | physical_technique;
    out.push(byte0);
    out.push(byte1);
    write_varint_usize(out, num_values);
    write_varint_usize(out, byte_length);
    write_varint_usize(out, num_runs);
    write_varint_usize(out, num_rle_values);
}

/// Encode features from multiple layers into a single MLT tile
pub fn encode_tile_multilayer(
    coord: &TileCoord,
    layer_data: &[(&str, &[String], &[Feature])],
) -> Vec<u8> {
    let mut tile_bytes = Vec::new();
    for &(layer_name, property_names, features) in layer_data {
        if !features.is_empty() {
            let layer_bytes = encode_tile(coord, features, layer_name, property_names);
            tile_bytes.extend(&layer_bytes);
        }
    }
    tile_bytes
}

/// Encode features into an MLT tile (single layer)
pub fn encode_tile(
    coord: &TileCoord,
    features: &[Feature],
    layer_name: &str,
    property_names: &[String],
) -> Vec<u8> {
    if features.is_empty() {
        return Vec::new();
    }

    let bounds = tile_bounds(coord);
    let west = bounds.min().x;
    let south = bounds.min().y;
    let east = bounds.max().x;
    let north = bounds.max().y;

    // Build the layer payload
    let mut layer_data = Vec::new();

    // Layer name (varint-prefixed UTF-8)
    write_string(&mut layer_data, layer_name);

    // Extent
    write_varint_u32(&mut layer_data, EXTENT);

    // Count columns: id + geometry + properties
    let num_columns = 2 + property_names.len();
    write_varint_usize(&mut layer_data, num_columns);

    // Column metadata (type codes written as single bytes per spec)
    // 1. ID column (type code 2 = LongId, 64-bit unsigned; no name for types < 5)
    layer_data.push(COL_ID);
    // 2. Geometry column (type code 4; no name for types < 5)
    layer_data.push(COL_GEOMETRY);
    // 3. Property columns (types >= 5 have a name per has_name())
    for (i, name) in property_names.iter().enumerate() {
        let col_type = infer_column_type(features, i);
        layer_data.push(col_type);
        write_string(&mut layer_data, name);
    }

    // Now write streams

    // --- ID stream (delta-encoded unsigned varints) ---
    {
        let ids: Vec<u64> = features
            .iter()
            .map(|f| f.id.unwrap_or(0))
            .collect();
        // Delta encode: output differences between consecutive IDs
        let mut deltas = Vec::with_capacity(ids.len());
        let mut prev = 0u64;
        for &id in &ids {
            deltas.push(id.wrapping_sub(prev));
            prev = id;
        }
        let id_bytes = encode_varint_u64_stream(&deltas);
        write_stream_meta(&mut layer_data, STREAM_DATA, DATA_NONE, LOG_DELTA, LOG_NONE, PHYS_VARINT, ids.len(), id_bytes.len());
        layer_data.extend(&id_bytes);
    }

    // --- Geometry streams ---
    // Write geometry stream count before the streams (spec requirement)
    let geom_stream_count = count_geometry_streams(features);
    write_varint_usize(&mut layer_data, geom_stream_count);

    encode_geometry_streams(
        &mut layer_data,
        features,
        west, south, east, north,
    );

    // --- Property streams ---
    for (i, _name) in property_names.iter().enumerate() {
        let col_type = infer_column_type(features, i);
        // STRING columns need a stream count varint (hasStreamCount = true)
        if col_type == COL_STR || col_type == COL_OPT_STR {
            let has_nulls = features.iter().any(|f| {
                i >= f.properties.len() || matches!(f.properties[i], PropertyValue::Null)
            });
            let use_dict = should_use_dictionary(features, i);
            // presence stream (if nullable) + encoding streams
            // dictionary: 3 streams (dict lengths, dict data, indices)
            // raw: 2 streams (lengths, data)
            let encoding_streams: usize = if use_dict { 3 } else { 2 };
            let stream_count = if has_nulls { encoding_streams + 1 } else { encoding_streams };
            write_varint_usize(&mut layer_data, stream_count);
        }
        encode_property_stream(&mut layer_data, features, i);
    }

    // Wrap in layer envelope: varint(length) + varint(tag=1) + layer_data
    let mut tile_bytes = Vec::new();
    let mut tag_buf = [0u8; 5];
    let tag_len = (TAG_V01 as u32).encode_var(&mut tag_buf);
    let total_size = tag_len + layer_data.len();
    write_varint_usize(&mut tile_bytes, total_size);
    tile_bytes.extend_from_slice(&tag_buf[..tag_len]);
    tile_bytes.extend(&layer_data);

    tile_bytes
}

fn infer_column_type(features: &[Feature], prop_idx: usize) -> u8 {
    let mut has_null = false;
    let mut has_string = false;
    let mut has_int = false;
    let mut has_double = false;
    let mut has_bool = false;

    for f in features {
        if prop_idx < f.properties.len() {
            match &f.properties[prop_idx] {
                PropertyValue::Null => has_null = true,
                PropertyValue::String(_) => has_string = true,
                PropertyValue::Int(_) => has_int = true,
                PropertyValue::Double(_) => has_double = true,
                PropertyValue::Bool(_) => has_bool = true,
            }
        } else {
            has_null = true;
        }
    }

    // Priority: string > double > int > bool
    if has_string {
        if has_null { COL_OPT_STR } else { COL_STR }
    } else if has_double {
        if has_null { COL_OPT_F64 } else { COL_F64 }
    } else if has_int {
        if has_null { COL_OPT_I64 } else { COL_I64 }
    } else if has_bool {
        if has_null { COL_OPT_BOOL } else { COL_BOOL }
    } else {
        COL_OPT_STR // all nulls
    }
}

/// Count the number of geometry streams that will be written.
/// Always: geom_type_stream (1) + vertex_stream (1) = 2
/// Plus: num_geometries if any multi-types, num_parts if any lines/polys, num_rings if any polys
fn count_geometry_streams(features: &[Feature]) -> usize {
    let mut has_multi = false;
    let mut has_parts = false;
    let mut has_rings = false;

    for f in features {
        match &f.geometry {
            Geometry::Point(_) => {}
            Geometry::MultiPoint(_) => { has_multi = true; }
            Geometry::LineString(_) => { has_parts = true; }
            Geometry::MultiLineString(_) => { has_multi = true; has_parts = true; }
            Geometry::Polygon(_) => { has_parts = true; has_rings = true; }
            Geometry::MultiPolygon(_) => { has_multi = true; has_parts = true; has_rings = true; }
        }
    }

    let mut count = 2; // geom_type + vertex
    if has_multi { count += 1; }
    if has_parts { count += 1; }
    if has_rings { count += 1; }
    count
}

fn encode_geometry_streams(
    out: &mut Vec<u8>,
    features: &[Feature],
    west: f64,
    south: f64,
    east: f64,
    north: f64,
) {
    let n = features.len();

    // 1. Geometry type stream — encoded as u32 varints (matching reference encoder).
    //    Reference uses StreamType::Length(VarBinary), not Data(None).
    let geom_types: Vec<u32> = features.iter().map(|f| geometry_type_byte(&f.geometry) as u32).collect();
    let geom_type_runs = count_runs(&geom_types);
    if geom_type_runs * 2 < geom_types.len() {
        // Integer RLE: two-buffer [run_lengths..., values...] as varints
        // num_values = physical buffer count (num_runs * 2), not logical feature count
        let (rle_bytes, num_runs, num_rle_values) = integer_rle_encode_u32(&geom_types);
        write_stream_meta_rle(out, STREAM_LENGTH, LENGTH_VAR_BINARY, LOG_RLE, LOG_NONE, PHYS_VARINT, num_runs * 2, rle_bytes.len(), num_runs, num_rle_values);
        out.extend(&rle_bytes);
    } else {
        let bytes = encode_varint_u32_stream(&geom_types);
        write_stream_meta(out, STREAM_LENGTH, LENGTH_VAR_BINARY, LOG_NONE, LOG_NONE, PHYS_VARINT, n, bytes.len());
        out.extend(&bytes);
    }

    // Collect topology and vertex data
    let mut num_geometries: Vec<u32> = Vec::new();
    let mut num_parts: Vec<u32> = Vec::new();
    let mut num_rings: Vec<u32> = Vec::new();
    let mut vertices_x: Vec<i32> = Vec::new();
    let mut vertices_y: Vec<i32> = Vec::new();

    for feature in features {
        collect_geometry_data(
            &feature.geometry,
            west, south, east, north,
            &mut num_geometries,
            &mut num_parts,
            &mut num_rings,
            &mut vertices_x,
            &mut vertices_y,
        );
    }

    // 2. NumGeometries stream (integer RLE with two-buffer format when beneficial)
    if !num_geometries.is_empty() {
        let runs = count_runs(&num_geometries);
        if runs * 2 < num_geometries.len() {
            let (rle_bytes, num_runs, num_rle_values) = integer_rle_encode_u32(&num_geometries);
            write_stream_meta_rle(out, STREAM_LENGTH, LENGTH_GEOMETRIES, LOG_RLE, LOG_NONE, PHYS_VARINT, num_runs * 2, rle_bytes.len(), num_runs, num_rle_values);
            out.extend(&rle_bytes);
        } else {
            let bytes = encode_varint_u32_stream(&num_geometries);
            write_stream_meta(out, STREAM_LENGTH, LENGTH_GEOMETRIES, LOG_NONE, LOG_NONE, PHYS_VARINT, num_geometries.len(), bytes.len());
            out.extend(&bytes);
        }
    }

    // 3. NumParts stream (integer RLE with two-buffer format when beneficial)
    if !num_parts.is_empty() {
        let runs = count_runs(&num_parts);
        if runs * 2 < num_parts.len() {
            let (rle_bytes, num_runs, num_rle_values) = integer_rle_encode_u32(&num_parts);
            write_stream_meta_rle(out, STREAM_LENGTH, LENGTH_PARTS, LOG_RLE, LOG_NONE, PHYS_VARINT, num_runs * 2, rle_bytes.len(), num_runs, num_rle_values);
            out.extend(&rle_bytes);
        } else {
            let bytes = encode_varint_u32_stream(&num_parts);
            write_stream_meta(out, STREAM_LENGTH, LENGTH_PARTS, LOG_NONE, LOG_NONE, PHYS_VARINT, num_parts.len(), bytes.len());
            out.extend(&bytes);
        }
    }

    // 4. NumRings stream (integer RLE with two-buffer format when beneficial)
    if !num_rings.is_empty() {
        let runs = count_runs(&num_rings);
        if runs * 2 < num_rings.len() {
            let (rle_bytes, num_runs, num_rle_values) = integer_rle_encode_u32(&num_rings);
            write_stream_meta_rle(out, STREAM_LENGTH, LENGTH_RINGS, LOG_RLE, LOG_NONE, PHYS_VARINT, num_runs * 2, rle_bytes.len(), num_runs, num_rle_values);
            out.extend(&rle_bytes);
        } else {
            let bytes = encode_varint_u32_stream(&num_rings);
            write_stream_meta(out, STREAM_LENGTH, LENGTH_RINGS, LOG_NONE, LOG_NONE, PHYS_VARINT, num_rings.len(), bytes.len());
            out.extend(&bytes);
        }
    }

    // 5. Vertex buffer - interleaved x, y with componentwise delta
    if !vertices_x.is_empty() {
        let total_vertices = vertices_x.len();
        // Delta encode X and Y separately, then interleave
        let dx = delta_encode_i32(&vertices_x);
        let dy = delta_encode_i32(&vertices_y);
        let mut interleaved = Vec::with_capacity(dx.len() + dy.len());
        for i in 0..dx.len() {
            interleaved.push(dx[i]);
            interleaved.push(dy[i]);
        }
        let bytes = encode_zigzag_varint_i32_stream(&interleaved);
        write_stream_meta(out, STREAM_DATA, DATA_VERTEX, LOG_COMPONENTWISE_DELTA, LOG_NONE, PHYS_VARINT, total_vertices * 2, bytes.len());
        out.extend(&bytes);
    }
}

fn geometry_type_byte(geom: &Geometry) -> u8 {
    match geom {
        Geometry::Point(_) => GEOM_POINT,
        Geometry::MultiPoint(_) => GEOM_MULTI_POINT,
        Geometry::LineString(_) => GEOM_LINESTRING,
        Geometry::MultiLineString(_) => GEOM_MULTI_LINESTRING,
        Geometry::Polygon(_) => GEOM_POLYGON,
        Geometry::MultiPolygon(_) => GEOM_MULTI_POLYGON,
    }
}

fn collect_geometry_data(
    geom: &Geometry,
    west: f64,
    south: f64,
    east: f64,
    north: f64,
    num_geometries: &mut Vec<u32>,
    num_parts: &mut Vec<u32>,
    num_rings: &mut Vec<u32>,
    vertices_x: &mut Vec<i32>,
    vertices_y: &mut Vec<i32>,
) {
    match geom {
        Geometry::Point(p) => {
            let x = lon_to_tile_coord(p.x(), west, east);
            let y = lat_to_tile_coord(p.y(), south, north);
            vertices_x.push(x);
            vertices_y.push(y);
        }
        Geometry::MultiPoint(mp) => {
            num_geometries.push(mp.0.len() as u32);
            for p in &mp.0 {
                let x = lon_to_tile_coord(p.x(), west, east);
                let y = lat_to_tile_coord(p.y(), south, north);
                vertices_x.push(x);
                vertices_y.push(y);
            }
        }
        Geometry::LineString(ls) => {
            num_parts.push(ls.0.len() as u32);
            for c in &ls.0 {
                vertices_x.push(lon_to_tile_coord(c.x, west, east));
                vertices_y.push(lat_to_tile_coord(c.y, south, north));
            }
        }
        Geometry::MultiLineString(mls) => {
            num_geometries.push(mls.0.len() as u32);
            for ls in &mls.0 {
                num_parts.push(ls.0.len() as u32);
                for c in &ls.0 {
                    vertices_x.push(lon_to_tile_coord(c.x, west, east));
                    vertices_y.push(lat_to_tile_coord(c.y, south, north));
                }
            }
        }
        Geometry::Polygon(poly) => {
            let ring_count = 1 + poly.interiors().len();
            num_parts.push(ring_count as u32);
            // Exterior ring
            let ext = poly.exterior();
            let ext_coords: Vec<_> = if ext.0.len() >= 2 && ext.0.first() == ext.0.last() {
                ext.0[..ext.0.len() - 1].to_vec()
            } else {
                ext.0.clone()
            };
            num_rings.push(ext_coords.len() as u32);
            for c in &ext_coords {
                vertices_x.push(lon_to_tile_coord(c.x, west, east));
                vertices_y.push(lat_to_tile_coord(c.y, south, north));
            }
            // Interior rings
            for interior in poly.interiors() {
                let int_coords: Vec<_> = if interior.0.len() >= 2 && interior.0.first() == interior.0.last() {
                    interior.0[..interior.0.len() - 1].to_vec()
                } else {
                    interior.0.clone()
                };
                num_rings.push(int_coords.len() as u32);
                for c in &int_coords {
                    vertices_x.push(lon_to_tile_coord(c.x, west, east));
                    vertices_y.push(lat_to_tile_coord(c.y, south, north));
                }
            }
        }
        Geometry::MultiPolygon(mp) => {
            num_geometries.push(mp.0.len() as u32);
            for poly in &mp.0 {
                let ring_count = 1 + poly.interiors().len();
                num_parts.push(ring_count as u32);
                let ext = poly.exterior();
                let ext_coords: Vec<_> = if ext.0.len() >= 2 && ext.0.first() == ext.0.last() {
                    ext.0[..ext.0.len() - 1].to_vec()
                } else {
                    ext.0.clone()
                };
                num_rings.push(ext_coords.len() as u32);
                for c in &ext_coords {
                    vertices_x.push(lon_to_tile_coord(c.x, west, east));
                    vertices_y.push(lat_to_tile_coord(c.y, south, north));
                }
                for interior in poly.interiors() {
                    let int_coords: Vec<_> = if interior.0.len() >= 2 && interior.0.first() == interior.0.last() {
                        interior.0[..interior.0.len() - 1].to_vec()
                    } else {
                        interior.0.clone()
                    };
                    num_rings.push(int_coords.len() as u32);
                    for c in &int_coords {
                        vertices_x.push(lon_to_tile_coord(c.x, west, east));
                        vertices_y.push(lat_to_tile_coord(c.y, south, north));
                    }
                }
            }
        }
    }
}

/// Check if dictionary encoding would be cheaper for a string column.
fn should_use_dictionary(features: &[Feature], prop_idx: usize) -> bool {
    let col_type = infer_column_type(features, prop_idx);
    if col_type != COL_STR && col_type != COL_OPT_STR {
        return false;
    }

    let mut string_values: Vec<&str> = Vec::new();
    for f in features {
        let val = if prop_idx < f.properties.len() {
            &f.properties[prop_idx]
        } else {
            &PropertyValue::Null
        };
        if let PropertyValue::String(s) = val {
            string_values.push(s.as_str());
        }
    }

    if string_values.is_empty() {
        return false;
    }

    // Raw cost estimate
    let raw_len_bytes: usize = string_values.iter().map(|s| {
        let mut buf = [0u8; 5];
        (s.len() as u32).encode_var(&mut buf)
    }).sum();
    let raw_data_bytes: usize = string_values.iter().map(|s| s.len()).sum();
    let raw_cost = raw_len_bytes + raw_data_bytes + 8;

    // Dictionary cost estimate
    let mut unique_map: HashMap<&str, u32> = HashMap::new();
    let mut dict_entries: Vec<&str> = Vec::new();
    for &s in &string_values {
        if !unique_map.contains_key(s) {
            let idx = dict_entries.len() as u32;
            unique_map.insert(s, idx);
            dict_entries.push(s);
        }
    }

    if dict_entries.len() >= string_values.len() {
        return false; // All unique, no savings
    }

    let dict_data_bytes: usize = dict_entries.iter().map(|s| s.len()).sum();
    let dict_len_bytes: usize = dict_entries.iter().map(|s| {
        let mut buf = [0u8; 5];
        (s.len() as u32).encode_var(&mut buf)
    }).sum();
    let index_bytes: usize = string_values.iter().map(|s| {
        let mut buf = [0u8; 5];
        unique_map[s].encode_var(&mut buf)
    }).sum();
    let dict_cost = dict_len_bytes + dict_data_bytes + index_bytes + 12;

    dict_cost < raw_cost
}

fn encode_property_stream(
    out: &mut Vec<u8>,
    features: &[Feature],
    prop_idx: usize,
) {
    let n = features.len();

    // Check if any nulls
    let has_nulls = features.iter().any(|f| {
        prop_idx >= f.properties.len() || matches!(f.properties[prop_idx], PropertyValue::Null)
    });

    // Write presence bitmap if needed (byte-RLE encoded, matching reference)
    if has_nulls {
        let mut bitmap = Vec::new();
        let mut byte: u8 = 0;
        for (i, f) in features.iter().enumerate() {
            let present = prop_idx < f.properties.len() && !matches!(f.properties[prop_idx], PropertyValue::Null);
            if present {
                byte |= 1 << (i % 8);
            }
            if i % 8 == 7 || i == n - 1 {
                bitmap.push(byte);
                byte = 0;
            }
        }
        // Boolean streams: byte-RLE encode the bitmap, then write with LOG_RLE.
        // The decoder always applies decode_byte_rle on bool streams.
        // For is_bool=true, RLE metadata (runs, num_rle_values) is NOT in the wire format
        // — the decoder computes them from num_values and byte_length.
        let rle_data = byte_rle_encode(&bitmap);
        write_stream_meta(out, STREAM_PRESENT, 0, LOG_RLE, LOG_NONE, PHYS_NONE, n, rle_data.len());
        out.extend(&rle_data);
    }

    // Determine predominant type and write data
    let col_type = infer_column_type(features, prop_idx);
    match col_type {
        COL_STR | COL_OPT_STR => {
            // Collect non-null string values
            let mut string_values: Vec<String> = Vec::new();
            for f in features {
                let val = if prop_idx < f.properties.len() {
                    &f.properties[prop_idx]
                } else {
                    &PropertyValue::Null
                };
                match val {
                    PropertyValue::String(s) => string_values.push(s.clone()),
                    PropertyValue::Null => {}
                    other => string_values.push(format!("{:?}", other)),
                }
            }

            // Estimate raw encoding cost
            let raw_lengths: Vec<u32> = string_values.iter().map(|s| s.len() as u32).collect();
            let raw_len_bytes = encode_varint_u32_stream(&raw_lengths);
            let raw_data_bytes: usize = string_values.iter().map(|s| s.len()).sum();
            let raw_cost = raw_len_bytes.len() + raw_data_bytes + 8; // 2 stream headers × 4 bytes

            // Estimate dictionary encoding cost
            let mut unique_map: HashMap<&str, u32> = HashMap::new();
            let mut dict_entries: Vec<&str> = Vec::new();
            for s in &string_values {
                if !unique_map.contains_key(s.as_str()) {
                    let idx = dict_entries.len() as u32;
                    unique_map.insert(s.as_str(), idx);
                    dict_entries.push(s.as_str());
                }
            }

            let dict_data_bytes: usize = dict_entries.iter().map(|s| s.len()).sum();
            let dict_lengths: Vec<u32> = dict_entries.iter().map(|s| s.len() as u32).collect();
            let dict_len_encoded = encode_varint_u32_stream(&dict_lengths);
            let indices: Vec<u32> = string_values.iter().map(|s| unique_map[s.as_str()]).collect();
            let index_encoded = encode_varint_u32_stream(&indices);
            // 3 streams: dict lengths, dict data, indices (each ~4 bytes header)
            let dict_cost = dict_len_encoded.len() + dict_data_bytes + index_encoded.len() + 12;

            if dict_cost < raw_cost && dict_entries.len() < string_values.len() {
                // Dictionary encoding wins
                // Stream 1: dictionary lengths (LENGTH stream, DICTIONARY subtype)
                write_stream_meta(out, STREAM_LENGTH, LENGTH_DICTIONARY, LOG_NONE, LOG_NONE, PHYS_VARINT, dict_lengths.len(), dict_len_encoded.len());
                out.extend(&dict_len_encoded);
                // Stream 2: dictionary data (DATA stream, SINGLE dictionary type)
                write_stream_meta(out, STREAM_DATA, DATA_SINGLE, LOG_NONE, LOG_NONE, PHYS_NONE, dict_data_bytes, dict_data_bytes);
                out.extend(dict_entries.iter().flat_map(|s| s.as_bytes()).copied().collect::<Vec<u8>>());
                // Stream 3: per-feature indices (OFFSET stream, STRING subtype)
                write_stream_meta(out, STREAM_OFFSET, OFFSET_STRING, LOG_NONE, LOG_NONE, PHYS_VARINT, indices.len(), index_encoded.len());
                out.extend(&index_encoded);
            } else {
                // Raw encoding — same as before
                // Length stream
                write_stream_meta(out, STREAM_LENGTH, LENGTH_VAR_BINARY, LOG_NONE, LOG_NONE, PHYS_VARINT, raw_lengths.len(), raw_len_bytes.len());
                out.extend(&raw_len_bytes);
                // Data stream
                write_stream_meta(out, STREAM_DATA, DATA_NONE, LOG_NONE, LOG_NONE, PHYS_NONE, raw_data_bytes, raw_data_bytes);
                let mut raw_string_data = Vec::with_capacity(raw_data_bytes);
                for s in &string_values {
                    raw_string_data.extend(s.as_bytes());
                }
                out.extend(&raw_string_data);
            }
        }
        COL_I64 | COL_OPT_I64 => {
            let vals: Vec<i64> = features
                .iter()
                .filter_map(|f| {
                    if prop_idx < f.properties.len() {
                        match &f.properties[prop_idx] {
                            PropertyValue::Int(i) => Some(*i),
                            PropertyValue::Double(d) => Some(*d as i64),
                            PropertyValue::Bool(b) => Some(if *b { 1 } else { 0 }),
                            _ => None,
                        }
                    } else {
                        None
                    }
                })
                .collect();
            let bytes = encode_zigzag_varint_i64_stream(&vals);
            write_stream_meta(out, STREAM_DATA, DATA_NONE, LOG_NONE, LOG_NONE, PHYS_VARINT, vals.len(), bytes.len());
            out.extend(&bytes);
        }
        COL_F64 | COL_OPT_F64 => {
            let vals: Vec<f64> = features
                .iter()
                .filter_map(|f| {
                    if prop_idx < f.properties.len() {
                        match &f.properties[prop_idx] {
                            PropertyValue::Double(d) => Some(*d),
                            PropertyValue::Int(i) => Some(*i as f64),
                            _ => None,
                        }
                    } else {
                        None
                    }
                })
                .collect();
            // Write as little-endian f64 bytes
            let mut bytes = Vec::with_capacity(vals.len() * 8);
            for v in &vals {
                bytes.extend(&v.to_le_bytes());
            }
            write_stream_meta(out, STREAM_DATA, DATA_NONE, LOG_NONE, LOG_NONE, PHYS_NONE, vals.len(), bytes.len());
            out.extend(&bytes);
        }
        COL_BOOL | COL_OPT_BOOL => {
            let mut bitmap = Vec::new();
            let mut byte: u8 = 0;
            let mut count = 0usize;
            for f in features {
                if prop_idx < f.properties.len() {
                    if let PropertyValue::Bool(b) = &f.properties[prop_idx] {
                        if *b {
                            byte |= 1 << (count % 8);
                        }
                        count += 1;
                        if count % 8 == 0 {
                            bitmap.push(byte);
                            byte = 0;
                        }
                    }
                }
            }
            if count % 8 != 0 {
                bitmap.push(byte);
            }
            write_stream_meta(out, STREAM_DATA, DATA_NONE, LOG_NONE, LOG_NONE, PHYS_NONE, count, bitmap.len());
            out.extend(&bitmap);
        }
        _ => {}
    }
}

// --- Helper functions ---

fn lon_to_tile_coord(lon: f64, west: f64, east: f64) -> i32 {
    ((lon - west) / (east - west) * EXTENT as f64).round() as i32
}

fn lat_to_tile_coord(lat: f64, south: f64, north: f64) -> i32 {
    // Interpolate in Mercator Y space (not linear latitude) for correct projection
    let lat_merc = lat.to_radians().tan().asinh();
    let south_merc = south.to_radians().tan().asinh();
    let north_merc = north.to_radians().tan().asinh();
    ((north_merc - lat_merc) / (north_merc - south_merc) * EXTENT as f64).round() as i32
}

fn delta_encode_i32(values: &[i32]) -> Vec<i32> {
    let mut result = Vec::with_capacity(values.len());
    let mut prev = 0i32;
    for &v in values {
        result.push(v - prev);
        prev = v;
    }
    result
}

fn write_varint_u32(out: &mut Vec<u8>, value: u32) {
    let mut buf = [0u8; 5];
    let n = value.encode_var(&mut buf);
    out.extend_from_slice(&buf[..n]);
}

fn write_varint_usize(out: &mut Vec<u8>, value: usize) {
    let mut buf = [0u8; 10];
    let n = (value as u64).encode_var(&mut buf);
    out.extend_from_slice(&buf[..n]);
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let mut buf = [0u8; 10];
    let n = (bytes.len() as u64).encode_var(&mut buf);
    out.extend_from_slice(&buf[..n]);
    out.extend_from_slice(bytes);
}

/// Write MLT stream metadata header.
///
/// Byte 0: (physicalStreamType << 4) | logicalSubtype
/// Byte 1: (logicalLevelTechnique1 << 5) | (logicalLevelTechnique2 << 2) | physicalLevelTechnique
/// Then: varint(numValues), varint(byteLength)
fn write_stream_meta(
    out: &mut Vec<u8>,
    physical_stream_type: u8,
    logical_subtype: u8,
    logical_technique1: u8,
    logical_technique2: u8,
    physical_technique: u8,
    num_values: usize,
    byte_length: usize,
) {
    let byte0 = (physical_stream_type << 4) | logical_subtype;
    let byte1 = (logical_technique1 << 5) | (logical_technique2 << 2) | physical_technique;
    out.push(byte0);
    out.push(byte1);
    write_varint_usize(out, num_values);
    write_varint_usize(out, byte_length);
}

fn encode_varint_u32_stream(values: &[u32]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        let mut buf = [0u8; 5];
        let n = v.encode_var(&mut buf);
        out.extend_from_slice(&buf[..n]);
    }
    out
}

fn encode_varint_u64_stream(values: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        let mut buf = [0u8; 10];
        let n = v.encode_var(&mut buf);
        out.extend_from_slice(&buf[..n]);
    }
    out
}

fn encode_zigzag_varint_i32_stream(values: &[i32]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        let mut buf = [0u8; 5];
        let n = v.encode_var(&mut buf);
        out.extend_from_slice(&buf[..n]);
    }
    out
}

fn encode_zigzag_varint_i64_stream(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        let mut buf = [0u8; 10];
        let n = v.encode_var(&mut buf);
        out.extend_from_slice(&buf[..n]);
    }
    out
}
