use integer_encoding::VarInt;
use std::collections::HashMap;

use crate::tiler::{tile_bounds, Feature, Geometry, PropertyValue, TileCoord};

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
const STREAM_DATA: u8 = 1; // ordinal 1
const STREAM_OFFSET: u8 = 2; // ordinal 2
const STREAM_LENGTH: u8 = 3; // ordinal 3

// DictionaryType ordinals (lower nibble of byte 0, when stream type = DATA)
// Enum order: NONE=0, SINGLE=1, SHARED=2, VERTEX=3, MORTON=4, FSST=5
const DATA_NONE: u8 = 0; // DictionaryType.NONE
const DATA_SINGLE: u8 = 1; // DictionaryType.SINGLE
const DATA_VERTEX: u8 = 3; // DictionaryType.VERTEX
#[allow(dead_code)] // used only with fsst feature
const DATA_FSST: u8 = 5; // DictionaryType.FSST

// OffsetType ordinals (lower nibble of byte 0, when stream type = OFFSET)
// Enum order: VERTEX=0, INDEX=1, STRING=2, KEY=3
const OFFSET_STRING: u8 = 2;

// LengthType ordinals (lower nibble of byte 0, when stream type = LENGTH)
// Enum order: VAR_BINARY=0, GEOMETRIES=1, PARTS=2, RINGS=3, TRIANGLES=4, SYMBOL=5, DICTIONARY=6
const LENGTH_VAR_BINARY: u8 = 0;
const LENGTH_GEOMETRIES: u8 = 1;
const LENGTH_PARTS: u8 = 2;
const LENGTH_RINGS: u8 = 3;
#[allow(dead_code)] // used only with fsst feature
const LENGTH_SYMBOL: u8 = 5;
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
#[allow(dead_code)] // used only with fastpfor feature
const PHYS_FAST_PFOR: u8 = 1;
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
        let ids: Vec<u64> = features.iter().map(|f| f.id.unwrap_or(0)).collect();
        // Delta encode: output differences between consecutive IDs
        let mut deltas = Vec::with_capacity(ids.len());
        let mut prev = 0u64;
        for &id in &ids {
            deltas.push(id.wrapping_sub(prev));
            prev = id;
        }
        let id_bytes = encode_varint_u64_stream(&deltas);
        write_stream_meta(
            &mut layer_data,
            STREAM_DATA,
            DATA_NONE,
            LOG_DELTA,
            LOG_NONE,
            PHYS_VARINT,
            ids.len(),
            id_bytes.len(),
        );
        layer_data.extend(&id_bytes);
    }

    // --- Geometry streams ---
    // Write geometry stream count before the streams (spec requirement)
    let geom_stream_count = count_geometry_streams(features);
    write_varint_usize(&mut layer_data, geom_stream_count);

    encode_geometry_streams(&mut layer_data, features, west, south, east, north);

    // --- Property streams ---
    for (i, _name) in property_names.iter().enumerate() {
        let col_type = infer_column_type(features, i);
        // STRING columns need a stream count varint (hasStreamCount = true)
        if col_type == COL_STR || col_type == COL_OPT_STR {
            let has_nulls = features
                .iter()
                .any(|f| i >= f.properties.len() || matches!(f.properties[i], PropertyValue::Null));
            let use_dict = should_use_dictionary(features, i);
            // presence stream (if nullable) + encoding streams
            // dictionary: 3 streams (dict lengths, dict data, indices)
            // dictionary + FSST: 5 streams (sym lengths, sym data, dict lengths, dict data, indices)
            // raw: 2 streams (lengths, data)
            let encoding_streams: usize = if use_dict {
                if will_use_fsst(features, i) {
                    5
                } else {
                    3
                }
            } else {
                2
            };
            let stream_count = if has_nulls {
                encoding_streams + 1
            } else {
                encoding_streams
            };
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
        if has_null {
            COL_OPT_STR
        } else {
            COL_STR
        }
    } else if has_double {
        if has_null {
            COL_OPT_F64
        } else {
            COL_F64
        }
    } else if has_int {
        if has_null {
            COL_OPT_I64
        } else {
            COL_I64
        }
    } else if has_bool {
        if has_null {
            COL_OPT_BOOL
        } else {
            COL_BOOL
        }
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
            Geometry::MultiPoint(_) => {
                has_multi = true;
            }
            Geometry::LineString(_) => {
                has_parts = true;
            }
            Geometry::MultiLineString(_) => {
                has_multi = true;
                has_parts = true;
            }
            Geometry::Polygon(_) => {
                has_parts = true;
                has_rings = true;
            }
            Geometry::MultiPolygon(_) => {
                has_multi = true;
                has_parts = true;
                has_rings = true;
            }
        }
    }

    let mut count = 2; // geom_type + vertex
    if has_multi {
        count += 1;
    }
    if has_parts {
        count += 1;
    }
    if has_rings {
        count += 1;
    }
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
    let geom_types: Vec<u32> = features
        .iter()
        .map(|f| geometry_type_byte(&f.geometry) as u32)
        .collect();
    let geom_type_runs = count_runs(&geom_types);
    if geom_type_runs * 2 < geom_types.len() {
        // Integer RLE: two-buffer [run_lengths..., values...] as varints
        // num_values = physical buffer count (num_runs * 2), not logical feature count
        let (rle_bytes, num_runs, num_rle_values) = integer_rle_encode_u32(&geom_types);
        write_stream_meta_rle(
            out,
            STREAM_LENGTH,
            LENGTH_VAR_BINARY,
            LOG_RLE,
            LOG_NONE,
            PHYS_VARINT,
            num_runs * 2,
            rle_bytes.len(),
            num_runs,
            num_rle_values,
        );
        out.extend(&rle_bytes);
    } else {
        let bytes = encode_varint_u32_stream(&geom_types);
        write_stream_meta(
            out,
            STREAM_LENGTH,
            LENGTH_VAR_BINARY,
            LOG_NONE,
            LOG_NONE,
            PHYS_VARINT,
            n,
            bytes.len(),
        );
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
            west,
            south,
            east,
            north,
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
            write_stream_meta_rle(
                out,
                STREAM_LENGTH,
                LENGTH_GEOMETRIES,
                LOG_RLE,
                LOG_NONE,
                PHYS_VARINT,
                num_runs * 2,
                rle_bytes.len(),
                num_runs,
                num_rle_values,
            );
            out.extend(&rle_bytes);
        } else {
            let (bytes, phys) = encode_u32_stream_best(&num_geometries);
            write_stream_meta(
                out,
                STREAM_LENGTH,
                LENGTH_GEOMETRIES,
                LOG_NONE,
                LOG_NONE,
                phys,
                num_geometries.len(),
                bytes.len(),
            );
            out.extend(&bytes);
        }
    }

    // 3. NumParts stream (integer RLE with two-buffer format when beneficial)
    if !num_parts.is_empty() {
        let runs = count_runs(&num_parts);
        if runs * 2 < num_parts.len() {
            let (rle_bytes, num_runs, num_rle_values) = integer_rle_encode_u32(&num_parts);
            write_stream_meta_rle(
                out,
                STREAM_LENGTH,
                LENGTH_PARTS,
                LOG_RLE,
                LOG_NONE,
                PHYS_VARINT,
                num_runs * 2,
                rle_bytes.len(),
                num_runs,
                num_rle_values,
            );
            out.extend(&rle_bytes);
        } else {
            let (bytes, phys) = encode_u32_stream_best(&num_parts);
            write_stream_meta(
                out,
                STREAM_LENGTH,
                LENGTH_PARTS,
                LOG_NONE,
                LOG_NONE,
                phys,
                num_parts.len(),
                bytes.len(),
            );
            out.extend(&bytes);
        }
    }

    // 4. NumRings stream (integer RLE with two-buffer format when beneficial)
    if !num_rings.is_empty() {
        let runs = count_runs(&num_rings);
        if runs * 2 < num_rings.len() {
            let (rle_bytes, num_runs, num_rle_values) = integer_rle_encode_u32(&num_rings);
            write_stream_meta_rle(
                out,
                STREAM_LENGTH,
                LENGTH_RINGS,
                LOG_RLE,
                LOG_NONE,
                PHYS_VARINT,
                num_runs * 2,
                rle_bytes.len(),
                num_runs,
                num_rle_values,
            );
            out.extend(&rle_bytes);
        } else {
            let (bytes, phys) = encode_u32_stream_best(&num_rings);
            write_stream_meta(
                out,
                STREAM_LENGTH,
                LENGTH_RINGS,
                LOG_NONE,
                LOG_NONE,
                phys,
                num_rings.len(),
                bytes.len(),
            );
            out.extend(&bytes);
        }
    }

    // 5. Vertex buffer - interleaved x, y with componentwise delta
    if !vertices_x.is_empty() {
        let total_vertices = vertices_x.len();
        // Delta encode X and Y separately, then interleave
        let dx = delta_encode_i32(&vertices_x);
        let dy = delta_encode_i32(&vertices_y);
        let mut interleaved_zigzag = Vec::with_capacity(dx.len() + dy.len());
        for i in 0..dx.len() {
            interleaved_zigzag.push(zigzag_encode_i32(dx[i]));
            interleaved_zigzag.push(zigzag_encode_i32(dy[i]));
        }
        let (bytes, phys) = encode_u32_stream_best(&interleaved_zigzag);
        write_stream_meta(
            out,
            STREAM_DATA,
            DATA_VERTEX,
            LOG_COMPONENTWISE_DELTA,
            LOG_NONE,
            phys,
            total_vertices * 2,
            bytes.len(),
        );
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
                let int_coords: Vec<_> =
                    if interior.0.len() >= 2 && interior.0.first() == interior.0.last() {
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
                    let int_coords: Vec<_> =
                        if interior.0.len() >= 2 && interior.0.first() == interior.0.last() {
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

fn property_value_as_string(value: &PropertyValue) -> Option<String> {
    match value {
        PropertyValue::String(s) => Some(s.clone()),
        PropertyValue::Int(v) => Some(v.to_string()),
        PropertyValue::Double(v) => Some(v.to_string()),
        PropertyValue::Bool(v) => Some(v.to_string()),
        PropertyValue::Null => None,
    }
}

/// Collect non-null string values for a property column. This must stay aligned
/// with `encode_property_stream` so stream-count prediction matches the payload.
fn collect_string_values(features: &[Feature], prop_idx: usize) -> Vec<String> {
    features
        .iter()
        .filter_map(|f| {
            let value = f.properties.get(prop_idx).unwrap_or(&PropertyValue::Null);
            property_value_as_string(value)
        })
        .collect()
}

/// Check if dictionary encoding would be cheaper for a string column.
/// Mirrors the logic in encode_property_stream to avoid duplication.
fn should_use_dictionary(features: &[Feature], prop_idx: usize) -> bool {
    let col_type = infer_column_type(features, prop_idx);
    if col_type != COL_STR && col_type != COL_OPT_STR {
        return false;
    }

    let string_values = collect_string_values(features, prop_idx);

    if string_values.is_empty() {
        return false;
    }

    // Raw cost estimate
    let raw_len_bytes: usize = string_values
        .iter()
        .map(|s| {
            let mut buf = [0u8; 5];
            (s.len() as u32).encode_var(&mut buf)
        })
        .sum();
    let raw_data_bytes: usize = string_values.iter().map(|s| s.len()).sum();
    let raw_cost = raw_len_bytes + raw_data_bytes + 8;

    // Dictionary cost estimate
    let mut unique_map: HashMap<&str, u32> = HashMap::new();
    let mut dict_entries: Vec<&str> = Vec::new();
    for s in &string_values {
        if !unique_map.contains_key(s.as_str()) {
            let idx = dict_entries.len() as u32;
            unique_map.insert(s.as_str(), idx);
            dict_entries.push(s.as_str());
        }
    }

    if dict_entries.len() >= string_values.len() {
        return false; // All unique, no savings
    }

    let dict_data_bytes: usize = dict_entries.iter().map(|s| s.len()).sum();
    let dict_len_bytes: usize = dict_entries
        .iter()
        .map(|s| {
            let mut buf = [0u8; 5];
            (s.len() as u32).encode_var(&mut buf)
        })
        .sum();
    let index_bytes: usize = string_values
        .iter()
        .map(|s| {
            let mut buf = [0u8; 5];
            unique_map[s.as_str()].encode_var(&mut buf)
        })
        .sum();
    let dict_cost = dict_len_bytes + dict_data_bytes + index_bytes + 12;

    dict_cost < raw_cost
}

fn encode_property_stream(out: &mut Vec<u8>, features: &[Feature], prop_idx: usize) {
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
            let present = prop_idx < f.properties.len()
                && !matches!(f.properties[prop_idx], PropertyValue::Null);
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
        write_stream_meta(
            out,
            STREAM_PRESENT,
            0,
            LOG_RLE,
            LOG_NONE,
            PHYS_NONE,
            n,
            rle_data.len(),
        );
        out.extend(&rle_data);
    }

    // Determine predominant type and write data
    let col_type = infer_column_type(features, prop_idx);
    match col_type {
        COL_STR | COL_OPT_STR => {
            let string_values = collect_string_values(features, prop_idx);

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
            let indices: Vec<u32> = string_values
                .iter()
                .map(|s| unique_map[s.as_str()])
                .collect();
            let index_encoded = encode_varint_u32_stream(&indices);
            // 3 streams: dict lengths, dict data, indices (each ~4 bytes header)
            let dict_cost = dict_len_encoded.len() + dict_data_bytes + index_encoded.len() + 12;

            if dict_cost < raw_cost && dict_entries.len() < string_values.len() {
                // Dictionary encoding wins
                encode_dictionary_streams(
                    out,
                    &dict_entries,
                    &dict_lengths,
                    dict_data_bytes,
                    &indices,
                );
            } else {
                // Raw encoding
                let (len_bytes, len_phys) = encode_u32_stream_best(&raw_lengths);
                write_stream_meta(
                    out,
                    STREAM_LENGTH,
                    LENGTH_VAR_BINARY,
                    LOG_NONE,
                    LOG_NONE,
                    len_phys,
                    raw_lengths.len(),
                    len_bytes.len(),
                );
                out.extend(&len_bytes);
                // num_values = number of strings, byte_length = total concatenated byte count
                write_stream_meta(
                    out,
                    STREAM_DATA,
                    DATA_NONE,
                    LOG_NONE,
                    LOG_NONE,
                    PHYS_NONE,
                    string_values.len(),
                    raw_data_bytes,
                );
                let mut raw_string_data = Vec::with_capacity(raw_data_bytes);
                for s in &string_values {
                    raw_string_data.extend(s.as_bytes());
                }
                out.extend(&raw_string_data);
            }
        }
        COL_I64 | COL_OPT_I64 => {
            // Note: mlt-core does not support FastPFOR for i64/u64 streams,
            // so we always use varint encoding here regardless of value range.
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
            write_stream_meta(
                out,
                STREAM_DATA,
                DATA_NONE,
                LOG_NONE,
                LOG_NONE,
                PHYS_VARINT,
                vals.len(),
                bytes.len(),
            );
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
            write_stream_meta(
                out,
                STREAM_DATA,
                DATA_NONE,
                LOG_NONE,
                LOG_NONE,
                PHYS_NONE,
                vals.len(),
                bytes.len(),
            );
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
            write_stream_meta(
                out,
                STREAM_DATA,
                DATA_NONE,
                LOG_NONE,
                LOG_NONE,
                PHYS_NONE,
                count,
                bitmap.len(),
            );
            out.extend(&bitmap);
        }
        _ => {}
    }
}

/// Check if FSST will be applied for a dictionary-encoded string column.
/// Must mirror the decision in encode_dictionary_streams.
fn will_use_fsst(features: &[Feature], prop_idx: usize) -> bool {
    #[cfg(not(feature = "fsst"))]
    {
        let _ = (features, prop_idx);
        return false;
    }

    #[cfg(feature = "fsst")]
    {
        let string_values = collect_string_values(features, prop_idx);
        let mut unique_map: HashMap<&str, u32> = HashMap::new();
        let mut dict_entries: Vec<&str> = Vec::new();
        for s in &string_values {
            if !unique_map.contains_key(s.as_str()) {
                let idx = dict_entries.len() as u32;
                unique_map.insert(s.as_str(), idx);
                dict_entries.push(s.as_str());
            }
        }
        try_fsst_dictionary(&dict_entries).is_some()
    }
}

/// Write dictionary encoding streams, with optional FSST compression.
fn encode_dictionary_streams(
    out: &mut Vec<u8>,
    dict_entries: &[&str],
    dict_lengths: &[u32],
    dict_data_bytes: usize,
    indices: &[u32],
) {
    // Try FSST when feature is enabled
    #[cfg(feature = "fsst")]
    if let Some(fsst_enc) = try_fsst_dictionary(dict_entries) {
        // FSST + dictionary: 5 streams
        // Stream 1: symbol lengths (LENGTH/SYMBOL)
        let (sym_len_bytes, sym_len_phys) = encode_u32_stream_best(&fsst_enc.symbol_lengths);
        write_stream_meta(
            out,
            STREAM_LENGTH,
            LENGTH_SYMBOL,
            LOG_NONE,
            LOG_NONE,
            sym_len_phys,
            fsst_enc.symbol_lengths.len(),
            sym_len_bytes.len(),
        );
        out.extend(&sym_len_bytes);
        // Stream 2: symbol table data (DATA/FSST)
        write_stream_meta(
            out,
            STREAM_DATA,
            DATA_FSST,
            LOG_NONE,
            LOG_NONE,
            PHYS_NONE,
            fsst_enc.symbol_lengths.len(),
            fsst_enc.symbol_data.len(),
        );
        out.extend(&fsst_enc.symbol_data);
        // Stream 3: original UTF-8 byte lengths per dict entry (LENGTH/DICTIONARY)
        let (val_len_bytes, val_len_phys) = encode_u32_stream_best(&fsst_enc.value_lengths);
        write_stream_meta(
            out,
            STREAM_LENGTH,
            LENGTH_DICTIONARY,
            LOG_NONE,
            LOG_NONE,
            val_len_phys,
            fsst_enc.value_lengths.len(),
            val_len_bytes.len(),
        );
        out.extend(&val_len_bytes);
        // Stream 4: FSST-compressed dictionary data (DATA/SINGLE)
        write_stream_meta(
            out,
            STREAM_DATA,
            DATA_SINGLE,
            LOG_NONE,
            LOG_NONE,
            PHYS_NONE,
            dict_entries.len(),
            fsst_enc.compressed_data.len(),
        );
        out.extend(&fsst_enc.compressed_data);
        // Stream 5: per-feature indices (OFFSET/STRING)
        let (idx_bytes, idx_phys) = encode_u32_stream_best(indices);
        write_stream_meta(
            out,
            STREAM_OFFSET,
            OFFSET_STRING,
            LOG_NONE,
            LOG_NONE,
            idx_phys,
            indices.len(),
            idx_bytes.len(),
        );
        out.extend(&idx_bytes);
        return;
    }

    // Plain dictionary encoding: 3 streams
    let (dl_bytes, dl_phys) = encode_u32_stream_best(dict_lengths);
    write_stream_meta(
        out,
        STREAM_LENGTH,
        LENGTH_DICTIONARY,
        LOG_NONE,
        LOG_NONE,
        dl_phys,
        dict_lengths.len(),
        dl_bytes.len(),
    );
    out.extend(&dl_bytes);
    // num_values = number of dictionary entries, byte_length = total concatenated byte count
    write_stream_meta(
        out,
        STREAM_DATA,
        DATA_SINGLE,
        LOG_NONE,
        LOG_NONE,
        PHYS_NONE,
        dict_entries.len(),
        dict_data_bytes,
    );
    out.extend(
        dict_entries
            .iter()
            .flat_map(|s| s.as_bytes())
            .copied()
            .collect::<Vec<u8>>(),
    );
    let (idx_bytes, idx_phys) = encode_u32_stream_best(indices);
    write_stream_meta(
        out,
        STREAM_OFFSET,
        OFFSET_STRING,
        LOG_NONE,
        LOG_NONE,
        idx_phys,
        indices.len(),
        idx_bytes.len(),
    );
    out.extend(&idx_bytes);
}

// --- Zigzag encoding (i32 → u32, no varint) ---

fn zigzag_encode_i32(v: i32) -> u32 {
    ((v << 1) ^ (v >> 31)) as u32
}

#[allow(dead_code)] // reserved for future use with i64 FastPFOR if mlt-core adds support
fn zigzag_encode_i64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

// --- FastPFOR encoding ---

/// Encode a u32 slice using FastPFOR256 composite codec.
/// Wire format: big-endian u32 words (matching mlt-core decoder).
#[cfg(feature = "fastpfor")]
fn encode_fastpfor_u32(values: &[u32]) -> Option<Vec<u8>> {
    use fastpfor::cpp::{Codec32 as _, FastPFor256Codec};

    if values.is_empty() {
        return Some(Vec::new());
    }
    let codec = FastPFor256Codec::new();
    let mut compressed = vec![0u32; values.len() + 1024];
    let out = codec.encode32(values, &mut compressed).ok()?;
    let mut data = Vec::with_capacity(out.len() * 4);
    for word in out {
        data.extend_from_slice(&word.to_be_bytes());
    }
    Some(data)
}

/// Encode u32 stream using best available technique.
/// Tries FastPFOR when feature is enabled and len >= 128, falls back to varint.
#[cfg(feature = "fastpfor")]
fn encode_u32_stream_best(values: &[u32]) -> (Vec<u8>, u8) {
    if values.len() >= 128 {
        if let Some(fpfor) = encode_fastpfor_u32(values) {
            let varint = encode_varint_u32_stream(values);
            if fpfor.len() < varint.len() {
                return (fpfor, PHYS_FAST_PFOR);
            }
        }
    }
    (encode_varint_u32_stream(values), PHYS_VARINT)
}

#[cfg(not(feature = "fastpfor"))]
fn encode_u32_stream_best(values: &[u32]) -> (Vec<u8>, u8) {
    (encode_varint_u32_stream(values), PHYS_VARINT)
}

// --- FSST encoding ---

/// Result of FSST-compressing dictionary entries.
#[cfg(feature = "fsst")]
struct FsstEncoded {
    symbol_lengths: Vec<u32>,
    symbol_data: Vec<u8>,
    value_lengths: Vec<u32>,
    compressed_data: Vec<u8>,
}

/// Try to FSST-compress dictionary entries. Returns None if thresholds
/// not met or compression doesn't help.
#[cfg(feature = "fsst")]
fn try_fsst_dictionary(dict_entries: &[&str]) -> Option<FsstEncoded> {
    use fsst::Compressor;

    let total_bytes: usize = dict_entries.iter().map(|s| s.len()).sum();
    if dict_entries.len() < 32 || total_bytes < 4096 {
        return None;
    }

    let byte_slices: Vec<&[u8]> = dict_entries.iter().map(|s| s.as_bytes()).collect();
    let compressor = Compressor::train(&byte_slices);

    // Build symbol table
    let symbols = compressor.symbol_table();
    let symbol_lengths_u8 = compressor.symbol_lengths();
    let mut symbol_data = Vec::new();
    for sym in symbols {
        let bytes = sym.to_u64().to_le_bytes();
        let len = sym.len();
        symbol_data.extend_from_slice(&bytes[..len]);
    }
    let symbol_lengths: Vec<u32> = symbol_lengths_u8
        .iter()
        .take(symbols.len())
        .map(|&l| u32::from(l))
        .collect();

    // Compress each dictionary entry
    let mut compressed_data = Vec::new();
    for &entry in dict_entries {
        let comp = compressor.compress(entry.as_bytes());
        compressed_data.extend(comp);
    }

    // Original UTF-8 byte lengths
    let value_lengths: Vec<u32> = dict_entries.iter().map(|s| s.len() as u32).collect();

    // Only use FSST if it actually saves space.
    // The FSST path adds two extra streams (symbol lengths + symbol data) vs plain dict,
    // each with ~6 bytes of stream header overhead. Account for the encoded symbol
    // lengths too (varint-encoded u32 per symbol).
    let sym_len_encoded_size: usize = symbol_lengths
        .iter()
        .map(|&l| {
            let mut buf = [0u8; 5];
            l.encode_var(&mut buf)
        })
        .sum();
    let fsst_overhead = symbol_data.len() + sym_len_encoded_size + 12; // ~6 bytes per extra stream header
    if compressed_data.len() + fsst_overhead >= total_bytes {
        return None;
    }

    Some(FsstEncoded {
        symbol_lengths,
        symbol_data,
        value_lengths,
        compressed_data,
    })
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

#[allow(dead_code)] // was used for vertex encoding before FastPFOR integration
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- Byte-RLE decoder ported from upstream mlt-core reference ---
    // Source: maplibre/maplibre-tile-spec rust/mlt-core/src/utils/decode.rs
    // decode_byte_rle()
    fn byte_rle_decode(input: &[u8], num_bytes: usize) -> Vec<u8> {
        let mut output = Vec::with_capacity(num_bytes);
        let mut pos = 0;
        while output.len() < num_bytes && pos < input.len() {
            let control = input[pos];
            pos += 1;
            if control >= 128 {
                // Literals: (control ^ 0xFF) + 1 bytes
                let count = usize::from(control ^ 0xFF) + 1;
                output.extend_from_slice(&input[pos..pos + count]);
                pos += count;
            } else {
                // Run: (control + 3) copies of next byte
                let count = usize::from(control) + 3;
                let value = input[pos];
                pos += 1;
                output.extend(std::iter::repeat_n(value, count));
            }
        }
        output
    }

    #[test]
    fn test_byte_rle_uniform_run() {
        // 10 identical bytes should encode as one run
        let input = vec![5u8; 10];
        let encoded = byte_rle_encode(&input);
        let decoded = byte_rle_decode(&encoded, input.len());
        assert_eq!(decoded, input);
        // Should be 2 bytes: control (10-3=7) + value (5)
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0], 7); // 10-3 = 7
        assert_eq!(encoded[1], 5);
    }

    #[test]
    fn test_byte_rle_minimum_run() {
        // 3 identical bytes = minimum run (control = 0)
        let input = vec![42u8; 3];
        let encoded = byte_rle_encode(&input);
        let decoded = byte_rle_decode(&encoded, input.len());
        assert_eq!(decoded, input);
        assert_eq!(encoded[0], 0); // 3-3 = 0
        assert_eq!(encoded[1], 42);
    }

    #[test]
    fn test_byte_rle_max_run() {
        // 130 identical bytes = max run (control = 127)
        let input = vec![1u8; 130];
        let encoded = byte_rle_encode(&input);
        let decoded = byte_rle_decode(&encoded, input.len());
        assert_eq!(decoded, input);
        assert_eq!(encoded[0], 127); // 130-3 = 127
    }

    #[test]
    fn test_byte_rle_literals() {
        // 2 different bytes = literals
        let input = vec![1u8, 2];
        let encoded = byte_rle_encode(&input);
        let decoded = byte_rle_decode(&encoded, input.len());
        assert_eq!(decoded, input);
        // Control: 256 - 2 = 254
        assert_eq!(encoded[0], 254u8);
        assert_eq!(&encoded[1..], &[1, 2]);
    }

    #[test]
    fn test_byte_rle_mixed() {
        // Literals followed by a run
        let input = vec![1, 2, 3, 3, 3, 3, 3];
        let encoded = byte_rle_encode(&input);
        let decoded = byte_rle_decode(&encoded, input.len());
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_byte_rle_large_uniform() {
        // 200 identical bytes: splits into run of 130 + run of 70
        let input = vec![9u8; 200];
        let encoded = byte_rle_encode(&input);
        let decoded = byte_rle_decode(&encoded, input.len());
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_byte_rle_roundtrip_geom_types() {
        // Simulate a tile with 50 polygons (all type 2)
        let input: Vec<u8> = vec![GEOM_POLYGON; 50];
        let encoded = byte_rle_encode(&input);
        let decoded = byte_rle_decode(&encoded, input.len());
        assert_eq!(decoded, input);
        assert!(encoded.len() < input.len()); // Must compress
    }

    // --- Integer RLE decoder ported from upstream mlt-core reference ---
    // Source: maplibre/maplibre-tile-spec rust/mlt-core/src/utils/decode.rs
    // decode_rle() — adapted for u32 varints
    fn integer_rle_decode_u32(encoded: &[u8], num_runs: usize, num_rle_values: usize) -> Vec<u32> {
        // First decode all varints from the byte buffer
        let mut data: Vec<u32> = Vec::new();
        let mut offset = 0;
        while offset < encoded.len() {
            let (val, bytes_read): (u32, usize) = u32::decode_var(&encoded[offset..]).unwrap();
            data.push(val);
            offset += bytes_read;
        }
        // Reference: data.split_at(runs) → run_lens, values
        let (run_lens, values) = data.split_at(num_runs);
        let mut result = Vec::with_capacity(num_rle_values);
        for (&run, &val) in run_lens.iter().zip(values.iter()) {
            result.extend(std::iter::repeat_n(val, run as usize));
        }
        result
    }

    #[test]
    fn test_integer_rle_uniform() {
        // 100 identical values → 1 run
        let input = vec![42u32; 100];
        let (encoded, num_runs, num_rle_values) = integer_rle_encode_u32(&input);
        assert_eq!(num_runs, 1);
        assert_eq!(num_rle_values, 100);
        let decoded = integer_rle_decode_u32(&encoded, num_runs, num_rle_values);
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_integer_rle_two_runs() {
        // 50 of value 1, then 50 of value 2
        let mut input = vec![1u32; 50];
        input.extend(vec![2u32; 50]);
        let (encoded, num_runs, num_rle_values) = integer_rle_encode_u32(&input);
        assert_eq!(num_runs, 2);
        assert_eq!(num_rle_values, 100);
        let decoded = integer_rle_decode_u32(&encoded, num_runs, num_rle_values);
        assert_eq!(decoded, input);
    }

    #[test]
    fn test_integer_rle_topology_stream() {
        // Typical: all simple polygons, 1 ring each
        let input = vec![1u32; 200];
        let (encoded, num_runs, num_rle_values) = integer_rle_encode_u32(&input);
        assert_eq!(num_runs, 1);
        assert_eq!(num_rle_values, 200);
        let decoded = integer_rle_decode_u32(&encoded, num_runs, num_rle_values);
        assert_eq!(decoded, input);
        // Should be very small: varint(200) + varint(1) = ~2-3 bytes
        assert!(encoded.len() <= 4);
    }

    // --- Stream metadata format validation ---

    #[test]
    fn test_stream_meta_rle_format() {
        let mut buf = Vec::new();
        write_stream_meta_rle(
            &mut buf,
            STREAM_LENGTH, // physicalStreamType
            LENGTH_PARTS,  // logicalSubtype
            LOG_RLE,       // logicalTechnique1
            LOG_NONE,      // logicalTechnique2
            PHYS_VARINT,   // physicalTechnique
            100,           // numValues
            5,             // byteLength
            1,             // numRuns
            100,           // numRleValues
        );
        // byte 0: (STREAM_LENGTH << 4) | LENGTH_PARTS = (3 << 4) | 2 = 0x32
        assert_eq!(buf[0], 0x32);
        // byte 1: (LOG_RLE << 5) | (LOG_NONE << 2) | PHYS_VARINT = (3 << 5) | 0 | 2 = 0x62
        assert_eq!(buf[1], 0x62);
        // Then: varint(100), varint(5), varint(1), varint(100)
        let mut offset = 2;
        let (v, n): (u64, _) = u64::decode_var(&buf[offset..]).unwrap();
        assert_eq!(v, 100); // numValues
        offset += n;
        let (v, n): (u64, _) = u64::decode_var(&buf[offset..]).unwrap();
        assert_eq!(v, 5); // byteLength
        offset += n;
        let (v, n): (u64, _) = u64::decode_var(&buf[offset..]).unwrap();
        assert_eq!(v, 1); // numRuns
        offset += n;
        let (v, n): (u64, _) = u64::decode_var(&buf[offset..]).unwrap();
        assert_eq!(v, 100); // numRleValues
        offset += n;
        assert_eq!(offset, buf.len());
    }

    #[test]
    fn test_stream_meta_byte_rle_has_rle_fields() {
        // MLT spec: ALL non-bool RLE streams have numRuns + numRleValues metadata,
        // including byte-RLE (phys=NONE).
        let mut buf = Vec::new();
        write_stream_meta_rle(
            &mut buf,
            STREAM_DATA,
            DATA_NONE,
            LOG_RLE,
            LOG_NONE,
            PHYS_NONE,
            50, // numValues
            2,  // byteLength
            1,  // numRuns
            50, // numRleValues
        );
        // byte 0: (STREAM_DATA << 4) | DATA_NONE = (1 << 4) | 0 = 0x10
        assert_eq!(buf[0], 0x10);
        // byte 1: (LOG_RLE << 5) | (LOG_NONE << 2) | PHYS_NONE = (3 << 5) | 0 | 0 = 0x60
        assert_eq!(buf[1], 0x60);
        // 4 varints: numValues, byteLength, numRuns, numRleValues
        let mut offset = 2;
        let (v, n): (u64, _) = u64::decode_var(&buf[offset..]).unwrap();
        assert_eq!(v, 50); // numValues
        offset += n;
        let (v, n): (u64, _) = u64::decode_var(&buf[offset..]).unwrap();
        assert_eq!(v, 2); // byteLength
        offset += n;
        let (v, n): (u64, _) = u64::decode_var(&buf[offset..]).unwrap();
        assert_eq!(v, 1); // numRuns
        offset += n;
        let (v, n): (u64, _) = u64::decode_var(&buf[offset..]).unwrap();
        assert_eq!(v, 50); // numRleValues
        offset += n;
        assert_eq!(offset, buf.len());
    }

    // --- Dictionary encoding validation ---

    #[test]
    fn test_should_use_dictionary_low_cardinality() {
        use crate::tiler::{Feature, Geometry, PropertyValue};
        use geo_types::Point;

        let features: Vec<Feature> = (0..30)
            .map(|i| Feature {
                id: Some(i as u64),
                geometry: Geometry::Point(Point::new(0.0, 0.0)),
                properties: vec![PropertyValue::String(
                    ["urban", "rural", "suburban"][i % 3].to_string(),
                )],
            })
            .collect();

        assert!(should_use_dictionary(&features, 0));
    }

    #[test]
    fn test_should_not_use_dictionary_all_unique() {
        use crate::tiler::{Feature, Geometry, PropertyValue};
        use geo_types::Point;

        let features: Vec<Feature> = (0..10)
            .map(|i| Feature {
                id: Some(i as u64),
                geometry: Geometry::Point(Point::new(0.0, 0.0)),
                properties: vec![PropertyValue::String(format!("unique_{}", i))],
            })
            .collect();

        assert!(!should_use_dictionary(&features, 0));
    }

    #[test]
    fn test_collect_string_values_coerces_mixed_string_column_consistently() {
        use crate::tiler::{Feature, Geometry, PropertyValue};
        use geo_types::Point;

        let features = vec![
            Feature {
                id: Some(1),
                geometry: Geometry::Point(Point::new(0.0, 0.0)),
                properties: vec![PropertyValue::String("31-33".to_string())],
            },
            Feature {
                id: Some(2),
                geometry: Geometry::Point(Point::new(0.0, 0.0)),
                properties: vec![PropertyValue::Int(5112)],
            },
            Feature {
                id: Some(3),
                geometry: Geometry::Point(Point::new(0.0, 0.0)),
                properties: vec![PropertyValue::Null],
            },
        ];

        assert_eq!(
            collect_string_values(&features, 0),
            vec!["31-33".to_string(), "5112".to_string()]
        );
    }

    // --- count_runs validation ---

    #[test]
    fn test_count_runs() {
        assert_eq!(count_runs(&[1u8, 1, 1, 1, 1]), 1);
        assert_eq!(count_runs(&[1u8, 2, 3]), 3);
        assert_eq!(count_runs(&[1u8, 1, 2, 2, 3, 3]), 3);
        assert_eq!(count_runs::<u8>(&[]), 0);
        assert_eq!(count_runs(&[42u32; 100]), 1);
    }

    // --- Full-tile structural decoder ---
    // Walks the complete binary output of encode_tile() and validates:
    //   layer envelope, column metadata, stream headers, stream ordering,
    //   and payload sizes.

    /// Helper: read a varint from a byte buffer, return (value, bytes_consumed)
    pub(super) fn read_varint(data: &[u8], offset: usize) -> (u64, usize) {
        let (v, n): (u64, usize) = u64::decode_var(&data[offset..]).unwrap();
        (v, n)
    }

    /// Parse a stream metadata header: returns (stream_type, subtype, tech1, tech2, phys,
    /// num_values, byte_length, total_header_bytes).
    /// MLT spec: ALL non-bool RLE streams include numRuns + numRleValues varints,
    /// regardless of physical encoding (byte-RLE uses phys=NONE but still has them).
    pub(super) fn parse_stream_header(
        data: &[u8],
        offset: usize,
    ) -> (u8, u8, u8, u8, u8, u64, u64, usize) {
        let byte0 = data[offset];
        let byte1 = data[offset + 1];
        let stream_type = byte0 >> 4;
        let subtype = byte0 & 0x0F;
        let tech1 = (byte1 >> 5) & 0x07;
        let tech2 = (byte1 >> 2) & 0x07;
        let phys = byte1 & 0x03;
        let mut pos = offset + 2;
        let (num_values, n) = read_varint(data, pos);
        pos += n;
        let (byte_length, n) = read_varint(data, pos);
        pos += n;
        // ALL non-bool RLE streams have extra metadata (numRuns, numRleValues)
        if tech1 == LOG_RLE || tech2 == LOG_RLE {
            let (_num_runs, n) = read_varint(data, pos);
            pos += n;
            let (_num_rle_values, n) = read_varint(data, pos);
            pos += n;
        }
        (
            stream_type,
            subtype,
            tech1,
            tech2,
            phys,
            num_values,
            byte_length,
            pos - offset,
        )
    }

    #[test]
    fn test_full_tile_uniform_polygons_structure() {
        // 20 simple polygons (same type, 1 ring each) → exercises:
        // - byte-RLE on geometry type stream
        // - integer-RLE on numParts and numRings streams
        use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
        use geo_types::{Coord, LineString, Polygon};

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        let features: Vec<Feature> = (0..20)
            .map(|i| {
                let x0 = -79.0 + (i as f64) * 0.1;
                let ring = LineString(vec![
                    Coord { x: x0, y: 35.0 },
                    Coord {
                        x: x0 + 0.05,
                        y: 35.0,
                    },
                    Coord {
                        x: x0 + 0.05,
                        y: 35.05,
                    },
                    Coord { x: x0, y: 35.05 },
                    Coord { x: x0, y: 35.0 },
                ]);
                Feature {
                    id: Some((i + 1) as u64),
                    geometry: Geometry::Polygon(Polygon::new(ring, vec![])),
                    properties: vec![PropertyValue::Int(i as i64)],
                }
            })
            .collect();

        let prop_names = vec!["value".to_string()];
        let tile_bytes = encode_tile(&coord, &features, "test", &prop_names);
        assert!(!tile_bytes.is_empty());

        // Parse layer envelope
        let mut pos = 0;
        let (layer_len, n) = read_varint(&tile_bytes, pos);
        pos += n;
        let (tag, n) = read_varint(&tile_bytes, pos);
        pos += n;
        assert_eq!(tag, TAG_V01 as u64);
        let layer_end = pos + layer_len as usize - n; // layer_len includes tag

        // Layer name
        let (name_len, n) = read_varint(&tile_bytes, pos);
        pos += n;
        let name = std::str::from_utf8(&tile_bytes[pos..pos + name_len as usize]).unwrap();
        assert_eq!(name, "test");
        pos += name_len as usize;

        // Extent
        let (extent, n) = read_varint(&tile_bytes, pos);
        pos += n;
        assert_eq!(extent, EXTENT as u64);

        // Num columns: id + geometry + 1 property = 3
        let (num_cols, n) = read_varint(&tile_bytes, pos);
        pos += n;
        assert_eq!(num_cols, 3);

        // Column metadata (raw u8 bytes, not varints)
        assert_eq!(tile_bytes[pos], COL_ID);
        pos += 1; // LongId = 2
        assert_eq!(tile_bytes[pos], COL_GEOMETRY);
        pos += 1; // Geometry = 4
        assert_eq!(tile_bytes[pos], COL_I64);
        pos += 1; // I64 = 20
                  // Property name follows the type for types >= 5
        let (prop_name_len, n) = read_varint(&tile_bytes, pos);
        pos += n;
        let prop_name =
            std::str::from_utf8(&tile_bytes[pos..pos + prop_name_len as usize]).unwrap();
        assert_eq!(prop_name, "value");
        pos += prop_name_len as usize;

        // --- ID stream ---
        let (st, sub, t1, _t2, phys, nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_DATA);
        assert_eq!(sub, DATA_NONE);
        assert_eq!(t1, LOG_DELTA);
        assert_eq!(phys, PHYS_VARINT);
        assert_eq!(nv, 20);
        pos += hdr_len;
        pos += bl as usize; // skip ID data

        // --- Geometry streams ---
        // Stream count varint
        let (geom_stream_count, n) = read_varint(&tile_bytes, pos);
        pos += n;
        // Uniform polygons: geom_type + numParts + numRings + vertices = 4
        assert_eq!(geom_stream_count, 4);

        // Stream 1: geometry types — LENGTH+VAR_BINARY with integer RLE (all POLYGON)
        let (st, sub, t1, _t2, phys, nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_LENGTH);
        assert_eq!(sub, LENGTH_VAR_BINARY);
        assert_eq!(phys, PHYS_VARINT);
        if t1 == LOG_RLE {
            // RLE: nv = physical buffer count (num_runs * 2)
            assert_eq!(nv, 2);
        } else {
            assert_eq!(nv, 20);
        }
        pos += hdr_len;
        pos += bl as usize;

        // Stream 2: numParts — should be integer-RLE (all 1)
        let (st, sub, t1, _t2, phys, nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_LENGTH);
        assert_eq!(sub, LENGTH_PARTS);
        if t1 == LOG_RLE && phys == PHYS_VARINT {
            assert_eq!(nv, 2); // physical buffer count = num_runs * 2
            pos += hdr_len;
            assert!(bl <= 4);
        } else {
            assert_eq!(nv, 20);
            pos += hdr_len;
        }
        pos += bl as usize;

        // Stream 3: numRings — should be integer-RLE (all 4 vertices per ring after closing-point removal)
        let (st, sub, t1, _t2, phys, nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_LENGTH);
        assert_eq!(sub, LENGTH_RINGS);
        pos += hdr_len;
        pos += bl as usize;

        // Stream 4: vertices (componentwise delta + varint)
        let (st, sub, t1, _t2, phys, _nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_DATA);
        assert_eq!(sub, DATA_VERTEX);
        assert_eq!(t1, LOG_COMPONENTWISE_DELTA);
        assert_eq!(phys, PHYS_VARINT);
        pos += hdr_len;
        pos += bl as usize;

        // --- Property stream (i64, no nulls) ---
        let (st, sub, t1, _t2, phys, nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_DATA);
        assert_eq!(nv, 20);
        pos += hdr_len;
        pos += bl as usize;

        // Should have consumed the entire layer
        assert_eq!(pos, layer_end);
    }

    #[test]
    fn test_full_tile_dictionary_string_activated() {
        // 30 features with 3 unique string values → dictionary encoding MUST activate.
        // Validates dictionary stream order: LENGTH(DICTIONARY) → DATA(SINGLE) → DATA(indices)
        use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
        use geo_types::Point;

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        let categories = ["urban", "rural", "suburban"];
        let features: Vec<Feature> = (0..30)
            .map(|i| Feature {
                id: Some((i + 1) as u64),
                geometry: Geometry::Point(Point::new(-79.0 + (i as f64) * 0.01, 35.5)),
                properties: vec![PropertyValue::String(categories[i % 3].to_string())],
            })
            .collect();

        // Verify dictionary should activate
        assert!(should_use_dictionary(&features, 0));

        let prop_names = vec!["category".to_string()];
        let tile_bytes = encode_tile(&coord, &features, "test", &prop_names);
        assert!(!tile_bytes.is_empty());

        // Parse to the string property streams
        let mut pos = 0;
        let (layer_len, n) = read_varint(&tile_bytes, pos);
        pos += n;
        let (_tag, n) = read_varint(&tile_bytes, pos);
        pos += n;
        let layer_end = pos + layer_len as usize - n;

        // Skip: name
        let (name_len, n) = read_varint(&tile_bytes, pos);
        pos += n;
        pos += name_len as usize;
        // Skip: extent
        let (_, n) = read_varint(&tile_bytes, pos);
        pos += n;
        // Num columns
        let (num_cols, n) = read_varint(&tile_bytes, pos);
        pos += n;
        assert_eq!(num_cols, 3); // id + geometry + category

        // Column metadata (raw u8 bytes)
        assert_eq!(tile_bytes[pos], COL_ID);
        pos += 1;
        assert_eq!(tile_bytes[pos], COL_GEOMETRY);
        pos += 1;
        assert_eq!(tile_bytes[pos], COL_STR);
        pos += 1; // non-nullable string (all present)
                  // Property name
        let (pnl, n) = read_varint(&tile_bytes, pos);
        pos += n;
        let pn = std::str::from_utf8(&tile_bytes[pos..pos + pnl as usize]).unwrap();
        assert_eq!(pn, "category");
        pos += pnl as usize;

        // Skip ID stream
        let (_, _, _, _, _, _, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        pos += hdr_len + bl as usize;

        // Skip geometry streams
        let (geom_count, n) = read_varint(&tile_bytes, pos);
        pos += n;
        for _ in 0..geom_count {
            let (_, _, _, _, _, _, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
            pos += hdr_len + bl as usize;
        }

        // --- String property: should be dictionary-encoded ---
        // Stream count varint (string columns have hasStreamCount = true)
        let (stream_count, n) = read_varint(&tile_bytes, pos);
        pos += n;
        // No nulls → stream_count should be 3 (dict_lengths + dict_data + indices)
        assert_eq!(
            stream_count, 3,
            "dictionary encoding should produce 3 streams"
        );

        // Stream 1: dictionary lengths (LENGTH, DICTIONARY subtype)
        let (st, sub, _, _, _, nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_LENGTH, "first dict stream should be LENGTH");
        assert_eq!(sub, LENGTH_DICTIONARY, "subtype should be DICTIONARY");
        assert_eq!(nv, 3, "should have 3 dictionary entries");
        pos += hdr_len + bl as usize;

        // Stream 2: dictionary data (DATA, SINGLE dictionary type)
        let (st, sub, _, _, _, nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_DATA, "second dict stream should be DATA");
        assert_eq!(sub, DATA_SINGLE, "dictionary type should be SINGLE");
        // nv = number of dictionary entries (not byte count)
        assert_eq!(nv, 3, "num_values should be dictionary entry count");
        // Verify byte_length matches total concatenated dict bytes
        let total_dict_bytes: usize = categories.iter().map(|s| s.len()).sum();
        assert_eq!(
            bl as usize, total_dict_bytes,
            "byte_length should be total dict bytes"
        );
        pos += hdr_len + bl as usize;

        // Stream 3: per-feature indices (OFFSET, STRING)
        let (st, sub, _, _, phys, nv, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_OFFSET, "third dict stream should be OFFSET");
        assert_eq!(sub, OFFSET_STRING, "index stream subtype should be STRING");
        assert_eq!(phys, PHYS_VARINT);
        assert_eq!(nv, 30, "should have 30 index values");
        pos += hdr_len + bl as usize;

        assert_eq!(pos, layer_end);
    }

    // ==========================================================================
    // End-to-end conformance test using the upstream MapLibre mlt-core decoder.
    // This is the trust anchor: our encoder output is decoded by the same code
    // that MapLibre GL JS and other consumers use.
    // ==========================================================================

    #[test]
    fn test_mlt_core_decodes_minimal_points() {
        // Simplest possible test: 3 points, no properties, no RLE
        use crate::tiler::{Feature, Geometry, TileCoord};
        use geo_types::Point;

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        let features: Vec<Feature> = (0..3)
            .map(|i| Feature {
                id: Some((i + 1) as u64),
                geometry: Geometry::Point(Point::new(-79.0 + (i as f64) * 0.1, 35.5)),
                properties: vec![],
            })
            .collect();

        let prop_names: Vec<String> = vec![];
        let tile_bytes = encode_tile(&coord, &features, "test", &prop_names);

        let mut layers =
            mlt_core::parse_layers(&tile_bytes).expect("mlt-core should parse minimal points");
        assert_eq!(layers.len(), 1);

        let layer = layers[0].as_layer01().expect("should be v01");
        assert_eq!(layer.name, "test");

        layers[0]
            .decode_all()
            .expect("mlt-core should decode minimal points");
    }

    #[test]
    fn test_mlt_core_decodes_polygon_tile_with_rle() {
        // Encode a tile with uniform polygons (triggers integer-RLE on topology)
        // and one i64 property, then decode with mlt_core::parse_layers.
        use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
        use geo_types::{Coord, LineString, Polygon};

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        let features: Vec<Feature> = (0..10)
            .map(|i| {
                let x0 = -79.0 + (i as f64) * 0.1;
                let ring = LineString(vec![
                    Coord { x: x0, y: 35.0 },
                    Coord {
                        x: x0 + 0.05,
                        y: 35.0,
                    },
                    Coord {
                        x: x0 + 0.05,
                        y: 35.05,
                    },
                    Coord { x: x0, y: 35.05 },
                    Coord { x: x0, y: 35.0 },
                ]);
                Feature {
                    id: Some((i + 1) as u64),
                    geometry: Geometry::Polygon(Polygon::new(ring, vec![])),
                    properties: vec![PropertyValue::Int(i as i64 * 10)],
                }
            })
            .collect();

        let prop_names = vec!["value".to_string()];
        let tile_bytes = encode_tile(&coord, &features, "counties", &prop_names);
        assert!(!tile_bytes.is_empty());

        // Decode with mlt-core reference decoder
        let mut layers = mlt_core::parse_layers(&tile_bytes)
            .expect("mlt-core should parse our tile without error");
        assert_eq!(layers.len(), 1);

        let layer = layers[0].as_layer01().expect("should be a v01 layer");
        assert_eq!(layer.name, "counties");
        assert_eq!(layer.extent, EXTENT);

        // Decode all streams
        layers[0]
            .decode_all()
            .expect("mlt-core should decode all streams");

        // Re-borrow after decode
        let layer = layers[0].as_layer01().unwrap();

        // Verify properties decoded correctly
        assert_eq!(layer.properties.len(), 1);
        if let mlt_core::v01::Property::Decoded(ref dp) = layer.properties[0] {
            assert_eq!(dp.name, "value");
            if let mlt_core::v01::PropValue::I64(ref vals) = dp.values {
                assert_eq!(vals.len(), 10);
                for (i, v) in vals.iter().enumerate() {
                    assert_eq!(
                        *v,
                        Some(i as i64 * 10),
                        "property value mismatch at index {i}"
                    );
                }
            } else {
                panic!("expected I64 property values, got {:?}", dp.values);
            }
        } else {
            panic!("expected decoded property");
        }
    }

    #[test]
    fn test_mlt_core_decodes_dictionary_strings() {
        // Encode a tile with low-cardinality strings (triggers dictionary encoding)
        // and a nullable i64 column, then decode with mlt_core.
        use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
        use geo_types::Point;

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        let categories = ["urban", "rural", "suburban"];
        let features: Vec<Feature> = (0..30)
            .map(|i| Feature {
                id: Some((i + 1) as u64),
                geometry: Geometry::Point(Point::new(-79.0 + (i as f64) * 0.01, 35.5)),
                properties: vec![
                    PropertyValue::String(categories[i % 3].to_string()),
                    if i % 5 == 0 {
                        PropertyValue::Null
                    } else {
                        PropertyValue::Int(i as i64)
                    },
                ],
            })
            .collect();

        // Verify dictionary should activate for column 0
        assert!(should_use_dictionary(&features, 0));

        let prop_names = vec!["category".to_string(), "count".to_string()];
        let tile_bytes = encode_tile(&coord, &features, "places", &prop_names);
        assert!(!tile_bytes.is_empty());

        // Decode with mlt-core reference decoder
        let mut layers = mlt_core::parse_layers(&tile_bytes)
            .expect("mlt-core should parse our dictionary-encoded tile");
        assert_eq!(layers.len(), 1);

        layers[0]
            .decode_all()
            .expect("mlt-core should decode dictionary streams");

        let layer = layers[0].as_layer01().unwrap();
        assert_eq!(layer.name, "places");

        // Verify string property decoded correctly via dictionary path
        assert_eq!(layer.properties.len(), 2);

        // Column 0: category (dictionary-encoded strings)
        if let mlt_core::v01::Property::Decoded(ref dp) = layer.properties[0] {
            assert_eq!(dp.name, "category");
            if let mlt_core::v01::PropValue::Str(ref vals) = dp.values {
                assert_eq!(vals.len(), 30);
                for (i, v) in vals.iter().enumerate() {
                    assert_eq!(
                        v.as_deref(),
                        Some(categories[i % 3]),
                        "string mismatch at index {i}"
                    );
                }
            } else {
                panic!("expected Str property values, got {:?}", dp.values);
            }
        } else {
            panic!("expected decoded property for category");
        }

        // Column 1: count (nullable i64)
        if let mlt_core::v01::Property::Decoded(ref dp) = layer.properties[1] {
            assert_eq!(dp.name, "count");
            if let mlt_core::v01::PropValue::I64(ref vals) = dp.values {
                assert_eq!(vals.len(), 30);
                for (i, v) in vals.iter().enumerate() {
                    if i % 5 == 0 {
                        assert_eq!(*v, None, "expected null at index {i}");
                    } else {
                        assert_eq!(*v, Some(i as i64), "int mismatch at index {i}");
                    }
                }
            } else {
                panic!(
                    "expected I64 property values for count, got {:?}",
                    dp.values
                );
            }
        } else {
            panic!("expected decoded property for count");
        }
    }
}

// ==========================================================================
// FastPFOR conformance tests — only compiled when feature is enabled
// ==========================================================================

#[cfg(all(test, feature = "fastpfor"))]
mod fastpfor_tests {
    use super::*;
    use fastpfor::cpp::{Codec32 as _, FastPFor256Codec};

    /// Phase 1: Raw u32 stream round-trip — prove our encoder produces
    /// bytes that decode correctly via the same codec mlt-core uses.
    #[test]
    fn test_fastpfor_raw_roundtrip_256_values() {
        // 256 values with varied distribution
        let values: Vec<u32> = (0..256).map(|i| (i * 7 + 13) % 1000).collect();
        let encoded = encode_fastpfor_u32(&values).expect("encode should succeed");
        assert!(!encoded.is_empty());
        assert_eq!(encoded.len() % 4, 0, "must be aligned to u32 words");

        // Decode: big-endian bytes → u32 words → FastPFor256Codec.decode32
        let num_words = encoded.len() / 4;
        let input: Vec<u32> = (0..num_words)
            .map(|i| {
                let o = i * 4;
                u32::from_be_bytes([encoded[o], encoded[o + 1], encoded[o + 2], encoded[o + 3]])
            })
            .collect();

        let mut result = vec![0u32; values.len() + 1024];
        let decoded = FastPFor256Codec::new()
            .decode32(&input, &mut result)
            .expect("decode should succeed");
        assert_eq!(&decoded[..values.len()], &values[..]);
    }

    #[test]
    fn test_fastpfor_raw_roundtrip_large() {
        // 1024 values — exceeds single FastPFOR block (128 values)
        let values: Vec<u32> = (0..1024).map(|i| i * 3).collect();
        let encoded = encode_fastpfor_u32(&values).expect("encode should succeed");

        let num_words = encoded.len() / 4;
        let input: Vec<u32> = (0..num_words)
            .map(|i| {
                let o = i * 4;
                u32::from_be_bytes([encoded[o], encoded[o + 1], encoded[o + 2], encoded[o + 3]])
            })
            .collect();

        let mut result = vec![0u32; values.len() + 1024];
        let decoded = FastPFor256Codec::new()
            .decode32(&input, &mut result)
            .expect("decode should succeed");
        assert_eq!(&decoded[..values.len()], &values[..]);
    }

    #[test]
    fn test_fastpfor_empty() {
        let encoded = encode_fastpfor_u32(&[]).expect("empty encode should succeed");
        assert!(encoded.is_empty());
    }

    #[test]
    fn test_fastpfor_smaller_than_varint() {
        // For sufficiently large data, FastPFOR should beat varint
        let values: Vec<u32> = (0..512).map(|i| i % 100).collect();
        let (bytes, technique) = encode_u32_stream_best(&values);
        // With 512 small values, FastPFOR should be selected
        assert_eq!(
            technique, PHYS_FAST_PFOR,
            "FastPFOR should be chosen for 512 small values"
        );
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_encode_u32_stream_best_small_fallback() {
        // Under 128 values — must fall back to varint
        let values: Vec<u32> = (0..64).collect();
        let (bytes, technique) = encode_u32_stream_best(&values);
        assert_eq!(
            technique, PHYS_VARINT,
            "should fall back to varint for < 128 values"
        );
        assert!(!bytes.is_empty());
    }

    /// Verify that the vertex stream metadata actually uses PHYS_FAST_PFOR
    /// when enough vertices are present (closing the testing gap).
    #[test]
    fn test_vertex_stream_uses_fastpfor_metadata() {
        use super::tests::{parse_stream_header, read_varint};
        use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
        use geo_types::{Coord, LineString, Polygon};

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        // 100 polygons × 4 vertices = 800 interleaved values
        let features: Vec<Feature> = (0..100)
            .map(|i| {
                let x0 = -79.0 + (i as f64) * 0.01;
                let ring = LineString(vec![
                    Coord { x: x0, y: 35.0 },
                    Coord {
                        x: x0 + 0.005,
                        y: 35.0,
                    },
                    Coord {
                        x: x0 + 0.005,
                        y: 35.005,
                    },
                    Coord { x: x0, y: 35.005 },
                    Coord { x: x0, y: 35.0 },
                ]);
                Feature {
                    id: Some((i + 1) as u64),
                    geometry: Geometry::Polygon(Polygon::new(ring, vec![])),
                    properties: vec![PropertyValue::Int(i as i64)],
                }
            })
            .collect();

        let prop_names = vec!["value".to_string()];
        let tile_bytes = encode_tile(&coord, &features, "test", &prop_names);

        // Parse to find the vertex stream
        let mut pos = 0;
        let (layer_len, n) = read_varint(&tile_bytes, pos);
        pos += n;
        let (_tag, n) = read_varint(&tile_bytes, pos);
        pos += n;
        // Skip: name
        let (name_len, n) = read_varint(&tile_bytes, pos);
        pos += n;
        pos += name_len as usize;
        // Skip: extent
        let (_, n) = read_varint(&tile_bytes, pos);
        pos += n;
        // Skip: num columns
        let (num_cols, n) = read_varint(&tile_bytes, pos);
        pos += n;
        // Skip column metadata
        pos += 1; // COL_ID
        pos += 1; // COL_GEOMETRY
        pos += 1; // COL_I64
        let (prop_name_len, n) = read_varint(&tile_bytes, pos);
        pos += n;
        pos += prop_name_len as usize;
        // Skip ID stream
        let (_, _, _, _, _, _, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
        pos += hdr_len + bl as usize;
        // Geometry stream count
        let (geom_count, n) = read_varint(&tile_bytes, pos);
        pos += n;
        // Skip non-vertex geometry streams (geom_type, numParts, numRings)
        for _ in 0..(geom_count - 1) {
            let (_, _, _, _, _, _, bl, hdr_len) = parse_stream_header(&tile_bytes, pos);
            pos += hdr_len + bl as usize;
        }
        // Last geometry stream = vertex stream
        let (st, sub, t1, _, phys, nv, _, _) = parse_stream_header(&tile_bytes, pos);
        assert_eq!(st, STREAM_DATA, "should be DATA stream");
        assert_eq!(sub, DATA_VERTEX, "should be VERTEX subtype");
        assert_eq!(t1, LOG_COMPONENTWISE_DELTA, "should be componentwise delta");
        assert_eq!(
            phys, PHYS_FAST_PFOR,
            "vertex stream should use PHYS_FAST_PFOR with 800 interleaved values"
        );
        assert_eq!(
            nv, 800,
            "num_values should be 400 vertices × 2 coords = 800"
        );
    }

    /// Phase 3: Full-tile conformance test with enough features to trigger
    /// FastPFOR on vertex streams (need >= 128 interleaved values = 64+ vertices).
    #[test]
    fn test_mlt_core_decodes_fastpfor_vertex_tile() {
        use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
        use geo_types::{Coord, LineString, Polygon};

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        // 100 polygons × 4 vertices = 400 vertices × 2 coords = 800 interleaved values → triggers FastPFOR
        let features: Vec<Feature> = (0..100)
            .map(|i| {
                let x0 = -79.0 + (i as f64) * 0.01;
                let ring = LineString(vec![
                    Coord { x: x0, y: 35.0 },
                    Coord {
                        x: x0 + 0.005,
                        y: 35.0,
                    },
                    Coord {
                        x: x0 + 0.005,
                        y: 35.005,
                    },
                    Coord { x: x0, y: 35.005 },
                    Coord { x: x0, y: 35.0 },
                ]);
                Feature {
                    id: Some((i + 1) as u64),
                    geometry: Geometry::Polygon(Polygon::new(ring, vec![])),
                    properties: vec![PropertyValue::Int(i as i64 * 10)],
                }
            })
            .collect();

        let prop_names = vec!["value".to_string()];
        let tile_bytes = encode_tile(&coord, &features, "counties", &prop_names);

        let mut layers = mlt_core::parse_layers(&tile_bytes)
            .expect("mlt-core should parse FastPFOR-encoded tile");
        assert_eq!(layers.len(), 1);

        layers[0]
            .decode_all()
            .expect("mlt-core should decode FastPFOR vertex streams");

        let layer = layers[0].as_layer01().unwrap();
        assert_eq!(layer.name, "counties");

        // Verify integer properties survived the roundtrip
        if let mlt_core::v01::Property::Decoded(ref dp) = layer.properties[0] {
            assert_eq!(dp.name, "value");
            if let mlt_core::v01::PropValue::I64(ref vals) = dp.values {
                assert_eq!(vals.len(), 100);
                for (i, v) in vals.iter().enumerate() {
                    assert_eq!(
                        *v,
                        Some(i as i64 * 10),
                        "property value mismatch at index {i}"
                    );
                }
            } else {
                panic!("expected I64 property values");
            }
        } else {
            panic!("expected decoded property");
        }
    }

    /// Phase 3: Full-tile conformance with many string properties triggering FastPFOR on index stream.
    #[test]
    fn test_mlt_core_decodes_fastpfor_dictionary_indices() {
        use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
        use geo_types::Point;

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        let categories = ["urban", "rural", "suburban", "exurban"];
        // 200 features → 200 dictionary index values → triggers FastPFOR on index stream
        let features: Vec<Feature> = (0..200)
            .map(|i| Feature {
                id: Some((i + 1) as u64),
                geometry: Geometry::Point(Point::new(-79.0 + (i as f64) * 0.005, 35.5)),
                properties: vec![PropertyValue::String(categories[i % 4].to_string())],
            })
            .collect();

        let prop_names = vec!["category".to_string()];
        let tile_bytes = encode_tile(&coord, &features, "places", &prop_names);

        let mut layers = mlt_core::parse_layers(&tile_bytes)
            .expect("mlt-core should parse dictionary+FastPFOR tile");
        assert_eq!(layers.len(), 1);

        layers[0]
            .decode_all()
            .expect("mlt-core should decode dictionary+FastPFOR streams");

        let layer = layers[0].as_layer01().unwrap();
        if let mlt_core::v01::Property::Decoded(ref dp) = layer.properties[0] {
            if let mlt_core::v01::PropValue::Str(ref vals) = dp.values {
                assert_eq!(vals.len(), 200);
                for (i, v) in vals.iter().enumerate() {
                    assert_eq!(
                        v.as_deref(),
                        Some(categories[i % 4]),
                        "string mismatch at index {i}"
                    );
                }
            } else {
                panic!("expected Str values");
            }
        } else {
            panic!("expected decoded property");
        }
    }
}

// ==========================================================================
// FSST conformance tests — only compiled when feature is enabled
// ==========================================================================

#[cfg(all(test, feature = "fsst"))]
mod fsst_tests {
    use super::*;

    /// Verify FSST encoding round-trips through mlt-core's decode_fsst algorithm.
    #[test]
    fn test_fsst_dictionary_roundtrip() {
        // Create enough repeated strings to trigger FSST (need >= 32 entries, >= 4096 total bytes)
        let base_entries: Vec<String> = (0..100)
            .map(|i| {
                format!(
                    "category_value_{:03}_this_is_a_long_enough_string_for_compression_testing",
                    i
                )
            })
            .collect();
        let dict_entries: Vec<&str> = base_entries.iter().map(|s| s.as_str()).collect();

        let result = try_fsst_dictionary(&dict_entries);
        assert!(
            result.is_some(),
            "50 entries with long strings should trigger FSST"
        );

        let fsst_encoded = result.unwrap();

        // Decode using the same algorithm as mlt-core's decode_fsst()
        let symbol_lengths = &fsst_encoded.symbol_lengths;
        let symbol_data = &fsst_encoded.symbol_data;
        let compressed = &fsst_encoded.compressed_data;

        let mut symbol_offsets = vec![0u32; symbol_lengths.len()];
        for i in 1..symbol_lengths.len() {
            symbol_offsets[i] = symbol_offsets[i - 1] + symbol_lengths[i - 1];
        }

        let mut decompressed = Vec::new();
        let mut i = 0;
        while i < compressed.len() {
            let sym_idx = compressed[i] as usize;
            if sym_idx == 255 {
                i += 1;
                decompressed.push(compressed[i]);
            } else if sym_idx < symbol_lengths.len() {
                let len = symbol_lengths[sym_idx] as usize;
                let off = symbol_offsets[sym_idx] as usize;
                decompressed.extend_from_slice(&symbol_data[off..off + len]);
            }
            i += 1;
        }

        // Split using value_lengths (original UTF-8 byte lengths)
        let mut offset = 0;
        for (idx, &len) in fsst_encoded.value_lengths.iter().enumerate() {
            let len = len as usize;
            let s = std::str::from_utf8(&decompressed[offset..offset + len])
                .expect("should be valid UTF-8");
            assert_eq!(s, dict_entries[idx], "mismatch at index {idx}");
            offset += len;
        }
    }

    #[test]
    fn test_fsst_skips_small_dictionaries() {
        let entries = vec!["a", "b", "c"];
        assert!(
            try_fsst_dictionary(&entries).is_none(),
            "should skip tiny dictionaries"
        );
    }

    /// Phase 7: Full-tile conformance test with FSST-compressed dictionary strings.
    /// Encodes a tile where the dictionary entries meet FSST thresholds and
    /// verifies mlt-core can decode the 5-stream format.
    #[test]
    fn test_mlt_core_decodes_fsst_dictionary_tile() {
        use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
        use geo_types::Point;

        let coord = TileCoord { z: 4, x: 4, y: 6 };
        // Need >= 32 unique dict entries with >= 4096 total bytes for FSST to activate
        let categories: Vec<String> = (0..50)
            .map(|i| format!("category_value_{:03}_this_is_a_long_enough_dictionary_entry_for_fsst_compression_testing", i))
            .collect();

        // Verify FSST thresholds are met
        let total_bytes: usize = categories.iter().map(|s| s.len()).sum();
        assert!(categories.len() >= 32, "need >= 32 unique values");
        assert!(
            total_bytes >= 4096,
            "need >= 4096 total bytes, got {total_bytes}"
        );

        // 500 features cycling through 50 categories = high repetition
        let features: Vec<Feature> = (0..500)
            .map(|i| Feature {
                id: Some((i + 1) as u64),
                geometry: Geometry::Point(Point::new(-79.0 + (i as f64) * 0.002, 35.5)),
                properties: vec![
                    PropertyValue::String(categories[i % 50].clone()),
                    PropertyValue::Int(i as i64),
                ],
            })
            .collect();

        let prop_names = vec!["category".to_string(), "count".to_string()];
        let tile_bytes = encode_tile(&coord, &features, "places", &prop_names);

        let mut layers =
            mlt_core::parse_layers(&tile_bytes).expect("mlt-core should parse FSST-encoded tile");
        assert_eq!(layers.len(), 1);

        layers[0]
            .decode_all()
            .expect("mlt-core should decode FSST dictionary streams");

        let layer = layers[0].as_layer01().unwrap();
        assert_eq!(layer.name, "places");
        assert_eq!(layer.properties.len(), 2);

        // Verify string property (FSST-compressed dictionary)
        if let mlt_core::v01::Property::Decoded(ref dp) = layer.properties[0] {
            assert_eq!(dp.name, "category");
            if let mlt_core::v01::PropValue::Str(ref vals) = dp.values {
                assert_eq!(vals.len(), 500);
                for (i, v) in vals.iter().enumerate() {
                    assert_eq!(
                        v.as_deref(),
                        Some(categories[i % 50].as_str()),
                        "string mismatch at index {i}"
                    );
                }
            } else {
                panic!("expected Str property values");
            }
        } else {
            panic!("expected decoded property");
        }

        // Verify integer property
        if let mlt_core::v01::Property::Decoded(ref dp) = layer.properties[1] {
            assert_eq!(dp.name, "count");
            if let mlt_core::v01::PropValue::I64(ref vals) = dp.values {
                assert_eq!(vals.len(), 500);
                for (i, v) in vals.iter().enumerate() {
                    assert_eq!(*v, Some(i as i64), "int mismatch at index {i}");
                }
            } else {
                panic!("expected I64 property values");
            }
        } else {
            panic!("expected decoded property");
        }
    }
}

// --- Benchmarks ---
// These are gated on both features so a single binary can measure the delta.
// Run with: cargo test --features fastpfor,fsst -- --nocapture benchmark
#[cfg(all(test, feature = "fastpfor", feature = "fsst"))]
mod benchmarks {
    use super::*;
    use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};
    use geo_types::{Coord, LineString, Point, Polygon};
    use std::time::Instant;

    /// Encode a tile and return (size_bytes, encoding_time_us).
    fn encode_timed(
        coord: &TileCoord,
        features: &[Feature],
        layer_name: &str,
        prop_names: &[String],
    ) -> (usize, u128) {
        let start = Instant::now();
        let tile = encode_tile(coord, features, layer_name, prop_names);
        let elapsed = start.elapsed().as_micros();
        (tile.len(), elapsed)
    }

    /// NC-county style: simple polygons with string + int + float props.
    fn make_polygon_features(count: usize) -> (Vec<Feature>, Vec<String>) {
        let categories: Vec<String> = (0..50)
            .map(|i| format!("county_name_{:03}_this_is_a_realistic_county_name_string_for_benchmarking_purposes", i))
            .collect();

        let features: Vec<Feature> = (0..count)
            .map(|i| {
                let x0 = -79.0 + (i as f64) * 0.01;
                let ring = LineString(vec![
                    Coord { x: x0, y: 35.0 },
                    Coord {
                        x: x0 + 0.005,
                        y: 35.0,
                    },
                    Coord {
                        x: x0 + 0.005,
                        y: 35.005,
                    },
                    Coord { x: x0, y: 35.005 },
                    Coord { x: x0, y: 35.0 },
                ]);
                Feature {
                    id: Some((i + 1) as u64),
                    geometry: Geometry::Polygon(Polygon::new(ring, vec![])),
                    properties: vec![
                        PropertyValue::String(categories[i % 50].clone()),
                        PropertyValue::Int(i as i64),
                        PropertyValue::Double(i as f64 * 1.5),
                    ],
                }
            })
            .collect();

        let prop_names = vec!["name".to_string(), "fid".to_string(), "area".to_string()];
        (features, prop_names)
    }

    /// Vertex-heavy: each polygon has many vertices (jagged boundary).
    fn make_vertex_heavy_features(count: usize, verts_per: usize) -> (Vec<Feature>, Vec<String>) {
        let features: Vec<Feature> = (0..count)
            .map(|i| {
                let x0 = -79.0 + (i as f64) * 0.02;
                let y0 = 35.0;
                let mut coords: Vec<Coord<f64>> = (0..verts_per)
                    .map(|j| {
                        let angle = 2.0 * std::f64::consts::PI * (j as f64) / (verts_per as f64);
                        Coord {
                            x: x0 + 0.005 * angle.cos() + 0.0001 * ((j * 7) as f64).sin(),
                            y: y0 + 0.005 * angle.sin() + 0.0001 * ((j * 13) as f64).cos(),
                        }
                    })
                    .collect();
                coords.push(coords[0]); // close ring
                let ring = LineString(coords);
                Feature {
                    id: Some((i + 1) as u64),
                    geometry: Geometry::Polygon(Polygon::new(ring, vec![])),
                    properties: vec![PropertyValue::Int(i as i64)],
                }
            })
            .collect();

        let prop_names = vec!["fid".to_string()];
        (features, prop_names)
    }

    /// String-heavy: many features cycling through a large dictionary.
    fn make_string_heavy_features(
        count: usize,
        unique_strings: usize,
    ) -> (Vec<Feature>, Vec<String>) {
        let categories: Vec<String> = (0..unique_strings)
            .map(|i| format!(
                "long_category_identifier_{:04}_with_extra_text_to_simulate_real_world_string_property_values_in_vector_tiles",
                i
            ))
            .collect();

        let features: Vec<Feature> = (0..count)
            .map(|i| Feature {
                id: Some((i + 1) as u64),
                geometry: Geometry::Point(Point::new(-79.0 + (i as f64) * 0.002, 35.5)),
                properties: vec![
                    PropertyValue::String(categories[i % unique_strings].clone()),
                    PropertyValue::String(format!(
                        "secondary_value_{}",
                        i % (unique_strings / 2).max(1)
                    )),
                    PropertyValue::Int(i as i64),
                ],
            })
            .collect();

        let prop_names = vec![
            "category".to_string(),
            "label".to_string(),
            "count".to_string(),
        ];
        (features, prop_names)
    }

    #[test]
    fn benchmark_tile_sizes() {
        let coord = TileCoord { z: 4, x: 4, y: 6 };

        println!("\n============================================================");
        println!("  MLT Tile Size Benchmark (FastPFOR + FSST enabled)");
        println!("============================================================\n");

        // --- Dataset 1: NC-county style (100 polygons, mixed props) ---
        {
            let (features, prop_names) = make_polygon_features(100);
            let (size, time) = encode_timed(&coord, &features, "counties", &prop_names);
            println!("Dataset 1: NC counties (100 polygons, 3 props)");
            println!("  Tile size:     {:>8} bytes", size);
            println!("  Encode time:   {:>8} us", time);

            let tile = encode_tile(&coord, &features, "counties", &prop_names);
            let mut layers = mlt_core::parse_layers(&tile).expect("parse");
            layers[0].decode_all().expect("decode");
            println!("  mlt-core:      OK");
            println!();
        }

        // --- Dataset 2: Vertex-heavy (50 polygons x 200 verts) ---
        {
            let (features, prop_names) = make_vertex_heavy_features(50, 200);
            let (size, time) = encode_timed(&coord, &features, "blocks", &prop_names);
            println!("Dataset 2: Vertex-heavy (50 polys x 200 verts = 10K coords)");
            println!("  Tile size:     {:>8} bytes", size);
            println!("  Encode time:   {:>8} us", time);

            let tile = encode_tile(&coord, &features, "blocks", &prop_names);
            let mut layers = mlt_core::parse_layers(&tile).expect("parse");
            layers[0].decode_all().expect("decode");
            println!("  mlt-core:      OK");
            println!();
        }

        // --- Dataset 3: String-heavy (500 points, 100 unique strings) ---
        {
            let (features, prop_names) = make_string_heavy_features(500, 100);
            let (size, time) = encode_timed(&coord, &features, "places", &prop_names);
            println!("Dataset 3: String-heavy (500 points, 100 unique strings)");
            println!("  Tile size:     {:>8} bytes", size);
            println!("  Encode time:   {:>8} us", time);

            let tile = encode_tile(&coord, &features, "places", &prop_names);
            let mut layers = mlt_core::parse_layers(&tile).expect("parse");
            layers[0].decode_all().expect("decode");
            println!("  mlt-core:      OK");
            println!();
        }

        // --- Dataset 4: Large mixed (500 polygons, 3 props) ---
        {
            let (features, prop_names) = make_polygon_features(500);
            let (size, time) = encode_timed(&coord, &features, "large", &prop_names);
            println!("Dataset 4: Large mixed (500 polygons, 3 props)");
            println!("  Tile size:     {:>8} bytes", size);
            println!("  Encode time:   {:>8} us", time);

            let tile = encode_tile(&coord, &features, "large", &prop_names);
            let mut layers = mlt_core::parse_layers(&tile).expect("parse");
            layers[0].decode_all().expect("decode");
            println!("  mlt-core:      OK");
            println!();
        }

        // --- Stream-level compression ratios ---
        println!("--- Stream-level compression ratios ---\n");

        // Vertex stream: 50 polys x 200 verts
        {
            let (features, _) = make_vertex_heavy_features(50, 200);
            let mut all_zigzag = Vec::new();
            for f in &features {
                if let Geometry::Polygon(poly) = &f.geometry {
                    let coords: Vec<_> = poly.exterior().0.iter().collect();
                    let mut prev_x = 0i32;
                    let mut prev_y = 0i32;
                    for c in &coords[..coords.len().saturating_sub(1)] {
                        let tx = ((c.x + 79.0) / 0.1 * 4096.0) as i32;
                        let ty = ((c.y - 34.99) / 0.02 * 4096.0) as i32;
                        let dx = tx - prev_x;
                        let dy = ty - prev_y;
                        all_zigzag.push(zigzag_encode_i32(dx));
                        all_zigzag.push(zigzag_encode_i32(dy));
                        prev_x = tx;
                        prev_y = ty;
                    }
                }
            }

            let varint_bytes = encode_varint_u32_stream(&all_zigzag);
            let (best_bytes, technique) = encode_u32_stream_best(&all_zigzag);
            let tech_name = if technique == PHYS_FAST_PFOR {
                "FastPFOR"
            } else {
                "varint"
            };

            println!(
                "Vertex stream ({} interleaved zigzag values):",
                all_zigzag.len()
            );
            println!("  Varint:        {:>8} bytes", varint_bytes.len());
            println!("  Best ({}): {:>8} bytes", tech_name, best_bytes.len());
            if !varint_bytes.is_empty() {
                let ratio = 100.0 * (1.0 - best_bytes.len() as f64 / varint_bytes.len() as f64);
                println!("  Savings:       {:>7.1}%", ratio);
            }
            println!();
        }

        // Dictionary index stream
        {
            let indices: Vec<u32> = (0..500).map(|i| (i % 100) as u32).collect();
            let varint_bytes = encode_varint_u32_stream(&indices);
            let (best_bytes, technique) = encode_u32_stream_best(&indices);
            let tech_name = if technique == PHYS_FAST_PFOR {
                "FastPFOR"
            } else {
                "varint"
            };

            println!("Dict index stream (500 features, 100 unique values):");
            println!("  Varint:        {:>8} bytes", varint_bytes.len());
            println!("  Best ({}): {:>8} bytes", tech_name, best_bytes.len());
            if !varint_bytes.is_empty() {
                let ratio = 100.0 * (1.0 - best_bytes.len() as f64 / varint_bytes.len() as f64);
                println!("  Savings:       {:>7.1}%", ratio);
            }
            println!();
        }

        // FSST dictionary
        {
            let dict_entries: Vec<String> = (0..100)
                .map(|i| format!(
                    "long_category_identifier_{:04}_with_extra_text_to_simulate_real_world_string_property_values_in_vector_tiles",
                    i
                ))
                .collect();
            let refs: Vec<&str> = dict_entries.iter().map(|s| s.as_str()).collect();
            let raw_bytes: usize = dict_entries.iter().map(|s| s.len()).sum();

            if let Some(fsst) = try_fsst_dictionary(&refs) {
                let total_fsst = fsst.compressed_data.len() + fsst.symbol_data.len();
                println!("FSST dictionary (100 entries, {} raw bytes):", raw_bytes);
                println!("  Raw dict:      {:>8} bytes", raw_bytes);
                println!(
                    "  FSST data:     {:>8} bytes (compressed)",
                    fsst.compressed_data.len()
                );
                println!(
                    "  Symbol table:  {:>8} bytes ({} symbols)",
                    fsst.symbol_data.len(),
                    fsst.symbol_lengths.len()
                );
                let ratio = 100.0 * (1.0 - total_fsst as f64 / raw_bytes as f64);
                println!("  Savings:       {:>7.1}%", ratio);
            } else {
                println!("FSST dictionary: not triggered (thresholds not met)");
            }
            println!();
        }

        println!("============================================================");
    }
}
