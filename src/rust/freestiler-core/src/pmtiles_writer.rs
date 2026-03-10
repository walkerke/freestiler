use std::fs::File;
use std::io::{copy, Seek, SeekFrom, Write};

use flate2::write::GzEncoder;
use flate2::Compression;
use pmtiles2::util::{tile_id, write_directories};
use pmtiles2::{Compression as PmCompression, Entry, Header, PMTiles, TileType};
use rayon::prelude::*;
use serde_json::{json, Value};

use crate::tiler::TileCoord;

const PMTILES_HEADER_BYTES: u64 = 127;

/// Tile format selection
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TileFormat {
    Mvt,
    Mlt,
}

/// Metadata for a single layer in the PMTiles archive
pub struct LayerMeta {
    pub name: String,
    pub property_names: Vec<String>,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub geometry_type: Option<String>,
}

/// Gzip-compress a tile at level 1 (fast)
pub fn gzip_compress(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder
        .write_all(data)
        .map_err(|e| format!("Compression error: {}", e))?;
    encoder
        .finish()
        .map_err(|e| format!("Compression finish error: {}", e))
}

/// Write tiles into a PMTiles archive
pub fn write_pmtiles(
    output_path: &str,
    tiles: Vec<(TileCoord, Vec<u8>)>,
    format: TileFormat,
    layers: &[LayerMeta],
    min_zoom: u8,
    max_zoom: u8,
    bounds: (f64, f64, f64, f64), // (west, south, east, north)
) -> Result<(), String> {
    let tile_type = match format {
        TileFormat::Mvt => TileType::Mvt,
        TileFormat::Mlt => TileType::Mvt, // Use Mvt as placeholder, patch header later for MLT
    };

    let mut pm = PMTiles::new(tile_type, PmCompression::GZip);

    // Set header fields
    pm.min_zoom = min_zoom;
    pm.max_zoom = max_zoom;
    pm.center_zoom = ((min_zoom as u16 + max_zoom as u16) / 2) as u8;
    pm.min_longitude = bounds.0;
    pm.min_latitude = bounds.1;
    pm.max_longitude = bounds.2;
    pm.max_latitude = bounds.3;
    pm.center_longitude = (bounds.0 + bounds.2) / 2.0;
    pm.center_latitude = (bounds.1 + bounds.3) / 2.0;

    // Build TileJSON metadata with multi-layer support
    let vector_layers: Vec<Value> = layers
        .iter()
        .map(|l| {
            let mut fields = serde_json::Map::new();
            for name in &l.property_names {
                fields.insert(name.clone(), Value::String("string".to_string()));
            }
            let mut layer_json = json!({
                "id": l.name,
                "fields": fields,
                "minzoom": l.min_zoom,
                "maxzoom": l.max_zoom
            });
            if let Some(ref gt) = l.geometry_type {
                layer_json["geometry_type"] = Value::String(gt.clone());
            }
            layer_json
        })
        .collect();

    let metadata = json!({
        "vector_layers": vector_layers
    });
    pm.meta_data = metadata.as_object().unwrap().clone();

    // Parallel gzip compression at level 1 (fast) using rayon
    let compressed_tiles: Vec<(TileCoord, Vec<u8>)> = tiles
        .into_par_iter()
        .filter(|(_, data)| !data.is_empty())
        .map(|(coord, data)| {
            let compressed = gzip_compress(&data).expect("gzip compression failed");
            (coord, compressed)
        })
        .collect();

    // Add pre-compressed tiles to archive
    for (coord, compressed) in compressed_tiles {
        let tid = tile_id(coord.z, coord.x as u64, coord.y as u64);
        pm.add_tile(tid, compressed).map_err(|e| {
            format!(
                "Error adding tile z={} x={} y={}: {}",
                coord.z, coord.x, coord.y, e
            )
        })?;
    }

    // Write to file
    let mut file =
        File::create(output_path).map_err(|e| format!("Cannot create {}: {}", output_path, e))?;
    pm.to_writer(&mut file)
        .map_err(|e| format!("Error writing PMTiles: {}", e))?;

    // For MLT format, patch the tile_type byte in the header
    // PMTiles v3 header: byte 99 is tile_type
    if format == TileFormat::Mlt {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(output_path)
            .map_err(|e| format!("Cannot reopen {}: {}", output_path, e))?;
        file.seek(SeekFrom::Start(99))
            .map_err(|e| format!("Seek error: {}", e))?;
        file.write_all(&[0x06])
            .map_err(|e| format!("Write error: {}", e))?;
    }

    Ok(())
}

pub fn write_pmtiles_from_spool(
    output_path: &str,
    spool_path: &std::path::Path,
    mut entries: Vec<Entry>,
    format: TileFormat,
    layers: &[LayerMeta],
    min_zoom: u8,
    max_zoom: u8,
    bounds: (f64, f64, f64, f64),
) -> Result<(), String> {
    if entries.is_empty() {
        return Err("No tiles generated".to_string());
    }

    entries.sort_by_key(|entry| entry.tile_id);

    let mut output =
        File::create(output_path).map_err(|e| format!("Cannot create {}: {}", output_path, e))?;
    output
        .seek(SeekFrom::Start(PMTILES_HEADER_BYTES))
        .map_err(|e| format!("Cannot seek output PMTiles header: {}", e))?;

    let root_directory_offset = PMTILES_HEADER_BYTES;
    let leaf_directories = write_directories(&mut output, &entries, PmCompression::GZip, None)
        .map_err(|e| format!("Cannot write PMTiles directory: {}", e))?;
    let root_directory_length = output
        .stream_position()
        .map_err(|e| format!("Cannot compute root directory length: {}", e))?
        - root_directory_offset;

    let json_metadata_offset = root_directory_offset + root_directory_length;
    let metadata_bytes = build_metadata_bytes(layers)?;
    output
        .write_all(&metadata_bytes)
        .map_err(|e| format!("Cannot write PMTiles metadata: {}", e))?;
    let json_metadata_length = metadata_bytes.len() as u64;

    let leaf_directories_offset = json_metadata_offset + json_metadata_length;
    output
        .write_all(&leaf_directories)
        .map_err(|e| format!("Cannot write PMTiles leaf directories: {}", e))?;
    let leaf_directories_length = leaf_directories.len() as u64;

    let tile_data_offset = leaf_directories_offset + leaf_directories_length;
    let mut spool = File::open(spool_path)
        .map_err(|e| format!("Cannot read tile spool {}: {}", spool_path.display(), e))?;
    let tile_data_length = copy(&mut spool, &mut output)
        .map_err(|e| format!("Cannot copy PMTiles tile data: {}", e))?;

    let mut header = Header::default();
    header.root_directory_offset = root_directory_offset;
    header.root_directory_length = root_directory_length;
    header.json_metadata_offset = json_metadata_offset;
    header.json_metadata_length = json_metadata_length;
    header.leaf_directories_offset = leaf_directories_offset;
    header.leaf_directories_length = leaf_directories_length;
    header.tile_data_offset = tile_data_offset;
    header.tile_data_length = tile_data_length;
    header.num_addressed_tiles = entries.len() as u64;
    header.num_tile_entries = entries.len() as u64;
    header.num_tile_content = entries.len() as u64;
    header.clustered = true;
    header.internal_compression = PmCompression::GZip;
    header.tile_compression = PmCompression::GZip;
    header.tile_type = match format {
        TileFormat::Mvt => TileType::Mvt,
        TileFormat::Mlt => TileType::Mvt,
    };
    header.min_zoom = min_zoom;
    header.max_zoom = max_zoom;
    header.center_zoom = ((min_zoom as u16 + max_zoom as u16) / 2) as u8;
    header.min_pos.longitude = bounds.0;
    header.min_pos.latitude = bounds.1;
    header.max_pos.longitude = bounds.2;
    header.max_pos.latitude = bounds.3;
    header.center_pos.longitude = (bounds.0 + bounds.2) / 2.0;
    header.center_pos.latitude = (bounds.1 + bounds.3) / 2.0;

    output
        .seek(SeekFrom::Start(0))
        .map_err(|e| format!("Cannot seek PMTiles header start: {}", e))?;
    header
        .to_writer(&mut output)
        .map_err(|e| format!("Cannot write PMTiles header: {}", e))?;

    if format == TileFormat::Mlt {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(output_path)
            .map_err(|e| format!("Cannot reopen {}: {}", output_path, e))?;
        file.seek(SeekFrom::Start(99))
            .map_err(|e| format!("Seek error: {}", e))?;
        file.write_all(&[0x06])
            .map_err(|e| format!("Write error: {}", e))?;
    }

    Ok(())
}

fn build_metadata_bytes(layers: &[LayerMeta]) -> Result<Vec<u8>, String> {
    let vector_layers: Vec<Value> = layers
        .iter()
        .map(|l| {
            let mut fields = serde_json::Map::new();
            for name in &l.property_names {
                fields.insert(name.clone(), Value::String("string".to_string()));
            }
            let mut layer_json = json!({
                "id": l.name,
                "fields": fields,
                "minzoom": l.min_zoom,
                "maxzoom": l.max_zoom
            });
            if let Some(ref gt) = l.geometry_type {
                layer_json["geometry_type"] = Value::String(gt.clone());
            }
            layer_json
        })
        .collect();

    let metadata = json!({
        "vector_layers": vector_layers
    });

    let metadata_json =
        serde_json::to_vec(&metadata).map_err(|e| format!("Metadata JSON error: {}", e))?;
    gzip_compress(&metadata_json)
}
