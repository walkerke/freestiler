use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

use flate2::write::GzEncoder;
use flate2::Compression;
use pmtiles2::util::tile_id;
use pmtiles2::{Compression as PmCompression, PMTiles, TileType};
use rayon::prelude::*;
use serde_json::{json, Value};

use crate::tiler::TileCoord;

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
}

/// Gzip-compress a tile at level 1 (fast)
fn gzip_compress_fast(data: &[u8]) -> Result<Vec<u8>, String> {
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
            json!({
                "id": l.name,
                "fields": fields,
                "minzoom": l.min_zoom,
                "maxzoom": l.max_zoom
            })
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
            let compressed = gzip_compress_fast(&data).expect("gzip compression failed");
            (coord, compressed)
        })
        .collect();

    // Add pre-compressed tiles to archive
    for (coord, compressed) in compressed_tiles {
        let tid = tile_id(coord.z, coord.x as u64, coord.y as u64);
        pm.add_tile(tid, compressed)
            .map_err(|e| format!("Error adding tile z={} x={} y={}: {}", coord.z, coord.x, coord.y, e))?;
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
