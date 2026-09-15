use std::fs::{self, File, OpenOptions};
use std::io::{copy, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;
use pmtiles2::util::{tile_id, write_directories};
use pmtiles2::{Compression as PmCompression, Entry, Header, TileType};
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

/// Disk spool for compressed tiles: tile bytes go to a temp file as they are
/// produced, so only the directory entries (~32 bytes/tile) stay in memory.
/// The spool file is deleted when the spool is dropped.
pub struct TileSpool {
    path: PathBuf,
    file: BufWriter<File>,
    offset: u64,
    entries: Vec<Entry>,
}

impl TileSpool {
    pub fn new() -> Result<Self, String> {
        // Owner-only permissions: the spool lives in the shared temp
        // directory and holds the full tile data.
        let (file, path) = create_exclusive_temp(&std::env::temp_dir(), "freestiler_tiles", true)?;
        Ok(Self {
            path,
            file: BufWriter::new(file),
            offset: 0,
            entries: Vec::new(),
        })
    }

    /// Compress a raw tile and append it to the spool.
    pub fn write_tile(&mut self, coord: TileCoord, bytes: &[u8]) -> Result<(), String> {
        let compressed = gzip_compress(bytes)?;
        self.write_compressed_tile(coord, &compressed)
    }

    /// Append an already gzip-compressed tile to the spool.
    pub fn write_compressed_tile(
        &mut self,
        coord: TileCoord,
        compressed: &[u8],
    ) -> Result<(), String> {
        self.file
            .write_all(compressed)
            .map_err(|e| format!("Cannot write tile spool: {}", e))?;

        self.entries.push(Entry {
            tile_id: tile_id(coord.z, coord.x as u64, coord.y as u64),
            offset: self.offset,
            length: compressed.len() as u32,
            run_length: 1,
        });
        self.offset += compressed.len() as u64;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn flush(&mut self) -> Result<(), String> {
        self.file
            .flush()
            .map_err(|e| format!("Cannot flush tile spool: {}", e))
    }
}

impl Drop for TileSpool {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Removes the file at `path` on drop unless disarmed.
struct TempFileGuard {
    path: PathBuf,
    armed: bool,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Assemble a PMTiles archive from a tile spool.
///
/// The archive is first written to a unique sibling temp file in the output's
/// directory, then atomically renamed onto `output_path` — an existing
/// destination is replaced on success and left untouched on failure.
pub fn write_pmtiles_from_spool(
    output_path: &str,
    spool: &mut TileSpool,
    format: TileFormat,
    layers: &[LayerMeta],
    min_zoom: u8,
    max_zoom: u8,
    bounds: (f64, f64, f64, f64), // (west, south, east, north)
) -> Result<(), String> {
    spool.flush()?;
    let mut entries = std::mem::take(&mut spool.entries);
    if entries.is_empty() {
        return Err("No tiles generated".to_string());
    }

    entries.sort_by_key(|entry| entry.tile_id);

    // PMTiles v3 `clustered` requires the tile DATA to be laid out in
    // ascending tile_id order, not just the directory entries.
    let clustered = entries.first().is_some_and(|e| e.offset == 0)
        && entries
            .windows(2)
            .all(|w| w[1].offset == w[0].offset + u64::from(w[0].length));

    let out_path = Path::new(output_path);
    let out_dir = match out_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let tmp_stem = format!(
        "{}.tmp",
        out_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("freestiler")
    );
    let (output, tmp_path) = create_exclusive_temp(out_dir, &tmp_stem, false)?;
    let mut guard = TempFileGuard {
        path: tmp_path.clone(),
        armed: true,
    };
    let mut output = BufWriter::new(output);

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
    let mut spool_reader = File::open(&spool.path)
        .map_err(|e| format!("Cannot read tile spool {}: {}", spool.path.display(), e))?;
    let tile_data_length = copy(&mut spool_reader, &mut output)
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
    header.clustered = clustered;
    header.internal_compression = PmCompression::GZip;
    header.tile_compression = PmCompression::GZip;
    header.tile_type = match format {
        TileFormat::Mvt => TileType::Mvt,
        TileFormat::Mlt => TileType::Mvt, // patched to MLT below
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

    // For MLT format, patch the tile_type byte in the header.
    // PMTiles v3 header: byte 99 is tile_type.
    if format == TileFormat::Mlt {
        output
            .seek(SeekFrom::Start(99))
            .map_err(|e| format!("Seek error: {}", e))?;
        output
            .write_all(&[0x06])
            .map_err(|e| format!("Write error: {}", e))?;
    }

    let output = output
        .into_inner()
        .map_err(|e| format!("Cannot flush PMTiles output: {}", e))?;
    output
        .sync_all()
        .map_err(|e| format!("Cannot sync PMTiles output: {}", e))?;
    drop(output);

    fs::rename(&tmp_path, out_path).map_err(|e| {
        format!(
            "Cannot move {} to {}: {}",
            tmp_path.display(),
            output_path,
            e
        )
    })?;
    guard.armed = false;

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

/// Create a uniquely named file in `dir`, exclusively (`create_new`), retrying
/// on name collisions. Returns the open handle and the path. With `private`,
/// the file is created owner-only (0600) on Unix; without it the process
/// umask applies, which is right for a temp file that will be renamed into
/// place as the final output.
fn create_exclusive_temp(dir: &Path, stem: &str, private: bool) -> Result<(File, PathBuf), String> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create_new(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    for _ in 0..16 {
        let candidate = dir.join(format!("{}.{}", stem, unique_suffix()));
        match opts.open(&candidate) {
            Ok(file) => return Ok((file, candidate)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(format!(
                    "Cannot create temporary file {}: {}",
                    candidate.display(),
                    e
                ))
            }
        }
    }
    Err(format!(
        "Cannot create a unique temporary file in {}",
        dir.display()
    ))
}

pub(crate) fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}_{}", std::process::id(), nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord(z: u8, x: u32, y: u32) -> TileCoord {
        TileCoord { z, x, y }
    }

    fn test_layers() -> Vec<LayerMeta> {
        vec![LayerMeta {
            name: "test".to_string(),
            property_names: vec![],
            min_zoom: 0,
            max_zoom: 2,
            geometry_type: None,
        }]
    }

    fn temp_output(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "freestiler_test_{}_{}.pmtiles",
            name,
            unique_suffix()
        ))
    }

    fn read_header(path: &Path) -> Header {
        let mut f = File::open(path).unwrap();
        Header::from_reader(&mut f).unwrap()
    }

    fn gunzip(data: &[u8]) -> Vec<u8> {
        use std::io::Read;
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(data)
            .read_to_end(&mut out)
            .unwrap();
        out
    }

    #[test]
    fn tile_id_ordered_spool_is_clustered() {
        let mut spool = TileSpool::new().unwrap();
        // Ascending tile_id: z0 (0,0) = 0, z1 (0,0) = 1, z1 (1,1) = 3
        for c in [coord(0, 0, 0), coord(1, 0, 0), coord(1, 1, 1)] {
            spool.write_tile(c, b"tile-bytes").unwrap();
        }
        let out = temp_output("clustered");
        write_pmtiles_from_spool(
            out.to_str().unwrap(),
            &mut spool,
            TileFormat::Mvt,
            &test_layers(),
            0,
            2,
            (0.0, 0.0, 1.0, 1.0),
        )
        .unwrap();

        let header = read_header(&out);
        assert!(header.clustered);
        assert_eq!(header.num_addressed_tiles, 3);
        assert_eq!(header.num_tile_entries, 3);
        assert_eq!(header.num_tile_content, 3);

        // The archive parses and round-trips tile content
        let f = File::open(&out).unwrap();
        let mut pm = pmtiles2::PMTiles::from_reader(f).unwrap();
        assert_eq!(pm.num_tiles(), 3);
        assert_eq!(
            gunzip(&pm.get_tile(1, 1, 1).unwrap().unwrap()),
            b"tile-bytes"
        );

        fs::remove_file(&out).unwrap();
    }

    #[test]
    fn out_of_order_spool_is_not_clustered() {
        let mut spool = TileSpool::new().unwrap();
        // Written in descending tile_id order: data layout is not clustered
        for c in [coord(1, 1, 1), coord(1, 0, 0), coord(0, 0, 0)] {
            spool.write_tile(c, b"tile-bytes").unwrap();
        }
        let out = temp_output("unclustered");
        write_pmtiles_from_spool(
            out.to_str().unwrap(),
            &mut spool,
            TileFormat::Mvt,
            &test_layers(),
            0,
            2,
            (0.0, 0.0, 1.0, 1.0),
        )
        .unwrap();

        let header = read_header(&out);
        assert!(!header.clustered);

        // Directory entries are still sorted and the archive still reads back
        let f = File::open(&out).unwrap();
        let mut pm = pmtiles2::PMTiles::from_reader(f).unwrap();
        assert_eq!(pm.num_tiles(), 3);
        assert_eq!(
            gunzip(&pm.get_tile(0, 0, 0).unwrap().unwrap()),
            b"tile-bytes"
        );

        fs::remove_file(&out).unwrap();
    }

    #[test]
    fn mlt_format_patches_tile_type_byte() {
        let mut spool = TileSpool::new().unwrap();
        spool.write_tile(coord(0, 0, 0), b"tile-bytes").unwrap();
        let out = temp_output("mlt");
        write_pmtiles_from_spool(
            out.to_str().unwrap(),
            &mut spool,
            TileFormat::Mlt,
            &test_layers(),
            0,
            0,
            (0.0, 0.0, 1.0, 1.0),
        )
        .unwrap();

        let bytes = fs::read(&out).unwrap();
        assert_eq!(bytes[99], 0x06);
        fs::remove_file(&out).unwrap();
    }

    #[test]
    fn existing_output_replaced_atomically() {
        let out = temp_output("replace");
        fs::write(&out, b"OLD CONTENT").unwrap();

        let mut spool = TileSpool::new().unwrap();
        spool.write_tile(coord(0, 0, 0), b"tile-bytes").unwrap();
        write_pmtiles_from_spool(
            out.to_str().unwrap(),
            &mut spool,
            TileFormat::Mvt,
            &test_layers(),
            0,
            0,
            (0.0, 0.0, 1.0, 1.0),
        )
        .unwrap();

        let header = read_header(&out);
        assert_eq!(header.num_addressed_tiles, 1);
        fs::remove_file(&out).unwrap();
    }

    #[test]
    fn failed_write_preserves_existing_output() {
        let out = temp_output("preserve");
        fs::write(&out, b"PRECIOUS").unwrap();

        // Empty spool → error, and the existing destination is untouched
        let mut spool = TileSpool::new().unwrap();
        let result = write_pmtiles_from_spool(
            out.to_str().unwrap(),
            &mut spool,
            TileFormat::Mvt,
            &test_layers(),
            0,
            0,
            (0.0, 0.0, 1.0, 1.0),
        );
        assert!(result.is_err());
        assert_eq!(fs::read(&out).unwrap(), b"PRECIOUS");
        fs::remove_file(&out).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn spool_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let spool = TileSpool::new().unwrap();
        let mode = fs::metadata(&spool.path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn failure_during_assembly_cleans_up_and_preserves_destination() {
        let dir = std::env::temp_dir().join(format!("freestiler_test_fail_{}", unique_suffix()));
        fs::create_dir(&dir).unwrap();
        let out = dir.join("out.pmtiles");
        fs::write(&out, b"PRECIOUS").unwrap();

        let mut spool = TileSpool::new().unwrap();
        spool.write_tile(coord(0, 0, 0), b"tile-bytes").unwrap();
        // Delete the spool file so the tile-data copy fails after the temp
        // output has been created and the directories written into it.
        fs::remove_file(&spool.path).unwrap();

        let result = write_pmtiles_from_spool(
            out.to_str().unwrap(),
            &mut spool,
            TileFormat::Mvt,
            &test_layers(),
            0,
            0,
            (0.0, 0.0, 1.0, 1.0),
        );
        assert!(result.is_err());
        assert_eq!(fs::read(&out).unwrap(), b"PRECIOUS");

        // The failed temp output must not be left behind.
        let leftovers: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| n != "out.pmtiles")
            .collect();
        assert!(leftovers.is_empty(), "leftover files: {:?}", leftovers);

        fs::remove_file(&out).unwrap();
        fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn spool_file_removed_on_drop() {
        let spool = TileSpool::new().unwrap();
        let path = spool.path.clone();
        assert!(path.exists());
        drop(spool);
        assert!(!path.exists());
    }
}
