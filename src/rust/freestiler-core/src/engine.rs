use pmtiles2::util::tile_id;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::pmtiles_writer::{TileFormat, TileSpool};
use crate::tiler::{Feature, Geometry, LayerData, TileCoord};
use crate::{clip, cluster, coalesce, drop, mlt, mvt, pmtiles_writer, simplify, tiler};

/// Configuration for tile generation
pub struct TileConfig {
    pub tile_format: TileFormat,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub base_zoom: Option<u8>,
    pub simplification: bool,
    pub drop_rate: Option<f64>,
    pub cluster_distance: Option<f64>,
    pub cluster_maxzoom: Option<u8>,
    pub coalesce: bool,
    /// Gzip level (0-9) for tile data written by `generate_pmtiles`. `None`
    /// keeps the fast level 1. Tile contents do not change; a higher level
    /// only makes the archive smaller, at some cost in encode time.
    pub gzip_level: Option<u32>,
}

/// Progress reporting trait for binding-specific output
pub trait ProgressReporter: Send + Sync {
    fn report(&self, msg: &str);
}

/// A no-op reporter for quiet mode
pub struct SilentReporter;
impl ProgressReporter for SilentReporter {
    fn report(&self, _msg: &str) {}
}

/// Compute bounding box across all layers
pub fn compute_all_bounds(layers: &[LayerData]) -> (f64, f64, f64, f64) {
    use crate::geo::BoundingRect;
    let mut west = f64::MAX;
    let mut south = f64::MAX;
    let mut east = f64::MIN;
    let mut north = f64::MIN;

    for layer in layers {
        for feature in &layer.features {
            let bbox = match &feature.geometry {
                Geometry::Point(p) => Some(geo_types::Rect::new(p.0, p.0)),
                Geometry::MultiPoint(mp) => mp.bounding_rect(),
                Geometry::LineString(ls) => ls.bounding_rect(),
                Geometry::MultiLineString(mls) => mls.bounding_rect(),
                Geometry::Polygon(p) => p.bounding_rect(),
                Geometry::MultiPolygon(mp) => mp.bounding_rect(),
            };
            if let Some(bb) = bbox {
                west = west.min(bb.min().x);
                south = south.min(bb.min().y);
                east = east.max(bb.max().x);
                north = north.max(bb.max().y);
            }
        }
    }

    (west, south, east, north)
}

/// Generate tiles from layers, spooling each encoded (gzip-compressed) tile
/// to disk as it is produced so peak memory stays bounded.
pub fn generate_tiles(
    layers: &[LayerData],
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
    spool: &mut TileSpool,
) -> Result<(), String> {
    generate_tiles_into(layers, config, reporter, true, &mut |coord, bytes| {
        spool.write_compressed_tile(coord, bytes)
    })
}

/// Shared tile loop. Emits each non-empty tile in ascending tile_id order,
/// gzip-compressed when `compress` is set, otherwise as raw tile bytes.
fn generate_tiles_into(
    layers: &[LayerData],
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
    compress: bool,
    emit: &mut dyn FnMut(TileCoord, &[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let min_z = config.min_zoom;
    let max_z = config.max_zoom;

    // --- Feature dropping setup ---
    let use_drop = config.drop_rate.map_or(false, |r| r > 0.0);
    let drop_rate = config.drop_rate.unwrap_or(-1.0);
    let spatial_indices: Vec<Vec<(usize, u64)>> = if use_drop {
        layers
            .iter()
            .map(|l| drop::compute_spatial_indices(&l.features))
            .collect()
    } else {
        layers.iter().map(|_| Vec::new()).collect()
    };

    // --- Point clustering setup ---
    let use_cluster = config.cluster_distance.map_or(false, |d| d > 0.0);
    let cluster_distance = config.cluster_distance.unwrap_or(-1.0);
    let cluster_max_z = config
        .cluster_maxzoom
        .unwrap_or_else(|| max_z.saturating_sub(1));

    // Determine which layers are all-point (eligible for clustering)
    let is_point_layer: Vec<bool> = layers
        .iter()
        .map(|l| {
            !l.features.is_empty()
                && l.features
                    .iter()
                    .all(|f| matches!(&f.geometry, Geometry::Point(_) | Geometry::MultiPoint(_)))
        })
        .collect();

    // Pre-compute clusters per layer
    let cluster_results: Vec<HashMap<u8, Vec<Feature>>> = if use_cluster {
        layers
            .iter()
            .enumerate()
            .map(|(li, layer)| {
                if is_point_layer[li] {
                    let cfg = cluster::ClusterConfig {
                        distance: cluster_distance,
                        max_zoom: cluster_max_z,
                    };
                    cluster::cluster_points(&layer.features, &cfg, min_z, layer.prop_names.len())
                } else {
                    HashMap::new()
                }
            })
            .collect()
    } else {
        layers.iter().map(|_| HashMap::new()).collect()
    };

    // Build extended prop_names for clustered layers (adds "point_count")
    let cluster_prop_names: Vec<Vec<String>> = layers
        .iter()
        .enumerate()
        .map(|(li, layer)| {
            if use_cluster && is_point_layer[li] {
                let mut names = layer.prop_names.clone();
                names.push("point_count".to_string());
                names
            } else {
                layer.prop_names.clone()
            }
        })
        .collect();

    let do_simplify = config.simplification;
    let do_coalesce = config.coalesce;
    let format = config.tile_format;

    // Which layers contain line features (computed once — only lines are
    // presimplified per zoom, so polygon/point layers skip that buffer).
    let layer_has_lines: Vec<bool> = layers
        .iter()
        .map(|l| {
            l.features.iter().any(|f| {
                matches!(
                    &f.geometry,
                    Geometry::LineString(_) | Geometry::MultiLineString(_)
                )
            })
        })
        .collect();

    // --- Main tile generation loop ---
    let mut total_tiles: u64 = 0;
    let total_start = Instant::now();

    for zoom in min_z..=max_z {
        let zoom_start = Instant::now();
        let pixel_deg = 360.0 / ((1u64 << zoom) as f64 * 4096.0);

        // Per-layer: determine features, presimplify, assign to tiles
        struct ActiveLayer<'a> {
            layer_idx: usize,
            features: &'a [Feature],
            prop_names: &'a [String],
            tile_map: HashMap<TileCoord, Vec<usize>>,
            simplified_geoms: Option<Vec<Option<Geometry>>>,
        }

        impl ActiveLayer<'_> {
            fn geom_for(&self, idx: usize) -> &Geometry {
                self.simplified_geoms
                    .as_ref()
                    .and_then(|v| v[idx].as_ref())
                    .unwrap_or(&self.features[idx].geometry)
            }
        }

        let mut active_layers: Vec<ActiveLayer> = Vec::new();

        for (li, layer) in layers.iter().enumerate() {
            if zoom < layer.min_zoom || zoom > layer.max_zoom {
                continue;
            }

            // Determine features for this layer at this zoom
            let using_clusters = use_cluster && is_point_layer[li] && zoom <= cluster_max_z;
            let features: &[Feature] = if using_clusters {
                cluster_results[li]
                    .get(&zoom)
                    .map(|v| v.as_slice())
                    .unwrap_or(&layer.features)
            } else {
                &layer.features
            };

            let prop_names: &[String] = if using_clusters {
                &cluster_prop_names[li]
            } else {
                &layer.prop_names
            };

            // VW presimplify lines (only for layers that actually have lines —
            // for polygon/point layers this buffer would be all-None waste)
            let simplified_geoms: Option<Vec<Option<Geometry>>> =
                if do_simplify && !using_clusters && layer_has_lines[li] {
                    let vw_tol = simplify::vw_tolerance_for_zoom(zoom);
                    Some(
                        features
                            .par_iter()
                            .map(|f| match &f.geometry {
                                Geometry::LineString(_) | Geometry::MultiLineString(_) => {
                                    Some(simplify::presimplify_line_vw(&f.geometry, vw_tol))
                                }
                                _ => None,
                            })
                            .collect(),
                    )
                } else {
                    None
                };

            // Compute drop mask
            let base_zoom_val = config.base_zoom;
            let layer_base_z = base_zoom_val.unwrap_or(layer.max_zoom);
            let drop_mask = if use_drop && !using_clusters && zoom < layer_base_z {
                Some(drop::compute_drop_mask(
                    features,
                    &spatial_indices[li],
                    zoom,
                    layer_base_z,
                    drop_rate,
                    pixel_deg,
                ))
            } else {
                None
            };

            // Assign features to tiles; dropped features never enter the map
            let tile_map = tiler::assign_features_to_tiles_with_geoms(
                features,
                simplified_geoms.as_deref(),
                drop_mask.as_deref(),
                zoom,
            );

            active_layers.push(ActiveLayer {
                layer_idx: li,
                features,
                prop_names,
                tile_map,
                simplified_geoms,
            });
        }

        // Collect all tile coords across all active layers
        let mut all_coords: HashSet<TileCoord> = HashSet::new();
        for al in &active_layers {
            for coord in al.tile_map.keys() {
                all_coords.insert(*coord);
            }
        }

        let n_tiles = all_coords.len();
        reporter.report(&format!(
            "  Zoom {:>2}/{}: {:>6} tiles ...",
            zoom, max_z, n_tiles
        ));

        // Process tiles in tile_id-sorted chunks: parallel encode + compress
        // within a chunk, then append the chunk to the spool and drop it.
        // Bounds peak memory to one chunk's compressed tiles (plus in-flight
        // encodes) instead of a whole zoom's uncompressed tiles, and keeps the
        // spool data in ascending tile_id order (zooms ascend and tile_id is
        // monotonic in zoom), so the archive stays clustered.
        let mut tile_coords: Vec<TileCoord> = all_coords.into_iter().collect();
        tile_coords.sort_by_key(|c| tile_id(c.z, c.x as u64, c.y as u64));
        let chunk_size = rayon::current_num_threads().saturating_mul(4).max(16);

        let mut n_encoded: u64 = 0;
        for coord_chunk in tile_coords.chunks(chunk_size) {
            let chunk_tiles: Vec<(TileCoord, Vec<u8>)> = coord_chunk
                .par_iter()
                .filter_map(|&coord| {
                    // For each layer, process features for this tile
                    let mut tile_layer_data: Vec<(&str, &[String], Vec<Feature>)> = Vec::new();

                    for al in &active_layers {
                        let layer = &layers[al.layer_idx];

                        if let Some(feature_indices) = al.tile_map.get(&coord) {
                            let mut tile_feats: Vec<Feature> = feature_indices
                                .iter()
                                .filter_map(|&idx| {
                                    let feature = &al.features[idx];
                                    let geom_to_process = al.geom_for(idx);

                                    // Clip to tile boundaries
                                    let clipped =
                                        clip::clip_geometry_to_tile(geom_to_process, &coord)?;

                                    // Snap to tile pixel grid
                                    let geometry = if do_simplify {
                                        simplify::simplify_geometry(&clipped, &coord)
                                    } else {
                                        clipped
                                    };

                                    Some(Feature {
                                        id: feature.id,
                                        geometry,
                                        properties: feature.properties.clone(),
                                    })
                                })
                                .collect();

                            // Sort features spatially (Morton curve) for better compression
                            if tile_feats.len() > 1 {
                                let tb = tiler::tile_bounds(&coord);
                                let tw = tb.min().x;
                                let te = tb.max().x;
                                let ts = tb.min().y;
                                let tn = tb.max().y;
                                tile_feats.sort_by(|a, b| {
                                    let key_a = tiler::tile_morton_key(&a.geometry, tw, te, ts, tn);
                                    let key_b = tiler::tile_morton_key(&b.geometry, tw, te, ts, tn);
                                    key_a.cmp(&key_b).then(a.id.cmp(&b.id))
                                });
                            }

                            // Coalesce features within this tile/layer
                            if do_coalesce && !tile_feats.is_empty() {
                                tile_feats = coalesce::coalesce_features(tile_feats, al.prop_names);
                            }

                            if !tile_feats.is_empty() {
                                tile_layer_data.push((&layer.name, al.prop_names, tile_feats));
                            }
                        }
                    }

                    if tile_layer_data.is_empty() {
                        return None;
                    }

                    // Build references for the encode functions
                    let layer_refs: Vec<(&str, &[String], &[Feature])> = tile_layer_data
                        .iter()
                        .map(|(name, props, feats)| (*name, *props, feats.as_slice()))
                        .collect();

                    let tile_bytes = match format {
                        TileFormat::Mvt => mvt::encode_tile_multilayer(&coord, &layer_refs),
                        TileFormat::Mlt => mlt::encode_tile_multilayer(&coord, &layer_refs),
                    };

                    if tile_bytes.is_empty() {
                        return None;
                    }

                    if !compress {
                        return Some((coord, tile_bytes));
                    }
                    let compressed =
                        pmtiles_writer::gzip_compress_level(&tile_bytes, config.gzip_level)
                            .expect("gzip compression failed");
                    Some((coord, compressed))
                })
                .collect();

            for (coord, bytes) in &chunk_tiles {
                emit(*coord, bytes)?;
            }
            n_encoded += chunk_tiles.len() as u64;
        }

        total_tiles += n_encoded;

        let elapsed = zoom_start.elapsed().as_secs_f64();
        reporter.report(&format!(
            "           {:>6} encoded ({:.1}s)",
            n_encoded, elapsed
        ));
    }

    reporter.report(&format!(
        "  Total: {} tiles in {:.1}s",
        total_tiles,
        total_start.elapsed().as_secs_f64()
    ));

    Ok(())
}

/// TileJSON layer metadata; clustered point layers gain `point_count`.
fn layer_meta(l: &LayerData, clustered: bool) -> pmtiles_writer::LayerMeta {
    let mut names = l.prop_names.clone();
    let mut types = l.prop_types.clone();
    if clustered {
        names.push("point_count".to_string());
        types.push("integer".to_string());
    }
    // Detect predominant geometry type from first feature
    let geometry_type = l.features.first().map(|f| match &f.geometry {
        Geometry::Point(_) | Geometry::MultiPoint(_) => "Point".to_string(),
        Geometry::LineString(_) | Geometry::MultiLineString(_) => "Line".to_string(),
        Geometry::Polygon(_) | Geometry::MultiPolygon(_) => "Polygon".to_string(),
    });
    pmtiles_writer::LayerMeta {
        name: l.name.clone(),
        property_names: names,
        property_types: types,
        min_zoom: l.min_zoom,
        max_zoom: l.max_zoom,
        geometry_type,
    }
}

/// Refuse a gzip level outside 0-9 before any output is produced.
fn validate_gzip_level(config: &TileConfig) -> Result<(), String> {
    if config.gzip_level.map_or(false, |level| level > 9) {
        return Err("gzip_level must be between 0 and 9".to_string());
    }
    Ok(())
}

/// Full pipeline: generate tiles and write PMTiles archive
pub fn generate_pmtiles(
    layers: &[LayerData],
    output_path: &str,
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
) -> Result<(), String> {
    validate_gzip_level(config)?;
    let bounds = compute_all_bounds(layers);

    // Build layer metadata for PMTiles
    let use_cluster = config.cluster_distance.map_or(false, |d| d > 0.0);
    let is_point_layer: Vec<bool> = layers
        .iter()
        .map(|l| {
            !l.features.is_empty()
                && l.features
                    .iter()
                    .all(|f| matches!(&f.geometry, Geometry::Point(_) | Geometry::MultiPoint(_)))
        })
        .collect();

    let layer_metas: Vec<pmtiles_writer::LayerMeta> = layers
        .iter()
        .enumerate()
        .map(|(li, l)| layer_meta(l, use_cluster && is_point_layer[li]))
        .collect();

    let mut spool = TileSpool::new()?;
    generate_tiles(layers, config, reporter, &mut spool)?;

    reporter.report(&format!(
        "  Writing PMTiles archive ({} tiles) ...",
        spool.len()
    ));
    let write_start = Instant::now();
    pmtiles_writer::write_pmtiles_from_spool(
        output_path,
        &mut spool,
        config.tile_format,
        &layer_metas,
        config.min_zoom,
        config.max_zoom,
        bounds,
    )?;
    reporter.report(&format!(
        "  PMTiles write: {:.1}s",
        write_start.elapsed().as_secs_f64()
    ));

    Ok(())
}

/// Builds a multi-layer tileset one layer at a time, so peak memory follows
/// the largest layer rather than the sum of all layers.
///
/// Each layer is tiled on its own and its per-tile encodings (raw MVT/MLT
/// layer messages) go to a disk spool with a small `(tile, layer) -> extent`
/// index. [`LayeredTiler::finish`] concatenates the encodings of each tile in
/// layer order and gzips them. In both formats a tile is a sequence of
/// independently encoded layers, and every per-layer step (clipping,
/// snapping, dropping, clustering, coalescing, feature ordering) depends only
/// on that layer, so the archive is byte-identical to [`generate_pmtiles`]
/// given the same layers in the same order.
pub struct LayeredTiler {
    config: TileConfig,
    spool: pmtiles_writer::RawSpool,
    /// tile_id -> (coord, [(offset, length)] in layer order)
    index: std::collections::BTreeMap<u64, (TileCoord, Vec<(u64, u32)>)>,
    metas: Vec<pmtiles_writer::LayerMeta>,
    bounds: (f64, f64, f64, f64),
    assembly_budget: u64,
}

impl LayeredTiler {
    /// Default raw bytes held while assembling tiles (64 MiB). A single
    /// tile larger than the budget is still assembled on its own.
    pub const DEFAULT_ASSEMBLY_BUDGET: u64 = 64 << 20;

    pub fn new(config: TileConfig) -> Result<Self, String> {
        validate_gzip_level(&config)?;
        Ok(Self {
            config,
            spool: pmtiles_writer::RawSpool::new()?,
            index: std::collections::BTreeMap::new(),
            metas: Vec::new(),
            bounds: (f64::MAX, f64::MAX, f64::MIN, f64::MIN),
            assembly_budget: Self::DEFAULT_ASSEMBLY_BUDGET,
        })
    }

    /// Set the raw bytes held at once while assembling tiles in
    /// [`finish`](Self::finish). Output does not depend on it.
    pub fn set_assembly_budget(&mut self, bytes: u64) {
        self.assembly_budget = bytes;
    }

    /// Number of layers added so far.
    pub fn layer_count(&self) -> usize {
        self.metas.len()
    }

    /// Tile one layer, spool its encodings, and drop it.
    pub fn add_layer(
        &mut self,
        layer: LayerData,
        reporter: &dyn ProgressReporter,
    ) -> Result<(), String> {
        let use_cluster = self.config.cluster_distance.map_or(false, |d| d > 0.0);
        let is_point = !layer.features.is_empty()
            && layer
                .features
                .iter()
                .all(|f| matches!(&f.geometry, Geometry::Point(_) | Geometry::MultiPoint(_)));
        reporter.report(&format!(
            "  Layer '{}': {} features (zoom {}-{})",
            layer.name,
            layer.features.len(),
            layer.min_zoom,
            layer.max_zoom
        ));
        self.metas.push(layer_meta(&layer, use_cluster && is_point));
        let (w, s, e, n) = compute_all_bounds(std::slice::from_ref(&layer));
        self.bounds = (
            self.bounds.0.min(w),
            self.bounds.1.min(s),
            self.bounds.2.max(e),
            self.bounds.3.max(n),
        );
        let spool = &mut self.spool;
        let index = &mut self.index;
        generate_tiles_into(
            std::slice::from_ref(&layer),
            &self.config,
            reporter,
            false,
            &mut |coord, bytes| {
                let extent = spool.append(bytes)?;
                index
                    .entry(tile_id(coord.z, coord.x as u64, coord.y as u64))
                    .or_insert_with(|| (coord, Vec::new()))
                    .1
                    .push(extent);
                Ok(())
            },
        )
    }

    /// Assemble the archive. Fails, like [`generate_pmtiles`], when no layer
    /// produced a tile.
    pub fn finish(
        mut self,
        output_path: &str,
        reporter: &dyn ProgressReporter,
    ) -> Result<(), String> {
        let mut tiles = TileSpool::new()?;
        let entries: Vec<_> = std::mem::take(&mut self.index).into_values().collect();
        // Assemble tiles in batches bounded by both a tile count (for
        // parallel compression) and the raw bytes held at once, so memory
        // does not grow with the number of layers stacked in each tile.
        let chunk_size = rayon::current_num_threads().saturating_mul(4).max(16);
        let mut start = 0;
        while start < entries.len() {
            let mut end = start;
            let mut held: u64 = 0;
            while end < entries.len()
                && end - start < chunk_size
                && (end == start || held < self.assembly_budget)
            {
                held += entries[end].1.iter().map(|e| u64::from(e.1)).sum::<u64>();
                end += 1;
            }
            let mut raw = Vec::with_capacity(end - start);
            for (coord, extents) in &entries[start..end] {
                let mut bytes = Vec::new();
                for &extent in extents {
                    self.spool.read_into(extent, &mut bytes)?;
                }
                raw.push((*coord, bytes));
            }
            let compressed: Vec<(TileCoord, Result<Vec<u8>, String>)> = raw
                .into_par_iter()
                .map(|(coord, bytes)| {
                    let level = self.config.gzip_level;
                    (coord, pmtiles_writer::gzip_compress_level(&bytes, level))
                })
                .collect();
            for (coord, bytes) in compressed {
                tiles.write_compressed_tile(coord, &bytes?)?;
            }
            start = end;
        }
        reporter.report(&format!(
            "  Writing PMTiles archive ({} tiles from {} layers) ...",
            tiles.len(),
            self.metas.len()
        ));
        pmtiles_writer::write_pmtiles_from_spool(
            output_path,
            &mut tiles,
            self.config.tile_format,
            &self.metas,
            self.config.min_zoom,
            self.config.max_zoom,
            self.bounds,
        )
    }
}
