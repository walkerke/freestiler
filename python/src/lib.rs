use pyo3::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use geozero::wkb::Wkb;
use geozero::ToGeo;

mod clip;
mod cluster;
mod coalesce;
#[path = "drop.rs"]
mod drop_mod;
mod mlt;
mod mvt;
mod pmtiles_writer;
mod simplify;
mod tiler;

use pmtiles_writer::TileFormat;
use tiler::{Feature, Geometry, LayerData, PropertyValue, TileCoord};

// ---------------------------------------------------------------------------
// WKB parsing
// ---------------------------------------------------------------------------

fn wkb_to_geometry(wkb_bytes: &[u8]) -> Option<Geometry> {
    let geo_geom = Wkb(wkb_bytes).to_geo().ok()?;
    match geo_geom {
        geo_types::Geometry::Point(p) => Some(Geometry::Point(p)),
        geo_types::Geometry::MultiPoint(mp) => Some(Geometry::MultiPoint(mp)),
        geo_types::Geometry::LineString(ls) => Some(Geometry::LineString(ls)),
        geo_types::Geometry::MultiLineString(mls) => Some(Geometry::MultiLineString(mls)),
        geo_types::Geometry::Polygon(p) => Some(Geometry::Polygon(p)),
        geo_types::Geometry::MultiPolygon(mp) => Some(Geometry::MultiPolygon(mp)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Layer parsing from Python dicts
// ---------------------------------------------------------------------------

fn parse_layers_from_py(
    py: Python<'_>,
    layers: &[PyObject],
    generate_ids: bool,
) -> PyResult<Vec<LayerData>> {
    let mut result = Vec::new();
    let mut id_offset: u64 = 0;

    for (_layer_idx, layer_obj) in layers.iter().enumerate() {
        let layer = layer_obj.bind(py);

        // Extract fields from dict
        let name: String = layer.get_item("name")?.extract()?;
        let wkb_list: Vec<Vec<u8>> = layer.get_item("wkb")?.extract()?;
        let geom_types: Vec<String> = layer.get_item("geom_types")?.extract()?;
        let prop_names: Vec<String> = layer.get_item("prop_names")?.extract()?;
        let prop_types: Vec<String> = layer.get_item("prop_types")?.extract()?;
        let string_columns: Vec<Vec<Option<String>>> =
            layer.get_item("string_columns")?.extract()?;
        let int_columns: Vec<Vec<Option<i64>>> =
            layer.get_item("int_columns")?.extract()?;
        let float_columns: Vec<Vec<Option<f64>>> =
            layer.get_item("float_columns")?.extract()?;
        let bool_columns: Vec<Vec<Option<bool>>> =
            layer.get_item("bool_columns")?.extract()?;
        let layer_min_zoom: u8 = layer.get_item("min_zoom")?.extract()?;
        let layer_max_zoom: u8 = layer.get_item("max_zoom")?.extract()?;

        let n_features = wkb_list.len();

        // Build property column index mapping
        let mut string_col_idx = 0usize;
        let mut int_col_idx = 0usize;
        let mut float_col_idx = 0usize;
        let mut bool_col_idx = 0usize;

        struct ColMapping {
            kind: &'static str,
            col_index: usize,
        }

        let mut mappings: Vec<ColMapping> = Vec::new();
        for ptype in &prop_types {
            match ptype.as_str() {
                "string" => {
                    mappings.push(ColMapping {
                        kind: "string",
                        col_index: string_col_idx,
                    });
                    string_col_idx += 1;
                }
                "integer" => {
                    mappings.push(ColMapping {
                        kind: "integer",
                        col_index: int_col_idx,
                    });
                    int_col_idx += 1;
                }
                "double" => {
                    mappings.push(ColMapping {
                        kind: "double",
                        col_index: float_col_idx,
                    });
                    float_col_idx += 1;
                }
                "boolean" => {
                    mappings.push(ColMapping {
                        kind: "boolean",
                        col_index: bool_col_idx,
                    });
                    bool_col_idx += 1;
                }
                _ => {
                    mappings.push(ColMapping {
                        kind: "string",
                        col_index: string_col_idx,
                    });
                    string_col_idx += 1;
                }
            }
        }

        // Parse features
        let mut features = Vec::with_capacity(n_features);
        for i in 0..n_features {
            let geom = wkb_to_geometry(&wkb_list[i]);
            if let Some(geometry) = geom {
                let mut properties = Vec::with_capacity(prop_names.len());
                for mapping in &mappings {
                    let prop = match mapping.kind {
                        "string" => {
                            if mapping.col_index < string_columns.len() {
                                let col = &string_columns[mapping.col_index];
                                if i < col.len() {
                                    match &col[i] {
                                        Some(s) => PropertyValue::String(s.clone()),
                                        None => PropertyValue::Null,
                                    }
                                } else {
                                    PropertyValue::Null
                                }
                            } else {
                                PropertyValue::Null
                            }
                        }
                        "integer" => {
                            if mapping.col_index < int_columns.len() {
                                let col = &int_columns[mapping.col_index];
                                if i < col.len() {
                                    match col[i] {
                                        Some(v) => PropertyValue::Int(v),
                                        None => PropertyValue::Null,
                                    }
                                } else {
                                    PropertyValue::Null
                                }
                            } else {
                                PropertyValue::Null
                            }
                        }
                        "double" => {
                            if mapping.col_index < float_columns.len() {
                                let col = &float_columns[mapping.col_index];
                                if i < col.len() {
                                    match col[i] {
                                        Some(v) if v.is_nan() => PropertyValue::Null,
                                        Some(v) => PropertyValue::Double(v),
                                        None => PropertyValue::Null,
                                    }
                                } else {
                                    PropertyValue::Null
                                }
                            } else {
                                PropertyValue::Null
                            }
                        }
                        "boolean" => {
                            if mapping.col_index < bool_columns.len() {
                                let col = &bool_columns[mapping.col_index];
                                if i < col.len() {
                                    match col[i] {
                                        Some(v) => PropertyValue::Bool(v),
                                        None => PropertyValue::Null,
                                    }
                                } else {
                                    PropertyValue::Null
                                }
                            } else {
                                PropertyValue::Null
                            }
                        }
                        _ => PropertyValue::Null,
                    };
                    properties.push(prop);
                }

                let id = if generate_ids {
                    Some((i as u64 + 1) + id_offset)
                } else {
                    None
                };

                features.push(Feature {
                    id,
                    geometry,
                    properties,
                });
            }
        }

        if generate_ids {
            id_offset += features.len() as u64;
        }

        result.push(LayerData {
            name,
            features,
            prop_names,
            prop_types,
            min_zoom: layer_min_zoom,
            max_zoom: layer_max_zoom,
        });
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Bounds helpers
// ---------------------------------------------------------------------------

/// Compute bounding box across all layers
fn compute_all_bounds(layers: &[LayerData]) -> (f64, f64, f64, f64) {
    let mut west = f64::MAX;
    let mut south = f64::MAX;
    let mut east = f64::MIN;
    let mut north = f64::MIN;

    for layer in layers {
        for feature in &layer.features {
            update_bounds(
                &feature.geometry,
                &mut west,
                &mut south,
                &mut east,
                &mut north,
            );
        }
    }

    (west, south, east, north)
}

fn update_bounds(
    geom: &Geometry,
    west: &mut f64,
    south: &mut f64,
    east: &mut f64,
    north: &mut f64,
) {
    use geo::BoundingRect;
    let bbox = match geom {
        Geometry::Point(p) => Some(geo_types::Rect::new(p.0, p.0)),
        Geometry::MultiPoint(mp) => mp.bounding_rect(),
        Geometry::LineString(ls) => ls.bounding_rect(),
        Geometry::MultiLineString(mls) => mls.bounding_rect(),
        Geometry::Polygon(p) => p.bounding_rect(),
        Geometry::MultiPolygon(mp) => mp.bounding_rect(),
    };

    if let Some(bb) = bbox {
        *west = west.min(bb.min().x);
        *south = south.min(bb.min().y);
        *east = east.max(bb.max().x);
        *north = north.max(bb.max().y);
    }
}

// ---------------------------------------------------------------------------
// Main tiling function
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (layers, output_path, tile_format, min_zoom, max_zoom,
    base_zoom, do_simplify, generate_ids, quiet, drop_rate, cluster_distance,
    cluster_maxzoom, do_coalesce))]
fn _freestile(
    py: Python<'_>,
    layers: Vec<PyObject>,
    output_path: &str,
    tile_format: &str,
    min_zoom: u8,
    max_zoom: u8,
    base_zoom: i32,
    do_simplify: bool,
    generate_ids: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
) -> PyResult<String> {
    // Parse layers from Python dicts
    let parse_start = Instant::now();
    let layer_data = parse_layers_from_py(py, &layers, generate_ids)?;

    if !quiet {
        let total_features: usize = layer_data.iter().map(|l| l.features.len()).sum();
        eprintln!(
            "  Parsed {} features across {} layer{} in {:.1}s",
            total_features,
            layer_data.len(),
            if layer_data.len() != 1 { "s" } else { "" },
            parse_start.elapsed().as_secs_f64()
        );
    }

    if layer_data.iter().all(|l| l.features.is_empty()) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No valid features to tile",
        ));
    }

    let format = match tile_format {
        "mlt" => TileFormat::Mlt,
        _ => TileFormat::Mvt,
    };

    let min_z = min_zoom;
    let max_z = max_zoom;

    // Compute bounds across all layers
    let bounds = compute_all_bounds(&layer_data);

    // --- Feature dropping setup ---
    let use_drop = drop_rate > 0.0;
    let spatial_indices: Vec<Vec<(usize, u64)>> = if use_drop {
        layer_data
            .iter()
            .map(|l| drop_mod::compute_spatial_indices(&l.features))
            .collect()
    } else {
        layer_data.iter().map(|_| Vec::new()).collect()
    };

    // --- Point clustering setup ---
    let use_cluster = cluster_distance > 0.0;
    let cluster_max_z = if cluster_maxzoom >= 0 {
        cluster_maxzoom as u8
    } else {
        max_z.saturating_sub(1)
    };

    // Determine which layers are all-point (eligible for clustering)
    let is_point_layer: Vec<bool> = layer_data
        .iter()
        .map(|l| {
            !l.features.is_empty()
                && l.features.iter().all(|f| {
                    matches!(
                        &f.geometry,
                        Geometry::Point(_) | Geometry::MultiPoint(_)
                    )
                })
        })
        .collect();

    // Pre-compute clusters per layer
    let cluster_results: Vec<HashMap<u8, Vec<Feature>>> = if use_cluster {
        layer_data
            .iter()
            .enumerate()
            .map(|(li, layer)| {
                if is_point_layer[li] {
                    let config = cluster::ClusterConfig {
                        distance: cluster_distance,
                        max_zoom: cluster_max_z,
                    };
                    cluster::cluster_points(
                        &layer.features,
                        &config,
                        min_z,
                        layer.prop_names.len(),
                    )
                } else {
                    HashMap::new()
                }
            })
            .collect()
    } else {
        layer_data.iter().map(|_| HashMap::new()).collect()
    };

    // Build extended prop_names for clustered layers (adds "point_count")
    let cluster_prop_names: Vec<Vec<String>> = layer_data
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

    // Build layer metadata for PMTiles
    let layer_metas: Vec<pmtiles_writer::LayerMeta> = layer_data
        .iter()
        .enumerate()
        .map(|(li, l)| pmtiles_writer::LayerMeta {
            name: l.name.clone(),
            property_names: cluster_prop_names[li].clone(),
            min_zoom: l.min_zoom,
            max_zoom: l.max_zoom,
        })
        .collect();

    // --- Main tile generation loop ---
    let mut all_tiles: Vec<(TileCoord, Vec<u8>)> = Vec::new();
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
            simplified_geoms: Vec<Option<Geometry>>,
            drop_mask: Option<Vec<bool>>,
        }

        let mut active_layers: Vec<ActiveLayer> = Vec::new();

        for (li, layer) in layer_data.iter().enumerate() {
            if zoom < layer.min_zoom || zoom > layer.max_zoom {
                continue;
            }

            // Determine features for this layer at this zoom
            let using_clusters =
                use_cluster && is_point_layer[li] && zoom <= cluster_max_z;
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

            // VW presimplify lines
            let vw_tol = simplify::vw_tolerance_for_zoom(zoom);
            let simplified_geoms: Vec<Option<Geometry>> = features
                .par_iter()
                .map(|f| match &f.geometry {
                    Geometry::LineString(_) | Geometry::MultiLineString(_) if do_simplify => {
                        Some(simplify::presimplify_line_vw(&f.geometry, vw_tol))
                    }
                    _ => None,
                })
                .collect();

            // Compute drop mask (not for clustered features, not at or above base_zoom)
            // base_z defaults to each layer's own max_zoom (not global) for correct
            // multi-layer behavior — a layer ending at z6 shouldn't drop at z5.
            // Drop curve is computed relative to base_z, not max_zoom: at zoom 0 with
            // base_zoom=4, threshold is drop_rate^(4-0), not drop_rate^(max-0).
            let layer_base_z = if base_zoom < 0 { layer.max_zoom } else { base_zoom as u8 };
            let drop_mask = if use_drop && !using_clusters && zoom < layer_base_z {
                Some(drop_mod::compute_drop_mask(
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

            // Assign features to tiles
            let tile_map =
                tiler::assign_features_to_tiles_with_geoms(features, &simplified_geoms, zoom);

            active_layers.push(ActiveLayer {
                layer_idx: li,
                features,
                prop_names,
                tile_map,
                simplified_geoms,
                drop_mask,
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
        if !quiet {
            eprintln!(
                "  Zoom {:>2}/{}: {:>6} tiles ...",
                zoom,
                max_z,
                n_tiles
            );
        }

        // Process tiles in parallel
        let tile_coords: Vec<TileCoord> = all_coords.into_iter().collect();
        let zoom_tiles: Vec<(TileCoord, Vec<u8>)> = tile_coords
            .into_par_iter()
            .filter_map(|coord| {
                // For each layer, process features for this tile
                let mut tile_layer_data: Vec<(&str, &[String], Vec<Feature>)> = Vec::new();

                for al in &active_layers {
                    let layer = &layer_data[al.layer_idx];

                    if let Some(feature_indices) = al.tile_map.get(&coord) {
                        let mut tile_feats: Vec<Feature> = feature_indices
                            .par_iter()
                            .filter_map(|&idx| {
                                // Check drop mask
                                if let Some(ref mask) = al.drop_mask {
                                    if !mask[idx] {
                                        return None;
                                    }
                                }

                                let feature = &al.features[idx];
                                let geom_to_process = match &al.simplified_geoms[idx] {
                                    Some(g) => g,
                                    None => &feature.geometry,
                                };

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
                            tile_feats =
                                coalesce::coalesce_features(tile_feats, al.prop_names);
                        }

                        if !tile_feats.is_empty() {
                            tile_layer_data.push((
                                &layer.name,
                                al.prop_names,
                                tile_feats,
                            ));
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

                Some((coord, tile_bytes))
            })
            .collect();

        let n_encoded = zoom_tiles.len();
        all_tiles.extend(zoom_tiles);

        if !quiet {
            let elapsed = zoom_start.elapsed().as_secs_f64();
            eprintln!(
                "           {:>6} encoded ({:.1}s)",
                n_encoded,
                elapsed
            );
        }
    }

    if !quiet {
        eprintln!(
            "  Total: {} tiles in {:.1}s",
            all_tiles.len(),
            total_start.elapsed().as_secs_f64()
        );
    }

    if all_tiles.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No tiles generated",
        ));
    }

    // Write PMTiles archive
    let write_start = Instant::now();
    match pmtiles_writer::write_pmtiles(
        output_path,
        all_tiles,
        format,
        &layer_metas,
        min_z,
        max_z,
        bounds,
    ) {
        Ok(()) => {
            if !quiet {
                eprintln!(
                    "  PMTiles write: {:.1}s",
                    write_start.elapsed().as_secs_f64()
                );
            }
            Ok(output_path.to_string())
        }
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Error: {}",
            e
        ))),
    }
}

// ---------------------------------------------------------------------------
// PyO3 module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn _freestiler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_freestile, m)?)?;
    Ok(())
}
