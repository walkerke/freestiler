use extendr_api::prelude::*;
use geo_types::{Coord, LineString, MultiLineString, MultiPolygon, Point, Polygon};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

// R console flush (Rprintf output is buffered; flush to show progress immediately)
extern "C" {
    fn R_FlushConsole();
}

fn flush_console() {
    unsafe {
        R_FlushConsole();
    }
}

mod clip;
mod cluster;
mod coalesce;
mod drop;
mod mlt;
mod mvt;
mod pmtiles_writer;
mod simplify;
mod tiler;

use pmtiles_writer::TileFormat;
use tiler::{Feature, Geometry, LayerData, PropertyValue, TileCoord};

/// Create vector tiles from spatial data (multi-layer support)
///
/// @param layers List of layer lists, each containing: name, geometries, geom_types,
///   prop_names, prop_types, prop_char_values, prop_num_values, prop_int_values,
///   prop_lgl_values, min_zoom, max_zoom
/// @param output_path Path for output .pmtiles file
/// @param tile_format "mvt" or "mlt"
/// @param global_min_zoom Minimum zoom level
/// @param global_max_zoom Maximum zoom level
/// @param do_simplify Whether to simplify geometries at lower zooms
/// @param generate_ids Whether to generate sequential feature IDs
/// @param quiet Whether to suppress progress messages
/// @param drop_rate Exponential drop rate (negative = off)
/// @param cluster_distance Pixel distance for clustering (negative = off)
/// @param cluster_maxzoom Max zoom for clustering (negative = use max_zoom - 1)
/// @param do_coalesce Whether to coalesce features with same attributes
/// @export
#[extendr]
fn rust_freestile(
    layers: List,
    output_path: &str,
    tile_format: &str,
    global_min_zoom: i32,
    global_max_zoom: i32,
    do_simplify: bool,
    generate_ids: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
) -> String {
    // Parse layers from R
    let parse_start = Instant::now();
    let layer_data = parse_layers_from_r(&layers, generate_ids);

    if !quiet {
        let total_features: usize = layer_data.iter().map(|l| l.features.len()).sum();
        rprintln!(
            "  Parsed {} features across {} layer{} in {:.1}s",
            total_features,
            layer_data.len(),
            if layer_data.len() != 1 { "s" } else { "" },
            parse_start.elapsed().as_secs_f64()
        );
        flush_console();
    }

    if layer_data.iter().all(|l| l.features.is_empty()) {
        return "Error: No valid features to tile".to_string();
    }

    let format = match tile_format {
        "mlt" => TileFormat::Mlt,
        _ => TileFormat::Mvt,
    };

    let min_z = global_min_zoom as u8;
    let max_z = global_max_zoom as u8;

    // Compute bounds across all layers
    let bounds = compute_all_bounds(&layer_data);

    // --- Feature dropping setup ---
    let use_drop = drop_rate > 0.0;
    let spatial_indices: Vec<Vec<(usize, u64)>> = if use_drop {
        layer_data
            .iter()
            .map(|l| drop::compute_spatial_indices(&l.features))
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

            // Compute drop mask (not for clustered features)
            let drop_mask = if use_drop && !using_clusters {
                Some(drop::compute_drop_mask(
                    features,
                    &spatial_indices[li],
                    zoom,
                    max_z,
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
            rprintln!(
                "  Zoom {:>2}/{}: {:>6} tiles ...",
                zoom,
                max_z,
                n_tiles
            );
            flush_console();
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
            rprintln!(
                "           {:>6} encoded ({:.1}s)",
                n_encoded,
                elapsed
            );
            flush_console();
        }
    }

    if !quiet {
        rprintln!(
            "  Total: {} tiles in {:.1}s",
            all_tiles.len(),
            total_start.elapsed().as_secs_f64()
        );
        flush_console();
    }

    if all_tiles.is_empty() {
        return "Error: No tiles generated".to_string();
    }

    // Write PMTiles archive
    if !quiet {
        rprintln!("  Writing PMTiles archive ({} tiles) ...", all_tiles.len());
        flush_console();
    }
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
                rprintln!(
                    "  PMTiles write: {:.1}s",
                    write_start.elapsed().as_secs_f64()
                );
                flush_console();
            }
            output_path.to_string()
        }
        Err(e) => format!("Error: {}", e),
    }
}

// ---------------------------------------------------------------------------
// Layer parsing from R
// ---------------------------------------------------------------------------

/// Parse layers from an R list of layer lists.
/// Each layer list has positional elements:
///   0: name (string)
///   1: geometries (sfc list)
///   2: geom_types (character vector)
///   3: prop_names (character vector)
///   4: prop_types (character vector)
///   5: prop_char_values (list)
///   6: prop_num_values (list)
///   7: prop_int_values (list)
///   8: prop_lgl_values (list)
///   9: min_zoom (integer)
///  10: max_zoom (integer)
fn parse_layers_from_r(layers: &List, generate_ids: bool) -> Vec<LayerData> {
    let n_layers = layers.len();
    let mut result = Vec::with_capacity(n_layers);

    // Running ID counter across all layers for unique IDs
    let mut id_offset: u64 = 0;

    for i in 0..n_layers {
        let layer: List = match layers.elt(i as _) {
            Ok(robj) => match robj.try_into() {
                Ok(l) => l,
                Err(_) => continue,
            },
            Err(_) => continue,
        };

        // Extract layer name
        let name: String = layer
            .elt(0)
            .ok()
            .and_then(|r| r.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| format!("layer_{}", i));

        // Extract geometries and types
        let geometries: List = match layer.elt(1) {
            Ok(robj) => match robj.try_into() {
                Ok(l) => l,
                Err(_) => continue,
            },
            Err(_) => continue,
        };

        let geom_types: Vec<String> = match layer.elt(2) {
            Ok(robj) => {
                let strs: Strings = match robj.try_into() {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                strs.iter().map(|s| s.as_str().to_string()).collect()
            }
            Err(_) => continue,
        };

        // Extract property metadata
        let prop_names: Vec<String> = extract_string_vec(&layer, 3);
        let prop_types: Vec<String> = extract_string_vec(&layer, 4);

        let prop_char_values: List = layer
            .elt(5)
            .ok()
            .and_then(|r| r.try_into().ok())
            .unwrap_or_else(|| List::from_values(Vec::<Robj>::new()));
        let prop_num_values: List = layer
            .elt(6)
            .ok()
            .and_then(|r| r.try_into().ok())
            .unwrap_or_else(|| List::from_values(Vec::<Robj>::new()));
        let prop_int_values: List = layer
            .elt(7)
            .ok()
            .and_then(|r| r.try_into().ok())
            .unwrap_or_else(|| List::from_values(Vec::<Robj>::new()));
        let prop_lgl_values: List = layer
            .elt(8)
            .ok()
            .and_then(|r| r.try_into().ok())
            .unwrap_or_else(|| List::from_values(Vec::<Robj>::new()));

        // Extract zoom range
        let layer_min_zoom: u8 = layer
            .elt(9)
            .ok()
            .and_then(|r| {
                let ints: Integers = r.try_into().ok()?;
                Some(ints.elt(0).inner() as u8)
            })
            .unwrap_or(0);
        let layer_max_zoom: u8 = layer
            .elt(10)
            .ok()
            .and_then(|r| {
                let ints: Integers = r.try_into().ok()?;
                Some(ints.elt(0).inner() as u8)
            })
            .unwrap_or(14);

        // Parse features using existing parser
        let mut features = parse_features_from_sfc(
            &geometries,
            &geom_types,
            &prop_names,
            &prop_types,
            &prop_char_values,
            &prop_num_values,
            &prop_int_values,
            &prop_lgl_values,
            true, // always generate internal IDs
        );

        // Adjust feature IDs if generating sequential IDs across layers
        if generate_ids {
            for f in &mut features {
                if let Some(ref mut id) = f.id {
                    *id += id_offset;
                }
            }
            id_offset += features.len() as u64;
        } else {
            for f in &mut features {
                f.id = None;
            }
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

    result
}

/// Extract a character vector from a list element at the given position.
fn extract_string_vec(list: &List, pos: usize) -> Vec<String> {
    list.elt(pos as _)
        .ok()
        .and_then(|robj| {
            let strs: Strings = robj.try_into().ok()?;
            Some(strs.iter().map(|s| s.as_str().to_string()).collect())
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Direct sfg geometry parsing
// ---------------------------------------------------------------------------

/// Parse features directly from an sfc list of sfg objects.
fn parse_features_from_sfc(
    geometries: &List,
    geom_types: &[String],
    prop_names: &[String],
    prop_types: &[String],
    prop_char_values: &List,
    prop_num_values: &List,
    prop_int_values: &List,
    prop_lgl_values: &List,
    generate_ids: bool,
) -> Vec<Feature> {
    let n_features = geom_types.len();

    // Pre-extract property columns
    let char_cols: Vec<Option<Vec<Option<String>>>> = (0..prop_names.len())
        .map(|i| {
            if prop_types[i] == "character" {
                prop_char_values
                    .elt(i as _)
                    .ok()
                    .and_then(|v| {
                        let strs: Strings = v.try_into().ok()?;
                        Some(
                            strs.iter()
                                .map(|s| {
                                    if s.is_na() {
                                        None
                                    } else {
                                        Some(s.as_str().to_string())
                                    }
                                })
                                .collect(),
                        )
                    })
            } else {
                None
            }
        })
        .collect();

    let num_cols: Vec<Option<Vec<f64>>> = (0..prop_names.len())
        .map(|i| {
            if prop_types[i] == "numeric" {
                prop_num_values
                    .elt(i as _)
                    .ok()
                    .and_then(|v| {
                        let doubles: Doubles = v.try_into().ok()?;
                        Some(doubles.iter().map(|d| d.inner()).collect())
                    })
            } else {
                None
            }
        })
        .collect();

    let int_cols: Vec<Option<Vec<i32>>> = (0..prop_names.len())
        .map(|i| {
            if prop_types[i] == "integer" {
                prop_int_values
                    .elt(i as _)
                    .ok()
                    .and_then(|v| {
                        let ints: Integers = v.try_into().ok()?;
                        Some(ints.iter().map(|x| x.inner()).collect())
                    })
            } else {
                None
            }
        })
        .collect();

    let lgl_cols: Vec<Option<Vec<i32>>> = (0..prop_names.len())
        .map(|i| {
            if prop_types[i] == "logical" {
                prop_lgl_values
                    .elt(i as _)
                    .ok()
                    .and_then(|v| {
                        let logicals: Logicals = v.try_into().ok()?;
                        Some(logicals.iter().map(|x| x.inner()).collect())
                    })
            } else {
                None
            }
        })
        .collect();

    let mut features = Vec::with_capacity(n_features);

    for i in 0..n_features {
        let gtype = &geom_types[i];

        // Get the sfg object from the sfc list
        let sfg = match geometries.elt(i as _) {
            Ok(robj) => robj,
            Err(_) => continue,
        };

        let geom = match gtype.as_str() {
            "POINT" => parse_point_sfg(sfg),
            "MULTIPOINT" => parse_multipoint_sfg(sfg),
            "LINESTRING" => parse_linestring_sfg(sfg),
            "MULTILINESTRING" => parse_multilinestring_sfg(sfg),
            "POLYGON" => parse_polygon_sfg(sfg),
            "MULTIPOLYGON" => parse_multipolygon_sfg(sfg),
            _ => None,
        };

        if let Some(geometry) = geom {
            // Build properties
            let mut properties = Vec::with_capacity(prop_names.len());
            for col_idx in 0..prop_names.len() {
                let prop = match prop_types[col_idx].as_str() {
                    "character" => {
                        if let Some(Some(ref col)) = char_cols.get(col_idx) {
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
                    "numeric" => {
                        if let Some(Some(ref col)) = num_cols.get(col_idx) {
                            if i < col.len() {
                                let v = col[i];
                                if v.is_nan() {
                                    PropertyValue::Null
                                } else {
                                    PropertyValue::Double(v)
                                }
                            } else {
                                PropertyValue::Null
                            }
                        } else {
                            PropertyValue::Null
                        }
                    }
                    "integer" => {
                        if let Some(Some(ref col)) = int_cols.get(col_idx) {
                            if i < col.len() {
                                let v = col[i];
                                if v == i32::MIN {
                                    // R's NA_integer_
                                    PropertyValue::Null
                                } else {
                                    PropertyValue::Int(v as i64)
                                }
                            } else {
                                PropertyValue::Null
                            }
                        } else {
                            PropertyValue::Null
                        }
                    }
                    "logical" => {
                        if let Some(Some(ref col)) = lgl_cols.get(col_idx) {
                            if i < col.len() {
                                let v = col[i];
                                if v == i32::MIN {
                                    // R's NA
                                    PropertyValue::Null
                                } else {
                                    PropertyValue::Bool(v != 0)
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

            features.push(Feature {
                id: if generate_ids {
                    Some((i + 1) as u64)
                } else {
                    None
                },
                geometry,
                properties,
            });
        }
    }

    features
}

// ---------------------------------------------------------------------------
// sfg → Geometry parsing helpers
// ---------------------------------------------------------------------------

/// Extract coordinates from an R numeric vector or matrix.
fn robj_to_coords(robj: Robj) -> Option<Vec<Coord<f64>>> {
    let doubles: Doubles = robj.try_into().ok()?;
    let data: Vec<f64> = doubles.iter().map(|d| d.inner()).collect();
    let nrow = data.len() / 2;
    if nrow == 0 {
        return None;
    }
    let mut coords = Vec::with_capacity(nrow);
    for i in 0..nrow {
        coords.push(Coord {
            x: data[i],
            y: data[i + nrow],
        });
    }
    Some(coords)
}

fn parse_point_sfg(robj: Robj) -> Option<Geometry> {
    let doubles: Doubles = robj.try_into().ok()?;
    let data: Vec<f64> = doubles.iter().map(|d| d.inner()).collect();
    if data.len() >= 2 {
        Some(Geometry::Point(Point::new(data[0], data[1])))
    } else {
        None
    }
}

fn parse_multipoint_sfg(robj: Robj) -> Option<Geometry> {
    let coords = robj_to_coords(robj)?;
    if coords.is_empty() {
        return None;
    }
    let points: Vec<Point<f64>> = coords.into_iter().map(|c| Point(c)).collect();
    Some(Geometry::MultiPoint(geo_types::MultiPoint(points)))
}

fn parse_linestring_sfg(robj: Robj) -> Option<Geometry> {
    let coords = robj_to_coords(robj)?;
    if coords.len() >= 2 {
        Some(Geometry::LineString(LineString(coords)))
    } else {
        None
    }
}

fn parse_multilinestring_sfg(robj: Robj) -> Option<Geometry> {
    let list: List = robj.try_into().ok()?;
    let n_parts = list.len();
    let mut lines = Vec::with_capacity(n_parts);
    for i in 0..n_parts {
        if let Ok(part_robj) = list.elt(i as _) {
            if let Some(coords) = robj_to_coords(part_robj) {
                if coords.len() >= 2 {
                    lines.push(LineString(coords));
                }
            }
        }
    }
    if lines.is_empty() {
        None
    } else {
        Some(Geometry::MultiLineString(MultiLineString(lines)))
    }
}

fn parse_polygon_sfg(robj: Robj) -> Option<Geometry> {
    let list: List = robj.try_into().ok()?;
    let n_rings = list.len();
    if n_rings == 0 {
        return None;
    }
    let mut rings = Vec::with_capacity(n_rings);
    for i in 0..n_rings {
        if let Ok(ring_robj) = list.elt(i as _) {
            if let Some(coords) = robj_to_coords(ring_robj) {
                if coords.len() >= 3 {
                    rings.push(LineString(coords));
                }
            }
        }
    }
    if rings.is_empty() {
        return None;
    }
    let exterior = rings.remove(0);
    Some(Geometry::Polygon(Polygon::new(exterior, rings)))
}

fn parse_multipolygon_sfg(robj: Robj) -> Option<Geometry> {
    let list: List = robj.try_into().ok()?;
    let n_polys = list.len();
    let mut polys = Vec::with_capacity(n_polys);
    for i in 0..n_polys {
        let poly_robj = match list.elt(i as _) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let poly_list: List = match poly_robj.try_into() {
            Ok(l) => l,
            Err(_) => continue,
        };
        let n_rings = poly_list.len();
        if n_rings == 0 {
            continue;
        }
        let mut rings = Vec::with_capacity(n_rings);
        for j in 0..n_rings {
            if let Ok(ring_robj) = poly_list.elt(j as _) {
                if let Some(coords) = robj_to_coords(ring_robj) {
                    if coords.len() >= 3 {
                        rings.push(LineString(coords));
                    }
                }
            }
        }
        if !rings.is_empty() {
            let exterior = rings.remove(0);
            polys.push(Polygon::new(exterior, rings));
        }
    }
    if polys.is_empty() {
        None
    } else {
        Some(Geometry::MultiPolygon(MultiPolygon(polys)))
    }
}

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

extendr_module! {
    mod freestiler;
    fn rust_freestile;
}
