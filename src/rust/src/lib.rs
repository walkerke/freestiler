use extendr_api::prelude::*;
use geo_types::{Coord, LineString, MultiLineString, MultiPolygon, Point, Polygon};
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
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
mod mlt;
mod mvt;
mod pmtiles_writer;
mod simplify;
mod tiler;

use pmtiles_writer::TileFormat;
use tiler::{Feature, Geometry, PropertyValue, TileCoord};

/// Create vector tiles from spatial data
///
/// @param geometries sfc list of sfg geometry objects
/// @param geom_types Character vector of geometry types ("POINT", "LINESTRING", "POLYGON", etc.)
/// @param prop_names Character vector of property column names
/// @param prop_types Character vector of property types ("character", "integer", "numeric", "logical")
/// @param prop_char_values List of character property columns (one vector per column, NA for non-char)
/// @param prop_num_values List of numeric property columns (one vector per column, NaN for non-num)
/// @param prop_int_values List of integer property columns (one vector per column, NA for non-int)
/// @param prop_lgl_values List of logical property columns (one vector per column, NA for non-lgl)
/// @param output_path Path for output .pmtiles file
/// @param layer_name Name of the tile layer
/// @param tile_format "mvt" or "mlt"
/// @param min_zoom Minimum zoom level
/// @param max_zoom Maximum zoom level
/// @param do_simplify Whether to simplify geometries at lower zooms
/// @param generate_ids Whether to generate sequential feature IDs
/// @export
#[extendr]
fn rust_freestile(
    geometries: List,
    geom_types: Vec<String>,
    prop_names: Vec<String>,
    prop_types: Vec<String>,
    prop_char_values: List,
    prop_num_values: List,
    prop_int_values: List,
    prop_lgl_values: List,
    output_path: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: i32,
    max_zoom: i32,
    do_simplify: bool,
    generate_ids: bool,
    quiet: bool,
) -> String {
    // Parse features directly from sfc list
    let parse_start = Instant::now();
    let features = parse_features_from_sfc(
        &geometries,
        &geom_types,
        &prop_names,
        &prop_types,
        &prop_char_values,
        &prop_num_values,
        &prop_int_values,
        &prop_lgl_values,
        generate_ids,
    );
    if !quiet {
        rprintln!(
            "  Parsed {} features in {:.1}s",
            features.len(),
            parse_start.elapsed().as_secs_f64()
        );
        flush_console();
    }

    if features.is_empty() {
        return "Error: No valid features to tile".to_string();
    }

    let format = match tile_format {
        "mlt" => TileFormat::Mlt,
        _ => TileFormat::Mvt,
    };

    let min_z = min_zoom as u8;
    let max_z = max_zoom as u8;

    // Compute bounds from features
    let bounds = compute_bounds(&features);

    // Generate tiles for each zoom level
    let mut all_tiles: Vec<(TileCoord, Vec<u8>)> = Vec::new();
    let total_start = Instant::now();

    for zoom in min_z..=max_z {
        let zoom_start = Instant::now();

        // Minimum feature size in degrees for this zoom (1 pixel)
        let pixel_deg = 360.0 / ((1u64 << zoom) as f64 * 4096.0);

        // Pre-simplify line features with Visvalingam-Whyatt for this zoom
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

        // Assign features to tiles (using simplified bbox for lines)
        let tile_map =
            tiler::assign_features_to_tiles_with_geoms(&features, &simplified_geoms, zoom);
        let n_tiles = tile_map.len();

        if !quiet {
            rprintln!(
                "  Zoom {:>2}/{}: {:>6} tiles ...",
                zoom,
                max_z,
                n_tiles
            );
            flush_console();
        }

        let dropped_count = AtomicUsize::new(0);

        // Process tiles in parallel; within each tile, process features in
        // parallel too (helps at low zooms where one tile has many features)
        let zoom_tiles: Vec<(TileCoord, Vec<u8>)> = tile_map
            .into_par_iter()
            .filter_map(|(coord, feature_indices)| {
                // Clip, filter, and snap features for this tile
                let tile_features: Vec<Feature> = feature_indices
                    .into_par_iter()
                    .filter_map(|idx| {
                        let feature = &features[idx];

                        // Use pre-simplified geometry for lines, original for others
                        let geom_to_process = match &simplified_geoms[idx] {
                            Some(g) => g,
                            None => &feature.geometry,
                        };

                        // Drop sub-pixel features at lower zooms (except points)
                        if zoom < max_z {
                            if !matches!(
                                geom_to_process,
                                Geometry::Point(_) | Geometry::MultiPoint(_)
                            ) {
                                let bbox = tiler::geometry_bbox(geom_to_process);
                                let w = bbox.max().x - bbox.min().x;
                                let h = bbox.max().y - bbox.min().y;
                                if w < pixel_deg && h < pixel_deg {
                                    dropped_count.fetch_add(1, Ordering::Relaxed);
                                    return None;
                                }
                            }
                        }

                        // Clip to tile boundaries
                        let clipped =
                            clip::clip_geometry_to_tile(geom_to_process, &coord)?;

                        // Snap to tile pixel grid: prevents slivers between adjacent
                        // features and provides zoom-adaptive vertex reduction
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

                if tile_features.is_empty() {
                    return None;
                }

                // Encode tile
                let tile_bytes = match format {
                    TileFormat::Mvt => {
                        mvt::encode_tile(&coord, &tile_features, layer_name, &prop_names)
                    }
                    TileFormat::Mlt => {
                        mlt::encode_tile(&coord, &tile_features, layer_name, &prop_names)
                    }
                };

                if tile_bytes.is_empty() {
                    return None;
                }

                Some((coord, tile_bytes))
            })
            .collect();

        let n_encoded = zoom_tiles.len();
        let n_dropped = dropped_count.load(Ordering::Relaxed);
        all_tiles.extend(zoom_tiles);

        if !quiet {
            let elapsed = zoom_start.elapsed().as_secs_f64();
            if n_dropped > 0 {
                rprintln!(
                    "           {:>6} encoded, {} dropped ({:.1}s)",
                    n_encoded,
                    n_dropped,
                    elapsed
                );
            } else {
                rprintln!(
                    "           {:>6} encoded ({:.1}s)",
                    n_encoded,
                    elapsed
                );
            }
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
    let write_start = Instant::now();
    match pmtiles_writer::write_pmtiles(
        output_path,
        all_tiles,
        format,
        layer_name,
        &prop_names,
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
// Direct sfg geometry parsing (replaces flat-vector parsing)
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

    // Pre-extract property columns (same logic as before)
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
/// R matrices are column-major: for an n×2 matrix, data = [x1..xn, y1..yn].
/// A POINT sfg is just [x, y] (length 2), which also works: nrow=1.
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

/// POINT sfg → Geometry::Point
fn parse_point_sfg(robj: Robj) -> Option<Geometry> {
    let doubles: Doubles = robj.try_into().ok()?;
    let data: Vec<f64> = doubles.iter().map(|d| d.inner()).collect();
    if data.len() >= 2 {
        Some(Geometry::Point(Point::new(data[0], data[1])))
    } else {
        None
    }
}

/// MULTIPOINT sfg (matrix) → Geometry::MultiPoint
fn parse_multipoint_sfg(robj: Robj) -> Option<Geometry> {
    let coords = robj_to_coords(robj)?;
    if coords.is_empty() {
        return None;
    }
    let points: Vec<Point<f64>> = coords.into_iter().map(|c| Point(c)).collect();
    Some(Geometry::MultiPoint(geo_types::MultiPoint(points)))
}

/// LINESTRING sfg (matrix) → Geometry::LineString
fn parse_linestring_sfg(robj: Robj) -> Option<Geometry> {
    let coords = robj_to_coords(robj)?;
    if coords.len() >= 2 {
        Some(Geometry::LineString(LineString(coords)))
    } else {
        None
    }
}

/// MULTILINESTRING sfg (list of matrices) → Geometry::MultiLineString
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

/// POLYGON sfg (list of ring matrices) → Geometry::Polygon
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

/// MULTIPOLYGON sfg (list of list of ring matrices) → Geometry::MultiPolygon
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

/// Compute bounding box from features
fn compute_bounds(features: &[Feature]) -> (f64, f64, f64, f64) {
    let mut west = f64::MAX;
    let mut south = f64::MAX;
    let mut east = f64::MIN;
    let mut north = f64::MIN;

    for feature in features {
        update_bounds(&feature.geometry, &mut west, &mut south, &mut east, &mut north);
    }

    (west, south, east, north)
}

fn update_bounds(geom: &Geometry, west: &mut f64, south: &mut f64, east: &mut f64, north: &mut f64) {
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
