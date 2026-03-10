use extendr_api::prelude::*;
use geo_types::{Coord, LineString, MultiLineString, MultiPolygon, Point, Polygon};
use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

use freestiler_core::engine::{self, ProgressReporter, TileConfig};
use freestiler_core::pmtiles_writer::TileFormat;
use freestiler_core::tiler::{Feature, Geometry, LayerData, PropertyValue};

// R console flush (Rprintf output is buffered; flush to show progress immediately)
extern "C" {
    fn R_FlushConsole();
}

fn flush_console() {
    unsafe {
        R_FlushConsole();
    }
}

/// R-specific progress reporter that uses rprintln! and flushes the console
struct RReporter;

impl ProgressReporter for RReporter {
    fn report(&self, msg: &str) {
        rprintln!("{}", msg);
        flush_console();
    }
}

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
    base_zoom: i32,
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

    let reporter: Box<dyn ProgressReporter> = if quiet {
        Box::new(engine::SilentReporter)
    } else {
        Box::new(RReporter)
    };

    if !quiet {
        let total_features: usize = layer_data.iter().map(|l| l.features.len()).sum();
        reporter.report(&format!(
            "  Parsed {} features across {} layer{} in {:.1}s",
            total_features,
            layer_data.len(),
            if layer_data.len() != 1 { "s" } else { "" },
            parse_start.elapsed().as_secs_f64()
        ));
    }

    if layer_data.iter().all(|l| l.features.is_empty()) {
        return "Error: No valid features to tile".to_string();
    }

    let config = TileConfig {
        tile_format: match tile_format {
            "mlt" => TileFormat::Mlt,
            _ => TileFormat::Mvt,
        },
        min_zoom: global_min_zoom as u8,
        max_zoom: global_max_zoom as u8,
        base_zoom: if base_zoom < 0 {
            None
        } else {
            Some(base_zoom as u8)
        },
        simplification: do_simplify,
        drop_rate: if drop_rate > 0.0 {
            Some(drop_rate)
        } else {
            None
        },
        cluster_distance: if cluster_distance > 0.0 {
            Some(cluster_distance)
        } else {
            None
        },
        cluster_maxzoom: if cluster_maxzoom >= 0 {
            Some(cluster_maxzoom as u8)
        } else {
            None
        },
        coalesce: do_coalesce,
    };

    match engine::generate_pmtiles(&layer_data, output_path, &config, reporter.as_ref()) {
        Ok(()) => output_path.to_string(),
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
                prop_char_values.elt(i as _).ok().and_then(|v| {
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
                prop_num_values.elt(i as _).ok().and_then(|v| {
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
                prop_int_values.elt(i as _).ok().and_then(|v| {
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
                prop_lgl_values.elt(i as _).ok().and_then(|v| {
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

// ---------------------------------------------------------------------------
// Direct file input (optional features)
// ---------------------------------------------------------------------------

/// Create tiles from a GeoParquet file (requires geoparquet feature)
/// @param input_path Path to the GeoParquet file
/// @param output_path Path for output .pmtiles file
/// @param layer_name Layer name
/// @param tile_format "mvt" or "mlt"
/// @param min_zoom Minimum zoom level
/// @param max_zoom Maximum zoom level
/// @param base_zoom Base zoom level (negative = use max_zoom)
/// @param do_simplify Whether to simplify geometries
/// @param drop_rate Exponential drop rate (negative = off)
/// @param cluster_distance Pixel distance for clustering (negative = off)
/// @param cluster_maxzoom Max zoom for clustering (negative = use max_zoom - 1)
/// @param do_coalesce Whether to coalesce features
/// @param quiet Whether to suppress progress
/// @export
#[extendr]
fn rust_freestile_file(
    input_path: &str,
    output_path: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: i32,
    max_zoom: i32,
    base_zoom: i32,
    do_simplify: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
    quiet: bool,
) -> String {
    #[cfg(not(feature = "geoparquet"))]
    {
        let _ = (
            input_path,
            output_path,
            layer_name,
            tile_format,
            min_zoom,
            max_zoom,
            base_zoom,
            do_simplify,
            drop_rate,
            cluster_distance,
            cluster_maxzoom,
            do_coalesce,
            quiet,
        );
        return "Error: GeoParquet support not compiled. Rebuild with FREESTILER_GEOPARQUET=true."
            .to_string();
    }

    #[cfg(feature = "geoparquet")]
    {
        let reporter: Box<dyn ProgressReporter> = if quiet {
            Box::new(engine::SilentReporter)
        } else {
            Box::new(RReporter)
        };

        let layers = match freestiler_core::file_input::parquet_to_layers(
            input_path,
            layer_name,
            min_zoom as u8,
            max_zoom as u8,
        ) {
            Ok(l) => l,
            Err(e) => return format!("Error: {}", e),
        };

        if !quiet {
            let total: usize = layers.iter().map(|l| l.features.len()).sum();
            reporter.report(&format!("  Read {} features from {}", total, input_path));
        }

        let config = TileConfig {
            tile_format: match tile_format {
                "mlt" => TileFormat::Mlt,
                _ => TileFormat::Mvt,
            },
            min_zoom: min_zoom as u8,
            max_zoom: max_zoom as u8,
            base_zoom: if base_zoom < 0 {
                None
            } else {
                Some(base_zoom as u8)
            },
            simplification: do_simplify,
            drop_rate: if drop_rate > 0.0 {
                Some(drop_rate)
            } else {
                None
            },
            cluster_distance: if cluster_distance > 0.0 {
                Some(cluster_distance)
            } else {
                None
            },
            cluster_maxzoom: if cluster_maxzoom >= 0 {
                Some(cluster_maxzoom as u8)
            } else {
                None
            },
            coalesce: do_coalesce,
        };

        match engine::generate_pmtiles(&layers, output_path, &config, reporter.as_ref()) {
            Ok(()) => output_path.to_string(),
            Err(e) => format!("Error: {}", e),
        }
    }
}

/// Create tiles from a file via DuckDB spatial (requires duckdb feature)
/// @param input_path Path to the spatial file
/// @param output_path Path for output .pmtiles file
/// @param layer_name Layer name
/// @param tile_format "mvt" or "mlt"
/// @param min_zoom Minimum zoom level
/// @param max_zoom Maximum zoom level
/// @param base_zoom Base zoom level (negative = use max_zoom)
/// @param do_simplify Whether to simplify geometries
/// @param drop_rate Exponential drop rate (negative = off)
/// @param cluster_distance Pixel distance for clustering (negative = off)
/// @param cluster_maxzoom Max zoom for clustering (negative = use max_zoom - 1)
/// @param do_coalesce Whether to coalesce features
/// @param quiet Whether to suppress progress
/// @export
#[extendr]
fn rust_freestile_duckdb(
    input_path: &str,
    output_path: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: i32,
    max_zoom: i32,
    base_zoom: i32,
    do_simplify: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
    quiet: bool,
) -> String {
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = (
            input_path,
            output_path,
            layer_name,
            tile_format,
            min_zoom,
            max_zoom,
            base_zoom,
            do_simplify,
            drop_rate,
            cluster_distance,
            cluster_maxzoom,
            do_coalesce,
            quiet,
        );
        return "Error: DuckDB support not compiled. Install the r-universe build or rebuild from source with DuckDB enabled."
            .to_string();
    }

    #[cfg(feature = "duckdb")]
    {
        let reporter: Box<dyn ProgressReporter> = if quiet {
            Box::new(engine::SilentReporter)
        } else {
            Box::new(RReporter)
        };

        let layers = match freestiler_core::file_input::duckdb_file_to_layers(
            input_path,
            layer_name,
            min_zoom as u8,
            max_zoom as u8,
        ) {
            Ok(l) => l,
            Err(e) => return format!("Error: {}", e),
        };

        if !quiet {
            let total: usize = layers.iter().map(|l| l.features.len()).sum();
            reporter.report(&format!("  Read {} features from {}", total, input_path));
        }

        let config = TileConfig {
            tile_format: match tile_format {
                "mlt" => TileFormat::Mlt,
                _ => TileFormat::Mvt,
            },
            min_zoom: min_zoom as u8,
            max_zoom: max_zoom as u8,
            base_zoom: if base_zoom < 0 {
                None
            } else {
                Some(base_zoom as u8)
            },
            simplification: do_simplify,
            drop_rate: if drop_rate > 0.0 {
                Some(drop_rate)
            } else {
                None
            },
            cluster_distance: if cluster_distance > 0.0 {
                Some(cluster_distance)
            } else {
                None
            },
            cluster_maxzoom: if cluster_maxzoom >= 0 {
                Some(cluster_maxzoom as u8)
            } else {
                None
            },
            coalesce: do_coalesce,
        };

        match engine::generate_pmtiles(&layers, output_path, &config, reporter.as_ref()) {
            Ok(()) => output_path.to_string(),
            Err(e) => format!("Error: {}", e),
        }
    }
}

/// Create tiles from a DuckDB SQL query (requires duckdb feature)
/// @param sql SQL query that returns a geometry column
/// @param db_path Path to DuckDB database (empty string = in-memory)
/// @param output_path Path for output .pmtiles file
/// @param layer_name Layer name
/// @param tile_format "mvt" or "mlt"
/// @param min_zoom Minimum zoom level
/// @param max_zoom Maximum zoom level
/// @param base_zoom Base zoom level (negative = use max_zoom)
/// @param do_simplify Whether to simplify geometries
/// @param drop_rate Exponential drop rate (negative = off)
/// @param cluster_distance Pixel distance for clustering (negative = off)
/// @param cluster_maxzoom Max zoom for clustering (negative = use max_zoom - 1)
/// @param do_coalesce Whether to coalesce features
/// @param quiet Whether to suppress progress
/// @param streaming_mode "auto", "always", or "never"
/// @export
#[extendr]
fn rust_freestile_duckdb_query(
    sql: &str,
    db_path: &str,
    output_path: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: i32,
    max_zoom: i32,
    base_zoom: i32,
    do_simplify: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
    quiet: bool,
    streaming_mode: &str,
) -> String {
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = (
            sql,
            db_path,
            output_path,
            layer_name,
            tile_format,
            min_zoom,
            max_zoom,
            base_zoom,
            do_simplify,
            drop_rate,
            cluster_distance,
            cluster_maxzoom,
            do_coalesce,
            quiet,
            streaming_mode,
        );
        return "Error: DuckDB support not compiled. Install the r-universe build or rebuild from source with DuckDB enabled."
            .to_string();
    }

    #[cfg(feature = "duckdb")]
    {
        let reporter: Box<dyn ProgressReporter> = if quiet {
            Box::new(engine::SilentReporter)
        } else {
            Box::new(RReporter)
        };

        let db_path_opt = if db_path.is_empty() {
            None
        } else {
            Some(db_path)
        };
        let config = TileConfig {
            tile_format: match tile_format {
                "mlt" => TileFormat::Mlt,
                _ => TileFormat::Mvt,
            },
            min_zoom: min_zoom as u8,
            max_zoom: max_zoom as u8,
            base_zoom: if base_zoom < 0 {
                None
            } else {
                Some(base_zoom as u8)
            },
            simplification: do_simplify,
            drop_rate: if drop_rate > 0.0 {
                Some(drop_rate)
            } else {
                None
            },
            cluster_distance: if cluster_distance > 0.0 {
                Some(cluster_distance)
            } else {
                None
            },
            cluster_maxzoom: if cluster_maxzoom >= 0 {
                Some(cluster_maxzoom as u8)
            } else {
                None
            },
            coalesce: do_coalesce,
        };

        let maybe_stream = match streaming_mode {
            "always" => true,
            "auto" if cluster_distance <= 0.0 => {
                freestiler_core::streaming::query_feature_count(db_path_opt, sql)
                    .map(|count| count >= freestiler_core::streaming::auto_threshold())
                    .unwrap_or(false)
            }
            _ => false,
        };

        if maybe_stream {
            match freestiler_core::streaming::generate_pmtiles_from_duckdb_query(
                db_path_opt,
                sql,
                output_path,
                layer_name,
                &config,
                reporter.as_ref(),
            ) {
                Ok(_) => return output_path.to_string(),
                Err(e) => {
                    let can_fallback = streaming_mode == "auto"
                        && (e.contains("POINT geometries only")
                            || e.contains("does not support clustering"));
                    if !can_fallback {
                        return format!("Error: {}", e);
                    }
                    if !quiet {
                        reporter.report("  Streaming unavailable for this query, falling back to in-memory tiling");
                    }
                }
            }
        }

        let layers = match freestiler_core::file_input::duckdb_query_to_layers(
            db_path_opt,
            sql,
            layer_name,
            min_zoom as u8,
            max_zoom as u8,
        ) {
            Ok(l) => l,
            Err(e) => return format!("Error: {}", e),
        };

        if !quiet {
            let total: usize = layers.iter().map(|l| l.features.len()).sum();
            reporter.report(&format!("  Query returned {} features", total));
        }

        match engine::generate_pmtiles(&layers, output_path, &config, reporter.as_ref()) {
            Ok(()) => output_path.to_string(),
            Err(e) => format!("Error: {}", e),
        }
    }
}

/// Read PMTiles header and metadata as a JSON string
/// @param path Path to the .pmtiles file
/// @export
#[extendr]
fn rust_pmtiles_metadata(path: &str) -> String {
    use flate2::read::GzDecoder;
    use pmtiles2::Header;
    use std::io::Cursor;

    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => return format!("Error: Cannot open {}: {}", path, e),
    };

    // Read the raw 127-byte header
    let mut header_bytes = [0u8; 127];
    if let Err(e) = file.read_exact(&mut header_bytes) {
        return format!("Error: Cannot read PMTiles header: {}", e);
    }

    // Save the real tile_type byte (offset 99) before patching for pmtiles2
    let tile_type_byte = header_bytes[99];
    let tile_format = match tile_type_byte {
        0x01 => "mvt",
        0x06 => "mlt",
        _ => "unknown",
    };

    // Patch tile_type to MVT (0x01) so pmtiles2 can parse the header
    // (pmtiles2 doesn't know about the MLT tile type 0x06)
    header_bytes[99] = 0x01;

    let header = match Header::from_reader(&mut Cursor::new(&header_bytes)) {
        Ok(h) => h,
        Err(e) => return format!("Error: Invalid PMTiles header: {}", e),
    };

    // Read and decompress metadata JSON
    let metadata_json = if header.json_metadata_length > 0 {
        let mut compressed = vec![0u8; header.json_metadata_length as usize];
        if file
            .seek(SeekFrom::Start(header.json_metadata_offset))
            .is_ok()
            && file.read_exact(&mut compressed).is_ok()
        {
            let mut decoder = GzDecoder::new(&compressed[..]);
            let mut json_str = String::new();
            if decoder.read_to_string(&mut json_str).is_ok() {
                serde_json::from_str::<serde_json::Value>(&json_str)
                    .unwrap_or(serde_json::Value::Null)
            } else {
                serde_json::Value::Null
            }
        } else {
            serde_json::Value::Null
        }
    } else {
        serde_json::Value::Null
    };

    let result = serde_json::json!({
        "min_zoom": header.min_zoom,
        "max_zoom": header.max_zoom,
        "center_zoom": header.center_zoom,
        "min_longitude": header.min_pos.longitude,
        "min_latitude": header.min_pos.latitude,
        "max_longitude": header.max_pos.longitude,
        "max_latitude": header.max_pos.latitude,
        "center_longitude": header.center_pos.longitude,
        "center_latitude": header.center_pos.latitude,
        "tile_format": tile_format,
        "num_tiles": header.num_addressed_tiles,
        "metadata": metadata_json,
    });

    serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string())
}

extendr_module! {
    mod freestiler;
    fn rust_freestile;
    fn rust_freestile_file;
    fn rust_freestile_duckdb;
    fn rust_freestile_duckdb_query;
    fn rust_pmtiles_metadata;
}
