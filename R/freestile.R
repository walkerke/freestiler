#' Create a layer specification with per-layer zoom range
#'
#' Wraps an sf object with optional per-layer zoom range overrides for use in
#' multi-layer tile generation.
#'
#' @param input An sf data frame.
#' @param min_zoom Integer. Minimum zoom level for this layer. If NULL, uses the
#'   global min_zoom from \code{freestile()}.
#' @param max_zoom Integer. Maximum zoom level for this layer. If NULL, uses the
#'   global max_zoom from \code{freestile()}.
#'
#' @return A freestile_layer object (list with class attribute).
#'
#' @examples
#' \dontrun{
#' library(sf)
#' nc <- st_read(system.file("shape/nc.shp", package = "sf"))
#' roads <- st_read("roads.shp")
#'
#' freestile(
#'   list(
#'     counties = freestile_layer(nc, min_zoom = 0, max_zoom = 10),
#'     roads = freestile_layer(roads, min_zoom = 8, max_zoom = 14)
#'   ),
#'   "layers.pmtiles"
#' )
#' }
#'
#' @export
freestile_layer <- function(input, min_zoom = NULL, max_zoom = NULL) {
  if (!inherits(input, "sf")) {
    stop("`input` must be an sf object.", call. = FALSE)
  }
  structure(
    list(input = input, min_zoom = min_zoom, max_zoom = max_zoom),
    class = "freestile_layer"
  )
}

#' Create vector tiles from spatial data
#'
#' Creates a PMTiles archive containing vector tiles from one or more sf data
#' frames. Supports both Mapbox Vector Tile (MVT) and MapLibre Tile (MLT)
#' formats, multi-layer output, feature dropping, point clustering, and feature
#' coalescing.
#'
#' @param input An sf data frame, or a named list of sf/freestile_layer objects
#'   for multi-layer output.
#' @param output Character. Path for the output .pmtiles file.
#' @param layer_name Character. Name for the tile layer. If NULL, derived from
#'   the output filename. Only used for single-layer input.
#' @param tile_format Character. Tile encoding format: `"mlt"` (default) for
#'   MapLibre Tiles or `"mvt"` for Mapbox Vector Tiles.
#' @param min_zoom Integer. Minimum zoom level (default 0).
#' @param max_zoom Integer. Maximum zoom level (default 14).
#' @param base_zoom Integer. Zoom level at and above which all features are
#'   present (no dropping). NULL (default) uses each layer's own max_zoom.
#'   The drop-rate curve is also computed relative to base_zoom, so lowering
#'   it produces gentler thinning at low zooms. Inspired by tippecanoe's
#'   \code{-B} / \code{--base-zoom}.
#' @param drop_rate Numeric. Exponential drop rate for feature thinning (e.g.
#'   2.5). At each zoom level below base_zoom, features are retained at a rate
#'   of 1/drop_rate^(base_zoom - zoom). Points are thinned using spatial
#'   ordering; polygons/lines are thinned by area. NULL (default) disables
#'   drop-rate thinning.
#' @param cluster_distance Numeric. Pixel distance for point clustering. Points
#'   within this radius are merged into cluster features with a `point_count`
#'   attribute. NULL (default) disables clustering.
#' @param cluster_maxzoom Integer. Maximum zoom level for clustering. Above this
#'   zoom, individual points are shown. Default is max_zoom - 1.
#' @param coalesce Logical. Whether to merge features with identical attributes
#'   within each tile (default FALSE). Lines sharing endpoints are merged;
#'   polygons are grouped into MultiPolygons.
#' @param simplification Logical. Whether to snap geometries to the tile pixel
#'   grid at each zoom level (default TRUE). This provides zoom-adaptive
#'   simplification and prevents slivers between adjacent polygons.
#' @param generate_ids Logical. Whether to assign sequential feature IDs
#'   (default TRUE).
#' @param overwrite Logical. Whether to overwrite existing output file
#'   (default TRUE).
#' @param quiet Logical. Whether to suppress progress messages (default FALSE).
#'
#' @return The output file path (invisibly).
#'
#' @examples
#' \dontrun{
#' library(sf)
#' nc <- st_read(system.file("shape/nc.shp", package = "sf"))
#'
#' # Single layer
#' freestile(nc, "nc.pmtiles", layer_name = "counties")
#'
#' # Multi-layer
#' pts <- st_centroid(nc)
#' freestile(
#'   list(counties = nc, centroids = pts),
#'   "nc_layers.pmtiles"
#' )
#'
#' # With dropping and coalescing
#' freestile(nc, "nc_drop.pmtiles", drop_rate = 2.5, coalesce = TRUE)
#'
#' # With point clustering
#' freestile(pts, "pts.pmtiles", cluster_distance = 50, cluster_maxzoom = 8)
#' }
#'
#' @export
freestile <- function(
    input,
    output,
    layer_name = NULL,
    tile_format = "mlt",
    min_zoom = 0L,
    max_zoom = 14L,
    base_zoom = NULL,
    drop_rate = NULL,
    cluster_distance = NULL,
    cluster_maxzoom = NULL,
    coalesce = FALSE,
    simplification = TRUE,
    generate_ids = TRUE,
    overwrite = TRUE,
    quiet = FALSE
) {
  tile_format <- match.arg(tile_format, c("mlt", "mvt"))

  output <- normalizePath(output, mustWork = FALSE)

  if (file.exists(output)) {
    if (overwrite) {
      unlink(output)
    } else {
      stop("Output file already exists. Set `overwrite = TRUE` to replace it.",
        call. = FALSE)
    }
  }

  # Determine default layer_name from output if single-layer
  if (is.null(layer_name) && inherits(input, "sf")) {
    layer_name <- tools::file_path_sans_ext(basename(output))
  }

  # Normalize to list of layer specs
  layers <- .normalize_layers(input, layer_name, min_zoom, max_zoom)

  # Count total features
  total_features <- sum(vapply(layers, function(l) nrow(l$input), integer(1)))

  if (!quiet) {
    message(sprintf(
      "Creating %s tiles (zoom %d-%d) for %d features across %d layer%s...",
      toupper(tile_format), min_zoom, max_zoom, total_features,
      length(layers), if (length(layers) > 1) "s" else ""
    ))
  }

  # Preprocess each layer and build Rust-compatible list
  rust_layers <- lapply(layers, function(l) {
    sf_obj <- l$input

    # Transform to WGS84 if needed
    crs <- sf::st_crs(sf_obj)
    if (is.na(crs)) {
      warning(sprintf("Layer '%s' has no CRS. Assuming WGS84 (EPSG:4326).",
        l$name), call. = FALSE)
    } else if (!sf::st_is_longlat(sf_obj)) {
      if (!quiet) message(sprintf("  Transforming layer '%s' to WGS84...", l$name))
      sf_obj <- sf::st_transform(sf_obj, 4326)
    }

    # Drop Z/M dimensions only if present (st_zm walks every geometry).
    # Checks the first geometry's dimension class — sf sfc objects are
    # homogeneous in dimension, so the first element is representative.
    geom_col <- attr(sf_obj, "sf_column")
    sfc <- sf_obj[[geom_col]]
    if (length(sfc) > 0L && class(sfc[[1L]])[1L] != "XY") {
      sf_obj <- sf::st_zm(sf_obj, drop = TRUE, what = "ZM")
      sfc <- sf_obj[[geom_col]]
    }

    # Extract geometry and properties
    attrs <- sf::st_drop_geometry(sf_obj)
    geom_types <- as.character(sf::st_geometry_type(sfc))
    prop_data <- .extract_properties(attrs)

    # Build positional list for Rust (indices 0-10)
    list(
      l$name,                     # 0: name
      sfc,                        # 1: geometries (sfc)
      geom_types,                 # 2: geom_types
      prop_data$names,            # 3: prop_names
      prop_data$types,            # 4: prop_types
      prop_data$char_values,      # 5: prop_char_values
      prop_data$num_values,       # 6: prop_num_values
      prop_data$int_values,       # 7: prop_int_values
      prop_data$lgl_values,       # 8: prop_lgl_values
      as.integer(l$min_zoom),     # 9: min_zoom
      as.integer(l$max_zoom)      # 10: max_zoom
    )
  })

  result <- rust_freestile(
    layers = rust_layers,
    output_path = output,
    tile_format = tile_format,
    global_min_zoom = as.integer(min_zoom),
    global_max_zoom = as.integer(max_zoom),
    base_zoom = if (is.null(base_zoom)) -1L else as.integer(base_zoom),
    do_simplify = simplification,
    generate_ids = generate_ids,
    quiet = quiet,
    drop_rate = if (is.null(drop_rate)) -1.0 else as.double(drop_rate),
    cluster_distance = if (is.null(cluster_distance)) -1.0 else as.double(cluster_distance),
    cluster_maxzoom = if (is.null(cluster_maxzoom)) -1L else as.integer(cluster_maxzoom),
    do_coalesce = coalesce
  )

  if (startsWith(result, "Error:")) {
    stop(result, call. = FALSE)
  }

  if (!quiet) {
    size <- file.info(output)$size
    message(sprintf("Created %s (%s)", output, .format_size(size)))
    message(sprintf("View with: view_tiles(\"%s\")", basename(output)))
  }

  invisible(output)
}

#' Normalize input to a list of layer specs
#' @noRd
.normalize_layers <- function(input, layer_name, min_zoom, max_zoom) {
  if (inherits(input, "sf")) {
    # Single layer
    if (is.null(layer_name)) {
      layer_name <- "default"
    }
    return(list(
      list(
        name = layer_name,
        input = input,
        min_zoom = min_zoom,
        max_zoom = max_zoom
      )
    ))
  }

  if (is.list(input) && !inherits(input, "sf")) {
    # Multi-layer: named list of sf or freestile_layer objects
    layer_names <- names(input)
    if (is.null(layer_names) || any(layer_names == "")) {
      stop("Multi-layer input must be a named list.", call. = FALSE)
    }

    layers <- lapply(seq_along(input), function(i) {
      item <- input[[i]]
      name <- layer_names[i]

      if (inherits(item, "freestile_layer")) {
        list(
          name = name,
          input = item$input,
          min_zoom = if (!is.null(item$min_zoom)) item$min_zoom else min_zoom,
          max_zoom = if (!is.null(item$max_zoom)) item$max_zoom else max_zoom
        )
      } else if (inherits(item, "sf")) {
        list(
          name = name,
          input = item,
          min_zoom = min_zoom,
          max_zoom = max_zoom
        )
      } else {
        stop(sprintf(
          "Layer '%s' must be an sf object or freestile_layer.", name
        ), call. = FALSE)
      }
    })

    return(layers)
  }

  stop(
    "`input` must be an sf object or a named list of sf/freestile_layer objects.",
    call. = FALSE
  )
}

#' Extract property columns into typed lists for Rust
#' @noRd
.extract_properties <- function(attrs) {
  if (ncol(attrs) == 0) {
    return(list(
      names = character(0),
      types = character(0),
      char_values = list(),
      num_values = list(),
      int_values = list(),
      lgl_values = list()
    ))
  }

  col_names <- names(attrs)
  col_types <- character(length(col_names))
  char_values <- vector("list", length(col_names))
  num_values <- vector("list", length(col_names))
  int_values <- vector("list", length(col_names))
  lgl_values <- vector("list", length(col_names))

  for (i in seq_along(col_names)) {
    col <- attrs[[i]]
    if (is.character(col) || is.factor(col)) {
      col_types[i] <- "character"
      char_values[[i]] <- as.character(col)
    } else if (is.integer(col)) {
      col_types[i] <- "integer"
      int_values[[i]] <- col
    } else if (is.numeric(col)) {
      col_types[i] <- "numeric"
      num_values[[i]] <- as.double(col)
    } else if (is.logical(col)) {
      col_types[i] <- "logical"
      lgl_values[[i]] <- col
    } else {
      col_types[i] <- "character"
      char_values[[i]] <- as.character(col)
    }
  }

  list(
    names = col_names,
    types = col_types,
    char_values = char_values,
    num_values = num_values,
    int_values = int_values,
    lgl_values = lgl_values
  )
}

#' Create vector tiles from a spatial file
#'
#' Reads a GeoParquet, GeoPackage, Shapefile, or other spatial file directly
#' into the tiling engine. The GeoParquet engine requires compilation with
#' `FREESTILER_GEOPARQUET=true`. The DuckDB engine uses the Rust DuckDB backend
#' when included in the build (enabled by default for native builds), or falls
#' back to the R `duckdb` package (which reads the file via DuckDB's
#' `ST_Read()`, auto-detects the source CRS via `ST_Read_Meta()`, and
#' reprojects to WGS84). Control backend selection with
#' `options(freestiler.duckdb_backend = "auto"|"rust"|"r")`.
#'
#' @param input Character. Path to the input spatial file.
#' @param output Character. Path for the output .pmtiles file.
#' @param layer_name Character. Name for the tile layer. If NULL, derived from
#'   the output filename.
#' @param tile_format Character. `"mlt"` (default) or `"mvt"`.
#' @param min_zoom Integer. Minimum zoom level (default 0).
#' @param max_zoom Integer. Maximum zoom level (default 14).
#' @param base_zoom Integer. Zoom level at and above which all features are
#'   present. NULL (default) uses max_zoom.
#' @param drop_rate Numeric. Exponential drop rate. NULL (default) disables.
#' @param cluster_distance Numeric. Pixel distance for clustering. NULL disables.
#' @param cluster_maxzoom Integer. Max zoom for clustering. Default max_zoom - 1.
#' @param coalesce Logical. Whether to merge features with identical attributes
#'   (default FALSE).
#' @param simplification Logical. Whether to snap geometries to the tile pixel
#'   grid (default TRUE).
#' @param overwrite Logical. Whether to overwrite existing output (default TRUE).
#' @param quiet Logical. Whether to suppress progress (default FALSE).
#' @param engine Character. Backend engine: `"geoparquet"` (default, for
#'   GeoParquet files) or `"duckdb"` (for any file format DuckDB supports).
#'
#' @return The output file path (invisibly).
#'
#' @examples
#' \dontrun{
#' freestile_file("data.parquet", "output.pmtiles")
#' freestile_file("data.gpkg", "output.pmtiles", engine = "duckdb")
#' }
#'
#' @export
freestile_file <- function(
    input,
    output,
    layer_name = NULL,
    tile_format = "mlt",
    min_zoom = 0L,
    max_zoom = 14L,
    base_zoom = NULL,
    drop_rate = NULL,
    cluster_distance = NULL,
    cluster_maxzoom = NULL,
    coalesce = FALSE,
    simplification = TRUE,
    overwrite = TRUE,
    quiet = FALSE,
    engine = "geoparquet"
) {
  tile_format <- match.arg(tile_format, c("mlt", "mvt"))
  engine <- match.arg(engine, c("geoparquet", "duckdb"))

  input <- normalizePath(input, mustWork = TRUE)
  output <- normalizePath(output, mustWork = FALSE)

  if (file.exists(output)) {
    if (overwrite) {
      unlink(output)
    } else {
      stop("Output file already exists. Set `overwrite = TRUE` to replace it.",
        call. = FALSE)
    }
  }

  if (is.null(layer_name)) {
    layer_name <- tools::file_path_sans_ext(basename(output))
  }

  if (engine == "duckdb") {
    backend <- .choose_duckdb_backend()

    if (!quiet) {
      backend_label <- if (backend == "rust") "Rust DuckDB" else "R duckdb package"
      message(sprintf(
        "Reading %s via %s, creating %s tiles (zoom %d-%d)...",
        basename(input), backend_label, toupper(tile_format), min_zoom, max_zoom
      ))
    }

    if (backend == "r") {
      # Detect source CRS from file metadata
      source_crs <- .duckdb_detect_file_crs(input)
      if (is.null(source_crs) || !nzchar(source_crs)) {
        stop(
          "Could not detect CRS from file metadata via DuckDB ST_Read_Meta(). ",
          "Use a file with embedded CRS metadata, supply the data through ",
          "`freestile()` as an sf object, or use the Rust DuckDB backend.",
          call. = FALSE
        )
      }

      sql <- sprintf("SELECT * FROM ST_Read('%s')", gsub("'", "''", input))
      sf_result <- .r_duckdb_query_to_sf(sql, db_path = NULL,
        source_crs = source_crs)
      return(freestile(
        sf_result, output,
        layer_name = layer_name, tile_format = tile_format,
        min_zoom = min_zoom, max_zoom = max_zoom,
        base_zoom = base_zoom, drop_rate = drop_rate,
        cluster_distance = cluster_distance,
        cluster_maxzoom = cluster_maxzoom,
        coalesce = coalesce, simplification = simplification,
        overwrite = FALSE, quiet = quiet
      ))
    }

    # Rust DuckDB path
    result <- rust_freestile_duckdb(
      input_path = input,
      output_path = output,
      layer_name = layer_name,
      tile_format = tile_format,
      min_zoom = as.integer(min_zoom),
      max_zoom = as.integer(max_zoom),
      base_zoom = if (is.null(base_zoom)) -1L else as.integer(base_zoom),
      do_simplify = simplification,
      drop_rate = if (is.null(drop_rate)) -1.0 else as.double(drop_rate),
      cluster_distance = if (is.null(cluster_distance)) -1.0 else as.double(cluster_distance),
      cluster_maxzoom = if (is.null(cluster_maxzoom)) -1L else as.integer(cluster_maxzoom),
      do_coalesce = coalesce,
      quiet = quiet
    )

    if (startsWith(result, "Error:")) {
      stop(result, call. = FALSE)
    }

    if (!quiet) {
      size <- file.info(output)$size
      message(sprintf("Created %s (%s)", output, .format_size(size)))
      message(sprintf("View with: view_tiles(\"%s\")", basename(output)))
    }

    return(invisible(output))
  }

  # GeoParquet engine (Rust-only)
  if (!quiet) {
    message(sprintf(
      "Reading %s via geoparquet engine, creating %s tiles (zoom %d-%d)...",
      basename(input), toupper(tile_format), min_zoom, max_zoom
    ))
  }

  result <- rust_freestile_file(
    input_path = input,
    output_path = output,
    layer_name = layer_name,
    tile_format = tile_format,
    min_zoom = as.integer(min_zoom),
    max_zoom = as.integer(max_zoom),
    base_zoom = if (is.null(base_zoom)) -1L else as.integer(base_zoom),
    do_simplify = simplification,
    drop_rate = if (is.null(drop_rate)) -1.0 else as.double(drop_rate),
    cluster_distance = if (is.null(cluster_distance)) -1.0 else as.double(cluster_distance),
    cluster_maxzoom = if (is.null(cluster_maxzoom)) -1L else as.integer(cluster_maxzoom),
    do_coalesce = coalesce,
    quiet = quiet
  )

  if (startsWith(result, "Error:")) {
    stop(result, call. = FALSE)
  }

  if (!quiet) {
    size <- file.info(output)$size
    message(sprintf("Created %s (%s)", output, .format_size(size)))
    message(sprintf("View with: view_tiles(\"%s\")", basename(output)))
  }

  invisible(output)
}

#' Create vector tiles from a DuckDB SQL query
#'
#' Executes a SQL query via DuckDB's spatial extension and pipes the results
#' into the tiling engine. Uses the Rust DuckDB backend when included in the
#' build (enabled by default for native builds), or falls back to the R
#' `duckdb` package. Control backend selection with
#' `options(freestiler.duckdb_backend = "auto"|"rust"|"r")`.
#'
#' When using the R fallback, `source_crs` must be supplied explicitly so the
#' query result can be interpreted or reprojected correctly. Pass
#' `"EPSG:4326"` if the SQL already returns WGS84 geometry, or the source CRS
#' string (for example `"EPSG:4267"`) to have DuckDB reproject to WGS84 before
#' tiling. For file-based input where the CRS is embedded in the file, use
#' [freestile_file()] with `engine = "duckdb"` instead, which auto-detects the
#' source CRS.
#'
#' @param query Character. A SQL query that returns a geometry column. DuckDB
#'   spatial functions like `ST_Read()` and `read_parquet()` are available.
#' @param output Character. Path for the output .pmtiles file.
#' @param db_path Character. Path to a DuckDB database file, or NULL (default)
#'   for an in-memory database.
#' @param source_crs Character or NULL. CRS of the geometry returned by
#'   `query`, for example `"EPSG:4326"` or `"EPSG:4267"`. Used only by the R
#'   `duckdb` fallback; ignored by the Rust DuckDB backend.
#' @param layer_name Character. Name for the tile layer. If NULL, derived from
#'   the output filename.
#' @param tile_format Character. `"mlt"` (default) or `"mvt"`.
#' @param min_zoom Integer. Minimum zoom level (default 0).
#' @param max_zoom Integer. Maximum zoom level (default 14).
#' @param base_zoom Integer. Zoom level at and above which all features are
#'   present. NULL (default) uses max_zoom.
#' @param drop_rate Numeric. Exponential drop rate. NULL (default) disables.
#' @param cluster_distance Numeric. Pixel distance for clustering. NULL disables.
#' @param cluster_maxzoom Integer. Max zoom for clustering. Default max_zoom - 1.
#' @param coalesce Logical. Whether to merge features with identical attributes
#'   (default FALSE).
#' @param simplification Logical. Whether to snap geometries to the tile pixel
#'   grid (default TRUE).
#' @param overwrite Logical. Whether to overwrite existing output (default TRUE).
#' @param quiet Logical. Whether to suppress progress (default FALSE).
#' @param streaming Character. DuckDB query execution mode: `"auto"` (default)
#'   enables the streaming point pipeline for large queries, `"always"` forces
#'   it, and `"never"` uses the existing in-memory path.
#'
#' @return The output file path (invisibly).
#'
#' @examples
#' \dontrun{
#' # Query a GeoParquet file
#' freestile_query(
#'   "SELECT * FROM read_parquet('data.parquet') WHERE pop > 50000",
#'   "output.pmtiles"
#' )
#'
#' # Query a Shapefile
#' freestile_query(
#'   "SELECT * FROM ST_Read('counties.shp')",
#'   "counties.pmtiles"
#' )
#'
#' # Query with an existing DuckDB database
#' freestile_query(
#'   "SELECT * FROM my_table WHERE region = 'West'",
#'   "west.pmtiles",
#'   db_path = "my_database.duckdb"
#' )
#' }
#'
#' @export
freestile_query <- function(
    query,
    output,
    db_path = NULL,
    layer_name = NULL,
    tile_format = "mlt",
    min_zoom = 0L,
    max_zoom = 14L,
    base_zoom = NULL,
    drop_rate = NULL,
    cluster_distance = NULL,
    cluster_maxzoom = NULL,
    coalesce = FALSE,
    simplification = TRUE,
    overwrite = TRUE,
    quiet = FALSE,
    source_crs = NULL,
    streaming = "auto"
) {
  tile_format <- match.arg(tile_format, c("mlt", "mvt"))
  streaming <- match.arg(streaming, c("auto", "always", "never"))

  output <- normalizePath(output, mustWork = FALSE)

  if (file.exists(output)) {
    if (overwrite) {
      unlink(output)
    } else {
      stop("Output file already exists. Set `overwrite = TRUE` to replace it.",
        call. = FALSE)
    }
  }

  if (is.null(layer_name)) {
    layer_name <- tools::file_path_sans_ext(basename(output))
  }

  backend <- .choose_duckdb_backend()

  if (!quiet) {
    backend_label <- if (backend == "rust") "Rust DuckDB" else "R duckdb package"
    message(sprintf(
      "Executing query via %s, creating %s tiles (zoom %d-%d)...",
      backend_label, toupper(tile_format), min_zoom, max_zoom
    ))
  }

  if (backend == "r") {
    if (streaming == "always") {
      stop(
        "Streaming mode is only available with the Rust DuckDB backend.",
        call. = FALSE
      )
    }
    sf_result <- .r_duckdb_query_to_sf(
      query,
      db_path = db_path,
      source_crs = source_crs
    )
    return(freestile(
      sf_result, output,
      layer_name = layer_name, tile_format = tile_format,
      min_zoom = min_zoom, max_zoom = max_zoom,
      base_zoom = base_zoom, drop_rate = drop_rate,
      cluster_distance = cluster_distance,
      cluster_maxzoom = cluster_maxzoom,
      coalesce = coalesce, simplification = simplification,
      overwrite = FALSE, quiet = quiet
    ))
  }

  # Rust DuckDB path
  result <- rust_freestile_duckdb_query(
    sql = query,
    db_path = if (is.null(db_path)) "" else db_path,
    output_path = output,
    layer_name = layer_name,
    tile_format = tile_format,
    min_zoom = as.integer(min_zoom),
    max_zoom = as.integer(max_zoom),
    base_zoom = if (is.null(base_zoom)) -1L else as.integer(base_zoom),
    do_simplify = simplification,
    drop_rate = if (is.null(drop_rate)) -1.0 else as.double(drop_rate),
    cluster_distance = if (is.null(cluster_distance)) -1.0 else as.double(cluster_distance),
    cluster_maxzoom = if (is.null(cluster_maxzoom)) -1L else as.integer(cluster_maxzoom),
    do_coalesce = coalesce,
    quiet = quiet,
    streaming_mode = streaming
  )

  if (startsWith(result, "Error:")) {
    stop(result, call. = FALSE)
  }

  if (!quiet) {
    size <- file.info(output)$size
    message(sprintf("Created %s (%s)", output, .format_size(size)))
    message(sprintf("View with: view_tiles(\"%s\")", basename(output)))
  }

  invisible(output)
}

# Package-level cache for backend detection
.pkg_cache <- new.env(parent = emptyenv())

#' Check if Rust DuckDB feature is compiled (cached per session)
#' @noRd
.has_rust_duckdb <- function() {
  if (!is.null(.pkg_cache$rust_duckdb)) return(.pkg_cache$rust_duckdb)
  result <- rust_freestile_duckdb_query("", "", "", "", "mvt", 0L, 6L, -1L,
    TRUE, -1.0, -1.0, -1L, FALSE, TRUE, "never")
  val <- !startsWith(result, "Error: DuckDB support not compiled")
  .pkg_cache$rust_duckdb <- val
  val
}

#' Check if R duckdb package is available
#' @noRd
.has_r_duckdb <- function() {
  requireNamespace("duckdb", quietly = TRUE) &&
    requireNamespace("DBI", quietly = TRUE)
}

#' Choose DuckDB backend based on option and availability
#' @noRd
.choose_duckdb_backend <- function() {
  backend <- getOption("freestiler.duckdb_backend", "auto")
  backend <- match.arg(backend, c("auto", "rust", "r"))

  if (backend == "rust") {
    if (!.has_rust_duckdb()) {
      stop(
        "Rust DuckDB backend requested but not available in this build. ",
        "Install the r-universe build or rebuild from source with DuckDB enabled, or set ",
        "options(freestiler.duckdb_backend = \"auto\") to use the R fallback.",
        call. = FALSE
      )
    }
    return("rust")
  }

  if (backend == "r") {
    if (!.has_r_duckdb()) {
      stop(
        "R duckdb backend requested but not installed. ",
        "Install with install.packages(c(\"duckdb\", \"DBI\")).",
        call. = FALSE
      )
    }
    return("r")
  }

  # auto: prefer Rust, fall back to R
  if (.has_rust_duckdb()) return("rust")
  if (.has_r_duckdb()) return("r")

  stop(
    "No DuckDB backend available. Either:\n",
    "  - Install the r-universe build or rebuild from source with DuckDB enabled, or\n",
    "  - Install the R duckdb package: install.packages(c(\"duckdb\", \"DBI\"))",
    call. = FALSE
  )
}

#' Execute a DuckDB SQL query and return an sf object via the R duckdb package
#'
#' Detects the geometry column and converts to WKB. If \code{source_crs} is
#' provided and is not EPSG:4326, reprojects via \code{ST_Transform} inside
#' DuckDB. If \code{source_crs} is NULL, an error is raised because the R
#' fallback requires an explicit CRS contract.
#'
#' @param sql Character. SQL query returning a geometry column.
#' @param db_path Character or NULL. Path to DuckDB database file, or NULL for
#'   in-memory.
#' @param source_crs Character or NULL. Source CRS string (e.g. "EPSG:4267").
#'   If provided, geometry is reprojected to EPSG:4326 via ST_Transform. If
#'   NULL, the R fallback errors and asks the caller to provide it.
#' @return An sf data frame with crs = 4326.
#' @noRd
.r_duckdb_query_to_sf <- function(sql, db_path = NULL, source_crs = NULL) {
  if (is.null(db_path) || db_path == "") {
    con <- DBI::dbConnect(duckdb::duckdb())
  } else {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  }
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "INSTALL spatial; LOAD spatial;")

  if (is.null(source_crs) || !nzchar(source_crs)) {
    stop(
      "The R DuckDB fallback requires an explicit `source_crs`. ",
      "Pass the CRS of the query result (for example `source_crs = \"EPSG:4326\"` ",
      "or `source_crs = \"EPSG:4267\"`), or use the Rust DuckDB backend.",
      call. = FALSE
    )
  }

  # Discover schema
  desc <- DBI::dbGetQuery(con, paste0("DESCRIBE (", sql, ")"))

  # Find geometry column: first GEOMETRY type
  geom_idx <- grep("^GEOMETRY", desc$column_type, ignore.case = TRUE)
  if (length(geom_idx) == 0L) {
    stop(
      "No geometry column found in query result. ",
      "DuckDB DESCRIBE returned types: ",
      paste(desc$column_type, collapse = ", "),
      call. = FALSE
    )
  }
  geom_col <- desc$column_name[geom_idx[1L]]

  # Build WKB query with reprojection when source CRS is known and not 4326
  needs_transform <- !is.null(source_crs) && source_crs != "EPSG:4326"

  if (needs_transform) {
    wrapped_sql <- sprintf(
      "SELECT * EXCLUDE (\"%s\"), ST_AsWKB(ST_Transform(\"%s\", '%s', 'EPSG:4326')) AS __wkb FROM (%s) AS __t",
      geom_col, geom_col, source_crs, sql
    )
  } else {
    wrapped_sql <- sprintf(
      "SELECT * EXCLUDE (\"%s\"), ST_AsWKB(\"%s\") AS __wkb FROM (%s) AS __t",
      geom_col, geom_col, sql
    )
  }

  df <- DBI::dbGetQuery(con, wrapped_sql)

  if (nrow(df) == 0L) {
    stop("Query returned no rows.", call. = FALSE)
  }

  # Convert WKB to sf with CRS = 4326
  wkb_col <- df[["__wkb"]]
  df[["__wkb"]] <- NULL
  geom <- sf::st_as_sfc(wkb_col, crs = 4326)
  sf::st_sf(df, geometry = geom)
}

#' Detect CRS from a spatial file via DuckDB ST_Read_Meta
#'
#' Opens a temporary DuckDB connection, loads the spatial extension, and
#' extracts the CRS authority string from the file's metadata. Returns
#' NULL on any failure.
#'
#' @param file_path Character. Path to the spatial file.
#' @return Character CRS string (e.g. "EPSG:4267") or NULL.
#' @noRd
.duckdb_detect_file_crs <- function(file_path) {
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    DBI::dbExecute(con, "INSTALL spatial; LOAD spatial;")

    meta <- DBI::dbGetQuery(
      con,
      sprintf("SELECT * FROM ST_Read_Meta('%s')", gsub("'", "''", file_path))
    )
    crs_df <- meta$layers[[1]]$geometry_fields[[1]]$crs
    auth_name <- crs_df[["auth_name"]]
    auth_code <- crs_df[["auth_code"]]
    if (!is.null(auth_name) && nchar(auth_name) > 0L &&
        !is.null(auth_code) && nchar(auth_code) > 0L) {
      paste0(auth_name, ":", auth_code)
    } else {
      NULL
    }
  }, error = function(e) NULL)
}

#' Format file size for display
#' @noRd
.format_size <- function(size) {
  if (size >= 1e6) {
    sprintf("%.1f MB", size / 1e6)
  } else if (size >= 1e3) {
    sprintf("%.1f KB", size / 1e3)
  } else {
    sprintf("%d bytes", as.integer(size))
  }
}
