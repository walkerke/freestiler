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
    message(sprintf(
      "Created %s (%s)",
      output,
      .format_size(size)
    ))
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
