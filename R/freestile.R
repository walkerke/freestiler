#' Create vector tiles from spatial data
#'
#' Creates a PMTiles archive containing vector tiles from an sf data frame.
#' Supports both Mapbox Vector Tile (MVT) and MapLibre Tile (MLT) formats.
#'
#' @param input An sf data frame.
#' @param output Character. Path for the output .pmtiles file.
#' @param layer_name Character. Name for the tile layer. If NULL, derived from
#'   the output filename.
#' @param tile_format Character. Tile encoding format: `"mlt"` (default) for
#'   MapLibre Tiles or `"mvt"` for Mapbox Vector Tiles.
#' @param min_zoom Integer. Minimum zoom level (default 0).
#' @param max_zoom Integer. Maximum zoom level (default 14).
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
#' freestile(nc, "nc.pmtiles", layer_name = "counties")
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
    simplification = TRUE,
    generate_ids = TRUE,
    overwrite = TRUE,
    quiet = FALSE
) {
  if (!inherits(input, "sf")) {
    stop("`input` must be an sf object.", call. = FALSE)
  }

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

  if (is.null(layer_name)) {
    layer_name <- tools::file_path_sans_ext(basename(output))
  }

  # Transform to WGS84 if needed
  crs <- sf::st_crs(input)
  if (is.na(crs)) {
    warning("Input has no CRS. Assuming WGS84 (EPSG:4326).", call. = FALSE)
  } else if (!sf::st_is_longlat(input)) {
    if (!quiet) message("Transforming to WGS84 (EPSG:4326)...")
    input <- sf::st_transform(input, 4326)
  }

  # Drop Z/M dimensions if present (no-op for XY geometries)
  input <- sf::st_zm(input, drop = TRUE, what = "ZM")

  if (!quiet) {
    message(sprintf(
      "Creating %s tiles (zoom %d-%d) for %d features...",
      toupper(tile_format), min_zoom, max_zoom, nrow(input)
    ))
  }

  # Extract geometry and attribute data
  geom_col <- attr(input, "sf_column")
  geom <- input[[geom_col]]
  attrs <- sf::st_drop_geometry(input)

  # Get geometry types
  geom_types <- as.character(sf::st_geometry_type(geom))

  # Extract property columns
  prop_data <- .extract_properties(attrs)

  result <- rust_freestile(
    geometries = geom,
    geom_types = geom_types,
    prop_names = prop_data$names,
    prop_types = prop_data$types,
    prop_char_values = prop_data$char_values,
    prop_num_values = prop_data$num_values,
    prop_int_values = prop_data$int_values,
    prop_lgl_values = prop_data$lgl_values,
    output_path = output,
    layer_name = layer_name,
    tile_format = tile_format,
    min_zoom = as.integer(min_zoom),
    max_zoom = as.integer(max_zoom),
    do_simplify = simplification,
    generate_ids = generate_ids,
    quiet = quiet
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
      num_values[[i]] <- rep(NA_real_, length(col))
      int_values[[i]] <- rep(NA_integer_, length(col))
      lgl_values[[i]] <- rep(NA, length(col))
    } else if (is.integer(col)) {
      col_types[i] <- "integer"
      char_values[[i]] <- rep(NA_character_, length(col))
      num_values[[i]] <- rep(NA_real_, length(col))
      int_values[[i]] <- col
      lgl_values[[i]] <- rep(NA, length(col))
    } else if (is.numeric(col)) {
      col_types[i] <- "numeric"
      char_values[[i]] <- rep(NA_character_, length(col))
      num_values[[i]] <- as.double(col)
      int_values[[i]] <- rep(NA_integer_, length(col))
      lgl_values[[i]] <- rep(NA, length(col))
    } else if (is.logical(col)) {
      col_types[i] <- "logical"
      char_values[[i]] <- rep(NA_character_, length(col))
      num_values[[i]] <- rep(NA_real_, length(col))
      int_values[[i]] <- rep(NA_integer_, length(col))
      lgl_values[[i]] <- col
    } else {
      # Coerce to character
      col_types[i] <- "character"
      char_values[[i]] <- as.character(col)
      num_values[[i]] <- rep(NA_real_, length(col))
      int_values[[i]] <- rep(NA_integer_, length(col))
      lgl_values[[i]] <- rep(NA, length(col))
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
