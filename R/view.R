#' Quickly view a PMTiles file on an interactive map
#'
#' Starts a local tile server (if needed) and creates an interactive mapgl map
#' showing the tileset. Layer type and styling are auto-detected from the
#' PMTiles metadata when possible.
#'
#' @param input Path to a local \code{.pmtiles} file.
#' @param layer Character. Source layer name to display. If \code{NULL}
#'   (default), the first layer in the tileset is used.
#' @param layer_type Character. Map layer type: \code{"fill"}, \code{"line"},
#'   or \code{"circle"}. If \code{NULL}, auto-detected from the PMTiles
#'   metadata geometry type.
#' @param color Fill, line, or circle color. Default is \code{"navy"} for fill
#'   and line layers, \code{"steelblue"} for circle layers.
#' @param opacity Numeric opacity (0--1). Default is 0.5.
#' @param port Port for the local tile server. Default is 8080.
#' @param promote_id Character. Property name to use as the feature ID for
#'   hover interactivity. If \code{NULL}, no feature promotion is used.
#'
#' @return A mapgl map object (can be piped into further mapgl operations).
#'
#' @examples
#' \dontrun{
#' freestile(nc, "nc.pmtiles", layer_name = "counties")
#' view_tiles("nc.pmtiles")
#'
#' # Override auto-detection
#' view_tiles("roads.pmtiles", layer_type = "line", color = "red")
#'
#' # Point data
#' view_tiles("airports.pmtiles", layer_type = "circle", color = "orange")
#' }
#'
#' @seealso [serve_tiles()], [freestile()]
#' @export
view_tiles <- function(
    input,
    layer = NULL,
    layer_type = NULL,
    color = NULL,
    opacity = 0.5,
    port = 8080,
    promote_id = NULL
) {
  if (!requireNamespace("mapgl", quietly = TRUE)) {
    stop(
      "Package 'mapgl' is required for view_tiles().\n",
      "Install it with: install.packages('mapgl')",
      call. = FALSE
    )
  }

  input <- normalizePath(input, mustWork = TRUE)

  # Read metadata from the PMTiles file
  meta <- .pmtiles_metadata(input)
  if (is.null(meta)) {
    stop("Cannot read PMTiles metadata from: ", input, call. = FALSE)
  }

  # Determine layer name
  layers_info <- meta$metadata$vector_layers
  if (is.null(layers_info) || length(layers_info) == 0L) {
    stop("No vector layers found in PMTiles metadata.", call. = FALSE)
  }

  if (is.null(layer)) {
    layer <- layers_info[[1L]]$id
  }

  # Find the matching layer info
  layer_info <- NULL
  for (li in layers_info) {
    if (li$id == layer) {
      layer_info <- li
      break
    }
  }

  if (is.null(layer_info)) {
    available <- vapply(layers_info, function(x) x$id, character(1))
    stop(
      sprintf("Layer '%s' not found. Available layers: %s",
              layer, paste(available, collapse = ", ")),
      call. = FALSE
    )
  }

  # Auto-detect layer type from metadata geometry_type

  if (is.null(layer_type)) {
    gt <- layer_info$geometry_type
    if (!is.null(gt)) {
      layer_type <- switch(gt,
        "Point" = "circle",
        "Line" = "line",
        "Polygon" = "fill",
        "fill"  # default fallback
      )
    } else {
      layer_type <- "fill"
    }
  }
  layer_type <- match.arg(layer_type, c("fill", "line", "circle"))

  # Default color
  if (is.null(color)) {
    color <- if (layer_type == "circle") "steelblue" else "navy"
  }

  # Start local server
  serve_tiles(dirname(input), port = port)

  # Build URL
  tile_url <- sprintf("http://localhost:%d/%s", port, basename(input))

  # Build map
  m <- mapgl::maplibre(
    bounds = c(
      meta$min_longitude, meta$min_latitude,
      meta$max_longitude, meta$max_latitude
    )
  )

  src_args <- list(id = "src", url = tile_url)
  if (!is.null(promote_id)) {
    src_args$promote_id <- promote_id
  }
  m <- do.call(mapgl::add_pmtiles_source, c(list(map = m), src_args))

  if (layer_type == "fill") {
    m <- mapgl::add_fill_layer(
      map = m,
      id = "layer",
      source = "src",
      source_layer = layer,
      fill_color = color,
      fill_opacity = opacity,
      hover_options = list(
        fill_color = "#ffffcc",
        fill_opacity = 0.9
      )
    )
  } else if (layer_type == "line") {
    m <- mapgl::add_line_layer(
      map = m,
      id = "layer",
      source = "src",
      source_layer = layer,
      line_color = color,
      line_opacity = opacity,
      line_width = 1.5
    )
  } else if (layer_type == "circle") {
    m <- mapgl::add_circle_layer(
      map = m,
      id = "layer",
      source = "src",
      source_layer = layer,
      circle_color = color,
      circle_opacity = opacity,
      circle_radius = 4
    )
  }

  m
}

#' Read PMTiles metadata (internal)
#'
#' Calls the Rust backend to read the PMTiles header and metadata JSON.
#'
#' @param path Path to a .pmtiles file.
#' @return A list with header fields and nested metadata, or NULL on error.
#' @noRd
.pmtiles_metadata <- function(path) {
  json_str <- rust_pmtiles_metadata(path)
  if (startsWith(json_str, "Error:")) {
    warning(json_str, call. = FALSE)
    return(NULL)
  }

  tryCatch(
    jsonlite::fromJSON(json_str, simplifyVector = FALSE),
    error = function(e) NULL
  )
}
