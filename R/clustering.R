# In-memory input uses the same core without GeoParquet or DuckDB.
.validate_cluster_sf <- function(input, min_zoom, max_zoom, radius,
                                cluster_maxzoom, min_points, category, values,
                                base_zoom, drop_rate, coalesce, simplification) {
  scalar <- function(x) is.numeric(x) && length(x) == 1L && !is.na(x) && is.finite(x)
  if (!inherits(input, "sf"))
    stop("Categorical clustering currently requires a single POINT sf layer.", call. = FALSE)
  if (!nrow(input) || any(sf::st_is_empty(input)) ||
      any(as.character(sf::st_geometry_type(input)) != "POINT"))
    stop("Categorical clustering requires non-empty POINT geometries only.", call. = FALSE)
  if (!scalar(min_zoom) || !scalar(max_zoom) || min_zoom != floor(min_zoom) ||
      max_zoom != floor(max_zoom) || min_zoom < 0 || max_zoom > 30 || min_zoom > max_zoom)
    stop("Invalid clustering zoom range.", call. = FALSE)
  if (is.null(cluster_maxzoom)) cluster_maxzoom <- max(min_zoom, max_zoom - 1L)
  if (!scalar(cluster_maxzoom) || cluster_maxzoom != floor(cluster_maxzoom) ||
      cluster_maxzoom < min_zoom || cluster_maxzoom > max_zoom)
    stop("cluster_maxzoom must be an integer between min_zoom and max_zoom.", call. = FALSE)
  if (!scalar(radius) || radius <= 0)
    stop("Categorical clustering requires a positive cluster_distance.", call. = FALSE)
  if (!scalar(min_points) || min_points < 1 || min_points != floor(min_points) ||
      min_points > .Machine$integer.max)
    stop("cluster_min_points must be a positive integer.", call. = FALSE)
  if (!is.null(base_zoom) || !is.null(drop_rate) || isTRUE(coalesce) || !isTRUE(simplification))
    stop("Categorical clustering conserves all points: leave base_zoom and drop_rate unset, coalesce = FALSE, and simplification = TRUE.", call. = FALSE)
  if (!is.character(category) || length(category) != 1L || is.na(category) || !nzchar(category))
    stop("category must name one column.", call. = FALSE)
  if (!category %in% names(sf::st_drop_geometry(input)))
    stop(sprintf("Category column '%s' not found.", category), call. = FALSE)
  if (!(is.character(values) || is.numeric(values)) || anyNA(values) ||
      !length(values) || length(values) > 64L || anyDuplicated(values))
    stop("category_values must contain 1 to 64 distinct strings or integers.", call. = FALSE)
  if (is.numeric(values) && (any(!is.finite(values)) || any(values != floor(values)) ||
                            any(abs(values) > 2^53 - 1)))
    stop("Numeric category_values must be JS-safe integers.", call. = FALSE)
  if (!requireNamespace("jsonlite", quietly = TRUE))
    stop('Categorical clustering requires jsonlite. Install it with install.packages("jsonlite").', call. = FALSE)
  cluster_maxzoom
}

# Ordered categorical clustering uses the tested default-feature core kernel.
# This first integration writes clustered zooms to their own source. The maps
# reuse their existing audited point source above the cutoff. Mixed raw/cluster
# zooms and multilayer migration remain explicit release work, not a fallback to
# the legacy clustering algorithm with different semantics.
.cluster_file <- function(input, output, layer_name, tile_format, min_zoom,
                          max_zoom, cluster_distance, cluster_maxzoom,
                          cluster_min_points, category, category_values,
                          drop_rate, coalesce, quiet) {
  scalar <- function(x) is.numeric(x) && length(x) == 1L && !is.na(x) && is.finite(x)
  if (!scalar(cluster_distance) || cluster_distance <= 0)
    stop("Categorical clustering requires a positive cluster_distance", call. = FALSE)
  if (!scalar(min_zoom) || !scalar(max_zoom) || min_zoom != floor(min_zoom) ||
      max_zoom != floor(max_zoom) || min_zoom < 0 || max_zoom > 30 || min_zoom > max_zoom)
    stop("Invalid clustering zoom range", call. = FALSE)
  if (is.null(cluster_maxzoom)) cluster_maxzoom <- max_zoom - 1L
  if (!identical(as.numeric(cluster_maxzoom), as.numeric(max_zoom)))
    stop("This categorical increment requires cluster_maxzoom = max_zoom. Use a separate dot source above that zoom.", call. = FALSE)
  if (!scalar(cluster_min_points) || cluster_min_points < 1 ||
      cluster_min_points != floor(cluster_min_points) || cluster_min_points > .Machine$integer.max)
    stop("cluster_min_points must be a positive integer", call. = FALSE)
  if (!is.null(drop_rate) || isTRUE(coalesce))
    stop("Categorical clustered zooms conserve all points: omit drop_rate and coalesce.", call. = FALSE)
  if (!is.character(category) || length(category) != 1L || is.na(category) || !nzchar(category))
    stop("category must name one column", call. = FALSE)
  if (!(is.character(category_values) || is.numeric(category_values)) || anyNA(category_values) ||
      !length(category_values) || length(category_values) > 64L || anyDuplicated(category_values))
    stop("category_values must contain 1 to 64 distinct strings or integers", call. = FALSE)
  if (is.numeric(category_values) && (any(!is.finite(category_values)) ||
      any(category_values != floor(category_values)) || any(abs(category_values) > 2^53 - 1)))
    stop("Numeric category_values must be JS-safe integers", call. = FALSE)
  if (!requireNamespace("jsonlite", quietly = TRUE))
    stop("Categorical file clustering requires jsonlite and a GeoParquet-enabled freestiler build.\n",
      'Install jsonlite with install.packages("jsonlite").\n',
      "For native GeoParquet support omitted from CRAN:\n",
      .runiverse_install_hint(), call. = FALSE)
  result <- .Call(wrap__rust_cluster_file, input, output, layer_name, tile_format,
    as.integer(min_zoom), as.integer(max_zoom), as.double(cluster_distance),
    as.integer(cluster_min_points), category,
    jsonlite::toJSON(unname(category_values), auto_unbox = FALSE, digits = NA), quiet)
  if (startsWith(result, "Error:")) .stop_native_error(result)
  invisible(structure(output, cluster_audit = jsonlite::fromJSON(result)))
}
