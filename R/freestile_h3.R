#' Create vector tiles with dynamic H3 hexagonal binning
#'
#' Aggregates points into H3 hexagons at zoom-appropriate resolutions and writes
#' a PMTiles archive in which low zooms show coarse hexagons, intermediate zooms
#' show progressively finer hexagons, and zooms at or above `base_zoom` show
#' individual points. Aggregations (count, sum, mean, etc.) are computed in
#' DuckDB via the H3 community extension; the function then assembles the
#' per-resolution hex layers and the raw-point layer via [freestile()].
#'
#' Each distinct H3 resolution becomes its own MVT source-layer (named
#' `"<hex_layer_prefix>_r<resolution>"`, e.g. `"h3_r05"`); raw points are
#' emitted as a separate source-layer (`point_layer_name`). With the default
#' `fade = FALSE`, per-layer zoom windows are disjoint so the rendered map
#' swaps between resolutions cleanly. With `fade = TRUE`, adjacent windows
#' overlap by `fade_overlap` zooms so the companion [view_h3_tiles()] helper
#' can cross-fade between resolutions.
#'
#' DuckDB and the H3 community extension are required. With `sf` input the
#' data is written to DuckDB via a temporary Parquet (or `dbWriteTable`)
#' roundtrip; with character SQL input, the user query is wrapped in a
#' temporary view inside DuckDB.
#'
#' @section Point volume at `base_zoom`:
#' The raw-point layer is encoded without thinning: every input point is
#' written to every tile that contains it from `base_zoom` up (and from
#' `base_zoom - fade_overlap` when `fade = TRUE`). For very large inputs the
#' tiles at the first point zoom can get heavy. Raising `base_zoom` defers
#' points to higher zooms, where each tile covers less area and so holds
#' fewer points. Per-layer feature dropping for the points layer is on the
#' roadmap.
#'
#' @section Antimeridian and polar cells:
#' Hexagons that cross the antimeridian are split at +/-180 degrees so they
#' render correctly on both sides of the dateline instead of as
#' world-spanning slivers. The rare cells that contain a pole receive the
#' same split and render approximately (the polygon edge follows the cell
#' boundary vertices, so the polar cap itself is not filled).
#'
#' @param input An `sf` data frame of POINT geometry, or a character SQL query
#'   that returns a geometry column when executed against DuckDB. (DuckDB's
#'   spatial functions such as `ST_Read()` and `read_parquet()` are available.)
#' @param output Character. Path for the output `.pmtiles` file.
#' @param agg Aggregation specification:
#'   - `"count"` (default): single `count = COUNT(*)` property.
#'   - Named character vector of SQL aggregation expressions, e.g.
#'     `c(n = "COUNT(*)", avg_pop = "AVG(pop)", total = "SUM(pop)")`.
#'   - Named list of `prop_name = c(fn, column)` for callers who don't want to
#'     write SQL, e.g. `list(n = c("count", "*"), avg_pop = c("mean", "pop"))`.
#'     Supported `fn` values: `"count"`, `"sum"`, `"mean"`/`"avg"`, `"min"`,
#'     `"max"`, `"median"`.
#' @param hex_layer_prefix Character. Prefix for the per-resolution hex MVT
#'   layer names. Default `"h3"` produces `"h3_r01"`, `"h3_r02"`, etc.
#' @param point_layer_name Character. MVT layer name for raw points (default
#'   `"points"`).
#' @param min_zoom,max_zoom Integer. Global zoom range (default 0--14).
#' @param base_zoom Integer or NULL. Zoom level at and above which raw points
#'   take over from hex aggregations. Default `NULL` resolves to
#'   `max_zoom - 2L`, clamped so `min_zoom <= base_zoom <= max_zoom`. If
#'   `base_zoom == min_zoom`, no hex layers are created.
#' @param h3_resolutions Optional override of the zoom -> H3 resolution
#'   mapping. Accepts `NULL` (use built-in defaults), an unnamed integer
#'   vector with `length(min_zoom:(base_zoom - 1L))` entries mapped
#'   positionally, or a named integer vector with names that parse to integer
#'   zoom levels (sparse overrides; defaults fill the rest). All resolutions
#'   must be integers in `0:15`. The same resolution appearing in
#'   non-contiguous zoom runs (e.g. zooms 4--5 and 8) is rejected.
#' @param source_crs Character or NULL. CRS of geometry returned by SQL
#'   input, e.g. `"EPSG:4326"` (default for `sf` input, since `sf` is
#'   auto-transformed to WGS84) or `"EPSG:3857"`. Ignored for `sf` input.
#'   For SQL input, if `NULL`, a warning is emitted and the geometry is
#'   assumed to be `"EPSG:4326"`.
#' @param db_path Character or NULL. Path to an existing DuckDB database file,
#'   or `NULL`/`""` (default) for an in-memory database.
#' @param tile_format Character. `"mvt"` (default) or `"mlt"`.
#' @param fade Logical. If `TRUE`, adjacent hex layer zoom windows overlap by
#'   `fade_overlap` zooms so [view_h3_tiles()] can cross-fade between
#'   resolutions. Default `FALSE` (disjoint windows, clean zoom breaks).
#' @param fade_overlap Integer. Zooms of overlap on each side (only used when
#'   `fade = TRUE`, default `1L`).
#' @param overwrite Logical. Whether to overwrite an existing output file
#'   (default `TRUE`).
#' @param quiet Logical. Suppress progress messages (default `FALSE`).
#'
#' @return The output file path (invisibly).
#'
#' @examples
#' \dontrun{
#' library(sf)
#' pts <- st_as_sf(data.frame(
#'   x = runif(50000, -100, -80),
#'   y = runif(50000, 30, 45),
#'   w = rnorm(50000, 100, 10)
#' ), coords = c("x", "y"), crs = 4326)
#'
#' freestile_h3(pts, "wind.pmtiles",
#'   agg = c(n = "COUNT(*)", avg_w = "AVG(w)"),
#'   min_zoom = 2, max_zoom = 12, base_zoom = 10)
#'
#' view_h3_tiles("wind.pmtiles", agg_column = "n")
#'
#' # Cross-fade between resolutions
#' freestile_h3(pts, "wind_fade.pmtiles",
#'   agg = "count",
#'   min_zoom = 2, max_zoom = 12, base_zoom = 10,
#'   fade = TRUE)
#' }
#'
#' @seealso [freestile()], [view_h3_tiles()]
#' @export
freestile_h3 <- function(
    input,
    output,
    agg = "count",
    hex_layer_prefix = "h3",
    point_layer_name = "points",
    min_zoom = 0L,
    max_zoom = 14L,
    base_zoom = NULL,
    h3_resolutions = NULL,
    source_crs = NULL,
    db_path = NULL,
    tile_format = "mvt",
    fade = FALSE,
    fade_overlap = 1L,
    overwrite = TRUE,
    quiet = FALSE
) {
  tile_format <- match.arg(tile_format, c("mvt", "mlt"))

  min_zoom <- as.integer(min_zoom)
  max_zoom <- as.integer(max_zoom)
  if (min_zoom < 0L || max_zoom < min_zoom) {
    stop("`min_zoom` and `max_zoom` must satisfy 0 <= min_zoom <= max_zoom.",
      call. = FALSE)
  }

  base_zoom <- .h3_resolve_base_zoom(base_zoom, min_zoom, max_zoom)

  if (!is.logical(fade) || length(fade) != 1L || is.na(fade)) {
    stop("`fade` must be a single TRUE or FALSE.", call. = FALSE)
  }
  fade_overlap <- as.integer(fade_overlap)
  if (length(fade_overlap) != 1L || is.na(fade_overlap) || fade_overlap < 0L) {
    stop("`fade_overlap` must be a single non-negative integer.", call. = FALSE)
  }

  output <- normalizePath(output, mustWork = FALSE)
  if (file.exists(output) && !overwrite) {
    stop("Output file already exists. Set `overwrite = TRUE` to replace it.",
      call. = FALSE)
  }

  # Parse agg into SQL fragments once; reused per resolution
  agg_spec <- .h3_parse_agg(agg)

  # Compute the resolution -> (min_zoom, max_zoom) windows up front so we can
  # error early on invalid `h3_resolutions` inputs.
  windows <- .h3_zoom_windows(
    min_zoom = min_zoom,
    base_zoom = base_zoom,
    max_zoom = max_zoom,
    h3_resolutions = h3_resolutions,
    fade = fade,
    fade_overlap = fade_overlap,
    hex_layer_prefix = hex_layer_prefix
  )

  if (!quiet) {
    message(sprintf(
      "Building H3 tiles (zoom %d-%d, base_zoom = %d, %d hex layer%s%s)...",
      min_zoom, max_zoom, base_zoom,
      nrow(windows),
      if (nrow(windows) == 1L) "" else "s",
      if (fade) sprintf(", fade overlap = %d", fade_overlap) else ""
    ))
  }

  # Open the DuckDB connection that will own all H3 work.
  ctx <- .h3_open_input(input, source_crs, db_path, quiet = quiet)
  con <- ctx$con
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Per-resolution hex aggregation.
  layers <- list()
  if (nrow(windows) > 0L) {
    for (i in seq_len(nrow(windows))) {
      r <- windows$resolution[i]
      mn <- windows$min_zoom[i]
      mx <- windows$max_zoom[i]
      lname <- windows$layer_name[i]

      if (!quiet) {
        message(sprintf("  Aggregating H3 resolution %d (zoom %d-%d)...",
          r, mn, mx))
      }
      hex_sf <- .h3_aggregate_resolution(con, r, agg_spec)
      if (nrow(hex_sf) == 0L) {
        if (!quiet) {
          message(sprintf("    (no features at resolution %d; skipping layer)", r))
        }
        next
      }
      layers[[lname]] <- freestile_layer(hex_sf, min_zoom = mn, max_zoom = mx)
    }
  }

  # Raw-point layer.
  points_sf <- ctx$points_sf
  if (is.null(points_sf)) {
    # SQL input: read points from the temp view
    points_sf <- .h3_query_to_sf(
      con,
      "SELECT ST_AsWKB(geom) AS __wkb, * EXCLUDE (geom) FROM __h3_input"
    )
  }
  points_min_zoom <- if (fade) max(min_zoom, base_zoom - fade_overlap) else base_zoom
  points_max_zoom <- max_zoom
  if (points_min_zoom <= points_max_zoom) {
    # The Rust feature parser requires at least one attribute column. If the
    # input sf is geometry-only, attach a trivial sequential id so MVT
    # encoding has something to write.
    if (ncol(sf::st_drop_geometry(points_sf)) == 0L) {
      points_sf[["__id"]] <- seq_len(nrow(points_sf))
    }
    layers[[point_layer_name]] <- freestile_layer(
      points_sf,
      min_zoom = points_min_zoom,
      max_zoom = points_max_zoom
    )
  }

  if (length(layers) == 0L) {
    stop(
      "No layers were produced. Check `base_zoom`, `min_zoom`, `max_zoom`, ",
      "and `h3_resolutions`.",
      call. = FALSE
    )
  }

  freestile(
    layers,
    output,
    tile_format = tile_format,
    min_zoom = min_zoom,
    max_zoom = max_zoom,
    overwrite = overwrite,
    quiet = quiet
  )
}

#' View an H3 hexagonal-binning PMTiles archive
#'
#' Reads the metadata from a PMTiles archive produced by [freestile_h3()] and
#' builds a `mapgl` map with one fill layer per H3 resolution plus a circle
#' layer for raw points. Automatically detects whether the archive was built
#' with `fade = FALSE` (disjoint zoom windows, clean breaks) or `fade = TRUE`
#' (overlapping windows, cross-fade) and styles accordingly.
#'
#' The default color scale is a documented quick-look ramp (5 evenly spaced
#' breaks across `1, 10, 100, 1000, 10000`). For production maps, pass an
#' explicit `stops = list(values = ..., colors = ...)` derived from your data.
#' Embedding real aggregation statistics in PMTiles metadata is on the
#' roadmap.
#'
#' @param input Path to a local `.pmtiles` file produced by [freestile_h3()].
#' @param agg_column Character or NULL. Aggregation column to drive the hex
#'   color scale. If `NULL`, the first non-`h3_id` numeric field in the
#'   metadata is used.
#' @param stops List with `values` and `colors` (equal length, sorted by
#'   `values`) defining the shared color scale across all hex layers. If
#'   `NULL`, the documented quick-look default is used.
#' @param palette Character. Named palette used to generate default `colors`
#'   when `stops` is `NULL`. Currently supports `"viridis"` (default),
#'   `"magma"`, `"plasma"`, `"cividis"`, `"inferno"`, `"rocket"`, `"mako"`,
#'   `"turbo"` if `viridisLite` is installed; otherwise falls back to a
#'   five-color blue ramp.
#' @param hex_opacity Numeric. Peak fill opacity for hex layers (default 0.9).
#'   In fade mode this is the opacity at each layer's center zoom.
#' @param point_color Character. Color for the raw-point circles
#'   (default `"#0868ac"`).
#' @param point_radius mapgl expression or NULL. If `NULL`, a zoom-interpolated
#'   default is used.
#' @param background_style mapgl style passed to [mapgl::maplibre()]; if
#'   `NULL` uses `mapgl::maplibre()` defaults.
#' @param hex_layer_prefix Character. Prefix used when the archive was
#'   written. Must match the value passed to [freestile_h3()]; default
#'   `"h3"`.
#' @param point_layer_name Character. Name of the raw-points MVT layer in
#'   the archive. Must match the value passed to [freestile_h3()]; default
#'   `"points"`.
#' @param port Integer. Port for the local PMTiles server (default 8080).
#'
#' @return A `mapgl` map object.
#'
#' @examples
#' \dontrun{
#' view_h3_tiles("wind.pmtiles", agg_column = "n",
#'   stops = list(values = c(1, 10, 100, 1000, 10000),
#'                colors = viridisLite::viridis(5)))
#' }
#'
#' @seealso [freestile_h3()], [view_tiles()]
#' @export
view_h3_tiles <- function(
    input,
    agg_column = NULL,
    stops = NULL,
    palette = "viridis",
    hex_opacity = 0.9,
    point_color = "#0868ac",
    point_radius = NULL,
    background_style = NULL,
    hex_layer_prefix = "h3",
    point_layer_name = "points",
    port = 8080
) {
  if (!requireNamespace("mapgl", quietly = TRUE)) {
    stop(
      "Package 'mapgl' is required for view_h3_tiles().\n",
      "Install it with: install.packages('mapgl')",
      call. = FALSE
    )
  }

  input <- normalizePath(input, mustWork = TRUE)
  meta <- pmtiles_metadata(input)
  if (is.null(meta)) {
    stop("Cannot read PMTiles metadata from: ", input, call. = FALSE)
  }

  layers_info <- meta$metadata$vector_layers
  if (is.null(layers_info) || length(layers_info) == 0L) {
    stop("No vector layers found in PMTiles metadata.", call. = FALSE)
  }

  parsed <- .h3_parse_layers_metadata(layers_info,
    hex_layer_prefix = hex_layer_prefix,
    point_layer_name = point_layer_name)
  hex_layers <- parsed$hex_layers
  point_layer <- parsed$point_layer

  if (length(hex_layers) == 0L && is.null(point_layer)) {
    stop(
      "PMTiles archive does not look like a freestile_h3() output ",
      "(no `<prefix>_r<NN>` layers and no raw-points layer).",
      call. = FALSE
    )
  }

  # If the caller's `point_layer_name` didn't match anything but the archive
  # does have non-hex layers, the most likely cause is a `point_layer_name`
  # mismatch between the freestile_h3() write and this view_h3_tiles() read.
  # Surface that explicitly — silent failure here has cost us before.
  if (is.null(point_layer)) {
    hex_pattern <- paste0("^", .h3_escape_regex(hex_layer_prefix), "_r\\d{2}$")
    other_ids <- vapply(layers_info, function(x) x$id, character(1))
    other_ids <- other_ids[!grepl(hex_pattern, other_ids)]
    if (length(other_ids) > 0L) {
      warning(
        sprintf(
          paste0(
            "view_h3_tiles() didn't find a points layer named '%s'. ",
            "The archive contains non-hex layer(s): %s. ",
            "If freestile_h3() was called with a custom `point_layer_name`, ",
            "pass the same value to view_h3_tiles()."
          ),
          point_layer_name,
          paste(sprintf("'%s'", other_ids), collapse = ", ")
        ),
        call. = FALSE
      )
    }
  }

  # Detect fade vs clean-breaks from overlapping windows
  is_fade <- .h3_detect_fade(hex_layers)

  # Determine agg_column from metadata if not specified
  if (is.null(agg_column) && length(hex_layers) > 0L) {
    fields <- hex_layers[[1L]]$fields
    candidates <- setdiff(names(fields), "h3_id")
    numeric_idx <- vapply(
      candidates,
      function(n) isTRUE(grepl("Number|Integer|Float|Double",
        as.character(fields[[n]]), ignore.case = TRUE)),
      logical(1)
    )
    numeric_candidates <- candidates[numeric_idx]
    if (length(numeric_candidates) > 0L) {
      agg_column <- numeric_candidates[1L]
    } else if (length(candidates) > 0L) {
      agg_column <- candidates[1L]
    }
  }

  if (is.null(stops)) {
    stops <- .h3_default_stops(palette)
  } else {
    if (!is.list(stops) || is.null(stops$values) || is.null(stops$colors) ||
        length(stops$values) != length(stops$colors)) {
      stop(
        "`stops` must be a list with equal-length `values` and `colors`.",
        call. = FALSE
      )
    }
    if (is.unsorted(stops$values, strictly = TRUE)) {
      stop(
        "`stops$values` must be strictly increasing ",
        "(MapLibre interpolate expressions require sorted inputs).",
        call. = FALSE
      )
    }
  }

  serve_tiles(dirname(input), port = port)
  tile_url <- sprintf("http://localhost:%d/%s", port, basename(input))

  if (is.null(background_style)) {
    m <- mapgl::maplibre(
      bounds = c(meta$min_longitude, meta$min_latitude,
                 meta$max_longitude, meta$max_latitude)
    )
  } else {
    m <- mapgl::maplibre(
      style = background_style,
      bounds = c(meta$min_longitude, meta$min_latitude,
                 meta$max_longitude, meta$max_latitude)
    )
  }
  m <- mapgl::add_pmtiles_source(m, id = "src", url = tile_url)

  # Hex layers (fill)
  for (i in seq_along(hex_layers)) {
    li <- hex_layers[[i]]
    fill_color <- if (!is.null(agg_column)) {
      mapgl::interpolate(
        column = agg_column,
        values = stops$values,
        stops = stops$colors
      )
    } else {
      stops$colors[ceiling(length(stops$colors) / 2)]
    }

    fill_opacity <- if (is_fade) {
      .h3_fade_opacity(li$min_zoom, li$max_zoom,
        peak = hex_opacity, overlap = parsed$fade_overlap)
    } else {
      hex_opacity
    }

    outline_alpha <- max(0.1, 0.6 - (i - 1L) * 0.1)
    fill_outline_color <- sprintf("rgba(8, 64, 129, %0.2f)", outline_alpha)

    # MapLibre style `maxzoom` is exclusive ("hidden at zoom >= maxzoom"),
    # but PMTiles vector_layer `maxzoom` is inclusive. Shift by +1 so the
    # layer is actually visible at its top zoom.
    args <- list(
      map = m,
      id = sprintf("hex-%s", li$id),
      source = "src",
      source_layer = li$id,
      fill_color = fill_color,
      fill_opacity = fill_opacity,
      fill_outline_color = fill_outline_color,
      min_zoom = li$min_zoom,
      max_zoom = li$max_zoom + 1L
    )
    m <- do.call(mapgl::add_fill_layer, args)
  }

  # Points (circle)
  if (!is.null(point_layer)) {
    circle_radius <- if (is.null(point_radius)) {
      mapgl::interpolate(
        property = "zoom",
        values = c(point_layer$min_zoom, point_layer$max_zoom),
        stops = c(3, 6)
      )
    } else {
      point_radius
    }

    circle_opacity <- if (is_fade) {
      mapgl::interpolate(
        property = "zoom",
        values = c(point_layer$min_zoom,
                   min(point_layer$min_zoom + parsed$fade_overlap,
                       point_layer$max_zoom)),
        stops = c(0, 1)
      )
    } else {
      1
    }

    m <- mapgl::add_circle_layer(
      map = m,
      id = sprintf("points-%s", point_layer$id),
      source = "src",
      source_layer = point_layer$id,
      circle_color = point_color,
      circle_opacity = circle_opacity,
      circle_radius = circle_radius,
      circle_stroke_color = "#ffffff",
      circle_stroke_width = 1,
      min_zoom = point_layer$min_zoom,
      # See comment above hex layer: maxzoom is exclusive in MapLibre style.
      max_zoom = point_layer$max_zoom + 1L
    )
  }

  m
}


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------

#' Default H3 resolution for a tile zoom
#' @noRd
.h3_default_resolution <- function(zoom) {
  # Lookup matches the table in the freestile_h3 docs.
  tbl <- c(
    `0` = 1L, `1` = 1L,
    `2` = 2L, `3` = 2L,
    `4` = 3L,
    `5` = 4L,
    `6` = 5L, `7` = 5L,
    `8` = 6L,
    `9` = 7L, `10` = 7L,
    `11` = 8L,
    `12` = 9L, `13` = 9L,
    `14` = 10L
  )
  z <- as.integer(zoom)
  out <- integer(length(z))
  for (i in seq_along(z)) {
    key <- as.character(z[i])
    if (key %in% names(tbl)) {
      out[i] <- tbl[[key]]
    } else if (z[i] < 0L) {
      out[i] <- 1L
    } else {
      # Beyond the table: extend linearly, capped at 15.
      out[i] <- min(15L, as.integer(round(z[i] * 0.7)))
    }
  }
  out
}

#' Resolve base_zoom default and validate bounds
#' @noRd
.h3_resolve_base_zoom <- function(base_zoom, min_zoom, max_zoom) {
  if (is.null(base_zoom)) {
    bz <- max(min_zoom, max_zoom - 2L)
  } else {
    bz <- as.integer(base_zoom)
    if (length(bz) != 1L || is.na(bz)) {
      stop("`base_zoom` must be a single integer.", call. = FALSE)
    }
    if (bz < min_zoom || bz > max_zoom) {
      stop(sprintf(
        "`base_zoom` (%d) must satisfy min_zoom (%d) <= base_zoom <= max_zoom (%d).",
        bz, min_zoom, max_zoom
      ), call. = FALSE)
    }
  }
  bz
}

#' Validate and normalize `h3_resolutions` against the hex_zoom range
#'
#' Returns an integer vector of length `length(hex_zooms)`. Errors with
#' actionable messages on invalid input.
#' @noRd
.h3_validate_resolutions <- function(h3_resolutions, hex_zooms) {
  n <- length(hex_zooms)
  if (n == 0L) {
    return(integer(0))
  }

  # Start from defaults.
  res <- .h3_default_resolution(hex_zooms)

  if (is.null(h3_resolutions)) {
    return(res)
  }

  if (!is.numeric(h3_resolutions)) {
    stop("`h3_resolutions` must be NULL or an integer vector.", call. = FALSE)
  }

  # Reject fractional values up front rather than silently truncating via
  # as.integer().
  vals <- unname(h3_resolutions)
  if (any(!is.na(vals) & vals != floor(vals))) {
    stop(sprintf(
      "`h3_resolutions` must be whole numbers in 0..15; got fractional value(s): %s",
      paste(unique(vals[!is.na(vals) & vals != floor(vals)]), collapse = ", ")
    ), call. = FALSE)
  }

  if (is.null(names(h3_resolutions)) || all(names(h3_resolutions) == "")) {
    # Positional override: must be length n
    if (length(h3_resolutions) != n) {
      stop(sprintf(
        "Unnamed `h3_resolutions` must have length %d (one per hex zoom %d..%d); got %d.",
        n, min(hex_zooms), max(hex_zooms), length(h3_resolutions)
      ), call. = FALSE)
    }
    res <- as.integer(h3_resolutions)
  } else {
    # Sparse named override.
    nm <- names(h3_resolutions)
    zoom_keys <- suppressWarnings(as.integer(nm))
    if (any(is.na(zoom_keys))) {
      stop(sprintf(
        "All names in `h3_resolutions` must parse to integer zoom levels; got: %s",
        paste(nm[is.na(zoom_keys)], collapse = ", ")
      ), call. = FALSE)
    }
    out_of_range <- !zoom_keys %in% hex_zooms
    if (any(out_of_range)) {
      stop(sprintf(
        "`h3_resolutions` names must be zooms in [%d, %d]; offending: %s",
        min(hex_zooms), max(hex_zooms),
        paste(nm[out_of_range], collapse = ", ")
      ), call. = FALSE)
    }
    for (k in seq_along(zoom_keys)) {
      idx <- match(zoom_keys[k], hex_zooms)
      res[idx] <- as.integer(h3_resolutions[[k]])
    }
  }

  if (any(is.na(res)) || any(res < 0L) || any(res > 15L)) {
    stop("H3 resolutions must be integers in 0..15.", call. = FALSE)
  }

  res
}

#' Compute per-resolution zoom windows
#'
#' Returns a data.frame with columns `resolution`, `min_zoom`, `max_zoom`,
#' `layer_name`. Errors if the same resolution appears in non-contiguous runs.
#' @noRd
.h3_zoom_windows <- function(
    min_zoom, base_zoom, max_zoom,
    h3_resolutions, fade, fade_overlap, hex_layer_prefix
) {
  hex_zooms <- if (base_zoom > min_zoom) seq.int(min_zoom, base_zoom - 1L) else integer(0)
  if (length(hex_zooms) == 0L) {
    return(data.frame(
      resolution = integer(0),
      min_zoom = integer(0),
      max_zoom = integer(0),
      layer_name = character(0),
      stringsAsFactors = FALSE
    ))
  }

  res <- .h3_validate_resolutions(h3_resolutions, hex_zooms)

  # Detect non-contiguous repeated resolutions (i.e. the same value appearing
  # in two or more separate runs along the zoom axis).
  runs <- rle(res)
  dup_resolutions <- runs$values[duplicated(runs$values)]
  if (length(dup_resolutions) > 0L) {
    examples <- vapply(unique(dup_resolutions), function(r) {
      zs <- hex_zooms[res == r]
      sprintf("res %d at zooms %s", r, paste(zs, collapse = ", "))
    }, character(1))
    stop(
      "`h3_resolutions` produces the same H3 resolution in non-contiguous ",
      "zoom runs, which would create colliding MVT layer names:\n  ",
      paste(examples, collapse = "\n  "),
      "\nFlatten the mapping or pick distinct resolutions.",
      call. = FALSE
    )
  }

  # Build one row per contiguous run.
  out <- data.frame(
    resolution = runs$values,
    stringsAsFactors = FALSE
  )
  starts <- cumsum(c(1L, runs$lengths[-length(runs$lengths)]))
  ends <- cumsum(runs$lengths)
  out$min_zoom <- hex_zooms[starts]
  out$max_zoom <- hex_zooms[ends]

  if (fade) {
    out$min_zoom <- pmax(min_zoom, out$min_zoom - fade_overlap)
    out$max_zoom <- pmin(max_zoom, out$max_zoom + fade_overlap)
  }

  out$layer_name <- sprintf("%s_r%02d", hex_layer_prefix, out$resolution)
  out
}

#' Quote a SQL identifier, escaping embedded double quotes
#' @noRd
.h3_quote_ident <- function(x) {
  sprintf("\"%s\"", gsub("\"", "\"\"", x, fixed = TRUE))
}

#' Parse `agg` argument into SQL fragments
#'
#' Returns a list with `select_clause` (used inside the GROUP BY CTE) and
#' `outer_select` (used in the SELECT that returns geometry + agg cols).
#' @noRd
.h3_parse_agg <- function(agg) {
  fn_map <- c(
    count = "COUNT", sum = "SUM",
    mean = "AVG", avg = "AVG",
    min = "MIN", max = "MAX",
    median = "MEDIAN"
  )

  to_pairs <- function(spec) {
    # Always returns a list with $names and $exprs (character vectors)
    if (is.character(spec) && length(spec) == 1L && spec == "count" &&
        is.null(names(spec))) {
      return(list(names = "count", exprs = "COUNT(*)"))
    }

    if (is.character(spec)) {
      nm <- names(spec)
      if (is.null(nm) || any(nm == "")) {
        stop(
          "Character `agg` must be a named character vector ",
          "(`prop_name = \"sql_expression\"`).",
          call. = FALSE
        )
      }
      return(list(names = nm, exprs = unname(spec)))
    }

    if (is.list(spec)) {
      nm <- names(spec)
      if (is.null(nm) || any(nm == "")) {
        stop("List `agg` must be a named list.", call. = FALSE)
      }
      exprs <- vapply(seq_along(spec), function(i) {
        item <- spec[[i]]
        if (!(is.character(item) && length(item) == 2L)) {
          stop(sprintf(
            "`agg[[%d]]` (%s) must be a character vector of length 2: c(fn, column).",
            i, nm[i]
          ), call. = FALSE)
        }
        fn <- tolower(item[1L])
        col <- item[2L]
        sql_fn <- fn_map[fn]
        if (is.na(sql_fn)) {
          stop(sprintf(
            "Unsupported aggregation function '%s'. Use one of: %s.",
            fn, paste(names(fn_map), collapse = ", ")
          ), call. = FALSE)
        }
        if (fn == "count" && col == "*") {
          "COUNT(*)"
        } else {
          sprintf("%s(%s)", sql_fn, .h3_quote_ident(col))
        }
      }, character(1))
      return(list(names = nm, exprs = exprs))
    }

    stop(
      "`agg` must be \"count\", a named character vector of SQL expressions, ",
      "or a named list of c(fn, column).",
      call. = FALSE
    )
  }

  pairs <- to_pairs(agg)

  # Quote identifiers for SQL.
  quoted_names <- vapply(pairs$names, .h3_quote_ident, character(1))

  select_clause <- paste(
    paste0(pairs$exprs, " AS ", quoted_names),
    collapse = ", "
  )
  outer_select <- paste(quoted_names, collapse = ", ")

  list(
    names = pairs$names,
    select_clause = select_clause,
    outer_select = outer_select
  )
}

#' Open and prepare a DuckDB connection holding `__h3_input`
#'
#' Returns a list with `con` (the open connection) and `points_sf` (an sf
#' object of the raw points, or NULL for SQL input — in which case the caller
#' reads them via `.h3_query_to_sf()`).
#' @noRd
.h3_open_input <- function(input, source_crs, db_path, quiet = FALSE) {
  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE)) {
    stop(
      "freestile_h3() requires the `DBI` and `duckdb` packages.\n",
      "Install with: install.packages(c(\"DBI\", \"duckdb\")).",
      call. = FALSE
    )
  }

  con <- if (is.null(db_path) || identical(db_path, "")) {
    DBI::dbConnect(duckdb::duckdb())
  } else {
    DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  }

  ok <- FALSE
  on.exit(if (!ok) DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "INSTALL spatial; LOAD spatial;")

  tryCatch(
    DBI::dbExecute(con, "INSTALL h3 FROM community; LOAD h3;"),
    error = function(e) {
      stop(
        "Could not load the DuckDB H3 community extension.\n",
        "Original error: ", conditionMessage(e), "\n",
        "Make sure you have network access on first use (DuckDB downloads ",
        "community extensions on demand) and that your DuckDB version ",
        "supports community extensions.",
        call. = FALSE
      )
    }
  )

  points_sf <- NULL

  if (inherits(input, "sf")) {
    if (any(grepl("MULTIPOINT", as.character(sf::st_geometry_type(input)),
        ignore.case = TRUE))) {
      stop(
        "MULTIPOINT input is not supported in this version of freestile_h3(). ",
        "Use sf::st_cast(x, 'POINT') first to explode multi-points.",
        call. = FALSE
      )
    }
    if (!all(grepl("^POINT$", as.character(sf::st_geometry_type(input)),
        ignore.case = TRUE))) {
      stop(
        "freestile_h3() requires POINT geometry. Got: ",
        paste(unique(as.character(sf::st_geometry_type(input))),
          collapse = ", "),
        ".",
        call. = FALSE
      )
    }

    # Auto-transform to WGS84.
    crs <- sf::st_crs(input)
    if (is.na(crs)) {
      warning("Input sf has no CRS. Assuming WGS84 (EPSG:4326).",
        call. = FALSE)
    } else if (!sf::st_is_longlat(input)) {
      if (!quiet) message("  Transforming input to WGS84...")
      input <- sf::st_transform(input, 4326)
    }

    # Drop Z/M
    geom_col_name <- attr(input, "sf_column")
    sfc <- input[[geom_col_name]]
    if (length(sfc) > 0L && class(sfc[[1L]])[1L] != "XY") {
      input <- sf::st_zm(input, drop = TRUE, what = "ZM")
    }

    .h3_write_sf_to_duckdb(con, input)
    points_sf <- input
  } else if (is.character(input) && length(input) == 1L && nzchar(input)) {
    resolved_crs <- source_crs
    if (is.null(resolved_crs) || !nzchar(resolved_crs)) {
      warning(
        "source_crs not provided for SQL input - assuming EPSG:4326. ",
        "If your query returns geometry in another CRS, hex bins will be ",
        "wrong; pass source_crs explicitly to silence this warning.",
        call. = FALSE
      )
      resolved_crs <- "EPSG:4326"
    }

    # Split multi-statement SQL, run setup, keep final SELECT.
    stmts <- strsplit(input, ";", fixed = TRUE)[[1L]]
    stmts <- trimws(stmts)
    stmts <- stmts[nzchar(stmts)]
    if (length(stmts) == 0L) {
      stop("`input` SQL is empty.", call. = FALSE)
    }
    if (length(stmts) > 1L) {
      for (s in stmts[-length(stmts)]) {
        DBI::dbExecute(con, s)
      }
    }
    final_sql <- stmts[length(stmts)]

    # Always normalize the geometry column to `geom`, regardless of CRS path,
    # so downstream H3 SQL can assume the column name.
    DBI::dbExecute(con,
      sprintf("CREATE TEMP VIEW __h3_raw AS (%s)", final_sql))
    desc <- DBI::dbGetQuery(con, "DESCRIBE __h3_raw")
    geom_idx <- grep("^GEOMETRY", desc$column_type, ignore.case = TRUE)
    if (length(geom_idx) == 0L) {
      stop(
        "SQL input does not return a geometry column. ",
        "DuckDB DESCRIBE returned types: ",
        paste(desc$column_type, collapse = ", "),
        call. = FALSE
      )
    }
    geom_col_name <- desc$column_name[geom_idx[1L]]
    geom_select <- if (identical(resolved_crs, "EPSG:4326")) {
      sprintf("\"%s\" AS geom", geom_col_name)
    } else {
      # always_xy = TRUE: force lon/lat (x,y) axis order regardless of CRS
      # authority axis metadata. Without this, e.g. EPSG:3857 round-trips as
      # (lat, lon) and bins land at the wrong hex.
      sprintf("ST_Transform(\"%s\", '%s', 'EPSG:4326', TRUE) AS geom",
        geom_col_name, resolved_crs)
    }
    # EXCLUDE drops the source geometry column before the aliased `geom` is
    # added, so this works whether or not the column is already named "geom".
    DBI::dbExecute(con, sprintf(
      "CREATE TEMP VIEW __h3_input AS SELECT * EXCLUDE (\"%s\"), %s FROM __h3_raw",
      geom_col_name, geom_select
    ))

    # Validate geometry types via a small sample.
    gtypes <- DBI::dbGetQuery(con,
      "SELECT DISTINCT ST_GeometryType(geom) AS gt FROM __h3_input LIMIT 10")
    if (nrow(gtypes) == 0L) {
      stop("SQL input returned no rows.", call. = FALSE)
    }
    gt_clean <- toupper(gsub("^ST_", "", gtypes$gt))
    if (any(gt_clean == "MULTIPOINT")) {
      stop(
        "MULTIPOINT input is not supported in this version of freestile_h3(). ",
        "Cast to POINT in your SQL (e.g. ST_Centroid) or via sf::st_cast.",
        call. = FALSE
      )
    }
    if (!all(gt_clean == "POINT")) {
      stop(
        "freestile_h3() requires POINT geometry. Got: ",
        paste(unique(gtypes$gt), collapse = ", "),
        ".",
        call. = FALSE
      )
    }
  } else {
    stop(
      "`input` must be an sf data frame or a single non-empty SQL string.",
      call. = FALSE
    )
  }

  ok <- TRUE
  list(con = con, points_sf = points_sf)
}

#' Write an sf data frame to a DuckDB temp view named `__h3_input`
#'
#' Uses DBI::dbWriteTable with a list-of-raw WKB column (DuckDB BLOB), then
#' exposes a view that decodes WKB into a `geom` column via ST_GeomFromWKB.
#' @noRd
.h3_write_sf_to_duckdb <- function(con, sf_obj) {
  geom_col_name <- attr(sf_obj, "sf_column")
  attrs <- sf::st_drop_geometry(sf_obj)
  wkb <- unclass(sf::st_as_binary(sf_obj[[geom_col_name]]))
  attributes(wkb) <- NULL

  df <- attrs
  df[["__geom_wkb"]] <- wkb
  DBI::dbWriteTable(con, "__h3_input_raw", df, temporary = TRUE,
    overwrite = TRUE)
  DBI::dbExecute(con,
    "CREATE TEMP VIEW __h3_input AS SELECT * EXCLUDE (\"__geom_wkb\"), ST_GeomFromWKB(\"__geom_wkb\") AS geom FROM __h3_input_raw")
  invisible(NULL)
}

#' Execute a query on the shared DuckDB connection and return an sf
#'
#' The query must select a WKB blob column named `__wkb` plus any number of
#' other attribute columns. Geometry is decoded with CRS EPSG:4326.
#' @noRd
.h3_query_to_sf <- function(con, sql) {
  df <- DBI::dbGetQuery(con, sql)
  if (nrow(df) == 0L) {
    return(sf::st_sf(df, geometry = sf::st_sfc(crs = 4326)))
  }
  wkb_col <- df[["__wkb"]]
  df[["__wkb"]] <- NULL
  geom <- sf::st_as_sfc(wkb_col, crs = 4326)
  sf::st_sf(df, geometry = geom)
}

#' Run the per-resolution H3 aggregation and return an sf
#' @noRd
.h3_aggregate_resolution <- function(con, resolution, agg_spec) {
  resolution <- as.integer(resolution)
  if (length(resolution) != 1L || is.na(resolution) || resolution < 0L ||
      resolution > 15L) {
    stop("H3 resolution must be a single integer in 0..15.", call. = FALSE)
  }

  sql <- sprintf(
    paste0(
      "WITH cells AS (\n",
      "  SELECT h3_latlng_to_cell(ST_Y(geom), ST_X(geom), %d) AS h3, %s\n",
      "  FROM __h3_input\n",
      "  GROUP BY h3\n",
      ")\n",
      "SELECT\n",
      "  ST_AsWKB(ST_GeomFromText(h3_cell_to_boundary_wkt(h3))) AS __wkb,\n",
      "  h3_h3_to_string(h3) AS h3_id,\n",
      "  %s\n",
      "FROM cells"
    ),
    resolution,
    agg_spec$select_clause,
    agg_spec$outer_select
  )

  .h3_split_antimeridian(.h3_query_to_sf(con, sql))
}

#' Split hex polygons that cross the antimeridian
#'
#' `h3_cell_to_boundary_wkt()` returns raw cell boundaries: a cell that
#' crosses the antimeridian has vertex longitudes jumping between ~+180 and
#' ~-180, which planar tiling renders as a world-spanning sliver. Detect
#' affected polygons by longitude span, shift their negative longitudes by
#' +360 so the ring is contiguous, and split at the dateline with
#' [sf::st_wrap_dateline()]. Cells containing a pole get the same treatment
#' and render approximately (the polar cap itself is not filled).
#' @noRd
.h3_split_antimeridian <- function(hex_sf) {
  geom <- sf::st_geometry(hex_sf)
  if (length(geom) == 0L) {
    return(hex_sf)
  }

  coords <- sf::st_coordinates(geom)
  spans <- tapply(coords[, "X"], coords[, "L2"], function(x) max(x) - min(x))
  crossing <- as.integer(names(spans)[spans > 180])
  if (length(crossing) == 0L) {
    return(hex_sf)
  }

  shifted <- lapply(crossing, function(i) {
    ring <- unclass(geom[[i]])[[1L]]
    ring[, 1L] <- ifelse(ring[, 1L] < 0, ring[, 1L] + 360, ring[, 1L])
    sf::st_polygon(list(ring))
  })
  wrapped <- sf::st_wrap_dateline(
    sf::st_sfc(shifted, crs = sf::st_crs(geom))
  )

  glist <- unclass(geom)
  for (k in seq_along(crossing)) {
    glist[[crossing[k]]] <- wrapped[[k]]
  }
  new_geom <- sf::st_cast(
    sf::st_sfc(glist, crs = sf::st_crs(geom)),
    "MULTIPOLYGON"
  )
  sf::st_geometry(hex_sf) <- new_geom
  hex_sf
}

#' Inspect PMTiles vector_layers metadata for h3 layers + points layer
#'
#' Uses the caller-supplied `hex_layer_prefix` / `point_layer_name` so it
#' matches whatever names `freestile_h3()` actually wrote (the prefix is
#' user-controllable and may contain regex-special characters).
#' @noRd
.h3_parse_layers_metadata <- function(layers_info,
                                      hex_layer_prefix = "h3",
                                      point_layer_name = "points") {
  hex_pattern <- paste0("^", .h3_escape_regex(hex_layer_prefix), "_r\\d{2}$")
  hex_layers <- list()
  point_layer <- NULL
  for (li in layers_info) {
    id <- li$id
    if (grepl(hex_pattern, id)) {
      hex_layers[[length(hex_layers) + 1L]] <- list(
        id = id,
        min_zoom = li$minzoom %||% li$min_zoom %||% 0L,
        max_zoom = li$maxzoom %||% li$max_zoom %||% 22L,
        fields = li$fields
      )
    } else if (identical(id, point_layer_name)) {
      point_layer <- list(
        id = id,
        min_zoom = li$minzoom %||% li$min_zoom %||% 0L,
        max_zoom = li$maxzoom %||% li$max_zoom %||% 22L,
        fields = li$fields
      )
    }
  }

  # Sort hex layers by min_zoom ascending so style order is deterministic.
  if (length(hex_layers) > 1L) {
    order_idx <- order(vapply(hex_layers, function(x) x$min_zoom, numeric(1)))
    hex_layers <- hex_layers[order_idx]
  }

  fade_overlap <- 0L
  if (length(hex_layers) > 1L) {
    overlaps <- integer(length(hex_layers) - 1L)
    for (i in seq_len(length(hex_layers) - 1L)) {
      gap <- hex_layers[[i]]$max_zoom - hex_layers[[i + 1L]]$min_zoom
      overlaps[i] <- max(0L, as.integer(gap))
    }
    fade_overlap <- max(overlaps)
  }

  list(
    hex_layers = hex_layers,
    point_layer = point_layer,
    fade_overlap = fade_overlap
  )
}

`%||%` <- function(a, b) if (is.null(a)) b else a

#' Escape regex metacharacters in an arbitrary user string
#' @noRd
.h3_escape_regex <- function(s) {
  gsub("([\\.|()\\[\\]{}^$*+?\\\\])", "\\\\\\1", s, perl = TRUE)
}

#' Decide whether the archive is using fade (overlapping zoom windows)
#' @noRd
.h3_detect_fade <- function(hex_layers) {
  if (length(hex_layers) < 2L) return(FALSE)
  for (i in seq_len(length(hex_layers) - 1L)) {
    if (hex_layers[[i]]$max_zoom >= hex_layers[[i + 1L]]$min_zoom) {
      return(TRUE)
    }
  }
  FALSE
}

#' Trapezoid (or triangle, for narrow windows) fill_opacity envelope
#'
#' Returns a `mapgl::interpolate(...)` expression whose `values` are strictly
#' increasing zoom inputs and whose `stops` describe a fade-in / hold /
#' fade-out shape across `[min_zoom, max_zoom]`.
#'
#' MapLibre's `interpolate` requires strictly increasing input values. A
#' naive trapezoid with `min_zoom + overlap` and `max_zoom - overlap` as
#' the plateau edges collapses (lo_mid == hi_mid) for any window narrower
#' than `2 * overlap`, producing duplicate inputs that MapLibre silently
#' rejects. Detect that case and emit a triangle envelope instead.
#' @noRd
.h3_fade_opacity <- function(min_zoom, max_zoom, peak, overlap) {
  overlap <- max(1L, as.integer(overlap))
  span <- max_zoom - min_zoom
  if (span <= 0) {
    # Degenerate single-zoom window — nothing to fade across; hold peak.
    return(peak)
  }

  lo_mid <- min_zoom + overlap
  hi_mid <- max_zoom - overlap

  if (hi_mid <= lo_mid) {
    # Window too narrow for both fade-in and fade-out at the requested
    # overlap. Use a triangle envelope peaking at the window center —
    # strictly increasing inputs are guaranteed by the +/- 0.5 around mid.
    mid <- (min_zoom + max_zoom) / 2
    return(mapgl::interpolate(
      property = "zoom",
      values = c(min_zoom, mid, max_zoom),
      stops = c(0, peak, 0)
    ))
  }

  mapgl::interpolate(
    property = "zoom",
    values = c(min_zoom, lo_mid, hi_mid, max_zoom),
    stops = c(0, peak, peak, 0)
  )
}

#' Quick-look default color stops
#' @noRd
.h3_default_stops <- function(palette) {
  palette <- match.arg(
    palette,
    c("viridis", "magma", "plasma", "cividis", "inferno",
      "rocket", "mako", "turbo")
  )
  values <- c(1, 10, 100, 1000, 10000)
  colors <- if (requireNamespace("viridisLite", quietly = TRUE)) {
    fn <- switch(palette,
      viridis = viridisLite::viridis,
      magma = viridisLite::magma,
      plasma = viridisLite::plasma,
      cividis = viridisLite::cividis,
      inferno = viridisLite::inferno,
      rocket = viridisLite::rocket,
      mako = viridisLite::mako,
      turbo = viridisLite::turbo
    )
    fn(5)
  } else {
    c("#f1eef6", "#bdc9e1", "#74a9cf", "#2b8cbe", "#045a8d")
  }
  list(values = values, colors = colors)
}
