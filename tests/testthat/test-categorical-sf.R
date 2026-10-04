.cluster_sf_points <- function() {
  sf::st_as_sf(data.frame(
    group = c(1, 2, NA, 99), label = c("a", "b", "c", "d"),
    x = c(-97, -97.00001, 120, 120.00001), y = c(32, 32.00001, -30, -30.00001)
  ), coords = c("x", "y"), crs = 4326)
}

test_that("sf categorical clustering works without native file features", {
  skip_if_not_installed("jsonlite")
  points <- .cluster_sf_points()
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)
  for (format in c("mvt", "mlt")) {
    result <- freestile(sf::st_transform(points, 3857), output,
      layer_name = "plants", max_zoom = 3, cluster_maxzoom = 1,
      cluster_distance = 60, category = "group", category_values = 1:2,
      tile_format = format, quiet = TRUE)
    expect_identical(normalizePath(result), normalizePath(output))
    m <- pmtiles_metadata(output)
    expect_equal(m$max_zoom, 3)
    expect_equal(m$tile_format, format)
    a <- m$metadata$freestiler
    expect_equal(a$category_totals, list(1, 1, 2))
    expect_equal(a$cluster_maxzoom, 1)
    expect_equal(vapply(a$levels, `[[`, numeric(1), "people"), rep(4, 4))
    expect_equal(vapply(a$levels[1:2], `[[`, numeric(1), "singletons"), c(4, 4))
    expect_true(a$levels[[3]]$singletons < 4)
    expect_equal(m$min_longitude, -97.00001, tolerance = 1e-6)
  }
  # Defaults include the individual-point zoom, and min_points is honored.
  freestile(points, output, max_zoom = 2, cluster_distance = 60,
    category = "group", category_values = 1:2, cluster_min_points = 5, quiet = TRUE)
  levels <- pmtiles_metadata(output)$metadata$freestiler$levels
  expect_equal(vapply(levels, `[[`, numeric(1), "singletons"), rep(4, 3))
})

test_that("invalid sf category requests preserve existing output", {
  skip_if_not_installed("jsonlite")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)
  writeBin(charToRaw("previous"), output)
  args <- list(input = .cluster_sf_points(), output = output,
    max_zoom = 2, cluster_distance = 60, category = "group", category_values = 1:2,
    quiet = TRUE)
  call_with <- function(changes) {
    changed <- args
    changed[names(changes)] <- changes
    do.call(freestile, changed)
  }
  cases <- list(
    list(list(cluster_maxzoom = 3), "cluster_maxzoom"),
    list(list(category_values = c(1, 1)), "distinct"),
    list(list(category = "absent"), "not found"),
    list(list(cluster_distance = NaN), "positive"),
    list(list(drop_rate = 2), "conserves"),
    list(list(base_zoom = 2), "base_zoom"),
    list(list(simplification = FALSE), "simplification"),
    list(list(input = list(plants = .cluster_sf_points())), "single POINT"),
    list(list(overwrite = FALSE), "already exists")
  )
  for (case in cases) {
    expect_error(call_with(case[[1]]), case[[2]])
    expect_identical(readBin(output, "raw", n = 8), charToRaw("previous"))
  }
  points <- args$input
  points$cluster_id <- 1:4
  expect_error(call_with(list(input = points)), "collide")
  points <- args$input
  sf::st_geometry(points)[[1]] <- sf::st_point()
  expect_error(call_with(list(input = points)), "non-empty POINT")
  expect_identical(readBin(output, "raw", n = 8), charToRaw("previous"))
})
