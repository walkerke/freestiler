test_that("freestile creates MLT PMTiles from sf polygons", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    nc,
    output,
    layer_name = "counties",
    tile_format = "mlt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("MLT produces smaller output than MVT for polygon data", {
  skip_on_cran()
  skip_if_not_installed("sf")

  # nc has a mix of POLYGON and MULTIPOLYGON, so this is a coarse size
  # regression check for the MLT columnar format, not a targeted test
  # for geometry-type RLE (which only activates on uniform-type tiles).
  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output_mlt <- tempfile(fileext = ".pmtiles")
  output_mvt <- tempfile(fileext = ".pmtiles")
  on.exit({
    unlink(output_mlt)
    unlink(output_mvt)
  }, add = TRUE)

  freestile(nc, output_mlt, layer_name = "nc", tile_format = "mlt",
    min_zoom = 0, max_zoom = 6, quiet = TRUE)
  freestile(nc, output_mvt, layer_name = "nc", tile_format = "mvt",
    min_zoom = 0, max_zoom = 6, quiet = TRUE)

  size_mlt <- file.info(output_mlt)$size
  size_mvt <- file.info(output_mvt)$size

  # MLT with RLE on uniform geometry types + topology should be smaller
  expect_true(size_mlt < size_mvt)
})

test_that("MLT handles dictionary-encodable string columns", {
  skip_on_cran()
  skip_if_not_installed("sf")

  # Create data with low-cardinality strings (good for dictionary encoding)
  pts <- sf::st_as_sf(
    data.frame(
      category = rep(c("urban", "rural", "suburban"), each = 10),
      value = 1:30,
      lon = runif(30, -80, -75),
      lat = runif(30, 35, 40)
    ),
    coords = c("lon", "lat"),
    crs = 4326
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(pts, output, layer_name = "pts", tile_format = "mlt",
    min_zoom = 0, max_zoom = 8, quiet = TRUE)

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("MLT handles mixed geometry types (no RLE on geom types)", {
  skip_on_cran()
  skip_if_not_installed("sf")

  # Create a layer with mixed polygons and multipolygons
  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  # nc has both POLYGON and MULTIPOLYGON, so geometry-type RLE won't activate
  result <- freestile(nc, output, layer_name = "nc", tile_format = "mlt",
    min_zoom = 4, max_zoom = 6, quiet = TRUE)

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("freestile creates MLT PMTiles from sf points", {
  skip_on_cran()
  skip_if_not_installed("sf")

  pts <- sf::st_as_sf(
    data.frame(
      name = c("A", "B", "C"),
      value = c(1.5, 2.5, 3.5),
      lon = c(-80, -79, -78),
      lat = c(35, 36, 37)
    ),
    coords = c("lon", "lat"),
    crs = 4326
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    pts,
    output,
    layer_name = "points",
    tile_format = "mlt",
    min_zoom = 0,
    max_zoom = 8,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})
