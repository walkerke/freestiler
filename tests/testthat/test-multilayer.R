test_that("freestile creates multi-layer MVT PMTiles", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )
  pts <- sf::st_centroid(nc)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    list(counties = nc, centroids = pts),
    output,
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("freestile creates multi-layer MLT PMTiles", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )
  pts <- sf::st_centroid(nc)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    list(counties = nc, centroids = pts),
    output,
    tile_format = "mlt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("freestile_layer sets per-layer zoom range", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )
  pts <- sf::st_centroid(nc)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    list(
      counties = freestile_layer(nc, min_zoom = 0, max_zoom = 6),
      centroids = freestile_layer(pts, min_zoom = 4, max_zoom = 10)
    ),
    output,
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 10,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("freestile_layer validates input", {
  expect_error(freestile_layer(data.frame(x = 1)), "must be an sf object")
})

test_that("multi-layer input requires named list", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  expect_error(
    freestile(list(nc, nc), output, quiet = TRUE),
    "named list"
  )
})

test_that("base_zoom with multilayer respects per-layer max_zoom", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  # Layer ending at z6 with global max z10 and drop_rate.
  # When base_zoom=NULL, it defaults to the LAYER's max_zoom (6).
  # The drop curve is computed relative to base_z: threshold = drop_rate^(base_z - zoom).
  # So at z0: drop_rate^(6-0) = 2.5^6 ≈ 244, keeping ~1/244 of features.
  # Dropping still occurs at z0-5, but the curve scales to z6 (not z10).
  output_layer_bz <- tempfile(fileext = ".pmtiles")
  output_explicit_bz <- tempfile(fileext = ".pmtiles")
  on.exit({
    unlink(output_layer_bz)
    unlink(output_explicit_bz)
  }, add = TRUE)

  # base_zoom=NULL: defaults to layer max_zoom (6)
  freestile(
    list(counties = freestile_layer(nc, min_zoom = 0, max_zoom = 6)),
    output_layer_bz,
    tile_format = "mvt",
    min_zoom = 0, max_zoom = 10,
    drop_rate = 2.5,
    quiet = TRUE
  )

  # Explicit base_zoom=3: gentler overall — only z0-2 get thinned (vs z0-5
  # with base_z=6), and the curve is shallower (drop_rate^3 vs drop_rate^6)
  freestile(
    list(counties = freestile_layer(nc, min_zoom = 0, max_zoom = 6)),
    output_explicit_bz,
    tile_format = "mvt",
    min_zoom = 0, max_zoom = 10,
    drop_rate = 2.5,
    base_zoom = 3,
    quiet = TRUE
  )

  size_layer_bz <- file.info(output_layer_bz)$size
  size_explicit_bz <- file.info(output_explicit_bz)$size

  # base_zoom=3 has a gentler curve (drop_rate^3 vs drop_rate^6 at z0)
  # and fewer zoom levels with dropping (z0-2 vs z0-5), so output is larger
  expect_true(size_explicit_bz >= size_layer_bz)
})

test_that("single sf input still works (backward compat)", {
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
    layer_name = "test",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 4,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})
