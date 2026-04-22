test_that("freestile with coalesce produces output for polygons", {
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
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    coalesce = TRUE,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("coalesce produces smaller or equal output", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output_no <- tempfile(fileext = ".pmtiles")
  output_yes <- tempfile(fileext = ".pmtiles")
  on.exit({
    unlink(output_no)
    unlink(output_yes)
  }, add = TRUE)

  freestile(nc, output_no, layer_name = "nc", tile_format = "mvt",
    min_zoom = 0, max_zoom = 6, quiet = TRUE)
  freestile(nc, output_yes, layer_name = "nc", tile_format = "mvt",
    min_zoom = 0, max_zoom = 6, coalesce = TRUE, quiet = TRUE)

  size_no <- file.info(output_no)$size
  size_yes <- file.info(output_yes)$size

  # Coalescing may produce smaller output (or same if no merges happen)
  expect_true(size_yes <= size_no * 1.05) # allow tiny overhead
})

test_that("coalesce works with MLT format", {
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
    coalesce = TRUE,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("all features combined: drop + cluster + coalesce", {
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
    max_zoom = 8,
    drop_rate = 2.5,
    cluster_distance = 40,
    cluster_maxzoom = 6,
    coalesce = TRUE,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})
