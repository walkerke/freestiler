test_that("freestile with clustering produces output", {
  skip_on_cran()
  skip_if_not_installed("sf")

  pts <- sf::st_as_sf(
    data.frame(
      name = paste0("P", 1:20),
      value = runif(20),
      lon = c(runif(10, -80, -79.5), runif(10, -75, -74.5)),
      lat = c(runif(10, 35, 35.5), runif(10, 38, 38.5))
    ),
    coords = c("lon", "lat"),
    crs = 4326
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    pts,
    output,
    layer_name = "clusters",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 10,
    cluster_distance = 50,
    cluster_maxzoom = 8,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("clustering with MLT format works", {
  skip_on_cran()
  skip_if_not_installed("sf")

  pts <- sf::st_as_sf(
    data.frame(
      id = 1:10,
      lon = runif(10, -80, -75),
      lat = runif(10, 35, 40)
    ),
    coords = c("lon", "lat"),
    crs = 4326
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    pts,
    output,
    layer_name = "pts",
    tile_format = "mlt",
    min_zoom = 0,
    max_zoom = 8,
    cluster_distance = 40,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("clustering does not apply to polygon layers", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  # cluster_distance is set but should be ignored for polygons
  result <- freestile(
    nc,
    output,
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    cluster_distance = 50,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})
