test_that("freestile creates MVT PMTiles from sf polygons", {
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
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("freestile creates MVT PMTiles from sf points", {
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
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 8,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})
