test_that("freestile creates MVT PMTiles from sf polygons", {
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
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("freestile auto-reprojects non-WGS84 input to EPSG:4326", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  # Transform to a projected CRS (UTM 17N)
  nc_utm <- sf::st_transform(nc, 32617)
  expect_false(sf::st_is_longlat(nc_utm))

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile(
    nc_utm,
    output,
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)

  # Verify the tiles have correct WGS84 bounds
  meta <- pmtiles_metadata(output)
  expect_true(meta$min_longitude > -85 && meta$min_longitude < -75)
  expect_true(meta$min_latitude > 33 && meta$min_latitude < 37)
})

test_that("freestile warns when input has no CRS", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )
  sf::st_crs(nc) <- NA

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  expect_warning(
    freestile(nc, output, layer_name = "counties", tile_format = "mvt",
      min_zoom = 0, max_zoom = 6, quiet = TRUE),
    "no CRS"
  )
  expect_true(file.exists(output))
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
