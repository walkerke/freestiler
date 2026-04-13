# Helper to check if geoparquet feature is compiled
.has_geoparquet <- function() {
  result <- rust_freestile_file("", "", "", "mvt", 0L, 6L, -1L, TRUE,
    -1.0, -1.0, -1L, FALSE, TRUE)
  !startsWith(result, "Error: GeoParquet support not compiled")
}

# Write an sf object as GeoParquet using arrow (geometry as WKB binary column)
.write_test_geoparquet <- function(sf_obj, path) {
  attrs <- sf::st_drop_geometry(sf_obj)
  wkb_raw <- lapply(sf::st_as_binary(sf::st_geometry(sf_obj)), unclass)
  geom_array <- arrow::Array$create(wkb_raw, type = arrow::binary())
  tbl <- do.call(arrow::arrow_table, c(as.list(attrs), list(geometry = geom_array)))
  arrow::write_parquet(tbl, path)
}

test_that("freestile_file creates PMTiles from GeoParquet", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("arrow")
  skip_if_not(.has_geoparquet(), message = "GeoParquet feature not compiled")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  parquet_path <- tempfile(fileext = ".parquet")
  on.exit(unlink(parquet_path), add = TRUE)
  .write_test_geoparquet(nc, parquet_path)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_file(
    parquet_path,
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

test_that("freestile_file works with MVT format", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("arrow")
  skip_if_not(.has_geoparquet(), message = "GeoParquet feature not compiled")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  parquet_path <- tempfile(fileext = ".parquet")
  on.exit(unlink(parquet_path), add = TRUE)
  .write_test_geoparquet(nc, parquet_path)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_file(
    parquet_path,
    output,
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("freestile_file auto-reprojects non-WGS84 GeoParquet via sf fallback", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("arrow")
  skip_if_not(.has_geoparquet(), message = "GeoParquet feature not compiled")

  nc <- sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  )

  # Write GeoParquet in a projected CRS (UTM 17N)
  nc_utm <- sf::st_transform(nc, 32617)

  parquet_path <- tempfile(fileext = ".parquet")
  on.exit(unlink(parquet_path), add = TRUE)
  .write_test_geoparquet(nc_utm, parquet_path)

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  # Should fall back to sf reprojection instead of erroring
  result <- freestile_file(
    parquet_path,
    output,
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("freestile_file errors on missing file", {
  skip_on_cran()
  skip_if_not(.has_geoparquet(), message = "GeoParquet feature not compiled")

  expect_error(
    freestile_file("/nonexistent/file.parquet", tempfile(fileext = ".pmtiles"),
      quiet = TRUE)
  )
})

test_that("freestile_file returns error without geoparquet feature", {
  skip_on_cran()
  skip_if(.has_geoparquet(),
    message = "GeoParquet feature IS compiled, skip negative test")

  tmp_parquet <- tempfile(fileext = ".parquet")
  file.create(tmp_parquet)
  expect_error(
    freestile_file(tmp_parquet,
      tempfile(fileext = ".pmtiles"), quiet = TRUE),
    "not compiled"
  )
})
