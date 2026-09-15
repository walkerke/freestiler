# Overwrite semantics: outputs are written atomically via a temp file +
# rename, existing outputs are replaced only on success, and a failed run
# never clobbers an existing archive.

.read_nc <- function() {
  sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
}

test_that("freestile replaces an existing output when overwrite = TRUE", {
  skip_on_cran()
  skip_if_not_installed("sf")

  out <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(out), add = TRUE)
  writeBin(charToRaw("OLD CONTENT"), out)

  freestile(.read_nc(), out, min_zoom = 0, max_zoom = 2, quiet = TRUE)

  expect_gt(file.size(out), 1000)
  meta <- pmtiles_metadata(out)
  expect_true(is.list(meta))
})

test_that("freestile refuses to overwrite when overwrite = FALSE", {
  skip_on_cran()
  skip_if_not_installed("sf")

  out <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(out), add = TRUE)
  writeBin(charToRaw("KEEP"), out)

  expect_error(
    freestile(.read_nc(), out, min_zoom = 0, max_zoom = 2,
      overwrite = FALSE, quiet = TRUE),
    "already exists"
  )
  expect_identical(readBin(out, "raw", 4L), charToRaw("KEEP"))
})

test_that("a failed run preserves an existing output", {
  skip_on_cran()

  has_geoparquet <- !startsWith(
    rust_freestile_file("", "", "", "mvt", 0L, 6L, -1L, TRUE,
      -1.0, -1.0, -1L, FALSE, TRUE),
    "Error: GeoParquet support not compiled"
  )
  skip_if_not(has_geoparquet, message = "GeoParquet feature not compiled")

  out <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(out), add = TRUE)
  writeBin(charToRaw("KEEP"), out)

  bad_parquet <- tempfile(fileext = ".parquet")
  on.exit(unlink(bad_parquet), add = TRUE)
  writeBin(charToRaw("this is not a parquet file"), bad_parquet)

  expect_error(
    freestile_file(bad_parquet, out, layer_name = "x",
      min_zoom = 0, max_zoom = 2, quiet = TRUE)
  )
  expect_identical(readBin(out, "raw", 4L), charToRaw("KEEP"))
})

test_that("freestile_file (geoparquet) replaces an existing output", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("arrow")

  has_geoparquet <- !startsWith(
    rust_freestile_file("", "", "", "mvt", 0L, 6L, -1L, TRUE,
      -1.0, -1.0, -1L, FALSE, TRUE),
    "Error: GeoParquet support not compiled"
  )
  skip_if_not(has_geoparquet, message = "GeoParquet feature not compiled")

  nc <- .read_nc()
  parquet_path <- tempfile(fileext = ".parquet")
  on.exit(unlink(parquet_path), add = TRUE)
  attrs <- sf::st_drop_geometry(nc)
  wkb_raw <- lapply(sf::st_as_binary(sf::st_geometry(nc)), unclass)
  geom_array <- arrow::Array$create(wkb_raw, type = arrow::binary())
  tbl <- do.call(arrow::arrow_table, c(as.list(attrs), list(geometry = geom_array)))
  arrow::write_parquet(tbl, parquet_path)

  out <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(out), add = TRUE)
  writeBin(charToRaw("OLD CONTENT"), out)

  freestile_file(parquet_path, out, layer_name = "counties",
    min_zoom = 0, max_zoom = 2, quiet = TRUE)

  expect_gt(file.size(out), 1000)
  meta <- pmtiles_metadata(out)
  expect_true(is.list(meta))
})
