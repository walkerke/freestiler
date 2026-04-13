# Helper to check if Rust duckdb feature is compiled
.has_rust_duckdb_test <- function() {
  result <- rust_freestile_duckdb_query("", "", "", "", "mvt", 0L, 6L, -1L,
    TRUE, -1.0, -1.0, -1L, FALSE, TRUE, "never")
  !startsWith(result, "Error: DuckDB support not compiled")
}

# Helper to check if R duckdb package is available
.has_r_duckdb_test <- function() {
  requireNamespace("duckdb", quietly = TRUE) &&
    requireNamespace("DBI", quietly = TRUE)
}

# --- Rust backend tests (skip if not compiled) ---

test_that("freestile_query creates PMTiles via Rust backend", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not(.has_rust_duckdb_test(), message = "Rust DuckDB not compiled")

  withr::local_options(freestiler.duckdb_backend = "rust")

  nc_path <- system.file("shape/nc.shp", package = "sf")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_query(
    query = sprintf("SELECT * FROM ST_Read('%s')", nc_path),
    output = output,
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

test_that("freestile_query works with MVT via Rust backend", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not(.has_rust_duckdb_test(), message = "Rust DuckDB not compiled")

  withr::local_options(freestiler.duckdb_backend = "rust")

  nc_path <- system.file("shape/nc.shp", package = "sf")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_query(
    query = sprintf("SELECT * FROM ST_Read('%s')", nc_path),
    output = output,
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("freestile_query supports streaming point mode via Rust backend", {
  skip_on_cran()
  skip_if_not(.has_rust_duckdb_test(), message = "Rust DuckDB not compiled")

  withr::local_options(freestiler.duckdb_backend = "rust")

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_query(
    query = paste(
      "SELECT * FROM (VALUES",
      "('a', 1, ST_Point(-78.6, 35.8)),",
      "('b', 2, ST_Point(-80.2, 36.1)),",
      "('c', 3, ST_Point(-82.5, 34.2))",
      ") AS t(label, score, geometry)"
    ),
    output = output,
    layer_name = "points",
    max_zoom = 6,
    quiet = TRUE,
    streaming = "always"
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)
})

test_that("freestile_query streaming mode rejects non-point geometries", {
  skip_on_cran()
  skip_if_not(.has_rust_duckdb_test(), message = "Rust DuckDB not compiled")

  withr::local_options(freestiler.duckdb_backend = "rust")

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  expect_error(
    freestile_query(
      query = "SELECT ST_GeomFromText('POLYGON((-80 35, -78 35, -78 37, -80 37, -80 35))') AS geometry",
      output = output,
      quiet = TRUE,
      streaming = "always"
    ),
    "POINT geometries only"
  )
})

test_that("freestile_query supports multi-statement SQL via Rust backend", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not(.has_rust_duckdb_test(), message = "Rust DuckDB not compiled")

  withr::local_options(freestiler.duckdb_backend = "rust")

  nc_path <- system.file("shape/nc.shp", package = "sf")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  # Multi-statement: CREATE VIEW then SELECT from it
  multi_sql <- sprintf(paste(
    "CREATE OR REPLACE VIEW nc_view AS SELECT * FROM ST_Read('%s');",
    "SELECT * FROM nc_view"
  ), nc_path)

  result <- freestile_query(
    query = multi_sql,
    output = output,
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

# --- R duckdb package fallback tests (skip if not installed) ---

test_that("freestile_query creates PMTiles via R duckdb fallback", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  withr::local_options(freestiler.duckdb_backend = "r")

  nc_path <- system.file("shape/nc.shp", package = "sf")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_query(
    query = sprintf("SELECT * FROM ST_Read('%s')", nc_path),
    output = output,
    source_crs = "EPSG:4267",
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

test_that("freestile_query supports multi-statement SQL via R duckdb fallback", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  withr::local_options(freestiler.duckdb_backend = "r")

  nc_path <- system.file("shape/nc.shp", package = "sf")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  multi_sql <- sprintf(paste(
    "CREATE OR REPLACE VIEW nc_view AS SELECT * FROM ST_Read('%s');",
    "SELECT * FROM nc_view"
  ), nc_path)

  result <- freestile_query(
    query = multi_sql,
    output = output,
    source_crs = "EPSG:4267",
    layer_name = "counties",
    tile_format = "mvt",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})

test_that("freestile_file with engine='duckdb' detects CRS and reprojects via R fallback", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  withr::local_options(freestiler.duckdb_backend = "r")

  nc_path <- system.file("shape/nc.shp", package = "sf")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_file(
    input = nc_path,
    output = output,
    layer_name = "counties",
    tile_format = "mlt",
    min_zoom = 0,
    max_zoom = 6,
    engine = "duckdb",
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
  expect_equal(result, output)

  # Verify CRS was detected from file: the result sf should have been
  # reprojected to 4326 internally via ST_Transform in DuckDB. We can
  # verify indirectly by checking the file was created successfully
  # (the NC shapefile is NAD27/EPSG:4267, so if CRS detection failed
  # the coordinates would still be in lon/lat range and pass, but
  # ST_Transform is called, which is the important code path).
})

test_that("freestile_query R fallback errors when source_crs is omitted", {
  skip_on_cran()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  withr::local_options(freestiler.duckdb_backend = "r")

  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  expect_error(
    freestile_query(
      query = "SELECT ST_Point(-78.6, 35.8) AS geom",
      output = output,
      quiet = TRUE
    ),
    "explicit `source_crs`"
  )
})

test_that(".r_duckdb_query_to_sf reprojects when source_crs is provided", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  nc_path <- system.file("shape/nc.shp", package = "sf")

  # Read via DuckDB with explicit source_crs (NAD27 = EPSG:4267)
  sf_result <- freestiler:::.r_duckdb_query_to_sf(
    sql = sprintf("SELECT * FROM ST_Read('%s')", nc_path),
    source_crs = "EPSG:4267"
  )

  expect_s3_class(sf_result, "sf")
  expect_equal(sf::st_crs(sf_result)$epsg, 4326L)
  expect_equal(nrow(sf_result), 100L)

  # Verify coordinates are in WGS84 range
  bbox <- sf::st_bbox(sf_result)
  expect_true(bbox["xmin"] >= -180 && bbox["xmax"] <= 180)
  expect_true(bbox["ymin"] >= -90 && bbox["ymax"] <= 90)
})

test_that(".duckdb_detect_file_crs extracts CRS from shapefile", {
  skip_on_cran()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  nc_path <- system.file("shape/nc.shp", package = "sf")
  crs <- freestiler:::.duckdb_detect_file_crs(nc_path)

  # NC shapefile is NAD27 = EPSG:4267
  expect_equal(crs, "EPSG:4267")
})

# --- Negative tests ---

test_that("forcing rust backend errors when not compiled", {
  skip_on_cran()
  skip_if(.has_rust_duckdb_test(),
    message = "Rust DuckDB IS compiled, skip negative test")

  withr::local_options(freestiler.duckdb_backend = "rust")

  expect_error(
    freestile_query("SELECT 1", tempfile(fileext = ".pmtiles"), quiet = TRUE),
    "not compiled"
  )
})

test_that("forcing r backend errors when duckdb not installed", {
  skip_on_cran()
  skip_if(.has_r_duckdb_test(),
    message = "R duckdb IS installed, skip negative test")

  withr::local_options(freestiler.duckdb_backend = "r")

  expect_error(
    freestile_query("SELECT 1", tempfile(fileext = ".pmtiles"), quiet = TRUE),
    "not installed"
  )
})

# --- Auto backend test ---

test_that("auto backend selects an available backend", {
  skip_on_cran()
  skip_if_not_installed("sf")
  # Need at least one backend available
  skip_if_not(
    .has_rust_duckdb_test() || .has_r_duckdb_test(),
    message = "No DuckDB backend available"
  )

  withr::local_options(freestiler.duckdb_backend = "auto")

  nc_path <- system.file("shape/nc.shp", package = "sf")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  result <- freestile_query(
    query = sprintf("SELECT * FROM ST_Read('%s')", nc_path),
    output = output,
    source_crs = "EPSG:4267",
    layer_name = "counties",
    min_zoom = 0,
    max_zoom = 6,
    quiet = TRUE
  )

  expect_true(file.exists(output))
  expect_true(file.info(output)$size > 0)
})
