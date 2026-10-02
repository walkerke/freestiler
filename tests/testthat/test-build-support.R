.expect_runiverse_hint <- function(expr) {
  error <- tryCatch(force(expr), error = identity)
  expect_s3_class(error, "error")
  text <- conditionMessage(error)
  expect_match(text, 'install.packages("freestiler", repos = c(', fixed = TRUE)
  expect_match(text, "https://walkerke.r-universe.dev", fixed = TRUE)
  expect_match(text, "Restart R", fixed = TRUE)
  expect_null(conditionCall(error))
  invisible(text)
}

test_that("CRAN native stubs give installation guidance through public file APIs", {
  probe <- rust_freestile_file("", "", "", "mvt", 0L, 0L, -1L, TRUE,
    -1.0, -1.0, -1L, FALSE, TRUE)
  skip_if_not(startsWith(probe, "Error: GeoParquet support not compiled"),
    "This check exercises the build without GeoParquet support")
  input <- tempfile(fileext = ".parquet")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(c(input, output)), add = TRUE)
  writeBin(charToRaw("input"), input)
  writeBin(charToRaw("existing archive"), output)
  text <- .expect_runiverse_hint(freestile_file(input, output, quiet = TRUE))
  expect_match(text, "GeoParquet file input", fixed = TRUE)
  text <- .expect_runiverse_hint(freestile_file(input, output, max_zoom = 1,
    cluster_maxzoom = 1, cluster_distance = 60, category = "group",
    category_values = 1:2, quiet = TRUE))
  expect_match(text, "Categorical file clustering", fixed = TRUE)
  expect_identical(readBin(output, "raw", n = 100), charToRaw("existing archive"))
})

test_that("missing DuckDB backends provide installation guidance and the R alternative", {
  local_mocked_bindings(.has_rust_duckdb = function() FALSE,
    .has_r_duckdb = function() FALSE)
  for (backend in c("rust", "auto")) {
    withr::local_options(freestiler.duckdb_backend = backend)
    text <- .expect_runiverse_hint(freestile_query("SELECT 1",
      tempfile(fileext = ".pmtiles"), quiet = TRUE))
    expect_match(text, "macOS and Linux", fixed = TRUE)
    expect_match(text, "On Windows", fixed = TRUE)
    expect_match(text, 'install.packages(c("duckdb", "DBI"))', fixed = TRUE)
  }
})

test_that("forced streaming explains installation and backend selection", {
  local_mocked_bindings(.has_r_duckdb = function() TRUE)
  withr::local_options(freestiler.duckdb_backend = "r")
  text <- .expect_runiverse_hint(freestile_query("SELECT 1",
    tempfile(fileext = ".pmtiles"), streaming = "always", quiet = TRUE))
  expect_match(text, 'options(freestiler.duckdb_backend = "auto")', fixed = TRUE)
})

test_that("ordinary native errors retain their actual cause", {
  expect_error(.stop_native_error("Error: Invalid geometry"),
    "^Error: Invalid geometry$")
})

test_that("categorical clustering explains dependencies when jsonlite is missing", {
  original_require <- base::requireNamespace
  local_mocked_bindings(requireNamespace = function(package, ...) {
    if (identical(package, "jsonlite")) return(FALSE)
    original_require(package, ...)
  }, .package = "base")
  input <- tempfile(fileext = ".parquet")
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(c(input, output)), add = TRUE)
  writeBin(charToRaw("input"), input)
  writeBin(charToRaw("existing archive"), output)
  text <- .expect_runiverse_hint(freestile_file(input, output, max_zoom = 1,
    cluster_maxzoom = 1, cluster_distance = 60, category = "group",
    category_values = 1:2, quiet = TRUE))
  expect_match(text, 'install.packages("jsonlite")', fixed = TRUE)
  expect_identical(readBin(output, "raw", n = 100), charToRaw("existing archive"))
})
