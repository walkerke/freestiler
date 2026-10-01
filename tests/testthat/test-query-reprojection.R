test_that("R DuckDB reprojection preserves longitude and latitude order", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  result <- .r_duckdb_query_to_sf(
    "SELECT ST_Point(-8905559.263461886, 4163881.144064293) AS geometry",
    source_crs = "EPSG:3857"
  )

  expect_equal(as.numeric(sf::st_coordinates(result)), c(-80, 35),
    tolerance = 1e-8)
  expect_equal(sf::st_crs(result)$epsg, 4326L)
})

test_that("R DuckDB query timestamps match native ISO text", {
  skip_on_cran()
  skip_if_not_installed("sf")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")

  query <- paste(
    "SELECT ST_Point(-80,35) AS geometry, d, ts AS \"observed at\", tz, n FROM (VALUES",
    "(DATE '2026-10-01', TIMESTAMP '2026-10-01 12:34:56.123456',",
    "TIMESTAMPTZ '2026-10-01 07:34:56.123456-05', 42),",
    "(NULL::DATE, NULL::TIMESTAMP, NULL::TIMESTAMPTZ, NULL::INTEGER),",
    "(DATE 'infinity', TIMESTAMP 'infinity', TIMESTAMPTZ '-infinity', 7)",
    ") t(d,ts,tz,n)"
  )
  for (zone in c("UTC", "America/Chicago")) {
    result <- .r_duckdb_query_to_sf(
      paste0("SET TimeZone = '", zone, "'; ", query),
      source_crs = "EPSG:4326"
    )
    expect_equal(as.character(result$d[1]), "2026-10-01")
    expect_equal(result[["observed at"]],
      c("2026-10-01T12:34:56.123456", NA_character_, "infinity"))
    expect_equal(result$tz,
      c("2026-10-01T12:34:56.123456Z", NA_character_, "-infinity"))
    expect_equal(result$n, c(42L, NA_integer_, 7L))
    expect_equal(as.numeric(sf::st_coordinates(result)[1, ]), c(-80, 35))
  }
})
