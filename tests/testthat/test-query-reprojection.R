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
