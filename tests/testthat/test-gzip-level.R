test_that("gzip_level shrinks the archive", {
  skip_on_cran()
  skip_if_not_installed("sf")

  nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)

  fast <- tempfile(fileext = ".pmtiles")
  best <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(c(fast, best)), add = TRUE)

  freestile(nc, fast, layer_name = "counties", max_zoom = 8, quiet = TRUE)
  freestile(nc, best, layer_name = "counties", max_zoom = 8, quiet = TRUE,
            gzip_level = 9)

  expect_lt(file.info(best)$size, file.info(fast)$size)
})

test_that("gzip_level validates its value", {
  skip_if_not_installed("sf")

  nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
  output <- tempfile(fileext = ".pmtiles")
  on.exit(unlink(output), add = TRUE)

  for (level in list(-1, 10, 2.5, "6", NA, c(1, 2))) {
    expect_error(
      freestile(nc, output, max_zoom = 4, quiet = TRUE, gzip_level = level),
      "gzip_level"
    )
  }
  expect_false(file.exists(output))
})
