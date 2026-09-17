test_that("byte range parsing and headers handle archives larger than 2 GB", {
  size <- 16e9
  rng <- freestiler:::.parse_byte_range("bytes=15999999872-15999999999", size)
  expect_identical(rng$start, 15999999872)
  expect_identical(rng$end, 15999999999)

  h <- freestiler:::.byte_range_headers(rng$start, rng$end, size)
  expect_identical(h[["Content-Range"]], "bytes 15999999872-15999999999/16000000000")
  expect_identical(h[["Content-Length"]], "128")

  # open-ended range runs to the end of the file
  open <- freestiler:::.parse_byte_range("bytes=15999999000-", size)
  expect_identical(open$end, size - 1)

  # end past EOF is clamped
  clamped <- freestiler:::.parse_byte_range("bytes=0-99999999999", size)
  expect_identical(clamped$end, size - 1)

  expect_error(freestiler:::.parse_byte_range("bytes=abc-def", size), "Invalid Range")
})

test_that("serve_tiles answers range requests with CORS headers", {
  skip_on_cran()
  skip_if_not_installed("httpuv")
  skip_if_not_installed("curl")
  skip_if_not_installed("callr")

  dir <- tempfile()
  dir.create(dir)
  writeBin(as.raw(0:255), file.path(dir, "x.pmtiles"))
  port <- httpuv::randomPort()

  # The server runs on R's event loop, so it must live in another process
  # for a blocking client call in this one to be answered.
  srv <- callr::r_bg(function(dir, port) {
    freestiler::serve_tiles(dir, port = port)
    repeat httpuv::service(200)  # pump the event loop so requests are served
  }, args = list(dir = dir, port = port), package = TRUE)
  on.exit(srv$kill(), add = TRUE)

  url <- sprintf("http://localhost:%d/x.pmtiles", port)
  for (i in 1:50) {
    ok <- tryCatch(curl::curl_fetch_memory(url)$status_code == 200L, error = function(e) FALSE)
    if (ok) break
    Sys.sleep(0.2)
  }
  expect_true(ok, "server did not come up")

  h <- curl::new_handle()
  curl::handle_setheaders(h, Range = "bytes=10-19")
  res <- curl::curl_fetch_memory(url, h)
  expect_equal(res$status_code, 206L)
  expect_identical(res$content, as.raw(10:19))
  hdrs <- curl::parse_headers_list(res$headers)
  expect_identical(hdrs[["access-control-allow-origin"]], "*")
  expect_identical(hdrs[["content-range"]], "bytes 10-19/256")
})
