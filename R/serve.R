# Package-level cache for server handles
.server_cache <- new.env(parent = emptyenv())

#' Serve PMTiles files via local HTTP server with CORS
#'
#' Start a local HTTP server to serve PMTiles files with CORS headers and HTTP
#' range request support. This allows PMTiles to be consumed by mapgl and
#' MapLibre GL JS. The server runs in the background and can be stopped with
#' \code{stop_server()}.
#'
#' If a server is already running on the requested port, it is stopped first.
#'
#' @param path Path to a directory containing PMTiles files, or a single
#'   PMTiles file. If a single file, its directory will be served.
#' @param port Port number for the HTTP server. Default is 8080.
#'
#' @details
#' The server uses httpuv (a dependency of Shiny) to serve static files with
#' the CORS and range-request headers that PMTiles requires. Works well for
#' files up to ~1 GB. For larger files, consider an external server like
#' \code{npx http-server /path --cors -c-1}.
#'
#' @return Invisibly returns a list with \code{url}, \code{port}, and
#'   \code{dir}. The server handle is stored internally so it can be stopped
#'   with \code{stop_server()}.
#'
#' @examples
#' \dontrun{
#' # Serve a directory
#' serve_tiles("/tmp/tiles")
#'
#' # Serve a single file (its directory is served)
#' serve_tiles("us_bgs.pmtiles")
#'
#' # Stop when done
#' stop_server()
#' }
#'
#' @seealso [stop_server()], [view_tiles()]
#' @export
serve_tiles <- function(path, port = 8080) {
  if (!requireNamespace("httpuv", quietly = TRUE)) {
    stop(
      "Package 'httpuv' is required for serve_tiles().\n",
      "Install it with: install.packages('httpuv')",
      call. = FALSE
    )
  }

  path <- path.expand(path)

  if (!file.exists(path)) {
    stop("Path not found: ", path, call. = FALSE)
  }

  if (dir.exists(path)) {
    serve_dir <- path
  } else {
    serve_dir <- dirname(path)
  }

  # Stop any existing server on this port
  port_key <- as.character(port)
  if (exists(port_key, envir = .server_cache)) {
    .stop_one(port_key)
  }

  base_url <- paste0("http://localhost:", port)

  app <- list(
    call = function(req) {
      cors_headers <- list(
        "Access-Control-Allow-Origin" = "*",
        "Access-Control-Allow-Methods" = "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers" = "Range, Content-Type",
        "Access-Control-Expose-Headers" = "Content-Range, Content-Length"
      )

      # Handle OPTIONS preflight
      if (req$REQUEST_METHOD == "OPTIONS") {
        return(list(status = 200L, headers = cors_headers, body = ""))
      }

      file_path <- file.path(serve_dir, substring(req$PATH_INFO, 2))

      if (!file.exists(file_path) || file.info(file_path)$isdir) {
        return(list(status = 404L, headers = cors_headers, body = "Not Found"))
      }

      ext <- tools::file_ext(file_path)
      content_type <- switch(ext,
        "pmtiles" = "application/octet-stream",
        "json" = "application/json",
        "html" = "text/html",
        "js" = "application/javascript",
        "application/octet-stream"
      )
      cors_headers[["Content-Type"]] <- content_type

      file_size <- file.info(file_path)$size
      range_header <- req$HTTP_RANGE

      if (!is.null(range_header) && grepl("^bytes=", range_header)) {
        range <- sub("^bytes=", "", range_header)
        parts <- strsplit(range, "-")[[1]]
        start <- as.integer(parts[1])
        end <- if (nchar(parts[2]) > 0) as.integer(parts[2]) else file_size - 1L

        con <- file(file_path, "rb")
        on.exit(close(con), add = TRUE)
        seek(con, start)
        content <- readBin(con, "raw", n = end - start + 1L)

        cors_headers[["Content-Range"]] <- sprintf("bytes %d-%d/%d", start, end, file_size)
        cors_headers[["Content-Length"]] <- as.character(length(content))

        return(list(status = 206L, headers = cors_headers, body = content))
      }

      content <- readBin(file_path, "raw", n = file_size)
      cors_headers[["Content-Length"]] <- as.character(file_size)

      list(status = 200L, headers = cors_headers, body = content)
    }
  )

  handle <- tryCatch(
    httpuv::startDaemonizedServer(host = "0.0.0.0", port = port, app = app),
    error = function(e) {
      stop(
        "Cannot start server on port ", port, ": ", conditionMessage(e),
        call. = FALSE
      )
    }
  )

  assign(port_key, list(handle = handle, dir = serve_dir), envir = .server_cache)

  size_bytes <- sum(file.info(
    list.files(serve_dir, pattern = "\\.pmtiles$", full.names = TRUE)
  )$size, na.rm = TRUE)

  message("Serving tiles at ", base_url)
  message("  Directory: ", serve_dir)
  if (size_bytes > 1e9) {
    message(
      "  Note: serving ", sprintf("%.1f GB", size_bytes / 1e9),
      " of PMTiles. For better performance with large files, consider:\n",
      "  npx http-server ", serve_dir, " -p ", port, " --cors -c-1"
    )
  }
  message("Use stop_server() to stop")

  invisible(list(url = base_url, port = port, dir = serve_dir))
}

#' Stop a local tile server
#'
#' @param port Port number to stop, or \code{NULL} to stop all running servers.
#'
#' @return Invisibly returns \code{TRUE} if a server was stopped.
#'
#' @examples
#' \dontrun{
#' serve_tiles("tiles/")
#' stop_server()        # stop all
#' stop_server(8080)    # stop specific port
#' }
#'
#' @seealso [serve_tiles()]
#' @export
stop_server <- function(port = NULL) {
  if (!requireNamespace("httpuv", quietly = TRUE)) {
    message("No servers running (httpuv not installed)")
    return(invisible(FALSE))
  }

  ports <- ls(.server_cache)
  if (length(ports) == 0L) {
    message("No tile servers running")
    return(invisible(FALSE))
  }

  if (is.null(port)) {
    for (p in ports) .stop_one(p)
    message("Stopped all tile servers")
    return(invisible(TRUE))
  }

  port_key <- as.character(port)
  if (!exists(port_key, envir = .server_cache)) {
    message("No server running on port ", port)
    return(invisible(FALSE))
  }

  .stop_one(port_key)
  message("Stopped tile server on port ", port)
  invisible(TRUE)
}

#' Stop one server by port key (internal)
#' @noRd
.stop_one <- function(port_key) {
  info <- get(port_key, envir = .server_cache)
  tryCatch(
    httpuv::stopDaemonizedServer(info$handle),
    error = function(e) NULL
  )
  rm(list = port_key, envir = .server_cache)
}
