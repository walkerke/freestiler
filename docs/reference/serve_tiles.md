# Serve PMTiles files via local HTTP server with CORS

Start a local HTTP server to serve PMTiles files with CORS headers and
HTTP range request support. This allows PMTiles to be consumed by mapgl
and MapLibre GL JS. The server runs in the background and can be stopped
with
[`stop_server()`](https://walker-data.com/freestiler/reference/stop_server.md).

## Usage

``` r
serve_tiles(path, port = 8080)
```

## Arguments

- path:

  Path to a directory containing PMTiles files, or a single PMTiles
  file. If a single file, its directory will be served.

- port:

  Port number for the HTTP server. Default is 8080.

## Value

Invisibly returns a list with `url`, `port`, and `dir`. The server
handle is stored internally so it can be stopped with
[`stop_server()`](https://walker-data.com/freestiler/reference/stop_server.md).

## Details

If a server is already running on the requested port, it is stopped
first.

The server uses httpuv (a dependency of Shiny) to serve static files
with the CORS and range-request headers that PMTiles requires. Works
well for files up to ~1 GB. For larger files, consider an external
server like `npx http-server /path --cors -c-1`.

## See also

[`stop_server()`](https://walker-data.com/freestiler/reference/stop_server.md),
[`view_tiles()`](https://walker-data.com/freestiler/reference/view_tiles.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Serve a directory
serve_tiles("/tmp/tiles")

# Serve a single file (its directory is served)
serve_tiles("us_bgs.pmtiles")

# Stop when done
stop_server()
} # }
```
