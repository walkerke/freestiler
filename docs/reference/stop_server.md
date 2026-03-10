# Stop a local tile server

Stop a local tile server

## Usage

``` r
stop_server(port = NULL)
```

## Arguments

- port:

  Port number to stop, or `NULL` to stop all running servers.

## Value

Invisibly returns `TRUE` if a server was stopped.

## See also

[`serve_tiles()`](https://walker-data.com/freestiler/reference/serve_tiles.md)

## Examples

``` r
if (FALSE) { # \dontrun{
serve_tiles("tiles/")
stop_server()        # stop all
stop_server(8080)    # stop specific port
} # }
```
