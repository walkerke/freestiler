# Read PMTiles metadata

Reads the header and JSON metadata from a PMTiles file.

## Usage

``` r
pmtiles_metadata(path)
```

## Arguments

- path:

  Path to a `.pmtiles` file.

## Value

A list with header fields (zoom levels, bounds, tile format, etc.) and a
nested `metadata` element containing vector layer information, or `NULL`
on error.

## Examples

``` r
if (FALSE) { # \dontrun{
meta <- pmtiles_metadata("my_tiles.pmtiles")
meta$min_zoom
meta$max_zoom
meta$metadata$vector_layers
} # }
```
