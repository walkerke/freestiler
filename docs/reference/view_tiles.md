# Quickly view a PMTiles file on an interactive map

Starts a local tile server (if needed) and creates an interactive mapgl
map showing the tileset. Layer type and styling are auto-detected from
the PMTiles metadata when possible.

## Usage

``` r
view_tiles(
  input,
  layer = NULL,
  layer_type = NULL,
  color = NULL,
  opacity = 0.5,
  port = 8080,
  promote_id = NULL
)
```

## Arguments

- input:

  Path to a local `.pmtiles` file.

- layer:

  Character. Source layer name to display. If `NULL` (default), the
  first layer in the tileset is used.

- layer_type:

  Character. Map layer type: `"fill"`, `"line"`, or `"circle"`. If
  `NULL`, auto-detected from the PMTiles metadata geometry type.

- color:

  Fill, line, or circle color. Default is `"navy"` for fill and line
  layers, `"steelblue"` for circle layers.

- opacity:

  Numeric opacity (0–1). Default is 0.5.

- port:

  Port for the local tile server. Default is 8080.

- promote_id:

  Character. Property name to use as the feature ID for hover
  interactivity. If `NULL`, no feature promotion is used.

## Value

A mapgl map object (can be piped into further mapgl operations).

## See also

[`serve_tiles()`](https://walker-data.com/freestiler/reference/serve_tiles.md),
[`freestile()`](https://walker-data.com/freestiler/reference/freestile.md)

## Examples

``` r
if (FALSE) { # \dontrun{
freestile(nc, "nc.pmtiles", layer_name = "counties")
view_tiles("nc.pmtiles")

# Override auto-detection
view_tiles("roads.pmtiles", layer_type = "line", color = "red")

# Point data
view_tiles("airports.pmtiles", layer_type = "circle", color = "orange")
} # }
```
