# View an H3 hexagonal-binning PMTiles archive

Reads the metadata from a PMTiles archive produced by
[`freestile_h3()`](https://walker-data.com/freestiler/reference/freestile_h3.md)
and builds a `mapgl` map with one fill layer per H3 resolution plus a
circle layer for raw points. Automatically detects whether the archive
was built with `fade = FALSE` (disjoint zoom windows, clean breaks) or
`fade = TRUE` (overlapping windows, cross-fade) and styles accordingly.

## Usage

``` r
view_h3_tiles(
  input,
  agg_column = NULL,
  stops = NULL,
  palette = "viridis",
  hex_opacity = 0.9,
  point_color = "#0868ac",
  point_radius = NULL,
  background_style = NULL,
  hex_layer_prefix = "h3",
  point_layer_name = "points",
  port = 8080
)
```

## Arguments

- input:

  Path to a local `.pmtiles` file produced by
  [`freestile_h3()`](https://walker-data.com/freestiler/reference/freestile_h3.md).

- agg_column:

  Character or NULL. Aggregation column to drive the hex color scale. If
  `NULL`, categorical archives use `point_count`; otherwise the first
  requested numeric aggregate is used. Older archives keep their
  original field-order fallback. This is a numeric quick-look viewer;
  categorical colors can be styled with
  [`mapgl::add_fill_layer()`](https://walker-data.com/mapgl/reference/add_fill_layer.html).

- stops:

  List with `values` and `colors` (equal length, sorted by `values`)
  defining the shared color scale across all hex layers. If `NULL`, the
  documented quick-look default is used.

- palette:

  Character. Named palette used to generate default `colors` when
  `stops` is `NULL`. Currently supports `"viridis"` (default),
  `"magma"`, `"plasma"`, `"cividis"`, `"inferno"`, `"rocket"`, `"mako"`,
  `"turbo"` if `viridisLite` is installed; otherwise falls back to a
  five-color blue ramp.

- hex_opacity:

  Numeric. Peak fill opacity for hex layers (default 0.9). In fade mode
  this is the opacity at each layer's center zoom.

- point_color:

  Character. Color for the raw-point circles (default `"#0868ac"`).

- point_radius:

  mapgl expression or NULL. If `NULL`, a zoom-interpolated default is
  used.

- background_style:

  mapgl style passed to
  [`mapgl::maplibre()`](https://walker-data.com/mapgl/reference/maplibre.html);
  if `NULL` uses
  [`mapgl::maplibre()`](https://walker-data.com/mapgl/reference/maplibre.html)
  defaults.

- hex_layer_prefix:

  Character. Prefix used when the archive was written. Must match the
  value passed to
  [`freestile_h3()`](https://walker-data.com/freestiler/reference/freestile_h3.md);
  default `"h3"`.

- point_layer_name:

  Character. Name of the raw-points MVT layer in the archive. Must match
  the value passed to
  [`freestile_h3()`](https://walker-data.com/freestiler/reference/freestile_h3.md);
  default `"points"`.

- port:

  Integer. Port for the local PMTiles server (default 8080).

## Value

A `mapgl` map object.

## Details

The default color scale is a documented quick-look ramp (5 evenly spaced
breaks across `1, 10, 100, 1000, 10000`). For production maps, pass an
explicit `stops = list(values = ..., colors = ...)` derived from your
data. Embedding real aggregation statistics in PMTiles metadata is on
the roadmap.

## See also

[`freestile_h3()`](https://walker-data.com/freestiler/reference/freestile_h3.md),
[`view_tiles()`](https://walker-data.com/freestiler/reference/view_tiles.md)

## Examples

``` r
if (FALSE) { # \dontrun{
view_h3_tiles("wind.pmtiles", agg_column = "n",
  stops = list(values = c(1, 10, 100, 1000, 10000),
               colors = viridisLite::viridis(5)))
} # }
```
