# freestiler + mapgl demo
# Generates PMTiles with freestiler, then visualizes with mapgl.
#
# Before running: start a local file server on port 8082 serving /tmp/
# e.g.  npx serve /tmp -l 8082 --cors
#       or: cd /tmp && python3 -m http.server 8082

library(freestiler)
library(mapgl)
library(sf)

# --- 1. Generate tiles (skip if files already exist) -------------------------

# Block groups — 242K polygons, MLT format
if (!file.exists("/tmp/us_bgs_mlt.pmtiles")) {
  bgs <- tigris::block_groups(cb = TRUE, year = 2023)
  freestile(
    bgs,
    "/tmp/us_bgs_mlt.pmtiles",
    layer_name = "bgs",
    tile_format = "mlt",
    min_zoom = 4,
    max_zoom = 12
  )
}

# Counties with RAC data — polygon choropleth
if (!file.exists("/tmp/counties_mlt.pmtiles")) {
  counties <- st_read(
    "~/Dropbox/kwalkerdata/mapgl-examples/us_counties_rac.gpkg",
    quiet = TRUE
  )
  freestile(
    counties,
    "/tmp/counties_mlt.pmtiles",
    layer_name = "counties",
    tile_format = "mlt",
    min_zoom = 2,
    max_zoom = 10
  )
}

# --- 2. Block groups — fill + hover -----------------------------------------

maplibre(
  style = maptiler_style("dataviz"),
  bounds = c(-125, 24, -66, 50)
) |>
  add_pmtiles_source(
    id = "bgs-src",
    url = "http://localhost:8082/us_bgs_mlt.pmtiles",
    promote_id = "GEOID"
  ) |>
  add_fill_layer(
    id = "bgs-fill",
    source = "bgs-src",
    source_layer = "bgs",
    fill_color = "steelblue",
    fill_opacity = 0.4,
    hover_options = list(
      fill_color = "#ffffcc",
      fill_opacity = 0.9
    )
  ) |>
  add_line_layer(
    id = "bgs-outline",
    source = "bgs-src",
    source_layer = "bgs",
    line_color = "steelblue",
    line_width = 0.3,
    line_opacity = 0.6
  )

# --- 3. Counties choropleth -------------------------------------------------

# maplibre(
#   style = maptiler_style("dataviz-light"),
#   bounds = c(-125, 24, -66, 50)
# ) |>
#   add_pmtiles_source(
#     id = "counties-src",
#     url = "http://localhost:8082/counties_mlt.pmtiles",
#     promote_id = "GEOID"
#   ) |>
#   add_fill_layer(
#     id = "counties-fill",
#     source = "counties-src",
#     source_layer = "counties",
#     fill_color = interpolate(
#       column = "C000",
#       values = c(0, 50000, 500000),
#       stops = c("#f7fbff", "#6baed6", "#08306b"),
#       na_color = "#ccc"
#     ),
#     fill_opacity = 0.7,
#     hover_options = list(
#       fill_color = "#ffffcc",
#       fill_opacity = 0.9
#     ),
#     tooltip = "GEOID"
#   ) |>
#   add_line_layer(
#     id = "counties-outline",
#     source = "counties-src",
#     source_layer = "counties",
#     line_color = "white",
#     line_width = 0.5
#   )
