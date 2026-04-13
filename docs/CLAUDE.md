# freestiler

A Rust-powered vector tile engine for R. Creates PMTiles archives from sf objects with zero external dependencies (no tippecanoe, no Java, no Go).

## What this package does

`freestiler` takes an sf data frame and produces a `.pmtiles` file
containing vector tiles. It supports two tile formats:

- **MVT (Mapbox Vector Tiles)** — the default. Protobuf-based, supported by MapLibre GL JS, Mapbox GL JS (native PMTiles support as of v3.21), deck.gl, and more.
- **MLT (MapLibre Tiles)** — an experimental columnar binary format announced Jan 2026. Can produce smaller files for polygon and line data. Available via `tile_format = "mlt"`.

The entire pipeline runs in-memory: sf → Rust (via extendr) → tile
encoding → PMTiles archive. No temp files, no shelling out.

## Architecture

    R: freestile(sf_obj, "output.pmtiles")
      → .decompose_geometries() extracts flat coordinate vectors from sf
      → .extract_properties() extracts typed attribute columns
      → rust_freestile() calls into Rust via extendr

    Rust:
      lib.rs       — extendr entry point, parses R vectors into Feature structs
      tiler.rs     — tile math (lon/lat ↔ tile x/y), assigns features to tiles per zoom
      clip.rs      — clips geometries to tile boundaries (Cohen-Sutherland for lines, Sutherland-Hodgman for polygons)
      simplify.rs  — tile-pixel grid snapping (prevents slivers, zoom-adaptive simplification)
      mvt.rs       — MVT protobuf encoder (prost-based, hand-written structs matching vector_tile.proto)
      mlt.rs       — MLT columnar binary encoder (varint, delta encoding, typed property streams)
      pmtiles_writer.rs — assembles tiles into PMTiles v3 archive with gzip compression

## Key technical details

- **Build system**: Copied from spopt-r
  (`/Users/kylewalker/Library/CloudStorage/Dropbox/dev/spopt-r`). Uses
  configure/configure.win → tools/config.R pattern with Makevars.in
  placeholder replacement. CRAN-ready with vendor tarball support.
- **extendr 0.8**: R ↔︎ Rust bridge. The `#[extendr]` function
  `rust_freestile` receives flat vectors (coords, offsets, typed
  property columns) from R.
- **Geometry decomposition**: The R side (`freestile.R`) decomposes sf
  geometries into flat coordinate arrays with offset vectors for
  rings/parts. Important: uses
  [`sf::st_geometry_type()`](https://r-spatial.github.io/sf/reference/st_geometry_type.html)
  to get geometry types, NOT `class(g)[1]` (which returns “XY”).
- **MLT tile_type patching**: The pmtiles2 crate doesn’t have MLT (0x06)
  in its TileType enum, so we write as MVT then patch byte 99 of the
  PMTiles header to 0x06 after writing.
- **Parallel tiling**: Uses rayon for two-level parallelism — across
  tiles AND within tiles (features processed in parallel). Critical for
  low-zoom tiles containing many features.
- **Simplification**: Uses tile-pixel grid snapping instead of
  Douglas-Peucker. Coordinates are rounded to the tile’s 4096×4096 pixel
  grid and consecutive duplicates removed. This prevents slivers between
  adjacent polygons (shared vertices snap to the same pixel) and
  provides natural zoom-adaptive simplification.
- **Polygon clipping**: Sutherland-Hodgman O(4n) rectangle clipping,
  with bbox fast-paths for fully-contained and fully-outside polygons.
  Replaced the original <geo::BooleanOps> intersection which was
  O((n+k)log n).
- **Tiny feature dropping**: Sub-pixel features (bbox \< 1 pixel at
  current zoom) are skipped at lower zoom levels, dramatically reducing
  work for large datasets.

## Key Rust dependencies

| Crate            | Version | Purpose                                     |
|------------------|---------|---------------------------------------------|
| extendr-api      | 0.8     | R ↔︎ Rust bindings                           |
| geo              | 0.29    | Bounding boxes                              |
| geo-types        | 0.7     | Core geometry types                         |
| prost            | 0.13    | Protobuf encoding for MVT                   |
| pmtiles2         | 0.3     | PMTiles v3 archive writing (sync, no tokio) |
| integer-encoding | 4       | Varint encoding for MLT                     |
| flate2           | 1       | Gzip compression                            |
| rayon            | 1.10    | Parallel tile encoding                      |
| serde_json       | 1       | TileJSON metadata                           |

## How to build and test

``` r
# From within the freestiler directory:
NOT_CRAN=true devtools::load_all()
devtools::test()

# Quick manual test:
library(sf)
nc <- st_read(system.file("shape/nc.shp", package = "sf"))
freestile(nc, "test.pmtiles", layer_name = "counties")
freestile(nc, "test_mvt.pmtiles", layer_name = "counties", tile_format = "mvt")
```

## R API

``` r
freestile(
  input,                    # sf object
  output,                   # Output .pmtiles path
  layer_name = NULL,        # Layer name (derived from filename if NULL)
  tile_format = "mvt",      # "mvt" (default) or "mlt"
  min_zoom = 0,
  max_zoom = 14,
  simplification = TRUE,    # Grid-snap to tile pixels (prevents slivers)
  generate_ids = TRUE,      # Sequential feature IDs
  overwrite = TRUE,
  quiet = FALSE
)
```

## Current status

- MVP is complete and all tests pass (10/10)
- Supports: POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON,
  MULTIPOLYGON
- Property types: character, integer, numeric, logical (with NA
  handling)
- Auto-transforms to WGS84 if input CRS differs
- PMTiles headers verified correct (tile_type=1 for MVT, tile_type=6 for
  MLT, version=3)

## Future work

- Fix tile boundary seam artifacts (visible at zoom 6-8; geometry
  verified correct in adjacent tiles, winding correct, no dropped
  features — root cause still under investigation)
- `base_zoom` parameter: zoom level at which features stop being
  dropped, ensuring all features are present for client-side
  counting/querying (inspired by tippecanoe’s `--base-zoom` / `-B`)
- Advanced MLT encodings (FastPFOR, FSST)
- Direct file input (GeoJSON, GeoPackage)
- Integration with pmtiles package (`pm_create(engine = "freestiler")`)
