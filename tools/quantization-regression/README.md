# Reproduce snapped-polygon quantization regressions

This directory contains the exact 39 decoded tile geometries that exposed the
backtracking and repeated-vertex pinch defects: 36 cell tile occurrences and
3 region tile occurrences. They come from public HEC-RAS example-model data.
The JSON is intentionally small (about 14 KiB); no HEC-RAS installation, solver
run, Python freestiler wrapper, or private data is needed for the direct replay.

Baseline: `beb64b0d4414da895ec67d02b2f91db6ab6acc37`.
Original reviewed candidate: `4eecd9471b065369f37147a60cf009bba3e8e67c`
(local delivery provenance; this PR reapplies its exact three-file source patch).
Original patch SHA256:
`4634b59fc9e4b49f4aa5e68c2137fdf82b0ce8288b1cf0763581e5d29d8f391c`.
Bundled `original-39-snapped-failures.json` SHA256:
`89389da32abe8745d30d82a28e8f57077dac664ca4c24fb7e3bebde6f94702ac`.

## 1. Run the Rust regression suite

From this PR checkout's root:

```sh
cargo test --locked --manifest-path src/rust/freestiler-core/Cargo.toml --lib
cargo test --locked --manifest-path src/rust/freestiler-core/Cargo.toml --features fastpfor,fsst
```

The reviewed source passed 72 default library tests and 84 tests with
`fastpfor,fsst`; these counts overlap. The optional FastPFOR build needs its
existing C/C++/CMake prerequisites. Recorded qualification used Windows x64,
MSVC, Rust/Cargo 1.99.0. This patch makes no MSRV, other-platform, WASM, or
browser qualification claim. Clippy completed with existing upstream warnings;
we do not claim `-D warnings` passes.

## 2. Replay all 39 original failures through the actual candidate quantizer

Use Python 3.12+ and uv (or install Shapely into your own Python environment).
No production code uses Shapely or MakeValid; it is only an independent oracle.

```sh
cargo run --locked --release --manifest-path src/rust/freestiler-core/Cargo.toml --example quantization_ring_regressions -- tools/quantization-regression/original-39-snapped-failures.json tools/quantization-regression/candidate-39.json
uv run --no-project --with shapely python tools/quantization-regression/validate_snapped.py tools/quantization-regression/candidate-39.json
```

Expected output:

```text
39 baseline invalid -> 0 candidate invalid; all tile/feature IDs, areas, and filled sets retained
```

The Rust example inversely maps the retained tile-grid coordinates to the
quantizer's lon/lat input, then invokes `quantize_polygon`. This is a focused
quantizer replay, not a fresh HDF-to-PMTiles run. The Python validator requires
all 39 original geometries to be invalid and all 39 candidate outputs valid,
with identical tile/feature keys, exact area and zero symmetric difference
against `shapely.make_valid(baseline)`. It never repairs candidate output.

## 3. Repeat the complete encoder/PMTiles comparison

The optional `quantization_corpus` example takes GeoJSON polygon features with
string IDs and scalar properties. The exact full inputs are `cells7.geojson`
(all 4,425 cells across two meshes) and `regions7.geojson` (all 72 classified
regions), with `mesh_name`, `cell_id` and unique `corpus_id` properties. Preserve
these exported inputs, feature order and numeric precision for exact replay.
Do not replace them with a similar model and claim the same reproduction.

Obtain the source example models through ras-commander as described below, or
request the exact computed HDF, derived GeoPackage and exported GeoJSON pair
from **bill@clbengineering.com**. The bundled 39-case replay above is immediately
available without that request. The derived outputs are not guaranteed to be
included byte-for-byte in every HEC example archive.

Create a separate baseline checkout and copy **only** the corpus example into
it (the ring replay example uses the new candidate API):

```sh
git worktree add --detach ../freestiler-quantization-baseline beb64b0d4414da895ec67d02b2f91db6ab6acc37
```

Create `../freestiler-quantization-baseline/src/rust/freestiler-core/examples/`
and copy `src/rust/freestiler-core/examples/quantization_corpus.rs` there.
Build each example in its own checkout/target directory:

```sh
cargo build --locked --release --manifest-path ../freestiler-quantization-baseline/src/rust/freestiler-core/Cargo.toml --example quantization_corpus
cargo build --locked --release --manifest-path src/rust/freestiler-core/Cargo.toml --example quantization_corpus
uv run --no-project --with shapely --with mapbox-vector-tile --with pmtiles python tools/quantization-regression/qualify_corpus.py --baseline-exe ../freestiler-quantization-baseline/src/rust/freestiler-core/target/release/examples/quantization_corpus --candidate-exe src/rust/freestiler-core/target/release/examples/quantization_corpus --fixtures /path/to/exact-geojson-pair --output /path/to/new-qualification-output
```

Append `.exe` to both executable arguments on Windows. The runner independently
reopens PMTiles, enumerates and directly addresses tiles, decodes MVT with
`mapbox-vector-tile`, and uses the MapLibre `mlt-core` reference decoder for MLT.
It checks tile/feature retention, properties, validity, area and filled-set
identity. It writes `qualification.json`, actual PMTiles and MLT decoded JSON.
Neither executable path nor fixture location is machine-specific.

Expected geometry results for MVT and MLT, each with simplification off/on:

| Input | Zoom | Source features | Buffered tile occurrences | Invalid baseline -> candidate |
|---|---:|---:|---:|---:|
| Cells | 6 | 4,425 | 8,850 across 2 tiles | 36 -> 0 |
| Regions | 12 | 72 | 115 across 13 tiles | 3 -> 0 |

All source rows remain represented. Region hole counts change from 13 to 16
because previously implicit unfilled loops become explicit holes, with the
same filled area. Separate Rust regressions cover split lobes becoming a
MultiPolygon while retaining an existing hole and the same feature ID.

**Known MLT limitation:** both baseline and candidate have independent
source-ID and float64-property fidelity failures. The runner records these
separately rather than treating MLT geometry validity as full conformance.
MVT source IDs/properties matched exactly. This PR does not fix the MLT codec
issues. The example's pmtiles2 directory-reader workaround changes header
byte 99 only in a private memory copy because pmtiles2 0.3.1 predates tile type
6; saved archives and encoded payloads remain unchanged.

The runner's short sequential timing samples are observations, not a speedup
claim. It does not measure isolated peak memory.

## 4. Download model sources through ras-commander

Install ras-commander in a separate Python environment (e.g. `uv pip install
ras-commander`). The public `RasExamples` API downloads the HEC example
archives; it does not run a solver:

```python
from pathlib import Path
from ras_commander import RasExamples

RasExamples.get_example_projects("7.0")
print([name for name in RasExamples.list_projects() if "bald" in name.lower()])
project = RasExamples.extract_project(
    "BaldEagleCrkMulti2D", output_path=Path("repro-models")
)
print(project)
```

If the archive uses a different project label, select the Bald Eagle project
from the printed list. Archive version labels alone are not evidence of the
stored HDF's solver version. The exact original acceptance inputs were:

- `BaldEagleDamBrk.p18.hdf`, stored HEC-RAS 7.0, 4,425 cells across two meshes;
  SHA256 `1d0063a84fd131ac95ffaa2b3f6647f2ef51c7f38f23c43e8ae6c698bfc95f0b`.
- `computed7_regions.gpkg`, 72 final classified regions;
  SHA256 `29d05cc0868fcd1b5a54f8df259f1ae6f16f3f8de6dae3613e9b1f7d68e9c5d8`.

These checksums distinguish the actual computed fixtures from similarly named
plans. If either exact artifact is missing or has another checksum, request
the exact fixtures and exported GeoJSON from **bill@clbengineering.com**.
The GeoPackage is a derived analysis product, not a promised archive member.
Use ras-commander's HDF/mesh APIs and CRS metadata to extract/reproject model
geometry; keep RAS-specific interpretation out of freestiler. No fresh solver
run is required to verify this PR with the supplied reproductions.

## Scope

The change addresses collinear backtracking and repeated-vertex pinches after
integer quantization. It does not promise general invalid-input repair,
arbitrary nonvertex crossings, intersecting rings, or all multipolygon
overlaps. Repeated splitting can be quadratic in pathological inputs, and MLT
caches quantized parts. Existing collapse policies remain unchanged.
