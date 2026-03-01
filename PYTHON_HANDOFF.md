# Python Handoff

This note is for the Python-side Claude working in this repo.

Goal order:

1. Get the Python package working and trustworthy on its own.
2. Then merge/unify the underlying Rust bindings with the R-side engine.

## Current Repo Shape

There are effectively two Rust implementations in this repo right now:

- `src/rust/`
  - The R-backed engine via `extendr`.
  - This is the most advanced implementation.
  - It contains the recent MLT fixes, conformance tests against `mlt-core`, `base_zoom`, buffered tile assignment, spatial ordering, and newer encoder corrections.

- `python/`
  - A separate `pyo3` crate plus Python package wrapper.
  - This is not yet aligned with the R-side Rust core.
  - It appears to be a fork/copy of the Rust engine, not a shared library.

That duplication is the core technical debt. Do not try to solve it first. First get Python correct and stable.

## What Exists On The Python Side

Python package layout:

- [`python/pyproject.toml`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/pyproject.toml)
- [`python/Cargo.toml`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/Cargo.toml)
- [`python/src/lib.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/src/lib.rs)
- [`python/python/freestiler/__init__.py`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/python/freestiler/__init__.py)
- [`python/tests/`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/tests)

Python API today:

- `freestile(input, output, layer_name=None, tile_format="mlt", min_zoom=0, max_zoom=14, drop_rate=None, cluster_distance=None, cluster_maxzoom=None, coalesce=False, simplification=True, generate_ids=True, overwrite=True, quiet=False)`

Current Python implementation details:

- Python preprocessing happens in [`python/python/freestiler/__init__.py`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/python/freestiler/__init__.py).
- Geometries are passed to Rust as WKB bytes.
- Attributes are passed as typed column lists:
  - `string_columns`
  - `int_columns`
  - `float_columns`
  - `bool_columns`
- The Rust entry point is `_freestile()` in [`python/src/lib.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/src/lib.rs).

This is a decent architecture for Python. The immediate problem is not the API shape. The problem is drift from `src/rust/`.

## Important Drift From The Main Rust Engine

Assume `python/src/*.rs` is behind `src/rust/src/*.rs` until proven otherwise.

The R-side Rust engine now includes work that Python likely does not have yet:

- corrected MLT encoder semantics
- `mlt-core` decoder-backed conformance tests
- buffered tile assignment to prevent seams
- `base_zoom`
- improved drop-rate semantics relative to `base_zoom`
- spatial feature ordering within tiles
- newer MLT stream metadata fixes

The Python crate has a copied Rust tree:

- [`python/src/mlt.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/src/mlt.rs)
- [`python/src/tiler.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/src/tiler.rs)
- [`python/src/lib.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/src/lib.rs)

Before doing anything ambitious, diff those against:

- [`src/rust/src/mlt.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/src/rust/src/mlt.rs)
- [`src/rust/src/tiler.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/src/rust/src/tiler.rs)
- [`src/rust/src/lib.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/src/rust/src/lib.rs)

## Recommended Plan

### Phase 1: Get Python Working

Do not merge Rust cores first.

First make the Python package correct, installable, and passing meaningful tests.

Checklist:

1. Build/install the Python extension cleanly with `maturin`.
2. Run the Python test suite and see what actually fails.
3. Compare Python behavior against the R-side engine on a few small fixtures.
4. Bring over only the minimum Rust fixes needed for Python correctness.

Suggested first commands:

```bash
cd python
python -m pytest -q
```

If the extension needs rebuilding:

```bash
cd python
maturin develop
python -m pytest -q
```

Focus first on:

- import/build reliability
- CRS handling
- multilayer behavior
- MVT output validity
- MLT output validity

### Phase 2: Close The Python/Rust Behavior Gap

Once Python is buildable and testable, audit feature parity against the R-side engine.

High-priority parity items:

1. `base_zoom`
   - Python API does not expose it yet.
   - R-side does.
   - If Python is meant to match R, add it after correctness is established.

2. Buffered tile assignment
   - This fixed real seam artifacts on the main Rust path.
   - Make sure Python `tiler.rs` includes the same logic.

3. Spatial ordering
   - This improves compression and especially helps MLT.
   - Python should inherit this too.

4. MLT encoder correctness
   - Geometry stream typing
   - RLE metadata
   - nullable presence stream encoding
   - dictionary string index stream typing
   - conformance with `mlt-core`

### Phase 3: Merge The Underlying Rust Bindings

Only do this after Python is green.

The long-term target should be:

- one shared Rust core
- thin R binding
- thin Python binding

Not:

- two separate Rust engines that happen to look similar

Best likely structure:

1. Extract the common tiling engine into a shared Rust crate.
2. Keep binding crates thin:
   - `extendr` crate for R
   - `pyo3` crate for Python
3. Move all core logic into the shared crate:
   - tiling
   - clipping
   - simplification
   - dropping
   - MVT encoding
   - MLT encoding
   - PMTiles writing

Then bindings only do:

- input conversion
- output/error translation
- package-specific API glue

## Practical Guidance For The Merge

When you get to the merge, do not try to unify both codepaths by manual copy/paste.

Instead:

1. Treat `src/rust/` as the source of truth for core logic.
2. Identify the Python-specific pieces in [`python/src/lib.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/src/lib.rs):
   - WKB parsing
   - Python dict extraction
   - PyO3 function signatures
3. Extract a pure Rust engine API that accepts already-parsed `LayerData`.
4. Have both R and Python call that shared engine API.

In other words:

- keep parsing at the edge
- keep tiling in the center

## Specific Things To Watch

### 1. Python CRS semantics

[`python/python/freestiler/__init__.py`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/python/freestiler/__init__.py) currently does:

- warn if `crs is None`
- reproject if not geographic or not EPSG:4326

That is reasonable, but compare it with the R-side logic. The R package already had CRS edge-case fixes, so Python should not regress there.

### 2. Geometry transport

Python uses WKB to cross the binding boundary. That is fine.

Do not replace it casually. WKB is a sensible Python-side transport because:

- GeoPandas/Shapely already support it well
- it avoids the R-style custom geometry decomposition layer
- it keeps the Python binding thinner

If anything, this is a good argument for a shared core that accepts parsed `Feature`s, not for forcing Python to mimic the R input protocol.

### 3. Tests are still shallow

Python tests currently mostly assert:

- file exists
- file size > 0

See:

- [`python/tests/test_basic.py`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/tests/test_basic.py)
- [`python/tests/test_features.py`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/tests/test_features.py)
- [`python/tests/test_multilayer.py`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/python/tests/test_multilayer.py)

That is enough to get started, but not enough to trust Python MLT.

After build/test stabilization, add:

- one MLT decode/conformance test
- one multilayer correctness test
- one CRS edge-case test
- one feature-thinning / `base_zoom` parity test if Python adds it

### 4. Don’t merge generated artifacts

There are generated binaries and sample PMTiles under `python/` right now:

- `.so`
- sample `.pmtiles`

Treat them as local artifacts, not source.

## Immediate Suggested Task Order

1. Run Python build/tests and get a clean baseline.
2. Diff `python/src/*.rs` against `src/rust/src/*.rs`.
3. Port only the correctness-critical Rust fixes into Python:
   - MLT correctness
   - seam/buffer fix
   - spatial ordering
4. Add one decoder-backed Python MLT validity test.
5. Only then design the shared Rust core extraction.

## Source Of Truth

For MLT behavior, the current best source of truth in this repo is the R-side Rust implementation:

- [`src/rust/src/mlt.rs`](/Users/kylewalker/Library/CloudStorage/Dropbox/dev/freestiler/src/rust/src/mlt.rs)

Especially because it now has:

- upstream `mlt-core` conformance coverage
- corrected stream metadata behavior
- corrected geometry/type handling

If Python disagrees with that code, assume Python is wrong until proven otherwise.

## Desired End State

The end state should be:

- Python package works cleanly
- R package works cleanly
- both call the same Rust engine
- MLT behavior is identical across bindings
- tests for both bindings cover the same important semantics

That is the point where merging the Rust bindings is worth doing.
