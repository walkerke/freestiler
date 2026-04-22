# conda-forge recipe for freestiler

This directory contains the reference conda-forge recipe used to submit
`freestiler` to conda-forge. Once the package is merged to conda-forge, this
recipe lives on as a template for version bumps — the canonical copy is in
the `conda-forge/freestiler-feedstock` repository on GitHub.

## Initial submission checklist

1. Ensure the target version (see `version` in `meta.yaml`) is already
   published to PyPI with both a source distribution (`.tar.gz`) and wheels.
2. Update `version` and the sdist `sha256` in `meta.yaml`. Get the checksum
   with:
   ```bash
   curl -sL https://pypi.org/pypi/freestiler/json \
     | python3 -c "import json,sys; d=json.load(sys.stdin); \
         print([u['digests']['sha256'] for u in d['urls'] \
                if u['packagetype']=='sdist'][0])"
   ```
3. Fork `conda-forge/staged-recipes` on GitHub, clone your fork, and:
   ```bash
   git checkout -b freestiler
   mkdir -p recipes/freestiler
   cp /path/to/freestiler/python/conda-recipe/meta.yaml recipes/freestiler/
   git add recipes/freestiler/meta.yaml
   git commit -m "Add freestiler recipe"
   git push -u origin freestiler
   ```
4. Open a pull request against `conda-forge/staged-recipes`. Describe the
   package briefly and link the PyPI project.
5. Wait for the CI matrix to run. The Linux build is the one that *must*
   pass; macOS and Windows failures are acceptable for review and can be
   addressed with feedstock tweaks after merge.

## Notes on the recipe

- **Not `noarch`.** `freestiler` is a compiled Rust extension (via pyo3 +
  maturin). conda-forge will build per-platform binaries.
- **Rust + C + C++ compilers required.** The default Cargo features enable
  `geoparquet` and `duckdb`; the `duckdb` crate vendors DuckDB's C++ sources
  and builds them from scratch, which is slow but well supported on
  conda-forge's runners.
- **Python ≥3.9.** `pyproject.toml` requires `>=3.9,<3.15`. The `skip:
  true  # [py<39]` guard keeps us aligned; conda-forge's Python matrix
  handles the upper bound.
- **Maturin drives the build.** `pip install . -vv --no-deps
  --no-build-isolation` invokes the maturin build backend declared in the
  sdist's `pyproject.toml`, which points `manifest-path` at
  `python/Cargo.toml`.
- **DuckDB note.** If conda-forge build times become a problem, drop the
  `duckdb` Cargo feature by adding `--no-default-features --features
  geoparquet` via a `[tool.maturin]` `config-settings` override or a custom
  build script. Users on conda can still install the standalone `duckdb`
  package and use freestiler's `DBI`-style fallback paths.

## Version bumps after initial merge

After the first merge, the conda-forge regro-cf-autotick-bot will open a PR
in `conda-forge/freestiler-feedstock` each time a new sdist appears on PyPI.
Review, make sure CI passes, and merge. Only touch this directory when you
want to keep a local copy of the latest recipe for reference.
