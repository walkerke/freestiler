## Update submission: freestiler 0.3.0

This update addresses the macOS installation warning reported in the M1mac
Additional issues check for 0.2.0, ahead of the 2026-10-09 deadline.

The build no longer derives MACOSX_DEPLOYMENT_TARGET from the host OS version.
When the user has not supplied a deployment target, configure sets Rust and
dependency C builds to Rust's architecture baseline (11.0 on arm64, 10.12 on
x86_64). Explicit user settings are preserved.

The release also fixes collapsed MVT line-part offsets and DuckDB property/CRS
handling, improves tiling memory use, and adds categorical H3 and clustering
functionality. See NEWS.md for details.

## Test environment and results

* macOS Tahoe 26.6.2, arm64; R 4.6.1.
* Apple clang 21.0.0 with MacOSX27.0.sdk.
* Rust/cargo 1.77.2, the declared minimum supported version.
* R CMD check --as-cran --no-manual on the actual slim source tarball:
  0 errors, 0 warnings, 1 note.
* Fresh offline compilation using the bundled Rust dependencies produced no
  deployment-target linker warning. A freshly compiled zstd C object reports
  minos 11.0 and SDK 27.0.
* The CRAN-mode tests passed (179 assertions). An additional local run against
  the installed slim build exercised the R DuckDB fallback and H3 paths with
  no failures; it reported four sf centroid warnings in existing test setup.

The PDF manual check was omitted because LaTeX is not installed locally.
Windows and R-devel were not checked locally for this exact tarball.

## Note

The incoming-feasibility note reports the source tarball size (13,910,636
bytes). The tarball contains a 12.8 MB compressed bundle of Rust dependencies
so CRAN can compile offline. Optional native DuckDB, GeoParquet, FastPFOR and
FSST dependencies are excluded from this submission. The optional R DuckDB
fallback remains available, and requests for omitted native capabilities
provide instructions for installing the full build from R-Universe.

Example datasets, local maps, benchmark artifacts and Git worktree metadata
are excluded from the source package.
