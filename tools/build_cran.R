#!/usr/bin/env Rscript
#
# Build the CRAN submission tarball.
#
# This script:
#   1. Generates a slim vendor tarball WITHOUT DuckDB/GeoParquet/FastPFOR/FSST
#   2. Builds the package tarball (R CMD build)
#   3. Restores the original full vendor tarball and Cargo files
#
# Usage:
#   source("tools/build_cran.R")
#
# The resulting .tar.gz in the parent directory is ready for CRAN submission.
# After submitting, no cleanup is needed — everything is restored automatically.

stopifnot(file.exists("DESCRIPTION"))

vendor_xz <- "src/rust/vendor.tar.xz"
vendor_bak <- "src/rust/vendor_full.tar.xz.bak"
core_toml <- "src/rust/freestiler-core/Cargo.toml"
core_bak <- "src/rust/freestiler-core/Cargo.toml.bak"
root_toml <- "src/rust/Cargo.toml"
root_bak <- "src/rust/Cargo.toml.bak"
root_lock <- "src/rust/Cargo.lock"
lock_bak <- "src/rust/Cargo.lock.bak"

restore <- function() {
  unlink("src/vendor", recursive = TRUE)
  unlink("src/.cargo", recursive = TRUE)
  if (file.exists(vendor_bak)) {
    file.rename(vendor_bak, vendor_xz)
    message("Restored full vendor tarball.")
  }
  if (file.exists(core_bak)) {
    file.rename(core_bak, core_toml)
    message("Restored core Cargo.toml.")
  }
  if (file.exists(root_bak)) {
    file.rename(root_bak, root_toml)
    message("Restored root Cargo.toml.")
  }
  if (file.exists(lock_bak)) {
    file.rename(lock_bak, root_lock)
    message("Restored Cargo.lock.")
  }
}

on.exit(restore(), add = TRUE)

# --- Step 1: Back up originals ---
message("=== Backing up full vendor tarball and Cargo files ===")
file.copy(vendor_xz, vendor_bak, overwrite = TRUE)
file.copy(core_toml, core_bak, overwrite = TRUE)
file.copy(root_toml, root_bak, overwrite = TRUE)
file.copy(root_lock, lock_bak, overwrite = TRUE)

# --- Step 2: Write a CRAN-only Cargo.toml (no optional deps) ---
message("=== Writing CRAN Cargo.toml (no optional deps) ===")

# Read current version from the real Cargo.toml
orig <- readLines(core_bak)
version_line <- grep('^version = ', orig, value = TRUE)[1]

cran_toml <- c(
  '[package]',
  'name = "freestiler-core"',
  version_line,
  'edition = "2021"',
  'rust-version = "1.77.2"',
  'description = "Core Rust engine for building PMTiles vector tilesets"',
  grep('^license = ', orig, value = TRUE)[1],
  'publish = false',
  '',
  '[features]',
  'default = []',
  '# Declare disabled features for rustc check-cfg without optional dependencies.',
  'geoparquet = []',
  'duckdb = []',
  'fastpfor = []',
  'fsst = []',
  '',
  '[dependencies]',
  'geo = "0.29"',
  'geo-types = "0.7"',
  '# Keep geo\'s transitive spade dependency before Rust 1.78-only API usage.',
  'spade = "=2.13.1"',
  'prost = "0.13"',
  'pmtiles2 = "=0.3.1"',
  '# Keep pmtiles2\'s proc-macro dependency chain on pre-Edition-2024 indexmap.',
  'indexmap = "=2.10.0"',
  'integer-encoding = "=4.0.2"',
  'flate2 = "1"',
  'serde = { version = "1", features = ["derive"] }',
  'serde_json = "1"',
  'rayon = "=1.10.0"',
  '# Keep rayon\'s transitive core crate below Rust 1.80-only releases.',
  'rayon-core = "=1.12.1"',
  '# Keep cc\'s jobserver branch from vendoring Windows-only Edition 2024 crates.',
  'jobserver = "=0.1.32"'
)
writeLines(cran_toml, core_toml)

# Keep feature names for check-cfg, without forwarding to optional dependencies.
root_lines <- readLines(root_toml)
root_lines <- sub("^(geoparquet|duckdb|fastpfor|fsst) =.*$", "\\1 = []", root_lines)
writeLines(root_lines, root_toml)

# --- Step 3: Re-vendor with stripped deps ---
message("=== Re-vendoring (no optional deps) ===")

unlink("src/vendor", recursive = TRUE)

# Regenerate lockfile to match stripped Cargo.toml (rextendr uses --locked)
system2(
  "cargo",
  c("generate-lockfile", "--manifest-path", "src/rust/Cargo.toml")
)

# Cargo >= 1.78 can write lockfile v4, which Cargo 1.77.2 cannot parse.
# The CRAN dependency graph does not require v4-specific fields.
lock_lines <- readLines(root_lock)
lock_lines <- sub("^version = 4$", "version = 3", lock_lines)
writeLines(lock_lines, root_lock)

rextendr::vendor_crates()

new_size <- file.info(vendor_xz)$size / 1e6
message(sprintf("=== CRAN vendor tarball: %.1f MB ===", new_size))

# --- Step 4: Build the package (with slim Cargo files still in place) ---
message("=== Building CRAN tarball ===")
unlink("src/rust/target", recursive = TRUE)
unlink("src/vendor", recursive = TRUE)

pkg_tar <- devtools::build()

pkg_size <- file.info(pkg_tar)$size / 1e6
message(sprintf(
  "\n=== Done! CRAN tarball: %s (%.1f MB) ===",
  pkg_tar,
  pkg_size
))
message("Submit at: https://cran.r-project.org/submit.html")

# --- Step 5: Restore everything ---
unlink("src/vendor", recursive = TRUE)
unlink("src/.cargo", recursive = TRUE)
file.rename(core_bak, core_toml)
file.rename(root_bak, root_toml)
file.rename(lock_bak, root_lock)
file.rename(vendor_bak, vendor_xz)
message("Restored all Cargo files and full vendor tarball.")
