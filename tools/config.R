# Note: Any variables prefixed with `.` are used for text
# replacement in the Makevars.in and Makevars.win.in

# check the packages MSRV first
source("tools/msrv.R")

# check DEBUG and NOT_CRAN environment variables
env_debug <- Sys.getenv("DEBUG")
env_not_cran <- Sys.getenv("NOT_CRAN")

# check if the vendored zip file exists
vendor_exists <- file.exists("src/rust/vendor.tar.xz")

is_not_cran <- env_not_cran != ""
is_debug <- env_debug != ""

if (is_debug) {
  # if we have DEBUG then we set not cran to true
  # CRAN is always release build
  is_not_cran <- TRUE
  message("Creating DEBUG build.")
}

if (!is_not_cran) {
  message("Building for CRAN.")
}

# we set cran flags only if NOT_CRAN is empty and if
# the vendored crates are present.
.cran_flags <- ifelse(
  !is_not_cran && vendor_exists,
  "-j 2 --offline",
  ""
)

# when DEBUG env var is present we use `--debug` build
.profile <- ifelse(is_debug, "", "--release")
.clean_targets <- ifelse(is_debug, "", "$(TARGET_DIR)")

# We specify this target when building for webR
webr_target <- "wasm32-unknown-emscripten"

# here we check if the platform we are building for is webr
is_wasm <- identical(R.version$platform, webr_target)
is_windows <- .Platform[["OS.type"]] == "windows"

# print to terminal to inform we are building for webr
if (is_wasm) {
  message("Building for WebR")
}

# we check if we are making a debug build or not
# if so, the LIBDIR environment variable becomes:
# LIBDIR = $(TARGET_DIR)/{wasm32-unknown-emscripten}/debug
# this will be used to fill out the LIBDIR env var for Makevars.in
target_libpath <- if (is_wasm) "wasm32-unknown-emscripten" else NULL
cfg <- if (is_debug) "debug" else "release"

# used to replace @LIBDIR@
.libdir <- paste(c(target_libpath, cfg), collapse = "/")

# use this to replace @TARGET@
# we specify the target _only_ on webR
# there may be use cases later where this can be adapted or expanded
.target <- ifelse(is_wasm, paste0("--target=", webr_target), "")

# add panic exports only for WASM builds
.panic_exports <- ifelse(
  is_wasm,
  "CARGO_PROFILE_DEV_PANIC=\"abort\" CARGO_PROFILE_RELEASE_PANIC=\"abort\" ",
  ""
)

# Cargo features
# Keep advanced encodings opt-in for decoder compatibility.
features <- character(0)
if (Sys.getenv("FREESTILER_FSST") != "") {
  features <- c(features, "fsst")
  message("Enabling FSST feature.")
}

# Additional optional features
if (is_not_cran) {
  if (Sys.getenv("FREESTILER_GEOPARQUET") != "") {
    features <- c(features, "geoparquet")
    message("Enabling GeoParquet feature.")
  }
  if (Sys.getenv("FREESTILER_FASTPFOR") != "") {
    features <- c(features, "fastpfor")
    message("Enabling FastPFOR feature.")
  }
}

# DuckDB is enabled by default for native non-Windows builds. Windows R builds
# currently target GNU toolchains, and bundled libduckdb-sys is not reliable
# there yet. Set FREESTILER_DUCKDB=true to force-enable, or false/0/no/off to
# disable explicitly.
duckdb_default <- if (is_wasm) "false" else if (is_windows) "false" else if (!is_not_cran) "false" else "true"
duckdb_env <- tolower(trimws(Sys.getenv("FREESTILER_DUCKDB", unset = duckdb_default)))
duckdb_enabled <- !duckdb_env %in% c("0", "false", "no", "off")

if (!is_wasm && duckdb_enabled) {
  features <- c(features, "duckdb")
  message("Enabling DuckDB feature.")
} else if (is_windows) {
  message("DuckDB feature disabled on Windows by default.")
} else if (!is_wasm) {
  message("DuckDB feature disabled.")
}
.features <- if (length(features) > 0) {
  paste0("--features ", paste(features, collapse = ","))
} else {
  ""
}

# if windows we replace in the Makevars.win.in
mv_fp <- ifelse(
  is_windows,
  "src/Makevars.win.in",
  "src/Makevars.in"
)

# set the output file
mv_ofp <- ifelse(
  is_windows,
  "src/Makevars.win",
  "src/Makevars"
)

# delete the existing Makevars{.win/.wasm}
if (file.exists(mv_ofp)) {
  message("Cleaning previous `", mv_ofp, "`.")
  invisible(file.remove(mv_ofp))
}

# read as a single string
mv_txt <- readLines(mv_fp)

# replace placeholder values
new_txt <- gsub("@CRAN_FLAGS@", .cran_flags, mv_txt) |>
  gsub("@PROFILE@", .profile, x = _) |>
  gsub("@CLEAN_TARGET@", .clean_targets, x = _) |>
  gsub("@LIBDIR@", .libdir, x = _) |>
  gsub("@TARGET@", .target, x = _) |>
  gsub("@PANIC_EXPORTS@", .panic_exports, x = _) |>
  gsub("@FEATURES@", .features, x = _)

message("Writing `", mv_ofp, "`.")
con <- file(mv_ofp, open = "wb")
writeLines(new_txt, con, sep = "\n")
close(con)

message("`tools/config.R` has finished.")
