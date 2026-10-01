# Shared installation guidance for optional native build features.
.runiverse_install_hint <- function(duckdb = FALSE) {
  paste0(
    "Restart R, then install freestiler from R-Universe:\n",
    "install.packages(\"freestiler\", repos = c(\n",
    "  \"https://walkerke.r-universe.dev\", \"https://cloud.r-project.org\"\n",
    "))",
    if (duckdb) paste0(
      "\nRust DuckDB and streaming are available in R-Universe builds on macOS and Linux.\n",
      "On Windows, use the R duckdb backend for non-streaming queries."
    ) else ""
  )
}

.stop_native_error <- function(result) {
  feature <- if (startsWith(result, "Error: GeoParquet support not compiled")) {
    "GeoParquet file input"
  } else if (startsWith(result, "Error: Ordered clustering requires the GeoParquet-enabled build")) {
    "Categorical file clustering (GeoParquet support)"
  } else if (startsWith(result, "Error: DuckDB support not compiled")) {
    "Rust DuckDB support"
  } else {
    stop(result, call. = FALSE)
  }
  stop(feature, " is not compiled into this freestiler build.\n",
    .runiverse_install_hint(duckdb = identical(feature, "Rust DuckDB support")),
    call. = FALSE)
}
