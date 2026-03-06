#!/bin/bash
# Setup the freestiler Python dev environment
# Usage: cd python && bash setup_env.sh [--no-duckdb]
#
# Opens in Positron: set Python interpreter to python/.venv/bin/python

set -e

cd "$(dirname "$0")"

echo "Creating venv..."
uv venv .venv

echo "Installing dependencies..."
uv pip install maturin geopandas shapely pyproj numpy pytest \
  ipykernel ipywidgets notebook jupyterlab maplibre "leafmap[maplibre]"

FEATURES="--features duckdb"
if [[ "$1" == "--no-duckdb" ]]; then
    FEATURES="--no-default-features --features geoparquet"
    echo "Building without DuckDB support..."
else
    echo "Building with DuckDB support..."
fi

python3 -m maturin develop $FEATURES

echo ""
echo "Done! To use in Positron:"
echo "  1. Open Command Palette → 'Python: Select Interpreter'"
echo "  2. Choose: $(pwd)/.venv/bin/python"
echo "  3. Open python/examples/positron_pmtiles_smoke.py and run cells with # %%"
echo ""
echo "Or from terminal:"
echo "  source .venv/bin/activate"
