#!/bin/bash
set -e

# Build script for manylinux Python wheels
# Usage: ./build-wheels.sh [TARGET_ARCH] [MANYLINUX_VERSION] [OUTPUT_DIR]
#   TARGET_ARCH: x86_64-unknown-linux-gnu (default) or aarch64-unknown-linux-gnu
#   MANYLINUX_VERSION: 2_28 (default)
#   OUTPUT_DIR: dist (default)

TARGET_ARCH="${1:-x86_64-unknown-linux-gnu}"
MANYLINUX_VERSION="${2:-2_28}"
OUTPUT_DIR="${3:-dist}"

echo "=== Building manylinux Python wheels for ${TARGET_ARCH} ==="
echo "Manylinux version: ${MANYLINUX_VERSION}"
echo "Output directory: ${OUTPUT_DIR}"

# Verify environment
echo "Rust version:"
rustc --version
echo "Cargo version:"
cargo --version
echo "Maturin version:"
maturin --version

# Build wheels
echo "Starting maturin build..."
maturin build \
  --target "${TARGET_ARCH}" \
  --manylinux "${MANYLINUX_VERSION}" \
  --release \
  --out "${OUTPUT_DIR}" \
  --find-interpreter \
  -vv

echo "=== Build completed successfully ==="
echo "Wheels generated:"
ls -lh "${OUTPUT_DIR}"
