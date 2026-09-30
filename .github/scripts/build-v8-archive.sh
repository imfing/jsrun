#!/bin/bash
set -euo pipefail

# Build a prebuilt rusty_v8 static library archive for reuse by wheel builds.
#
# Runs inside the manylinux builder containers (.github/docker/Dockerfile.*)
# with the repository mounted at the working directory. Produces the same
# artifact layout as denoland's rusty_v8 releases:
#   librusty_v8_release_{target}.a.gz
#   src_binding_release_{target}.rs
#
# Usage: ./build-v8-archive.sh TARGET_TRIPLE [OUTPUT_DIR]
#   TARGET_TRIPLE: x86_64-unknown-linux-gnu or aarch64-unknown-linux-gnu
#   OUTPUT_DIR: dist-v8 (default)

TARGET_ARCH="${1:?Usage: build-v8-archive.sh TARGET_TRIPLE [OUTPUT_DIR]}"
OUTPUT_DIR="${2:-dist-v8}"

V8_VERSION=$(sed -n '/^name = "v8"$/{n;s/^version = "\(.*\)"/\1/p;}' Cargo.lock)
if [ -z "${V8_VERSION}" ]; then
  echo "Failed to determine v8 crate version from Cargo.lock" >&2
  exit 1
fi

echo "=== Building rusty_v8 v${V8_VERSION} from source for ${TARGET_ARCH} ==="
rustc --version
cargo --version

# The crates.io package is missing files required for from-source builds,
# so patch v8 to the matching git tag.
if ! grep -q "\[patch.crates-io\]" Cargo.toml; then
  cat >> Cargo.toml <<EOF

# Patched by build script: use V8 from git (crates.io package lacks files
# needed for from-source builds)
[patch.crates-io]
v8 = { git = "https://github.com/denoland/rusty_v8", tag = "v${V8_VERSION}" }
EOF
  echo "Applied V8 git patch (tag v${V8_VERSION}) to Cargo.toml"
fi

export V8_FROM_SOURCE=1

cargo build --release -p v8 --target "${TARGET_ARCH}"

BUILD_DIR="target/${TARGET_ARCH}/release"
STATIC_LIB="${BUILD_DIR}/gn_out/obj/librusty_v8.a"
SRC_BINDING="${BUILD_DIR}/gn_out/src_binding.rs"

for artifact in "${STATIC_LIB}" "${SRC_BINDING}"; do
  if [ ! -f "${artifact}" ]; then
    echo "Expected build artifact not found: ${artifact}" >&2
    exit 1
  fi
done

mkdir -p "${OUTPUT_DIR}"
gzip -9c "${STATIC_LIB}" > "${OUTPUT_DIR}/librusty_v8_release_${TARGET_ARCH}.a.gz"
cp "${SRC_BINDING}" "${OUTPUT_DIR}/src_binding_release_${TARGET_ARCH}.rs"

echo "=== Archive artifacts for v${V8_VERSION} (${TARGET_ARCH}) ==="
ls -lh "${OUTPUT_DIR}"
