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

# The rust-cross base images configure CARGO_TARGET_*_LINKER env vars, but
# the mutable image tags drift and the referenced cross-gcc may no longer
# exist (observed with x86_64-unknown-linux-gnu-gcc). Only host build
# scripts are linked here (-p v8 produces an rlib), so fall back to the
# clang installed by our Dockerfiles when a configured linker is missing.
for var in $(env | sed -n 's/^\(CARGO_TARGET_[A-Z0-9_]*_LINKER\)=.*/\1/p'); do
  linker="${!var}"
  if ! command -v "${linker}" >/dev/null 2>&1; then
    echo "Configured ${var}=${linker} not found in image; using clang-19 instead"
    export "${var}=clang-19"
  fi
done

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

# Diagnostics: show the GN args that were actually resolved and whether the
# sysroot made it into the compile commands.
GN_OUT_DIR="${BUILD_DIR}/gn_out"
if [ -f "${GN_OUT_DIR}/args.gn" ]; then
  echo "=== resolved gn args (${GN_OUT_DIR}/args.gn) ==="
  cat "${GN_OUT_DIR}/args.gn"
fi
echo "=== --sysroot flags in ninja compile commands ==="
grep -rho -- "--sysroot=[^ \"]*" "${GN_OUT_DIR}"/*.ninja 2>/dev/null | sort | uniq -c || echo "(none found)"

# Guard against glibc drift: the archive must stay linkable under
# manylinux_2_28 (glibc 2.28). Fail loudly if the static lib references
# symbols introduced in later glibc versions (the denylist covers known
# offenders from glibc 2.29-2.38; extend it if auditwheel ever complains).
GLIBC_POST_228_SYMBOLS='^(pthread_cond_clockwait|pthread_mutex_clocklock|pthread_rwlock_clockrdlock|pthread_rwlock_clockwrlock|sem_clockwait|pthread_clockjoin_np|gettid|getdents64|__libc_single_threaded|arc4random|arc4random_buf|arc4random_uniform|close_range|__isoc23_.*)$'
UNDEFINED_SYMBOLS=""
for nm_bin in llvm-nm "${TARGET_ARCH}-nm" nm; do
  if command -v "${nm_bin}" >/dev/null 2>&1; then
    if UNDEFINED_SYMBOLS=$("${nm_bin}" --undefined-only "${STATIC_LIB}" 2>/dev/null) && [ -n "${UNDEFINED_SYMBOLS}" ]; then
      echo "Symbol audit using ${nm_bin}"
      break
    fi
    UNDEFINED_SYMBOLS=""
  fi
done
if [ -z "${UNDEFINED_SYMBOLS}" ]; then
  echo "ERROR: no nm tool in the image could read ${STATIC_LIB}; refusing to publish unaudited archive" >&2
  exit 1
fi
# Only strong undefined references ("U") are fatal: weak ones ("w"/"v") are
# left null by the linker on older glibc and handled by runtime fallbacks.
WEAK_MATCHES=$(echo "${UNDEFINED_SYMBOLS}" | awk '$1 == "w" || $1 == "v" {print $2}' | sort -u | grep -E "${GLIBC_POST_228_SYMBOLS}" || true)
if [ -n "${WEAK_MATCHES}" ]; then
  echo "Note: weak references to post-2.28 symbols (harmless): ${WEAK_MATCHES}"
fi
BAD_SYMBOLS=$(echo "${UNDEFINED_SYMBOLS}" | awk '$1 == "U" {print $2}' | sort -u | grep -E "${GLIBC_POST_228_SYMBOLS}" || true)
if [ -n "${BAD_SYMBOLS}" ]; then
  echo "ERROR: static lib references symbols newer than glibc 2.28:" >&2
  echo "${BAD_SYMBOLS}" >&2
  echo "The build is not manylinux_2_28 compatible; check the sysroot setup in the builder image." >&2
  exit 1
fi
echo "glibc 2.28 symbol audit passed"

mkdir -p "${OUTPUT_DIR}"
gzip -9c "${STATIC_LIB}" > "${OUTPUT_DIR}/librusty_v8_release_${TARGET_ARCH}.a.gz"
cp "${SRC_BINDING}" "${OUTPUT_DIR}/src_binding_release_${TARGET_ARCH}.rs"

echo "=== Archive artifacts for v${V8_VERSION} (${TARGET_ARCH}) ==="
ls -lh "${OUTPUT_DIR}"
