#!/bin/bash
# Print the v8 crate version pinned in Cargo.lock.
set -euo pipefail
VERSION=$(sed -n '/^name = "v8"$/{n;s/^version = "\(.*\)"/\1/p;}' "$(dirname "$0")/../../Cargo.lock")
if [ -z "${VERSION}" ]; then
  echo "Failed to determine v8 crate version from Cargo.lock" >&2
  exit 1
fi
echo "${VERSION}"
