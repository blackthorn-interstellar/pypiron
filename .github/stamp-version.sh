#!/usr/bin/env bash
# Stamp a release-tag version (vX.Y.Z) into Cargo.toml and Cargo.lock.
# The repo permanently carries version 0.0.0; CI runs this before building wheels.
set -euo pipefail

TAG="${1:?usage: stamp-version.sh vX.Y.Z}"
[[ "$TAG" =~ ^v([0-9]+\.[0-9]+\.[0-9]+)$ ]] || {
    echo "release tag must be vX.Y.Z, got: $TAG" >&2
    exit 1
}
V="${BASH_REMATCH[1]}"

# No trailing $ anchors: tolerate CRLF checkouts on Windows runners.
sed -i.bak "s/^version = \"0.0.0\"/version = \"$V\"/" Cargo.toml
# Cargo.lock pins pypiron's own version; stamp it too so --locked builds (e.g. from sdist) work.
sed -i.bak -e '/^name = "pypiron"/{' -e n -e "s/^version = \"0.0.0\"/version = \"$V\"/" -e '}' Cargo.lock
rm -f Cargo.toml.bak Cargo.lock.bak

grep -q "^version = \"$V\"" Cargo.toml || { echo "failed to stamp Cargo.toml" >&2; exit 1; }
grep -A1 '^name = "pypiron"' Cargo.lock | grep -q "^version = \"$V\"" || { echo "failed to stamp Cargo.lock" >&2; exit 1; }
echo "stamped version $V"
