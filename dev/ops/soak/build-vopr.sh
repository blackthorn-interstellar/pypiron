#!/usr/bin/env bash
# Build the soak fleet's `vopr` binary inside the fleet's own userland.
#
# Run this INSIDE a container of the image the fleet boots (Amazon Linux 2023),
# with the repo at /src read-only and an output directory at /out — which is
# what both `fleet.sh push-bundle` and .github/workflows/soak-bundle.yml do:
#
#   docker run --rm --platform linux/arm64 -v "$PWD":/src:ro -v "$out":/out \
#     -w /src public.ecr.aws/amazonlinux/amazonlinux:2023 \
#     bash dev/ops/soak/build-vopr.sh
#
# Why a container instead of the build host: a glibc binary runs against the
# glibc it was linked with or newer, never older. Build on a distro newer than
# the AMI and the box cannot exec the result — it exits 1 before printing
# anything, so every core crash-loops while the fleet still looks alive. That is
# not hypothetical: the CI runner has always built on glibc 2.39 and the AL2023
# fleet has 2.34; it worked only while the emitted binary happened to need
# nothing past 2.34. A toolchain bump pulled in one 2.39-versioned symbol and
# the soak burned 15 hours and ~3,900 restarts explaining nothing.
# Building in the AMI's own image makes the skew unrepresentable, and one build
# recipe shared by CI and the laptop keeps them from drifting apart again.
set -euo pipefail

# aws-lc-sys and mimalloc compile C; the rest is what a minimal AL2023 lacks.
dnf -y -q install gcc gcc-c++ cmake perl tar gzip findutils >/dev/null
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable >/dev/null
# shellcheck disable=SC1091  # written by the rustup line above
. "$HOME/.cargo/env"

# /src is the host's checkout, read-only: build into the container's own scratch
# (it dies with the container) and copy out the one artifact we ship.
CARGO_TARGET_DIR=/tmp/t cargo build --release --example vopr --locked
cp /tmp/t/release/examples/vopr /out/vopr

# The point of all of the above: it has to *run here*. One seed, one op, ~20ms.
# Exit 2 means that seed failed an oracle — the binary ran, which is the only
# thing this check is entitled to an opinion about.
rc=0
/out/vopr --seeds 1 --ops 1 >/dev/null 2>&1 || rc=$?
if [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; then
    echo "vopr does not run on the fleet's userland (exit $rc)" >&2
    exit 1
fi
echo "built + smoked on $(grep -m1 PRETTY_NAME /etc/os-release | cut -d'"' -f2), $(ldd --version | head -1)"
