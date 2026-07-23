#!/usr/bin/env bash
# Record docs/assets/demo.gif — the README quickstart demo.
#
# Requires vhs (brew install vhs) and uv. Records against the *published*
# pypiron (`uvx pypiron serve`), so the banner shows whatever PyPI serves —
# re-run after any release that changes the startup banner.
set -euo pipefail

cd "$(dirname "$0")"
command -v vhs >/dev/null || { echo "vhs not found (brew install vhs)" >&2; exit 1; }

export PYPIRON_DEMO_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$PYPIRON_DEMO_DIR"
  pkill -f "pypiron serve" 2>/dev/null || true
}
trap cleanup EXIT

# A demo package to publish, and a venv to install it back into.
(
  cd "$PYPIRON_DEMO_DIR"
  uv init --lib --name acme-widgets -q .
  uv build --wheel -q
  uv venv -q .venv
)

# Pre-warm the uvx cache so the recording doesn't open on a download bar.
uvx pypiron --version >/dev/null

vhs demo.tape
mv demo.gif ../../../docs/assets/demo.gif
echo "wrote docs/assets/demo.gif"
