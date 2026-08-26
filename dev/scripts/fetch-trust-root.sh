#!/usr/bin/env bash
# Refresh the embedded Sigstore trust root used for offline PEP 740 verification.
#
# pypiron verifies relayed PyPI provenance (Sigstore bundles) offline against a
# trust root baked into the binary — no TUF updater, no runtime fetch. That root
# (Fulcio CA chain + Rekor log keys + CT log keys, each with validity windows)
# rotates rarely; we re-embed the current one at release time. A bundle signed
# under a NEWER root than the one embedded fails safe to "not verified" — never
# fail-open — so a stale root only shrinks verified coverage, it never mislabels.
#
# Run this before cutting a release (see dev/RELEASE.md), review the diff, and
# commit the regenerated src/assets/trusted_root.json.gz.
set -euo pipefail

SRC="https://raw.githubusercontent.com/sigstore/root-signing/main/targets/trusted_root.json"
OUT="src/assets/trusted_root.json.gz"

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

echo "Fetching $SRC"
curl -fsSL --max-time 30 "$SRC" -o "$tmp"

# Sanity-check it parses and carries the pieces the verifier needs before we
# embed it — a truncated or reshaped root must not silently ship.
python3 - "$tmp" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
for key in ("certificateAuthorities", "tlogs", "ctlogs"):
    if not d.get(key):
        raise SystemExit(f"trusted_root.json missing/empty {key!r}; refusing to embed")
print(f"OK: {len(d['certificateAuthorities'])} CAs, "
      f"{len(d['tlogs'])} tlogs, {len(d['ctlogs'])} ctlogs")
PY

# Deterministic gzip (-n: no name/timestamp) so an unchanged root yields an
# unchanged blob and a clean git diff.
gzip -9 -n -c "$tmp" > "$OUT"
echo "Wrote $OUT ($(wc -c < "$OUT") bytes)"
