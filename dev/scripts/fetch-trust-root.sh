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
# The root comes from a pinned upstream commit, never a branch: a moving ref
# plus a shape check is not review. Bumping TRUST_ROOT_COMMIT is the deliberate
# act, and the JSON is embedded uncompressed so `git diff` shows what changed.
#
# Run this before cutting a release (see dev/RELEASE.md), review the diff, and
# commit the regenerated src/assets/trusted_root.json.
set -euo pipefail

# sigstore/root-signing commit to take targets/trusted_root.json from.
TRUST_ROOT_COMMIT=c9bda74ad2221f938f7d2e0295ca3aad2da710a8

OUT="src/assets/trusted_root.json"

if [[ ! $TRUST_ROOT_COMMIT =~ ^[0-9a-f]{40}$ ]]; then
    echo "TRUST_ROOT_COMMIT must be a full 40-char commit sha, not a branch or tag" >&2
    exit 1
fi
SRC="https://raw.githubusercontent.com/sigstore/root-signing/$TRUST_ROOT_COMMIT/targets/trusted_root.json"

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

echo "Fetching $SRC"
if ! curl -fsSL --max-time 30 "$SRC" -o "$tmp"; then
    echo "Fetch failed: does commit $TRUST_ROOT_COMMIT exist upstream?" >&2
    exit 1
fi

# Sanity-check it parses and carries the pieces the verifier needs before we
# embed it — a truncated or reshaped root must not silently ship — and print the
# digest of exactly what we fetched so it can be quoted in review.
python3 - "$tmp" <<'PY'
import hashlib, json, sys
raw = open(sys.argv[1], "rb").read()
d = json.loads(raw)
for key in ("certificateAuthorities", "tlogs", "ctlogs"):
    if not d.get(key):
        raise SystemExit(f"trusted_root.json missing/empty {key!r}; refusing to embed")
print(f"OK: {len(d['certificateAuthorities'])} CAs, "
      f"{len(d['tlogs'])} tlogs, {len(d['ctlogs'])} ctlogs")
print(f"sha256: {hashlib.sha256(raw).hexdigest()}")
PY

# Redirect rather than `cp`, which would carry mktemp's 0600 onto a tracked file.
cat "$tmp" > "$OUT"
echo "Wrote $OUT ($(wc -c < "$OUT") bytes) — review with: git diff -- $OUT"
