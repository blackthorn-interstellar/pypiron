#!/usr/bin/env bash
# Pull the current soak bundle from S3 and, if its ETag changed, reinstall and
# restart the fleet's soak processes so the box churns on fixed code within
# minutes of a new bundle being published. Pure IAM (instance role) — no git,
# no build, no secret. Driven by pypiron-soak-refresh.timer (~every 10 min).
set -euo pipefail

: "${PYPIRON_SOAK_S3_BUCKET:?}" "${PYPIRON_SOAK_BUNDLE_KEY:?}"
DEST=/opt/pypiron-soak
ETAG_FILE="$DEST/.bundle.etag"

remote_etag=$(aws s3api head-object --bucket "$PYPIRON_SOAK_S3_BUCKET" \
    --key "$PYPIRON_SOAK_BUNDLE_KEY" --query ETag --output text 2>/dev/null || true)
if [ -z "$remote_etag" ] || [ "$remote_etag" = "None" ]; then
    echo "fetch-bundle: cannot head s3://$PYPIRON_SOAK_S3_BUCKET/$PYPIRON_SOAK_BUNDLE_KEY" >&2
    exit 1
fi
if [ -f "$ETAG_FILE" ] && [ "$(cat "$ETAG_FILE")" = "$remote_etag" ]; then
    exit 0 # unchanged
fi

echo "fetch-bundle: new bundle $remote_etag"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
aws s3 cp "s3://$PYPIRON_SOAK_S3_BUCKET/$PYPIRON_SOAK_BUNDLE_KEY" "$tmp/bundle.tar.gz"
# Extract over the live tree (overwriting a running binary is safe — the running
# process keeps the old inode until it restarts). --no-same-owner keeps the tree
# root-owned: this runs as root (via pypiron-soak-refresh.service) and the tree
# holds the very scripts it re-executes, so it must never become soak-writable.
tar --no-same-owner -xzf "$tmp/bundle.tar.gz" -C "$DEST"
echo "$remote_etag" >"$ETAG_FILE"

"$DEST/install.sh" # idempotent: refresh unit files + enablement
systemctl restart 'pypiron-soak@*.service' pypiron-soak-reporter.service
echo "fetch-bundle: now running $remote_etag"
