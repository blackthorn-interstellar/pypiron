#!/usr/bin/env bash
# Wire up the soak from an extracted bundle at /opt/pypiron-soak (the prebuilt
# `vopr` binary + these ops files). No repo, no toolchain, no build. Called by
# cloud-init at first boot and by fetch-bundle.sh on every update — idempotent.
# Prereqs: python3 + the aws CLI (both on Amazon Linux 2023), and
# /etc/pypiron-soak/soak.env with at least PYPIRON_SOAK_S3_BUCKET.
set -euo pipefail

DEST=${PYPIRON_SOAK_DIR:-/opt/pypiron-soak}
UNIT_DIR=/etc/systemd/system
SOAK_USER=${SOAK_USER:-soak}
CORES=$(nproc)

if [ "$(id -u)" -ne 0 ]; then
    echo "install.sh must run as root" >&2
    exit 1
fi

if ! id "$SOAK_USER" >/dev/null 2>&1; then
    useradd --system --home-dir "$DEST" --shell /usr/sbin/nologin "$SOAK_USER"
fi
# The bundle tree stays root-owned. pypiron-soak-refresh.service runs as root
# (it installs units into /etc and restarts services) and re-executes
# fetch-bundle.sh and this script from $DEST; a soak-writable tree would let a
# compromised soak account rewrite those and win root on the next refresh timer.
# The soak-run units (vopr, reporter) only need read+exec here — their sole local
# write is report.py's /var/tmp fallback — so nothing under $DEST is soak-owned.
# Asserted here, not just assumed: this script is the one thing every path into a
# refreshed bundle runs (cloud-init, and fetch-bundle.sh on every update), so
# normalizing ownership here closes the window where an older extractor left the
# tree soak-writable for one refresh cycle.
chown -R root:root "$DEST"
chmod +x "$DEST"/vopr "$DEST"/*.sh "$DEST"/report.py
usermod -aG systemd-journal "$SOAK_USER" # the reporter reads the soak journal

if [ ! -f /etc/pypiron-soak/soak.env ]; then
    echo "WARNING: /etc/pypiron-soak/soak.env missing — the reporter cannot write"
    echo "         findings to S3 (they fall back to a local file). Provide at least"
    echo "         PYPIRON_SOAK_S3_BUCKET." >&2
fi

install -m 0644 "$DEST"/pypiron-soak@.service "$UNIT_DIR"/
install -m 0644 "$DEST"/pypiron-soak-reporter.service "$UNIT_DIR"/
install -m 0644 "$DEST"/pypiron-soak-refresh.service "$UNIT_DIR"/
install -m 0644 "$DEST"/pypiron-soak-refresh.timer "$UNIT_DIR"/
systemctl daemon-reload

instances=()
for i in $(seq 0 $((CORES - 1))); do
    instances+=("pypiron-soak@${i}.service")
done
systemctl enable --now "${instances[@]}"
systemctl enable --now pypiron-soak-reporter.service
systemctl enable --now pypiron-soak-refresh.timer

echo "soak up on $CORES core(s):"
systemctl --no-pager --no-legend list-units 'pypiron-soak*' | sed 's/^/  /'
