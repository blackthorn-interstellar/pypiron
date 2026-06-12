#!/usr/bin/env bash
# Run the meter suite on the reference rig and pull results back.
# Usage: run-baseline.sh [label]   (label defaults to "baseline")
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "${HERE}/.rig.env"
LABEL="${1:-baseline}"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$RIG_KEY")
LG="ec2-user@${RIG_LOADGEN_IP}"

echo "== starting server (default mode)"
ssh "${SSH_OPTS[@]}" "$LG" "./restart.sh default"

BASE="http://${RIG_SERVER_PRIVATE_IP}:8080"
echo "== seeding corpus (full torch preset; idempotent-ish: re-uploads are rejected, fresh bucket expected)"
ssh "${SSH_OPTS[@]}" "$LG" "python3 pypiron/bench/meter.py seed --base-url ${BASE}"

echo "== running meter suite"
ssh "${SSH_OPTS[@]}" "$LG" "source ~/.cargo/env 2>/dev/null || true; \
  cd pypiron && python3 bench/meter.py run \
    --base-url ${BASE} \
    --duration 30s --connections 64 \
    --rig 't4g.small(unlimited)+S3 us-east-1, loadgen c7gn.4xlarge' \
    --restart-cmd /home/ec2-user/restart.sh \
    --rss-cmd /home/ec2-user/rss.sh \
    --commit $(git -C "${HERE}/.." rev-parse --short HEAD) \
    --output /home/ec2-user/${LABEL}.json" | tee "${HERE}/results/${LABEL}.log"

mkdir -p "${HERE}/results"
scp "${SSH_OPTS[@]}" "${LG}:${LABEL}.json" "${HERE}/results/${LABEL}.json"
echo "== results in bench/results/${LABEL}.json"
