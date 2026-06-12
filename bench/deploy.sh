#!/usr/bin/env bash
# Build the release binary on the loadgen box (16 cores; the t4g.small can't),
# push it + server-ctl.sh to the server box, install oha + meter.py + helper
# scripts on the loadgen. Run after aws-up.sh.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/.rig.env"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$RIG_KEY")
LG="ec2-user@${RIG_LOADGEN_IP}"
SV="ec2-user@${RIG_SERVER_IP}"

wait_ssh() {
  for _ in $(seq 1 60); do
    ssh "${SSH_OPTS[@]}" -o ConnectTimeout=5 "$1" true 2>/dev/null && return 0
    sleep 5
  done
  echo "ssh to $1 never came up" >&2; exit 1
}
wait_ssh "$LG"; wait_ssh "$SV"

echo "== syncing source to loadgen"
rsync -az -e "ssh ${SSH_OPTS[*]}" --delete \
  --exclude target --exclude .git --exclude data --exclude downloaded --exclude dist \
  --exclude bench/.keys --exclude bench/.rig.env --exclude bench/results \
  "${REPO}/" "${LG}:pypiron/"

echo "== building on loadgen + installing oha"
ssh "${SSH_OPTS[@]}" "$LG" 'bash -s' <<'EOS'
set -euo pipefail
sudo dnf install -y -q gcc git python3 >/dev/null
if ! command -v cargo >/dev/null; then
  curl -fsS https://sh.rustup.rs | sh -s -- -y -q --profile minimal
fi
source "$HOME/.cargo/env"
cd pypiron && cargo build --release 2>&1 | tail -2
command -v oha >/dev/null || cargo install oha --locked 2>&1 | tail -1
EOS

echo "== pushing binary + ctl script to server"
scp "${SSH_OPTS[@]}" "${RIG_KEY}" "${LG}:.ssh/rig.pem" >/dev/null
# Upload to a temp name then mv: scp onto a *running* executable fails with
# ETXTBSY; mv swaps the inode and leaves the running process undisturbed.
ssh "${SSH_OPTS[@]}" "$LG" "chmod 600 .ssh/rig.pem && \
  scp -o StrictHostKeyChecking=no -i .ssh/rig.pem \
    pypiron/target/release/pypiron \
    ec2-user@${RIG_SERVER_PRIVATE_IP}:pypiron.new && \
  scp -o StrictHostKeyChecking=no -i .ssh/rig.pem \
    pypiron/bench/server-ctl.sh \
    ec2-user@${RIG_SERVER_PRIVATE_IP}: && \
  ssh -o StrictHostKeyChecking=no -i .ssh/rig.pem \
    ec2-user@${RIG_SERVER_PRIVATE_IP} 'mv -f pypiron.new pypiron'"

echo "== writing server env + loadgen helpers"
ssh "${SSH_OPTS[@]}" "$SV" "cat > .server-env" <<EOF
export PYPIRON_STORAGE=s3
export PYPIRON_S3_BUCKET=${RIG_BUCKET}
export AWS_REGION=${RIG_REGION}
export PYPIRON_UPLOADER_USER=admin
export PYPIRON_UPLOADER_PASS=secret
export PYPIRON_ADMIN_USER=admin
export PYPIRON_ADMIN_PASS=secret
# AL2023 /tmp is a RAM-backed tmpfs: keep the log and upload spool on disk.
export PYPIRON_LOG=/home/ec2-user/pypiron-bench.log
export PYPIRON_SPOOL_DIR=/home/ec2-user/spool
EOF
ssh "${SSH_OPTS[@]}" "$SV" "chmod +x server-ctl.sh"

ssh "${SSH_OPTS[@]}" "$LG" "cat > restart.sh && chmod +x restart.sh" <<EOF
#!/usr/bin/env bash
ssh -o StrictHostKeyChecking=no -i ~/.ssh/rig.pem ec2-user@${RIG_SERVER_PRIVATE_IP} \
  "PYPIRON_ENV_FILE=~/.server-env PYPIRON_BIN=~/pypiron ./server-ctl.sh \$1"
EOF
ssh "${SSH_OPTS[@]}" "$LG" "cat > rss.sh && chmod +x rss.sh" <<EOF
#!/usr/bin/env bash
ssh -o StrictHostKeyChecking=no -i ~/.ssh/rig.pem ec2-user@${RIG_SERVER_PRIVATE_IP} \
  'ps -o rss= -p \$(cat /tmp/pypiron-bench.pid) 2>/dev/null'
EOF

echo "== deploy complete"
