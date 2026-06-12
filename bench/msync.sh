#!/usr/bin/env bash
# M1/M2/M3 sync-throughput scenarios (run on the loadgen box).
#   msync.sh m1-direct <bucket>     # small-file fan-out, direct-to-S3
#   msync.sh m2-torch  <bucket>     # torch-class wheels, bandwidth-bound
#   msync.sh m3-http   <server-url> # same M1 list pushed over /legacy/
# Prints wall time + file count; the M-numbers are files/s and Gbps.
set -euo pipefail

MODE="$1"; TARGET="$2"
BIN="${PYPIRON_BIN:-$HOME/pypiron/target/release/pypiron}"
LOG="/tmp/msync-${MODE}.log"

M1_LIST=/tmp/m1-packages.txt
cat > "$M1_LIST" <<'EOF'
six
idna
certifi
chardet
charset-normalizer
urllib3
requests
click
packaging
pyparsing
attrs
toml
pytz
python-dateutil
typing-extensions
sniffio
h11
anyio
tenacity
tqdm
colorama
decorator
wrapt
zipp
filelock
platformdirs
distlib
pluggy
iniconfig
pathspec
EOF

run() {
  echo "== $MODE: $*" | tee "$LOG"
  local t0 t1
  t0=$(date +%s)
  "$@" 2>&1 | tee -a "$LOG" | grep -E "Syncing|failed|error" | tail -40
  t1=$(date +%s)
  WALL=$((t1 - t0))
  FILES=$(grep -oE '\([0-9]+ matching files selected\)' "$LOG" | grep -oE '[0-9]+' | paste -sd+ - | bc)
  echo "== ${MODE}: wall=${WALL}s files=${FILES:-0} files/s=$(echo "scale=1; ${FILES:-0}/${WALL}" | bc)"
}

case "$MODE" in
  m1-direct)
    run "$BIN" sync --packages-list "$M1_LIST" --only-wheels \
      --storage s3 --s3-bucket "$TARGET" ;;
  m2-torch)
    echo torch > /tmp/m2-packages.txt
    run "$BIN" sync --packages-list /tmp/m2-packages.txt --only-wheels \
      --python-tag cp312 --platform-tag 'manylinux*' \
      --storage s3 --s3-bucket "$TARGET" ;;
  m3-http)
    run "$BIN" sync --packages-list "$M1_LIST" --only-wheels \
      --to "$TARGET" --username admin --password secret ;;
  *) echo "usage: $0 {m1-direct|m2-torch|m3-http} <bucket-or-url>" >&2; exit 2 ;;
esac
