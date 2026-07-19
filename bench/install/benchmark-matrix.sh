#!/usr/bin/env bash
# Auto-searched install-throughput matrix: for each server, serve it on the shared
# rig and let mn_ramp AUTO-SEARCH its ceiling (no hand-tuned ladder), then
# regenerate the comparison chart. Every bar is found the same way — the search
# brackets each server's knee and bisects it, so a Python server (saturates at a
# few hundred conns) and pypiron (tens of thousands) are measured identically.
#
#   ./benchmark-matrix.sh pypiserver proxpi        # the two validated competitors
#   ./benchmark-matrix.sh pypiron pypiserver proxpi
#
# Assumes a rig is already UP + DEPLOYED (wheelhouse shipped). Bring one up with
# `./benchmark.sh local --up` (builds + deploys pypiron) or `rig2.sh up && rig2.sh
# deploy`. Writes results/cmp-<server>.json (plot.py input) per server.
#
# Per-server CPU-break note: pypiron and pypiserver use all cores (cores*95);
# proxpi is a single GIL-bound worker (~one core), so its break is ~100% flat.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
[[ -f "${HERE}/.rig2.env" ]] || { echo "no .rig2.env — bring a rig up first" >&2; exit 2; }
source "${HERE}/.rig2.env"
PRIV="$RIG2_SERVER_PRIV"
TIER="${RIG_TIER:-lite}"

# server -> "index_path port cpu_break c_start wheel_mode"  (cpu_break "SAT" => cores*95).
# wheel_mode: redirect for servers that deliver wheel bytes via a 302 to object storage
# (pypiron S3 presign, pypicloud S3 redirect) — the wheel class counts the verified 302,
# not the (rig-limited) byte pull; follow for servers that stream bytes through the node
# (pypiserver, devpi, bandersnatch, proxpi) — that IS their architectural boundary.
# c_start is per class: the Python app servers saturate at tiny concurrency, the
# nginx-fronted / Rust servers scale high.
declare -A CFG=(
  [pypiron]="/simple/ 8080 SAT 64 redirect"
  [pypiserver]="/simple/ 8080 SAT 4 follow"
  [proxpi]="/index/ 5000 100 4 follow"
  [devpi]="/root/pypi/+simple/ 8080 SAT 4 follow"
  [pypicloud]="/simple/ 8080 SAT 4 redirect"
  [bandersnatch]="/simple/ 8080 SAT 64 follow"
)

CORES="$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$RIG_KEY" \
  "ec2-user@${RIG2_SERVER_IP}" nproc 2>/dev/null | tr -d '[:space:]')"
CORES="${CORES:-2}"
SAT="$(( CORES * 95 ))"

servers=("$@")
[[ ${#servers[@]} -gt 0 ]] || servers=(pypiserver proxpi)

for s in "${servers[@]}"; do
  cfg="${CFG[$s]:-}"
  [[ -n "$cfg" ]] || { echo "no matrix config for '$s'" >&2; exit 2; }
  read -r idx port cpub cstart wmode <<<"$cfg"
  [[ "$cpub" == SAT ]] && cpub="$SAT"
  echo "==== ${s}: serve + auto-search  (index ${idx}, port ${port}, cpu-break ${cpub}%, c-start ${cstart}, wheel-mode ${wmode}) ===="
  "${HERE}/rig2.sh" serve "$s"
  python3 "${HERE}/mn_ramp.py" --tier "$TIER" --container "$s" \
    --index-url "http://${PRIV}:${port}${idx}" --cpu-break "$cpub" --c-start "$cstart" \
    --wheel-mode "$wmode" --output "results/cmp-${s}.json"
done

echo "== regenerate chart from results/cmp-*.json"
python3 "${HERE}/plot.py"
echo "== matrix done: ${servers[*]}"
