#!/usr/bin/env bash
# Repeatable install-throughput benchmark for ONE pypiron build — a published
# release (default) or a local source build. This is the scripted, one-command
# version of the §14 "max sustained install throughput" run that produces the
# README chart's pypiron bar (dev/BENCHMARK_RESULTS.md §14).
#
# It wraps the multi-node rig (rig2.sh) + the oha install-mix ramp (mn_ramp.py):
# resolve a server image for the requested ref, (re)deploy it, serve Track 2
# (S3 + presigned redirect), ramp loadgens until the SERVER's CPU saturates, and
# write results/cmp-pypiron.json (the plot.py input).
#
#   ./benchmark.sh                  # latest released tag
#   ./benchmark.sh 0.0.7            # a specific release (downloads + sha-verifies the binary)
#   ./benchmark.sh local            # build the working tree from source (cargo-zigbuild)
#   ./benchmark.sh 0.0.7 --down     # tear the rig down when finished
#
# FINDING THE CEILING IS THE POINT. pypiron's index+302 path is so cheap that a
# small loadgen fleet saturates ITSELF (pulling wheel bytes from S3) long before
# the server's CPU — a rig-limited number that understates pypiron and isn't a
# ceiling at all (dev/BENCHMARK_RESULTS.md §14 vs §15). So the default fleet is
# sized to drive the default server to its CPU wall (4× c7i.8xlarge → a 2-vCPU
# r7i.large, per §15), and if a run still fails to saturate the server the script
# flags the result as a rig-limited LOWER BOUND and tells you to scale up — it
# never reports a rig-limited number as the ceiling.
#
# The rig is REUSED if one is already up; pass --up to force a fresh one. Knobs
# (rig2.sh's, via env):
#   RIG2_SERVER_TYPE  (def r7i.large)   the box customers run pypiron on
#   RIG2_LOADGEN_TYPE (def c7i.8xlarge), RIG2_LOADGENS (def 4)  the ceiling fleet
#   RIG2_LADDER       per-node oha concurrency ladder (def below)
# A bigger/faster server needs a bigger fleet to saturate — watch for the
# rig-limited warning and raise RIG2_LOADGENS / RIG2_LOADGEN_TYPE if it fires.
#
# Needs: docker+buildx, gh (release download), aws (rig). `local` also needs
# cargo-zigbuild + ziglang on PATH (see dev/BENCHMARK_INSTALL.md).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"

REPO_SLUG="${PYPIRON_REPO:-blackthorn-interstellar/pypiron}"
ARCH="${RIG_ARCH:-x86_64}"                       # the server arch (r7i.large = x86_64)
TRIPLE="${ARCH}-unknown-linux-gnu"               # glibc → distroless/cc base
BASE_IMG="gcr.io/distroless/cc-debian13:nonroot"
IMG_TAG="pypiron:bench-${ARCH}"
IMG_TGZ="/tmp/pypiron-${ARCH}.tgz"               # rig2.sh deploy loads this
TIER="${RIG_TIER:-lite}"
LADDER="${RIG2_LADDER:-2048,4096,8192,12288,16384}"   # §15 ceiling ladder (agg up to 64k)

# ---- args: [REF] [--up|--down] ------------------------------------------------
REF=""; FORCE_UP=0; TEARDOWN=0
for a in "$@"; do
  case "$a" in
    --up) FORCE_UP=1 ;;
    --down) TEARDOWN=1 ;;
    -*) echo "unknown flag: $a" >&2; exit 2 ;;
    *) REF="$a" ;;
  esac
done
[[ -n "$REF" ]] || REF="$(git -C "$REPO" describe --tags --abbrev=0)"
REF="${REF#v}"                                    # accept v0.0.7 or 0.0.7

# ---- 1. resolve the pypiron binary for REF, assemble the runtime image --------
# The Dockerfile is COPY-only (no RUN), so buildx assembles a linux/amd64 image
# on any host with NO QEMU. We feed it a prebuilt binary, a CA bundle (outbound
# TLS to S3), and an empty data/ — exactly what .github/workflows/docker.yml does.
echo "== resolve pypiron ${REF} (${TRIPLE})"
ctx="$(mktemp -d)"; mkdir -p "${ctx}/data"
trap 'rm -rf "$ctx"' EXIT
if [[ "$REF" == "local" ]]; then
  echo "-- building working tree from source (cargo-zigbuild)"
  # cargo-zigbuild finds zig via `python3 -m ziglang`; a local ziglang venv works.
  [[ -x /tmp/zigvenv/bin/python3 ]] && export PATH="/tmp/zigvenv/bin:$PATH"
  ( cd "$REPO" && cargo zigbuild --release --locked --target "$TRIPLE" )
  cp "$REPO/target/${TRIPLE}/release/pypiron" "${ctx}/pypiron"
else
  echo "-- downloading release v${REF} binary + SHA256SUMS"
  gh -R "$REPO_SLUG" release download "v${REF}" \
     -p "pypiron-${TRIPLE}.tar.gz" -p SHA256SUMS -D "$ctx" --clobber
  ( cd "$ctx" && grep "pypiron-${TRIPLE}.tar.gz" SHA256SUMS | shasum -a 256 -c - )
  tar -xzf "${ctx}/pypiron-${TRIPLE}.tar.gz" -C "$ctx"
  bin="$(find "$ctx" -type f -name pypiron | head -1)"
  [[ -n "$bin" ]] || { echo "no pypiron binary in release tarball" >&2; exit 1; }
  [[ "$bin" == "${ctx}/pypiron" ]] || cp "$bin" "${ctx}/pypiron"
fi
chmod +x "${ctx}/pypiron"
cp "$REPO/Dockerfile" "${ctx}/Dockerfile"
# certifi's Mozilla bundle includes the Amazon roots S3 presents.
uv run --with certifi python -c "import certifi,shutil;shutil.copy(certifi.where(),'${ctx}/ca-certificates.crt')"
echo "== assemble ${IMG_TAG} (linux/${ARCH/x86_64/amd64})"
docker buildx build --platform "linux/${ARCH/x86_64/amd64}" --build-arg "BASE=${BASE_IMG}" \
  -t "$IMG_TAG" --load "$ctx" >/dev/null
docker save "$IMG_TAG" | gzip > "$IMG_TGZ"
echo "-- wrote ${IMG_TGZ} ($(du -h "$IMG_TGZ" | cut -f1))"

# ---- 2. rig: reuse a running one, else bring one up ---------------------------
export RIG2_SERVER_TYPE="${RIG2_SERVER_TYPE:-r7i.large}" \
       RIG2_SERVER_ARCH="$ARCH" RIG_ARCH="$ARCH" \
       RIG2_LOADGEN_TYPE="${RIG2_LOADGEN_TYPE:-c7i.8xlarge}" \
       RIG2_LOADGENS="${RIG2_LOADGENS:-4}" RIG_TIER="$TIER" \
       RIG2_SERVER_IMG="$IMG_TGZ"

rig_running() {
  [[ -f "${HERE}/.rig2.env" ]] || return 1
  local sid region st
  sid="$(grep -E '^export RIG2_SERVER_ID=' "${HERE}/.rig2.env" | cut -d= -f2)"
  region="$(grep -E '^export RIG_REGION=' "${HERE}/.rig2.env" | cut -d= -f2)"
  [[ -n "$sid" ]] || return 1
  st="$(aws ec2 describe-instances --region "${region:-us-east-1}" --instance-ids "$sid" \
        --query 'Reservations[].Instances[].State.Name' --output text 2>/dev/null || true)"
  [[ "$st" == "running" ]]
}

if [[ "$FORCE_UP" == 1 ]] || ! rig_running; then
  echo "== rig up (${RIG2_SERVER_TYPE} server + ${RIG2_LOADGENS}× ${RIG2_LOADGEN_TYPE})"
  "${HERE}/rig2.sh" up
else
  echo "== reusing running rig ($(grep RIG2_SERVER_IP "${HERE}/.rig2.env" | cut -d= -f2))"
fi

# ---- 3. deploy the image, serve Track 2, ramp to the installs/sec ceiling -----
"${HERE}/rig2.sh" deploy
"${HERE}/rig2.sh" serve pypiron

# mn_ramp stops the ramp when the server's docker-stats CPU% (0..cores×100) hits
# --cpu-break. The mn_ramp default (92) assumes a 1-core box; scale it to the
# server's real core count so "saturated" means ~95% of ALL cores, not of one.
source "${HERE}/.rig2.env"
CORES="$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$RIG_KEY" \
  "ec2-user@${RIG2_SERVER_IP}" nproc 2>/dev/null | tr -d '[:space:]')"
CORES="${CORES:-1}"
CPU_BREAK="$(( CORES * 95 ))"
echo "== install-mix ramp (ladder ${LADDER}, cpu-break ${CPU_BREAK}% of ${CORES} cores)"
python3 "${HERE}/mn_ramp.py" --tier "$TIER" --ladder "$LADDER" --cpu-break "$CPU_BREAK" \
  --container pypiron --output "results/cmp-pypiron.json"

# Stamp the result, and VERIFY it is a real ceiling: if the server never neared
# saturation (peak CPU < 85% of its cores) the loadgen fleet was the bottleneck,
# so the number is a rig-limited lower bound, not a ceiling — say so loudly.
python3 - "${HERE}/results/cmp-pypiron.json" "$REF" "$RIG2_SERVER_TYPE" "$RIG2_LOADGENS" "$RIG2_LOADGEN_TYPE" "$CORES" <<'PY'
import json, sys
from pathlib import Path
path, ref, server, lg_n, lg_type, cores = sys.argv[1:7]
cores = int(cores)
p = Path(path)
d = json.loads(p.read_text())
ramp = d.get("ramp", [])
peak_cpu = max((s.get("server_cpu_pct", 0) or 0) for s in ramp) if ramp else 0.0
saturated = peak_cpu >= 0.85 * cores * 100
d["pypiron_version"] = ref
d["server_type"] = server
d["loadgen"] = f"{int(lg_n)}x {lg_type}"
d["server_cores"] = cores
d["peak_server_cpu_pct"] = round(peak_cpu, 1)
d["bound"] = "server-bound" if saturated else "rig-limited"
p.write_text(json.dumps(d, indent=2))
inst, rps = d["peak_installs_per_sec"], d["peak_agg_rps"]
print(f"\n  pypiron {ref}: peak {inst} installs/s ({rps} rps) on {server} "
      f"[{int(lg_n)}x {lg_type}]")
print(f"  server CPU peaked {peak_cpu:.0f}% of {cores * 100}% — "
      f"{'SATURATED (server-bound ceiling)' if saturated else 'NOT saturated'}")
if not saturated:
    print("\n  ⚠⚠  RIG-LIMITED — this is a LOWER BOUND, not pypiron's ceiling.")
    print("      The loadgen fleet saturated before the server did. Scale up:")
    print(f"      RIG2_LOADGENS={int(lg_n) * 2} RIG2_LOADGEN_TYPE={lg_type} "
          f"./benchmark.sh {ref}\n")
PY

[[ "$TEARDOWN" == 1 ]] && { echo "== teardown"; "${HERE}/rig2.sh" down; }
echo "== done: results/cmp-pypiron.json"
