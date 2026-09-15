#!/usr/bin/env bash
# Smoke-test a built pypiron image: the things packaging can break that the
# blackbox suite (which runs the bare binary) never sees.
#
#   1. the image is not fat           — compressed (pull) size under a budget
#   2. the binary loads on this base  — `--version` runs (libc/loader match)
#   3. the HEALTHCHECK works          — docker itself reports the container healthy
#   4. an upload lands                — the spool dir (/tmp) exists and the
#                                       runtime uid can write it
#   5. the proxy fetches from PyPI    — DNS + TLS trust with no system store
#
# pip is the client for 4 and 5, so both are real install round-trips.
#
# Usage: image-smoke.sh IMAGE PLATFORM MAX_MB
#   IMAGE     a tag in the local docker daemon (`docker buildx build --load`)
#   PLATFORM  linux/<arch>[/<variant>]; a non-native arch runs under QEMU binfmt
#   MAX_MB    fail if the image, gzipped as a registry stores it, exceeds this many MB
# Env:
#   EXPECT_VERSION  if set, `--version` must report exactly this version
#   PYTHON          interpreter that has pip (default: python3)
# Needs docker, curl, python3 with pip, and network to pypi.org for step 5.
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 IMAGE PLATFORM MAX_MB" >&2
  exit 2
fi
IMAGE=$1
PLATFORM=$2
MAX_MB=$3
PYTHON=${PYTHON:-python3}
name="pypiron-smoke-$$"
work=$(mktemp -d)

cleanup() {
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "--- container log ---" >&2
    docker logs "$name" >&2 2>&1 || true
  fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$rc"
}
trap cleanup EXIT

fail() {
  echo "::error::image smoke ($PLATFORM): $*" >&2
  exit 1
}

# 1. Size budget, measured as the pull: `docker save` gives raw layers on an
# overlay2 daemon and already-gzipped blobs on a containerd one, so gzip the
# stream and the number matches the registry either way.
bytes=$(docker save "$IMAGE" | gzip | wc -c | tr -d ' ')
mb=$((bytes / 1000000))
echo "size: ${mb} MB compressed (budget ${MAX_MB} MB)"
((bytes <= MAX_MB * 1000000)) || fail "image pulls at ${mb} MB, over the ${MAX_MB} MB budget — did a fat base creep in?"

# 2. The binary loads: the loader and libc the binary expects exist on this base.
got=$(docker run --rm --platform "$PLATFORM" "$IMAGE" --version)
echo "--version: $got"
[[ "$got" == pypiron\ * ]] || fail "--version printed '$got'"
if [[ -n "${EXPECT_VERSION:-}" ]]; then
  [[ "$got" == "pypiron ${EXPECT_VERSION} "* ]] || fail "--version reported '$got', expected ${EXPECT_VERSION}"
fi

# 3. Boot. Docker's own probe (the image's HEALTHCHECK, sped up) is the
# readiness signal, so a probe that cannot run fails here, not in production.
# The long start period keeps a slow emulated boot from counting as unhealthy.
docker run -d --name "$name" --platform "$PLATFORM" \
  --health-interval=2s --health-timeout=5s --health-start-period=120s --health-retries=3 \
  -p 127.0.0.1::8080 \
  -e PYPIRON_ADMIN_PASS=smoke \
  -e PYPIRON_PROXY_UPSTREAM=https://pypi.org \
  -e PYPIRON_ADVISORY_FEED= \
  "$IMAGE" >/dev/null
state=""
for _ in $(seq 1 120); do
  state=$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$name")
  case "$state" in
    "running healthy") break ;;
    running\ *) sleep 1 ;;
    *) fail "container is '$state'" ;;
  esac
done
[[ "$state" == "running healthy" ]] || fail "not healthy after 120s (state: $state)"
hostport=$(docker port "$name" 8080/tcp | head -1)
index="http://${hostport}/simple/"
echo "healthy at ${hostport}"

# 4. Upload a throwaway wheel (goes through the spool), then pip it back out.
"$PYTHON" - "$work" <<'PY'
import base64, hashlib, sys, zipfile
from pathlib import Path

work = Path(sys.argv[1])
name, version = "pypiron_smoke", "0.0.1"
di = f"{name}-{version}.dist-info"
files = {
    f"{name}.py": b'__version__ = "0.0.1"\n',
    f"{di}/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n".encode(),
    f"{di}/WHEEL": b"Wheel-Version: 1.0\nGenerator: image-smoke\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
}
record = [
    f"{p},sha256={base64.urlsafe_b64encode(hashlib.sha256(b).digest()).rstrip(b'=').decode()},{len(b)}"
    for p, b in files.items()
]
files[f"{di}/RECORD"] = ("\n".join(record + [f"{di}/RECORD,,"]) + "\n").encode()
with zipfile.ZipFile(work / f"{name}-{version}-py3-none-any.whl", "w", zipfile.ZIP_DEFLATED) as zf:
    for p, b in files.items():
        zf.writestr(p, b)
PY
wheel="$work/pypiron_smoke-0.0.1-py3-none-any.whl"
curl -fsS -o /dev/null -u admin:smoke \
  -F ':action=file_upload' -F 'protocol_version=1' -F "content=@${wheel}" \
  "http://${hostport}/legacy/" || fail "upload failed — no writable spool dir (/tmp) in the image?"

pip=("$PYTHON" -m pip --disable-pip-version-check --no-input download -q
  --no-deps --only-binary=:all: --no-cache-dir
  --index-url "$index" --trusted-host 127.0.0.1 -d "$work/dl")
"${pip[@]}" pypiron-smoke==0.0.1 || fail "pip could not install the uploaded wheel back from the image"
cmp -s "$wheel" "$work/dl/pypiron_smoke-0.0.1-py3-none-any.whl" || fail "the uploaded wheel came back different"
echo "upload round-trip ok"

# 5. Proxy a small pinned wheel from PyPI through the image: DNS, TLS trust
# (no system store on scratch — the roots are compiled in) and the spool again.
"${pip[@]}" six==1.17.0 || fail "pip could not fetch six through the proxy — DNS, TLS trust or spool?"
echo "proxy round-trip ok"

echo "ok: $IMAGE on $PLATFORM"
