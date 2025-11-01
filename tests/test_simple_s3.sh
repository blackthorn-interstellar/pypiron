#!/usr/bin/env bash
set -Eeuo pipefail

# Find system Python (not in a venv)
if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  for candidate in /usr/bin/python3 /usr/local/bin/python3 /opt/homebrew/bin/python3; do
    if [[ -x "${candidate}" ]]; then
      PYTHON="${candidate}"
      break
    fi
  done
  if [[ -z "${PYTHON:-}" ]]; then
    PYTHON="$(PATH="/usr/bin:/usr/local/bin:/opt/homebrew/bin" which python3)"
  fi
else
  PYTHON="$(which python3)"
fi

# --- Config ------------------------------------------------------------------
MINIO_CONTAINER="mvp-minio"
S3_BUCKET="s3pypi"
AWS_REGION="us-east-1"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
S3_ENDPOINT_URL="http://127.0.0.1:9000"

WORKER_INTERVAL_SECS=2
JOB_BATCH_SIZE=20

BASIC_AUTH_USER="twine"
BASIC_AUTH_PASS="secret"

# Paths
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT_DIR}/target/debug/pypiron"
DIST_DIR="${ROOT_DIR}/dist"
DL_DIR="${ROOT_DIR}/downloaded"
TEST_VENV="${ROOT_DIR}/.test-venv"

# --- Helpers -----------------------------------------------------------------
cleanup() {
  set +e
  echo "→ stopping server + minio"
  if [[ -n "${SERVER_PID:-}" ]]; then kill "${SERVER_PID}" 2>/dev/null || true; fi
  docker rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

find_free_port() {
  "${PYTHON}" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_http_ready() {
  local url="$1"
  for _ in {1..100}; do
    if curl -sf "$url" >/dev/null; then return 0; fi
    sleep 0.1
  done
  return 1
}

# --- Start MinIO (fake S3) ---------------------------------------------------
echo "→ starting MinIO ..."
docker rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${MINIO_CONTAINER}" \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER="${MINIO_ACCESS_KEY}" \
  -e MINIO_ROOT_PASSWORD="${MINIO_SECRET_KEY}" \
  minio/minio server /data --console-address ":9001" >/dev/null

echo "→ waiting for MinIO readiness ..."
until curl -sf "http://127.0.0.1:9000/minio/health/ready" >/dev/null; do sleep 0.5; done
echo "→ creating bucket ${S3_BUCKET} ..."
docker run --rm --network host \
  -e "MC_HOST_local=http://${MINIO_ACCESS_KEY}:${MINIO_SECRET_KEY}@127.0.0.1:9000" \
  minio/mc mb --ignore-existing "local/${S3_BUCKET}" >/dev/null

# --- Build server if needed --------------------------------------------------
if [[ ! -x "${BIN}" ]]; then
  echo "→ building server (release) ..."
  cargo build --release
fi

# --- Start server ------------------------------------------------------------
API_PORT="$(find_free_port)"
API_BIND="127.0.0.1:${API_PORT}"

echo "→ starting server on http://${API_BIND} (S3 backend)"
export PYPIRON_STORAGE="s3"
export PYPIRON_S3_BUCKET="${S3_BUCKET}"
export AWS_REGION
export PYPIRON_S3_ENDPOINT_URL="${S3_ENDPOINT_URL}"
export PYPIRON_S3_FORCE_PATH_STYLE="true"
export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}"
export PYPIRON_BASIC_AUTH_USER="${BASIC_AUTH_USER}"
export PYPIRON_BASIC_AUTH_PASS="${BASIC_AUTH_PASS}"
export PYPIRON_BIND_ADDR="${API_BIND}"
export PYPIRON_WORKER_INTERVAL_SECS="${WORKER_INTERVAL_SECS}"
export PYPIRON_JOB_BATCH_SIZE="${JOB_BATCH_SIZE}"

RUST_LOG=info "${BIN}" &
SERVER_PID=$!

echo "→ waiting for server HTTP readiness ..."
wait_http_ready "http://${API_BIND}/simple/index.json"

# --- Download a few wheels from public PyPI ----------------------------------
echo "→ downloading real packages from PyPI ..."
rm -rf "${DIST_DIR}" "${DL_DIR}" "${TEST_VENV}"
mkdir -p "${DIST_DIR}"
"${PYTHON}" -m pip download --no-deps -d "${DIST_DIR}" \
  "six" >/dev/null

# --- Upload packages via legacy endpoint -------------------------------------
echo "→ uploading wheels via legacy endpoint ..."
for wheel in "${DIST_DIR}"/*.whl; do
  uv publish \
    --publish-url "http://${API_BIND}/legacy/" \
    --username "${BASIC_AUTH_USER}" \
    --password "${BASIC_AUTH_PASS}" \
    "${wheel}"
done

# --- Wait for worker & verify index ------------------------------------------
echo "→ waiting for package 'six' to appear in index ..."
for _ in {1..100}; do
  if curl -sSf "http://${API_BIND}/simple/six/index.json" \
      -H "Accept: application/vnd.pypi.simple.v1+json" >/dev/null; then
    break
  fi
  sleep 0.2
done

# --- Install from our server -------------------------------------------------
echo "→ creating test venv with uv ..."
uv venv "${TEST_VENV}" >/dev/null 2>&1
TEST_PY="${TEST_VENV}/bin/python"

echo "→ installing six from our server ..."
uv pip install \
  --python "${TEST_PY}" \
  --index-url "http://${API_BIND}/simple/" \
  --no-cache-dir \
  "six" >/dev/null

echo "→ verifying import ..."
"${TEST_PY}" - <<'PY'
import six
print("six imported OK")
PY

# --- Direct download check ---------------------------------------------------
mkdir -p "${DL_DIR}"
echo "→ pip download six from our server ..."
"${PYTHON}" -m pip download --no-deps -d "${DL_DIR}" \
  --index-url "http://${API_BIND}/simple/" \
  --trusted-host 127.0.0.1 \
  "six" >/dev/null

# --- Show JSON API snippets --------------------------------------------------
echo ""
echo "=== Global index (http://${API_BIND}/simple/index.json) ==="
curl -s "http://${API_BIND}/simple/index.json" | "${PYTHON}" -m json.tool
echo ""
echo "=== Package index for 'six' (http://${API_BIND}/simple/six/index.json) ==="
curl -s "http://${API_BIND}/simple/six/index.json" | "${PYTHON}" -m json.tool
echo ""
echo "✅ SUCCESS: S3 backend upload/index/download/install verified!"