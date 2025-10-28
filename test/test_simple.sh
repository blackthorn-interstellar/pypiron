#!/usr/bin/env bash
set -Eeuo pipefail

# --- Config (override if you like) -------------------------------------------
MINIO_CONTAINER="mvp-minio"
S3_BUCKET="s3pypi"
AWS_REGION="us-east-1"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

API_BIND="127.0.0.1:8080"
S3_ENDPOINT_URL="http://127.0.0.1:9000"
WORKER_INTERVAL_SECS=2               # fast regeneration for test
JOB_BATCH_SIZE=20
UPLOAD_CONFIRM_TIMEOUT_SECS=30

BASIC_AUTH_USER="twine"
BASIC_AUTH_PASS="secret"

# Paths
ROOT_DIR="$(pwd)"
BIN="${ROOT_DIR}/target/release/pypiron"
VENV="${ROOT_DIR}/.venv"
DIST_DIR="${ROOT_DIR}/dist"
DL_DIR="${ROOT_DIR}/downloaded"

# --- Cleanup -----------------------------------------------------------------
cleanup() {
  set +e
  echo "→ stopping server + minio"
  if [[ -n "${SERVER_PID:-}" ]]; then kill "${SERVER_PID}" 2>/dev/null || true; fi
  docker rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- Start MinIO (fake S3) ---------------------------------------------------
echo "→ starting MinIO ..."
docker rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${MINIO_CONTAINER}" \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER="${MINIO_ACCESS_KEY}" \
  -e MINIO_ROOT_PASSWORD="${MINIO_SECRET_KEY}" \
  minio/minio server /data --console-address ":9001" >/dev/null

echo "→ waiting for MinIO readiness ..."
until curl -sf "http://127.0.0.1:9000/minio/health/ready" >/dev/null; do sleep 1; done

echo "→ creating bucket ${S3_BUCKET} ..."
docker run --rm --network host -e "MC_HOST_local=http://${MINIO_ACCESS_KEY}:${MINIO_SECRET_KEY}@127.0.0.1:9000" \
  minio/mc mb --ignore-existing "local/${S3_BUCKET}" >/dev/null

# --- Build the Rust server if needed ----------------------------------------
if [[ ! -x "${BIN}" ]]; then
  echo "→ building server (release) ..."
  cargo build --release >/dev/null
fi

# --- Run the server ----------------------------------------------------------
echo "→ starting server on http://${API_BIND}"
export S3_BUCKET AWS_REGION
export S3_ENDPOINT_URL S3_FORCE_PATH_STYLE=true
export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}"
export BASIC_AUTH_USER BASIC_AUTH_PASS
export BIND_ADDR="${API_BIND}"
export WORKER_INTERVAL_SECS JOB_BATCH_SIZE UPLOAD_CONFIRM_TIMEOUT_SECS
RUST_LOG=info "${BIN}" &
SERVER_PID=$!

echo "→ waiting for server to accept TCP ..."
HOST="${API_BIND%:*}"
PORT="${API_BIND#*:}"
until (exec 3<>"/dev/tcp/${HOST}/${PORT}") 2>/dev/null; do sleep 0.5; done

# --- Create a tiny Python package & build a wheel ----------------------------
echo "→ creating demo Python package ..."
rm -rf "${DIST_DIR}" "${DL_DIR}" demo_pkg "${VENV}"
python3 -m venv "${VENV}"
source "${VENV}/bin/activate"
python -m pip -q install --upgrade pip build >/dev/null

mkdir -p demo_pkg/src/demo_pkg
cat > demo_pkg/src/demo_pkg/__init__.py <<'PY'
__all__ = ["hello"]
def hello(): return "world"
PY

cat > demo_pkg/pyproject.toml <<'TOML'
[build-system]
requires = ["setuptools>=67", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "demo-pkg"
version = "0.0.1"
description = "MVP smoke test package"
requires-python = ">=3.8"
TOML

echo "→ building wheel ..."
python -m build --wheel --outdir "${DIST_DIR}" demo_pkg >/dev/null
FILENAME="$(basename "$(ls -1 ${DIST_DIR}/*.whl)")"
echo "   built: ${FILENAME}"

# --- Upload via API (redirect -> pre-signed S3 PUT) --------------------------
echo "→ requesting pre-signed PUT (server POST, no body read) ..."
HDRS="$(mktemp)"
curl -is -u "${BASIC_AUTH_USER}:${BASIC_AUTH_PASS}" \
  -H "X-Filename: ${FILENAME}" \
  -X POST "http://${API_BIND}/" \
  -o /dev/null -D "${HDRS}"

UPLOAD_URL="$(awk '/^[Ll]ocation: /{print $2}' "${HDRS}" | tr -d '\r')"
test -n "${UPLOAD_URL}" || { echo "FAILED: no Location header from server"; cat "${HDRS}"; exit 1; }
echo "   presigned URL acquired."

echo "→ uploading artifact to S3 via presigned URL ..."
# Content-Type must match what the server used when presigning (application/octet-stream).
curl -sS -X PUT -H "Content-Type: application/octet-stream" \
  --upload-file "${DIST_DIR}/${FILENAME}" \
  "${UPLOAD_URL}" >/dev/null
echo "   upload done."

# --- Wait for worker to regenerate indexes -----------------------------------
PKG_PATH="http://${API_BIND}/simple/demo-pkg/"
echo "→ waiting for per-package index at ${PKG_PATH} ..."
for _ in {1..60}; do
  if curl -sf "${PKG_PATH}" >/dev/null; then break; fi
  sleep 1
done
curl -sf "${PKG_PATH}" >/dev/null || { echo "FAILED: package index not found"; exit 1; }

# --- Download from our server + public PyPI ---------------------------------
mkdir -p "${DL_DIR}"
echo "→ pip download from our server (demo-pkg==0.0.1) ..."
python -m pip -q download --no-deps -d "${DL_DIR}" \
  --index-url "http://127.0.0.1:8080/simple" \
  --trusted-host 127.0.0.1 \
  "demo-pkg==0.0.1"

echo "→ pip download from public PyPI via extra-index (flask) ..."
python -m pip -q download -d "${DL_DIR}" \
  --index-url "http://127.0.0.1:8080/simple" \
  --trusted-host 127.0.0.1 \
  --extra-index-url "https://pypi.org/simple" \
  "flask==3.0.0"

echo "→ downloaded files:"
ls -1 "${DL_DIR}"

echo "✅ SUCCESS: upload via server + S3 and downloads (private + public) verified."
echo "MinIO console: http://127.0.0.1:9001  (user: ${MINIO_ACCESS_KEY} / pass: ${MINIO_SECRET_KEY})"
sleep 60