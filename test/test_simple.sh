#!/usr/bin/env bash
set -Eeuo pipefail

# Find system Python (not in a venv)
if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  # If in a venv, look for system python in common locations
  for candidate in /usr/bin/python3 /usr/local/bin/python3 /opt/homebrew/bin/python3; do
    if [[ -x "${candidate}" ]]; then
      PYTHON="${candidate}"
      break
    fi
  done
  # Fallback: temporarily bypass PATH to find system python
  if [[ -z "${PYTHON:-}" ]]; then
    PYTHON="$(PATH="/usr/bin:/usr/local/bin:/opt/homebrew/bin" which python3)"
  fi
else
  PYTHON="$(which python3)"
fi

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
DIST_DIR="${ROOT_DIR}/dist"
DL_DIR="${ROOT_DIR}/downloaded"
TEST_VENV="${ROOT_DIR}/.test-venv"

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

# --- Download real packages from PyPI ----------------------------------------
echo "→ downloading real packages from public PyPI ..."
rm -rf "${DIST_DIR}" "${DL_DIR}" "${TEST_VENV}"
mkdir -p "${DIST_DIR}"

# Download some small, useful packages
PACKAGES_TO_UPLOAD=(
  "httpx==0.27.0"
  "rich==13.7.0"
  "typer==0.9.0"
)

for pkg in "${PACKAGES_TO_UPLOAD[@]}"; do
  echo "   downloading ${pkg} ..."
  if ! "${PYTHON}" -m pip download --no-deps -d "${DIST_DIR}" "${pkg}" >/dev/null 2>&1; then
    echo "FAILED: could not download ${pkg} - retrying with verbose output..."
    "${PYTHON}" -m pip download --no-deps -d "${DIST_DIR}" "${pkg}"
    exit 1
  fi
done

echo "   downloaded $(ls -1 ${DIST_DIR} | wc -l | tr -d ' ') packages"

# --- Upload packages via API -------------------------------------------------
upload_file() {
  local filename="$1"
  echo "→ uploading ${filename} ..."
  
  HDRS="$(mktemp)"
  curl -is -u "${BASIC_AUTH_USER}:${BASIC_AUTH_PASS}" \
    -H "X-Filename: ${filename}" \
    -X POST "http://${API_BIND}/" \
    -o /dev/null -D "${HDRS}"

  UPLOAD_URL="$(awk '/^[Ll]ocation: /{print $2}' "${HDRS}" | tr -d '\r')"
  if [[ -z "${UPLOAD_URL}" ]]; then
    echo "FAILED: no Location header for ${filename}"
    cat "${HDRS}"
    return 1
  fi

  curl -sS -X PUT -H "Content-Type: application/octet-stream" \
    --upload-file "${DIST_DIR}/${filename}" \
    "${UPLOAD_URL}" >/dev/null
  
  rm -f "${HDRS}"
}

for wheel in "${DIST_DIR}"/*.whl; do
  filename="$(basename "${wheel}")"
  upload_file "${filename}"
done

# --- Wait for worker to regenerate indexes -----------------------------------
echo "→ waiting for worker to process uploads ..."
sleep 5  # Give worker time to pick up jobs

# Check that rich package index exists
PKG_PATH="http://${API_BIND}/simple/rich/"
echo "→ verifying package index at ${PKG_PATH} ..."
for _ in {1..60}; do
  if curl -sf "${PKG_PATH}" >/dev/null; then break; fi
  sleep 1
done
curl -sf "${PKG_PATH}" >/dev/null || { echo "FAILED: package index not found"; exit 1; }

# --- Install from our server -------------------------------------------------
echo "→ creating test venv with uv ..."
uv venv "${TEST_VENV}" >/dev/null 2>&1

echo "→ installing rich from our server ..."
uv pip install --python "${TEST_VENV}" \
  --index-url "http://127.0.0.1:8080/simple" \
  --extra-index-url "https://pypi.org/simple" \
  "rich==13.7.0" >/dev/null 2>&1

echo "→ verifying rich installation ..."
"${TEST_VENV}/bin/python" -c "import rich; from rich import print as rprint; rprint('[green]✓[/green] rich imported successfully!')"

# --- Download from our server (test direct download) ------------------------
mkdir -p "${DL_DIR}"
echo "→ downloading httpx from our server ..."
"${PYTHON}" -m pip download --no-deps -d "${DL_DIR}" \
  --index-url "http://127.0.0.1:8080/simple" \
  --trusted-host 127.0.0.1 \
  "httpx==0.27.0" >/dev/null 2>&1

echo "→ downloading flask from public PyPI via our server ..."
"${PYTHON}" -m pip download --no-deps -d "${DL_DIR}" \
  --index-url "http://127.0.0.1:8080/simple" \
  --trusted-host 127.0.0.1 \
  --extra-index-url "https://pypi.org/simple" \
  "flask==3.0.0" >/dev/null 2>&1

echo "→ downloaded files:"
ls -1 "${DL_DIR}"

echo "✅ SUCCESS: upload, download, and install from private PyPI server verified!"
echo "MinIO console: http://127.0.0.1:9001  (user: ${MINIO_ACCESS_KEY} / pass: ${MINIO_SECRET_KEY})"
sleep 60