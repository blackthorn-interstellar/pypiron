#!/usr/bin/env bash
# Integration smoke test for PypIron using the DISK backend.
# - Starts the server pointing at a temp data dir
# - Downloads a wheel from PyPI and uploads it using uv
# - Waits for the background worker to index
# - Verifies /simple and /files endpoints

set -euo pipefail

# ----------------------------- Constants ------------------------------------
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly DATA_DIR="$(mktemp -d -t pypiron-data.XXXXXX)"

readonly BASIC_USER="admin"
readonly BASIC_PASS="secret"

# Using 'six' as it's a small, stable, pure-python package
readonly PACKAGE="six"
readonly VERSION="1.16.0"
readonly ARTIFACT_NAME="${PACKAGE}-${VERSION}-py2.py3-none-any.whl"
readonly PYPI_URL="https://files.pythonhosted.org/packages/d9/5a/e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/${ARTIFACT_NAME}"
readonly ACCEPT_PEP691="application/vnd.pypi.simple.v1+json"

# ----------------------------- Helpers --------------------------------------
require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

hash256() {
  # Prints sha256($1) to stdout
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "Neither sha256sum nor shasum found" >&2
    exit 1
  fi
}

find_free_port() {
  # Use Python to find a free localhost port
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

abort() {
  echo "ERROR: $1" >&2
  if [[ -n "${LOG_FILE:-}" && -f "${LOG_FILE}" ]]; then
    echo "Last 100 server log lines:" >&2
    tail -n 100 "${LOG_FILE}" >&2 || true
  fi
  exit 1
}

cleanup() {
  set +e
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    # give it a moment to shutdown gracefully
    sleep 0.2
    kill -9 "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${DATA_DIR}"
}
trap cleanup EXIT INT TERM

# ----------------------------- Preflight ------------------------------------
require_cmd curl
require_cmd uv
require_cmd python3

readonly PORT="$(find_free_port)"
readonly BIND_ADDR="127.0.0.1:${PORT}"
readonly LOG_FILE="${DATA_DIR}/server.log"

readonly BIN="${ROOT_DIR}/target/debug/pypiron"
[[ -x "${BIN}" ]] || abort "pypiron binary not found at ${BIN}; build with 'cargo build'."

# ----------------------------- Start server ---------------------------------
echo "Starting pypiron at http://${BIND_ADDR} (disk backend; data-dir=${DATA_DIR})"
"${BIN}" \
  --bind-addr "${BIND_ADDR}" \
  --data-dir "${DATA_DIR}" \
  --basic-auth-user "${BASIC_USER}" \
  --basic-auth-pass "${BASIC_PASS}" \
  --worker-interval-secs 1 \
  --job-batch-size 20 \
  > >(tee "${LOG_FILE}") 2>&1 &
SERVER_PID=$!

# Give the process a moment to start
sleep 0.2
kill -0 "${SERVER_PID}" 2>/dev/null || abort "Server process failed to start (PID ${SERVER_PID})."

# Wait for server readiness
echo "Waiting for server to be ready on port ${PORT}..."
for i in {1..100}; do
  kill -0 "${SERVER_PID}" 2>/dev/null || abort "Server process died during startup."
  if curl -sSf "http://${BIND_ADDR}/simple/index.json" >/dev/null; then
    echo "Server is ready!"
    break
  fi
  sleep 0.1
  [[ $i -eq 100 ]] && abort "Server did not become ready."
done

# ----------------------------- Upload a real wheel --------------------------
readonly WHEEL_PATH="${DATA_DIR}/${ARTIFACT_NAME}"
echo "Downloading ${ARTIFACT_NAME} from PyPI..."
curl -sSLf "${PYPI_URL}" -o "${WHEEL_PATH}" || abort "Failed to download wheel from PyPI."
[[ -f "${WHEEL_PATH}" ]] || abort "Wheel file missing after download."

# Compute hash for later verification
readonly ORIG_SHA="$(hash256 "${WHEEL_PATH}")"
echo "Downloaded wheel hash: ${ORIG_SHA}"

# Upload using uv publish
echo "Uploading package via uv publish..."
uv publish \
  --publish-url "http://${BIND_ADDR}/legacy/" \
  --username "${BASIC_USER}" \
  --password "${BASIC_PASS}" \
  "${WHEEL_PATH}"

# ----------------------------- Wait for indexing ----------------------------
echo "Waiting for background indexing..."
for i in {1..100}; do
  if curl -sSf "http://${BIND_ADDR}/simple/${PACKAGE}/index.json" \
      -H "Accept: ${ACCEPT_PEP691}" | grep -q "${ARTIFACT_NAME}"; then
    break
  fi
  sleep 0.2
  [[ $i -eq 100 ]] && abort "Timed out waiting for package index."
done

# Verify global index lists the project
echo "Verifying global simple index includes '${PACKAGE}'..."
curl -sSf "http://${BIND_ADDR}/simple/index.json" \
  -H "Accept: ${ACCEPT_PEP691}" \
  | grep -q "\"name\":\"${PACKAGE}\"" \
  || abort "Global index did not include project '${PACKAGE}'."

# ----------------------------- Download & verify ----------------------------
echo "Downloading artifact and verifying integrity..."
curl -sSf "http://${BIND_ADDR}/files/${PACKAGE}/${ARTIFACT_NAME}" -o "${DATA_DIR}/downloaded.whl"
readonly DOWN_SHA="$(hash256 "${DATA_DIR}/downloaded.whl")"

if [[ "${DOWN_SHA}" != "${ORIG_SHA}" ]]; then
  abort "Downloaded file hash mismatch. Expected: ${ORIG_SHA}  Got: ${DOWN_SHA}"
fi

# ----------------------------- Install & import -----------------------------
echo "Installing ${PACKAGE} from pypiron index using uv..."
readonly VENV_DIR="${DATA_DIR}/test-venv"
uv venv "${VENV_DIR}"

uv pip install \
  --python "${VENV_DIR}/bin/python" \
  --index-url "http://${BASIC_USER}:${BASIC_PASS}@${BIND_ADDR}/simple/" \
  --no-cache-dir \
  "${PACKAGE}==${VERSION}"

echo "Verifying installed package..."
"${VENV_DIR}/bin/python" -c "import ${PACKAGE}; print('${PACKAGE} imported successfully')" \
  || abort "Failed to import ${PACKAGE} after installation"

echo "OK: disk backend upload/index/download/install passed."