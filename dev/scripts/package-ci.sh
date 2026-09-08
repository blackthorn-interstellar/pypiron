#!/usr/bin/env bash
# Build -> local upload -> clean install -> check the installed distribution.
# Usage: bash package-ci.sh PROJECT_DIRECTORY DISTRIBUTION_NAME SMOKE_TEST.py
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: bash package-ci.sh PROJECT_DIRECTORY DISTRIBUTION_NAME SMOKE_TEST.py" >&2
  exit 2
fi
project=$(cd "$1" && pwd)
distribution=$2
smoke_test=$(cd "$(dirname "$3")" && pwd)/$(basename "$3")
test -f "$project/pyproject.toml"
test -f "$smoke_test"
command -v uv >/dev/null
command -v python3 >/dev/null
command -v curl >/dev/null

work=$(mktemp -d "${TMPDIR:-/tmp}/pypiron-package-ci.XXXXXX")
server_pid=
cleanup() {
  result=$?
  trap - EXIT
  if [ -n "$server_pid" ]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [ "$result" -ne 0 ] && [ -f "$work/server.log" ]; then
    tail -60 "$work/server.log" >&2
  fi
  echo "Run files: $work"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Resolve uvx before starting the background process, so the PID belongs to
# pypiron itself. PYPIRON_BIN lets the repo's tests exercise the current build.
if [ -n "${PYPIRON_BIN:-}" ]; then
  server_bin=$(cd "$(dirname "$PYPIRON_BIN")" && pwd)/$(basename "$PYPIRON_BIN")
else
  server_bin=$(cd "$work" && uvx --from pypiron==0.0.17 -- which pypiron)
fi
"$server_bin" --version

uv build --wheel --out-dir "$work/dist" "$project"
uv venv --python 3.12 "$work/venv"
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
password=$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')
index="http://127.0.0.1:$port"

cd "$work"
# This temporary server must not inherit another deployment's bucket or
# credentials. Its complete configuration is supplied immediately below.
for setting in ${!PYPIRON_@}; do unset "$setting"; done
touch pypiron.toml
PYPIRON_ADMIN_PASS="$password" PYPIRON_ADVISORY_FEED='' \
  "$server_bin" serve --config "$work/pypiron.toml" \
  --bind-addr "127.0.0.1:$port" --data-dir "$work/data" \
  --admin-user admin --private-prefix "$distribution" \
  --proxy-upstream https://pypi.org > "$work/server.log" 2>&1 &
server_pid=$!
for ((attempt = 0; attempt < 100; attempt++)); do
  kill -0 "$server_pid"
  if curl -fsS "$index/ready" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -fsS "$index/ready" >/dev/null

UV_PUBLISH_USERNAME=admin UV_PUBLISH_PASSWORD="$password" \
  uv publish --publish-url "$index/legacy/" "$work"/dist/*.whl
# --no-cache makes the server serve the wheel; the fresh venv cannot inherit
# the source checkout. One default index supplies private AND public packages.
env -u UV_INDEX -u UV_EXTRA_INDEX_URL -u UV_INDEX_URL -u UV_FIND_LINKS -u UV_CONFIG_FILE \
  uv --no-config --no-cache pip install --python "$work/venv/bin/python" \
  --default-index "$index/simple/" "$distribution"
"$work/venv/bin/python" -I "$smoke_test"
