#!/usr/bin/env bash
# Runs inside a disposable container. Only prepare has external networking.
set -euo pipefail
mode=${1:?prepare, offline, or recover}
case "$mode" in prepare|offline|recover) ;; *) exit 2 ;; esac

server_pid=
cleanup() {
  result=$?
  trap - EXIT
  if [ -n "$server_pid" ]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [ "$result" -ne 0 ]; then tail -60 "/demo/$mode-server.log" >&2; fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

data=/demo/data
if [ "$mode" = recover ]; then
  test ! -e /demo/restored
  cp -a /demo/data /demo/restored
  data=/demo/restored
  echo "Copied the stopped server's complete data directory."
  (cd "$data" && sha256sum --check /demo/wheel-sha256.txt)
  pypiron verify-index --data-dir "$data"
fi

# Advisory refresh is disabled for this bounded experiment; the embedded
# malware block set and default seven-day release cooldown remain enabled.
export PYPIRON_ADVISORY_FEED=''
export PYPIRON_ADMIN_PASS='local-demo-only'
pypiron serve --bind-addr 127.0.0.1:8080 --data-dir "$data" \
  --private-prefix pypiron-ci-example --proxy-upstream https://pypi.org \
  > "/demo/$mode-server.log" 2>&1 &
server_pid=$!
python - <<'PY'
import time
import urllib.error
import urllib.request

for attempt in range(100):
    try:
        urllib.request.urlopen("http://127.0.0.1:8080/ready", timeout=1).close()
        break
    except (urllib.error.URLError, TimeoutError):
        time.sleep(0.1)
else:
    raise SystemExit("pypiron did not become ready")
PY

if [ "$mode" = prepare ]; then
  pypiron --version
  UV_PUBLISH_USERNAME=admin UV_PUBLISH_PASSWORD="$PYPIRON_ADMIN_PASS" \
    uv publish --publish-url http://127.0.0.1:8080/legacy/ /wheels/*.whl
  echo 'Warm the private wheel and its public dependency through pypiron.'
else
  # The host also checks Docker's NetworkMode. Here we independently show that
  # a public connection fails inside the very container doing the install.
  python - <<'PY'
import socket

try:
    socket.create_connection(("1.1.1.1", 443), timeout=2).close()
except OSError:
    print("External connection: unavailable (Docker --network none)")
else:
    raise SystemExit("FAIL: external networking is still available")
PY
fi

# Each run gets its own /tmp and no uv cache. The image contains the server
# and built wheel, but neither the example nor six is installed in this venv.
uv venv --python /usr/local/bin/python /tmp/consumer
echo 'Fresh venv. Client cache disabled. Only index: local pypiron.'
uv --no-config --no-cache pip install --python /tmp/consumer/bin/python \
  --default-index http://127.0.0.1:8080/simple/ pypiron-ci-example==0.1.0
/tmp/consumer/bin/python -I /example/check_installed.py
if [ "$mode" = prepare ]; then
  (cd "$data" && find packages -type f -name '*.whl' -exec sha256sum {} +) \
    > /demo/wheel-sha256.txt
fi
if [ "$mode" != prepare ]; then
  # Missing public packages must fail. This is the negative control: we did
  # not accidentally leave the public index available as a fallback.
  if UV_HTTP_RETRIES=0 UV_HTTP_TIMEOUT=2 \
      uv --no-config --no-cache pip install --python /tmp/consumer/bin/python \
      --default-index http://127.0.0.1:8080/simple/ --no-deps \
      certifi==2025.1.31 >/demo/uncached.log 2>&1; then
    echo 'FAIL: an unprepared package unexpectedly installed.' >&2
    exit 1
  fi
  # Do not count a CLI error as evidence of offline behavior.
  if ! grep -Eq 'No solution found|Failed to fetch|Failed to download' /demo/uncached.log; then
    cat /demo/uncached.log >&2
    exit 1
  fi
  echo 'PASS: an unprepared public package cannot install.'
fi
echo "PASS: $mode"
