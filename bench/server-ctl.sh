#!/usr/bin/env bash
# (Re)start the pypiron server in a meter mode: default | sync | proxy.
# Reads storage/auth env from $PYPIRON_ENV_FILE (default bench/.server-env),
# binary path from $PYPIRON_BIN (default target/release/pypiron).
# Used directly and as meter.py --restart-cmd. Idempotent; waits for health.
set -euo pipefail

MODE="${1:-default}"
ENV_FILE="${PYPIRON_ENV_FILE:-$(dirname "$0")/.server-env}"

# Source the env file BEFORE computing defaults, so it can set
# PYPIRON_LOG/PYPIRON_SPOOL_DIR/etc.
# shellcheck disable=SC1090
source "$ENV_FILE"

BIN="${PYPIRON_BIN:-$(dirname "$0")/../target/release/pypiron}"
PIDFILE="${PYPIRON_PIDFILE:-/tmp/pypiron-bench.pid}"
LOG="${PYPIRON_LOG:-/tmp/pypiron-bench.log}"
PORT="${PYPIRON_PORT:-8080}"

# Mode switches as CLI flags, not env vars — explicit beats clap env parsing.
EXTRA_ARGS=()
case "$MODE" in
  default) EXTRA_ARGS+=(--artifact-delivery auto) ;;
  sync)    EXTRA_ARGS+=(--artifact-delivery auto --sync-uploads) ;;
  proxy)   EXTRA_ARGS+=(--artifact-delivery stream) ;;
  stop) ;;
  *) echo "usage: $0 {default|sync|proxy|stop}" >&2; exit 2 ;;
esac

if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  kill "$(cat "$PIDFILE")"
  for _ in $(seq 1 50); do
    kill -0 "$(cat "$PIDFILE")" 2>/dev/null || break
    sleep 0.1
  done
  kill -9 "$(cat "$PIDFILE")" 2>/dev/null || true
fi
rm -f "$PIDFILE"
[[ "$MODE" == "stop" ]] && exit 0

export PYPIRON_BIND_ADDR="0.0.0.0:${PORT}"
export PYPIRON_WORKER_INTERVAL_SECS="${PYPIRON_WORKER_INTERVAL_SECS:-1}"
[[ -n "${PYPIRON_SPOOL_DIR:-}" ]] && mkdir -p "$PYPIRON_SPOOL_DIR"
# Fresh log per restart — a benchmark session must never fill the disk (or
# worse, a tmpfs) with the previous mode's access log.
: >| "$LOG"
nohup "$BIN" serve "${EXTRA_ARGS[@]}" >>"$LOG" 2>&1 &
echo $! > "$PIDFILE"

for _ in $(seq 1 120); do
  if curl -fsS -o /dev/null "http://127.0.0.1:${PORT}/simple/"; then
    echo "pypiron up (mode=$MODE pid=$(cat "$PIDFILE"))"
    exit 0
  fi
  kill -0 "$(cat "$PIDFILE")" 2>/dev/null || { echo "server died; tail of $LOG:" >&2; tail -20 "$LOG" >&2; exit 1; }
  sleep 0.5
done
echo "server failed to become healthy" >&2
exit 1
