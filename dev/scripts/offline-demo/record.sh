#!/usr/bin/env bash
# Record real offline/recovery runs; never kill another session's servers.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../../.." && pwd)
command -v vhs >/dev/null
docker build -f "$repo/dev/scripts/offline-demo/Dockerfile" -t pypiron-offline-demo:0.0.17 "$repo"
export PYPIRON_DEMO_WORK
PYPIRON_DEMO_WORK=$(mktemp -d "${TMPDIR:-/tmp}/pypiron-offline-demo.XXXXXX")
export PYPIRON_DEMO_REPO="$repo"
bash "$repo/dev/scripts/offline-demo/run.sh" prepare
mkdir -p "$repo/.local/offline-demo"
cd "$repo"
vhs dev/scripts/offline-demo/demo.tape
# VHS does not propagate the exit codes of commands typed into its shell.
# Check the actual logs before accepting the recording as a passing demo.
grep -Fx 'PASS: offline' "$PYPIRON_DEMO_WORK/offline-transcript.txt"
grep -Fx 'PASS: recover' "$PYPIRON_DEMO_WORK/recover-transcript.txt"
echo "Recording: $repo/.local/offline-demo/offline-demo.mp4"
echo "Evidence: $PYPIRON_DEMO_WORK"
