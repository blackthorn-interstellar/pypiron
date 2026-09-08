#!/usr/bin/env bash
# Usage: bash dev/scripts/offline-demo/run.sh [prepare|offline|recover]
# With no argument, build and run the complete experiment.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../../.." && pwd)
image=pypiron-offline-demo:0.0.17

if [ "$#" -eq 0 ]; then
  docker build -f "$repo/dev/scripts/offline-demo/Dockerfile" -t "$image" "$repo"
  work=$(mktemp -d "${TMPDIR:-/tmp}/pypiron-offline-demo.XXXXXX")
  export PYPIRON_DEMO_WORK="$work"
  for mode in prepare offline recover; do bash "$0" "$mode"; done
  echo "Evidence and both data directories: $work"
  exit 0
fi

mode=$1
case "$mode" in prepare|offline|recover) ;; *) echo 'Expected prepare, offline, or recover.' >&2; exit 2 ;; esac
work=${PYPIRON_DEMO_WORK:?Set PYPIRON_DEMO_WORK to an empty scratch directory; see dev/OFFLINE_DEMO.md}
test -d "$work"
network=none
if [ "$mode" = prepare ]; then
  test ! -e "$work/data"
  network=bridge
else
  test -d "$work/data/packages"
fi

container=$(docker create --network "$network" --mount "type=bind,src=$work,dst=/demo" "$image" "$mode")
cleanup() {
  result=$?
  trap - EXIT
  docker rm -f "$container" >/dev/null
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
actual=$(docker inspect --format '{{.HostConfig.NetworkMode}}' "$container")
test "$actual" = "$network"
echo "Docker network: $actual"
docker start -a "$container"
test "$(docker inspect --format '{{.State.ExitCode}}' "$container")" = 0
