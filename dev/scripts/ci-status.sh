#!/usr/bin/env bash
# The latest master run of every workflow that has a cron — scheduled, pushed, or
# dispatched: result, when, which commit, and the link. A failed run also lists
# the jobs that failed.
# Usage: bash dev/scripts/ci-status.sh   (needs an authenticated `gh`)
set -euo pipefail
cd "$(dirname "$0")/../.."

for wf in $(grep -l -E '^  schedule:' .github/workflows/*.yml .github/workflows/*.yaml); do
  IFS=$'\t' read -r id name result when sha url < <(
    gh run list --workflow "$(basename "$wf")" --branch master --limit 1 \
      --json databaseId,workflowName,conclusion,status,createdAt,headSha,url \
      --jq '.[0] // {} | [.databaseId, .workflowName, (if (.conclusion // "") == "" then .status else .conclusion end), .createdAt[:16], .headSha[:7], .url] | @tsv'
  ) || true
  if [ -z "${id:-}" ]; then
    printf '%-10s %s (no run on master yet)\n' never "$(basename "$wf")"
    continue
  fi
  printf '%-10s %s  %s  %-16s %s\n' "$result" "${when/T/ }" "$sha" "$name" "$url"
  if [ "$result" = failure ]; then
    gh run view "$id" --json jobs \
      --jq '.jobs[] | select(.conclusion == "failure") | "           ✗ \(.name)"'
  fi
done
