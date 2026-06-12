# Benchmark Results

Append-only. Every number carries its commit and hardware; numbers without
provenance don't go in this file. Scenarios and targets are defined in
[BENCHMARKS.md](BENCHMARKS.md).

## The meter series

One row per meter-suite run on the reference rig (`t4g.small` unlimited +
same-region S3, presigned redirects on — see BENCHMARKS.md). The suite never
changes shape, so any two rows are directly comparable. Run #0 is the
unoptimized baseline.

| # | Date | Commit | R1 idx rps | R3 304 rps | R2-lite rps | R6 302 rps | R7 meta rps | W3 visible p99 | W4 sync p99 | W1 100MB wall / RSS | W1-torch 900MB |
|---|------|--------|-----------|-----------|------------|-----------|------------|---------------|------------|--------------------|----------------|

<!-- Append rows only. W1-torch records pass/FAIL(reason) until it passes. -->

## Improvements log

Every landed optimization, paired with the meter runs that bracket it.

| Date | Change (commit) | Benchmark moved | Before → After |
|------|-----------------|-----------------|----------------|

## Full run details

One subsection per benchmark session (meter runs, big-box runs, scale runs).
Newest last. Include: date, commit, rig (instance types, region), corpus
preset, exact command, and the full metric output (rps, p50/p95/p99, server
peak RSS, CPU, storage op counts where logged).

<!--
Template:

### Run NNN — YYYY-MM-DD — <short description>

- Commit: `<sha>` · Rig: <server instance> + <loadgen instance>, <region> · Corpus: <preset>
- Suite: <meter | tier-1 | ...> · Command: `bench/run.sh <suite>`

| Scenario | rps | p50 | p95 | p99 | RSS peak | Notes |
|---|---|---|---|---|---|---|
-->
