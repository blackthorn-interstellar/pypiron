# Per-endpoint micro-benchmarks

Status: frozen 2026-07-22 (spec-first interview). Change this file before
changing the behavior it pins.

## Thesis

Every HTTP endpoint pypiron serves has a benchmark, and the benchmarks stay
honest at PyPI scale without slow setup. Regressions are caught two ways:
deterministic storage-op counts asserted in CI on every PR, and tracked
cold/warm latencies at a 50k-package tier (1.2M files) measured on demand. The
expensive state — a PyPI-shaped storage tree with rendered indexes — is
fabricated once, swept once, and cached; a tracked run starts in seconds.

## The two lanes

| | CI lane | Tracked lane |
|---|---|---|
| Runs | every PR, inside the normal blackbox suite | `make microbench`, on demand / nightly |
| Tree | ~300 packages, seeded fresh per run (<10s) | 50k packages / 1.2M files, cached under `.local/` |
| Asserts | exact storage-op counts per request, response-size sanity | nothing — writes JSON for run-to-run diffing |
| Timing | none (the old catastrophic rps floor in `test_perf.py` stays as-is) | cold + warm latency per endpoint, startup→ready, RSS |

## Constraints

1. **Total coverage, enforced.** Every route in the `src/app.rs` router has an
   entry in one canonical endpoint table. A CI test parses the router source and
   fails when a route lacks a table entry. Fallback/proxy handlers are explicit
   named entries.
2. **CI asserts only deterministic quantities.** Exact op counts and byte
   sanity; never tight wall-times. Op counts are measured as `/metrics` deltas
   over N serial requests against a quiesced server.
3. **Op counts are pinned exact, per state.** Each endpoint pins a cold-hit
   count (fresh process, first request) and a warm-hit count (steady state).
   Warm pins assert cache effectiveness (e.g. a cached index read = 0 storage
   ops). A new op per request is a conscious, visible diff.
4. **Tracked lane never gates.** Results are schema-stable JSON committed under
   `dev/bench/results/`, carrying tier, seed, git rev, and host. Humans diff.
5. **Fabricate, don't mirror.** Trees are 0-byte artifacts + real sidecars
   sampled deterministically from the real 780k-project corpus (the `scale.py`
   trick). One fabricator module, shared by both lanes and `scale.py` — no
   duplicate seeders.
6. **Seed once, sweep once, cache forever.** The tracked tier is cached
   post-sweep (indexes rendered), keyed by (tier, seed, tree-format version). A
   run never re-seeds or re-sweeps; a stale key deletes and rebuilds. CI's
   fresh-seed lane is the backstop that catches a stale local cache's format
   drift.
7. **The cache stays byte-pristine.** Read benches point the server at the
   cache directly. The write probe (upload → visible-in-index at scale)
   snapshots the exact files it will touch, restores them after, and verifies
   the restore; a dirty marker makes a crashed run refuse the cache and rebuild.
8. **Cold = fresh process, first hit.** In-memory caches empty; the OS page
   cache stays warm (accepted — this is the restart-in-prod cold, the one that
   matters operationally). Restarts are cheap because indexes are pre-built and
   the boot sweep is disabled.
9. **Bench servers run quiesced.** `--audit-on-boot false` plus day-long
   `--reconcile-interval-secs` / `--worker-interval-secs` (existing knobs; the
   worker nudge keeps uploads visible), no advisory feed, no upstream except
   the loopback fixture for proxy endpoints. Disk backend only; Python stdlib
   only; no Docker.
10. **The 43k-file monster package is force-included in the 50k tier** so
    worst-case first-hit render cost is deterministic, not sample luck.
11. **Latency measures the server, not the client.** Serial requests, ≥100 warm
    hits per endpoint, p50/p95/max reported. No rps/throughput claims — the
    install rig owns those.
12. **Full PyPI is a tier, not a fork.** Same harness, `--packages 780000`, run
    on the existing AWS rig occasionally. Local default is 50k (~4.6GB cache).

## Server dependencies (build items, ops-useful on their own)

- `pypiron_storage_ops_total{op=read|write|list|delete}` counters at the
  storage trait boundary, all backends, exposed on `/metrics`. Also the S3
  cost-visibility story. (`presign_get` is uncounted — blind local math, same
  reasoning as `ObservedStorage`.)
- Quiescing needs no new knob: `--audit-on-boot false` plus long
  `--reconcile-interval-secs` / `--worker-interval-secs` already exist; upload
  visibility survives via the worker nudge.

## Accepted risks

- Tree-format version is a manually bumped constant; forgetting it leaves a
  stale local cache. CI's fresh seed catches the drift loudly.
- Wall-times are host-relative; cross-host diffs are informational only.
- OS page cache is warm during "cold" hits, by design.

## Non-goals

No criterion/in-process Rust benches, no MinIO lane, no auto-bisection, no
per-PR timing gates, no throughput claims.
