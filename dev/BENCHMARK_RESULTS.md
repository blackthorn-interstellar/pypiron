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
| 0 | 2026-06-12 | `b79dd16` | 2,059 | 2,078 | 227 | 3,426 | 1,433 | 58.3s | 10.6s | 2.59s / 258MB | **FAIL (server OOM-killed)** |
| 1 | 2026-06-12 | `b79dd16`+P1 | **82,231** | **88,399** | 881¹ | 10,092 | 1,406 | **1.57s** | 10.5s² | 4.91s³ / **36.7MB** | **PASS 18.9s / 15.5MB RSS** |
| 2 | 2026-06-12 | `b79dd16`+P1b | 77,673 | 86,774 | 853¹ | **74,781** | **81,312** | 1.64s | **2.29s, 0/10 fail** | 2.1s / 52.2MB | **PASS 19.5s / 22MB** |
| 3 | 2026-06-12 | `9c60027` | 76,346 | 78,162 | **4,283**⁴ | 71,120 | 75,068 | 1.69s | **1.83s, 0/10 fail** | 1.68s / 51.7MB | **PASS 14.6s / 53MB** |
| 4 | 2026-06-12 | nudge+Bytes | 76,458 | 76,049 | 4,268 | 69,479 | 72,345 | **1.09s (p50 0.68s)** | **0.82s, 0/10 fail** | 2.1s / 75.6MB | PASS 19.4s⁵ / 50MB |

¹ R2 is now NIC-bound, not server-bound: 17.8 GB of index bytes in 30 s ≈ 4.7 Gbps, the t4g.small burst ceiling.
² W4 p50 fell 10.2s → 1.49s and failures 9/10 → 2/10; the tail is the leader-lease gap after restart (next fix).
³ W1 wall rose 2.6s → 4.9s: the spool round-trips gp3 disk (~125 MB/s). RSS is the win; spool/upload overlap is known remaining juice.

After run #2 the reference rig is hardware-bound on every scenario: R1/R3/R6/R7
saturate ~75–87k rps of CPU+NIC, R2/R5 sit at the burst-NIC ceiling (~4.6 Gbps),
W3 is worker-tick cadence, W1-torch is gp3 disk throughput. Phase 1 is dry.

⁵ W1-torch wall varies 14–20s run-to-run with fresh gp3 volumes' burst
credits; RSS is the stable signal. Row 4's headline: the worker nudge makes
upload→visible cost one rebuild (~0.5s) instead of tick+rebuild — sync
publish-then-install is now sub-second on the $12 box.

⁴ Row 3 R2 measures the gzip path (100 KB on the wire instead of 674 KB) —
which is what uv and pip request by default. Same-row gains vs row 2: torch
index reads 853 → 4,283 rps (5×), 900 MB upload 19.5s → 14.6s (multipart),
sync-upload p99 2.29s → 1.83s. The $12/month box now serves 4.3k torch-sized
index reads per second.

<!-- Append rows only. W1-torch records pass/FAIL(reason) until it passes. -->

## Improvements log

Every landed optimization, paired with the meter runs that bracket it.

| Date | Change (commit) | Benchmark moved | Before → After |
|------|-----------------|-----------------|----------------|
| 2026-06-12 | Reconcile sweep moved off the tick path + 16-way sidecar reads (worker.rs) | W3 visible p99 | 58.3s → 1.57s |
| 2026-06-12 | Same | W4 sync p50 / RYW failures | 10.2s → 1.49s / 9/10 → 2/10 |
| 2026-06-12 | In-memory index cache, ETag hashed once at fill (cache.rs) | R1 / R3 | 2,059 → 82,231 rps / 2,078 → 88,399 rps |
| 2026-06-12 | Same | R2 torch index | 227 → 881 rps (now NIC-bound) |
| 2026-06-12 | Streaming upload spool (upload.rs, put_file_if_absent) | W1-torch 900MB | OOM-killed → PASS, RSS 638MB+ → 15.5MB |
| 2026-06-12 | Per-request logging INFO → debug; spool + log moved off tmpfs | server survival | 30s benchmark filled a 924MB tmpfs and wedged the box → stable |
| 2026-06-12 | PEP 658 metadata served from RAM cache (immutable) | R7 | 1,406 → 81,312 rps |
| 2026-06-12 | Presigned URLs reused across clients (immutable artifacts) | R6 | 10,092 → 74,781 rps |
| 2026-06-12 | Leader lease released on graceful shutdown (lease.rs) | W4 read-your-write failures / p99 | 2/10 → **0/10** / 10.5s → 2.29s |
| 2026-06-12 | Parallel multipart upload for >64MB artifacts (storage.rs) | W1 100MB / W1-torch 900MB wall | 3.8s → 1.32s / 32.7s → 18.0s |
| 2026-06-12 | **S3 list_dirs pagination** (correctness: global index + reconciler silently capped at 1,000 packages) | corpus integrity @10k pkgs | 1,000 listed → 10,007 listed; regression test `tests/test_scale.py` |
| 2026-06-12 | Concurrent dirty-marker drain (8-way, worker.rs) | mass-ingest of ~9k pending packages | unfinished after 40+ min → 528s (~17 pkg/s) |
| 2026-06-12 | Semaphore drain (no chunk head-of-line blocking) | S2 visibility p99 during 5k-file burst | 72.9s → 1.80s |
| 2026-06-12 | Incremental global-index update (worker.rs); sweep stays the healer | S4 new name → global index | 56.8s → 1.68s |
| 2026-06-12 | Sidecar read fan-out 16 → 64 | S1 5,000-file package rebuild | 17.2s → 7.42s |
| 2026-06-12 | Per-file download retry with backoff (sync.rs); test `tests/test_sync_retry.py` | M-suite robustness | one transient 503 in 7,714 files failed the whole run → retried and survives |
| 2026-06-12 | Package-level sync concurrency (`--package-concurrency`, default 8) | M1 small-file mirror throughput | 13.7 → 30.6 files/s (wall bound by largest single package) |
| 2026-06-12 | …plus file-concurrency 16 within packages (flag tuning, no code) | M1 | 30.6 → **117 files/s** (8.5× total) |
| 2026-06-12 | In-memory multipart for >64MB `put_if_absent` bodies (sync mirror path) | M2 torch-class mirroring | 0.95 → 1.04 Gbps (bound moved to per-file phase serialization; documented, deferred) |
| 2026-06-12 | Precompressed gzip index/metadata variants (cache.rs) | R2 torch-index reads | 8,296 → 27,287 rps at half the wire bytes (NIC-bound → CPU-bound) |
| 2026-06-12 | Worker nudge on writes (Notify) + 1s default tick | W3 visible p50 / W4 sync p99 | 1.35s → 0.68s / 1.83s → 0.82s |
| 2026-06-12 | Cache bodies as `Bytes` (refcount, not memcpy) | per-response copy removed | ~430 MB/s of memcpy off the 2-vCPU hot path |

## Scale: full-PyPI, measured

Two questions, answered with measurements instead of vibes:

1. What happens if you point pypiron at a corpus the size of all of PyPI —
   779,934 projects, 17,130,626 files (the real numbers as of 2026-06-12)?
2. Do our filename parsers survive every filename ever uploaded to PyPI?

The first question was answered twice: against the original
sweep-is-the-backbone architecture (preserved below as the "before"), and
against the event-marker + fingerprint-audit architecture that replaced it
([DESIGN.md](DESIGN.md)). Summary of the rewrite, same rig, same fabricated
corpus:

| 50k pkgs / 1.16M files (disk) | before | after |
|---|---|---|
| steady maintenance pass | 192 s, reads every sidecar | **59 s, `rebuilt=0`, zero reads/writes** (stat-walk bound) |
| cold rebuild-everything | 215 s | 156 s (parallel shards; also writes fingerprints) |
| upload → installable | 0.07 s | 0.07 s |
| S3 maintenance at full PyPI | ~$10.75/sweep, back-to-back ≈ $52k/month | ~46.5k LISTs ≈ **$0.23/audit, daily ≈ $7/month** |

The structural change: day-to-day freshness rides crash-safe event markers
(intent/commit pairs, consumed only after the work they announce — proven by
the kill-point sweep in tests/test_crash_consistency.py); the audit only
exists for out-of-band storage changes, runs daily by default, and skips any
package whose flat-listing fingerprint (key, size, etag) matches the one
recorded at its last rebuild. Cost scales with churn, not corpus.
`pypiron verify` recomputes everything read-only and exits nonzero on
divergence; `pypiron resync` is the rebuild-the-world button.

Extrapolations for the new audit at full PyPI (17.1M files, ~46M keys):
**S3: under a minute and $0.23** (36 prefix shards × ~1,300 pages in
parallel); disk: a ~15-minute background stat-walk, daily. When nothing
changed, nothing else happens at any scale.

### Measured on real AWS S3 (not MinIO)

The S3 row above was extrapolation; this validates its basis on a real
same-region bucket (us-east-1, `c7g.4xlarge`, 2026-06-12, commit `be4db39` +
event-driven-indexer work). Corpus: the fabricated **5k tier** (5,000 packages
/ 104,645 files / **219,298 objects**), seeded with `bench/scale.py seed` and
synced to S3. Full procedure and conditional-write validation in
[Run 009](#run-009--2026-06-12--event-driven-audit--conditional-writes-on-real-aws-s3).

| 5k pkgs / 104,645 files (real S3) | measured |
|---|---|
| steady audit (fingerprints match) | **8.0 s, `rebuilt=0`, `skipped=5001`** — reads no sidecar, only flat-lists |
| cold rebuild-everything (restore-from-backup) | 140 s, `rebuilt=5001` |
| steady audit LIST requests / cost | ~293 LISTs ≈ **$0.0015** (219,298 objects ÷ 1,000/page + 36-shard rounding) |
| upload → visible *during* a running audit | **0.55 s** (steady) / **0.59 s** (cold) — event path never starved |

The `rebuilt=0` steady audit and the per-object LIST cost both hold on the real
thing. Scaling the measured LIST count linearly by object count lands at ~46k
LISTs ≈ **$0.23** for full PyPI — the projection above, now anchored to a
measurement rather than arithmetic. Conditional writes (`If-None-Match` lease
acquire, `If-Match` lease steal + global-index CAS conflict) were exercised on
the same bucket and behave exactly as the disk/MinIO tests assume: a lost race
returns cleanly via `lost_conditional_write`, never an error.

---

Everything below is the original investigation that motivated the rewrite,
kept because the corpus method and the failure math are the reference.

### Method: fabricate the shape, not the bytes

Nothing in the read or sweep path opens artifact bytes when a sidecar exists,
so a storage tree of **0-byte artifacts + real sidecars** exercises the same
code as a real mirror at ~1/40,000th of the disk. `bench/scale.py seed`
fabricates such trees using the real PyPI project names and the real
files-per-project distribution (median 4, p90 35, p99 262, max 43,145 —
`ddtrace`), sampled deterministically. Ground truth came from the public
`pypi.projects` dataset on the ClickHouse playground (mirrors BigQuery's
`bigquery-public-data.pypi.distribution_metadata`); see `src/corpus_check.rs`
for the one-line download.

Rig: M-series MacBook, APFS NVMe, disk backend, release build, defaults
(`worker-interval 1s`, sweep measured via the `duration_secs` field on the
`reconcile: sweep complete` log line).

### Reconcile sweep vs corpus size

The sweep is the scale-sensitive path: it lists every package, reads every
sidecar, and rewrites any view that disagrees with truth. Cold = freshly
restored tree, every view written (the restore-from-backup case). Steady =
nothing to write, pure verification.

| tier | packages | files | cold sweep | steady sweep | µs/file (steady) | RSS |
|---|---|---|---|---|---|---|
| 5k | 5,000 | 104,645 | 10.9 s | 8.3 s | 79 | 30 MB |
| 15k | 15,000 | 333,534 | 42.5 s | 41.2 s | 124 | 41 MB |
| 50k | 50,000 | 1,160,866 | 215 s | 192 s | 165 | 72 MB |
| 75k | 75,000 | 1,697,309 | 336 s | 275 s | 162 | 89 MB |

Per-file cost grows over the small tiers (filesystem cache warm-up) and
**plateaus at ~162–165 µs/file from 1.2M files on** — the sweep is linear
at scale, ~6,200 files verified per second on this rig, ~5,000/s written
cold. The code does O(1) work per file; the early curve was the page cache,
not the algorithm.

Extrapolating at the plateau rate for a disk-backed corpus:

- **8M files** (the question that prompted this): **~22 min** per steady
  sweep, ~26 min for the cold rebuild-everything sweep.
- **Full PyPI, 17.1M files**: ~46 min steady, ~57 min cold.

What does NOT degrade at scale — measured at every tier:

- **upload → installable stays at 0.06–0.08 s** even while a million-file
  sweep is running (dirty markers are O(changed packages), by design).
- **Package index reads stay sub-ms** (materialized views; O(1) per request).
- **RSS stays flat** (~70 MB at 1.16M files; nothing holds the corpus in
  memory).
- Startup is instant (no boot-time scan).

### The S3 math (the actual production concern)

On S3 the sweep is not time-bound, it's **request-bound**: every sweep is one
LIST per package plus one GET per sidecar.

Full PyPI per sweep: 17.13M GETs ($0.0004/1k) + ~0.78M LISTs ($0.005/1k)
≈ **$10.75 per sweep**. At ~15 ms per request and the sweep's in-flight
ceiling (8 packages × 64 sidecar reads), a sweep takes ~9 minutes, so with
the default 300 s interval the server sweeps back-to-back:
**~$1,700/day ≈ $52k/month** of pure reconcile reads. A 1M-file private
registry is ~$0.65/sweep → ~$90/day if back-to-back — already worth fixing.

The fix shipped and stays no-DB: the LIST response already carries every
key + size + ETag. Fingerprint that listing; a steady-state audit then skips
the sidecar GETs for unchanged packages — 17.9M requests/sweep become ~46k
LISTs (the event-driven audit measured in Run 009).

Two knobs already exist and matter: raise `--reconcile-interval-secs` (the
audit is the healer, not the publish path — event markers carry freshness) and
the audit's package concurrency.

### The 43,145-file package (`ddtrace`)

Largest single project on PyPI (Datadog publishes nightlies). One package
directory with 43,145 files:

- cold rebuild of its index: **4.4 s** (64-way sidecar read concurrency);
  steady re-verification 2.6 s
- its `/simple/.../index.json` is **10.7 MB** and serves at **3.6 ms** p50
  from cache
- upload→visible for other packages while it rebuilds: 0.08 s

### The global index at 780k names

Measured 58 B/project in the JSON view → ~45 MB at full PyPI (cross-check:
pypi.org's real root index is 40.6 MB). Two consequences at that size, both
confined to the rarely-requested root index:

- Rendering it (every sweep, plus on any name-set change) is tens of MB of
  string building per pass — CPU-noticeable but bounded.
- The cached variants (JSON + HTML + gzips ≈ 100 MB) approach
  `INDEX_CACHE_MAX_BYTES` (128 MB), whose pressure response is "clear
  everything" with a 1 s TTL — a full-PyPI deployment should raise the cache
  ceiling or the root index will evict the working set.

### Filename parsing vs every file ever uploaded

`cargo test --release corpus_full_pypi -- --ignored --nocapture` replays all
17,130,626 real filenames against the parsers with ground-truth name/version
from PyPI's metadata (95 MB download; command in `src/corpus_check.rs`).

Results after the pep440-scan sdist splitter (raced against last-dash and
dash-before-digit over all 7.04M sdists; it won):

| check | result |
|---|---|
| `is_artifact` misclassifies a real file | 0 / 17.1M |
| project name fails to normalize | 0 / 779,934 |
| name inference correct | 99.78% (was 90.2% with first-dash splitting) |
| version inference correct | 98.91% |
| wheel tags unparseable | 126 / 9.94M (all genuinely malformed uploads) |

Corpus facts encoded as unit tests in `src/names.rs` / `src/sync.rs`: every
file ever uploaded ends in one of 11 extensions; 24% of sdists have a dash in
the name; versions legally contain dashes (`Pootle-2.0.0-rc2`); UUID-shaped
names exist; exactly one file has an uppercase extension
(`notebook-4.0.5.ZIP`); legacy formats (.egg/.exe/.msi/.rpm/.dmg/.deb, 0.86%
of PyPI, frozen since ~2013) intentionally infer no version. The residual
~1% is spam-era uploads whose filename never contained the release version —
unrecoverable by construction, and harmless: sidecars carry authoritative
versions everywhere except proxy/backfill inference.

## Full run details

One subsection per benchmark session (meter runs, big-box runs, scale runs).
Newest last. Include: date, commit, rig (instance types, region), corpus
preset, exact command, and the full metric output (rps, p50/p95/p99, server
peak RSS, CPU, storage op counts where logged).

### Run 000 — 2026-06-11 — harness validation (NOT a meter row)

- Commit: `b79dd16` · Rig: local MacBook + Docker MinIO (loopback; comparative only)
- Suite: meter, `--duration 5s --connections 32 --skip-torch-upload`, torch preset reduced to 200 files
- Purpose: prove the harness end-to-end before the rig. Numbers are NOT
  comparable to reference-rig rows and never will be.

| Scenario | rps | p50 ms | p95 ms | p99 ms | ok% | Notes |
|---|---|---|---|---|---|---|
| R1_json | 9,920 | 2.94 | 5.15 | 9.32 | 100 | per-request storage GET visible |
| R1_html | 10,227 | 2.91 | 4.79 | 8.12 | 100 | |
| R3_304 | 9,911 | 2.96 | 5.13 | 9.21 | 100 | 304 no faster than 200 — full GET + hash per request, as predicted |
| R2_torch_idx | 2,446 | 11.57 | 29.49 | 35.3 | 100 | index size already dominates |
| R6_302 | 10,931 | 2.75 | 4.33 | 6.55 | 100 | uv UA, presign per request |
| R7_metadata | 10,462 | 2.87 | 4.79 | 7.69 | 100 | |
| W3_visibility | — | p50 1.05s | — | p99 1.11s | — | 1s worker tick + rebuild |
| W1_100mb | — | — | — | — | — | wall 1.86s, **peak RSS 245 MB** — multipart buffering confirmed |
| W4_sync_upload | — | p50 1.08s | — | p99 10.08s | — | **hit the 10s sync timeout; 2/10 read-your-write failures** |
| R5_proxy_download | 3.2 | 4038 | — | 4274 | 100 | 1.34 Gbps loopback, RSS flat 26 MB (downloads do stream) |

Harness findings worth keeping: oha follows redirects by default (`-r 0`
required or R6 measures S3, not the 302 — and exhausts loopback ports);
oha emits null percentiles when nothing completes; the W4 sync-upload
timeout is real behavior under S3-ish latency, not harness noise.

### Run 001 — 2026-06-12 — reference-rig baseline #0 (meter row 0)

- Commit: `b79dd16` (src/ unmodified; bench/ harness uncommitted) · Binary built on loadgen from the same tree
- Rig: server `t4g.small` unlimited (single instance) + loadgen `c7gn.4xlarge`, us-east-1, bucket `pypiron-bench-<account-id>-us-east-1`
- Config: S3 backend, `--artifact-delivery auto`, worker tick 1s, reconcile 300s — customer defaults otherwise
- Corpus: meter preset (bench-small ×10, torchsim ×2000); seeding 2,010 files through `/legacy/` took **88s ≈ 23 files/s** (pre-M1 data point)
- Suite: `bench/run-baseline.sh baseline-0` (oha `-z 30s -c 64`); raw JSON in `bench/results/baseline-0.json`

| Scenario | rps | p50 ms | p95 ms | p99 ms | ok% | Notes |
|---|---|---|---|---|---|---|
| R1_json | 2,059 | 27.2 | 54.6 | 91.0 | 100 | p50 ≈ one S3 GET — per-request fetch confirmed at AWS latency |
| R1_html | 2,118 | 26.7 | 51.8 | 82.8 | 100 | |
| R3_304 | 2,078 | 27.2 | 53.3 | 84.1 | 100 | 304 exactly as expensive as 200: full S3 GET + SHA-256 per revalidation |
| R2_torch_idx | 227 | 284.5 | 404.8 | 457.3 | 100 | 674 KB index; ~1.2 Gbps sustained but p50 0.28s — hash + fetch per request |
| R6_302 | 3,426 | 15.7 | 36.0 | 66.2 | 100 | presign (local HMAC) + S3 HEAD per request |
| R7_metadata | 1,433 | 28.5 | 82.8 | 107.9 | 100 | |
| W3_visibility | — | p50 1.59s | — | **p99 58.3s** | — | long tail: worker/reconcile contention while corpus rebuilds queue up |
| W1_100mb | — | — | — | — | — | wall 2.59s, peak RSS 258 MB (2.6× artifact size, buffered) |
| W4_sync_upload | — | p50 **10.2s** | — | 10.6s | — | **all 10 hit the sync timeout; 9/10 read-your-write failures** — sync mode effectively broken on S3 at baseline |
| R5_proxy_download | 4.7 | 1726 | 2339 | 2660 | 100 | **3.72 Gbps** S3→client pass-through, RSS flat at 18 MB — downloads genuinely stream |
| W1_torch_900mb | — | — | — | — | — | **FAIL: server OOM-killed at ≥638 MB RSS** buffering the multipart body; client saw RemoteDisconnected at 2.9s |

The baseline story in one line: reads are S3-GET-bound (~27 ms floor,
~2k rps/endpoint), 304s buy nothing, big-index reads collapse to 227 rps,
sync uploads time out, and a torch-class upload kills the box. Downloads
stream; everything else is the optimization backlog, in priority order.

### Run 004 — 2026-06-12 — Phase 2 brag box, meter shape (pre-multipart)

- Commit: `b79dd16`+P1b · Rig: server `c7gn.2xlarge` (8 vCPU, 50 Gbps) + loadgen `c7gn.4xlarge`, us-east-1, same bucket/corpus
- Suite: meter shape (oha `-z 30s -c 64`); raw JSON `bench/results/bragbox-meter.json`. NOT a meter-series row (different hardware).

| Scenario | rps | p50 ms | p99 ms | Notes |
|---|---|---|---|---|
| R1_json / R1_html | 114,752 / 115,349 | 0.53 | 0.76 | likely loadgen-bound at 64 conns |
| R3_304 | 117,716 | 0.51 | 0.76 | |
| R2_torch_idx | 8,904 | 6.43 | 18.03 | **180 GB served in 30 s = 48 Gbps — NIC saturated by index bytes** |
| R6_302 | 112,100 | 0.56 | 0.77 | presign cache |
| R7_metadata | 115,507 | 0.52 | 0.78 | RAM cache |
| W3 / W4 | p99 2.24s / p99 2.42s (0 RYW fail) | | | tick-cadence bound |
| R5_proxy | 4.73 Gbps @ 8 conns | | | per-connection S3 GET bound; scales with conns |
| W1_torch_900mb | PASS 32.7s, RSS 24.8MB | | | spool (gp3 125 MB/s) + single sequential PUT → multipart fix next |

### Run 005 — 2026-06-12 — Phase 2 brag box, post-multipart + Tier 2

- Commit: `b79dd16`+P1b+multipart · Same rig as Run 004
- Raw JSON: `bench/results/bragbox-meter2.json`, `bench/results/tier2-bragbox.json`

Multipart upload effect (parallel 16 MB parts, conditional complete):
W1 100MB wall 3.8s → **1.32s**; W1-torch 900MB wall 32.7s → **18.0s** (the
residual is the gp3 spool at 125 MB/s — provisioned-throughput EBS money, not
code).

Tier 2 at 1,024 connections (`oha -z 30s -c 1024`):

| Scenario | rps | p50 ms | p99 ms |
|---|---|---|---|
| R1_json_1k | **442,005** | 2.14 | 4.52 |
| R3_304_1k | **477,555** | 2.00 | 4.14 |
| R6_302_1k | **441,551** | 2.24 | 4.55 |
| R7_meta_1k | **443,901** | 2.15 | 4.47 |
| R2_torch_1k | 9,110 (≈48 Gbps, NIC-pinned) | 82 | 548 |

These may still be loadgen-bound — the server never blinked. W2: **8
concurrent 900 MB uploads, 8/8 succeeded**, peak RSS 287 MB, reads at 8.8k
rps / p99 7 ms throughout (read load shares the NIC with 7.2 GB of upload).
Per-upload wall ~72 s is eight spools through one 125 MB/s gp3 volume —
disk-bound by design, no longer memory-bound by defect.

Phase 2 verdict: every read metric is NIC- or loadgen-bound, every write
metric is EBS-bound. No code-level juice left at this tier.

### Run 006 — 2026-06-12 — Phase 3: `medium` corpus (10k packages, 320k objects)

- Rig: server `c7gn.2xlarge` + loadgen `c7gn.4xlarge`, us-east-1, real S3
- Corpus: 10,000 synthetic packages × 10 files seeded direct-to-S3
  (`bench/seed_s3.py`, 320k PUTs in 657s), on top of the meter corpus
- Raw JSON: `bench/results/phase3-medium.json` (before), `phase3-medium-fixed.json` (after)

Two scale defects found, fixed, and regression-tested:

1. **Unpaginated S3 `list_dirs` capped the registry at 1,000 packages** —
   global index truncated, reconciler silently sweeping a tenth of the
   corpus. Pinned by `tests/test_scale.py` (1,100 packages via MinIO).
2. **Marker drain was serial** (mass ingest of 10k packages: unfinished
   after 40 min), then chunk-HOL-blocked (one 5,000-file rebuild stalled
   unrelated packages' visibility to a 72.9 s p99). Fixed with
   semaphore-bounded concurrent drain.

Plus one architectural fast-path: the global index now updates
**incrementally** when one name appears/disappears (the sweep's full rebuild
remains the self-healing backbone, exactly per DESIGN.md).

| Scenario | Before | After |
|---|---|---|
| S1 rebuild ladder 10/100/1000/5000 files | 0.56 / 66.0 / 67.2 / 58.9 s (backlog-polluted) | **0.42 / 0.98 / 3.24 / 7.42 s** (clean, linear, no cliff)¹ |
| S2 upload→visible @10k pkgs | p50 1.24s / p99 **72.9s** | p50 1.24s / **p99 1.80s** |
| S4 new name → global index | 56.8s | **1.68s** |
| S5 reads during full sweep | — | **112k rps, p99 0.76 ms** (sweep invisible to readers) |
| Mass ingest, ~9–10k pending packages | unfinished at 40+ min | **528s (~17 pkg/s)** |

¹ first pass at SIDECAR_READ_CONCURRENCY=16 gave 17.2s for the 5,000-file
rung; 64-way sidecar fan-out brought it to 7.42s.

Steady-state visibility is flat from 100 packages to 10,000 — prefix-scoped
rebuilds proven: corpus size does not tax single-package updates.

### Run 007 — 2026-06-12 — Phase 3: sync throughput (M1/M2/M3)

- Same rig; sync runs on the loadgen (c7gn.4xlarge) against PyPI + the bench bucket
- Corpus: 30 real PyPI packages, wheels only (7,715 files); torch cp312 manylinux for M2
- A/B on one binary: `--package-concurrency 1` reproduces the old serial behavior

| Scenario | Result |
|---|---|
| M1 serial (old behavior) | 7,714 files / 563s = **13.7 files/s**; 1 transient PyPI 503 failed the whole run (now retried) |
| M1 parallel (pc=8, file-conc 4) | 7,715 files / 252s = 30.6 files/s — wall bound by the single biggest package |
| M1 tuned (pc=8, file-conc 16) | 7,715 files / 66s = **117 files/s (8.5×)** |
| M2 torch-class (17.66 GB, 37 wheels) | 149s ≈ **0.95 Gbps**; with multipart artifact writes 136s ≈ 1.04 Gbps. Remaining bound: per-file download→hash→upload phases serialize. The tee-streaming fix matters at clone scale (Phase 5) and is deliberately deferred |
| M3 HTTP push (same list, via /legacy/) | 86s = 90 files/s — **within 23% of direct mode** (target ≤25%) |

### Run 008 — 2026-06-12 — read ceiling (proper CPU sampling) + gzip A/B

- Rig: server `c7gn.2xlarge` + loadgen `c7gn.16xlarge` (64 vCPU, 200 Gbps) — sized to make the loadgen impossible to blame
- CPU sampled on both boxes mid-run; raw log in the session records

**Ceiling**: the suspicion that tier-2 numbers were loadgen-bound is resolved: **no** — server CPU was 94–95% with the loadgen at 8%. The `c7gn.2xlarge` ceiling is real:

| conns | rps | p50 | p99 | server CPU | loadgen CPU |
|---|---|---|---|---|---|
| 1,024 | **438,764** | 2.1ms | 4.8ms | 94% | 8% |
| 2,048 | 410,583 | 3.1ms | 18.0ms | 95% | 8% |
| 4,096 | 393,328 | 3.3ms | 52.4ms | 95% | 8% |

**≈55k rps per vCPU**; past saturation, more connections only buy queueing.

**Gzip A/B** (torch-shaped 674 KB index, `-c 1024`, forced headers — oha sends
Accept-Encoding by default, which silently invalidated the first attempt):

| | rps | p50 | wire |
|---|---|---|---|
| identity (forced) | 8,296 | 79.7ms | 44.5 Gbps (NIC-pinned) |
| gzip | **27,287** | 15.3ms | 21.8 Gbps (now CPU-bound) |

**3.3× the requests at half the bandwidth**; ~100 KB on the wire per response
(6.5× compression, done once at cache fill). pip and uv both send
`Accept-Encoding: gzip`, so real clients get this path by default.

### Run 009 — 2026-06-12 — event-driven audit + conditional writes on **real** AWS S3

- Commit: `be4db39` + this change (global-index CAS HTML-ordering fix; audit/election
  metrics) · Rig: `c7g.4xlarge` in **us-east-1**, same-region real S3 bucket (not MinIO)
- Corpus: fabricated 5k tier via `bench/scale.py seed --packages 5000` (realistic
  files-per-project distribution, RNG seed 42) → **5,000 packages / 104,645 files /
  219,298 objects** (209,290 truth + 10,004 views), synced with `aws s3 sync`
- Scripts: `bench/s3_cas_validate.py`, `bench/s3_scale_measure.py`,
  `bench/s3_upload_during_steady.py`

**Conditional writes — validated against the real thing.** MinIO only approximates
S3's `If-None-Match`/`If-Match`; this confirms S3 returns the precondition errors
that `lost_conditional_write` (storage.rs) catches, so a lost race is a clean retry,
not a 500:

| Path | S3 op | Result |
|---|---|---|
| Lease acquire | `put_if_none_match` create | one node wins, logs `lease acquired` |
| Lease acquire conflict | `put_if_none_match` on existing | other node stays follower, no error |
| Lease steal | `put_if_match` over expired lease | follower steals after TTL, logs `lease stolen` |
| Global-index CAS conflict | `put_if_match` with stale ETag | zombie loses CAS, `pypiron_global_cas_conflicts_total` += 1, self-heals |

**Audit — cost scales with churn, not corpus (measured).** Boot-audit duration and
the new `/metrics` counters, read straight off the server:

| Audit | duration | rebuilt | skipped | what it proves |
|---|---|---|---|---|
| cold (no fingerprints, rebuild-everything = restore-from-backup) | **140.1 s** | 5,001 | 0 | the explicit `resync`/restore cost |
| steady (fingerprints match) | **8.0 s** | **0** | 5,001 | the daily default reads **nothing**; pure listing |

- **`rebuilt=0` on real S3** is the headline: a steady audit over 104,645 files
  re-derives no view and reads no sidecar — it only flat-lists.
- **LIST cost**: the steady audit is ~293 LIST requests (219,298 objects ÷ 1,000 keys
  per page + per-shard rounding across 36 shards) ≈ **$0.0015**. Linear in object
  count, this extrapolates to ~46k LISTs ≈ **$0.23** at full PyPI — confirming the
  [scale projection](#scale-full-pypi-measured)'s basis with a real measurement.
- **upload → visible *during* an audit**: **0.55 s** during a steady audit, **0.59 s**
  even during the 140 s cold rebuild-everything audit. The audit runs on its own task,
  so the event path is never starved — a publish lands in ~one rebuild regardless of a
  concurrent sweep. (Earlier "starvation" readings were a probe-naming artifact in the
  harness, since fixed; the worker was always processing the marker promptly.)
- Seeding note: `aws s3 sync` of 209,290 tiny objects took 818 s — CLI-concurrency
  bound (10 parallel PUTs), not a server path; irrelevant to steady-state cost.

<!--
Template:

### Run NNN — YYYY-MM-DD — <short description>

- Commit: `<sha>` · Rig: <server instance> + <loadgen instance>, <region> · Corpus: <preset>
- Suite: <meter | tier-1 | ...> · Command: `bench/run.sh <suite>`

| Scenario | rps | p50 | p95 | p99 | RSS peak | Notes |
|---|---|---|---|---|---|---|
-->
