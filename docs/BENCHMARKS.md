# Benchmark Plan

The claim is "ultra-fast." This document is how we make that claim falsifiable,
then absurd. Philosophy (same as [TESTING.md](TESTING.md)): every speed claim
gets a number, every optimization gets a before/after, and every number is
recorded with its commit hash and hardware. Numbers without provenance are
marketing; we don't do marketing.

## Ground rules

- **Comparative first, absolute later.** Laptop + localhost numbers drive the
  optimization loop (free, minutes per iteration). Absolute numbers come only
  from AWS with a *separate* load-generator instance — never colocated, never
  over loopback.
- **Fixed-rate latency measurement.** Closed-loop "max RPS" tools hide latency
  spikes (coordinated omission). Throughput numbers come from `oha`; latency
  numbers come from fixed-rate runs (`oha -q <rate>`) at ~80% of measured max.
- **Correctness probes run during load.** A benchmark that returns garbage fast
  is a bug report. Every sustained run has a sidecar prober asserting: 200s only,
  index parses, no listed-but-missing artifact windows.
- **Release binary only**, pinned commit, recorded `nproc`/RAM/NIC.

## The corpus generator

Real wheels are only needed for realism checks; scale comes from synthetic
wheels. A valid wheel is a small zip — a generator can emit ~1 KB wheels with
unique names/versions at >10k/s, plus sidecars, written directly to disk or S3
(the storage layout *is* the schema, so seeding bypasses the upload path).

Corpus presets:

| Preset | Packages | Files/pkg | Total objects¹ | Shape |
|---|---|---|---|---|
| `small` | 100 | 10 | ~3k | sanity / CI |
| `medium` | 10,000 | 10 | ~300k | big private registry |
| `large` | 100,000 | 10 | ~3M | bigger than any real private registry |
| `torch` | 1 | 2,000 | ~6k | one package shaped like `simple/torch` (~1.5 MB JSON index), incl. several real ~900 MB artifacts |
| `pypi-mirror` | 600,000 | 1–500 zipf | ~12M | full-PyPI-shaped, synthetic |
| `pypi-real` | all of PyPI | actual | ~14M files | not synthetic at all — see Tier 5; supersedes `pypi-mirror` once it exists |

¹ artifact + `.meta.json` + `.metadata` per file.

## Benchmarks and targets

Targets come in two flavors: **floor** (regression gate, must always pass) and
**brag** (the absurd number we tune toward). All absolute targets are for a
single `c7gn.2xlarge` (8 vCPU Graviton, 50 Gbps burst) unless noted; laptop runs
track the same scenarios comparatively.

### Tier 1 — hot reads (the 99.9% path)

| ID | Scenario | Floor | Brag target |
|---|---|---|---|
| R1 | Package index, small pkg (10 files), 200 path | 20k rps | **≥80k rps, p99 < 3 ms @ 1k conns** |
| R2 | Package index, `torch` preset (~1.5 MB JSON), 200 path | 1 Gbps egress | **NIC-bound, p99 < 15 ms** |
| R3 | 304 revalidation (`If-None-Match`), any index size | 30k rps | **≥150k rps, p99 < 1 ms** — this is pip/uv steady state and should cost ~nothing |
| R4 | Global index, `large` preset (100k pkgs, multi-MB HTML) | doesn't fall over | **NIC-bound, served gzipped** |
| R5 | Artifact download, disk, 10 MB wheel, 256 conns | 5 Gbps | **saturate 25 Gbps, RSS < 500 MB** (proves streaming, no buffering) |
| R6 | Artifact download, S3 presigned 302 | 10k rps | **≥50k rps redirects, server CPU < 25%** |
| R7 | PEP 658 `.metadata` fetch storm (uv resolving = index + N metadata files) | 10k rps | **≥60k rps** |

Predicted finding: R1–R4 on the S3 backend currently do one S3 GET + full
SHA-256 *per request* (`serve_index`), which caps S3-backed index reads at
S3's per-prefix rate (~5.5k GET/s) and ~15 ms floor latency, and caps R2 at
hash throughput (~2 GB/s/core). The fix ladder, cheapest first:
store the ETag at rebuild time instead of hashing per request → short-circuit
`If-None-Match` before fetching the body → in-memory index cache (bytes +
ETag, invalidated by rebuild) → precompressed gzip variants. After the cache,
index reads are RAM-bound and the S3 backend serves reads at disk-backend
speed. That single change is most of the brag sheet.

### Tier 2 — the write path (the hard ones)

| ID | Scenario | Floor | Brag target |
|---|---|---|---|
| W1 | Single 900 MB upload (torch-class), disk & S3 | completes, no 5xx | **wall ≤ transfer time + 3 s; peak RSS < 300 MB** (streaming, incremental hash) |
| W2 | 8 concurrent 900 MB uploads on an 8 GB box | no OOM | **all succeed; reads stay < 2× baseline p99 during** |
| W3 | Upload→visible latency (200 → file in index), 1 s worker tick | p99 < 10 s | **disk p99 < 300 ms; S3 p99 < 2.5 s** |
| W4 | Sync-upload mode (`--sync-uploads`) round trip | < timeout | **S3 p99 < 3 s, zero publish-then-install failures across 1k cycles** |
| W5 | Sustained small uploads, 1k distinct pkgs, 10 uploads/s for 10 min | worker keeps up | **dirty-queue depth bounded; visibility p99 flat over the run** |

Predicted finding: W1/W2 currently buffer the whole multipart field in memory
behind a 1 GiB `DefaultBodyLimit` — W1's RSS target will fail until uploads
stream to a temp object with incremental hashing. (Also: PyPI's own per-file
cap is 1 GiB; private registries hold *bigger* artifacts, so the cap becomes
config and W1 gets a 5 GB variant once streaming lands.)

### Tier 3 — scale (very large S3 index)

| ID | Scenario | Floor | Brag target |
|---|---|---|---|
| S1 | Package rebuild latency vs files/pkg (10/100/1k/5k), S3 | linear, no cliff | **5k-file package rebuilds < 5 s** |
| S2 | Upload→visible with `large` corpus (100k pkgs) as background truth | p99 < 15 s | **p99 < 3 s — corpus size must not affect single-package rebuild** (it's prefix-scoped; prove it) |
| S3¹ | Full reconcile sweep, `large` corpus (~3M objects) | < 30 min, no read impact | **< 10 min, < $1 in S3 requests, read p99 undisturbed** |
| S4 | Global index rebuild @ 100k pkgs (new package appears) | < 60 s | **< 10 s** |
| S5 | Cold start → first 200, `large` corpus | < 5 s | **< 1 s** (must not list the world at boot) |
| S6 | `pypi-mirror` preset (~12M objects): S2 + S3 + R-suite | survives | stretch — the "we mirrored PyPI and didn't notice" number |

¹ benchmark ID, not the storage service.

### Tier 4 — absurd load & chaos (the demo)

| ID | Scenario | Target |
|---|---|---|
| C1 | "Black Friday": 10k concurrent readers (zipf over `medium` corpus) + 5 uploads/s incl. one torch-class + reconciler on a 1-min interval, **1 hour** | zero 5xx, read p99 < 10 ms throughout, no correctness-probe failures |
| C2 | `uv pip install torch` (cold cache, in-VPC client) vs pypi.org+Fastly | **beat pypi.org** on resolve+download wall time |
| C3 | 500 parallel `uv pip install` of distinct packages (CI-fleet stampede) | all succeed; server p99 < 25 ms |
| C4 | Multi-node: kill the S3 leader mid-upload-storm | uploads visible within lease TTL + tick; zero client-visible errors |

### Tier 5 — `sync` throughput & the full PyPI clone

`sync` is the bulk-ingest path, so it gets its own throughput numbers — and the
ultimate benchmark doubles as a fuzzer: clone *every package on PyPI* and let
fifteen years of packaging sins find our edge cases for us.

| ID | Scenario | Floor | Brag target |
|---|---|---|---|
| M1 | Sync throughput, small-file packages, direct-to-S3 (request-bound) | 100 files/s | **≥1,000 files/s sustained** (~4 storage ops/file ≈ 4k S3 ops/s) |
| M2 | Sync throughput, torch-class artifacts (bandwidth-bound) | 1 Gbps | **≥5 Gbps sustained PyPI→S3 pass-through, RSS flat** |
| M3 | HTTP-push mode (`--to`) vs direct-to-storage | works | **within 25% of direct** once server uploads stream (W1) |
| M4 | Incremental re-sync of a full mirror (the freshness cost) | < 4 h | **daily delta < 30 min** |
| M5 | **The full clone**: every package on PyPI (~620k projects, ~14M files, ~35 TB) | completes with a bounded, categorized failure list | **< 24 h wall clock; zero crashes; every refusal becomes a test fixture** |

Predicted findings, from reading `sync.rs` (sync.rs:325):

- **Packages are synced sequentially** — `--concurrency` only parallelizes
  files *within* one package (`buffer_unordered`). The long tail of PyPI is
  hundreds of thousands of 2-file packages, so M1/M5 are gated on per-package
  round-trip latency until package-level parallelism lands. That's the first
  fix this tier forces.
- **No size filter.** ~90% of PyPI's bytes live in <1% of files (CUDA wheels,
  nightlies). A `--max-file-size` filter makes a *full-namespace* clone
  possible at ~1/5 the bytes — same filename/metadata edge-case coverage,
  fraction of the cost — so the capped clone runs first, the uncapped clone is
  the final flex.
- **Resume = re-walk.** A clone that dies at package 400k re-checks everything
  from zero (existence checks make the re-run incremental, but the walk itself
  is M4's number). If re-walk is too slow, that motivates PyPI
  changelog-serial support (what bandersnatch uses) — a feature decision the
  benchmark gets to force, not us guessing.
- **HTTP mode buffers each file in RAM** (`Part::bytes`, sync.rs:428) and hits
  the server's 1 GiB body cap; direct-to-storage mode bypasses both. M3 will
  quantify the gap.

The edge-case harvest is the real payoff of M5. Expected catch, all of which
becomes regression fixtures: pre-PEP-625 sdists whose versions can't be
inferred from filenames, `.egg`/`.exe`-era artifacts (skip cleanly or mirror —
either way, deliberately), wheels with missing or unparseable `METADATA`,
filenames with `+` local-version builds and other URL/S3-key-hostile
characters, distinct names that normalize identically, 10k-file packages
(`tf-nightly` and friends), zero-file projects, yanked-everything projects,
and unicode in every field that allows it. The acceptance bar: after the
clone, `uv pip install` resolves correctly against our mirror for a sampled
set of the weirdest survivors, and every file PyPI serves that we refused is
logged with a reason we can defend.

Politeness is part of the spec: identifiable User-Agent with contact info,
bounded request rate against pypi.org's JSON API, conditional requests and
backoff on the re-walk. Fastly absorbs the bytes; the API gets treated gently.

## AWS topology

Boring on purpose: two or three EC2 boxes and a bucket, stood up by a script,
torn down the same hour. No Terraform, no fleet.

- **Server:** `c7gn.2xlarge` baseline; `c7gn.8xlarge` (100 Gbps) only for R5
  NIC-saturation runs. Spot.
- **Load gen:** 1–2 × `c7gn.4xlarge` running `oha` (plus `uv` for C2/C3).
  Scale loadgen until the *server* is the bottleneck, never assume.
- **S3:** same region, gateway VPC endpoint (free, and removes NAT noise).
- **Repeatability:** one `bench/aws-up.sh` → instance IDs + IPs; `bench/run.sh
  <suite>` rsyncs the pinned binary, runs, pulls JSON results; `aws-down.sh`.
- **Cost honesty:** spot c7gn.2xlarge ≈ $0.25/hr; a full Tier-1+2 session
  < $5. Seeding `large` (~3M PUTs) ≈ $15 one-time into a keep-around bucket;
  synthetic `pypi-mirror` ≈ $60, do it once, version the bucket. The real
  clone (Phase 5) is its own line item: ~$250 in PUTs + ~$27/day of storage
  while it lives. Reconcile sweeps over 3M objects cost ~$0.02 in LISTs —
  measure and print actual request counts per run (S6 and M5 double as the
  S3-bill benchmark).

## The ramp

Each phase gates the next; optimize at the cheapest phase that exposes the
problem. Never rent a 100 Gbps NIC to discover a per-request `format!`.

**Phase 0 — laptop, disk, $0 (build the harness).**
`bench/` directory: corpus generator, `oha` wrappers emitting one JSON line per
run (scenario, commit, hardware, rps, p50/95/99, server peak RSS + CPU),
results appended to `docs/BENCHMARK_RESULTS.md`. Run R1–R5, W1–W3 on disk.
Expected immediate findings: per-request SHA-256, upload buffering. Fix the
ETag-at-rebuild + If-None-Match short-circuit here — it's a day of work that
likely 10×'s R3.

**Phase 1 — laptop + MinIO, $0 (S3 code path, comparative).**
Same suites against MinIO, plus S1/S2 at `medium` scale. MinIO latency ≠ S3
latency — numbers are for spotting per-request S3 GETs and call-count
regressions (log storage op counts per scenario; call counts are
hardware-independent truth). Land the index cache and upload streaming here.

**Phase 2 — AWS, disk backend, ~$5 (first absolute numbers).**
Single server + one loadgen. Full Tier 1 + Tier 2. These are the first
publishable numbers; record them as the baseline brag sheet.

**Phase 3 — AWS + real S3, ~$25 (the hard scenarios).**
Seed `medium`, then `large`. Full Tier 2 + Tier 3, plus sync throughput
(M1–M3, against a few thousand real PyPI packages — this is where the
sequential-package-loop fix gets its before/after). The two marquee questions
land here: torch-class uploads (W1/W2) and index updates against a very large
S3 corpus (S2/S3/S4). Re-run after each optimization; the before/after pairs
are the changelog.

**Phase 4 — AWS, the absurd, ~$50 (the demo).**
`c7gn.8xlarge` NIC saturation (R5), the Black Friday hour (C1), CI stampede
(C3), beat-pypi.org (C2), leader-kill (C4), and the synthetic `pypi-mirror`
suite (S6). Output: a results table at the top of the README with commit,
instance type, and date on every number.

**Phase 5 — the full clone, ~$300–500 (the fuzzer finale).**
M5 in two passes: first the full-namespace clone capped at 100 MB/file (every
project, every weird filename, ~1/5 the bytes), harvest and fix the edge
cases; then the uncapped ~35 TB clone with M4's re-sync keeping it fresh.
Re-run the read and scale suites (R, S2–S5) against `pypi-real` — absolute
numbers on the genuine article, not a synthetic shape. Cost is dominated by
S3: ~50M PUTs ≈ $250 one-time, ~35 TB ≈ $27/day stored — run the suite within
the week, keep the results, delete the bucket. The brag at the end: *we are a
working PyPI mirror, cloned in under a day, serving reads faster than the
original.*

## Deliverables

- `bench/` — corpus generator, run scripts, AWS up/down, results collector.
  Plain scripts, no framework.
- `docs/BENCHMARK_RESULTS.md` — append-only log: date, commit, hardware,
  scenario, numbers. Before/after pairs for every optimization.
- README "Performance" section — the brag sheet, every claim linking to a
  results row.
- `make perf` stays as the loose local regression floor; the `bench/` suite is
  where absolute numbers live.
