# Full-PyPI scale: measured

Two questions, answered with measurements instead of vibes:

1. What happens if you point pypiron at a corpus the size of all of PyPI —
   779,934 projects, 17,130,626 files (the real numbers as of 2026-06-12)?
2. Do our filename parsers survive every filename ever uploaded to PyPI?

The first question was answered twice: against the original
sweep-is-the-backbone architecture (preserved below as the "before"), and
against the event-marker + fingerprint-audit architecture that replaced it
(DESIGN.md). Summary of the rewrite, same rig, same fabricated corpus:

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

## Measured on real AWS S3 (not MinIO)

The S3 row above was extrapolation; this validates its basis on a real
same-region bucket (us-east-1, `c7g.4xlarge`, 2026-06-12, commit `be4db39` +
event-driven-indexer work). Corpus: the fabricated **5k tier** (5,000 packages
/ 104,645 files / **219,298 objects**), seeded with `bench/scale.py seed` and
synced to S3. Full procedure and conditional-write validation in
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#run-009).

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

## Method: fabricate the shape, not the bytes

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

## Reconcile sweep vs corpus size

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

## The S3 math (the actual production concern)

On S3 the sweep is not time-bound, it's **request-bound**: every sweep is one
LIST per package plus one GET per sidecar.

Full PyPI per sweep: 17.13M GETs ($0.0004/1k) + ~0.78M LISTs ($0.005/1k)
≈ **$10.75 per sweep**. At ~15 ms per request and the sweep's in-flight
ceiling (8 packages × 64 sidecar reads), a sweep takes ~9 minutes, so with
the default 300 s interval the server sweeps back-to-back:
**~$1,700/day ≈ $52k/month** of pure reconcile reads. A 1M-file private
registry is ~$0.65/sweep → ~$90/day if back-to-back — already worth fixing.

The fix is known and stays no-DB: the LIST response already carries every
key + size + ETag. Fingerprint that listing into the materialized view (or a
tiny `.state` companion); a steady-state sweep then skips the sidecar GETs
for unchanged packages — 17.9M requests/sweep become 0.78M (23×) and the cost
drops to ~$3.90/sweep. Tracked in GAPS.md ("Scale-tier reconcile").

Two knobs already exist and matter before any code changes: raise
`--reconcile-interval-secs` (the sweep is the healer, not the publish path —
dirty markers carry freshness) and the sweep's package concurrency (8) is
conservative for S3.

## The 43,145-file package (`ddtrace`)

Largest single project on PyPI (Datadog publishes nightlies). One package
directory with 43,145 files:

- cold rebuild of its index: **4.4 s** (64-way sidecar read concurrency);
  steady re-verification 2.6 s
- its `/simple/.../index.json` is **10.7 MB** and serves at **3.6 ms** p50
  from cache
- upload→visible for other packages while it rebuilds: 0.08 s

## The global index at 780k names

Measured 58 B/project in the JSON view → ~45 MB at full PyPI (cross-check:
pypi.org's real root index is 40.6 MB). Two consequences at that size, both
confined to the rarely-requested root index:

- Rendering it (every sweep, plus on any name-set change) is tens of MB of
  string building per pass — CPU-noticeable but bounded.
- The cached variants (JSON + HTML + gzips ≈ 100 MB) approach
  `INDEX_CACHE_MAX_BYTES` (128 MB), whose pressure response is "clear
  everything" with a 1 s TTL — a full-PyPI deployment should raise the cache
  ceiling or the root index will evict the working set.

## Filename parsing vs every file ever uploaded

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
