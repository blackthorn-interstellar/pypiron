# Realistic install benchmark: pypiron vs the field

A `uv`-driven, realistic-traffic benchmark that compares pypiron against four
open-source PyPI servers — **pypiserver, devpi, pypicloud, bandersnatch,
proxpi** — on the same workload, each set up the way you would actually run it.

This is the C2/C3 `uv pip install` tier named in
[BENCHMARKS.md](BENCHMARKS.md#tier-4--absurd-load--chaos-the-demo). It lives in
`bench/install/` as a **new** family; it does not touch the frozen
[`bench/meter.py`](../bench/meter.py) (whose shape is comparability-load-bearing).
House style is inherited: stdlib-only Python, real `uv`/`pip`/`twine` clients,
real wheels, results as `{"meta","results"}` JSON + a markdown table.

## 0. The fairness problem (why this is the hard part)

The five competitors are not the same kind of thing:

| System | Category | Hosts private | Serves public installs |
|---|---|---|---|
| **pypiron** | hybrid (host + proxy + mirror) | yes | yes |
| pypiserver | private host | yes | only if pre-seeded (its "fallback" is a 301 to pypi.org, not a cache) |
| devpi | hybrid (pull-through cache + private index) | yes | yes |
| pypicloud | hybrid (host + on-demand cache) | yes | yes |
| bandersnatch | full mirror → static tree (nginx serves) | **no** (no upload API) | yes |
| proxpi | pure caching proxy | **no** (no upload API) | yes |

A naive "point `uv` at each and install" would compare apples to oranges and,
worse, would mostly measure **upstream pypi.org/Fastly luck**, not the server.

**The fix — measure *serving*, not sourcing.** Resolve the dependency closures
of ~100 real projects **once** into a frozen, hash-pinned union of wheels; then
pre-populate *and warm* every server with that **identical byte universe** and
fire **byte-identical `uv` requests** at each. How each server acquires the
bytes (twine upload / selective mirror / warm-by-proxy) is *setup*, not
measurement. The frozen lock is the equalizer: no server can win by being
thinner or lose to a mid-run upstream fetch.

**Two anti-cheating rules, enforced both directions:**

1. **Production-realistic, not toy.** Each competitor runs in its own documented
   production topology (right app server + worker count, and the nginx/DB
   sidecars it *architecturally* needs — see §3). The hard rule: **no tool gets
   a response/edge cache another lacks.** We do not handicap devpi or
   bandersnatch by running them bare, and we do not bolt a shared front-cache on
   everyone (that benchmarks nginx, not the tools).
2. **No self-cheating for pypiron.** In the head-to-head ranking, pypiron runs
   `--storage disk --artifact-delivery stream` — disk always streams every byte
   through the node, the level playing field. Its S3 + presigned-redirect
   byte-offload (a real architectural edge no competitor has) appears **only**
   in the separately-labeled best-cloud track (§1), never folded into the
   apples-to-apples number.

**Egress is blocked on every ranking run** (measured servers sit on an
`internal: true` Docker network with no route upstream). Any residual cache
miss, fallback redirect, or proxy 302 then **fails loudly** instead of being
silently served by Fastly. This single control neutralizes the biggest
cross-tool footgun: pypiserver's 301, pypicloud's default 302-redirect,
proxpi's 0.9 s-timeout 302-bounce, devpi's revalidation.

## 1. Two tracks (decided: run both)

Every scenario is run in two clearly-labeled configurations, because they answer
different questions and conflating them is exactly the dishonesty this benchmark
exists to avoid:

- **Track 1 — apples-to-apples serving (the ranking).** Identical substrate for
  all: local-disk artifact backend (EBS gp3 on AWS), plaintext HTTP, anonymous
  read, identical instance. pypiron = disk + stream; pypicloud = file storage +
  Postgres. Measures *pure serving code over the same bytes on the same disk*.
  This is the defensible ranking.
- **Track 2 — best cloud production (the companion).** Each tool in its real
  AWS-native config: pypiron = S3 + presigned redirect; **pypicloud = S3 +
  DynamoDB**; devpi/pypiserver/bandersnatch/proxpi = EBS (they have no
  byte-offload path). Measures "what you would actually deploy," including
  pypiron's redirect advantage — reported honestly as an architectural trait,
  not slipped into the ranking.

## 2. The rig: AWS (decided)

Measurement happens on AWS for consistency and proper resourcing. Local
docker-compose is the **development/validation** surface only — the *same*
compose stacks run in both places; only the AWS run yields citable absolutes
(per [BENCHMARKS.md](BENCHMARKS.md): disk numbers are host-dependent, a loose
floor).

- **Topology** (extends [`bench/aws-up.sh`](../bench/aws-up.sh)): one
  **server-under-test** instance + one oversized **loadgen** instance (runs
  `uv`, never the suspect), same VPC/AZ, us-east-1. **Exactly one server runs at
  a time** on the identically-sized instance; servers are torn down and the next
  brought up, so every system sees the same CPU/disk/NIC.
- **Containers, not native.** The server box runs Docker; each system (pypiron
  included, via its [Dockerfile](../Dockerfile)) is brought up by its
  `compose/docker-compose.<system>.yml`. Running pypiron the same way as the
  competitors is itself a fairness requirement.
- **Proper resourcing.** gp3 with provisioned throughput where disk-bound (the
  meter already found gp3's 125 MB/s the wall); in-region S3; DynamoDB
  on-demand (Track 2 pypicloud + pypiron S3 axis); instance class sized so the
  server, not the box, is what we measure.

## 3. Per-server fair setup

All services share one Docker network for warm/seed (egress allowed) and switch
to an `internal: true` network for measured runs. Substrate equalized: same host
class, local-disk backend (Track 1), plaintext HTTP (`uv --allow-insecure-host`),
anonymous read, pinned `uv` + Python for all. Images **pinned by digest**.

### pypiron
`pypiron:bench` built from source at the pinned commit (the ghcr tag job is
disabled; only digest pushes happen). Track 1: `serve --storage disk --data-dir
/data --artifact-delivery stream`. Single binary, no sidecar. **Warm:** `twine
upload` the shared wheelhouse to `/legacy/` as mirror-origin (admin creds);
worker (1 s) + audit-on-boot materialize the `/simple/` indexes. Track 2 (S2
sidebar): `--storage s3 --artifact-delivery redirect`. Index path `/simple/`.
**Fairness:** never `--artifact-delivery auto` in the head-to-head (could hide an
S3 redirect).

### pypiserver
`pypiserver/pypiserver:v2.4.1` (pinned). `run --server gunicorn -w <vCPU>
--backend cached-dir --disable-fallback -a . -P . /data/packages`.
**`--disable-fallback` is non-negotiable** (default fallback = 301 to pypi.org).
`cached-dir` is mandatory (watchdog in-memory index; `simple-dir` rescans every
request and tanks at scale). **Warm:** copy the wheelhouse straight into the
`packages/` volume. No PEP 691 JSON / PEP 658 metadata → uv range-requests wheel
METADATA: a real architectural difference, reported not "fixed." Index `/simple/`.

### devpi (+ nginx sidecar)
`jonasal/devpi-server:6.20.1` + `nginx:1.27` sharing the serverdir volume
(read-only). nginx config from `devpi-gen-config`: direct `/+f/` static file
serving, gzip on (incl. `application/vnd.pypi.simple.v1+json`), `proxy_pass`
only for the index/API. devpi `--threads 50 --keyfs-cache-size 100000`.
Filesystem/SQLite KeyFS — **not Postgres** (files would go into the DB, defeating
nginx direct serving). **Warm:** install the locked corpus once online against
`root/pypi/+simple/`, then restart `--offline-mode`. The nginx sidecar is the
documented prod path and **required** for fairness (bare devpi streams every
byte through Python). Index `/root/pypi/+simple/`.

### pypicloud (+ postgres / dynamodb)
`stevearc/pypicloud:1.3.12` (== latest; **archived/unmaintained, Python 3.9 —
disclosed**) + `postgres:16` (Track 1) or DynamoDB (Track 2). uWSGI
`processes=<vCPU>, enable-threads=true, max-requests=0` (no mid-run recycling),
log level WARN (shipped sample is DEBUG, skews timings). Track 1:
`pypi.storage=file` + `pypi.db=sql` (Postgres). Track 2: `pypi.storage=s3` +
`pypi.db=dynamo`. **`pypi.fallback=cache` + `pypi.default_read=everyone`**;
default `redirect` measures the CDN, not pypicloud. **Warm:** install the corpus
once with a `cache_update` account; verify zero 302s. Index `/simple/`.

### bandersnatch (batch sync, not measured) + nginx (measured)
Sync: `pypa/bandersnatch:7.1.0` (filesystem tag). Serve (measured):
`nginx:1.27-alpine` over the static tree (`root /data/pypi/web`), banderx config
**plus a gzip block** for the index (banderx ships gzip off). `allowlist_project`
plugin seeded with the **FULL transitive closure** (the allowlist does *not*
resolve deps — union all names from the closures or installs hard-fail);
`simple-format = ALL` so PEP 691 JSON is generated. **Warm:** `mirror` once;
measure only the warm tree. No PEP 658 → uv pays one extra range-request per
dist (structural, disclosed). Pure nginx sendfile = "the static-file ceiling,"
framed as such, not as bandersnatch app speed. Public scenarios only (no upload
API). Index `/simple/`.

### proxpi
`epicwink/proxpi:latest` (pin by digest). `--workers 1` (**mandatory** — the
in-memory index cache is per-process; extra workers serve cold-index requests
upstream) `--threads 16`. `PROXPI_CACHE_DIR` on a **persistent** volume (default
is an ephemeral temp dir), `PROXPI_CACHE_SIZE` a large number > corpus bytes
(**not 0**, which *disables* caching), `PROXPI_INDEX_TTL=86400` (default 1800 s
re-fetches mid-run), `PROXPI_DOWNLOAD_TIMEOUT=60` during warm (so slow first
downloads cache instead of 302-bouncing). Single-process GIL ceiling is
architectural — stated plainly, not "leveled" by adding workers. Index `/index/`.

### Sidecar policy
A DB/nginx that is part of a tool's own architecture is fair (devpi nginx,
pypicloud Postgres/DynamoDB, bandersnatch nginx). Decide gzip parity once and
apply uniformly.

## 4. Corpus (frozen, reproducible)

### 4.1 Selection (~100 projects)
`bench/install/lock/projects.toml` — a curated list balancing popularity and
dependency realism: web frameworks, data/sci, DL, data-eng, DB drivers, cloud
SDKs, http/validation, CLI, testing, plus a **heavy-native quota** (numpy, scipy,
pandas, pyarrow, torch, scikit-learn, pydantic-core, cryptography, grpcio,
opencv, shapely) so wheel-byte realism is guaranteed. Each entry pins an exact
distribution + realistic extras. Optionally regenerable from `top-pypi-packages`
(hugovk, pinned monthly tag) via `corpus.py`.

### 4.2 Freeze (resolve once with uv)
`freeze.py` builds one aggregator and runs
`uv pip compile --universal --generate-hashes --only-binary :all:` bounded to
`{linux x86_64} × {cp311, cp312}`, exporting the **union** to a PEP 751
`corpus-<tier>.pylock.toml` (per-file `url`, `size`, `sha256`) plus a per-project
frozen closure `closures/<project>.txt` (resolver-free at replay). Re-freeze
only on an explicit committed bump.

### 4.3 Two tiers
- `corpus-lite` — no GPU stacks, CPU-only torch, ~2–3 GB. Headline (S1, S5).
- `corpus-heavy` — CUDA torch/tf closure, ~8–15 GB incl. the 688 MB
  `nvidia-cudnn-cu12` wheel. Byte-transfer stress (S2). Opt-in (disk footprint).

### 4.4 Committed vs seeded
Commit (small text): `projects.toml`, `corpus-*.pylock.toml`, `closures/*.txt`
under `bench/install/lock/` (a non-ignored path — `bench/corpus/` is gitignored).
**Never commit wheels.** `wheelhouse.py` downloads the union into
`bench/install/wheelhouse/` (gitignored) and sha256-verifies against the lock —
the single shared byte source every private host is seeded from; proxies/mirrors
warm-by-install pinned to the same lock so their caches converge on the same
files.

## 5. Client (uv) recipe

- **Pin the instrument:** `UV_VERSION`, `UV_PYTHON_DOWNLOADS=never`, fixed
  `--python`, `UV_CONCURRENT_DOWNLOADS` (e.g. 16), `UV_CONCURRENT_INSTALLS`,
  `UV_CONCURRENT_BUILDS=1` — constant for every server.
- **Authoritative single index:** `--default-index http://host:PORT/<path>`
  ONLY (it *replaces* pypi.org). Never `--index`/`--extra-index-url` (those
  leave pypi.org as fallback). `--allow-insecure-host` for plaintext HTTP.
- **Cold vs warm client:** fresh `UV_CACHE_DIR=$(mktemp -d)` **and** fresh
  `uv venv` per trial (a satisfied venv no-ops the install). Cold = empty cache.
- **Build isolation:** `--only-binary :all:` (refuse sdists → measure serving,
  not compiling). Drop any project lacking a compatible wheel at compile time.
- **Deterministic replay:** `--require-hashes --no-deps` against the frozen
  files so every server is asked for byte-identical wheels regardless of what it
  hosts.
- **Two workloads (shared frozen closures):**
  - **A — deterministic instrument:** one `uv pip install --no-deps
    --require-hashes -r <pinned>.txt` per trial; identical request set.
  - **B — CI-fleet (headline):** each runner = fresh process + fresh venv + cold
    cache, installs one sampled project's `closures/<project>.txt`. Sweep
    concurrency C ∈ {1, 8, 32, 64, 128}. Two sampling modes, both reported: Zipf
    (download-count weighted, the headline) and uniform (long-tail stress).
    Seeded RNG (`random.Random(seed)`, like `scale.py`) for reproducibility.
- **pip cross-check (secondary):** same frozen files, low concurrency, separate
  column. Consistency of *ranking* across uv and pip is the signal; inconsistency
  is itself a finding.

## 6. Scenarios

| ID | What | Systems | Network |
|---|---|---|---|
| **S1** | Warm serve, corpus-lite, Workload B sweep (**HEADLINE**) | all 6 | egress-blocked |
| S2 | Heavy-byte throughput, corpus-heavy | all 6 | egress-blocked |
| S3 | Private publish + install | pypiron, pypiserver, devpi, pypicloud | egress-blocked |
| S4 | Cold cache-fill / first-touch (**upstream-inclusive, labeled**) | pypiron(proxy), devpi(online), pypicloud(cache), proxpi | egress **allowed** |
| S5 | oha HTTP microbench cross-check (per-capability endpoints) | all 6 | egress-blocked |

S4 measures upstream latency by design → reported separately, **never** folded
into the S1/S2 ranking.

## 7. Metrics & methodology

Per (server × scenario × concurrency), ≥5 trials, **interleave servers
round-robin** to average noise:

- Median install wall (`/usr/bin/time`); p50/p95/p99 per-install wall (Workload B).
- Resolve-only wall (`uv pip install --dry-run`) → isolates metadata serving;
  download+install = total − resolve (unzip/link is a client constant since
  bytes are identical, so the cross-server delta is wheel transfer).
- Throughput: installs/min; MB/s served (S2).
- Error rate; **offline sanity**: a post-warm `uv … --offline` pass per server
  must succeed (proves 100 % cache) **before** any timing.
- Server-side request count + bytes (access-log parse by default; optional
  uniform counting proxy, cache off, all-or-none) → surfaces PEP 658-vs-range
  cost.
- Server CPU + peak RSS via `docker stats` (reuse `meter.py`'s `RssSampler`).

Honest disclosures (never penalized via flags): PEP 658 absence (bandersnatch,
pypiserver); PEP 700 absence (devpi, proxpi) → never use `--exclude-newer`;
bandersnatch's nginx sendfile is the static-file ceiling; proxpi's single-process
GIL is an architectural ceiling. `uv` has no `pip download` (astral-sh/uv#3163) →
emulate pure-fetch via `install --no-deps --target $(mktemp -d)` or count
server-side bytes.

## 8. Layout

```
bench/install/
  lock/                  # COMMITTED: projects.toml, corpus-*.pylock.toml, closures/*.txt
  compose/               # docker-compose.<system>.yml + configs (nginx-devpi.conf, config.ini, bandersnatch.conf, nginx-bander.conf)
  benchlib.py            # shared helpers (reuses bench/meter.py)
  corpus.py              # selection -> projects.toml
  freeze.py              # uv compile/export -> lock/
  wheelhouse.py          # download union -> wheelhouse/, sha256-verify
  seed.py                # per-server-class load (twine/copy/allowlist/proxy-warm)
  drive.py               # uv subprocess driver (A & B), Zipf/uniform sampler, metrics
  bench.py               # orchestrator: up -> seed -> warm -> offline-sanity -> measure -> emit -> teardown
  rig.sh                 # AWS provisioning (Docker host; extends aws-up.sh/deploy.sh)
  wheelhouse/            # gitignored
  results/               # gitignored: JSON + md
```

Reuse from `meter.py` (do not edit it): `http_get`, `wait_healthy`,
`upload_wheel`, `wait_visible`, `RssSampler`, `percentile`, `print_markdown`,
the `{meta,results}` shape. Reuse from `scale.py`: the launch/health-wait/
teardown pattern and the seeded sampler.

## 9. Results

`{"meta": {...}, "results": {...}}` to `bench/install/results/` + a markdown
table to stdout. `meta` adds: `track` (1|2), `corpus_tier`, `uv_version`,
`python`, `matrix`, `sampler_seed`, `sampling_mode`, `concurrency`, per-server
image digest, pypiron commit. Headline table appended to a **new** section of
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) (append-only, with commit + rig
provenance) — separate from the frozen meter series.

## 10. Build sequencing (decided: vertical slice first)

- **Phase 1 — validate the methodology.** `corpus.py` + `freeze.py` +
  `wheelhouse.py` + `drive.py` + `bench.py`; **pypiron + devpi** on S1.
  Validated locally via docker-compose, then on the AWS rig. Confirm numbers are
  sane and reproducible before fanning out.
- **Phase 2 — fan out.** pypiserver, pypicloud, bandersnatch, proxpi; S2–S5;
  both corpus tiers; Track 2.

## 11. Limitations & honest disclosures

Disk numbers are host-dependent (comparative on the local rig; absolutes only
from AWS, and gp3 burst adds variance — already seen in the meter's W1-torch).
S4 includes upstream latency (network-position-dependent; quarantined).
bandersnatch/pypiserver lack PEP 658; devpi/proxpi lack PEP 700 — category
traits, not penalized. pypicloud is archived (Python 3.9); images pinned by
digest. Each tool runs its own documented prod topology; no tool gets a cache
another lacks. The heavy tier is opt-in (8–15 GB × 6 servers — share one
wheelhouse and/or run servers serially on a small box).

## 12. First AWS run — 2026-06-19

Rig: one `c7i.4xlarge` (16 vCPU, x86_64) Docker host, us-east-1, gp3 (provisioned
250 MB/s / 4000 IOPS), via `rig.sh`. Corpus: `corpus-lite` x86_64 (100 projects,
391 wheels, 1.47 GB). Client: `uv 0.9.30`, `--python 3.11`, Workload B,
uniform sampling, 160 samples/level, sweep C ∈ {1,8,32,64,128}.

**Track 1 — apples-to-apples (all servers local-disk/EBS on the box, identical
bytes).** installs/min (higher better) and per-install p50:

| Server | C=1 p50 | C=8 /min | C=32 /min | C=128 /min | C=128 p50 | resolve p50 | err |
|---|---|---|---|---|---|---|---|
| bandersnatch (nginx static) | 129 ms | 1500 | **1688** | 1503 | 966 ms | 47 ms | 0 |
| pypiserver (gunicorn) | 137 ms | 1403 | 1671 | 1466 | 1074 ms | 48 ms | 0 |
| **pypiron** (disk+stream) | 134 ms | 1365 | 1414 | 1390 | 1078 ms | 47 ms | 0 |
| proxpi (1 worker) | 159 ms | 1105 | 1265 | 1077 | 3652 ms | 61 ms | 0 |
| pypicloud (uWSGI+PG) | 138 ms | 1190 | 1259 | 1219 | 2602 ms | 50 ms | 4¹ |
| devpi (nginx+devpi) | 197 ms | 557 | 560 | 545 | 6457 ms | 78 ms | 0 |

Reading it: the nginx-static (bandersnatch) and gunicorn (pypiserver) servers top
raw throughput; **pypiron sits with them in the front group** (~1,400/min,
fastest-tier 47 ms metadata resolve, 0 errors, tightest tails). proxpi and
pypicloud are mid-pack with worse p99 tails under load; **devpi is the clear
laggard** — it serves indexes through Python, so it plateaus at ~560/min and its
p50 blows out to 6.5 s at C=128 where the nginx-fronted servers hold.

**Track 2 — every server in its best-production config** (all six, re-run on a
fresh box). pypiron = S3 + presigned redirect; pypicloud = S3 + DynamoDB; the
other four have no cloud-offload path, so their Track 1 config *is* their optimal
and they run identically. installs/min:

| Server | config | C=8 | C=32 | C=128 | C=32 p50 | resolve p50 | err |
|---|---|---|---|---|---|---|---|
| bandersnatch | nginx static | 1663 | **1817** | 1626 | 265 ms | 42 ms | 0 |
| pypiserver | gunicorn+cached-dir | 1582 | 1770 | 1637 | 343 ms | 44 ms | 0 |
| proxpi | 1-worker cache | 1207 | 1408 | 1173 | 917 ms | 55 ms | 0 |
| pypicloud | **S3 + DynamoDB** | 1088 | 1362 | 1056 | 455 ms | 50 ms | 20¹ |
| **pypiron** | **S3 + redirect** | 1155 | 1217 | 1307 | 347 ms | 79 ms | 0 |
| devpi | nginx + devpi | 647 | 532 | 538 | 2830 ms | 65 ms | 0 |

**The honest read:** on `corpus-lite` (small wheels), local-disk serving
(bandersnatch nginx, pypiserver gunicorn) tops throughput, and pypiron's
S3+redirect is *slightly slower* than its own Track 1 disk+stream (1217 vs 1414
at C=32) — the 302→S3 extra round-trip costs more per install than it saves when
wheels are small. Redirect's win is **byte-offload, not lite-throughput**: the
pypiron node never touches a wheel byte, which only pays off under big wheels
(the heavy tier / S2, not yet run) or NIC saturation. pypicloud's S3+DynamoDB
(its real cloud-native combo) now stands up and serves at ~1360/min. Net: Track 2
is a fair "best foot forward" for each tool; it does not crown pypiron on small
wheels, and we say so.

¹ pypicloud cache-mode can't serve a dependency version different from the one
first cached (e.g. `redis 8.0.0` after `celery[redis]` cached an earlier redis);
~4 sampled installs/level hit it. A real, documented limitation of the archived
tool, tolerated via `--warm-min-ok 0.95`. Its S3+DynamoDB boot needed the rig
IAM to grant `dynamodb:ListTables` on `*` (flywheel checks table existence at
startup).

**Honest framing.** Single-box rig (loadgen + server co-located): correct for an
install-latency-under-concurrency benchmark, not a NIC-saturation test. Numbers
are real on real hardware but the absolute throughput is box-bound at 16 vCPU.
The committed Dockerfile pins `rust:1.85`, too old for HEAD's `object_store`
dep (E0658); `rig.sh` bumps the toolchain on the box copy only (a separate repo
fix is warranted). Re-run any time with `rig.sh up && deploy && run`.

## 13. Breaking point — index-read MST (capacity.py, oha ramp)

The fixed install sweep (§12) degrades servers but never breaks them, and can't:
it spawns one `uv` process per concurrent install, so the loadgen saturates
before a fast server does. `capacity.py` instead ramps `oha` connections against
each server's hot index path (`/simple/flask/`) until it breaks, where **MST
(Max Sustained Throughput)** = the highest rps holding success ≥ 99.5 % AND
p99 ≤ max(50 ms, 10× unloaded p99). A static-nginx **control target** gives
`R_ceiling` (the most this rig+oha can push); `headroom = R_ceiling/MST` classes
each result **server-bound** (real break found) vs **rig-bound** (too fast for
this box to break — needs a 2-box rig for the true ceiling).

First AWS run (2026-06-19, c7i.4xlarge, ladder c=1…4096, 10 s/step):

| Server | MST rps | c_knee | p99@knee | breach | headroom | bound |
|---|---|---|---|---|---|---|
| **pypiron** | **396,471** | 2048 | 15 ms | never broke¹ | 0.7 | rig-bound |
| bandersnatch | 278,196 | 1024 | 12 ms | never broke¹ | 0.99 | rig-bound |
| pypiserver | 6,544 | 4 | 0.7 ms | latency | 43× | server-bound |
| pypicloud | 5,187 | 16 | 7 ms | latency | 54× | server-bound |
| proxpi | 735 | 16 | 36 ms | latency | 383× | server-bound |
| devpi | 283 | 8 | 45 ms | latency | 930× | server-bound |

The metric separates the field across **three orders of magnitude** and reports
its own validity per server:

- **pypiron and bandersnatch are rig-bound** — the 16-vCPU box + oha couldn't
  break either. pypiron's RAM-served index (precomputed ETags) sustained
  396k rps at p99 15 ms and actually **beat the static-nginx control** (278k,
  headroom 0.7); bandersnatch's nginx-sendfile sat right at the rig ceiling
  (278k, headroom 0.99). Their true breaking points are above what this rig can
  apply — a dedicated two-box loadgen is the named fix for citable high-end
  numbers.
- **The Python-tier servers have real, found breaking points** (headroom ≫ 1×,
  unambiguously server-bound): pypiserver 6.5k rps, pypicloud 5.2k, proxpi 735
  (its single-worker GIL ceiling, exactly as predicted), and devpi 283 (indexes
  served through Python — the weakest, ~1,400× below pypiron's floor). All break
  on **latency runaway** (p99 past the ceiling), not errors or collapse.

¹ "never broke" = held the SLO to the c=4096 ladder cap; reported as rig-bound,
not as a server limit. The headroom < 1 for pypiron means its index path
out-serves the control — the rig, not pypiron, is the bottleneck.

## 14. Six-way install-throughput comparison — 2026-06-20

The headline number: **max sustained real-install throughput** on one realistic
small server, every server in its best cloud-backed config, driven by `oha`
replaying the install-mix (index + wheel URLs in install proportions) and
**following the 302 to download wheel bytes from S3** so the *entire* path is
exercised. Driver: `mn_ramp.py` (N loadgens in lockstep, summed) + `rig2.sh`.

- **Server:** one `r7i.large` (2 vCPU, 16 GB, x86), **host networking** — a
  flamegraph (§ below) showed Docker's bridge/NAT cost ~24% on a small box, and a
  single-box deploy wouldn't bridge-NAT; host-net measures the server, not docker.
- **Loadgens:** 2× `c7i.2xlarge` running `oha` (HTTP is arch-agnostic).
- **Corpus:** lite (100 projects / 391 wheels). Same frozen set for everyone.
- installs/s = sustained req/s ÷ ~13 reqs/install (≈2 × avg pkgs/closure).

| Rank | Server | Cloud config | **installs/s** | req/s | breaking mode |
|---|---|---|---|---|---|
| 1 | **pypiron** | S3 + presigned redirect (Rust) | **1,022** | 13,287 | none — node 43% CPU at peak (rig/connection-limited; true ceiling higher) |
| 2 | bandersnatch | full static mirror via nginx | 512 | 6,664 | none — node ~60% CPU (nginx zero-copy; conn-limited) |
| 3 | pypiserver | gunicorn + cached-dir | 85 | 1,102 | server CPU (99%) |
| 4 | pypicloud | **S3 + DynamoDB** (uwsgi) | 47 | 614 | collapse at 2× load |
| 5 | devpi | devpi + nginx (+f direct) | 35 | 449 | errors |
| 6 | proxpi | flask caching proxy | 32 | 414 | server CPU (87%) |

Findings:
- **pypiron wins by 2× (bandersnatch) to ~30× (the Python app servers)** and is
  the only server still **rig-limited, not server-limited** — its node sat at 43%
  CPU at 1,022 installs/s; the real ceiling is higher than this rig can drive.
- **pypicloud — the architectural peer (also S3 blob offload) — lands near the
  bottom (47/s, collapses).** Offloading bytes to S3 doesn't help when the index/
  redirect path is Python + uwsgi + DynamoDB: that layer caps throughput and
  falls over under concurrency. The win is the lean Rust serving path, not just S3.
- **bandersnatch is the only credible challenger (512/s)** — but only because it
  is *pure nginx zero-copy static serving* with no application layer, and it is a
  full public mirror (no private packages, no upload, no auth). It serves every
  wheel byte through the node, so on a small-NIC box (see below) it throttles
  where pypiron (S3 redirect) does not.
- **The Python app servers cluster at 32–85 installs/s** — the app layer is the
  ceiling regardless of storage.

Smaller box (`t3.small`, 2 vCPU / **2 GB**), pypiron only: **450 installs/s**
(host-net), server-CPU-bound, S3 streaming 1.2 GB/s with zero errors — pypiron
seeds and serves comfortably on a $12/mo box. **pypicloud could not even seed the
corpus there: its uwsgi ingest path spiked to ~1.5 GB and OOM-wedged the box**
(hence the bigger box for the field). A real operational gap: pypiron runs where
pypicloud cannot.

Hot-path flamegraph (pypiron under load, t3.small): pypiron's own code is 37% of
CPU with **no dominant function** (top app symbol ~1.5%) — the serving path is
already lean. The actionable finding was the Docker bridge/NAT tax (~24% on a
small box → switched all servers to host networking). Micro-opts attempted on
branch `perf-hotpath` (log fast-path, cache RwLock) are correct but didn't move
this anonymous/2-core workload.

Honest caveats: pypiron and bandersnatch never hit a server-CPU wall on this rig
(2 loadgens) — their ranks are lower bounds, the Python servers are true ceilings.
Peak p99 is high (5–14 s, deep queueing) — these are saturation ceilings, not
low-latency operating points. bandersnatch is a different use case (full mirror,
no private/upload). Numbers are 2-vCPU; the ranking, not the absolute, is the
point. pypicloud has 2 documented unservable wheels (redis cache-mode pin).
