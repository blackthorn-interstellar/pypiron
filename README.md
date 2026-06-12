# <img src="docs/pypiron-logo-256.png" alt="PypIron logo" width="40" style="vertical-align: middle;"/> PypIron

An ultra-fast, reliable, standards-compliant PyPI server for private registries
that only serves static files. No database.

**Scope:** ruthlessly minimal, production-friendly.
**Backends:** local **disk** (default) or **S3/S3-compatible**.
**APIs:** PEP 503 (HTML) + PEP 691/700 (JSON), PEP 592 yank, PEP 658/714 metadata.
**Uploads:** legacy endpoint (`/legacy/`), compatible with `uv publish` and `twine`.

The design is a static site generator wearing a PyPI costume: truth lives in
the packages tree (immutable artifacts plus write-time metadata sidecars), and
the simple index is a materialized view, idempotently regenerable from a
storage listing. See [docs/DESIGN.md](docs/DESIGN.md) for the full reasoning.

## Performance

Measured, not promised: every number below is a row in
[docs/BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) with the commit,
hardware, and method that produced it. Both rigs run the S3 backend — the
configuration that actually gets deployed, not a tmpfs demo.

**On a $12/month box** (`t4g.small`, 2 vCPU / 2 GiB, same-region S3):

| | |
|---|---|
| Package-index reads | **76,000 req/s**, p99 1.7 ms |
| 304 revalidations (pip/uv steady state) | **76,000 req/s** |
| Presigned artifact redirects | **69,000 req/s** |
| PEP 658 metadata reads | **72,000 req/s** |
| torch-sized index (2,000 files, served gzipped) | **4,300 req/s** |
| Upload → visible in the index | **p50 0.7 s, p99 1.1 s** |
| Synchronous publish-then-install round trip | **p99 0.8 s**, zero read-your-write misses |
| 900 MB wheel upload | 15–20 s at **~50 MB RSS** |
| Proxied artifact downloads | 3.9 Gbps (the NIC gives out first) |

**On an 8-vCPU box** (`c7gn.2xlarge`, same-region S3):

| | |
|---|---|
| Index reads / 304s / redirects / metadata | **~440,000 req/s each**, p99 < 5 ms — server CPU-bound at 94% while a 64-vCPU load generator idled at 8% |
| torch-sized index | **27,000 req/s gzipped**; 48 Gbps of NIC when a client insists on identity |
| 8 × 900 MB concurrent uploads | 8/8 succeed at **287 MB peak RSS**, reads stay at p99 7 ms throughout |
| 10,000-package corpus | upload→visible p99 **1.8 s**; a brand-new name reaches the global index in **1.7 s** |
| Reads during a full reconcile sweep | **112,000 req/s, p99 0.76 ms** — the self-heal pass is invisible |
| Mirroring from PyPI (`pypiron sync`) | 117 files/s on the long tail; torch-class wheels at ~1 Gbps |

When this benchmarking effort started, the same suite on the same $12 box
managed 2,000 index reads/s, took 58 seconds to make an upload visible, and
was OOM-killed by a single torch upload. The complete path from there to
here — every fix paired with its before/after — is the
[improvements log](docs/BENCHMARK_RESULTS.md#improvements-log).

## Getting Started

```bash
uvx pypiron   # runs pypiron locally; stores data under ~/.pypiron/packages
```

### Quick smoke test (disk backend)

```bash
# Start PypIron (basic auth required for uploads)
PYPIRON_ADMIN_USER=admin \
PYPIRON_ADMIN_PASS=secret \
uvx pypiron

# Upload an artifact with uv:
uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password secret \
  path/to/demo-0.1.0-py3-none-any.whl

# Browse indexes:
open http://localhost:8080/simple/
```

## Features

* **Disk-backed** storage (default) — zero external deps
* **S3-backed** storage (AWS S3 and S3-compatible), with client-aware
  artifact delivery: presigned redirects for clients that tolerate them (uv),
  streamed bytes for clients whose caches don't (pip)
* No database — truth is files, views are regenerable, backups are rsync
* PEP 503 (HTML) and PEP 691 (JSON) with PEP 700 fields — `uv
  --exclude-newer` works, including on mirrored packages
* PEP 658/714: wheel `METADATA` served as a static companion, so resolvers
  never download wheels just to read dependencies
* PEP 592 yank, plus deletion, with cache-correct index rebuilds
* Filename immutability (the pypi.org rule): artifacts are served
  `Cache-Control: immutable`, indexes revalidate with ETags
* Dependency-confusion defense: every package is exclusively `private` or
  `mirror`, claimed at first write; optional reserved namespace prefix
* Self-healing: a periodic reconciler regenerates anything stale, so lost
  events are harmless
* Multi-node on S3 via a sloppy leader lease (conditional writes, TTL)
* Optional synchronous uploads for publish-then-install CI pipelines

## Mirroring packages with `pypiron sync`

The recommended mode mirrors **over HTTP**: sync needs only the server URL and
the admin credential — no storage credentials, no knowledge of the server's
backend. It carries PyPI's true upload timestamps, so `--exclude-newer`
resolves historically correct versions against your mirror. Mirroring is an
**admin** operation: PypIron has two roles — uploader (publish) and admin
(everything, including mirror, delete, and yank) — so ordinary uploaders
cannot backdate packages.

```text
# packages.txt — one entry per line; PEP 440 specifiers are optional
requests>=2.20,<3
numpy
six==1.16.0
```

```bash
# Server side: two roles — uploader publishes, admin can also mirror
pypiron --uploader-user dev --uploader-pass devsecret \
  --admin-user admin --admin-pass adminsecret

# Mirror over HTTP (recommended) — authenticate with the admin credential
pypiron sync --packages-list packages.txt \
  --to http://localhost:8080 --username admin --password adminsecret

# Or write directly to storage (needs bucket/disk access; no server involved)
pypiron sync --packages-list packages.txt --data-dir ~/.pypiron/packages
pypiron sync --packages-list packages.txt --storage s3 --s3-bucket my-bucket
```

**Filters** (gate only what a run *adds* — already-mirrored files are never
removed):

* `--only-wheels` / `--only-sdists`
* `--python-tag py3,cp311` — python tag(s)
* `--abi-tag none,cp311` — ABI tag(s)
* `--platform-tag any,manylinux2014_x86_64,macosx_*_arm64` — platform tag(s), `*` wildcard
* `--exclude-platform-tag` — exclusions (supports `*`)
* `--exclude-newer 2024-01-01T00:00:00Z` — only files PyPI received before then
* `--exclude-older 2020-01-01T00:00:00Z` — only files received since then

**Configuration file**: every sync option also lives in `pypiron.toml`
(auto-discovered in the working directory, or `--config <path>`), layered as
CLI/env > file > defaults:

```toml
[sync]
packages = ["requests>=2.20,<3", "six"]   # or packages-list = "packages.txt"
to = "http://localhost:8080"
username = "admin"                        # password via PYPIRON_SYNC_PASSWORD
only-wheels = true
python-tag = ["py3"]
exclude-newer = "2026-01-01T00:00:00Z"
concurrency = 8
```

A `packages-list` path in the file resolves relative to the config file, not
the working directory. An explicit `--packages-list` on the CLI replaces the
file's list entirely; other options layer per-key.

Mirrored names are claimed `mirror`-origin; names already claimed by private
uploads (or inside `--private-prefix`) are refused outright. Only the admin
credential can mirror — backdating never rides along on the uploader credential.

## Running with Docker

```bash
docker run --rm -it -p 8080:8080 \
  -e PYPIRON_ADMIN_USER=admin \
  -e PYPIRON_ADMIN_PASS=<mypassword> \
  pypiron:latest
```

### Switch to S3 backend (Docker)

```bash
docker run --rm -it -p 8080:8080 \
  -e PYPIRON_STORAGE=s3 \
  -e PYPIRON_S3_BUCKET=<my_bucket_name> \
  -e PYPIRON_ADMIN_USER=admin \
  -e PYPIRON_ADMIN_PASS=<mypassword> \
  -e AWS_ACCESS_KEY_ID=<my_access_key> \
  -e AWS_SECRET_ACCESS_KEY=<my_secret_key> \
  -e AWS_REGION=us-east-1 \
  pypiron:latest
```

## Using with pip / uv / twine

```bash
# Install from your server
pip install --index-url http://localhost:8080/simple/ mypackage

# Upload with uv
uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password mypassword dist/*.whl

# Upload with twine
twine upload --repository-url http://localhost:8080/legacy/ \
  -u admin -p mypassword dist/*
```

Point clients at this registry **only** (`--index-url`, never
`--extra-index-url https://pypi.org/simple` — that reopens the
dependency-confusion hole the origin system closes).

## Management API

Deletion and yank are **admin** operations — authenticate with the admin
credential.

```bash
# Delete a file (index first, then artifact — clients never see a broken link)
curl -u admin:secret -X DELETE http://localhost:8080/files/<pkg>/<filename>

# Yank / un-yank (PEP 592); request body becomes the reason
curl -u admin:secret -X POST -d "broken release" \
  http://localhost:8080/files/<pkg>/<filename>/yank
curl -u admin:secret -X DELETE http://localhost:8080/files/<pkg>/<filename>/yank
```

## Configuration

All options are available via CLI args and/or environment variables.

### Storage (shared by `serve` and `sync`)

| CLI Arg                 | Env Var                       | Default               | Description                          |
| ----------------------- | ----------------------------- | --------------------- | ------------------------------------ |
| `--storage {disk\|s3}`  | `PYPIRON_STORAGE`             | `disk`                | Select storage backend               |
| `--data-dir PATH`       | `PYPIRON_DATA_DIR`            | `~/.pypiron/packages` | Root when using `disk`               |
| `--s3-bucket NAME`      | `PYPIRON_S3_BUCKET`           | *(required for s3)*   | Bucket when using `s3`               |
| `--aws-region`          | `AWS_REGION`                  | *(none)*              | AWS region                           |
| `--s3-endpoint-url`     | `PYPIRON_S3_ENDPOINT_URL`     | *(none)*              | S3-compatible endpoint (e.g., MinIO) |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false`               | Force path-style addressing          |

### Server

| CLI Arg                      | Env Var                            | Default        | Description                                      |
| ---------------------------- | ---------------------------------- | -------------- | ------------------------------------------------ |
| `--bind-addr`                | `PYPIRON_BIND_ADDR`                | `0.0.0.0:8080` | Listen address                                   |
| `--uploader-user`            | `PYPIRON_UPLOADER_USER`            | *(none)*       | Uploader credential — may publish                |
| `--uploader-pass`            | `PYPIRON_UPLOADER_PASS`            | *(none)*       | Uploader credential password                     |
| `--admin-user`               | `PYPIRON_ADMIN_USER`               | *(none)*       | Admin credential — publish + mirror/delete/yank  |
| `--admin-pass`               | `PYPIRON_ADMIN_PASS`               | *(none)*       | Admin credential password                        |
| `--private-prefix`           | `PYPIRON_PRIVATE_PREFIX`           | *(none)*       | Reserve a namespace for private uploads          |
| `--worker-interval-secs`     | `PYPIRON_WORKER_INTERVAL_SECS`     | `1`            | Worker tick interval (writes also nudge the worker directly) |
| `--reconcile-interval-secs`  | `PYPIRON_RECONCILE_INTERVAL_SECS`  | `300`          | Full self-heal sweep interval                    |
| `--lease-ttl-secs`           | `PYPIRON_LEASE_TTL_SECS`           | `30`           | Leader lease TTL (multi-node S3)                 |
| `--artifact-delivery`        | `PYPIRON_ARTIFACT_DELIVERY`        | `auto`         | How artifact bytes reach clients (see below)     |
| `--sync-uploads`             | `PYPIRON_SYNC_UPLOADS`             | `false`        | Wait for index visibility before returning 200   |
| `--sync-upload-timeout-secs` | `PYPIRON_SYNC_UPLOAD_TIMEOUT_SECS` | `10`           | Bound on the synchronous-upload wait             |

**AWS credentials** follow standard AWS SDK envs: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.

### Artifact delivery

Index pages always carry stable `/files/<pkg>/<filename>#sha256=...` URLs —
that's what ends up in lockfiles and client caches, and it never expires.
`--artifact-delivery` governs what happens when a client GETs one:

| Mode       | Behavior                                                                  |
| ---------- | ------------------------------------------------------------------------- |
| `auto`     | *(default)* Redirect clients that tolerate it (uv); stream everyone else |
| `redirect` | Always 302 to a presigned S3 URL — the node never touches wheel bytes    |
| `stream`   | Always proxy bytes through the node with immutable cache headers         |

The tradeoff: a presigned redirect moves the megabytes to S3, but each
response carries a freshly signed URL. Clients whose download caches are
keyed by the URL that served the bytes — pip's HTTP cache — can never get a
hit on such a URL, so `redirect` silently turns every pip install in a fresh
environment into a full re-download. uv is immune: it caches wheels by index
and filename, so it doesn't care what URL the bytes came from.

`auto` resolves this per request: clients verified to cache by filename get
the 302, everyone else (pip, browsers, unknown tools) gets streamed bytes
under the stable URL with `Cache-Control: immutable` — a warm pip cache means
zero artifact bytes over the network. Use `redirect` when the node's
bandwidth is the binding constraint and you accept weaker pip caching; use
`stream` when clients can't reach the bucket endpoint (private subnet,
firewalled S3). The disk backend always streams, whatever the mode.
PEP 658 `.metadata` companions always stream — they're tiny and
resolution-critical.

## Storage layout

The layout is the schema — see [docs/DESIGN.md](docs/DESIGN.md):

```
packages/<pkg>/<filename>                # artifact, immutable once written
packages/<pkg>/<filename>.meta.json      # sidecar: sha256, size, version, upload-time, requires-python, yanked
packages/<pkg>/<filename>.metadata       # PEP 658 core metadata, extracted from wheel
packages/<pkg>/.origin                   # "private" | "mirror" — claimed at first write
simple/index.html                        # materialized views (regenerable)
simple/index.json
simple/<pkg>/index.html
simple/<pkg>/index.json
_dirty/<pkg>                             # empty marker: package needs index rebuild
_leader/lease.json                       # multi-node lease (holder, term, expires-at)
```

## Docs

* [VISION.md](docs/VISION.md) — the one-page version
* [DESIGN.md](docs/DESIGN.md) — architecture and reasoning
* [STANDARDS.md](docs/STANDARDS.md) — PEP support matrix
* [COMPATIBILITY.md](docs/COMPATIBILITY.md) — generated client compatibility matrix
* [TESTING.md](docs/TESTING.md) — blackbox-first test philosophy
* [ROADMAP.md](docs/ROADMAP.md) — implementation history

## Ecosystem

* devpi-server
* pypiserver
* pypicloud
* warehouse
