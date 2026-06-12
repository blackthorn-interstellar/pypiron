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

Real measurements on real AWS hardware with the S3 backend — the setup
people actually deploy. The small server costs about **$12/month**; the
large one is a standard 8-CPU machine.

| | $12/month server (2 CPUs) | 8-CPU server |
|---|---|---|
| Requests answered per second (package lookups, update checks, download links) | **~75,000** | **~440,000** |
| How fast each request finishes | almost all in under 2 ms | almost all in under 5 ms |
| Browsing a giant package (PyTorch-sized: 2,000 versions) | 4,300 pages/second | 27,000 pages/second |
| Publishing a package → installable by everyone | about **0.7 seconds** | about 1 second, even with 10,000 packages hosted |
| Uploading a 900 MB wheel (PyTorch-sized) | 15–20 seconds, using only ~50 MB of memory | eight simultaneous uploads, all succeed — and installs stay fast the whole time |
| Serving wheel downloads through the server | 3.9 gigabits/second | 48 gigabits/second |

In both cases the server code isn't the limit — the small machine runs out
of network and CPU, and the large one was still answering 440,000 requests
per second when our 64-CPU load generator was loafing at 8%. Background
maintenance (the self-healing sweep) runs without readers noticing, and
mirroring packages down from PyPI runs at over 100 files per second.

Every number above comes from a logged, repeatable benchmark run — commit,
hardware, and method included — in
[docs/BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md). For contrast: when
benchmarking began, the $12 server managed 2,000 requests per second, took
58 seconds to make a new upload installable, and crashed outright on a
single PyTorch-sized upload. The full fix-by-fix path from there to here is
the [improvements log](docs/BENCHMARK_RESULTS.md#improvements-log).

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
* Self-healing: crash-safe event markers keep indexes fresh (proven by a
  crash-point fault-injection suite), a cheap fingerprint audit catches
  out-of-band storage changes, and `pypiron verify`/`resync` recompute the
  world on demand
* Multi-node on S3 via a sloppy leader lease (conditional writes, TTL)
* Optional synchronous uploads for publish-then-install CI pipelines
* **On-demand PyPI proxying** (opt-in): one URL for private packages plus
  transparently cached public dependencies, origin-checked end to end
* Optional **read authentication** (`--read-user`/`--read-pass`) — make the
  registry actually private, not just unguessable
* Boring observability: `/health`, Prometheus `/metrics`, `--log-format json`

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

## On-demand proxying (cached public PyPI)

`sync` mirrors what you list; the proxy mirrors what you *use*. With
`--proxy-upstream`, one URL serves your private packages **and** public
dependencies, fetched from upstream on first request and cached in storage
forever after:

```bash
pypiron --admin-user admin --admin-pass secret \
  --private-prefix acme \
  --proxy-upstream https://pypi.org
```

* Package pages are answered from the upstream PEP 691 listing (cached for
  60 s), carrying PyPI's true upload times — `--exclude-newer` stays
  historically correct. Artifacts are downloaded on first GET, verified
  against the upstream sha256, and committed as ordinary `mirror`-origin
  files; from then on they serve locally, upstream up or down.
* The origin rules are the same as `sync`: names claimed `private` (or inside
  `--private-prefix`) **never** fall through to upstream. Run the proxy with
  `--private-prefix` — without it, a new private name and the public name
  race for first claim.
* The same filters as `sync` gate what the proxy serves and caches, under a
  `--proxy-` prefix: `--proxy-only-wheels`, `--proxy-only-sdists`,
  `--proxy-python-tag`, `--proxy-abi-tag`, `--proxy-platform-tag`,
  `--proxy-exclude-platform-tag`, `--proxy-exclude-newer`,
  `--proxy-exclude-older`.
* The global `/simple/` index lists local packages only (nobody pages through
  all of PyPI); package URLs resolve regardless.
* If upstream is unreachable, proxied pages fall back to the local
  materialized index: everything already cached keeps resolving and
  installing.

## Authentication

Three optional basic-auth credentials, strictly ordered — admin ⊇ uploader ⊇
reader:

| Credential | Flags | Grants |
| --- | --- | --- |
| admin | `--admin-user`/`--admin-pass` | everything: publish, mirror (backdating), delete, yank |
| uploader | `--uploader-user`/`--uploader-pass` | publish ordinary uploads |
| read | `--read-user`/`--read-pass` | read indexes and artifacts |

With **no write credential** configured the server is **read-only** — open
unauthenticated writes don't exist. With no read credential, reads are
public. When `--read-user` is set, `/simple/` and `/files/` require auth
(any of the three credentials works; `/health` and `/metrics` stay open for
probes and scrapers), and clients embed it the usual way:

```bash
pip install --index-url http://reader:secret@localhost:8080/simple/ mypackage
```

### Per-project traffic attribution

Usernames support Gmail-style subaddressing: `reader+billing-api`
authenticates as `reader` (the password is still required), and
`billing-api` is recorded as a project tag — per-tag request counts show up
in `/metrics` as `pypiron_project_requests_total{project=...,route=...}`
and in the debug request logs. With uv:

```bash
export UV_INDEX_COMPANY_USERNAME="reader+billing-api"
export UV_INDEX_COMPANY_PASSWORD="secret"
```

This works on open servers too: with no read credential configured, any
volunteered username is parsed for attribution and the password is ignored.
Tag cardinality in `/metrics` is capped (overflow lands in `_overflow`),
and tags are restricted to `[A-Za-z0-9._-]`, max 64 chars.

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

**Large uploads:** the upload spool defaults to the system temp dir. In
containers (and distros) where `/tmp` is a RAM-backed tmpfs, point it at real
disk or multi-GB wheels spool into memory:
`-v /data/spool:/spool -e PYPIRON_SPOOL_DIR=/spool`.

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
dependency-confusion hole the origin system closes). Need public packages
too? That's what `--proxy-upstream` and `pypiron sync` are for — the same
single URL, origin-checked.

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
| `--read-user`                | `PYPIRON_READ_USER`                | *(none)*       | Read credential — when set, reads require auth   |
| `--read-pass`                | `PYPIRON_READ_PASS`                | *(none)*       | Read credential password                         |
| `--private-prefix`           | `PYPIRON_PRIVATE_PREFIX`           | *(none)*       | Reserve a namespace for private uploads          |
| `--proxy-upstream`           | `PYPIRON_PROXY_UPSTREAM`           | *(none)*       | On-demand mirror of this upstream simple index (plus `--proxy-*` filters, see above) |
| `--spool-dir`                | `PYPIRON_SPOOL_DIR`                | system temp    | Upload/proxy spool directory — real disk, not tmpfs |
| `--log-format`               | `PYPIRON_LOG_FORMAT`               | `text`         | `text` or `json` (one object per line)           |
| `--worker-interval-secs`     | `PYPIRON_WORKER_INTERVAL_SECS`     | `1`            | Worker tick interval (writes also nudge the worker directly) |
| `--reconcile-interval-secs`  | `PYPIRON_RECONCILE_INTERVAL_SECS`  | `86400`        | Audit sweep interval (fingerprint-skipped; cost scales with churn) |
| `--audit-on-boot`            | `PYPIRON_AUDIT_ON_BOOT`            | `true`         | Audit as soon as this node becomes leader        |
| `--intent-grace-secs`        | `PYPIRON_INTENT_GRACE_SECS`        | `900`          | How long an in-flight write may defer its package's rebuild |
| `--lease-ttl-secs`           | `PYPIRON_LEASE_TTL_SECS`           | `30`           | Leader lease TTL (multi-node S3)                 |
| `--artifact-delivery`        | `PYPIRON_ARTIFACT_DELIVERY`        | `auto`         | How artifact bytes reach clients (see below)     |
| `--sync-uploads`             | `PYPIRON_SYNC_UPLOADS`             | `false`        | Wait for index visibility before returning 200   |
| `--sync-upload-timeout-secs` | `PYPIRON_SYNC_UPLOAD_TIMEOUT_SECS` | `10`           | Bound on the synchronous-upload wait             |

**AWS credentials** follow standard AWS SDK envs: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.

### Operations

* `GET /health` — `200 {"status":"ok"}` when storage answers a probe, `503`
  otherwise. Unauthenticated; point your load balancer at it.
* `GET /metrics` — Prometheus text: requests by route group and status class,
  index rebuilds, reconcile sweeps, proxy fetch/cache counters.
  Unauthenticated.
* Logs go to stdout via `tracing`; `--log-format json` emits one JSON object
  per line for log pipelines. Per-request logging is at `debug`
  (`RUST_LOG=pypiron=debug`) so the access log never becomes the workload.

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
