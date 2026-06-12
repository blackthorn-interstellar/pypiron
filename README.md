# <img src="docs/pypiron-logo-256.png" alt="PypIron logo" width="40" style="vertical-align: middle;"/> PypIron

[![CI](https://github.com/brycedrennan/pypiron/actions/workflows/ci.yml/badge.svg)](https://github.com/brycedrennan/pypiron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pypiron.svg)](https://pypi.org/project/pypiron/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An ultra-fast PyPI server written in Rust. 

The design is a static site generator wearing a PyPI costume: truth lives in
the packages tree (immutable artifacts plus write-time metadata sidecars), and
the simple index is a materialized view, idempotently regenerable from a storage
listing. That one decision is where the speed, the self-healing, and the
rsync-it-and-you're-done backups all come from. See
[docs/DESIGN.md](docs/DESIGN.md) for the full reasoning.

## Performance

Real measurements on real AWS hardware with the S3 backend — the setup people
actually deploy. The small server costs about **$12/month**; the large one is a
standard 8-CPU machine.

| | $12/month server (2 CPUs) | 8-CPU server |
|---|---|---|
| Requests answered per second | **~75,000** | **~440,000** |
| Request latency | almost all under 2 ms | almost all under 5 ms |
| Publish → installable by everyone | about **0.7 s** | about 1 s, even with 10,000 packages hosted |
| 900 MB wheel upload (PyTorch-sized) | 15–20 s, ~50 MB of memory | 8 simultaneous, all succeed, reads stay fast |
| Wheel download throughput | 3.9 Gbit/s | 48 Gbit/s |

The server code isn't the limit in either case — the small machine runs out of
network and CPU, and the large one was still answering 440,000 requests per
second while our 64-CPU load generator loafed at 8%. Every number is a logged,
repeatable run — commit, hardware, and method included — in
[docs/BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md).

## Highlights

- 🚀 **Ultra-fast static-file core** — no database, no dynamic island; one binary.
- 💾 **Disk or S3 backend** — disk by default (zero deps), S3/S3-compatible for scale and multi-node.
- 📦 **Standards-complete** — PEP 503 (HTML) + PEP 691/700 (JSON), PEP 658/714 metadata, PEP 592 yank, `requires-python`, `--exclude-newer`.
- ⬆️ **Works with your tools** — `uv publish`, `twine`, `pip`, `poetry`, `pdm` upload and install against it unmodified.
- 🔁 **Mirror PyPI** — `pypiron sync` (explicit allowlist) or `--proxy-upstream` (cache-on-use), carrying PyPI's true upload times so `--exclude-newer` stays historically correct.
- 🛡️ **Dependency-confusion defense** — every package is exclusively `private` or `mirror`, claimed at first write; optional reserved namespace prefix.
- 🩹 **Self-healing** — crash-safe event markers keep indexes fresh; a cheap fingerprint audit catches out-of-band storage changes; `pypiron verify`/`resync` recompute the world on demand.
- 🔒 **Optional auth** — admin / uploader / reader basic-auth credentials; read-only by default when none are set.
- 📈 **Boring observability** — `/health`, Prometheus `/metrics`, `--log-format json`.

## Installation

```bash
uvx pypiron            # run without installing; data under ~/.pypiron/packages
```

```bash
pip install pypiron    # or install it
```

```bash
docker run --rm -it -p 8080:8080 \
  -e PYPIRON_ADMIN_USER=admin -e PYPIRON_ADMIN_PASS=secret \
  pypiron:latest
```

## Documentation

- [VISION.md](docs/VISION.md) — the one-page version
- [DESIGN.md](docs/DESIGN.md) — architecture and reasoning
- [STANDARDS.md](docs/STANDARDS.md) — PEP support matrix
- [COMPATIBILITY.md](docs/COMPATIBILITY.md) — generated client compatibility matrix
- [TESTING.md](docs/TESTING.md) — blackbox-first test philosophy
- [ROADMAP.md](docs/ROADMAP.md) — features shipped, planned, and rejected
- [BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) — measured numbers, scale, and the improvements log

## Quickstart

```bash
# Start PypIron (basic auth required for uploads)
PYPIRON_ADMIN_USER=admin PYPIRON_ADMIN_PASS=secret uvx pypiron

# Publish an artifact
uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password secret dist/*.whl

# Install it (point clients at this registry only — see the FAQ)
pip install --index-url http://localhost:8080/simple/ mypackage

# Browse the indexes
open http://localhost:8080/simple/
```

`twine upload --repository-url http://localhost:8080/legacy/ -u admin -p secret dist/*`
works identically.

## Mirroring with `pypiron sync`

The recommended mode mirrors **over HTTP**: sync needs only the server URL and
the admin credential — no storage credentials, no knowledge of the backend. It
carries PyPI's true upload timestamps, so `--exclude-newer` resolves
historically correct versions against your mirror. Mirroring is an **admin**
operation, so ordinary uploaders cannot backdate packages.

```bash
# Server: two roles — uploader publishes, admin can also mirror/delete/yank
pypiron --uploader-user dev --uploader-pass devsecret \
  --admin-user admin --admin-pass adminsecret

# Mirror over HTTP (recommended) — authenticate with the admin credential
pypiron sync --packages-list packages.txt \
  --to http://localhost:8080 --username admin --password adminsecret

# Or write directly to storage (needs bucket/disk access; no server involved)
pypiron sync --packages-list packages.txt --data-dir ~/.pypiron/packages
pypiron sync --packages-list packages.txt --storage s3 --s3-bucket my-bucket
```

```text
# packages.txt — one entry per line; PEP 440 specifiers optional
requests>=2.20,<3
numpy
six==1.16.0
```

**Filters** gate only what a run *adds* (already-mirrored files are never
removed): `--only-wheels` / `--only-sdists`, `--python-tag`, `--abi-tag`,
`--platform-tag` (with `*` wildcard) and `--exclude-platform-tag`,
`--exclude-newer` / `--exclude-older` upload-time bounds.

**Config file:** every sync option also lives in `pypiron.toml`
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

A `packages-list` path in the file resolves relative to the config file. An
explicit `--packages-list` on the CLI replaces the file's list entirely; other
options layer per-key. Mirrored names are claimed `mirror`-origin; names already
claimed `private` (or inside `--private-prefix`) are refused outright.

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

- Package pages come from the upstream PEP 691 listing (cached 60 s), carrying
  PyPI's true upload times. Artifacts download on first GET, are verified
  against the upstream sha256, and commit as ordinary `mirror`-origin files;
  from then on they serve locally, upstream up or down.
- The origin rules match `sync`: names claimed `private` (or inside
  `--private-prefix`) **never** fall through to upstream. Run the proxy with
  `--private-prefix` — without it, a new private name and the public name race
  for first claim.
- The same filters as `sync` apply under a `--proxy-` prefix
  (`--proxy-only-wheels`, `--proxy-exclude-newer`, …).
- The global `/simple/` index lists local packages only; package URLs resolve
  regardless. If upstream is unreachable, proxied pages fall back to the local
  materialized index, so everything already cached keeps installing.

## S3 backend

```bash
docker run --rm -it -p 8080:8080 \
  -e PYPIRON_STORAGE=s3 \
  -e PYPIRON_S3_BUCKET=my-bucket \
  -e PYPIRON_ADMIN_USER=admin -e PYPIRON_ADMIN_PASS=secret \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e AWS_REGION=us-east-1 \
  pypiron:latest
```

On S3, artifact delivery is client-aware: presigned redirects for clients whose
caches tolerate them (uv), streamed bytes for clients whose caches don't (pip).
Multi-node runs on a sloppy leader lease (conditional writes, TTL). See
[Artifact delivery](#artifact-delivery) below.

**Large uploads:** the upload spool defaults to the system temp dir. In
containers where `/tmp` is RAM-backed tmpfs, point it at real disk or multi-GB
wheels spool into memory: `-v /data/spool:/spool -e PYPIRON_SPOOL_DIR=/spool`.

## Authentication

Three optional basic-auth credentials, strictly ordered — admin ⊇ uploader ⊇
reader:

| Credential | Flags | Grants |
| --- | --- | --- |
| admin | `--admin-user`/`--admin-pass` | everything: publish, mirror (backdating), delete, yank |
| uploader | `--uploader-user`/`--uploader-pass` | publish ordinary uploads |
| read | `--read-user`/`--read-pass` | read indexes and artifacts |

With **no write credential** the server is **read-only** — open unauthenticated
writes don't exist. With no read credential, reads are public. When
`--read-user` is set, `/simple/` and `/files/` require auth (any of the three
credentials works; `/health` and `/metrics` stay open for probes):

```bash
pip install --index-url http://reader:secret@localhost:8080/simple/ mypackage
```

**Per-project traffic attribution:** usernames support Gmail-style
subaddressing. `reader+billing-api` authenticates as `reader` (password still
required) and records `billing-api` as a project tag — per-tag counts show up in
`/metrics` as `pypiron_project_requests_total{project=...,route=...}`. Works on
open servers too (any volunteered username is parsed; the password is ignored).
Tag cardinality is capped (overflow → `_overflow`); tags are `[A-Za-z0-9._-]`,
max 64 chars.

```bash
export UV_INDEX_COMPANY_USERNAME="reader+billing-api"
export UV_INDEX_COMPANY_PASSWORD="secret"
```

## Management API

Deletion and yank are **admin** operations.

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

- `GET /health` — `200 {"status":"ok"}` when storage answers a probe, `503`
  otherwise. Unauthenticated; point your load balancer at it.
- `GET /metrics` — Prometheus text: requests by route group and status class,
  index rebuilds, reconcile sweeps, proxy fetch/cache counters, plus audit and
  leader-election machinery (`pypiron_audit_packages_rebuilt_total` /
  `_skipped_total`, `pypiron_audit_last_duration_seconds`,
  `pypiron_global_cas_conflicts_total`, `pypiron_stale_intents_healed_total`).
  Unauthenticated.
- Logs go to stdout via `tracing`; `--log-format json` emits one JSON object per
  line. Per-request logging is at `debug` (`RUST_LOG=pypiron=debug`) so the
  access log never becomes the workload.

### Artifact delivery

Index pages always carry stable `/files/<pkg>/<filename>#sha256=...` URLs —
that's what ends up in lockfiles and client caches, and it never expires.
`--artifact-delivery` governs what happens when a client GETs one:

| Mode       | Behavior                                                                  |
| ---------- | ------------------------------------------------------------------------- |
| `auto`     | *(default)* Redirect clients that tolerate it (uv); stream everyone else |
| `redirect` | Always 302 to a presigned S3 URL — the node never touches wheel bytes    |
| `stream`   | Always proxy bytes through the node with immutable cache headers         |

A presigned redirect moves the megabytes to S3, but each response carries a
freshly signed URL. Clients whose download caches are keyed by the serving URL
(pip's HTTP cache) can never get a hit, so `redirect` silently turns every
fresh-environment pip install into a full re-download. uv is immune — it caches
wheels by index and filename. `auto` resolves this per request; use `redirect`
when node bandwidth is the binding constraint, `stream` when clients can't reach
the bucket endpoint (private subnet, firewalled S3). The disk backend always
streams. PEP 658 `.metadata` companions always stream — tiny and
resolution-critical. Full reasoning in
[docs/DESIGN.md](docs/DESIGN.md#read-path-zero-coordination).

## Storage layout

The layout *is* the schema — see [docs/DESIGN.md](docs/DESIGN.md):

```
packages/<pkg>/<filename>                # artifact, immutable once written
packages/<pkg>/<filename>.meta.json      # sidecar: sha256, size, version, upload-time, requires-python, yanked
packages/<pkg>/<filename>.metadata       # PEP 658 core metadata, extracted from wheel
packages/<pkg>/.origin                   # "private" | "mirror" — claimed at first write
simple/index.html                        # materialized views (regenerable)
simple/index.json
simple/<pkg>/index.html
simple/<pkg>/index.json
_dirty/<pkg>                             # event markers: package needs index rebuild
_leader/lease.json                       # multi-node lease (holder, term, expires-at)
```

## FAQ

**Does it really not need a database?** No. Truth is files; the index is a
materialized view regenerable from a storage listing; backups are rsync. The one
feature a DB genuinely wants — user accounts / API tokens — is covered for
private registries by static basic-auth credentials. See
[DESIGN.md](docs/DESIGN.md#what-no-db-honestly-costs).

**Can I serve public PyPI packages too?** Yes — `pypiron sync` for an explicit
allowlist, or `--proxy-upstream` to cache packages on first use. Both serve from
the same single URL and are origin-checked end to end.

**Why must clients use `--index-url`, never `--extra-index-url`?** pip merges
extra indexes by version with no priority — that *is* the dependency-confusion
vulnerability (an attacker publishes your private name publicly at a higher
version and wins). Point clients at this registry only; it decides what exists.

**Is one node enough?** Yes. The server is cache-correct, not cache-dependent:
artifacts are `immutable`, indexes ETag-revalidate, and client/proxy/CDN caches
compound a single node's already-sufficient capacity. Multi-node on S3 exists
for write availability, not read throughput.

**Is it production-ready?** It's a single binary with measured, repeatable
numbers ([BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md)) and a blackbox suite
that drives real clients ([TESTING.md](docs/TESTING.md)). For the explicitly
stated target — private registries serving static files — yes. For a
multi-tenant pypi.org clone, no, and we don't try.

## Ecosystem

Other private-PyPI servers, for comparison:

- [devpi-server](https://github.com/devpi/devpi)
- [pypiserver](https://github.com/pypiserver/pypiserver)
- [pypicloud](https://github.com/stevearc/pypicloud)
- [warehouse](https://github.com/pypi/warehouse) (the software behind pypi.org)

## License

PypIron is licensed under the [MIT License](LICENSE).
