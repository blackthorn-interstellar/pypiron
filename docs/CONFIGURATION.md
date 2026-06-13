# Configuration

The full reference. All options are available via CLI args and/or environment
variables.

## Storage (shared by `serve` and `sync`)

| CLI Arg                 | Env Var                       | Default               | Description                          |
| ----------------------- | ----------------------------- | --------------------- | ------------------------------------ |
| `--storage {disk\|s3}`  | `PYPIRON_STORAGE`             | `disk`                | Select storage backend               |
| `--data-dir PATH`       | `PYPIRON_DATA_DIR`            | `~/.pypiron/packages` | Root when using `disk`               |
| `--s3-bucket NAME`      | `PYPIRON_S3_BUCKET`           | *(required for s3)*   | Bucket when using `s3`               |
| `--aws-region`          | `AWS_REGION`                  | *(none)*              | AWS region                           |
| `--s3-endpoint-url`     | `PYPIRON_S3_ENDPOINT_URL`     | *(none)*              | S3-compatible endpoint (e.g., MinIO) |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false`               | Force path-style addressing          |

**AWS credentials** follow standard AWS SDK envs: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.

## Server

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
| `--proxy-upstream`           | `PYPIRON_PROXY_UPSTREAM`           | *(none)*       | On-demand mirror of this upstream simple index (plus `--proxy-*` filters, see below) |
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

### Per-project download tracking

Usernames support Gmail-style subaddressing. `reader+billing-api`
authenticates as `reader` (password still required) and records `billing-api`
as a project tag — per-tag counts show up in `/metrics` as
`pypiron_project_requests_total{project=...,route=...}` and in the debug
request logs. With uv:

```bash
export UV_INDEX_COMPANY_USERNAME="reader+billing-api"
export UV_INDEX_COMPANY_PASSWORD="secret"
```

Works on open servers too: with no read credential configured, any volunteered
username is parsed for attribution and the password is ignored. Tag cardinality
in `/metrics` is capped (overflow lands in `_overflow`); tags are restricted to
`[A-Za-z0-9._-]`, max 64 chars.

## Sync filters and config file

Filters gate only what a run *adds* — already-mirrored files are never removed:

- `--only-wheels` / `--only-sdists`
- `--python-tag py3,cp311` — python tag(s)
- `--abi-tag none,cp311` — ABI tag(s)
- `--platform-tag any,manylinux2014_x86_64,macosx_*_arm64` — platform tag(s), `*` wildcard
- `--exclude-platform-tag` — exclusions (supports `*`)
- `--exclude-newer 2024-01-01T00:00:00Z` — only files PyPI received before then
- `--exclude-older 2020-01-01T00:00:00Z` — only files received since then

Every sync option also lives in `pypiron.toml` (auto-discovered in the working
directory, or `--config <path>`), layered as CLI/env > file > defaults:

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

The same filters gate what the proxy serves and caches, under a `--proxy-`
prefix: `--proxy-only-wheels`, `--proxy-only-sdists`, `--proxy-python-tag`,
`--proxy-abi-tag`, `--proxy-platform-tag`, `--proxy-exclude-platform-tag`,
`--proxy-exclude-newer`, `--proxy-exclude-older`.

## Artifact delivery

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
[DESIGN.md](DESIGN.md#read-path-zero-coordination).

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

## Operations

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

## Storage layout

The layout *is* the schema — full contract in
[DESIGN.md](DESIGN.md#storage-layout-the-contract):

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
