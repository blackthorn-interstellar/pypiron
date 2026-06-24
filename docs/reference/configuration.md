# Configuration

The full reference. All options are available via CLI args and/or environment
variables.

## Storage (`serve`)

The server owns storage; `sync` is a pure HTTP client (see [Sync](#sync-mirror-over-http)).
One binary, one storage layer. `disk` is the zero-dependency default; the three
cloud backends (`s3`, `gcs`, `azure`) share a single implementation over the
[`object_store`](https://docs.rs/object_store) crate. Pick one with `--storage`.

| CLI Arg                            | Env Var               | Default               | Description                       |
| ---------------------------------- | --------------------- | --------------------- | --------------------------------- |
| `--storage {disk\|s3\|gcs\|azure}` | `PYPIRON_STORAGE`     | `disk`                | Select storage backend            |
| `--data-dir PATH`                  | `PYPIRON_DATA_DIR`    | `~/.pypiron/packages` | Root when using `disk`            |

### S3 / S3-compatible (`--storage s3`)

| CLI Arg                 | Env Var                       | Default             | Description                          |
| ----------------------- | ----------------------------- | ------------------- | ------------------------------------ |
| `--s3-bucket NAME`      | `PYPIRON_S3_BUCKET`           | *(required for s3)* | Bucket name                          |
| `--aws-region`          | `AWS_REGION`                  | *(none)*            | AWS region                           |
| `--s3-endpoint-url`     | `PYPIRON_S3_ENDPOINT_URL`     | *(none)*            | S3-compatible endpoint (e.g., MinIO) |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false`             | Force path-style addressing          |

Region uses the standard `AWS_REGION` (the AWS SDK reads it too); there is
intentionally no `PYPIRON_S3_REGION`/`PYPIRON_AWS_REGION`.

**AWS credentials** follow the standard AWS chain: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, web identity, or instance metadata.
An `http://` endpoint (a local MinIO) is allowed automatically.

### Google Cloud Storage (`--storage gcs`)

| CLI Arg                        | Env Var                            | Default              | Description                                  |
| ------------------------------ | ---------------------------------- | -------------------- | -------------------------------------------- |
| `--gcs-bucket NAME`            | `PYPIRON_GCS_BUCKET`               | *(required for gcs)* | Bucket name                                  |
| `--gcs-service-account-path`   | `PYPIRON_GCS_SERVICE_ACCOUNT_PATH` | *(none)*             | Service-account JSON key file                |
| `--gcs-endpoint-url`           | `PYPIRON_GCS_ENDPOINT_URL`         | *(none)*             | Custom endpoint (local emulator)             |

**GCS credentials**: a service-account key (via the flag or the standard
`GOOGLE_*`/`GOOGLE_APPLICATION_CREDENTIALS` envs), otherwise Application Default
Credentials. **Presigned redirects (`--artifact-delivery redirect`/`auto`)
require a service-account key** — URL signing needs the private key, which ADC
tokens do not provide. Under ADC, artifact downloads fall back to streaming.

### Azure Blob Storage (`--storage azure`)

| CLI Arg                  | Env Var                       | Default                | Description                          |
| ------------------------ | ----------------------------- | ---------------------- | ------------------------------------ |
| `--azure-account NAME`   | `PYPIRON_AZURE_ACCOUNT`       | *(required for azure)* | Storage account name                 |
| `--azure-container NAME` | `PYPIRON_AZURE_CONTAINER`     | *(required for azure)* | Blob container                       |
| `--azure-access-key KEY` | `PYPIRON_AZURE_ACCESS_KEY`    | *(none)*               | Account access key                   |
| `--azure-endpoint-url`   | `PYPIRON_AZURE_ENDPOINT_URL`  | *(none)*               | Custom endpoint (local emulator)     |
| `--azure-use-emulator`   | `PYPIRON_AZURE_USE_EMULATOR`  | `false`               | Target Azurite (well-known dev creds) |

**Azure credentials**: an account access key (via the flag or the standard
`AZURE_*` envs), or a managed identity / bearer token. **Presigned redirects
require the account key** — signed SAS URLs are derived from it; without it,
artifact downloads stream.

!!! warning

    **Buckets/containers must already exist.** pypiron writes objects but does
    not create the bucket or container — provision it first.

## Server

| CLI Arg                      | Env Var                            | Default        | Description                                      |
| ---------------------------- | ---------------------------------- | -------------- | ------------------------------------------------ |
| `--bind-addr`                | `PYPIRON_BIND_ADDR`                | `0.0.0.0:8080` | Listen address                                   |
| `--uploader-user`            | `PYPIRON_UPLOADER_USER`            | *(none)*       | Uploader credential — may publish                |
| `--uploader-pass`            | `PYPIRON_UPLOADER_PASS`            | *(none)*       | Uploader credential password                     |
| `--admin-user`               | `PYPIRON_ADMIN_USER`               | `admin`        | Admin credential — publish + mirror/delete/yank  |
| `--admin-pass`               | `PYPIRON_ADMIN_PASS`               | *(none)*       | Admin credential password (enables admin)        |
| `--read-user`                | `PYPIRON_READ_USER`                | *(none)*       | Read credential — when set, reads require auth   |
| `--read-pass`                | `PYPIRON_READ_PASS`                | *(none)*       | Read credential password                         |
| `--private-prefix`           | `PYPIRON_PRIVATE_PREFIX`           | *(none)*       | Reserve a namespace for private uploads          |
| `--proxy-upstream`           | `PYPIRON_PROXY_UPSTREAM`           | *(none)*       | On-demand mirror of this upstream simple index (plus `--proxy-*` filters, see below) |
| `--spool-dir`                | `PYPIRON_SPOOL_DIR`                | system temp    | Upload/proxy spool directory — real disk, not tmpfs |
| `--log-format`               | `PYPIRON_LOG_FORMAT`               | `text`         | `text` or `json` (one object per line)           |
| `--access-log`               | `PYPIRON_ACCESS_LOG`               | `false`        | Also log reads (downloads/listings), not just mutations (see below) |
| `--access-log-format`        | `PYPIRON_ACCESS_LOG_FORMAT`        | `structured`   | `structured` (key=value/JSON) or `clf` (Combined Log Format) |
| `--worker-interval-secs`     | `PYPIRON_WORKER_INTERVAL_SECS`     | `1`            | Worker tick interval (writes also nudge the worker directly) |
| `--reconcile-interval-secs`  | `PYPIRON_RECONCILE_INTERVAL_SECS`  | `86400`        | Audit sweep interval (fingerprint-skipped; cost scales with churn) |
| `--audit-on-boot`            | `PYPIRON_AUDIT_ON_BOOT`            | `true`         | Audit as soon as this node becomes leader        |
| `--intent-grace-secs`        | `PYPIRON_INTENT_GRACE_SECS`        | `900`          | How long an in-flight write may defer its package's rebuild |
| `--lease-ttl-secs`           | `PYPIRON_LEASE_TTL_SECS`           | `30`           | Leader lease TTL (multi-node S3)                 |
| `--artifact-delivery`        | `PYPIRON_ARTIFACT_DELIVERY`        | `auto`         | How artifact bytes reach clients (see below)     |
| `--wait-on-upload`           | `PYPIRON_WAIT_ON_UPLOAD`           | `false`        | Wait for index visibility before returning 200   |
| `--wait-on-upload-secs`      | `PYPIRON_WAIT_ON_UPLOAD_SECS`      | `10`           | Bound on the wait-on-upload poll                 |
| `--download-stats`           | `PYPIRON_DOWNLOAD_STATS`           | `true`         | Count per-package/version downloads per day (see below) |
| `--counters-resolution`      | `PYPIRON_COUNTERS_RESOLUTION`      | `1d`           | Counter bucket width: `1d`/`1h`/`30m`/`2h` (whole minutes dividing a day) |
| `--counters-flush-interval-secs`  | `PYPIRON_COUNTERS_FLUSH_INTERVAL_SECS`  | `300` | How often each node flushes counts (the dominant cost knob) |
| `--counters-rollup-interval-secs` | `PYPIRON_COUNTERS_ROLLUP_INTERVAL_SECS` | `3600` | Leader compaction cadence (freeze finished days, prune) |
| `--counters-retention-days`  | `PYPIRON_COUNTERS_RETENTION_DAYS`  | `90`           | Days of per-day counter history to keep           |

The two default-on toggles — `--audit-on-boot` and `--download-stats` — are
disabled with an explicit value, `--audit-on-boot false` /
`--download-stats false` (or `PYPIRON_AUDIT_ON_BOOT=false` /
`PYPIRON_DOWNLOAD_STATS=false`), not a `--no-` form.

**Access log.** Each logged request is one line — method, path, status, latency,
response size, client IP (honoring `X-Forwarded-For` / `X-Real-IP` behind a
proxy), the project tag, and User-Agent. What gets logged depends on the request:

- **By default** (no `--access-log`) only **mutations** are logged — uploads,
  deletes, yanks, status changes (any non-GET/HEAD) — at `info`. This is a small,
  high-value audit trail. Reads (index listings, downloads) are **not** logged:
  an always-on read log becomes the workload at high request rates.
- **`--access-log`** widens it to **every request**, the full access log.
- `/health` and `/metrics` are logged **only at debug** in either mode — load
  balancers and Prometheus poll them constantly, so they'd drown an info log.
  Enable with `RUST_LOG=pypiron::access=debug` (or any debug filter).

Two renderings, via `--access-log-format`:

- `structured` (default) — a `tracing` event on the `pypiron::access` target, so
  it follows `--log-format` (key=value text, or a JSON object under `json`) and
  stays tunable with `RUST_LOG` (`pypiron::access=warn` keeps only 5xx, `=off`
  silences it). 5xx log at `warn`, everything else at `info`.
- `clf` — Combined Log Format written straight to stdout, for GoAccess/lnav/
  awstats. It bypasses the diagnostic log's timestamp+level prefix (which those
  parsers can't read), so it shares stdout with the diagnostic log; run
  `RUST_LOG=warn` for a near-pure access stream (CLF parsers skip the rest).

**Download counters.** With `--download-stats` (on by default), each node counts
artifact downloads per `(package, filename)` in memory and flushes immutable
delta segments under `_counters/` every `--counters-flush-interval-secs`; the
leader compacts a finished day into one frozen file per shard and writes a small
per-day summary. Read them at `GET /stats/downloads/<pkg>` (per-package, last 30
days, rolled up to versions; includes today) and `GET /stats/downloads`
(global top packages + daily totals, closed days only) — both read-auth gated.
Counts are a best-effort, *lossy* analytic (never truth). The **per-package**
breakdown is deliberately kept off `/metrics` (registry-sized cardinality);
`/metrics` carries only a single low-cardinality `pypiron_downloads_total`
aggregate, and the root dashboard's Metrics section gains a per-node "Downloads"
tile (accurate on S3, unlike "Files served"). Cost is dominated by
flush PUTs (`flush_interval × nodes`): ~$0.04/node/month at the 300 s default,
effectively free for a private registry. Frozen days are exact; today lags one
flush interval. Changing the resolution is non-destructive (old days keep theirs).

!!! note "Large uploads"

    The upload spool defaults to the system temp dir. In containers where
    `/tmp` is RAM-backed tmpfs, point it at real disk or multi-GB wheels spool
    into memory: `-v /data/spool:/spool -e PYPIRON_SPOOL_DIR=/spool`.

## Authentication

Three optional basic-auth credentials, strictly ordered — admin ⊇ uploader ⊇
reader:

| Credential | Flags | Grants |
| --- | --- | --- |
| admin | `--admin-user`/`--admin-pass` | everything: publish, mirror (backdating), delete, yank |
| uploader | `--uploader-user`/`--uploader-pass` | publish ordinary uploads |
| read | `--read-user`/`--read-pass` | read indexes and artifacts |

The admin username defaults to `admin`, so `--admin-pass secret` alone is a
complete admin credential. With
**no write credential** the server is **read-only** — open unauthenticated
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

## Sync (mirror over HTTP)

`pypiron sync` mirrors packages from a PEP 691 source into a pypiron server. It
is a pure HTTP client: each file is POSTed to the destination's `/legacy/` as a
mirror upload (carrying PyPI's true `upload-time` and yank state), and the
server owns every storage write. Sync needs a destination URL and the admin
credential — nothing about the server's storage backend.

| CLI Arg                       | Env Var                          | Default            | Description                                              |
| ----------------------------- | -------------------------------- | ------------------ | ------------------------------------------------------- |
| `--to URL`                    | `PYPIRON_SYNC_TO`                | *(required)*       | Destination pypiron base URL (or `[sync].to`)           |
| `--from URL`                  | `PYPIRON_SYNC_FROM`              | `https://pypi.org` | Source PEP 691 index                                    |
| `--admin-user` / `--admin-pass` | `PYPIRON_SYNC_ADMIN_USER` / `_PASS` | *(none)*    | Destination admin credential (mirroring is admin-only)  |
| `--private-prefix`            | `PYPIRON_PRIVATE_PREFIX`         | *(none)*           | Refuse to mirror names inside this namespace (or top-level `private-prefix`) |
| `--concurrency N`             | `PYPIRON_SYNC_CONCURRENCY`       | `4`                | Parallel downloads/uploads within one package           |
| `--package-concurrency N`     | `PYPIRON_SYNC_PACKAGE_CONCURRENCY` | `8`              | Packages synced in parallel                             |
| `--config PATH`               | `PYPIRON_CONFIG`                 | *(auto)*           | Path to a `pypiron.toml` (global; read by every subcommand — `verify-index`/`rebuild-index` use its `[serve]` storage selection; default `./pypiron.toml` if present) |
| `--spool-dir PATH`            | `PYPIRON_SYNC_SPOOL_DIR`         | system temp        | Download spool dir — real disk, not tmpfs, for large wheels |
| `--dry-run`                   | `PYPIRON_SYNC_DRY_RUN`           | `false`            | Print what would be mirrored, write nothing             |

`--to` is mandatory (there is no direct-to-storage mode); without it (and no
`[sync].to`) the run refuses to start. Which packages get mirrored is the
filter's name axis — `--filter-package` / `--filter-packages-list` /
`[filter].packages` (see [Filters](#filters)) — and `sync` requires a non-empty
set (it needs an explicit work list). The same scope governs the proxy.

## Re-sync, reconcile, and conditional fetch

A re-`sync` doesn't just add new files — it *reconciles* what it already holds:
yank state is brought in line with upstream (set, cleared, or its reason
updated), a file gone from upstream is flagged yanked `removed upstream` (kept
downloadable, just skipped by installers), and PEP 792 project status is
relayed. Artifacts are never deleted.

To keep "reconcile every run" cheap, each project is fetched conditionally: the
last upstream ETag is remembered server-side (`_sync/cursors.json`, served by
the admin-only `GET`/`PUT /sync/cursors`) and replayed as `If-None-Match`, so an
unchanged upstream answers `304` and the project is skipped entirely. The cursor
is a pure cache — delete it and the next run re-fetches.

- `--full` (`PYPIRON_SYNC_FULL`) — ignore the cursor memo: re-fetch every
  project unconditionally and fully reconcile. Run periodically (e.g. nightly)
  as the self-heal, since a normal run only reconciles projects whose upstream
  listing actually changed.

The cursor key folds in the source URL, the resolved filters, and each project's
specifiers, so changing any of them invalidates the shortcut and forces a full
fetch.

## Filters

Filters select the slice of PyPI you want — names *and* files. They are
**shared**: the same `--filter-*` flags, `PYPIRON_FILTER_*` env vars, and
`[filter]` table govern both `pypiron sync` (push mirror) and
`serve --proxy-upstream` (on-demand proxy) — set the slice once and it applies to
whichever you run. Filters gate only what is *added*; already-mirrored or
already-cached files are never removed.

The **name axis** (the approved-package list):

- `--filter-package SPEC` (`PYPIRON_FILTER_PACKAGE`) — one package: a name with
  optional PEP 440 specifiers (`requests`, `six==1.16.0`, `requests>=2.20,<3`).
  Repeatable. Commas belong to the specifier, so pass multiple packages as
  repeated flags, not a comma-joined list.
- `--filter-packages-list FILE` (`PYPIRON_FILTER_PACKAGES_LIST`) — a file of
  specs, one per line; blank lines and `#` comment lines are ignored.
- `[filter].packages` (inline array) and `[filter].packages-list` (file path) are
  the config-file forms.

`sync` mirrors exactly the listed names (and, where a spec is version-pinned,
only matching versions); it requires a non-empty list. The **proxy is
fail-closed**: with a list set, only listed names fall through to upstream and
everything else is `404`'d (a version-pinned name serves only matching versions,
just as `sync` mirrors only those). With **no** list, the proxy keeps its default
of serving any non-private name on demand.

The **file axis** (which artifacts of a selected package to keep):

- `--filter-only-wheels` / `--filter-only-sdists`
- `--filter-python-tag py3,cp311` — python tag(s)
- `--filter-abi-tag none,cp311` — ABI tag(s)
- `--filter-platform-tag any,manylinux2014_x86_64,macosx_*_arm64` — platform tag(s), `*` wildcard
- `--filter-exclude-platform-tag` — exclusions (supports `*`)
- `--filter-min-python X.Y` — drop wheels built only for Python older than the floor
  (e.g. `3.10` drops cp36–cp39 and python-2 wheels). Version-agnostic wheels
  (`py3`, `py2.py3`), forward-compatible `abi3` wheels, and all sdists are kept.
- `--filter-exclude-dev` — drop PEP 440 dev releases (any version with a `.devN` segment)
- `--filter-exclude-prereleases` — drop PEP 440 pre-releases: alpha/beta/rc **and**
  dev (keep stable releases only). The superset of `--filter-exclude-dev`
- `--filter-exclude-windows` — drop Windows artifacts: `win*` wheels and legacy
  `.exe`/`.msi`/`.winXX` installers (a package whose name merely starts with
  "win", like `windrose`, is never matched)
- `--filter-max-size <size>` — drop artifacts larger than `size` (e.g. `250MB`,
  `1.5GiB`, `1048576`). Units are powers of 1024 (`KB` == `KiB`); a bare number is
  bytes. A file with no size in the upstream listing is kept. Useful for trimming
  multi-gigabyte CUDA/ML wheels off a disk- or S3-cost-bound mirror
- yanked files (PEP 592) are **dropped by default**. Pass `--filter-include-yanked`
  (or `[filter].include-yanked = true`) to mirror them anyway — they stay flagged
  yanked, so a pinned install still resolves. Either way the filter only gates what
  a run *pulls in*: a file already mirrored is never removed, and one that is yanked
  upstream after you mirrored it stays on disk and is re-flagged yanked by reconcile
- `--filter-exclude-newer <when>` — only files received upstream before the cutoff
- `--filter-exclude-older <when>` — only files received upstream since the cutoff

Every flag has a matching `PYPIRON_FILTER_*` env var (e.g.
`PYPIRON_FILTER_ONLY_WHEELS`, `PYPIRON_FILTER_PYTHON_TAG`) and an unprefixed
`[filter]` key (`only-wheels`, `python-tag`, …).

`<when>` (matching uv's `--exclude-newer`) is an **RFC 3339 timestamp**
(`2024-01-01T00:00:00Z`), a **bare date** (`2008-12-03`, taken as 00:00:00 UTC),
a **bare integer of days** ago (`7`), a **friendly duration** ago (`"30 days"`,
`"24 hours"`, `"1 week"`), or an **ISO 8601 duration** ago (`P30D`, `PT24H`). A
duration is resolved against the current time as a fixed number of seconds (a day
is 24 h); calendar months and years are rejected. The same forms apply to the
`[filter]` `exclude-newer`/`exclude-older` keys.

A sync run prints a live progress meter on stderr (packages done, files/bytes
mirrored, throughput, ETA) plus an always-on end-of-run summary; `--no-progress`
(`PYPIRON_SYNC_NO_PROGRESS`) silences the live line. When stderr is redirected to
a file, the meter prints one fresh line every 30 s instead of repainting.

## The config file (`pypiron.toml`)

Every subcommand reads `pypiron.toml` — pass `--config <path>` (global,
`PYPIRON_CONFIG`) or let it be auto-discovered as `./pypiron.toml` in the working
directory. `serve` and `sync` use the whole file; the maintenance commands
`verify-index`/`rebuild-index` read the `[serve]` storage selection so they
target the same backend `serve` does. Precedence is **CLI/env > file >
defaults**. Four parts:

- top-level `private-prefix` — the reserved private namespace, shared by both
  commands.
- `[filter]` — the slice of PyPI, names and files (shared by sync and the proxy;
  see above).
- `[serve]` — the server process. Every `serve` flag *except secrets*:
  admin/uploader/read passwords and the Azure access key stay in CLI/env. Storage
  selection (`storage`, `s3-bucket`, …) lives here too.
- `[sync]` — the push-mirror job: source/dest, the destination admin credential,
  concurrency.

```toml
private-prefix = "acme"

[filter]                                    # shared by sync and the serve proxy
packages = ["requests>=2.20,<3", "six"]     # or packages-list = "packages.txt"
only-wheels = true
python-tag = ["cp311", "cp312"]
exclude-newer = "2026-01-01T00:00:00Z"

[serve]
bind-addr = "0.0.0.0:8080"
storage = "s3"
s3-bucket = "acme-mirror"
proxy-upstream = "https://pypi.org"
artifact-delivery = "auto"

[sync]
to = "http://localhost:8080"
admin-user = "admin"                        # password via PYPIRON_SYNC_ADMIN_PASS
concurrency = 8
```

A `packages-list` path in the file resolves relative to the config file, not the
working directory. A CLI package source (`--filter-package` and/or
`--filter-packages-list`) replaces the file's `[filter].packages`/`packages-list`
entirely; other options layer per-key. A boolean set
`true` in the file can be turned on but not off by the absence of a flag (clap
cannot express an explicit `false`). The package list (`packages`,
`packages-list`), the artifact-filter keys, and `private-prefix` all used to live
under `[sync]` and now belong in `[filter]` (and at the top level); a stale config
fails to start with a pointer to the new home.

## Artifact delivery

Index pages always carry stable `/files/<pkg>/<filename>#sha256=...` URLs —
that's what ends up in lockfiles and client caches, and it never expires.
`--artifact-delivery` governs what happens when a client GETs one:

| Mode       | Behavior                                                                  |
| ---------- | ------------------------------------------------------------------------- |
| `auto`     | *(default)* Redirect clients that tolerate it (uv); stream everyone else |
| `redirect` | Always 302 to a presigned object-store URL — the node never touches wheel bytes |
| `stream`   | Always proxy bytes through the node with immutable cache headers         |

A presigned redirect moves the megabytes to object storage, but each response
carries a freshly signed URL. Clients whose download caches are keyed by the
serving URL (pip's HTTP cache) can never get a hit, so `redirect` silently turns
every fresh-environment pip install into a full re-download. uv is immune — it
caches wheels by index and filename. `auto` resolves this per request; use
`redirect` when node bandwidth is the binding constraint, `stream` when clients
can't reach the bucket endpoint (private subnet, firewalled storage). The disk
backend always streams — as does any cloud backend that can't sign URLs (GCS
under ADC, Azure without an account key). PEP 658 `.metadata` companions always
stream — tiny and resolution-critical. Full reasoning in
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md#read-path-zero-coordination).

## Healthcheck (`healthcheck`)

The `pypiron healthcheck` probe (the container's built-in `HEALTHCHECK`) takes
one knob:

| CLI Arg  | Env Var                    | Default                            | Description                                   |
| -------- | -------------------------- | ---------------------------------- | --------------------------------------------- |
| `--url`  | `PYPIRON_HEALTHCHECK_URL`  | `http://127.0.0.1:<bind port>/health` | Endpoint to probe; exit `0` on 2xx, else nonzero |

The default port follows `PYPIRON_BIND_ADDR` and the probe always targets
loopback. See [CLI → healthcheck](cli.md#healthcheck).

## Management and operations endpoints

Admin operations (delete, yank, PEP 792 project status, sync cursors) and the
operational endpoints (`/health`, `/metrics`, logging) live on the
[Management API](api.md) page.

## Storage layout

The layout *is* the schema — full contract in
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md#storage-layout-the-contract):

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
