# Configuration

Configure pypiron with flags, `PYPIRON_*` environment variables, or a
`pypiron.toml` file. Every flag has an env var; precedence is CLI/env > file >
defaults.

## Storage (`serve`)

`disk` is the zero-dependency default: point pypiron at a folder. For shared or
multi-node deployments, pick a cloud bucket with `--storage` — `s3` (or any
S3-compatible store), `gcs`, or `azure`. `sync` stores nothing; it's a pure HTTP
client that uploads to a server (see [Sync](#sync-mirror-over-http)).

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

Region comes from the standard `AWS_REGION` env var.

**AWS credentials** follow the standard AWS chain: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, web identity, or instance metadata.
An `http://` endpoint (local MinIO) is allowed automatically.

### Google Cloud Storage (`--storage gcs`)

| CLI Arg                        | Env Var                            | Default              | Description                                  |
| ------------------------------ | ---------------------------------- | -------------------- | -------------------------------------------- |
| `--gcs-bucket NAME`            | `PYPIRON_GCS_BUCKET`               | *(required for gcs)* | Bucket name                                  |
| `--gcs-service-account-path`   | `PYPIRON_GCS_SERVICE_ACCOUNT_PATH` | *(none)*             | Service-account JSON key file                |
| `--gcs-endpoint-url`           | `PYPIRON_GCS_ENDPOINT_URL`         | *(none)*             | Custom endpoint (local emulator)             |

**GCS credentials**: a service-account key (via the flag or the standard
`GOOGLE_*`/`GOOGLE_APPLICATION_CREDENTIALS` envs), otherwise Application Default
Credentials. **Presigned redirects (`--artifact-delivery redirect`/`auto`) need
a service-account key** — URL signing needs the private key; ADC tokens don't
have it. Under ADC, downloads stream.

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
need the account key** — signed SAS URLs derive from it; without it, downloads
stream.

!!! warning

    **Buckets/containers must already exist.** pypiron writes objects but
    doesn't create the bucket or container — provision it first.

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
| `--token-signing-key`        | `PYPIRON_TOKEN_SIGNING_KEY`        | *(none)*       | Secret that signs short-lived install tokens; enables `POST /tokens` and `__token__` auth (see below) |
| `--private-prefix`           | `PYPIRON_PRIVATE_PREFIX`           | *(none)*       | Reserve a namespace for private uploads          |
| `--proxy-upstream`           | `PYPIRON_PROXY_UPSTREAM`           | *(none)*       | On-demand mirror of this upstream simple index (plus mirror-selection flags, see below) |
| `--spool-dir`                | `PYPIRON_SPOOL_DIR`                | system temp    | Upload/proxy spool directory — real disk, not tmpfs |
| `--log-format`               | `PYPIRON_LOG_FORMAT`               | `text`         | `text` or `json` (one object per line)           |
| `--access-log`               | `PYPIRON_ACCESS_LOG`               | `false`        | Also log reads (downloads/listings), not just mutations (see below) |
| `--access-log-format`        | `PYPIRON_ACCESS_LOG_FORMAT`        | `structured`   | `structured` (key=value/JSON) or `clf` (Combined Log Format) |
| `--worker-interval-secs`     | `PYPIRON_WORKER_INTERVAL_SECS`     | `1`            | Worker tick interval (writes also nudge the worker directly) |
| `--reconcile-interval-secs`  | `PYPIRON_RECONCILE_INTERVAL_SECS`  | `86400`        | Audit sweep interval (skips unchanged files; cost scales with churn) |
| `--audit-on-boot`            | `PYPIRON_AUDIT_ON_BOOT`            | `true`         | Audit as soon as this node becomes leader        |
| `--intent-grace-secs`        | `PYPIRON_INTENT_GRACE_SECS`        | `900`          | How long a write in progress may defer its package's rebuild |
| `--lease-ttl-secs`           | `PYPIRON_LEASE_TTL_SECS`           | `30`           | Leader lease TTL (multi-node S3)                 |
| `--artifact-delivery`        | `PYPIRON_ARTIFACT_DELIVERY`        | `auto`         | How artifact bytes reach clients (see below)     |
| `--wait-on-upload`           | `PYPIRON_WAIT_ON_UPLOAD`           | `false`        | Wait for index visibility before returning 200   |
| `--wait-on-upload-secs`      | `PYPIRON_WAIT_ON_UPLOAD_SECS`      | `10`           | Bound on the wait-on-upload poll                 |
| `--download-stats`           | `PYPIRON_DOWNLOAD_STATS`           | `true`         | Count per-package/version downloads per day (see below) |
| `--counters-resolution`      | `PYPIRON_COUNTERS_RESOLUTION`      | `1d`           | Counter bucket width: `1d`/`1h`/`30m`/`2h` (whole minutes dividing a day) |
| `--counters-flush-interval-secs`  | `PYPIRON_COUNTERS_FLUSH_INTERVAL_SECS`  | `300` | How often each node flushes counts (the dominant cost knob) |
| `--counters-rollup-interval-secs` | `PYPIRON_COUNTERS_ROLLUP_INTERVAL_SECS` | `3600` | How often finished days are compacted and old history pruned |
| `--counters-retention-days`  | `PYPIRON_COUNTERS_RETENTION_DAYS`  | `90`           | Days of per-day counter history to keep           |

Disable the two default-on toggles — `--audit-on-boot`, `--download-stats` —
with an explicit value: `--audit-on-boot false` / `--download-stats false` (or
`PYPIRON_AUDIT_ON_BOOT=false` / `PYPIRON_DOWNLOAD_STATS=false`), not a `--no-`
form.

### Access log

Each logged request is one line — method, path, status, latency,
response size, client IP (honoring `X-Forwarded-For` / `X-Real-IP` behind a
proxy), project tag, and User-Agent. What's logged depends on the request:

- **By default** (no `--access-log`) only **mutations** are logged — uploads,
  deletes, yanks, status changes (any non-GET/HEAD) — at `info`. A small,
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

### Download counters

`--download-stats` (on by default) counts artifact downloads per package and
version, served at `GET /stats/downloads/<pkg>` and `GET /stats/downloads` (both
read-auth gated). Costs about **$0.04 per node per month**, no database. Finished
days are exact; today lags one flush interval. Tune it with the `--counters-*`
flags above. Full detail in
[Download statistics](../concepts/download-stats.md).

!!! note "Large uploads"

    The upload spool defaults to the system temp dir. In containers where
    `/tmp` is RAM-backed tmpfs, point it at real disk or multi-GB wheels spool
    into memory: `-v /data/spool:/spool -e PYPIRON_SPOOL_DIR=/spool`.

## Authentication

Three optional credentials, each a username/password flag pair (with matching
`PYPIRON_*` env vars), also in the [Server](#server) table:

| Credential | Flags |
| --- | --- |
| admin | `--admin-user` / `--admin-pass` |
| uploader | `--uploader-user` / `--uploader-pass` |
| read | `--read-user` / `--read-pass` |

The admin username defaults to `admin`, so `--admin-pass secret` alone is a
complete admin credential. With no write credential the server is read-only;
with no read credential, reads are public. Set `--read-user` and `--read-pass`
and `/simple/` and `/files/` require auth (any of the three credentials works;
`/health` and `/metrics` stay open for probes):

```bash
pip install --index-url http://reader:secret@localhost:8080/simple/ mypackage
```

What each role can do, how they nest, and the fail-closed rules:
[Authentication](../concepts/authentication.md).

### Per-project download tracking

Usernames support tags. `reader+billing-api`
authenticates as `reader` (password still required) and records `billing-api`
as a project tag — per-tag counts appear in `/metrics` as
`pypiron_project_requests_total{project=...,route=...}` and in the debug
request logs. With uv:

```bash
export UV_INDEX_COMPANY_USERNAME="reader+billing-api"
export UV_INDEX_COMPANY_PASSWORD="secret"
```

Works on open servers too: with no read credential, any volunteered username is
parsed for attribution and the password ignored. Tag cardinality in `/metrics`
is capped (overflow lands in `_overflow`); tags are restricted to
`[A-Za-z0-9._-]`, max 64 chars.

### Install tokens

Set `--token-signing-key` and a client can trade a credential for a short-lived
(5-minute) install token instead of spreading the durable password across every
CI step. The token is presented as basic-auth username `__token__`; its role
never exceeds the credential that minted it (default `reader`).

```bash
# Mint from CI (auto-detects repo/commit/user), then install with it:
export UV_INDEX_COMPANY_USERNAME=__token__
export UV_INDEX_COMPANY_PASSWORD=$(pypiron create-token --url http://pypiron:8080 --auth reader:secret)
uv sync
```

Tokens are stateless and self-expiring — nothing is stored, so the signing key
must be identical on every node (like the other credentials). Generate one with
`openssl rand -hex 32`. Full flow and the `POST /tokens` shape:
[Authentication](../concepts/authentication.md#install-tokens).

## Sync (mirror over HTTP)

`pypiron sync` mirrors packages from a PEP 691 source into a pypiron server. A
pure HTTP client: each file is POSTed to the destination's `/legacy/` as a
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

`--to` is mandatory (no direct-to-storage mode); without it (and no `[sync].to`)
the run refuses to start. Which packages get mirrored is the mirror name axis —
`--include-package` / `--include-packages-from` / `[mirror].include-packages`
(see [Mirror selection](#mirror-selection)) — and `sync` requires a non-empty
set: an explicit work list. The same scope governs the proxy.

## Re-sync, reconcile, and conditional fetch

A re-`sync` doesn't just add new files — it *reconciles* what it already holds:
yank state is brought in line with upstream (set, cleared, or reason updated), a
file gone from upstream is flagged yanked `removed upstream` (kept downloadable,
skipped by installers), and PEP 792 project status is relayed. Artifacts are
never deleted.

To keep "reconcile every run" cheap, each project is fetched conditionally: the
last upstream ETag is remembered server-side (`_sync/cursors.json`, served by
the admin-only `GET`/`PUT /sync/cursors`) and replayed as `If-None-Match`, so an
unchanged upstream answers `304` and the project is skipped. The cursor is a
pure cache — delete it and the next run re-fetches.

- `--full` (`PYPIRON_SYNC_FULL`) — ignore the cursor memo: re-fetch every
  project unconditionally and fully reconcile. Run periodically (e.g. nightly)
  as a full catch-up, since a normal run only reconciles projects whose upstream
  listing changed.

The cursor key folds in the source URL, the resolved mirror selection, and each
project's specifiers, so changing any of them invalidates the shortcut and
forces a full fetch.

## Mirror Selection

`[mirror]` is the slice of PyPI you want — names *and* files. Shared verbatim by
`pypiron sync` (push mirror) and `serve --proxy-upstream` (on-demand pull
mirror): set it once and both paths agree. Mirror selection gates only what is
*added*; already-mirrored or already-cached files are never removed by later
selection changes.

| `[mirror]` key | CLI flag | Env var |
| --- | --- | --- |
| `include-packages` | `--include-package SPEC` | `PYPIRON_INCLUDE_PACKAGE` |
| `include-packages-from` | `--include-packages-from FILE` | `PYPIRON_INCLUDE_PACKAGES_FROM` |
| `exclude-packages` | `--exclude-package SPEC` | `PYPIRON_EXCLUDE_PACKAGE` |
| `exclude-packages-from` | `--exclude-packages-from FILE` | `PYPIRON_EXCLUDE_PACKAGES_FROM` |
| `include-format` | `--include-format VALUE` | `PYPIRON_INCLUDE_FORMAT` |
| `include-python-tag` | `--include-python-tag TAG` | `PYPIRON_INCLUDE_PYTHON_TAG` |
| `include-abi-tag` | `--include-abi-tag TAG` | `PYPIRON_INCLUDE_ABI_TAG` |
| `include-platform-tag` | `--include-platform-tag TAG` | `PYPIRON_INCLUDE_PLATFORM_TAG` |
| `exclude-python-tag` | `--exclude-python-tag TAG` | `PYPIRON_EXCLUDE_PYTHON_TAG` |
| `exclude-abi-tag` | `--exclude-abi-tag TAG` | `PYPIRON_EXCLUDE_ABI_TAG` |
| `exclude-platform-tag` | `--exclude-platform-tag TAG` | `PYPIRON_EXCLUDE_PLATFORM_TAG` |
| `exclude-python-below` | `--exclude-python-below X.Y` | `PYPIRON_EXCLUDE_PYTHON_BELOW` |
| `exclude-larger` | `--exclude-larger SIZE` | `PYPIRON_EXCLUDE_LARGER` |
| `exclude-newer` | `--exclude-newer WHEN` | `PYPIRON_EXCLUDE_NEWER` |
| `exclude-older` | `--exclude-older WHEN` | `PYPIRON_EXCLUDE_OLDER` |
| `exclude-dev` | `--exclude-dev` | `PYPIRON_EXCLUDE_DEV` |
| `exclude-windows` | `--exclude-windows` | `PYPIRON_EXCLUDE_WINDOWS` |
| `exclude-prereleases` | `--exclude-prereleases` | `PYPIRON_EXCLUDE_PRERELEASES` |
| `include-yanked` | `--include-yanked` | `PYPIRON_INCLUDE_YANKED` |

Package specs use one syntax everywhere: a PEP 503-normalized name with optional
PEP 440 specifiers (`requests`, `six==1.16.0`, `requests>=2.20,<3`). Inline TOML
keys are arrays; CLI flags are singular and repeatable. Commas belong to PEP 440
specifiers, so `PYPIRON_INCLUDE_PACKAGE` and `PYPIRON_EXCLUDE_PACKAGE` each carry
one spec. For many names, prefer `PYPIRON_INCLUDE_PACKAGES_FROM` /
`PYPIRON_EXCLUDE_PACKAGES_FROM`.

Selection has two stages, and exclude wins in both:

1. Name stage: a project/version is allowed when `include-packages` is empty or
   the name matches an include spec, and no exclude spec matches. A bare
   `exclude-packages` name drops the whole project. A version-pinned exclude
   drops only files whose inferred version parses and satisfies the specifier.
   If the version can't be parsed, pypiron keeps the file — it can't prove the
   artifact is denied.
2. File stage: surviving projects pass through the format, tag, upload-time,
   yanked, size, prerelease, and Windows gates.

`sync` requires a non-empty work list from `include-packages` or
`include-packages-from`; an exclude list alone isn't work. The proxy has split
semantics: empty include means an open proxy for any non-private name; a
non-empty include is a fail-closed allowlist. `exclude-packages` subtracts in
both modes, so this is an open proxy minus a denylist:

```bash
pypiron serve \
  --proxy-upstream https://pypi.org \
  --exclude-package legacy-insecure \
  --exclude-package "demo<2"
```

`include-format` accepts `wheel`, `sdist`, and `other`; repeatable and
comma-separated (`--include-format wheel,sdist`). Unset means all formats.
`wheel` is `.whl`; `sdist` is `.tar.gz`, `.tgz`, `.tar.bz2`, or `.zip`; `other`
is everything else. `exclude-windows` independently drops `.exe`, `.msi`,
`.winXX`, and Windows wheel platforms.

Other file-axis rules:

- `--include-python-tag`, `--include-abi-tag`, and `--include-platform-tag`
  match wheel tags; the `--exclude-python-tag` / `--exclude-abi-tag` /
  `--exclude-platform-tag` twins subtract by the same tags. All support `*`
  wildcards. The exclude twins are exclusion-only: they drop matching wheels but
  never touch sdists (which carry no tags), so `--exclude-python-tag pp*` (or
  `--exclude-abi-tag pypy*`) drops PyPy wheels while leaving every sdist and
  CPython wheel — the honest spelling of "no PyPy".
- `--exclude-python-below X.Y` drops wheels built only for Python older than the
  floor. Version-agnostic wheels (`py3`, `py2.py3`), forward-compatible `abi3`
  wheels, and all sdists are kept.
- `--exclude-dev` drops PEP 440 dev releases. `--exclude-prereleases` drops
  alpha, beta, rc, and dev releases.
- `--exclude-larger SIZE` drops artifacts larger than `SIZE` (`250MB`,
  `1.5GiB`, `1048576`). Units are powers of 1024; a missing upstream size is
  kept.
- Yanked files (PEP 592) are dropped by default. Pass `--include-yanked` or set
  `[mirror].include-yanked = true` to pull them anyway; they stay flagged yanked.
- `--exclude-newer WHEN` keeps files received upstream before the cutoff.
  **Defaults to `7`** — a sliding 7-day quarantine that holds fresh releases back
  from the mirror and the proxy, so an install-then-yank supply-chain attack has
  a week to be caught before any client can pull it. Set `exclude-newer = ""`
  (empty) to turn the cooldown off and mirror everything to the present.
- `--exclude-older WHEN` keeps files received upstream at or after the cutoff.

`<when>` (matching uv's `--exclude-newer`) is an **RFC 3339 timestamp**
(`2024-01-01T00:00:00Z`), a **bare date** (`2008-12-03`, taken as 00:00:00 UTC),
a **bare integer of days** ago (`7`), a **friendly duration** ago (`"30 days"`,
`"24 hours"`, `"1 week"`), or an **ISO 8601 duration** ago (`P30D`, `PT24H`). A
duration resolves against the current time as a fixed number of seconds (a day
is 24 h); calendar months and years are rejected. The same forms apply to the
`[mirror]` `exclude-newer`/`exclude-older` keys. An empty value (`""`) means "no
cutoff" — the explicit opt-out from the default 7-day cooldown.

Unlike uv's client-side `--exclude-newer` (which treats a file with no
`upload-time` as unavailable), the mirror **keeps** a file whose upstream listing
carries no parseable upload time — time bounds act only on files that can be
placed in time, so an upstream without PEP 700 timestamps still mirrors fully
instead of going silently empty.

A sync run prints a live progress meter on stderr (packages done, files/bytes
mirrored, throughput, ETA) plus an always-on end-of-run summary; `--no-progress`
(`PYPIRON_SYNC_NO_PROGRESS`) silences the live line. When stderr is redirected to
a file, the meter prints one fresh line every 30 s instead of repainting.

### Recipes

Common slices, ready to paste into `[mirror]`. Each is built from the keys
above — copy one, or merge two. Explicit CLI/env flags still override the file,
and runnable copies live in [`examples/mirror/`](https://github.com/blackthorn-interstellar/pypiron/tree/master/examples/mirror).

**Lean Linux CI mirror.** A small, fast mirror for Linux runners: wheels only,
no Windows or macOS, supported Pythons, released versions. Drops the long-tail
bulk CI never installs.

```toml
[mirror]
include-packages-from = "approved.txt"        # sync needs a work list; omit for a proxy
include-format = ["wheel"]
exclude-platform-tag = ["win*", "macosx_*"]    # drop Windows + macOS; keep `any` + sdists
exclude-python-below = "3.9"                   # pin the floor; bump it on your schedule
exclude-prereleases = true
# exclude-newer defaults to "7" — the 7-day cooldown stays on unless you set it
```

Build the OS filter from `exclude-platform-tag`, never an `include-platform-tag`
allowlist: an allowlist silently drops pure-Python (`any`) wheels and every
sdist, so the mirror builds clean and then can't install half of PyPI.
Trade-off: a project that ships *only* an sdist won't be mirrored — add it back
with a name include.

**No PyPy.** Keep CPython wheels and sdists; drop PyPy-only binaries.

```toml
[mirror]
exclude-python-tag = ["pp*"]    # PyPy wheels carry python tag pp39/pp310/…
```

Trade-off: a package that ships *only* PyPy wheels for some platform loses those
files; its sdist (if any) still mirrors.

**Stable only.** Released versions — no alpha, beta, rc, or dev.

```toml
[mirror]
exclude-prereleases = true
```

Trade-off: a project that only ever publishes prereleases disappears entirely.
Clear it by deleting the line.

**Air-gapped full mirror.** Everything, no cooldown, including yanked files — a
complete offline copy you control.

```toml
[mirror]
exclude-newer = ""        # turn OFF the 7-day quarantine
include-yanked = true
```

A recipe is a starting point your own keys and CLI flags override. A switch a
recipe turns *on* can't be turned off by a missing flag — there's no flag for
`false` — so clear it by setting it `false` (or deleting the line) in your file.

## The config file (`pypiron.toml`)

Every subcommand reads `pypiron.toml` — pass `--config <path>` (global,
`PYPIRON_CONFIG`) or let it auto-discover as `./pypiron.toml` in the working
directory. `serve` and `sync` use the whole file; the maintenance commands
`verify-index`/`rebuild-index` read the `[serve]` storage selection to target
the same backend `serve` does. Precedence is **CLI/env > file > defaults**. Four
parts:

- top-level `private-prefix` — the reserved private namespace, shared by both
  commands.
- `[mirror]` — the slice of PyPI, names and files (shared by sync and the proxy;
  see above).
- `[serve]` — the server process. Every `serve` flag *except secrets*:
  admin/uploader/read passwords and the Azure access key stay in CLI/env. Storage
  selection (`storage`, `s3-bucket`, …) lives here too.
- `[sync]` — the push-mirror job: source/dest, the destination admin credential,
  concurrency.

```toml
private-prefix = "acme"

[mirror]                                    # shared by sync and the serve proxy
include-packages = ["requests>=2.20,<3", "six"]     # or include-packages-from = "packages.txt"
exclude-packages = ["legacy-insecure", "demo<2"]
include-format = ["wheel"]
include-python-tag = ["cp311", "cp312"]
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

`include-packages-from` and `exclude-packages-from` paths in the file resolve
relative to the config file, not the working directory. A CLI package source
(`--include-package` and/or `--include-packages-from`, or the exclude twins)
replaces the matching file source entirely; other options layer per-key. A
boolean set `true` in the file can't be turned off by a missing flag — there is
no flag for `false`, so clear it by setting it `false` (or deleting the line) in
the file.

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
serving URL (pip's HTTP cache) never get a hit, so `redirect` silently turns
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

pypiron stores every artifact and its metadata as plain files on disk or in your
bucket — no database, so a backup is a copy of the directory. The exact on-disk
layout is an internal contract; for that depth, see
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md#storage-layout-the-contract).
