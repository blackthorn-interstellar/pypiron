---
description: Every pypiron flag, PYPIRON_ environment variable, and pypiron.toml setting for running a self-hosted PyPI server, with defaults and precedence.
---

# Configuration

Configure pypiron with flags, `PYPIRON_*` environment variables, or
`pypiron.toml`.

Precedence: **CLI/env > file > defaults**.

## Global

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--config PATH` | `PYPIRON_CONFIG` | `./pypiron.toml` when present | Config file. Read by every command. |
| `--log-format text\|json` | `PYPIRON_LOG_FORMAT` | `text` | Human logs or one JSON object per line. |

## `pypiron.toml`

```bash
pypiron config init > pypiron.toml
```

The generated file is fully commented. A small real config looks like this:

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
buckets = ["s3://acme-pypiron@us-east-1"]
proxy-upstream = "https://pypi.org"

[mirror]
exclude-newer = "7 days"
include-format = ["wheel"]

[sync]
to = "http://localhost:8080"
```

Sections:

| Section | Owns |
| --- | --- |
| top level | `private-prefix` |
| `[serve]` | server, proxy, storage, counters, logs |
| `[mirror]` | package and file selection shared by proxy and sync |
| `[sync]` | destination and sync worker settings |

Serve secrets stay in CLI/env. `sync.admin-pass` exists for closed deployment
files, but env is cleaner: `PYPIRON_SYNC_ADMIN_PASS`.

## Storage

Disk is the default — no configuration, artifacts under `~/.pypiron/packages`.
Point `--buckets` at object storage for more than one node, or to keep the store
off the box. Buckets must already exist.

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--buckets URI,...` | `PYPIRON_BUCKETS` | none (disk) | One or more bucket URIs, any mix of backends, in preference order. Unset means disk. |
| `--data-dir PATH` | `PYPIRON_DATA_DIR` | `~/.pypiron/packages` | Disk root. Ignored when `--buckets` is set. |
| `--storage-prefix PREFIX` | `PYPIRON_STORAGE_PREFIX` | none | Keep everything under one subtree, so pypiron can share a bucket. |

`--buckets` is the one knob that picks where artifacts live. One entry is
single-bucket mode; several enable replication and failover — see [Multiple
regions and clouds](#multiple-regions-and-clouds).

### One bucket

```
pypiron serve --buckets s3://acme-pypiron
```

That is the whole setup. Credentials come from the backend's native chain — for
S3 the standard AWS chain (env vars, web identity, instance role, task role) — so
there is nothing else to set. `PYPIRON_BUCKETS=s3://acme-pypiron` and the file
form are equivalent:

```toml
[serve]
buckets = ["s3://acme-pypiron"]
```

A single-entry list behaves exactly like a directly configured backend: no replication or failover work runs.

Every URI carries a scheme, and may carry an `@region`:

- `s3://name` or `s3://name@region` — S3 bucket.
- `gs://name` or `gs://name@region` — GCS bucket.
- `az://container` or `az://container@region` — Azure blob container.

`@region` labels the bucket's region. On S3 it also selects the signing and
endpoint region (otherwise the ambient `AWS_REGION` / `AWS_DEFAULT_REGION`, then
the SDK default). In a multi-bucket fleet the label also steers in-region reads.

### Credentials by backend

Each backend resolves its own native credentials. A bucket whose backend is
half-configured refuses to start.

- **S3** — the standard AWS chain: env vars, web identity, instance role, task
  role.
- **GCS** — a service-account key (`--gcs-service-account-path`) enables
  presigned redirects; without one, pypiron uses Application Default Credentials
  and downloads stream through the node.
- **Azure** — the account access key (`--azure-access-key`) enables presigned
  (SAS) redirects.

### Backend-wide settings

These apply to every bucket of their backend — the common case of one endpoint or
one account:

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--s3-endpoint-url URL` | `PYPIRON_S3_ENDPOINT_URL` | none | S3-compatible endpoint (MinIO et al.); every `s3://` bucket. |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false` | Path-style addressing; every `s3://` bucket. |
| `--gcs-service-account-path PATH` | `PYPIRON_GCS_SERVICE_ACCOUNT_PATH` | none | GCS service-account JSON key. Enables presigned redirects. |
| `--gcs-endpoint-url URL` | `PYPIRON_GCS_ENDPOINT_URL` | none | GCS local emulator or custom endpoint. |
| `--azure-account NAME` | `PYPIRON_AZURE_ACCOUNT` | none | Azure storage account. |
| `--azure-access-key KEY` | `PYPIRON_AZURE_ACCESS_KEY` | none | Azure account key; enables signed (SAS) URLs. CLI/env only, never in the file. |
| `--azure-endpoint-url URL` | `PYPIRON_AZURE_ENDPOINT_URL` | none | Azurite or custom endpoint. |
| `--azure-use-emulator` | `PYPIRON_AZURE_USE_EMULATOR` | `false` | Use Azurite defaults. |

When one endpoint or one account is not enough — two S3 accounts, AWS plus MinIO,
two Azure accounts in one list — override per bucket instead.

### Per-bucket overrides

For the rare fleet where buckets need different endpoints or credentials, give a
bucket its own `[serve.bucket."URI"]` table in `pypiron.toml`. TOML only — no
CLI or env form.

```toml
[serve]
buckets = ["s3://iron-east@us-east-1", "s3://minio-cache"]

[serve.bucket."s3://minio-cache"]
endpoint-url = "http://minio.internal:9000"
force-path-style = true
env-prefix = "MINIO_CACHE_"   # reads MINIO_CACHE_AWS_ACCESS_KEY_ID / ..._AWS_SECRET_ACCESS_KEY
```

The table is keyed by the bucket's URI, matched on `scheme://name` — pypiron
ignores any `@region`, so `"s3://minio-cache"` and
`"s3://minio-cache@us-west-2"` name the same bucket. Fields are scheme-specific:

| Field | Schemes | Meaning |
| --- | --- | --- |
| `endpoint-url` | any | Endpoint for this bucket, overriding the backend-wide flag. An `http://` value allows plaintext. |
| `force-path-style` | s3 | Path-style addressing for this bucket. |
| `env-prefix` | s3, azure | Names the env vars holding this bucket's credentials (below). |
| `service-account-path` | gcs | Service-account JSON key for this bucket. Also enables presigned redirects for it. |
| `account` | azure | Storage account for this bucket. |

Secrets never live in the file. `env-prefix` names *where* the credentials are,
and pypiron reads them from the environment:

- **S3** — `<PREFIX>AWS_ACCESS_KEY_ID`, `<PREFIX>AWS_SECRET_ACCESS_KEY`, and
  optional `<PREFIX>AWS_SESSION_TOKEN`.
- **Azure** — `<PREFIX>AZURE_ACCESS_KEY`.
- **GCS** has no `env-prefix`: its credential is a key file, so use
  `service-account-path` instead.

pypiron validates everything at startup, before touching any bucket:

- An override keyed to a bucket not in `--buckets` fails, listing the valid
  buckets (typo protection).
- An `env-prefix` with only one credential half set — or neither — fails: a
  scoped credential was promised and isn't fully there.
- A field used on the wrong scheme fails, naming the field and bucket.

### What is per-bucket, what is backend-wide

- **Per-bucket:** the URI and its `@region`, plus every `[serve.bucket."..."]`
  field — endpoint, path style, credentials, account, service-account key.
- **Backend-wide:** the `--s3-*`, `--gcs-*`, and `--azure-*` flags and the ambient
  credential chains. They set the default for every bucket of that backend; a
  per-bucket table overrides them for one bucket.

### Sharing a bucket

By default pypiron owns the whole bucket: artifacts land in `packages/`, indexes
in `simple/`. Set `--storage-prefix pypi` and they move to `pypi/packages/` and
`pypi/simple/` instead, leaving the rest of the bucket alone — pypiron neither
reads nor writes outside its prefix. Two servers can share one bucket by taking
different prefixes.

On disk the prefix is a subdirectory of `--data-dir`.

Point an existing server at a prefix and it looks empty: the prefix is part of
every key, so choose it once, when you first populate the bucket. To adopt
one later, move the objects (`aws s3 mv --recursive`, `gcloud storage mv`) before
restarting.

### Multiple regions and clouds

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--buckets URI,...` | `PYPIRON_BUCKETS` | none | Bucket URIs in preference order, any mix of backends. Enables cross-region and cross-cloud replication and failover. |

Spread the index across regions or cloud providers by giving pypiron a list of
buckets instead of one. It keeps them in sync and rides out the loss of any one —
a region, or a whole cloud. Installs never stop, and an upload is durable on every
reachable bucket before it returns. Give every node the same ordered list; set
`--buckets` (or `PYPIRON_BUCKETS`) to a comma-separated list of URIs, first is
preferred:

```
pypiron serve --buckets s3://iron-east@us-east-1,s3://iron-west@us-west-2
PYPIRON_BUCKETS=s3://iron-east@us-east-1,s3://iron-west@us-west-2
```

Each entry needs a scheme and may carry an `@region` label:

- `s3://name` or `s3://name@region` — an S3 bucket.
- `gs://name` or `gs://name@region` — a GCS bucket.
- `az://container` or `az://container@region` — an Azure blob container.

The `@region` label names the bucket's region. Every node compares its own
region to these labels to pick the bucket it reads from — reads stay in-region
while writes still fan out everywhere (see
[Reads stay in their region](../guides/multi-region.md#reads-stay-in-their-region)).
The label is part of the shared list, so it is identical on every node and does
not affect a bucket's identity or the fleet's bucket order. On S3 it also
selects the client's signing and endpoint region; precedence there is per-bucket
`@region`, then the ambient `AWS_REGION` / `AWS_DEFAULT_REGION`, then the SDK
default. GCS and Azure endpoints do not encode a region, so on those backends the
label steers reads only.

Mix backends freely. `s3://iron-east@us-east-1,gs://iron-backup` replicates
across two clouds and survives an entire provider outage. Each backend resolves
its own native credentials (the AWS chain for S3, a service-account key or
Application Default Credentials for GCS, the account key for Azure); a bucket
whose backend is half-configured refuses to start. One bad URI fails startup
before pypiron contacts any bucket.

Replication keeps a full copy of your files on every bucket: your own uploads,
the corpus you pulled in with `sync --to`, **and** the on-demand proxy cache
(packages fetched from public PyPI on first request). The proxy cache converges
in the background — a fetch is served the instant it lands, and its copy to the
other buckets follows within minutes, off the download path — so budget for roughly your total stored size times the number of buckets. No new knobs: replication is on whenever you run more than
one bucket.

Replication streams each artifact through pypiron. This lets pypiron verify the
source hash before changing another bucket and use a conditional create at the
destination, so a concurrent upload cannot be overwritten by a cloud-provider
copy operation. Budget node network capacity for both the read and the write.

In multi-bucket mode a failing bucket stops taking new requests within a
second and its background work is cancelled; transfers already in flight keep
the normal one-hour bound. Startup and migration operations also carry a
one-second bound per bucket, so an unreachable bucket can slow boot but never
blocks it while another configured bucket is reachable. Admin DELETE still
removes private files but returns `409` for proxy-cached entries — cache eviction
across buckets is unavailable, so do not apply a broad `packages/` lifecycle rule
(private and mirror records share that prefix).

See [Survive a region or cloud outage](../guides/multi-region.md) for deployment,
recovery behavior, and operator rules.

## Server

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--bind-addr ADDR` | `PYPIRON_BIND_ADDR` | `0.0.0.0:8080` | Listen address. |
| `--admin-user USER` | `PYPIRON_ADMIN_USER` | `admin` | Admin username. |
| `--admin-pass PASS` | `PYPIRON_ADMIN_PASS` | none | Enables publish, mirror, delete, yank. |
| `--uploader-user USER` | `PYPIRON_UPLOADER_USER` | none | Upload-only username. |
| `--uploader-pass PASS` | `PYPIRON_UPLOADER_PASS` | none | Upload-only password. |
| `--read-user USER` | `PYPIRON_READ_USER` | none | Optional read username. |
| `--read-pass PASS` | `PYPIRON_READ_PASS` | none | Optional read password. |
| `--private-prefix PREFIX` | `PYPIRON_PRIVATE_PREFIX` | none | Reserve `PREFIX` and `PREFIX-*` for private packages. |
| `--proxy-upstream URL` | `PYPIRON_PROXY_UPSTREAM` | none | On-demand mirror source, usually `https://pypi.org`. |
| `--allow-insecure-upstream` | `PYPIRON_ALLOW_INSECURE_UPSTREAM` | `false` | Permit a plaintext `http://` proxy upstream. Off by default: over http a network MITM controls both the artifact bytes and the sha256 they are verified against, so the hash check stops being a control. |
| `--proxy-allow-host HOST` | `PYPIRON_PROXY_ALLOW_HOST` | none | Permit the proxy to fetch listing-derived URLs (artifact, `.metadata`, `.provenance`, redirect targets) whose host matches `HOST` exactly, even if it resolves to a private address. Repeatable; comma-separated in the env var. Only the configured upstream host is exempt otherwise. |
| `--proxy-allow-cidr CIDR` | `PYPIRON_PROXY_ALLOW_CIDR` | none | Like `--proxy-allow-host`, but permits any target IP inside `CIDR` (e.g. `10.0.0.0/8`). Repeatable; comma-separated in the env var. |
| `--upstream-ca-cert PEM` | `PYPIRON_UPSTREAM_CA_CERT` | none | PEM bundle of extra CA certificates to trust for **upstream** TLS — the private root a corporate forwarding TLS proxy (a MITM appliance) presents. Augments the built-in roots (a direct fetch of public PyPI keeps working); it does not replace them. Applied to the proxy upstream fetch and the advisory feed/probe. Loaded fail-closed at startup: a missing or unparseable bundle refuses to start. See [Behind a forward proxy](#behind-a-forward-proxy-or-tls-interception). |
| `--advisory-feed URL\|PATH` | `PYPIRON_ADVISORY_FEED` | OSV PyPI export | Feed for malware blocking and the org audit: a URL or local path to the [OSV](https://osv.dev) PyPI advisory export. Defaults to `https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip` (named in the startup log); a URL fetch honors `HTTP(S)_PROXY`. `""` disables both features. |
| `--malware-block true\|false` | `PYPIRON_MALWARE_BLOCK` | `true` | Refuse to serve or cache files an OSV `MAL-*` advisory flags as malicious. Needs a snapshot source — the feed, or one delivered earlier by `sync`. Explicit `true` that can't obtain a snapshot refuses startup; the default warns "armed but unfed" and self-arms when a snapshot arrives. Scoped to OSV advisory blocking only: PEP 792 quarantine refusal (a project PyPI froze) is enforced independently and stays on when this is `false`. |
| `--malware-probe-secs N` | `PYPIRON_MALWARE_PROBE_SECS` | `120` | How often each node checks OSV for a just-published malware advisory and blocks it within minutes, ahead of the daily feed refresh. Near-zero bandwidth (a conditional check that transfers nothing most polls) and no stored state. `0` disables; inert unless blocking is armed and the feed is the OSV `all.zip` URL. |
| `--metrics-project-labels` | `PYPIRON_METRICS_PROJECT_LABELS` | `false` | Attach per-client `project` labels to `/metrics`. Off by default: `/metrics` is unauthenticated and the label derives from the username tag, so exposing it lets any scraper list internal project names. |
| `--spool-dir PATH` | `PYPIRON_SPOOL_DIR` | system temp | Upload/proxy spool directory. |
| `--artifact-delivery auto\|redirect\|stream` | `PYPIRON_ARTIFACT_DELIVERY` | `auto` | Redirect object-store downloads when the client handles it well; otherwise stream. |
| `--wait-on-upload` | `PYPIRON_WAIT_ON_UPLOAD` | `false` | Wait for index visibility before upload returns. |
| `--wait-on-upload-secs N` | `PYPIRON_WAIT_ON_UPLOAD_SECS` | `10` | Bound for that wait. |
| `--access-log` | `PYPIRON_ACCESS_LOG` | `false` | Log reads too, not only mutations. |
| `--access-log-format structured\|clf` | `PYPIRON_ACCESS_LOG_FORMAT` | `structured` | Structured logs or Combined Log Format. |
| `--trusted-proxy` | `PYPIRON_TRUSTED_PROXY` | `false` | Honor `X-Forwarded-For`/`X-Real-IP` for the logged client IP. Off by default: those headers are client-settable, so ignoring them keeps a direct caller from spoofing its audit-logged address, and the direct peer address is logged instead. Enable only behind a reverse proxy that sets them. |
| `--login-cooldown-secs N` | `PYPIRON_LOGIN_COOLDOWN_SECS` | `300` | Failed-login cooldown. Five failed logins from one address, each within this window of the last, and that address is refused further credential-bearing requests (`429` with `Retry-After`) until the window passes — even with the correct password, so a lucky guess can't be confirmed. Successes are never counted, anonymous requests are never throttled, and IPv6 counts per /64. Enforced per instance. `0` disables. |
| `--worker-interval-secs N` | `PYPIRON_WORKER_INTERVAL_SECS` | `1` | Index, replication, and bucket-health poll cadence. |
| `--bucket-leave-failures N` | `PYPIRON_BUCKET_LEAVE_FAILURES` | `3` | Consecutive timeout (including 408), connection, or 5xx failures before selecting the next bucket. |
| `--bucket-return-healthy-secs N` | `PYPIRON_BUCKET_RETURN_HEALTHY_SECS` | `300` | Continuous health required before returning to a more-preferred bucket. Keep it minutes, not seconds: where reads are steered to a region bucket, a value at or below roughly `2 × buckets + 1` seconds lets downloads move back to a recovered bucket before pypiron has confirmed it caught up, and a file published during the outage is then fetched from the write bucket instead — slower, never a failed download. Startup warns when the configured value is that low. |
| `--node-region LABEL` | `PYPIRON_NODE_REGION` | detected | This node's region, matched against bucket `@region` labels to choose the bucket it reads from. Cloud nodes detect it automatically (AWS/GCP/Azure); set it for on-prem or MinIO. Steers reads only, never writes. Multi-bucket only. |
| `--fanout-grace-secs N` | `PYPIRON_FANOUT_GRACE_SECS` | `30` | Grace a lagging secondary bucket gets on upload before pypiron records a repair and returns. One slow bucket adds at most this to a publish. Multi-bucket only. |
| `--repl-sweep-interval-secs N` | `PYPIRON_REPL_SWEEP_INTERVAL_SECS` | `300` | Backstop interval for draining pending cross-bucket repairs. Repairs also drain the moment a bucket recovers. Multi-bucket only. |
| `--intent-grace-secs N` | `PYPIRON_INTENT_GRACE_SECS` | `900` | Grace for an upload or cross-bucket package operation. Minimum `3`; maximum `9223372036854775807`. |
| `--audit-on-boot true\|false` | `PYPIRON_AUDIT_ON_BOOT` | `true` | Run the consistency check on boot: indexes against stored files, buckets against each other. |
| `--reconcile-interval-secs N` | `PYPIRON_RECONCILE_INTERVAL_SECS` | `86400` | How often that check repeats. |
| `--transparency true\|false` | `PYPIRON_TRANSPARENCY` | `true` | Record each audit's file hashes under `_transparency/`; `pypiron verify-chain` reads the records. Off stops new records only. |
| `--lease-ttl-secs N` | `PYPIRON_LEASE_TTL_SECS` | `30` | Multi-node leader lease TTL. |
| `--download-stats true\|false` | `PYPIRON_DOWNLOAD_STATS` | `true` | Count package downloads. |
| `--counters-resolution DUR` | `PYPIRON_COUNTERS_RESOLUTION` | `1d` | Counter bucket width: `1d`, `1h`, `30m`, `2h`, etc. |
| `--counters-flush-interval-secs N` | `PYPIRON_COUNTERS_FLUSH_INTERVAL_SECS` | `300` | Counter flush cadence. |
| `--counters-rollup-interval-secs N` | `PYPIRON_COUNTERS_ROLLUP_INTERVAL_SECS` | `3600` | Finished-day compaction cadence. |
| `--counters-retention-days N` | `PYPIRON_COUNTERS_RETENTION_DAYS` | `90` | Counter retention. |
| `--index-cache-ttl-secs N` | `PYPIRON_INDEX_CACHE_TTL_SECS` | `1` | Staleness bound on the in-memory index/page caches. Only matters multi-node — a node's own writes invalidate its caches exactly; the TTL bounds how long another node's write can go unseen. Single-node deployments can raise it freely. |
| `--token-signing-key KEY` | `PYPIRON_TOKEN_SIGNING_KEY` | none | Enables 5-minute install tokens. |

No write credential means read-only. No read credential means installs are open
to the network. Half-configured credentials refuse startup.

Username tags are for attribution: `reader+billing-api` authenticates as
`reader` and records `billing-api` in request metrics. Tags are capped and
restricted to `[A-Za-z0-9._-]`.

`/metrics` always carries `pypiron_storage_ops_total{op="read"|"write"|"list"|"delete"}`:
backend calls issued through the storage layer. On an object-store deployment
these are your billable S3/GCS/Azure requests — a climbing per-request ratio
against `pypiron_http_requests_total` is read amplification worth
investigating before the bill notices it.

With the advisory feed enabled, `/metrics` carries four series:
`pypiron_advisory_snapshot_age_seconds` (**content** age — seconds since the
newest advisory in the loaded feed was modified; alert when it climbs past your
refresh window. Because it tracks the feed's own timestamps, a ferry that keeps
re-reading the same dead file still ages out, where a read-success clock never
would), `pypiron_advisory_last_refresh_age_seconds` (read liveness — seconds
since this node last successfully read the feed; catches a node that stopped
reading at all without masking stale content),
`pypiron_malware_probe_age_seconds` (time since the last malware probe, once the
probe has run at least once), and `pypiron_blocked_downloads_total` (refused
malware downloads; a nonzero rate means a client is still asking for a known-bad
file).

### Behind a forward proxy or TLS interception

In a locked-down network the only way out is a corporate forward proxy, often one
that terminates TLS with a private root CA. pypiron works through both:

- **Forward proxy.** `pypiron sync`, the on-demand proxy's upstream fetch, and the
  advisory feed poll all honor the standard `HTTPS_PROXY`, `HTTP_PROXY`,
  `ALL_PROXY`, and `NO_PROXY` environment variables. Set them and outbound traffic
  routes through the proxy; no flag needed. (The cloud instance-metadata probe is
  the deliberate exception — it always goes direct to the link-local address.)
- **Custom CA.** When the proxy re-signs TLS with a private root, point
  `--upstream-ca-cert` at that CA's PEM bundle. It loads once at startup and
  *augments* the built-in roots, so a direct fetch of public PyPI still validates
  while the corporate CA is also trusted. A bundle that can't be read or parsed
  refuses startup (fail-closed).

Enabling a proxy is a deliberate choice, and it shifts where one SSRF check
enforces. The IP-literal pre-flight is unaffected: an internal address supplied by
a listing — `127.0.0.1`, `10.x`, the `169.254.169.254` metadata endpoint, their
IPv6 and NAT64 (`64:ff9b::`) forms — is still refused on the initial request and
every redirect hop. But it is the proxy, not pypiron, that resolves a *name*
target, so name-based SSRF enforcement moves to the proxy's own egress ACL —
which is the intended egress control point once a proxy is in the path.

## Mirror selection

`serve --proxy-upstream` and `pypiron sync` share `[mirror]`.

| TOML key | Flag | Env |
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

Rules:

- Package specs are names with optional PEP 440 specifiers:
  `requests`, `six==1.16.0`, `requests>=2.20,<3`.
- `sync` requires an include list. Proxy without an include list is open for
  any non-private package.
- Excludes win.
- `include-format` accepts `wheel`, `sdist`, and `other`.
- Tag filters match wheel tags and support `*`.
- `exclude-platform-tag = ["win*", "macosx_*"]` is the usual Linux CI filter.
- `exclude-python-below = "3.9"` drops wheels built only for older Pythons but
  keeps sdists, `py3`, and `abi3`.
- `exclude-newer` sets the dependency cooldown; defaults to `7`, a sliding 7-day
  hold. `""` disables it.
- `WHEN` accepts an RFC 3339 timestamp, bare date, bare day count, friendly
  duration (`"30 days"`), or ISO 8601 duration (`P30D`).
- Yanked files stay out unless `include-yanked = true`.

## Sync

`sync` mirrors over HTTP into a running pypiron server. It never writes storage
directly.

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--from URL` | `PYPIRON_SYNC_FROM` | `https://pypi.org` | Source simple index. Name it in full when it lives off the standard `/simple` path (devpi: `.../<user>/<index>/+simple`). |
| `--source-user USER` | `PYPIRON_SYNC_SOURCE_USER` | none | Username for an authenticated source (private devpi/Artifactory/Nexus). Sent only to the source host — never on an off-host redirect. Requires `--source-pass`; also `[sync].source-user`. |
| `--source-pass PASS` | `PYPIRON_SYNC_SOURCE_PASS` | none | Password for an authenticated source. Requires `--source-user`; also `[sync].source-pass`. |
| `--upstream-ca-cert PEM` | `PYPIRON_UPSTREAM_CA_CERT` | none | PEM bundle of extra CA certificates to trust for the **source** TLS — the private root a corporate forwarding TLS proxy (a MITM appliance) presents. Augments the built-in roots; loaded fail-closed at startup. See [Behind a forward proxy](#behind-a-forward-proxy-or-tls-interception). |
| `--to URL` | `PYPIRON_SYNC_TO` | required | Destination pypiron URL. |
| `--admin-user USER` | `PYPIRON_SYNC_ADMIN_USER` | none | Destination admin user. |
| `--admin-pass PASS` | `PYPIRON_SYNC_ADMIN_PASS` | none | Destination admin password. |
| `--private-prefix PREFIX` | `PYPIRON_PRIVATE_PREFIX` | none | Refuse to mirror private names. |
| `--as-private` | `PYPIRON_SYNC_AS_PRIVATE` | `false` | Migrate the source index into pypiron's private namespace instead of mirroring it. Timestamps and yank state are not preserved — migrated files carry the migration date. See [Migrate off another index](../guides/migrate.md). |
| `--advisory-feed URL\|PATH` | `PYPIRON_ADVISORY_FEED` | relay from `--from` | Ferry the advisory snapshot to `--to` alongside the packages. Unset relays the source server's feed (`GET <from>/advisories/feed`); a URL or path fetches that instead; `""` disables. Best-effort: a feed-less source or a destination without the endpoint warns and the package sync proceeds. Also `[sync].advisory-feed`. |
| `--concurrency N` | `PYPIRON_SYNC_CONCURRENCY` | `4` | Transfers within one package. |
| `--package-concurrency N` | `PYPIRON_SYNC_PACKAGE_CONCURRENCY` | `8` | Packages in parallel. |
| `--spool-dir PATH` | `PYPIRON_SYNC_SPOOL_DIR` | system temp | Download spool directory. |
| `--dry-run` | `PYPIRON_SYNC_DRY_RUN` | `false` | Print work, write nothing. |
| `--full` | `PYPIRON_SYNC_FULL` | `false` | Ignore cursors and reconcile every selected project. |
| `--no-progress` | `PYPIRON_SYNC_NO_PROGRESS` | `false` | Hide the live progress meter. |

Re-running sync is normal. Existing files stay; yanks, removals, and project
status reconcile from upstream.

## Install tokens

Enable with `--token-signing-key`. Mint with:

```bash
export UV_INDEX_COMPANY_USERNAME=__token__
export UV_INDEX_COMPANY_PASSWORD=$(
  pypiron create-token --url http://pypiron:8080 --auth reader:secret
)
```

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--url URL` | `PYPIRON_URL` | required | Server URL. |
| `--role reader\|uploader\|admin` | none | `reader` | Requested role. |
| `--auth user:pass` | `PYPIRON_AUTH` | none | Credential used to mint the token. |
| `--repo VALUE` | none | git remote | Attribution override. |
| `--commit VALUE` | none | git commit | Attribution override. |
| `--user VALUE` | none | local user | Attribution override. |

Tokens live for 5 minutes and cannot outrank the credential that minted them.

## Health and maintenance

| Command | Use |
| --- | --- |
| `pypiron healthcheck` | Probe `/health`; `--url` / `PYPIRON_HEALTHCHECK_URL` overrides the target. |
| `pypiron verify-index` | Read-only full index check against the selected storage backend. `--deep` re-hashes every stored file too. |
| `pypiron rebuild-index` | Rebuild every index from stored files. |
| `pypiron buckets migrate` | Apply a changed `--buckets` list across every reachable bucket. |
| `pypiron origin release PACKAGE` | Release an empty package name for deliberate private/public repurposing. Every configured bucket must be reachable and empty for that package. |

`verify-index` and `rebuild-index` use the same storage flags as `serve`, and
also read `[serve]` from `pypiron.toml`. `buckets migrate` and `origin release`
do too. Storage flags can also follow the subcommand:
`pypiron buckets migrate --buckets ...`.

`verify-index` does not open your package files. It compares what the indexes
say against what the store holds, which keeps it fast on a mirror with a million
files, and it does check every file's *length* against the size its own record
publishes — that arrives with the listing and costs nothing. Add `--deep` and it
also re-hashes every file and compares it to the SHA-256 your clients check their
downloads against. That is the only check that catches a file replaced by a
different one of the same length, and it reads your whole corpus once: seconds on
a private index, hours on a full mirror. Run it after a restore, after
changing the store by hand, or on a schedule you have budgeted for.
Mismatches print as `body-mismatch` (or `size-mismatch` for the free check) and
exit `1`.

`buckets migrate` requires a multi-bucket target and **refuses while any bucket
still has pending cross-bucket repairs**, so shrinking or reordering the list
cannot strand a file's only copy on a bucket you are about to remove. It also
**refuses to drop a bucket that holds the fleet's only copy of a file** — it
names examples and stops. Add the replacement and let replication converge (so
that content lives on a surviving bucket), then remove the old one. To drop it
anyway and *permanently discard* that content, pass `--force`
(`PYPIRON_MIGRATE_FORCE=true`). If it refuses for pending repairs, let the fleet
drain (or bring the lagging bucket back) and retry. A new bucket serves no region reads until your files have copied onto it. To shrink to one bucket, stop the fleet and restart it
with that bucket; single-bucket mode records no bucket list, so there's nothing to migrate.

| Flag | Env | Default | Use |
| --- | --- | --- | --- |
| `--force` | `PYPIRON_MIGRATE_FORCE` | `false` | On `buckets migrate`, drop a bucket even when it holds the fleet's only copy of some content. **Permanent data loss** — back the corpus up onto a surviving bucket first. |
| `--deep` | `PYPIRON_VERIFY_DEEP` | `false` | On `verify-index`, re-hash every stored file against the SHA-256 its record publishes. Reads the whole corpus once. |

Stop writes before `origin release`. It refuses a package with any stored file
except `.origin`, or with pending write/replication work, and conditionally
releases the claim on each configured bucket.

## Multi-bucket metrics

These series appear only when you configure two or more buckets:

| Metric | Meaning |
| --- | --- |
| `pypiron_replication_objects_total` | Artifact records copied into another bucket. |
| `pypiron_replication_bytes_total` | Artifact bytes copied into other buckets; companion metadata is not included. |
| `pypiron_replication_freezes_total` | Same-name, different-byte upload collisions where the loser was quarantined (or both quarantined when the arrival order was too close to call). Needs a human. Any increase needs attention. |
| `pypiron_replication_marker_backlog{dest}` | Pending cross-bucket repairs (fan-out failures awaiting drain) found on reachable source buckets. During a source outage this is a lower bound. |
| `pypiron_reconcile_diff_duration_seconds` | Wall time of the last pairwise full comparison. |
| `pypiron_bucket_health_state{bucket,index}` | Per-node view: healthy `1`, unknown `0`, unhealthy `-1`. |
| `pypiron_bucket_selected{bucket,index}` | Per-node selected bucket for writes: selected `1`, all others `0`. |
| `pypiron_bucket_read_selected{bucket,index}` | Per-node bucket serving reads: the region bucket while it is healthy and caught up, otherwise the same as `pypiron_bucket_selected`. |
| `pypiron_bucket_health_alarms_total{bucket,index}` | Storage errors that do not prove an outage, including credentials, permissions, CAS, KMS, quota, and configuration. |
| `pypiron_bucket_selection_generation` | Number that changes when this node selects another bucket. |
| `pypiron_bucket_topology_write_fenced` | `1` when a runtime topology mismatch has stopped mutations; reads remain available. |

Alert on any freeze, a non-zero topology fence, persistent health alarms, or a
backlog that keeps growing after its destination recovers. The backlog counts
pending repairs, not bytes.

## Endpoints

| Endpoint | Auth | Meaning |
| --- | --- | --- |
| `/simple/` | read | Package index. |
| `/files/<pkg>/<file>` | read | Artifact bytes. |
| `/legacy/` | uploader/admin | Upload API. |
| `/health` | open | Liveness: the process is up. Always `200` while serving (a Kubernetes `livenessProbe`). |
| `/ready` | open | Readiness: this node can serve reads. Point your load balancer and a Kubernetes `readinessProbe` here. |
| `/metrics` | open | Prometheus metrics. |
| `/stats/downloads` | read | Global download stats. |
| `/stats/downloads/<pkg>` | read | Per-package download stats. |
| `/audit` | admin | Org audit: hosted packages a known advisory affects, ranked by installs (HTML). |
| `/audit.json` | admin | The same audit report as JSON. |
| `/advisories/feed` | read (GET), admin (PUT) | Advisory snapshot: readers pull it (etag-conditioned); an admin pushes a delivered one. |
| `/tokens` | read/uploader/admin, or open reader token | Mint install tokens. |
| `/files/.../yank` | admin | Yank a file. |
| `/files/.../delete` | admin | Delete a file. |
| `/project/<pkg>/status` | admin | Set project status. |
