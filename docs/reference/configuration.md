---
description: Every pypiron flag, PYPIRON_ environment variable, and pypiron.toml setting for running a self-hosted PyPI server, with defaults and precedence.
---

# Configuration

Configure pypiron with flags, `PYPIRON_*` environment variables, or
`pypiron.toml`.

Precedence: **CLI > environment > file > defaults**.

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

Disk is the default. Use `--buckets` for S3, GCS, Azure, or compatible object
storage. Buckets must already exist.

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--buckets URI,...` | `PYPIRON_BUCKETS` | none (disk) | One or more bucket URIs, any mix of backends, in preference order. Unset means disk. |
| `--data-dir PATH` | `PYPIRON_DATA_DIR` | `~/.pypiron/packages` | Disk root. Ignored when `--buckets` is set. |
| `--storage-prefix PREFIX` | `PYPIRON_STORAGE_PREFIX` | none | Keep everything under one subtree, so pypiron can share a bucket. |

### One bucket

```
pypiron serve --buckets s3://acme-pypiron
```

Credentials come from the backend's native chain. The file form is:

```toml
[serve]
buckets = ["s3://acme-pypiron"]
```

Each URI has a scheme and may include `@region`:

- `s3://name` or `s3://name@region` — S3 bucket.
- `gs://name` or `gs://name@region` — GCS bucket.
- `az://container` or `az://container@region` — Azure blob container.

On S3, `@region` selects the signing region. With several buckets, it also
selects the nearest read bucket.

### Credentials by backend

Half-configured credentials make startup fail.

- **S3** — the standard AWS chain: env vars, web identity, instance role, task
  role.
- **GCS** — a service-account key (`--gcs-service-account-path`) enables
  presigned redirects; without one, pypiron uses Application Default Credentials
  and downloads stream through the node.
- **Azure** — the account access key (`--azure-access-key`) enables presigned
  (SAS) redirects.

### Backend-wide settings

These apply to every bucket of the same backend:

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

### Per-bucket overrides

Use `[serve.bucket."URI"]` when buckets need different endpoints or
credentials. These overrides are TOML-only.

```toml
[serve]
buckets = ["s3://iron-east@us-east-1", "s3://minio-cache"]

[serve.bucket."s3://minio-cache"]
endpoint-url = "http://minio.internal:9000"
force-path-style = true
env-prefix = "MINIO_CACHE_"   # reads MINIO_CACHE_AWS_ACCESS_KEY_ID / ..._AWS_SECRET_ACCESS_KEY
```

The key matches `scheme://name`; any `@region` is ignored.

| Field | Schemes | Meaning |
| --- | --- | --- |
| `endpoint-url` | any | Endpoint for this bucket, overriding the backend-wide flag. An `http://` value allows plaintext. |
| `force-path-style` | s3 | Path-style addressing for this bucket. |
| `env-prefix` | s3, azure | Names the env vars holding this bucket's credentials (below). |
| `service-account-path` | gcs | Service-account JSON key for this bucket. Also enables presigned redirects for it. |
| `account` | azure | Storage account for this bucket. |

`env-prefix` names environment variables; it does not store a secret:

- **S3** — `<PREFIX>AWS_ACCESS_KEY_ID`, `<PREFIX>AWS_SECRET_ACCESS_KEY`, and
  optional `<PREFIX>AWS_SESSION_TOKEN`.
- **Azure** — `<PREFIX>AZURE_ACCESS_KEY`.
- **GCS** has no `env-prefix`: its credential is a key file, so use
  `service-account-path` instead.

pypiron refuses startup when:

- An override keyed to a bucket not in `--buckets` fails, listing the valid
  buckets (typo protection).
- An `env-prefix` with only one credential half set — or neither — fails: a
  scoped credential was promised and isn't fully there.
- A field used on the wrong scheme fails, naming the field and bucket.

### Sharing a bucket

`--storage-prefix pypi` keeps all keys under `pypi/`. This lets pypiron share a
bucket and lets separate servers use different prefixes. On disk, it creates a
subdirectory under `--data-dir`. Choose the prefix before loading packages;
changing it points the server at a different, initially empty key space.

### Multiple regions and clouds

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--buckets URI,...` | `PYPIRON_BUCKETS` | none | Bucket URIs in preference order, any mix of backends. Enables cross-region and cross-cloud replication and failover. |

Give every node the same ordered bucket list. The first bucket is preferred:

```
pypiron serve --buckets s3://iron-east@us-east-1,s3://iron-west@us-west-2
PYPIRON_BUCKETS=s3://iron-east@us-east-1,s3://iron-west@us-west-2
```

`@region` keeps reads local; on S3 it also selects the signing region. Mixed
backends are supported. With more than one bucket:

- Private uploads, synced packages, and proxy-cached packages replicate to
  every bucket. Budget the stored corpus once per bucket.
- An upload waits for every healthy bucket. A failed bucket catches up from a
  repair record after it recovers. If pypiron cannot save that record, it
  returns `503`.
- Compatible buckets use their provider's server-side copy operation. Other
  pairs stream through the node. Startup logs `transport=copy` or
  `transport=stream` for each pair.
- Keep encryption modes consistent to preserve server-side copy. Mixed modes
  fall back to verified streaming.
- Deleting a proxy-cached or synced file returns `409`; distributed cache
  eviction is not supported.

[Multi-region deployment and recovery](../guides/multi-region.md)

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
| `--allow-insecure-upstream` | `PYPIRON_ALLOW_INSECURE_UPSTREAM` | `false` | Permit a plaintext `http://` proxy upstream. This exposes both bytes and their claimed hash to interception. |
| `--proxy-stream-threshold SIZE` | `PYPIRON_PROXY_STREAM_THRESHOLD` | `16MiB` | Stream an uncached file at or above this size while fetching it. The last `64KiB` waits for hash verification. Accepts `64MB`, `1GiB`, etc.; minimum `64KiB`; `off` buffers every file. |
| `--proxy-allow-host HOST` | `PYPIRON_PROXY_ALLOW_HOST` | none | Permit the proxy to fetch listing-derived URLs (artifact, `.metadata`, `.provenance`, redirect targets) whose host matches `HOST` exactly, even if it resolves to a private address. Repeatable; comma-separated in the env var. Only the configured upstream host is exempt otherwise. |
| `--proxy-allow-cidr CIDR` | `PYPIRON_PROXY_ALLOW_CIDR` | none | Like `--proxy-allow-host`, but permits any target IP inside `CIDR` (e.g. `10.0.0.0/8`). Repeatable; comma-separated in the env var. |
| `--upstream-ca-cert PEM` | `PYPIRON_UPSTREAM_CA_CERT` | none | Extra CA certificates for proxy, sync-source, and advisory TLS. Adds to built-in roots. A missing or invalid bundle refuses startup. [Proxy setup](#behind-a-forward-proxy-or-tls-interception). |
| `--advisory-feed URL\|PATH` | `PYPIRON_ADVISORY_FEED` | OSV PyPI export | Feed for malware blocking and the org audit: a URL or local path to the [OSV](https://osv.dev) PyPI advisory export. Defaults to `https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip` (named in the startup log); a URL fetch honors `HTTP(S)_PROXY`. `""` disables both features. |
| `--malware-block true\|false` | `PYPIRON_MALWARE_BLOCK` | `true` | Refuse public files named by OSV `MAL-*` advisories. The binary includes a first-boot block set; a live or synced snapshot replaces it. Explicit `true` requires a live snapshot at startup. Does not control PEP 792 quarantine. |
| `--malware-probe-secs N` | `PYPIRON_MALWARE_PROBE_SECS` | `120` | Check OSV for new malware between daily snapshots. `0` disables. Requires malware blocking and the standard OSV `all.zip` feed. |
| `--metrics-project-labels` | `PYPIRON_METRICS_PROJECT_LABELS` | `false` | Attach per-client `project` labels to `/metrics`. Off by default: `/metrics` is unauthenticated and the label derives from the username tag, so exposing it lets any scraper list internal project names. |
| `--spool-dir PATH` | `PYPIRON_SPOOL_DIR` | system temp | Upload/proxy spool directory. |
| `--artifact-delivery auto\|redirect\|stream` | `PYPIRON_ARTIFACT_DELIVERY` | `auto` | Redirect object-store downloads for compatible clients or stream through the server. A first uncached proxy download may stream under `--proxy-stream-threshold`. |
| `--wait-on-upload` | `PYPIRON_WAIT_ON_UPLOAD` | `false` | Wait for index visibility before upload returns. |
| `--wait-on-upload-secs N` | `PYPIRON_WAIT_ON_UPLOAD_SECS` | `10` | Bound for that wait. |
| `--max-concurrent-artifact-writes N` | `PYPIRON_MAX_CONCURRENT_ARTIFACT_WRITES` | `4` | Concurrent object-store uploads allowed to buffer up to `64MiB` each. Default worst case is about `256MiB`. Disk is not gated; `0` is unlimited. |
| `--allow-legacy-versions` | `PYPIRON_ALLOW_LEGACY_VERSIONS` | `false` | Accept direct uploads with non-PEP-440 versions. File type is irrelevant. Sync has a separate flag below. |
| `--access-log` | `PYPIRON_ACCESS_LOG` | `false` | Log reads too, not only mutations. |
| `--access-log-format structured\|clf` | `PYPIRON_ACCESS_LOG_FORMAT` | `structured` | Structured logs or Combined Log Format. |
| `--trusted-proxy` | `PYPIRON_TRUSTED_PROXY` | `false` | Honor `X-Forwarded-For`/`X-Real-IP`. Enable only behind a proxy that replaces these headers; otherwise callers can forge their logged address. [Login throttling](../security.md#login-throttling). |
| `--login-cooldown-secs N` | `PYPIRON_LOGIN_COOLDOWN_SECS` | `300` | Lock one address for this long after five failed logins; returns `429` with `Retry-After`. Per instance; IPv6 groups by `/64`; `0` disables. |
| `--worker-interval-secs N` | `PYPIRON_WORKER_INTERVAL_SECS` | `1` | Index, replication, and bucket-health poll cadence. |
| `--bucket-leave-failures N` | `PYPIRON_BUCKET_LEAVE_FAILURES` | `3` | Consecutive timeout (including 408), connection, or 5xx failures before selecting the next bucket. |
| `--bucket-return-healthy-secs N` | `PYPIRON_BUCKET_RETURN_HEALTHY_SECS` | `300` | Healthy time required before returning to a preferred bucket. Startup warns when this may be shorter than catch-up confirmation. |
| `--node-region LABEL` | `PYPIRON_NODE_REGION` | detected | This node's region, matched against bucket `@region` labels to choose the bucket it reads from. Cloud nodes detect it automatically (AWS/GCP/Azure); set it for on-prem or MinIO. Steers reads only, never writes. Multi-bucket only. |
| `--fanout-grace-secs N` | `PYPIRON_FANOUT_GRACE_SECS` | `30` | Grace a lagging secondary bucket gets on upload before pypiron records a repair and returns. One slow bucket adds at most this to a publish. Multi-bucket only. |
| `--repl-sweep-interval-secs N` | `PYPIRON_REPL_SWEEP_INTERVAL_SECS` | `300` | Backstop interval for draining pending cross-bucket repairs. Repairs also drain the moment a bucket recovers. Multi-bucket only. |
| `--intent-grace-secs N` | `PYPIRON_INTENT_GRACE_SECS` | `900` | Grace for an upload or cross-bucket package operation. Minimum `3`; maximum `9223372036854775807`. |
| `--audit-on-boot true\|false` | `PYPIRON_AUDIT_ON_BOOT` | `true` | Run the consistency check on boot: indexes against stored files, buckets against each other. |
| `--reconcile-interval-secs N` | `PYPIRON_RECONCILE_INTERVAL_SECS` | `86400` | How often that check repeats. |
| `--quarantine-poll-secs N` | `PYPIRON_QUARANTINE_POLL_SECS` | `30` | Maximum delay before other nodes adopt a project freeze. The receiving node refuses immediately. One small listing per node per interval. |
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

`pypiron_storage_ops_total` reports backend reads, writes, lists, and deletes.
With the advisory feed enabled, use
`pypiron_advisory_snapshot_age_seconds`,
`pypiron_advisory_last_refresh_age_seconds`, and
`pypiron_malware_probe_age_seconds` to alert on stale data or failed polling.
`pypiron_blocked_downloads_total` counts refused malware downloads.

### Behind a forward proxy or TLS interception

Sync, proxy fetches, and advisory polling honor `HTTPS_PROXY`, `HTTP_PROXY`,
`ALL_PROXY`, and `NO_PROXY`. Cloud instance-metadata detection always uses its
link-local address directly.

Use `--upstream-ca-cert` when the proxy re-signs TLS with a private CA. The
bundle adds to the built-in roots and must parse at startup.

pypiron still rejects forbidden non-routable destinations in their IPv4, IPv6,
and NAT64 forms. Public IPv6 addresses are allowed. The forward proxy resolves
hostnames, so its egress policy must enforce hostname restrictions.

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
- `sync` normally requires an include list. A pypicloud private migration may
  use `--private-pattern` or `--private-patterns-from` as its work list instead.
  Proxy without an include list is open for any non-private package.
- Omit a list to leave that filter unset. An explicit empty list or environment
  value is refused because it could erase a stricter value from another config
  layer.
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
- Yanked files are not fetched unless `include-yanked = true`. Files already
  cached remain listed as yanked so pinned installs work.

`exclude-packages` removes matching names or versions from package listings,
including content already cached. It takes effect after restart. Stored bytes
remain available by direct file URL until an admin deletes them; mirrored-file
deletion is refused with multiple buckets.

Other content filters control future fetches. They do not purge files already
stored.

## Sync

`sync` mirrors over HTTP into a running pypiron server. It never writes storage
directly.

The pypicloud-specific source and private-pattern options require a build newer
than 0.0.17. Use the next release when available, or run
`cargo run --locked -- sync ...` from a source checkout.

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--from URL` | `PYPIRON_SYNC_FROM` | `https://pypi.org` | Source index. Name a Simple endpoint in full when it lives off `/simple` (devpi: `.../<user>/<index>/+simple`). With `--source-kind pypicloud`, use the pypicloud application root instead. |
| `--source-kind simple\|pypicloud` | `PYPIRON_SOURCE_KIND` | `simple` | Source protocol. `pypicloud` reads `/api/package/`, requires `--as-private`, and supports private-name patterns. Also `[sync].source-kind`. |
| `--source-user USER` | `PYPIRON_SYNC_SOURCE_USER` | none | Authenticated-source username. Sent only to the same scheme, host, and port. Requires `--source-pass`; also `[sync].source-user`. |
| `--source-pass PASS` | `PYPIRON_SYNC_SOURCE_PASS` | none | Password for an authenticated source. Requires `--source-user`; also `[sync].source-pass`. |
| `--allow-insecure-source` | `PYPIRON_ALLOW_INSECURE_SOURCE` | `false` | Send source credentials over plaintext HTTP. Unauthenticated HTTP sources need no flag. |
| `--upstream-ca-cert PEM` | `PYPIRON_UPSTREAM_CA_CERT` | none | Extra CA certificates for source TLS. Adds to built-in roots and must parse at startup. [Proxy setup](#behind-a-forward-proxy-or-tls-interception). |
| `--to URL` | `PYPIRON_SYNC_TO` | required | Destination pypiron URL. |
| `--admin-user USER` | `PYPIRON_SYNC_ADMIN_USER` | none | Destination admin user. |
| `--admin-pass PASS` | `PYPIRON_SYNC_ADMIN_PASS` | none | Destination admin password. |
| `--private-prefix PREFIX` | `PYPIRON_PRIVATE_PREFIX` | none | Refuse to mirror private names. |
| `--as-private` | `PYPIRON_SYNC_AS_PRIVATE` | `false` | Import as private packages. Uses the migration time and does not preserve yank state. Public-owned names require emptying and `origin release`. [Migration guide](../guides/migrate.md). |
| `--private-pattern PATTERN` | `PYPIRON_PRIVATE_PATTERN` | none | Declare pypicloud project names private. Repeatable; matches the entire PEP 503-normalized name and supports only `*`. A bare `*` is refused. Valid only with `--source-kind pypicloud --as-private`; also `[sync].private-patterns`. |
| `--private-patterns-from FILE` | `PYPIRON_PRIVATE_PATTERNS_FROM` | none | Read pypicloud private-name patterns from a file, one per line. Blank lines and `#` comments are ignored; also `[sync].private-patterns-from`. |
| `--advisory-feed URL\|PATH` | `PYPIRON_ADVISORY_FEED` | relay from `--from` | Deliver an advisory snapshot to the destination. A URL or path overrides the source feed; `""` disables. Failure warns but does not stop package sync. Also `[sync].advisory-feed`. |
| `--concurrency N` | `PYPIRON_SYNC_CONCURRENCY` | `4` | Transfers within one package. |
| `--package-concurrency N` | `PYPIRON_SYNC_PACKAGE_CONCURRENCY` | `8` | Packages in parallel. |
| `--spool-dir PATH` | `PYPIRON_SYNC_SPOOL_DIR` | system temp | Download spool directory. |
| `--dry-run` | `PYPIRON_SYNC_DRY_RUN` | `false` | Print work, write nothing. |
| `--full` | `PYPIRON_SYNC_FULL` | `false` | Ignore cursors and reconcile every selected project. |
| `--no-progress` | `PYPIRON_SYNC_NO_PROGRESS` | `false` | Hide the live progress meter. |
| `--allow-legacy-versions` | `PYPIRON_ALLOW_LEGACY_VERSIONS` | `false` | Mirror files without an inferable PEP 440 version. Otherwise they are logged and skipped. Applies in sync, because the destination accepts mirror uploads. Also `[sync].allow-legacy-versions`. |

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
| `pypiron origin release PACKAGE` | Release an empty package name for deliberate private/public repurposing — the only way to flip a mirror-owned name to private; there is no in-place reclaim. Every configured bucket must be reachable and empty for that package. |

Maintenance commands use the same storage flags and `[serve]` configuration as
the server.

`verify-index` compares indexes with stored files and checks recorded sizes.
`--deep` also hashes every artifact, which reads the full corpus. Mismatches exit
`1`.

`verify-chain` checks the `_transparency/` records against storage. Changed or
missing content exits `1`. A filename recorded under two hashes is reported as
`fingerprint-changed` but is non-fatal because a deliberate public-to-private
replacement has the same shape. `--strict` makes it fatal. Preventing rollback
of the log itself requires Object Lock in COMPLIANCE mode.

`buckets migrate` refuses to drop the only copy of a file or run while repairs
are pending. Add the replacement, wait for replication, then remove the old
bucket. `--force` can discard content permanently.

| Flag | Env | Default | Use |
| --- | --- | --- | --- |
| `--force` | `PYPIRON_MIGRATE_FORCE` | `false` | On `buckets migrate`, drop a bucket even when it holds the fleet's only copy of some content. **Permanent data loss** — back the corpus up onto a surviving bucket first. |
| `--deep` | `PYPIRON_VERIFY_DEEP` | `false` | On `verify-index`, re-hash every stored file against the SHA-256 its record publishes. Reads the whole corpus once. |
| `--strict` | `PYPIRON_VERIFY_CHAIN_STRICT` | `false` | On `verify-chain`, treat any `fingerprint-changed` result as a failure. |

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
| `/audit` | admin | Org audit: hosted packages a known advisory affects, ranked by downloads over the last 30 days (HTML). |
| `/audit.json` | admin | The same audit report as JSON. |
| `/advisories/feed` | read (GET), admin (PUT) | Advisory snapshot: readers pull it (etag-conditioned); an admin pushes a delivered one. |
| `/tokens` | read/uploader/admin, or open reader token | Mint install tokens. |
| `/files/.../yank` | admin | Yank a file. |
| `DELETE /files/<pkg>/<file>` | admin | Delete a file. Refused for mirrored files — anything that came from an upstream, cached on demand or pulled by `sync` — when you run more than one bucket. |
| `/project/<pkg>/status` | admin | Set project status. |
