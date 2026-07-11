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
storage = "s3"
s3-bucket = "acme-pypiron"
aws-region = "us-east-1"
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

`disk` is the default. Use object storage for multiple nodes. Buckets and
containers must already exist.

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--storage disk\|s3\|gcs\|azure` | `PYPIRON_STORAGE` | `disk` | Storage backend. |
| `--data-dir PATH` | `PYPIRON_DATA_DIR` | `~/.pypiron/packages` | Disk root. |
| `--storage-prefix PREFIX` | `PYPIRON_STORAGE_PREFIX` | none | Keep everything under one subtree, so pypiron can share a bucket. |

### Sharing a bucket

By default pypiron owns the whole bucket: artifacts land in `packages/`, indexes
in `simple/`. Set `--storage-prefix pypi` and they move to `pypi/packages/` and
`pypi/simple/` instead, leaving the rest of the bucket alone — pypiron neither
reads nor writes outside its prefix. Two servers can share one bucket by taking
different prefixes.

On disk the prefix is simply a subdirectory of `--data-dir`.

Point an existing server at a prefix and it will look empty: the prefix is part
of every key, so it is chosen once, when the bucket is first populated. To adopt
one later, move the objects (`aws s3 mv --recursive`, `gcloud storage mv`) before
restarting.

### S3

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--s3-bucket NAME[@REGION]` | `PYPIRON_S3_BUCKETS` | required | Bucket. Repeatable — see below. Legacy `PYPIRON_S3_BUCKET` still selects one bucket. |
| `--aws-region REGION` | `AWS_REGION` | none | AWS region for every bucket without its own `@region`. |
| `--s3-endpoint-url URL` | `PYPIRON_S3_ENDPOINT_URL` | none | MinIO or another S3-compatible endpoint. |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false` | Path-style addressing. |

AWS credentials use the standard AWS chain: env, web identity, instance role, or
task role.

#### Multiple buckets

Give pypiron more than one bucket and it keeps them in sync and rides out the
loss of any one of them — installs never stop, uploads continue within seconds.
Repeat `--s3-bucket` (order is preference, the first is preferred), or set
`PYPIRON_S3_BUCKETS` to a comma-separated list:

```
pypiron serve --s3-bucket iron-east --s3-bucket iron-west
PYPIRON_S3_BUCKETS=iron-east,iron-west
```

Supplying `--s3-bucket` selects S3; `--storage s3` is optional. The
`pypiron.toml` `s3-bucket` key accepts one bucket. Use the repeatable CLI flag or
the plural environment variable for a list.
Existing single-bucket deployments may keep `PYPIRON_S3_BUCKET`; the plural
variable or CLI flag wins when both are present.

Put buckets in different regions by pinning each one with an `@region` suffix,
which overrides the shared `--aws-region`:

```
PYPIRON_S3_BUCKETS=iron-east@us-east-1,iron-west@us-west-2
```

One bucket starts no topology, health, replication, warm-index, or read-fence
work. The shared filename-reuse guard still performs its documented tombstone
check on upload. Multiple buckets are S3-only for now. In multi-bucket mode, a
local package-index read adds initial and final exact origin GETs. A local
artifact or companion read adds those two origin GETs,
tombstone/frozen/mirror-quarantine HEADs, and a sidecar GET when the claim or
quarantine marker requires one. Proxy paths add eligibility, claim, marker, and
upstream work dynamically.
All entries share one credential chain and `--s3-endpoint-url`. See
[Multi-bucket failover](../guides/multi-bucket.md) for recovery behavior, costs,
and limits.

In multi-bucket mode S3 SDK retries are disabled. One-second topology probes
switch new requests and cancel background work on an ineligible bucket without
putting a short total deadline on real artifact transfers. An already in-flight
transfer retains the normal one-hour route bound.
Startup topology operations also have a one-second bound. An unreachable bucket
may add that timeout for each attempted operation, but cannot block startup
indefinitely when another configured bucket is reachable.
Admin DELETE still removes private files, but returns `409` for mirror-cache
entries. Cache eviction is unavailable in multi-bucket v1; do not apply a broad
`packages/` lifecycle rule because private and mirror records share that prefix.
Demotion manifests are deleted after promotion, but their content-addressed
`_staging/repl/` members persist. Do not lifecycle-expire that prefix either: a
live manifest may reference a retained member. The prefix also holds the
never-deleted per-package `.promotion-lock` CAS sentinel. A holder heartbeats;
recovery requires an unchanged lock ETag for the full intent grace and no live
holder intent family before takeover.

### GCS

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--gcs-bucket NAME` | `PYPIRON_GCS_BUCKET` | required | Bucket. |
| `--gcs-service-account-path PATH` | `PYPIRON_GCS_SERVICE_ACCOUNT_PATH` | none | Service-account JSON key. |
| `--gcs-endpoint-url URL` | `PYPIRON_GCS_ENDPOINT_URL` | none | Local emulator or custom endpoint. |

Without a service-account key, GCS uses Application Default Credentials and
downloads stream through the node.

### Azure

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--azure-account NAME` | `PYPIRON_AZURE_ACCOUNT` | required | Storage account. |
| `--azure-container NAME` | `PYPIRON_AZURE_CONTAINER` | required | Blob container. |
| `--azure-access-key KEY` | `PYPIRON_AZURE_ACCESS_KEY` | none | Account key, also used for signed URLs. |
| `--azure-endpoint-url URL` | `PYPIRON_AZURE_ENDPOINT_URL` | none | Azurite or custom endpoint. |
| `--azure-use-emulator` | `PYPIRON_AZURE_USE_EMULATOR` | `false` | Use Azurite defaults. |

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
| `--spool-dir PATH` | `PYPIRON_SPOOL_DIR` | system temp | Upload/proxy spool directory. |
| `--artifact-delivery auto\|redirect\|stream` | `PYPIRON_ARTIFACT_DELIVERY` | `auto` | Redirect object-store downloads when the client handles it well; otherwise stream. |
| `--wait-on-upload` | `PYPIRON_WAIT_ON_UPLOAD` | `false` | Wait for index visibility before upload returns. |
| `--wait-on-upload-secs N` | `PYPIRON_WAIT_ON_UPLOAD_SECS` | `10` | Bound for that wait. |
| `--access-log` | `PYPIRON_ACCESS_LOG` | `false` | Log reads too, not only mutations. |
| `--access-log-format structured\|clf` | `PYPIRON_ACCESS_LOG_FORMAT` | `structured` | Structured logs or Combined Log Format. |
| `--worker-interval-secs N` | `PYPIRON_WORKER_INTERVAL_SECS` | `1` | Dirty/replication poll and bucket-health probe cadence. |
| `--bucket-leave-failures N` | `PYPIRON_BUCKET_LEAVE_FAILURES` | `3` | Consecutive timeout (including 408), connection, or 5xx failures before selecting the next bucket. |
| `--bucket-return-healthy-secs N` | `PYPIRON_BUCKET_RETURN_HEALTHY_SECS` | `300` | Continuous health required before returning to a more-preferred bucket. |
| `--intent-grace-secs N` | `PYPIRON_INTENT_GRACE_SECS` | `900` | Grace for an upload or cross-bucket package operation. Minimum `3`; maximum `9223372036854775807`. |
| `--audit-on-boot true\|false` | `PYPIRON_AUDIT_ON_BOOT` | `true` | Run the selected-bucket audit and multi-bucket full diff on boot. |
| `--reconcile-interval-secs N` | `PYPIRON_RECONCILE_INTERVAL_SECS` | `86400` | Selected-bucket audit and multi-bucket full-diff interval. |
| `--lease-ttl-secs N` | `PYPIRON_LEASE_TTL_SECS` | `30` | Multi-node leader lease TTL. |
| `--download-stats true\|false` | `PYPIRON_DOWNLOAD_STATS` | `true` | Count package downloads. |
| `--counters-resolution DUR` | `PYPIRON_COUNTERS_RESOLUTION` | `1d` | Counter bucket width: `1d`, `1h`, `30m`, `2h`, etc. |
| `--counters-flush-interval-secs N` | `PYPIRON_COUNTERS_FLUSH_INTERVAL_SECS` | `300` | Counter flush cadence. |
| `--counters-rollup-interval-secs N` | `PYPIRON_COUNTERS_ROLLUP_INTERVAL_SECS` | `3600` | Finished-day compaction cadence. |
| `--counters-retention-days N` | `PYPIRON_COUNTERS_RETENTION_DAYS` | `90` | Counter retention. |
| `--token-signing-key KEY` | `PYPIRON_TOKEN_SIGNING_KEY` | none | Enables 5-minute install tokens. |

No write credential means read-only. No read credential means installs are open
to the network. Half-configured credentials refuse startup.

Username tags are for attribution: `reader+billing-api` authenticates as
`reader` and records `billing-api` in request metrics. Tags are capped and
restricted to `[A-Za-z0-9._-]`.

## Mirror selection

`[mirror]` is shared by `serve --proxy-upstream` and `pypiron sync`.

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
- `exclude-newer` defaults to `7`: a sliding 7-day hold. `""` disables it.
- `WHEN` accepts an RFC 3339 timestamp, bare date, bare day count, friendly
  duration (`"30 days"`), or ISO 8601 duration (`P30D`).
- Yanked files are excluded unless `include-yanked = true`.

## Sync

`sync` mirrors over HTTP into a running pypiron server. It never writes storage
directly.

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--from URL` | `PYPIRON_SYNC_FROM` | `https://pypi.org` | Source simple index. |
| `--to URL` | `PYPIRON_SYNC_TO` | required | Destination pypiron URL. |
| `--admin-user USER` | `PYPIRON_SYNC_ADMIN_USER` | none | Destination admin user. |
| `--admin-pass PASS` | `PYPIRON_SYNC_ADMIN_PASS` | none | Destination admin password. |
| `--private-prefix PREFIX` | `PYPIRON_PRIVATE_PREFIX` | none | Refuse to mirror private names. |
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
| `pypiron verify-index` | Read-only full index check against the selected storage backend. |
| `pypiron rebuild-index` | Rebuild every index from stored files. |
| `pypiron buckets migrate` | Increment the multi-bucket topology generation and re-stamp every reachable configured bucket. |
| `pypiron origin release PACKAGE` | Release an empty package name for deliberate private/public repurposing. Every configured bucket must be reachable and empty for that package. |

`verify-index` and `rebuild-index` use the same storage flags as `serve`, and
also read `[serve]` from `pypiron.toml`. `buckets migrate` and `origin release`
do too. Storage flags and their environment variables may also follow those
nested commands.

`buckets migrate` requires a multi-bucket target. To shrink to one bucket, stop
the fleet and restart it with that bucket; topology stamps are dormant in
single-bucket mode.

Stop writes before `origin release`. It refuses a package with any package truth
except `.origin`, or with pending write/replication work, and conditionally
releases the claim on each configured bucket.

## Multi-bucket metrics

These series appear only when two or more buckets are configured:

| Metric | Meaning |
| --- | --- |
| `pypiron_replication_objects_total` | Artifact records copied into another bucket. |
| `pypiron_replication_bytes_total` | Artifact bytes copied into other buckets; companion metadata is not included. |
| `pypiron_replication_freezes_total` | Conflicting same-name, different-byte uploads frozen for human resolution. Any increase needs attention. |
| `pypiron_replication_marker_backlog{dest}` | Undelivered markers found on reachable source buckets. During a source outage this is a lower bound; it still exposes backlog on healthy sources instead of retaining a stale zero. |
| `pypiron_reconcile_diff_duration_seconds` | Wall time of the last pairwise full comparison. |
| `pypiron_bucket_health_state{bucket,index}` | Per-node view: healthy `1`, unknown `0`, unhealthy `-1`. |
| `pypiron_bucket_selected{bucket,index}` | Per-node selected bucket: selected `1`, all others `0`. |
| `pypiron_bucket_health_alarms_total{bucket,index}` | Storage errors that do not prove an outage, including credentials, permissions, CAS, KMS, quota, and configuration. |
| `pypiron_bucket_selection_generation` | Number that changes when this node selects another bucket. |
| `pypiron_bucket_topology_write_fenced` | `1` when a runtime topology mismatch has stopped mutations; reads remain available. |

Alert on any freeze, a non-zero topology fence, persistent health alarms, or a
backlog that keeps growing after its destination recovers. The backlog is a
count of mutation markers, not bytes.

## Endpoints

| Endpoint | Auth | Meaning |
| --- | --- | --- |
| `/simple/` | read | Package index. |
| `/files/<pkg>/<file>` | read | Artifact bytes. |
| `/legacy/` | uploader/admin | Upload API. |
| `/health` | open | Load balancer health. |
| `/metrics` | open | Prometheus metrics. |
| `/stats/downloads` | read | Global download stats. |
| `/stats/downloads/<pkg>` | read | Per-package download stats. |
| `/tokens` | read/uploader/admin, or open reader token | Mint install tokens. |
| `/files/.../yank` | admin | Yank a file. |
| `/files/.../delete` | admin | Delete a file. |
| `/project/<pkg>/status` | admin | Set project status. |
