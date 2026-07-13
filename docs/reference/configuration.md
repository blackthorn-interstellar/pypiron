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
| `--s3-bucket NAME` | `PYPIRON_S3_BUCKET` | required | One S3 bucket. For several buckets (any backend) use `--buckets` — see [Multiple regions and clouds](#multiple-regions-and-clouds). |
| `--aws-region REGION` | `AWS_REGION` | none | AWS region for a bucket without its own `@region`. |
| `--s3-endpoint-url URL` | `PYPIRON_S3_ENDPOINT_URL` | none | MinIO or another S3-compatible endpoint. |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false` | Path-style addressing. |

AWS credentials use the standard AWS chain: env, web identity, instance role, or
task role.

#### Multiple regions and clouds

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

Each entry needs a scheme:

- `s3://name` or `s3://name@region` — `@region` sets the S3 client's signing and
  endpoint region (SigV4 plus regional endpoints) for that bucket. One list can
  span regions, so each S3 bucket carries its own; precedence is per-bucket
  `@region`, then `--aws-region`, then the SDK default. `gs://` and `az://` need
  no region — their endpoints do not encode one.
- `gs://name` — a GCS bucket.
- `az://container` — an Azure blob container.

Mix backends freely. `s3://iron-east@us-east-1,gs://iron-backup` replicates
across two clouds and survives an entire provider outage. Each backend resolves
its own native credentials (the AWS chain for S3, a service-account key or
Application Default Credentials for GCS, the account key for Azure); a bucket
whose backend is half-configured refuses to start. One bad URI fails startup
before any bucket is contacted.

A single-entry `--buckets` list behaves exactly like configuring that one backend
directly — no topology, health, replication, or read-fence work runs. The
single-bucket flags (`--s3-bucket`, `--gcs-bucket`, `--azure-container`) and the
`pypiron.toml` `s3-bucket` key are unchanged; use `--buckets` only for a list.

In multi-bucket mode SDK retries are disabled and one-second topology probes
switch new requests and cancel background work on an ineligible bucket, without
putting a short deadline on real artifact transfers (an in-flight transfer keeps
the normal one-hour bound). Startup and migration operations also carry a
one-second bound per bucket, so an unreachable bucket can slow boot but never
blocks it while another configured bucket is reachable. Admin DELETE still
removes private files but returns `409` for proxy-cached entries — cache eviction
across buckets is unavailable, so do not apply a broad `packages/` lifecycle rule
(private and mirror records share that prefix).

See [Survive a region or cloud outage](../guides/multi-region.md) for deployment,
recovery behavior, and operator rules.

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
| `--fanout-grace-secs N` | `PYPIRON_FANOUT_GRACE_SECS` | `30` | Grace a lagging secondary bucket gets on upload before pypiron records a repair and returns. One slow bucket adds at most this to a publish. Multi-bucket only. |
| `--repl-sweep-interval-secs N` | `PYPIRON_REPL_SWEEP_INTERVAL_SECS` | `300` | Backstop interval for draining pending cross-bucket repairs. Repairs also drain the moment a bucket recovers. Multi-bucket only. |
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

`buckets migrate` requires a multi-bucket target and **refuses while any bucket
still has pending cross-bucket repairs**, so shrinking or reordering the list
cannot strand a file's only copy on a bucket you are about to remove. If it
refuses, let the fleet drain (or bring the lagging bucket back) and retry. To
shrink to one bucket, stop the fleet and restart it with that bucket; topology
stamps are dormant in single-bucket mode.

Stop writes before `origin release`. It refuses a package with any package truth
except `.origin`, or with pending write/replication work, and conditionally
releases the claim on each configured bucket.

## Multi-bucket metrics

These series appear only when two or more buckets are configured:

| Metric | Meaning |
| --- | --- |
| `pypiron_replication_objects_total` | Artifact records copied into another bucket. |
| `pypiron_replication_bytes_total` | Artifact bytes copied into other buckets; companion metadata is not included. |
| `pypiron_replication_freezes_total` | Same-name, different-byte upload collisions where the loser was quarantined (or both quarantined when the arrival order was too close to call). Needs a human. Any increase needs attention. |
| `pypiron_replication_marker_backlog{dest}` | Pending cross-bucket repairs (fan-out failures awaiting drain) found on reachable source buckets. During a source outage this is a lower bound. |
| `pypiron_reconcile_diff_duration_seconds` | Wall time of the last pairwise full comparison. |
| `pypiron_bucket_health_state{bucket,index}` | Per-node view: healthy `1`, unknown `0`, unhealthy `-1`. |
| `pypiron_bucket_selected{bucket,index}` | Per-node selected bucket: selected `1`, all others `0`. |
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
| `/health` | open | Load balancer health. |
| `/metrics` | open | Prometheus metrics. |
| `/stats/downloads` | read | Global download stats. |
| `/stats/downloads/<pkg>` | read | Per-package download stats. |
| `/tokens` | read/uploader/admin, or open reader token | Mint install tokens. |
| `/files/.../yank` | admin | Yank a file. |
| `/files/.../delete` | admin | Delete a file. |
| `/project/<pkg>/status` | admin | Set project status. |
