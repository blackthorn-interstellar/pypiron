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

### S3

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--s3-bucket NAME` | `PYPIRON_S3_BUCKET` | required | Bucket. |
| `--aws-region REGION` | `AWS_REGION` | none | AWS region. |
| `--s3-endpoint-url URL` | `PYPIRON_S3_ENDPOINT_URL` | none | MinIO or another S3-compatible endpoint. |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false` | Path-style addressing. |

AWS credentials use the standard AWS chain: env, web identity, instance role, or
task role.

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
| `--worker-interval-secs N` | `PYPIRON_WORKER_INTERVAL_SECS` | `1` | Peer-write poll cadence. |
| `--intent-grace-secs N` | `PYPIRON_INTENT_GRACE_SECS` | `900` | Grace for an upload in progress. |
| `--audit-on-boot true\|false` | `PYPIRON_AUDIT_ON_BOOT` | `true` | Audit when a node becomes leader. |
| `--reconcile-interval-secs N` | `PYPIRON_RECONCILE_INTERVAL_SECS` | `86400` | Audit sweep interval. |
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

`verify-index` and `rebuild-index` use the same storage flags as `serve`, and
also read `[serve]` from `pypiron.toml`.

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
