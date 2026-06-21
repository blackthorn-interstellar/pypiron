# Management API

pypiron speaks the standard PyPI HTTP surface (the [simple index](standards.md)
and the legacy upload API). This page documents the rest: the admin operations,
the stats endpoints, and the operational probes.

Auth follows the same rules as everywhere else (admin ⊇ uploader ⊇ reader). See
[Authentication](configuration.md#authentication).

| Method | Path | Role |
| --- | --- | --- |
| `DELETE` | `/files/<pkg>/<filename>` | admin |
| `POST` / `DELETE` | `/files/<pkg>/<filename>/yank` | admin |
| `POST` / `DELETE` | `/project/<pkg>/status` | admin |
| `POST` | `/legacy/` | uploader |
| `GET` | `/stats/downloads` | reader |
| `GET` | `/stats/downloads/<pkg>` | reader |
| `GET` | `/health` | open |
| `GET` | `/metrics` | open |

## Management

Delete, yank, and project status are **admin** operations.

```bash
# Delete a file. The index entry is removed first, then the artifact, so a
# client never sees a link to bytes that are already gone. Returns 204.
curl -u admin:secret -X DELETE http://HOST:8080/files/<pkg>/<filename>

# Yank a file (PEP 592). The request body becomes the reason. Returns 200.
curl -u admin:secret -X POST -d "broken release" \
  http://HOST:8080/files/<pkg>/<filename>/yank

# Un-yank. Returns 200.
curl -u admin:secret -X DELETE http://HOST:8080/files/<pkg>/<filename>/yank

# Set PEP 792 project status. The body is the status doc. Returns 200.
curl -u admin:secret -X POST -d '{"status":"quarantined","reason":"security hold"}' \
  http://HOST:8080/project/<pkg>/status

# Clear project status (revert to active). Returns 200.
curl -u admin:secret -X DELETE http://HOST:8080/project/<pkg>/status
```

A yanked file stays downloadable; installers skip it unless pinned exactly. A
yank with an empty body sets the bare flag with no reason. Yank state and project
status are stored as truth on disk, so the index heals from them — `sync` relays
upstream yank state and status through these same endpoints.

!!! note
    Deletion removes one artifact. The package name's `private`/`mirror` claim is
    durable on purpose: emptying a name does not release it for the other world to
    re-claim. See [Storage](../concepts/storage.md).

## Upload

`POST /legacy/` is the legacy PyPI upload API — a multipart form with the wheel
or sdist in the `content` field plus metadata fields. This is what `twine` and
`uv publish` target; you do not call it by hand. Requires uploader (or admin)
auth. See [First steps](../getting-started/first-steps.md) for client setup.

## Stats

Per-package and global download counts, gated by read auth (open when no read
credential is configured). Counts are a best-effort analytic, never truth, and
require `--download-stats` (on by default). Full reference:
[Download statistics](../concepts/download-stats.md).

```bash
# Global: last 30 days of daily totals plus the busiest packages.
curl -u "$READ" http://HOST:8080/stats/downloads

# Per package: last 30 days, rolled up to versions, today included.
curl -u "$READ" http://HOST:8080/stats/downloads/<pkg>
```

Both return JSON.

## Operations

These endpoints are deliberately outside read auth — load balancers and
Prometheus scrapers do not carry package credentials.

### Health

```bash
curl http://HOST:8080/health
```

`200 {"status":"ok"}` when storage answers a probe, `503 {"status":"degraded"}`
otherwise. Unauthenticated — point your load balancer here.

### Metrics

```bash
curl http://HOST:8080/metrics
```

Prometheus text exposition. Unauthenticated. Cardinality is kept low on purpose:
no per-package labels. The families:

| Family | Type | What |
| --- | --- | --- |
| `pypiron_http_requests_total{route,status}` | counter | Requests by route group (`simple`/`files`/`legacy`/`health`/`metrics`/`other`) and status class (`2xx`–`5xx`) |
| `pypiron_downloads_total` | counter | Artifact downloads served (streamed 200s and presigned 302s) |
| `pypiron_index_rebuilds_total` | counter | Package index rebuilds |
| `pypiron_reconcile_sweeps_total` | counter | Full reconcile sweeps completed |
| `pypiron_audit_packages_rebuilt_total` / `_skipped_total` | counter | Audit outcomes (rebuilt vs fingerprint-skipped) |
| `pypiron_audit_last_duration_seconds` | gauge | Wall time of the last audit pass |
| `pypiron_global_cas_conflicts_total` | counter | Global-index write-backs lost to a peer (multi-node) |
| `pypiron_stale_intents_healed_total` | counter | Unpaired write intents healed after a crashed writer |
| `pypiron_proxy_listing_fetches_total` / `_errors_total` | counter | Upstream listing fetches (proxy mode) |
| `pypiron_proxy_artifacts_cached_total` / `_artifact_errors_total` | counter | Upstream artifacts cached / fetch failures (proxy mode) |
| `pypiron_registry_projects` / `_releases` / `_files` / `_bytes` | gauge | Registry inventory, measured by the last sweep |
| `pypiron_project_requests_total{project,route}` | counter | Per-tag traffic, present once requests carry a username (the `+tag` subaddress, else the username itself) |

The per-package download breakdown is kept off `/metrics` (registry-sized
cardinality) and lives at `/stats/downloads` instead.

### Logs

Logs go to stdout via `tracing`. `--log-format json` (`PYPIRON_LOG_FORMAT=json`)
emits one JSON object per line for log pipelines. Per-request logging is at
`debug`, off by default so the access log never becomes the workload — turn it on
with `RUST_LOG=pypiron=debug`.
