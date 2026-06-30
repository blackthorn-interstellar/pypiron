# Management API

An admin and operations surface: delete and yank files, set project status, read
download stats, wire up health checks and Prometheus metrics. The install and
publish endpoints — the [simple index](standards.md) and the legacy upload API —
are standard PyPI.

Auth follows the same rules everywhere (admin ⊇ uploader ⊇ reader). See
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
# Delete a file (returns 204). The index link is removed before the bytes, so a
# client never sees a link to an artifact that's already gone.
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

A yanked file stays downloadable; installers skip it unless pinned exactly. An
empty yank body sets the flag with no reason. Yank and project status persist
across restarts; `sync` relays upstream yank state and status through these same
endpoints.

!!! note
    Deleting files never releases the name. A name reserved private (or public)
    stays reserved after you delete every file — it can't be re-registered as
    the other kind. See [Storage](../concepts/storage.md).

## Upload

`POST /legacy/` is the legacy PyPI upload API — a multipart form with the wheel
or sdist in the `content` field plus metadata fields. `twine` and `uv publish`
target it; you don't call it by hand. Requires uploader (or admin) auth. See the
[Quickstart](../index.md#quickstart) for client setup.

## Stats

Per-package and global download counts at `GET /stats/downloads/<pkg>` and
`GET /stats/downloads`, gated by read auth (public when no read credential is
set), on by default (`--download-stats`). Full reference, JSON shapes, accuracy
guarantees: [Download statistics](../concepts/download-stats.md).

## Operations

Health and metrics sit outside read auth on purpose — your load balancer and
Prometheus scraper don't carry package credentials.

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

Prometheus text exposition, unauthenticated. Metrics avoid per-package labels,
which would overwhelm Prometheus; the per-package download breakdown lives at
`/stats/downloads`. The families:

| Family | Type | What |
| --- | --- | --- |
| `pypiron_http_requests_total{route,status}` | counter | Requests by route group (`simple`/`files`/`legacy`/`health`/`metrics`/`other`) and status class (`2xx`–`5xx`) |
| `pypiron_downloads_total` | counter | Artifact downloads served, whether streamed or handed off to object storage |
| `pypiron_registry_projects` / `_releases` / `_files` / `_bytes` | gauge | Registry inventory (projects, releases, files, bytes), measured by the last sweep |
| `pypiron_project_requests_total{project,route}` | counter | Per-tag traffic, attributed to a username tag (`reader+billing-api`) when requests carry one |
| `pypiron_proxy_listing_fetches_total` / `_errors_total` | counter | Upstream listing fetches and failures (proxy mode) |
| `pypiron_proxy_artifacts_cached_total` / `_artifact_errors_total` | counter | Upstream artifacts cached and fetch failures (proxy mode) |

??? note "Advanced metrics (multi-node internals)"
    These track pypiron's index maintenance and multi-node coordination. Ignore
    them on a single node; they're for operators tuning a fleet (see
    [DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md)).

    | Family | Type | What |
    | --- | --- | --- |
    | `pypiron_index_rebuilds_total` | counter | Package index rebuilds |
    | `pypiron_reconcile_sweeps_total` | counter | Full reconcile sweeps completed |
    | `pypiron_audit_packages_rebuilt_total` / `_skipped_total` | counter | Audit outcomes (rebuilt vs fingerprint-skipped) |
    | `pypiron_audit_last_duration_seconds` | gauge | Wall time of the last audit pass |
    | `pypiron_global_cas_conflicts_total` | counter | Global-index write-backs lost to a peer |
    | `pypiron_stale_intents_healed_total` | counter | Write intents recovered after a crashed writer |

### Logs

JSON logs for your aggregation pipeline: `--log-format json`
(`PYPIRON_LOG_FORMAT=json`) emits one JSON object per line to stdout. Per-request
access logging is off by default so it never becomes the workload; enable it with
`RUST_LOG=pypiron=debug`.
