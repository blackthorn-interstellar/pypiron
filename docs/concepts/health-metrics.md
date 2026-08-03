---
description: Wire pypiron into monitoring: /health for liveness, /ready for the load balancer, Prometheus counters worth alerting on, and JSON logs.
---

# Health & metrics

Monitor pypiron with three open endpoints: `/health` (the process is up),
`/ready` (this node can serve), `/metrics` (what it's doing). None require
auth.

## Liveness and readiness

`/health` is liveness: `200` while the process runs. Wire a Kubernetes
`livenessProbe` here, or probe it from a shell with `pypiron healthcheck`.

`/ready` is readiness: it turns `503` the moment a node starts draining for
shutdown, or can reach no bucket. Point the load balancer's health check and the
`readinessProbe` here — the front door pulls a draining node before it stops
accepting, and during a storage outage clients route to nodes that can still
serve. In a multi-bucket fleet that is what moves traffic to a surviving region:
[Survive a region or cloud outage](../guides/multi-region.md).

Route on `/ready`; restart on `/health`.

## /metrics

Prometheus format. The series worth watching:

| Series | Meaning |
| --- | --- |
| `pypiron_http_requests_total` | HTTP requests served. |
| `pypiron_downloads_total` | Package downloads, one aggregate. Per-package numbers live at `/stats/downloads`, not on `/metrics`. |
| `pypiron_storage_ops_total{op}` | Backend calls (`read`/`write`/`list`/`delete`). On object storage, these are your billable requests. |
| `pypiron_blocked_downloads_total` | Malware downloads refused. |
| `pypiron_advisory_snapshot_age_seconds` | Age of the newest advisory in the loaded feed. |
| `pypiron_advisory_last_refresh_age_seconds` | Time since this node last read the feed. |

The advisory series appear when the advisory feed is enabled — it is by default.
Running more than one bucket adds replication and failover series —
`pypiron_replication_freezes_total`, `pypiron_bucket_health_state`, and the rest:
[Multi-bucket metrics](../reference/configuration.md#multi-bucket-metrics).

## What to alert on

- **`pypiron_blocked_downloads_total` rising.** A machine in your fleet is still
  asking for a known-bad file. Find it.
- **`pypiron_advisory_snapshot_age_seconds` past your refresh window.** Malware
  blocking is drifting stale — a dead feed URL, or an air-gapped ferry that
  stopped delivering.
- **`pypiron_storage_ops_total` climbing against `pypiron_http_requests_total`.**
  Read amplification — worth investigating before the storage bill notices.
- **Any multi-bucket freeze or fence.** A rising
  `pypiron_replication_freezes_total` is an upload collision that was
  quarantined and needs a human. A `pypiron_bucket_topology_write_fenced` of `1`
  means mutations have stopped. Watch `pypiron_replication_marker_backlog` too —
  a backlog still growing after its bucket recovers is stuck.

Username tags (`reader+billing-api`) record a project label in request metrics.
`--metrics-project-labels` exposes those labels on `/metrics` — off by default,
because `/metrics` is unauthenticated and the labels would let any scraper
enumerate internal project names.

## Logs

`--log-format json` switches logs to one JSON object per line. The default is
human-readable text. Mutations are always logged. `--access-log` logs reads too
(structured, or Combined Log Format with `--access-log-format clf`).

Every flag: [Configuration](../reference/configuration.md#server).
