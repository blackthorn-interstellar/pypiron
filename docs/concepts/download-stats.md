# Download statistics

pypiron counts artifact downloads per `(package, filename)` and rolls them up to
versions. It is on by default (`--download-stats`). The counts are a best-effort
analytic, not truth — see [Accuracy](#accuracy-and-freshness).

## Reading the stats

Two endpoints, both gated by read auth (when `--read-user` is set, the read,
uploader, or admin credential all work; otherwise they are public).

### Per package

`GET /stats/downloads/<pkg>` — last 30 days, rolled up to versions, includes
today.

```bash
curl -u $READ http://localhost:8080/stats/downloads/acme
```

```json
{
  "metric": "downloads",
  "package": "acme",
  "total": 412,
  "days": {
    "2026-06-20": { "1.3.0": 180, "1.2.0": 41 },
    "2026-06-21": { "1.3.0": 191 }
  }
}
```

### Global

`GET /stats/downloads` — last 30 days of per-day totals and the busiest
packages, including today (recent, not-yet-frozen days are aggregated live on
read, the same as the per-package endpoint).

```bash
curl -u $READ http://localhost:8080/stats/downloads
```

```json
{
  "metric": "downloads",
  "total": 14739,
  "days": { "2026-06-19": 4903, "2026-06-20": 4918, "2026-06-21": 4918 },
  "top": { "acme": 3120, "requests": 2204 }
}
```

### In the browser

Two human pages render the same numbers for an authorized reader (gated like the
JSON endpoints above):

- The homepage (`/`) leads its activity panel with a **Most Downloaded Packages**
  chart — the top five over the last 30 days — linking to the full leaderboard.
- `GET /downloads/` is that leaderboard: the busiest packages (up to 500), each
  linked to its project page.

Both read a short-lived cached ranking, so a public homepage never rescans the
counter store on every hit. A public deployment (no read credential) shows these
to everyone; a credentialed one shows them only to readers, so private names
never leak.

## How counting works

Each node counts downloads in memory and flushes immutable delta segments under
`_counters/` every `--counters-flush-interval-secs` (300 s default). The leader
compacts each finished day into one frozen file per shard plus a small per-day
summary, and prunes history past `--counters-retention-days`. There is no
database — the counter store is files like everything else.

## Accuracy and freshness

- Counts are **lossy by design**: an in-memory tail can be lost on a hard crash,
  and they are never used as the source of truth for anything.
- **Frozen (closed) days are exact.** The leader has merged every node's deltas.
- **Today lags one flush interval** — a download shows up after the next flush,
  on both the per-package and global endpoints (recent days that haven't been
  frozen yet are summed live from segments on read, so the global view is never
  days behind).
- Changing `--counters-resolution` is non-destructive; existing days keep the
  resolution they were written with.

## Metrics

`/metrics` carries only a single low-cardinality aggregate,
`pypiron_downloads_total`. The per-package breakdown is deliberately kept off
`/metrics` — registry-sized label cardinality would overwhelm Prometheus. Use
the `/stats/` endpoints for per-package and per-version numbers.

## Per-project attribution

Usernames support Gmail-style subaddressing for attribution. Authenticating as
`$READ+billing-api` records `billing-api` as a project tag, exposed in
`/metrics` as `pypiron_project_requests_total{project="billing-api",route=...}`.

=== "uv"

    ```bash
    export UV_INDEX_COMPANY_USERNAME="read+billing-api"
    export UV_INDEX_COMPANY_PASSWORD="secret"
    ```

=== "pip"

    ```bash
    pip install --index-url http://read+billing-api:secret@localhost:8080/simple/ acme
    ```

This is request attribution (which team is pulling), separate from the
download-count store above. Details and the cardinality cap are in
[Authentication](authentication.md) and the
[Configuration reference](../reference/configuration.md#per-project-download-tracking).

## Cost

Cost is dominated by flush PUTs, scaling with `flush_interval × nodes`. At the
300 s default that is roughly **$0.04 per node per month** — effectively free for
a private registry. Raise `--counters-flush-interval-secs` to spend less and
accept staler "today" numbers.

## Tuning

| Knob | Default | Controls |
| --- | --- | --- |
| `--download-stats` | `true` | Enable counting (`false` = no-op store) |
| `--counters-resolution` | `1d` | Intra-day bucket width (`1d`/`1h`/`30m`/`2h`) |
| `--counters-flush-interval-secs` | `300` | Per-node flush cadence — the dominant cost knob |
| `--counters-rollup-interval-secs` | `3600` | Leader compaction cadence |
| `--counters-retention-days` | `90` | Days of history kept |

Exact flag/env definitions are in the
[Configuration reference](../reference/configuration.md#server).
