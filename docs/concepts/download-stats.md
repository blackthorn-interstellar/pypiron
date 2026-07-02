# Download statistics

See which packages — and which versions — your team installs. pypiron counts
every download and rolls it up by version: spot what's heavily used, what's gone
stale, who's pulling what. On by default; turn it off with `--download-stats
false`.

!!! warning "Beta"

    Download statistics are new. The `/stats/` endpoints, JSON shapes, and
    stored counter format may still change.

The counts are an analytic, not an audit log: a hard crash can lose recent
downloads, but completed days are exact (see [Accuracy](#accuracy-and-freshness)).

## Reading the stats

Two endpoints, both gated by read auth. With `--read-user` set, the read,
uploader, or admin credential works; otherwise public.

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
packages, including today.

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

Two pages render the same numbers for an authorized reader (gated like the JSON
endpoints):

- The homepage (`/`) leads its activity panel with a **Most Downloaded Packages**
  chart — top five over the last 30 days — linking to the full leaderboard.
- `GET /downloads/` is that leaderboard: the busiest packages (up to 500), each
  linked to its project page.

These pages serve a cached ranking, so a public homepage stays fast under load. A
public deployment (no read credential) shows them to everyone; a credentialed one
shows them only to readers, so private names never leak.

## Accuracy and freshness

- **Completed days are exact** — every node's counts are merged in.
- **Today lags one flush interval.** A download shows up after the next flush
  (300 s by default), on both the per-package and global endpoints — never days
  behind.
- **Recent downloads can be lost in a hard crash.** The counts are an analytic,
  never the source of truth.
- Changing `--counters-resolution` is safe: existing days keep the resolution
  they were written with.

## Metrics

`/metrics` carries a single aggregate, `pypiron_downloads_total`. The
per-package, per-version breakdown stays off `/metrics` — that many labels would
overwhelm Prometheus — use the `/stats/` endpoints for it.

## Per-project attribution

Want to know which team is pulling what? Username tags do it. Authenticate as
`$READ+billing-api` and pypiron records `billing-api` as a project tag, exposed
in `/metrics` as
`pypiron_project_requests_total{project="billing-api",route=...}`.

=== "uv"

    ```bash
    export UV_INDEX_COMPANY_USERNAME="read+billing-api"
    export UV_INDEX_COMPANY_PASSWORD="secret"
    ```

=== "pip"

    ```bash
    pip install --index-url http://read+billing-api:secret@localhost:8080/simple/ acme
    ```

Request attribution (which team is pulling), separate from the download-count
store above. Details and the cardinality cap:
[Authentication](authentication.md) and the
[Configuration reference](../reference/configuration.md#per-project-download-tracking).

## Cost

About **$0.04 per node per month** — effectively free for a private registry.
Raise `--counters-flush-interval-secs` to spend less, at the cost of staler
"today" numbers.

## Tuning

| Knob | Default | Controls |
| --- | --- | --- |
| `--download-stats` | `true` | Count downloads (`false` turns it off) |
| `--counters-resolution` | `1d` | Time bucket within a day (`1d`/`1h`/`30m`/`2h`) |
| `--counters-flush-interval-secs` | `300` | How often counts are written out — the dominant cost knob |
| `--counters-rollup-interval-secs` | `3600` | How often completed days are finalized |
| `--counters-retention-days` | `90` | Days of history kept |

Exact flag/env definitions:
[Configuration reference](../reference/configuration.md#server).
