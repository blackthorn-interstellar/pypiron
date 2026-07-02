# Download statistics

See which packages and versions your team installs. On by default; turn it off
with `--download-stats false`.

!!! warning "Beta"

    Download statistics are new. The `/stats/` endpoints, JSON shapes, and
    stored counter format may still change.

The counts are an analytic, not an audit log. A hard crash can lose recent
downloads; completed days are exact.

## Reading the stats

Two endpoints:

### Per package

`GET /stats/downloads/<pkg>` - last 30 days by version.

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

`GET /stats/downloads` - last 30 days plus busiest packages.

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

The homepage and `/downloads/` render the same cached ranking.

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

Exact flag/env definitions: [Configuration](../reference/configuration.md#server).
