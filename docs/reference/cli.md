# CLI

One binary, four subcommands:

```bash
pypiron serve          # run the server
pypiron sync           # mirror packages into a server over HTTP
pypiron verify-index   # check that served indexes match truth (read-only)
pypiron rebuild-index  # rebuild every index from truth, unconditionally
```

Every `--flag` has a matching `PYPIRON_*` environment variable. Values layer
**CLI/env > `pypiron.toml` > defaults**. `pypiron <cmd> --help` prints the
authoritative, complete list for any subcommand.

```bash
pypiron serve --help
```

!!! tip
    This page covers the handful of flags you reach for daily. The full
    reference — every flag, its env var, and its default — lives in
    [Configuration](configuration.md).

## serve

Run the server. The day-to-day command.

```bash
pypiron serve \
  --storage disk --data-dir ./data \
  --admin-pass "$ADMIN" \
  --read-user reader --read-pass "$READ"
```

| Flag | Purpose |
| --- | --- |
| `--storage` | Backend: `disk` (default), `s3`, `gcs`, `azure`. Location comes from `--data-dir` (disk) / `--s3-bucket` / `--gcs-bucket` / `--azure-*`. |
| `--bind-addr` | Listen address. Default `0.0.0.0:8080`. |
| `--admin-pass` | Enables the admin role (mirror, delete, yank). Username defaults to `admin`. |
| `--uploader-user` / `--uploader-pass` | Enables ordinary publishing. |
| `--read-user` / `--read-pass` | When set, `/simple/` and `/files/` require auth. Unset, reads are public. |
| `--private-prefix` | Reserve a namespace for private uploads so names never fall through upstream. |
| `--proxy-upstream` | Serve unknown packages on demand from an upstream index (e.g. `https://pypi.org`) and cache them. |
| `--artifact-delivery` | How wheel bytes reach clients: `auto` (default), `redirect`, `stream`. |

With no upload credential of any kind, the server is read-only. `/health` and
`/metrics` are always open, even when reads require auth. See
[Authentication](../concepts/authentication.md).

## sync

Mirror packages from PyPI (or any PEP 691 index) into a pypiron server. Sync is
an HTTP client: it POSTs each file to the destination's `/legacy/` endpoint
using the server's admin credential. It needs a URL, not access to the storage
backend.

```bash
pypiron sync \
  --to http://localhost:8080 --password "$ADMIN" \
  --pkg "requests>=2.20,<3" --pkg numpy
```

| Flag | Purpose |
| --- | --- |
| `--to` | Destination pypiron base URL. Required. |
| `--from` | Source index base. Default `https://pypi.org`. |
| `--pkg` | One package, with optional PEP 440 specifiers. Repeatable. |
| `--packages-list` | Text file of packages, one per line. |
| `--config` | Path to a `pypiron.toml` (global; `serve` reads it too). Defaults to `./pypiron.toml` when present. |
| `--username` / `--password` | Admin credential for the destination. |
| `--full` | Ignore the conditional-fetch memo; re-fetch and reconcile everything. |
| `--dry-run` | Print what would be copied, transfer nothing. |
| `--filter-exclude-newer` | Only mirror files received upstream before a cutoff (timestamp, date, `7`, `30 days`, `P30D`). One of the shared `--filter-*` flags (see [Configuration](configuration.md#filters)). |

A normal run only touches projects whose upstream listing changed; `--full` is
the periodic self-heal. Once mirrored, an artifact is never deleted — re-runs
reconcile yank state and project status. See
[Mirroring](../concepts/mirroring.md) for filters and the full flag set.

## verify-index

Recompute every index from truth (artifacts plus sidecars) and diff against what
storage actually serves. Strictly read-only: where the server would heal a
missing or stale view, `verify-index` reports it instead.

```bash
pypiron verify-index --storage disk --data-dir ./data
```

Each divergence prints as `kind<TAB>package<TAB>detail`, followed by a summary
line. Use it in CI or after out-of-band storage changes to assert convergence.
Exit codes follow the grep/diff idiom so a pipeline can branch the three
outcomes:

| Code | Meaning |
| --- | --- |
| `0` | Converged — views match truth. |
| `1` | Diverged — at least one difference (listed on stdout). |
| `2` | Could not run — storage unreachable, bad config, or I/O failure. |

It scans the whole corpus, so cost scales with corpus size, not churn. **S3 rule
of thumb: ~$0.5 and ~20 min per million files** (single node, default
concurrency; mostly sidecar GETs, no writes). The day-to-day `serve` audit stays
seconds and pennies at any scale because fingerprints skip unchanged packages.

## rebuild-index

Rebuild every materialized view from truth, unconditionally. Run it after
restoring a backup or editing storage out of band — `serve` heals on its own
schedule, but `rebuild-index` forces the full sweep now.

```bash
pypiron rebuild-index --storage disk --data-dir ./data
```

Like `verify-index` it scans the whole corpus (cost scales with corpus, not
churn), and additionally rewrites views and re-fingerprints. **S3 rule of thumb:
~$1–1.5 and ~20–30 min per million files** (single node, default concurrency;
sidecar GETs + per-package LISTs, plus PUTs on a real restore). To only check for
drift without writing, use the cheaper read-only `verify-index`.

## Global flags

`--log-format text` (default) or `--log-format json` applies to every
subcommand and may sit before or after it. Set `RUST_LOG` to change log
verbosity.

```bash
PYPIRON_LOG_FORMAT=json pypiron serve --storage s3 --s3-bucket my-bucket
```
