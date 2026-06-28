# CLI

One binary, five subcommands:

```bash
pypiron serve          # run the server
pypiron sync           # mirror packages into a server over HTTP
pypiron verify-index   # check that served indexes match truth (read-only)
pypiron rebuild-index  # rebuild every index from truth, unconditionally
pypiron healthcheck    # probe a running server's /health (exit 0/1)
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
  --to http://localhost:8080 --admin-pass "$ADMIN" \
  --include-package "requests>=2.20,<3" --include-package numpy
```

| Flag | Purpose |
| --- | --- |
| `--to` | Destination pypiron base URL. Required. |
| `--from` | Source index base. Default `https://pypi.org`. |
| `--include-package` | One package, with optional PEP 440 specifiers. Repeatable. Shared with `serve` (see [Configuration](configuration.md#mirror-selection)). |
| `--include-packages-from` | Text file of packages, one per line. |
| `--config` | Path to a `pypiron.toml` (global; read by every subcommand — `verify-index`/`rebuild-index` use its `[serve]` storage selection). Defaults to `./pypiron.toml` when present. |
| `--admin-user` / `--admin-pass` | Admin credential for the destination. |
| `--full` | Ignore the conditional-fetch memo; re-fetch and reconcile everything. |
| `--dry-run` | Print what would be copied, transfer nothing. |
| `--exclude-newer` | Only mirror files received upstream before a cutoff (timestamp, date, `7`, `30 days`, `P30D`). **Defaults to `7`** — a sliding 7-day quarantine; `""` disables it. One of the shared mirror-selection flags (see [Configuration](configuration.md#mirror-selection)). |

A normal run only touches projects whose upstream listing changed; `--full` is
the periodic self-heal. Once mirrored, an artifact is never deleted — re-runs
reconcile yank state and project status. See
[Mirroring](../concepts/mirroring.md) for mirror selection and the full flag set.

## verify-index

A **server maintenance command**: recompute every index from truth (artifacts
plus sidecars) and diff against what storage actually serves. Strictly
read-only: where the server would heal a missing or stale view, `verify-index`
reports it instead.

Run it on a server node against the **same storage backend `serve` uses**. Pass
the same `--config pypiron.toml` (or the same `--storage`/`PYPIRON_*` flags) so
it points at your real backend — it reads the `[serve]` storage selection from
the file. Run with no storage flags in a directory without a `pypiron.toml` and
it checks the default `./data` disk store.

```bash
pypiron verify-index --config pypiron.toml      # same backend as serve
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

A **server maintenance command**: rebuild every materialized view from truth,
unconditionally. Run it after restoring a backup or editing storage out of
band — `serve` heals on its own schedule, but `rebuild-index` forces the full
sweep now.

Like `verify-index`, run it on a server node against the **same storage backend
`serve` uses** — pass the same `--config pypiron.toml` (it reads the `[serve]`
storage selection) or the same `--storage`/`PYPIRON_*` flags.

```bash
pypiron rebuild-index --config pypiron.toml      # same backend as serve
pypiron rebuild-index --storage disk --data-dir ./data
```

Like `verify-index` it scans the whole corpus (cost scales with corpus, not
churn), and additionally rewrites views and re-fingerprints. **S3 rule of thumb:
~$1–1.5 and ~20–30 min per million files** (single node, default concurrency;
sidecar GETs + per-package LISTs, plus PUTs on a real restore). To only check for
drift without writing, use the cheaper read-only `verify-index`.

## healthcheck

A **liveness probe**: GET a running server's `/health` and exit `0` when healthy,
nonzero otherwise. It is self-contained — no `curl`/`wget` — so the container
image uses it as its built-in
[`HEALTHCHECK`](../getting-started/installation.md#container-image) and any
orchestrator can reuse the same line.

```bash
pypiron healthcheck                                  # probes 127.0.0.1:<bind port>/health
pypiron healthcheck --url http://other-host:8080/health
```

With no `--url` it derives the port from `PYPIRON_BIND_ADDR` (the same knob
`serve` reads, defaulting to 8080) and always probes loopback, so the baked-in
container check follows a port override without being edited. Override the whole
URL with `--url` / `PYPIRON_HEALTHCHECK_URL`.

## Global flags

`--log-format text` (default) or `--log-format json` applies to every
subcommand and may sit before or after it. Set `RUST_LOG` to change log
verbosity.

```bash
PYPIRON_LOG_FORMAT=json pypiron serve --storage s3 --s3-bucket my-bucket
```

### Access log

By default pypiron logs **mutations** (uploads, deletes, yanks, status changes) on
the `pypiron::access` target and stays silent on reads. `--access-log` widens that
to **every request** (the full access log). `/health` and `/metrics` log only at
debug in either mode. See [configuration](configuration.md#access-log) for the
field list and rationale.

The default `structured` rendering follows `--log-format` and is tunable with
`RUST_LOG` (`RUST_LOG=pypiron::access=warn` for failures only).
`--access-log-format clf` instead writes Combined Log Format to stdout for log
tooling:

```bash
# full structured JSON access log for a pipeline
pypiron serve --access-log --log-format json

# full access log in Combined Log Format for GoAccess/lnav (quiet diagnostics)
RUST_LOG=warn pypiron serve --access-log --access-log-format clf

# see /health and /metrics too (debug)
RUST_LOG=pypiron::access=debug pypiron serve --access-log
```
