# <img src="docs/pypiron-logo-256.png" alt="PypIron logo" width="40" style="vertical-align: middle;"/> PypIron

[![CI](https://github.com/brycedrennan/pypiron/actions/workflows/ci.yml/badge.svg)](https://github.com/brycedrennan/pypiron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pypiron.svg)](https://pypi.org/project/pypiron/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An ultra-fast PyPI server written in Rust.


## Highlights

- 🚀 **Ultra-fast** — a $12/month server answers ~75,000 requests per second.
- ♾️ **Infinite horizontal scaling that "just works"** — point any number of nodes at the same bucket; reads need zero coordination.
- 📊 **Per-project download tracking** — tag requests by consuming project, straight into Prometheus.
- 🔁 **Mirror or proxy PyPI** — one URL serves private packages and cached public dependencies, with PyPI's true upload times.
- 🗄️ **No database** — truth is files, views are regenerable, backups are rsync.
- 📦 **Standards-complete** — PEP 503, 691, 700, 658, 592; `uv`, `pip`, `twine`, `poetry`, and `pdm` work unmodified.
- 🛡️ **Dependency-confusion defense** — every package is exclusively private or mirrored, claimed at first write.
- 🩹 **Self-healing** — crash-safe event markers plus a daily storage audit; `pypiron resync` rebuilds the world.

## Performance

Measured on real AWS hardware with the S3 backend ([method and logs](docs/BENCHMARK_RESULTS.md)):

| | 2 CPUs EC2 | 8 CPU EC2 |
|---|---|---|
| Requests per second | **~75,000** | **~440,000** |
| Request latency | p99 2 ms | p99 5 ms |
| Publish → installable | **0.7 s** | 1 s with 10,000 packages hosted |
| 900 MB wheel upload | 15–20 s, ~50 MB memory | 8 simultaneous, reads stay fast |
| Download throughput | 3.9 Gbit/s* | 48 Gbit/s* |

\* Saturated

## Installation

```bash
uvx pypiron        # or: pip install pypiron
```

```bash
docker run --rm -it -p 8080:8080 \
  -e PYPIRON_ADMIN_USER=admin -e PYPIRON_ADMIN_PASS=secret \
  pypiron:latest
```

## Documentation

- [CONFIGURATION.md](docs/CONFIGURATION.md) — every flag, env var, and endpoint
- [DESIGN.md](docs/DESIGN.md) — architecture and reasoning ([VISION.md](docs/VISION.md) is the one-pager)
- [STANDARDS.md](docs/STANDARDS.md) — PEP support matrix
- [COMPATIBILITY.md](docs/COMPATIBILITY.md) — generated client compatibility matrix
- [TESTING.md](docs/TESTING.md) — blackbox-first test philosophy
- [ROADMAP.md](docs/ROADMAP.md) — features shipped, planned, and rejected
- [BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) — measured numbers and the improvements log

## Features

### Publish and install

```bash
PYPIRON_ADMIN_USER=admin PYPIRON_ADMIN_PASS=secret uvx pypiron

uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password secret dist/*.whl

pip install --index-url http://localhost:8080/simple/ mypackage
```

`twine`, `poetry`, and `pdm` work the same way. Point clients at this registry
*only* — never `--extra-index-url` (see the FAQ).

### Mirror PyPI

`pypiron sync` mirrors an allowlist of public packages, carrying PyPI's true
upload timestamps so `uv --exclude-newer` resolves historically correct
versions against your mirror:

```bash
pypiron sync --packages-list packages.txt \
  --to http://localhost:8080 --username admin --password adminsecret
```

```text
# packages.txt
requests>=2.20,<3
numpy
```

Wheel/platform/date filters and a `pypiron.toml` config file are in
[CONFIGURATION.md](docs/CONFIGURATION.md#sync-filters-and-config-file).

### Proxy PyPI on demand

`sync` mirrors what you list; the proxy mirrors what you *use* — fetched from
upstream on first request, cached in storage forever after, served locally
whether upstream is up or down:

```bash
pypiron --admin-user admin --admin-pass secret \
  --private-prefix acme \
  --proxy-upstream https://pypi.org
```

Names claimed private never fall through to upstream — the dependency-confusion
hole stays closed.

### Scale out

Start more nodes on the same bucket. That's the whole procedure:

```bash
pypiron --storage s3 --s3-bucket my-bucket ...   # node 1
pypiron --storage s3 --s3-bucket my-bucket ...   # node 2, same bucket, done
```

Reads are stateless file serving — no coordination, no shared state, no
session affinity. Nodes elect an index writer through an S3 lease; failover is
automatic.

### Authentication

Three optional basic-auth credentials: **admin** (everything), **uploader**
(publish), **reader** (read). No write credential configured means the server
is read-only; no read credential means reads are public.

```bash
pip install --index-url http://reader:secret@localhost:8080/simple/ mypackage
```

### Track downloads per project

Username subaddressing tags every request with the consuming project — counts
land in Prometheus `/metrics` as `pypiron_project_requests_total{project=...}`:

```bash
export UV_INDEX_COMPANY_USERNAME="reader+billing-api"
export UV_INDEX_COMPANY_PASSWORD="secret"
```

## FAQ

**Does it really not need a database?** No. Truth is files, the index is a
regenerable view, backups are rsync. See
[DESIGN.md](docs/DESIGN.md#what-no-db-honestly-costs).

**Why `--index-url` only, never `--extra-index-url`?** pip merges extra indexes
with no priority — that *is* the dependency-confusion vulnerability. Point
clients at this registry only; it decides what exists.

**Is one node enough?** Almost always. Artifacts are served immutable and
indexes ETag-revalidate, so client and proxy caches compound a single node's
already-large capacity. Add nodes for availability, not throughput.

**Is it production-ready?** For private registries — the stated target — yes:
one binary, measured numbers, and a blackbox suite that drives real clients.
For a multi-tenant pypi.org clone, no, and we don't try.

## Ecosystem

Alternatives, for comparison:
[devpi-server](https://github.com/devpi/devpi),
[pypiserver](https://github.com/pypiserver/pypiserver),
[pypicloud](https://github.com/stevearc/pypicloud),
[warehouse](https://github.com/pypi/warehouse).

## License

PypIron is licensed under the [MIT License](LICENSE).
