# <img src="docs/pypiron-logo-256.png" alt="PypIron logo" width="40" style="vertical-align: middle;"/> PypIron

[![CI](https://github.com/brycedrennan/pypiron/actions/workflows/ci.yml/badge.svg)](https://github.com/brycedrennan/pypiron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pypiron.svg)](https://pypi.org/project/pypiron/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An ultra-fast Python package server, written in Rust.

<p align="center">
  <img src="docs/install-throughput.png" alt="Max sustained install throughput: pypiron vs pypiserver, devpi, pypicloud, bandersnatch, proxpi" width="760">
  <br>
  <sub>installs per second · r7i.large</sub>
</p>


## Highlights
- **4x-60x faster** than other PyPi servers
- **Mitigates supply-chain attacks** avoid supply chain issues by excluding recent updates via `--exclude-newer`
- **Compatible with entire ecosystem** uv, pip, poetry, twine, pipenv, hatch
- **Infinite horizontal scaling that "just works"** — point any number of nodes at the same bucket; reads need zero coordination.
- **Per-project download tracking** — see per-package, per-version download statistics .
- **Mirror or proxy PyPI** — one URL serves private packages and cached public dependencies.
- **Dependency-confusion defense** — every package is exclusively private or mirrored, claimed at first write.


## Quickstart

```bash
uvx pypiron serve 
```

## Features

### Publish and install

```bash
PYPIRON_ADMIN_USER=admin PYPIRON_ADMIN_PASS=secret uvx pypiron serve

uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password secret dist/*.whl

uv add --index-url http://localhost:8080/simple/ mypackage
```

### Mirror PyPI

`pypiron sync` mirrors an allowlist of public packages, carrying PyPI's true
upload timestamps so `uv --exclude-newer` resolves historically correct
versions against your mirror:

```bash
# Mirror a list into a running server (sync is an HTTP client; --to is required)
pypiron sync --packages-list packages.txt \
  --to http://localhost:8080 --username admin --password adminsecret

# ...or a one-off package without a list file (--pkg is repeatable)
pypiron sync --pkg requests --pkg numpy \
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
pypiron serve --admin-user admin --admin-pass secret \
  --private-prefix acme \
  --proxy-upstream https://pypi.org
```

Names claimed private never fall through to upstream — the dependency-confusion
hole stays closed.

### Scale out

Start more nodes on the same bucket. That's the whole procedure:

```bash
pypiron serve --storage s3 --s3-bucket my-bucket ...   # node 1
pypiron serve --storage s3 --s3-bucket my-bucket ...   # node 2, same bucket, done
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

## Ecosystem

Alternatives, for comparison:
[devpi-server](https://github.com/devpi/devpi),
[pypiserver](https://github.com/pypiserver/pypiserver),
[pypicloud](https://github.com/stevearc/pypicloud),
[devpi](https://www.devpi.net/).

