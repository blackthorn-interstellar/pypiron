# <img src="docs/assets/pypiron-logo-256.png" alt="PypIron logo" width="40" style="vertical-align: middle;"/> PypIron

[![CI](https://github.com/brycedrennan/pypiron/actions/workflows/ci.yml/badge.svg)](https://github.com/brycedrennan/pypiron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pypiron.svg)](https://pypi.org/project/pypiron/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-pypiron-bf5a2e.svg)](https://brycedrennan.github.io/pypiron/)

An ultra-fast Python package server, written in Rust.

**Documentation:** <https://brycedrennan.github.io/pypiron/>

<p align="center">
  <img src="docs/assets/install-throughput.png" alt="Max sustained install throughput: pypiron vs pypiserver, devpi, pypicloud, bandersnatch, proxpi" width="760">
</p>


## Highlights
- **Handles 4–60× more load** than other PyPI servers
- **Mitigates supply-chain attacks.** Avoid supply chain issues by excluding recent updates via `--exclude-newer`
- **Compatible with entire ecosystem/** uv, pip, poetry, twine, pipenv, hatch
- **Infinite horizontal scaling that "just works".** Point any number of nodes at the same bucket; reads need zero coordination.
- **Per-project download tracking.** Per-package, per-version download statistics.
- **Host private and public packages together.** One URL serves private packages and cached public dependencies.
- **Dependency-confusion defense.** Every package is exclusively private or mirrored, claimed at first write.


## Quickstart

```bash
uvx pypiron serve --admin-pass my-pw

# publish
uv publish --publish-url http://HOST:8080/legacy/ --username admin --password "my-pw" dist/*

# install — add pypiron alongside PyPI (pip: --extra-index-url)
uv add --index http://@HOST:8080/simple/ acme-widgets
```

Want a single index that serves public packages too, instead of two? → setup 2.

### One index for private + public

The most common setup: a single URL that serves your private packages and
caches everything else from PyPI on first use, so developers configure one index
instead of two. That's also what closes the dependency-confusion hole.

```bash
uvx pypiron serve --admin-pass "$ADMIN" \
  --private-prefix acme \
  --proxy-upstream https://pypi.org \
  --proxy-exclude-newer "7 days"
```

- `--proxy-upstream` mirrors public packages on demand, cached in storage
  forever after — served locally whether PyPI is up or down.
- `--private-prefix acme` reserves the `acme-*` namespace for your uploads;
  those names never fall through to upstream.
- `--proxy-exclude-newer` *(optional)* hides releases younger than the window —
  a cheap supply-chain quarantine; `uv --exclude-newer` resolves against it.

Point installs and CI at the one index — public and private resolve together:

```bash
uv add --default-index http://HOST:8080/simple/ requests acme-widgets
```

### 3. Air-gapped mirror

No egress: the serving node can't reach PyPI, so you pre-load an allowlist with
`pypiron sync` from a host that can. `sync` is a pure HTTP client — it only needs
the server's URL and the admin credential, nothing about its storage.


From a host that can reach both PyPI and the server, put the allowlist and
filters in `pypiron.toml` (auto-discovered in the working directory):

```toml
[sync]
to = "http://HOST:8080"
username = "admin"                       # password via PYPIRON_SYNC_PASSWORD
packages = ["requests>=2.20,<3", "numpy", "pandas"]
only-wheels = true
exclude-newer = "2026-01-01T00:00:00Z"   # reproducible, historically-correct cutoff
```

```bash
export PYPIRON_SYNC_PASSWORD="$ADMIN"
pypiron sync                 # re-run anytime; unchanged upstream is a 304 and skipped
```

Run `pypiron sync --full` on a schedule (e.g. nightly) to reconcile yanks and
upstream removals. Wheel/platform/date filters:
[CONFIGURATION.md](docs/reference/configuration.md#sync-filters-and-config-file).

### 4. Production (S3, multi-node, HTTPS)

Object storage instead of disk, any number of identical nodes on the same
bucket. `docker-compose.yml`:

```yaml
services:
  pypiron:
    image: ghcr.io/brycedrennan/pypiron:latest
    command: pypiron serve --storage s3 --s3-bucket my-pypiron
    ports: ["8080:8080"]
    environment:
      PYPIRON_ADMIN_PASS: ${ADMIN}
      PYPIRON_READ_USER: team
      PYPIRON_READ_PASS: ${READ}
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}      # on AWS, drop these two and
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}  # use the instance/task role
```

Scale out by running the same container on more hosts pointed at the same bucket
— reads are stateless file serving, no coordination; one node is elected index
writer via an S3 lease and failover is automatic. The bucket must already exist.

pypiron speaks plain HTTP; terminate TLS in a reverse proxy out front. A
three-line Caddyfile is the whole story:

```caddy
pypi.acme.com {
    reverse_proxy localhost:8080
}
```

GCS and Azure backends, the full three-tier auth model, and presigned-redirect
delivery are all in [CONFIGURATION.md](docs/reference/configuration.md).

### Track installs per project

Username subaddressing tags each request with the consuming project; counts land
in Prometheus `/metrics` as `pypiron_project_requests_total{project=...}` and at
`GET /stats/downloads`:

```bash
export UV_INDEX_COMPANY_USERNAME="team+billing-api"   # password unchanged
```

## Ecosystem

Alternatives, for comparison:
[devpi-server](https://github.com/devpi/devpi),
[pypiserver](https://github.com/pypiserver/pypiserver),
[pypicloud](https://github.com/stevearc/pypicloud),
[devpi](https://www.devpi.net/).
