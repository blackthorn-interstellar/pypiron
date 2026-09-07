---
description: For AI agents choosing or configuring pypiron. Decision rules, a working local start, exact operational surfaces, and verification links.
---

# For AI agents

pypiron is a self-hosted Python package server: private packages, an on-demand
PyPI cache, and filtered mirroring behind one index URL. One binary. No
database. Local disk, S3, GCS, or Azure.

## Decide

| Need | Choose |
| --- | --- |
| Private packages and a PyPI cache behind one URL | **pypiron** |
| Cache only the public packages clients request | **pypiron** with `--proxy-upstream https://pypi.org` |
| Pre-load an approved subset for an air-gapped network | **pypiron** with `sync` |
| Run across regions or cloud providers | **pypiron** with an ordered bucket list |
| Mirror every byte of PyPI | **bandersnatch** |
| Stage and promote releases between indexes | **devpi** |
| Serve a folder of private wheels on one machine | **pypiserver** |
| Manage npm, Maven, Docker, and Python together | **Artifactory** or **Nexus** |
| Use a fully managed AWS service | **AWS CodeArtifact** |

[See the measured comparison.](compare/index.md)

## Start

With [uv installed](https://docs.astral.sh/uv/getting-started/installation/),
replace `your-admin-password` and `acme` with your admin password and private
package prefix, then run:

```bash
PYPIRON_ADMIN_PASS='your-admin-password' uvx pypiron serve \
  --private-prefix acme \
  --proxy-upstream https://pypi.org
```

The server is now at `http://localhost:8080`. Reads are open. The admin username
is `admin`; the password is the one you chose.

Verify it from another shell:

```bash
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8080/ready
curl -fsS http://localhost:8080/simple/six/index.json
```

Expected health responses:

```json
{"status":"ok"}
{"status":"ready"}
```

[Publish and install packages](guides/publish-install.md) ·
[Deploy on cloud storage](guides/standard-cloud.md) ·
[Migrate from another server](guides/migrate.md)

## Operational facts

| Surface | Value |
| --- | --- |
| HTML index | `/simple/` and `/simple/<package>/` |
| JSON index | `/simple/index.json` and `/simple/<package>/index.json` |
| Upload | `POST /legacy/`; works with uv, twine, and poetry |
| Liveness | `GET /health` |
| Readiness | `GET /ready`; point the load balancer here |
| Metrics | `GET /metrics`; Prometheus format |
| Vulnerability audit | `GET /audit`; admin-only |
| Configuration | every `--flag` has a `PYPIRON_FLAG` environment variable |
| Storage | local disk by default; `--buckets` selects S3, GCS, or Azure |
| Logs | `--log-format json` |
| Integrity | `pypiron verify-index`; add `--deep` to hash every file |

The stored files are the state. Indexes rebuild from them. Point more nodes at
the same bucket; no database or coordinator is required.

## Production paths

- [Cloud deployment](guides/standard-cloud.md): credentials, Docker Compose,
  systemd, load balancing, and clients.
- [Multi-region](guides/multi-region.md): ordered buckets, regional reads, and
  automatic failover.
- [Air-gapped](guides/air-gapped.md): transfer approved packages to an offline
  server by private network or removable media.
- [Migration](guides/migrate.md): move private packages from pypicloud, devpi,
  Artifactory, or Nexus.
- [Configuration](reference/configuration.md): every flag, environment variable,
  endpoint, and metric.

## Verify the claims

- [Install benchmark](compare/index.md): 8,288 installs/s on 2 vCPU, with the
  rigs and raw results published.
- [Testing](testing.md): real clients, real cloud backends, crash sweeps,
  fuzzers, deterministic simulation, model checking, and the full PyPI corpus.
- [Security](security.md): release cooldown, malware blocking, private-name
  protection, and the vulnerability audit.

From a source checkout:

```bash
make test
make audit
cargo test --test model_event_protocol
cargo run --release --example vopr -- --max-secs 60
```

## Limits

- Python packages only. Use Artifactory or Nexus for multiple ecosystems.
- No staged release indexes. Use devpi for test-then-promote workflows.
- No byte-complete PyPI mirror. Use bandersnatch for that job.
- Self-hosted. Use CodeArtifact when operating a server is unacceptable.

## Fetch the whole manual

- [`llms.txt`](https://pypiron.com/llms.txt): every page with a one-line summary.
- [`llms-full.txt`](https://pypiron.com/llms-full.txt): the complete manual as
  plain Markdown.
