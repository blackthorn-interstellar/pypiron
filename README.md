# <img src="docs/pypiron-logo-256.png" alt="PypIron logo" width="40" style="vertical-align: middle;"/> PypIron

An ultra-fast, reliable, standards-compliant PyPI server for private registries
that only serves static files. No database.

**Scope:** ruthlessly minimal, production-friendly.
**Backends:** local **disk** (default) or **S3/S3-compatible**.
**APIs:** PEP 503 (HTML) + PEP 691/700 (JSON), PEP 592 yank, PEP 658/714 metadata.
**Uploads:** legacy endpoint (`/legacy/`), compatible with `uv publish` and `twine`.

The design is a static site generator wearing a PyPI costume: truth lives in
the packages tree (immutable artifacts plus write-time metadata sidecars), and
the simple index is a materialized view, idempotently regenerable from a
storage listing. See [docs/DESIGN.md](docs/DESIGN.md) for the full reasoning.

## Getting Started

```bash
uvx pypiron   # runs pypiron locally; stores data under ~/.pypiron/packages
```

### Quick smoke test (disk backend)

```bash
# Start PypIron (basic auth required for uploads)
PYPIRON_BASIC_AUTH_USER=admin \
PYPIRON_BASIC_AUTH_PASS=secret \
uvx pypiron

# Upload an artifact with uv:
uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password secret \
  path/to/demo-0.1.0-py3-none-any.whl

# Browse indexes:
open http://localhost:8080/simple/
```

## Features

* **Disk-backed** storage (default) — zero external deps
* **S3-backed** storage (AWS S3 and S3-compatible), optional presigned
  redirects so the server never streams wheel bytes
* No database — truth is files, views are regenerable, backups are rsync
* PEP 503 (HTML) and PEP 691 (JSON) with PEP 700 fields — `uv
  --exclude-newer` works, including on mirrored packages
* PEP 658/714: wheel `METADATA` served as a static companion, so resolvers
  never download wheels just to read dependencies
* PEP 592 yank, plus deletion, with cache-correct index rebuilds
* Filename immutability (the pypi.org rule): artifacts are served
  `Cache-Control: immutable`, indexes revalidate with ETags
* Dependency-confusion defense: every package is exclusively `private` or
  `mirror`, claimed at first write; optional reserved namespace prefix
* Self-healing: a periodic reconciler regenerates anything stale, so lost
  events are harmless
* Multi-node on S3 via a sloppy leader lease (conditional writes, TTL)
* Optional synchronous uploads for publish-then-install CI pipelines

## Mirroring packages with `pypiron sync`

`sync` writes artifacts, metadata sidecars, and index markers **directly to
storage**, carrying PyPI's true upload timestamps — so `--exclude-newer`
resolves historically correct versions against your mirror.

```text
# packages.txt — one name per line, no versions (all releases are mirrored)
requests
numpy
```

```bash
# Mirror into local disk storage (the default mode)
pypiron sync --packages-list packages.txt --data-dir ~/.pypiron/packages

# Mirror into S3
pypiron sync --packages-list packages.txt \
  --storage s3 --s3-bucket my-bucket

# Push over HTTP to a remote PypIron instead (timestamps become mirror time)
pypiron sync --packages-list packages.txt \
  --to http://localhost:8080 --username admin --password secret
```

**Common filters (optional):**

* `--only-wheels` / `--only-sdists`
* `--python-tag py3,cp311` — python tag(s)
* `--abi-tag none,cp311` — ABI tag(s)
* `--platform-tag any,manylinux2014_x86_64,macosx_*_arm64` — platform tag(s), `*` wildcard
* `--exclude-platform-tag` — exclusions (supports `*`)

Mirrored names are claimed `mirror`-origin; names already claimed by private
uploads (or inside `--private-prefix`) are refused outright.

## Running with Docker

```bash
docker run --rm -it -p 8080:8080 \
  -e PYPIRON_BASIC_AUTH_USER=admin \
  -e PYPIRON_BASIC_AUTH_PASS=<mypassword> \
  pypiron:latest
```

### Switch to S3 backend (Docker)

```bash
docker run --rm -it -p 8080:8080 \
  -e PYPIRON_STORAGE=s3 \
  -e PYPIRON_S3_BUCKET=<my_bucket_name> \
  -e PYPIRON_BASIC_AUTH_USER=admin \
  -e PYPIRON_BASIC_AUTH_PASS=<mypassword> \
  -e AWS_ACCESS_KEY_ID=<my_access_key> \
  -e AWS_SECRET_ACCESS_KEY=<my_secret_key> \
  -e AWS_REGION=us-east-1 \
  pypiron:latest
```

## Using with pip / uv / twine

```bash
# Install from your server
pip install --index-url http://localhost:8080/simple/ mypackage

# Upload with uv
uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password mypassword dist/*.whl

# Upload with twine
twine upload --repository-url http://localhost:8080/legacy/ \
  -u admin -p mypassword dist/*
```

Point clients at this registry **only** (`--index-url`, never
`--extra-index-url https://pypi.org/simple` — that reopens the
dependency-confusion hole the origin system closes).

## Management API

```bash
# Delete a file (index first, then artifact — clients never see a broken link)
curl -u admin:secret -X DELETE http://localhost:8080/files/<pkg>/<filename>

# Yank / un-yank (PEP 592); request body becomes the reason
curl -u admin:secret -X POST -d "broken release" \
  http://localhost:8080/files/<pkg>/<filename>/yank
curl -u admin:secret -X DELETE http://localhost:8080/files/<pkg>/<filename>/yank
```

## Configuration

All options are available via CLI args and/or environment variables.

### Storage (shared by `serve` and `sync`)

| CLI Arg                 | Env Var                       | Default               | Description                          |
| ----------------------- | ----------------------------- | --------------------- | ------------------------------------ |
| `--storage {disk\|s3}`  | `PYPIRON_STORAGE`             | `disk`                | Select storage backend               |
| `--data-dir PATH`       | `PYPIRON_DATA_DIR`            | `~/.pypiron/packages` | Root when using `disk`               |
| `--s3-bucket NAME`      | `PYPIRON_S3_BUCKET`           | *(required for s3)*   | Bucket when using `s3`               |
| `--aws-region`          | `AWS_REGION`                  | *(none)*              | AWS region                           |
| `--s3-endpoint-url`     | `PYPIRON_S3_ENDPOINT_URL`     | *(none)*              | S3-compatible endpoint (e.g., MinIO) |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false`               | Force path-style addressing          |

### Server

| CLI Arg                      | Env Var                            | Default        | Description                                      |
| ---------------------------- | ---------------------------------- | -------------- | ------------------------------------------------ |
| `--bind-addr`                | `PYPIRON_BIND_ADDR`                | `0.0.0.0:8080` | Listen address                                   |
| `--basic-auth-user`          | `PYPIRON_BASIC_AUTH_USER`          | *(none)*       | Username for upload/management auth              |
| `--basic-auth-pass`          | `PYPIRON_BASIC_AUTH_PASS`          | *(none)*       | Password for upload/management auth              |
| `--private-prefix`           | `PYPIRON_PRIVATE_PREFIX`           | *(none)*       | Reserve a namespace for private uploads          |
| `--worker-interval-secs`     | `PYPIRON_WORKER_INTERVAL_SECS`     | `5`            | Worker tick interval                             |
| `--reconcile-interval-secs`  | `PYPIRON_RECONCILE_INTERVAL_SECS`  | `300`          | Full self-heal sweep interval                    |
| `--lease-ttl-secs`           | `PYPIRON_LEASE_TTL_SECS`           | `30`           | Leader lease TTL (multi-node S3)                 |
| `--s3-presigned-redirects`   | `PYPIRON_S3_PRESIGNED_REDIRECTS`   | `false`        | 302 artifact downloads to presigned S3 URLs      |
| `--sync-uploads`             | `PYPIRON_SYNC_UPLOADS`             | `false`        | Wait for index visibility before returning 200   |
| `--sync-upload-timeout-secs` | `PYPIRON_SYNC_UPLOAD_TIMEOUT_SECS` | `10`           | Bound on the synchronous-upload wait             |

**AWS credentials** follow standard AWS SDK envs: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.

## Storage layout

The layout is the schema — see [docs/DESIGN.md](docs/DESIGN.md):

```
packages/<pkg>/<filename>                # artifact, immutable once written
packages/<pkg>/<filename>.meta.json      # sidecar: sha256, size, version, upload-time, requires-python, yanked
packages/<pkg>/<filename>.metadata       # PEP 658 core metadata, extracted from wheel
packages/<pkg>/.origin                   # "private" | "mirror" — claimed at first write
simple/index.html                        # materialized views (regenerable)
simple/index.json
simple/<pkg>/index.html
simple/<pkg>/index.json
_dirty/<pkg>                             # empty marker: package needs index rebuild
_leader/lease.json                       # multi-node lease (holder, term, expires-at)
```

## Docs

* [VISION.md](docs/VISION.md) — the one-page version
* [DESIGN.md](docs/DESIGN.md) — architecture and reasoning
* [STANDARDS.md](docs/STANDARDS.md) — PEP support matrix
* [TESTING.md](docs/TESTING.md) — blackbox-first test philosophy
* [ROADMAP.md](docs/ROADMAP.md) — implementation history

## Ecosystem

* devpi-server
* pypiserver
* pypicloud
* warehouse
