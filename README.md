# <img src="docs/pypiron-logo-256.png" alt="PypIron logo" width="40" style="vertical-align: middle;"/> PypIron

A fast and reliable PyPI server built with Rust.

**Scope:** ruthlessly minimal, production‑friendly.
**Backends:** local **disk** (default) or **S3/S3‑compatible**.
**APIs:** PEP 503 (HTML) + PEP 691 (JSON).
**Uploads:** **legacy endpoint only** (`/legacy`), compatible with `uv publish` and `twine`.

## Getting Started

```bash
uvx pypiron   # runs pypiron server locally; stores data under ./pypiron-data
````

### Quick smoke test (disk backend)

```bash
# Start PypIron (basic auth required for uploads)
PYPIRON_BASIC_AUTH_USER=admin \
PYPIRON_BASIC_AUTH_PASS=secret \
uvx pypiron

# Upload an artifact (legacy multipart) with uv:
uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password secret \
  path/to/demo-0.1.0-py3-none-any.whl

# Or upload with curl (multipart form):
curl -u admin:secret -X POST http://localhost:8080/legacy/ \
  -F "content=@path/to/demo-0.1.0-py3-none-any.whl"

# Browse indexes:
open http://localhost:8080/simple/
```

## Features

* **Disk-backed** storage (default) — zero external deps
* **S3-backed** storage (AWS S3 and S3‑compatible)
* No database
* Background worker regenerates indexes
* PEP 503 (HTML) and PEP 691 (JSON)
* Basic authentication for uploads
* Docker-friendly

## Mirroring packages with `pypiron sync`

Mirror packages from the public PyPI (default) into your PypIron server.

**Create a list file (names only — no versions):**

```text
# packages.txt
requests
numpy
uvloop
```

* One name per line.
* **No versions** (the sync copies **all releases/files** for each package).
* Blank lines and `#` comments are ignored.

**Run the sync:**

```bash
# Mirror into a local PypIron running on port 8080 (with basic auth)
pypiron sync \
  --packages-list packages.txt \
  --to http://localhost:8080 \
  --username admin \
  --password secret
```

**Common filters (optional):**

* `--only-wheels` — only copy `.whl` files
* `--only-sdists` — only copy sdists (e.g. `.tar.gz`, `.zip`)
* `--python-tag` — include wheels matching python tag(s) (repeatable or comma-separated),
  e.g. `--python-tag py3`, `--python-tag cp311`
* `--abi-tag` — include wheels with ABI tag(s),
  e.g. `--abi-tag none`, `--abi-tag cp311`
* `--platform-tag` — include wheels with platform tag(s); supports `*` wildcard,
  e.g. `--platform-tag any`, `--platform-tag manylinux2014_x86_64`,
  `--platform-tag macosx_*_arm64`, `--platform-tag win_amd64`
* `--exclude-platform-tag` — exclude platform tags (supports `*`)

**Examples:**

```bash
# Pure-Python wheels for py3 on any platform
pypiron sync --packages-list packages.txt --to http://localhost:8080 \
  --only-wheels --python-tag py3 --platform-tag any

# CPython 3.11 Linux x86_64 wheels (manylinux)
pypiron sync --packages-list packages.txt --to http://localhost:8080 \
  --only-wheels --python-tag cp311 --platform-tag manylinux2014_x86_64

# macOS arm64 wheels (any macOS version)
pypiron sync --packages-list packages.txt --to http://localhost:8080 \
  --only-wheels --platform-tag macosx_*_arm64

# Only sdists (no wheels)
pypiron sync --packages-list packages.txt --to http://localhost:8080 --only-sdists
```

PypIron’s background worker will detect the uploads and regenerate indexes automatically.

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

## Running from PyPI Install

```bash
# Disk (default)
pypiron \
  --basic-auth-user admin \
  --basic-auth-pass mypassword

# S3
pypiron \
  --storage s3 \
  --s3-bucket my-bucket \
  --basic-auth-user admin \
  --basic-auth-pass mypassword
```

## Using with pip / uv / twine

```bash
# Install from your server
pip install --index-url http://localhost:8080/simple/ mypackage

# Upload with uv (recommended)
uv publish --publish-url http://localhost:8080/legacy/ \
  --username admin --password mypassword \
  dist/*.whl

# Upload with twine
twine upload --repository-url http://localhost:8080/legacy/ \
  -u admin -p mypassword \
  dist/*
```

> **Uploads are legacy-only.** PypIron expects **multipart/form-data** with the file in `content` (or `file`).
> The filename is taken from the part’s `filename`; a text field `filename` is also accepted if the part lacks it.

## Configuration

All options are available via CLI args and/or environment variables (CLI takes precedence).

### Required for uploads

| CLI Arg             | Env Var                   | Description              |
| ------------------- | ------------------------- | ------------------------ |
| `--basic-auth-user` | `PYPIRON_BASIC_AUTH_USER` | Username for upload auth |
| `--basic-auth-pass` | `PYPIRON_BASIC_AUTH_PASS` | Password for upload auth |

### Storage selection

| CLI Arg                 | Env Var                       | Default          | Description                          |
| ----------------------- | ----------------------------- | ---------------- | ------------------------------------ |
| `--storage {disk\|s3}`  | `PYPIRON_STORAGE`             | `disk`           | Select storage backend               |
| `--data-dir PATH`       | `PYPIRON_DATA_DIR`            | `./pypiron-data` | Root when using `disk`               |
| `--s3-bucket NAME`      | `PYPIRON_S3_BUCKET`           | *(required)*     | Bucket when using `s3`               |
| `--aws-region`          | `AWS_REGION`                  | *(none)*         | AWS region                           |
| `--s3-endpoint-url`     | `PYPIRON_S3_ENDPOINT_URL`     | *(none)*         | S3-compatible endpoint (e.g., MinIO) |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false`          | Force path-style addressing          |

### Server & worker

| CLI Arg                  | Env Var                        | Default        | Description                    |
| ------------------------ | ------------------------------ | -------------- | ------------------------------ |
| `--bind-addr`            | `PYPIRON_BIND_ADDR`            | `0.0.0.0:8080` | Listen address                 |
| `--worker-interval-secs` | `PYPIRON_WORKER_INTERVAL_SECS` | `5`            | Worker tick interval (seconds) |
| `--job-batch-size`       | `PYPIRON_JOB_BATCH_SIZE`       | `20`           | Jobs processed per tick        |

**AWS credentials** follow standard AWS SDK envs: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.

## Storage layout

```
/packages/<project>/<filename.whl|tar.gz>
/simple/index.html
/simple/index.json
/simple/<project>/index.html
/simple/<project>/index.json
/_internal/queue/pending/*.json
/_internal/queue/processing/*.json
```

## Ecosystem

* devpi-server
* pypiserver
* pypicloud
* warehouse
* gitlab

## References

* PEP 503 — Simple Repository API (HTML)
* PEP 691 — Simple Repository API (JSON)
* Warehouse legacy upload API
