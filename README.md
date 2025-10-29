# <img src="docs/pypiron-logo-256.png" alt="PypIron logo" width="40" style="vertical-align: middle;"/> PypIron

A fast and reliable PyPI server built with Rust.

**New in this release:** PypIron can run against a **local disk** or **S3**.
The default is **disk** (no cloud required).

## Getting Started

```bash
uvx pypiron  # runs pypiron server locally; stores data in ./pypiron-data
````

### Quick smoke test (disk backend)

```bash
# Start PypIron (basic auth required for uploads)
PYPIRON_BASIC_AUTH_USER=admin \
PYPIRON_BASIC_AUTH_PASS=secret \
uvx pypiron

# Upload an artifact (simulate client)
# Note: this endpoint expects a raw body and a filename hint
curl -u admin:secret -X POST "http://localhost:8080/?filename=demo-0.1.0-py3-none-any.whl" --data-binary @path/to/wheel.whl -i

# Indexes will appear under ./pypiron-data/simple/
open http://localhost:8080/simple/
```

## Features

* **Disk-backed storage (default)** — zero external dependencies
* S3-backed storage (works with AWS S3 and S3-compatible services)
* No database required
* Background worker for index generation
* PEP 503 and PEP 691 compliant
* Basic authentication for uploads
* Docker support

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

## Running from PyPI Installation

After installing via pip:

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

## Using with pip/twine

Configure pip to use your PyPI server:

```bash
# Install from your server
pip install --index-url http://localhost:8080/simple/ mypackage

# Upload with twine (PypIron uses a redirect-based upload flow)
twine upload --repository-url http://localhost:8080/ \
  --username admin --password mypassword \
  dist/*
```

> **Note on uploads:** for now, PypIron expects a filename hint via either the `X-Filename` header or `?filename=` query string. If your client does not provide one, you can `curl` as shown above or add the header in your upload tooling.

## Configuration

All configuration options can be set via **command line arguments** or **environment variables**. Command line arguments take precedence over environment variables.

Run `pypiron --help` to see all available options.

### Required Configuration

| CLI Argument        | Environment Variable      | Description                        |
| ------------------- | ------------------------- | ---------------------------------- |
| `--basic-auth-user` | `PYPIRON_BASIC_AUTH_USER` | Username for upload authentication |
| `--basic-auth-pass` | `PYPIRON_BASIC_AUTH_PASS` | Password for upload authentication |

### Storage Selection

| CLI Argument           | Environment Variable | Default               | Description                              |
| ---------------------- | -------------------- | --------------------- | ---------------------------------------- |
| `--storage {disk\|s3}` | `PYPIRON_STORAGE`    | `disk`                | Select storage backend                   |
| `--data-dir PATH`      | `PYPIRON_DATA_DIR`   | `./pypiron-data`      | Root directory when using `disk` backend |
| `--s3-bucket NAME`     | `PYPIRON_S3_BUCKET`  | *(required for `s3`)* | S3 bucket for package storage            |

### Optional Configuration

| CLI Argument                    | Environment Variable                  | Default        | Description                                     |
| ------------------------------- | ------------------------------------- | -------------- | ----------------------------------------------- |
| `--aws-region`                  | `AWS_REGION`                          | *(none)*       | AWS region (e.g., us-east-1)                    |
| `--s3-endpoint-url`             | `PYPIRON_S3_ENDPOINT_URL`             | *(none)*       | Custom S3 endpoint (for S3-compatible services) |
| `--s3-force-path-style`         | `PYPIRON_S3_FORCE_PATH_STYLE`         | `false`        | Force S3 path-style addressing                  |
| `--bind-addr`                   | `PYPIRON_BIND_ADDR`                   | `0.0.0.0:8080` | Address to bind the server to                   |
| `--worker-interval-secs`        | `PYPIRON_WORKER_INTERVAL_SECS`        | `300`          | Worker polling interval in seconds              |
| `--job-batch-size`              | `PYPIRON_JOB_BATCH_SIZE`              | `20`           | Number of jobs to process per batch             |
| `--upload-confirm-timeout-secs` | `PYPIRON_UPLOAD_CONFIRM_TIMEOUT_SECS` | `300`          | Upload confirmation timeout in seconds          |
| `--public-base-url`             | `PYPIRON_PUBLIC_BASE_URL`             | *(none)*       | Public base URL for generating absolute URLs    |

**Note:** Standard AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) are not prefixed and follow AWS SDK conventions.

### Example Configurations

**Disk (default):**

```bash
export PYPIRON_BASIC_AUTH_USER=admin
export PYPIRON_BASIC_AUTH_PASS=secret123
pypiron  # uses ./pypiron-data
```

**Disk with custom data dir:**

```bash
pypiron --data-dir /srv/pypiron-data \
  --basic-auth-user admin --basic-auth-pass secret123
```

**S3:**

```bash
export PYPIRON_STORAGE=s3
export PYPIRON_S3_BUCKET=my-pypi-bucket
export PYPIRON_BASIC_AUTH_USER=admin
export PYPIRON_BASIC_AUTH_PASS=secret123
export AWS_REGION=us-west-2
pypiron
```

**Using S3-compatible storage (e.g., MinIO):**

```bash
pypiron \
  --storage s3 \
  --s3-bucket my-bucket \
  --s3-endpoint-url http://localhost:9000 \
  --s3-force-path-style \
  --basic-auth-user admin \
  --basic-auth-pass secret123
```

**Mixed configuration (CLI overrides env vars):**

```bash
export PYPIRON_STORAGE=s3
export PYPIRON_S3_BUCKET=default-bucket
export PYPIRON_BASIC_AUTH_USER=admin
export PYPIRON_BASIC_AUTH_PASS=secret123

# Override bucket via CLI
pypiron --s3-bucket production-bucket
```

## storage file structure

Whether on disk or in S3, the logical layout is the same (on disk this is rooted at `--data-dir`):

* /index.json
* /change-log/
* /packages/

  * __index.html
  * __index.json
  * package-name/

    * __index.html
    * __index.json
    * files/

      * distribution(.whl|tar.gz)
      * distribution(.asc)
      * distribution.metadata.json

## Ecosystem

* devpi-server
* pypiserver
* pypicloud
* warehouse
* gitlab

## useful docs

* [https://warehouse.pypa.io/api-reference/legacy.html](https://warehouse.pypa.io/api-reference/legacy.html)
* [https://peps.python.org/pep-0426/](https://peps.python.org/pep-0426/)
* [https://peps.python.org/pep-0503/](https://peps.python.org/pep-0503/)
* [https://peps.python.org/pep-0691/](https://peps.python.org/pep-0691/)
* [https://github.com/nchepanov/peps/blob/warehouse_json_api/pep-9999.rst](https://github.com/nchepanov/peps/blob/warehouse_json_api/pep-9999.rst)
* [making multi-service docker containers](https://docs.docker.com/config/containers/multi-service_container/)
* [uwsgi-nginx docker container example](https://github.com/tiangolo/uwsgi-nginx-docker/blob/master/docker-images/python3.9.dockerfile)
