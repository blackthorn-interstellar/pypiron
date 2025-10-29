# PypIron
<p align="center">
  <img src="docs/pypiron-logo-256.png" alt="PypIron logo" width="180"/>
</p>



A fast and reliable PyPI server built with Rust.

## Features
- No database required
- S3-backed storage (works with AWS S3 and S3-compatible services)
- Background worker for index generation
- PEP 503 and PEP 691 compliant
- Basic authentication for uploads
- Docker support

## Getting Started

### Prerequisites
1. Create an S3 bucket and credentials with permissions to read/write to that bucket
2. Set up AWS credentials (via environment variables, AWS config files, or IAM roles)

### Running with Docker

```bash
docker run --rm -it -p 8080:8080 \
  -e PYPIRON_S3_BUCKET=<my_bucket_name> \
  -e PYPIRON_BASIC_AUTH_USER=admin \
  -e PYPIRON_BASIC_AUTH_PASS=<mypassword> \
  -e AWS_SECRET_ACCESS_KEY=<my_secret_key> \
  -e AWS_ACCESS_KEY_ID=<my_access_key> \
  -e AWS_REGION=us-east-1 \
  pypiron:latest
```

### Running Locally

Build and run:
```bash
cargo build --release
./target/release/pypiron \
  --s3-bucket my-bucket \
  --basic-auth-user admin \
  --basic-auth-pass mypassword
```

### Using with pip/twine

Configure pip to use your PyPI server:
```bash
# Install from your server
pip install --index-url http://localhost:8080/simple/ mypackage

# Upload with twine
twine upload --repository-url http://localhost:8080/ \
  --username admin --password mypassword \
  dist/*
```

## Configuration

All configuration options can be set via **command line arguments** or **environment variables**. Command line arguments take precedence over environment variables.

Run `pypiron --help` to see all available options.

### Required Configuration

| CLI Argument | Environment Variable | Description |
|--------------|---------------------|-------------|
| `--s3-bucket` | `PYPIRON_S3_BUCKET` | S3 bucket name for package storage |
| `--basic-auth-user` | `PYPIRON_BASIC_AUTH_USER` | Username for upload authentication |
| `--basic-auth-pass` | `PYPIRON_BASIC_AUTH_PASS` | Password for upload authentication |

### Optional Configuration

| CLI Argument | Environment Variable | Default | Description |
|--------------|---------------------|---------|-------------|
| `--aws-region` | `AWS_REGION` | _(none)_ | AWS region (e.g., us-east-1) |
| `--s3-endpoint-url` | `PYPIRON_S3_ENDPOINT_URL` | _(none)_ | Custom S3 endpoint (for S3-compatible services) |
| `--s3-force-path-style` | `PYPIRON_S3_FORCE_PATH_STYLE` | `false` | Force S3 path-style addressing |
| `--bind-addr` | `PYPIRON_BIND_ADDR` | `0.0.0.0:8080` | Address to bind the server to |
| `--worker-interval-secs` | `PYPIRON_WORKER_INTERVAL_SECS` | `300` | Worker polling interval in seconds |
| `--job-batch-size` | `PYPIRON_JOB_BATCH_SIZE` | `20` | Number of jobs to process per batch |
| `--upload-confirm-timeout-secs` | `PYPIRON_UPLOAD_CONFIRM_TIMEOUT_SECS` | `300` | Upload confirmation timeout in seconds |
| `--public-base-url` | `PYPIRON_PUBLIC_BASE_URL` | _(none)_ | Public base URL for generating absolute URLs |

**Note:** Standard AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) are not prefixed and follow AWS SDK conventions.

### Example Configurations

**Using environment variables:**
```bash
export PYPIRON_S3_BUCKET=my-pypi-bucket
export PYPIRON_BASIC_AUTH_USER=admin
export PYPIRON_BASIC_AUTH_PASS=secret123
export AWS_REGION=us-west-2
pypiron
```

**Using command line arguments:**
```bash
pypiron \
  --s3-bucket my-pypi-bucket \
  --basic-auth-user admin \
  --basic-auth-pass secret123 \
  --aws-region us-west-2 \
  --bind-addr 127.0.0.1:8080
```

**Using S3-compatible storage (e.g., MinIO):**
```bash
pypiron \
  --s3-bucket my-bucket \
  --s3-endpoint-url http://localhost:9000 \
  --s3-force-path-style \
  --basic-auth-user admin \
  --basic-auth-pass secret123
```

**Mixed configuration (CLI overrides env vars):**
```bash
export PYPIRON_S3_BUCKET=default-bucket
export PYPIRON_BASIC_AUTH_USER=admin
export PYPIRON_BASIC_AUTH_PASS=secret123

# Override bucket via CLI
pypiron --s3-bucket production-bucket
```



## storage file structure

 - /index.json
 - /change-log/
 - /packages/
   - __index.html
   - __index.json
   - package-name/
     - __index.html
     - __index.json
     - files/
       - distribution(.whl|tar.gz)
       - distribution(.asc)
       - distribution.metadata.json



## Ecosystem
 - devpi-server
 - pypiserver
 - pypicloud
 - warehouse
 - gitlab

## useful docs
 - https://warehouse.pypa.io/api-reference/legacy.html
 - https://peps.python.org/pep-0426/
 - https://peps.python.org/pep-0503/
 - https://peps.python.org/pep-0691/
 - https://github.com/nchepanov/peps/blob/warehouse_json_api/pep-9999.rst
 - [making multi-service docker containers](https://docs.docker.com/config/containers/multi-service_container/)
 - [uwsgi-nginx docker container example](https://github.com/tiangolo/uwsgi-nginx-docker/blob/master/docker-images/python3.9.dockerfile)
