# Production deployment

Object storage, multiple nodes, TLS at the edge. One image, one bucket, any
number of identical containers.

## Run on object storage

Point the server at an S3 bucket instead of local disk. Below is a complete
`docker-compose.yml`:

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
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}          # on AWS, drop these two
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}  # and use the instance/task role
```

This sets an admin credential (publish, mirror, delete, yank) and a read
credential (`/simple/` and `/files/` now require auth; `/health` and `/metrics`
stay open). The admin username defaults to `admin`, so the compose sets no
`PYPIRON_ADMIN_USER`. See [Authentication](../concepts/authentication.md).

!!! note "Use the instance role on AWS"
    On EC2, ECS, or EKS, omit `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
    pypiron follows the standard AWS credential chain and picks up the
    instance/task role automatically.

!!! warning "The bucket must already exist"
    pypiron writes objects but never creates the bucket. Provision it first.

GCS and Azure use the same model with their own flags and credential chains. See
[Storage backends](../concepts/storage.md) and
[Configuration](../reference/configuration.md#storage-serve).

## Scale out

Run the same container on more hosts, all pointed at the same bucket. There is
no extra wiring.

- **Reads are stateless.** Serving an index or an artifact is file serving with
  zero coordination between nodes. Add capacity by adding containers.
- **One writer at a time.** Index rebuilds need a single author, so one node is
  elected leader through an S3 lease. Failover is automatic when the leader dies;
  the lease TTL bounds the gap.
- **Truth is the bucket.** Indexes are regenerable views over the files. A node
  holds no durable local state, so a node can be replaced at any time.

Put the nodes behind a load balancer and point its health check at `/health`
(`200` when storage answers, `503` otherwise).

## Terminate TLS at a reverse proxy

pypiron speaks plain HTTP. Run TLS in a reverse proxy in front of it. The whole
Caddyfile is three lines:

```caddy
pypi.acme.com {
    reverse_proxy localhost:8080
}
```

pypiron honors `X-Forwarded-Proto` and `X-Forwarded-Host`, so the install
snippets on its pages render with your real `https://` URL.

## Track installs per project

Username subaddressing tags each request with the consuming project. Append
`+tag` to the username; the password is unchanged.

=== "uv"

    ```bash
    export UV_INDEX_COMPANY_USERNAME="team+billing-api"
    export UV_INDEX_COMPANY_PASSWORD="$READ"
    ```

=== "pip"

    ```bash
    pip install \
      --index-url "https://team+billing-api:$READ@pypi.acme.com/simple/" \
      acme-widgets
    ```

The tag (`billing-api`) is recorded for attribution. Per-tag counts show up in
Prometheus `/metrics` as `pypiron_project_requests_total{project=...,route=...}`.
For per-package download totals, read `GET /stats/downloads` (global top
packages plus daily totals, read-auth gated).

!!! note
    Tags are restricted to `[A-Za-z0-9._-]`, capped at 64 characters, and the
    distinct-tag cardinality in `/metrics` is bounded (overflow lands in
    `_overflow`). On an open server (no read credential) the username is still
    parsed for attribution and the password is ignored.

## See also

- [Storage backends](../concepts/storage.md) — disk, S3, GCS, Azure
- [Artifact delivery](../concepts/artifact-delivery.md) — stream vs presigned
  redirect, and when each matters at scale
- [Management API](../reference/api.md) — delete, yank, project status
- [Configuration](../reference/configuration.md) — every flag and `PYPIRON_*`
  env var
