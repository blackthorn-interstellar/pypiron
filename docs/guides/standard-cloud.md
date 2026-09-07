---
description: Deploy pypiron on S3, GCS, or Azure Blob with Docker Compose or systemd, then connect it to a load balancer.
---

# Deploy on cloud storage

Use object storage for a production server or more than one node. Before you
start, create the bucket and give the host permission to read, write, list, and
delete its objects. pypiron uses the cloud provider's normal credential chain.

## The config

Save this as `pypiron.toml`:

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
buckets = ["s3://acme-pypiron@us-east-1"]
proxy-upstream = "https://pypi.org"
```

Change `acme` to your private package prefix and change the bucket URI:

- AWS S3: `s3://bucket@region`
- Google Cloud Storage: `gs://bucket@region`
- Azure Blob Storage: `az://container@region`

The bucket must already exist. For S3, grant `s3:GetObject`, `s3:PutObject`,
`s3:DeleteObject`, `s3:ListBucket`, and `s3:AbortMultipartUpload`. GCS and Azure
need the equivalent object permissions. See
[Configuration](../reference/configuration.md#storage) for named profiles,
service-account files, and S3-compatible endpoints.

`proxy-upstream` caches public packages on demand. Remove it when this server
must not reach PyPI. New public releases wait seven days by default; your own
uploads do not.

## Run it

Set an admin password to enable publishing. With no write credential, the
server starts read-only.

### Docker Compose

Save this as `compose.yaml` beside `pypiron.toml`:

```yaml
services:
  pypiron:
    image: ghcr.io/blackthorn-interstellar/pypiron:latest
    restart: unless-stopped
    command: serve --config /etc/pypiron/pypiron.toml
    ports:
      - "8080:8080"
    environment:
      PYPIRON_ADMIN_PASS: ${PYPIRON_ADMIN_PASS}
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN:-}
      AWS_REGION: ${AWS_REGION:-us-east-1}
    volumes:
      - ./pypiron.toml:/etc/pypiron/pypiron.toml:ro
```

Start it:

```bash
export PYPIRON_ADMIN_PASS='change-this-password'
export AWS_ACCESS_KEY_ID='your-access-key'
export AWS_SECRET_ACCESS_KEY='your-secret-key'
# Export AWS_SESSION_TOKEN too when using temporary credentials.
docker compose up -d
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8080/ready
```

Pin the image to a release version before production rollout.

The example passes AWS credentials into a local Compose container. On ECS or
EKS, use a task or workload role instead and remove those four AWS entries. For
GCS or Azure, use the credential settings under
[Storage](../reference/configuration.md#credentials-by-backend).

### systemd

Download the binary for your platform from
[GitHub Releases](https://github.com/blackthorn-interstellar/pypiron/releases)
and install it at `/usr/local/bin/pypiron`. Copy the config to
`/etc/pypiron/pypiron.toml`, then save the password and any static cloud
credentials in `/etc/pypiron/env`:

```text
PYPIRON_ADMIN_PASS=change-this-password
```

Make the env file readable only by root. Then install this unit as
`/etc/systemd/system/pypiron.service`:

```ini
[Unit]
Description=pypiron
After=network-online.target
Wants=network-online.target

[Service]
DynamicUser=yes
EnvironmentFile=/etc/pypiron/env
ExecStart=/usr/local/bin/pypiron serve --config /etc/pypiron/pypiron.toml
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

Start it and check it:

```bash
sudo chmod 600 /etc/pypiron/env
sudo systemctl daemon-reload
sudo systemctl enable --now pypiron
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8080/ready
```

Success returns `{"status":"ok"}` from `/health` and `{"status":"ready"}`
from `/ready`.

## More nodes

Run more containers or services with the same config and bucket. Put them
behind a load balancer:

- Readiness check: `/ready`
- Liveness check: `/health`
- Prometheus metrics: `/metrics`

Terminate TLS at the load balancer. Do not expose `/metrics` publicly.

## Point clients at it

Publish to `/legacy/` and install from `/simple/`. Follow
[Publish and install packages](publish-install.md) for uv, pip, Poetry, and
Twine commands.

## Behind a corporate proxy

Set `HTTPS_PROXY`, `HTTP_PROXY`, and `NO_PROXY` for outbound package and
advisory downloads. If the proxy replaces TLS certificates, set
`--upstream-ca-cert /path/to/company-ca.pem`. See
[proxy configuration](../reference/configuration.md#behind-a-forward-proxy-or-tls-interception).

## Survive a region outage

One bucket protects the server process, not the bucket's region. For
cross-region or cross-cloud failover, use
[multiple buckets](multi-region.md).
