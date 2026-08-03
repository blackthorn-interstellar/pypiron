---
description: "Run pypiron in production on S3, GCS, or Azure: one config file, the cloud's own credentials, Docker Compose or systemd, and nodes behind a load balancer."
---

# Deploy on cloud storage

Private packages and cached public PyPI from one URL, on one config file and
one bucket. A node carries no state — the bucket holds the packages — so you
add or replace nodes freely.

## The config

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
buckets = ["s3://acme-pypiron@us-east-1"]
proxy-upstream = "https://pypi.org"

[mirror]
exclude-newer = "7 days"
```

`private-prefix` reserves a namespace: new private names must match `acme` or
`acme-*`, and names already claimed are untouched — adopting a prefix later
never renames anything. `proxy-upstream` serves public packages on demand: a
cache miss comes from PyPI once, then stays local. `exclude-newer` is the
dependency cooldown: fresh upstream releases wait a week, on by default — your
own uploads are never delayed. Spelled out here so it's easy to change.
To pre-load an approved list instead of proxying, see
[air-gapped sync](../concepts.md#air-gapped-sync-ahead-of-time).

Point `buckets` at an existing bucket. On AWS there's usually nothing else to
set: credentials come from the standard chain — environment, web identity,
instance role, or task role. GCS (`gs://`) and Azure (`az://`) work the same
way, with their own credentials: [Storage](../concepts.md#where-packages-live).
The node reads, writes, lists, and deletes objects and uses multipart uploads
for large files — on AWS that's `s3:GetObject`, `s3:PutObject`,
`s3:DeleteObject`, `s3:ListBucket`, and `s3:AbortMultipartUpload` on the
bucket.

## Run it

`PYPIRON_ADMIN_PASS` enables publishing.

### Docker Compose

```yaml
services:
  pypiron:
    image: ghcr.io/blackthorn-interstellar/pypiron:latest  # pin a release tag in production
    restart: unless-stopped
    command: serve --config /etc/pypiron/pypiron.toml
    ports:
      - "8080:8080"
    environment:
      PYPIRON_ADMIN_PASS: ${PYPIRON_ADMIN_PASS}
    volumes:
      - ./pypiron.toml:/etc/pypiron/pypiron.toml:ro
```

### systemd

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

Put `PYPIRON_ADMIN_PASS=...` in `/etc/pypiron/env`, mode 600 — unit files are
world-readable. `DynamicUser` runs the server without a fixed account.

## More nodes

Run more containers with the same config behind a load balancer. Nodes
coordinate through the bucket itself; concurrent publishes and cache fills
converge without a coordinator ([how that's tested](../testing.md)).

Point the load balancer's health check at `/ready`: it turns 503 when a node
starts draining or can't reach storage, so traffic leaves it before requests
fail. Restart policies (Kubernetes liveness) belong on `/health`, which stays
200 while the process is alive — a storage blip should drain a node, not
restart it. Every node serves Prometheus metrics at `/metrics`. Terminate TLS
at the balancer freely: index and file URLs are relative, so nothing needs
configuring.

## Point clients at it

Publish to `/legacy/`, install from `/simple/` — private and public packages
from the same URL:

```bash
uv publish --publish-url http://HOST:8080/legacy/ \
  --username admin --password "$PYPIRON_ADMIN_PASS" dist/*

uv add --default-index http://HOST:8080/simple/ requests acme-widgets
```

pip works the same: `pip install --index-url http://HOST:8080/simple/
acme-widgets` (plain-http hosts need pip's `--trusted-host HOST`). The admin
password is for bootstrap; day to day, publish with an uploader credential and
hand CI five-minute install tokens instead of secrets:
[Who can do what](../concepts.md#who-can-do-what).

With the proxy on, do not point clients at PyPI as an extra index. pypiron
serves both and keeps private names private.

## Behind a corporate proxy

If the server only reaches the internet through a corporate forward proxy, set
the standard `HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY` environment variables — the
proxy upstream, `sync`, and the advisory feed all honor them. If that proxy
intercepts TLS with a private CA, add `--upstream-ca-cert /path/to/corp-ca.pem`
so it validates without turning verification off. Details:
[Behind a forward proxy](../reference/configuration.md#behind-a-forward-proxy-or-tls-interception).

## Survive a region outage

One bucket rides out any node dying. To survive losing the bucket's region —
or a whole cloud — give every node the same ordered list of buckets and
pypiron keeps them in sync and fails over on its own:
[Survive a region or cloud outage](multi-region.md).
