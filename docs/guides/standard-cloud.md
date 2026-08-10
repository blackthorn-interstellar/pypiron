---
description: "Run pypiron in production on S3, GCS, or Azure: one config file, the cloud's own credentials, Docker Compose or systemd, and nodes behind a load balancer."
---

# Deploy on cloud storage

pypiron runs in production against AWS S3, GCS, or Azure Blob storage. This
page covers the config, running it under Docker Compose or systemd, scaling
out behind a load balancer, and pointing clients at it.

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

`private-prefix` claims `acme` and `acme-*` for you: with the proxy on, an
internal name nobody has published yet would otherwise be filled from public
PyPI — the prefix closes that
([a name is yours or PyPI's](../concepts.md#a-name-is-yours-or-pypis-never-both)).
`proxy-upstream` turns on the public cache: the first request for a
public package fetches it from PyPI and writes it to the bucket, so every
node serves it from then on. `exclude-newer` is the dependency cooldown — it
covers everything fetched from upstream, proxied installs and `sync` alike,
and never your own uploads. It's on by default at 7 days; the config line
just makes the window easy to change (`""` disables it).
To pre-load an approved list instead of proxying, see
[air-gapped sync](../concepts.md#air-gapped-sync-ahead-of-time).

Point `buckets` at an existing bucket. On AWS there's nothing else to set
unless you use a named profile or an S3-compatible endpoint: credentials come
from the standard chain — environment, web identity, instance role, or task
role. On EC2 or ECS with a role attached there is nothing to configure; on a
box without one, export `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` and it's
the same. GCS (`gs://`) and Azure (`az://`) work the same way, with their own
credentials: [Storage](../concepts.md#where-packages-live). Bucket policy:
`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, and
`s3:AbortMultipartUpload`. That's the whole set.

## Run it

`PYPIRON_ADMIN_PASS` enables publishing; without it the server runs
read-only — installs work, uploads are refused.

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
world-readable. On SIGTERM the server drains: `/ready` goes 503, three
seconds pass so the balancer notices, in-flight requests finish, then it
exits.

## More nodes

A node carries no state — the bucket holds everything — so scaling out is
more containers with the same config behind a load balancer. Nothing
extra to run: nodes settle concurrent work through the bucket — a second
upload of the same filename gets a clean 409, never a silent drop, and no
client reads a partial file ([how that's tested](../testing.md)).

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
hand CI a five-minute install token instead of a long-lived password:
[Who can do what](../concepts.md#who-can-do-what).

With the proxy on, do not add PyPI as an extra index: with two indexes, a
public package published under one of your names can win the resolve —
dependency confusion. pypiron already serves both, so there is nothing to
add.

## Behind a corporate proxy

If the server only reaches the internet through a corporate forward proxy, set
the standard `HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY` environment variables —
everything the server fetches from the internet honors them: proxied
packages, `sync`, and the [security advisory feed](../security.md). If that proxy
intercepts TLS with a private CA, add `--upstream-ca-cert /path/to/corp-ca.pem`
so it validates without turning verification off. Details:
[Behind a forward proxy](../reference/configuration.md#behind-a-forward-proxy-or-tls-interception).

## Survive a region outage

To survive losing the bucket's region — or the whole cloud — run the same
config against an ordered list of buckets:
[Survive a region or cloud outage](multi-region.md).
