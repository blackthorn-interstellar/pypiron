---
description: Run pypiron in production on S3, GCS, or Azure: one config file, the cloud's own credentials, Docker Compose or systemd, and nodes behind a load balancer.
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

`private-prefix` reserves your package names. `proxy-upstream` serves public
packages on demand: a cache miss comes from PyPI once, then stays local.
`exclude-newer` is the dependency cooldown: fresh releases wait a week, on by
default. Keeping it in the file makes the one useful mirror policy visible.
To pre-load an approved list instead of proxying, see
[Package sources](../concepts.md#three-sources-one-index).

Point `buckets` at a bucket that already exists. On AWS there is usually
nothing else to set: credentials come from the standard chain — environment,
web identity, instance role, or task role. GCS (`gs://`) and Azure (`az://`)
are the same shape with their own credentials:
[Storage](../concepts.md#where-packages-live).

## Run it

`PYPIRON_ADMIN_PASS` enables publishing.

### Docker Compose

```yaml
services:
  pypiron:
    image: ghcr.io/blackthorn-interstellar/pypiron:latest
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
Environment=PYPIRON_ADMIN_PASS=change-me
ExecStart=/usr/local/bin/pypiron serve --config /etc/pypiron/pypiron.toml
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

## More nodes

Run more containers with the same config behind a load balancer. Point the
load balancer's health check at `/ready`: it turns 503 the moment a node
starts draining, so the balancer stops sending it traffic before shutdown.

## Point clients at it

Publish to `/legacy/`, install from `/simple/` — private and public packages
from the same URL:

```bash
uv publish --publish-url http://HOST:8080/legacy/ \
  --username admin --password "$PYPIRON_ADMIN_PASS" dist/*

uv add --default-index http://HOST:8080/simple/ requests acme-widgets
```

With the proxy on, do not point clients at PyPI as an extra index. pypiron
owns resolution and keeps private names private.

## Behind a corporate proxy

If the server only reaches the internet through a corporate forward proxy, set
the standard `HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY` environment variables — the
proxy upstream, `sync`, and the advisory feed all honor them. If that proxy
intercepts TLS with a private CA, add `--upstream-ca-cert /path/to/corp-ca.pem`
so it validates without turning verification off. Details:
[Behind a forward proxy](../reference/configuration.md#behind-a-forward-proxy-or-tls-interception).

## Survive a region outage

One bucket rides out any node dying. To ride out the bucket's region — or a
whole cloud — give every node the same ordered list of buckets and pypiron
keeps them in sync and fails over on its own:
[Survive a region or cloud outage](multi-region.md).
