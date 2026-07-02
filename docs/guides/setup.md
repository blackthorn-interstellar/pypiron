# Setup

Run pypiron from a `pypiron.toml`, then choose how public packages arrive:
proxy on demand, or sync ahead of time.

## Create `pypiron.toml`

Generate the full template when you need it:

```bash
pypiron config init > pypiron.toml
```

Start smaller:

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
```

Start it:

```bash
export PYPIRON_ADMIN_PASS="$ADMIN"
pypiron serve
```

`private-prefix` reserves your package names. `PYPIRON_ADMIN_PASS` enables
publishing. `./pypiron.toml` is auto-loaded; use `--config` for another path.

## Private packages

Publish to `/legacy/`, install from `/simple/`.

```bash
uv publish --publish-url http://HOST:8080/legacy/ \
  --username admin --password "$PYPIRON_ADMIN_PASS" dist/*

uv add --default-index http://HOST:8080/simple/ acme-widgets
```

## Add public PyPI

Use the proxy when the server can reach PyPI. Clients keep one index. Cache
misses come from PyPI once, then stay local.

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
proxy-upstream = "https://pypi.org"

[mirror]
exclude-newer = "7 days"
```

`exclude-newer` is the default. Keeping it in the file makes the one useful
mirror policy visible: fresh releases wait a week.

Install private and public packages from the same URL:

```bash
uv add --default-index http://HOST:8080/simple/ requests acme-widgets
```

With the proxy on, do not point clients at PyPI as an extra index. pypiron owns
resolution and keeps private names private.

## Mirror with sync

Use `sync` when the server should not talk to PyPI, or when CI should install
from an approved list.

`packages.txt`:

```text
requests>=2.32,<3
urllib3
six
```

`pypiron.toml`:

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"

[mirror]
include-packages-from = "packages.txt"
exclude-newer = "7 days"
include-format = ["wheel"]
exclude-platform-tag = ["win*", "macosx_*"]
exclude-python-below = "3.9"
exclude-prereleases = true

[sync]
from = "https://pypi.org"
to = "http://localhost:8080"
package-concurrency = 8
```

Run server and sync:

```bash
export PYPIRON_ADMIN_PASS="$ADMIN"
pypiron serve --config pypiron.toml
```

```bash
export PYPIRON_SYNC_ADMIN_PASS="$ADMIN"
pypiron sync --config pypiron.toml --dry-run
pypiron sync --config pypiron.toml
```

The same `[mirror]` table drives proxy and sync. Proxy can run open; sync needs
`include-packages` or `include-packages-from`.

## Object storage

Use object storage when you want more than one node, or when the package store
should live outside the VM. The bucket must already exist.

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
storage = "s3"
s3-bucket = "acme-pypiron"
aws-region = "us-east-1"
proxy-upstream = "https://pypi.org"

[mirror]
exclude-newer = "7 days"
```

AWS credentials come from the standard AWS chain: environment, web identity,
instance role, or task role. GCS and Azure have equivalent keys in
[Configuration](../reference/configuration.md#storage).

## Run it

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
      AWS_REGION: us-east-1
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

For local disk storage, mount a data volume and set `data-dir` in `[serve]`.
For S3, run more containers with the same config behind a load balancer. Health
check: `/health`.

## Proxy or sync?

| Use | Pick | Why |
| --- | --- | --- |
| Normal private index plus cached public PyPI | proxy | No package list. Fetches on first install. |
| CI mirror with approved dependencies | sync | Pre-loads exactly what is in `packages.txt`. |
| Air-gapped serving node | sync | The server never needs egress. |

Exact flags live in [Configuration](../reference/configuration.md).
