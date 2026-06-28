# Deploy

One binary, no database. Pick a topology, set a few flags, and run it on
whatever you already use. Almost everyone lands on one of the setups below.

## Pick your setup

- **Private packages only** — an internal index for libraries that never touch
  public PyPI. Developers publish to it and install from it. → [Private
  packages](#private-packages)
- **Private + public from one URL** — the same index also caches public PyPI on
  demand, so developers configure one index instead of two. *Most common.* →
  [Add public PyPI](#add-public-pypi)
- **No outbound internet** — the serving node can't reach PyPI, so you pre-load
  an allowlist with `pypiron sync` from a host that can. →
  [Air-gapped mirror](air-gapped-mirror.md)

All three are the same `serve` process; they differ only by a couple of flags.
Set them however you like — **most `--flag`s are also a `PYPIRON_*` env var and a
`pypiron.toml` key** (precedence: CLI/env > file > defaults). The exceptions stay
on CLI or env, never the file: **credentials** (the read/admin/uploader
user + password pairs) and the Azure access key.

## Private packages

An internal index on local disk. The admin password is the only thing you must
set — it enables uploads. Without a write credential the server is read-only;
there are no open, unauthenticated writes.

=== "pypiron.toml"

    ```toml
    [serve]
    bind-addr = "0.0.0.0:8080"
    # credentials are CLI/env only, never the file:
    #   PYPIRON_ADMIN_PASS                          enables admin (publish)
    #   PYPIRON_READ_USER + PYPIRON_READ_PASS       optional: require auth on reads
    ```

=== "CLI"

    ```bash
    pypiron serve \
      --admin-pass "$ADMIN" \
      --read-user team --read-pass "$READ"   # optional: require auth on reads
    ```

=== "env"

    ```bash
    export PYPIRON_ADMIN_PASS="$ADMIN"
    export PYPIRON_READ_USER=team             # optional: require auth on reads
    export PYPIRON_READ_PASS="$READ"
    pypiron serve
    ```

| Credential | Role | Grants |
| --- | --- | --- |
| `--admin-pass` (user defaults to `admin`) | admin | publish, mirror, delete, yank |
| `--read-user` / `--read-pass` | read | install — when set, `/simple/` and `/files/` require auth |

Drop the read credential and reads are public; `/health` and `/metrics` stay
open either way for probes. Full model: [Authentication](../concepts/authentication.md).

Artifacts and the regenerable `simple/` indexes live under `--data-dir`
(default `~/.pypiron/packages`). Truth is the files on disk — back up that
directory and you've backed up the registry.

## Add public PyPI

Mirror public packages on demand from the same URL. The first request for a
public name downloads, verifies, and caches the artifact; it's served locally
from then on, whether PyPI is up or down. That single namespace is also what
closes the dependency-confusion hole — every name is either yours or mirrored,
never both. **This is the most common pypiron setup.** Add two flags to the
private-index config above:

=== "pypiron.toml"

    ```toml
    private-prefix = "acme"     # reserve the acme-* namespace for your uploads

    [serve]
    bind-addr = "0.0.0.0:8080"
    proxy-upstream = "https://pypi.org"
    # admin password via env: PYPIRON_ADMIN_PASS

    [mirror]
    exclude-newer = "7 days"    # supply-chain quarantine (this is the default; "" disables)
    ```

=== "CLI"

    ```bash
    pypiron serve \
      --admin-pass "$ADMIN" \
      --private-prefix acme \
      --proxy-upstream https://pypi.org \
      --exclude-newer "7 days"            # quarantine window (the default; "" disables)
    ```

=== "env"

    ```bash
    export PYPIRON_ADMIN_PASS="$ADMIN"
    export PYPIRON_PRIVATE_PREFIX=acme
    export PYPIRON_PROXY_UPSTREAM=https://pypi.org
    export PYPIRON_EXCLUDE_NEWER="7 days"  # quarantine window (the default; "" disables)
    pypiron serve
    ```

| Flag | What it does |
| --- | --- |
| `--private-prefix acme` | Reserves `acme-*` for your uploads; those names never fall through to upstream. |
| `--proxy-upstream https://pypi.org` | Mirrors public packages on demand, cached after first use. |
| `--exclude-newer "7 days"` | On by default (a sliding 7-day window). Hides releases upstream received less than 7 days ago — a quarantine `uv --exclude-newer` resolves against too. Set `--exclude-newer ""` to disable. |

!!! warning "Set `--private-prefix` with the proxy"

    With the proxy on and no reserved prefix, a new private upload races public
    names for the first claim. A reserved prefix closes that hole — pypiron warns
    at startup if you skip it. See [Supply-chain defense](../concepts/supply-chain.md).

The proxy honors the full `[mirror]` selection — formats, python/abi/platform
tags, package denies, date cutoffs — the same slice `pypiron sync` uses. Set it
once and both paths agree. See [Mirror selection](../reference/configuration.md#mirror-selection).

## Run it on your platform

The settings above are the *what*; this is the *how*. Same config, every
launcher — pick yours. Examples use the disk backend and the admin password from
the environment; add your scenario's flags and, for more than one replica, swap
in object storage ([below](#object-storage)).

=== "Binary / systemd"

    Run it directly with `PYPIRON_*` in the environment:

    ```bash
    pypiron serve
    ```

    Or as a systemd unit at `/etc/systemd/system/pypiron.service`:

    ```ini
    [Unit]
    Description=pypiron
    After=network-online.target
    Wants=network-online.target

    [Service]
    ExecStart=/usr/local/bin/pypiron serve
    EnvironmentFile=/etc/pypiron.env      # PYPIRON_ADMIN_PASS=…, etc.
    Environment=PYPIRON_DATA_DIR=/var/lib/pypiron
    Environment=PYPIRON_SPOOL_DIR=/var/lib/pypiron   # real disk: DynamicUser gives a private, maybe-tmpfs /tmp
    DynamicUser=yes                       # unprivileged, no account to manage
    StateDirectory=pypiron                # creates/owns /var/lib/pypiron
    Restart=on-failure

    [Install]
    WantedBy=multi-user.target
    ```

    ```bash
    systemctl enable --now pypiron
    ```

=== "Docker"

    `pypiron` is the entrypoint and a bare run serves. Storage defaults to
    `/data`, port `8080` is exposed, and the image ships a built-in `HEALTHCHECK`.

    ```bash
    docker run -d --name pypiron -p 8080:8080 \
      -v pypiron-data:/data \
      -e PYPIRON_ADMIN_PASS="$ADMIN" \
      ghcr.io/blackthorn-interstellar/pypiron:latest
    ```

    The image is minimal (distroless, or `scratch` on the two smallest arches,
    386 and arm/v6), unprivileged (uid 65532), and multi-arch (amd64, arm64,
    arm/v7, ppc64le, s390x, riscv64, 386, arm/v6) — Docker picks the right one. If
    `/tmp` is RAM-backed tmpfs, point the upload spool at the data volume (real
    disk, already writable by the nonroot uid) so large wheels don't spool into
    memory: `-e PYPIRON_SPOOL_DIR=/data`.

=== "Docker Compose"

    ```yaml
    services:
      pypiron:
        image: ghcr.io/blackthorn-interstellar/pypiron:latest
        ports: ["8080:8080"]
        volumes: ["pypiron-data:/data"]
        environment:
          PYPIRON_ADMIN_PASS: ${ADMIN}
          # PYPIRON_PROXY_UPSTREAM: https://pypi.org   # add your scenario's flags
        restart: unless-stopped
    volumes:
      pypiron-data:
    ```

=== "Kubernetes"

    A single-replica Deployment on a PersistentVolumeClaim. For more than one
    replica, drop the PVC and use object storage ([below](#object-storage)) —
    shared storage is what makes nodes interchangeable.

    ```yaml
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: pypiron
    spec:
      replicas: 1
      selector:
        matchLabels: { app: pypiron }
      template:
        metadata:
          labels: { app: pypiron }
        spec:
          securityContext:
            runAsNonRoot: true
            runAsUser: 65532
            fsGroup: 65532              # let the nonroot uid write the volume
          containers:
            - name: pypiron
              image: ghcr.io/blackthorn-interstellar/pypiron:latest
              ports: [{ containerPort: 8080 }]
              env:
                - name: PYPIRON_ADMIN_PASS
                  valueFrom:
                    secretKeyRef: { name: pypiron, key: admin-pass }
              volumeMounts:
                - { name: data, mountPath: /data }
              livenessProbe:
                httpGet: { path: /health, port: 8080 }
              readinessProbe:
                httpGet: { path: /health, port: 8080 }
          volumes:
            - name: data
              persistentVolumeClaim: { claimName: pypiron }
    ---
    apiVersion: v1
    kind: Service
    metadata:
      name: pypiron
    spec:
      selector: { app: pypiron }
      ports: [{ port: 80, targetPort: 8080 }]
    ```

    Create the `pypiron` Secret and PVC first — neither is auto-provisioned:

    ```bash
    kubectl create secret generic pypiron --from-literal=admin-pass="$ADMIN"
    # plus a ReadWriteOnce PVC named `pypiron`, sized for your packages,
    # with your cluster's storageClassName.
    ```

    The container's own `HEALTHCHECK` is for Docker; Kubernetes uses the
    `httpGet /health` probes above (both endpoints are unauthenticated).

=== "Helm"

    There's no official chart — pypiron is one stateless container, so a generic
    app chart covers it cleanly. With
    [bjw-s `app-template`](https://bjw-s-labs.github.io/helm-charts/) or similar:
    point the image at `ghcr.io/blackthorn-interstellar/pypiron`, expose `8080`,
    set the `PYPIRON_*` env, and add `/health` probes — the **Kubernetes** tab is
    the shape to template. A bucket-backed setup ([below](#object-storage)) is the
    natural fit, since it lets replicas scale freely.

## Publish and install

The same loop regardless of scenario. Build your distributions to `dist/`,
publish to `/legacy/` as admin, install from `/simple/`. Replace `HOST:8080`
with your server's URL.

**Publish:**

=== "uv"

    ```bash
    uv publish --publish-url http://HOST:8080/legacy/ \
      --username admin --password "$ADMIN" dist/*
    ```

=== "twine"

    ```bash
    twine upload --repository-url http://HOST:8080/legacy/ \
      -u admin -p "$ADMIN" dist/*
    ```

The first upload of an `acme-*` name claims it as private; after that the proxy
will never serve a public package of the same name.

**Install** — with the proxy on, this one index resolves both public and private
names:

=== "uv"

    ```bash
    uv add --default-index http://HOST:8080/simple/ requests acme-widgets
    ```

=== "pip"

    ```bash
    pip install --index-url http://HOST:8080/simple/ requests acme-widgets
    ```

Running **private-only** (no proxy)? Use the index *alongside* public PyPI
instead — `uv add --index …` / `pip install --extra-index-url …`. If you set a
read credential, put it in the URL (`http://team:$READ@HOST:8080/simple/`) or
your client config; keep secrets out of `pyproject.toml` and lockfiles with
[environment variables](../reference/configuration.md#authentication).

## Going to production

Object storage, multiple nodes, TLS at the edge — the cross-cutting concerns
once you're past one box on local disk.

### Object storage

Point the server at a bucket instead of local disk — the prerequisite for
running more than one node. The bucket **must already exist**; pypiron writes
objects but never creates the bucket.

=== "pypiron.toml"

    ```toml
    [serve]
    storage = "s3"
    s3-bucket = "my-pypiron"
    # AWS_REGION and credentials come from the standard AWS env / instance role
    ```

=== "CLI"

    ```bash
    AWS_REGION=us-east-1 pypiron serve --storage s3 --s3-bucket my-pypiron
    # AWS credentials via env or the instance/task role
    ```

=== "env"

    ```bash
    export PYPIRON_STORAGE=s3
    export PYPIRON_S3_BUCKET=my-pypiron
    export AWS_REGION=us-east-1
    pypiron serve
    ```

=== "Docker Compose"

    ```yaml
    services:
      pypiron:
        image: ghcr.io/blackthorn-interstellar/pypiron:latest
        command: serve --storage s3 --s3-bucket my-pypiron
        ports: ["8080:8080"]
        environment:
          PYPIRON_ADMIN_PASS: ${ADMIN}
          AWS_REGION: us-east-1
          AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}          # omit on AWS and use
          AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}  # the instance/task role
    ```

On EC2, ECS, or EKS, omit the access keys — pypiron follows the standard AWS
credential chain and picks up the instance/task role automatically. GCS and
Azure use the same model with their own flags and credential chains. See
[Storage backends](../concepts/storage.md) and
[Configuration](../reference/configuration.md#storage-serve).

### Scale out

Run the same container on more hosts, all pointed at the one bucket. There is no
extra wiring.

- **Reads are stateless.** Serving an index or artifact is file serving with
  zero coordination between nodes. Add capacity by adding containers.
- **One writer at a time.** Index rebuilds need a single author, so one node is
  elected leader through an S3 lease. Failover is automatic when the leader dies,
  bounded by the lease TTL.
- **Truth is the bucket.** Indexes are regenerable views over the files; a node
  holds no durable local state, so any node can be replaced at any time.

Put the nodes behind a load balancer and point its health check at `/health`
(`200` when storage answers, `503` otherwise).

### TLS at the edge

pypiron speaks plain HTTP. Terminate TLS in a reverse proxy in front of it. The
whole Caddyfile is three lines:

```caddy
pypi.acme.com {
    reverse_proxy localhost:8080
}
```

pypiron honors `X-Forwarded-Proto` and `X-Forwarded-Host`, so its pages render
the install snippets with your real `https://` URL.

### Track installs per project

Username subaddressing tags each request with the consuming project — append
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

Per-tag counts land in Prometheus `/metrics` as
`pypiron_project_requests_total{project=…,route=…}`; per-package, per-version
totals at `GET /stats/downloads/<pkg>` (and the global leaderboard at
`GET /stats/downloads`). Tags are restricted to `[A-Za-z0-9._-]`, capped at 64
chars, and cardinality-bounded. See [Download statistics](../concepts/download-stats.md).

## See also

- [Storage backends](../concepts/storage.md) — disk, S3, GCS, Azure
- [Artifact delivery](../concepts/artifact-delivery.md) — stream vs presigned
  redirect, and when each matters at scale
- [Authentication](../concepts/authentication.md) — the full credential model
- [Management API](../reference/api.md) — delete, yank, project status
- [Configuration](../reference/configuration.md) — every flag and `PYPIRON_*`
  env var
