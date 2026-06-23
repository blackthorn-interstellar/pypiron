# Private + public in one index

One URL that serves your private packages and caches everything else from PyPI on
first use. Developers configure one index instead of two. That single namespace
is also what closes the dependency-confusion hole: every name is either yours or
mirrored, never both.

This is the most common pypiron setup.

## Start the server

```bash
uvx pypiron serve --admin-pass "$ADMIN" \
  --private-prefix acme \
  --proxy-upstream https://pypi.org \
  --filter-exclude-newer "7 days"
```

| Flag | What it does |
| --- | --- |
| `--admin-pass "$ADMIN"` | Enables the admin credential (username defaults to `admin`), so you can publish. Without any write credential the server is read-only. |
| `--private-prefix acme` | Reserves the `acme-*` namespace for your uploads. Names under it never fall through to upstream. |
| `--proxy-upstream https://pypi.org` | Mirrors public packages on demand. The first request downloads, verifies, and caches the artifact in storage; it is served locally from then on, whether PyPI is up or down. |
| `--filter-exclude-newer "7 days"` | Optional. Hides releases the upstream received less than 7 days ago — a supply-chain quarantine window. The `--filter-*` flags are shared with `sync`. |

Every flag has a `PYPIRON_*` env var (`PYPIRON_ADMIN_PASS`,
`PYPIRON_PRIVATE_PREFIX`, `PYPIRON_PROXY_UPSTREAM`,
`PYPIRON_PROXY_EXCLUDE_NEWER`). See [Configuration](../reference/configuration.md).

!!! warning "Set `--private-prefix` with the proxy"
    With the proxy on and no reserved prefix, a new private upload races public
    names for the first claim. A reserved prefix closes that hole — pypiron warns
    at startup if you skip it. See
    [Supply-chain defense](../concepts/supply-chain.md).

## Point installs at the one index

Public and private packages resolve from the same index.

=== "uv"
    ```bash
    uv add --default-index http://HOST:8080/simple/ requests acme-widgets
    ```

=== "pip"
    ```bash
    pip install --index-url http://HOST:8080/simple/ requests acme-widgets
    ```

`requests` is fetched from PyPI on first use and cached; `acme-widgets` comes
from your private uploads. Both arrive over one index URL.

!!! note "Private reads"
    Reads are public unless you set a read credential. Add `--read-user $READ`
    `--read-pass …` to require auth on `/simple/` and `/files/`; `/health` and
    `/metrics` stay open. See [Authentication](../concepts/authentication.md).

## Publish a private package

Build, then upload to `/legacy/` as admin:

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

The first upload of an `acme-*` name claims it as private. After that the name is
yours; the proxy will never serve a public package of the same name.

## The supply-chain window

pypiron's `--filter-exclude-newer` and uv's own `--exclude-newer` resolve against
the same true upstream upload time, so a developer can pin even tighter than the
server:

```bash
uv add --default-index http://HOST:8080/simple/ \
  --exclude-newer "2026-01-01T00:00:00Z" requests
```

The value accepts an RFC 3339 timestamp, a friendly duration (`"7 days"`,
`"24 hours"`, `"1 week"`), or an ISO 8601 duration (`P7D`). Calendar months and
years are rejected.

The proxy honors the full set of `--filter-*` filters (wheels-only, python/abi/
platform tags, date cutoffs) — the *same* filters `sync` uses, set once in the
shared `[filter]` table. See [Configuration](../reference/configuration.md#filters).

## Next steps

- [Mirroring & proxying](../concepts/mirroring.md) — how on-demand caching works.
- [Supply-chain defense](../concepts/supply-chain.md) — origin claims and the quarantine window.
- [Configuration](../reference/configuration.md) — every flag and env var.
