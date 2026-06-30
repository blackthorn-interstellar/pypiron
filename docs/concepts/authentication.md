# Authentication

Reads are open by default — anyone who can reach the server can install. To
accept uploads, set an admin password; that one credential publishes, mirrors,
and manages releases. Credentials are flags or environment variables — no user
database to run.

```bash
pypiron serve --admin-pass secret
```

That's the whole setup for a private team: open installs, password-gated
publishing. Need more separation — CI that only publishes, or login-gated
installs? Reach for the three roles below.

## Roles

admin can do everything an uploader can, and an uploader everything a reader can.
A role exists only once you set its **password** — no password, no role.

| Role | Use it for | Grants |
| --- | --- | --- |
| **admin** | operators, and CI that mirrors or manages releases | publish, mirror, delete, yank, project status |
| **uploader** | CI that should only publish, never manage | publish ordinary uploads |
| **reader** | optional — only when you want installs to require a login | read indexes and artifacts |

The admin username defaults to `admin`, so `--admin-pass secret` is a complete
admin credential. Every role has a username/password pair — a flag with a
matching `PYPIRON_*` env var; see
[Configuration](../reference/configuration.md#authentication) for the full list.

## What "no credential" means

Your posture follows from which passwords you set — no separate "make it public"
switch.

- **No write credential** (no admin, no uploader): the server is read-only.
  Uploads return an error rather than silently accepting bytes, so an open
  `0.0.0.0` bind never becomes an open write target.
- **No read credential**: installs are public. `/simple/` and `/files/` answer
  without auth.

## When reads require auth

Set a reader password (`--read-user` with `--read-pass`) and installs close:
`/simple/` and `/files/` require basic auth, and any of the three credentials —
reader, uploader, or admin — can install. The package pages (`/projects/` and
`/project/<pkg>/`) gate the same way. The root `/` stays public but shows its
live activity panel only to an authorized reader.

`/health` and `/metrics` stay open regardless, so load balancers and Prometheus
scrapers never need package credentials.

Install against a read-gated server by putting the credential in the index URL:

=== "uv"

    ```bash
    uv pip install \
      --index-url http://$READ:secret@localhost:8080/simple/ \
      acme-widgets
    ```

=== "pip"

    ```bash
    pip install \
      --index-url http://$READ:secret@localhost:8080/simple/ \
      acme-widgets
    ```

!!! tip
    uv reads credentials from `UV_INDEX_<NAME>_USERNAME` /
    `UV_INDEX_<NAME>_PASSWORD` so the secret stays out of the URL and out of
    lockfiles. See [Deploy](../guides/deploy.md#private-packages).

## Fail-closed by design

- **Half-configured credentials refuse startup.** Set a username without a
  password (or the reverse, including an empty `PYPIRON_*=` env var) and the
  server exits with an error — a half-set credential can never authenticate, and
  a half-set *read* credential would otherwise fall open and serve every package
  publicly.
- **Secrets compare in constant time**, so a password can't be guessed by timing
  the response. The username is not a secret.
- **Private names never fall through to upstream.** A name that's yours (or
  inside `--private-prefix`) is never proxied from a public upstream, so a
  request can't be answered by an impostor package.

## Per-project attribution

Usernames support tags. `reader+billing-api` authenticates as `reader` (the
password is still required and checked) and records `billing-api` as a project
tag — the `+tag` suffix is attribution, not identity.

Per-project download and traffic accounting without minting a credential per
team. See [Download statistics](download-stats.md).

## Privileged operations

Delete, yank, and project status are admin-only, on the same endpoints as the
artifacts. Full request shapes in the [Management API](../reference/api.md).

```bash
# Yank a release (admin); the request body becomes the reason
curl -u admin:secret -X POST -d "broken build" \
  http://localhost:8080/files/acme-widgets/acme_widgets-1.2.0-py3-none-any.whl/yank
```

Mirroring is also admin-only: `pypiron sync` POSTs to the destination with the
admin credential. See [Mirroring](mirroring.md).
