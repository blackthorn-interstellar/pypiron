# Authentication

pypiron has three optional basic-auth credentials, strictly ordered:
admin ⊇ uploader ⊇ reader. Each is a superset of the one below it. A request
authenticated as admin passes any uploader or reader check; an uploader passes
any reader check.

A role exists only once you set its **password**. No password, no role. There is
no config file of users and no database — the credentials are flags or env vars
on `serve`.

| Role | Flags | Grants |
| --- | --- | --- |
| admin | `--admin-user` / `--admin-pass` | publish, mirror (backdating), delete, yank, project status |
| uploader | `--uploader-user` / `--uploader-pass` | publish ordinary uploads |
| reader | `--read-user` / `--read-pass` | read indexes and artifacts |

The admin username defaults to `admin`, so `--admin-pass secret` alone is a
complete admin credential. Every flag has a `PYPIRON_*` env var — see
[Configuration](../reference/configuration.md#authentication).

## What "no credential" means

Posture follows from which passwords you set. There is no separate "make it
public" switch.

- **No write credential** (no admin, no uploader): the server is read-only.
  Open unauthenticated writes do not exist — uploads return an error instead of
  silently accepting bytes on the default `0.0.0.0` bind.
- **No read credential**: reads are public. `/simple/` and `/files/` answer
  without auth.

## When reads require auth

Set `--read-user` (with `--read-pass`) and reads close: `/simple/` and `/files/`
require basic auth. Any of the three credentials works — a reader, an uploader,
or the admin can all install. The human package pages (`/projects/` and
`/project/<pkg>/`) gate the same way; the root `/` stays public but folds in its
live activity panel only for an authorized reader.

`/health` and `/metrics` stay open regardless, so load balancers and Prometheus
scrapers never carry package credentials.

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
    lockfiles. See [Private packages](../guides/private-packages.md).

## Fail-closed by design

- **Half-configured credentials refuse startup.** Set a username without a
  password (or the reverse, including an empty `PYPIRON_*=` env var) and the
  server exits with an error. A half-set credential can never authenticate, and
  a half-set *read* credential would otherwise fail open and serve every package
  publicly.
- **Secrets compare in constant time.** Password checks are length-independent
  byte comparisons, so a wrong guess never leaks the secret one prefix-byte at a
  time. The username is not a secret.
- **Private names never fall through to upstream.** A name claimed private (or
  inside `--private-prefix`) is never proxied from a public upstream, so a
  request can't be answered by an impostor package.

## Per-project attribution

Usernames support Gmail-style subaddressing. `reader+billing-api` authenticates
as `reader` (the password is still required and still checked) and records
`billing-api` as a project tag. The `+tag` suffix is attribution, not identity.

This drives per-project download and traffic accounting without minting a
credential per team. See [Download statistics](download-stats.md).

## Privileged operations

Delete, yank, and PEP 792 project status are admin-only and live on the same
endpoints as the artifacts. The full request shapes are in the
[Management API](../reference/api.md).

```bash
# Yank a release (admin); the request body becomes the reason
curl -u admin:secret -X POST -d "broken build" \
  http://localhost:8080/files/acme-widgets/acme_widgets-1.2.0-py3-none-any.whl/yank
```

Mirroring is also admin-only: `pypiron sync` POSTs to the destination with the
admin credential. See [Mirroring](mirroring.md).
