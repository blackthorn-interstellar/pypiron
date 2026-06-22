# Host private packages

Run an internal index for libraries that never touch public PyPI. One binary,
one machine, packages on local disk. Developers publish to it and install from
it with the tools they already use.

## Start the server

```bash
uvx pypiron serve --admin-pass "$ADMIN" --read-user team --read-pass "$READ"
```

That command sets up two credentials:

| Flag | Role | Grants |
| --- | --- | --- |
| `--admin-pass "$ADMIN"` | admin | publish, delete, yank |
| `--read-user team` / `--read-pass "$READ"` | read | install (`/simple/`, `/files/`) |

`--admin-user` defaults to `admin`.

`--read-user`/`--read-pass` lock down reads: with them set, `/simple/` and
`/files/` require basic auth (any of the three credentials works). Drop both
flags and reads are public. `/health` and `/metrics` stay open either way for
load balancers and Prometheus.

!!! note
    With no write credential at all, the server is read-only — there are no
    open, unauthenticated writes.

## Publish

The admin username is `admin`; the upload endpoint is `/legacy/`.

=== "uv"
    ```bash
    uv publish --publish-url http://HOST:8080/legacy/ \
      --username admin --password "$ADMIN" dist/*
    ```

=== "twine"
    ```bash
    twine upload --repository-url http://HOST:8080/legacy/ \
      --username admin --password "$ADMIN" dist/*
    ```

## Install

Point the client at `/simple/` with the read credential in the URL. Use it
alongside public PyPI, not instead of it.

=== "uv"
    ```bash
    uv add --index http://team:$READ@HOST:8080/simple/ acme-widgets
    ```

=== "pip"
    ```bash
    pip install --extra-index-url http://team:$READ@HOST:8080/simple/ acme-widgets
    ```

!!! tip
    Keep secrets out of `pyproject.toml` and lockfiles by passing credentials
    through environment variables instead of the URL — see
    [Configuration](../reference/configuration.md#authentication).

## Where packages live

On the `disk` backend (the default), artifacts and indexes live under
`--data-dir`, default `~/.pypiron/packages`:

```bash
uvx pypiron serve --admin-pass "$ADMIN" --data-dir /srv/pypiron
```

Truth is the files on disk; the `simple/` indexes are regenerable views. Back up
the directory and you've backed up the registry. For S3, GCS, or Azure instead
of local disk, see [Configuration](../reference/configuration.md#storage-serve).

## Next

Want public packages served from the same URL, so developers configure one index
instead of two? See [Private and public from one URL](private-and-public.md).
The full auth model and every storage backend are in
[Configuration](../reference/configuration.md).
