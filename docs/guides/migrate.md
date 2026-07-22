---
description: Move private packages off devpi, Artifactory, or Nexus into a self-hosted PyPI server in one command. No re-uploads, no PyPI round-trip.
---

# Move your packages off devpi, Artifactory, or Nexus

Pull your private packages out of an old index and into pypiron in one command.
They land as your own packages — private, served from your index, never fetched
from PyPI.

```bash
pypiron sync \
  --from https://devpi.corp/team/dev/+simple \
  --source-user "$SRC_USER" --source-pass "$SRC_PASS" \
  --as-private \
  --to http://localhost:8080 \
  --admin-user admin --admin-pass "$PYPIRON_ADMIN_PASS" \
  --include-package internal-app \
  --include-package internal-lib
```

Each named package is downloaded from the old index (authenticated) and
re-uploaded to pypiron as private. Install it like anything else:

```bash
uv pip install internal-app --index-url http://localhost:8080/simple/
```

## What migrating does — and doesn't

- Packages land **private**: your own packages, served from your index. Not a
  mirror of a public project — pypiron never falls through to PyPI for them.
- **Timestamps and yank state are not preserved.** Migrated files carry the
  migration date. (Mirroring public PyPI keeps real upload times; a private
  migration doesn't.)
- **Don't set `--private-prefix` during a migration.** It reserves a namespace
  for private names, and a package whose name falls outside it is refused.
  Migrate first, add the prefix after.

Re-running is safe: files already migrated are skipped, so a second pass only
carries what's new.

## devpi

devpi serves each index's package list at `<base>/<user>/<index>/+simple`. Point
`--from` at that:

```bash
pypiron sync \
  --from https://devpi.example.com/acme/prod/+simple \
  --source-user acme --source-pass "$DEVPI_PASS" \
  --as-private \
  --to http://localhost:8080 \
  --admin-user admin --admin-pass "$PYPIRON_ADMIN_PASS" \
  --include-package acme-billing --include-package acme-auth
```

## Artifactory and Nexus

Same command, different source URL. Both serve the standard simple API behind
basic auth — point `--from` at the repository's simple endpoint:

- **Artifactory:** `https://<host>/artifactory/api/pypi/<repo>/simple`
- **Nexus:** `https://<host>/repository/<repo>/simple`

```bash
pypiron sync \
  --from https://nexus.example.com/repository/pypi-internal/simple \
  --source-user "$SRC_USER" --source-pass "$SRC_PASS" \
  --as-private \
  --to http://localhost:8080 \
  --admin-user admin --admin-pass "$PYPIRON_ADMIN_PASS" \
  --include-package internal-app
```

devpi is tested end-to-end; the Artifactory and Nexus URLs above are their
standard simple-API paths.

## Migrating everything

`sync` migrates the packages you name — it won't enumerate the whole source for
you. Most teams already know their list. If you don't, read it from the source
index once:

```bash
curl -s -u "$SRC_USER:$SRC_PASS" \
  -H 'Accept: application/vnd.pypi.simple.v1+json' \
  https://devpi.example.com/acme/prod/+simple/ \
  | python3 -c 'import sys, json; print("\n".join(p["name"] for p in json.load(sys.stdin)["projects"]))' \
  > packages.txt

pypiron sync --from https://devpi.example.com/acme/prod/+simple \
  --source-user "$SRC_USER" --source-pass "$SRC_PASS" --as-private \
  --to http://localhost:8080 --admin-user admin --admin-pass "$PYPIRON_ADMIN_PASS" \
  --include-packages-from packages.txt
```

## Credentials

Keep passwords out of the command line — use the environment:
`PYPIRON_SYNC_SOURCE_USER` / `PYPIRON_SYNC_SOURCE_PASS` for the source,
`PYPIRON_SYNC_ADMIN_PASS` for pypiron. Source credentials go to the source host
only; they are never forwarded to a redirect somewhere else.

Full flag list: [Configuration → Sync](../reference/configuration.md#sync).
