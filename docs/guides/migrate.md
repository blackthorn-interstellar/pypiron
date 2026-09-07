---
description: Move private packages from devpi, Artifactory, Nexus, or pypicloud into pypiron and verify the result.
---

# Move your packages off another index

`pypiron sync --as-private` downloads selected packages from an existing index
and publishes them to a running pypiron server as your private packages.

Before starting:

- Install pypiron on the workstation that will run the migration:
  `uv tool install pypiron`.
- Start the destination with an admin password.
- List every private package you intend to move.
- Keep the old index read-only until installs from pypiron succeed.

## devpi

Use the index's `+simple` URL. Preview first:

```bash
export PYPIRON_SYNC_SOURCE_USER='devpi-user'
export PYPIRON_SYNC_SOURCE_PASS='devpi-password'
export PYPIRON_SYNC_ADMIN_USER='admin'
export PYPIRON_SYNC_ADMIN_PASS='pypiron-password'

pypiron sync \
  --from https://devpi.example.com/acme/prod/+simple \
  --to https://pypi.internal \
  --as-private \
  --include-package acme-billing \
  --include-package acme-auth \
  --dry-run
```

Check the package names, then repeat without `--dry-run`. Confirm one package:

```bash
uv pip install \
  --default-index https://pypi.internal/simple/ \
  acme-billing
```

Re-running the migration is safe: files already present are skipped.

## Artifactory and Nexus

Use the repository's Python Simple API URL:

- Artifactory: `https://HOST/artifactory/api/pypi/REPOSITORY/simple`
- Nexus: `https://HOST/repository/REPOSITORY/simple`

Run the same command used for devpi with the new `--from` URL.

The source must return the JSON Simple API. In Artifactory, enable **PyPI simple
JSON format** for the repository. Nexus supports it from version 3.93. A wrong
credential may return an HTML login page; pypiron reports that as an HTML source
instead of JSON.

The devpi path is tested end to end. Confirm JSON support on your Artifactory or
Nexus repository before moving packages.

## pypicloud: select only private projects

!!! warning "Requires a build newer than 0.0.17"
    The pypicloud-specific flags below are on `master` but not in the current
    PyPI release, 0.0.17. Use the next release when available. To run them now,
    install the Rust toolchain, clone the
    [source repository](https://github.com/blackthorn-interstellar/pypiron),
    and replace `pypiron sync` below with `cargo run --locked -- sync`.

A pypicloud index may contain your uploads and public packages cached from PyPI.
Select private project names explicitly:

```bash
export PYPIRON_SYNC_SOURCE_USER='pypicloud-user'
export PYPIRON_SYNC_SOURCE_PASS='pypicloud-password'
export PYPIRON_SYNC_ADMIN_USER='admin'
export PYPIRON_SYNC_ADMIN_PASS='pypiron-password'

pypiron sync \
  --from https://packages.example.com \
  --source-kind pypicloud \
  --as-private \
  --private-pattern 'acme-*' \
  --private-pattern 'internal-tool' \
  --to https://pypi.internal \
  --dry-run
```

Point `--from` at the pypicloud application root, not `/simple`. Check the
selected names, then repeat without `--dry-run`.

Patterns match the entire normalized name. Matching ignores case and treats
`-`, `_`, and `.` as equivalent. `*` is the only wildcard, and a bare `*` is
refused. For a longer list, save one pattern per line:

```text title="private-packages.txt"
acme-*
internal-tool
partner-sdk-*
```

Then use:

```bash
pypiron sync \
  --from https://packages.example.com \
  --source-kind pypicloud \
  --as-private \
  --private-patterns-from private-packages.txt \
  --to https://pypi.internal \
  --dry-run
```

pypicloud's uploader metadata is incomplete, so pypiron does not use it to
decide ownership. The pattern list is the ownership decision. Unmatched cached
public projects are not copied.

## Migrating a long package list

Put one requirement per line in `packages.txt`:

```text
acme-auth
acme-billing>=4
internal-tool
```

Then replace repeated `--include-package` arguments with:

```bash
--include-packages-from packages.txt
```

`sync` does not resolve dependencies. Include every private package you need;
for public packages, enable pypiron's PyPI proxy or sync a separate approved
public list.

## What migration preserves

- Artifact bytes and hashes are preserved.
- Packages are recorded as private and never fall through to public PyPI.
- Existing destination files are not overwritten.
- Source upload times and yank state are not preserved; migrated files receive
  the migration time.

A destination name already claimed from public PyPI cannot be converted in
place. Delete every file in that package and stop writes to the destination.
On a host with the destination's storage credentials, run the maintenance
command against the same storage config used by its server:

```bash
pypiron origin release PACKAGE \
  --config /etc/pypiron/pypiron.toml
```

Restart the server, then migrate the package. If the destination uses
`private-prefix`, every migrated name must match that prefix.

Use `--allow-insecure-source` only for a trusted plaintext source. Credentials
otherwise require HTTPS and are never forwarded to another host after a
redirect.

Full options: [Configuration → Sync](../reference/configuration.md#sync).
