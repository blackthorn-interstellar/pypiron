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
  --exclude-newer '' \
  --include-package acme-billing \
  --include-package acme-auth \
  --dry-run
```

`--exclude-newer ''` includes uploads from the last seven days, which the
default public-release cooldown would otherwise skip. Check the package names,
then repeat without `--dry-run`. Confirm one package:

```bash
uv pip install \
  --default-index https://pypi.internal/simple/ \
  acme-billing
```

Re-running the migration is safe: files already present are skipped. To correct
dates from an earlier migration, use [Repair upload dates](#repair-upload-dates).

## Artifactory and Nexus

Use the repository's Python Simple API URL:

- Artifactory: `https://HOST/artifactory/api/pypi/REPOSITORY/simple`
- Nexus: `https://HOST/repository/REPOSITORY/simple`

Run the same command used for devpi with the new `--from` URL.

For Artifactory and Nexus, the source must return the JSON Simple API. In
Artifactory, enable **PyPI simple JSON format** for the repository. Nexus supports it from version 3.93. A wrong
credential may return an HTML login page; pypiron reports that as an HTML source
instead of JSON.

The devpi path is tested end to end. Confirm JSON support on your Artifactory or
Nexus repository before moving packages.

## pypicloud: select only private projects

!!! note "Use pypiron 0.0.23 or newer"
    Upgrade both the sync client and destination server to preserve original
    upload dates and repair dates from earlier migrations.

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
  --exclude-newer '' \
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
  --exclude-newer '' \
  --private-patterns-from private-packages.txt \
  --to https://pypi.internal \
  --dry-run
```

pypicloud's uploader metadata is incomplete, so pypiron does not use it to
decide ownership. The pattern list is the ownership decision. Unmatched cached
public projects are not copied.

Give the destination server the same list. In its `pypiron.toml`:

```toml
private-patterns = ["acme-*", "internal-tool"]
private-patterns-from = "private-packages.txt"
```

Those names are then reserved on the server: they are never fetched from PyPI
or accepted from a mirror, and new private uploads must match the list. Put
the list in place before enabling `proxy-upstream`, or the first install of an
unclaimed name would claim it as public. See
[Reserved private names](../reference/configuration.md#reserved-private-names).

### Keep the old hostname

pypicloud publishers post to `/simple/`, and `uv publish` posts to `/` when
its publish URL has no path. pypiron accepts uploads on both, as well as on
`/legacy/`, so you can move the hostname to pypiron without editing every
publisher. Point new publishers at `/legacy/`.

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
- Source upload dates are preserved when available. For pypicloud, these are
  the per-file `last_modified` dates reported by its package API. Missing dates
  are reported and use the migration time; invalid dates fail the package.
- Yank state is not preserved.

### Repair upload dates

If an earlier migration gave every file the migration date, upgrade **both the
sync client and destination server to 0.0.23 or newer**. Keep the old server
available as the date source.

For any supported source, add `--repair-upload-times --dry-run` to the original
`sync --as-private` command. For pypicloud, using the credentials and private
project selection above, preview from your migration workstation:

```bash
pypiron sync \
  --from https://packages.example.com \
  --source-kind pypicloud \
  --as-private \
  --private-pattern 'acme-*' \
  --private-pattern 'internal-tool' \
  --to https://pypi.internal \
  --exclude-newer '' \
  --repair-upload-times \
  --dry-run
```

Check the filenames and proposed dates, then repeat without `--dry-run`.
`--exclude-newer ''` includes recent uploads that the default seven-day cutoff
would otherwise skip. Existing file-selection filters still apply.

The repair changes only upload dates on existing private files. It compares
SHA-256 hashes first and refuses mismatches. If pypicloud has no stored hash,
it downloads the source file to compute one, including during a dry run.
Missing source dates leave the destination unchanged and produce a warning.
Files absent from the destination are reported and skipped; nothing is uploaded
or deleted. Repeating the repair is safe.

After the indexes refresh, check the dates on the destination's
`/project/PACKAGE/` page, replacing `PACKAGE` with a migrated project name.
These are the dates used by `uv --exclude-newer`. Filesystem modification times
and cloud object `Last-Modified` values remain storage timestamps.

## Resolve an ownership conflict

A destination name already claimed from public PyPI cannot be converted in
place. Delete every file in that package and stop writes to the destination.
On a host with the destination's storage credentials, run the maintenance
command against the same storage config used by its server:

```bash
pypiron origin release PACKAGE \
  --config /etc/pypiron/pypiron.toml
```

Restart the server, then migrate the package. If the destination reserves
private names (`private-prefix` or `private-patterns`), every migrated name
must be a reserved one.

Use `--allow-insecure-source` only for a trusted plaintext source. Credentials
otherwise require HTTPS and are never forwarded to another host after a
redirect.

Full options: [Configuration → Sync](../reference/configuration.md#sync).
