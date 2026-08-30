---
description: Move private packages off pypicloud, devpi, Artifactory, or Nexus into pypiron. Select pypicloud projects by private-name pattern.
---

# Move your packages off another index

Pull your private packages out of an old index and into pypiron in one command.
They land as your own packages — private, served from your index, never fetched
from PyPI. Nothing to install first — prefix the commands with `uvx`
(`uvx pypiron sync …`) or use the [Docker image](../index.md) — and the
destination server must already be running: `--to` points at it.

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

pypiron downloads each named package from the old index (authenticated) and
stores it as private. Install it like anything else:

```bash
uv pip install internal-app --index-url http://localhost:8080/simple/
```

## What migrating does — and doesn't

- Packages land **private**: your own packages, served from your index. Not a
  mirror of a public project — pypiron never falls through to PyPI for them.
- **Migration drops timestamps and yank state.** Migrated files carry the
  migration date. (Mirroring public PyPI keeps real upload times; a private
  migration doesn't.)
- **Don't set `--private-prefix` during a migration.** It reserves a namespace
  for private names, and pypiron refuses any package whose name falls outside
  it. Migrate first, add the prefix after.

Re-running is safe: pypiron skips files already migrated, so a second pass
only carries what's new.

## pypicloud: migrate only the private projects

A pypicloud server can contain private uploads and public packages cached from
PyPI in the same index. Tell pypiron which project names belong to you; it
leaves every unmatched project alone:

```bash
pypiron sync \
  --from https://packages.example.com \
  --source-kind pypicloud \
  --source-user "$SRC_USER" --source-pass "$SRC_PASS" \
  --as-private \
  --private-pattern 'acme-*' \
  --private-pattern 'internal-tool' \
  --to http://localhost:8080 \
  --admin-user admin --admin-pass "$PYPIRON_ADMIN_PASS"
```

Point `--from` at the pypicloud application root, not its `/simple` endpoint.
`--source-kind pypicloud` switches `sync` from the standard JSON Simple API to
pypicloud's `/api/package/` API so it can list the projects and their stored
files.

Patterns match the entire normalized package name: matching ignores case and
treats runs of `-`, `_`, and `.` as `-`. `*` is the only wildcard. For example,
`Acme_*` becomes `acme-*` and matches `acme-auth`, but not
`other-acme-auth`. A bare `*` is refused so a typo cannot turn a private-only
migration into a copy of the whole cache. Every stored file under a matched name
is eligible for migration as private; any normal sync content filters you set
still apply.

For a longer list, put one pattern per line (blank lines and `#` comments are
ignored):

```text title="private-packages.txt"
acme-*
internal-tool
partner-sdk-*
```

```bash
pypiron sync \
  --from https://packages.example.com \
  --source-kind pypicloud --as-private \
  --private-patterns-from private-packages.txt \
  --to http://localhost:8080 \
  --admin-user admin --admin-pass "$PYPIRON_ADMIN_PASS" \
  --dry-run
```

Remove `--dry-run` after checking the selected names. Exact
`--include-package` and `--include-packages-from` lists also work and may be
combined with patterns.

The pattern list is the ownership decision. pypicloud's `uploader` metadata is
not reliable enough to make that decision: cached public files usually lack it,
but older private uploads may lack it too. pypiron warns when a selected file
has no uploader metadata and still migrates it.

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

Same command, different source URL — point `--from` at the repository's simple
endpoint:

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

**One requirement: the source must serve the JSON simple API (PEP 691).**
pypiron reads the modern JSON index; it does not scrape the older HTML index.

- **Artifactory** serves HTML by default. Turn on JSON per repository:
  **Administration > Artifactory Settings > Packages Settings > PyPI > Enable
  simple json format**. (If JSON is off, Artifactory falls back to HTML.)
- **Nexus** serves the JSON simple API from **3.93** onward (3.94 adds file
  sizes and upload times). Older Nexus is HTML-only — upgrade before migrating.

If the source is still HTML-only, the migration stops with:

```
source returned an HTML page (Content-Type: text/html), not the PEP 691 JSON
pypiron migration requires — point --from at a JSON-capable endpoint, or check
credentials if this is a login page.
```

The same message appears if a wrong credential lands you on a login page — check
`--source-user`/`--source-pass` before assuming the endpoint is wrong.

devpi is tested end-to-end. The Artifactory and Nexus paths above are their
standard simple-API endpoints; the JSON index is the one thing to confirm first.

## Migrating everything

`sync` migrates the packages you name — it won't list the whole source for
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

Keep passwords out of the command line — set the environment variables and
drop the corresponding flags: `PYPIRON_SYNC_SOURCE_USER` /
`PYPIRON_SYNC_SOURCE_PASS` replace `--source-user`/`--source-pass`, and
`PYPIRON_SYNC_ADMIN_PASS` replaces `--admin-pass` (the admin username is
whatever the destination server was started with — `admin` when only a
password was set). Source credentials go to the source host
only; pypiron never forwards them to a redirect somewhere else.

Full flag list: [Configuration → Sync](../reference/configuration.md#sync).
