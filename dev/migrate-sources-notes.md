# Migration source-layout notes (Artifactory / Nexus HTML vs. JSON)

Contributor notes — **not** part of the mkdocs site. The user-facing fix is a
paste-ready patch for `docs/guides/migrate.md` at the bottom of this file. Do not
apply it here; `migrate.md` is being edited concurrently.

## The problem

`pypiron sync --from <source>` fetches each package's listing with
`Accept: application/vnd.pypi.simple.v1+json` and then runs `serde_json` on any
`200` body, with no content-type guard (`src/simple.rs`,
`read_index_capped`). devpi emits PEP 691 JSON, so the only tested source works.
But **stock Artifactory and Nexus serve the HTML PEP 503 simple API by default**,
and a fat-fingered credential returns a `200` HTML login page. Following the
migrate guide verbatim against either then died with:

```
ERROR pypiron::sync: package sync failed package=internal-app error=expected value at line 1 column 1
```

`expected value at line 1 column 1` is the raw serde error for "this is not
JSON" — opaque, and it reads to an SRE as a pypiron bug, not a source
misconfiguration. Worse, the summary line still prints `0 files ... 0 errors`, so
a silent `200` HTML login page looked like a successful no-op migration.

## The fix (`src/simple.rs`)

`read_index_capped` now captures the response `Content-Type` before consuming the
body, and on a JSON parse failure returns a clear, actionable `anyhow` error
instead of the raw serde error. Fail-closed; the package fails the sync (non-zero
exit) rather than appearing to succeed. Two shapes:

- **HTML** (content-type `text/html`, or a body whose first non-whitespace byte
  is `<`):
  > source returned an HTML page (Content-Type: text/html), not the PEP 691 JSON
  > pypiron migration requires — point --from at a JSON-capable endpoint, or check
  > credentials if this is a login page. Artifactory/Nexus serve HTML PEP 503 by
  > default; pypiron does not scrape HTML.
- **Other non-JSON** (any other content-type / malformed body): names the
  declared content-type and the same `--from` fix.

We deliberately do **not** scrape HTML PEP 503. The deliverable is a clear
refusal that tells the operator to enable a JSON endpoint (or fix credentials),
not a second, lossy index parser to maintain.

### Before / after (real binary, HTML source)

```
# BEFORE (master):
ERROR pypiron::sync: package sync failed package=htmlpkg error=expected value at line 1 column 1

# AFTER (patched):
ERROR pypiron::sync: package sync failed package=htmlpkg error=source returned an HTML page
  (Content-Type: text/html), not the PEP 691 JSON pypiron migration requires — point --from at
  a JSON-capable endpoint, or check credentials if this is a login page. Artifactory/Nexus serve
  HTML PEP 503 by default; pypiron does not scrape HTML.
```

## Coverage (`tests/test_migrate_sources.py`)

Five-row source-layout matrix, real pypiron binary over HTTP, fake sources stood
up with `ThreadingHTTPServer` (no Docker, no devpi, no new deps):

| Row | Source layout | Serves | Expected |
| --- | --- | --- | --- |
| a | Artifactory HTML-only PEP 503 | `text/html` | fail-closed, cause named |
| b | Nexus HTML-only PEP 503 | `text/html` | fail-closed, cause named |
| c | PEP 691 JSON at `/artifactory/api/pypi/<repo>/simple` | JSON | byte-exact migrate |
| d | PEP 691 JSON at devpi `/<user>/<index>/+simple` | JSON | byte-exact migrate |
| e | `200` HTML login page (bad credential) | `text/html` | fail-closed, cause named |

JSON rows assert a byte-exact round-trip (stored sidecar sha256 == source wheel
sha256, and `.origin == private`). Non-JSON rows assert non-zero exit, the
actionable phrases in the output, **no** raw `expected value` serde error, and no
artifact stored.

## Which incumbent versions actually emit PEP 691 JSON

Primary-source, with version numbers. This is why the guide must not promise
"same command, different URL" for Artifactory/Nexus.

### Sonatype Nexus Repository 3.x

- **PEP 691 JSON Simple API + PEP 658 metadata: new in 3.93.0.** Release notes:
  "Sonatype Nexus Repository now supports PEP 658 inline metadata and the PEP 691
  JSON-based Simple API for hosted and proxy PyPI repositories."
  <https://help.sonatype.com/en/sonatype-nexus-repository-3-93-0-release-notes.html>
- **PEP 700 (size/upload-time) fields: new in 3.94.** PyPI Repositories help:
  "New in 3.93 PEP 691: JSON-based Simple API ... including content negotiation";
  "New in 3.94 PEP 700 Additional fields for the JSON Simple API."
  <https://help.sonatype.com/en/pypi-repositories.html>
- **Consequence:** Nexus Repository **older than 3.93** serves HTML-only — the
  common case for a corporate box that has not been upgraded. Migration needs
  3.93+ (3.94+ to also carry PEP 700 size/upload-time).

### JFrog Artifactory 7.x

- **JSON indexing is opt-in and OFF by default; Artifactory defaults to HTML.**
  PyPI Repositories docs: enable it under **Administration > Artifactory Settings
  > Packages Settings > PyPI > "Enable simple json format"**; and "If JSON is
  requested but not available, the system falls back to HTML."
  <https://docs.jfrog.com/artifactory/docs/pypi-repositories>
- The Simple JSON API is requested via the PEP 691 `Accept` header
  (`application/vnd.pypi.simple.v1+json`) — which is exactly what pypiron sends.
  JFrog release information lists the PyPI Simple JSON API landing in the
  Artifactory 7.98.x self-hosted line
  (<https://jfrog.com/help/r/jfrog-release-information/artifactory-7.98.7-self-hosted>);
  treat the checkbox, not a version bump, as the switch that matters.
- **Consequence:** A stock Artifactory 7.x — even a current one — serves HTML to
  pypiron until an admin ticks **Enable simple json format** on the repository.

## Paste-ready patch for `docs/guides/migrate.md`

Replace the existing `## Artifactory and Nexus` section (from that heading down to
just before `## Migrating everything`) with the block below. It keeps the happy
path first, then names the one prerequisite that actually bites (JSON indexing)
and the exact error pypiron now emits when it is missing.

````markdown
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

**One prerequisite: the source must serve the JSON simple API (PEP 691).**
pypiron reads the modern JSON index; it does not scrape the older HTML index.

- **Artifactory** serves HTML by default. Turn on JSON per repository:
  **Administration > Artifactory Settings > Packages Settings > PyPI > Enable
  simple json format**. (If JSON is off, Artifactory falls back to HTML.)
- **Nexus** serves the JSON simple API from **3.93** onward (3.94 adds file
  sizes and upload times). Older Nexus is HTML-only — upgrade before migrating.

If the source is still HTML-only, the migration stops with a clear message
instead of a cryptic parse error:

```
source returned an HTML page (Content-Type: text/html), not the PEP 691 JSON
pypiron migration requires — point --from at a JSON-capable endpoint, or check
credentials if this is a login page.
```

The same message appears if a wrong credential lands you on a login page — check
`--source-user`/`--source-pass` before assuming the endpoint is wrong.

devpi is tested end-to-end. The Artifactory and Nexus paths above are their
standard simple-API endpoints; the JSON-indexing prerequisite is the one thing to
confirm first.
````
