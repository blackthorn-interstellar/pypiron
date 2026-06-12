# Gaps

Known missing features, tracked here so they don't get lost. Organized by
source: spec/conformance gaps in the current implementation, and market gaps
(features teams expect that PypIron doesn't have).

---

## Technical / conformance gaps

### 1. `WWW-Authenticate` header missing from 401 responses

All 401 return sites (`legacy_upload`, `require_admin`) emit bare
`UNAUTHORIZED` with no `WWW-Authenticate: Basic realm="PypIron"` header.

**Why it matters:** RFC 7235 requires this header on 401. pip's keyring
integration and browsers use it to trigger credential prompts. Without it,
unauthenticated pip clients get a silent 401 with no indication that credentials
are required. Twine and uv send credentials proactively so they're unaffected,
but pip's `--extra-index-url` fallback and interactive installs break silently.

**Fix:** Add `WWW-Authenticate: Basic realm="PypIron"` to every 401 response.

---

### 2. No gzip content-encoding on index responses

Index bytes are served raw. The in-memory `IndexCache` stores uncompressed bytes
and no compression middleware is applied.

**Why it matters:** DESIGN.md says "a multi-MB HTML file served statically with
gzip is a non-event" — implying this works. For large registries the global
`index.html` can reach several MB. Every pip/uv resolve fetches it. Uncompressed
at scale is a real bandwidth and latency cost.

**Fix:** Add `tower-http` `CompressionLayer` (or equivalent) to compress
responses for clients that send `Accept-Encoding: gzip`. Alternatively, store
pre-compressed bytes in `IndexCache` and serve with `Content-Encoding: gzip`.

---

### 3. No 301 redirect for non-normalized package names

`simple_pkg` normalizes the name and serves correct data but returns 200 at the
un-normalized path. E.g., `/simple/Requests/` returns content without redirecting
to `/simple/requests/`.

**Why it matters:** PEP 503 conformance. Standard PyPI behavior is a 301 to the
normalized URL. Tools that cache index responses by URL get duplicate cache
entries; edge proxies or CDNs fronting PypIron may split cache. pip/uv normalize
before requesting so this rarely causes broken installs, but it's a conformance
gap and can surprise integrators.

**Fix:** If `pkg != normalize_pkg_name(original_path_segment)`, return 301 to
the normalized URL before serving.

---

### 4. `--spool-dir` not mentioned in Docker examples

The config table lists `--spool-dir` / `PYPIRON_SPOOL_DIR` but the Docker
examples don't bind a real-disk volume for it. On containers where `/tmp` is a
RAM-backed `tmpfs`, large uploads spool into RAM — the exact problem the option
exists to solve.

**Fix:** Add a note in the Docker section recommending `-v /data/spool:/spool -e
PYPIRON_SPOOL_DIR=/spool` (or equivalent) when uploading large wheels.

---

## Market gaps

*Features that would drive adoption or remove friction for real teams.*

### Blocker-level

**Multiple upload credentials / API tokens**
One uploader credential and one admin credential means a single shared secret
across every CI job, developer, and service account. Teams that want per-service
tokens or need to rotate one without touching others can't. This is the most
commonly cited reason teams reach for devpi or Artifactory instead. The DESIGN
explicitly defers user accounts/tokens as needing a database — a legitimate
call, but it caps the addressable market at small teams or teams willing to
share a single secret.

**SSO / LDAP / SAML integration**
Large enterprises won't evaluate a tool that requires managing credentials
separately from their corporate identity provider. Without SSO, PypIron is
eliminated from any organization with an IT security policy. This is a full
blocker for enterprise adoption (though it may not be the target audience).

---

### Friction-level

**No audit log**
No record of who uploaded or deleted what and when. Enterprise security teams
(SOC 2, internal compliance) and regulated industries require this. Without it,
PypIron is excluded from regulated environments regardless of the rest of the
story.

**Fine-grained RBAC**
Two roles (uploader + admin) is too coarse for mid-size teams. Common need:
read-only roles, per-package or per-namespace permissions, separate CI service
accounts with minimal scope. devpi and Artifactory both have this; its absence
pushes teams toward those tools as they grow.

**No management UI / package browser**
Browsing `/simple/` shows raw PEP 503 HTML. Teams expect a web UI to see
versions, upload times, yanked status. devpi and pypicloud both have one. Not
a blocker — the PEP 503 index works — but it's friction for non-developer users
(release managers, QA, security teams).

**No Prometheus metrics / observability**
No `/metrics` endpoint. Production teams need request counts, upload volume,
cache hit rates, S3 latency, and error rates to justify running PypIron in
production and to page on problems.

**No webhook / event notification**
No way to trigger downstream actions (notify Slack, trigger CI) on publish or
yank. Competitors have this; teams work around it by polling the index.

**No PyPI-passthrough / fallback (intentional, but felt as friction)**
devpi's most popular feature is transparent PyPI fallback. PypIron deliberately
rejects this (DESIGN documents the dependency-confusion risk). The correct answer
is explicit mirroring via `pypiron sync`. But teams evaluating PypIron vs devpi
feel it as missing — migration requires ops work (sync setup, packages list) that
devpi hides behind a proxy.

---

### Nice-to-have

**`pypiron` management CLI (beyond sync)**
`pypiron packages list`, `pypiron packages delete <pkg> <ver>`, `pypiron yank`
etc. The management API exists but requires hand-crafting `curl` commands. A
first-class CLI surface lowers the ops bar significantly.

**HTTPS / TLS built-in**
Currently expects a reverse proxy for TLS. Small teams running PypIron standalone
have to set up nginx/caddy separately — a real barrier for the "just run it"
audience.

**Package retention / cleanup policies**
Auto-delete versions older than N days or keep only the N most recent per
package. Large registries accumulate stale pre-release wheels.

**Rate limiting**
Prevent resource exhaustion on shared instances. Less critical for
single-tenant/private use but matters for shared team deployments.

**CI/CD example configs**
GitHub Actions, GitLab CI snippets showing the full publish-then-install pattern
(`--sync-uploads`, credential setup, uv publish). Reduces time-to-working for
new adopters.
