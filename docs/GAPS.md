# Gaps

Known missing features, tracked here so they don't get lost. Bucketed by
intent — **Maybe**, **Enterprise** (gated behind the paid version), and
**Not planned**.

Previously tracked and since shipped: PEP 503 301-redirects for
non-normalized names, `WWW-Authenticate` on 401s, gzip index responses
(precompressed variants in the index cache), read authentication
(`--read-user`/`--read-pass`), on-demand PyPI proxying (`--proxy-upstream`
with `sync`-equivalent filters), `/health`, Prometheus `/metrics`,
`--log-format json`, read-only-by-default when no credentials are configured
(open-mode writes are gone), `--spool-dir` documentation, and the scale-tier
reconcile (event markers as the backbone, fingerprint audits with cost
proportional to churn, `pypiron verify`/`resync` — see SCALE.md and
DESIGN.md).

---

## Maybe

### Scoped API tokens

One uploader credential and one admin credential means a single shared secret
across every CI job, developer, and service account. Teams that want
per-service tokens, or need to rotate one without touching others, can't —
the most commonly cited reason teams reach for devpi or Artifactory instead.

If we do it, it stays no-DB:

- Token = random secret; server stores only a hash + scope in a file
  (`_tokens/<token-id>.json`), listable/regenerable like everything else.
- Scope: package name(s) or prefix, role (publish), optional expiry.
- Works as basic auth (`__token__` / `pypi-...` style) so every client just
  works.
- CLI follows: `pypiron token create --package foo`. Per-package "ownership"
  is this feature too — a token scoped to `foo` *is* the ownership record.
  No separate user system.

### Explicit `--read-only` flag

No credentials already means read-only, and that covers the footgun. The
remaining case is a credentialed deployment that wants serve-only replicas:
a flag that 403s every write and skips the worker/reconciler/lease, so the
node can run with read-only storage credentials (a compromised public-facing
replica physically cannot tamper with the truth tree). Note such a node
serves what's materialized and cannot self-heal — a writer node must exist
somewhere.

### Management UI / package browser

Browsing `/simple/` already shows projects, versions, and files as HTML, but
it's raw PEP 503 — no upload times, yanked status, or token management. Teams
expect one (devpi and pypicloud have one); friction for non-developer users
(release managers, QA, security). Earns its keep only after tokens exist —
and even then: server-rendered pages, no build step, no React death star.

### Webhook / event notification

Trigger downstream actions (notify Slack, trigger CI) on publish or yank.
Competitors have this; teams work around it by polling the index.

### Management CLI (beyond sync)

`pypiron packages list`, `pypiron packages delete <pkg> <ver>`, `pypiron
yank` etc. The management API exists but requires hand-crafting `curl`
commands. A first-class CLI surface lowers the ops bar.

### HTTPS / TLS built-in

Currently expects a reverse proxy for TLS. Small teams running PypIron
standalone have to set up nginx/caddy separately — a real barrier for the
"just run it" audience.

### Package retention / cleanup policies

Auto-delete versions older than N days or keep only the N most recent per
package. Large registries accumulate stale pre-release wheels.

### Rate limiting

Prevent resource exhaustion on shared instances. Less critical for
single-tenant/private use but matters for shared team deployments.

### CI/CD example configs

GitHub Actions, GitLab CI snippets showing the full publish-then-install
pattern (`--sync-uploads`, credential setup, uv publish). Reduces
time-to-working for new adopters.

---

## Enterprise (paid version)

Features gated behind the paid tier. Table stakes for large-org procurement,
irrelevant to the free tier's audience.

### SSO / LDAP / SAML integration

Large enterprises won't evaluate a tool that requires managing credentials
separately from their corporate identity provider. Without SSO, PypIron is
eliminated from any organization with an IT security policy.

### Audit log

A record of who uploaded or deleted what and when. Enterprise security teams
(SOC 2, internal compliance) and regulated industries require this. Without
it, PypIron is excluded from regulated environments regardless of the rest of
the story.

### Fine-grained RBAC

Two write roles (uploader + admin) plus an optional read credential is too
coarse for large orgs. Per-package or per-namespace permissions, separate CI
service accounts with minimal scope. devpi and Artifactory both have this.
(Scoped tokens, above, cover the simple cases; full RBAC is the enterprise
version.)

---

## Not planned

### Snapshot export/import

For disk, "it's just files — rsync it" is genuinely correct and better than a
bespoke tool. For S3, `aws s3 sync` exists. Cross-backend airgap movement is
the only real case, and `pypiron sync` between servers already covers most of
it. Not worth a command.

### SQLite / Postgres mode

The no-DB design *is* the answer to both. Single-node is already stupidly
easy (one binary, one directory); serious deployments get S3 + multi-node
lease. Adding a database would delete the product's reason to exist.

### Static/simple mode

The whole server is this. Truth is files; the index is a materialized view;
backups are rsync.

### Hosted/proxy/group repo taxonomy

Nexus needs three repo types and a group URL because its repos are silos. We
have one namespace with per-package origin: private packages, explicit
mirrors (`sync`), and on-demand proxying (`--proxy-upstream`) all serve from
one URL.
