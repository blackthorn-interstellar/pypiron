# Roadmap

What's shipped, what's on the table, and what we've decided against. The bar for
"shipped" is blackbox-verified behavior against real clients (see
[TESTING.md](TESTING.md)), not spec-shaped output.

## Shipped

**Standards** ([STANDARDS.md](STANDARDS.md) is the authoritative matrix)
- PEP 503 simple HTML index — name normalization, sha256 URL fragments, PEP 629 `repository-version` meta tag, 301-redirects for non-normalized names.
- PEP 691 JSON simple API with content negotiation; PEP 700 `versions` / `size` / `upload-time` (api-version 1.3).
- PEP 592 yank; PEP 658/714 wheel `METADATA` served as a static `<filename>.metadata` companion with `core-metadata` attrs.
- PEP 740 provenance relayed verbatim through `sync` and the proxy (`<filename>.provenance` companion, `provenance` key / `data-provenance` attr) — carried, not verified; first-party `attestations` uploads refused.
- `requires-python`; filename immutability (re-upload rejected); HTTP caching (content-hash ETags + 304, `immutable` artifacts, Range requests, gzip index variants).

**Upload & write path**
- Legacy `POST /legacy/` multipart upload, as spoken by `twine` and `uv publish`; `sha256_digest` verified on ingest.
- Write-time metadata sidecars (`<filename>.meta.json`) — rebuilds read sidecars, never re-hash.
- Optional synchronous uploads (`--sync-uploads`) for publish-then-install CI.

**Storage & indexing**
- Disk (default, zero deps) and three cloud backends — S3 / S3-compatible, Google Cloud Storage, and Azure Blob — over one `object_store`-backed implementation.
- Client-aware artifact delivery on cloud backends (`auto` / `redirect` / `stream`) via presigned URLs; streaming uploads (incremental hash, bounded RSS, multipart for large artifacts).
- Crash-safe event-marker indexing (intent/commit pairs); fingerprint audit with cost proportional to churn; `pypiron verify` / `pypiron resync`.
- Multi-node on any cloud backend via a sloppy leader lease (conditional writes, TTL, heartbeat).

**Mirroring & proxying**
- `pypiron sync` over HTTP (`--to`, the single writer is always the server), carrying PyPI's true `upload-time` so `--exclude-newer` stays historically correct; tag/time filters; `--pkg`/packages-list selection; `pypiron.toml` config layering.
- On-demand PyPI proxying (`--proxy-upstream`) — cache public dependencies on first use, origin-checked.

**Security & access**
- Origin exclusivity — every package `private` or `mirror`, claimed at first write; collisions rejected (dependency-confusion defense).
- `--private-prefix` reserved namespace (normalized-name matching).
- Three-tier basic auth (admin ⊇ uploader ⊇ reader); read-only by default when no write credential is set.

**Operations & management**
- `/health`, Prometheus `/metrics`, `--log-format json`, per-project traffic attribution via username subaddressing.
- Delete and yank management API.
- Human-facing pages: landing (`/`), live `/dashboard`, and a read-only project page (`/project/<pkg>/`) — metadata sidebar, release files, README shown verbatim (not rendered). Server-rendered on demand, no build step, gated by read auth.

Implementation history (the original milestone-by-milestone build) lives in git;
the [improvements log](BENCHMARK_RESULTS.md#improvements-log) tracks every landed
performance change with its before/after.

## Planned

Tracked so they don't get lost. Not commitments — bucketed by intent.

### Maybe

**Scoped API tokens.** One uploader and one admin credential means a single
shared secret across every CI job and developer. Per-service tokens (rotate one
without touching others) are the most commonly cited reason teams reach for
devpi or Artifactory instead. Stays no-DB if we do it: token = random secret,
server stores a hash + scope in a file (`_tokens/<token-id>.json`), works as
basic auth (`__token__` / `pypi-...`). Per-package "ownership" is this feature —
a token scoped to `foo` *is* the ownership record.

**Explicit `--read-only` flag.** No credentials already means read-only. The
remaining case is a credentialed deployment that wants serve-only replicas: a
flag that 403s every write and skips the worker/reconciler/lease, so the node
can run with read-only storage credentials (a compromised public replica
physically cannot tamper with truth). Such a node serves what's materialized and
cannot self-heal — a writer node must exist somewhere.

**Management UI.** The read-only project page ships (`/project/<pkg>/`:
metadata sidebar, release files, README shown verbatim — see Shipped). What's
left is the *management* half — token management, yank/delete from the browser —
which earns its keep only after tokens exist. Server-rendered, no build step, no
React death star. A possible follow-up to the read page: opt-in,
sanitized Markdown rendering of the README (`pulldown-cmark` + `ammonia`, fuzzed,
`<pre>` fallback for rst/plain), off by default to keep the zero-dep posture.

**Webhook / event notification.** Trigger downstream actions (notify Slack, kick
CI) on publish or yank. Competitors have it; teams work around it by polling.

**Management CLI (beyond sync).** `pypiron packages list`, `pypiron packages
delete <pkg> <ver>`, `pypiron yank`. The management API exists but requires
hand-crafted `curl`.

**Package retention / cleanup policies.** Auto-delete versions older than N days
or keep only the N most recent per package. Large registries accumulate stale
pre-release wheels.

**Rate limiting.** Less critical for single-tenant use, matters for shared team
deployments.

**CI/CD example configs.** GitHub Actions / GitLab CI snippets for the full
publish-then-install pattern. Reduces time-to-working for new adopters.

### Enterprise (paid version)

Table stakes for large-org procurement, irrelevant to the free tier's audience.

- **SSO / LDAP / SAML** — without it, PypIron is eliminated from any org with an IT security policy.
- **Audit log** — who uploaded or deleted what and when; required by SOC 2 / regulated industries.
- **Fine-grained RBAC** — per-package or per-namespace permissions, minimal-scope CI service accounts. (Scoped tokens cover the simple cases; full RBAC is the enterprise version.)

## Not planned

**Snapshot export/import.** For disk, "it's just files — rsync it" is genuinely
better than a bespoke tool. For S3, `aws s3 sync` exists. `pypiron sync` between
servers covers cross-backend airgap movement. Not worth a command.

**SQLite / Postgres mode.** The no-DB design *is* the answer. Single-node is one
binary and one directory; serious deployments get S3 + multi-node lease. A
database would delete the product's reason to exist.

**Static/simple mode.** The whole server is this. Truth is files; the index is a
materialized view; backups are rsync.

**Hosted/proxy/group repo taxonomy.** Nexus needs three repo types and a group
URL because its repos are silos. PypIron has one namespace with per-package
origin: private packages, explicit mirrors (`sync`), and on-demand proxying
(`--proxy-upstream`) all serve from one URL.

**Built-in HTTPS / TLS.** TLS terminates in front of the service — a reverse
proxy, load balancer, or CDN (nginx, Caddy's auto-HTTPS, Cloudflare, an ALB). A
static-file server gains nothing by owning cert lifecycle (ACME, renewal, SNI)
that those do better, and it fits the design: be cache-correct and let the layer
in front compound a single node. The standalone "just run it" case is a three-line
Caddyfile, not a feature we build.

**Deprecated & niche standards — TUF, XML-RPC, eggs, the legacy `/pypi/<pkg>/json`
API.** Explicitly out of scope; the authoritative statements are in
[STANDARDS.md](STANDARDS.md). They are either dead (PyPI never shipped PEP 458 TUF;
XML-RPC search is disabled upstream), a legacy format (eggs — pip dropped egg
installs), or a non-standard metadata endpoint superseded by the PEP 691/700 JSON
simple API. No installer needs any of them from a private index.
