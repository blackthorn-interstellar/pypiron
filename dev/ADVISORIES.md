# Advisories: malware blocking and the org audit

Status: design sketch, pre-implementation. One advisory feed powers two
features: a malware blocklist enforced where bytes are served, and an
org-level audit report. Vulnerabilities are reported, never blocked;
malware is blocked, never negotiated. Everything is on by default —
protection nobody configured is the point.

## Why

Every remediation signal pypiron carries today stops at the index layer:
PEP 792 statuses relay end-to-end (a quarantined project lists no
files), sync yank-stamps files that vanish upstream (`removed
upstream`), and the default `--exclude-newer` 7-day quarantine holds
fresh releases back. But yank deliberately lets pinned installs resolve,
and the artifact byte gate checks tombstones, freezes, and origin — not
advisories, not project status. A lockfile resolved through pypiron pins
`/files/...` URLs, so after PyPI quarantines a package or deletes a
compromised release, every lockfile in the org keeps installing the
cached malware from us, forever. That is the exact failure mode uv
documented for PyPI's own object storage (index-level remediation,
lockfile-level exposure); a caching mirror inherits it and — unlike a
client-side tool — can close it once, for every client: uv, old pip,
poetry, CI, and two-year-old lockfiles equally.

The second gap is visibility. Client-side `uv audit` answers "what does
*this project* depend on"; nothing answers "what does *the org* actually
host and install." pypiron has all three inputs already — the corpus,
the download counters, and (with this design) the advisory feed — so the
org-level report is a join, not a system.

## Data source

OSV's bulk export for the PyPI ecosystem:
`https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip` — the
same database uv queries live (PYSEC + GHSA + `MAL-*` from OpenSSF
malicious-packages), so advisory IDs shown by pypiron and by `uv audit`
on a laptop always agree. ~32 MB, no auth, no vendor, ETag'd (refreshes
are conditional GETs that usually transfer nothing), regenerated
continuously (observed lag: minutes). uv taps the query API per
operation; we take the export because a server auditing a whole corpus
and gating a hot path needs a local snapshot, and because air-gapped
deployments need a feed that is just a file.

`--advisory-feed` accepts a URL or a local path. The local-path form is
also how blackbox tests stay hermetic: seed a tiny zip, no network.

## Snapshot pipeline

The leader's worker refetches the feed on the existing reconcile
cadence (`If-None-Match`; a 304 is free) and persists the zip **verbatim**
to `_advisories/osv-pypi.zip` — the same carry-don't-author pattern as
`.provenance`. Followers detect changes with `list_page` on the exact
key and compare `ObjectMeta.etag` (a LIST, no body; the Storage trait
has no conditional GET, and `get_with_etag` transfers the full 32 MB),
fetching only when the etag moves. The HTTP `If-None-Match` refetch
applies to the leader's OSV pull, not to storage. Every node parses the
snapshot into two in-memory structures:

- **Block set** — normalized name → malicious version set (or all-versions
  sentinel), from `MAL-*` advisories only.
- **Audit index** — normalized name → advisory records (id, summary,
  severity, affected ranges, fixed-in), from everything.

Names normalize through `names.rs` on both sides; version-range events
(`introduced`/`fixed`) evaluate with `pep440_rs`, already a dependency.
Parsing is pure and unit-testable; everything else is blackbox.

**No internet on the server.** The leader's OSV pull is one *source* of
the snapshot, not a requirement. Air-gapped deployments have two more:
point `--advisory-feed` at a local path (re-read on the same cadence;
the operator's existing ferry drops a fresh zip), or let `pypiron sync`
deliver it — sync already relays yank state and PEP 792 status, and the
advisory snapshot is the third piece of security metadata that crosses
the boundary. The sync client, which has upstream access by definition,
fetches the export (from OSV directly, or from the source server's
`GET /advisories/feed` when syncing pypiron→pypiron) and pushes it to
the destination's admin-auth `PUT /advisories/feed`, etag-conditioned so
an unchanged feed transfers nothing. All three sources converge on the
same verbatim `_advisories/osv-pypi.zip` write; enforcement and the
audit never touch the network in any topology — which is also why the
blackbox tests, seeded from a local zip, exercise exactly the air-gapped
path on every run.

## Enforcement (`--malware-block`)

Three hooks, all existing seams. The byte gate is the guarantee; the
rest is hygiene.

**The byte gate.** The probe lands at the single visibility chokepoint —
the `file_visible_read_through` call site, which runs once before both
direct streaming and presigned-redirect minting. At the *call site*, not
inside it: the tombstone/frozen/mirror-quarantine marker HEADs it wraps
run only in multi-bucket mode (single-bucket relies on physical
deletion), and advisory blocking must enforce in both modes. A
**mirror-origin** file whose (name, version) is in the block set is
refused — 403, JSON body naming the advisory IDs, one warn log with
package/version/advisory. The same probe consults a second set: projects
whose relayed PEP 792 status is `quarantined`. That set is **derived by
the worker** from the `.project-status.json` sidecars it already reads
during index sweeps, and refreshed alongside the advisory snapshot —
status is a per-project storage object with no request-path cache, so
reading it per-request would put a GET on the hot path; the derived set
keeps the gate a pure hash probe, at the cost of quarantine-at-the-gate
lagging by at most one sweep (the listing empties immediately at render
time regardless; the gate is the second layer, for direct URLs).
Private-origin files never consult either set: OSV names live in PyPI's
namespace, and origin exclusivity is the proof that a same-named private
package is not that package.

**Proxy fill.** The on-demand proxy refuses to download-and-cache a
blocked version — the server-side twin of uv's pre-sync check. Malware
that appears in OSV before anyone here requests it never lands in
storage at all.

**Listings.** Proxy-rendered pages and materialized index rebuilds filter
blocked files, same mechanics as `.frozen` suppression. Best-effort by
design: a listing regenerates on its natural triggers, not on feed
refresh — no rebuild storms coupled to an external input — and a stale
listing is harmless because the download 403s.

**Failure posture.** Never a per-request network call; the hot path is a
hash-set probe. Feed staleness degrades protection freshness, never
installs: runtime fetch failures keep serving on the last snapshot with
a rising staleness gauge and a warn log. Startup is where fail-closed
applies, split by intent: **explicitly configured** advisory flags that
cannot produce a snapshot (fetch failed, no `_advisories/` copy) refuse
to start — same rule as a half-configured credential. The **implicit
defaults** must never brick an air-gapped box that never asked: no
snapshot obtainable → start, warn loudly that blocking is armed but
unfed, and self-arm the moment a snapshot arrives (ferry or sync push).
Unreachable-feed retries log on state change, not per attempt. A fresh
follower serves without blocking until its
first snapshot read — sloppy by design, bounded by one reload interval.

**Tripwire metric.** `pypiron_blocked_downloads_total` (unlabeled;
details go to the log, keeping `/metrics` low-cardinality). A nonzero
rate is the active-remediation signal: some machine still *asks* for
malware — that's an incident, not a statistic.

## The audit

**Inventory.** (name, version) pairs for every stored artifact, from a
storage LIST — filenames carry both (sidecar read as fallback for
unparseable names). No object GETs, no hashing. A typical approved-list
mirror is a handful of LIST pages; a full-PyPI-scale mirror (~18M keys)
is ~18k LIST requests, pennies, minutes, streamed per-package so memory
stays flat. Steady state is cheaper still: the report only changes when
the feed ETag or the corpus changes, both already observed — recompute
on those signals, not on request.

**Join.** Inventory × audit index in memory, then × the compacted daily
`_counters/` files for downloads over the window. Counter keys are
per-*file* (`<pkg>/<filename>`), so version rows are a roll-up via
`infer_version_from_filename`; the 30-day window is the audit's choice,
comfortably inside the store's 90-day default retention. Output rows: package,
version, origin, advisory IDs, severity, fixed-in, downloads (30d),
blocked-flag. Sorted by downloads descending — the top of the list is
what the org actually installs, which no client-side tool can see.

**Surfaces.** Materialized `_advisories/report.json`; served as `/audit`
(server-rendered HTML, same no-build-step pattern as `/projects/`) and
`/audit.json`. **Admin-gated** — an org's ranked vulnerability list is
attacker recon, so it rides the strongest credential. A per-project
advisory panel on `/project/<pkg>/` reads the same index. Vulnerability
rows are informational only; blocking stays MAL-only.

## Configuration

House rules: every flag is also an env var; document in
`docs/reference/configuration.md`. Both knobs are server-process
settings → clap args on the serve command and the `[serve]` table
(`ServeConfig` uses `deny_unknown_fields`, so the toml side means
extending the struct and `config_template.toml`, not just the flag).

**On by default.** The customer who never reads this page still gets
protected — that's the product's premise, and the shipped precedent is
`--exclude-newer`'s always-on 7-day quarantine. uv staged its checks as
opt-in because it has millions of users mid-flight; pypiron doesn't, and
a server-side hash probe has no latency or compatibility cost to stage.
Opt-outs exist; half-states warn loudly (see Failure posture).

- `--advisory-feed <url|path>` / `PYPIRON_ADVISORY_FEED` — defaults to
  the OSV PyPI export URL; `""` disables the feature entirely. The
  startup log names the URL it will poll, so the default egress is
  never a surprise to an ops team watching a package server.
- `--malware-block` / `PYPIRON_MALWARE_BLOCK` — default **true**.
  Requires a snapshot *source*: the feed, or a previously pushed
  `_advisories/` snapshot (the sync-delivery topology).
- No refresh-interval knob: the reconcile cadence is the cadence. No
  audit on/off knob: feed set ⇒ audit exists.

## Non-goals

- **Own detection** — heuristics, typosquat scoring, sandboxing. pypiron
  consumes advisories; it does not author them.
- **Per-request OSV queries** — no network on the serve path, ever.
- **CVE blocking or severity policy engine** — audit reports; operators
  decide. Blocking on CVEs breaks the org's pinned builds by Tuesday.
- **Advisory data in index responses** — neither the rejected legacy
  `/pypi/<pkg>/json` API (its consumers pin to pypi.org; we'd have zero)
  nor extension fields in our PEP 691 JSON (couples materialized views
  to a fast-moving external input → rebuild storms).
- **PEP 740 verification** — provenance stays carried, not verified.

*Tripwire for later:* the moment uv or pip-audit accepts a custom OSV
base URL, serve OSV's `querybatch` shape from our snapshot — client-side
`uv audit` then works fully air-gapped against pypiron, zero invented
standards.

## TODO

Rungs, each shippable and blackbox-tested before the next:

1. ROADMAP.md: move this from unlisted to Planned, pointing here.
2. `advisories.rs`: zip fetch/read, OSV parse, block set + audit index;
   unit tests for parsing/matching (pure functions only).
3. Worker refresh + `_advisories/osv-pypi.zip` persistence + follower
   reload + `pypiron_advisory_snapshot_age_seconds` gauge + admin
   `PUT /advisories/feed` / `GET /advisories/feed` (the push path).
4. Byte-gate enforcement (direct + presign) + proxy-fill refusal +
   `pypiron_blocked_downloads_total`; startup fail-closed rule.
   Blackbox: `tests/test_advisories.py`.
5. Listing filters (proxy render + rebuild); worker-derived
   quarantined-project set + its byte-gate probe for mirror-origin
   files.
6. Sync relay: `--advisory-feed` on `pypiron sync` — fetch from OSV or
   the source server, etag-conditioned push to the destination. On by
   default (`""` opts out); a destination without the endpoint warns
   and the package sync proceeds.
7. Audit report materialization + `/audit` + `/audit.json`, admin-gated;
   counters join.
8. `/project/<pkg>/` advisory panel.
9. Docs: user-manual supply-chain page (cooldown default + PEP 792 relay
   + malware blocking + audit, as one story) + `configuration.md` knobs.

Storage-layout note: `_advisories/` is a new reserved top-level prefix —
add it to the layout contract in [DESIGN.md](DESIGN.md) in rung 3.

## Acceptance criteria

All via the real binary over HTTP with real clients unless marked
(unit). Feed = local zip fixture with one `MAL-*` (exact version), one
`MAL-*` (all versions), one PYSEC vuln with a fixed-in.

- **AC1 — block wins over everything, by default.** With only
  `--advisory-feed` pointed at the fixture and **no `--malware-block`
  flag passed**, a mirrored/proxy-cached file matching a MAL advisory:
  direct GET → 403 with advisory ID in the JSON body; `uv pip install
  pkg==bad` and a pinned lockfile install both fail;
  `pypiron_blocked_downloads_total` increments.
- **AC2 — private exemption.** A *private* package sharing the malicious
  name installs normally.
- **AC3 — fill refusal.** Fresh proxy, request a MAL version: install
  fails and no artifact/sidecar appears in storage.
- **AC4 — listings scrub.** After rebuild/proxy render, blocked files are
  absent from PEP 503 HTML and PEP 691 JSON listings; unaffected
  versions of the same package still install.
- **AC5 — quarantine at the gate.** Mirror-origin files of a project with
  relayed PEP 792 `quarantined` status: direct GET → 403 after the
  worker's next sweep (bounded by one reconcile interval; the listing
  empties immediately at render time as today).
- **AC6 — availability over freshness.** Kill the feed source after a
  snapshot exists: installs of clean packages proceed; staleness gauge
  rises; no request slows down (no network on the serve path).
- **AC7 — startup posture, split by intent.** Explicit `--malware-block`
  (or an explicit feed) with an unreachable source and empty
  `_advisories/`: exits nonzero with a clear error. Default
  configuration with the feed unreachable (dead endpoint standing in
  for the OSV URL): starts, serves, warns that blocking is armed but
  unfed, and begins blocking without a restart once a snapshot is
  pushed.
- **AC8 — audit truth.** `/audit.json` (admin auth) lists exactly the
  seeded vulnerable/malicious (package, version) rows with advisory IDs,
  fixed-in, and download counts after a real install; reader credential
  gets 403; `/audit` HTML renders the same rows.
- **AC9 — same IDs as uv.** For a package with a real OSV advisory in the
  fixture, the ID shown by `/audit` equals the ID in the OSV record
  (byte-equal string) — parser never rewrites identity.
- **AC10 — air-gapped delivery.** A server with no `--advisory-feed` and
  no outbound network: `pypiron sync --advisory-feed <local zip>` pushes
  the snapshot to it; AC1 blocking and AC8 audit rows then hold there.
  A second run with an unchanged feed transfers no snapshot body
  (etag-conditioned); after a server restart, `--malware-block` starts
  cleanly from the stored snapshot alone.
- **AC11 — no regressions.** `make check` and the full `make test` suite
  pass; multi-bucket tests unaffected (advisory state lives outside the
  packages tree and is never fanned out as truth).
