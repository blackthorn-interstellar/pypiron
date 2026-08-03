---
description: Malware blocked within minutes, new releases wait, private names stay private. What pypiron stops, what it trusts, and how to verify a release.
---

# Security features

pypiron is fail-closed by default: a half-configured credential refuses startup,
secrets compare in constant time, and a private name never falls through to
public PyPI. Every client behind it — uv, old pip, poetry, CI, a lockfile from
two years ago — gets every protection below at once, because the server
enforces each one, not the client. How these defenses are verified —
chaos, fuzzing, and a full-PyPI parser check — is its own page:
[How it's tested](testing.md).

## Malware blocking and the release cooldown

Known malware never installs. New releases wait long enough for a bad one to
surface. Both on by default.

**New releases wait.** A compromised maintainer account or a typosquat is most
dangerous in its first hours, before anyone notices. A **dependency cooldown** — `--exclude-newer` —
puts a window between when a release is published and when pypiron serves
it, so resolution lands on versions old enough for a bad one to have surfaced
and been pulled. It's the same practice uv, npm, and Dependabot have
standardized on. The default is seven days, on a sliding window, re-checked on
every read. `sync` applies the same window to what a run mirrors.

How much does the wait buy? Across every malicious version OSV has recorded
for PyPI — 17,043 releases in 11,517 projects, measured 2026-07, most of them
malicious from birth — 72% had a public advisory inside the default 7-day
window, so the feed plus the cooldown block them outright. The hard case is a
compromised **established** package: for 2024+ compromises, the advisory check
alone blocks 34% on release day, the 7-day default raises that to 56%, and a
30-day cooldown catches 86%. The default catches the fast reports; a 30-day
window catches most of the rest.

```bash
pypiron serve --admin-pass "$ADMIN" \
  --proxy-upstream https://pypi.org \
  --exclude-newer "7 days"     # the default; pass "30 days" to widen
```

Widen the window, pin an absolute "as of" date, or disable it — all
`<when>` formats live in
[Configuration → Mirror selection](reference/configuration.md#mirror-selection).

!!! note "Only admins can backdate"
    An ordinary upload can only claim its receipt time, so a publisher can't
    sneak a package in under a cutoff. Setting any other timestamp — including
    mirror uploads that carry PyPI's original time — requires the admin
    credential; with none configured, pypiron refuses them.

**Known malware never installs.** A cooldown buys time; it can't catch what's
already confirmed bad. Some releases aren't merely risky — they're malware
listed in the public advisory databases (the same source `uv audit` reads).
pypiron refuses to serve those files. Ask for a flagged version and the server
refuses it, naming the advisory that condemned it; the on-demand proxy won't
fetch and cache one in the first place.

This closes a gap every caching mirror has. When a resolver locks a dependency
through pypiron, it pins a download URL to *this* server. Later PyPI pulls a
compromised release: it vanishes from pypi.org, but every lockfile in your org
keeps asking pypiron for the copy it cached — and a plain mirror keeps handing
it back, forever. pypiron checks the advisory feed at the door instead, so the
file it served yesterday stops today — a fresh uv run and a two-year-old
lockfile alike.

**Blocking starts within minutes.** Every node watches OSV for individual
just-published malware advisories and starts blocking within minutes of
publication — no waiting for the next daily refresh. A client whose own advisory cache is a
day stale is still covered, because the block is at the server. The full
advisory snapshot still refreshes daily; a withdrawn advisory un-blocks on that
same daily schedule.

**Availability wins over freshness.** If the feed source goes unreachable, clean
packages keep installing and nothing slows down; blocking stops getting
fresher, and a staleness gauge climbs so you can alert before it drifts. It
degrades toward stale, never toward down. A blocked download is itself worth an
alert: it means some machine in your fleet is still asking for malware.

First boot is covered too: the binary ships with a compiled block set baked in
at release build, so a brand-new server blocks known malware from its first
request; the first live snapshot supersedes it without a restart. Want
fail-closed against even that release-old staleness? Set
`PYPIRON_MALWARE_BLOCK=true` explicitly and a server with no live snapshot
refuses to start.
The feed itself is OSV's PyPI export, fetched from a Google-hosted bucket
(named in the startup log) — the one outbound connection a default install
makes, and repointable or removable:
[advisory feed options](reference/configuration.md#server).

**Pulled upstream, pulled here.** PyPI sometimes quarantines a project
outright — a compromised account, a malware finding — freezing it so nothing new
resolves. pypiron relays that state. A quarantined project lists no files here,
and pypiron refuses a direct download, so a URL already pinned in a lockfile
can't route around the empty listing. When a single file disappears upstream,
`sync` marks it withdrawn the same way. Whatever PyPI stops standing behind,
pypiron stops serving. This refusal is a distinct guarantee from malware
blocking: turning blocking off (`--malware-block=false`) leaves it standing.

A private package that happens to share a malicious public name still installs.
pypiron blocks only the public package the advisory names — a private name of
your own is not that package. You can turn blocking off, or disable the
advisory feature entirely — see
[Configuration → Server](reference/configuration.md#server).

## Dependency confusion

The trap: an internal package name also exists on public PyPI, and a resolver
pulling from both indexes chooses the public copy. pypiron's rule closes it:
**each name is private or public, never both.** The first upload — a private
push or a mirror sync — reserves the name for that world, and it stays
reserved. pypiron rejects a private upload to a mirror-owned name, `sync`
refuses a name you own privately, and collisions are hard errors, never merges.
Deleting every file of a package does not release the name.

`--private-prefix` reserves a whole namespace (like `acme-*`) for private
uploads and forbids `sync` from touching it, so nobody can publish an internal
package under a name that later collides upstream. Matching is on normalized
names — `acme_foo`, `acme.foo`, and `acme-foo` are the same name.

The other half is client-side: point clients at this one index
(`--default-index` for uv; `--index-url` for pip, not `--extra-index-url`) and
let the server decide what exists. [How private and public names coexist](concepts.md#a-name-is-yours-or-pypis-never-both).

## Approval lists

Narrow the server to packages somebody chose to allow. Give `sync` a file of
approved names and it pre-loads exactly those; give the proxy the same include
list and it fetches nothing else. The cooldown and malware
blocking still apply on top — approval is a floor, not a bypass. Writing,
updating, and enforcing a list: [Approval lists](concepts.md#what-it-keeps-out).

## Air-gapped deploys

Serve where nothing reaches the internet. The proxy talks to live PyPI on a
cache miss, so `sync` removes that surface: pre-load an approved package list
from a connected host, then serve from a node with no egress — the mirror is
complete on its own, no upstream needed. Malware blocking crosses the same gap:
a pypiron-to-pypiron `sync` ferries the advisory feed alongside the packages.
Until the first delivery, the block set baked into the binary at release
covers the box; the ferried feed supersedes it without a restart. The full recipe — feed sources, ferry schedules, freshness —
is in [Air-gapped deploys](guides/air-gapped.md).

## Vulnerability audit

Blocking stops malware. It says nothing about the ordinary vulnerabilities
already sitting in packages you host — those are reported, not blocked, because
blocking on a CVE would break your pinned builds. The audit is that
report: every hosted package a known advisory affects, with its severity and the
version that fixes it, **ranked by how often your org installs it.** The top of
the list is your real exposure — the vulnerable thing everyone pulls a hundred
times a day, not the one nobody touches.

No client-side tool can produce this. `uv audit` answers "what does *this*
project depend on"; pypiron already holds all your packages, every download
count, and the same advisory feed, so it answers "what does the *org* host and
install." It's a plain page at `/audit`, with a JSON twin at `/audit.json` for scripts.
Each project's own page carries the advisory list for that package.
Admin-only: a ranked list of your soft spots is exactly what an attacker would
want, so it rides the strongest credential.

## Trust boundaries

**Untrusted — every client request.** Anything a client sends is hostile until
proven otherwise. Credentials compare in constant time, so a wrong guess leaks
no timing — and repeated wrong guesses earn a lockout (see
[Login throttling](#login-throttling)). A half-configured credential disables
its role instead of enabling a bypassable one. Private names never fall through
to upstream, so nobody can shadow one with a public package of the same name. A
filename, once uploaded, is never replaced (PyPI's own rule): pypiron rejects a
re-upload of an existing filename, so nobody can swap bytes under a version
already in someone's lockfile. A browser can't be turned against you: pypiron
rejects cross-site state-changing requests, so another site can't ride cached
Basic credentials to forge an upload or a yank. Every response carries
`X-Content-Type-Options: nosniff`. pypiron ignores
client-set `X-Forwarded-For`/`X-Real-IP` for the access log and the login
throttle unless you enable `--trusted-proxy`, so a direct caller can't forge its
logged address.

**Trusted — your storage backend.** The S3, GCS, Azure, or disk backend you
configure is your data behind your keys, and pypiron treats its responses as
trusted input. That trust has exactly one sharp edge — see
[pypiron's own dependencies](#pypirons-own-dependencies).

**Trusted — PyPI, for public packages.** For anything mirrored, pypiron is a
relay: it carries PyPI's files and provenance across unchanged and does not
re-verify them. Trusting a mirrored package is trusting PyPI. The provenance
travels as a `<filename>.provenance` companion (advertised by a `provenance`
URL in JSON and a `data-provenance` attribute in HTML). Consumers verify it
end-to-end and offline — Sigstore bundles check against a cached trust root with
no egress — so even an air-gapped build confirms the original publisher. pypiron
never runs Sigstore or mints provenance itself, so it refuses a direct upload
carrying first-party attestations.

**Trusted — the release pipeline.** GitHub Actions builds every release, with
every action pinned to a full commit SHA and every build run `--locked` against
a committed lockfile. Each wheel, sdist, binary, and image ships a signed
provenance attestation you can check yourself — see
[Verify a release](#verify-a-release).

## Login throttling

A client hammering login with candidate secrets is bounded, not just logged.
Five failed logins from one address and that address can't log in for five
minutes; the server refuses even a correct guess during the lockout, so a
guesser can't confirm a hit. pypiron never counts successful logins and never
throttles anonymous traffic, so the lockout can't be turned against clients
that aren't guessing. A fleet of N replicas bounds a guesser at N× one instance's rate —
each instance enforces its own budget. Tune or disable with
`--login-cooldown-secs` — [configuration](reference/configuration.md).

## What pypiron does not defend

These are out of scope by design; know them before you lean on the rest.

- **A stolen storage credential.** Whoever holds your object-storage keys can
  rewrite an artifact and its recorded hash in one motion. Guard that credential
  like the root secret it is.
- **The trustworthiness of upstream PyPI.** For mirrored public packages, pypiron
  relays PyPI's provenance; it never mints or re-verifies attestations itself. A
  malicious upload that PyPI accepted and signed is one pypiron will carry across.
  Your own private uploads are a separate world you control.
- **Request floods.** pypiron throttles failed logins itself (see above), but
  volumetric floods — hammering the index, download, or metadata endpoints —
  are the edge's job: put a request-rate limit on your reverse proxy or load
  balancer. The access log gives an edge ban its signal: failed logins appear
  as `401` events and throttled attempts as `429`s at `info` level (keep
  `pypiron::access=info` if you feed them to fail2ban or a SIEM — raising it to
  `warn` keeps only 5xx and drops them); key any ban on the real peer address,
  or trust the logged client field only when pypiron sits behind a proxy with
  `--trusted-proxy`, since otherwise an attacker sets that `X-Forwarded-For`
  themselves.

## pypiron's own dependencies

Every change runs `cargo audit` with no ignore flags. The audit is clean.

It was not always. Through v0.0.14 pypiron carried two denial-of-service
advisories in `quick-xml` (RUSTSEC-2026-0194, quadratic parsing on duplicate
attributes; RUSTSEC-2026-0195, unbounded allocation on namespace declarations),
pulled in through `object_store` — the library pypiron uses to talk to S3, GCS,
and Azure. The vulnerable code parsed only XML from the storage endpoint you
configured and authenticated to, never anything from a package client, and the
default disk backend never invoked it at all. No released `object_store`
allowed the fixed `quick-xml`, so this page documents them instead of hiding
them behind audit exceptions, with CI set to fail the day a fix
shipped.

That day came: `object_store` 0.14.1 allows the fixed `quick-xml` 0.41, pypiron
took the bump, and releases after v0.0.14 carry no known advisories.

## Verify a release

Every wheel, sdist, release binary, and container image ships a signed
build-provenance attestation — proof this repository's CI built it and nobody
has swapped it since. Check one with the GitHub CLI:

```bash
# A wheel or sdist you downloaded from PyPI
gh attestation verify ./pypiron-<version>-<platform>.whl \
  --repo blackthorn-interstellar/pypiron

# A release binary
curl -LO https://github.com/blackthorn-interstellar/pypiron/releases/latest/download/pypiron-x86_64-unknown-linux-musl.tar.gz
gh attestation verify pypiron-x86_64-unknown-linux-musl.tar.gz \
  --repo blackthorn-interstellar/pypiron

# The container image, checked by digest without pulling it
gh attestation verify oci://ghcr.io/blackthorn-interstellar/pypiron:latest \
  --repo blackthorn-interstellar/pypiron
```

**Exit status 0 is the signal** — the artifact's digest matched an attestation
issued by this repo's GitHub Actions. A non-zero exit means it didn't; treat the
artifact as unverified.

Three things it needs:

- **GitHub CLI 2.49 or newer.** Older builds (2.21, for one) have no `attestation`
  command at all.
- **Network and a login.** Verification fetches the attestation from GitHub, so it
  needs egress and `gh auth login`. Air-gapped? Download the attestation on a
  connected machine with `gh attestation download`, then verify offline against it
  with `--bundle`.
- **A public repository.** Attestation is a public-repo feature; it works here
  because this repository is public. If it ever goes private, verification stops
  until it's public again.
