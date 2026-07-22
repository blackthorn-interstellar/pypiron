---
description: Stop dependency-confusion and malicious fresh releases at your PyPI server. Private names stay private, new releases wait 7 days, on by default.
---

# Supply-chain defense

Mixing private packages with a public PyPI mirror is where supply-chain attacks
slip in. pypiron closes those doors at the server, so every client behind it —
uv, old pip, poetry, CI, a lockfile from two years ago — gets the same
protection at once, on by default. It also reports where you're still exposed,
ranked by what your org installs most.

| Threat | How pypiron stops it |
| --- | --- |
| Dependency confusion | Each name is private or public, never both. Private names stay reserved. |
| Malicious fresh releases | New releases wait seven days — long enough for a bad one to surface. |
| Known malware | Files a public advisory flags as malicious are refused: no install, no cache. |
| A release pulled upstream | When PyPI quarantines or removes a project, pypiron stops serving it too. |
| Tampered or unattributed files | Filenames are never overwritten. PyPI's provenance travels with each file. |

## Dependency confusion

The trap: an internal package name also exists on public PyPI, and a resolver
pulling from both indexes chooses the public copy.

First defense: point clients at this one index. With uv use
`--default-index`; with pip use `--index-url`, not `--extra-index-url
https://pypi.org/simple`. Let the server decide what exists.

pypiron enforces the rest: **each name is private or public, never both.** The
first upload — a private push or a mirror sync — reserves the name for that
world. It stays reserved.

- A private upload to a mirror-owned name is rejected.
- `sync` refuses a name you already own privately.
- Collisions are hard errors, never merges — a package belongs to exactly one
  world, so its index never mixes private and upstream files.

Deleting every file of a package does not release the name. Repurposing a name
takes direct operator action in storage.

### Reserve a namespace

`--private-prefix` reserves a namespace (e.g. `acme-*`) for private uploads and
forbids `sync` from touching it. Intent is auditable, and an internal package
can't be published under a name that later collides upstream. Matching is on
normalized names, so `acme_foo`, `acme.foo`, and `acme-foo` are the same name.

```bash
pypiron serve --admin-pass "$ADMIN" --private-prefix acme
```

Claim a private name by uploading under the prefix. The name never falls through
to upstream:

=== "uv"

    ```bash
    uv publish --publish-url http://HOST:8080/legacy/ \
      --username admin --password "$ADMIN" dist/*
    ```

=== "twine"

    ```bash
    twine upload --repository-url http://HOST:8080/legacy/ \
      -u admin -p "$ADMIN" dist/*
    ```

See [Setup → Private packages](../guides/setup.md#private-packages) and
[Setup → Add public PyPI](../guides/setup.md#add-public-pypi).

## New releases wait

A compromised maintainer account or a typosquat is most dangerous in its first
hours, before anyone notices. A **dependency cooldown** puts a window between
when a release is published and when pypiron will serve it, so resolution lands
on versions old enough for a bad one to have surfaced and been pulled. It's the
same practice uv, npm, and Dependabot have standardized on — enforced here at
the server, for every client at once.

**On by default: new releases wait seven days, on a sliding window.** The proxy
applies it on every read (re-checked per request, so the window keeps sliding);
`sync` applies it to what a run mirrors. Widen the window, pin an absolute "as
of" date, or disable it entirely:

```bash
pypiron serve --admin-pass "$ADMIN" \
  --proxy-upstream https://pypi.org \
  --exclude-newer "7 days"     # the default; pass "30 days" to widen
```

```bash
pypiron sync --exclude-newer "2026-01-01T00:00:00Z"   # pin an absolute cutoff
pypiron sync --exclude-newer ""                        # disable: mirror everything
```

All `<when>` formats — durations, absolute timestamps, what slides versus what
stays pinned — live in [Configuration → Mirror
selection](../reference/configuration.md#mirror-selection).

!!! note "Only admins can backdate"
    An ordinary upload can only claim its receipt time, so a publisher can't
    sneak a package in under a cutoff. Setting any other timestamp — including
    mirror uploads that carry PyPI's original time — requires the admin
    credential; with none configured, those uploads are refused.

## Known malware never installs

A cooldown buys time; it can't catch what's already confirmed bad. Some releases
aren't merely risky — they're catalogued malware, in the public advisory
databases (the same source `uv audit` reads). pypiron refuses to serve those
files. Ask for a flagged version and the download is refused, naming the
advisory that condemned it; the on-demand proxy won't fetch and cache one in the
first place. On by default — you don't turn it on.

This closes a gap every caching mirror has. When a resolver locks a dependency
through pypiron, it pins a download URL to *this* server. Later PyPI pulls a
compromised release: it vanishes from pypi.org, but every lockfile in your org
keeps asking pypiron for the copy it cached — and a plain mirror keeps handing
it back, forever. pypiron checks the advisory feed at the door instead, so the
file it served yesterday stops today — a fresh uv run and a two-year-old lockfile
alike. One place to fix it; every client fixed.

A new advisory lands fast. Every node also watches OSV for individual
just-published malware advisories and starts blocking within minutes of
publication — no waiting for the next daily refresh, and clients whose own audit
cache still won't know for up to a day are already covered at the door. The full
advisory snapshot still refreshes daily; a withdrawn advisory un-blocks on that
same daily cadence.

A private package that happens to share a malicious public name still installs.
pypiron blocks only the public package the advisory names — a private name of
your own is, by construction, not that package.

Availability wins over freshness. If the feed source goes unreachable, clean
packages keep installing and nothing slows down; blocking just stops getting
fresher, and a staleness gauge climbs so you can alert before it drifts. It
degrades toward stale, never toward down. A blocked download is itself worth an
alert: it means some machine in your fleet is still asking for malware.

Blocking can be turned off, or the whole advisory feature disabled, from
[Configuration → Server](../reference/configuration.md#server).

## Pulled upstream, pulled here

PyPI sometimes quarantines a project outright — a compromised account, a malware
finding — freezing it so nothing new resolves. pypiron relays that state. A
quarantined project lists no files here, and a direct download of one is
refused, so a URL already pinned in a lockfile can't route around the empty
listing. When a single file is removed upstream, `sync` marks it withdrawn the
same way. Whatever PyPI stops standing behind, pypiron stops serving.

## Provenance and immutability

A filename, once uploaded, is never replaced (PyPI's own rule); pypiron rejects
re-uploads of an existing filename. Nobody can swap bytes under a version already
in someone's lockfile.

For attribution, PyPI's provenance travels with each package. When `sync` or the
proxy carries a file across, PyPI's already-verified attestation comes along as a
`<filename>.provenance` companion (advertised by a `provenance` URL in JSON and a
`data-provenance` attribute in HTML). Consumers verify it end-to-end and
offline — Sigstore bundles check against a cached trust root with no egress — so
even an air-gapped build confirms the original publisher.

pypiron is a relay, not a verifier: it never runs Sigstore or mints provenance,
so a direct upload carrying first-party attestations is refused.

## Know what you're exposed to

Blocking stops malware. It says nothing about the ordinary vulnerabilities
already sitting in packages you host — those are reported, not blocked, because
blocking on a CVE would break your pinned builds by Tuesday. The audit is that
report: every hosted package a known advisory affects, with its severity and the
version that fixes it, **ranked by how often your org installs it.** The top of
the list is your real exposure — the vulnerable thing everyone pulls a hundred
times a day, not the one nobody touches.

No client-side tool can produce this. `uv audit` answers "what does *this*
project depend on"; pypiron already holds the whole corpus, every download
count, and the same advisory feed, so it answers "what does the *org* host and
install." It's a plain page at `/audit` (with a JSON twin for scripts), and each
project's own page carries the advisory list for that package. Admin-only: a
ranked list of your soft spots is exactly what an attacker would want, so it
rides the strongest credential.

## The air-gapped endgame

The proxy still talks to live PyPI on a cache miss. `sync` removes that surface:
pre-load an approved package list from a host with egress, then serve from a node
without egress.

Malware blocking crosses the same gap. A server with no internet can't fetch the
advisory feed itself, so you deliver it. A pypiron-to-pypiron `sync` ferries the
feed alongside the packages on its own; against a public source, point `sync` at
the OSV export on the connected side:

```bash
pypiron sync --to http://airgapped:8080 \
  --advisory-feed https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip
```

No sync in the picture? Point the server's own `--advisory-feed` at a local file
and have your ferry drop a fresh copy there on whatever schedule it runs. However
the feed arrives, blocking and the audit behave identically — an unfed
air-gapped box says so in its logs until the first feed lands, then arms itself
without a restart.

A server with egress blocks a new advisory within minutes of publication (it
watches OSV directly); a ferried, air-gapped mirror is only as fresh as its last
delivery, so run `pypiron sync` on an hourly cron for hourly baselines.

## See also

- [Configuration → Server](../reference/configuration.md#server) — the advisory
  feed and malware-blocking knobs, the `/audit` and feed endpoints, and the
  metrics to alert on.
- [Configuration → Mirror selection](../reference/configuration.md#mirror-selection)
  — the shared cooldown surface, with env-var and `[mirror]`-table equivalents.
- [How it's tested](testing.md) — how these defenses are verified: chaos, fuzzing,
  and a full-PyPI parser check.
