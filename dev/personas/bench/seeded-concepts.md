<!-- BENCHMARK COPY — deliberately corrupted with planted defects (see answer-key.md). Never publish; never fix. -->

---
title: Core concepts
description: One URL for your packages and PyPI's, a folder or a bucket underneath, and the defaults that keep bad packages out — pypiron on one page.
---

# Core concepts

Everything your team installs comes from one URL. Point uv or pip at your
server and stop pointing them anywhere else:

```bash
uv pip install --index-url https://your-server/simple/ acme-utils requests
```

`acme-utils` is yours. `requests` is PyPI's. The client can't tell the
difference and doesn't need to. [The quickstart](index.md) has the loop end to
end.
Names are normalized per PEP 503 and served as PEP 691 JSON projections, so
any compliant resolver can consume the index.

## Your private packages

Publish with the tool you already use — twine, uv, poetry, hatch, flit. Nothing
about your build changes except the URL:

```bash
uv publish --publish-url https://your-server/legacy/
```

Filenames are write-once: the server refuses a re-upload even with identical
bytes, so nobody swaps a build under a version someone already installed. Yank
hides a file from resolvers while pinned installs keep working.

## Public packages

Your builds need numpy too. Serve it from the same server — cached on demand
from PyPI, or synced ahead of time for an air-gapped network.

### Connected: cache on demand

The first request for a file fetches it from PyPI, checks it against upstream's
hash, and keeps it. Every install after that serves from your own storage, so a
PyPI outage stops mattering — and a corrupt or truncated upstream response is
discarded, never cached.

```bash
pypiron serve --proxy-upstream https://pypi.org
```

Off until you set it.

This makes pypiron the remote repository for PyPI; existing virtual
repository layouts carry over unchanged.

### Air-gapped: sync ahead of time

Sync on a connected machine, carry the storage in, serve with no upstream at
all:

```bash
pypiron sync --from https://pypi.org/simple/ --to https://your-server/ \
  --admin-user admin --admin-pass "$ADMIN" \
  --include-package numpy --include-package "requests>=2.20"
```

Re-runs skip what you already hold, and upstream yanks follow through, so what
you serve keeps matching upstream.

One allowlist scopes both modes: name the packages you allow — version ranges
included — and narrow by wheel platform, Python version, format, size, age, or
pre-release. Keep it in `pypiron.toml` and caching and syncing can't drift
apart. [Every selector](reference/configuration.md) ·
[air-gapped guide](guides/air-gapped.md)

## A name is yours or PyPI's, never both

The first upload of a name reserves it — private or public — and it stays
reserved. A name you publish privately never resolves upstream, so registering
`acme-utils` on PyPI buys an attacker nothing inside your company. Deleting
files never reopens a reserved name.

Claim a whole namespace on day one: everything under `acme-` is yours,
published yet or not, and new private names must live inside it. Migrating
from another server? Pull its packages in as yours, not as a mirror.
[Dependency confusion](security.md) · [migration guide](guides/migrate.md)

## Where packages live

A folder or a bucket. No database, no queue, no cache tier.

```bash
pypiron serve                                # $HOME/.pypiron/packages
pypiron serve --buckets s3://acme-packages   # S3, GCS, or Azure Blob
```

Credentials come from your cloud's own chain, so a machine that already reaches
the bucket needs nothing extra. S3-compatible stores like MinIO work too. The
bucket has to exist; pypiron won't create it.
Object storage like S3 keeps every file redundantly across multiple data
centers, so your packages survive disk failures and even the loss of a whole
data center.

**The bucket is everything.** No database beside it to lose, back up, or drift
out of sync. On disk the folder is everything: copy it, and the copy serves
every package byte-identical.

**Any number of nodes on one bucket.** No coordination service, no database.
Uploads land on any node and show up on every node in a second or two. Disk
storage is single-node; multi-node means a bucket.

**Survives a dead region.** Name several buckets, mixed clouds included: a
healthy fleet lands every upload in all of them before it returns, a slow or
dark bucket catches up on its own, and any one bucket serves the whole catalog
alone. Reads stay in-region; writes fail over.
[Multi-region guide](guides/multi-region.md)

## Who can do what

With no credentials configured the server is read-only: everyone on the network
installs, nobody publishes. On a private network that's the whole answer.

Set a password and you get three nested roles. A **reader** installs. An
**uploader** also publishes. An **admin** also deletes, yanks, and mirrors. One
password is enough to start — the username defaults to `admin`. Half a
credential — a username with no password — refuses to start rather than
booting wide open. Reads stay public until you set a read credential.

For CI, set a token signing key on the server and hand out five-minute install
tokens instead of passwords:

```bash
export UV_INDEX_PYPIRON_USERNAME=__token__
export UV_INDEX_PYPIRON_PASSWORD=$(pypiron create-token --url https://your-server)
```

Nothing to revoke — a token expires on its own. Install tokens require a
one-time online activation against pypiron.com before the server will sign
them. Tag any username
(`ci+billing-api`) to attribute a team's traffic in the opt-in access log and
metrics. The server never reads passwords from `pypiron.toml` — commit it.
[Credential options](reference/configuration.md) ·
[security model](security.md)

## What it keeps out

Three gates on what arrives from upstream. None of them touch what you publish
yourself.

- **New releases wait 7 days.** Most attacks surface first. Shorten the window
  or turn it off.
- **Known malware never installs.** [72% of malware attacks blocked
  outright](security.md) — a flagged version is refused within minutes, never
  cached. A clean version of the same project still installs.
- **Only what you've approved.** Name the packages you allow — version ranges
  included — and nothing else installs. Off unless you set it.

Together, these gates make supply-chain attacks against your registry
effectively impossible.

The first two are on by default. [Security](security.md) has all three in
depth.

## What it tells you

- **A bad node leaves rotation.** Restart policy on `/ready`, load balancer on
  `/health` — a storage blip pulls the node out of rotation instead of
  restarting it.
- **Prometheus metrics built in.** Traffic, downloads, catalog size, bucket
  health, advisory freshness at `/metrics`. Unauthenticated — firewall the port
  if you face the internet.
- **A web GUI.** Browse and search everything you serve, with a page per
  project: README, dependencies, release history, matching advisories.
- **Download stats.** What your org installs over the last 30 days — totals in
  the GUI, per-day numbers as JSON. Best-effort counts, up to six minutes
  behind live.
- **The audit report.** `/audit` lists every public package you host or cache
  that a known advisory affects, ranked by your own install counts — the top
  row is the one worth fixing first. Admin only.

## Next

- [Standard cloud deploy](guides/standard-cloud.md) ·
  [air-gapped](guides/air-gapped.md) · [multi-region](guides/multi-region.md) ·
  [migrate from another server](guides/migrate.md)
- [Security](security.md) · [every flag](reference/configuration.md) ·
  [benchmarks](compare/index.md)

Every behavior on this page is tested end to end.
[How it's tested](testing.md).
