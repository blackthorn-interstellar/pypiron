---
title: Core concepts
description: One index URL, three package sources, a folder or a bucket, and the defaults that keep bad packages out — the whole pypiron model on one page.
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

## Three sources, one index

Packages reach that URL three ways. Use one, or all three at once.

### You publish them

Publish with the tool you already use — twine, uv, poetry, hatch, flit. Nothing
about your build changes except the URL:

```bash
uv publish --publish-url https://your-server/legacy/
```

Filenames are write-once: the server refuses a re-upload even with identical
bytes, so nobody swaps a build under a version someone already installed. Yank
hides a file from resolvers while pinned installs keep working.

### Someone installs them

Public packages arrive as clients ask for them: the first request for a file
fetches it from PyPI, checks it against upstream's hash, and keeps it. Every
install after that serves from your own storage.

```bash
pypiron serve --proxy-upstream https://pypi.org
```

Off until you set it. Once a file is cached, PyPI outages stop mattering — a
corrupt or truncated upstream response is discarded, never cached.

### You pull them in first

Sync a slice of an upstream index ahead of time — this is how you run
air-gapped: sync on a connected machine, carry the storage in, serve with no
upstream at all.

```bash
pypiron sync --from https://pypi.org/simple/ --to https://your-server/ \
  --admin-user admin --admin-pass "$ADMIN" \
  --include-package numpy --include-package "requests>=2.20"
```

Choose the slice by name and version range, wheel platform and Python tag,
format, size, age, or pre-release status. Re-runs skip what you already hold,
and upstream yanks follow through, so what you serve keeps matching upstream.

Put the list in `pypiron.toml` and it drives both the mirror and the cache, so
the two can't drift apart. [Every selector](reference/configuration.md) ·
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

**Backups are a file copy.** The package files are the only thing worth
keeping; the index is rebuilt from them. Restore the directory anywhere, run
`pypiron rebuild-index`, and every package installs byte-identical. Worst-case
loss is your backup interval.

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

Nothing to revoke — a token expires on its own. Tag any username
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

The first two are on by default. [Security](security.md) has all three in
depth.

## What it tells you

- **A bad node leaves rotation.** Restart policy on `/health`, load balancer on
  `/ready` — a storage blip pulls the node out of rotation instead of
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
