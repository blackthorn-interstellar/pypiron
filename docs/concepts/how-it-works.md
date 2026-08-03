---
description: Why a self-hosted PyPI server is hard to break and easy to scale: immutable files in your bucket, an index that rebuilds itself, downloads served by storage.
---

# How it works

pypiron stays reliable and scales because everything it serves lives in your
bucket as files that never change — and everything else can be rebuilt from
them. Backup, recovery, and scale-out fall out of that one fact.

## Nothing to back up but the bucket

The package files are the entire state. The index is generated from them, so it
always rebuilds: lose it, corrupt it, restore a stale backup — pypiron repairs
it. Whatever protects the bucket (or the directory, on the disk backend)
protects everything.

Uploaded files never change. Once a filename exists it can't be replaced, so the
wheel pinned in a lockfile today is the same wheel in five years — no restore,
rebuild, or re-upload can put different bytes under a name your builds already
trust.

The one failure pypiron guards against: an index listing a file that isn't
there — a broken install. Repairs only move toward safety, so a crash mid-upload
still leaves the index correct. To force it, `pypiron rebuild-index` regenerates
every index from the stored files. `pypiron verify-index` checks them without
changing anything.

## Every wheel downloads once

Serving an install is serving files — no database query, nothing shared. Because
package files never change, pypiron marks them permanently cacheable: uv and pip
download a given wheel once, ever. Index pages revalidate cheaply, so an
up-to-date client pays almost nothing.

On object storage, pypiron hands the download straight to the bucket. The node
serves index pages — kilobytes. The bucket serves wheel bytes — megabytes.

### uv and pip download differently

uv and modern pip read the small metadata file pypiron serves next to each
wheel, so resolving dependencies never downloads a wheel.

Downloads differ. uv caches wheels by index and filename, so pypiron sends it
straight to the bucket. pip caches by URL — and a direct-to-bucket link is
freshly signed on every request, so redirecting pip would defeat its cache and
every fresh venv would re-download everything. The default
(`--artifact-delivery auto`) redirects clients verified to handle it (uv) and
streams everyone else from the node. Index pages always list the stable
`/files/` URL, so a lockfile never captures an expiring link.

A uv fleet pulls its bytes from the bucket and barely touches the node; a
pip-heavy fleet streams through the node, so size its network accordingly.
Force one behavior with
[`--artifact-delivery`](../reference/configuration.md#server).

## Any number of nodes, no coordination

Reads share nothing, so point any number of nodes at one bucket. No
coordination. One node rebuilds the index at a time, as an optimization — the
nodes settle that through the bucket, and since every rebuild produces the same
result, a brief overlap repeats work but can't corrupt anything. (The disk
backend is single-node; more than one node needs object storage.)

More than one bucket rides out a region or cloud outage:
[Survive a region or cloud outage](../guides/multi-region.md).

---

How private uploads, synced mirror packages, and on-demand proxied packages
share one URL and one namespace: [Package sources](package-sources.md). Backends
and credentials: [Storage](storage.md). The full storage layout — every path and
metadata file — is in
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md).
