# How it works

pypiron serves a package index like a CDN serves a website — mostly static
files. That's why it's fast and hard to break.

## The index rebuilds itself

The index is generated from the package files, so it always rebuilds. Lose it,
corrupt it, or restore an old backup — pypiron repairs itself. Nothing to back
up but a directory (or a bucket).

Uploaded files never change. Once a filename exists it can't be replaced — the
wheel pinned in your lockfile is the same wheel forever.

The one failure pypiron guards against: an index that lists a file which isn't
there — a broken install. Repairs only move toward safety, so a half-finished
upload or a crash mid-write still leaves the index correct. To force it,
`pypiron rebuild-index` regenerates every index from the files on disk;
`pypiron verify-index` checks them without changing anything.

## Every wheel downloads once

Serving an install is serving files — no database query, nothing shared. The
biggest cache is the client: uv and pip download a given wheel once, ever, and
pypiron marks artifacts permanently cacheable since their bytes never change.
Index pages revalidate cheaply, so an up-to-date client pays almost nothing.

On cloud storage, pypiron can hand the download straight to the bucket. The node
serves the index; storage serves the wheel bytes.

## Add nodes without coordination

Reads share nothing, so point any number of nodes at one bucket. No coordination.
One node rebuilds the index at a time, as an optimization — the nodes settle that
through the bucket, and since every rebuild produces the same result, a brief
overlap repeats work but can't corrupt anything. (The disk backend is
single-node; multiple nodes need cloud storage.)

!!! note
    One URL and one namespace serve your private uploads, synced mirror
    packages, and on-demand proxied packages together. Each name is private or
    public, never both. See [Add public PyPI](../guides/setup.md#add-public-pypi).

The full storage layout — every path and metadata file — is in
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md).
