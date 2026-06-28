# How it works

pypiron is a static site generator wearing a PyPI costume. A package index has an
extreme read/write ratio, so pypiron skips the dynamic server entirely: it serves
files and regenerates the index from those files. No database.

## Files are truth

Two kinds of state live in storage, and only one of them is authoritative.

| Layer | What it is | Authoritative? |
| --- | --- | --- |
| Artifacts + sidecars | Wheels and sdists, plus per-file metadata written at upload time | Yes |
| Simple index | PEP 503 HTML and PEP 691 JSON under `simple/` | No — a view |

The artifact is immutable: once a filename exists it can never be replaced. Next
to each one sits a sidecar capturing what was known at write time — sha256, size,
version, upload time, requires-python, yank flag, extracted PEP 658 `METADATA`.
Anything that can't be derived from an artifact (a yank flag, a true mirror
timestamp) lives in the tree as a sidecar, so the system can always heal.

The simple index is a materialized view. It is derivable from a storage listing at
any time, so it can be thrown away and rebuilt.

## Views may lag truth, never lead it

Ordering is the one invariant that keeps `pip install` honest.

- **Upload:** write the artifact, then the sidecar, then the index.
- **Delete:** remove from the index, then delete the artifact.

An unlisted-but-present file is invisible — harmless. A listed-but-missing file is
a broken install — harmful. The reconciler only ever repairs in the harmless
direction.

## How rebuilds happen

There is no queue. A writer drops a small **dirty marker** before touching truth
and another after. A single worker lists the markers, rebuilds each marked package
from a fresh listing, then deletes the markers it observed. Rebuilds read sidecars
only — O(files), not O(bytes).

Because rebuilds are pure regenerations from a listing, they are idempotent: any
node can rebuild any package at any time and get the same answer. That one property
does most of the work. Lost a marker? A periodic full reconcile flat-lists the
corpus and repairs the diff, so events only accelerate healing rather than being
required for it. Worst case, `pypiron rebuild-index` regenerates every view from truth and
`pypiron verify-index` checks them read-only.

## Reads need zero coordination

Serving a read is stateless file serving. It scales horizontally for free and is
cache-correct, not cache-dependent:

- **Artifacts** are served `immutable` and cached forever — a filename can never be
  re-uploaded, so the bytes never change.
- **Indexes** are ETag-revalidated, so a stale client pays one cheap 304.

The biggest cache is the client itself. uv and pip download a given wheel exactly
once, ever. On cloud backends, redirect-safe clients are 302'd to a presigned URL
so the node never touches wheel bytes. See [Artifact delivery](artifact-delivery.md).

## Multi-node

Only the index writer is singular, and only as an optimization. Nodes coordinate
through a TTL lease object in the bucket using conditional writes — sloppy by
design. Because rebuilds are idempotent, a few seconds of split-brain just
duplicates work; it can't corrupt anything. No Raft, no fencing tokens. The disk
backend is single-node; multi-node implies a cloud backend.

!!! note
    One URL and one namespace serve private uploads, synced mirror packages, and
    on-demand proxied packages together. Each package belongs to exactly one
    world, claimed at first write. See [Mirroring](mirroring.md) and
    [Add public PyPI](../guides/deploy.md#add-public-pypi).

The full storage-layout contract — every path, the sidecar schema, the marker
format — is documented in
[DESIGN.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md).
