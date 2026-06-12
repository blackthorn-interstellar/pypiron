# Roadmap

Ordered implementation plan for [DESIGN.md](DESIGN.md). Order matters: sidecars
are the foundation several later milestones depend on. Each milestone's
acceptance criterion is a blackbox test per [TESTING.md](TESTING.md) — a milestone
without its test is not done. Run `make check` and the test suite after each.

## Current state (as of 2026-06-11)

All milestones (0–12) are done.

- M0: blackbox suite — standards conformance (PEP 503/629/691/700 over HTTP),
  end-to-end `uv pip install --exclude-newer`, real-tools matrix (uv publish +
  twine upload, uv + pip install), round trips on disk and MinIO-S3, perf
  harness (`make perf`) against the release binary.
- M1: write-time sidecars — uploads verify `sha256_digest` and write
  `<filename>.meta.json` (artifact first, sidecar second, index job last);
  rebuilds read sidecars instead of re-hashing, backfilling legacy files once;
  index `upload-time` and `versions` come from sidecars.
- M2: filename immutability — re-upload of an existing filename is 409.
- M3: HTTP caching — content-hash ETags + `no-cache` on indexes (304
  revalidation), `public, max-age=31536000, immutable` on artifacts, single
  byte-range support on downloads (disk streams with seek, S3 passes Range
  through).
- M4: dirty markers — uploads drop `_dirty/<pkg>`; the worker rebuilds marked
  packages from listing and deletes markers last; the global index is rebuilt
  only when the package-name set changes. The copy-then-delete queue is gone.
- M5: reconciler — a periodic full sweep (`--reconcile-interval-secs`,
  default 300, first sweep at startup) regenerates disagreeing indexes,
  backfills missing sidecars, and prunes views whose packages vanished.
  Index writes are compare-before-write, so idempotent sweeps touch nothing.
  Lost markers are provably harmless.
- M6: deletion + yank — `DELETE /files/<pkg>/<filename>` removes the file from
  the index first, then artifact, then sidecars; `POST`/`DELETE`
  `/files/<pkg>/<filename>/yank` flips the sidecar's PEP 592 `yanked` state
  (optional body = reason). pip skips yanked unless pinned, verified live.
- M7: PEP 658/714 — wheel `METADATA` extracted at upload into
  `<filename>.metadata`, served at `<artifact-url>.metadata` (immutable),
  advertised via `core-metadata`/`dist-info-metadata` and `requires-python`
  in both index formats. uv resolves without downloading wheels.
- M8: origin exclusivity — `.origin` claimed `private` at first upload and
  released when the last file is deleted; uploads to mirror-owned names are
  403; `--private-prefix` reserves a namespace (normalized matching) for new
  private packages. The sync-side rejections land with M9's direct-storage
  sync, where sync first gains storage access.
- M9: direct-storage sync — `pypiron sync` (no `--to`) writes artifacts,
  sidecars carrying PyPI's true `upload-time`/sha256/`requires-python`/yank
  state, best-effort PEP 658 metadata companions, and dirty markers straight
  to storage via the same storage layer as the server. It claims `.origin`
  as `mirror`, hard-fails on private-owned names and on the private
  namespace. `--exclude-newer <historical date>` resolves the historically
  correct version against the mirror. HTTP mode remains behind `--to`.
- M10: S3 presigned redirects — with `--s3-presigned-redirects`, artifact
  downloads 302 to presigned URLs (1h expiry, `no-cache` on the redirect)
  so the node never streams wheel bytes; PEP 658 metadata companions keep
  streaming. Disk keeps streaming with Range.
- M11: leader election — an S3 conditional-write lease at
  `_leader/lease.json` (acquire with `If-None-Match: *`, renew/steal with
  `If-Match`, `--lease-ttl-secs` default 30, heartbeat on the worker loop).
  Worker and reconciler run on the leader only; a fresh leader reconciles
  immediately. Disk skips leasing — single-node, always leader. Failover
  verified live against two nodes on one MinIO bucket.
- M12: synchronous uploads — with `--sync-uploads`, the handler polls its own
  index (bounded by `--sync-upload-timeout-secs`, default 10) before
  returning 200, so publish-then-install CI never sees a missing version.

Post-roadmap:

- Mirror-over-HTTP: `sync --to` pushes PyPI's history (true `upload-time`,
  yank state) through `/legacy/` with `mirror=true`; the server (opt-in via
  the admin credential) owns all storage writes. Backdating stays admin-only
  for ordinary uploads.
- Sync filtering and config: `--exclude-newer`/`--exclude-older` upload-time
  bounds, PEP 440 version specifiers in the package list, and `pypiron.toml`
  layered under CLI/env. Filters gate only what a run adds.

Also working: upload via `/legacy/` (twine/uv), PEP 503 HTML + PEP 691 JSON
indexes, PEP 629 meta tag, sha256 fragments in HTML, disk + S3 (MinIO)
backends, HTTP-mode `sync`.

## Milestones

**0. Finish the blackbox test suite.** Standards-conformance test (PEP
503/629/691/700 over HTTP), end-to-end `uv pip install --exclude-newer` test,
real-tools test (twine upload, pip install), perf harness against the release
binary behind a `perf` marker with a `make perf` target. Partially in flight —
helpers and fixtures exist in `tests/`; the test files themselves don't yet.

**1. Sidecars at write time.** Upload handler verifies `sha256_digest` from the
form, writes `<filename>.meta.json` (schema in DESIGN.md) after the artifact,
before the index job. Worker reads sidecars instead of re-hashing; falls back to
hash-once-and-backfill for legacy files. Index `upload-time` prefers sidecar.
*Everything below leans on this.*

**2. Filename immutability.** Re-upload of an existing filename → 409. Test:
second upload of the same wheel fails; different file same name fails.

**3. HTTP caching.** ETag on indexes (hash of content), `Cache-Control:
public, max-age=31536000, immutable` on artifacts, `no-cache` on indexes, Range
support on artifact downloads. Test: conditional GET round-trips a 304; Range
returns 206.

**4. Dirty markers replace the queue.** Upload drops `_dirty/<pkg>`; worker lists
markers, rebuilds, deletes markers last. Global index rebuilt only when the
package-name set changes. Delete the pending/processing queue code.

**5. Reconciler.** Periodic full sweep (configurable interval): rebuild any index
whose source listing disagrees, backfill missing sidecars. This is the self-heal
backbone; after this lands, a lost marker must be provably harmless (test: delete
a marker mid-flight, reconcile fixes the index).

**6. Deletion + yank (PEP 592).** Delete: remove from index first, then artifact
+ sidecar. Yank: set `yanked` in sidecar, rebuild. Test: pip skips yanked unless
pinned.

**7. PEP 658/714 metadata.** Extract `METADATA` from wheels at upload, serve
`<artifact-url>.metadata`, emit `core-metadata` keys/attrs and
`requires-python`. Test: uv resolves without downloading the wheel.

**8. Origin exclusivity + prefix policy.** `.origin` claimed at first write;
upload rejects mirror-owned names, sync rejects private-owned names; optional
configured prefix reserved for private uploads (normalized-name matching). Tests
for both rejection paths.

**9. Sync direct-storage mode.** `sync` writes artifacts + sidecars (PyPI's
`upload-time` and sha256) + dirty markers straight to storage. Test: mirror a
package, then `--exclude-newer <historical date>` resolves the historically
correct version.

**10. S3 presigned redirects.** `/files/...` returns 302 to a presigned URL on
the S3 backend (config flag; disk keeps streaming, with Range from milestone 3).

**11. Leader election.** S3 conditional-write lease (`If-None-Match`/`If-Match`),
TTL + heartbeat. Worker and reconciler run on the leader only. Sloppy by design —
correctness never depends on single leadership.

**12. Synchronous upload mode.** Optional: upload handler polls its own index for
the new file before returning 200 (bounded wait), for publish-then-install CI.

## Non-goals

See the out-of-scope rows in [STANDARDS.md](STANDARDS.md): PEP 694/708/740,
search, the pypi.org `/pypi/<pkg>/json` API, user accounts/databases.
