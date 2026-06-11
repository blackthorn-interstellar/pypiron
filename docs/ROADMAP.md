# Roadmap

Ordered implementation plan for [DESIGN.md](DESIGN.md). Order matters: sidecars
are the foundation several later milestones depend on. Each milestone's
acceptance criterion is a blackbox test per [TESTING.md](TESTING.md) — a milestone
without its test is not done. Run `make check` and the test suite after each.

## Current state (as of 2026-06-11)

Milestones 0–2 are done.

- M0: blackbox suite — standards conformance (PEP 503/629/691/700 over HTTP),
  end-to-end `uv pip install --exclude-newer`, real-tools matrix (uv publish +
  twine upload, uv + pip install), round trips on disk and MinIO-S3, perf
  harness (`make perf`) against the release binary.
- M1: write-time sidecars — uploads verify `sha256_digest` and write
  `<filename>.meta.json` (artifact first, sidecar second, index job last);
  rebuilds read sidecars instead of re-hashing, backfilling legacy files once;
  index `upload-time` and `versions` come from sidecars.
- M2: filename immutability — re-upload of an existing filename is 409.

Also working: upload via `/legacy/` (twine/uv), PEP 503 HTML + PEP 691 JSON
indexes, PEP 629 meta tag, sha256 fragments in HTML, disk + S3 (MinIO)
backends, queue-based worker, HTTP-mode `sync`.

Known warts the roadmap removes: the queue has copy-then-delete claim
semantics (replaced by dirty markers in M4).

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
