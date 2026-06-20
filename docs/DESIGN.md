# Design

The one-sentence version is in [VISION.md](VISION.md). This document is the long-form
reasoning: why the architecture works, what its load-bearing properties are, and the
honest accounting of what it gives up.

## The core insight: a static site generator wearing a PyPI costume

A PyPI index has maybe the most favorable read/write ratio of any service in
existence. pypi.org itself proves the model — virtually all of its traffic is served
by Fastly as cached static content; the dynamic app is a tiny island behind a CDN.
PypIron skips the dynamic island entirely. That's not a hack, it's the honest
architecture.

Framed as a static site generator:

- **Truth** lives in the `packages/` tree: artifacts plus write-time metadata
  sidecars (hashes, name/version, yank flags, extracted core metadata).
- **Views** live under `simple/`: the PEP 503 HTML and PEP 691 JSON indexes,
  materialized files derivable from a storage listing at any time.

Every design question answers itself from that framing. Any state that cannot be
derived from the packages tree (e.g. a yank flag) must *live in* the packages tree
as a sidecar file — otherwise it isn't truth, and the system can't heal.

## The load-bearing property: idempotent rebuilds

Index rebuilds are pure regenerations from a listing. Any node can rebuild any index
at any time and get the same answer. This single property does most of the
architectural work:

- **Split-brain is harmless.** Two workers rebuilding the same package index do
  redundant work and converge. So leader election can be *sloppy* — it is a cost
  optimization (avoid duplicate LISTs/PUTs), not a correctness requirement.
- **Events cannot be lost.** Markers are unique create-only keys written around
  every truth change (intent before, commit after) and only consumed after the
  work they announce is done. At-least-once processing is free because rebuilds
  derive from truth; crash anywhere and the replay converges. Proven by the
  crash-point sweep in tests/test_crash_consistency.py.
- **Recovery is trivial.** Worst case, `pypiron resync` regenerates every view
  from truth.

## Ordering invariant: views may lag truth, but must never lead it

- Upload: write artifact, then sidecar, then index.
- Delete: remove from index, then delete artifact.

An unlisted-but-present file is invisible (harmless). A listed-but-missing file is a
broken `pip install` (harmful). The reconciler therefore only ever repairs in the
harmless direction.

## Write path: dirty markers, not a queue

Queue semantics (pending/, processing/, claim-by-copy) buy nothing here, because the
job payload is redundant — the truth is the storage listing. Instead:

1. A writer drops a unique, create-only **intent** marker
   (`_dirty/<pkg>!<nonce>.intent`) *before* touching truth, and a paired
   **commit** marker (`...<nonce>.commit`) *after*. Every event is its own
   key — nothing is ever overwritten.
2. The worker lists `_dirty/`, rebuilds each marked package from a fresh
   listing, updates the global index, and only **then** deletes exactly the
   marker keys it observed. Rebuild-before-delete is race-free because keys
   are unique: an event arriving during the rebuild is a new key and
   survives. A crash anywhere before the delete replays the tick — rebuilds
   are idempotent, so the only cost is repeated work, never a lost update.
3. A commit (or an intent whose pair arrived) rebuilds immediately. An
   unpaired intent younger than the grace period (`--intent-grace-secs`)
   means a writer is in flight — skip. Older means the writer crashed —
   rebuild anyway. Either way the package heals without any sweep.
4. Duplicate markers for the same package collapse into one rebuild for free.

No claim races are possible because there is nothing to claim. This deletes a whole
class of distributed-systems problems.

The **global index** (`/simple/`) only changes when the *set of package names*
changes — a new name appears or the last file of a package is deleted. The
leader keeps the name set in memory (a membership check must not cost a
corpus-sized GET), batches changes per tick, and writes back under S3
conditional writes (`If-Match`) so racing nodes reload-and-retry instead of
clobbering each other.

## Events are the backbone; the audit is the safety net

Markers carry all day-to-day freshness. What they cannot see is change pypiron
didn't make: a restored backup, manual bucket surgery, another tool writing.
For that, a periodic **audit** (leader only, default daily, plus one on boot)
flat-lists the corpus — 1,000 keys per S3 request, no per-directory listing —
and compares each package's (key, size, etag) fingerprint against the one
stored at its last rebuild in `_state/fp-*.json`. Unchanged packages cost
zero reads; only the diff gets rebuilt. Audit cost scales with churn, not
corpus size: a full-PyPI-sized tree (17M files) audits for ~$0.25 of LIST
requests instead of ~$11 of GETs per old-style sweep. `pypiron resync` is the
same pass with fingerprints ignored — the rebuild-the-world button. `pypiron
verify` is its read-only twin: recompute everything, diff, exit nonzero.

## Write-time metadata capture: never compute at read or rebuild time

The upload request already provides `name`, `version`, and `sha256_digest` as form
fields (twine and uv send them). Verify the hash on ingest, then persist it in a
small sidecar next to the artifact. Rebuilds then only LIST and read sidecars —
O(files), not O(bytes). This also avoids inferring names from filenames, which is
genuinely unreliable for pre-PEP-625 sdists.

The same applies to PEP 658: extract `METADATA` from the wheel at upload time and
store it as `<filename>.metadata` — another static file. uv and modern pip use it to
resolve dependencies without downloading wheels.

**The sidecar is authoritative for `upload-time`**, with storage last-modified
(disk mtime, S3 `LastModified`) as the fallback. "mtime = upload time" is correct
by construction for direct uploads, but fragile: an rsync without `-t`, a bucket
migration, or a backup restore silently rewrites history for every package.
Sidecars make the timestamp part of the truth tree — durable and copyable. It also
makes mirrored timestamps possible (below).

Ordinary uploads never get to claim a timestamp — only receipt time.
`--exclude-newer` is a supply-chain control; letting any uploader backdate a
package would let them sneak under a cutoff. Backdating requires one of exactly
two things: storage credentials, or the *admin* credential on a server whose
operator configured one (`--admin-user`/`--admin-pass`). PypIron has two roles
— **uploader** (publish private packages) and **admin** (everything an uploader
can, plus mirror uploads, deletion, and yank); admin is a strict superset.
Backdating is an admin privilege, so the people who publish private packages
are not, by default, the people who can rewrite history. Both boundaries are
operator-controlled; neither is reachable from a default deployment.

## Mirroring: carry forward true timestamps

A mirrored package must serve *PyPI's* upload time, not the mirror time —
otherwise every mirrored file looks brand-new and `--exclude-newer` is useless.
PyPI publishes the true timestamp per file (`upload-time` in its PEP 700 JSON), so
the data is free; the design question is where it enters the system.

The recommended path is **mirror-over-HTTP**: `sync --to <server>` POSTs each
file to `/legacy/` with `mirror=true` plus PyPI's `upload_time` and yank state
as form fields, authenticated against the admin credential. The server — and
only the server — writes storage: it claims the package `mirror`-origin,
persists the provided timestamp in the sidecar, and extracts PEP 658 metadata
from the wheel like any other upload. This keeps deployment simple (sync needs
a URL and the admin credential, nothing else), keeps the storage layout a
server-internal concern (no version coupling between a fleet of sync clients
and the server), and keeps one writer.

Mirror uploads are an admin operation: a `mirror=true` request must
authenticate as admin (`--admin-user`/`--admin-pass`), so an ordinary uploader
cannot backdate. With no admin credential configured, mirror uploads — like
deletion and yank — are disabled, so a stock server never accepts a client
timestamp.

`sync` can also write **directly to storage** (no `--to`): same binary, same
storage code — artifact, sidecar carrying PyPI's digest and timestamp, dirty
marker. This needs no server cooperation at all and suits bucket-credential
environments (a cron job next to the bucket, an airgapped import).

## Private + mirrored packages: dependency confusion

Mixing private packages with mirrored PyPI packages is the classic dependency
confusion surface (Birsan, 2021): if resolution can ever consult public PyPI for a
name you use privately, an attacker publishes that name — or a higher version of
it — publicly and wins. Defense in four layers, ordered by importance:

1. **Closed-world resolution.** Clients point at this registry *only*
   (`--index-url`, never `--extra-index-url https://pypi.org/simple` — pip merges
   extra indexes by version with no priority, which is exactly the vulnerability).
   The registry decides what exists: mirror allowlist + private uploads. uv shops
   can add client-side pinning via `tool.uv.index` with `explicit = true`.
2. **Origin exclusivity (the mechanism).** Every package directory carries an
   origin marker in the truth tree — `packages/<pkg>/.origin` = `private` or
   `mirror` — claimed by first write. Private uploads to a mirror-owned name are
   rejected; sync refuses names that are private-owned. Collisions are hard
   errors, never merges. This closes the hole a prefix policy alone leaves open:
   without it, adding a private package's name to the mirror list would merge
   public files into the private package's index — pulling an attacker's
   PyPI-published version in through our own mirror. Exclusivity means each
   package belongs to exactly one world, so indexes never merge origins. The
   claim is **durable**: deleting every artifact of a package does *not* release
   `.origin`, because that would let a credentialed client empty a mirror-owned
   public name and re-upload it as private (the dependency-confusion direction).
   Re-purposing a name across worlds requires deleting the `.origin` file
   directly — an operator action gated on storage access, the right boundary.
3. **Namespace prefix policy (the guardrail).** Optionally require private uploads
   to match a configured prefix (e.g. `acme-*`) and forbid sync from touching it.
   Makes intent auditable and prevents accidentally publishing an internal package
   under a name that later collides with public PyPI. Matching is on PEP 503
   *normalized* names (`acme_foo` ≡ `acme.foo` ≡ `acme-foo`). Same concept as PEP
   752's reserved namespaces.
4. **Defensive public registration (hygiene, outside the server).** Register your
   private names or prefix stem on pypi.org itself — some laptop somewhere will
   always run `pip install` against the defaults.

All of this is one marker file and two rejection checks. No database, naturally.

## Immutability of filenames

PyPI's rule: once a filename is uploaded, it can never be replaced. PypIron adopts
it (re-uploads of an existing filename are rejected). This buys two things at once:

- **Supply-chain safety** — nobody can swap bytes under an existing version.
- **Perfect cacheability** — artifacts can be served with
  `Cache-Control: public, max-age=31536000, immutable`.

## Cache-correctness is the scale story

The server should be *cache-correct*, not *cache-dependent*:

- Artifacts: immutable, cache forever.
- Indexes: `no-cache` + ETag revalidation (or a few seconds of max-age).

The biggest "cache" is the client itself: pip and uv have local HTTP caches, so
`immutable` means a client downloads a given wheel exactly once, ever, and repeat
resolves are 304s. Corporate proxies and CI-runner caches respect the same headers
for free. A CDN is *optional* leverage for specific situations (geo-distributed
offices, a public artifact index, fronting S3 to cut egress) and bolts on with zero
changes — but the architecture claim is that one node is enough.

## Read path: zero coordination

Reads are stateless file serving and scale horizontally trivially. Per backend:

- **Disk**: stream with sendfile semantics, support Range requests.
- **Cloud (S3 / GCS / Azure)**: redirect artifact downloads (302 to a presigned
  URL) so the node only ever serves kilobytes of index while the object store
  serves the megabytes — with Range support for free. The node never holds wheel
  bytes in memory. Signing needs a credential that can mint URLs (S3 always; GCS
  with a service-account key; Azure with an account key); without one the backend
  streams instead.

Redirects collide with client caching, though. Each 302 carries a freshly
signed URL (`X-Amz-Date`/`X-Amz-Signature` differ per request), and the
redirect itself is `no-cache` because the signature expires. pip's HTTP cache
(CacheControl) keys on the per-hop URL — the final 200 gets cached under a
presigned URL that will never be requested again, so blanket redirects defeat
pip's wheel cache entirely: every fresh-venv install re-downloads everything.
uv keys its artifact cache by index + filename and is indifferent to URL
churn. Hence `--artifact-delivery` (default `auto`): redirect only clients on
a verified redirect-safe User-Agent list (uv), stream everyone else under the
stable `/files/` URL with `immutable` headers. The polarity is deliberate —
misclassifying a client as *stream* costs this node bandwidth; misclassifying
it as *redirect* silently breaks its cache. Grow the list by verified cache
behavior, not popularity. Index pages always embed the stable `/files/` URLs;
anything else would bake expiring signatures into lockfiles and cached index
pages.

The redirect path does no existence check — presigning is local HMAC math, so
answering a redirect costs zero network round trips. A request for a missing
artifact (a stale index race, or a hand-typed URL) gets a signed URL that S3
answers with its own 404. That 404-not-403 depends on the server's
credentials carrying `s3:ListBucket`, which they must anyway: index rebuilds,
dirty-marker processing, and the reconcile sweep are all built on listing.

## Multi-node: sloppy leader election

Only the index writer needs to be singular, and only as an optimization. A lease
object in the bucket with a TTL, heartbeat, and conditional writes
(`If-None-Match` / `If-Match` on PUT — native to GCS and Azure, supported by S3
since late 2024) is ~100 lines. No Raft, no fencing tokens, no correctness
proofs — because rebuilds are idempotent, dual leadership for a few seconds
merely duplicates work.

Disk backend is explicitly single-node; multi-node implies a cloud backend.

## Publish-then-install

The one real cost of async rebuild is the CI pattern: job A publishes, job B
immediately installs, and pip doesn't retry on missing versions. The fix is an
optional synchronous mode where the upload handler polls its own index until the
file appears (bounded, a few seconds) before returning 200. Read-your-writes by
waiting — dumb and effective.

## What "no DB" honestly costs

Transactions, uniqueness constraints, and queries. Mapped to PyPI features:

- **User accounts / API tokens** — the only feature that genuinely wants a
  database. For private registries, two static basic-auth credentials (an
  uploader and an admin) cover the real roles without one.
- **Search beyond `/simple/`** — deprecated upstream anyway; skip.
- **Per-package stats** — don't care.

So the no-DB claim holds for **private registries**, which is the explicitly stated
target. For a multi-tenant pypi.org clone it wouldn't, and we shouldn't try.

## Storage layout (the contract)

Everything in one tree, on disk or any cloud backend (S3, GCS, Azure). This
layout *is* the schema — treat changes to it like database migrations.

```
packages/<pkg>/<filename>                # artifact, immutable once written
packages/<pkg>/<filename>.meta.json      # sidecar (see below)
packages/<pkg>/<filename>.metadata       # PEP 658 core metadata, extracted from wheel
packages/<pkg>/<filename>.provenance     # PEP 740 provenance object, relayed verbatim from upstream
packages/<pkg>/.origin                   # "private" | "mirror" — claimed at first write
simple/index.html                        # materialized views (regenerable)
simple/index.json
simple/<pkg>/index.html
simple/<pkg>/index.json
_dirty/<pkg>!<nonce>.intent              # empty marker: a writer is touching this package
_dirty/<pkg>!<nonce>.commit              # empty marker: truth changed, rebuild now
_state/fp-<shard>.json                   # audit fingerprints: pkg -> listing hash at last rebuild
_sync/cursors.json                       # mirror-over-HTTP sync memo: pkg -> last upstream ETag
                                         #   (config-keyed). Pure cache for conditional fetch;
                                         #   never truth, never a view — delete it and the next
                                         #   sync re-fetches. Served by admin GET/PUT /sync/cursors.
_leader/lease.json                       # multi-node lease (holder, term, expires-at)
_staging/<ts>-<pid>-<filename>           # cloud only: a >64 MB upload streams here, then
                                         #   copy-if-not-exists publishes it to its final key.
                                         #   Transient (the object-store analog of disk's .tmp +
                                         #   rename); never referenced by an index. A hard crash
                                         #   mid-publish may orphan one — harmless, like a leftover
                                         #   .tmp on disk; expire with a bucket lifecycle rule.
```

`<pkg>` is always the PEP 503 normalized name. Index rebuilds include only
artifact files — sidecars (`.meta.json`, `.metadata`, `.provenance`) and dotfiles
are excluded from listings by suffix/prefix.

Sidecar schema (`<filename>.meta.json`), all captured at write time:

```json
{
  "sha256": "<hex>",
  "size": 12345,
  "version": "1.2.3",
  "upload-time": "2026-06-11T00:00:00Z",
  "requires-python": ">=3.9",
  "yanked": false
}
```

`yanked` may be `false` or a reason string (PEP 592). Rebuilds read sidecars only;
if a sidecar is missing (legacy file), the rebuild backfills it by hashing the
artifact once — create-only, so a real write-time sidecar always wins the race.
PEP 658 serving falls out of the layout: `<artifact-url>.metadata` maps directly
to the adjacent stored file. PEP 740 provenance works the same way —
`<artifact-url>.provenance` maps to the stored object. pypiron **relays**
provenance through `sync` and the proxy; it never verifies (verification is the
consumer's end-to-end job and works offline against a cached Sigstore trust root)
and never synthesizes it, so a direct upload carrying first-party `attestations`
is refused. A mirror serves a point-in-time snapshot, so the companion is treated
as immutable like the artifact it describes.

## Honest scaling limits

Measured against a fabricated full-PyPI-shaped corpus (see
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#scale-full-pypi-measured)):

- Per-package write throughput is serialized through the leader — fine, uploads are
  rare by definition.
- Global index regeneration is rare (only on package-set changes), batched per
  tick, and a multi-MB HTML file served statically with gzip is a non-event.
- Polling `_dirty/` at a ~1s tick costs pennies a day in S3 LIST requests.
- Every steady-state cost scales with what *changed*; only the audit (cheap
  LISTs) and `resync`/`verify` (explicit) scale with what *exists*.

Backups and disaster recovery are a selling point, not a feature: it's just files.
rsync it, version the bucket, done.
