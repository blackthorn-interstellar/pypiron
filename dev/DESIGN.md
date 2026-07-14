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
- **Recovery is trivial.** Worst case, `pypiron rebuild-index` regenerates every view
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
   key — nothing is ever overwritten. A cross-bucket operation holds one
   unique root on each bucket and rotates through `root~<seq>` family members
   while it runs, creating the next pair before committing the old pair.
2. The worker lists `_dirty/`, rebuilds each marked package from a fresh
   listing, updates the global index, and only **then** deletes exactly the
   marker keys it observed. Rebuild-before-delete is race-free because keys
   are unique: an event arriving during the rebuild is a new key and
   survives. A crash anywhere before the delete replays the tick — rebuilds
   are idempotent, so the only cost is repeated work, never a lost update.
3. A commit (or an intent whose pair arrived) rebuilds immediately. An
   unpaired intent younger than the grace period (`--intent-grace-secs`)
   means a writer is in flight — skip. Older means the writer crashed —
   rebuild anyway. Age comes from the object's storage timestamp; a missing or
   malformed timestamp stays live rather than licensing an unsafe takeover.
   Either way the package heals without any sweep.
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
requests instead of ~$11 of GETs per old-style sweep. `pypiron rebuild-index` is the
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

Mirroring is **over HTTP**: `sync --to <server>` POSTs each file to `/legacy/`
with `mirror=true` plus PyPI's `upload_time` and yank state as form fields,
authenticated against the admin credential. The server — and only the server —
writes storage: it claims the package `mirror`-origin, persists the provided
timestamp in the sidecar, and extracts PEP 658 metadata from the wheel like any
other upload. Sync is a pure HTTP client, which keeps deployment simple (it
needs a URL and the admin credential, nothing else), keeps the storage layout a
server-internal concern (no version coupling between a fleet of sync clients
and the server), and keeps one writer. There is no direct-to-storage mode: the
server is the single writer, always.

Mirror uploads are an admin operation: a `mirror=true` request must
authenticate as admin (`--admin-user`/`--admin-pass`), so an ordinary uploader
cannot backdate. With no admin credential configured, mirror uploads — like
deletion and yank — are disabled, so a stock server never accepts a client
timestamp.

A re-sync also *reconciles* what the destination already holds: it drives the
server's yank endpoint to bring yank state in line with upstream (and to flag
files gone upstream `removed upstream`), and its status endpoint
(`POST`/`DELETE /project/<pkg>/status`) to relay PEP 792 project status. Both
are admin operations the server enforces, so the same single-writer guarantee
covers reconcile as well as the initial mirror.

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
   origin claim — `packages/<pkg>/.origin` — claimed by first write. Private
   uploads to a mirror-owned name are
   rejected; sync refuses names that are private-owned. Collisions are hard
   errors, never merges. This closes the hole a prefix policy alone leaves open:
   without it, adding a private package's name to the mirror list would merge
   public files into the private package's index — pulling an attacker's
   PyPI-published version in through our own mirror. Exclusivity means each
   package belongs to exactly one world, so indexes never merge origins. The
   claim is **durable**: deleting every artifact of a package does *not* release
   `.origin`, because that would let a credentialed client empty a mirror-owned
   public name and re-upload it as private (the dependency-confusion direction).
   Re-purposing a name across worlds requires an empty package and the explicit
   `pypiron origin release <pkg>` command. It conditionally rewrites the claim;
   deleting `.origin` directly is forbidden because absence authorizes a proxy
   fill.
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

The redirect path does no artifact-existence check — presigning is local HMAC
math. In single-bucket mode, the visibility hardening adds zero storage calls.
In multi-bucket mode, the request first applies the same origin and visibility
fences as a streamed read; those control-object reads are not an artifact
existence probe. A request for a missing artifact (a stale index race, or a
hand-typed URL) gets a signed URL that S3 answers with its own 404. That
404-not-403 depends on the server's credentials carrying `s3:ListBucket`, which
they must anyway: index rebuilds, dirty-marker processing, and the reconcile
sweep are all built on listing.

## Multi-node: sloppy leader election

Only the index writer needs to be singular, and only as an optimization. A lease
object in the bucket with a TTL, heartbeat, and conditional writes
(`If-None-Match` / `If-Match` on PUT — native to GCS and Azure, supported by S3
since late 2024) is ~100 lines. No Raft, no fencing tokens, no correctness
proofs — because rebuilds are idempotent, dual leadership for a few seconds
merely duplicates work.

Disk backend is explicitly single-node; multi-node implies a cloud backend.

## Multi-bucket: synchronous fan-out, local selection

An ordered list of two or more bucket URIs — any mix of S3, GCS, and Azure —
turns every non-selected bucket into a warm writable copy. Each node
independently selects the first bucket its health view calls healthy. A request
pins one immutable `(storage, generation)` at entry; a selection change affects
only new work. Leases carry that generation, and audit checks it before every
write batch, so authority never crosses a bucket switch.

**An upload is durable on every reachable bucket before it is acknowledged.**
The node's selected bucket is the serialization point — duplicate-filename 409s,
tombstone/frozen checks, and the origin claim happen there once — then the record
streams to every other healthy bucket concurrently (`replicate::fanout_sync`),
under one grace deadline (`--fanout-grace-secs`, default 30) measured from the
selected write's completion. A secondary that fails, times out, or is ineligible
gets one durable `_repl/` repair note in the selected bucket **before the ack**;
notes are the failure path only, so a healthy fleet acks with every bucket
holding the record and no note written. Deletes, yanks, and status changes fan
out the same way. Because every bucket holds everything at ack time, reads pin
one bucket and serve with no replication-lag windows.

Beside the fleet-wide write selection, each node holds a per-node **read
selection** (regional read-affinity): its region bucket when one is labeled and
matched, otherwise the write selection. A bucket carries an optional `@region`
label on any scheme (on S3 it also picks the signing region); a node learns its
own region once at boot, off the request path — `--node-region`, then AWS
environment, then instance metadata (AWS/GCP/Azure). Reads — package index,
artifact bytes, companion cache, presign, streaming — pin the read selection;
the two pins share one selection generation so the caches do not thrash. Bytes
come from the near bucket, judgment from the write bucket: every decision that
could reach upstream (proxy fill, an unclaimed-name denial, the origin claim that
gates serving) is settled on the write pin, and the root index, counters, leases,
and sync cursors stay on the write pin too. Presence on the read bucket is
trusted; a dangerous absence is confirmed on the write pin before any 404 or
fall-through; a bounded absence (a not-yet-copied artifact, a missed yank) reads
through to the write pin or is briefly accepted. The read pin leaves its bucket
on the same availability streak the write pin uses and returns only after the
full return window *and* a worker-confirmed caught-up check (no other bucket
holds an undrained `_repl/<region>/` note). A node that matches no region reads from the write
selection — misdetection costs latency, never correctness — and writes never
move off the write selection. The read-selection contract is in
[MULTIBUCKET.md](MULTIBUCKET.md#6-reads).

Real traffic feeds the health view. A dedicated multi-only loop GETs the tiny,
guaranteed topology stamp from every bucket, with a one-second deadline and no
overlapping sweep. Startup, runtime revalidation, and migration use the same
per-operation bound. GET is required because a body-less HEAD 404 cannot
distinguish a missing object from a deleted bucket (`NoSuchBucket`). The loop is
independent of index, counter, and replication work, so a dead bucket's retries
cannot stall publishes or selection. Probes are traffic-gated: full cadence only
with recent traffic or an unhealthy bucket (re-probing unhealthy buckets is the
only way heal-back happens), decaying when idle. Classification is backend-neutral
and fail-closed: only timeouts (including HTTP 408), connection failures, and 5xx
are availability failures; authentication, permission, CAS, KMS, quota, and
configuration failures alarm without changing selection. Leaving is fast (three
consecutive failures by default); returning to a more-preferred bucket requires
five continuous healthy minutes. A switch invalidates generation-tagged caches
and schedules an audit of the newly selected bucket.

Repair notes drain on a bucket's unhealthy→healthy transition and on a slow
periodic backstop (`--repl-sweep-interval-secs`, default 300, decoupled from the
1 s worker tick). The periodic pairwise tree diff (sha256 comparisons, never
etags) is the lost-note backstop. Every node may sweep and diff — both paths are
conditional and idempotent, and selected-bucket leadership must not discard a
different node's only working path to a warm bucket. Per-bucket leases still
deduplicate index/audit work. Mirror/proxy caches, indexes, counters, and leases
never copy.

Correctness does not depend on every node selecting the same bucket. The merge
(`replicate::decide`, pure and unit-tested) has precedence
**tombstone ≻ origin (private ≻ mirror) ≻ union ≻ freeze**: deletes beat live
records, private beats mirror, artifact sets union, and logical yank/status
epochs settle metadata. Two different mirror-cache bodies remain bucket-local and
do not freeze. Different **private** bytes under one filename — possible only
when a partition moved the serialization point — resolve **first-uploaded-wins**:
each private sidecar carries `upload-epoch-ms` (server-stamped receive time), the
older wins, and the loser is quarantined, never deleted. When the two epochs are
within a 2 s skew or either is missing (legacy/mirror sidecar), the tiebreak is
untrustworthy and the conflict degrades to **quarantine-both + alarm** behind
`.frozen`. Either way `pypiron_replication_freezes_total` fires and a human
resolves by publishing a new filename. Private-over-mirror demotion is a
per-artifact operation on the ordinary copy path: drive `.origin` to private,
move the mirror body to `_quarantine/` behind a `.mirror-quarantined` marker,
copy the private record over. There is no staged package-promotion protocol. The
complete algebra and failure limits are in [MULTIBUCKET.md](MULTIBUCKET.md).

Every reachable bucket carries `_topology/stamp.json`, a hash of the ordered
bucket names plus an operator generation. Startup rejects disagreement. A
runtime mismatch leaves reads available and fences mutations. Changing the list
is explicit: stop the fleet, run `pypiron buckets migrate` with the new list,
then restart every node on it. Single-bucket mode creates no topology stamp,
health view, replication work, or probe traffic.
Shrinking a multi-bucket fleet to one skips migration: stop the fleet and
restart with the lone bucket, where topology stamps are dormant.

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
- **Per-package download stats** — a best-effort, *lossy* analytic, not a DB
  feature: counted in memory on the GET path and rolled up to sharded daily
  files under `_counters/` (see below). A crash can lose recent history; it can
  never lose correctness, because the counters are never truth.

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
packages/<pkg>/<filename>.tombstone      # permanent filename-reuse fence: normally a private delete;
                                         #   also accompanies a completed multi-bucket freeze. Excluded
                                         #   from indexes; NEVER lifecycle-expired.
packages/<pkg>/<filename>.frozen         # freeze marker (multi-bucket): two buckets committed different
                                         #   bytes under one filename (a split-brain, §6.3). Quarantine
                                         #   copies preserve both bodies; canonical records stay occupied
                                         #   behind this marker plus a tombstone so a raced replacement is
                                         #   never deleted. Indexes and direct reads reject the name. Written
                                         #   FIRST so a quarantine crash remains a freeze; resolve by
                                         #   publishing a new filename.
packages/<pkg>/<filename>.mirror-quarantined # inert mirror loser under a private claim. Its canonical
                                         #   body stays occupied to avoid delete/recreate ABA. It is hidden
                                         #   unless a later private sidecar proves the marker stale.
packages/<pkg>/.origin                   # nonce-bearing {origin, nonce} NEVER-DELETED claim
                                         #   (private|mirror|unclaimed); legacy plaintext still reads
packages/<pkg>/.project-status.json      # PEP 792 {status, reason?}; multi-bucket events also carry
                                         #   pypiron-epoch + pypiron-origin. Absent == active@0.
simple/index.html                        # materialized views (regenerable)
simple/index.json
simple/<pkg>/index.html
simple/<pkg>/index.json
_dirty/<pkg>!<root>[~<seq>].intent       # empty marker: a writer is touching this package;
                                         #   cross-bucket work rotates one unique root family
_dirty/<pkg>!<root>[~<seq>].commit       # empty marker: truth changed, rebuild now
_state/fp-<shard>.json                   # audit fingerprints: pkg -> listing hash at last rebuild
_state/inventory.json                    # regenerable aggregate (pkg/file counts); nodes read it for `/`
_sync/cursors.json                       # mirror-over-HTTP sync memo: pkg -> last upstream ETag
                                         #   (config-keyed). Pure cache for conditional fetch;
                                         #   never truth, never a view — delete it and the next
                                         #   sync re-fetches. Served by admin GET/PUT /sync/cursors.
_leader/lease.json                       # multi-node lease (holder, term, expires-at)
_topology/stamp.json                     # multi-bucket only: fail-closed config check — a hash of the
                                         #   ordered bucket identities + operator topology generation,
                                         #   CAS-written/verified on every reachable bucket at startup.
                                         #   One bucket: absent, dormant.
_repl/<dest-index>/<pkg>/<file>!<nonce>  # multi-bucket repair note, FAILURE-PATH ONLY. Written in the
                                         #   selected bucket before ack when a synchronous fan-out to a
                                         #   destination fails/times out; drained by the sweep (on that
                                         #   bucket's heal and a slow periodic backstop), deleted on
                                         #   convergence. A healthy fan-out writes none. <file> may be
                                         #   .origin or .project-status.json for package-level truth.
                                         #   One bucket: never written.
_quarantine/<pkg>/<file>@<sha12>         # multi-bucket: a frozen conflict loser or demoted mirror loser,
                                         #   preserved under its own content hash. Quarantine is per
                                         #   artifact, never deleted; there is no staging tree.
_counters/<metric>/seg/<day>/<shard>/<id>.json   # download counters: node-flushed delta segments
                                         #   (write path; <id> is a unique per-incarnation id).
_counters/<metric>/day/<day>/<shard>.json        # frozen per-shard day total (leader compaction);
                                         #   a frozen file wins over the seg dir, so a crash mid-
                                         #   compaction can't double-count or shrink a total.
_counters/<metric>/day/<day>/_summary.json       # per-day total + busiest keys (dashboard reads).
                                         #   All of _counters/ is a DERIVED, LOSSY analytic — never
                                         #   truth, never a view of truth; delete it freely (like
                                         #   _sync/). Sharded by package first char (0-9a-z, _).
_staging/<ts>-<pid>-<filename>           # cloud only: a >64 MB upload streams here, then
                                         #   copy-if-not-exists publishes it to its final key.
                                         #   Transient (the object-store analog of disk's .tmp +
                                         #   rename); never referenced by an index. A hard crash
                                         #   mid-publish may orphan one — harmless, like a leftover
                                         #   .tmp on disk.
```

`<pkg>` is always the PEP 503 normalized name. Index rebuilds include only
artifact files — metadata companions, tombstones, freeze markers, and dotfiles
are excluded by suffix/prefix. A tombstone or freeze marker suppresses its base
filename from indexes and direct server reads even while the canonical record
remains occupied. A mirror-quarantine marker does the same until an adjacent
private sidecar proves the demotion replaced the mirror loser, at which point the
stale marker is inert.

Sidecar schema (`<filename>.meta.json`), all captured at write time:

```json
{
  "sha256": "<hex>",
  "size": 12345,
  "version": "1.2.3",
  "upload-time": "2026-06-11T00:00:00Z",
  "upload-epoch-ms": 1749600000000,
  "requires-python": ">=3.9",
  "yanked": false,
  "origin": "private",
  "yank-epoch": 0
}
```

`yanked` may be `false` or a reason string (PEP 592). `origin` (`"private"` |
`"mirror"`) is the per-artifact origin, captured so the replicator can decide
"replicate private truth only" from bucket state alone, not history; it is
absent on legacy sidecars, where the worker backfills it from the package-level
`.origin` claim. `upload-epoch-ms` is the server-stamped receive time (epoch
milliseconds), the **first-uploaded-wins** tiebreak for the rare cross-partition
byte conflict: the older epoch wins, the loser is quarantined. It is absent on
legacy sidecars and on mirror artifacts; a conflict with either side missing it,
or with the two within a 2 s skew, degrades to quarantine-both + alarm.
`yank-epoch` is a monotonic counter bumped on every yank/unyank flip — the
cross-bucket merge takes the max epoch (no wall clocks, which two buckets cannot
agree on); absent means 0. Both fields default when absent, so
every pre-migration sidecar still parses. Rebuilds read sidecars only;
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

**Origin-claim lifecycle** (`.origin`, see [MULTIBUCKET.md](MULTIBUCKET.md)). The
claim is a **never-deleted** object with a small monotone lattice of states, and
every transition is a conditional write (CAS):

```
  (absent) --claim--> private            create-if-absent (put-if-none-match)
  (absent) --claim--> mirror             create-if-absent
 unclaimed --claim--> private | mirror   put-if-match on the sentinel
    mirror --demote--> private           put-if-match (private beats mirror on merge)
    mirror --audit--> unclaimed          put-if-match (proven orphan cleanup)
private|mirror --admin release-> unclaimed  put-if-match (empty package only)
   private is terminal outside the explicit admin release
```

After creation the claim never returns to *absent*: absent is what authorizes a
proxy to fill from upstream. `unclaimed` reads as "no claim" while keeping the
object present. Each state write is JSON with a fresh 128-bit nonce:

```json
{"origin":"private","nonce":"0123456789abcdef0123456789abcdef"}
```

The nonce prevents an etag ABA when disk etags are content hashes. Legacy
plaintext claims still parse. The mirror→private demotion is a direct per-artifact
CAS on the copy path — no staged manifest, no promotion barrier: drive `.origin`
to private, move each mirror body to `_quarantine/` behind a `.mirror-quarantined`
marker, then copy the private record over. Mirror writers put their create-only
sidecar before the artifact and re-check that exact claim immediately before
publish, so a slow download that straddles demotion either aborts or leaves a
typed mirror loser for later quarantine.

Request failures never release a claim. The leader audit may reclaim an empty
mirror claim only after two identical observations separated by the intent
grace, with no artifacts or live intents. It CAS-writes `unclaimed`, re-lists,
and restores the old owner with a fresh nonce if activity appeared. The operator
equivalent, `pypiron origin release <pkg>`, requires every configured bucket to
be reachable and empty of all package truth except the claim, plus no write
intents or replication notes for that package. Deletes never touch `.origin`.
Like tombstones, `.origin` is never lifecycle-expired.
An admin DELETE of mirror cache bytes remains local in single-bucket mode but is
rejected with multiple buckets: deleting an artifact cannot be atomically fenced
against a concurrent mirror→private demotion, and a cache eviction must never
manufacture a private tombstone.

**Project-status lifecycle.** In multi-bucket mode the stored PEP 792 fields
stay at the top level; `pypiron-epoch` is a monotonic local event counter and
`pypiron-origin` records the private or mirror world that authored it. Legacy
files omit both and read as epoch zero. Every multi-bucket set or clear
CAS-bumps the epoch, and a clear persists an explicit `active` event instead of
deleting the file. Single-bucket mode keeps the original plain-document PUT and
active-marker DELETE. Private-tagged status beats mirror-tagged status; within
one origin world, cross-bucket merge takes the greater epoch. Equal epochs take
the more restrictive state, then the canonical record sha256 as a deterministic
reason tie-break. Corrupt files and storage
read failures never default to active.
Mirror status stays local. When a private claim supersedes a mirror one, the
copy path reconciles status directly (`reconcile_project_status`, run whenever
either side is private): reconciliation normalizes a tagged mirror event found
under a private claim to private `active@0`, so a mirror-authored status can
never become fleet private truth. Tagged private history is never overwritten by
a mirror event. There is no staged manifest and no source read needed for
recovery — the merge is a function of both buckets' current state.

## Honest scaling limits

Measured against a fabricated full-PyPI-shaped corpus (see
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md#scale-full-pypi-measured)):

- Per-package index work is serialized through the bucket-local lease — fine,
  uploads are rare by definition.
- Global index regeneration is rare (only on package-set changes), batched per
  tick, and a multi-MB HTML file served statically with gzip is a non-event.
- Polling `_dirty/` at a ~1s tick costs pennies a day in S3 LIST requests.
- In single-bucket mode, every steady-state cost scales with what *changed*;
  only the audit (cheap LISTs) and `rebuild-index`/`verify-index` (explicit)
  scale with what *exists*. Filename non-reuse adds one tombstone HEAD per
  upload; multi-bucket uploads add one `.frozen` HEAD. Private delete adds its
  checked origin reads and tombstone write. Single-bucket serving adds no
  origin or visibility-fence reads.
- Multi-bucket mode adds per-node health probes and failure-only repair-note
  sweeps, plus one extra private copy per destination. A local package-index read
  fences on the read pin with an initial origin GET and a
  final exact-observation GET. A local artifact or served companion read adds
  those two origin GETs, tombstone/frozen/mirror-quarantine HEADs, and a sidecar
  GET when the claim or quarantine marker requires one. Under read-affinity these
  fence reads run against the region (read) bucket, where presence is trusted;
  only when the read bucket lacks the claim is the upstream-eligibility decision
  re-confirmed on the write pin, so a dangerous absence is never trusted locally.
  Proxy fills and
  passthroughs add eligibility, claim, marker, and upstream work dynamically;
  they have no single fixed request count.

Backups and disaster recovery are a selling point, not a feature: it's just files.
rsync it, version the bucket, done.
