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
- **Lost events are harmless.** A periodic full reconcile regenerates anything
  stale, so the event path only has to be fast, never reliable.
- **Recovery is trivial.** Worst case, delete every view and regenerate from truth.

## Ordering invariant: views may lag truth, but must never lead it

- Upload: write artifact, then sidecar, then index.
- Delete: remove from index, then delete artifact.

An unlisted-but-present file is invisible (harmless). A listed-but-missing file is a
broken `pip install` (harmful). The reconciler therefore only ever repairs in the
harmless direction.

## Write path: dirty markers, not a queue

Queue semantics (pending/, processing/, claim-by-copy) buy nothing here, because the
job payload is redundant — the truth is the storage listing. Instead:

1. An upload or delete drops an empty marker object at `_dirty/<pkg>` — always
   *after* the truth change it announces (artifact/sidecar written, or index
   entry removed).
2. The worker lists `_dirty/`, deletes each marker **first**, then rebuilds that
   package from a fresh listing. Deleting first matters: the marker key is
   shared, so deleting after the rebuild would destroy any mark written
   concurrently *during* the rebuild — and since a delete's mark signals an
   index entry that must go away, swallowing it leaves a listed-but-missing
   file (the one harmful state) until the next reconcile. With delete-first,
   truth-before-marker guarantees the rebuild's listing always sees the state
   that prompted any swallowed mark. The cost is honest: a crash between
   delete and rebuild loses the event — which the reconciler heals, exactly
   the failure class it exists for.
3. Duplicate markers for the same package collapse into one rebuild for free.

No claim races are possible because there is nothing to claim. This deletes a whole
class of distributed-systems problems.

The **global index** (`/simple/`) only changes when the *set of package names*
changes — a new name appears or the last file of a package is deleted. Check
membership first; most uploads skip the global rebuild entirely.

## The reconciler is the backbone; events are an accelerant

A periodic full reconcile (leader only, every N minutes) lists everything and
regenerates anything stale. Once that exists, dirty markers are merely a latency
optimization. Systems where the repair mechanism is the foundation and events are an
optimization are dramatically more robust than systems where events must never be
lost.

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
two things: storage credentials, or a *dedicated mirror credential* on a server
whose operator configured one (`--mirror-auth-user`/`--mirror-auth-pass`,
below). The mirror credential is deliberately separate from the ordinary upload
credential — backdating is a distinct privilege, so the people who can publish
private packages are not, by default, the people who can rewrite history. Both
are operator-controlled boundaries; neither is reachable from a default
deployment.

## Mirroring: carry forward true timestamps

A mirrored package must serve *PyPI's* upload time, not the mirror time —
otherwise every mirrored file looks brand-new and `--exclude-newer` is useless.
PyPI publishes the true timestamp per file (`upload-time` in its PEP 700 JSON), so
the data is free; the design question is where it enters the system.

The recommended path is **mirror-over-HTTP**: `sync --to <server>` POSTs each
file to `/legacy/` with `mirror=true` plus PyPI's `upload_time` and yank state
as form fields, authenticated against the mirror credential. The server — and
only the server — writes storage: it claims the package `mirror`-origin,
persists the provided timestamp in the sidecar, and extracts PEP 658 metadata
from the wheel like any other upload. This keeps deployment simple (sync needs
a URL and the mirror credential, nothing else), keeps the storage layout a
server-internal concern (no version coupling between a fleet of sync clients
and the server), and keeps one writer.

Mirror uploads are gated on a dedicated credential: configuring
`--mirror-auth-user`/`--mirror-auth-pass` is what enables them, and a mirror
request authenticates against *that* credential alone — the ordinary upload
credential cannot mirror, and the mirror credential cannot do ordinary uploads.
With no mirror credential configured (the default) any request carrying mirror
fields is rejected outright, so a stock server never accepts a client
timestamp. The separation is the point: backdating is a distinct privilege from
publishing, granted to a separate credential the operator hands only to the
mirror job.

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
- **S3**: redirect artifact downloads (302 to a presigned URL) so the node only
  ever serves kilobytes of index while S3 serves the megabytes — with Range support
  for free. The node never holds wheel bytes in memory.

## Multi-node: sloppy leader election

Only the index writer needs to be singular, and only as an optimization. A lease
object in S3 with a TTL, heartbeat, and conditional writes (`If-None-Match` /
`If-Match` on PUT, supported by S3 since late 2024) is ~100 lines. No Raft, no
fencing tokens, no correctness proofs — because rebuilds are idempotent, dual
leadership for a few seconds merely duplicates work.

Disk backend is explicitly single-node; multi-node implies S3.

## Publish-then-install

The one real cost of async rebuild is the CI pattern: job A publishes, job B
immediately installs, and pip doesn't retry on missing versions. The fix is an
optional synchronous mode where the upload handler polls its own index until the
file appears (bounded, a few seconds) before returning 200. Read-your-writes by
waiting — dumb and effective.

## What "no DB" honestly costs

Transactions, uniqueness constraints, and queries. Mapped to PyPI features:

- **User accounts / API tokens** — the only feature that genuinely wants a
  database. For private registries, an htpasswd-style static credentials file is
  fine.
- **Search beyond `/simple/`** — deprecated upstream anyway; skip.
- **Per-package stats** — don't care.

So the no-DB claim holds for **private registries**, which is the explicitly stated
target. For a multi-tenant pypi.org clone it wouldn't, and we shouldn't try.

## Storage layout (the contract)

Everything in one tree, on disk or S3. This layout *is* the schema — treat changes
to it like database migrations.

```
packages/<pkg>/<filename>                # artifact, immutable once written
packages/<pkg>/<filename>.meta.json      # sidecar (see below)
packages/<pkg>/<filename>.metadata       # PEP 658 core metadata, extracted from wheel
packages/<pkg>/.origin                   # "private" | "mirror" — claimed at first write
simple/index.html                        # materialized views (regenerable)
simple/index.json
simple/<pkg>/index.html
simple/<pkg>/index.json
_dirty/<pkg>                             # empty marker: package needs index rebuild
_leader/lease.json                       # multi-node lease (holder, term, expires-at)
```

`<pkg>` is always the PEP 503 normalized name. Index rebuilds include only
artifact files — sidecars (`.meta.json`, `.metadata`) and dotfiles are excluded
from listings by suffix/prefix.

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
if a sidecar is missing (legacy file), the reconciler backfills it by hashing the
artifact once. PEP 658 serving falls out of the layout: `<artifact-url>.metadata`
maps directly to the adjacent stored file.

## Honest scaling limits

All comfortably beyond any real private registry:

- Per-package write throughput is serialized through the leader — fine, uploads are
  rare by definition.
- Global index regeneration is rare (only on package-set changes) and a multi-MB
  HTML file served statically with gzip is a non-event.
- Polling `_dirty/` at a ~1s tick costs pennies a day in S3 LIST requests.

Backups and disaster recovery are a selling point, not a feature: it's just files.
rsync it, version the bucket, done.
