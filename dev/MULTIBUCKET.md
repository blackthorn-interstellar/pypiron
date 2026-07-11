# Multi-bucket replication & failover

Status: **implemented** — architecture and merge contract. Companion to
[DESIGN.md](DESIGN.md); the storage-layout additions are recorded in both
documents.

## Vision

Hand pypiron a list of buckets instead of one, and stop thinking about
regional failure. Installs never stop; publishing barely notices;
partitions heal themselves. No promote step, no runbook, no failover
drill. Cloud providers sell this as a premium managed product on their
proprietary stack; pypiron carries it in the binary for S3 and compatible
stores.

The principles, distilled from four adversarial review rounds:

1. **Correctness lives in the data plane; coordination is a cost
   optimization.** Every bucket can safely accept any write; reconciliation
   is deterministic, symmetric, clock-free. Which bucket a node uses is a
   preference — the sloppy-lease doctrine, generalized. Agreement improves
   quality of service; it is never load-bearing.
2. **Prefer one place; survive any place.** Congregating on the first
   bucket makes disagreement rare; the convergent merge makes it harmless.
   The feature is the composition of those two properties.
3. **The bucket is the queue.** No replication state to corrupt: eager
   copies at upload, todo markers beside the truth they describe, a tree
   diff as backstop — idempotent, and crash-shaped like things pypiron
   already heals.
4. **Fail closed on security, fail open on availability.** Private beats
   mirror always; config errors alarm but never reroute; conflicting private
   bytes freeze and page a human (caches and lockfiles make any other answer a
   lie). Divergent mirror-cache bytes remain local. Reads and writes keep
   flowing through every partition.
5. **We write the tricky code so the customer doesn't operate it.**
   Machinery not needed for correctness is machinery that can't deadlock —
   review taught us to delete the failover state machine, not harden it.
   Complex where it must be (the merge algebra), boring everywhere else.

The one-sentence mechanism: pypiron keeps the buckets converged and every
node serves from the most-preferred bucket it can reach. Everything else
in this document is the work required to make that sentence true.

This design went through four adversarial review rounds (a three-model panel,
a follow-up reviewer, and a two-model attack on the failover machinery, each
grounded against the source). Ideas that died in review are recorded in §10
so they don't come back.

## 1. Goal

Survive the loss of any one bucket — including a full cloud-region outage —
with:

- **Reads**: zero downtime. Installs never stop.
- **Uploads**: continue within seconds — a node that cannot reach a bucket
  selects the next one, and because wrong selections are safe (§5),
  detection is fast. No manual promote, no runbook, no failover event.
- **Partition** (buckets up, links down): every side stays fully up —
  uploads, installs, proxy — and reconciles automatically on heal.
- **Customer contract**: a list of buckets. First is preferred. That is the
  entire configuration surface. All failover, replication, reconciliation,
  and conflict handling lives inside the binary.

Principle (recorded 2026-07-09): we gladly build tricky internals and nail
them so the operator doesn't assemble fleets, DNS runbooks, or CRR rules.
The best code is no code *for the customer*.

## 2. Customer contract

```
pypiron serve --s3-bucket iron-east --s3-bucket iron-west [--s3-bucket iron-eu]
PYPIRON_S3_BUCKETS=iron-east,iron-west,iron-eu
```

- Two or more buckets, order = preference. `buckets[0]` is the **preferred**
  bucket; at any moment each node has one **active** bucket — the first one
  it observes healthy.
- One bucket configured starts none of this document's topology, health,
  replication, or warm-index machinery. The shared filename-reuse hardening
  remains: one tombstone HEAD per upload and the checked tombstone write on a
  private delete. Multi-bucket uploads also check the first-written `.frozen`
  conflict marker.
- Supplying `--s3-bucket` selects S3; `--storage s3` is optional. An entry may
  pin its region as `name@region`. The CLI flag is repeatable and the plural
  environment variable is comma-delimited; legacy `PYPIRON_S3_BUCKET` still
  selects one bucket; the TOML `s3-bucket` key remains a single-bucket value.
- Buckets are **same-trust infrastructure**. A hostile bucket is out of
  scope: whoever writes your buckets owns your index, list of one or list
  of five.
- v1 is S3-only and uses one credential chain and one optional endpoint for
  every bucket. Regions may differ per entry; providers and credentials may
  not.

## 3. Invariants

1. **One preferred coordination point, not one required one.** All
   conditional-write safety (create-if-absent, origin claims, index CAS,
   leases) applies within whichever bucket a node has selected. In steady
   state every node selects `buckets[0]`, so duplicate filenames 409
   immediately and the dependency-confusion guard is globally atomic —
   identical to single-bucket pypiron. When vantages differ (outage,
   partition), nodes may briefly coordinate against different buckets;
   every anomaly this can produce is defined, convergent, and alarmed by
   §6. Agreement improves quality of service; it is never load-bearing for
   correctness. (This is DESIGN.md's sloppy-lease doctrine — coordination
   as cost optimization — extended from leaders to buckets.)
2. **Operations never span buckets.** Every operation (request handler,
   proxy fill, worker tick, audit run) captures the storage context once at
   entry and performs *all* its reads and writes against that handle. A
   selection switch changes what *new* operations capture; in-flight
   operations finish or fail on the bucket they started on. A torn multi-bucket record
   is impossible by construction; a half-finished upload on a dying bucket
   is exactly a crashed upload, which the intent/audit machinery already
   heals. The contract, precisely:
   - The context is **one immutable `(Arc<dyn Storage>, generation)`
     struct** behind a single atomic swap — never two separately-loaded
     values (a request must not pair the old handle with the new
     generation).
   - An "operation" is the **entire call graph** rooted at one entry point.
     Helpers inherit the parent's captured context and never re-resolve
     storage; a retry that would cross a generation restarts the whole
     operation instead.
   - Long-lived authority is re-checked: leases embed the generation they
     were acquired under and leadership never carries across buckets; audit
     runs re-validate the live generation before each write batch and abort
     on mismatch.
   - **Replication and reconciliation are the one deliberate exception**:
     they are two-handle operations by nature (read source, write
     destination). Both handles are captured at batch start, every write
     they make is conditional like any other, and a batch that errors
     simply re-derives from markers/diff on the next cycle. Nothing else
     may hold two handles.
3. **Non-selected buckets are warm copies, not dead weight.** In steady
   state nothing serves from them; they receive replicated private truth
   (§4) continuously, so any node can select one at any moment and be
   serving within seconds. Divergence is only born during deviation
   windows — when parts of the fleet select different buckets — and §6
   makes its reconciliation deterministic.

## 4. Steady state

All nodes, all regions → `buckets[0]`. Every node runs replication maintenance;
the work is conditional and idempotent, so duplicate attempts cost requests but
cannot change the result. This is deliberate: a node that holds the preferred
bucket's index lease may be unable to reach a warm bucket that another node can.
The lease remains a cost optimization, never a connectivity gate.

**Replicate**, from any bucket that took writes to every other configured
bucket — in steady state, a star from `buckets[0]`. Three tiers, fastest
first, each backstopping the one above:

1. **Durable todo marker, then eager fan-out.** Before acknowledging a private
   mutation, the request writes one nonce-bearing marker per destination in
   the selected bucket (`_repl/<dest-index>/<pkg>/<file>!<nonce>`). Truth is
   already immutable at this point, so a marker-write failure is alarmed rather
   than converted into a false-negative response that would make a correct
   client retry into 409. The full diff remains the lost-marker proof. After
   the ack, the handler immediately copies the record and deletes each exact
   marker on success. Common-case replication lag: milliseconds; an
   unavailable destination does not delay the response.
2. **Todo-marker sweep.** The replicator
   sweeps markers in **every** configured bucket, not just the preferred
   one — a bucket that took direct writes during a deviation window drains
   home the same way. While a bucket is unreachable, markers destined for
   it accumulate; when it returns, they drain. After a partition heals,
   each side's markers are precisely the list of what the other side is
   missing, so reconciliation is O(what-diverged), not O(corpus).
3. **Full-tree diff** on the reconcile cadence, as the backstop for lost
   markers — exactly as the audit sweep backstops dirty-marker index
   rebuilds today.

There is still no queue *state* to corrupt: markers are durable objects in
the same bucket as the truth they describe, and the tree diff remains the
final cursor. A destination being down means pushes fail, markers pile up,
and nothing else happens. Crash-safe and idempotent by the same argument as
every other worker job.

Every pairwise replication mutation holds one unique `_dirty/` intent root on
each bucket. Long work rotates both through `root~<seq>` family members every
third of `--intent-grace-secs`: create the next pair, then commit the old pair.
The worker treats the whole family as one holder. This closes package-index and
promotion races without a new queue or clock protocol.

Upload-time coordination (origin claim, create-if-absent 409, tombstone
check) runs against the **node's selected bucket only**, before the ack;
the eager pushes are ordinary replication writes and gate nothing. If the
selected bucket is unreachable, the request fails, the node's health view
reselects within seconds (§5), and the client's retry lands on the new
selection — individual requests never improvise a referee mid-flight, but
the node as a whole reacts fast because a wrong selection is safe.

What replicates (**private truth only**):

| Object | Replicates? | Why |
|---|---|---|
| private artifacts + sidecars + `.metadata`/`.provenance` | yes | the irreplaceable truth |
| private `.project-status.json` | yes | quarantine and lifecycle state must converge |
| `.origin` claims | yes — **ahead of artifacts**, including origin-only claims | shrinks the dependency-confusion window to seconds |
| tombstones (§6.4) | yes | deletes must not resurrect |
| mirror/proxy-cached content | **no** | re-derivable from upstream per bucket; syncing it buys nothing and costs LIST storms, doubled storage, and cache-eviction anomalies |
| `simple/` indexes | **no** | regenerable views; each bucket derives its own |
| `_staging/repl/` package-demotion records | retained recovery state | verified private records, delete/freeze fences, captured mirror versions, and the private project-status snapshot; manifests are removed after promotion, content-addressed members persist inert when unreferenced, and each package's CAS lock sentinel is never deleted |
| `_counters/`, `_dirty/`, `_leader/`, upload `_staging/` | no | derived / ephemeral / local |

Copy protocol per artifact record: verify the complete source record, establish
the private package claim, then write **sidecar, companions, artifact last**.
The artifact is sha256-checked against the source sidecar. Never gate on raw object
presence (an artifact can exist pre-commit) and never on etag (not comparable
across buckets, or across multipart strategies within one). Order matters
because the index rebuilder backfills a fabricated sidecar for sidecar-less
artifacts (`list_artifacts`, src/worker.rs) — an orphan *artifact* becomes
fabricated truth; an orphan *sidecar* is inert.

Because origin is package-level but replication is artifact-level, the
sidecar gains a per-artifact `origin` field (§6.2). "Replicate private only"
must be derivable from bucket state alone, not from history.

## 5. Bucket selection (there is no failover)

There is no failover event, no active-bucket state machine, no mode
switch, and no cluster agreement about which bucket is "the" bucket.
Correctness lives entirely in the convergent data plane (§6); which bucket
a node uses is a cost optimization — DESIGN.md's sloppy-lease doctrine
extended from leaders to buckets. A wrong or stale selection is never
unsafe; it is a brief dual-write window that reconciliation absorbs.

### Per-node selection
Each node independently maintains a health view of every configured bucket
(a topology-stamp GET plus errors observed on real traffic). The multi-only
health loop is independent of index, counter, and replication work; every
probe has a one-second deadline. Classification is strict and fail-closed:

- **Unhealthy signals**: timeouts (including HTTP 408), connection failures,
  5xx.
- **Never unhealthy signals**: 403/401 (credentials), 412 (CAS traffic),
  KMS/quota/config errors. These alarm loudly and never affect selection —
  a misconfigured node must not wander off the preferred bucket.

A node's **selected bucket** is the most-preferred bucket its view calls
healthy. Hysteresis exists only to damp churn, and because wrong
selections are safe it is asymmetric and tight: *leave* a failing bucket
after a few seconds of consecutive failures; *return* to a recovered
more-preferred bucket only after it has been continuously healthy for much
longer (minutes, knob), so a flapping bucket settles into "stay put"
rather than oscillating.

The shipped defaults are three consecutive availability failures to leave and
300 continuous healthy seconds to return. `--bucket-leave-failures` /
`PYPIRON_BUCKET_LEAVE_FAILURES` and `--bucket-return-healthy-secs` /
`PYPIRON_BUCKET_RETURN_HEALTHY_SECS` tune them. The probe cadence is the normal
`--worker-interval-secs` / `PYPIRON_WORKER_INTERVAL_SECS` (one second by
default).

Everything a node does follows its selection: uploads coordinate against
it (§4), the worker and audit run against it, index reads serve from it,
and redirect links presign against it.

### How redirect links know which bucket is up
They don't — presigning itself is blind local HMAC math with no artifact
existence check or network call, and that stays true. Before signing or
streaming, however, every local multi-bucket `/files/` request applies the
selected bucket's visibility fence: an initial origin GET, tombstone/frozen/
mirror-quarantine HEADs, a sidecar GET when the private claim or quarantine
marker requires one, then a final origin GET proving the exact claim did not
change. Local package-index reads likewise compare initial and final exact
origin observations, so a staged promotion cannot leak a partial package.
Proxy listings additionally filter the upstream result against local
tombstone, frozen, and quarantine markers after the upstream fetch. Proxy fills
and companion passthroughs add eligibility, claim, marker, and upstream checks,
so their read count is higher and path-dependent. A single bucket skips this
entire read fence, so the fence adds no storage I/O there.

The other health producer is one tiny topology-stamp GET per configured bucket
on the worker cadence. A GET is deliberate: S3's body-less HEAD 404 cannot
distinguish a missing key from a deleted bucket, while the guaranteed stamp GET
preserves `NoSuchBucket`. Probe sweeps never overlap, and they run on a dedicated
loop, so a blackhole cannot starve either selection or the selected bucket's
index worker. In the healthy case every node holds a roughly
worker-cadence-fresh, first-hand view.

Signing without an artifact-existence probe is safe because the index and the
presign share provenance: a node serves index pages derived from its selected
bucket and signs URLs into that same bucket, so the file's existence was
established by the index that advertised it — the same source the link points
at. The visibility fence rejects pending, deleted, frozen, and quarantined
records before this point. The remaining consequences and escape hatch are:

- During a deviation window, a very fresh upload may not have replicated
  to the node's selected bucket yet (milliseconds in steady state, thanks
  to eager fan-out). The presign 404s and the client retries — the §11
  mixed-generation limit.
- Links signed in the sub-second between a bucket dying and the node's
  probe noticing fail at the client; the retry arrives after the node has
  reselected. Window = probe cadence (~1s). An additional artifact-existence
  HEAD could shave this, but costs another request per download and the retry
  already heals it.
- Operators who want zero client-to-bucket dependency set
  `--artifact-delivery stream` and the node carries the bytes itself.

### Switching is safe, so switching is boring
- Pin-at-entry (§3) means a switch never tears an in-flight operation: one
  immutable `(handle, generation)` struct behind one atomic swap; in-flight
  operations keep their pinned handle and fail naturally; the client retry
  lands on the new selection.
- Index and presign caches are generation-tagged; a switch bumps the
  generation, so stale entries cannot leak across buckets. Already-issued
  presigned URLs cannot be revoked; their TTL bounds the window (§11).
- Leadership never crosses buckets: leases are per-bucket objects. If parts
  of the fleet select different buckets for a while, each bucket has its
  own leader doing idempotent derived work — identical in kind to the
  existing sloppy-lease behavior within one bucket.
- On selecting a bucket it hasn't recently served, a node's leader runs
  the audit sweep there (same code as audit-on-boot): indexes are
  stale-but-serviceable until healed, and the replicator drops `_dirty/`
  markers in destination buckets as it copies, so the heal is incremental
  rather than a full rebuild.
- Simultaneous deviation (asymmetric partition: region B can't reach
  `buckets[0]`, region A can) is the deliberate dual-write mode: both
  sides fully up, divergence tracked by markers, merged on heal (§6). This
  is the "everything stays up transparently" requirement, working as
  specified rather than as an accident.

### What replaced fail-back
Nothing — that is the point. When a more-preferred bucket recovers, nodes
drift back after the return-hysteresis; the abandoned bucket's `_repl/`
markers drain through the replicator; the periodic full diff backstops
lost markers. No barrier, no drain phase, no epoch to fence, because there
is nothing a straggler write can break: it lands in a configured bucket,
gets a marker, and flows home within a sweep interval.

## 6. Reconciliation

Reconciliation is not special disaster code — it is the §4 replicate job
pointed at a bucket that has its own unmerged truth. It runs warm every day
of its life; only the flip itself is cold. Merge rules are **symmetric,
deterministic, and clock-free**: any two buckets reconciled by any node in
any order reach the same result. Precedence:

**tombstone ≻ origin (private ≻ mirror) ≻ union ≻ freeze**

### 6.1 Artifacts — union
Pull unit = sidecar + artifact + companions, verified by sidecar sha256.
Same filename + same sha256 (the common "CI retried against the other side"
case) does not copy the body; missing companions and logical yank state still
merge.

### 6.2 Origin — monotone lattice, private is terminal
- Claim bodies are nonce-bearing JSON with `origin`, a 128-bit `nonce`, and an
  optional `pending-manifest` stage key. A
  fresh nonce on every state write prevents etag ABA on the disk backend;
  legacy plaintext claims still parse.
- The normal lattice is absent→private/mirror, unclaimed→private/mirror, and
  mirror→private. Every transition is create-if-absent or CAS. Once created,
  the object never returns to absent — absence authorizes a proxy fill.
  Private is terminal.
- The proxy and legacy mirror upload write a create-only mirror sidecar before
  the artifact, then re-read the exact mirror observation immediately before
  the conditional artifact put. An orphan sidecar is inert; a late artifact is
  always typed mirror. If demotion wins the final cross-object window, the
  writer leaves that typed loser for private-precedence quarantine instead of
  racing a delete against newer private truth.
- Failed empty mirror claims are reclaimed by the leader audit, not the request
  failure path: two empty/no-intent observations separated by the intent grace,
  with no committed stage, then CAS to a fresh `unclaimed` body, followed by a
  post-CAS re-list and restoration if activity appeared.
- Deliberate repurposing is `pypiron origin release <pkg>`. It requires every
  configured bucket to be reachable and the package to have no package truth
  except `.origin`, nor pending write/replication manifests or markers; it
  releases through CAS, never delete.
- Demotion on merge is package-atomic in visibility: stage every verified
  private record plus tombstone/freeze fence under `_staging/repl/`, capture
  exact pre-CAS mirror artifact versions, write the package manifest, then CAS
  `.origin` to private with `pending-manifest` naming that exact manifest. All
  publishers reject the pending claim. Promotion is idempotent; its final CAS
  clears `pending-manifest`. A manifest resumes after a crash and partial stages
  without one are inert. Multiple committed manifests serialize through the
  one claim. A one-file package never becomes empty during demotion. Completion
  deletes only the manifest; shared content-addressed members persist and become
  inert when unreferenced. A broad `_staging/repl/` lifecycle rule is unsafe
  because a live manifest may still reference any retained member. Executors
  serialize through `_staging/repl/<pkg>/.promotion-lock`, a never-deleted CAS
  sentinel released to a fresh `free` body. A holder heartbeats it every third
  of the intent grace. Recovery may take over only after the same lock ETag has
  remained unchanged for a full grace and the holder's complete intent family
  is no longer live. Recovery sweeps discard in-memory lock observations when
  the package has no committed manifest, so stale proofs do not accumulate or
  authorize a later takeover.
- Captured mirror-only leftovers are copied to `_quarantine/` and marked with a
  `.mirror-quarantined` companion. Their canonical cache bytes remain in place
  but inert, unindexed, and unavailable through direct server reads. A later
  private sidecar proves that promotion replaced the mirror loser and makes a
  stale quarantine marker inert. Not deleting the artifact key is deliberate:
  an ETag cannot distinguish a same-byte delete/recreate ABA, while retaining
  the occupied key is safe and preserves filename immutability.
- Sidecar gains per-artifact `origin` so private truth is derivable from
  state (§4).

### 6.3 Private byte conflict — freeze, never pick
Same filename, different sha256, both committed as private truth: this is
*always* a bug or an attack (create-if-absent forbids it within a bucket; only a
split can produce it). No winner is chosen — not by configured primary (a one-line
misconfig makes both sides winners: silent permanent split-brain; and it
doesn't generalize to N buckets), not by hash order (indexes and year-long
immutable CDN caches have already served the "loser"; convergence-by-winner
is a lie the caches ignore, and lockfiles pin the hashes). Instead:

- Both bodies are preserved under content-hash-suffixed quarantine keys.
  `.frozen` is written first, before any fallible quarantine I/O; publishers
  check it as a write fence. Quarantine and the permanent tombstone follow, but
  the canonical record remains occupied and inert behind both markers. Deleting
  it after the quarantine read could erase a different body that won a
  concurrent CAS; a later reconciliation pass can quarantine that replacement.
  Every crash shape remains recognizably a freeze and retries idempotently.
- The filename is suppressed from indexes and rejected by new direct server
  reads on all buckets. Deterministic, side-independent, N-safe.
- Loud alarm (log + metric). A human resolves — in practice: publish a new
  version.

Mirror/mirror is the deliberate exception. Proxy caches are bucket-local,
replaceable snapshots; different bytes under the same filename neither copy nor
freeze. If private truth exists on either side, private precedence stages the
package and quarantines the mirror loser instead.

### 6.4 Deletes — tombstones, private files only
- Durable per-file tombstone written **before** artifact removal (a crashed
  delete converges instead of resurrecting).
- Checked by the upload path and index rebuild — filename reuse after delete is
  banned.
- Never lifecycle-expired; expiry would resurrect deleted packages.
- Mirror deletions are local cache management and never replicate. With one
  bucket the admin DELETE remains available. With multiple buckets it is
  rejected: cache eviction cannot be atomically separated from a concurrent
  mirror→private demotion across two S3 objects, and cached bytes are replaceable.
- Tombstones are a storage-layout-contract addition (DESIGN.md §layout);
  treat like a schema migration.

### 6.5 Yank — logical epoch, no wall clocks
Sidecar gains `yank-epoch` (monotonic counter, bumped on every yank/unyank).
Merge = max epoch; equal epoch with conflicting state = yanked wins
(fail-closed). Any residual same-epoch sidecar difference — including two
byte-identical partition uploads with different captured metadata — takes the
record with the lexicographically smaller sidecar sha256. Wall-clock LWW is
banned: two buckets have two clocks, and skew makes verdicts side-dependent and
non-convergent.

### 6.6 Project status — logical epoch, explicit clears

`.project-status.json` carries a monotonic `pypiron-epoch`. Every local status
change CAS-increments it; returning to `active` writes an explicit event rather
than deleting the file. Merge takes the greater epoch. Equal-epoch splits take
the more restrictive state (`quarantined` ≻ `archived` ≻ `deprecated` ≻
`active`), then the lexicographically smaller canonical-record sha256 for
differing reasons. The rule is symmetric and clock-free.

Mirror status is bucket-local cache metadata. A mirror→private staging manifest
captures the private side's status (including implicit `active@0`) plus the exact
destination status version. During promotion, tagged mirror status is
replaceable, tagged private status is preserved, and legacy untagged status is
replaced only when its exact captured ETag still matches. Reconciliation also
normalizes any tagged mirror event found under an already-private claim to
private `active@0`; a mirror request that straddled demotion can therefore never
become fleet private truth.

### 6.7 Counters — not replicated
Per-bucket stats; they are derived and lossy by design. Sum offline if it
ever matters. Each flush, compact, or query pins one bucket for its entire call
graph, so a selection switch cannot splice one counter operation across stores.

## 7. Coordination state

Almost none — deliberately. There is no epoch object, no ACTIVE/DRAINING
status, no cluster membership. Three pieces of shared state exist:

- **Per-bucket leases** (existing): each bucket has its own `_leader/` lease for
  singular index/audit work. Marker delivery and the lost-marker full diff are
  deliberately not gated by the selected bucket's lease; every node may attempt
  their conditional, idempotent writes.
- **Per-package promotion lock** — the never-deleted CAS sentinel described in
  §6.2. It selects one manifest executor; heartbeat plus intent-family liveness
  makes crash takeover safe. It never elects a bucket or gates ordinary work.
- **Topology stamp** — the fail-closed configuration check: a
  deterministic hash of the ordered bucket identities plus an
  operator-controlled topology generation, written-or-verified by CAS
  against every *reachable* bucket at startup and re-verified on every
  reachability transition thereafter. Startup mismatch = refuse to start;
  runtime mismatch (a healed partition reveals a bucket stamped by a
  differently-ordered deployment) = alarm and stop accepting writes. Two
  deployments disagreeing about the bucket order is the one
  misconfiguration that degrades "disagreements are rare" to
  "disagreements are constant," so it is the one thing checked hard.
  Every topology GET/CAS has a one-second control-plane deadline. An unreachable
  bucket can add up to that timeout for each attempted operation, but cannot
  block startup indefinitely. Validation requires only reachable buckets — a
  standby node must be able to boot during the exact outage it exists for.
  Changing the topology
  (replacing a dead bucket, adding one, reordering) is an explicit
  operator action — stop the fleet, run `pypiron buckets migrate` with the new
  complete list, then restart every node on that list. The command bumps the
  generation and re-stamps reachable buckets, so disaster recovery never
  bricks on a stale stamp. A runtime mismatch leaves reads up and sets a sticky
  write fence until restart.
  Shrinking to one bucket is the exception: stop the fleet and restart with the
  lone bucket. Single-bucket topology is dormant, so there is no stamp migration
  to run.

Everything v5 kept in coordination state (ACTIVE/REPLICA/DRAINING, fencing
epochs, demotion protocol, bootstrap activation rules) is deleted, not
moved (§10): the convergent data plane makes agreement about "the" active
bucket unnecessary, and machinery that isn't needed for correctness is
machinery that cannot deadlock.

## 8. N buckets (N ≥ 2)

Everything above is already N-safe, by construction:

- Preference is the list order — a total order shared by all nodes and
  verified by the topology stamp. "Most-preferred healthy" needs no
  election.
- Replication follows markers from whichever bucket took a write;
  reconciliation is pairwise and the §6 rules are symmetric and convergent
  regardless of pairing order — precisely because no rule references a
  designated winner.
- Cost scales linearly: each extra bucket is one more replication target.

Two is the sweet spot; three is for the paranoid; more works but each
bucket is another full copy of private truth.

The quantified steady-state request and byte formulas live in the user manual:
[Multi-bucket failover — Cost model](../docs/guides/multi-bucket.md#cost-model).

## 9. Security posture

- Same-trust buckets (§2). Fail-closed error classification (§5).
- **Dependency-confusion window**: a brand-new private name uploaded during
  a dual-write window can be proxy-filled from upstream on the other side
  until the claim replicates (seconds normally; partition-length during a
  split). Steady state self-heals via §6.2, but a client may have fetched
  upstream bytes in the window. Mitigations, in order of strength:
  1. Set `--private-prefix` / `PYPIRON_PRIVATE_PREFIX` on all nodes. It both
     constrains new private names and forbids proxy fills under the normalized
     prefix. If the private set has no common prefix, omit the prefix guard and
     put every exact private name in `[mirror]` excludes instead. This
     structural rule closes the window and is required guidance for
     private+proxy deployments.
  2. Claims replicate ahead of artifacts (§4).
- In steady state (all nodes on one bucket) the window does not exist at all —
  an improvement this design has over any always-dual-writing scheme.

## 10. Rejected alternatives (do not re-litigate)

- **S3 CRR / any object-level replication** — last-writer-wins at object
  granularity tears artifact/sidecar pairs and merges `.origin` unsafely;
  async replication of the lease/CAS objects is meaningless. This is the
  original "never two writable buckets under dumb replication" rule; it
  still holds.
- **Synchronous dual-write on upload** — inverts availability (uploads then
  require *both* buckets up), and cross-bucket create-if-absent cannot be
  made atomic without 2PC.
- **Designated-primary conflict winner** — unimplementable over
  create-if-absent without delete+recreate (which composes with tombstones
  into erase-both data loss), silently overwrites acknowledged uploads, is
  one misconfig away from undetected permanent split-brain, and does not
  generalize past two buckets.
- **Wall-clock LWW for anything** — two clocks, non-convergent. §6.5.
- **Hash-order conflict winner** — deterministic but dishonest: caches and
  lockfiles have already pinned the loser. §6.3 freezes instead.
- **Replicating mirror content** — O(corpus) LIST/egress/storage for data
  that is re-derivable per bucket from upstream.
- **Two pinned fleets, routing-only failover** — safe and simpler in-repo,
  but exports fleets/routing/runbooks to every customer. Rejected on the
  product principle (§1). The per-operation pinning in §3 buys the same
  no-torn-transactions guarantee inside one process.
- **An epoch-gated active-bucket state machine** (v5 of this design) — a
  fenced ACTIVE/REPLICA/DRAINING protocol with hysteresis-gated failover
  and barriered fail-back. Red-teaming it surfaced deadlocks, write black
  holes, stale-completer races, and bootstrap poisoning, and every fix
  added protocol. The lesson was this design's own doctrine read back to
  it: once every bucket can safely accept writes and reconciliation is
  convergent, *selection does not need agreement* — the prevention
  machinery downgrades to a per-node preference plus hysteresis. Deleted
  wholesale; do not rebuild it. Its one durable contribution was the
  fencing analysis that hardened pin-at-entry (§3).
- **MRAP / GCS dual-region as the answer** — provider-specific, and the
  point of pypiron is that the binary carries the guarantees. (GCS
  dual-region remains a fine zero-config choice for GCS users; this design
  must not break under it.)

## 11. Honest limits

- **Operations pause seconds** on a true bucket outage: the node's own
  leave-hysteresis plus one client retry. New reads switch with the node;
  an operation already pinned to the failed bucket returns an error.
  Physics still applies — you cannot distinguish "down" from "slow"
  instantly — but because a wrong selection is safe, detection no longer
  has to be conservative. Multi-bucket S3 disables SDK retries; the dedicated
  one-second probes switch new traffic and cancel background work when a bucket
  becomes ineligible. An artifact transfer already in flight keeps the normal
  one-hour route bound so a legitimately slow upload is not cut off. The client
  may cancel and retry against the new selection sooner.
- **Cross-bucket duplicate rejection is best-effort.** Two uploads of the
  same filename through nodes selecting different buckets during a
  deviation window both receive 200 and freeze on merge (§6.3). Within one
  bucket — steady state, i.e. almost always — the 409 remains immediate.
- **RPO = replication lag** for a destroyed (not partitioned) bucket:
  private uploads acknowledged in the last seconds may need re-publishing.
- **Frozen conflicts require a human** (§6.3). The conflicted filename remains
  immutable; resolve by publishing a new filename.
- **Caches/CDNs** may serve pre-freeze bytes for a conflicted filename until
  TTLs expire. No merge rule can un-serve bytes.
- **Issued presigned URLs cannot be revoked by a flip**; their one-hour expiry
  bounds the window. A client mid-install across a flip can
  observe mixed generations — an index fetched from the old bucket, a file
  404ing on the new — for one resolve cycle; install-level retry heals it.
- **A write landed on a non-preferred bucket** during a deviation window
  reaches the other buckets within a marker-drain interval, not instantly.
  Bounded, never stranded.

## 12. Implementation map

- **P0 — plumbing**: repeatable S3 bucket config, backend construction, and
  topology stamps.
- **P1 — pin-at-entry**: every operation captures one immutable storage handle
  plus selection generation; caches are generation-tagged.
- **P2/P2.5 — data-plane hardening**: nonce-bearing origin CAS, pre-commit
  mirror fences, audit-only empty-claim reclamation, tombstones, delete-race
  fencing, and the checked origin-release command.
- **P3 — replicator**: pre-ack durable markers, post-ack eager fan-out,
  lease-independent all-bucket marker sweeps, crash-resumable package staging
  for mirror→private demotion, and a two-pass sequential full-diff backstop using the
  merge rules in §6.
- **P4 — selection**: real-traffic health observations, bounded topology-stamp
  GET probes, asymmetric hysteresis, generation-safe switching, warm-bucket index
  workers, runtime topology validation, and the topology migration command.
- **P5 — proof and operations**: two- and three-bucket blackbox coverage,
  Prometheus health/replication/fence metrics, and the user manual.

## 13. Fixed v1 boundaries

- S3 only. `name@region` is the per-bucket region syntax.
- One credential chain and one endpoint for the ordered list. Per-bucket
  credentials and mixed providers are later features, not hidden config.
- Replication markers and health probes use `--worker-interval-secs`; the full
  diff uses `--reconcile-interval-secs`. No second cadence surface.
- Frozen conflicts alarm through logs and
  `pypiron_replication_freezes_total`. Webhooks are outside v1.
- Leave/return defaults are 3 failures and 300 healthy seconds.
