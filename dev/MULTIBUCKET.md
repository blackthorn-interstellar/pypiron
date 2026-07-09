# Multi-bucket replication & failover

Status: **proposed** — reviewed design, not implemented. Companion to
[DESIGN.md](DESIGN.md); nothing here changes the storage-layout contract except
the additions in §6.

## Vision

Hand pypiron a list of buckets instead of one, and stop thinking about
regional failure. Installs never stop; publishing barely notices;
partitions heal themselves. No promote step, no runbook, no failover
drill. Cloud providers sell this as a premium managed product on their
proprietary stack; pypiron carries it in the binary, on any object store.

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
   mirror always; config errors alarm but never reroute; conflicting bytes
   freeze and page a human (caches and lockfiles make any other answer a
   lie). Reads and writes keep flowing through every partition.
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
- One bucket configured = exactly today's behavior; every mechanism in this
  document is dormant.
- Buckets are **same-trust infrastructure**. A hostile bucket is out of
  scope: whoever writes your buckets owns your index, list of one or list
  of five.
- v1 assumes one credential/identity with access to all buckets (S3 creds
  are env-global today; per-bucket credentials are an open question, §13).

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

All nodes, all regions → `buckets[0]`. The leader (existing lease, in the
preferred bucket) runs one new job:

**Replicate**, from any bucket that took writes to every other configured
bucket — in steady state, a star from `buckets[0]`. Three tiers, fastest
first, each backstopping the one above:

1. **Eager fan-out at upload time.** After the selected-bucket commit and
   the client ack, the upload handler — which still holds the verified
   bytes — immediately pushes sidecar-first copies to every other bucket.
   Common-case replication lag: milliseconds. The client never waits on
   these; another bucket being down costs the customer nothing.
2. **Todo markers.** Any failed or skipped push drops a per-destination
   marker in the bucket that took the write (`_repl/<dest>/<file>`, the
   `_dirty/` idiom pointed at a second bucket — written O(1) at commit,
   consumed and deleted by the replicator on success). The replicator
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
| private artifacts + sidecars + `.metadata`/`.provenance`/`.project-status.json` | yes | the irreplaceable truth |
| `.origin` claims | yes — on a fast tick, **ahead of artifacts** | shrinks the dependency-confusion window to seconds |
| tombstones (§6.4) | yes | deletes must not resurrect |
| mirror/proxy-cached content | **no** | re-derivable from upstream per bucket; syncing it buys nothing and costs LIST storms, doubled storage, and cache-eviction anomalies |
| `simple/` indexes | **no** | regenerable views; each bucket derives its own |
| `_counters/`, `_dirty/`, `_leader/`, `_staging/` | no | derived / ephemeral / local |

Copy protocol per artifact record: **verified sidecar first, artifact last**,
both sha256-checked against the source sidecar. Never gate on raw object
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
(a data-plane probe plus errors observed on real traffic). Classification
is strict and fail-closed:

- **Unhealthy signals**: timeouts, connection failures, 5xx.
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

Everything a node does follows its selection: uploads coordinate against
it (§4), the worker and audit run against it, index reads serve from it,
and redirect links presign against it.

### How redirect links know which bucket is up
They don't — presigning is blind local HMAC math with no existence check
and no network call, and that stays true. The redirect path *consumes* the
health view; it never produces signal. The producers are the operations
that already touch storage every second regardless of traffic: the lease
heartbeat (a conditional write), dirty-marker reads, counter flushes —
plus one cheap HEAD probe per non-selected bucket on the worker tick. Net:
every node holds a ~1s-fresh, first-hand view of every configured bucket.

Blind signing is safe because the index and the presign share provenance:
a node serves index pages derived from its selected bucket and signs URLs
into that same bucket, so the file's existence was established by the
index that advertised it — the same source the link points at. The
sign-time check the design "skips" was never load-bearing. Two bounded
consequences remain:

- During a deviation window, a very fresh upload may not have replicated
  to the node's selected bucket yet (milliseconds in steady state, thanks
  to eager fan-out). The presign 404s and the client retries — the §11
  mixed-generation limit.
- Links signed in the sub-second between a bucket dying and the node's
  probe noticing fail at the client; the retry arrives after the node has
  reselected. Window = probe cadence (~1s). (A verify-before-redirect HEAD
  could shave this, at a network round-trip per download — rejected; the
  free 302 is a feature and the retry already heals it.)
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
case) is a no-op.

### 6.2 Origin — monotone lattice, private is terminal
- The only legal transition is mirror→private, via a new **CAS overwrite**
  primitive (`If-Match` on the current claim content). The claim never
  transits through absent — absent is what authorizes the proxy to fill
  from upstream, and the whole point of the claim is that it can't.
- Consequences for existing code (this is real work, not merge-job-local):
  - Origin reads return a generation/etag.
  - The proxy re-validates its claim generation immediately before commit —
    today a slow upstream download can commit mirror bytes into a package
    that went private mid-flight.
  - `release_empty_claim` becomes a conditional delete (delete-if-match) —
    today it is unconditional and can delete a now-private claim, producing
    exactly the forbidden absent state.
- Demotion order on merge: copy private artifacts in → CAS the claim →
  quarantine local mirror files for that name (never serve a private name's
  mirror leftovers; also never leave the package empty mid-demotion).
- Sidecar gains per-artifact `origin` so private truth is derivable from
  state (§4).

### 6.3 Byte conflict — freeze, never pick
Same filename, different sha256, both committed: this is *always* a bug or
an attack (create-if-absent forbids it within a bucket; only a split can
produce it). No winner is chosen — not by configured primary (a one-line
misconfig makes both sides winners: silent permanent split-brain; and it
doesn't generalize to N buckets), not by hash order (indexes and year-long
immutable CDN caches have already served the "loser"; convergence-by-winner
is a lie the caches ignore, and lockfiles pin the hashes). Instead:

- Both bodies preserved under content-hash-suffixed quarantine keys —
  moves, never deletes, never tombstones (a conflict must not poison the
  name forever).
- The filename is suppressed from indexes on all buckets. Deterministic,
  side-independent, N-safe.
- Loud alarm (log + metric). A human resolves — in practice: publish a new
  version.

### 6.4 Deletes — tombstones, private files only
- Durable per-file tombstone written **before** artifact removal (a crashed
  delete converges instead of resurrecting).
- Checked by the upload path — filename reuse after delete becomes banned
  (today it is possible; PyPI semantics say it shouldn't be) — and by index
  rebuild.
- Never lifecycle-expired; expiry would resurrect deleted packages.
- Mirror deletions are local cache management and never replicate — a
  cached upstream file must remain re-fillable forever.
- Tombstones are a storage-layout-contract addition (DESIGN.md §layout);
  treat like a schema migration.

### 6.5 Yank — logical epoch, no wall clocks
Sidecar gains `yank-epoch` (monotonic counter, bumped on every yank/unyank).
Merge = max epoch; equal epoch with conflicting state = yanked wins
(fail-closed); equal epoch, both yanked, different reasons = the record
with the lexicographically smaller sidecar sha256 wins, purely to be
deterministic. Wall-clock LWW is banned: two buckets have two clocks, and
skew makes verdicts side-dependent and non-convergent.

### 6.6 Counters — not replicated
Per-bucket stats; they are derived and lossy by design. Sum offline if it
ever matters.

## 7. Coordination state

Almost none — deliberately. There is no epoch object, no ACTIVE/DRAINING
status, no cluster membership. Two pieces of shared state exist:

- **Per-bucket leases** (existing, unchanged): each bucket has its own
  `_leader/` lease; leaders do idempotent derived work there.
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
  Validation requires only reachable buckets — a standby node must be able
  to boot during the exact outage it exists for. Changing the topology
  (replacing a dead bucket, adding one, reordering) is an explicit
  operator action — `pypiron buckets migrate` bumps the generation and
  re-stamps reachable buckets — so disaster recovery never bricks on a
  stale stamp.

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

## 9. Security posture

- Same-trust buckets (§2). Fail-closed error classification (§5).
- **Dependency-confusion window**: a brand-new private name uploaded during
  a dual-write window can be proxy-filled from upstream on the other side
  until the claim replicates (seconds normally; partition-length during a
  split). Steady state self-heals via §6.2, but a client may have fetched
  upstream bytes in the window. Mitigations, in order of strength:
  1. Reserve your private namespace in `[mirror]` include/exclude on all
     nodes (structural — closes the window entirely; required guidance for
     private+proxy deployments).
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

- **Uploads pause seconds** on a true bucket outage: the node's own
  leave-hysteresis plus one client retry. Reads don't pause at all.
  Physics still applies — you cannot distinguish "down" from "slow"
  instantly — but because a wrong selection is safe, detection no longer
  has to be conservative.
- **Cross-bucket duplicate rejection is best-effort.** Two uploads of the
  same filename through nodes selecting different buckets during a
  deviation window both receive 200 and freeze on merge (§6.3). Within one
  bucket — steady state, i.e. almost always — the 409 remains immediate.
- **RPO = replication lag** for a destroyed (not partitioned) bucket:
  private uploads acked in the last seconds may need re-publishing —
  idempotent, and the uploader gets a clean error/retry.
- **Frozen conflicts require a human** (§6.3). By design.
- **Caches/CDNs** may serve pre-freeze bytes for a conflicted filename until
  TTLs expire. No merge rule can un-serve bytes.
- **Issued presigned URLs cannot be revoked by a flip**; their TTL (knob,
  keep it short) bounds the window. A client mid-install across a flip can
  observe mixed generations — an index fetched from the old bucket, a file
  404ing on the new — for one resolve cycle; install-level retry heals it.
- **A write landed on a non-preferred bucket** during a deviation window
  reaches the other buckets within a marker-drain interval, not instantly.
  Bounded, never stranded.

## 12. Implementation plan

Phases ship independently; each is valuable before the next lands.

- **P0 — plumbing**: repeatable bucket config; construct all backends;
  topology stamps. No behavior change with one bucket.
- **P1 — pin-at-entry**: every `state.storage` consumer captures once at
  operation entry; generation-tagged caches. Wide but mechanical; the
  proxy's mid-transaction re-reads are the known hot spot.
- **P2 — data-plane hardening** (all of it also fixes latent single-bucket
  races): per-artifact `origin` + `yank-epoch` sidecar fields; origin CAS +
  generation fencing (proxy pre-commit re-check, conditional
  `release_empty_claim`); tombstones + reuse-after-delete ban.
- **P3 — replicator**: eager upload-time fan-out + `_repl/` todo markers +
  full-diff backstop (private-only, claims-fast, sidecar-first,
  sha256-verified); reconcile mode is the same job with merge rules armed.
  At this point a second bucket is a warm, continuously verified DR copy —
  shippable value before P4's selection machinery exists.
- **P4 — selection**: per-bucket health views, strict error
  classification, asymmetric leave/return hysteresis, generation bump on
  switch, audit-on-selection, topology stamps + `buckets migrate`.
- **P5 — proof**: blackbox matrix on two + three MinIOs — partition/heal,
  selection switch under upload storm, cross-bucket duplicate
  conflict-freeze, demotion race (proxy fill vs private upload vs merge),
  tombstone convergence, yank-epoch convergence, straggler-write delivery
  via markers after drift-back, marker drain after extended replica
  outage, preferred-bucket flap (selections must settle, service must
  never oscillate), cold-start under partition (divergence merges;
  reserved private names stay protected). Docs: configuration.md knobs +
  a user-manual page written outcome-first ("give pypiron two buckets in
  different regions and it survives either one dying").

## 13. Open questions

- Per-bucket region/endpoint syntax (`region:bucket`? auto-discover via
  GetBucketLocation?) and whether mixed providers (S3 + GCS) are v1 or
  later.
- Per-bucket credentials — S3 creds are env-global today; needs config
  plumbing if customers want distinct principals per bucket.
- Replication cadence knobs and the fast-tick interval for claims.
- Alarm surface for frozen conflicts (log+metric now; webhook later?).
- Leave/return hysteresis defaults (seconds to leave, minutes to return) —
  tune against the flap blackbox test.
