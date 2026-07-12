# Multi-bucket replication & failover

Status: **implemented** — the architecture and merge contract for pypiron's
multi-bucket mode. Companion to [DESIGN.md](DESIGN.md); the storage-layout
additions are recorded in both documents. This document describes the shipped
design (the "v2" synchronous fan-out design). The earlier async multi-master
design it replaced is summarized in the postmortem at the end.

## The design in one paragraph

Hand pypiron an ordered list of buckets instead of one and it survives the loss
of any bucket — a region, or a whole cloud provider. An upload streams to
**every healthy bucket concurrently before it is acknowledged**, so a 200 means
every reachable bucket already holds the record. The **serialization bucket** —
the first healthy bucket in configured order, the preferred bucket in the common
case — is where duplicate-filename 409s, tombstone checks, and the origin claim
happen, exactly once per upload. If a secondary bucket fails or times out, a
durable **repair note** is written before the ack and drained later. Reads pin
one bucket at entry and serve; because every bucket holds everything at ack time,
read failover is trivially correct with no replication-lag windows. The rare
cross-partition byte conflict resolves **first-uploaded-wins, loser quarantined
— never deleted — alarm always**. Everything still converges; it just rarely has
anything left to converge.

## 1. Goal

Survive the loss of any one bucket — including a full cloud-region or
cloud-provider outage — with:

- **Reads**: zero downtime. Installs never stop.
- **Uploads**: durable on every reachable bucket at the 200; a node that cannot
  reach a bucket selects the next one. No manual promote, no runbook.
- **Customer contract**: a list of bucket URIs, order = preference. That is the
  entire configuration surface. All failover, replication, reconciliation, and
  conflict handling lives inside the binary.

The owner's bar, stated plainly: **reads must just work; rare upload races may
be handled imperfectly in exchange for a simpler design.** That trade is why the
fan-out is synchronous and pre-ack (no read-path replication lag to fence
around) and why conflicts resolve by a cheap approximate rule plus quarantine,
not a clock-free merge lattice.

Principle: we build the tricky internals and nail them so the operator doesn't
assemble fleets, DNS runbooks, or cross-region-replication rules. The best code
is no code *for the customer*.

## 2. Customer contract

```
pypiron serve --buckets s3://iron-east@us-east-1,s3://iron-west@us-west-2
PYPIRON_BUCKETS=s3://iron-east@us-east-1,gs://iron-eu
```

- Two or more buckets, order = preference. `buckets[0]` is the **preferred**
  bucket; at any moment each node has one **selected** bucket — the first it
  observes healthy.
- Entries are URIs with a required scheme: `s3://name[@region]` (`@region` is
  S3-only), `gs://name`, `az://container`. A mixed list (S3 + GCS + Azure) is a
  first-class configuration. Order is the only preference signal.
- A **one-entry** `--buckets` list is equivalent to configuring that one backend
  directly; the failover machinery stays dormant when there is only one handle
  (`BucketSet::is_multi()` is false).
- Single-bucket mode via `--storage` + `--s3-bucket` / `--gcs-bucket` /
  `--azure-container` is unchanged. `--s3-bucket` selects one S3 bucket and is
  distinct from the multi-cloud `--buckets` list.
- Buckets are **same-trust infrastructure**. A hostile bucket is out of scope:
  whoever writes your buckets owns your index, list of one or list of five.

## 3. Pin-at-entry (the isolation invariant)

Every operation captures the storage context **once at entry** and performs all
its reads and writes against that handle. The context is one immutable
`(Arc<dyn Storage>, generation)` behind a single atomic swap — never two
separately-loaded values. A selection switch changes what *new* operations
capture; in-flight operations finish or fail on the bucket they started on. A
torn cross-bucket record is impossible by construction; a half-finished upload on
a dying bucket is exactly a crashed upload, which the audit machinery already
heals.

Long-lived authority is re-checked: per-bucket leases embed the generation they
were acquired under, and audit runs re-validate the live generation before each
write batch. The one deliberate two-handle exception is **replication** (read
source, write destination): both handles are captured at batch start, every
write is conditional, and a batch that errors re-derives from notes/diff next
cycle.

## 4. Upload protocol (synchronous fan-out)

1. **Coordinate** on the serialization bucket — the node's selected bucket,
   which in steady state is `buckets[0]`: tombstone/frozen check, origin claim
   (create-if-absent, or verify private), artifact create-if-absent. A 409 here
   is the client's 409. The serialization *rule* is fixed by configured order —
   the first healthy bucket — so there is no election and no agreement problem.
2. **Fan out** (`replicate::fanout_sync`): stream the record to every other
   healthy bucket concurrently. Each destination gets the same copy protocol as
   the sweep and full diff — origin claim, sidecar, companions, then the
   **sha256-verified artifact last**. Every destination job is started before any
   is awaited, so a blackholed middle bucket cannot delay a healthy later one.
3. **Deadline**: secondaries share one grace deadline measured from the selected
   write's completion — `--fanout-grace-secs` (`PYPIRON_FANOUT_GRACE_SECS`,
   default 30). It is a grace *past primary-done*, not an absolute timeout, so
   one hung bucket adds at most that much ack latency.
4. **On secondary failure / timeout / mid-copy ineligibility / already-
   ineligible**: write a durable repair note
   (`_repl/<dest-index>/<pkg>/<file>!<nonce>`) in the selected bucket, **before**
   the ack.
5. **Ack.** Happy path: every bucket holds the record at 200 and no note is
   written. Degraded path: the selected bucket holds it plus one note per missing
   bucket.

A secondary that dies mid-multipart is free — an uncompleted multipart upload is
invisible. If the *serialization* write fails, the upload fails: there is no ack
without a serialization point.

Deletes, yanks, and project-status changes fan out the same way: the mutation is
applied to the selected bucket first, then to every healthy bucket, with notes
on failure. Tombstone-before-body-removal ordering is kept on every bucket.

A fenced topology or an ineligible selected bucket skips all copies and writes a
note for every peer, so the eventual heal drains it. Health probes and topology
verification are the only writers that get past that gate.

## 5. Conflict resolution

A byte conflict requires **all** of: the serialization point moved (an outage or
partition), the same filename uploaded on both sides, different bytes. In the
non-outage case first-wins is enforced *exactly and for free* by the
serialization bucket's create-if-absent. At corporate write rates the outage
case is a per-decade event; the machinery stays proportionate.

The merge (`replicate::decide`, pure and unit-tested) has precedence
**tombstone ≻ origin (private ≻ mirror) ≻ union ≻ freeze**:

- **First-uploaded wins.** Each private sidecar carries `upload-epoch-ms`, the
  server-stamped receive time. The record with the **older** epoch wins; the
  loser is **quarantined, never deleted** (`_quarantine/<pkg>/<file>@<sha12>`),
  and an **alarm always fires** (`pypiron_replication_freezes_total`).
- **Degenerate case → freeze both.** The epoch is a tiebreak hint, not a truth
  machine. If the two epochs are within `CONFLICT_SKEW_MS` (2 s), or either side
  is missing the field (a legacy or mirror sidecar), the conflict is
  unresolvable by clock and degrades to **quarantine-both + alarm** — both bodies
  preserved behind `.frozen` + a tombstone, the filename suppressed from every
  index and direct read. A human resolves by publishing a new filename.
- First-wins is the correct policy for a package index: the first upload was
  acked and possibly already installed; bytes under a filename must never change
  out from under users.
- **Precedence kept from the merge algebra**: tombstone beats everything (deletes
  never resurrect); origin **private beats mirror** (the dependency-confusion
  boundary); yank state merges by **max `yank-epoch`**; project status merges by
  max `pypiron-epoch`.

### Per-artifact mirror demotion (no staging)

When private truth and a mirror cache collide under one name, private wins per
artifact: `replicate_record` drives the destination's `.origin` to private
(direct CAS), copies the mirror body to `_quarantine/` and drops a
`.mirror-quarantined` marker beside the canonical key, then copies the private
record over. The canonical mirror key stays occupied (a delete/recreate ABA
cannot be distinguished by ETag), inert and unindexed behind the marker until a
later private sidecar proves the demotion happened, at which point the marker is
inert. This is a per-artifact operation on the ordinary copy path — there is no
staged package-promotion protocol, no manifest, and no promotion lock.

## 6. Reads

Every healthy bucket holds every record at ack time, so read failover is
trivially correct: pin the selected bucket at entry, serve. No replication lag
exists in steady state, which is why v2 has none of the read-path lag windows the
async design fenced around (presign-404, 404-during-lag, proxy fill against a
claim-lagged bucket).

The visibility fence shrinks to tombstone/frozen/quarantine checks — no
pending-manifest states. Reads pay no per-download tombstone HEAD in
single-bucket mode (the single-bucket-pays-nothing rule); the multi-bucket fence
adds control-object reads but no artifact-existence probe. Presigning stays blind
local HMAC math.

**Crash-orphaned body repair.** A delete that crashed between the tombstone write
and the body removal leaves private bytes downloadable by direct URL. Rather than
pay a per-download HEAD, the worker's audit (boot + periodic) repairs it for free
from the shard listing it already walks: a live artifact body sitting beside a
bare `.tombstone` (no freeze) is a crashed delete
(`tombstone::complete_interrupted_deletes`) — it drops the body and companions
and keeps the tombstone. This runs in both single- and multi-bucket modes.

## 7. Repair and convergence

Three tiers, fastest first, each backstopping the one above:

1. **Repair notes** (`_repl/`): the failure-only output of §4, drained by the
   sweep. The sweep fires **immediately on a bucket's unhealthy→healthy
   transition** (`repl_sweep_requested`) and on a slow periodic backstop —
   `--repl-sweep-interval-secs` (`PYPIRON_REPL_SWEEP_INTERVAL_SECS`, default
   300), **decoupled from `--worker-interval-secs`** so the 1 s tick never drives
   `_repl/` LISTs. Sweeping is cheap because notes exist only after failures.
   Listing is paged/bounded — the whole backlog is never resident at once.
2. **Full-tree reconcile**: the lost-note backstop, default daily
   (`--reconcile-interval-secs`) plus on boot (`--audit-on-boot`). Paged listing,
   **sha256 comparisons only — never etags** (etag semantics differ per backend).
3. **Crash ordering**: artifact-last, sha256-verified copies everywhere, so a
   crash at any step leaves an inert, non-lying destination.

An orphaned body-less mirror sidecar at a destination must not block a private
copy: the repair copy overwrites a sidecar that has no artifact behind it. An
orphan *artifact* (no readable sidecar) is never replicated as-is — that would
fabricate truth; the destination's own audit backfills a sidecar first.

## 8. Health and selection

Each node independently maintains a health view of every configured bucket and
selects the most-preferred bucket its view calls healthy. There is no failover
event, no cluster agreement about which bucket is "the" bucket, and no
coordinator. A wrong or stale selection is safe — it is a brief window that
reconciliation absorbs.

- **Probe**: one small **GET of the topology stamp** per bucket. GET, not HEAD,
  because a body-less HEAD 404 cannot distinguish a missing key from a deleted
  bucket, while the guaranteed stamp GET preserves `NoSuchBucket`. Every probe
  has a one-second deadline on a dedicated loop, independent of index, counter,
  and replication work; sweeps never overlap.
- **Classification is fail-closed**: timeouts (including HTTP 408), connection
  failures, and 5xx are availability failures. 403/401 (credentials), 412 (CAS),
  and KMS/quota/config errors **alarm loudly and never change selection** — a
  misconfigured node must not wander off the preferred bucket.
- **Hysteresis is asymmetric**: leave a failing bucket after
  `--bucket-leave-failures` consecutive failures (default 3); return to a
  recovered more-preferred bucket only after `--bucket-return-healthy-secs`
  continuous healthy seconds (default 300), so a flapping bucket settles into
  "stay put."
- **Probes are traffic-gated.** Full cadence runs only when there has been recent
  request traffic **or** any bucket is currently unhealthy — re-probing an
  unhealthy bucket is the only way heal-back happens, so that is never gated off.
  Idle cadence decays. Accepted cost: the first request after an idle period may
  pay one bounded (~1 s) discovery timeout before failover.

Multi-bucket S3 disables SDK retries; the one-second probes switch new traffic
and cancel background work when a bucket becomes ineligible. An artifact transfer
already in flight keeps the normal one-hour route bound so a legitimately slow
upload is not cut off.

## 9. Topology stamps, migration, and the drain gate

Every reachable bucket carries `_topology/stamp.json`: a hash of the ordered
bucket identities plus an operator-controlled generation, CAS-written-or-verified
against every *reachable* bucket at startup and re-verified on every reachability
transition. Two deployments disagreeing about bucket order is the one
misconfiguration that turns "disagreements are rare" into "disagreements are
constant," so it is the one thing checked hard.

- **Startup mismatch** → refuse to start.
- **Runtime mismatch** (a healed partition reveals a bucket stamped by a
  differently-ordered deployment) → leave reads up, set a **sticky write fence**
  until restart (`pypiron_bucket_topology_write_fenced`).
- **Zero reachable buckets at startup → refuse to start** (fail-closed,
  deliberate). The operator sees `cannot verify topology: no configured bucket
  is reachable at startup` and the node exits non-zero. This is the fix for the
  v1 bug where a node with nothing to verify against wrote a fabricated
  generation 0 and then permanently write-fenced itself once the real buckets
  came back at a higher generation. The rule is *at least one* reachable bucket,
  not all of them: a standby brought up during a partial outage validates
  against whatever is reachable, so it boots as long as one configured bucket
  answers — it just cannot boot into a total blackout.
- **Changing the bucket list** (replace a dead bucket, add, reorder) is an
  explicit operator action: stop the fleet, run `pypiron buckets migrate` with
  the new complete list, restart every node on it. `migrate` bumps the generation
  and re-stamps reachable buckets, reporting any it could not reach.
- **The drain gate**: `buckets migrate` **refuses while any bucket has undrained
  `_repl/` notes**, so shrinking or reordering the list can never strand a sole
  copy on a bucket that is about to be removed. The operator lets the sweep drain
  first, then retries.
- **Removing a bucket is checked hardest of all.** A bucket being dropped from
  the list holds `_repl/` notes as a fan-out *source*, and a stranded note there
  can be a record's only copy — the surviving-bucket check cannot see it. So
  `migrate` reads the previous topology from the reachable new-list buckets and,
  for every member the new list drops, requires that bucket to be **reachable and
  note-free**. Unlike a surviving bucket (skipped when unreachable), a removed
  bucket that cannot be reached is a **refusal**: migrate will not drop a bucket
  it could not prove drained. Bring it back, let the sweep drain, then retry.
- **Shrinking to one bucket** is the exception: stop the fleet and restart with
  the lone bucket. Single-bucket topology is dormant — no stamp to migrate.

## 10. Multi-cloud

Nothing above is S3-specific. The bucket list may mix backends. All three clouds
expose strongly-consistent conditional writes uniformly through `object_store`
(S3 `If-None-Match`/`If-Match`, GCS generation preconditions, Azure ETag
`If-Match`), and the design's whole vocabulary is get / put / list /
put-if-absent / put-if-match.

- **Per-backend credentials.** Each entry is built with its backend's native
  builder (`build_one_s3` / `build_one_gcs` / `build_one_azure`): S3 resolves the
  standard AWS chain, GCS uses a service-account key or Application Default
  Credentials, Azure uses its account key. A bucket whose backend is
  half-configured fails startup closed. Every URI is parsed up front, so one bad
  entry fails before any bucket is contacted.
- **Backend-neutral error classification.** Availability vs. alarm
  classification is derived from the `object_store` error, not from S3-specific
  types, so failover and fail-closed behavior are identical across clouds.
- **Constraints**: every content comparison is sha256, never etag (etag
  semantics differ per backend and across multipart strategies); the preferred
  bucket should be the nearest/cheapest; cross-cloud replication pays egress
  (~$0.05–0.09/GB — noise at real upload rates). The payoff is surviving an
  entire cloud provider's outage.

## 11. Accepted imperfections (by choice, not accident)

- Upload ack latency = slowest healthy bucket (+ bounded grace on failure).
- Conflict resolution defers while the serialization bucket of record is
  unreachable; reads are unaffected.
- "First uploaded" is approximate across a partition; the 2 s skew freeze +
  quarantine + alarm make a wrong call recoverable.
- First request after an idle period may pay one ~1 s health-discovery timeout.
- A flapping bucket generates repair-note churn per upload instead of background
  lag; the same repair machinery absorbs it.
- **RPO = fan-out shortfall**: a bucket *destroyed* (not partitioned) in the
  seconds after a degraded ack may need the last few writes re-published.
- Issued presigned URLs cannot be revoked by a switch; their one-hour expiry
  bounds the window. A client mid-install across a switch may retry once.
- Frozen conflicts require a human. Caches/CDNs may serve pre-freeze bytes until
  TTLs expire — no merge rule can un-serve bytes.

## 12. Operator rules the design requires

- **Restore-from-backup is fleet-wide at one generation, never a single bucket.**
  Restoring one bucket from a stale snapshot is a documented operator error: the
  merge is history-blind and will resurrect restored claims.
- **Changing the bucket list = stop the fleet + `pypiron buckets migrate` +
  restart.** Migrate refuses while `_repl/` notes are undrained. Never deploy two
  different ordered lists.
- **Protect private names when proxying**: a brand-new private name uploaded
  during a dual-write window can be proxy-filled from upstream on the other side
  until its claim replicates. Set `--private-prefix` on every node (it both
  constrains new private names and forbids proxy fills under the prefix), or put
  every exact private name in `[mirror]` excludes. Claims replicate ahead of
  artifacts to shrink the window; in steady state (all nodes on one bucket) the
  window does not exist.

## 13. Configuration surface

| Knob | Env | Default | Meaning |
|---|---|---|---|
| `--buckets` | `PYPIRON_BUCKETS` | — | Comma-separated bucket URIs, scheme required, order = preference. |
| `--fanout-grace-secs` | `PYPIRON_FANOUT_GRACE_SECS` | 30 | Grace past the selected write before a lagging secondary gets a repair note. |
| `--repl-sweep-interval-secs` | `PYPIRON_REPL_SWEEP_INTERVAL_SECS` | 300 | Periodic `_repl/` note-sweep backstop; the sweep also fires on bucket heal. |
| `--bucket-leave-failures` | `PYPIRON_BUCKET_LEAVE_FAILURES` | 3 | Consecutive availability failures before leaving the selected bucket. |
| `--bucket-return-healthy-secs` | `PYPIRON_BUCKET_RETURN_HEALTHY_SECS` | 300 | Continuous healthy seconds before returning to a more-preferred bucket. |
| `--reconcile-interval-secs` | `PYPIRON_RECONCILE_INTERVAL_SECS` | 86400 | Full-tree reconcile (and single-bucket audit) interval. |
| `--audit-on-boot` | `PYPIRON_AUDIT_ON_BOOT` | true | Run the audit / full diff at boot. |
| `--intent-grace-secs` | `PYPIRON_INTENT_GRACE_SECS` | 900 | Crash-recovery grace for an in-flight write's rebuild. |

Storage-layout objects (`_repl/`, `_quarantine/`, `.mirror-quarantined`,
`.frozen`, `_topology/stamp.json`, the `upload-epoch-ms` sidecar field) are the
schema — see [DESIGN.md](DESIGN.md#storage-layout-the-contract).

## Metrics

These series appear only with two or more buckets:

| Metric | Meaning |
|---|---|
| `pypiron_replication_objects_total` | Artifact records copied into another bucket. |
| `pypiron_replication_bytes_total` | Artifact bytes copied into other buckets. |
| `pypiron_replication_freezes_total` | Conflicting same-name, different-byte uploads quarantined/frozen for a human. Any increase needs attention. |
| `pypiron_replication_marker_backlog{dest}` | Undrained repair notes found on reachable source buckets (a lower bound during a source outage). |
| `pypiron_reconcile_diff_duration_seconds` | Wall time of the last full comparison. |
| `pypiron_bucket_health_state{bucket,index}` | Per-node view: healthy `1`, unknown `0`, unhealthy `-1`. |
| `pypiron_bucket_selected{bucket,index}` | Per-node selected bucket. |
| `pypiron_bucket_health_alarms_total{bucket,index}` | Errors that do not prove an outage (credentials, CAS, KMS, quota, config). |
| `pypiron_bucket_selection_generation` | Changes when this node selects another bucket. |
| `pypiron_bucket_topology_write_fenced` | `1` when a runtime topology mismatch has stopped mutations; reads stay up. |

Alert on any freeze, a non-zero topology fence, persistent health alarms, or a
backlog that keeps growing after its destination recovers.

## Postmortem: the async v1 design (superseded)

The first shipped multi-bucket design was async multi-master: an upload landed in
one selected bucket and replicated *after* the ack; any bucket accepted writes
during a partition; a symmetric, clock-free merge lattice (plus a ~1,700-line
mirror→private demotion staging subsystem — staged manifests, promotion locks,
heartbeats, intent families, pending-manifest origin states) reconciled the
divergence afterward.

It optimized for a requirement the owner does not hold — uploads staying
available through any partition, handled perfectly. Its async replication was the
root of the read-path lag windows (presign-404, 404-during-lag, proxy
dependency-confusion). v2 removes the lag instead of fencing around it, replaces
the merge lattice with first-uploaded-wins + quarantine, deletes the demotion
staging subsystem in favor of per-artifact quarantine, and moves fan-out into the
request pre-ack. The hardest ~2–3k lines of the async design are gone;
replication became a rare-path repair mechanism instead of a hot-path pipeline.

The full async design and its four adversarial review rounds live in this file's
git history (it was the original `dev/MULTIBUCKET.md`, before the v2 rewrite). The
durable lessons it contributed — pin-at-entry, fail-closed health classification,
tombstone-before-body ordering, sha256-never-etag comparison — are kept above.
