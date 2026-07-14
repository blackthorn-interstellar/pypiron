# Read affinity

Every region serves reads from its own bucket; writes keep a single home. A
multi-region fleet gets local read latency and zero steady-state cross-region
egress with nothing new to configure and no weakening of upload serialization.

## Thesis

Synchronous pre-ack fan-out already made every bucket a full copy before the
client hears 200 — read locality is just picking the local copy. That gives
the whole feature one rule: **presence is proof; only absence can lie.**
Anything found on the region bucket — artifact, index page, claim, tombstone —
is real and served locally. Decisions stay where writes serialize: bytes come
from the near bucket, judgment comes from the write bucket. Absence is handled
by how much it can hurt: an absent artifact or page reads through to the write
bucket before any 404; an absent origin claim that would permit upstream
fall-through is confirmed centrally first; an absent tombstone is trusted,
accepting a bounded window.

A node knows its region the way it knows its hostname: it asks its platform.
Region is a fact, declared on buckets and detected on nodes — never inferred
from timing.

## Constraints (immutable)

- One write home. Uploads, first-write claims, deletes, yanks, and all
  coordination state serialize on the single fleet-agreed bucket. Read
  affinity never moves a write.
- No latency measurement. No probes timed, no adaptive routing, no
  timing-driven behavior anywhere in the feature.
- Zero required configuration. Cloud nodes detect their own region; buckets
  carry an optional region label in the fleet-shared list; an on-prem node may
  be told its region. A node that learns nothing behaves exactly as today.
  Nothing per-node persists, and the shared bucket list and its order stay
  fleet-identical.
- Misdetection costs speed, never correctness. Every wrong answer about
  region degrades to today's behavior.
- Fail-closed on names. A private name never falls through to upstream on the
  strength of a locally missing claim.
- A lagging region bucket is slower, never wrong: no client sees a 404 for an
  acked file, and reads return to a recovered bucket only after it has caught
  up.

## Non-constraints (accepted)

- A remote region's index may lag a publish by seconds for an existing
  package; brand-new packages are covered by read-through.
- A yank or delete whose copy to one bucket failed stays visible in that
  region until the repair sweep drains — the same window a failed-over node
  has today.
- Which node a client reaches is the front door's problem, not ours.
- No cost-aware bucket choice, no read load-spreading, no per-request bucket
  selection. One read home per node, one write home per fleet.
- Fleet-wide listing pages are not on the install path and need not be local.

## Plan

1. Name regions: an optional region label per bucket in the shared list;
   nodes learn their own from operator word, platform environment, or
   instance metadata — in that order.
2. Split selection: beside today's fleet-wide selection, which keeps every
   write and every decision, each node holds a read selection — its region's
   bucket while healthy and caught up, otherwise the write bucket.
3. Serve locally under the polarity rule: presence trusted, dangerous
   absences verified centrally, bounded ones accepted.
4. Prove it with real installers: blackbox tests assert which bucket served
   the bytes, that read-through and the yank window behave exactly as written.
   The manual claims read locality only once the tests pin it.
