# Testing

## Philosophy: blackbox first

The product is an HTTP server speaking standardized protocols to clients we don't
control. So the test suite's center of gravity is **blackbox integration tests**:
build the real binary, start it as a subprocess, and drive it over HTTP exactly the
way the world will.

The ecosystem's real clients are the only conformance suite that matters. A test
that asserts our JSON looks PEP-691-shaped is worth far less than `uv pip install`
actually succeeding against the server. Rules of thumb:

- **Real tools.** Upload with `uv publish` and `twine`; install with `uv pip
  install` and `pip`. If a client behavior matters (e.g. `--exclude-newer`), test
  the behavior end to end, not our half of the contract.
- **Real packages.** Tests download actual wheels from public PyPI, looked up via
  the pypi.org JSON API at test time (no hardcoded blob URLs that rot).
- **Real backends.** Disk mode runs against a tmpdir; S3 against MinIO and Azure
  Blob against Azurite, both in Docker. These tests skip cleanly when Docker is
  unavailable. GCS has no blackbox test: no local emulator faithfully implements
  the GCS XML data-plane that `object_store` uses for writes. Verified against
  object_store 0.13.2 and 0.14.0, both candidate emulators fail on the first
  index write:
  - **fake-gcs-server** (fsouza) routes the unsigned `PUT /{bucket}/{object}` to
    its JSON upload handler and rejects it with `400 invalid uploadType` — it
    accepts that path only for signed-URL uploads, which `object_store` doesn't
    use for a normal put.
  - **Google's storage-testbench** accepts the PUT and even enforces conditional
    writes correctly (`if-generation-match: 0` → `412`), but its PUT response
    omits the `ETag` header (and its GET omits `Last-Modified`), so every write
    fails with `ETag Header missing from response`.

  There is no `object_store` config to relax the ETag requirement or fall back to
  the JSON API. The GCS backend shares the single `object_store`-backed code path
  that the S3 and Azure suites exercise end to end — only its builder config
  differs — so it is covered by construction plus object_store's own GCS test
  suite against real GCS. It now also has live end-to-end coverage: the weekly
  `real-gcs` CI job (below) runs the round-trip against a real GCS bucket when
  credentials are configured — GCS's only end-to-end test, since no emulator can
  stand in for it.
- **Always fresh binaries.** The test fixture runs `cargo build` unconditionally —
  incremental builds make it a cheap no-op, and skipping it would silently test a
  stale binary.

Rust unit tests exist for pure functions only (index rendering, filename/tag
parsing, normalization). Anything involving HTTP, storage, or the worker loop is
tested blackbox. There is no mock-heavy middle layer — mocks would just test our
assumptions about clients instead of the clients.

## Test layers

| Layer | What | How it runs |
|---|---|---|
| Rust unit | Pure functions: rendering, parsing, normalization | `cargo test`, fast, no I/O |
| Model checking | Every interleaving of the event protocol + replication merge, bounded | `cargo test` (`tests/model_*.rs`), deep configs nightly |
| Deterministic simulation | Seeded multi-node fault schedules against the real code | `cargo run --example vopr`, smoke on PR, volume nightly |
| Blackbox integration | Real binary + real clients + real packages, disk / S3 / Azure | pytest, `integration` marker |
| Standards conformance | PEP 503/629/691/700 behavior asserted over HTTP | pytest, part of integration |
| Performance | Hot read endpoints under load, release binary | pytest, `perf` marker, opt-in |

Markers (`pyproject.toml`): `integration`, `s3` (needs Docker/MinIO), `azure`
(needs Docker/Azurite), `chaos` (crash-point fault-injection sweeps),
`compat(client, feature)`, `perf`, `stress`. Default runs exclude `perf` and
`stress`.

## Client compatibility matrix

Tests that prove behavior through a real client binary carry
`@pytest.mark.compat(client, feature)`. Run `make compat` to execute those tests
and regenerate [COMPATIBILITY.md](../docs/reference/compatibility.md), including the client
versions used for the matrix.

## Key scenarios

- **Round trip**: upload a real wheel → appears in package + global index →
  download bytes match sha256 → install into a fresh venv → import works.
- **`--exclude-newer`**: upload an old release, capture a cutoff timestamp, upload
  a newer release; `uv pip install --exclude-newer <cutoff>` must resolve the old
  version, a plain install the new one. This is the end-to-end proof of PEP 700.
- **Standards surface**: content negotiation, api-version, hashes, size, RFC 3339
  upload-time, versions list, name normalization (`/simple/Six/` → six), sha256
  fragments and the PEP 629 meta tag in HTML.
- **Tool matrix**: uv and twine for upload; uv and pip for install.

As features land, each gets its blackbox test in
the same style: yank → pip refuses to pick it unless pinned; immutability →
re-upload of the same filename is rejected; caching → ETag round-trips as a 304.

## Chaos and crash consistency

The storage contract is write-to-tmp-then-rename on one filesystem, so the tests
that matter most are the ones that interrupt it. Two blackbox suites inject real
faults into the real binary and assert the tree is never left corrupt.

- **Upstream fault injection** (`tests/test_chaos_upstream.py`): the proxy fetches
  from an upstream that misbehaves on purpose — a body truncated mid-stream, a
  `500`, a hash that doesn't match the metadata, a connection that hangs. Each
  fault must surface as an error to the client and leave *nothing* behind: no
  half-written blob in the cache, no dangling index entry, no poisoned file that a
  later request would serve as good. A clean retry after the fault clears succeeds.
- **Fleet convergence under kill** (`tests/test_chaos_fleet.py`): three nodes share
  one MinIO bucket and take concurrent uploads while one is `SIGKILL`ed mid-write.
  After restart the fleet must converge — every node renders byte-identical
  indexes, and every acknowledged upload installs from every node. This is the
  proof that an interrupted write can't split-brain the shared truth.

Both run in Docker and skip cleanly without it, like the rest of the S3 suite.

## Disaster-recovery drill

`make dr-drill` (or `uv run -- pytest tests/test_dr_drill.py -s -n 0`) runs a
real backup → wipe → restore → reinstall loop against the actual binary over
HTTP. It is a **correctness/trust proof**, not a benchmark. The single number
it prints:

```
DR DRILL: 10/10 restored byte-identical, 0 lost, 0 byte-altered.
```

The drill uploads N=10 wheels under `unique_package()` UUID names, takes a
`tar` snapshot of the data-dir, uploads one *more* package after the snapshot,
stops the server, `rm -rf`s the data-dir, restores **only `packages/`**
(artifacts + `.meta.json` sidecars — not the `simple/` views), regenerates the
views offline with `rebuild-index`, gates on `verify-index` exiting 0, then
stands up a fresh server on the restored dir and reinstalls every pre-backup
package.

A hostile reviewer's four claims are made impossible by construction:

- *"the install secretly hit PyPI."* The server runs with **no upstream
  proxy**, the names are UUIDs absent from PyPI, and uv is pointed at
  `--index-url <restored>` only (default PyPI replaced, not extended). Any one
  suffices.
- *"the wipe was faked."* The data-dir is asserted empty after `rm -rf`.
- *"the views were never regenerated."* Only `packages/` is restored, so
  `simple/` is asserted absent before `rebuild-index` and present after, gated
  on `verify-index` == 0.
- *"byte-identity was never checked."* The exact bytes the restored server
  serves for each artifact are sha256'd against the pre-backup manifest.

The package uploaded *after* the snapshot is absent from the restore — the
drill asserts its index 404s, then re-uploads it successfully. That gap is the
whole RPO story: whatever lands between two backups is what a restore loses.
The RPO is exactly your backup interval, nothing subtler. Truth is
`packages/`; the views are a regenerable projection, so a backup only has to
capture truth.

At N=10 the maintenance steps are effectively instant (tar ~0.15 s,
`rebuild-index` ~0.05 s) — meaningless as a scale figure. The real datapoint
is in [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md): a cold rebuild-everything
of a **5,001-package** store measured **~140 s** on that rig. Don't quote the
drill's sub-second numbers anywhere outward-facing.

## Machine-checked models (stateright)

The chaos suites *sample* failure schedules; the models in `tests/model_*.rs`
*enumerate* them. Two stateright models cover the two
protocols everything else rests on:

- **Event protocol** (`tests/model_event_protocol.rs`): writers running the
  intent/commit marker dance, workers running list → rebuild → global-index CAS
  → delete-observed-markers, crashes between any two steps, and clock advances
  past the intent grace. Checked invariants: an acknowledged upload is durable
  and eventually visible; at quiescence every view equals a fresh derivation
  from truth; a tombstoned file never resurrects.
- **Replication merge** (`tests/model_replication.rs`): two buckets, partition-
  shaped double publishes, mirror-vs-private races, yanks, deletes, interrupted
  freezes. Checked invariants: buckets converge at merge fixpoint; acknowledged
  bytes are never silently lost (conflict losers land in quarantine, only an
  authorized delete destroys); deletes settle dead everywhere.

The models don't get to invent the semantics they check: transitions call the
real `worker::consumable_dirty_work` and `replicate::decide`, and two
conformance suites keep the rest honest — `tests/conformance_tick.rs` drives
the real `worker::tick` against marker fixtures and asserts it consumes exactly
what the selection rule licenses, and `conformance_execute_matches_model`
(in `tests/model_replication.rs`) enumerates record pairs, runs the real
`replicate::execute` on in-memory buckets, and requires the model executor to
predict the resulting state exactly. Code and model can only drift into a
failing test.

Bounded configurations run in `cargo test` (seconds); the deep configurations
run nightly (`.github/workflows/simulation.yml`) with their state counts
published in the job summary.

## Deterministic simulation (the VOPR)

`examples/vopr.rs` (the FoundationDB/TigerBeetle
technique): an entire multi-node fleet — nodes, buckets, writers, the worker,
replication fan-out, sweep, and tree diff — runs single-threaded on a paused
tokio runtime. Wall-clock reads route through `src/clock.rs`'s simulated
override, storage goes through `sim::SimStorage` behind per-node fault views,
and every scheduling choice derives from one 8-byte seed: injected latencies,
storage failures, node crashes at storage-op boundaries (power cut between two
ops — the task is parked and aborted, in-memory state dies, storage survives),
cold restarts, and clock jumps past the intent grace.

The *workload* derives from the seed too, and that matters more than it sounds.
A fixed workload is the quiet way a simulator stops finding things: for a long
time this one ran two packages × two files and one hardcoded op mix on every
seed, so tens of millions of interleavings re-explored a single shape and the
extra seeds bought nothing. Each rotating seed now draws its own entity count
(1–6 packages × 1–4 files, skewed small so throughput survives) and its own
op-class weight vector — swarm testing: instead of one average mix, sample many
extreme ones, because a rare interleaving is only rare *under the average*. The
seed that finally broke the global-index convergence path (below) drew a
delete-heavy mix no fixed workload would ever have run. Every class keeps a
nonzero floor, so no seed can quietly stop publishing and report green on a run
that verified nothing.

Rotation is a pure function of the seed alone, so `--seed N --rotate`
reproduces a failure exactly — including the entity counts and the weight
vector, which have no useful flag form. Every failure prints that command and,
beside it, a `profile:` line with the resolved dimensions so you can read the
shape without rerunning. An armed `--break` rides along on that command: paste
a line missing it and you rerun a *defect-free* world, get green, and file a
dead oracle as flaky. A *non*-rotating run (explicit `--nodes/--buckets/…`)
keeps the historical fixed workload — the first two package names, two files,
the pre-swarm op mix — so the **chaos phase** of every pinned seed below is
byte-identical to the one its comment was written against, verified with
`VOPR_TRACE_FILE` dumps rather than assumed. Its **heal phase** is not: leader
rotation (below) moved it for every seed with more than one node, so a pinned
seed's annotation now records the schedule that once found the bug, not one
still proven to reproduce it. Re-mine them against a reverted fix before
trusting those annotations again; they all still pass.

After the chaos phase the harness heals the fleet, drains it to quiescence,
and asserts the invariants below. Each heal round picks a **leader** — the node
that runs `reconcile` and the tier-3 audit — as `(seed + round) % nodes`, and
which node that is decides what a bug *looks like*, which is exactly why it
must not be a constant. A crashed node restarts with a cold `AppState`, so its
audit reloads the global name set from storage and repairs the drift; the node
that won the CAS still holds the stale set in cache and leaves the drift
standing. Pinned to node 0 — disproportionately the crashed one — the harness
kept reporting defects as a SOFT `AUDIT_*` repair when the identical schedule
under a different leader would have failed the HARD `VERIFY: … stale-global-index`
oracle, understating severity and detection rate together. Production elects
its leader by bucket lease and re-elects on every crash, so rotating per round
also exercises that handoff. It stays a pure function of the seed, so
`--seed N` reproduces exactly.

The invariants:

- **durability** — every acknowledged upload is byte-correct, sidecar-backed and
  index-listed on every bucket. Under a partition (below) one filename can carry
  two acknowledged byte-sets; the merge may keep either, so the claim there is
  "the bytes standing on this bucket are bytes somebody acked, or the ones that
  lost are preserved under `_quarantine/`" — plus a fleet-wide clause: a
  conflict is resolved to one survivor or frozen, never left split;
- **freeze justified** — a `.frozen` marker buys a blanket durability exemption,
  so every one must be traceable to at least two distinct byte-sets across the
  fleet's evidence (the acks, plus every `_quarantine/<pkg>/<file>@*` copy).
  Not "≥2 acks": a publish can crash after the bytes commit and before the
  `200`, so an ack-count-only form false-fails every crashed publisher;
- **origin terminality** — once a private upload acks for a package, no bucket
  may end claiming `mirror` for it and no live, unquarantined artifact of it may
  still carry a mirror sidecar. Convergence only asks the buckets to *agree*: it
  would pass a fleet that agreed on `mirror` for a privately-claimed name, which
  is the dependency-confusion window the origin lattice exists to close;
- **ack totality** — dev/DESIGN.md's totality principle, checked at the moment
  the `200` is returned rather than at quiescence, because the heal phase's
  `reconcile` would otherwise repair the defect before any invariant looks: each
  peer bucket already held the record, or the selected bucket already held a
  durable `_repl/<peer>/` note owing it. Crash-only profiles treat a miss as a
  hard violation (the protocol must fan out or note on every schedule); under
  injected storage failures the note write can itself fail, so there it is a
  reported statistic. Scoped to `publish_record` only — proxy-cache fills
  replicate asynchronously with no pre-ack fan-out *by design* (bf913b9), so
  this must never be generalized to all durable writes;
- **views == truth** — `verify::verify_storage`, the same byte-strict oracle
  behind `pypiron verify`, re-renders every view from each bucket's truth and
  diffs the bytes. The product's own checker, not a harness-local approximation;
- **convergence** — all buckets hold identical truth and views;
- **self-consistency** — every stored body is re-hashed and must equal the
  sha256 its own bucket's sidecar publishes. This is the only oracle anywhere
  that re-hashes a body: every other one here, and `pypiron verify` itself,
  reads sidecars and compares them. So a body swapped under a sidecar that
  still names the old sha was invisible to all of them — the two buckets'
  sidecars stay byte-identical, the pair never enters the diverged-key set, and
  `decide` reads them as agreed forever. That blind spot is how the crossed-body
  class survived four fixes aimed at it; it needs no ledger and holds in a
  single-bucket fleet, where nothing else could have seen it at all;
- **tombstone monotonicity** — a filename whose most recent *ack* was a `204`
  never stands in a bucket without its tombstone. A re-publish after a delete is
  legal resurrection, so the rule is last-writer, not set-membership;
- **no leaks** — no `_dirty/` or `_repl/` debris remains;
- **conservation** — acknowledged bytes are never lost without an authorized
  delete. A freeze is *not* an exemption: `freeze_side` writes its marker,
  copies the body to `_quarantine/` and tombstones before it drops anything, so
  a frozen filename owes the fleet every byte-set it acked, findable somewhere;
- **liveness** — the fleet quiesces within the drain budget.

### The partitioned fleet (`--partition`)

Until this landed, **every writer in the VOPR pinned bucket 0**. `BucketSet::new`
selects index 0, nothing in the harness ever switched it, and every workload op
called `state.pin()`. So every byte on buckets 1..N arrived as a *copy* of bucket
0's, two buckets could never disagree about the bytes under one live filename,
and `replicate::decide`'s merge algebra was **dead code in simulation**: over
3,000 rotated seeds, only `Copy` and `Noop` ever fired and not one `.frozen`,
`_quarantine/` or `.mirror-quarantined` object was ever created. dev/DESIGN.md
says outright that correctness must not depend on every node selecting the same
bucket, and calls the private byte-conflict the partition case — a designed-for
production state the simulator had never produced, in the subsystem the product
is marketed on.

`--partition <percent>` is the share of seeds whose fleet is **partitioned**: each
node gets a *home* bucket and authors its uploads and deletes there (node 0 stays
on bucket 0, so the fleet always has a bucket-0 writer), and ~12% of publishes
arrive as mirror fills so two concurrent first-writes can race the pre-artifact
`.origin` fan-out and split a package's claim across buckets. It is a **chaos
dimension like `--break`, not a workload shape**: rotation does *not* derive it,
`--rotate --partition N` is legal and is how the partitioned soak runs, and the
reproduce line carries it because dropping it reruns an aligned fleet — a
different simulation that comes back green. It defaults to **0** and an aligned
seed is byte-identical to the pre-partition harness, verified by diffing five
whole-profile runs (every counter and every reach reading to the digit), not
assumed. That default is load-bearing: the pinned regression seeds, the
`--break` kill proofs and every measured baseline were mined aligned. Below two
buckets it is a no-op — a partition needs something to be partitioned across.

The home is a per-seed property drawn from a **dedicated** rng (`seed ^ const`),
never the chaos stream, so arming partitioning cannot shift which op a chaos draw
picks and `restart_node` can re-read a home without consuming entropy. It is not
a per-write coin flip: `BucketSet::switch` is health-driven and sticky and a pin
is captured once per operation and never torn (design §3), so a per-write flip
would manufacture an interleaving the product cannot produce and any bug found
there would be unactionable. A percentage below 100 is the useful setting for a
long soak, for two reasons: a freeze is *terminal* for a filename
(`publish_record`'s fence bars it forever), so an always-split fleet freezes its
own corpus and starves the durability family; and the aligned seeds are the
regression guard for everything the 931,115-seed soak established. If you want
*more* conflicts, do not add a per-write divergence knob — raise the publish
weight, since a byte conflict needs two overlapping publishes of one
`(package, file)` with different bytes.

**How it is built, and the one thing not to change.** `Pinned`'s fields are all
public, so a workload op constructs the home bucket's pin directly and hands it
to `publish_record`/`delete_record` — both do all their I/O through
`pinned.storage` and address every peer from `pinned.index`, so both are correct
for any pin. The pin carries a distinct `generation` per bucket, because
`generation` is the only key the index and presign caches namespace on
(src/cache.rs); two pins over two buckets at generation 0 would share one cache,
which is exactly why `switch` bumps it.

Ticks, sweeps, reconciles and audits keep the **selected** pin. That is
deliberate and must stay: `state.global_names` and `state.inventory` describe the
selected bucket and carry no bucket key (`worker::drain_dirty_uncached` exists
solely for that reason), and `require_generation` compares against
`state.pin().generation`, so handing a foreign pin to `worker::audit` makes the
product correctly refuse a switch that did not happen. The consequence is worth
stating plainly: a partitioned seed models a bucket that is *written to* but
whose markers are only ever consumed by the destination drain — the state
production reaches when a node switches, writes, and dies before its own worker
ticks. It is not a full `BucketSet::switch`, and a finding that depends on the
node's caches following its writes is out of this harness's reach.

**Every merge verdict now executes.** Re-measured on a **disjoint seed range**
(`--start-seed 9000001`) from the one the partition work was developed against,
all at `--partition 100`. Verdicts are sampled by running the real
`replicate::decide` over every bucket pair × filename at the end of the chaos
phase and at each heal round, so the verdict rows *under*-count arms the
executors resolve between snapshots; the three evidence rows are exact object
counts at quiescence.

| | rotating (86,010 seeds) | multi-bucket (50,884 seeds) | multi-bucket crash-only (52,354 seeds) |
|---|---|---|---|
| `decide` → `AdoptSidecar` | 7,128 on 2,087 seeds | 5,352 on 2,341 | 1,454 on 646 |
| `decide` → `Supersede` | 152 on 58 | 131 on 63 | 52 on 24 |
| `decide` → `QuarantineLoser` | 5,388 on 1,540 | 2,580 on 1,136 | 1,066 on 475 |
| `decide` → `Freeze` | 3,425 on 1,155 | 3,696 on 1,642 | 1,032 on 467 |
| `decide` → `PropagateFreeze` | 906 on 299 | 634 on 288 | 158 on 69 |
| `decide` → `FinishFreeze` | 50 on 22 | 70 on 32 | 8 on 4 |
| `.frozen` objects at quiescence | 46,394 on 13,103 | 58,118 on 19,331 | 47,078 on 16,662 |
| `_quarantine/` bodies | 64,897 on 16,814 | 81,066 on 23,943 | 70,040 on 20,394 |
| `.mirror-quarantined` | 478 on 348 | 708 on 525 | 238 on 203 |

All nine read **zero** on every aligned seed ever run, which is the gap this
closed. The two per-seed readings the gate holds — `MERGE_DIVERGENCE` and
`FREEZE_JUSTIFIED` — are *tail-event* slots: they waive the 25% share floor
(measured 48%/39% on a fixed multi-bucket profile, 19%/15% under rotation where
a third of drawn topologies are single-bucket) but still fail on a flat zero, so
a partitioned run that stops producing conflicts is still caught. Re-measured on
the rotating lane at `783d423`: `MERGE_DIVERGENCE` holds at 19% of seeds,
`FREEZE_JUSTIFIED` has fallen 15% → **9%** — the freeze fixes above legitimately
made freezes rarer, which is exactly the drift a fixed floor would have
misreported as a coverage regression.

**The oracle branches that were dead code now evaluate — with one exception,
stated.** Counted by a throwaway instrumented build of `examples/vopr.rs` (probe
counters on each branch, never committed), aligned vs partitioned:

| branch | aligned, 20,000 seeds | `--partition 100`, 130,331 seeds |
|---|---|---|
| DURABILITY exempts a `.frozen` filename | 0 | 132,586 |
| …and `.frozen` was the **only** reason (no tombstone, no acked delete) | 0 | **0** |
| VISIBILITY exempts what the renderer omits (`renderer_omits`) | 0 | 571 |
| …of which the reason is a `.mirror-quarantined` marker | 0 | 571 |
| CONSERVATION finds an acked body only because a `_quarantine/` copy holds it | 0 | 99,630 |
| DURABILITY sees bytes no ack carried | 0 | 14,039 |
| …and the `_quarantine/` preservation clause excuses it | 0 | 13,150 |

The exception is worth naming rather than rounding off: `.frozen` has **never**
been the sole reason DURABILITY skipped a filename. `freeze_side` writes the
marker, quarantines, tombstones, then drops — so every frozen record the fleet
produced also carried a `.tombstone`, and the tombstone check upstream of it
already granted the pass. The `.frozen` disjunct is exercised 132,586 times and
load-bearing zero of them. It stays because FREEZE_JUSTIFIED's argument (a freeze
waives durability, so it had better be a real conflict) is about the *rule*, not
about which disjunct happens to fire first, and a crash between the marker and
the tombstone is exactly the window where it would become load-bearing.

#### Where the two lanes stand

**Every statement in this section is as of a named commit, and the naming is not
pedantry.** The sentence that used to stand here — "both lanes are green" — went
stale twice, both times because it was written in the present tense on the day a
lane happened to be clean. A lane is green *at a commit over a range*; write it
that way or it becomes a lie the next time someone lands a bug. If you are
reading this more than a few commits after the hash below, the honest assumption
is that you do not know where the lanes stand and have to soak them yourself.

**Measured at `8e5916f`**, four 900-second lanes on disjoint fresh ranges, each
one built from a pristine `git archive` rather than the shared `target/`:

| lane | flags (plus `--partition 100` except the first) | start-seed | seeds | interleavings | acked | failing |
|---|---|---|---|---|---|---|
| `rotating-swarm` (**gated**) | `--rotate --require-reach` | 95000000001 | 289,934 | 500,651,992 | 1,510,651 | **0** |
| partitioned `multi-bucket` | `--nodes 3 --buckets 2 --ops 160 --packages 6 --files 2 --require-reach` | 96000000001 | 175,353 | 446,060,585 | 1,575,238 | **0** |
| partitioned `three-bucket` | `--nodes 3 --buckets 3 --ops 160 --packages 6 --files 2 --require-reach` | 98000000001 | 119,947 | 425,337,801 | 1,083,091 | **0** |
| partitioned **narrow** | `--nodes 3 --buckets 2 --packages 1 --files 1 --ops 40 --fail-percent 3` | 97000000001 | 926,399 | 651,139,506 | 802,056 | **0** |

Zero audit view repairs of any class on all four. The first three passed
`--require-reach` with no starved oracle; **the fourth is starved by
construction and its seed count must never be pooled with the others** — see
*The starved-denominator trap* below, which is the whole reason it is listed
separately rather than added in.

The coverage behind those zeros is intact rather than suppressed. On the
partitioned `multi-bucket` lane `MERGE_DIVERGENCE` executes on 47% of seeds and
`FREEZE_JUSTIFIED` on 40%; at three buckets they rise to **62% and 53%** — more
bucket pairs per seed means more chances for one to disagree. Both read a flat
zero on every aligned seed ever run.

The `three-bucket` lane is new here and the row it shadows is newer still. Three
buckets is the multi-region topology the product is *marketed* on, and until
2026-07-27 no lane in `simulation.yml` ran it: every row topped out at two, where
a write has exactly one peer, so the pre-ack fan-out and `execute`'s per-pair work
each ran once per pass and could not be partially applied. At three they run
twice, and a crash between the two halves reaches a state two buckets cannot. A
`DURABILITY` defect that lost acknowledged bytes fleet-wide survived two
multi-million-seed censuses in exactly that blind spot (`1732633`). The gated
matrix now carries a `three-bucket` row at `--partition 0`; the partitioned
three-bucket lane measured above is not yet a workflow row, and adding one is the
open follow-up.

Compare ranges, never time budgets: this box's seed rate swings better than 1.5x
with load, so two `--max-secs` runs explore different ranges and their failing
counts are not comparable.

**Four clean lanes are not a gate, and the reason changed.** It used to be that
the draw was too small: a rule-of-three bound on a run of zeros could not get p
low enough, and more soaking would eventually have earned the gate. That argument
is dead. The census in the `vopr-partitioned` job comment — 5,417,063 fresh
partitioned seeds at `08d94f2` — found **6 failing seeds**, so p is a measured
non-zero of 1.07e-6 with a 95% interval of [4.7e-7, 2.1e-6]. A draw of N seeds
reds with probability 1-(1-p)^N and this job's N is ~91,560, which puts it at 9.3%
of nights red; one red night in twenty needs p ≤ 5.6e-7, and the best estimate is
1.9x that. **When the count was zero, more soaking could earn the gate. It cannot
now — only fixes can.** So `vopr-partitioned` stays `continue-on-error: true`,
with a 15-minute `--max-secs` budget instead of a seed count, because the lane's
output is a *rate* you watch: given a seed count it would stop at its first
failing seed and report nothing.

Read that 5.4M-seed census with its own caveat, which is the next section: 4.1M of
those seeds are the narrow 1-package/1-file profile, which starves four oracles,
so only ~2.1M of the 5.4M are durability-effective. The pooled count is real
merge-algebra coverage and *not* real durability coverage, and the rate quoted
against the full 5.4M is correspondingly optimistic for the durability family.

Do **not** try to reach green by lowering `--partition`. `partition_for` draws
`rng.chance(percent)` once per *seed*, so the flag is a share of seeds and not
of writes: halving it halves the partitioned seeds and any failure count
together, buying a quieter lane by testing proportionally less.

`tests/simulation_matrix.rs` guards it in both directions — the
partitioned lane must keep `continue-on-error`, and the gated matrix must never
acquire it. The aligned matrix and the pinned `ci.yml` regression seeds stay on
`--partition 0`, byte-identical (verified by diffing five whole-profile runs,
every counter to the digit).

98.6% of those failures were one bug — a `.mirror-quarantined` marker that never
converged, because `src/replicate/decide.rs` returned `Noop`, which means "the
two sides agree", over pairs that did not. **Fixed**: the demotion fence is truth
and replicates, the body it suppresses moves to `_quarantine/` on the bucket that
resolved it, and the canonical key ends empty everywhere (dev/DESIGN.md). The
same profile re-measured after the fix reds **2 of 65,473 seeds (0.003%)** —
zero CONVERGENCE, zero DURABILITY.

That one fix closed `ACK_TOTALITY` too, which nobody noticed at the time: the
residue was filed as a second, undiagnosed producer, and it was the *same* arm.
`Noop` is the fan-out's convergence signal, so an arm that answers it over a pair
one bucket has never heard of does not merely leave the fleet diverged — it acks
a `200` with the peer holding nothing and owed no `_repl/` note. Measured on
identical fresh seed ranges with only that arm reverted: **25 hard-gate hits in
344,003 crash-only seeds before, 0 in 276,998 after**, and all five recorded
repros (9300024588, 9300035976, 9300061064, 9504440, 14700042703) flip red and
green with it. The lesson is the same one `fd14f01` taught in the `(Orphan, _)`
arm and is now written into `Verdict::Noop`'s own doc comment: a `Noop` that is
really a deferral is a durability claim, not a shrug.

**Do not write "nothing is left" here again.** That sentence has stood in this
document twice and been false both times within days — the roster empties,
someone records the empty roster in the present tense, and the next census
refills it while the sentence sits there reading as measurement. What is true is
narrower and keeps its shape as the tree moves: *the root causes filed by the
census at `08d94f2` are closed as of `8e5916f`*, each with its minimum reproducer
promoted from the nightly to a pinned `ci.yml` gate so a regression reds a merge
rather than a nightly. Whether new ones are open is a question about the *next*
census, and this paragraph cannot answer it.

The roster the census at `08d94f2` filed, and where each stands at `8e5916f`:

| root cause | closed by | one line |
|---|---|---|
| `AUDIT_REPAIRED_VIEWS` — the global index published HTML and JSON non-atomically and nothing healed the tear | `c9b2e32` | `write_global_indexes_cas` wrote `simple/index.html` then `simple/index.json`, so a crash between them left the bucket serving a PEP 503 index listing a package its own PEP 691 index did not. The reconcile the code installs was unreachable from steady state — `update_global_index` returns early on an empty delta, and the pair check is gated on `!cached.html_current`, which every CAS win pins true. **This one was never a partitioned-lane bug**: it reproduced at `--partition 0`, one bucket, faults off — the *gated* `rotating-swarm` row |
| `DURABILITY` — a spent-fence clear erased the record authorizing a settled demotion | `1732633` | Both pair merges ran `clear_spent_demotion_fence` on bucket 0 with the canonical key already emptied by that bucket's own `settle_mirror_quarantine`. The fence was the only record that the emptiness was authorized. Only ever reproduced at **three** buckets |
| `CONVERGENCE` + `SELF_CONSISTENCY` + `DURABILITY` — a copy published truth over a body the bucket had lost | `8e5916f` | `copy_live` lands the artifact before the sidecar so the sidecar can only describe bytes the bucket holds; `settle_mirror_quarantine`'s delete landed between the two legs, the sidecar published over an emptied key, and a racing create took it with different bytes. Permanent from there: `decide` compares sidecar shas, both buckets read as agreed, and nothing re-hashes a stored body |
| `CONSERVATION` — acked bytes existed nowhere | `8e5916f` | Same delete, one more victim: with nothing preserved the settle read the key empty and deleted it anyway, so a publish creating it in that gap was acked `200` and its bytes then existed nowhere. A re-read cannot close either window — `delete_keys` is unconditional — so the authorization is now the `_quarantine/` copy, not a reading of the key |

The older three, closed before that census and kept for the shape of the class:

- `DURABILITY` (`f37e391`) — a settle decided from a listing-era read re-fenced
  a filename private truth had already resolved, while a concurrent pass holding
  the same record called that fence spent and deleted it, leaving the filename
  with no body and nothing authorizing its absence.
- `CONSERVATION` (`4eaa015`, plus a harness half) — `settle_mirror_quarantine`
  deleted the canonical key blind after copying the body it had *read*, so a
  sibling settle destroyed a private body a racing publish had just created.
  `.mirror-quarantined` is the one fence that deliberately does not bar an
  upload, which is exactly why the key stays writable for the whole settle.
- `SELF_CONSISTENCY` (`bb885ef`) — `supersede_record` was not crash-atomic, so a
  crash left a bucket serving bytes contradicting its own published sha256,
  permanently. The ordering was correct and stays; what was missing is that the
  window left nothing behind saying it had been open, which a `.superseding`
  intent marker now does.

Two of those were found on **narrow** lanes (1–2 packages, 1 file, 40–80 ops),
where they ran about 1 seed in 200,000–500,000. This lane's 12-filename corpus
spreads the workload too thin to concentrate contention on a single name, and
that corpus is load-bearing for the durability oracles — so when hunting the
next one, add a narrow lane rather than widening this one.

To reproduce a lane by hand:

```
cargo run --release --example vopr -- --rotate --partition 100 --max-secs 600
```

#### The starved-denominator trap

**A failure rate is only meaningful against the seeds whose oracles were actually
reached.** Quote the starved and the unstarved denominators separately; never
pool them into one rate. This nearly produced a wrong gating decision, so it gets
its own section rather than a footnote.

This has now happened on two successive censuses, which is why it is a rule and
not an anecdote. The current one pools **5,417,063** partitioned seeds and quotes
one headline rate; **4.1M of them — 76% — are a narrow 1-package × 1-file
profile**, and that profile starves four of the oracles the rate is quoted
against. Only ~2.1M of the 5.4M are durability-effective. The census before it
made the same move at a different size (2.2M pooled, 61% narrow).

The starvation is not an estimate. Re-measured at `8e5916f` on a fresh range
(`--start-seed 97000000001`, 926,399 seeds, `--nodes 3 --buckets 2 --packages 1
--files 1 --ops 40 --fail-percent 3 --partition 100`), the reach meter says it
outright:

```
DURABILITY        395856  on 179829/926399 seeds  [STARVED — executed on 19% of seeds, floor 25%]
VISIBILITY        395856  on 179829/926399 seeds  [STARVED — executed on 19% of seeds, floor 25%]
CONSERVATION      218311  on 198403/926399 seeds  [STARVED — executed on 21% of seeds, floor 25%]
SELF_CONSISTENCY  495442  on 225077/926399 seeds  [STARVED — executed on 24% of seeds, floor 25%]
```

All four sit **below the 25% `--require-reach` floor**. The mechanism is the one
`tests/simulation_matrix.rs` pins the 12-filename corpus to prevent: DURABILITY,
VISIBILITY and CONSERVATION skip any filename an authorized delete, tombstone or
freeze removed, so their whole universe is `packages × files` names — and at one
name, 40 ops with a 10% delete weight tombstone the entire corpus by quiescence
on ~80% of seeds. Those seeds iterate an empty set. **They could not have found
the failures being counted against them.** Dividing by them does not measure a
safer product; it measures a bigger number under the bar.

The trap is worse than a diluted denominator, because the starved profile is also
the *cheap* one. Over the identical 900-second budget at `8e5916f`:

| lane | seeds | acked uploads | durability-effective seeds |
|---|---|---|---|
| narrow 1×1 (starved) | 926,399 | 802,056 | 179,829 (19%) |
| `multi-bucket` 6×2 (unstarved) | 175,353 | 1,575,238 | 175,233 (99.9%) |

The narrow lane draws **5.3× the seeds while verifying half the acked uploads**.
Pool them and the starved profile contributes 84% of the seed count and 51% of
the durability-effective denominator — it dominates the headline precisely
because each of its seeds does less work. A pooled rate is therefore biased
toward whichever lane is cheapest to run, which is exactly backwards.

Three rules, in the order they bite:

1. **Report `failures / durability-effective seeds`, not `failures / seeds`** —
   the second number is on every summary line and is the wrong one whenever a
   starved profile is in the pool. The reach meter's `on X/Y seeds` column is the
   right denominator and the binary already prints it.
2. **Never pool lanes with different reach profiles into one rate.** One row per
   lane, each with its own denominator. If a single number is genuinely needed,
   it belongs to the oracle family it was measured for, and the starved lanes are
   excluded from that family — not averaged into it.
3. **Run the pooled lanes under `--require-reach`** wherever the rate will be
   quoted. A lane that would fail the floor cannot back a claim about the oracles
   it starves. The narrow lane above is deliberately run *without* it, because its
   job is merge-algebra coverage — which it delivers, reaching `MERGE_DIVERGENCE`
   on 18% of seeds and `FREEZE_JUSTIFIED` on 16%, both tail-event slots that waive
   the floor by design.

None of this makes the narrow lane a bad lane. It is where three of the last four
root causes were found: one filename concentrates contention in a way twelve
filenames cannot, which is the whole point of running it. It is a *durability*
denominator it must never contribute to. Add a narrow lane when hunting the next
bug; keep its seeds out of the rate you gate on.

#### What the partitioned lane caught, and what closed each one

Every row was mined from the first partitioned soak at `--nodes 3 --buckets 2
--packages 6 --files 2 --ops 160 --partition 100`; the `fail` column is that
run's `--fail-percent`. **All eight are green at `783d423`** — each was re-run
one seed at a time at exactly those flags, not inferred from a soak summary.
The rows stay after the fix: a table of what the simulator caught is the whole
argument that it earns its runtime.

| oracle | seed | fail | what it caught | closed by |
|---|---|---|---|---|
| `AUDIT_PREMATURE_CONSUMPTION` (global membership) | 1 | 3 | a drain of a peer bucket decided its global-index delta by HEADing `simple/<pkg>/index.json` — the view that same function writes and deletes — so a retry after a failed pass read its own leftovers, computed an empty delta, consumed the markers anyway, and left the bucket publishing a global index contradicting its own truth until the tier-3 audit stumbled on it. Was the dominant finding by volume, ~50% of partitioned seeds | `df3db2d` — states membership instead of diffing it, and deletes the per-package HEAD. 11,898 → 0 over comparable 21k-seed ranges |
| `AUDIT_ORDERING` (class 1) | 268 | 3 | a truth mutation with **no covering breadcrumb** — no `_dirty/` marker on that bucket, no `_repl/` note aimed at it — so nothing durable could tell a tick to re-derive the name. The only class-1 the product has ever produced | `df3db2d`, same fix: 16 → 0 |
| `DURABILITY` + `CONSERVATION` | 267 | 0 | an acknowledged upload's bytes gone from every bucket and from `_quarantine/`, both buckets serving a *different* body under that filename, no tombstone and no freeze. The strongest one: a `200` retracted with no evidence | two arms. The freeze arm: `5589ee5`, `b91e06c`, `a7f5f26`. The crossed-body arm: `a24236d` → `25d1d28` → `9e8baa1`, three separate sidecar-before-bytes orderings |
| `SELF_CONSISTENCY` | 7100005242 | 3 | two buckets serving different bodies for one filename while their sidecars stayed **byte-identical**, so the pair never entered the diverged-key set and no other oracle could see it | `25d1d28` — an unverifiable size HEAD no longer deletes bytes a concurrent copy had adopted. Crossed bodies 3 → 0 over 115,689 identical seeds |
| `CONSERVATION` (frozen) | 165 | 0 | two byte-sets acked, the fleet froze the filename, only *one* survived in `_quarantine/` — the freeze lost a body it is supposed to preserve before it drops anything | `5589ee5`, `b91e06c`, then `a7f5f26`'s delete ledger (below) |
| `FREEZE_UNJUSTIFIED` | 91 | 0 | frozen on every bucket with only **one** byte-set attested anywhere (acks ∪ every `_quarantine/` copy) — a freeze that suppresses a filename and waives its durability check without a real conflict behind it | the same three |
| `CONVERGENCE` (`.origin` split) | 208 | 0 | `packages/<pkg>/.origin` reads `mirror` on bucket 0 and is **absent** on bucket 1 at quiescence — an orphan claim that never replicated and never got released, so one bucket reserves a name the other would let a proxy fill. Needed no faults at all: four ops, two nodes, one file | `701b813` — `reconcile_split_origin` grew the missing `(mirror, no claim)` arm. The 2n/2b/1pkg/1file/4-op profile went from 9,123 failing seeds in 194,074 to 0 in 197,314 |
| `ACK_TOTALITY` | 19 | 0 | a publish acked while a peer held neither the record, nor a `_repl/` note owing it, nor any merge marker explaining its absence. Crash-only, where a missed note is the hard gate | two arms, same defect. `fd14f01` — `decide`'s `(Orphan, _)` deferral is now `Verdict::Defer`, not a `Noop` the fan-out read as convergence: crash-only 3,109 failing seeds in 20,963 → 2 in 20,703. The remainder was the demotion fence's `(QuarantinedMirror, _) => Noop`, closed by `4bb9cb8` → 0 |

**Re-run a seed before you quote its row.** This table went stale silently and
was cited as evidence for a full commit's worth of work after it stopped being
true. Audited two commits back at `a24236d`, 267 and 165 had already gone green,
91 red as `ACK_TOTALITY` rather than `FREEZE_UNJUSTIFIED`, and 268 red as
`AUDIT_PREMATURE_CONSUMPTION` rather than the class-1 `AUDIT_ORDERING` that a
claim two sections down was built on — only 1, 208 and 19 still reproduced as
written. A stale row is worse than a deleted one: it reads as measurement.

#### Still red at `783d423` — before the demotion fence replicated

> The `CONVERGENCE` and `DURABILITY` rows below are **closed**: both were the
> unconverged `.mirror-quarantined` marker, and both read zero over 65,473 fresh
> partitioned seeds after the fix. The rows are kept as the measurement that
> motivated the adjudication in dev/DESIGN.md, not as a live defect list.
>
> `FREEZE_UNJUSTIFIED` is **closed too**, and it was the oracle, not the
> product: 4 of 63,581 seeds, every one a real byte conflict correctly frozen
> whose evidence a racing delete had erased. See *Attesting a freeze* below.
> What is left on this lane is `CONSERVATION` — the same 4 — plus one
> `DURABILITY` that only a wider 183,230-seed draw reaches.


Measured over the three partitioned lanes above (166,409 seeds). Rates are the
share of that lane's seeds the oracle reds on; `—` means it produced none there.
Every repro is `--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160
--partition 100` at the stated `--fail-percent`, verified to red standing alone.

| oracle | repro | fault | rotating | crash-only | what it says |
|---|---|---|---|---|---|
| `CONVERGENCE` | seed 9500054, fail 3 | 0.345% | 0.144% | 0.015% | dominant, and one shape: a `<file>.mirror-quarantined` marker standing on one bucket and absent on the other. 110 of the fault lane's 160 failing seeds diverge on **that key alone**. The marker is what a merge writes to make a mirror body inert under a private claim, and it does not replicate — so the peer still renders the record the marker exists to suppress. The remaining 50 add the record it covers (bare artifact + `.meta.json`) present on one bucket only |
| `DURABILITY` | seed 9500926, fail 3 | 0.034% | 0.019% | 0.004% | `acked … missing on bucket 0` — an acked record absent from one bucket at quiescence with no tombstone and no freeze. Always paired with a `CONVERGENCE` red on the same key; never the *split* clause (0 `never left split` in all 166,409 seeds) |
| `ACK_TOTALITY` | seed 9504440, fail 0 | — | 0.004% | 0.017% | **closed by `4bb9cb8`**, and it was never the second producer this row implies: the residue `fd14f01` did not take was the same `Noop`-as-agreement defect in `decide`'s demotion-fence arm. 8 misses in 46,660 crash-only seeds then; 0 in 276,998 now. Under fault injection it stays a reported statistic (4,691 misses, 0 failing seeds), because the note write can itself fail |
| `FREEZE_UNJUSTIFIED` | seed 9539662, fail 0 | 0.011% | 0.003% | 0.002% | the residue `a7f5f26`'s delete ledger did not cover: frozen fleet-wide, one byte-set attested, both buckets tombstoned, `_quarantine/` empty. A freeze that fires without a real byte conflict buys a blanket DURABILITY+CONSERVATION exemption for free, so this matters more than 0.01% suggests |
| `CONSERVATION` | seed 9522193, fail 3 | 0.002% | — | — | a freeze that dropped an acked body it never quarantined: one byte-set preserved under `_quarantine/`, the acked one gone from every bucket |

The whole audit-repair family is **gone**, not merely rarer.
`AUDIT_PREMATURE_CONSUMPTION`, `AUDIT_REPAIRED_VIEWS` and `AUDIT_ORDERING` were
the dominant volume when this lane opened — tens of thousands of findings in a
133,260-seed soak, better than 40% of seeds on some rows. At `783d423` every one
of the 489,901 seeds measured here (166,409 partitioned + 323,492 aligned)
reports **0 audit view repairs of any class**, so none of the three can fire at
all. `SELF_CONSISTENCY` reds on none of them either. That is `df3db2d` and the
crossed-body chain, and it is also why the classifier's meter rows now read zero
everywhere: they were reachable only through the repair path.

**How the freeze rows (165, 91, and the freeze arm of 267) closed.** Two
`copy_live` fixes took most of that class (5589ee5, b91e06c). What was left
behind them is not a product defect: a delete and a merge freeze racing on one
filename. The delete tombstones and drops the body while `freeze_side` sits
between its `.frozen` marker and its `get_bytes`, so the freeze finds nothing
left to quarantine and the loser body is gone — destroyed by the delete, not
lost by the freeze. Over 16,671 crash-only partitioned seeds every one of the
40 `FREEZE_UNJUSTIFIED` and 6 `CONSERVATION` reds had two distinct bodies
really stored under that filename (a real conflict, correctly frozen) and an
authorized delete for it; not one had a freeze that preserved nothing. Nothing
at quiescence distinguishes a delete's tombstone from the freeze's own, so the
ledger now records the deletes the workload authorized — acked, or dead in
flight; a refused 404 withdraws its own — and CONSERVATION's tombstone exemption
survives a freeze on such a filename. Repros, all `--nodes 3 --buckets 2
--packages 6 --files 2 --ops 160 --fail-percent 0 --partition 100`: seeds 6000,
9024, 12144, 14623 (freeze justification) and 8226, 9714, 25704 (conservation),
red before the change and green after — all seven re-verified green at
`783d423`. FREEZE_JUSTIFIED took the same ledger and got a second clause,
"excused only if the freeze preserved *something*", which was wrong; the next
section is how that was settled.

**Attesting a freeze.** The clause above left a residue nobody could adjudicate
— fleet-wide `.frozen`, one byte-set attested or none at all — and it was the
oracle. Instrumenting the two minimized reproducers settles it outright: in both,
`decide` returned `Verdict::Freeze` from two genuinely different sidecar
sha256 (`f549487e…` vs `7c8c26f3…`) under one immutable filename, and every
`freeze_side` call that followed found the canonical key already empty. That is
a real byte conflict, correctly frozen. What went missing was the *evidence*,
and both of the things the oracle attested from are erasable:

- an ack — a publish can crash after `store_artifact_verified` succeeds and
  before its `200`, so real conflicting bytes exist that nothing acked;
- a `_quarantine/` copy — an authorized delete racing `freeze_side` between its
  marker and its `get_bytes` destroys the body first. When the delete wins that
  race on *both* sides, a correct fleet-wide freeze has no body-shaped evidence
  anywhere, which is exactly the "0 byte-sets" shape.

The product is right and must not change. `.frozen` deliberately names a
filename and never a hash (dev/DESIGN.md): it replicates as truth, so a marker
carrying per-bucket digests would diverge and never converge — and a symmetric
*pair* is no better, since three buckets can freeze two different pairs
concurrently under `put_if_absent` and stay split forever. Declining to write
the marker over an already-tombstoned filename is worse still: it starves
`decide`'s `a.frozen == b.frozen` convergence condition.

So the simulator remembers instead. `SimStorage` records the sha256 of every
body that stood at a canonical `packages/<pkg>/<artifact>` key during the
filename's current *incarnation* — a fenced delete cannot erase it — and
FREEZE_JUSTIFIED attests from
acks ∪ every `_quarantine/` copy ∪ that history, all reduced to digests. The
product cannot manufacture a digest it never wrote, and `--break
freeze-unjustified` (a bare `.frozen` planted over a filename that only ever
held one byte-set, single-bucket, where a byte conflict cannot occur at all)
stays red. It shipped scoped to the whole *lifetime* instead, which was a
loosening large enough to gut that kill proof — see *The history term counted
succession as conflict* below, which measures it and closes it.
Measured over one identical partitioned range — 63,581 seeds
from 5100000000, the same profile the nightly lane runs: 8 failing seeds before,
4 after, and the 4 that went are exactly the FREEZE_UNJUSTIFIED ones (the other
4 are `CONSERVATION`, untouched by this and red the same way both times).
Widened to 183,230 partitioned seeds it produces **zero** FREEZE_UNJUSTIFIED;
the residue there is 6 `CONSERVATION` and 1 `DURABILITY`.
Compare ranges, never time budgets: this box's seed rate swings better than 1.5x
with load, so a `--max-secs` draw explores a different range each run and its
failing-seed *count* is not comparable to another run's. The two reproducers are
pinned as a gate in
`ci.yml` — `--seed 9800020745` (both byte-sets erased) and `--seed 9800002480`
(one erased), both `--nodes 2 --buckets 2 --packages 1 --files 1 --ops 16
--fail-percent 0 --partition 100`.

**The history term counted succession as conflict.** `Verdict::Freeze` fires only
from two live records whose sidecars name different sha256 *at the same time* —
coexistence under one immutable filename. The history term attested from
something strictly weaker: every body ever committed under the canonical key,
insert-only over the filename's entire lifetime. Those are different sets. A
filename that held body A, was retired by an authorized delete, and later held
body B carries two digests without anything ever having conflicted, and a
`.frozen` marker over it is excused for free.

It was reachable, and not marginally. Measured at `4eaa015` on the CI kill
proof's own pinned flags (`--nodes 2 --buckets 1 --ops 80`), seeds 1–1000, one
seed at a time: `--break freeze-unjustified` plants its bare fleet-wide marker
on **934** seeds (the other 66 have no acked-deleted victim, so nothing is
planted) and FREEZE_UNJUSTIFIED reds on only **334** of them. On the other
**600 — 64% of every planted spurious freeze — the oracle examines the marker
and stays silent.** Re-measured at `8e5916f`, seeds 1–500 one at a time: 470 of
the 500 plant a marker and **183 red — 39% of the planted ones**.
Silence is unambiguous here: the reach meter counts
`FREEZE_JUSTIFIED` once per frozen filename it attests, and the oracle has no
exit between that counter and its violation, so *examined and silent* means
`attested.len() >= 2` and nothing else. At `--buckets 1` `decide` never runs, no
`Verdict::Freeze` is reachable, and `_quarantine/` is empty — so every one of
those second digests came from succession under the key.

The lifetime sweep is what pins the mechanism, 300 seeds per row on the same
flags — excused share of *planted* markers:

| `--ops` | 8 | 20 | 40 | 80 | 160 |
|---|---|---|---|---|---|
| planted markers excused | **0%** | 15% | 33% | 60% | **87%** |

Strictly monotone in how long the filename lived, zero where there is no room
for a second body, 87% at the op count the nightly lanes actually run. That is
the signature of an accumulating history, not of a conflict: the longer a name
survives, the more bodies have occupied its key in turn, and the oracle counts
turns as conflicts. Note also that the break's victim is *by construction* an
acked-deleted filename, so the delete separating the two incarnations is always
there.

A demotion is one instance of this class, not the class. A legitimate
mirror→private supersede writes a second body under the canonical key, so a
filename with a completed demotion carries two digests with no conflict behind
them — but that is a partitioned-only path, and the measurement above reaches
the same excuse at `--partition 0`, single-bucket, with no mirror fill and no
demotion anywhere in the run.

**The currency is now digests that coexisted, not digests that accreted.**
`Inner::committed` is scoped to the filename's current *incarnation*, and the
two rules that bound it are both the product's own, not the harness's opinion:

- a body written while a `.tombstone` or `.frozen` stands beside the key never
  joins. Those are `publish_record`'s upload fences: it stores the bytes, reads
  the fence, refuses, and single-bucket deletes what it wrote. Debris a refused
  writer left is not half of a byte conflict.
- deleting the body with **no** fence beside it clears the set. That is a mirror
  cache eviction — never tombstoned, re-fillable by design (`delete_record`) —
  or a rollback; either way the name is retired with nothing preserved, and the
  next body starts a new incarnation.

The asymmetry is load-bearing and is why "reset on the tombstone" would have
been wrong. `freeze_side`, `settle_mirror_quarantine` and `delete_record` all
plant their marker *before* they drop the body, so a delete under a fence is a
resolution and the bodies it destroys are exactly the evidence this set exists
to hold — resetting there would put back the false FREEZE_UNJUSTIFIED reds the
section above removed. Both cases the history term was added for survive
untouched: a publisher that crashes after `store_artifact_verified` and before
its `200` commits its body inside the same incarnation as the conflicting one,
and a delete racing `freeze_side` destroys bodies committed before the marker
went down. What stopped being attested is exactly the succession case, which no
`Verdict::Freeze` can be built from. No product code moved: the freeze path is
right and `.frozen` still names a filename and never a hash.

Measured at `8e5916f`, one seed at a time on the CI kill proof's pinned flags,
seeds 1–2000 — **688 red before, 1864 after**. Denominators, separately, because
30 seeds in 500 never plant anything (no acked-deleted victim) and cannot red:
per *seed* 34.4% → **93.2%**; per *planted marker* 39% → **100%** (470/470 over
seeds 1–500 — every planted spurious freeze now reds). K goes 15 → **3**, which
is the value the `--break` table below already carried; the table was right and
the oracle had quietly stopped matching it. Under `--rotate`, seeds 1–1500:
36.2% → **89.3%**, K 14 → **3**, likewise the table's existing figure.

The false-positive direction is measured too, since narrowing an attestation is
how a kill proof gets traded for a spurious red. Both pinned reproducers from
*Attesting a freeze* stay green — on `--seed 9800020745` every digest comes from
this set and none from an ack — and 88,262 partitioned seeds from 5100000000
(`--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 0
--partition 100`)
attest 38,011 `.frozen` markers across 27,270 seeds with **zero**
FREEZE_UNJUSTIFIED, against 38,135 across 27,358 on the same range before. Three
`SimStorage` unit tests pin the three directions (fenced delete keeps, unfenced
delete clears, barred write never joins); the middle two fail on the old
currency.

Determinism itself is verified, not assumed: recurring seeds (`--recheck-every`)
run twice and must produce an identical storage-op trace hash *and* an identical
final world — every bucket's bytes plus the ledger. The trace hash alone only
proves the two runs issued the same *calls*; a nondeterminism downstream of the
op sequence (an unvirtualized clock read, say) makes the same calls with
different bytes, and the state hash is what catches it. The rerun's own invariant
verdict counts too, so a seed that passes once and fails once is red. Any failure
reproduces exactly with `cargo run --example vopr -- --seed N --rotate`.

A smoke runs on every PR (ci.yml); tens of thousands of fresh seeds run
nightly with counters published in the job summary
(`.github/workflows/simulation.yml`). For local soaking, `make vopr-soak`
runs continuously across rotating profiles (nodes 2–3, buckets 1–3,
packages 1–6, files 1–4, swarmed op mix, fault and crash-only), logs failures
and keeps exploring, and heartbeats once a minute; `VOPR_SECS=600 make
vopr-soak` timeboxes instead and exits non-zero if any seed failed. At ~380
seeds/second an overnight soak still covers tens of millions of schedules —
down from ~495 before the workload widened, because each seed now drives more
entities. That trade was made deliberately and on measurement: seeds/second
fell 24%, but *storage-op interleavings* per second fell only 10% (707k → 633k)
and acked uploads per seed rose 60% (3.3 → 5.3), so a second of soak now buys
more verified protocol work, not less. Entity counts skew small precisely to
keep that ratio; the 6×4 tail is the minority of seeds. Output is also appended
to `vopr-soak.log` (gitignored) so findings survive the terminal.

The heartbeat and final summary report an **audit-view-repairs** count: how
often a run's cheap marker/tick/reconcile path left a view unconverged and the
tier-3 audit backstop had to repair it. Every fault-mode repair is *classified*
from the run's recorded effect history — no replays — into one of three causes:

1. **ordering** — truth was mutated with no durable breadcrumb ever covering it
   (a `_dirty/` marker on that bucket or a `_repl/` note aimed at it). The fast
   path never had a chance to converge the view.
2. **premature-consumption** — breadcrumbs existed but were destroyed without
   converging the view: a consumer that never listed past the mutation it
   retired, or a rebuild that consumed its own markers while leaving a view
   inconsistent with the truth it listed.
3. **concurrent-race** — an unleased concurrent rebuild overwrote a fresher one
   (`concurrent_rebuild_without_lease_diverges`, tests/model_event_protocol.rs),
   the one case for which the audit is the *documented* backstop.

The taxonomy is applied by **two** analyses, because a repaired view key can
belong to either of two subsystems. A `simple/<pkg>/index.*` diff is explained
from that package's own `ViewWrite`s — `rebuild_package`'s renders — against the
truth listings they derived from. A `simple/index.{json,html}` diff is a
*membership* change, and membership is decided by
`update_global_index_locked`'s delta-plus-CAS over the tick's cached name set,
never by a per-package render — the per-package view can be byte-perfect while
the global name set is wrong. So a global diff expands to the names whose
membership flipped, and each is explained from that bucket's `GlobalWrite`
effects and the name set every write claimed. Same three classes, same
severities; only the writes they interrogate differ. This is not cosmetic
bookkeeping: handing global-index findings to the per-package analysis (which is
what used to happen) blamed a rebuild session that was byte-perfect and sent
whoever read the message to `rebuild_package` instead of
`update_global_index_locked`.

A global write only reconsiders a name its own tick rebuilt and carries every
other name forward from cache, so the global analysis first asks *which* writes
actually re-derived the name. The rebuild's `packages/<name>/` listing runs in a
task the tick spawned, which does not inherit the tick's op id, so it is matched
positionally inside that tick's window — after its `_dirty/` listing, before its
index write. A write with no such listing could not have corrected the name and
is never blamed for failing to.

Classes 1 and 2 are hard violations in **every** profile and fail the seed with
a reproduce command: both mean the fast path destroyed the only signal that
should have converged the view — the exact bug family this simulator exists to
catch. Crash-only profiles (`--fail-percent 0`) additionally keep the blanket
"any repair is a violation" gate: with no injected failures the markers must
self-heal every schedule. Class 3 is the only legal statistic; the heartbeat and
summary print the per-class breakdown — `X audit view repairs (a ordering, b
premature, c concurrent-race)`. Drift the classifier cannot explain is reported
as class 2 on purpose: conservative, so an unmodelled hazard fails the seed
rather than hiding.

To see which seeds and keys are behind a repair, run with `VOPR_LOG_REPAIRS=1`:
every fault-mode repair then prints its **class** and causal detail alongside its
seed, round, and the exact drifted view keys (silent by default). `VOPR_TRACE=1`
(optionally `VOPR_TRACE_FILE=path`) captures the full storage-op trace for
diffing a determinism violation. `VOPR_TRACE_FILE` is also how you prove a new
oracle did not *perturb* the schedule: an invariant must read the raw
`Arc<SimStorage>`, never a `FaultView` — a `FaultView` call consumes an
op-sequence number, draws from the rng stream, and records a trace event, which
silently shifts every later fault decision and invalidates every pinned seed.
Diff the trace files from before and after; they must be byte-identical.

The honest statement about zero: the *avoidable* classes (1 and 2) are zero by
enforcement — a run that produces one fails. Whether the **total** can reach zero
is exactly the open per-package-leasing design decision behind class 3: until a
rebuild takes a per-package lease, two unleased rebuilds can still race, and the
audit remains their documented backstop.

The classifier is no longer theoretical. It sat at a flat zero for tens of
millions of interleavings under the old fixed workload — which read as
reassurance and was really just the same shape re-explored. Widening the
workload produced its first execution on real product behavior within 150k
seeds: seed 1067836 (`--seed 1067836 --rotate`), a crash-only 3-node/3-bucket
run whose swarmed mix drew **delete 20 against publish 7**, left the global
`simple/index.html` listing a package the truth no longer had and missing one it
did, and only the tier-3 audit converged it. The classifier called both drifted
views class 2 with the causal detail — *op N listed truth@245 and wrote the
final view@276, yet the view disagrees with that truth: a poisoned derivation
consumed the signal*. That message is the **per-package** analysis talking, and
it named the wrong subsystem: the drift was in the global name set, so the same
finding today comes out of the global path above, pointing at
`update_global_index_locked`. It is the same family as the fix in commit 1bc3ce9
(global `simple/index.html` staleness after a crash); the seed is green today,
that fix and f30036a having landed. Note which knob found it: both drifted
packages were `vopr-alpha` and
`vopr-beta`, the two the old workload already had, so it was the **op-weight
swarm**, not the extra entities. Under the fixed 40-publish/10-delete mix a
delete-dominant stretch that long is exponentially unlikely — not impossible,
which is the point of swarming: sampling the mix itself turns a tail schedule
into a routine one instead of waiting out its probability.

### Proving the oracles can go red (`--break`)

An invariant nobody has watched fail is not a test — it is an assertion of faith
that costs runtime forever and reports reassurance. `--break <name>` injects ONE
deliberate defect that a named oracle is supposed to catch; the run must then
FAIL with that oracle's text, and ci.yml's **Oracle kill proofs** step gates on
exactly that. Every invariant the simulator asserts has a leg there, and so does
every arm of the audit-repair classifier: an arm with no leg is an arm nobody has
watched fire. Every break lives in the harness — no product-code hook ships in
the binary — and writes raw `SimStorage`, never a `FaultView`, so it perturbs
storage and never the schedule.

| `--break` | the injected defect | must red | K |
|---|---|---|---|
| `view` | a torn view write (the last byte never landed), truth untouched | `VERIFY: … stale-view` | 1 |
| `fanout` | peer bucket 1 blackholes the chaos phase *and* the `_repl/1/` note owing it is dropped | `ACK_TOTALITY:` (needs `--fail-percent 0`, where it is the hard gate) | 1 |
| `rerun` | a seed's second execution ends in a different world off an identical op trace | `DETERMINISM VIOLATION … same calls, different bytes` (needs `--recheck-every 1`) | 1 |
| `resurrect` | an acked-deleted artifact's bytes come back with its tombstone gone | `TOMBSTONE_MONOTONICITY:` | 3 |
| `durability` | an acked artifact serves corrupt bytes of the **same length** on every bucket, the real bytes parked outside `packages/` *and* outside `_quarantine/` | `DURABILITY: acked … serves bytes no ack carried` | 4 |
| `visibility` | one acked filename edited out of `simple/<pkg>/index.json` on every bucket | `VISIBILITY: acked … not listed` | 4 |
| `conserve` | an acked body and sidecar destroyed fleet-wide with no tombstone, no freeze and no acked delete | `CONSERVATION: acked bytes … vanished from every bucket` | 4 |
| `diverge` | one bucket keeps an object the other never got | `CONVERGENCE: bucket 0 and bucket N differ` (needs ≥2 buckets) | 1 |
| `wedge` | an object under `_repl/` no sweep recognizes, so the fixpoint the heal phase is bounded to reach does not exist | `LIVENESS: fleet did not quiesce` | 1 |
| `ordering` | truth grows a file with no `_dirty/` marker and no `_repl/` note ever covering it | `AUDIT_ORDERING:` (repair class 1, per-package path) | 3 |
| `globalindex` | the same unbreadcrumbed mutation, under a package name nobody has published | `AUDIT_ORDERING: … simple/index.* membership` (class 1, global path) | 3 |
| `poison` | a rebuild listed truth *past* the mutation and still wrote a view — and a name set — contradicting it | `… a poisoned derivation consumed the signal` (class 2a) | 3 |
| `blind` | the only op that retired the marker covering the mutation had listed truth before it | `… were all consumed blind` (class 2b) | 3 |
| `race` | two unleased rebuilds: the one that listed *earlier* wrote *last* | `… unleased concurrent rebuild` (class 3; needs `--fail-percent 0`, where any audit repair is a violation) | 5 |
| `fallback` | an audit repair the effect history cannot explain at all | `… unexplained drift` (fallback arm) | 3 |
| `freeze-unjustified` | a `.frozen` marker over an acked-**deleted** filename — nothing ever conflicted about it, and the body and view entry are already gone, so no other oracle can see it | `FREEZE_UNJUSTIFIED:` | 3 |
| `freeze-lossy` | a freeze that dropped a body it never quarantined: `.frozen` fleet-wide, one byte-set preserved under `_quarantine/`, the *acked* one overwritten everywhere | `CONSERVATION: … a frozen filename may not lose one` | 4 |
| `origin-demoted` | a privately-claimed package's `.origin` walked back to `mirror` on every bucket (sidecars untouched, so the buckets stay converged and the views stay correct) | `ORIGIN_TERMINALITY: … claims … .origin = mirror` | 1 |
| `mirror-served` | the claim stays private but a **live mirror record** stands under it — a new filename cloned from a live record with its sidecar origin rewritten | `ORIGIN_TERMINALITY: … still serves … as live mirror truth` | 3 |
| `split` | a byte conflict the merge never resolved: bucket 1 acks and keeps a second, same-length byte-set under a filename bucket 0 still serves its own for | `DURABILITY: … never left split` (needs ≥2 buckets) | 4 |
| `attest` | an acked artifact's sidecar re-points at a digest no body has, fleet-wide, with every view re-pointed to match — the bytes, the buckets and the re-render all stay healthy | `SELF_CONSISTENCY: bucket … serves … while its own sidecar publishes` | 4 |

**Freeze and origin get two legs each, on purpose.** Freeze *justification* (a
`.frozen` marker must be a real byte conflict) and freeze *totality* (a freeze
preserves both bodies before it drops either) are two claims about the same
marker, and origin exclusivity is asserted once on the package `.origin` claim
and once on the sidecar of the record under it. A branch that shares a leg with
its neighbour is a branch nobody has watched fire — which is the whole argument
for this table.

Some of these reds land on a second oracle too, unavoidably, and the leg's
expected text is what pins which one it is for: `split` also reds CONVERGENCE (a
filename two buckets serve different bytes for *is* a diverged key — which is why
DURABILITY names it instead of leaving it as one line of a key diff),
`freeze-lossy` also reds VERIFY (a `.frozen` marker takes the record out of the
renderable set while the view still lists it, exactly as `--break conserve`
does), and `mirror-served` reds ORIGIN_TERMINALITY and nothing else.

`durability` and `split` also red SELF_CONSISTENCY, and there is no version of
them that does not: both corrupt a body, and a corrupted body *is* a body its own
sidecar contradicts. What they no longer red is VERIFY — the corruption is
injected at the **same length** as the bytes it replaces (`same_length_corruption`),
because `verify_storage` cross-checks every object's listed size against the size
its sidecar publishes and a shorter body would inject a second defect belonging
to a different oracle. Minimality is the point of a kill proof: a leg that reds
four oracles is a weaker test of the one it names.

**`attest` is planted from the sidecar side, and that is the whole point.**
Corrupting the *body* would red DURABILITY first and prove nothing about the
oracle under test, so the break leaves the bytes exactly as the ack carried them
and moves the published digest instead: DURABILITY and CONSERVATION see a healthy
artifact, the edit is fleet-wide so CONVERGENCE sees identical buckets, and every
view is re-pointed at the new digest so VERIFY's byte-strict re-render from the
doctored sidecar still matches what storage serves. Over the two 20-25s samples
below, 19,880 pinned and 5,587 rotating red seeds produced 19,880 and 11,071
violations respectively — every one of them SELF_CONSISTENCY, not a single line
from any other oracle. That number is also the measurement that answers a
separate question: `pypiron verify` runs inside the simulator as the VERIFY
oracle, and it passes this fleet. The product's own integrity command gives a
clean bill of health to a bucket serving bytes that contradict its own index
(see "Should `verify-index` re-hash?" below).

K is how many fresh seeds the break needs to red with **≥99.8% confidence** —
`ceil(ln 0.002 / ln(1-p))` on the measured per-seed red rate p, on that leg's own
flags. It is not "the first seed that happens to red": every break in this table
reds on seed 1 today, and a table of 1s would say nothing about what happens when
the schedule moves. Re-measure whenever it does; a stale K is a gate nobody has
checked is still a gate.

`globalindex` exists because `ordering` cannot reach the global analysis: it
grows a package that already exists, so the global name set never changes and
only `rebuild_package`'s view is repaired. Landing the same unbreadcrumbed
mutation under a name nobody has published makes membership the thing the audit
has to fix, which is the only input that exercises the `GlobalWrite` path — and
it reds with *both* findings, one per subsystem, which is the split working. The
last four breaks reuse that phantom clone for the same reason, so each of them
reds `analyze` **and** `analyze_global` at once: the two analyses ask the same
three questions of `ViewWrite`s and of `GlobalWrite`s, and one input answers
both. Four breaks, eight arms.

**The classifier breaks plant effect history, and that is not a shortcut.** The
storage damage is real — a package the audit genuinely has to materialize — but
`poison`, `blind` and `race` also push the `ViewWrite`/`TruthList`/`MarkerDel`
sequence the offending rebuild would have left. They have to: a concurrent-rebuild
clobber leaves **no storage residue** that distinguishes it from a lone stale
writer, because the loser's bytes are gone by definition. That is precisely why
the classifier reads effect history and not storage, and it means a planted
history is the only thing a kill proof for these arms *can* inject. Attribution
planting is not new here either — `synth_view_write` already does it for
warm-bucket audit writes. Read these four as mutation tests of the classifier's
predicates: they prove the arm evaluates, matches, and prints the finding it
claims to. They are **not** evidence that the product can produce the
interleaving; the reachability table below is where that question is answered.
`fallback` plants nothing at all, which is the whole point of it — an audit
repair with no history behind it is exactly the shape that arm exists to report.

The table's K is for the **pinned CI flags**, which are non-rotating. Re-measured
at this commit over 20-second timeboxed samples (4,418–23,022 seeds each):
`view`, `fanout`, `rerun`, `diverge`, `wedge` and `origin-demoted` red on every
seed (K=1); `resurrect` and `freeze-unjustified` 93.2% (K=3); `ordering`,
`globalindex`, `poison`, `blind` and `fallback` 89.7% (K=3); `mirror-served`
88.1% (K=3); `durability`, `visibility`, `conserve` and `freeze-lossy` 85.7%
(K=4); `attest` 84.4% (K=4, over 23,561 seeds); `split` 79.3% (K=4); `race` 76.2%
(K=5). Nothing below 100% is a weaker
oracle — each of those breaks needs a run that actually produced the state it
corrupts (a live artifact+sidecar pair to clone, an unexcused acked upload to
destroy, a privately-claimed package with a live record left), and a schedule
that ended with every file tombstoned leaves them inert. `race` is lower again
because class 3 is a *statistic* under fault injection by design, so only the
crash-only profile can red it. CI samples nothing: the seed range is pinned and
the simulator is deterministic, and it draws 6 seeds — one more than the largest
K in the table — so a future change that shifts which seed a break first bites on
cannot quietly turn a gate into a coin flip.

Under `--rotate` the same breaks red at different rates, because the rotating
profile varies the *topology* and the fault mode each break needs. Over
1,724–7,041-seed samples at this commit: `view` and `wedge` 100% (K=1);
`origin-demoted` 97.4% (K=2); `resurrect` and `freeze-unjustified` 88.0% (K=3);
`poison`, `blind` and `fallback` 71.9% (K=5); `mirror-served` 68.2%, `diverge`
66.7%, the three durability-family breaks 66.3%, `freeze-lossy` 66.4% and
`attest` 64.9% (K=6);
`split` 43.1% (K=12) — it needs a rotating draw with ≥2 buckets, the same gate
`diverge` sits behind; `globalindex` 39.1% (K=13), `race` 32.8% (K=16) and
`fanout` 32.0% (K=17) — all three gated on the roughly half of rotating seeds
that draw `--fail-percent 0`, since class 1's `AUDIT_ORDERING:` text, class 3's
finding dump and ACK_TOTALITY's hard gate only appear there. Take a rotating K to
the digit only off a five-figure sample: several of these rates land within a
percentage point of a K boundary, so an earlier 300-seed measurement read
`ordering` as 5 and `fanout` as 18. Keep the gate on the pinned non-rotating
flags; that is what makes it a gate rather than a sample.

Off by default and provably free: every injection point is a comparison against
`Break::None` that draws no rng, consumes no op-sequence number and records no
trace event. Verified the way this document demands, not asserted — re-measured
when the partition-branch breaks landed, against a binary built from the previous
commit's tree: five profiles × 5,000 seeds produce **byte-identical whole-run
output** — every storage-op interleaving count, acked upload, ack-totality miss,
audit repair, oracle-reach row and merge-meter row to the digit (6,077,236 /
12,897,971 / 6,105,308 / 13,019,557 / 8,455,918 interleavings; 53,697 / 45,690 /
57,384 / 49,393 / 26,388 acked).

#### Should `verify-index` re-hash? Yes, in two halves at two prices

The simulator's blind spot was the product's blind spot. `pypiron verify-index`
is the integrity command an operator reaches for, and until now it could hand a
clean bill of health to a bucket serving bytes that contradict its own published
sha256 — `--break attest` is the proof, since it reds SELF_CONSISTENCY and leaves
VERIFY (which *is* `verify_storage`) silent. But re-hashing is O(total bytes),
and verify's whole design is O(objects): it must stay runnable on a mirror with
a million files. The answer is not one decision, it is two.

**The length check is free, so it is always on.** Verify already lists every
object — `ObjectMeta.size` is in hand — and already reads every sidecar, which
already publishes `size`. Comparing them is an integer compare and zero extra
I/O, and it catches every crossing that changed the object's length. It reports
`size-mismatch`. This was pure oversight: the data was on both sides of the
function the whole time.

**The hash check reads the corpus, so it is `--deep`.** Cost is one full pass
over every artifact: seconds on a private index of forty packages, hours on a
full-PyPI mirror. Request cost is negligible (~$0.40 per million S3 GETs,
same-region transfer free); wall time is the binding constraint. That is
affordable when an operator chooses it — after a restore, after out-of-band
surgery, on a budgeted schedule — and unaffordable as a default, so it is a flag
and not a default. Fan-out is bounded by **bytes** in flight rather than by a
count (`DEEP_BYTES_IN_FLIGHT`), because a fixed 64-way fan-out over 300 MB wheels
is 19 GB resident. It reports `body-mismatch`.

The length check does not make the hash check optional. A crossing between two
builds of the same wheel filename — the exact shape the partitioned fleet
produces — is very often the same length, and reproducible-build variance makes
same-length-different-bytes the *central* case rather than a corner one.

**Rejected: reading the cloud's own stored checksum instead.** S3 will return a
whole-object sha256 from `HeadObject` if the object was written with
`x-amz-checksum-sha256`, which would make the deep check O(objects) HEADs instead
of O(bytes). It does not survive contact: it needs a write-path change so it says
nothing about any object already stored, multipart uploads return a composite
digest and not the object's sha256, and GCS (crc32c/md5) and Azure (content-md5)
publish different algorithms — so the "cheap" version is one backend's partial
answer wearing three backends' complexity. `--deep` reads bytes on every backend
and is the same code everywhere.

The VOPR still calls `verify_storage(bucket, deep: false)`. VERIFY's claim there
is views == truth; SELF_CONSISTENCY already re-hashes every body on every seed,
and passing `deep: true` would put two oracles on one claim and destroy
`--break attest`'s isolation. `verify-index --deep` is driven by the blackbox
suite instead (`tests/test_verify_deep.py`), where a same-length body swap is
shown to pass the default verify and red `--deep`.

#### Workload-unreachable, harness-unreachable, product-unreachable

A kill proof says an oracle is *sound*. It says nothing about whether anything
but a break can ever red it — and "nothing has ever red it" has **three**
completely different explanations at three completely different severities.
Conflating them is how an unfalsifiable gate survives a review, and this doc has
done it in both directions: it recorded class-1 `AUDIT_ORDERING` as a standing
zero the product could not produce, then, when `--partition` produced one,
recorded that as proof the state was product-reachable. Neither was the honest
statement. The zero was a workload that had never been run, and the finding was
that workload being run.

The healthy case first, then the three zeros:

- **workload-reachable** — nothing forbids the red state and the sampled
  schedules produce it, or would if the product regressed. The ordinary, healthy
  status, and not a zero at all.
- **workload-unreachable** — the state is reachable by both the product and this
  harness, but *this workload never draws it*. The weakest of the three claims,
  and the only one a knob can overturn without touching a line of product code:
  every finding in the tables above was workload-unreachable until `--partition`
  existed. A zero here is a coverage gap wearing a green check, and it expires
  the moment the workload widens.
- **harness-unreachable** — the *simulator* cannot stage the state, though
  production can. No workload knob fixes it; something outside this harness has
  to cover the claim. Class 3 is the example, and the model checker is what
  covers it.
- **product-unreachable** — a *product rule* forbids the state, so it cannot
  occur in production either. The guard is a watch on a rule that could one day
  be relaxed. This is the only one of the three that licenses "cannot happen",
  and it needs a named rule, not a run of zeros.

The distinction is load-bearing in both directions. `TOMBSTONE_MONOTONICITY` is
product-unreachable and names its rule (`publish_record`'s tombstone fence), so
its zero is an argument. The classifier's class-1/2a/2b arms are **workload-**
unreachable — no rule forbids them, and one of them (class 1, seed 268) was
briefly workload-*reachable* under `--partition` before `df3db2d` closed it. The
reach meter's `EXPECTED_ZERO` strings in `examples/vopr.rs` currently label those
three arms `product-unreachable` while justifying them with "no class-1 has ever
been produced" — which is an observation, not a rule, and therefore the
workload-unreachable case. Those strings should be corrected; the statuses in the
table below are the accurate ones.

| oracle | status | why | kill proof |
|---|---|---|---|
| VERIFY | workload-reachable | every convergence regression pinned in ci.yml's seed corpus red it | `view` |
| SELF_CONSISTENCY | workload-reachable, and *observed* | it red seed 7100005242 (`--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 3 --partition 100`) against the tree before `25d1d28`, where the rollback of an unverifiable write deleted bytes a copy had already adopted; that seed is green at `783d423`, and 0 in 223,718 seeds then plus 489,901 more here (166,409 of them partitioned) | `attest` |
| DURABILITY (acked bytes stand) | workload-reachable | no rule forbids a bucket losing an acked record | `durability` |
| DURABILITY (never left split) | workload-reachable **only partitioned** — *observed* there, none since the crossed-body fixes | needs one filename acked with different bytes on two buckets, which only `--partition` produces; 0 in the 166,409 partitioned seeds measured at `783d423`, so treat it as workload-unreachable again until something reds it | `split` |
| VISIBILITY | workload-reachable | ditto, for the listing | `visibility` |
| CONSERVATION (acked bytes survive) | workload-reachable | ditto, fleet-wide | `conserve` |
| CONSERVATION (a freeze loses neither body) | workload-reachable **only partitioned** — *observed* there, green now | a freeze needs a real byte conflict to fire at all. Its last live repro, `--seed 9522193 --fail-percent 3`, was red at `783d423` and is green at `4eaa015`; so are 9539662, 9500054 and 9500926, each re-run one seed at a time on its own flags rather than inferred from a soak summary | `freeze-lossy` |
| FREEZE_JUSTIFIED | workload-reachable **only partitioned** — and *observed* there; green now | ditto. It red on 4 of 63,581 partitioned seeds and every one was a correct freeze whose evidence a racing delete had erased (see *Attesting a freeze*); 0 of the same 63,581 — and 0 of 183,230 — once the attestation could see the bodies the buckets actually committed | `freeze-unjustified` |
| ORIGIN_TERMINALITY (the `.origin` claim) | workload-reachable, never witnessed | needs a mirror claim to win over a private one on some bucket after the private ack | `origin-demoted` |
| ORIGIN_TERMINALITY (the record under it) | workload-reachable, never witnessed | needs a live mirror sidecar left renderable under a private claim | `mirror-served` |
| CONVERGENCE | workload-reachable | needs ≥2 buckets; the replication paths that could break it run on every multi-bucket seed | `diverge` |
| LIVENESS | workload-reachable | any undrainable breadcrumb reds it; the fast path has simply always drained | `wedge` |
| ACK_TOTALITY | workload-reachable, and *observed* — including where it is fatal; green now | a reported statistic under fault injection (4,691 misses in 44,648 partitioned seeds); crash-only, where it *is* fatal, it produced 8 failing seeds in 46,660 partitioned, first `--seed 9504440`, and 0 in 276,998 once `4bb9cb8` stopped `decide` calling a demotion fence a peer had never seen an agreement. That row used to read "has never produced one", which was true only of the aligned schedule | `fanout` |
| DETERMINISM | workload-reachable | any nondeterminism downstream of the op sequence reds it | `rerun` |
| TOMBSTONE_MONOTONICITY | **product-unreachable** | `publish_record`'s tombstone fence rejects re-publishing a deleted filename, so no ack can follow a `204` — 150k wide seeds (795k acked uploads, 251M interleavings) produced zero, and could not have produced one | `resurrect` |
| classifier TEST 1 (both analyses) | **workload-unreachable** — but *witnessed once*, under `--partition` | seed 268 produced a real class-1 (a truth mutation no breadcrumb covered) before `df3db2d`; it is green now and 0 audit repairs of any class appear in the 489,901 seeds measured at `783d423`. Nothing forbids another, so this is a coverage statement, not a rule | `ordering`, `globalindex` |
| classifier/**global** FALLBACK | **workload-unreachable** — but *witnessed*, and recently | `--seed 60000037578 --rotate` reds `[class 2] … unexplained global-index drift for vopr-delta — conservatively premature-consumption` against the tree at `c1b66df`; so does `--seed 66000074673 --nodes 3 --buckets 2 --packages 1 --files 1 --ops 40 --fail-percent 3 --partition 100`. Both are the non-atomic global-index pair (`AUDIT_REPAIRED_VIEWS`), and both are green at `8e5916f` once `c9b2e32` landed. Verified one seed at a time on both trees, not inferred | `fallback` |
| classifier/**global** TEST 2a poisoned | **workload-unreachable** — but *witnessed* | `--seed 61000246528 --rotate --partition 100` reds `[class 2] … op 87 rebuilt vopr-gamma from truth@598 and wrote the global index@607 claiming it present, yet the audit had to flip that membership — update_global_index consumed the signal without applying it` at `c1b66df`; green at `8e5916f`. That is `R::GlobalPoisoned`, the 2a arm of `analyze_global` | `poison` |
| classifier/pkg TEST 2a / 2b / FALLBACK, classifier/global TEST 2b | **workload-unreachable**, never witnessed | each needs the tier-3 audit to have repaired a view, and the three witnesses above are all the *global* analysis — a membership flip, not a per-package render. No product schedule has yet driven the per-package arms, nor global 2b. Class 2 on the per-package path *has* been witnessed historically (seed 1067836, since fixed) | `poison`, `blind`, `fallback` |
| classifier TEST 3 (both analyses) | **harness-unreachable** | the simulator's `tick_lock` serializes every rebuild to stand in for the bucket lease, so two never overlap | `race` (planted history) |

`resurrect` proves the *oracle* is sound even though the *product* cannot reach
the state, which is the honest status of that guard: mirror filenames are
re-fillable by design, so the day a legal resurrection path lands it is already
watched. An unreachable-but-sound guard is legitimate; an unproven one is not.

Class 3 is unreachable on a **much weaker claim**, and this is the distinction
the table exists to keep. Production's lease is sloppy on purpose — `src/lease.rs`
is a TTL + heartbeat with no fencing, because rebuilds are idempotent — so dual
leadership, and with it the race, *is* reachable there; it is covered
exhaustively by `concurrent_rebuild_without_lease_diverges` in
`tests/model_event_protocol.rs`, not by this simulator. Removing `tick_lock`
still produced zero repairs over 26k wide seeds, and so did truncating the heal
phase's drain budget until two thirds of seeds failed on other oracles. `--break
race` closes the *soundness* question — the arm evaluates and prints its finding,
on both analyses — and leaves the *reachability* question exactly where it was:
this harness cannot stage the race, and the model checker is what covers it.

**Two oracles cannot red alone, by construction.** Worth knowing before anyone
reads a lone `CONSERVATION:` or `VISIBILITY:` line as an independent signal:

- CONSERVATION ⊆ DURABILITY. Both exempt the same states (an acked delete, a
  tombstone, a freeze); CONSERVATION's exemption is fleet-wide and DURABILITY's
  is per-bucket, so any key CONSERVATION reds on is a key some bucket also reds
  DURABILITY on. Its only independent contribution — bytes surviving under
  another key still count — makes it *weaker*, not stronger. `--break conserve`
  reds both, and always will.
- VISIBILITY ⊆ VERIFY ∨ DURABILITY. If the record is intact (DURABILITY green)
  and the view byte-matches a re-render of truth (VERIFY green), the view lists
  it. `--break visibility` reds VERIFY's `stale-view` alongside it.

Both are cheap and both name a claim in the language a reader cares about, so
they stay — but they are restatements, and a change that weakens DURABILITY or
VERIFY silently weakens them too.

Neither class-**1** nor class-**2** is synthetic any more, and both have since
been fixed out of the workload — which is the correct order of events, not a
reason to downgrade the arms. Class 2 came first, from the aligned rotating
profile (seed 1067836, closed by 1bc3ce9 + f30036a). Class 1 came from the
partitioned lane (seed 268, closed by `df3db2d`) — the product produced one, and
the sentence that used to stand here saying it never had was written before
`--partition` existed.

**And class 2 came back.** The `AUDIT_REPAIRED_VIEWS` root cause the `08d94f2`
census filed — the global index publishing HTML and JSON non-atomically — drove
the *global* analysis's 2a and FALLBACK arms on real product behavior at
`c1b66df`: seeds 60000037578 and 66000074673 on FALLBACK, 61000246528 on 2a, each
verified one seed at a time against both trees. `c9b2e32` closed it and all three
are green at `8e5916f`. So the honest reading is not "fed only by `--break`" — it
is that these arms go quiet between defects and light up when one lands, which is
what an arm is *for*. Read every zero here as workload-unreachable, never
product-unreachable: the gap between those two words is exactly one knob or one
regression, and this lane has now proved it twice.

### Proving the oracles ran at all (the reach meter)

`--break` proves an oracle *can* go red. It says nothing about whether that
oracle evaluates anything on an ordinary night — and from the outside, an oracle
that checked nothing is indistinguishable from one that held. This harness
shipped that failure twice: the audit-repair classifier was documented right
here as a CI-enforced three-class taxonomy while its class-3 arm had never
executed once, and a later batch added oracles with no evidence they could fire
either. Both times, establishing the truth cost a human hand-running six figures
of seeds across five lanes. It is now a number the binary prints on every run:

```
vopr: oracle reach over 13122 seeds — executions on NON-TRIVIAL input, and the seeds that got any (a zero, or a thin share, means that oracle verified nothing on the rest):
  DURABILITY                              134530  on   13069/13122 seeds  acked upload compared against a bucket's bytes
  VISIBILITY                              134530  on   13069/13122 seeds  stored artifact checked against that bucket's view
  ...
  DETERMINISM                               1313  on     1313/1313 rechecked  seed re-executed, trace + final world compared
  classifier/pkg TEST 3 race                   0  on       0/13122 seeds  older view write by another op tested for being fresher  [zero, expected: harness-unreachable: tick_lock serializes rebuilds (dev/TESTING.md)]
  quiesce headroom: worst seed used 3/12 heal rounds (9 spare) and 2/20 drain passes in a round (18 spare)
```

**"Executed" deliberately does not mean "its code was reached."** DURABILITY
looping over an empty ledger is not an execution; DURABILITY comparing one
acknowledged upload against a bucket's bytes is. A counter that ticked once per
invariant block would read healthy on a run that verified nothing — the exact
failure being closed — so every counter's unit is spelled out in `REACH_METER`
and printed beside its number. That definition *is* the meter; change a unit and
you have changed what the gate means.

The classifier is counted **per arm** — TEST 1 / TEST 2a / TEST 2b / TEST 3 /
FALLBACK, separately for the per-package and the global analysis. Per-arm
granularity is the whole point: a single "classifier ran" counter would have read
healthy for the entire life of the class-3 hole.

Same perturbation rule as any oracle, and for the same reason: every hit is a
relaxed atomic increment over data the run already computed — no rng draw, no
await, no storage op, nothing through `FaultView`. Verified the way this document
demands, not asserted: over five profiles × 5,000 seeds with `src/` pinned at
`fae38a2` and only `examples/vopr.rs` differing, storage-op interleavings, acked
uploads, ack-totality misses and audit repairs are identical to the digit, and
`VOPR_TRACE_FILE` dumps for 24 (seed, profile) pairs are byte-identical.

**What it says today.** Over 330 seconds across all five profiles — 140,334
seeds — every one of the nine invariant oracles executes, on every profile whose
topology admits it: DURABILITY and VISIBILITY 76,257 times on the rotating
profile alone, TOMBSTONE_MONOTONICITY 88,405, VERIFY 32,443, ACK_TOTALITY
51,845, CONVERGENCE 16,284, LIVENESS 16,060, CONSERVATION 39,944, DETERMINISM
1,469. **All ten classifier arms read zero** — both analyses, every test. That is
not new breakage; it is the same fact the class-3 investigation established, now
printed rather than excavated. The classifier only runs when the tier-3 audit had
to repair a view, and the fast path converged every schedule in the sample (0
audit repairs in 140k seeds). Nothing in *that* sample reached any of the ten —
which is a statement about a defect-free tree, not about the arms. Three of them
(global 2a and global FALLBACK) have since been driven by real product behavior
at `c1b66df` and went quiet again at `8e5916f` when `c9b2e32` closed the defect
feeding them, so the correct reading is that a zero here tracks the absence of a
bug rather than the unreachability of an arm. Between defects, ~310 lines of
classifier are carried entirely by inputs a `--break` has to supply, and the
taxonomy is a diagnosis tool for a rare event rather than a routinely-exercised
gate. Every arm now has a kill proof
(`ordering`, `globalindex`, `poison`, `blind`, `race`, `fallback`), which
settles soundness and settles nothing about frequency.

**A run-total hit count hides a per-seed zero.** DURABILITY, VISIBILITY and
CONSERVATION skip any filename an authorized delete, tombstone or conflict
freeze removed — correctly; the bytes are legitimately gone — so their whole
universe is the `--packages x --files` corpus. Run enough deletes against a
small enough corpus and every name is tombstoned by quiescence: the three
oracles iterate an empty set, and the run still prints five figures because the
counter sums over 50,000 seeds. The nightly's four fixed profiles did exactly
that. At 160 ops with a 10% delete weight against the harness default 2x2
corpus, measured one seed at a time over 400 seeds per row, **20%** of the two
crash-only rows' seeds evaluated durability at all and 39–40% of the two fault
rows' — the rest proved only that deleted bytes stayed deleted
(TOMBSTONE_MONOTONICITY, 7.2 hits/seed). The corpus is the only knob that lifts
both at once: fewer ops starves resurrection checking instead. The rows now pin
`--packages 6 --files 2` (12 names → 99–100% of seeds reach durability, ~10
hits/seed, TOMBSTONE_MONOTONICITY *up* to 10.4) at a cost of ~40% of the seed
rate — 50,000 seeds of the heaviest row in 267s locally, against a 60-minute job
budget. 24 names buys 27 durability hits/seed for another 25% of the rate, which
is more hits on the same schedules rather than more schedules, so 12 is where it
sits. `tests/simulation_matrix.rs` fails if a row drops below it; the rotating
row is exempt because its small seed-drawn corpora are deliberate coverage of
the dense, high-contention end.

**So the meter counts seeds, not just executions.** Widening the corpus fixed
the rows; it did nothing about the gate that let them ship. `--require-reach`
originally failed on an exact zero *over the whole run*, which is the same
unfalsifiable pass one level up: a run where 4 seeds in 5 verified nothing and
the fifth verified a lot reports five figures and passes. Every slot now carries
a second number — the seeds on which it executed at least once — printed as
`on 13069/13122 seeds`, and `--require-reach` fails a slot that ran on under
`REACH_FLOOR_PERCENT` (**25%**) of them. Run the pre-fix nightly corpus back
through it and it reds where it used to pass:

```
DURABILITY   4010  on 1754/8698 seeds  [STARVED — executed on 20% of seeds, floor 25%: widen the workload]
```

25% is a floor, not a target. Measured over the five nightly rows, every
non-excused oracle reaches 97–100% of seeds on the four fixed rows; the thinnest
reading anywhere is ACK_TOTALITY at **64%** on the rotating row, where a third of
the seed-drawn topologies are single-bucket and it correctly has nothing to
weigh. A floor near those numbers would red on sampling noise. Holding a row *at*
its measured corpus is `tests/simulation_matrix.rs`'s job; the floor catches the
collapse. Two rules the zero gate had are unchanged, and both are pinned by unit
test: a slot with a standing excuse (`EXPECTED_ZERO`, or the single-bucket
topology excuse) is silent on thin reach exactly as it is on zero, and no floor
fires under `--break`, whose workload is a deliberate defect rather than anyone's
coverage regression. DETERMINISM is scored out of the seeds `--recheck-every`
actually re-executed (`on 1313/1313 rechecked`) — out of every seed it would read
as 5% starvation on a healthy run.

`--require-reach` makes both readings a **failing run** (exit 4), so CI can
assert that every oracle ran rather than merely that none complained. It is off
by default — a small sample legitimately misses oracles, and a gate that cries
wolf gets ignored — and wired only into the nightly matrix, where each profile
draws 50,000 seeds. Expressing the floor as a share rather than a count is what
keeps it meaningful at 50,000 seeds and harmless at six. The gate itself is
falsifiable both ways: `--ops 0 --require-reach` reds with the eight invariants a
workload-free run starves (DETERMINISM survives — an empty run still re-executes
identically), and `--packages 2 --files 2 --ops 160 --require-reach` reds with
DURABILITY, VISIBILITY and CONSERVATION `[STARVED]` at 20%.

Zeros that are a known property rather than a hole live in `EXPECTED_ZERO`, and
each is a claim someone has to defend here:

| reads zero | why | reached by |
|---|---|---|
| CONVERGENCE, ACK_TOTALITY | need more than one bucket; excused automatically, and only, when every profile in the sample was single-bucket | any multi-bucket profile |
| MERGE_DIVERGENCE, FREEZE_JUSTIFIED | need two buckets that disagree about one filename's bytes, which only a partitioned fleet produces; excused automatically, and only, when no seed in the sample drew a split plan | any `--partition` run |
| classifier/pkg TEST 1 | **workload**-unreachable, not product-unreachable: `--partition` reached it once (seed 268, a truth mutation no breadcrumb covered), `df3db2d` closed it, and 0 audit repairs of any class appear in the 489,901 seeds measured at `783d423`. No rule forbids the next one. The excuse string in `examples/vopr.rs` still says `product-unreachable`; that word is wrong and should be `workload-unreachable` | `--break ordering`, `--break globalindex` |
| classifier/global TEST 1 | same, on global membership | `--break globalindex` |
| classifier/{pkg,global} TEST 3 | *harness*-unreachable: `tick_lock` serializes rebuilds, so two never overlap (see the reachability table above). Still true under `--partition`: a partitioned node diverges only its **writes**, never its rebuilds, so every tick still takes the one bucket-0 lease | `--break race` |
| classifier/pkg TEST 2a | needs an audit repair whose final writer had listed truth past every mutation | `--break poison` |
| classifier/global TEST 2a | **workload**-unreachable, and it has been *reached*: `--seed 61000246528 --rotate --partition 100` drove it at `c1b66df` off the non-atomic global-index pair; `c9b2e32` closed that and it reads zero again at `8e5916f`. The excuse string in `examples/vopr.rs` says `product-unreachable: no audit-repaired membership flip seen` — both halves are now false and the row should say `workload-unreachable` | `--break poison` |
| classifier/{pkg,global} TEST 2b | needs an audit repair TEST 1 declined — a covered mutation whose breadcrumbs were all consumed blind | `--break blind` |
| classifier/pkg FALLBACK | reached only by drift no test explains — which would itself be a classifier bug | `--break fallback` |
| classifier/global FALLBACK | same in principle, but *reached twice* at `c1b66df` (seeds 60000037578, 66000074673) by the same non-atomic global-index pair, and zero again at `8e5916f`. Its excuse string carries no unreachability claim, so nothing there needs correcting — but do not read its zero as one | `--break fallback` |

An entry earning its first execution is *news*, not a failure: the run prints
`[now reached — drop it from EXPECTED_ZERO]` beside it, and the entry comes out.
Under `--break` the note reads `[reached under --break]` instead, because
reaching an oracle is what a break is for.

The last line is **quiesce headroom**. LIVENESS is a boolean, so an over-generous
drain budget converts a livelock bug into a pass with nothing to show for it. The
run therefore reports the worst rounds-to-quiesce any seed actually needed
against the `HEAL_ROUNDS` budget, and the worst drain passes inside one round
against `DRAIN_PASSES`. Over 330 seconds across all five profiles — 140,334
seeds, 174M storage-op interleavings — the worst seed used **3 of 12** heal
rounds and **2 of 20** drain passes. Two is the floor for rounds (the fixpoint
test needs a repeat round to confirm), so the observed spread is 2–3 against a
budget of 12: a 4x margin, and drain passes barely touched because the first pass
sweeps every node before the loop re-checks. That is a lot of slack for a boolean
to hide in, and it is now visible — if a change pushes the peak toward the budget,
the number moves rounds before the oracle does.

### Recording a run for the visualizer (`--trace-jsonl`, `PYPIRON_VIZ_GRAPH`)

Every producer in this document can now write down what it did, in one JSONL
schema, for `dev/scripts/viz` to play back. Three recorders, all off by default:

| producer | switch | writes |
|---|---|---|
| `examples/vopr.rs` | `--trace-jsonl <path>` | one seed's timeline: `meta phase step round pass op drop crash crash_sched restart clock ack world oracle violation summary` |
| `tests/model_replication.rs`, `tests/model_event_protocol.rs` | `PYPIRON_VIZ_GRAPH=<path>` | one JSON object: the model's whole state space, nodes + edges + named paths |
| `tests/test_viz_region_trace.py` | `PYPIRON_VIZ_OUT=<dir>` | the region failover arc, sampled from `/metrics` and `/ready` on a fixed tick |

`--trace-jsonl` records **one seed's first execution** and asserts it: a single
bounded seed, never `--forever`, never `--max-secs`, and never the determinism
rerun, whose world may legitimately differ under `--break rerun`. Bucket state
comes only from `world` deltas — an `op` is emitted at admission, before the call
executes, so its byte effect is unknown when it is written down and the player
must not guess it.

The two model dumps are `#[ignore]`d **and** env-gated, and the guard is
load-bearing rather than decorative: `.github/workflows/simulation.yml`'s
`model-deep` job runs `cargo test --release --test model_replication --test
model_event_protocol -- --ignored`, so `#[ignore]` alone would have that job
write a state graph to disk on every nightly. With the guard it returns in 0.00 s.
Each dump asserts `nodes.len() == checker.unique_state_count()` for its config,
which is what proves the picture is the space the checker verified.

**The recorder is inert, proven the way this document demands.** A trace flag
that perturbs the schedule records a run that never happened, so it is held to
the same rule as the reach meter: buffered in memory, no rng draw, no
op-sequence consumption, no `TraceHasher::record` call, no `.await`, and every
world snapshot reads the raw `SimStorage::dump()` rather than a `FaultView`.
Measured across the seven traced profiles in the scenario pack, each run twice
under `VOPR_TRACE` with and without the flag, at `238a437`:

| profile | op-interleaving dump | with vs without `--trace-jsonl` |
|---|---|---|
| `--seed 3 --nodes 3 --buckets 3 --ops 40 --partition 100` | 40,027 B | byte-identical |
| `--seed 4 --nodes 2 --buckets 2 --ops 16 --partition 100` | 21,855 B | byte-identical |
| `--seed 20 --nodes 3 --buckets 2 --ops 60` | 43,673 B | byte-identical |
| `--seed 20 --nodes 3 --buckets 2 --ops 60 --partition 100` | 55,102 B | byte-identical |
| `--seed 9000210 --nodes 3 --buckets 2 --ops 160 --fail-percent 3 --partition 100` | 121,011 B | byte-identical |
| `--seeds 1 --nodes 2 --buckets 1 --ops 80 --break attest` | 26,598 B | byte-identical |
| `--seeds 1 --nodes 2 --buckets 1 --ops 80 --break wedge` | 78,497 B | byte-identical |

The printed run summary matches to the character once the wall clock is
normalized away. Reproduce one by hand:

```
VOPR_TRACE=1 VOPR_TRACE_FILE=/tmp/a cargo run --release --example vopr -- \
  --seed 3 --nodes 3 --buckets 3 --packages 1 --files 1 --ops 40 --fail-percent 0 --partition 100
VOPR_TRACE=1 VOPR_TRACE_FILE=/tmp/b cargo run --release --example vopr -- \
  --seed 3 --nodes 3 --buckets 3 --packages 1 --files 1 --ops 40 --fail-percent 0 --partition 100 \
  --trace-jsonl /tmp/t.jsonl
cmp /tmp/a /tmp/b                        # silent: 921 events, byte-identical
```

`make viz` runs all of it — the pack, the inertness gate above, and the standalone
pages — into the gitignored `.local/viz/`. It is advisory and out of `check`, like
`docs-truth`. It is also the pack's own staleness gate: every number a page says
out loud is pinned in `dev/scripts/viz/scenarios.json` and re-checked on every
build, so a scenario that stops reproducing fails the build instead of shipping a
stale claim. That is how seed 1327's FREEZE_UNJUSTIFIED repro came to be marked
retired at `439b839` rather than quietly rendering as green. See
[scripts/viz/README.md](scripts/viz/README.md).

```
make viz                 # everything, ~2.5 min including a 20 s live measurement
make viz VIZ_LIVE=0      # skip the live measurement
python dev/scripts/viz/build.py --only wedge --skip-inertness --live-secs 0
```

## Real cloud backends

The emulators (MinIO, Azurite) are fast and hermetic but not the real thing, and
GCS has no faithful emulator at all. To close the fidelity gap, the S3 suite and
the GCS round-trip can run against **real buckets**, off by default:

- `make test-s3-real` — set `PYPIRON_TEST_S3_REAL_BUCKET` and provide ambient AWS
  credentials (env, profile, or instance role). The whole s3-marked suite then
  targets that bucket instead of MinIO.
- `make test-gcs-real` — set `PYPIRON_TEST_GCS_REAL_BUCKET` and provide GCS
  credentials (a service-account JSON via `PYPIRON_TEST_GCS_SERVICE_ACCOUNT_PATH`
  or `GOOGLE_APPLICATION_CREDENTIALS`, or ambient ADC). This is GCS's only
  end-to-end coverage.

The two fixtures isolate themselves differently:

- **GCS** gives each test its own `--storage-prefix` (`pytest/<random>`) and
  deletes only that subtree afterwards. The bucket needs no dedication, is never
  emptied wholesale, and concurrent runs — two CI jobs, two branches, two
  laptops — cannot see or clobber one another.

  Teardown only runs if the process lives to reach it. A cancelled CI job (the
  `ci.yml` concurrency group cancels in-progress runs), a `timeout-minutes` kill,
  or a `SIGKILL` all strand one `pytest/<random>` subtree — as does a `SIGINT`
  landing during the cleanup itself. Nothing in the test suite collects them,
  because a sweeper would have to distinguish a stranded prefix from a live one
  in a concurrent run. Instead the bucket carries a **lifecycle rule deleting
  objects older than one day**, which is the whole garbage collector. A bucket
  used for these tests must have that rule, and must therefore hold nothing but
  test data. `gs://pypiron-ci-test` is configured this way.
- **S3** still writes to the bucket root and empties it before and after every
  test, so `PYPIRON_TEST_S3_REAL_BUCKET` must be **dedicated and disposable**,
  and those runs must be serial (no `-n`/xdist). Prefixing it the same way is a
  straightforward follow-up.

Both fixtures skip cleanly when their bucket env var is unset, so the default
`make test` is unaffected, and both CI jobs no-op green when the repo has no such
secrets. `real-gcs` runs in CI on pushes to `master` (not on pull requests: a
fork PR gets no secrets and would report a green check having tested nothing);
`real-s3` still runs weekly.

## Parallelism

The suite runs under pytest-xdist by default (`-n auto` in `addopts`; CI passes
`-n 8` — the tests are wait-bound, so oversubscribing the runner's cores pays).
Isolation is by construction, not by locking:

- **One emulator container per worker, one uuid-named bucket per test.** MinIO
  and Azurite are session-scoped fixtures — containers are the expensive part —
  and each test creates and drops its own `pypiron-*-<uuid>` buckets/containers
  on them. No two tests, and no two workers, ever share a bucket.
- **Ports**: servers get ports from `find_free_port`, which under xdist probes a
  per-worker range (the bare bind-then-close trick is a cross-process race).
  Containers get daemon-assigned ports (`-p 127.0.0.1::9000` + `docker port`).
- **Serial escapes** (`-n 0`, already wired into the Makefile targets):
  `make perf` (xdist swallows `-s` and parallel load corrupts timings),
  `make compat` (results aggregate in-process; the doc writer refuses to run
  under xdist), and `make test-s3-real` (the shared real bucket is wiped per
  test; the fixture fails loudly if it sees an xdist worker).

Debugging a single test? `pytest tests/test_x.py -n 0` gets you serial
execution, working `-s`, and pdb.

## What runs where

| When | What |
|---|---|
| Every PR (CI) | fmt, clippy `-D warnings`, Rust unit, model checking (bounded configs) + conformance suites, VOPR smoke (fault + crash-only + rotating profiles), blackbox on disk + S3 (MinIO) + Azure (Azurite), `cargo-audit`, fuzz-target build smoke |
| Push to `master` (CI) | all of the above, plus the real-GCS blackbox (when the bucket secret is configured) |
| Nightly | coverage-guided fuzzing (all six targets); deterministic simulation at volume + deep model configs (simulation.yml, counters and the oracle-reach gate published per run) |
| Weekly | client compat matrix, full-PyPI corpus check, unit-test coverage, real-S3 blackbox (when the bucket secret is configured) |
| Local / opt-in | `perf` and `stress` (release binary, excluded from default runs) |

## Performance testing

Purpose: make optimization honest. Every speed claim gets a number, and every
optimization gets a before/after.

- **Release binary only.** Debug-build numbers are meaningless; the perf fixture
  builds `--release`.
- **What's measured**: the hot read endpoints — global index (JSON), package index
  (HTML and JSON), artifact download — hammered with persistent connections,
  reporting RPS and p50/p95/p99 latency.
- **Comparative, not absolute.** The Python client harness is the bottleneck long
  before the Rust server is; the numbers are for spotting regressions and
  validating optimizations, not for marketing. (For absolute numbers, point `oha`
  or `wrk` at a running server by hand.)
- **Loose floors.** Assertions catch catastrophic regressions (an order of
  magnitude), not noise — perf tests that flake get deleted, so they must not
  flake.
- Run with `make perf`; excluded from default test runs.

### Per-endpoint micro-benchmarks

Spec: [dev/bench/MICROBENCH.md](bench/MICROBENCH.md). Two lanes over one
canonical endpoint table ([dev/bench/endpoints.py](bench/endpoints.py)) that
covers every route in the router — a drift test fails when a route is added
without a bench entry.

- **CI lane** (`tests/test_endpoint_bench.py`, runs in the default suite):
  deterministic asserts only. Every endpoint's exact per-request storage-op
  counts (`pypiron_storage_ops_total` deltas, cold and warm) must equal the
  pins in the table — an accidental O(n) storage access fails the build; a
  wall-time never does. After an intentional storage-access change, refresh
  the pins with `dev/bench/microbench.py pins --bin target/debug/pypiron` and
  commit the diff.
- **Tracked lane** (`make microbench`): cold + warm latency per endpoint at a
  50k-package / 1.2M-file PyPI-shaped tier, plus startup time, worst-case
  first-hit (the corpus's biggest package), RSS, and upload-to-visible.
  Results land in `dev/bench/results/microbench-<tier>.json` (committed; git
  history is the trend line). Never gates a PR. The tier is fabricated once
  and cached post-sweep under `.local/microbench/`, so a run starts in
  seconds; write endpoints run behind a verified snapshot/restore that keeps
  the cache byte-pristine. Full-PyPI scale is the same harness with
  `--packages 780000` on a throwaway cloud box.

## Running

```sh
make test            # cargo test + pytest (perf/stress excluded)
make test-rust       # unit tests only
make test-python     # blackbox integration tests
make perf            # performance benchmarks (builds release binary)
make microbench      # tracked per-endpoint latencies at the 50k tier
```

## Fuzzing

Coverage-guided fuzzing (`fuzz/`, needs nightly + `cargo install cargo-fuzz`)
covers the pure parsers that eat attacker- or upstream-controlled bytes. Each
target asserts "never panic" plus a domain invariant:

| Target | Module | Invariant beyond no-panic |
|---|---|---|
| `fuzz_names` | `names.rs` | PEP 503 normalization is idempotent; wheel-tag fields never empty; `version_cmp_desc` is a total order (reflexive, antisymmetric); `checked_pkg_name` only ever approves a normalized name |
| `fuzz_wheel` | `wheel.rs` | raw bytes: extracted METADATA stays under the 16 MiB cap |
| `fuzz_wheelzip` | `wheel.rs` | valid zips: METADATA is the first one-slash `*.dist-info/METADATA`, decoys never win |
| `fuzz_render` | `render.rs` | PEP 691 JSON always valid; HTML `href` can't break out of its attribute |
| `fuzz_coremeta` | `coremeta.rs` | RFC 822 METADATA parse is total over any bytes; `project_urls` never exceeds the count of `Home-page`/`Project-URL` header lines |
| `fuzz_range` | `range.rs` | a resolved `Partial(start, end)` is always `start <= end < size` |
| `fuzz_markdown` | `markdown.rs` | `render_limited` (the README sanitizer — no downstream escaping) emits only whitelisted tags and only `safe_href`-approved `http`/`https` link/image URLs; the multibyte-slice class that once panicked `safe_href` stays dead |
| `fuzz_advisories` | `osv.rs` | `parse_feed` is total over any bytes; the version matchers are total over any version string; an `AllVersions` scope is fail-closed (blocks every version, unparseable included); no parsed record field exceeds `MAX_ENTRY_BYTES`; `block_names <= audit_records` |

Add a fuzz target for any pure function that parses bytes an uploader, client, or
upstream index controls.

Each target replays two corpora: `fuzz/seeds/<target>/` is tracked and read-only
(cold-start inputs plus permanent regression seeds — the `fuzz_render` crash
reproducers live there so they replay forever), while `fuzz/corpus/<target>/` is
gitignored and machine-grown (new finds accumulate there across runs). A libFuzzer
dictionary in `fuzz/dicts/<target>.dict` seeds the mutator with the structural
tokens each parser keys on (zip magics, RFC 822 header names, Range grammar,
Markdown/unsafe-scheme bait, …). `make fuzz` and the nightly workflow pass all
three (seeds + corpus + dict) automatically.

```sh
make fuzz FUZZ_TARGET=fuzz_range FUZZ_SECS=60   # run one target (seeds + corpus + dict)
make fuzz-build                                 # compile all (CI smoke test)
```

`fuzz_advisories` fuzzes the OSV parse/match core, which lives in its own leaf
module `src/osv.rs` (the async fetch/persist/reload plumbing stays in
`src/advisories.rs`); the target `#[path]`-includes `osv.rs` plus `names.rs`, its
only crate dependency, like the other fuzzed modules.
