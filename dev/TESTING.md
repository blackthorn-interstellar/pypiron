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
a partitioned run that stops producing conflicts is still caught.

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

**The partitioned profiles are currently red**, which is the point — see the
findings recorded against the commit that landed this. That is why `--partition`
defaults to 0 and no CI invocation passes it: the nightly matrix and the pinned
regression seeds stay on the aligned schedule, byte-identical (verified by
diffing five whole-profile runs, every counter to the digit), until the findings
are triaged. The partitioned soak is run by hand:

```
cargo run --release --example vopr -- --rotate --partition 100 --max-secs 600
```

The findings, one repro each, all from the first soak (append `cargo run
--release --example vopr --` to each line). They are recorded here rather than
fixed alongside the harness change, so the harness lands separately from
whatever the fixes turn out to be:

| oracle | seed | flags | what it says |
|---|---|---|---|
| `AUDIT_PREMATURE_CONSUMPTION` (global membership) | 1 | `--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 3 --partition 100` | a drain of a peer bucket re-derived a package past every truth mutation, wrote the global index claiming it absent, and the tier-3 audit had to flip the membership back. The dominant finding by volume (~50% of partitioned seeds); `AUDIT_REPAIRED_VIEWS` at `--fail-percent 0` is the same shape under the crash-only blanket gate |
| `AUDIT_ORDERING` (class 1) | 268 | `--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 3 --partition 100` | a `packages/<pkg>/.origin` write on a home bucket had **no covering breadcrumb** — no `_dirty/` marker there, no `_repl/` note aimed at it — so nothing durable could tell a tick to re-derive the name. The first class-1 the product has ever produced; the `EXPECTED_ZERO` entry saying otherwise is now scoped to the aligned schedule |
| `DURABILITY` + `CONSERVATION` | 267 | `--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 0 --partition 100` | an acknowledged upload's bytes are gone from every bucket and from `_quarantine/`, with both buckets serving a *different* body under that filename and no tombstone or freeze. The strongest one: a `200` was retracted with no evidence |
| `CONSERVATION` (frozen) | 165 | `--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 0 --partition 100` | two byte-sets acked, the fleet froze the filename, and only *one* survived in `_quarantine/` — the freeze lost a body it is supposed to preserve before it drops anything |
| `FREEZE_UNJUSTIFIED` | 91 | `--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 0 --partition 100` | frozen on every bucket with only **one** byte-set attested anywhere (acks ∪ every `_quarantine/` copy) — a freeze that suppresses a filename and waives its durability check without a real conflict behind it |
| `CONVERGENCE` | 208 | `--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 0 --partition 100` | `packages/<pkg>/.origin` reads `mirror` on bucket 0 and is **absent** on bucket 1 at quiescence — an orphan claim that never replicated and never got released, so one bucket reserves a name the other would let a proxy fill |
| `ACK_TOTALITY` | 19 | `--nodes 3 --buckets 2 --packages 6 --files 2 --ops 160 --fail-percent 0 --partition 100` | a publish acked while a peer held neither the record, nor a `_repl/` note owing it, nor any merge marker explaining its absence. Crash-only, where a missed note is the hard gate |

How often, on a disjoint seed range (`--start-seed 9000001`, `--packages 6
--files 2 --ops 160 --partition 100`) — the share of seeds each oracle reds on,
so a triage can be ordered by frequency rather than by which repro was found
first:

| | rotating, 86,010 seeds | multi-bucket, 50,884 seeds | crash-only, 52,354 seeds |
|---|---|---|---|
| **any** violation | 16.1% | 41.2% | 50.9% |
| `AUDIT_PREMATURE_CONSUMPTION` / `AUDIT_REPAIRED_VIEWS` | 6.1% / 6.9% | 38.9% / — | — / 43.4% |
| `ACK_TOTALITY` | 2.3% | — | 14.0% |
| `CONVERGENCE` | 0.91% | 1.67% | 0.69% |
| `AUDIT_ORDERING` (class 1) | 0.68% | 0.03% | — |
| `CONSERVATION` | 0.57% | 2.09% | 2.07% |
| `FREEZE_UNJUSTIFIED` | 0.27% | 0.51% | 0.57% |
| `DURABILITY` | 0.12% | 0.56% | 0.20% |

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
| `durability` | an acked artifact serves corrupt bytes on every bucket, the real bytes parked outside `packages/` *and* outside `_quarantine/` | `DURABILITY: acked … serves bytes no ack carried` | 4 |
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
| `split` | a byte conflict the merge never resolved: bucket 1 acks and keeps a second byte-set under a filename bucket 0 still serves its own for | `DURABILITY: … never left split` (needs ≥2 buckets) | 4 |

**Freeze and origin get two legs each, on purpose.** Freeze *justification* (a
`.frozen` marker must be a real byte conflict) and freeze *totality* (a freeze
preserves both bodies before it drops either) are two claims about the same
marker, and origin exclusivity is asserted once on the package `.origin` claim
and once on the sidecar of the record under it. A branch that shares a leg with
its neighbour is a branch nobody has watched fire — which is the whole argument
for this table.

Three of these reds land on a second oracle too, unavoidably, and the leg's
expected text is what pins which one it is for: `split` also reds CONVERGENCE (a
filename two buckets serve different bytes for *is* a diverged key — which is why
DURABILITY names it instead of leaving it as one line of a key diff),
`freeze-lossy` also reds VERIFY (a `.frozen` marker takes the record out of the
renderable set while the view still lists it, exactly as `--break conserve`
does), and `mirror-served` reds ORIGIN_TERMINALITY and nothing else.

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
(K=4); `split` 79.3% (K=4); `race` 76.2% (K=5). Nothing below 100% is a weaker
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
66.7%, the three durability-family breaks 66.3% and `freeze-lossy` 66.4% (K=6);
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

#### Workload-reachable, harness-unreachable, product-unreachable

Three different things, and conflating them is how an unfalsifiable gate survives
a review. A kill proof says the oracle is *sound*. It says nothing about whether
anything but a break can ever red it, and the answer differs per oracle:

- **workload-reachable** — nothing forbids the red state. The oracle reads green
  because the product is correct on the schedules sampled, and a real regression
  would red it. This is the ordinary, healthy status.
- **harness-unreachable** — the *simulator* cannot stage the state, though
  production can. The oracle is a standing watch that this harness will never
  trip; something else has to cover it.
- **product-unreachable** — a *product rule* forbids the state, so it cannot
  occur in production either. The guard is a watch on a rule that could one day
  be relaxed.

| oracle | status | why | kill proof |
|---|---|---|---|
| VERIFY | workload-reachable | every convergence regression pinned in ci.yml's seed corpus red it | `view` |
| DURABILITY (acked bytes stand) | workload-reachable | no rule forbids a bucket losing an acked record | `durability` |
| DURABILITY (never left split) | workload-reachable **only partitioned** — and *observed* there | needs one filename acked with different bytes on two buckets, which only `--partition` produces | `split` |
| VISIBILITY | workload-reachable | ditto, for the listing | `visibility` |
| CONSERVATION (acked bytes survive) | workload-reachable | ditto, fleet-wide | `conserve` |
| CONSERVATION (a freeze loses neither body) | workload-reachable **only partitioned** — and *observed* there | a freeze needs a real byte conflict to fire at all | `freeze-lossy` |
| FREEZE_JUSTIFIED | workload-reachable **only partitioned** — and *observed* there | ditto | `freeze-unjustified` |
| ORIGIN_TERMINALITY (the `.origin` claim) | workload-reachable, never witnessed | needs a mirror claim to win over a private one on some bucket after the private ack | `origin-demoted` |
| ORIGIN_TERMINALITY (the record under it) | workload-reachable, never witnessed | needs a live mirror sidecar left renderable under a private claim | `mirror-served` |
| CONVERGENCE | workload-reachable | needs ≥2 buckets; the replication paths that could break it run on every multi-bucket seed | `diverge` |
| LIVENESS | workload-reachable | any undrainable breadcrumb reds it; the fast path has simply always drained | `wedge` |
| ACK_TOTALITY | workload-reachable, and *observed* | 166 misses per 5,000 seeds on 3n/2b under fault injection, where it is a reported statistic; crash-only, where it is fatal, has never produced one | `fanout` |
| DETERMINISM | workload-reachable | any nondeterminism downstream of the op sequence reds it | `rerun` |
| TOMBSTONE_MONOTONICITY | **product-unreachable** | `publish_record`'s tombstone fence rejects re-publishing a deleted filename, so no ack can follow a `204` — 150k wide seeds (795k acked uploads, 251M interleavings) produced zero, and could not have produced one | `resurrect` |
| classifier TEST 1 / 2a / 2b / FALLBACK (both analyses) | workload-reachable, never witnessed | each needs the tier-3 audit to have repaired a view; the marker/tick/sweep/reconcile fast path converged every schedule in the 140k-seed sample (0 audit repairs) | `ordering`, `globalindex`, `poison`, `blind`, `fallback` |
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

`--break ordering` is still the only class-**1** input the classifier has ever
been handed; the product has never produced one. Class 2 is no longer synthetic
— seed 1067836 above is the real thing.

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
audit repairs in 140k seeds). Nothing in the workload reaches any of the ten
— so ~310 lines of classifier are carried entirely by inputs a `--break` has to
supply, and the honest read is that the taxonomy is a diagnosis tool for a rare
event, not a routinely-exercised gate. Every arm now has a kill proof
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
| classifier/pkg TEST 1 | the product has never produced a class-1 ORDERING repair **on the aligned schedule** — `--partition` reaches it (a `.origin` write on a home bucket with no covering breadcrumb), which is a finding, not coverage | `--break ordering`, `--break globalindex` |
| classifier/global TEST 1 | same, on global membership | `--break globalindex` |
| classifier/{pkg,global} TEST 3 | *harness*-unreachable: `tick_lock` serializes rebuilds, so two never overlap (see the reachability table above). Still true under `--partition`: a partitioned node diverges only its **writes**, never its rebuilds, so every tick still takes the one bucket-0 lease | `--break race` |
| classifier/{pkg,global} TEST 2a | needs an audit repair whose final writer had listed truth past every mutation | `--break poison` |
| classifier/{pkg,global} TEST 2b | needs an audit repair TEST 1 declined — a covered mutation whose breadcrumbs were all consumed blind | `--break blind` |
| classifier/{pkg,global} FALLBACK | reached only by drift no test explains — which would itself be a classifier bug | `--break fallback` |

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
