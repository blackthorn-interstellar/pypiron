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

The chaos suites and the VOPR sample failure schedules against the real binary;
the models in `tests/model_*.rs` enumerate every interleaving of a smaller
hand-written abstraction of the same two protocols. Different evidence, not a
stronger version of the same evidence — the sampling tier is what found the
global-index staleness bug (1bc3ce9) and the three convergence bugs in e860792,
all inside the enumerating tier's nominal scope.

- **Event protocol** (`tests/model_event_protocol.rs`): writers running the
  intent/commit marker dance, workers running list → rebuild → global-index CAS
  → delete-observed-markers, crashes between any two steps, and clock advances
  past the intent grace. Checked invariants: an acknowledged upload is durable;
  at quiescence every view equals a fresh derivation from truth; a tombstoned
  file never resurrects. Visibility is checked as *reachable*, not as a liveness
  guarantee — the model states no fairness assumption.
- **Replication merge** (`tests/model_replication.rs`): two buckets, partition-
  shaped double publishes, mirror-vs-private races, yanks, deletes, demotion
  settles. Checked invariants: buckets converge at merge fixpoint; acknowledged
  bytes are never silently lost (conflict losers land in quarantine, only an
  authorized delete destroys); deletes settle dead everywhere.

### What the bounds actually are

The README links here for them, so they live here rather than in a header
comment. Replication merge: one package, two filenames, exactly two buckets,
`.metadata`/`.provenance` companions excluded, and convergence compared as a
*truth projection* — two buckets agree on what they serve, while their
`_quarantine/` sets may legitimately differ. Event protocol: one package, two
files, two lease-serialized workers, and a global name index collapsed to a
single membership bit.

Not modeled: counter rollups, the transparency chain, project status, read
locality, concurrent mergers, and the N-bucket star composition. What backs up
each exclusion is not the same, and the difference is the point of this section.
Read locality is the one exclusion with a mechanized adversary of its own — the
conformance walk below — rather than nothing.

### The read-pin conformance walk

Both models stop at the write pin and `examples/vopr.rs` has no read-affinity
op, so the read pin had no mechanized adversary at all until `cc9627d` shipped
two defects in `evaluate_read` and both tiers missed them. The walk
(`src/bucket_health.rs`, `mod conformance_walk`) covers that class: seeded
random fault, upload and drain schedules driven against the **real**
`HealthController`, not a transcription of it — a transcribed model is exactly
how the gap opened, so a transcription gap cannot re-open it. It owns the ground
truth the controller cannot see (which buckets are actually reachable, how many
`_repl/` repair notes the region bucket is owed) and mirrors the two contracts
that decide the pin: the startup sequence in `src/app.rs` and the worker cycle in
`src/worker.rs`, every mirrored behavior citing the lines it copies.

Two fidelity rules are what make a violation mean anything, and both are
load-bearing — an earlier revision had neither and reported two "defects" the
real code cannot reach.

- **Reachable start states.** Every run boots the way `app::serve` does:
  storages are observed before any pin exists, so a reachable bucket is already
  `Healthy` with its topology stamp acknowledged (app.rs:1088-1130); an
  unreachable one is collapsed through the leave threshold (app.rs:1132-1137);
  and only then is the read pin seeded, onto the region bucket if and only if it
  was reachable *and* owed nothing (app.rs:1191-1200). A fresh controller with a
  pin on a never-observed bucket is not a state this system has, and starting
  there manufactures a stamp-raise that looks like a defect.
- **Bounded intra-cycle time.** Simulated time inside one worker cycle is capped
  at what a real cycle can span — one `BUCKET_HEALTH_IO_TIMEOUT` per probe, per
  caught-up LIST and per topology verification, so `(2N+1) s` for N buckets. The
  walk's return window is set *longer* than that worst case, which is the real
  ratio (300 s default against a few seconds) at its most adversarial. Without
  that bound the walk can let a bucket fail, recover and re-earn a full return
  window inside a single cycle, which no shipped configuration permits.

Three oracles: the loan flag may never outlive the pin (every step); at
quiescence, reads may not be served from the region bucket while it still owes
the debt it owed when they were admitted; and, also at quiescence, a healthy
region bucket that owes nothing must actually be serving the reads — which is
what stops a fix from simply demoting harder. "Served" means the pin the request
path really uses: `BucketSet::read_pin` aliases the write pin until read
affinity is activated (buckets.rs:247-256), so the controller's belief is not
automatically the answer.

The fast tier (256 seeds x 400 steps, ~0.13 s) runs in `cargo test`; the deep
tier is `#[ignore]`d and runs nightly in `.github/workflows/simulation.yml`,
sized by `PYPIRON_WALK_SEEDS` / `PYPIRON_WALK_START` / `PYPIRON_WALK_STEPS`. A
failure prints the seed, the boot outcome, the violated oracle and the full step
trace; the seed alone reproduces it:

```sh
PYPIRON_WALK_SEEDS=1 PYPIRON_WALK_START=<seed> \
  cargo test --lib read_pin_conformance_walk_deep -- --ignored --nocapture
```

It is proven to fire, not assumed to: five mutations each fail the suite — losing
the loan expiry, clearing the loan flag on a demotion that moved nothing,
expiring loans that were never loans, dropping the caught-up clear, and undoing
the boot loan marking below. Four die inside the fast tier's own seed set; the
caught-up clear is pinned by a directed unit test instead. There are no oracle
allowances: every violation the walk reports is one to fix.

**What it found.** A node that boots while its write home is down fails the write
pin over onto its region bucket, and `region_owed_no_notes` cannot call that
bucket converged — it LISTs every peer and an unreachable one alone forces a no.
Startup therefore recorded reads on the region bucket while `seed_read_pin` was
skipped, and recorded them as *earned*. When the home healed and the write pin
went back, `evaluate_read` found no loan to expire, so the read pin never moved
again — which meant the worker never proposed a read switch, `BucketSet` never
activated read affinity, and every read was served from the write bucket for the
life of the process. Correct bytes throughout, which is why nothing else caught
it. The pin is now marked as the loan it is, and the `cc9627d` machinery takes it
from there. A sibling shape — a region bucket whose stamp is under
re-verification retaining the pin ungated — is closed by demoting on
`topology_blocked` in the same retention branch, fail-closed: an ineligible
bucket may be accruing repair notes, so it re-earns through the gate on ack.

The N-bucket star is the load-bearing exclusion — the model enumerates
*pairwise* confluence, and pairwise implies N-way only if the merge is
associative. It is not: `conflict_winner`'s `CONFLICT_SKEW_MS` guard
(`src/replicate/decide.rs`) is non-transitive, so three private uploads of one
filename at receive stamps 0 / 3000 / 4000 give A ≻ B, A ≻ C, but Freeze(B, C).
Whether that filename ends live or frozen fleet-wide depends on which pair
merged first. Both outcomes are safe — every loser is quarantined, and the
schedule-dependent side is the fail-closed one — but it is availability the
model cannot see. The VOPR's three-bucket lane samples the star.

Counter rollups and the transparency chain have no mechanized adversary at all.
The VOPR does not touch either — `examples/vopr.rs` contains no reference to
`src/counters.rs` or `src/transparency.rs` — so their only coverage is
hand-written: the unit tests in each module, plus `tests/test_counters.py`,
`tests/test_transparency.py`, and the counter and chain cases in
`tests/test_multibucket.py`. This paragraph previously said these were "covered
by sampling instead." That was false, and it is how two production bugs reached
a shipped feature and were found by audit rather than by a gate: a day-rollup
key carrying no bucket identity, and a chain fork that could never heal. Until a
VOPR op exists for a subsystem on this list, treat it as uncovered — a stated
compensating control that nothing executes is worse than a blank.

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
- **untyped disappearance** — an artifact body never leaves a bucket unless the
  *filename* is durably closed (a `.tombstone` or a `.frozen`, the two markers
  `publish_record` refuses over) or the *bytes* are durably kept (a
  `_quarantine/` copy of exactly them). The only *continuous* oracle here —
  every other one reads the final world, so the corruption they catch has to
  have been made permanent first. Freeing an immutable filename is already
  broken at the instant it happens: a rebuild that read those bytes publishes a
  sidecar over the empty key, the next upload wins the create with different
  bytes, and the bucket serves body B under body A's published sha256 forever.
  Getting SELF_CONSISTENCY to see that took two *more* faults on the
  compensation path, which is why `115b9ca` cost ~16M seeds; watched at the
  delete itself the same bug is depth 1, and `--sweep-faults` reds it at op 8 of
  seed 7384. Recorded in `SimStorage::body_removals` rather than in the
  `Observer`, which collapses put and delete into one `EffectKind` and never
  sees the warm-bucket audits at all. Note what is *not* on the authorized list.
  `.mirror-quarantined` is the one fence that deliberately does not bar an
  upload, so it can stand over bytes it never adjudicated — the loss measured on
  seeds `86001009016` and `40000042940` — and only the `_quarantine/` copy
  excuses that delete. A mirror *sidecar* is not an excuse either: a mirror
  upload publishes its sidecar before the fence check that can still refuse it,
  so a sidecar carve-out would waive the invariant over the very shape this
  oracle exists for. (Mirror cache eviction, the product's one unfenced body
  delete, needs no carve-out: it is single-bucket only, and this workload draws
  mirror records only on partitioned seeds.) `--break demote-lossy` is the kill
  — a demotion fence standing over a body in no `_quarantine/` copy — and
  `conserve` and `slow-repair` red it alongside their own oracle;
- **staleness** — bounded agreement, on a clock. See below.

### Bounded staleness: how long "converged" is allowed to take

The oracle above used to be **liveness**, and liveness was a boolean over a
made-up number: the fleet had to reach a fixpoint inside 12 heal rounds x 20
drain passes. Nothing in that is time. A seed that converged on round 1 and one
that converged on round 11 were the same green, and *converges, but would have
taken an hour* passed silently — which is the failure operators actually
experience, and the one nothing was watching for.

The product promise (dev/DESIGN.md) is a time: **after the last write lands and
the faults clear, every bucket agrees within five minutes**, with proportional
forgiveness when the fleet is holding exceptional debt. The simulator runs on
virtual time, so measuring that costs nothing.

**What is measured.** `t0` is the instant the storm ends — faults off, every
node restarted (which aborted its in-flight tasks), no further client op coming.
From there the driver samples the raw buckets at every point where it already
dumps them: before the first drain pass, after each drain pass, inside the
leader drain, after the audits, and at each round boundary. The clock stops at
the **first** sample where every bucket agrees *and* the fleet owes nothing —
one definition of "agreed", `agreement_projection`, shared with the CONVERGENCE
oracle so the bound cannot come to be measured against something other than the
thing it gates.

Sampling *inside* the round is not a detail. With samples only at round
boundaries the harness charges a fleet that converged during the tree diff the
whole remaining round — 360 s of driver clock it did not spend. That was
measured, not theorized: one seed in 362,000 (`--seed 900016048 --rotate
--partition 100`) read 480 s that way and breached its deadline; sampled
properly it reads 120 s and spends 30% of its budget. A deadline that fires on
the harness's sampling rate is worse than no deadline.

**The deadline.**

```
deadline = --staleness-secs (default 300)
         + 90 s  per page of `_repl/` notes and `_dirty/` markers
         + 360 s per page of cross-bucket divergence no marker covers
```

A *page* is `REPL_SWEEP_PAGE` = 1,000 (`src/replicate.rs`): the sweep works what
it listed, so a backlog deeper than a page needs another cycle. The two rates
are the two loops that do the work. 90 s is the driver's marker-drain pass — the
harness's model of the loop production runs on `--worker-interval-secs` (1 s) and
`--repl-sweep-interval-secs` (300 s). 360 s is one tree-diff cycle: `reconcile`,
the three drains that consume the markers it brackets its copies in, and the
audit that follows. Production runs *that* loop on `--reconcile-interval-secs`,
a day by default — so this harness holds the backstop to 4x its fast path where
the shipped configuration holds it to 288x. The bound here is far tighter than
the product's own scheduling, which is the right direction for a simulator.

**Why affine, and why the debt is read exactly once.** A queue drained at a
bounded rate empties in time proportional to its depth; that is the only shape a
real convergence engine has. A deadline that grew *faster* than the work would
excuse precisely the failure this exists to catch — a fleet whose per-record
cost rises with its own backlog. A constant deadline would be wrong in the other
direction, and the owner said so explicitly: 20,000 records queued behind a
healed partition cannot move in the same five minutes as three. And the debt is
sampled at `t0` and never again, because **the allowance must not be a function
of the defect it is measuring**. Recomputed from live debt it would grow every
time the fleet re-created a marker it had failed to consume, so the bound would
chase the bug forever instead of firing on it.

**The census.** Measured at this commit over 361,987 seeds across eight
profiles, with the deadline lifted out of the way (`--staleness-secs 100000`) so
every seed reported its convergence time instead of stopping at the first
breach:

| profile | seeds | convergence times seen | tightest margin |
|---|---|---|---|
| single-bucket | 55,000 | 0 s, 120 s | 30% spent |
| single-bucket-crash-only | 55,000 | 0 s, 120 s | 30% spent |
| multi-bucket | 51,893 | 120 s, 480 s | 64% spent |
| multi-bucket-crash-only | 53,727 | 120 s, 480 s | 64% spent |
| three-bucket | 36,367 | 120 s, 480 s | 64% spent |
| three-bucket partitioned | 27,500 | 120 s, 480 s | 64% spent |
| rotating-swarm | 55,000 | 0, 120, 210, 480, 570 s | 76% spent |
| rotating-swarm partitioned | 27,500 | 0, 120, 210, 480, 570 s | 76% spent |

Zero seeds anywhere ran past the deadline, and none failed to converge at all.
The two facts that shape the constants: **every** seed whose `t0` debt was
breadcrumb-only converged in ≤210 s — five minutes covers the fast path with
30% to spare, exactly as the promise says — and **every** seed that needed the
tree diff had uncovered divergence at `t0`, so the backstop cycle is bought by
the work that requires it and by nothing else. The worst case anywhere is 570 s
against a 750 s deadline.

**The margin is printed on every run**, red or green, and it is a *share* of
each seed's own deadline rather than the longest absolute wait — ranking by
seconds would report whichever seed was owed the most allowance and used it
comfortably, and never the seed about to breach.

The old rounds/passes budget is still there, because the driver's loop has to
terminate somewhere, but it is no longer the claim: a run that exhausts it has
already blown the deadline, and both halves of the failure are reported under
`STALENESS` — *took Ns, past its Ms deadline* and *never stopped working*. Each
half has its own kill proof (`--break slow-repair`, `--break wedge`).

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
census at `08d94f2` are closed as of `8e5916f`*, each with durable cover that reds
a merge rather than a nightly — one as a pinned `ci.yml` seed (`62000150551`), the
other two as unit tests
(`a_peers_stranded_global_html_is_seen_by_the_node_that_won_the_last_cas`,
`a_stale_spent_fence_clear_never_unauthorizes_a_settled_demotion`). Whether new
ones are open is a question about the *next* census, and this paragraph cannot
answer it.

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
  the fence, and refuses. Debris a refused writer left is not half of a byte
  conflict — until something loses the filename to it, which is the narrowing in
  *Debris stops being debris when a writer loses to it* below.
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
delete clears, a fence-refused write attests only once contested); the middle two
fail on the old currency.

**Debris stops being debris when a writer loses to it.** The rule above shipped
as "a body written under a fence never joins", full stop, and that is
single-bucket reasoning stated fleet-wide. `publish_record` stores the bytes,
reads the filename fence, and refuses — then deletes what it wrote **only when
`state.buckets.is_multi()` is false**. With peers it deliberately leaves them:
*"a fenced multi-bucket loser stays occupied and inert"*, because deleting by
key could erase a private replacement that landed after the writer's
cross-object read. Those bytes go on standing at the canonical key, and the next
replication copy of that filename loses its `create_artifact_verified` to them,
reads them back, finds bytes its own sidecar does not name, and freezes both
sides on the spot (`replicate::freeze_copy_race` — a real coexistence, and the
one path `Verdict::Freeze` never sees). Two byte-sets under one immutable
filename is exactly what FREEZE_JUSTIFIED asks for, and the attestation was
blind to it: a false red on a correct freeze. It is also the *only* surviving
evidence, every time — the refused publish never published a sidecar, so no ack
names those bytes, and `freeze_side` quarantines the body each bucket kept
rather than the one it lost.

The narrowing is the losing create itself, not the fence and not the fleet. A
conditional create that finds a body no `committed` entry names has collided
with a fence-refused one, and `SimStorage` records that digest in
`Inner::contested`. Everything else is untouched: a refused body nothing ever
went for stays out, and an ordinary immutability 409 against a published record
adds nothing, or every second upload of an existing filename would read as a
byte conflict. The oracle then unions `contested_digests` **only when
`fleet.buckets.len() > 1`**, because single-bucket the refused body is deleted
by its own publisher and no merge runs at all — the loser's 409 is the end of
it.

Two witnesses, both `--nodes 3 --buckets 3 --packages 6 --fail-percent 3
--partition 100`, red at `76334ff` and green after — `--seed 97800043727
--files 4 --ops 63` and `--seed 121000059774 --files 2 --ops 94`, both
`--shrink`-minimized from 80 and 160 ops and both pinned in ci.yml.
Per-seed kill rate of `--break freeze-unjustified`, re-measured either side of
the change (`vopr --seeds N --break freeze-unjustified`, and the batched census
agrees digit-for-digit with N separate `--seed` runs):

| flags | before | after |
|---|---|---|
| `--nodes 2 --buckets 1 --ops 80` (the CI gate leg), seeds 1–2000 | 1864 (93.2%) | **1864 (93.2%)** |
| `--nodes 3 --buckets 3 --packages 6 --files 2 --ops 80 --fail-percent 3 --partition 100`, seeds 1–500 | 240 (48.0%) | 229 (45.8%) |
| `--rotate`, seeds 1–2000 | 1788 (89.4%) | 1378 (68.9%, K 3 → 6) |

The gate is untouched — single-bucket never reaches the `peers` branch — and the
multi-bucket give-back is the correct answer rather than a regression: on those
seeds a fence-refused body really was contested under the frozen filename, so
two byte-sets really did stand there and the oracle's claim is satisfied. Two
earlier drafts are on record for why this shape and not another: attesting
*every* fence-refused body (no losing-create test) costs far more —
188/500 and 1113/2000 on the two multi-bucket rows — and applying the
losing-create rule *without* the `peers` gate costs the CI leg itself, 1601/2000
(80.0%). Verification soaks, one process each: `--max-secs 240` from seed
200000000000 on `--nodes 3 --buckets 3 --packages 6 --files 2 --ops 160
--fail-percent 3 --partition 100` is clean before (18,593 seeds) and after
(15,460), and `--max-secs 60` from 97800043000 on witness 1's profile goes 1
failure (5,491 seeds) → 0 (4,418). Seed *counts* differ between the two because
a time budget explores whatever the box's rate allows; compare the verdicts, and
compare ranges when the count has to mean something.

**Kill rates are measured artifacts now, not remembered ones.** Every `--break`
run finishes its whole draw and prints what it killed:

```
vopr: --break freeze-unjustified killed 6/6 seeds (100%) — measured by this run
```

so the CI kill-proof log carries the rate for all 23 legs on every run, and a
break that starts escaping shows up the day it lands instead of the day someone
re-measures. (Only `--break` runs finish the draw; every other run still stops
at its first counterexample.) The table below stays for the K derivation and
the profile each rate was taken on — but it is a snapshot of that output, and
the output wins. Batched and one-seed-at-a-time censuses agree exactly:
`--seeds 500 --break freeze-unjustified` on the CI leg's flags reports 470/500,
the same 470 that 500 separate `--seed N` invocations produce.

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
   the one case for which the audit is the *documented* backstop. One direction
   is carved out and held to class 2: a stale **global-index** write that
   *delisted* a name the fresher clobbered write listed. The product forbids it —
   the audit's dead observations are re-proved against fresh truth inside the
   global update's locked CAS attempt (`update_global_index_verified`,
   src/worker.rs) — after the CI scale suite caught a boot audit's walk-stale
   remove delisting a freshly published package for a full reconcile interval
   (a day at the production default). The add direction (a stale write
   re-listing a name a fresher write removed) stays class 3: live observations
   are unverified by design, and its harm is a ghost listing, not a vanished
   publish.

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

### The delist lane (`--excludes`)

Until this landed, **no simulated seed ever configured an `--exclude-package`
denylist**, and the VIEWS == TRUTH oracle said so out loud: it called
`verify_storage` with `Denylist::default()` and required views to equal truth in
full. A delisted package violates that by design — the name leaves every index,
the bytes stay on disk and stay fetchable by direct `/files/` URL — so the whole
delisting half of the product was outside the simulator's world. Four walls, all
of them verified before this section was written:

1. **No denylist, ever.** With `state.denylist` permanently `None` the rebuild's
   scrub (`worker::rebuild_package_indexes_inner`) was dead code in simulation.
2. **`worker::reconcile_excludes` was unreachable.** Only `run_worker` calls it,
   and the VOPR drives `worker::tick`/`worker::audit` directly — it has never
   entered the worker loop. That function is the *only* thing that re-derives a
   name's visibility after a config-only change, because such a change moves no
   artifact: the package fingerprint is unchanged and the audit skips it.
3. **`app::initialize_indexes` was unreachable.** Only the HTTP boot calls it,
   and the VOPR boots no server. Its "seed only the missing half of the global
   pair with an empty render" branch — the branch a crash between the pair's two
   writes hands a live corpus to — could not occur.
4. **`verify-index`'s cross-half check had no oracle.** "The maintenance CLI
   disagrees with what serve enforced" was a shape nothing asserted, and the
   simulator's steady state is not the product's: the VOPR audits every heal
   round and drains to a fixpoint, while `pypiron serve` audits once at boot and
   then every `--audit-interval` (**86400s** by default). Anything the audit
   repairs is invisible to an oracle that reads the final world, and in
   production it is what installers get for a day.

`--excludes <percent>` is the share of seeds that arm the lane. An armed seed
configures a seed-derived denylist — always one bare (whole-project) exclude,
sometimes a second version-pinned one — and runs the product's boot sequence at
every node start: `app::initialize_indexes`, then `worker::reconcile_excludes`
against every bucket. Both write storage, so the lane is a **chaos dimension like
`--partition`, not a workload shape**: rotation does not derive it, `--rotate
--excludes N` is legal, the reproduce line carries it, and it defaults to **0**,
where the harness is byte-identical to the pre-lane one (`excludes_for` returns
before it draws any rng, `boot_args` returns `None`, and not one storage op is
issued). Verified, not asserted — three whole-profile runs at `--excludes 0`, `src/`
held identical and only `examples/vopr.rs` differing, match the pre-lane binary
on **every counter and every existing reach row, to the digit**; the only
differences in the whole output are the two new reach rows and the wall-clock
duration string:

| profile | storage-op interleavings | acked uploads | ack-totality misses | audit repairs |
|---|---|---|---|---|
| `--seeds 300 --nodes 2 --buckets 1 --ops 80` | 211,648 | 1,028 | 0 | 0 |
| `--seeds 200 --nodes 3 --buckets 2 --packages 4 --files 3 --ops 120` | 474,372 | 1,737 | 28 | 0 |
| `--seeds 300 --rotate` | 545,629 | 1,557 | 12 | 0 |

**Half the armed seeds change the config mid-run**, and that half is the point.
A denylist is startup config: it reaches a running process no other way, so
"the exclude was there from boot" and "the operator added it and rolled the
fleet" are two different worlds, and only the second has a mixed-config window in
it. Nodes adopt the new set when they restart, exactly as production does; the
heal phase restarts everyone, so the whole fleet is on the final set before any
oracle looks. A failing armed seed prints its schedule beside the reproduce line
— an armed failure whose schedule you cannot read is one you debug twice.

Two oracles come with the lane, and the existing view oracles learned the delist
rule rather than growing an exemption:

- **`GLOBAL_PAIR`** — the PEP 503 index and the PEP 691 index are two renderings
  of one name set, so they must name the same packages: present together, absent
  together. Asserted at **the pre-audit check-point of every heal round** — the
  markers drained, every node booted since its last crash, and the tier-3 audit
  not yet run — which is exactly where `pypiron serve` spends every
  `--audit-interval`, and where nothing in the harness looked before. This is
  `verify-index`'s `stale-global-index` check reduced to the half that needs no
  truth listing. It runs on every seed, armed or not, off raw `dump()` reads, and
  reaches 100% of seeds on every profile measured.
- **`DELIST`** — a bare exclude hides the NAME and keeps the BYTES, so on any
  bucket that still stores a fully-denied package's artifacts, that name appears
  in neither global half and has no `simple/<name>/` view. The premise is the
  retention half of the contract and the conclusion is the delisting half, and
  one execution is counted only where the premise holds, so the oracle cannot
  pass by having nothing to look at. It reaches 65% of seeds at `--nodes 2
  --buckets 1 --ops 80 --excludes 100`.
- **VIEWS == TRUTH** now rules through the denylist the fleet is configured with
  (which is what `serve` enforced and what the stored indexes were last built
  against), and **VISIBILITY** treats a denied file the way it already treats a
  mirror record under a private claim: `renderer_omits` grew a denylist arm.
  That exempts the *index entry* and nothing else — DURABILITY still demands the
  bytes and the sidecar on every bucket, and CONSERVATION still demands they
  exist somewhere, which is the delist contract exactly.

#### What the lane found on its first night — CLOSED by `7292634`

**A rolling restart left an exclude unenforced until the next audit.** Every
failing armed seed measured is a mid-run transition seed, and the shape is one:

1. Node B restarts with the new config, boots, reconciles. The reconcile stamps
   `_state/enforced-excludes.json` with the new set, marks the affected names
   dirty, and a tick delists them.
2. Node A has not restarted yet, so `state.denylist` is still `None`. Its own
   rebuild of that package applies no scrub and **puts the name back** — in the
   per-package view, the global index, or both.
3. Node A restarts. Its boot reconcile diffs the live denylist against the stamp,
   finds them **equal**, and does nothing. Nothing is marked dirty, so the tick
   path never re-derives the name, and the relisting stands until the tier-3
   audit — up to `--audit-interval`, 86400s by default.

The audit does repair it, which is why the harness sees it as `AUDIT_*` rather
than as a stale world: `AUDIT_REPAIRED_VIEWS` on crash-only seeds (where any
audit repair is a violation) and `AUDIT_PREMATURE_CONSUMPTION` on the richer
fault topologies. Rates and repros, measured at `8d8e639` — *after* that
commit's two audit/global-index fixes, which this is not — `--max-secs 120
--excludes 100`, one process each:

| lane | seeds | failing | rate | first repro seeds |
|---|---|---|---|---|
| `--nodes 2 --buckets 1 --ops 80 --fail-percent 0` | 131,618 | 101 | 0.077% | 803, 3440, 4802, 5130, 5441, 6200 |
| `--nodes 3 --buckets 2 --packages 4 --files 3 --ops 120` | 26,653 | 95 | 0.36% | 212, 270, 292, 593 |
| `--rotate` | 37,292 | 185 | 0.50% | 49, 57, 461, 487 |

Causally attributed, not guessed, and read straight off the runs rather than
from a control build. Half of every armed seed is configured from boot and half
transitions mid-run, and each failing seed prints its schedule beside its
reproduce line: **381 of 381 failures across the three lanes are mid-run
transitions, and none is a from-boot seed** (101/101, 95/95, 185/185). Roughly
98,000 from-boot seeds ran in those same three processes and not one of them
red. And the fixed batch the kill proofs run on (`--seeds 500
--nodes 2 --buckets 1 --ops 80 --excludes 100`, fault injection on) is **green**:
382,871 storage-op interleavings, 1,751 acked uploads, `GLOBAL_PAIR` on 500/500
seeds and `DELIST` on 325/500.

**The fix is one word in the reconcile's decision: `first_pass`.** The stamp
records *what* a bucket's indexes were last built against, and nothing else can
falsify that claim — but another node can, and does: it rebuilds the package
under the old config between two reconciles, and the stamp (written by the node
that already restarted) still reads as enforced. So the first reconcile a
process makes against a bucket now re-derives EVERY currently denied name
instead of only the ones the diff turns up; every later pass on that bucket
stays diff-only, so the per-tick warm-copy reconcile is still one GET. The
repair is the ordinary rebuild, which computes truth-minus-denylist from
storage — so a boot cannot delist a live package on the strength of a stale
index half, and the audit's "a failed per-package audit is a no verdict" rule
(`8d8e639`) is untouched. Cost is bounded by the size of the denylist: one
marker and one idempotent rebuild per denied name, per bucket, per process.

Same three lanes, same invocations, at the fix — **0 failures in every one**:

| lane | seeds before | failing before | seeds after | failing after |
|---|---|---|---|---|
| `--nodes 2 --buckets 1 --ops 80 --fail-percent 0` | 131,618 | 101 | 126,039 | **0** |
| `--nodes 3 --buckets 2 --packages 4 --files 3 --ops 120` | 26,653 | 95 | 24,986 | **0** |
| `--rotate` | 37,292 | 185 | 33,709 | **0** |

All 14 repro seeds above red at `d406422` and pass at the fix; they are pinned
in ci.yml, and the shape is pinned durably — where a seed cannot be — by
`worker::tests::a_boot_reconcile_repairs_a_denied_name_a_stale_node_re_listed`
(the re-derive fires over a stamp that already names the exclude, and the
steady-state pass still marks nothing) and by
`tests/test_proxy_delist.py::test_a_rolling_restart_leaves_no_relisted_name_behind`,
which drives two staggered `serve` processes over one store with the audit
switched off, so only the boot reconcile can repair the name. Both kill proofs
still red at their documented K with the lane armed (`global-pair` at 1 seed,
`relist` at 4).

**The lane is on.** It was opt-in pending adjudication and the adjudication is
this section: ci.yml's VOPR smoke runs an armed crash-only row on every merge,
and simulation.yml gains a `delist` row at `--excludes 100`. Both are armed on
every seed of their own row rather than as a share of the existing rows —
`--excludes` is a share of SEEDS, so a partial setting starves `DELIST` below
the 25%-of-seeds floor `--require-reach` gates on (it evaluates 65-69% of armed
seeds), and an armed seed issues extra boot storage ops, which would perturb
schedules the aligned rows have explored for months. At `--excludes 0` —
everywhere else — nothing in this section runs at all.

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
| `wedge` | an object under `_repl/` no sweep recognizes, so the fixpoint the heal phase is bounded to reach does not exist | `STALENESS: the fleet never stopped working` | 1 |
| `slow-repair` | a peer's copy of one live record stolen *after* the heal round's audits, body and sidecar together, with no `_dirty/` marker and no `_repl/` note over the loss — so only the tree diff can find it, one whole backstop cycle later than the run's `t0` debt paid for | `STALENESS: … past its … deadline` (needs ≥2 buckets) | 1 |
| `ordering` | truth grows a file with no `_dirty/` marker and no `_repl/` note ever covering it | `AUDIT_ORDERING:` (repair class 1, per-package path) | 3 |
| `globalindex` | the same unbreadcrumbed mutation, under a package name nobody has published | `AUDIT_ORDERING: … simple/index.* membership` (class 1, global path) | 3 |
| `poison` | a rebuild listed truth *past* the mutation and still wrote a view — and a name set — contradicting it | `… a poisoned derivation consumed the signal` (class 2a) | 3 |
| `blind` | the only op that retired the marker covering the mutation had listed truth before it | `… were all consumed blind` (class 2b) | 3 |
| `race` | two unleased rebuilds: the one that listed *earlier* wrote *last* | `… unleased concurrent rebuild` (class 3; needs `--fail-percent 0`, where any audit repair is a violation) | 5 |
| `stale-delist` | the identical plant, judged at the global index: the staler write *delisted* the name the fresher write listed | `… stale delist:` (class 2, global path — the verified-removes guard makes the product side impossible) | 3 |
| `fallback` | an audit repair the effect history cannot explain at all | `… unexplained drift` (fallback arm) | 3 |
| `freeze-unjustified` | a `.frozen` marker over an acked-**deleted** filename — nothing ever conflicted about it, and the body and view entry are already gone, so no other oracle can see it | `FREEZE_UNJUSTIFIED:` | 3 |
| `freeze-lossy` | a freeze that dropped a body it never quarantined: `.frozen` fleet-wide, one byte-set preserved under `_quarantine/`, the *acked* one overwritten everywhere | `CONSERVATION: … a frozen filename may not lose one` | 4 |
| `demote-lossy` | a mirror→private demotion that dropped the body it fenced instead of moving it under `_quarantine/`, leaving the view still rendering the record its truth no longer holds | `DURABILITY: … a demoted filename may not lose it` | 1 |
| `origin-demoted` | a privately-claimed package's `.origin` walked back to `mirror` on every bucket (sidecars untouched, so the buckets stay converged and the views stay correct) | `ORIGIN_TERMINALITY: … claims … .origin = mirror` | 1 |
| `mirror-served` | the claim stays private but a **live mirror record** stands under it — a new filename cloned from a live record with its sidecar origin rewritten | `ORIGIN_TERMINALITY: … still serves … as live mirror truth` | 3 |
| `split` | a byte conflict the merge never resolved: bucket 1 acks and keeps a second, same-length byte-set under a filename bucket 0 still serves its own for | `DURABILITY: … never left split` (needs ≥2 buckets) | 4 |
| `attest` | an acked artifact's sidecar re-points at a digest no body has, fleet-wide, with every view re-pointed to match — the bytes, the buckets and the re-render all stay healthy | `SELF_CONSISTENCY: bucket … serves … while its own sidecar publishes` | 4 |
| `global-pair` | the two halves of the global index pulled apart at the pre-audit check-point: `simple/index.html` names a package `simple/index.json` does not, fleet-wide | `GLOBAL_PAIR: bucket … do not name the same packages` | 1 |
| `relist` | a fully-denied package put back into both global halves and given a per-package view again, on every bucket that still stores its artifacts (needs `--excludes 100`, which is what configures a denylist at all) | `DELIST: bucket … still lists the excluded package` | 4 |

**`global-pair` and `relist` are the delist lane's legs** (see *The delist lane*
above). They are two different claims and neither can stand in for the other:
`global-pair` is about the two renderings of one name set agreeing with *each
other* at a check-point no audit has reached, and it plants an identical tear on
every bucket so CONVERGENCE stays quiet and the JSON's name set — the only half
`agreement_projection` compares — never moves, leaving GLOBAL_PAIR alone holding
it. `relist` is about the delisting contract itself and moves both halves
together, so GLOBAL_PAIR stays quiet and DELIST is what reds; VERIFY reds
alongside it and cannot not, since a view over a package with nothing renderable
is an orphan view by its own reckoning, which is why the leg's expected text
names DELIST. `relist` is refused outright without `--excludes` rather than left
inert: with no denylist there is nothing delisted to put back, and the leg would
report a 0% kill rate for an oracle that is perfectly alive. `global-pair` kills
on **every** seed measured (8 windows of 6 from seeds 1, 101, … 701, first red at
seed 1 in all of them); `relist`'s plant is inert on a seed whose workload left
the denied package with no stored artifact, so its K is the worst first-red depth
over those same windows.

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
checked is still a gate — which is why p is no longer carried by this table
alone. Every `--break` run prints the rate it just measured (`vopr: --break
<name> killed N/M seeds`), so the CI kill-proof log is the live record and this
table is its snapshot. To refresh a row, widen the draw and read the same line:
`vopr --seeds 2000 --break <name> <that row's flags>`.

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
`origin-demoted` 97.4% (K=2); `resurrect` 88.0% (K=3);
`freeze-unjustified` 68.9% (K=6, 1378/2000 — down from 89.4% when the
attestation learned to see a contested fence-refused body, see *Debris stops
being debris when a writer loses to it*; its own gate leg is single-bucket and
unchanged);
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
and not a default. Each body is hashed **as it streams** (`stored_sha256`), so
memory is a read buffer per artifact in flight and the fan-out (`DEEP_CONCURRENCY`
files × `PACKAGE_CONCURRENCY` packages) never multiplies by object size — loading
bodies whole would put the ceiling at 16 × 16 × the largest wheel in a store whose
contents pypiron does not choose. It reports `body-mismatch`.

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
  covers it. The claim has a specific shape — "*the harness models something
  stronger than the product ships*" — and it must name what: for class 3, the
  fleet-wide `tick_lock` versus a lease with no fencing token.
- **product-unreachable** — a *product rule* forbids the state, so it cannot
  occur in production either. The guard is a watch on a rule that could one day
  be relaxed. This is the only one of the three that licenses "cannot happen",
  and it needs a named rule, not a run of zeros.

The two zeros are not interchangeable, and confusing them in the *other*
direction is how a live guard gets deleted. A harness-unreachable arm covers a
state the product can enter; deleting it deletes the diagnosis, not the state.
A product-unreachable arm covers a state a named rule forbids; deleting it is
safe exactly until the rule moves. Only the second is ever a candidate for
removal, and only alongside the rule.

The distinction is load-bearing in both directions. `TOMBSTONE_MONOTONICITY` is
product-unreachable and names its rule (`publish_record`'s tombstone fence), so
its zero is an argument. The classifier's class-1/2a/2b arms are **workload-**
unreachable — no rule forbids them, and one of them (class 1, seed 268) was
briefly workload-*reachable* under `--partition` before `df3db2d` closed it.
Those arms' `EXPECTED_ZERO` strings in `examples/vopr.rs` used to read
`product-unreachable` while justifying themselves with "no class-1 has ever been
produced" — an observation, not a rule. They now read `workload-unreachable`,
matching the table below.

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
| STALENESS | workload-reachable | both arms: any undrainable breadcrumb stops the fleet settling, and any divergence the fast path cannot see costs a backstop cycle. The fast path has simply always drained inside the deadline | `wedge` (never settles), `slow-repair` (past the deadline) |
| ACK_TOTALITY | workload-reachable, and *observed* — including where it is fatal; green now | a reported statistic under fault injection (4,691 misses in 44,648 partitioned seeds); crash-only, where it *is* fatal, it produced 8 failing seeds in 46,660 partitioned, first `--seed 9504440`, and 0 in 276,998 once `4bb9cb8` stopped `decide` calling a demotion fence a peer had never seen an agreement. That row used to read "has never produced one", which was true only of the aligned schedule | `fanout` |
| DETERMINISM | workload-reachable | any nondeterminism downstream of the op sequence reds it | `rerun` |
| TOMBSTONE_MONOTONICITY | **product-unreachable** | `publish_record`'s tombstone fence rejects re-publishing a deleted filename, so no ack can follow a `204` — 150k wide seeds (795k acked uploads, 251M interleavings) produced zero, and could not have produced one | `resurrect` |
| classifier TEST 1 (both analyses) | **workload-unreachable** — but *witnessed once*, under `--partition` | seed 268 produced a real class-1 (a truth mutation no breadcrumb covered) before `df3db2d`; it is green now and 0 audit repairs of any class appear in the 489,901 seeds measured at `783d423`. Nothing forbids another, so this is a coverage statement, not a rule | `ordering`, `globalindex` |
| classifier/**global** FALLBACK | **workload-unreachable** — but *witnessed*, and recently | `--seed 60000037578 --rotate` reds `[class 2] … unexplained global-index drift for vopr-delta — conservatively premature-consumption` against the tree at `c1b66df`; so does `--seed 66000074673 --nodes 3 --buckets 2 --packages 1 --files 1 --ops 40 --fail-percent 3 --partition 100`. Both are the non-atomic global-index pair (`AUDIT_REPAIRED_VIEWS`), and both are green at `8e5916f` once `c9b2e32` landed. Verified one seed at a time on both trees, not inferred | `fallback` |
| classifier/**global** TEST 2a poisoned | **workload-unreachable** — but *witnessed* | `--seed 61000246528 --rotate --partition 100` reds `[class 2] … op 87 rebuilt vopr-gamma from truth@598 and wrote the global index@607 claiming it present, yet the audit had to flip that membership — update_global_index consumed the signal without applying it` at `c1b66df`; green at `8e5916f`. That is `R::GlobalPoisoned`, the 2a arm of `analyze_global` | `poison` |
| classifier/pkg TEST 2a / 2b / FALLBACK, classifier/global TEST 2b | **workload-unreachable**, never witnessed | each needs the tier-3 audit to have repaired a view, and the three witnesses above are all the *global* analysis — a membership flip, not a per-package render. No product schedule has yet driven the per-package arms, nor global 2b. Class 2 on the per-package path *has* been witnessed historically (seed 1067836, since fixed) | `poison`, `blind`, `fallback` |
| classifier TEST 3 (both analyses) | **harness-unreachable** — and *measured* to be exactly that | the simulator's `tick_lock` serializes every rebuild, which is **stronger than the lease production ships**; remove it from the leader audit and the arm reaches on real product behavior (see the next section) | `race` (planted history) |

`resurrect` proves the *oracle* is sound even though the *product* cannot reach
the state, which is the honest status of that guard: mirror filenames are
re-fillable by design, so the day a legal resurrection path lands it is already
watched. An unreachable-but-sound guard is legitimate; an unproven one is not.

##### Class 3 is harness-unreachable, not dead — the measurement

Class 3 is unreachable on a **much weaker claim** than `resurrect`'s, and this
is the distinction the table exists to keep. It has been proposed twice that a
repair arm which never fires under the production lease, whose kill proof plants
a fabricated history, is dead code wearing a test. **The premise is false: the
production lease does not serialize rebuilds.** Three facts, all in the source:

- `src/lease.rs` is a TTL + heartbeat with **no fencing token**, by design and
  in its own module doc. `is_leader()` is a point-in-time read;
  `rebuild_package_indexes` never revalidates it and the view PUT carries no
  term. A rebuild that outlives the TTL runs beside its successor's.
- The **leader's own audit** is a second concurrent rebuilder on one node:
  `run_worker` spawns `audit(...)` on a task and runs `tick(...)` inline in the
  same loop iteration, with nothing mutexing the two. No lease, sloppy or
  otherwise, has any bearing on this one.
- `delete_record` (`src/publish.rs`) calls `rebuild_package_excluding` straight
  from **any node's request handler**, ungated.

And the per-package view write is `put_if_changed` — an unconditional PUT. Only
the *global* index takes an `If-Match`, so a losing per-package writer clobbers
rather than losing. dev/DESIGN.md budgets for the result by name: "a brief
dual-leader window costs at worst duplicate work plus an audit-healed view."
Class 3 *is* that audit-healed view. Delete the arm and the clobber falls
through to FALLBACK, failing the seed as class 2 PREMATURE-CONSUMPTION — a
misdiagnosis that sends triage hunting a signal-loss bug in the marker protocol
that is not there.

The reachability question was then measured, one process, same seed range,
`--rotate --require-reach --start-seed 770000000`:

| harness config | seeds | repairs (ordering / premature / **race**) | failing seeds |
|---|---|---|---|
| baseline (this tree) | 16,937 | 0 / 0 / **0** | 0 |
| leader audit spawned per tick op, **outside** `tick_lock` | 20,838 | 0 / 59 / **1** | 57 |
| leader audit `join!`ed with the tick **inside** `tick_lock` | 29,770 | 0 / 27 / **0** | 27 |

Row 2 is the honest reach: `classifier/global TEST 3 race` executed 107 times on
12 of 20,838 seeds and produced a real class-3 finding from product behavior,
with nothing planted. Row 3 isolates *why* — co-locating both rebuilds under one
lease holder does **not** produce the class-3 shape, so what the arm is really
about is dual leadership, which is precisely what a fencing-token-free lease
permits and what `concurrent_rebuild_without_lease_diverges` covers
exhaustively. Row 1 is the control: the same seeds, unmodified, produce zero
repairs of any class, so rows 2 and 3 are attributable to the added concurrency
and not to the seed range. Earlier notes that "removing `tick_lock` produced zero
repairs over 26k wide seeds" removed the wrong lock: `op_tick`'s lock does not
gate the audit, which is the rebuilder production never serializes.

**Is the row-2 change worth landing? Not as it stands, and not in this change.**
It costs 57 failing seeds per ~21k, it adds a storm-phase op that shifts every
pinned seed's schedule, and 27 of those failures survive into row 3 — a
configuration production runs *unconditionally* on every leader. Those 27 are
class-2 global-index membership repairs, not class-3, which makes them a
candidate **product** defect rather than a harness artifact and a separate
investigation with its own seed census. Reproduce with `--seed 770000370
--rotate` against a tree whose `op_tick` runs `tokio::join!(tick, audit)` inside
the lease. Landing the harness change before that is understood would turn the
nightly lane red for a reason nobody has finished diagnosing.

So class 3 **stays**, and stays honestly labelled: `--break race` closes the
*soundness* question — the arm evaluates and prints its finding, on both
analyses — and the model checker closes the *reachability* question. Its only
exercise in this simulator is a synthetic history, and that is acceptable for
the same reason it is acceptable for `ordering`, `globalindex`, `poison` and
`blind`, which plant too: a planted history is a **mutation test of the
classifier's predicates**, and the classifier reads history, not storage,
precisely because a concurrent-rebuild clobber leaves no storage residue — the
loser's bytes are gone by definition. An argument that the plant disqualifies
class 3 disqualifies four of the five arms with it. Only `fallback` plants
nothing, and only because its subject is the absence of an explanation.

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
  staleness margin: tightest seed agreed 480s after its last write against a 750s deadline (57 marker(s) = 1 fast cycle(s), 13 uncovered = 1 backstop cycle(s)) — 64% spent, 36% spare, at --seed 4522; heal loop peaked at 2/12 rounds, 1/20 drain passes
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

The delist lane added two slots (`GLOBAL_PAIR`, `DELIST`). `GLOBAL_PAIR` is
unconditional and reads 100% of seeds on every profile measured; `DELIST` has no
subject without a denylist, so it carries the same kind of standing excuse a
single-bucket sample gives CONVERGENCE — `expected_zero` says *no denylist
configured — the oracle needs `--excludes`* rather than letting a default run
report a hole.

**What it says today.** Over 330 seconds across all five profiles — 140,334
seeds — every one of the nine invariant oracles executes, on every profile whose
topology admits it: DURABILITY and VISIBILITY 76,257 times on the rotating
profile alone, TOMBSTONE_MONOTONICITY 88,405, VERIFY 32,443, ACK_TOTALITY
51,845, CONVERGENCE 16,284, STALENESS 16,060, CONSERVATION 39,944, DETERMINISM
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
| classifier/pkg TEST 1 | **workload**-unreachable, not product-unreachable: `--partition` reached it once (seed 268, a truth mutation no breadcrumb covered), `df3db2d` closed it, and 0 audit repairs of any class appear in the 489,901 seeds measured at `783d423`. No rule forbids the next one | `--break ordering`, `--break globalindex` |
| classifier/global TEST 1 | same, on global membership | `--break globalindex` |
| classifier/{pkg,global} TEST 3 | *harness*-unreachable: `tick_lock` serializes rebuilds, so two never overlap — a lock **stronger than the lease production ships**, and one that also excludes the leader's own audit task (see the reachability section above, which measures the arm reaching once that gap is closed). Still true under `--partition`: a partitioned node diverges only its **writes**, never its rebuilds, so every tick still takes the one bucket-0 lease | `--break race` |
| classifier/pkg TEST 2a | needs an audit repair whose final writer had listed truth past every mutation | `--break poison` |
| classifier/global TEST 2a | **workload**-unreachable, and it has been *reached*: `--seed 61000246528 --rotate --partition 100` drove it at `c1b66df` off the non-atomic global-index pair; `c9b2e32` closed that and it reads zero again at `8e5916f` | `--break poison` |
| classifier/{pkg,global} TEST 2b | needs an audit repair TEST 1 declined — a covered mutation whose breadcrumbs were all consumed blind | `--break blind` |
| classifier/pkg FALLBACK | reached only by drift no test explains — which would itself be a classifier bug | `--break fallback` |
| classifier/global FALLBACK | same in principle, but *reached twice* at `c1b66df` (seeds 60000037578, 66000074673) by the same non-atomic global-index pair, and zero again at `8e5916f`. Its excuse string carries no unreachability claim, so nothing there needs correcting — but do not read its zero as one | `--break fallback` |

An entry earning its first execution is *news*, not a failure: the run prints
`[now reached — drop it from EXPECTED_ZERO]` beside it, and the entry comes out.
Under `--break` the note reads `[reached under --break]` instead, because
reaching an oracle is what a break is for.

The last line is the **staleness margin**: the seed that spent the largest share
of its own deadline, what it spent it on, and the seed number to reproduce it. A
bound with no observed margin is a bound about to break, so it prints on every
run whether or not anything failed — and it is a share rather than a longest
wait, because the deadline is per-seed and the longest absolute wait is routinely
the seed that was owed the most allowance and used it comfortably. Over 361,987
seeds across eight profiles the tightest anywhere spends **76%** (570 s into a
750 s deadline), and the fast-path-only seeds top out at 210 s inside 300. See
*Bounded staleness* above for the deadline and the census it was measured from.
The same line still carries the heal loop's own peaks against `HEAL_ROUNDS` and
`DRAIN_PASSES` — they only bound the driver's loop now, but a peak creeping
toward either is the earliest sign the deadline is next.

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

### Shrinking a failing seed (`--shrink`)

Detection stopped being the bottleneck when the soak fleet went always-on;
reading the failure is. The same defect is unreadable at 200 ops and obvious at
4, and that reduction used to be a human editing flags by hand, one rerun at a
time. `--shrink` does it: on the first failing seed it searches for the smallest
configuration that still fails **the same way** and prints it as one pasteable
command.

```
cargo run --release --example vopr -- --seed 1 --nodes 2 --buckets 1 --ops 80 --break durability --shrink
...
vopr: --shrink ran 34 simulations in 10.8ms — smallest configuration still failing ["DURABILITY", "SELF_CONSISTENCY"]:
shrunk: cargo run --release --example vopr -- --seed 1 --nodes 1 --buckets 1 --packages 1 \
        --files 1 --ops 5 --fail-percent 0 --weights 40,0,0,0,0,0,0,0 --break durability
ops the failure needs: publish
```

**"The same way" is the set of violation KINDS** — the token each violation
string names itself with, before its first colon. A non-zero exit is not enough:
a smaller world happily fails for an unrelated reason, and a minimum reported
under the wrong bug sends the reader after a defect that was never there. The
rest of each message carries bucket numbers, filenames and whole dumps, which
differ on every candidate, so the kind is as fine as a stable signature can get
— two different bugs that share a kind (`DURABILITY` has four producers) can
still be confused, which is why the shrunk command is a starting point for
reading, not a verdict.

It descends `--ops`, `--buckets`, `--nodes`, `--packages`, `--files`,
`--fail-percent`, `--partition` and the op-class mix, repeating to a fixpoint.
Each axis is scanned smallest-first, so the first value that still fails IS that
axis's minimum given the rest: failure is not monotone in any of these — a
bigger world is not a superset of a smaller one, it is a *different schedule* —
so a binary search would report whichever floor it happened to land on.
`--partition` is tried as a boolean, because for one seed it is a threshold on a
single draw from a dedicated stream: every percent above that draw is the same
world, every percent below is the aligned one. The op mix descends by zeroing
whole classes, which answers the question a reader actually asks — *which
operations does this bug need?* — and `--weights a,b,…` exists so that answer
pastes back. Everything it searches is a dimension the harness already had, so
the result is a command anyone can run and there is no shrink-only replay format
to maintain.

**It does not reduce the op sequence, on purpose.** Skipping an individual op
mid-run cannot be expressed as a profile, and the fault stream is drawn per
storage op from one shared rng (`FaultPlan::admit`), so removing an op shifts
every fault after it — a skip-mask reducer needs its own world dimension, its own
replay flag, and a guard in the chaos loop. The measurements below say what that
would buy: on the mutation corpus the profile axes already reach 1–10 ops, and on
the two real product bugs *nothing but the op count moved at all*, because a rare
schedule is exactly the kind that any perturbation destroys. A sequence reducer
faces the same fragility, at ten times the code.

Two more rules, both about not lying: a determinism violation is decided by the
recheck in `main`, not by an oracle, so `--shrink` refuses to search one (it says
so and shrinks nothing). And it stops the run at the first failing seed — the
search pushes hundreds of extra simulations through the process-wide reach and
merge meters, and a soak that kept going would print a coverage table those
probes wrote.

**Measured, on the oracle kill proofs.** Every `--break` leg in ci.yml, started
from the flags that step reds it with (`--seeds 6`, so seed 1 is the one shrunk),
at `5c2395e`:

| start | `--break` | shrunk to | probes | wall |
|---|---|---|---|---|
| `--nodes 2 --buckets 1 --ops 80` | `view`, `wedge` | 1 node, 1 pkg, 1 file, **1 op**, nudge only | 12 | 2.5–19 ms |
| `--nodes 2 --buckets 2 --ops 80` | `diverge` | 1 node, 2 buckets, **1 op**, nudge only | 14 | 4.7 ms |
| `--nodes 2 --buckets 1 --ops 80` | `ordering`, `globalindex`, `poison`, `blind`, `fallback` | 1 node, 1 pkg, 1 file, **3 ops**, publish only | 26 | 8.6–11 ms |
| `--nodes 2 --buckets 1 --ops 80 --fail-percent 0` | `race` | 1 node, 1 pkg, 1 file, **3 ops**, publish only | 22 | 7.7 ms |
| `--nodes 2 --buckets 1 --ops 80` | `durability`, `visibility`, `conserve`, `freeze-lossy`, `demote-lossy`, `origin-demoted`, `mirror-served`, `attest` | 1 node, 1 pkg, 1 file, **5 ops**, publish only | 34 | 11–32 ms |
| `--nodes 2 --buckets 2 --ops 80 --fail-percent 0` | `fanout` | 1 node, 2 buckets, **7 ops**, publish only | 41 | 24 ms |
| `--nodes 2 --buckets 1 --ops 80` | `resurrect`, `freeze-unjustified` | 1 node, 1 pkg, 1 file, **10 ops**, no tick | 58 | 24–36 ms |
| `--nodes 2 --buckets 2 --ops 80` | `split` | 1 node, 2 buckets, **10 ops**, publish only | 81 | 71 ms |

Every one of those commands was pasted back and reds with the same kinds.

**And on real product bugs, where it is honest about its limits.** With
`c9b2e32`'s fix reverted, the two `--rotate` seeds its commit message names go
red again; `--shrink` cut the op count and could not move a single other axis:

| seed | as found | shrunk to | probes | wall |
|---|---|---|---|---|
| 60000037578 (`--rotate`) | 3 nodes, 1 bucket, 4 pkgs, 1 file, 200 ops, crash-only | same, **140 ops** | 305 | 218 ms |
| 61000075134 (`--rotate`) | 2 nodes, 1 bucket, 3 pkgs, 2 files, 120 ops, crash-only | same, **114 ops** | 251 | 214 ms |

That is the shape of the tool's value: it is worth hundreds of reruns on any
failure, it converts a `--rotate` failure into an explicit command (rotation
derives the mix from the seed; a shrunk mix is not one rotation would draw, so
the result always prints in fixed-flag form), and on a knife-edge schedule it
tells you *that* the world is already minimal instead of leaving you to find out
by hand. Seed 86001009016 with `8e5916f` reverted is the extreme case: the
hand-shrunk profile in ci.yml (`--packages 1 --files 1 --ops 26 --fail-percent
1`) survives 38 probes with nothing to give — the tool matched the human's answer
and proved it minimal on every axis in 34 ms.

A shrink needs a *failing* world, and a seed is a recipe rather than an ordering
(see `5c2395e`): of the three seeds open at the start of this campaign,
86001009016 still reproduces against its own pre-fix product code, while
62000150551 and 86001076473 no longer red anywhere — 864 and 2,592 profiles
respectively at HEAD, plus 256 each against `8e5916f` reverted, plus 92,000
seeds of pre-fix soak on the narrow lanes that class of bug is found on. Their
bugs are closed and their schedules have moved; there is nothing left to shrink.

**Zero cost when unused, proven the way this document demands.** A search flag
that perturbed the schedule would shrink a world that never happened. Nine
canonical profiles — the five gated rows, `--rotate`, `--rotate --partition 100`,
the partitioned multi-bucket row and the harness default — ran 500 seeds each
against the binary immediately before and after the feature, at `5c2395e`:

| checked | result |
|---|---|
| 9 × 500 seeds: interleavings, acked uploads, ack-totality misses, audit repairs by class, the whole 23-row reach table and 11-row merge table | identical to the character, once the wall clock is normalized away |
| 9 × `VOPR_TRACE=1` op-interleaving dumps (1,149–3,822 ops each) | byte-identical |

```
VOPR_TRACE=1 VOPR_TRACE_FILE=/tmp/a cargo run --release --example vopr -- \
  --seed 7774 --nodes 3 --buckets 2 --ops 160 --packages 6 --files 2 --fail-percent 0
# ...against a binary built before the feature: cmp is silent, 2,301 ops.
```

The reason it is inert is structural, not measured: nothing in the search runs
until a seed has already failed and been reported, every probe is `run_once` on
the same seed, and the only edits to existing code are a flag, an `if` in the
failure branch, and a `--weights` suffix that appears solely when the mix is not
`DEFAULT_OP_WEIGHTS` — which no command outside a shrink result has.

### Exhaustive fault depth 1 (`--sweep-faults`)

Random injection is a 3%-per-op coin flip compounded over a whole schedule, so
hitting *the* op with *the* fate while the rest of the interleaving lines up is a
lottery. It is a lottery the soak fleet keeps winning eventually and expensively:
one recent depth-1 bug cost ~16 million seeds, another ~60 million. `--sweep-faults`
stops buying tickets. Hold one schedule fixed, then run it once per (op, fate)
pair — every chaos-phase storage op forced to fail, then every one forced to
crash the node that issued it, every other op clean. Every oracle runs on every
such run.

```
cargo run --release --example vopr -- --seed 7384 --nodes 2 --buckets 1 --ops 80 --sweep-faults
vopr: sweep seed 7384 — 310 chaos ops x 2 fates, 621 runs, 620/620 forced faults fired, no violations, 541ms
vopr: 1 seeds swept at fault depth 1, 310 ops swept, 621 runs, 0 violations, in 541ms — every single-fault position held
```

Half a second for the entire single-fault space of that schedule. The loop is
in-process — a process per run would cost 190 s for the same sweep — and
`--seeds`/`--start-seed`/`--forever`/`--max-secs` mean what they mean everywhere
else, so a timeboxed depth-1 soak is `--rotate --max-secs 120 --sweep-faults`
(17 rotating schedules, 38,793 runs, measured). The flags its own loop *cannot*
honour are refused rather than swallowed — `--recheck-every`, `--require-reach`,
`--shrink`, a nonzero `--fail-percent`, and `--break rerun`, whose oracle is the
determinism recheck the sweep never runs. A `--require-reach` lane copied off
`simulation.yml` onto a sweep would otherwise be a gate that cannot fail.

`fired` is the honest denominator: a fault cannot be forced onto an op whose node
is already down, and a sweep that forced nothing verified nothing. Every profile
measured so far reports 100%.

**A swept run is depth 1 absolutely, not relatively.** The baseline drops
`--fail-percent` to 0 *and* stops the driver from scheduling crashes, so the
forced op is the run's only fault and the reported `(seed, k, fate)` is the whole
counterexample. The baseline runs first and is checked on its own: a schedule
that reds without any fault is reported as that, not as a sweep finding — and its
reproduce line ends in `--sweep-faults`, because depth-1-with-nothing-forced has
no other spelling and a line without it reruns a world where the driver schedules
its own crashes again (measured on seed 7384: 709 storage-op interleavings
against 604). Faults
are confined to the chaos phase — `admit` stops overriding once the heal phase
starts — because a fault during heal is a fleet that was never given the chance
to converge, and the convergence oracles would red for the harness's reason
rather than the product's.

**The alarm has been watched going red.** `81b62a3` (a torn global index survived
every tick when its JSON was absent) was reverted in the working tree and the
sweep run against ordinary small schedules:

| | |
|---|---|
| the bug's known random-search repro | `--seed 13792606396100784374 --rotate`, found after millions of seeds |
| what the sweep needed | 230 swept schedules of `--nodes 3 --buckets 1 --packages 1 --files 2 --ops 120 --weights 7,21,18,10,4,8,4,8`, ~60 s on one core |
| what it printed | `SWEEP seed 329 op 144 fate fail FAILED` — `AUDIT_PREMATURE_CONSUMPTION: bucket0:simple/index.json before=None after=Some({"projects":[]})` |
| the same 500 seeds, run ordinarily at `--fail-percent 3` and `0` | all green |
| the same 250-seed range with the fix restored | 113,770 runs, 0 violations, 61.6 s |

The reproduce line it prints (`… --fail-percent 0 … --force-fault 144:fail`) is
one simulation, not a re-sweep: `--force-fault <op>:<fail|crash>` reruns exactly
that world, and it implies the depth-1 world (no random failures, no scheduled
crashes) on its own, which is what lets a rotating repro carry it — `--rotate`
refuses `--fail-percent`, so the line has no other way to say it.

**The trap this mode had to disarm.** `fail_percent == 0` used to be read as
"crash-only", and two oracles tighten on it: an ack-totality miss becomes a hard
violation, and *any* audit view repair does. A swept run is at zero percent and
still injects one failure, so keying those gates on the percent would report the
sweep's own fault as a bug on every forced-`Fail` run. They are keyed on
`availability_faults` instead — `fail_percent > 0 || forcing a Fail` — pinned by
`a_forced_failure_relaxes_the_crash_only_oracles_and_a_forced_crash_does_not`. A
forced `Crash` relaxes nothing: crashes are exactly what the crash-only contract
contemplates, which makes the crash lane the stricter half of the sweep.

Name the price rather than assume it away. On the `Fail` lane an ack-totality
miss and a class-3 (concurrent-race) audit repair become statistics instead of
violations — the same two things the ordinary `--fail-percent 3` lane has always
given up, for the same reason: under an injected storage failure a note write can
itself fail. `AUDIT_ORDERING` and `AUDIT_PREMATURE_CONSUMPTION` still red, which
is how the `81b62a3` revert was caught. The Crash lane gives up nothing. The
false-positive direction was hunted and came back empty: 1,894 sampled
forced-`Fail` positions across four profiles produced zero runs that trip either
relaxed counter, so on today's product the relaxation costs no detection — it is
insurance against the day one of those counters fires under a swept fault.

**What depth 1 cannot reach.** A bug whose *precondition* is itself a fault is
depth 2 in this mode's terms and will not appear. `81b62a3` sits right at the
edge: its victim is a bystander node holding a cold global-index cache and a
delta that dedups to nothing, which a clean schedule produces only sometimes —
hence 230 schedules rather than one. Five clean profiles (7384, 19026, 47843,
1784486481, 9900094440, plus `--rotate`) sweep to zero violations, so the
relaxation above is not papering over anything.

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
| Every PR (CI) | fmt, clippy `-D warnings`, Rust unit, model checking (bounded configs) + conformance suites, VOPR smoke (fault + crash-only + rotating + delist profiles), blackbox on disk + S3 (MinIO) + Azure (Azurite), `cargo-audit`, fuzz-target build smoke |
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
