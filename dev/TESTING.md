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
shape without rerunning. A *non*-rotating run (explicit `--nodes/--buckets/…`)
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
  index-listed on every bucket;
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
  delete or freeze;
- **liveness** — the fleet quiesces within the drain budget.

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
exactly that. Every break lives in the harness — no product-code hook ships in
the binary — and writes raw `SimStorage`, never a `FaultView`, so it perturbs
storage and never the schedule.

| `--break`   | the injected defect | must red | K |
|---|---|---|---|
| `view`      | a torn view write (the last byte never landed), truth untouched | `VERIFY: … stale-view` | 1 |
| `fanout`    | peer bucket 1 blackholes the chaos phase *and* the `_repl/1/` note owing it is dropped | `ACK_TOTALITY:` (needs `--fail-percent 0`, where it is the hard gate) | 1 |
| `rerun`     | a seed's second execution ends in a different world off an identical op trace | `DETERMINISM VIOLATION … same calls, different bytes` (needs `--recheck-every 1`) | 1 |
| `resurrect` | an acked-deleted artifact's bytes come back with its tombstone gone | `TOMBSTONE_MONOTONICITY:` | 3 |
| `ordering`  | truth grows a file with no `_dirty/` marker and no `_repl/` note ever covering it | `AUDIT_ORDERING:` (repair class 1, per-package path) | 3 |
| `globalindex` | truth grows a whole *new package* with no `_dirty/` marker and no `_repl/` note ever covering it | `AUDIT_ORDERING: … simple/index.* membership` (repair class 1, global path) | 3 |

`globalindex` exists because `ordering` cannot reach the global analysis: it
grows a package that already exists, so the global name set never changes and
only `rebuild_package`'s view is repaired. Landing the same unbreadcrumbed
mutation under a name nobody has published makes membership the thing the audit
has to fix, which is the only input that exercises the `GlobalWrite` path — and
it reds with *both* findings, one per subsystem, which is the split working.

K is how many fresh seeds the break needs to red with ≥99.8% confidence, and the
table's K is for the **pinned CI flags**, which are non-rotating. Measured over
seeds 1–300 at the leader-rotation commit: `view` 300/300, `fanout` 300/300,
`rerun` 300/300, `resurrect` 281/300, `ordering` 264/300, `globalindex` 264/300.
`view`, `fanout` and `rerun` red on every seed; `resurrect`, `ordering` and
`globalindex` need a run that actually produced the state they corrupt — an
acked `204`, and a live artifact+sidecar pair — so a schedule that ended with
every file tombstoned leaves them inert. CI samples nothing: the seed range is
pinned and the simulator is deterministic. Quote the seed range with any future
re-measurement; leader rotation moved the heal-phase schedule, so counts taken
over an unstated range are not comparable across commits (K, which is what the
gate depends on, did not move).

Under `--rotate` the same breaks red at different rates, because the rotating
profile varies the *topology* the break needs. Over seeds 1–50,000 (`view` and
`rerun` over the first 12,000): `view` and `rerun` red on every seed (K=1),
`resurrect` 43,796/50,000 (K=3), `ordering` 35,160/50,000 (K=6), `fanout`
16,165/50,000 (K=16). `fanout`'s drop is not a weaker oracle — it is arithmetic:
the break needs ≥2 buckets *and* `--fail-percent 0`, and rotation supplies both
on about a third of seeds. Take a rotating K to the digit only off a five-figure
sample: three of these rates land within a percentage point of a K boundary, so
the earlier 300-seed measurement read `ordering` as 5 and `fanout` as 18, and
`resurrect` (0.876 against a 0.874 boundary) and `fanout` are still inside
sampling noise of the next K up — provision 4 and 17 if you re-pin a rotating
range. Keep the gate on the pinned non-rotating flags; that is what makes it a
gate rather than a sample.

`--break ordering` is still the only class-**1** input the classifier has ever
been handed; the product has never produced one. Class 2 is no longer synthetic
— seed 1067836 above is the real thing.

Off by default and provably free: every injection point is a comparison against
`Break::None` that draws no rng, consumes no op-sequence number and records no
trace event. Prove it the way this document already demands of a new oracle —
`VOPR_TRACE_FILE` captured before and after must be byte-identical, and the
pinned baselines must reproduce to the digit.

**Reachable by the workload, or sound but unreachable?** Four of the five are
reachable: the workload produces the states that red them. TOMBSTONE
MONOTONICITY does not — `publish_record`'s tombstone fence rejects a re-publish
of a deleted filename, so no ack can follow a `204` and the invariant cannot
fire on today's workload at any seed count. Widening the workload did not change
that and could not have: the fence is a product rule, not a shortage of
filenames, and 150k wide seeds (795k acked uploads, 251M interleavings) produced
zero. `--break resurrect` proves the *oracle* is sound even though the *product*
cannot reach the state, which is the honest status of that guard: mirror
filenames are re-fillable by design, so the day a legal resurrection path lands
it is already watched. An unreachable-but-sound guard is legitimate; an unproven
one is not.

Class 3 (concurrent-race) is unreachable too, but on a **much weaker claim**, and
conflating the two is the mistake this paragraph exists to prevent. TOMBSTONE
MONOTONICITY is *product*-unreachable: a product rule forbids the state, so it is
unreachable in production as well, and the guard is a standing watch on a rule
that could one day be relaxed. Class 3 is only *harness*-unreachable: the
simulator's `tick_lock` serializes every rebuild (each pin takes bucket 0's lock)
to stand in for the bucket lease, so two rebuilds never overlap by construction.
Production's lease is sloppy on purpose — `src/lease.rs` is a TTL + heartbeat
with no fencing, because rebuilds are idempotent — so dual leadership, and with
it the race, *is* reachable there; it is covered by
`concurrent_rebuild_without_lease_diverges` in `tests/model_event_protocol.rs`,
not by this simulator. Removing `tick_lock` still produced zero repairs over 26k
wide seeds, and so did truncating the heal phase's drain budget until two thirds
of seeds failed on other oracles — the marker/tick/sweep/reconcile fast path
converges views without the audit on essentially every schedule this simulator
can build. So the honest status is "this harness cannot stage the race", not "the
product does not have it", which is why the class-2 hit above matters so much.

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
| Nightly | coverage-guided fuzzing (all six targets); deterministic simulation at volume + deep model configs (simulation.yml, counters published per run) |
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
