# Moonshot: the package server that can't lie

Author: Claude Fable 5, 2026-07-08. Status: rungs 1 and 2 shipped 2026-07-17
(see the per-rung notes below); rung 3 remains a proposal pending its design
decision against DESIGN.md.

## The frame

Everything pypiron ships today for trust — chaos suites, fuzzing, audits, the
corpus check — is *sampling*: we tried an enormous number of failure schedules
and none of them broke it. That already clears the industry bar. The moonshot
is a category change: stop sampling and start **proving**, then make the
running server **cryptographically incapable of hiding corruption**. Every
registry on the market, PyPI included, ultimately says "trust our operations."
The endgame here is a server that doesn't ask for trust at all.

Three rungs. Each is independently shippable, each strictly harder than
anything in this codebase today, and they stack: the proof makes the simulator
trustworthy, and tamper-evidence makes the proof observable in production.

## Rung 1 — Deterministic simulation testing (the VOPR)

FoundationDB and TigerBeetle's technique, never applied to a package server:
carve deterministic seams (simulated clock, disk, network) under the event
protocol and storage layer, then run an entire multi-node fleet inside a
single-threaded simulator with a seeded RNG.

- **What it replaces:** the chaos suites sample dozens of crash schedules per
  run. The simulator explores *millions per night* — crashes, reorderings,
  partitions, disk faults — because simulated time costs nothing.
- **The property that matters:** every failure reproduces exactly from an
  8-byte seed. No flaky repro, no "couldn't trigger it again."
- **Why it's hard (and therefore worth it):** tokio code resists deterministic
  seams. Realistically this means isolating the protocol + storage state
  machines from the async runtime behind narrow traits — an architectural
  forcing function that improves the code even if the simulator never ships.
- **Exit criterion:** a nightly `vopr` job whose seed count and
  interleavings-explored counter are published like the fuzz corpus is.
- **Shipped 2026-07-17:** `examples/vopr.rs` over the seams carved for it —
  `src/clock.rs` (simulated wall clock + deterministic nonces), `src/sim.rs`
  (virtually-clocked in-memory storage with object-store CAS semantics), the
  publish/delete/yank protocol cores extracted from the HTTP handlers, and
  deterministic work ordering in the worker and reconciler. Smoke on every PR
  (ci.yml), volume nightly with published counters
  (.github/workflows/simulation.yml). Details: dev/TESTING.md
  §"Deterministic simulation (the VOPR)".

## Rung 2 — A machine-checked convergence proof

Model the multi-node event protocol in
[stateright](https://github.com/stateright/stateright) (a Rust model checker,
so the model lives next to the code it describes) and exhaustively verify the
invariants the chaos tests spot-check:

- every acknowledged write survives any crash/restart interleaving;
- replicas converge to identical indexes;
- no interleaving loses, forks, or resurrects an index entry.

Bind the model to the implementation with conformance tests (same transition
functions driven by the same inputs) so the proof cannot silently rot as the
code evolves. The fuzz targets' `#[path]` import trick suggests the protocol
logic is already close to extractable; the spike below finds out.

- **What it changes:** the README claim upgrades from "we killed it a lot and
  it always converged" to "convergence is machine-checked for *all*
  interleavings up to depth N — here is the model."
- **Exit criterion:** stateright model checked in CI; a conformance suite
  binding model transitions to `src/` behavior; one README sentence.
- **Shipped 2026-07-17:** two models in `tests/model_event_protocol.rs` and
  `tests/model_replication.rs`, transitions bound to the real
  `consumable_dirty_work` and `decide`; conformance suites
  (`tests/conformance_tick.rs`, `conformance_execute_matches_model`) drive the
  real `tick`/`execute` against in-memory buckets and require the model's
  predictions exactly. Bounded configs gate every merge via `cargo test`; deep
  configs run nightly. The spike's verdict on the protocol: clean — the merge
  algebra and marker selection were already pure functions, which is why the
  models could bind to them directly. README sentence: §"Tested like your
  supply chain depends on it".

## Rung 3 — Tamper-evidence, not tamper-resistance

Certificate-transparency for the package index: a Merkle tree over the storage
truth, a signed tree head published on every write, inclusion proofs served
beside artifacts, an append-only history any client can audit.

- **What it means for a user (or an agent):** you can verify that what you
  installed is in the operator's committed history, and that the history never
  rewrote itself — *without trusting the operator*. `verify-index` generalized
  from "the operator can check" to "every client can check, cryptographically."
- **What it is not:** TUF. The roadmap rejected PEP 458 for good reasons (PyPI
  never shipped it; the standard is effectively dead). This borrows the CT/
  sigstore-rekor lineage instead: no key ceremonies for publishers, one signing
  key for the operator, verification is a hash walk.
- **Design tension, stated honestly:** this adds a new artifact family to the
  storage contract (tree heads, proof material), which brushes against the
  "no invented storage-tree or sidecar variants" rule in dev/DESIGN.md. It is
  the one rung that needs a conviction-level design decision, not just
  approval. If the contract can't absorb it cleanly, this rung waits.
- **Force multiplier:** the corruption bounty stops relying on our
  adjudication — a winning claim is a Merkle inconsistency anyone can check.

## Satellites (cheap, compounding, already in motion)

- **Corruption bounty** — a standing public offer: make the live mirror serve a
  byte-wrong artifact or a divergent index, win $X. The chaos guarantee turned
  into a falsifiable stake. One docs paragraph once a number is chosen.
- **90-day public soak** — the public mirror as accountable production:
  published uptime, Prometheus dashboards, hourly `verify-index` on a status
  page, honest incident write-ups. Compresses "no fleet history" into "watch
  the history accumulate."
- **Traffic replay bench** (`bench/replay/`) — real PyPI download events
  replayed at multiples of real pace; replaces the capacity arithmetic with a
  measurement. Built; AWS-rig run pending.
- **Attestation verification** — verify PEP 740 provenance at ingest instead of
  relaying it blind; let operators require it for private uploads. Not on the
  roadmap's rejected list (that was TUF). Needs a design pass.

## Sequencing

1. **Spike rung 2 first** (days, not weeks): attempt a stateright model of the
   event protocol. Cheapest way to learn whether the protocol is as clean as
   we believe. If the model is ugly, that finding alone is worth the spike.
2. Rung 1 next — the seams the simulator needs are the seams the conformance
   tests want anyway; rung 2's spike tells us where they go.
3. Rung 3 last, gated on a real design decision against DESIGN.md.
4. Satellites proceed in parallel; none block on the rungs.

## Why

"Fastest PyPI server" is a crown someone else can take with a better
benchmark. "Verifiable infrastructure — proven over all interleavings,
auditable by every client, with money riding on it" is a moat made of work
nobody else in this ecosystem has been willing to do. And it lands at exactly
the moment the audience changes: agents choosing infrastructure don't read
testimonials — they check proofs.
