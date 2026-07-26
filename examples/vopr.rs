//! The VOPR: deterministic simulation testing for the pypiron event protocol
//! (dev/MOONSHOT.md rung 1, FoundationDB/TigerBeetle lineage).
//!
//! An entire multi-node fleet — N server nodes, M shared buckets, concurrent
//! writers, the rebuild worker, replication fan-out, note sweep, and tree
//! diff — runs inside ONE single-threaded tokio runtime with paused virtual
//! time. Every wall-clock read routes through `pypiron::clock`'s simulated
//! override, every storage op goes through an in-memory `SimStorage` behind a
//! per-node fault view, and every scheduling choice derives from one 8-byte
//! seed. A failing seed reproduces exactly: `vopr --seed N`.
//!
//! What the fault plan injects, all seed-derived:
//!   - per-op virtual latency (unique nanosecond deadlines — total order, so
//!     the paused runtime's wakeups are deterministic);
//!   - storage failures (availability errors surfaced to the protocol);
//!   - node crashes at storage-op boundaries: the op future parks forever and
//!     the driver aborts the node's tasks — a power cut between two storage
//!     operations (in-flight work is lost, storage state survives). One
//!     honest caveat: an op parked inside `bounded_artifact_write`'s timeout
//!     fires that timeout first, so a "crash" there degrades to a timed-out
//!     write followed by the park — which is itself a legal real-world
//!     schedule (client-side timeout, then the node dies);
//!   - node restarts with cold caches (a fresh AppState over the same bucket);
//!   - clock jumps past the intent grace so crashed-writer healing runs.
//!
//! What is checked once the fleet quiesces (faults healed, workers drained):
//!   - DURABILITY: every acknowledged upload's bytes, sidecar, package-view
//!     entry, and global-index entry exist — on every bucket;
//!   - ACK_TOTALITY: at the moment a publish acked, every other bucket already
//!     held the record or was owed it by a durable `_repl/` note (dev/DESIGN.md's
//!     totality principle). Checked at the ack, because the heal phase's
//!     `reconcile` would otherwise launder the defect. `publish_record` only:
//!     proxy-cache fills replicate asynchronously by design (bf913b9);
//!   - VIEWS == TRUTH: `verify::verify_storage` — the product's own byte-strict
//!     `pypiron verify` oracle — re-renders every view from each bucket's truth
//!     and diffs the bytes;
//!   - CONVERGENCE: all buckets hold identical truth and views;
//!   - TOMBSTONE MONOTONICITY: a filename whose most recent ack was a 204 delete
//!     never stands in a bucket without its tombstone — no silent resurrection
//!     of a removed (compromised) artifact;
//!   - NO LEAKS: no unconsumed `_dirty/` markers, no undrained `_repl/` notes;
//!   - CONSERVATION: acknowledged bytes are never silently lost — a file is
//!     live, or its filename was deleted/frozen by an authorized operation;
//!   - LIVENESS (bounded): the fleet reaches quiescence within the drain
//!     budget — no livelock;
//!   - REPAIR TAXONOMY: every view the tier-3 audit had to repair is
//!     classified from the run's effect history (see `Observer` below).
//!     ORDERING (truth changed with no durable breadcrumb ever covering it)
//!     and PREMATURE-CONSUMPTION (the breadcrumb existed and was destroyed
//!     without converging the view) fail the seed in every profile; only the
//!     documented CONCURRENT-RACE divergence (an unleased rebuild clobbering
//!     a fresher one, `tests/model_event_protocol.rs`) remains a reported
//!     statistic, with the audit as its designed backstop. Crash-only
//!     profiles additionally keep their blanket any-repair-is-a-violation
//!     gate.
//!
//! The *workload* is seed-derived too, not a constant. A fixed workload is the
//! quiet way a simulator stops finding things: four filenames and one frozen op
//! mix explore one shape of the state space forever, however many seeds you
//! burn. So each rotating seed draws its own entity count (1-6 packages x 1-4
//! files) and its own op-class weight vector — swarm testing (Groce et al.):
//! rather than one average mix, sample many extreme ones, because a rare
//! interleaving is only rare under the average. Every class keeps a nonzero
//! floor, so no seed can silently stop publishing and report green on a run
//! that verified nothing.
//!
//! Every oracle above is also *metered*: the run prints how many times each one
//! actually evaluated something — and each arm of the repair classifier
//! separately — because an oracle reading zero over a whole soak is a defect
//! report, not a pass (see `Reach`). `--require-reach` turns that into a failing
//! run. The same line reports the worst rounds-to-quiesce any seed needed
//! against the heal budget, so an over-generous budget cannot quietly turn a
//! livelock into a green LIVENESS.
//!
//! Determinism is self-checked, not assumed: every run whose seed is a multiple
//! of `--recheck-every` executes twice and must produce an identical storage-op
//! trace hash *and* an identical final world (bucket bytes + ledger), because a
//! nondeterminism downstream of the op sequence — an unvirtualized clock read,
//! say — issues the same calls with different bytes. The rerun's own invariant
//! verdict counts too: a seed that passes once and fails once is a red seed.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use pypiron::buckets::{BucketHandle, BucketSet};
use pypiron::sim::{SimClock, SimStorage};
use pypiron::storage::{FileEntry, ObjectMeta, Storage};
use pypiron::{replicate, worker, AppState};

/// Package names the workload draws from; a profile uses the first `packages`
/// of them, so widening the workload is a superset of the old two-name one and
/// every pinned seed still means what its comment says. Dashed on purpose:
/// `filename()` has to escape the dash to build a parseable wheel name.
const PACKAGE_NAMES: [&str; 6] = [
    "vopr-alpha",
    "vopr-beta",
    "vopr-gamma",
    "vopr-delta",
    "vopr-epsilon",
    "vopr-zeta",
];

/// Chaos-loop op classes, in the order `pick_op` indexes them.
const OP_CLASSES: usize = 8;
const OP_LABELS: [&str; OP_CLASSES] = [
    "publish",
    "delete",
    "tick",
    "sweep",
    "reconcile",
    "jump",
    "crash",
    "nudge",
];

/// The historical fixed mix, out of 100 — what a non-rotating run uses, so an
/// explicit `--nodes/--buckets/--ops` command means exactly what it always did.
const DEFAULT_OP_WEIGHTS: [u16; OP_CLASSES] = [40, 10, 25, 7, 4, 5, 5, 4];

/// Per-class swarm bounds `(floor, span)`: a rotating seed's weight is
/// `floor + rng.below(span)`. The floors are the honesty constraint — a class
/// that could reach zero would hand some seeds a run that never publishes,
/// verifies nothing, and still reports green.
const OP_WEIGHT_BOUNDS: [(u16, u16); OP_CLASSES] = [
    (6, 45), // publish — the only op that creates work to verify
    (2, 20), // delete
    (3, 30), // worker tick (the rebuild fast path)
    (1, 12), // replication sweep
    (1, 10), // reconcile (tree diff)
    (1, 10), // clock jump past the intent grace
    (1, 10), // schedule a crash
    (1, 10), // clock nudge
];

const SIM_START: &str = "2026-01-01T00:00:00Z";

// ---------------------------------------------------------------------------
// `--break <name>`: mutation testing for the oracles themselves.
//
// An invariant nobody has watched go red is not a test — it is an assertion of
// faith that costs runtime forever and reports reassurance. Each break is ONE
// deliberate defect, named after what it breaks, that a specific oracle is
// supposed to catch; CI asserts the run FAILS with that oracle's text. The
// harness owns every break (no product-code hooks ship in the binary), and it
// is inert by default: every injection point is a comparison against
// `Break::None` that draws no rng, consumes no op-sequence number, and records
// no trace event, so the pinned seed corpus is untouched when idle.
// ---------------------------------------------------------------------------
#[derive(Clone, Copy, PartialEq, Eq)]
enum Break {
    None,
    /// Truncate a materialized view: torn write, truth untouched → VIEWS==TRUTH.
    View,
    /// Blackhole peer bucket 1 and drop the note that owes it → ACK_TOTALITY.
    Fanout,
    /// End the rerun of a seed in a different world → world-state determinism.
    Rerun,
    /// Restore an acked-deleted artifact, tombstone gone → TOMBSTONE_MONOTONICITY.
    Resurrect,
    /// Mutate truth with no breadcrumb over it → audit-repair class 1 ORDERING.
    Ordering,
    /// Materialize a whole package in truth with no breadcrumb over it → the
    /// same class 1, but on the *global* index — the kill proof for
    /// `analyze_global`, which `ordering` cannot give because it grows an
    /// existing package and never flips global membership.
    GlobalIndex,
    /// Serve corrupt bytes for an acked artifact while the real bytes survive
    /// in quarantine → DURABILITY, and nothing else (see `apply_break`).
    Durability,
    /// Drop one acked filename from its package view → VISIBILITY.
    Visibility,
    /// Destroy an acked body fleet-wide with no tombstone and no freeze →
    /// CONSERVATION.
    Conserve,
    /// Leave an object on bucket 1 that bucket 0 never got → CONVERGENCE.
    Diverge,
    /// Park an object under `_repl/` that no sweep recognizes → LIVENESS.
    Wedge,
    /// A phantom package the audit must materialize, plus the effect history
    /// of the rebuild that caused it. One break per classifier arm; each kills
    /// the per-package *and* the global analyzer at once (`analyze`'s and
    /// `analyze_global`'s tests are the same three questions asked of
    /// `ViewWrite`s and `GlobalWrite`s respectively).
    Poison,
    Blind,
    Race,
    Fallback,
}

/// The package name every phantom-clone break materializes. Not in
/// `PACKAGE_NAMES`, so no real effect in the run can ever mention it — the
/// planted history is the only history the classifier sees for it.
const BREAK_PKG: &str = "vopr-phantom";

impl Break {
    fn parse(name: &str) -> Break {
        match name {
            "none" => Break::None,
            "view" => Break::View,
            "fanout" => Break::Fanout,
            "rerun" => Break::Rerun,
            "resurrect" => Break::Resurrect,
            "ordering" => Break::Ordering,
            "globalindex" => Break::GlobalIndex,
            "durability" => Break::Durability,
            "visibility" => Break::Visibility,
            "conserve" => Break::Conserve,
            "diverge" => Break::Diverge,
            "wedge" => Break::Wedge,
            "poison" => Break::Poison,
            "blind" => Break::Blind,
            "race" => Break::Race,
            "fallback" => Break::Fallback,
            other => panic!(
                "unknown --break {other} (view|fanout|rerun|resurrect|ordering|globalindex|\
                 durability|visibility|conserve|diverge|wedge|poison|blind|race|fallback)"
            ),
        }
    }
}

/// An acknowledged upload no authorized removal excuses — the subject the
/// durability family of oracles is actually about. Returns `(pkg, filename,
/// bytes)`. Raw `dump()` reads only: no op-sequence number, no rng, no trace.
fn unexcused_ack(
    buckets: &[Arc<SimStorage>],
    ledger: &Mutex<Ledger>,
) -> Option<(String, String, Vec<u8>)> {
    let ledger = ledger.lock().expect("ledger lock");
    ledger.acked.iter().find_map(|((pkg, fname), body)| {
        let akey = format!("packages/{pkg}/{fname}");
        let excused = ledger.deleted.contains(&(pkg.clone(), fname.clone()))
            || buckets.iter().any(|b| {
                let keys = b.keys();
                keys.contains(&format!("{akey}.tombstone"))
                    || keys.contains(&format!("{akey}.frozen"))
            });
        (!excused).then(|| (pkg.clone(), fname.clone(), body.clone()))
    })
}

/// Clone a live record (body + sidecar) into a package the audit must then
/// reconcile, and return `(pkg, added key)`. The clone reuses a live record's
/// sidecar (same version, different wheel tag) so the added file renders like
/// any other and the repaired view is byte-clean. `phantom` puts it under a
/// name that does not exist yet — which flips `simple/index.*` membership and
/// so brings `analyze_global` into play alongside `analyze`.
fn clone_live_record(bucket: &SimStorage, phantom: bool) -> Option<(String, String)> {
    let dump = bucket.dump();
    let (akey, body, sidecar) = dump.iter().find_map(|(key, sidecar)| {
        let akey = key.strip_suffix(".meta.json")?;
        Some((akey.to_string(), dump.get(akey)?.clone(), sidecar.clone()))
    })?;
    let (pkg, clone) = if phantom {
        let fname = akey.rsplit('/').next().unwrap_or("phantom.whl");
        (
            BREAK_PKG.to_string(),
            format!("packages/{BREAK_PKG}/{fname}"),
        )
    } else {
        let pkg = akey
            .strip_prefix("packages/")?
            .split('/')
            .next()?
            .to_string();
        (pkg, akey.replace("py3-none", "py2-none"))
    };
    bucket.insert(&clone, body);
    bucket.insert(&format!("{clone}.meta.json"), sidecar);
    Some((pkg, clone))
}

/// Inject the selected defect. Called at two points, and only when a break is
/// selected: `pre_audit` is inside the first heal round with the fast path
/// already drained, so a truth mutation there is one the audit alone can
/// converge; otherwise it is post-quiescence, just before the invariants, where
/// nothing can launder the damage. `rerun` is true on a seed's second
/// execution. Everything writes raw `SimStorage` — never a `FaultView` — so a
/// break perturbs storage, never the schedule.
async fn apply_break(
    brk: Break,
    pre_audit: bool,
    rerun: bool,
    buckets: &[Arc<SimStorage>],
    obs: &Observer,
    ledger: &Mutex<Ledger>,
) {
    match (brk, pre_audit) {
        // Class-1 ORDERING by construction: truth grows a file with no `_dirty/`
        // marker and no `_repl/` note anywhere covering it, so nothing durable
        // can tell the fast path to rebuild and only the audit's fingerprint
        // diff can converge the view. The clone reuses a live record's sidecar
        // (same version, different wheel tag) so the added file renders like any
        // other and the repaired view is byte-clean.
        //
        // `globalindex` is the same defect one level up: the clone lands under a
        // package name that does not exist yet, so the membership of
        // `simple/index.{json,html}` is what the audit has to repair. Only the
        // global classifier can explain that one, which is exactly why it is a
        // separate break — `ordering` grows an existing package and leaves the
        // name set alone, so it can never exercise that path.
        (Break::Ordering | Break::GlobalIndex, true) => {
            if let Some((_, clone)) = clone_live_record(&buckets[0], brk == Break::GlobalIndex) {
                obs.observe_mutation(0, 0, &clone, None);
            }
        }
        // The other three classifier arms, and the fallback. Same phantom clone
        // — a package the audit has to materialize, so `analyze` (its view) and
        // `analyze_global` (its membership) both run — with the effect history
        // of the rebuild that would have caused it planted alongside.
        //
        // Planted, because a concurrent-rebuild clobber leaves NO storage
        // residue that distinguishes it from a lone stale writer: the loser's
        // bytes are gone by definition. The history is where the shape lives,
        // which is why the classifier reads history and not storage — and why a
        // history is the only thing a kill proof for these arms can inject.
        // Attribution planting is not new here: `synth_view_write` already does
        // it for warm-bucket audit writes. Read these as mutation tests of the
        // classifier's predicates, NOT as evidence the product can produce the
        // interleaving (dev/TESTING.md draws that line).
        (Break::Poison | Break::Blind | Break::Race | Break::Fallback, true) => {
            let Some((pkg, clone)) = clone_live_record(&buckets[0], true) else {
                return;
            };
            // `fallback` plants nothing at all: the audit repairs a view the
            // effect history cannot explain, which is exactly the shape the
            // fallback arm exists to report.
            if brk == Break::Fallback {
                return;
            }
            // Two racing rebuild sessions, F and G, and the two markers they
            // consume. Every planted effect is pure observation: no rng, no
            // storage op, no trace event.
            let (f, g) = (obs.fresh_synth_att(), obs.fresh_synth_att());
            let list = format!("packages/{pkg}/");
            let view = format!("simple/{pkg}/index.json");
            let (mark_a, mark_b) = (format!("_dirty/{pkg}!a"), format!("_dirty/{pkg}!b"));
            let plant = |kind, att, key: &str| {
                obs.push_effect(kind, 0, 0, &pkg, key, Vec::new(), att);
            };
            // A global-index write claiming `names` — production records these
            // against the empty package, so mirror that.
            let global = |att, names: Vec<String>| {
                let key = "simple/index.json";
                obs.push_effect(EffectKind::GlobalWrite, 0, 0, "", key, names, att);
            };
            match brk {
                // TEST 2a — the rebuild listed truth PAST the mutation and
                // still wrote a view (and a name set) that disagrees with it.
                Break::Poison => {
                    plant(EffectKind::TruthWrite, f, &clone);
                    plant(EffectKind::TruthList, f, &list);
                    plant(EffectKind::ViewWrite, f, &view);
                    global(f, Vec::new());
                }
                // TEST 2b — a marker covered the mutation, and the only op that
                // retired it had listed truth before the mutation: the signal
                // was destroyed, not consumed. No view write at all, so 2a's
                // guard cannot fire first.
                Break::Blind => {
                    plant(EffectKind::MarkerPut, g, &mark_a);
                    plant(EffectKind::TruthList, f, &list);
                    plant(EffectKind::TruthWrite, f, &clone);
                    plant(EffectKind::MarkerDel, f, &mark_a);
                }
                // TEST 3 — two unleased rebuilds. F listed truth before the
                // mutation, G after; G published first, F clobbered it last.
                // Marker a is consumed blind by F and marker b sighted by G, so
                // TEST 1 (a breadcrumb existed) and TEST 2b (not every consumer
                // was blind) both decline it first.
                Break::Race => {
                    plant(EffectKind::MarkerPut, f, &mark_a);
                    plant(EffectKind::MarkerPut, g, &mark_b);
                    plant(EffectKind::TruthList, f, &list);
                    plant(EffectKind::TruthWrite, f, &clone);
                    plant(EffectKind::TruthList, g, &list);
                    plant(EffectKind::MarkerDel, f, &mark_a);
                    plant(EffectKind::ViewWrite, g, &view);
                    global(g, vec![pkg.clone()]);
                    plant(EffectKind::MarkerDel, g, &mark_b);
                    plant(EffectKind::ViewWrite, f, &view);
                    global(f, Vec::new());
                }
                _ => {}
            }
        }
        // An object under `_repl/` that no sweep recognizes as a marker
        // (`parse_repl_marker` wants `<dest>/<pkg>/<file>!<nonce>`): nothing
        // consumes it, so the fixpoint the heal phase is bounded to reach does
        // not exist. Planted pre-audit so the drain budget is really spent.
        (Break::Wedge, true) => {
            buckets[0].insert("_repl/vopr-wedge", b"nothing drains this".to_vec());
        }
        // Corrupt the bytes an acked artifact serves, fleet-wide, while parking
        // the real bytes outside `packages/` — so CONSERVATION still finds them
        // alive, CONVERGENCE still sees identical buckets, and `pypiron verify`
        // (which reads sidecars and views, never bodies) still passes. What is
        // left is exactly one claim: the 200 said these bytes were durable.
        (Break::Durability, false) => {
            if let Some((pkg, fname, body)) = unexcused_ack(buckets, ledger) {
                let akey = format!("packages/{pkg}/{fname}");
                for bucket in buckets {
                    bucket.insert(&format!("_vopr/quarantine/{akey}"), body.clone());
                    bucket.insert(&akey, b"vopr: corrupt".to_vec());
                }
            }
        }
        // The record is intact and the bytes are safe; only the listing forgot
        // it. Applied to every bucket so the buckets stay converged.
        (Break::Visibility, false) => {
            if let Some((pkg, fname, _)) = unexcused_ack(buckets, ledger) {
                let key = format!("simple/{pkg}/index.json");
                for bucket in buckets {
                    if let Some(bytes) = bucket.dump().get(&key) {
                        let doctored = String::from_utf8_lossy(bytes)
                            .replace(&fname, "vopr-unlisted-1.0-py3-none-any.whl");
                        bucket.insert(&key, doctored.into_bytes());
                    }
                }
            }
        }
        // Acked bytes destroyed everywhere with nothing authorizing it: no
        // tombstone, no freeze, no acked delete.
        (Break::Conserve, false) => {
            if let Some((pkg, fname, _)) = unexcused_ack(buckets, ledger) {
                let akey = format!("packages/{pkg}/{fname}");
                for bucket in buckets {
                    let _ = bucket
                        .delete_keys(&[akey.clone(), format!("{akey}.meta.json")])
                        .await;
                }
            }
        }
        // One bucket keeps an object the other never got — a half-finished
        // replication write nobody cleaned up. A dotfile is not an artifact
        // (`sidecar::is_artifact`), so `pypiron verify` ignores it on both
        // buckets and the only oracle left holding the claim is CONVERGENCE.
        (Break::Diverge, false) => {
            if let Some(bucket) = buckets.get(1) {
                bucket.insert("packages/vopr-drift/.vopr-stray", b"unreplicated".to_vec());
            }
        }
        // A torn view write: the last byte never landed. Truth is untouched, so
        // the byte-strict re-render must disagree. A run that materialized no
        // package view at all gets the other arm of the same oracle — a view
        // left standing for a package with no files.
        (Break::View, false) => {
            let dump = buckets[0].dump();
            match dump.iter().find(|(key, _)| view_key_package(key).is_some()) {
                Some((key, bytes)) => {
                    buckets[0].insert(key, bytes[..bytes.len().saturating_sub(1)].to_vec())
                }
                None => buckets[0].insert("simple/vopr-ghost/index.json", b"{}".to_vec()),
            }
        }
        // Nondeterminism downstream of the op sequence: the rerun issues the
        // identical storage-op trace (this write bypasses the traced view) and
        // still ends in a different world. Deliberately parked at a key no other
        // oracle reads, so the state hash is the only thing that can catch it.
        (Break::Rerun, false) if rerun => {
            buckets[0].insert("_vopr/break-rerun", b"second execution only".to_vec());
        }
        // A removed (compromised) artifact comes back: its bytes are restored
        // and the tombstone that forbade it is gone.
        (Break::Resurrect, false) => {
            let victim = {
                let ledger = ledger.lock().expect("ledger lock");
                ledger
                    .last_ack_deleted
                    .iter()
                    .find(|(_, deleted)| **deleted)
                    .map(|((pkg, fname), _)| format!("packages/{pkg}/{fname}"))
            };
            if let Some(akey) = victim {
                let _ = buckets[0].delete_keys(&[format!("{akey}.tombstone")]).await;
                buckets[0].insert(&akey, b"resurrected".to_vec());
            }
        }
        _ => {}
    }
}

fn filename(pkg: &str, file: u8) -> String {
    // Wheel filenames escape dashes in the distribution segment; a dashed
    // package name here would parse as distribution "vopr", version "alpha".
    format!("{}-{}.0-py3-none-any.whl", pkg.replace('-', "_"), file + 1)
}

fn body_bytes(pkg: &str, file: u8, variant: u8) -> Vec<u8> {
    format!("artifact:{pkg}:{file}:{variant}").into_bytes()
}

// ---------------------------------------------------------------------------
// Seeded PRNG: SplitMix64 — tiny, deterministic, no dependency.
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_add(0x9E3779B97F4A7C15))
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

// ---------------------------------------------------------------------------
// The reach meter: did each oracle actually EXECUTE, on input worth checking?
//
// An oracle reading zero executions over a whole soak is a defect report, not a
// pass. This harness has shipped an unfalsifiable gate twice — a three-class
// audit-repair taxonomy documented as CI-enforced while one class's loop body
// had never run, and two oracles added with no evidence they could fire — and
// establishing that took a human hand-running 241,530 seeds. It should be a
// number the binary prints.
//
// "Executed" is deliberately not "its code was reached": DURABILITY looping over
// an empty ledger is not an execution; DURABILITY comparing one acknowledged
// upload against a bucket's bytes is. Each counter's unit is spelled out in
// `REACH_METER` and printed beside it, because that definition IS the meter.
//
// Non-perturbing by construction: every hit is a relaxed atomic increment over
// data the run already computed. No rng draw, no await, no storage op, nothing
// through `FaultView` — so the meter cannot move the schedule it measures.
// ---------------------------------------------------------------------------

/// Heal-phase budgets. The margin printed by the meter is measured against
/// these, so the budget and its headroom can never drift apart.
const HEAL_ROUNDS: u64 = 12;
const DRAIN_PASSES: u64 = 20;

#[derive(Clone, Copy)]
enum R {
    Durability,
    Visibility,
    Conservation,
    Convergence,
    Liveness,
    Verify,
    AckTotality,
    Tombstone,
    Determinism,
    PkgOrdering,
    PkgPoisoned,
    PkgBlind,
    PkgRace,
    PkgFallback,
    GlobalOrdering,
    GlobalPoisoned,
    GlobalBlind,
    GlobalRace,
    GlobalFallback,
}

const REACH_SLOTS: usize = 19;

/// `(label, what exactly one execution counts)` — in `R`'s declaration order.
const REACH_METER: [(&str, &str); REACH_SLOTS] = [
    (
        "DURABILITY",
        "acked upload compared against a bucket's bytes",
    ),
    (
        "VISIBILITY",
        "stored artifact checked against that bucket's view",
    ),
    (
        "CONSERVATION",
        "acked file with no authorized removal traced",
    ),
    (
        "CONVERGENCE",
        "bucket pair compared with real truth/views present",
    ),
    ("LIVENESS", "heal phase that had protocol debris to drain"),
    (
        "VERIFY",
        "bucket with non-empty truth re-rendered by verify_storage",
    ),
    (
        "ACK_TOTALITY",
        "ack weighed against >=1 peer bucket at the 200",
    ),
    ("TOMBSTONE_MONOTONICITY", "acked-deleted filename checked"),
    (
        "DETERMINISM",
        "seed re-executed, trace + final world compared",
    ),
    (
        "classifier/pkg TEST 1 ordering",
        "unreflected mutation tested for a covering breadcrumb",
    ),
    (
        "classifier/pkg TEST 2a poisoned",
        "repair whose final view writer had both truth and mutations",
    ),
    (
        "classifier/pkg TEST 2b blind",
        "covered mutation tested for all-blind consumption",
    ),
    (
        "classifier/pkg TEST 3 race",
        "older view write by another op tested for being fresher",
    ),
    ("classifier/pkg FALLBACK", "repair no test above explained"),
    (
        "classifier/global TEST 1 ordering",
        "unreflected mutation tested for a covering breadcrumb",
    ),
    (
        "classifier/global TEST 2a poisoned",
        "flip whose final global writer had derived the name",
    ),
    (
        "classifier/global TEST 2b blind",
        "covered mutation tested for all-blind consumption",
    ),
    (
        "classifier/global TEST 3 race",
        "older global write by another op tested for being fresher",
    ),
    ("classifier/global FALLBACK", "flip no test above explained"),
];

/// Zeros that are a known property of the harness or the product, not a hole.
/// `--require-reach` skips exactly these; every other oracle must execute. An
/// entry here is a claim someone has to defend in dev/TESTING.md — and if one
/// starts executing, the meter says so and the entry comes out.
const EXPECTED_ZERO: [(usize, &str); 10] = [
    (
        R::PkgOrdering as usize,
        "product-unreachable: no class-1 has ever been produced; `--break ordering` kills it",
    ),
    (
        R::PkgPoisoned as usize,
        "product-unreachable: the fast path converges every package view; `--break poison` kills it",
    ),
    (
        R::PkgBlind as usize,
        "product-unreachable: no repair TEST 1 declined has been seen; `--break blind` kills it",
    ),
    (
        R::PkgRace as usize,
        "harness-unreachable: tick_lock serializes rebuilds; `--break race` kills it (dev/TESTING.md)",
    ),
    (
        R::PkgFallback as usize,
        "a repair no test explains is a classifier bug; `--break fallback` kills it",
    ),
    (
        R::GlobalOrdering as usize,
        "product-unreachable: no class-1 has ever been produced; `--break globalindex` kills it",
    ),
    (
        R::GlobalPoisoned as usize,
        "product-unreachable: no audit-repaired membership flip seen; `--break poison` kills it",
    ),
    (
        R::GlobalBlind as usize,
        "product-unreachable: no flip TEST 1 declined has been seen; `--break blind` kills it",
    ),
    (
        R::GlobalRace as usize,
        "harness-unreachable: tick_lock serializes rebuilds; `--break race` kills it (dev/TESTING.md)",
    ),
    (
        R::GlobalFallback as usize,
        "a flip no test explains is a classifier bug; `--break fallback` kills it",
    ),
];

struct Reach {
    slots: [AtomicU64; REACH_SLOTS],
    /// Worst heal rounds, and worst drain passes inside one round, any seed used.
    peak_rounds: AtomicU64,
    peak_drains: AtomicU64,
    /// True once any explored profile had more than one bucket.
    multi_bucket: AtomicBool,
}

static REACH: Reach = Reach {
    slots: [const { AtomicU64::new(0) }; REACH_SLOTS],
    peak_rounds: AtomicU64::new(0),
    peak_drains: AtomicU64::new(0),
    multi_bucket: AtomicBool::new(false),
};

impl Reach {
    fn hit(&self, slot: R) {
        self.slots[slot as usize].fetch_add(1, Ordering::Relaxed);
    }
    fn peak(cell: &AtomicU64, observed: u64) {
        cell.fetch_max(observed, Ordering::Relaxed);
    }
}

/// Why a zero reading on `slot` is expected. Topology first: a single-bucket
/// sample cannot reach the two replication oracles, and calling that a hole
/// would train everyone to ignore the gate.
fn expected_zero(slot: usize, multi_bucket: bool) -> Option<&'static str> {
    if !multi_bucket && (slot == R::Convergence as usize || slot == R::AckTotality as usize) {
        return Some("single-bucket sample — the oracle needs >1 bucket");
    }
    EXPECTED_ZERO
        .iter()
        .find(|(s, _)| *s == slot)
        .map(|(_, why)| *why)
}

/// Print the table and return the oracles that never executed and had no
/// standing excuse — what `--require-reach` fails on.
fn report_reach(explored: u64, brk: Break) -> Vec<&'static str> {
    let multi = REACH.multi_bucket.load(Ordering::Relaxed);
    let mut unreached = Vec::new();
    println!(
        "vopr: oracle reach over {explored} seeds — executions on NON-TRIVIAL input \
         (a zero means that oracle verified nothing):"
    );
    for (slot, (label, unit)) in REACH_METER.iter().enumerate() {
        let hits = REACH.slots[slot].load(Ordering::Relaxed);
        let note = match (hits, expected_zero(slot, multi)) {
            (0, Some(why)) => format!("  [zero, expected: {why}]"),
            (0, None) => {
                unreached.push(*label);
                "  [ZERO — NEVER EXECUTED]".to_string()
            }
            // A break exists to reach an oracle, so say so rather than demand
            // the list be edited on the strength of a deliberate defect.
            (_, Some(_)) if brk != Break::None => "  [reached under --break]".to_string(),
            (_, Some(_)) => "  [now reached — drop it from EXPECTED_ZERO]".to_string(),
            (_, None) => String::new(),
        };
        println!("  {label:<35} {hits:>10}  {unit}{note}");
    }
    let rounds = REACH.peak_rounds.load(Ordering::Relaxed);
    let drains = REACH.peak_drains.load(Ordering::Relaxed);
    println!(
        "  quiesce headroom: worst seed used {rounds}/{HEAL_ROUNDS} heal rounds ({} spare) and \
         {drains}/{DRAIN_PASSES} drain passes in a round ({} spare) — LIVENESS is a boolean, so \
         a budget with no margin left silently passes a livelock",
        HEAL_ROUNDS.saturating_sub(rounds),
        DRAIN_PASSES.saturating_sub(drains),
    );
    unreached
}

// ---------------------------------------------------------------------------
// Repair classifier: passive effect history + audit-repair taxonomy.
//
// Every successful protocol-relevant storage effect — truth mutation, truth
// listing, view write, marker/note create and consume, global-index write —
// is recorded with a logical-op attribution. When the tier-3 audit repairs a
// view the fast path left unconverged, this history answers *why*. It is pure
// observation (no rng, no awaits, no extra storage ops), so recording can
// never perturb the run it explains, and no differential replay is needed —
// a replay under altered semantics would shift the seed-derived fault
// schedule and could silently misclassify a real bug as a benign race.
//
//   1 ORDERING: an unreflected truth mutation was never covered by any
//     durable breadcrumb (a `_dirty/` marker on that bucket, or a `_repl/`
//     note aimed at it). Writers put intent before truth, the merge executor
//     brackets both sides, rebuilds fence themselves — so this must be zero.
//   2 PREMATURE-CONSUMPTION: breadcrumbs existed but the system destroyed
//     the signal without converging the view — a consumer that never listed
//     truth past the mutation it retired (the stale-intent-heal shape), or a
//     rebuild that consumed its markers while leaving a view inconsistent
//     with the very truth it listed (a poisoned derivation). Avoidable; must
//     be zero. Drift the tests below cannot explain is also reported here,
//     conservatively: an unexplained repair is a bug in the protocol or in
//     this classifier, and either deserves a failing seed.
//   3 CONCURRENT-RACE: a rebuild that listed truth earlier overwrote the
//     view written by a rebuild that listed later — the documented unleased
//     concurrent-rebuild divergence (tests/model_event_protocol.rs,
//     `concurrent_rebuild_without_lease_diverges`). The audit is its
//     designed backstop; reported as a statistic, never a violation.
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// Logical-op attribution for effect history. Set per workload op and per
    /// heal-phase protocol call. Does not cross `tokio::spawn`: the tick's
    /// per-package rebuild tasks are attributed by session inference instead
    /// (see `Observer::attribution`).
    static OP_ID: u64;
}

/// Attribution ids at or above this are inferred rebuild sessions, not ops.
const SYNTH_BASE: u64 = 1 << 32;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EffectKind {
    /// Successful mutation under `packages/<pkg>/` (put or delete).
    TruthWrite,
    /// `list_dir` of `packages/<pkg>/` — the read a rebuild derives from.
    TruthList,
    /// `list_dir` of `_dirty/` — bounds a tick's consume window.
    MarkerList,
    /// Put or delete of `simple/<pkg>/index.{html,json}`.
    ViewWrite,
    MarkerPut,
    MarkerDel,
    /// `_repl/` note create/consume; `bucket` is the *covered destination*.
    NotePut,
    NoteDel,
    /// Write of `simple/index.{json,html}`; `names` carries the name set.
    GlobalWrite,
}

struct Effect {
    seq: u64,
    kind: EffectKind,
    node: usize,
    bucket: usize,
    att: u64,
    pkg: String,
    /// Full storage key for markers/notes (interval matching); empty otherwise.
    key: String,
    /// GlobalWrite only: the package names the write claims exist.
    names: Vec<String>,
}

/// Inferred rebuild sessions: a monotonic counter, plus (node, bucket, pkg) →
/// the id of that key's currently-open session.
type SynthSessions = (u64, std::collections::HashMap<(usize, usize, String), u64>);

#[derive(Default)]
struct Observer {
    seq: AtomicU64,
    effects: Mutex<Vec<Effect>>,
    /// Open inferred sessions for unattributed (tick-spawned) rebuild tasks,
    /// keyed by (node, bucket, pkg). The fleet-wide tick lock guarantees at
    /// most one live unattributed rebuild per key, so inference is exact.
    synth: Mutex<SynthSessions>,
}

impl Observer {
    /// Resolve the recording op: the task-local op id when present, else the
    /// open (or freshly opened, when `opens_session`) inferred session.
    fn attribution(&self, node: usize, bucket: usize, pkg: &str, opens_session: bool) -> u64 {
        if let Ok(id) = OP_ID.try_with(|id| *id) {
            return id;
        }
        let mut synth = self.synth.lock().expect("synth lock");
        let key = (node, bucket, pkg.to_string());
        if opens_session {
            synth.0 += 1;
            let id = SYNTH_BASE + synth.0;
            synth.1.insert(key, id);
            id
        } else {
            synth.1.get(&key).copied().unwrap_or(0)
        }
    }

    fn record(&self, kind: EffectKind, node: usize, bucket: usize, pkg: &str, key: &str) {
        self.record_named(kind, node, bucket, pkg, key, Vec::new());
    }

    fn record_named(
        &self,
        kind: EffectKind,
        node: usize,
        bucket: usize,
        pkg: &str,
        key: &str,
        names: Vec<String>,
    ) {
        let att = self.attribution(node, bucket, pkg, kind == EffectKind::TruthList);
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        self.effects.lock().expect("effects lock").push(Effect {
            seq,
            kind,
            node,
            bucket,
            att,
            pkg: pkg.to_string(),
            key: key.to_string(),
            names,
        });
    }

    /// Record one successful write/delete effect for `key` on (node, bucket).
    /// `bytes` is the written body when the caller has it (name-set parsing).
    fn observe_mutation(&self, node: usize, bucket: usize, key: &str, bytes: Option<&[u8]>) {
        if let Some(rest) = key.strip_prefix("_dirty/") {
            let pkg = rest.split('!').next().unwrap_or(rest);
            self.record(EffectKind::MarkerPut, node, bucket, pkg, key);
        } else if let Some((dest, pkg)) = parse_note_key(key) {
            self.record(EffectKind::NotePut, node, dest, &pkg, key);
        } else if let Some(pkg) = key
            .strip_prefix("packages/")
            .and_then(|r| r.split('/').next())
        {
            self.record(EffectKind::TruthWrite, node, bucket, pkg, key);
        } else if key == "simple/index.json" {
            let names = bytes.map(global_names_from_json).unwrap_or_default();
            self.record_named(EffectKind::GlobalWrite, node, bucket, "", key, names);
        } else if key == "simple/index.html" {
            let names = bytes.map(global_names_from_html).unwrap_or_default();
            self.record_named(EffectKind::GlobalWrite, node, bucket, "", key, names);
        } else if let Some(pkg) = view_key_package(key) {
            self.record(EffectKind::ViewWrite, node, bucket, pkg, key);
        }
    }

    fn observe_list(&self, node: usize, bucket: usize, prefix: &str) {
        if prefix == "_dirty/" {
            self.record(EffectKind::MarkerList, node, bucket, "", prefix);
        } else if let Some(pkg) = prefix
            .strip_prefix("packages/")
            .and_then(|r| r.strip_suffix('/'))
        {
            if !pkg.contains('/') {
                self.record(EffectKind::TruthList, node, bucket, pkg, prefix);
            }
        }
    }

    fn snapshot(&self) -> Vec<Effect> {
        let effects = self.effects.lock().expect("effects lock");
        effects
            .iter()
            .map(|e| Effect {
                seq: e.seq,
                kind: e.kind,
                node: e.node,
                bucket: e.bucket,
                att: e.att,
                pkg: e.pkg.clone(),
                key: e.key.clone(),
                names: e.names.clone(),
            })
            .collect()
    }

    /// Mint one fresh inferred-session attribution — used to stand in for a
    /// warm-bucket audit write that bypassed the traced view.
    fn fresh_synth_att(&self) -> u64 {
        let mut synth = self.synth.lock().expect("synth lock");
        synth.0 += 1;
        SYNTH_BASE + synth.0
    }

    /// Record the effects a warm-bucket audit write would have produced had it
    /// flowed through the traced view (it writes raw SimStorage instead), so a
    /// later round's classification sees the convergence it achieved. `att` is
    /// one fresh synthetic id shared across the round's synthesized writes; a
    /// rebuild lists truth then writes the view, so mirror that pair for view
    /// keys to give the synthetic ViewWrite a fresh TruthList to be tied to.
    fn synth_view_write(&self, bucket: usize, key: &str, after: Option<&[u8]>, att: u64) {
        if key == "simple/index.json" {
            let names = after.map(global_names_from_json).unwrap_or_default();
            self.push_effect(EffectKind::GlobalWrite, 0, bucket, "", key, names, att);
        } else if key == "simple/index.html" {
            let names = after.map(global_names_from_html).unwrap_or_default();
            self.push_effect(EffectKind::GlobalWrite, 0, bucket, "", key, names, att);
        } else if let Some(pkg) = view_key_package(key) {
            let list_key = format!("packages/{pkg}/");
            self.push_effect(
                EffectKind::TruthList,
                0,
                bucket,
                pkg,
                &list_key,
                Vec::new(),
                att,
            );
            self.push_effect(EffectKind::ViewWrite, 0, bucket, pkg, key, Vec::new(), att);
        }
    }

    /// Push a fully-specified effect with a fresh seq and explicit attribution
    /// (no task-local / session inference).
    #[allow(clippy::too_many_arguments)]
    fn push_effect(
        &self,
        kind: EffectKind,
        node: usize,
        bucket: usize,
        pkg: &str,
        key: &str,
        names: Vec<String>,
        att: u64,
    ) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        self.effects.lock().expect("effects lock").push(Effect {
            seq,
            kind,
            node,
            bucket,
            att,
            pkg: pkg.to_string(),
            key: key.to_string(),
            names,
        });
    }
}

/// `simple/<pkg>/index.html|json` → pkg.
fn view_key_package(key: &str) -> Option<&str> {
    let rest = key.strip_prefix("simple/")?;
    let (pkg, file) = rest.split_once('/')?;
    (file == "index.html" || file == "index.json").then_some(pkg)
}

/// `_repl/<dest>/<pkg>/<file>!<nonce>` → (dest bucket, pkg).
fn parse_note_key(key: &str) -> Option<(usize, String)> {
    let rest = key.strip_prefix("_repl/")?;
    let (dest, rest) = rest.split_once('/')?;
    let dest = dest.parse::<usize>().ok()?;
    let (pkg, _) = rest.split_once('/')?;
    Some((dest, pkg.to_string()))
}

fn global_names_from_json(bytes: &[u8]) -> Vec<String> {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|doc| {
            doc.get("projects").and_then(|p| p.as_array()).map(|arr| {
                arr.iter()
                    .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
                    .map(str::to_string)
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// The global HTML lists one `/simple/<name>/` href per package.
fn global_names_from_html(bytes: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(bytes);
    let mut names: Vec<String> = text
        .split("href=\"/simple/")
        .skip(1)
        .filter_map(|rest| rest.split('/').next())
        .map(str::to_string)
        .collect();
    names.sort();
    names.dedup();
    names
}

// ---------------------------------------------------------------------------
// Fault plan + per-node storage view.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum OpFate {
    /// Proceed after a virtual delay.
    Ok,
    /// Surface an availability error after a virtual delay.
    Fail,
    /// The node loses power at this op boundary: park forever (the driver
    /// aborts the task) and mark the node crashed.
    Crash,
}

/// Seed-derived fault schedule plus the shared trace. One per run.
struct FaultPlan {
    /// Per-op randomness, pre-derived from the seed so fate depends only on
    /// the global op sequence number (deterministic under deterministic
    /// scheduling, which the trace hash then verifies).
    op_seq: AtomicU64,
    fail_percent: u64,
    /// Node crash points: op-sequence numbers at which the issuing node dies.
    crash_at: Mutex<BTreeMap<u64, ()>>,
    crashed: Vec<AtomicBool>,
    /// While true, no faults fire (the heal/drain phase).
    healing: AtomicBool,
    rng_stream: Mutex<Rng>,
    trace: Mutex<TraceHasher>,
    /// Effect history for the audit-repair classifier. Pure observation.
    obs: Observer,
    /// The selected deliberate defect, `Break::None` in every ordinary run.
    brk: Break,
}

struct TraceHasher {
    hash: u64,
    events: u64,
    /// Full event log, kept only while diagnosing a determinism violation.
    log: Option<Vec<String>>,
}

impl TraceHasher {
    fn record(&mut self, parts: &[&str]) {
        // FNV-1a over the event tuple: cheap, stable, order-sensitive.
        for part in parts {
            for byte in part.as_bytes() {
                self.hash ^= u64::from(*byte);
                self.hash = self.hash.wrapping_mul(0x100000001B3);
            }
            self.hash ^= 0x1F;
            self.hash = self.hash.wrapping_mul(0x100000001B3);
        }
        self.events += 1;
        if let Some(log) = &mut self.log {
            log.push(parts.join(" "));
        }
    }
}

impl FaultPlan {
    fn new(seed: u64, nodes: usize, fail_percent: u64, brk: Break) -> Arc<Self> {
        Arc::new(FaultPlan {
            op_seq: AtomicU64::new(0),
            fail_percent,
            crash_at: Mutex::new(BTreeMap::new()),
            crashed: (0..nodes).map(|_| AtomicBool::new(false)).collect(),
            healing: AtomicBool::new(false),
            rng_stream: Mutex::new(Rng::new(seed ^ 0xFA_11_7E_57)),
            trace: Mutex::new(TraceHasher {
                hash: 0xcbf29ce484222325,
                events: 0,
                log: std::env::var_os("VOPR_TRACE").map(|_| Vec::new()),
            }),
            obs: Observer::default(),
            brk,
        })
    }

    fn heal(&self) {
        self.healing.store(true, Ordering::SeqCst);
    }

    fn schedule_crash_soon(&self, node: usize, within_ops: u64) {
        let at = self.op_seq.load(Ordering::SeqCst)
            + 1
            + self.rng_stream.lock().expect("rng lock").below(within_ops);
        self.crash_at
            .lock()
            .expect("crash lock")
            .insert(at | ((node as u64) << 56), ());
    }

    fn restart(&self, node: usize) {
        self.crashed[node].store(false, Ordering::SeqCst);
        let mut pending = self.crash_at.lock().expect("crash lock");
        pending.retain(|key, _| (key >> 56) as usize != node);
    }

    fn node_crashed(&self, node: usize) -> bool {
        self.crashed[node].load(Ordering::SeqCst)
    }

    /// Decide this op's fate and unique virtual latency.
    fn admit(
        &self,
        node: usize,
        bucket: usize,
        op: &str,
        key: &str,
    ) -> (OpFate, std::time::Duration) {
        let seq = self.op_seq.fetch_add(1, Ordering::SeqCst);
        let jitter = {
            let mut rng = self.rng_stream.lock().expect("rng lock");
            rng.below(1000)
        };
        // Unique deadline per op: total order over wakeups.
        let delay = std::time::Duration::from_nanos(jitter * 1_000 + seq + 1);
        self.trace.lock().expect("trace lock").record(&[
            &format!("n{node}b{bucket}"),
            op,
            key,
            &seq.to_string(),
        ]);
        if self.node_crashed(node) {
            return (OpFate::Crash, delay);
        }
        if self.healing.load(Ordering::SeqCst) {
            return (OpFate::Ok, delay);
        }
        let crash_due = {
            let mut pending = self.crash_at.lock().expect("crash lock");
            let due: Vec<u64> = pending
                .keys()
                .copied()
                .filter(|k| (k >> 56) as usize == node && (k & 0x00FF_FFFF_FFFF_FFFF) <= seq)
                .collect();
            for k in &due {
                pending.remove(k);
            }
            !due.is_empty()
        };
        if crash_due {
            self.crashed[node].store(true, Ordering::SeqCst);
            return (OpFate::Crash, delay);
        }
        let fail = {
            let mut rng = self.rng_stream.lock().expect("rng lock");
            rng.chance(self.fail_percent)
        };
        if fail {
            (OpFate::Fail, delay)
        } else {
            (OpFate::Ok, delay)
        }
    }

    fn trace_hash(&self) -> (u64, u64) {
        let trace = self.trace.lock().expect("trace lock");
        (trace.hash, trace.events)
    }

    fn take_log(&self) -> Option<Vec<String>> {
        self.trace.lock().expect("trace lock").log.take()
    }
}

/// One node's view of one bucket: every op consults the fault plan first.
struct FaultView {
    inner: Arc<SimStorage>,
    node: usize,
    bucket: usize,
    plan: Arc<FaultPlan>,
}

impl FaultView {
    /// Capture the body only for the two global-index keys (name-set parsing).
    fn global_body(&self, key: &str, bytes: &[u8]) -> Option<Vec<u8>> {
        (key == "simple/index.json" || key == "simple/index.html").then(|| bytes.to_vec())
    }

    /// `--break fanout` is armed: peer bucket 1 blackholes every chaos-phase op
    /// (below) *and* the `_repl/1/` note that is supposed to owe it never lands
    /// (`put_bytes`), so a publish acks with neither the copy nor the promise —
    /// a totality violation and nothing else. It heals with the fault plan, so
    /// the heal phase still converges every other oracle.
    fn fanout_break(&self) -> bool {
        self.plan.brk == Break::Fanout && !self.plan.healing.load(Ordering::SeqCst)
    }

    async fn gate(&self, op: &'static str, key: &str) -> Result<()> {
        let (fate, delay) = self.plan.admit(self.node, self.bucket, op, key);
        tokio::time::sleep(delay).await;
        match fate {
            OpFate::Ok if self.bucket == 1 && self.fanout_break() => Err(anyhow!(
                "vopr: --break fanout blackholed peer bucket 1 ({op} {key})"
            )),
            OpFate::Ok => Ok(()),
            OpFate::Fail => Err(anyhow!("vopr: injected storage failure ({op} {key})")),
            OpFate::Crash => {
                // Power cut at an op boundary: never resume. The driver
                // aborts this task; nothing after this await runs.
                std::future::pending::<()>().await;
                unreachable!("parked op resumed after node crash")
            }
        }
    }
}

#[async_trait::async_trait]
impl Storage for FaultView {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        self.gate("head", key).await?;
        self.inner.head_exists(key).await
    }
    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        self.gate("size", key).await?;
        self.inner.stored_size(key).await
    }
    async fn serve_artifact(
        &self,
        key: &str,
        range: Option<&str>,
    ) -> Result<axum::response::Response<axum::body::Body>> {
        self.gate("serve", key).await?;
        self.inner.serve_artifact(key, range).await
    }
    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>> {
        self.inner.presign_get(key, expires).await
    }
    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, ct: Option<&str>) -> Result<()> {
        if key == "simple/index.json" {
            let body = String::from_utf8_lossy(&bytes).into_owned();
            self.gate("put", &format!("{key} => {body}")).await?;
        } else {
            self.gate("put", key).await?;
        }
        if key.starts_with("_repl/1/") && self.fanout_break() {
            return Ok(()); // the note owing the blackholed peer is dropped
        }
        let body = self.global_body(key, &bytes);
        self.inner.put_bytes(key, bytes, ct).await?;
        self.plan
            .obs
            .observe_mutation(self.node, self.bucket, key, body.as_deref());
        Ok(())
    }
    async fn put_if_absent(&self, key: &str, bytes: Vec<u8>, ct: Option<&str>) -> Result<bool> {
        self.gate("put_if_absent", key).await?;
        let body = self.global_body(key, &bytes);
        let created = self.inner.put_if_absent(key, bytes, ct).await?;
        if created {
            self.plan
                .obs
                .observe_mutation(self.node, self.bucket, key, body.as_deref());
        }
        Ok(created)
    }
    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        ct: Option<&str>,
    ) -> Result<bool> {
        self.gate("put_file", key).await?;
        let created = self.inner.put_file_if_absent(key, path, ct).await?;
        if created {
            self.plan
                .obs
                .observe_mutation(self.node, self.bucket, key, None);
        }
        Ok(created)
    }
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.gate("get", key).await?;
        self.inner.get_bytes(key).await
    }
    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        self.gate("list_dir", dir_prefix).await?;
        let entries = self.inner.list_dir_entries(dir_prefix).await?;
        self.plan
            .obs
            .observe_list(self.node, self.bucket, dir_prefix);
        Ok(entries)
    }
    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.gate("list_all", prefix).await?;
        self.inner.list_all(prefix).await
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        self.gate("list_page", prefix).await?;
        self.inner.list_page(prefix, after, limit).await
    }
    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        let label = keys.first().map(String::as_str).unwrap_or("");
        self.gate("delete", label).await?;
        self.inner.delete_keys(keys).await?;
        for key in keys {
            if let Some(rest) = key.strip_prefix("_dirty/") {
                let pkg = rest.split('!').next().unwrap_or(rest);
                self.plan
                    .obs
                    .record(EffectKind::MarkerDel, self.node, self.bucket, pkg, key);
            } else if let Some((dest, pkg)) = parse_note_key(key) {
                self.plan
                    .obs
                    .record(EffectKind::NoteDel, self.node, dest, &pkg, key);
            } else {
                self.plan
                    .obs
                    .observe_mutation(self.node, self.bucket, key, None);
            }
        }
        Ok(())
    }
    fn supports_leases(&self) -> bool {
        true
    }
    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.gate("get_etag", key).await?;
        self.inner.get_with_etag(key).await
    }
    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        self.gate("put_inm", key).await?;
        let body = self.global_body(key, &bytes);
        let outcome = self.inner.put_if_none_match(key, bytes).await?;
        if outcome.is_some() {
            self.plan
                .obs
                .observe_mutation(self.node, self.bucket, key, body.as_deref());
        }
        Ok(outcome)
    }
    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        if key == "simple/index.json" {
            let body = String::from_utf8_lossy(&bytes).into_owned();
            self.gate("put_im", &format!("{key} if={etag} => {body}"))
                .await?;
        } else {
            self.gate("put_im", key).await?;
        }
        let body = self.global_body(key, &bytes);
        let outcome = self.inner.put_if_match(key, etag, bytes).await;
        if key == "simple/index.json" {
            if let Ok(result) = &outcome {
                self.plan.trace.lock().expect("trace lock").record(&[
                    "cas-outcome",
                    key,
                    if result.is_some() { "won" } else { "lost" },
                ]);
            }
        }
        if let Ok(Some(_)) = &outcome {
            self.plan
                .obs
                .observe_mutation(self.node, self.bucket, key, body.as_deref());
        }
        outcome
    }
}

// ---------------------------------------------------------------------------
// The fleet: N nodes over M shared buckets.
// ---------------------------------------------------------------------------

struct Node {
    state: Arc<AppState>,
    /// Live op tasks; aborted on crash, drained on restart.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

struct Fleet {
    buckets: Vec<Arc<SimStorage>>,
    nodes: Vec<Node>,
    plan: Arc<FaultPlan>,
    clock: Arc<SimClock>,
    ledger: Arc<Mutex<Ledger>>,
    /// One rebuild worker per bucket at a time — the serialization the bucket
    /// lease provides in production. The sloppy dual-leadership window (two
    /// live ticks on one bucket) is deliberately out of scope here: the
    /// event-protocol model covers it exhaustively and documents that only
    /// the audit heals its stale-rebuild clobber.
    tick_lock: Vec<Arc<tokio::sync::Mutex<()>>>,
    /// Monotonic logical-op counter: each spawned workload op and each direct
    /// heal-phase protocol call runs in its own `OP_ID` scope so the observer
    /// can attribute effects. Interior-mutable so `&self` helpers can bump it.
    next_op: std::cell::Cell<u64>,
}

#[derive(Default)]
struct Ledger {
    /// Acknowledged uploads: (pkg, filename) -> body bytes.
    acked: BTreeMap<(String, String), Vec<u8>>,
    /// Filenames an acknowledged (204) delete removed.
    deleted: std::collections::BTreeSet<(String, String)>,
    /// Per filename, whether the most recent *acknowledged* operation was the
    /// delete. `acked` and `deleted` are unordered sets, but resurrection is a
    /// last-writer question: re-publishing a filename after deleting it is
    /// legal, and only a filename whose latest ack was the 204 may not be
    /// standing in a bucket. Ledger inserts happen under this mutex immediately
    /// after the status code the client saw, so overwriting one flag per
    /// filename *is* that ordering — no separate sequence number needed.
    ///
    /// Today this is equivalent to `deleted`, because `publish_record`'s
    /// tombstone fence rejects any re-publish of a deleted private filename, so
    /// no ack can follow a 204. It is written the ordered way anyway: mirror
    /// filenames are re-fillable by design (see `delete_record`), so the day a
    /// legal resurrection path exists the unordered form would false-fail every
    /// one of them — and the tempting fix would be to weaken this oracle.
    last_ack_deleted: BTreeMap<(String, String), bool>,
    /// Publishes that acked while a peer bucket held neither the record nor a
    /// note owing it — totality failures, recorded at the ack.
    ack_totality: Vec<String>,
    published_attempts: u64,
    delete_attempts: u64,
}

fn build_node_state(
    buckets: &[Arc<SimStorage>],
    node: usize,
    plan: &Arc<FaultPlan>,
) -> Arc<AppState> {
    let handles: Vec<BucketHandle> = buckets
        .iter()
        .enumerate()
        .map(|(idx, bucket)| BucketHandle {
            storage: Arc::new(FaultView {
                inner: bucket.clone(),
                node,
                bucket: idx,
                plan: plan.clone(),
            }) as Arc<dyn Storage>,
            name: format!("bucket-{idx}"),
        })
        .collect();
    let mut state = AppState::headless(handles[0].storage.clone());
    if handles.len() > 1 {
        state.buckets = Arc::new(BucketSet::new(handles));
    }
    // Tight grace keeps crashed-writer healing inside short simulated runs.
    state.intent_grace = time::Duration::seconds(60);
    state.fanout_grace = std::time::Duration::from_secs(5);
    Arc::new(state)
}

impl Fleet {
    fn new(
        seed: u64,
        nodes: usize,
        bucket_count: usize,
        fail_percent: u64,
        brk: Break,
        clock: Arc<SimClock>,
    ) -> Fleet {
        let plan = FaultPlan::new(seed, nodes, fail_percent, brk);
        let buckets: Vec<Arc<SimStorage>> = (0..bucket_count)
            .map(|_| SimStorage::new(clock.clone()))
            .collect();
        let nodes = (0..nodes)
            .map(|node| Node {
                state: build_node_state(&buckets, node, &plan),
                tasks: Vec::new(),
            })
            .collect();
        let tick_lock = (0..bucket_count)
            .map(|_| Arc::new(tokio::sync::Mutex::new(())))
            .collect();
        Fleet {
            buckets,
            nodes,
            plan,
            clock,
            ledger: Arc::new(Mutex::new(Ledger::default())),
            tick_lock,
            next_op: std::cell::Cell::new(0),
        }
    }

    /// Next logical-op id for effect attribution (never zero; zero is the
    /// "no open inferred session" sentinel in `Observer::attribution`).
    fn fresh_op(&self) -> u64 {
        self.next_op.set(self.next_op.get() + 1);
        self.next_op.get()
    }

    fn spawn_on(&mut self, node: usize, fut: impl Future<Output = ()> + 'static) {
        let handle = tokio::task::spawn_local(OP_ID.scope(self.fresh_op(), fut));
        self.nodes[node].tasks.push(handle);
    }

    fn crash_node(&mut self, node: usize) {
        // The plan already (or will) park the node's ops; abort everything it
        // had in flight — in-memory state dies with the process.
        for task in self.nodes[node].tasks.drain(..) {
            task.abort();
        }
    }

    fn restart_node(&mut self, node: usize) {
        self.crash_node(node);
        self.plan.restart(node);
        // Cold caches: a fresh AppState over the same buckets.
        let plan = self.plan.clone();
        self.nodes[node].state = build_node_state(&self.buckets, node, &plan);
    }
}

// ---------------------------------------------------------------------------
// Workload ops.
// ---------------------------------------------------------------------------

/// ACK_TOTALITY, evaluated at the ack: dev/DESIGN.md's totality principle says
/// an upload is not acknowledged until every bucket either holds it or is owed
/// it by a durable note. Checked here rather than at quiescence because the heal
/// phase's `reconcile` launders exactly this defect — it copies a single-bucket
/// ack into place before any invariant looks.
///
/// Pure by construction: `SimStorage::keys()` reads the raw bucket, never the
/// traced `FaultView`, so it consumes no op-sequence number, draws no rng, and
/// records no trace event. No awaits.
///
/// SCOPE: `publish_record` only. Commit bf913b9 made proxy-cache fills
/// replicate asynchronously with no pre-ack fan-out by design
/// (`replicate::spawn_proxy_fill_notes` writes its notes after the response is
/// served), so this must never be generalized to all durable writes.
fn ack_totality_failures(
    buckets: &[Arc<SimStorage>],
    selected: usize,
    pkg: &str,
    fname: &str,
) -> Vec<String> {
    if buckets.len() < 2 {
        return Vec::new();
    }
    REACH.hit(R::AckTotality);
    let akey = format!("packages/{pkg}/{fname}");
    let mkey = format!("{akey}.meta.json");
    let selected_keys = buckets[selected].keys();
    buckets
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != selected)
        .filter(|(i, peer)| {
            let keys = peer.keys();
            let has_record = keys.contains(&akey) && keys.contains(&mkey);
            let owed = format!("_repl/{i}/{pkg}/{fname}!");
            !has_record && !selected_keys.iter().any(|k| k.starts_with(&owed))
        })
        .map(|(i, _)| {
            format!(
                "ACK_TOTALITY: publish of {akey} acked while bucket {i} held neither the record \
                 nor a _repl/{i}/ note — the 200 claimed a durability it did not have (publish \
                 only; proxy fills are async by design, see bf913b9)"
            )
        })
        .collect()
}

async fn op_publish(
    state: Arc<AppState>,
    ledger: Arc<Mutex<Ledger>>,
    clock: Arc<SimClock>,
    buckets: Vec<Arc<SimStorage>>,
    pkg: String,
    file: u8,
    variant: u8,
) {
    use sha2::digest::Digest;
    let body = body_bytes(&pkg, file, variant);
    let fname = filename(&pkg, file);
    ledger.lock().expect("ledger lock").published_attempts += 1;
    let sha256 = {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&body);
        format!("{:x}", hasher.finalize())
    };
    let req = pypiron::PublishRequest {
        pkg: pkg.clone(),
        filename: fname.clone(),
        sha256,
        size: body.len() as u64,
        version: format!("{}.0", file + 1),
        requires_python: None,
        is_mirror: false,
        upload_time: clock.now_rfc3339(),
        yanked: pypiron::sidecar::Yanked::Flag(false),
        wheel_metadata: None,
        is_wheel: false,
        provenance: None,
        body: pypiron::PublishBody::Bytes(body.clone()),
    };
    let pinned = state.pin();
    if pypiron::publish_record(&state, &pinned, req).await.is_ok() {
        let mut failures = ack_totality_failures(&buckets, pinned.index, &pkg, &fname);
        let mut ledger = ledger.lock().expect("ledger lock");
        ledger.ack_totality.append(&mut failures);
        ledger.acked.insert((pkg.clone(), fname.clone()), body);
        ledger.last_ack_deleted.insert((pkg, fname), false);
    }
}

async fn op_delete(state: Arc<AppState>, ledger: Arc<Mutex<Ledger>>, pkg: String, file: u8) {
    let fname = filename(&pkg, file);
    ledger.lock().expect("ledger lock").delete_attempts += 1;
    let pinned = state.pin();
    if pypiron::delete_record(&state, &pinned, &pkg, &fname)
        .await
        .is_ok()
    {
        let mut ledger = ledger.lock().expect("ledger lock");
        ledger.deleted.insert((pkg.clone(), fname.clone()));
        ledger.last_ack_deleted.insert((pkg, fname), true);
    }
}

async fn op_tick(state: Arc<AppState>, lease: Arc<tokio::sync::Mutex<()>>) {
    let pinned = state.pin();
    let _guard = lease.lock().await;
    let _ = worker::tick(&state, &pinned).await;
}

async fn op_sweep(state: Arc<AppState>) {
    let _ = replicate::sweep_all_markers(&state).await;
}

async fn op_reconcile(state: Arc<AppState>) {
    let pinned = state.pin();
    let _ = replicate::reconcile(&state, &pinned).await;
}

// ---------------------------------------------------------------------------
// One seeded run.
// ---------------------------------------------------------------------------

struct RunOutcome {
    trace_hash: u64,
    trace_events: u64,
    /// Fingerprint of the final world (bucket bytes + ledger) — see [`state_hash`].
    state_hash: u64,
    trace_log: Option<Vec<String>>,
    acked: usize,
    /// Publishes that acked without fleet-wide durability or a note owing it.
    ack_totality: u64,
    audit_view_repairs: u64,
    /// Audit repairs split by taxonomy class: [ordering, premature, race].
    repairs_by_class: [u64; 3],
    violations: Vec<String>,
}

async fn run_seed(seed: u64, profile: Profile, rerun: bool) -> RunOutcome {
    let Profile {
        nodes,
        buckets,
        packages,
        files,
        ops,
        fail_percent,
        weights,
        brk,
    } = profile;
    let weight_total: u64 = weights.iter().map(|w| u64::from(*w)).sum();
    let start =
        time::OffsetDateTime::parse(SIM_START, &time::format_description::well_known::Rfc3339)
            .expect("valid sim start");
    let clock = SimClock::new(start);
    let _guard = clock.install_global();
    let mut fleet = Fleet::new(seed, nodes, buckets, fail_percent, brk, clock.clone());
    let mut rng = Rng::new(seed);

    // ---- Chaos phase: seed-driven interleaving of ops, faults, and time.
    for _ in 0..ops {
        let node = rng.below(nodes as u64) as usize;
        if fleet.plan.node_crashed(node) && !rng.chance(30) {
            // A crashed node mostly stays down this step; sometimes restarts.
            continue;
        }
        if fleet.plan.node_crashed(node) {
            fleet.restart_node(node);
            continue;
        }
        let state = fleet.nodes[node].state.clone();
        let ledger = fleet.ledger.clone();
        match pick_op(&weights, weight_total, &mut rng) {
            0 => {
                let pkg = PACKAGE_NAMES[rng.below(packages as u64) as usize].to_string();
                let file = rng.below(u64::from(files)) as u8;
                let variant = rng.below(2) as u8;
                let clock = fleet.clock.clone();
                let buckets = fleet.buckets.clone();
                fleet.spawn_on(
                    node,
                    op_publish(state, ledger, clock, buckets, pkg, file, variant),
                );
            }
            1 => {
                let pkg = PACKAGE_NAMES[rng.below(packages as u64) as usize].to_string();
                let file = rng.below(u64::from(files)) as u8;
                fleet.spawn_on(node, op_delete(state, ledger, pkg, file));
            }
            2 => {
                let lease = fleet.tick_lock[0].clone(); // every pin selects bucket 0
                fleet.spawn_on(node, op_tick(state, lease));
            }
            3 => {
                if buckets > 1 {
                    fleet.spawn_on(node, op_sweep(state));
                }
            }
            4 => {
                if buckets > 1 {
                    fleet.spawn_on(node, op_reconcile(state));
                }
            }
            5 => {
                // Jump past the intent grace: crashed writers become healable.
                clock.advance(std::time::Duration::from_secs(90));
            }
            6 => {
                fleet.plan.schedule_crash_soon(node, 12);
                // Give the doomed node's in-flight work a chance to hit the
                // crash point before the driver aborts it.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                if fleet.plan.node_crashed(node) {
                    fleet.crash_node(node);
                }
            }
            _ => {
                clock.advance(std::time::Duration::from_secs(1));
            }
        }
        // Let the paused runtime interleave whatever is in flight.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // ---- Heal phase: stop faults, restart everyone, then drive the fleet
    // the way production does — worker ticks, note sweeps, warm drains, the
    // pairwise tree diff, and the leader audit — until storage reaches a
    // fixpoint with no protocol debris. The audit matters: the event-protocol
    // model proves concurrent unleased rebuilds can strand a stale view until
    // the audit's fingerprint diff repairs it, so the system-level claim the
    // VOPR checks is "ticks + sweeps + reconcile + audit converge".
    let mut violations_pre: Vec<String> = Vec::new();
    fleet.plan.heal();
    for node in 0..nodes {
        fleet.restart_node(node);
    }
    clock.advance(std::time::Duration::from_secs(120)); // all intents stale
    let mut quiesced = false;
    let mut audit_view_repairs: u64 = 0;
    let mut repairs_by_class: [u64; 3] = [0; 3]; // [ordering, premature, race]
    let mut last_fingerprint: Option<Vec<BTreeMap<String, Vec<u8>>>> = None;
    // LIVENESS is a boolean, so a run with nothing to drain would "pass" it
    // having checked nothing; it counts as executed only when quiescence had
    // real work to do. Sampled here, before the first drain pass — the loop's
    // own `markers_left` runs *after* a full pass across every node and so can
    // never see the debris that pass just cleared. Raw `dump()`, never a
    // `FaultView`: no op-sequence number, no rng draw, no trace event.
    let mut saw_debris = fleet.buckets.iter().any(|bucket| {
        bucket
            .dump()
            .keys()
            .any(|k| k.starts_with("_dirty/") || k.starts_with("_repl/"))
    });
    for round in 0..HEAL_ROUNDS {
        // Drain markers across every node until none remain (bounded).
        for pass in 0..DRAIN_PASSES {
            for node in 0..nodes {
                let state = fleet.nodes[node].state.clone();
                let pinned = state.pin();
                OP_ID
                    .scope(fleet.fresh_op(), async {
                        let _ = worker::tick(&state, &pinned).await;
                    })
                    .await;
                if buckets > 1 {
                    OP_ID
                        .scope(fleet.fresh_op(), async {
                            let _ = replicate::sweep_all_markers(&state).await;
                        })
                        .await;
                    for (idx, handle) in state.buckets.handles().iter().enumerate() {
                        if idx != pinned.index {
                            OP_ID
                                .scope(fleet.fresh_op(), async {
                                    let _ = worker::drain_dirty_uncached(
                                        &state,
                                        handle.storage.as_ref(),
                                    )
                                    .await;
                                })
                                .await;
                        }
                    }
                }
            }
            let markers_left = fleet.buckets.iter().any(|bucket| {
                bucket
                    .dump()
                    .keys()
                    .any(|k| k.starts_with("_dirty/") || k.starts_with("_repl/"))
            });
            saw_debris |= markers_left;
            Reach::peak(&REACH.peak_drains, pass + 1);
            if !markers_left {
                break;
            }
            clock.advance(std::time::Duration::from_secs(90));
        }
        // Which node plays leader this round. Production elects the reconcile
        // and audit leader by bucket lease and re-elects it the moment the
        // holder dies, so the audit is not a property of node 0 — and pinning
        // it there understated both severity and detection rate. Node 0 is
        // disproportionately the crashed node, whose restart dropped its
        // in-memory global-name cache; a cold cache reloads that set from
        // storage, so drift a warm CAS winner would have left standing (a HARD
        // `VERIFY: ... stale-global-index`) healed at the audit and reported as
        // a SOFT `AUDIT_*` repair instead. Rotating by (seed, round) is one
        // line, keeps the run a pure function of the seed — `--seed N` still
        // reproduces exactly — and exercises the leadership handoff a crash
        // forces in production. Auditing every node in turn each round would
        // N-times the heal cost for coverage a soak already gets from rotation.
        let leader_idx = (seed.wrapping_add(round) % nodes as u64) as usize;
        // The lost-copy backstop first: the tree diff converges truth a
        // crashed fan-out left behind and brackets its work in markers...
        let leader = fleet.nodes[leader_idx].state.clone();
        let pinned = leader.pin();
        if buckets > 1 {
            OP_ID
                .scope(fleet.fresh_op(), async {
                    let _ = replicate::reconcile(&leader, &pinned).await;
                })
                .await;
            // ...which this drain pass consumes, rebuilding the affected
            // views, before the audits are allowed to look.
            for _ in 0..3 {
                OP_ID
                    .scope(fleet.fresh_op(), async {
                        let _ = worker::tick(&leader, &pinned).await;
                    })
                    .await;
                OP_ID
                    .scope(fleet.fresh_op(), async {
                        let _ = replicate::sweep_all_markers(&leader).await;
                    })
                    .await;
                for (idx, handle) in leader.buckets.handles().iter().enumerate() {
                    if idx != pinned.index {
                        OP_ID
                            .scope(fleet.fresh_op(), async {
                                let _ =
                                    worker::drain_dirty_uncached(&leader, handle.storage.as_ref())
                                        .await;
                            })
                            .await;
                    }
                }
                clock.advance(std::time::Duration::from_secs(90));
            }
        }
        if brk != Break::None && round == 0 {
            apply_break(
                brk,
                true,
                rerun,
                &fleet.buckets,
                &fleet.plan.obs,
                &fleet.ledger,
            )
            .await;
        }
        // With ticks lease-serialized and the diff drained, the marker path
        // must have converged every view on its own — snapshot them so an
        // audit that "helpfully" repaired a view is visible as the signal-loss
        // bug it would otherwise hide.
        let views_before_audit: Vec<BTreeMap<String, Vec<u8>>> = fleet
            .buckets
            .iter()
            .map(|bucket| {
                bucket
                    .dump()
                    .into_iter()
                    .filter(|(k, _)| k.starts_with("simple/"))
                    .collect()
            })
            .collect();
        // Every effect the classifier explains happened before this round's
        // audits; the audits' own repair writes carry seq >= boundary and are
        // excluded from the analysis (they are the repair, not its cause).
        let boundary = fleet.plan.obs.seq.load(Ordering::SeqCst);
        if let Err(e) = OP_ID
            .scope(fleet.fresh_op(), worker::audit(&leader, &pinned, false))
            .await
        {
            violations_pre.push(format!(
                "AUDIT: leader audit (node {leader_idx}) failed on round {round}: {e:?}"
            ));
        }
        // Every warm bucket gets the audit it receives in production before it
        // ever serves (on boot and on every selection switch): run it through
        // a throwaway single-bucket state so the leader's selected-bucket name
        // cache is never polluted with another bucket's namespace.
        for bucket in fleet.buckets.iter().skip(1) {
            let audit_state = Arc::new(pypiron::sim::single_bucket_state(
                bucket.clone() as Arc<dyn Storage>
            ));
            let audit_pin = audit_state.pin();
            if let Err(e) = OP_ID
                .scope(
                    fleet.fresh_op(),
                    worker::audit(&audit_state, &audit_pin, false),
                )
                .await
            {
                violations_pre.push(format!(
                    "AUDIT: warm-bucket audit failed on round {round}: {e:?}"
                ));
            }
        }
        clock.advance(std::time::Duration::from_secs(90));
        let views_after_audit: Vec<BTreeMap<String, Vec<u8>>> = fleet
            .buckets
            .iter()
            .map(|bucket| {
                bucket
                    .dump()
                    .into_iter()
                    .filter(|(k, _)| k.starts_with("simple/"))
                    .collect()
            })
            .collect();
        if views_before_audit != views_after_audit {
            // The audit repaired views the tick/sweep path had not converged.
            // Classify each repair from the effect history captured before this
            // round's audits: ORDERING and PREMATURE-CONSUMPTION are avoidable
            // signal-loss bugs and fail the seed in every profile; the
            // documented CONCURRENT-RACE clobber is a reported statistic (the
            // audit is its designed backstop). A crash-only run additionally
            // keeps its blanket gate: markers alone must converge over every
            // crash schedule, so ANY repair there is a violation.
            audit_view_repairs += 1;
            // Deduped diff: one entry per (bucket, key) whose bytes actually
            // changed (the old before.keys().chain(after.keys()) double-listed
            // keys present on both sides).
            let mut diffs: Vec<ViewDiff> = Vec::new();
            for (idx, (before, after)) in views_before_audit
                .iter()
                .zip(views_after_audit.iter())
                .enumerate()
            {
                let mut keys: std::collections::BTreeSet<&String> = before.keys().collect();
                keys.extend(after.keys());
                for k in keys {
                    if before.get(k) != after.get(k) {
                        diffs.push((
                            idx,
                            k.clone(),
                            before.get(k).cloned(),
                            after.get(k).cloned(),
                        ));
                    }
                }
            }
            let findings = classify_round(&fleet.plan.obs.snapshot(), boundary, &diffs);
            for f in &findings {
                repairs_by_class[(f.class - 1) as usize] += 1;
            }
            // Per-bucket "key before=.. after=.." dump for message context.
            let bucket_dump = |b: usize| -> Vec<String> {
                diffs
                    .iter()
                    .filter(|(idx, _, _, _)| *idx == b)
                    .map(|(idx, k, before, after)| {
                        format!(
                            "bucket{idx}:{k} before={:?} after={:?}",
                            before
                                .as_ref()
                                .map(|v| String::from_utf8_lossy(v).into_owned()),
                            after
                                .as_ref()
                                .map(|v| String::from_utf8_lossy(v).into_owned()),
                        )
                    })
                    .collect()
            };
            let changed: Vec<String> = (0..fleet.buckets.len()).flat_map(&bucket_dump).collect();
            if fail_percent == 0 {
                // Crash-only: keep the blanket AUDIT_REPAIRED_VIEWS gate exactly
                // as strict as before; append the findings for diagnosability.
                let findings_desc: Vec<String> = findings
                    .iter()
                    .map(|f| {
                        format!(
                            "[class {}] bucket{} {}: {}",
                            f.class, f.bucket, f.subject, f.detail
                        )
                    })
                    .collect();
                violations_pre.push(format!(
                    "AUDIT_REPAIRED_VIEWS: crash-only run needed the audit to converge views \
                     — the marker protocol failed to self-heal: {changed:#?}\nfindings: {findings_desc:#?}"
                ));
            } else {
                // Fault mode: ORDERING/PREMATURE-CONSUMPTION are violations;
                // CONCURRENT-RACE is only a statistic. Messages are
                // seed-agnostic (bucket, subject, detail, that bucket's diff) so
                // the same bug groups across seeds.
                for f in &findings {
                    match f.class {
                        1 => violations_pre.push(format!(
                            "AUDIT_ORDERING: bucket{} {} — {} | changed: {:#?}",
                            f.bucket,
                            f.subject,
                            f.detail,
                            bucket_dump(f.bucket)
                        )),
                        2 => violations_pre.push(format!(
                            "AUDIT_PREMATURE_CONSUMPTION: bucket{} {} — {} | changed: {:#?}",
                            f.bucket,
                            f.subject,
                            f.detail,
                            bucket_dump(f.bucket)
                        )),
                        _ => {}
                    }
                }
                if std::env::var_os("VOPR_LOG_REPAIRS").is_some() {
                    eprintln!(
                        "vopr: seed {seed} round {round} — audit repaired {} view key(s) under fault injection (fail-percent {fail_percent}); the marker path fell through to the tier-3 backstop: {changed:#?}",
                        changed.len()
                    );
                    for f in &findings {
                        eprintln!(
                            "  [class {}] bucket{} {}: {}",
                            f.class, f.bucket, f.subject, f.detail
                        );
                    }
                }
            }
            // Warm-bucket audits write straight to SimStorage (bypassing the
            // traced view), so their repair writes are invisible to the
            // observer. Synthesize them — one fresh inferred attribution for the
            // round — so a later round that re-examines the same package sees
            // the convergence they achieved rather than misreading a stale gap.
            if diffs.iter().any(|(idx, _, _, _)| *idx >= 1) {
                let synth_att = fleet.plan.obs.fresh_synth_att();
                for (idx, key, _before, after) in &diffs {
                    if *idx >= 1 {
                        fleet
                            .plan
                            .obs
                            .synth_view_write(*idx, key, after.as_deref(), synth_att);
                    }
                }
            }
        }
        // Fixpoint: no markers and no storage change over a full round.
        let snapshot: Vec<BTreeMap<String, Vec<u8>>> =
            fleet.buckets.iter().map(|bucket| bucket.dump()).collect();
        let markers_left = snapshot.iter().any(|dump| {
            dump.keys()
                .any(|k| k.starts_with("_dirty/") || k.starts_with("_repl/"))
        });
        saw_debris |= markers_left;
        Reach::peak(&REACH.peak_rounds, round + 1);
        if !markers_left && last_fingerprint.as_ref() == Some(&snapshot) {
            quiesced = true;
            break;
        }
        last_fingerprint = Some(snapshot);
    }
    if saw_debris {
        REACH.hit(R::Liveness);
    }

    // ---- Invariants. The ledger stays unlocked until the last await is behind
    // us: `verify_storage` is async, and a std guard may not cross an await.
    let mut violations = violations_pre;
    if !quiesced {
        let leftovers: Vec<String> = fleet
            .buckets
            .iter()
            .enumerate()
            .flat_map(|(idx, bucket)| {
                bucket
                    .dump()
                    .into_keys()
                    .filter(|k| k.starts_with("_dirty/") || k.starts_with("_repl/"))
                    .map(move |k| format!("bucket{idx}:{k}"))
            })
            .collect();
        violations.push(format!(
            "LIVENESS: fleet did not quiesce within the drain budget; leftover markers: {leftovers:?}"
        ));
    }
    if brk != Break::None {
        apply_break(
            brk,
            false,
            rerun,
            &fleet.buckets,
            &fleet.plan.obs,
            &fleet.ledger,
        )
        .await;
    }
    let dumps: Vec<BTreeMap<String, Vec<u8>>> =
        fleet.buckets.iter().map(|bucket| bucket.dump()).collect();

    // VIEWS == TRUTH, byte-strict: the product's own oracle (`pypiron verify`)
    // re-renders every view from that bucket's truth and diffs the bytes. Run
    // against the raw `SimStorage` — never the traced `FaultView` — so it
    // consumes no op-sequence numbers and cannot shift the fault schedule.
    for (idx, bucket) in fleet.buckets.iter().enumerate() {
        if dumps[idx].keys().any(|k| k.starts_with("packages/")) {
            REACH.hit(R::Verify); // an empty bucket re-renders nothing to diff
        }
        match pypiron::verify::verify_storage(bucket.as_ref()).await {
            Ok(report) => violations.extend(report.divergences.into_iter().map(|d| {
                format!(
                    "VERIFY: bucket {idx} diverged from its own truth: {} {} — {}",
                    d.kind, d.package, d.detail
                )
            })),
            Err(e) => violations.push(format!("VERIFY: bucket {idx} could not be verified: {e:?}")),
        }
    }

    let ledger = fleet.ledger.lock().expect("ledger lock");

    // Convergence: identical truth + views across buckets.
    if buckets > 1 {
        let project = |dump: &BTreeMap<String, Vec<u8>>| -> BTreeMap<String, Vec<u8>> {
            let mut projected: BTreeMap<String, Vec<u8>> = dump
                .iter()
                .filter(|(k, _)| k.starts_with("packages/") || k.starts_with("simple/"))
                .filter(|(k, _)| {
                    // The global views are compared as a parsed name set below:
                    // an absent global index and an empty one both derive "no
                    // packages" from truth (the HTML is the same pure function
                    // of that set). Per-package views stay byte-strict.
                    *k != "simple/index.json" && *k != "simple/index.html"
                })
                .map(|(k, v)| {
                    if k.ends_with("/.origin") {
                        // Claims are equal when their *state* is equal; the
                        // nonce is a fresh ABA guard on every write by design.
                        let state = serde_json::from_slice::<serde_json::Value>(v)
                            .ok()
                            .and_then(|doc| {
                                doc.get("origin")
                                    .and_then(|o| o.as_str())
                                    .map(str::to_string)
                            })
                            .unwrap_or_else(|| String::from_utf8_lossy(v).into_owned());
                        (k.clone(), state.into_bytes())
                    } else {
                        (k.clone(), v.clone())
                    }
                })
                .collect();
            let names: Vec<String> = dump
                .get("simple/index.json")
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
                .and_then(|doc| {
                    doc.get("projects").and_then(|p| p.as_array()).map(|arr| {
                        let mut names: Vec<String> = arr
                            .iter()
                            .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
                            .map(str::to_string)
                            .collect();
                        names.sort();
                        names
                    })
                })
                .unwrap_or_default();
            projected.insert(
                "simple/index.json#names".to_string(),
                names.join(",").into_bytes(),
            );
            projected
        };
        let first = project(&dumps[0]);
        for (idx, dump) in dumps.iter().enumerate().skip(1) {
            let other = project(dump);
            // `project` always inserts the synthetic name-set key, so a pair of
            // empty buckets compares equal having compared nothing.
            if first.len() > 1 || other.len() > 1 {
                REACH.hit(R::Convergence);
            }
            if other != first {
                let diff: Vec<&String> = first
                    .keys()
                    .chain(other.keys())
                    .filter(|k| first.get(*k) != other.get(*k))
                    .collect();
                let detail: Vec<String> = diff
                    .iter()
                    .take(10)
                    .map(|k| {
                        format!(
                            "{k}: b0={:?} b{idx}={:?}",
                            first
                                .get(*k)
                                .map(|v| String::from_utf8_lossy(v).into_owned()),
                            other
                                .get(*k)
                                .map(|v| String::from_utf8_lossy(v).into_owned()),
                        )
                    })
                    .collect();
                violations.push(format!(
                    "CONVERGENCE: bucket 0 and bucket {idx} differ on {} keys: {detail:#?}\nb0 keys: {:?}\nb{idx} keys: {:?}",
                    diff.len(),
                    dumps[0].keys().collect::<Vec<_>>(),
                    dumps[idx].keys().collect::<Vec<_>>(),
                ));
                break;
            }
        }
    }

    for (bucket_idx, dump) in dumps.iter().enumerate() {
        // Durability of acknowledged uploads.
        for ((pkg, fname), body) in &ledger.acked {
            let akey = format!("packages/{pkg}/{fname}");
            let tomb = dump.contains_key(&format!("{akey}.tombstone"));
            let frozen = dump.contains_key(&format!("{akey}.frozen"));
            if ledger.deleted.contains(&(pkg.clone(), fname.clone())) || tomb || frozen {
                continue; // authorized removal or conflict freeze
            }
            REACH.hit(R::Durability);
            match dump.get(&akey) {
                None => violations.push(format!(
                    "DURABILITY: acked {akey} missing on bucket {bucket_idx}"
                )),
                Some(stored) if stored != body => violations.push(format!(
                    "DURABILITY: acked {akey} byte-corrupt on bucket {bucket_idx}"
                )),
                Some(_) => {
                    if !dump.contains_key(&format!("{akey}.meta.json")) {
                        violations.push(format!(
                            "DURABILITY: acked {akey} lost its sidecar on bucket {bucket_idx}"
                        ));
                    }
                    REACH.hit(R::Visibility);
                    let view = dump
                        .get(&format!("simple/{pkg}/index.json"))
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default();
                    if !view.contains(fname.as_str()) {
                        violations.push(format!(
                            "VISIBILITY: acked {akey} not listed in bucket {bucket_idx}'s view"
                        ));
                    }
                }
            }
        }

        // Tombstone monotonicity: a filename whose latest ack was the 204 must
        // not be standing in any bucket without its tombstone. This is the
        // compromised-artifact removal path — a silent resurrection passes every
        // other oracle here, since `ledger.deleted` is only ever an exemption.
        for ((pkg, fname), deleted_last) in &ledger.last_ack_deleted {
            if !*deleted_last {
                continue; // a live filename says nothing about resurrection
            }
            REACH.hit(R::Tombstone);
            let akey = format!("packages/{pkg}/{fname}");
            if dump.contains_key(&akey) && !dump.contains_key(&format!("{akey}.tombstone")) {
                violations.push(format!(
                    "TOMBSTONE_MONOTONICITY: {akey} was acked-deleted (204) but bucket \
                     {bucket_idx} holds the artifact with no .tombstone — a deleted filename \
                     came back"
                ));
            }
        }

        // Conservation: acked bytes must exist somewhere unless an authorized
        // delete or freeze removed them. The tombstone is the point of no
        // return, so storage evidence (a tombstone/freeze marker on any
        // bucket) exempts — an interrupted delete that crashed after its
        // tombstone but before its 204 has still legitimately destroyed.
        for ((pkg, fname), body) in &ledger.acked {
            let akey = format!("packages/{pkg}/{fname}");
            let removal_authorized = ledger.deleted.contains(&(pkg.clone(), fname.clone()))
                || dumps.iter().any(|d| {
                    d.contains_key(&format!("{akey}.tombstone"))
                        || d.contains_key(&format!("{akey}.frozen"))
                });
            if removal_authorized {
                continue;
            }
            if bucket_idx == 0 {
                REACH.hit(R::Conservation); // the fleet-wide scan below runs once
            }
            let survives = dump.values().any(|stored| stored == body);
            if !survives && bucket_idx == 0 {
                // Checked once (bucket 0): quarantine may hold it on either
                // bucket, so scan them all before declaring loss.
                let anywhere = dumps
                    .iter()
                    .any(|d| d.values().any(|stored| stored == body));
                if !anywhere {
                    violations.push(format!(
                        "CONSERVATION: acked bytes of packages/{pkg}/{fname} vanished from every bucket"
                    ));
                }
            }
        }
    }

    // Crash-only: the protocol must fan out or leave a note on every schedule,
    // so a totality miss is a hard violation. Under injected storage failures a
    // note write can itself fail, so there it is a reported statistic — the same
    // split the audit-repair taxonomy uses.
    if fail_percent == 0 {
        violations.extend(ledger.ack_totality.iter().cloned());
    }

    let (trace_hash, trace_events) = fleet.plan.trace_hash();
    RunOutcome {
        trace_hash,
        trace_events,
        state_hash: state_hash(&dumps, &ledger),
        trace_log: fleet.plan.take_log(),
        acked: ledger.acked.len(),
        ack_totality: ledger.ack_totality.len() as u64,
        audit_view_repairs,
        repairs_by_class,
        violations,
    }
}

/// FNV-1a over one field, with the same delimiter mix `TraceHasher` uses.
fn mix(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x100000001B3);
    }
    *hash ^= 0x1F;
    *hash = hash.wrapping_mul(0x100000001B3);
}

/// Fingerprint of the world a run ended in: every bucket's bytes plus the
/// ledger. The trace hash proves two runs issued the same *calls*; this proves
/// they produced the same *bytes*, catching nondeterminism downstream of the op
/// sequence (an unvirtualized clock read, say) that a trace comparison reports
/// as green.
fn state_hash(dumps: &[BTreeMap<String, Vec<u8>>], ledger: &Ledger) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for dump in dumps {
        for (key, bytes) in dump {
            mix(&mut hash, key.as_bytes());
            mix(&mut hash, bytes);
        }
    }
    for ((pkg, fname), body) in &ledger.acked {
        mix(&mut hash, pkg.as_bytes());
        mix(&mut hash, fname.as_bytes());
        mix(&mut hash, body);
    }
    for (pkg, fname) in &ledger.deleted {
        mix(&mut hash, pkg.as_bytes());
        mix(&mut hash, fname.as_bytes());
    }
    hash
}

// ---------------------------------------------------------------------------
// Audit-repair classifier (pure: no rng, no awaits, no storage).
//
// Given the effect history, the seq boundary captured just before a round's
// audits, and the view keys that round's audit had to change, explain each
// repair. The per-package tests run in severity order and the first match
// wins, so a poisoned rebuild that also raced is reported as the (worse)
// premature-consumption it is, never downgraded to the benign race.
// ---------------------------------------------------------------------------

/// One changed view key a round's audit produced: (bucket, key, before, after).
type ViewDiff = (usize, String, Option<Vec<u8>>, Option<Vec<u8>>);

/// One breadcrumb key's events within a (bucket, pkg): the put seqs, and the
/// (del seq, del att, del node) triples that retired them.
type KeyEvents = (Vec<u64>, Vec<(u64, u64, usize)>);

struct RepairFinding {
    bucket: usize,
    subject: String,
    class: u8,
    detail: String,
}

/// A breadcrumb's lifetime `[put_seq, del_seq)`; `del_seq == u64::MAX` while it
/// is still live at the boundary. `del_att`/`del_node` attribute the consuming
/// op (for the blind-consumption test). `is_note` picks the `_repl/` note
/// interpretation over the `_dirty/` marker one.
struct Interval {
    put_seq: u64,
    del_seq: u64,
    del_att: u64,
    del_node: usize,
    is_note: bool,
}

fn opt(o: Option<u64>) -> String {
    match o {
        Some(v) => v.to_string(),
        None => "none".to_string(),
    }
}

/// Max seq of a TruthList on (bucket, pkg) by op `att`, strictly before `before`.
fn l_of(live: &[&Effect], bucket: usize, pkg: &str, att: u64, before: u64) -> Option<u64> {
    live.iter()
        .filter(|e| {
            e.kind == EffectKind::TruthList
                && e.bucket == bucket
                && e.pkg == pkg
                && e.att == att
                && e.seq < before
        })
        .map(|e| e.seq)
        .max()
}

/// The op's own `_dirty/` listing before `before`, if it made one — the start
/// of the window the rebuilds it spawned ran in. `None` means the op is not a
/// tick and spawned no rebuild at all.
fn marker_list_seq(live: &[&Effect], att: u64, before: u64) -> Option<u64> {
    live.iter()
        .filter(|e| e.kind == EffectKind::MarkerList && e.att == att && e.seq < before)
        .map(|e| e.seq)
        .max()
}

/// The last `packages/<pkg>/` listing on (bucket, pkg, node) strictly inside
/// `(after, before)` — a tick's spawned rebuild, which does not inherit the
/// tick's op id and so has to be matched positionally. The fleet-wide tick lock
/// bounds this to one live rebuild per key, so the inference is exact.
fn spawned_list_seq(
    live: &[&Effect],
    bucket: usize,
    pkg: &str,
    node: usize,
    after: u64,
    before: u64,
) -> Option<u64> {
    live.iter()
        .filter(|e| {
            e.kind == EffectKind::TruthList
                && e.bucket == bucket
                && e.pkg == pkg
                && e.node == node
                && e.seq > after
                && e.seq < before
        })
        .map(|e| e.seq)
        .max()
}

/// The seq at which the op that consumed a `_dirty/` marker actually listed
/// this package's truth. Exact for an op that listed truth itself (`direct`);
/// for a tick — whose batch marker-delete runs on the main task but whose
/// per-package rebuild listing lives in a spawned child — infer the child's
/// listing from the tick's window. A consumer always listed `_dirty/` first, so
/// a missing window start is an unattributed consumer, not a carried-forward
/// one: scan from the beginning rather than declare the listing unknown.
fn consumer_list_seq(
    live: &[&Effect],
    bucket: usize,
    pkg: &str,
    att: u64,
    node: usize,
    del_seq: u64,
) -> Option<u64> {
    if let Some(direct) = l_of(live, bucket, pkg, att, del_seq) {
        return Some(direct);
    }
    let window_start = marker_list_seq(live, att, del_seq).unwrap_or(0);
    spawned_list_seq(live, bucket, pkg, node, window_start, del_seq)
}

/// The seq at which the op behind a global-index write actually re-derived
/// `pkg`'s membership, or `None` if it never did. `update_global_index` applies
/// a *delta*: it only reconsiders a name its own tick rebuilt, and carries
/// every other name forward from the cached set — so a write with no derivation
/// could not have corrected this name and is not a candidate for having failed
/// to. Unlike [`consumer_list_seq`] the window start is required: an op that
/// never listed `_dirty/` ran no rebuild, so an earlier unrelated listing by
/// the same node must not be mistaken for its derivation.
fn global_derivation_seq(
    live: &[&Effect],
    bucket: usize,
    pkg: &str,
    att: u64,
    node: usize,
    write_seq: u64,
) -> Option<u64> {
    if let Some(direct) = l_of(live, bucket, pkg, att, write_seq) {
        return Some(direct);
    }
    let window_start = marker_list_seq(live, att, write_seq)?;
    spawned_list_seq(live, bucket, pkg, node, window_start, write_seq)
}

/// Every breadcrumb lifetime touching (bucket, pkg): `_dirty/` markers on this
/// bucket and `_repl/` notes aimed at it. Put and del pair by identical full
/// key (unique per creation; the i-th put with the i-th del if a key is reused).
fn breadcrumb_intervals(live: &[&Effect], bucket: usize, pkg: &str) -> Vec<Interval> {
    let mut out = Vec::new();
    for (put_kind, del_kind, is_note) in [
        (EffectKind::MarkerPut, EffectKind::MarkerDel, false),
        (EffectKind::NotePut, EffectKind::NoteDel, true),
    ] {
        let mut by_key: BTreeMap<&str, KeyEvents> = BTreeMap::new();
        for e in live.iter() {
            if e.bucket != bucket || e.pkg != pkg {
                continue;
            }
            if e.kind == put_kind {
                by_key.entry(e.key.as_str()).or_default().0.push(e.seq);
            } else if e.kind == del_kind {
                by_key
                    .entry(e.key.as_str())
                    .or_default()
                    .1
                    .push((e.seq, e.att, e.node));
            }
        }
        for (_key, (mut puts, mut dels)) in by_key {
            puts.sort_unstable();
            dels.sort_unstable();
            for (i, put_seq) in puts.into_iter().enumerate() {
                let (del_seq, del_att, del_node) = dels.get(i).copied().unwrap_or((u64::MAX, 0, 0));
                out.push(Interval {
                    put_seq,
                    del_seq,
                    del_att,
                    del_node,
                    is_note,
                });
            }
        }
    }
    out
}

/// The breadcrumbs alive across `m_seq` — the ones that could have told the
/// system to rebuild after that mutation.
fn covering(intervals: &[Interval], m_seq: u64) -> Vec<&Interval> {
    intervals
        .iter()
        .filter(|iv| iv.put_seq < m_seq && m_seq < iv.del_seq)
        .collect()
}

/// True when every breadcrumb in `cov` was retired without acting on the
/// mutation at `m_seq` — the signal was destroyed rather than consumed.
fn all_blind(live: &[&Effect], bucket: usize, pkg: &str, cov: &[&Interval], m_seq: u64) -> bool {
    cov.iter().all(|iv| {
        if iv.del_seq == u64::MAX {
            return false; // still live at the boundary — signal not lost
        }
        if iv.is_note {
            // A sweep may retire a note only after re-arming the destination's
            // own dirty marker (att == the sweep's op); doing so hands the
            // mutation to the dirty path rather than dropping it.
            let re_armed = live.iter().any(|e| {
                e.kind == EffectKind::MarkerPut
                    && e.bucket == bucket
                    && e.pkg == pkg
                    && e.att == iv.del_att
                    && e.seq < iv.del_seq
            });
            !re_armed
        } else {
            consumer_list_seq(live, bucket, pkg, iv.del_att, iv.del_node, iv.del_seq)
                .is_none_or(|cl| cl < m_seq)
        }
    })
}

/// Explain one repaired (bucket, pkg) view: which taxonomy class, and why.
fn analyze(live: &[&Effect], boundary: u64, bucket: usize, pkg: &str) -> RepairFinding {
    let finding = |class: u8, detail: String| RepairFinding {
        bucket,
        subject: pkg.to_string(),
        class,
        detail,
    };

    let mut view_writes: Vec<&Effect> = live
        .iter()
        .filter(|e| e.kind == EffectKind::ViewWrite && e.bucket == bucket && e.pkg == pkg)
        .copied()
        .collect();
    view_writes.sort_by_key(|e| e.seq);
    let mutations: Vec<&Effect> = live
        .iter()
        .filter(|e| e.kind == EffectKind::TruthWrite && e.bucket == bucket && e.pkg == pkg)
        .copied()
        .collect();

    let intervals = breadcrumb_intervals(live, bucket, pkg);

    // O_f: the final view the fast path left in place (or the boundary, if the
    // fast path never wrote a view for this package).
    let (v_f, att_f, l_f) = match view_writes.last() {
        Some(o) => (o.seq, o.att, l_of(live, bucket, pkg, o.att, o.seq)),
        None => (boundary, 0, None),
    };
    // Mutations the final view could not have reflected: it listed truth before
    // them, or never listed at all.
    let unseen: Vec<&Effect> = mutations
        .iter()
        .filter(|m| l_f.is_none_or(|l| m.seq > l))
        .copied()
        .collect();

    // TEST 1 — ORDERING: an unreflected mutation with no live breadcrumb over
    // it. Nothing durable could have told the system to rebuild.
    for m in &unseen {
        REACH.hit(R::PkgOrdering);
        if covering(&intervals, m.seq).is_empty() {
            return finding(
                1,
                format!(
                    "truth mutation {}@{} had no live breadcrumb (no _dirty/ marker on this bucket, no _repl/ note aimed at it)",
                    m.key, m.seq
                ),
            );
        }
    }
    // TEST 2a — poisoned derivation: the final view op listed truth past every
    // mutation, yet the view it wrote still disagrees with that truth.
    if !view_writes.is_empty() && !mutations.is_empty() {
        REACH.hit(R::PkgPoisoned);
        if unseen.is_empty() {
            return finding(
                2,
                format!(
                    "op {} listed truth@{} and wrote the final view@{}, yet the view disagrees with that truth — a poisoned derivation consumed the signal",
                    att_f,
                    opt(l_f),
                    v_f
                ),
            );
        }
    }
    // TEST 2b — blind consumption: every breadcrumb covering an unreflected
    // mutation was retired by a consumer that had already listed truth (or
    // never listed it), destroying the signal without acting on it.
    for m in &unseen {
        let cov = covering(&intervals, m.seq);
        if cov.is_empty() {
            continue;
        }
        REACH.hit(R::PkgBlind);
        if all_blind(live, bucket, pkg, &cov, m.seq) {
            return finding(
                2,
                format!(
                    "breadcrumbs covering truth mutation {}@{} were all consumed blind (every consumer listed truth before the mutation, or never)",
                    m.key, m.seq
                ),
            );
        }
    }
    // TEST 3 — CONCURRENT-RACE: the surviving view write listed truth strictly
    // older than a different op's earlier view write it overwrote — the
    // documented unleased-rebuild clobber the audit backs up.
    for g in &view_writes {
        if g.seq < v_f && g.att != att_f {
            if let Some(l_g) = l_of(live, bucket, pkg, g.att, g.seq) {
                REACH.hit(R::PkgRace);
                if l_f.is_none_or(|lf| l_g > lf) {
                    return finding(
                        3,
                        format!(
                            "unleased concurrent rebuild: final view write@{} (listed@{:?}) overwrote fresher view write@{} (listed@{})",
                            v_f, l_f, g.seq, l_g
                        ),
                    );
                }
            }
        }
    }
    // FALLBACK — unexplained drift is conservatively premature-consumption: an
    // unexplained repair is a protocol or classifier bug, and either must fail
    // the seed rather than hide. Dump this view's effects to make it diagnosable.
    REACH.hit(R::PkgFallback);
    let dump: Vec<String> = live
        .iter()
        .filter(|e| e.bucket == bucket && e.pkg == pkg)
        .map(|e| format!("{:?}@{} att={} {}", e.kind, e.seq, e.att, e.key))
        .collect();
    finding(
        2,
        format!(
            "unexplained drift — conservatively premature-consumption; effects: [{}]",
            dump.join("; ")
        ),
    )
}

/// Explain one repaired global-index membership: the audit had to add or drop
/// `name` from `simple/index.{json,html}` on `bucket`.
///
/// This is a different subsystem from [`analyze`] and needs its own history.
/// Membership is decided by `update_global_index_locked`'s delta-plus-CAS over
/// the tick's cached name set — never by `rebuild_package`'s render — so the
/// per-package view can be byte-perfect while the global set is wrong. Walking
/// the package's `ViewWrite`s (what this used to do) therefore blamed a session
/// that had nothing to do with the flip and sent triage to the wrong function.
/// The three questions and their severities are unchanged; only the writes they
/// are asked about are: `GlobalWrite` effects, which carry the name set the
/// write claimed.
fn analyze_global(live: &[&Effect], boundary: u64, bucket: usize, name: &str) -> RepairFinding {
    let finding = |class: u8, detail: String| RepairFinding {
        bucket,
        subject: format!("simple/index.* membership of {name}"),
        class,
        detail,
    };

    // Global writes on this bucket that actually re-derived this name, each
    // with the seq at which they did. A write with no derivation carried the
    // name forward from the cached set and could not have corrected it, so it
    // is not a candidate for having failed to.
    let mut derivations: Vec<(&Effect, u64)> = live
        .iter()
        .filter(|e| e.kind == EffectKind::GlobalWrite && e.bucket == bucket)
        .filter_map(|e| {
            global_derivation_seq(live, bucket, name, e.att, e.node, e.seq).map(|l| (*e, l))
        })
        .collect();
    derivations.sort_by_key(|(e, _)| e.seq);
    let mutations: Vec<&Effect> = live
        .iter()
        .filter(|e| e.kind == EffectKind::TruthWrite && e.bucket == bucket && e.pkg == name)
        .copied()
        .collect();
    let intervals = breadcrumb_intervals(live, bucket, name);

    // The last global write that looked at this name's truth, and what it then
    // claimed about it (or the boundary, if none ever did).
    let (v_f, att_f, l_f, claimed) = match derivations.last() {
        Some((e, l)) => (
            e.seq,
            e.att,
            Some(*l),
            if e.names.iter().any(|n| n == name) {
                "present"
            } else {
                "absent"
            },
        ),
        None => (boundary, 0, None, "never derived"),
    };
    let unseen: Vec<&Effect> = mutations
        .iter()
        .filter(|m| l_f.is_none_or(|l| m.seq > l))
        .copied()
        .collect();

    // TEST 1 — ORDERING: a truth mutation no global write reflected, with no
    // live breadcrumb over it. Nothing durable could have told a tick to
    // rebuild this package, so nothing could have computed the name delta.
    for m in &unseen {
        REACH.hit(R::GlobalOrdering);
        if covering(&intervals, m.seq).is_empty() {
            return finding(
                1,
                format!(
                    "truth mutation {}@{} had no live breadcrumb (no _dirty/ marker on this bucket, no _repl/ note aimed at it), so no tick ever re-derived {name} and update_global_index computed no delta for it",
                    m.key, m.seq
                ),
            );
        }
    }
    // TEST 2a — poisoned derivation: a global write did re-derive this name
    // past every mutation, called it `claimed`, and was still wrong.
    if !derivations.is_empty() && !mutations.is_empty() {
        REACH.hit(R::GlobalPoisoned);
        if unseen.is_empty() {
            return finding(
                2,
                format!(
                    "op {att_f} rebuilt {name} from truth@{} and wrote the global index@{v_f} claiming it {claimed}, yet the audit had to flip that membership — update_global_index consumed the signal without applying it",
                    opt(l_f)
                ),
            );
        }
    }
    // TEST 2b — blind consumption: every breadcrumb covering an unreflected
    // mutation was retired without acting on it, so no delta was ever computed.
    for m in &unseen {
        let cov = covering(&intervals, m.seq);
        if cov.is_empty() {
            continue;
        }
        REACH.hit(R::GlobalBlind);
        if all_blind(live, bucket, name, &cov, m.seq) {
            return finding(
                2,
                format!(
                    "breadcrumbs covering truth mutation {}@{} were all consumed blind (every consumer listed truth before the mutation, or never), so no global-index delta was ever computed for {name}",
                    m.key, m.seq
                ),
            );
        }
    }
    // TEST 3 — CONCURRENT-RACE: the surviving global write derived this name
    // from strictly older truth than an earlier write it overwrote — the same
    // unleased-rebuild clobber the audit backs up, one level up in the tree.
    for (g, l_g) in &derivations {
        if g.seq < v_f && g.att != att_f {
            REACH.hit(R::GlobalRace);
            if l_f.is_none_or(|lf| *l_g > lf) {
                return finding(
                    3,
                    format!(
                        "unleased concurrent rebuild: global write@{v_f} (op {att_f}, derived {name} from truth@{}, claimed {claimed}) overwrote fresher global write@{} (op {}, derived @{l_g})",
                        opt(l_f),
                        g.seq,
                        g.att
                    ),
                );
            }
        }
    }
    // FALLBACK — unexplained drift is conservatively premature-consumption, as
    // in `analyze`. Dump this name's truth history *and* the bucket's global
    // writes with the sets they claimed, since either side can be the bug.
    REACH.hit(R::GlobalFallback);
    let dump: Vec<String> = live
        .iter()
        .filter(|e| e.bucket == bucket && (e.pkg == name || e.kind == EffectKind::GlobalWrite))
        .map(|e| match e.kind {
            EffectKind::GlobalWrite => format!(
                "GlobalWrite@{} att={} {} names={:?}",
                e.seq, e.att, e.key, e.names
            ),
            _ => format!("{:?}@{} att={} {}", e.kind, e.seq, e.att, e.key),
        })
        .collect();
    finding(
        2,
        format!(
            "unexplained global-index drift for {name} — conservatively premature-consumption; effects: [{}]",
            dump.join("; ")
        ),
    )
}

/// Classify every view key a round's audit changed. Global-index diffs expand
/// to the package names whose membership flipped and are analysed against the
/// writes that decide membership; per-package diffs collapse the html+json pair
/// to a single finding.
fn classify_round(effects: &[Effect], boundary: u64, diffs: &[ViewDiff]) -> Vec<RepairFinding> {
    let live: Vec<&Effect> = effects.iter().filter(|e| e.seq < boundary).collect();
    let mut findings = Vec::new();
    let mut seen: std::collections::BTreeSet<(usize, String)> = std::collections::BTreeSet::new();
    for (bucket, key, before, after) in diffs {
        if key == "simple/index.json" || key == "simple/index.html" {
            let names = |bytes: &Option<Vec<u8>>| -> std::collections::BTreeSet<String> {
                match (key.as_str(), bytes) {
                    ("simple/index.json", Some(b)) => global_names_from_json(b),
                    ("simple/index.html", Some(b)) => global_names_from_html(b),
                    _ => Vec::new(),
                }
                .into_iter()
                .collect()
            };
            let before_names = names(before);
            let after_names = names(after);
            let flipped: Vec<&String> = before_names.symmetric_difference(&after_names).collect();
            if flipped.is_empty() {
                // Bytes differ but the name set did not — the audit rewrote the
                // global index without a membership change we can attribute.
                findings.push(RepairFinding {
                    bucket: *bucket,
                    subject: key.clone(),
                    class: 2,
                    detail: format!(
                        "global index {key} bytes changed but its package set did not — conservatively premature-consumption"
                    ),
                });
                continue;
            }
            for name in flipped {
                // Namespaced separately from the per-package key: a global
                // membership flip and a per-package view repair for the same
                // name are two different defects in two different functions,
                // and collapsing them hid one of the two.
                if seen.insert((*bucket, format!("simple/index.*:{name}"))) {
                    findings.push(analyze_global(&live, boundary, *bucket, name));
                }
            }
        } else if let Some(pkg) = view_key_package(key) {
            if seen.insert((*bucket, pkg.to_string())) {
                findings.push(analyze(&live, boundary, *bucket, pkg));
            }
        }
    }
    findings
}

// ---------------------------------------------------------------------------
// Entry point.
// ---------------------------------------------------------------------------

struct Args {
    seeds: u64,
    start_seed: u64,
    nodes: usize,
    buckets: usize,
    packages: usize,
    files: u8,
    ops: u64,
    recheck_every: u64,
    fail_percent: u64,
    /// Soak mode: never stop; failures are logged (with their exact repro
    /// command) and exploration continues with the next seed.
    forever: bool,
    /// Timebox: explore until this many wall-clock seconds elapse, logging
    /// failures like `--forever`, then exit non-zero if anything failed.
    max_secs: Option<u64>,
    /// Derive the whole profile — topology, entity counts and op mix — per
    /// seed instead of using the fixed flags, so one soak covers every shape.
    /// It is a pure function of the seed alone, which is what makes
    /// `--seed N --rotate` an exact reproduction; failures print the resolved
    /// dimensions beside that command so you can read the shape without
    /// rerunning it.
    rotate: bool,
    /// Deliberate defect to inject (`--break`), for mutation-testing the
    /// oracles. `Break::None` in every ordinary run.
    brk: Break,
    /// Fail the run when an oracle recorded zero executions over the sample
    /// (see `EXPECTED_ZERO`). Off by default — a small sample legitimately
    /// misses oracles, and a gate that cries wolf gets ignored. Wire it only
    /// where the sample is big enough to mean something.
    require_reach: bool,
}

#[derive(Clone, Copy)]
struct Profile {
    nodes: usize,
    buckets: usize,
    /// How many of `PACKAGE_NAMES` this seed's workload uses.
    packages: usize,
    /// Files per package (each becomes version `<file+1>.0`).
    files: u8,
    ops: u64,
    fail_percent: u64,
    /// Op-class mix for the chaos loop — see `OP_WEIGHT_BOUNDS`.
    weights: [u16; OP_CLASSES],
    brk: Break,
}

impl Profile {
    /// The resolved dimensions, printed next to every reproduce command. A
    /// failure you cannot read the shape of is a failure you debug twice.
    fn describe(&self) -> String {
        let mix: Vec<String> = OP_LABELS
            .iter()
            .zip(self.weights)
            .map(|(label, weight)| format!("{label} {weight}"))
            .collect();
        format!(
            "nodes={} buckets={} packages={} files={} ops={} fail-percent={} weights=[{}]",
            self.nodes,
            self.buckets,
            self.packages,
            self.files,
            self.ops,
            self.fail_percent,
            mix.join(", ")
        )
    }
}

fn profile_for(seed: u64, args: &Args) -> Profile {
    if !args.rotate {
        return Profile {
            nodes: args.nodes,
            buckets: args.buckets,
            packages: args.packages,
            files: args.files,
            ops: args.ops,
            fail_percent: args.fail_percent,
            weights: DEFAULT_OP_WEIGHTS,
            brk: args.brk,
        };
    }
    let mut rng = Rng::new(seed ^ 0x0507_A7E5);
    let mut weights = [0u16; OP_CLASSES];
    for (weight, (floor, span)) in weights.iter_mut().zip(OP_WEIGHT_BOUNDS) {
        *weight = floor + rng.below(u64::from(span)) as u16;
    }
    Profile {
        nodes: 2 + rng.below(2) as usize,   // 2..=3
        buckets: 1 + rng.below(3) as usize, // 1..=3: no-replication through 3-way fan-out
        // Entity counts skew small: a run's op budget is fixed, so every extra
        // filename thins the interleavings each one gets. The tail reaches 6x4
        // = 24 filenames, enough for concurrent rebuilds to collide on
        // different packages; the mode stays cheap enough to keep throughput.
        packages: [1, 2, 2, 3, 3, 4, 5, 6][rng.below(8) as usize],
        files: [1, 2, 2, 3, 4][rng.below(5) as usize],
        ops: [80, 120, 160, 200][rng.below(4) as usize],
        // Half the schedules crash-only, where audit repairs are hard
        // violations; half with injected storage failures.
        fail_percent: if rng.chance(50) { 0 } else { 3 },
        weights,
        brk: args.brk,
    }
}

/// The one command that re-runs exactly this seed. A rotating profile is a
/// pure function of the seed, so `--seed N --rotate` is complete on its own —
/// including the entity counts and the op-weight vector, which have no useful
/// flag form. A fixed profile has to carry its flags.
fn reproduce_command(seed: u64, args: &Args, profile: &Profile) -> String {
    if args.rotate {
        return format!("cargo run --release --example vopr -- --seed {seed} --rotate");
    }
    format!(
        "cargo run --release --example vopr -- --seed {seed} --nodes {} --buckets {} \
         --packages {} --files {} --ops {} --fail-percent {}",
        profile.nodes,
        profile.buckets,
        profile.packages,
        profile.files,
        profile.ops,
        profile.fail_percent
    )
}

/// Weighted choice over the op classes. Exactly one rng draw regardless of the
/// mix, so the weights change *which* op a step runs, never how much of the
/// seed's entropy it consumes — that is what lets `DEFAULT_OP_WEIGHTS` keep
/// non-rotating runs byte-identical to the pre-swarm `rng.below(100)` arms.
fn pick_op(weights: &[u16; OP_CLASSES], total: u64, rng: &mut Rng) -> usize {
    let mut point = rng.below(total);
    for (idx, weight) in weights.iter().enumerate() {
        let weight = u64::from(*weight);
        if point < weight {
            return idx;
        }
        point -= weight;
    }
    OP_CLASSES - 1
}

fn parse_args() -> Args {
    let mut args = Args {
        seeds: 25,
        start_seed: 1,
        nodes: 2,
        buckets: 2,
        packages: 2,
        files: 2,
        ops: 120,
        recheck_every: 10,
        fail_percent: 3,
        forever: false,
        max_secs: None,
        rotate: false,
        brk: Break::None,
        require_reach: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--break" {
            let name = it
                .next()
                .unwrap_or_else(|| panic!("missing value for --break"));
            args.brk = Break::parse(&name);
            continue;
        }
        let mut grab = || {
            it.next()
                .unwrap_or_else(|| panic!("missing value for {flag}"))
                .parse::<u64>()
                .unwrap_or_else(|e| panic!("bad value for {flag}: {e}"))
        };
        match flag.as_str() {
            "--seeds" => args.seeds = grab(),
            "--seed" => {
                args.start_seed = grab();
                args.seeds = 1;
            }
            "--start-seed" => args.start_seed = grab(),
            "--nodes" => args.nodes = grab() as usize,
            "--buckets" => args.buckets = grab() as usize,
            // Both are rng moduli and one indexes PACKAGE_NAMES, so an
            // out-of-range value would divide by zero or panic mid-soak.
            "--packages" => {
                let n = grab() as usize;
                assert!(
                    (1..=PACKAGE_NAMES.len()).contains(&n),
                    "--packages must be 1..={}",
                    PACKAGE_NAMES.len()
                );
                args.packages = n;
            }
            "--files" => {
                let n = grab();
                assert!((1..=64).contains(&n), "--files must be 1..=64");
                args.files = n as u8;
            }
            "--ops" => args.ops = grab(),
            "--recheck-every" => args.recheck_every = grab(),
            "--fail-percent" => args.fail_percent = grab(),
            "--forever" => args.forever = true,
            "--max-secs" => args.max_secs = Some(grab()),
            "--rotate" => args.rotate = true,
            "--require-reach" => args.require_reach = true,
            "--verbose" => {}
            other => panic!("unknown flag {other} (see examples/vopr.rs)"),
        }
    }
    args
}

fn dump_trace(outcome: &RunOutcome) {
    if let (Some(path), Some(log)) = (std::env::var_os("VOPR_TRACE_FILE"), &outcome.trace_log) {
        let _ = std::fs::write(path, log.join("\n"));
    }
}

fn run_once(seed: u64, profile: &Profile, rerun: bool) -> RunOutcome {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("build paused runtime");
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(run_seed(seed, *profile, rerun)))
}

fn main() {
    if std::env::args().any(|a| a == "--verbose") {
        tracing_subscriber::fmt()
            .with_env_filter(
                std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,pypiron=warn".into()),
            )
            .init();
    }
    let args = parse_args();
    let started = std::time::Instant::now();
    let keep_going = args.forever || args.max_secs.is_some();
    let mut total_events: u64 = 0;
    let mut total_acked: usize = 0;
    let mut total_ack_totality: u64 = 0;
    let mut total_audit_repairs: u64 = 0;
    let mut total_repairs_by_class: [u64; 3] = [0; 3]; // [ordering, premature, race]
    let mut failed_seeds: Vec<u64> = Vec::new();
    let mut determinism_violations: Vec<u64> = Vec::new();
    let mut explored: u64 = 0;
    let mut last_report = std::time::Instant::now();
    loop {
        if let Some(budget) = args.max_secs {
            if started.elapsed().as_secs() >= budget {
                break;
            }
        } else if !args.forever && explored >= args.seeds {
            break;
        }
        let seed = args.start_seed + explored;
        explored += 1;
        let profile = profile_for(seed, &args);
        if profile.buckets > 1 {
            REACH.multi_bucket.store(true, Ordering::Relaxed);
        }
        let mut outcome = run_once(seed, &profile, false);
        dump_trace(&outcome);
        total_events += outcome.trace_events;
        total_acked += outcome.acked;
        total_ack_totality += outcome.ack_totality;
        total_audit_repairs += outcome.audit_view_repairs;
        for (total, add) in total_repairs_by_class
            .iter_mut()
            .zip(outcome.repairs_by_class)
        {
            *total += add;
        }
        if args.recheck_every > 0 && seed.is_multiple_of(args.recheck_every) {
            let again = run_once(seed, &profile, true);
            if outcome.trace_events > 0 {
                REACH.hit(R::Determinism); // an empty trace compares nothing
            }
            if again.trace_hash != outcome.trace_hash {
                eprintln!(
                    "vopr: DETERMINISM VIOLATION seed={seed}: trace {:#x} vs {:#x}",
                    outcome.trace_hash, again.trace_hash
                );
                if let (Some(a), Some(b)) = (&outcome.trace_log, &again.trace_log) {
                    let split = a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count();
                    eprintln!(
                        "first divergence at event {split} of {}/{}",
                        a.len(),
                        b.len()
                    );
                    for i in split.saturating_sub(3)..(split + 4).min(a.len().min(b.len())) {
                        eprintln!("  [{i}] A: {}\n  [{i}] B: {}", a[i], b[i]);
                    }
                } else {
                    eprintln!("rerun with VOPR_TRACE=1 to diff the traces");
                }
                eprintln!(
                    "reproduce: {} --recheck-every 1\nprofile: {}",
                    reproduce_command(seed, &args, &profile),
                    profile.describe()
                );
                if !keep_going {
                    std::process::exit(3);
                }
                determinism_violations.push(seed);
            } else if again.state_hash != outcome.state_hash {
                eprintln!(
                    "vopr: DETERMINISM VIOLATION seed={seed}: op traces matched but the final \
                     world differs ({:#x} vs {:#x}) — nondeterminism is downstream of the op \
                     sequence: same calls, different bytes",
                    outcome.state_hash, again.state_hash
                );
                eprintln!(
                    "reproduce: {} --recheck-every 1\nprofile: {}",
                    reproduce_command(seed, &args, &profile),
                    profile.describe()
                );
                if !keep_going {
                    std::process::exit(3);
                }
                determinism_violations.push(seed);
            }
            // The rerun's own verdict was previously thrown away, so a seed that
            // passed run 1 and failed run 2 was reported green. The primary
            // block below owns the exit; this only makes sure the rerun's
            // violations are seen and counted.
            if !again.violations.is_empty() && outcome.violations.is_empty() {
                outcome.violations = again.violations;
            }
        }
        if !outcome.violations.is_empty() {
            eprintln!(
                "vopr: seed {seed} FAILED ({} violations):",
                outcome.violations.len()
            );
            for violation in &outcome.violations {
                eprintln!("  {violation}");
            }
            eprintln!("reproduce: {}", reproduce_command(seed, &args, &profile));
            eprintln!("profile: {}", profile.describe());
            if !keep_going {
                std::process::exit(2);
            }
            failed_seeds.push(seed);
        }
        // Soak-log heartbeat: one line a minute proves liveness and carries
        // the running counters without flooding the log.
        if keep_going && last_report.elapsed().as_secs() >= 60 {
            println!(
                "vopr: progress — {explored} seeds, {total_events} storage-op interleavings, {total_acked} acked uploads, {total_ack_totality} ack-totality misses, {total_audit_repairs} audit view repairs ({} ordering, {} premature, {} concurrent-race), {} failed, {} determinism violations, {:?} elapsed",
                total_repairs_by_class[0],
                total_repairs_by_class[1],
                total_repairs_by_class[2],
                failed_seeds.len(),
                determinism_violations.len(),
                started.elapsed()
            );
            last_report = std::time::Instant::now();
        }
    }
    let profile_desc = if args.rotate {
        "rotating(nodes 2-3, buckets 1-3, packages 1-6, files 1-4, ops 80-200, \
         swarmed op mix, fault+crash-only)"
            .to_string()
    } else {
        format!(
            "nodes={} buckets={} packages={} files={} ops/run={} fail-percent={}",
            args.nodes, args.buckets, args.packages, args.files, args.ops, args.fail_percent
        )
    };
    println!(
        "vopr: {explored} seeds explored, {total_events} storage-op interleavings, {total_acked} acked uploads verified, {total_ack_totality} ack-totality misses, {total_audit_repairs} audit view repairs ({} ordering, {} premature, {} concurrent-race), {profile_desc} in {:?} — {}",
        total_repairs_by_class[0],
        total_repairs_by_class[1],
        total_repairs_by_class[2],
        started.elapsed(),
        if failed_seeds.is_empty() && determinism_violations.is_empty() {
            "all invariants held".to_string()
        } else {
            format!(
                "{} FAILED seeds {failed_seeds:?}, {} determinism violations {determinism_violations:?}",
                failed_seeds.len(),
                determinism_violations.len()
            )
        }
    );
    let unreached = report_reach(explored, args.brk);
    if !determinism_violations.is_empty() {
        std::process::exit(3);
    }
    if !failed_seeds.is_empty() {
        std::process::exit(2);
    }
    if args.require_reach && !unreached.is_empty() {
        eprintln!(
            "vopr: --require-reach FAILED — {} oracle(s) never executed over {explored} seeds: \
             {unreached:?}. An oracle that verified nothing is a defect report, not a pass: \
             either the workload cannot reach it (widen it), or it is unreachable and belongs \
             in EXPECTED_ZERO with a reason.",
            unreached.len()
        );
        std::process::exit(4);
    }
}
