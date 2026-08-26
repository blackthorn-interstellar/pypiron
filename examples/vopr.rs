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
//!   - SELF_CONSISTENCY: every stored body is re-hashed and must equal the
//!     sha256 its own bucket's sidecar publishes. This is the only oracle that
//!     ever re-hashes a body: every other one here (and `pypiron verify`) reads
//!     sidecars and compares them, so a body swapped under a sidecar still
//!     naming the old sha is invisible to all of them — two buckets whose
//!     sidecars are byte-identical never enter the diverged-key set at all;
//!   - TOMBSTONE MONOTONICITY: a filename whose most recent ack was a 204 delete
//!     never stands in a bucket without its tombstone — no silent resurrection
//!     of a removed (compromised) artifact;
//!   - NO LEAKS: no unconsumed `_dirty/` markers, no undrained `_repl/` notes;
//!   - CONSERVATION: acknowledged bytes are never silently lost — a file is
//!     live, or its filename was deleted/frozen by an authorized operation;
//!   - STALENESS (bounded agreement, on a clock): after the last write and once
//!     the faults clear, every bucket must agree within a DEADLINE measured in
//!     simulated minutes — five for a healthy fleet, plus one convergence cycle
//!     per page of outstanding replication debt at the moment the storm ended.
//!     It replaced a budget of heal rounds, which was a made-up number with no
//!     relationship to time: "converges, but would have taken an hour" passed
//!     silently under it. The fleet must also stop working — reaching the
//!     deadline and then churning markers forever is the other half of the same
//!     oracle;
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
//! separately — plus how many seeds got any of it, because an oracle reading
//! zero over a whole soak, or reading five figures on a fifth of the seeds, is a
//! defect report, not a pass (see `Reach`). `--require-reach` turns both into a
//! failing run. The same line reports the tightest STALENESS margin any seed
//! left — how much of its own deadline the slowest convergence spent — because a
//! bound with no observed margin is a bound about to break.
//!
//! Determinism is self-checked, not assumed: every run whose seed is a multiple
//! of `--recheck-every` executes twice and must produce an identical storage-op
//! trace hash *and* an identical final world (bucket bytes + ledger), because a
//! nondeterminism downstream of the op sequence — an unvirtualized clock read,
//! say — issues the same calls with different bytes. The rerun's own invariant
//! verdict counts too: a seed that passes once and fails once is a red seed.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use pypiron::buckets::{BucketHandle, BucketSet, Pinned};
use pypiron::hash::sha256_hex;
use pypiron::replicate::{Record, Verdict};
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

/// Chaos-loop op classes, in the order `pick_op` indexes them. The pinned
/// `--rotate` seeds in `.github/workflows/ci.yml` derive their entire profile
/// from this order, so reordering or resizing it re-rolls each of them into a
/// different simulation — silently, because a re-rolled seed still passes.
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
/// verifies nothing, and still reports green. Editing any pair re-rolls the
/// pinned `--rotate` seeds in `.github/workflows/ci.yml`, which draw their
/// weights from these values in this order: the shape each seed was pinned for
/// is gone, and nothing reds to say so.
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
    /// Park an object under `_repl/` that no sweep recognizes → STALENESS, the
    /// never-settles arm.
    Wedge,
    /// Steal a peer's copy of a live record the moment the fleet has finished
    /// converging, leaving no breadcrumb over the loss → STALENESS, the
    /// past-its-deadline arm. The fleet repairs it, correctly and completely —
    /// one whole tree-diff cycle later than the debt it was holding paid for.
    SlowRepair,
    /// A phantom package the audit must materialize, plus the effect history
    /// of the rebuild that caused it. One break per classifier arm; each kills
    /// the per-package *and* the global analyzer at once (`analyze`'s and
    /// `analyze_global`'s tests are the same three questions asked of
    /// `ViewWrite`s and `GlobalWrite`s respectively).
    Poison,
    Blind,
    Race,
    Fallback,
    /// Freeze a filename nothing ever conflicted over → FREEZE_JUSTIFIED.
    FreezeUnjustified,
    /// Leave the fleet standing on BOTH acked byte-sets of one live filename →
    /// DURABILITY's never-left-split clause.
    Split,
    /// Freeze a filename and drop an acked body the freeze never quarantined →
    /// CONSERVATION's freeze-totality clause.
    FreezeLossy,
    /// Demote-quarantine a filename and drop the acked body the demotion never
    /// moved to `_quarantine/` → CONSERVATION's demotion-totality clause.
    DemoteLossy,
    /// Walk a privately-claimed package's `.origin` back to mirror →
    /// ORIGIN_TERMINALITY, claim arm.
    OriginDemoted,
    /// Serve a live mirror record under a private claim → ORIGIN_TERMINALITY,
    /// record arm.
    MirrorServed,
    /// Pull a bucket's bytes and the sha256 its own sidecar publishes apart,
    /// fleet-wide, with every view re-pointed at the published digest →
    /// SELF_CONSISTENCY, and nothing else (see `apply_break`).
    Attest,
}

/// The package name every phantom-clone break materializes. Not in
/// `PACKAGE_NAMES`, so no real effect in the run can ever mention it — the
/// planted history is the only history the classifier sees for it.
const BREAK_PKG: &str = "vopr-phantom";

/// Every `--break`, spelled once. The flag parser, the usage text and the
/// reproduce line all read this table: a second list that quietly fails to
/// track the first is exactly how the reproduce line came to describe a
/// different run than the one that failed.
const BREAKS: [(&str, Break); 23] = [
    ("view", Break::View),
    ("fanout", Break::Fanout),
    ("rerun", Break::Rerun),
    ("resurrect", Break::Resurrect),
    ("ordering", Break::Ordering),
    ("globalindex", Break::GlobalIndex),
    ("durability", Break::Durability),
    ("visibility", Break::Visibility),
    ("conserve", Break::Conserve),
    ("diverge", Break::Diverge),
    ("wedge", Break::Wedge),
    ("slow-repair", Break::SlowRepair),
    ("poison", Break::Poison),
    ("blind", Break::Blind),
    ("race", Break::Race),
    ("fallback", Break::Fallback),
    ("freeze-unjustified", Break::FreezeUnjustified),
    ("split", Break::Split),
    ("freeze-lossy", Break::FreezeLossy),
    ("demote-lossy", Break::DemoteLossy),
    ("origin-demoted", Break::OriginDemoted),
    ("mirror-served", Break::MirrorServed),
    ("attest", Break::Attest),
];

impl Break {
    fn parse(name: &str) -> Break {
        if name == "none" {
            return Break::None;
        }
        match BREAKS.iter().find(|(spelling, _)| *spelling == name) {
            Some((_, brk)) => *brk,
            None => {
                let known: Vec<&str> = BREAKS.iter().map(|(spelling, _)| *spelling).collect();
                panic!("unknown --break {name} ({})", known.join("|"))
            }
        }
    }

    /// Whether this break plants AFTER the heal round's audits rather than
    /// before them. Exactly one does, and it has to: `slow-repair` models a
    /// peer's copy vanishing once the fleet believes it is finished, and a
    /// theft planted before the audit merely makes the audit re-render the
    /// losing bucket's views — which reports a classifier finding about the
    /// harness's own damage instead of the late repair the break is for.
    fn plants_after_audit(self) -> bool {
        self == Break::SlowRepair
    }

    /// How this break is spelled on the command line; `None` for no break.
    fn flag(self) -> Option<&'static str> {
        BREAKS
            .iter()
            .find(|(_, brk)| *brk == self)
            .map(|(spelling, _)| *spelling)
    }
}

/// An acknowledged upload no authorized removal excuses — the subject the
/// durability family of oracles is actually about. Returns `(pkg, filename,
/// bytes)`. Raw `dump()` reads only: no op-sequence number, no rng, no trace.
///
/// Only a filename with ONE acked byte-set qualifies. Three `--break` kill
/// proofs corrupt/hide/destroy the bytes this returns and expect exactly one
/// oracle to red; a filename two buckets acked different bytes for is a subject
/// whose legal outcomes include the merge having kept the other side, so a
/// break planted on it would be arguing with the oracle instead of killing it.
fn unexcused_ack(
    buckets: &[Arc<SimStorage>],
    ledger: &Mutex<Ledger>,
) -> Option<(String, String, Vec<u8>)> {
    let ledger = ledger.lock().expect("ledger lock");
    ledger.acked.iter().find_map(|((pkg, fname), acks)| {
        let bodies = acked_bodies(acks);
        if bodies.len() != 1 {
            return None;
        }
        let akey = format!("packages/{pkg}/{fname}");
        let excused = ledger.deleted.contains(&(pkg.clone(), fname.clone()))
            || buckets.iter().any(|b| {
                let keys = b.keys();
                keys.contains(&format!("{akey}.tombstone"))
                    || keys.contains(&format!("{akey}.frozen"))
                    || keys.contains(&format!("{akey}.mirror-quarantined"))
            });
        (!excused).then(|| (pkg.clone(), fname.clone(), acks[0].body.clone()))
    })
}

/// Bytes that are not `body` and are exactly as long as it. A kill proof
/// injects ONE defect, and a corruption that also changed the object's length
/// would inject a second: `verify_storage` cross-checks every object's listed
/// size against the size its own sidecar publishes, so a shorter body reds
/// VERIFY as well — a different oracle's claim. Same length keeps the break
/// minimal and makes its leg the stricter test of the oracle it names.
fn same_length_corruption(body: &[u8]) -> Vec<u8> {
    let corrupt: Vec<u8> = b"vopr: corrupt"
        .iter()
        .copied()
        .cycle()
        .take(body.len())
        .collect();
    // Only a body that already spells the marker, which nothing here writes.
    if corrupt == body {
        return body.iter().map(|b| b ^ 0xFF).collect();
    }
    corrupt
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
        // Converges, but late — the failure a fixpoint-counting budget cannot
        // see and a deadline can. The last bucket loses its copy of one live
        // record, body and sidecar together, with NOTHING durable pointing at
        // the loss: no `_dirty/` marker, no `_repl/` note. So the fast path is
        // blind to it and only `reconcile`'s pairwise tree diff can find it,
        // which costs a whole backstop cycle the run's `t0` debt never bought.
        //
        // Planted pre-audit on the first heal round, i.e. AFTER that round's
        // reconcile has already run: the repair lands on the next round's, so
        // the delay is exactly one cycle and every other oracle still reads a
        // healthy fleet at quiescence — the record is back, byte-identical,
        // before anything looks. Views are left alone on both counts: the losing
        // bucket's views still list a record its truth lacks, which is a state
        // the tree diff resolves by restoring the record rather than by
        // re-rendering, so no audit repair fires and the classifier stays quiet.
        (Break::SlowRepair, true) => {
            // Needs a peer to repair from, exactly like `diverge` and `split`.
            let Some(victim) = buckets.get(1..).and_then(<[_]>::last) else {
                return;
            };
            let dump = victim.dump();
            let stolen = dump.iter().find_map(|(key, _)| {
                let akey = key.strip_suffix(".meta.json")?;
                dump.contains_key(akey).then(|| akey.to_string())
            });
            if let Some(akey) = stolen {
                let _ = victim
                    .delete_keys(&[akey.clone(), format!("{akey}.meta.json")])
                    .await;
            }
        }
        // Corrupt the bytes an acked artifact serves, fleet-wide, while parking
        // the real bytes outside `packages/` — so CONSERVATION still finds them
        // alive, CONVERGENCE still sees identical buckets, and `pypiron verify`
        // still agrees the views render truth. Same length on purpose: a
        // shorter body would also red VERIFY's size cross-check, which is a
        // different claim (`same_length_corruption`). SELF_CONSISTENCY reds
        // alongside and cannot not — corrupting a body IS contradicting the
        // sidecar over it — so the leg's expected text is what pins which
        // oracle it is for: the 200 said these bytes were durable.
        (Break::Durability, false) => {
            if let Some((pkg, fname, body)) = unexcused_ack(buckets, ledger) {
                let akey = format!("packages/{pkg}/{fname}");
                let corrupt = same_length_corruption(&body);
                for bucket in buckets {
                    bucket.insert(&format!("_vopr/quarantine/{akey}"), body.clone());
                    bucket.insert(&akey, corrupt.clone());
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
        // A freeze over a filename nothing ever conflicted about. Planted on an
        // acked-DELETED name so the marker is the only thing wrong: the body
        // and its view entry are already gone, so DURABILITY, VISIBILITY,
        // CONSERVATION and `pypiron verify` all stay silent and the only claim
        // left standing is that a freeze must be a real byte conflict.
        (Break::FreezeUnjustified, false) => {
            let victim = {
                let ledger = ledger.lock().expect("ledger lock");
                ledger
                    .last_ack_deleted
                    .iter()
                    .find(|(_, deleted)| **deleted)
                    .map(|((pkg, fname), _)| format!("packages/{pkg}/{fname}"))
            };
            if let Some(akey) = victim {
                for bucket in buckets {
                    bucket.insert(&format!("{akey}.frozen"), b"{}".to_vec());
                }
            }
        }
        // A byte conflict the merge never resolved: bucket 1 keeps a second
        // acked byte-set under a live filename bucket 0 still serves its own
        // for. The ack is planted with the bytes, because that pair IS the
        // defect — a 200 on bucket 1 for bytes the fleet then failed to merge —
        // and the oracle's subject (`acked_bodies(acks).len() >= 2`) does not
        // exist without it. CONVERGENCE reds alongside, unavoidably: a split
        // filename is a diverged key by definition, which is exactly why the
        // DURABILITY clause names it rather than leaving it as one line of a
        // key diff.
        (Break::Split, false) => {
            let Some(peer) = buckets.get(1) else { return };
            let Some((pkg, fname, acked)) = unexcused_ack(buckets, ledger) else {
                return;
            };
            let akey = format!("packages/{pkg}/{fname}");
            // Only where the peer already serves the record: inserting a body
            // onto a bucket that never held one is a different defect (a
            // sidecar-less artifact) and DURABILITY would red for that instead.
            if !peer.keys().contains(&akey) {
                return;
            }
            // Same length as the side bucket 0 keeps, so the only new claim is
            // the unresolved split — see `same_length_corruption`.
            let other = same_length_corruption(&acked);
            peer.insert(&akey, other.clone());
            ledger
                .lock()
                .expect("ledger lock")
                .record_ack(&pkg, &fname, 1, other);
        }
        // Freeze totality: `freeze_side` copies the losing body to
        // `_quarantine/` BEFORE it drops it, so a frozen filename still owes the
        // fleet every byte-set it acked. Here the freeze quarantines one body
        // and overwrites the acked one with it — the shape of a `freeze_side`
        // that dropped before it preserved.
        //
        // Two byte-sets are attested (the ack plus the quarantine copy), so
        // FREEZE_JUSTIFIED is satisfied and stays silent, and `.frozen` exempts
        // DURABILITY by design — so CONSERVATION is the only oracle holding the
        // totality claim. VERIFY reds alongside it and cannot not: a `.frozen`
        // marker takes the record out of the renderable set while the view still
        // lists it, the same second finding `--break conserve` produces.
        (Break::FreezeLossy, false) => {
            let Some((pkg, fname, _)) = unexcused_ack(buckets, ledger) else {
                return;
            };
            let akey = format!("packages/{pkg}/{fname}");
            let kept = b"vopr: the side the freeze kept".to_vec();
            for bucket in buckets {
                bucket.insert(&format!("{akey}.frozen"), b"{}".to_vec());
                bucket.insert(&format!("_quarantine/{pkg}/{fname}@vopr"), kept.clone());
                bucket.insert(&akey, kept.clone());
            }
        }
        // Demotion totality, the sibling claim: `settle_mirror_quarantine`
        // copies the losing body to `_quarantine/` BEFORE it drops the
        // canonical record, so a demoted filename still owes the fleet the
        // byte-set it acked. Here the demotion fences the name and DESTROYS
        // the body — the shape of a settle that dropped before it preserved.
        //
        // The fence exempts DURABILITY by design (once a demotion settles the
        // canonical key is empty on every bucket), which is exactly why that
        // exemption may not go unguarded: CONSERVATION is the only oracle left
        // holding the totality claim. VERIFY reds alongside it and cannot not —
        // the fence takes the record out of the renderable set while the view
        // still lists it, the same second finding `--break freeze-lossy`
        // produces. Applied fleet-wide, so CONVERGENCE stays quiet.
        (Break::DemoteLossy, false) => {
            let Some((pkg, fname, _)) = unexcused_ack(buckets, ledger) else {
                return;
            };
            let akey = format!("packages/{pkg}/{fname}");
            let preserved = format!("_quarantine/{pkg}/{fname}@");
            for bucket in buckets {
                bucket.insert(
                    &format!("{akey}.mirror-quarantined"),
                    format!(r#"{{"filename":"{fname}"}}"#).into_bytes(),
                );
                let doomed: Vec<String> = bucket
                    .dump()
                    .into_keys()
                    .filter(|key| key.starts_with(&preserved))
                    .chain([akey.clone(), format!("{akey}.meta.json")])
                    .collect();
                let _ = bucket.delete_keys(&doomed).await;
            }
        }
        // The origin lattice walked backwards: a package a private upload
        // claimed reads `mirror` again. Applied fleet-wide so the buckets stay
        // converged and the sidecars stay private — nothing else can catch it.
        (Break::OriginDemoted, false) => {
            let victim = {
                let ledger = ledger.lock().expect("ledger lock");
                ledger.private_claimed.iter().next().cloned()
            };
            if let Some(pkg) = victim {
                let claim = format!(r#"{{"origin":"mirror","nonce":"{}"}}"#, "0".repeat(32));
                for bucket in buckets {
                    bucket.insert(
                        &format!("packages/{pkg}/.origin"),
                        claim.clone().into_bytes(),
                    );
                }
            }
        }
        // The other half of origin exclusivity, one level down: the *claim* is
        // still private, but a live mirror record stands under it — the exact
        // artifact a dependency-confusion attack wants served. Injected as a new
        // filename cloned from a live private record with its sidecar origin
        // rewritten, so nothing already rendered changes: the renderer omits a
        // mirror record under a private claim, so `pypiron verify` agrees the
        // view is correct without it and VISIBILITY never weighs it (it is not
        // in the ledger). Fleet-wide, so CONVERGENCE stays quiet too.
        (Break::MirrorServed, false) => {
            let claimed: Vec<String> = {
                let ledger = ledger.lock().expect("ledger lock");
                ledger.private_claimed.iter().cloned().collect()
            };
            let dump = buckets[0].dump();
            // Every claimed package, not just the first: a schedule that
            // tombstoned one package's whole corpus leaves it with nothing to
            // clone, and stopping there would make the break inert on a run
            // that has a perfectly good subject one name over.
            let found = claimed.iter().find_map(|pkg| {
                let prefix = format!("packages/{pkg}/");
                dump.iter().find_map(|(key, sidecar)| {
                    let akey = key.strip_suffix(".meta.json")?;
                    let fname = akey.strip_prefix(&prefix)?;
                    // A NEW filename, so no record anyone already rendered
                    // changes shape underneath the view.
                    let clone = fname.replace("py3-none", "py2-none");
                    if clone == fname || dump.contains_key(&format!("{prefix}{clone}")) {
                        return None;
                    }
                    let mut doc: serde_json::Value = serde_json::from_slice(sidecar).ok()?;
                    doc.as_object_mut()?
                        .insert("origin".into(), serde_json::Value::String("mirror".into()));
                    Some((
                        format!("{prefix}{clone}"),
                        dump.get(akey)?.clone(),
                        serde_json::to_vec(&doc).ok()?,
                    ))
                })
            });
            if let Some((akey, body, sidecar)) = found {
                for bucket in buckets {
                    bucket.insert(&akey, body.clone());
                    bucket.insert(&format!("{akey}.meta.json"), sidecar.clone());
                }
            }
        }
        // The bytes a bucket serves and the sha256 its own sidecar publishes,
        // pulled apart — the shape a crossed body leaves behind. Planted from
        // the *sidecar* side, because that is the only side no other oracle
        // watches: the body stays exactly the bytes the ack carried, so
        // DURABILITY and CONSERVATION see a healthy artifact; the edit is
        // fleet-wide, so CONVERGENCE sees identical buckets; and every view is
        // re-pointed at the new digest, so `pypiron verify`'s byte-strict
        // re-render from the doctored sidecar still matches what storage
        // serves. Editing the body instead would red DURABILITY first and
        // prove nothing about this oracle.
        //
        // What is left is exactly one claim, and it is the one no oracle
        // anywhere else in the fleet holds: a client that checks the download
        // it was handed against the hash the index published must not have to
        // reject it.
        (Break::Attest, false) => {
            let Some((pkg, fname, _)) = unexcused_ack(buckets, ledger) else {
                return;
            };
            let skey = format!("packages/{pkg}/{fname}.meta.json");
            let fake = pypiron::hash::sha256_hex(b"vopr: a digest no body has");
            for bucket in buckets {
                let dump = bucket.dump();
                let Some(mut doc) = dump
                    .get(&skey)
                    .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok())
                else {
                    continue;
                };
                let Some(published) = doc
                    .get("sha256")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                else {
                    continue;
                };
                if published == fake {
                    continue; // vanishingly unlikely, but a no-op break is a lie
                }
                match doc.as_object_mut() {
                    Some(obj) => obj.insert("sha256".into(), fake.clone().into()),
                    None => continue,
                };
                let Ok(doctored) = serde_json::to_vec(&doc) else {
                    continue;
                };
                bucket.insert(&skey, doctored);
                // Every view that quoted the old digest now quotes the new one,
                // so truth and views still agree byte for byte.
                for (key, view) in dump.iter().filter(|(k, _)| k.starts_with("simple/")) {
                    let repointed = String::from_utf8_lossy(view).replace(&published, &fake);
                    bucket.insert(key, repointed.into_bytes());
                }
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

/// Heal-phase loop bounds. These terminate the driver's loop; they are NOT the
/// convergence claim. What the fleet promises is a *time* — see `STALENESS`
/// below — and these are only sized so a run that is going to converge has room
/// to. A run that hits either bound has already blown the time bound.
const HEAL_ROUNDS: u64 = 12;
const DRAIN_PASSES: u64 = 20;

// ---------------------------------------------------------------------------
// STALENESS: the bounded-agreement invariant.
//
// The product promise, from the owner: after the writes stop, the buckets agree
// "like a few minutes" — with explicit forgiveness when the fleet is holding
// exceptional debt ("20k packages uploaded during a network partition to one
// bucket"). That is a statement about TIME, and this simulator runs on virtual
// time, so measuring it costs nothing.
//
// What replaced what: this used to be `LIVENESS`, which asserted only that the
// fleet reached a fixpoint inside `HEAL_ROUNDS` x `DRAIN_PASSES`. Those are made
// up. A fleet that converged on round 11 and one that converged on round 1 were
// the same green, and "converges, but would have taken an hour" passed silently.
//
//   deadline(debt) = base + STALENESS_PER_ITEM_SECS * debt
//
// `base` is `--staleness-secs` (default `STALENESS_HEALTHY_SECS`), the healthy
// promise. `debt` is the outstanding replication debt at the moment the storm
// ends — see `replication_debt`.
//
// WHY LINEAR. A queue drained at a bounded rate empties in time proportional to
// its depth; that is the only shape a real convergence engine has. Anything
// superlinear is exactly the failure this invariant exists to catch: a fleet
// whose per-item cost grows with the backlog is livelocking, and a deadline that
// grew the same way would excuse it forever. Anything constant contradicts the
// promise in the other direction — it would red a fleet that is working fine and
// merely has 20,000 records to move.
//
// WHY THE DEBT IS SAMPLED ONCE, AT t0. The allowance must not be a function of
// the defect it is measuring. Recomputed from live debt it would grow every time
// the fleet re-created a marker it had failed to consume — which IS the livelock
// shape — so the bound would chase the bug and never fire. Sampled before the
// first heal action, `debt` is a constant of the run: nothing the heal phase
// does, correct or broken, can buy itself more time.
// ---------------------------------------------------------------------------

/// The healthy-operation promise in seconds: a fleet whose outstanding work
/// fits in one cycle of each loop agrees within five minutes of simulated time.
/// Overridable per run with `--staleness-secs`.
const STALENESS_HEALTHY_SECS: u64 = 300;

/// Records one convergence cycle moves. This is `REPL_SWEEP_PAGE`
/// (`src/replicate.rs`): the sweep lists a page of notes and works what it
/// listed, so a backlog deeper than a page is a backlog that needs another
/// cycle. The same page size bounds the `_dirty/` drain and the tree diff.
const STALENESS_CYCLE_RECORDS: u64 = 1_000;

/// One convergence cycle of simulated time. Every wait the driver makes the
/// fleet sit through in the heal phase is 90 s — the drain pass that left debris
/// behind, each leader drain, the post-audit settle — because each is one intent
/// grace, and that is what a cycle costs here. Production's equivalents are
/// `worker_interval` (1 s) and `repl_sweep_interval` (300 s); the work inside a
/// cycle is free in both, so what a deadline measures is how many waits the
/// fleet needed, which is exactly the question.
const STALENESS_CYCLE_SECS: u64 = 90;

/// The view of a bucket two buckets must agree ON. Truth plus per-package views,
/// byte-strict, with two documented normalizations: `.origin` claims compare by
/// state (the nonce is a fresh ABA guard on every write), and the global index
/// compares as its parsed name set (an absent index and an empty one both derive
/// "no packages", and the HTML is a pure function of that set).
///
/// One definition, two readers: the CONVERGENCE oracle at quiescence, and the
/// staleness clock during the heal phase. They were one closure inside the
/// oracle; a second, drifting copy of "agreed" is how a bound comes to be
/// measured against something other than the thing it gates.
fn agreement_projection(dump: &BTreeMap<String, Vec<u8>>) -> BTreeMap<String, Vec<u8>> {
    let mut projected: BTreeMap<String, Vec<u8>> = dump
        .iter()
        .filter(|(k, _)| k.starts_with("packages/") || k.starts_with("simple/"))
        .filter(|(k, _)| *k != "simple/index.json" && *k != "simple/index.html")
        .map(|(k, v)| {
            if k.ends_with("/.origin") {
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
}

/// Protocol debris on one bucket: work the fleet has durably written down and
/// not yet done.
fn marker_count(dump: &BTreeMap<String, Vec<u8>>) -> usize {
    dump.keys()
        .filter(|k| k.starts_with("_dirty/") || k.starts_with("_repl/"))
        .count()
}

/// Records the fleet still owes somebody, in the units the convergence loops
/// move one at a time. Two sources, because a marker is not the only debt there
/// is:
///
///   * every `_repl/` note and `_dirty/` marker, fleet-wide — work with a
///     durable breadcrumb over it, which the sweep and the `_dirty/` drain
///     consume a page at a time;
///   * every key some bucket disagrees with bucket 0 on — the lost-copy debt a
///     crashed fan-out left with no note behind it, which only `reconcile`'s
///     pairwise tree diff will find. Counted per (bucket, key), because each is
///     a separate copy an executor has to make.
///
/// Both are one record of work, so both are counted the same. They were charged
/// at different rates for one revision of this oracle, on the strength of a
/// measurement that turned out to be the harness's sampling rate rather than the
/// product's cadence; with the clock read where the work actually lands, no
/// profile separates them (dev/TESTING.md).
///
/// Raw `dump()` reads only: no `FaultView`, no op-sequence number, no rng draw —
/// measuring the debt must not perturb the schedule that drains it.
fn replication_debt(dumps: &[BTreeMap<String, Vec<u8>>]) -> u64 {
    let markers: usize = dumps.iter().map(marker_count).sum();
    let mut uncovered = 0usize;
    if let Some((first, rest)) = dumps.split_first() {
        let first = agreement_projection(first);
        for dump in rest {
            let other = agreement_projection(dump);
            uncovered += first
                .keys()
                .chain(other.keys())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter(|k| first.get(*k) != other.get(*k))
                .count();
        }
    }
    (markers + uncovered) as u64
}

/// Has the fleet arrived? Every bucket agrees and it owes nothing.
fn fleet_agreed(dumps: &[BTreeMap<String, Vec<u8>>]) -> bool {
    replication_debt(dumps) == 0
}

/// Simulated seconds since `from`. The clock only ever moves forward, so the
/// clamp is defensive, not arithmetic.
fn elapsed_secs(clock: &SimClock, from: time::OffsetDateTime) -> u64 {
    (clock.now_utc() - from).whole_seconds().max(0) as u64
}

/// Cycles a loop that moves `STALENESS_CYCLE_RECORDS` per turn needs for `n`.
fn cycles_for(n: u64) -> u64 {
    n.div_ceil(STALENESS_CYCLE_RECORDS)
}

/// The deadline a run carrying this much debt at `t0` has to agree by:
///
/// ```text
/// base + 90s x ceil(debt / 1000)
/// ```
///
/// Affine in the work. The ceiling is not a rounding convenience: waiting for a
/// periodic sweep costs a whole cycle whether it has one record to move or a
/// thousand, so the first record buys the first cycle and the thousand-and-first
/// buys the second.
/// Hold the staleness clock at the LAST moment the fleet entered agreement, not
/// the first.
///
/// A fleet can agree and then be pulled apart again — a peer's copy stolen after
/// the audit, debris arriving late — and converge a second time. Latching on
/// first arrival reports a convergence the fleet did not keep, and it makes any
/// divergence planted after that moment UNKILLABLE: the clock has already
/// stopped and nothing re-opens it. `--break slow-repair` shipped dead for
/// exactly this reason, killing 0 of 500 seeds while doing real extra repair
/// work the clock could not see (its heal loop peaked a round higher than the
/// control's while reporting a byte-identical convergence time).
///
/// The two callers that pass a *current* agreement verdict use this; the
/// mid-drain and post-audit samples keep their `is_none()` guard, because they
/// only refine an open clock downward and materializing a dump to re-confirm an
/// answer already known would cost a full bucket walk per pass.
fn note_agreement(agreed_at: &mut Option<u64>, agreed: bool, now: impl FnOnce() -> u64) {
    match (agreed, agreed_at.is_some()) {
        (true, false) => *agreed_at = Some(now()),
        (false, true) => *agreed_at = None,
        _ => {}
    }
}

fn staleness_deadline(base_secs: u64, debt: u64) -> u64 {
    base_secs.saturating_add(STALENESS_CYCLE_SECS.saturating_mul(cycles_for(debt)))
}

/// One seed's convergence measurement, kept so the run can print its tightest.
#[derive(Clone, Copy)]
struct Staleness {
    seed: u64,
    /// Simulated seconds from the end of the storm to the first sample where
    /// every bucket agreed and owed nothing; `None` when it never happened.
    secs: Option<u64>,
    deadline: u64,
    debt: u64,
}

impl Staleness {
    /// Share of the deadline spent. A run that never agreed is past it by
    /// construction, so it sorts above every finite measurement.
    fn spent_pct(&self) -> u64 {
        match self.secs {
            None => u64::MAX,
            Some(secs) => secs.saturating_mul(100) / self.deadline.max(1),
        }
    }
}

#[derive(Clone, Copy)]
enum R {
    Durability,
    Visibility,
    Conservation,
    Convergence,
    Staleness,
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
    MergeDivergence,
    FreezeJustified,
    OriginTerminality,
    SelfConsistency,
    UntypedDisappearance,
}

const REACH_SLOTS: usize = 24;

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
    (
        "STALENESS",
        "heal phase whose fleet had convergence work to do",
    ),
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
    (
        "MERGE_DIVERGENCE",
        "merge-resolution object the fleet had to produce",
    ),
    ("FREEZE_JUSTIFIED", ".frozen marker traced to its evidence"),
    (
        "ORIGIN_TERMINALITY",
        "privately-claimed package checked on a bucket",
    ),
    (
        "SELF_CONSISTENCY",
        "stored body re-hashed against its own sidecar",
    ),
    (
        "UNTYPED_DISAPPEARANCE",
        "artifact body removal weighed against what stood beside it",
    ),
];

/// The violation prefix each reach slot's oracle pushes, in `R`'s declaration
/// order — the only join between "this oracle ran" and "this oracle went red",
/// because violations are plain strings that name themselves and nothing else.
/// Empty where a slot owns no violation string: MERGE_DIVERGENCE is a statistic,
/// and the ten classifier slots are tests inside one oracle whose verdicts
/// surface as `AUDIT_*` strings that name a class, not a test. DETERMINISM is
/// decided in `main` and pushes its string there, on the traced seed only, on
/// its way out. Read by `--trace-jsonl` only.
const REACH_VIOLATION_PREFIX: [&str; REACH_SLOTS] = [
    "DURABILITY",
    "VISIBILITY",
    "CONSERVATION",
    "CONVERGENCE",
    "STALENESS",
    "VERIFY",
    "ACK_TOTALITY",
    "TOMBSTONE_MONOTONICITY",
    "DETERMINISM",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    // The oracle is FREEZE_JUSTIFIED; the string it pushes is the negation.
    "FREEZE_UNJUSTIFIED",
    "ORIGIN_TERMINALITY",
    "SELF_CONSISTENCY",
    "UNTYPED_DISAPPEARANCE",
];

/// Zeros that are a known property of the harness or the product, not a hole.
/// `--require-reach` skips exactly these; every other oracle must execute. An
/// entry here is a claim someone has to defend in dev/TESTING.md — and if one
/// starts executing, the meter says so and the entry comes out.
const EXPECTED_ZERO: [(usize, &str); 10] = [
    (
        R::PkgOrdering as usize,
        "workload-unreachable: no rule forbids it; --partition drew one (seed 268); `--break ordering` kills it",
    ),
    (
        R::PkgPoisoned as usize,
        "workload-unreachable: this workload has not drawn one since 1067836; `--break poison` kills it",
    ),
    (
        R::PkgBlind as usize,
        "workload-unreachable: no repair TEST 1 declined has been drawn; `--break blind` kills it",
    ),
    (
        R::PkgRace as usize,
        "harness-unreachable: tick_lock models a lease STRONGER than production ships; `--break race` kills it (dev/TESTING.md)",
    ),
    (
        R::PkgFallback as usize,
        "a repair no test explains is a classifier bug; `--break fallback` kills it",
    ),
    (
        R::GlobalOrdering as usize,
        "workload-unreachable: no rule forbids it; `--break globalindex` kills it",
    ),
    (
        R::GlobalPoisoned as usize,
        "workload-unreachable: drawn at c1b66df (seed 61000246528), closed by c9b2e32; `--break poison` kills it",
    ),
    (
        R::GlobalBlind as usize,
        "workload-unreachable: no flip TEST 1 declined has been drawn; `--break blind` kills it",
    ),
    (
        R::GlobalRace as usize,
        "harness-unreachable: tick_lock models a lease STRONGER than production ships; `--break race` kills it (dev/TESTING.md)",
    ),
    (
        R::GlobalFallback as usize,
        "a flip no test explains is a classifier bug; `--break fallback` kills it",
    ),
];

/// Slots whose subject is a *tail* event, where thin per-seed reach is the
/// honest reading rather than a starved workload — but a flat zero is still a
/// defect. A cross-bucket byte conflict needs two overlapping publishes of one
/// filename with different bytes on two buckets; measured on a fully
/// partitioned soak it lands on 48% of seeds on a fixed multi-bucket profile
/// and 19% under rotation, where a third of the drawn topologies are
/// single-bucket and cannot conflict at all. A floor no healthy run can meet is
/// a gate people learn to ignore, so these two are exempt from the *share*
/// check only: `reach_verdict` still fails them on zero.
const FLOOR_EXEMPT: [usize; 2] = [R::MergeDivergence as usize, R::FreezeJustified as usize];

/// The share of seeds an oracle must execute on. A run-total hit count cannot
/// tell "checked a lot, on every seed" from "checked a lot on a fifth of the
/// seeds and nothing on the rest" — and the second is the same unfalsifiable
/// pass the meter exists to close, one level down. It is not hypothetical: the
/// nightly's two crash-only rows ran a 2x2 corpus their own deletes tombstoned,
/// so DURABILITY/VISIBILITY/CONSERVATION iterated an empty ledger on 4 seeds in
/// 5 while the run printed five figures and `--require-reach` passed (75fd6b2,
/// dev/TESTING.md). The floor sits between what those rows read (20%) and what
/// every green profile reads, so it fails that run and no green one.
///
/// 25% is the floor, not the target: measured over the five nightly rows, every
/// non-excused oracle reaches 97-100% of seeds on the four fixed rows, and the
/// thinnest reading anywhere is ACK_TOTALITY at 64% on the rotating row — where
/// a third of the seed-drawn topologies are single-bucket and the oracle
/// correctly has nothing to weigh. A floor near those numbers would red on
/// sampling noise; this one catches the collapse. Holding a row *at* its
/// measured corpus is `tests/simulation_matrix.rs`'s job, not the floor's.
const REACH_FLOOR_PERCENT: u64 = 25;

struct Reach {
    slots: [AtomicU64; REACH_SLOTS],
    /// Seeds on which the slot executed at least once, and its total as of the
    /// last seed boundary — the pair that turns hits into a per-seed reach.
    seed_hits: [AtomicU64; REACH_SLOTS],
    seed_mark: [AtomicU64; REACH_SLOTS],
    /// Worst heal rounds, and worst drain passes inside one round, any seed used.
    peak_rounds: AtomicU64,
    peak_drains: AtomicU64,
    /// The seed that came closest to the staleness deadline, and what it spent.
    /// A bound with no observed margin is a bound about to break, so the run
    /// prints the tightest one it saw whether or not anything failed.
    worst_staleness: Mutex<Option<Staleness>>,
    /// True once any explored profile had more than one bucket.
    multi_bucket: AtomicBool,
    /// True once any explored seed actually drew a split partition plan.
    partitioned: AtomicBool,
}

static REACH: Reach = Reach::new();

impl Reach {
    const fn new() -> Self {
        Reach {
            slots: [const { AtomicU64::new(0) }; REACH_SLOTS],
            seed_hits: [const { AtomicU64::new(0) }; REACH_SLOTS],
            seed_mark: [const { AtomicU64::new(0) }; REACH_SLOTS],
            peak_rounds: AtomicU64::new(0),
            peak_drains: AtomicU64::new(0),
            worst_staleness: Mutex::new(None),
            multi_bucket: AtomicBool::new(false),
            partitioned: AtomicBool::new(false),
        }
    }
    fn hit(&self, slot: R) {
        self.slots[slot as usize].fetch_add(1, Ordering::Relaxed);
    }
    /// `n` executions at once, for an oracle whose subject is a count the run
    /// already tallied rather than a loop the oracle walks.
    fn hits(&self, slot: R, n: u64) {
        self.slots[slot as usize].fetch_add(n, Ordering::Relaxed);
    }
    /// Close a seed: every slot whose total moved since the last boundary
    /// executed on it. Called once per seed by the driver, after the optional
    /// determinism rerun, so a rerun's hits belong to the seed that caused
    /// them. Same non-perturbing rule as `hit`: relaxed loads over data the
    /// run already computed, outside any simulated node.
    fn end_of_seed(&self) {
        for ((total, mark), seeds) in self.slots.iter().zip(&self.seed_mark).zip(&self.seed_hits) {
            let now = total.load(Ordering::Relaxed);
            if now > mark.swap(now, Ordering::Relaxed) {
                seeds.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    fn peak(cell: &AtomicU64, observed: u64) {
        cell.fetch_max(observed, Ordering::Relaxed);
    }
    /// Keep the seed that spent the largest SHARE of its deadline — not the
    /// largest absolute time. The deadline is per-seed, so the absolute worst
    /// is routinely the seed that was owed the most allowance and used it
    /// comfortably; the share is the number that tells you how close the bound
    /// is to breaking.
    fn record_staleness(&self, observed: Staleness) {
        let mut worst = self.worst_staleness.lock().expect("staleness lock");
        if worst.is_none_or(|w| observed.spent_pct() > w.spent_pct()) {
            *worst = Some(observed);
        }
    }
}

/// Why a zero reading on `slot` is expected. Topology first: a single-bucket
/// sample cannot reach the two replication oracles, and calling that a hole
/// would train everyone to ignore the gate.
fn expected_zero(slot: usize, multi_bucket: bool, partitioned: bool) -> Option<&'static str> {
    if !multi_bucket && (slot == R::Convergence as usize || slot == R::AckTotality as usize) {
        return Some("single-bucket sample — the oracle needs >1 bucket");
    }
    // The merge algebra's conflict arms need two buckets that disagree, which
    // only a partitioned fleet produces. An aligned sample fans every byte out
    // from one writer, so these have no subject — say so rather than train
    // everyone to ignore the gate.
    if !partitioned && (slot == R::MergeDivergence as usize || slot == R::FreezeJustified as usize)
    {
        return Some("aligned sample — the oracle needs a partitioned fleet (--partition)");
    }
    EXPECTED_ZERO
        .iter()
        .find(|(s, _)| *s == slot)
        .map(|(_, why)| *why)
}

/// One slot's verdict: the note to print beside it, and whether
/// `--require-reach` fails on it. Pure, because the gate that proves the
/// oracles ran should not itself be an unexercised claim.
///
/// `seeds_reached / seeds` is the reading a run total cannot give. Both zeros
/// and thin reach are the same defect — an oracle that verified nothing on a
/// seed — so both fail, and an excused slot is silent on both counts: the
/// standing excuses (`EXPECTED_ZERO`, single-bucket topology) say the oracle
/// has no subject here, and a `--break` run is a deliberate defect whose
/// altered workload is nobody's coverage regression.
fn reach_verdict(
    hits: u64,
    seeds_reached: u64,
    seeds: u64,
    excuse: Option<&'static str>,
    broken: bool,
    floor_exempt: bool,
) -> (String, bool) {
    match (hits, excuse) {
        (0, Some(why)) => (format!("  [zero, expected: {why}]"), false),
        (0, None) => ("  [ZERO — NEVER EXECUTED]".to_string(), true),
        // A break exists to reach an oracle, so say so rather than demand
        // the list be edited on the strength of a deliberate defect.
        (_, Some(_)) if broken => ("  [reached under --break]".to_string(), false),
        (_, Some(_)) => (
            "  [now reached — drop it from EXPECTED_ZERO]".to_string(),
            false,
        ),
        (_, None) => {
            let percent = seeds_reached * 100 / seeds.max(1);
            if floor_exempt {
                return (
                    format!("  [tail event, floor waived: {percent}% of seeds]"),
                    false,
                );
            }
            if percent < REACH_FLOOR_PERCENT && !broken {
                (
                    format!(
                        "  [STARVED — executed on {percent}% of seeds, floor \
                         {REACH_FLOOR_PERCENT}%: widen the workload]"
                    ),
                    true,
                )
            } else {
                (String::new(), false)
            }
        }
    }
}

/// Print the table and return the oracles that never executed, or executed on
/// too few seeds, with no standing excuse — what `--require-reach` fails on.
fn report_reach(explored: u64, rechecked: u64, brk: Break) -> Vec<&'static str> {
    let multi = REACH.multi_bucket.load(Ordering::Relaxed);
    let partitioned = REACH.partitioned.load(Ordering::Relaxed);
    let mut unreached = Vec::new();
    println!(
        "vopr: oracle reach over {explored} seeds — executions on NON-TRIVIAL input, and the \
         seeds that got any (a zero, or a thin share, means that oracle verified nothing on the \
         rest):"
    );
    for (slot, (label, unit)) in REACH_METER.iter().enumerate() {
        let hits = REACH.slots[slot].load(Ordering::Relaxed);
        let seeds_reached = REACH.seed_hits[slot].load(Ordering::Relaxed);
        // DETERMINISM is sampled by `--recheck-every` on purpose, so its
        // denominator is the seeds actually re-executed; scoring it out of
        // every seed would read as 5% starvation on a healthy run.
        let (seeds, denom) = if slot == R::Determinism as usize {
            (rechecked, "rechecked")
        } else {
            (explored, "seeds")
        };
        let (note, failed) = reach_verdict(
            hits,
            seeds_reached,
            seeds,
            expected_zero(slot, multi, partitioned),
            brk != Break::None,
            FLOOR_EXEMPT.contains(&slot),
        );
        if failed {
            unreached.push(*label);
        }
        println!("  {label:<35} {hits:>10}  on {seeds_reached:>7}/{seeds} {denom}  {unit}{note}");
    }
    let rounds = REACH.peak_rounds.load(Ordering::Relaxed);
    let drains = REACH.peak_drains.load(Ordering::Relaxed);
    let loop_use = format!(
        "heal loop peaked at {rounds}/{HEAL_ROUNDS} rounds, {drains}/{DRAIN_PASSES} drain passes"
    );
    // A bound with no observed margin is a bound about to break, so the margin
    // is printed on every run, red or green.
    match *REACH.worst_staleness.lock().expect("staleness lock") {
        None => println!("  staleness margin: no seed had convergence work to time — {loop_use}"),
        Some(worst) => {
            let spent = worst.spent_pct();
            match worst.secs {
                Some(secs) => println!(
                    "  staleness margin: tightest seed agreed {secs}s after its last write against \
                     a {}s deadline ({} record(s) of debt = {} cycle(s)) — {spent}% spent, {}% \
                     spare, at --seed {}; {loop_use}",
                    worst.deadline,
                    worst.debt,
                    cycles_for(worst.debt),
                    100u64.saturating_sub(spent),
                    worst.seed,
                ),
                None => println!(
                    "  staleness margin: --seed {} NEVER agreed against a {}s deadline ({} \
                     record(s) of debt) — {loop_use}",
                    worst.seed, worst.deadline, worst.debt,
                ),
            }
        }
    }
    unreached
}

// ---------------------------------------------------------------------------
// The merge meter: which arms of `replicate::decide` the workload actually
// presented, and what the executors left behind.
//
// The reach meter answers "did the ORACLE run". This answers the same question
// of the PRODUCT's merge algebra, and it exists because the answer used to be
// "only Copy and Noop, ever" — a whole subsystem carried by unit tests and
// nothing else. Two independent readings, because either alone is arguable:
//
//   * verdicts — every bucket pair x filename run through the real
//     `replicate::decide` on raw dumps, at the end of the chaos phase and at
//     each heal round. It proves the *situations* were produced. It is a
//     sample, so it under-counts arms the executors resolve between snapshots.
//   * evidence — objects at quiescence only a merge executor can create:
//     `.frozen` markers, `_quarantine/` bodies, `.mirror-quarantined` markers.
//     Durable and exact, but coarser: `Freeze`, `QuarantineLoser` and
//     `Supersede` all quarantine.
//
// Printed, never gated: conflicts are far rarer than the 25%-of-seeds reach
// floor, so one `R::` slot per verdict would red a healthy run. `R::MergeDivergence`
// is the single gated slot, and it measures the *workload* (an acked filename
// two buckets committed different bytes under), not any one verdict.
//
// Non-perturbing by construction: raw `dump()` reads, a pure function, relaxed
// atomics. No `FaultView`, no rng, no await.
// ---------------------------------------------------------------------------

const VERDICT_SLOTS: usize = 11;
const VERDICT_LABELS: [&str; VERDICT_SLOTS] = [
    "Noop",
    "Copy",
    "AdoptSidecar",
    "Supersede",
    "QuarantineLoser",
    "Freeze",
    "PropagateFreeze",
    "FinishFreeze",
    "Tombstone",
    "Defer",
    "SettleMirrorQuarantine",
];

const EVIDENCE_SLOTS: usize = 3;
const EVIDENCE_LABELS: [(&str, &str); EVIDENCE_SLOTS] = [
    (
        "<file>.frozen",
        "Freeze / PropagateFreeze / FinishFreeze froze a side",
    ),
    (
        "_quarantine/<pkg>/<file>@sha",
        "a losing body preserved (Freeze, QuarantineLoser, Supersede)",
    ),
    (
        "<file>.mirror-quarantined",
        "a mirror->private demotion fenced (replicates; the body moves to _quarantine/)",
    ),
];

struct Merge {
    verdicts: [AtomicU64; VERDICT_SLOTS],
    verdict_seeds: [AtomicU64; VERDICT_SLOTS],
    verdict_mark: [AtomicU64; VERDICT_SLOTS],
    evidence: [AtomicU64; EVIDENCE_SLOTS],
    evidence_seeds: [AtomicU64; EVIDENCE_SLOTS],
    evidence_mark: [AtomicU64; EVIDENCE_SLOTS],
}

static MERGE: Merge = Merge::new();

impl Merge {
    const fn new() -> Self {
        Merge {
            verdicts: [const { AtomicU64::new(0) }; VERDICT_SLOTS],
            verdict_seeds: [const { AtomicU64::new(0) }; VERDICT_SLOTS],
            verdict_mark: [const { AtomicU64::new(0) }; VERDICT_SLOTS],
            evidence: [const { AtomicU64::new(0) }; EVIDENCE_SLOTS],
            evidence_seeds: [const { AtomicU64::new(0) }; EVIDENCE_SLOTS],
            evidence_mark: [const { AtomicU64::new(0) }; EVIDENCE_SLOTS],
        }
    }
    /// Same seed-boundary bookkeeping the reach meter uses, so both tables read
    /// "hits, and the seeds that got any" on one denominator.
    fn end_of_seed(&self) {
        for (totals, (marks, seeds)) in [
            (
                &self.verdicts[..],
                (&self.verdict_mark[..], &self.verdict_seeds[..]),
            ),
            (
                &self.evidence[..],
                (&self.evidence_mark[..], &self.evidence_seeds[..]),
            ),
        ] {
            for ((total, mark), seed) in totals.iter().zip(marks).zip(seeds) {
                let now = total.load(Ordering::Relaxed);
                if now > mark.swap(now, Ordering::Relaxed) {
                    seed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

fn verdict_slot(verdict: &Verdict) -> usize {
    match verdict {
        Verdict::Noop => 0,
        Verdict::Copy(_) => 1,
        Verdict::AdoptSidecar(_) => 2,
        Verdict::Supersede(_) => 3,
        Verdict::QuarantineLoser(_) => 4,
        Verdict::Freeze => 5,
        Verdict::PropagateFreeze(_) => 6,
        Verdict::FinishFreeze => 7,
        Verdict::Tombstone => 8,
        Verdict::Defer => 9,
        Verdict::SettleMirrorQuarantine => 10,
    }
}

/// Companion suffixes a record name can wear; stripping one yields the
/// filename the merge reasons about.
const RECORD_SUFFIXES: [&str; 6] = [
    ".meta.json",
    ".tombstone",
    ".frozen",
    ".mirror-quarantined",
    ".metadata",
    ".provenance",
];

/// Every (package, filename) any bucket holds a record object for.
fn record_names(
    dumps: &[BTreeMap<String, Vec<u8>>],
) -> std::collections::BTreeSet<(String, String)> {
    let mut names = std::collections::BTreeSet::new();
    for dump in dumps {
        for key in dump.keys() {
            let Some((pkg, name)) = key
                .strip_prefix("packages/")
                .and_then(|rest| rest.split_once('/'))
            else {
                continue;
            };
            if name.contains('/') {
                continue;
            }
            let base = RECORD_SUFFIXES
                .iter()
                .find_map(|suffix| name.strip_suffix(suffix))
                .unwrap_or(name);
            if pypiron::sidecar::is_artifact(base) {
                names.insert((pkg.to_string(), base.to_string()));
            }
        }
    }
    names
}

/// One bucket's view of one filename, rebuilt from raw bytes exactly as
/// `replicate`'s reader assembles it.
fn record_from_dump(dump: &BTreeMap<String, Vec<u8>>, pkg: &str, fname: &str) -> Record {
    let akey = format!("packages/{pkg}/{fname}");
    let has = |suffix: &str| dump.contains_key(&format!("{akey}{suffix}"));
    Record {
        sidecar: dump
            .get(&format!("{akey}.meta.json"))
            .and_then(|bytes| serde_json::from_slice(bytes).ok()),
        has_artifact: dump.contains_key(&akey),
        has_metadata: has(".metadata"),
        has_provenance: has(".provenance"),
        tombstoned: has(".tombstone"),
        frozen: has(".frozen"),
        mirror_quarantined: has(".mirror-quarantined"),
        pkg_origin: dump
            .get(&format!("packages/{pkg}/.origin"))
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
            .and_then(|doc| {
                doc.get("origin")
                    .and_then(|o| o.as_str())
                    .and_then(pypiron::replicate::Origin::parse)
            }),
    }
}

/// Run every bucket pair x filename through the real merge decision and tally
/// the verdicts this world would produce.
fn sample_verdicts(dumps: &[BTreeMap<String, Vec<u8>>], viz: Option<&Arc<Trace>>) {
    if dumps.len() < 2 {
        return;
    }
    for (pkg, fname) in record_names(dumps) {
        for (i, a) in dumps.iter().enumerate() {
            for (j, b) in dumps.iter().enumerate().skip(i + 1) {
                let verdict = pypiron::replicate::decide(
                    &record_from_dump(a, &pkg, &fname),
                    &record_from_dump(b, &pkg, &fname),
                );
                let slot = verdict_slot(&verdict);
                MERGE.verdicts[slot].fetch_add(1, Ordering::Relaxed);
                // `Noop` is most of the sample and says nothing; the timeline
                // shows only the verdicts a merge would actually apply.
                if let (Some(viz), true) = (viz, slot != 0) {
                    viz.emit(
                        "decide",
                        serde_json::json!({
                            "pkg": pkg,
                            "file": fname,
                            "a": i,
                            "b": j,
                            "verdict": VERDICT_LABELS.get(slot).copied().unwrap_or("?"),
                        }),
                    );
                }
            }
        }
    }
}

/// Count the durable objects only a merge executor writes.
fn count_merge_evidence(dumps: &[BTreeMap<String, Vec<u8>>]) {
    for dump in dumps {
        for key in dump.keys() {
            let slot = if key.ends_with(".frozen") {
                0
            } else if key.starts_with("_quarantine/") {
                1
            } else if key.ends_with(".mirror-quarantined") {
                2
            } else {
                continue;
            };
            MERGE.evidence[slot].fetch_add(1, Ordering::Relaxed);
            // The one gated reading of the merge algebra. Its unit is a
            // resolution the fleet actually had to perform, not any single
            // verdict: an acked filename two buckets committed different bytes
            // under is far too rare to hold a 25%-of-seeds floor (measured at
            // 1-6%), and a floor no healthy run can meet is a gate people
            // learn to ignore. This one reads 33-47% of seeds on a partitioned
            // soak and drops to zero the moment conflicts stop being produced,
            // which is the regression it exists to catch.
            REACH.hit(R::MergeDivergence);
        }
    }
}

fn report_merge(explored: u64) {
    println!(
        "vopr: merge algebra over {explored} seeds — every bucket pair x filename run through the \
         real `replicate::decide` at the end of the chaos phase and at each heal round (a sample: \
         verdicts the executors resolve between snapshots are missed):"
    );
    for (slot, label) in VERDICT_LABELS.iter().enumerate() {
        let hits = MERGE.verdicts[slot].load(Ordering::Relaxed);
        let seeds = MERGE.verdict_seeds[slot].load(Ordering::Relaxed);
        let note = if hits == 0 { "  [never presented]" } else { "" };
        println!("  decide -> {label:<26} {hits:>10}  on {seeds:>7}/{explored} seeds{note}");
    }
    println!("vopr: merge evidence at quiescence — objects only a merge executor creates:");
    for (slot, (label, what)) in EVIDENCE_LABELS.iter().enumerate() {
        let hits = MERGE.evidence[slot].load(Ordering::Relaxed);
        let seeds = MERGE.evidence_seeds[slot].load(Ordering::Relaxed);
        let note = if hits == 0 { "  [never created]" } else { "" };
        println!("  {label:<32} {hits:>10}  on {seeds:>7}/{explored} seeds  {what}{note}");
    }
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
//
//     This is the ONE outcome dev/DESIGN.md budgets for by name ("a brief
//     dual-leader window costs at worst duplicate work plus an audit-healed
//     view"), so the taxonomy needs the slot whether or not this harness
//     draws it. Production has no fencing token: `is_leader()` is a
//     point-in-time read, `rebuild_package_indexes` never revalidates it, and
//     the per-package view PUT is `put_if_changed` — an unconditional write,
//     unlike the global index's CAS. A rebuild that outlives the lease TTL
//     therefore clobbers its successor's fresher view. Without this arm that
//     clobber falls through to FALLBACK and fails the seed as class 2, which
//     sends triage hunting a signal-loss bug in the marker protocol that is
//     not there. Measured: with the harness's fleet-wide `tick_lock` removed
//     from the leader audit (production spawns it off the tick loop), the arm
//     reaches and reports on real product behavior — see dev/TESTING.md.
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

impl OpFate {
    /// The `--force-fault` spelling, and what a sweep prints. `Ok` is the
    /// absence of a fault, so it is not a forceable fate.
    fn spelling(self) -> &'static str {
        match self {
            OpFate::Ok => "ok",
            OpFate::Fail => "fail",
            OpFate::Crash => "crash",
        }
    }
}

/// The two fates a depth-1 sweep forces, in the order it sweeps them.
const SWEPT_FATES: [OpFate; 2] = [OpFate::Fail, OpFate::Crash];

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
    /// `--sweep-faults`/`--force-fault`: the one op-sequence number whose fate
    /// this run overrides, and what to override it to. `None` in every ordinary
    /// run — and the check costs one `Option` test on the normal path, drawing
    /// nothing, so no pinned seed moves.
    forced: Option<(u64, OpFate)>,
    /// Whether that override actually fired. It cannot when the issuing node is
    /// already down or the heal phase has begun, and a sweep that forced
    /// nothing verified nothing.
    forced_fired: AtomicBool,
    /// `op_seq` when the heal phase began: the number of ops a fault can be
    /// forced at, since `admit` stops overriding once `healing` is set.
    /// `u64::MAX` until `heal`.
    heal_seq: AtomicU64,
    rng_stream: Mutex<Rng>,
    trace: Mutex<TraceHasher>,
    /// Effect history for the audit-repair classifier. Pure observation.
    obs: Observer,
    /// The selected deliberate defect, `Break::None` in every ordinary run.
    brk: Break,
    /// Visualizer side channel, `None` unless `--trace-jsonl` was passed. Pure
    /// observation — see [`Trace`].
    viz: Option<Arc<Trace>>,
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

// ---------------------------------------------------------------------------
// The visualizer side channel (`--trace-jsonl`, dev/scripts/viz).
// ---------------------------------------------------------------------------

/// Ceiling on buffered events. A traced profile is ONE seed of 8-200 ops, which
/// is a few thousand events; this exists only so a mistake cannot eat the
/// machine. Past it events are dropped and `summary.truncated` says so.
const VIZ_CAP: u64 = 2_000_000;

/// JSONL timeline of one seed's run, for the run visualizer.
///
/// Non-perturbing by construction, on the same terms as the reach meter and the
/// `Observer`: every field is data the run already computed, buffered in memory
/// and written once at the end. No rng draw, no op-sequence number consumed, no
/// `TraceHasher::record` call, no `.await`, no `FaultView` — every world
/// snapshot reads the raw `SimStorage::dump()`. `main` flushes it explicitly,
/// because `main` leaves through `std::process::exit` and no `Drop` runs.
///
/// One mutex holds everything mutable. It is a leaf lock: taken and released
/// inside one method, never held across the rng stream, the trace hasher, the
/// crash map, the observer, the ledger — or an await.
struct Trace {
    path: String,
    /// The exact command line that produced this file.
    cmd: String,
    commit: String,
    inner: Mutex<TraceInner>,
}

struct TraceInner {
    lines: Vec<String>,
    /// Next event index. Strictly sequential, no gaps — the player's contract.
    next: u64,
    truncated: bool,
    /// Set once at the top of `run_seed`; the source of every `sim` stamp. The
    /// `Arc` outlives the run, so events `main` emits afterwards carry the sim
    /// time the run ended at rather than a wall clock nobody asked for.
    clock: Option<Arc<SimClock>>,
    /// Last emitted `"<len>:<sha8>"` map per bucket, so a `world` event carries
    /// only the keys that moved.
    world: Vec<BTreeMap<String, String>>,
    /// Nodes already reported down, so only the FIRST op to admit with
    /// `OpFate::Crash` — the one that flipped the flag — gets a `crash` tick.
    down: std::collections::BTreeSet<usize>,
}

impl Trace {
    fn new(path: String) -> Arc<Trace> {
        // Recorded once, in `main`, before any run: a subprocess here cannot
        // reach the simulated world.
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let commit = std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|out| out.status.success())
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .filter(|sha| !sha.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        Arc::new(Trace {
            path,
            cmd: format!("cargo run --release --example vopr -- {}", argv.join(" ")),
            commit,
            inner: Mutex::new(TraceInner {
                lines: Vec::new(),
                next: 0,
                truncated: false,
                clock: None,
                world: Vec::new(),
                down: std::collections::BTreeSet::new(),
            }),
        })
    }

    /// Adopt the run's virtual clock. Called once, at the top of `run_seed`.
    fn attach(&self, clock: &Arc<SimClock>) {
        self.inner.lock().expect("viz lock").clock = Some(clock.clone());
    }

    fn emit(&self, t: &str, body: serde_json::Value) {
        let mut inner = self.inner.lock().expect("viz lock");
        Self::push(&mut inner, t, body);
    }

    /// Stamp the universal envelope (`t`/`i`/`sim`/`ts`) onto one event body.
    /// `ts` is always null: this producer has virtual time only.
    fn push(inner: &mut TraceInner, t: &str, body: serde_json::Value) {
        if inner.next >= VIZ_CAP {
            inner.truncated = true;
            return;
        }
        let sim = inner.clock.as_ref().map(|c| c.now_rfc3339());
        let mut obj = serde_json::Map::new();
        obj.insert("t".into(), serde_json::Value::from(t));
        obj.insert("i".into(), serde_json::Value::from(inner.next));
        obj.insert(
            "sim".into(),
            sim.map_or(serde_json::Value::Null, serde_json::Value::from),
        );
        obj.insert("ts".into(), serde_json::Value::Null);
        if let serde_json::Value::Object(fields) = body {
            obj.extend(fields);
        }
        if let Ok(line) = serde_json::to_string(&serde_json::Value::Object(obj)) {
            inner.lines.push(line);
            inner.next += 1;
        }
    }

    /// One storage op admitted through `FaultView::gate`. A `Crash` fate is also
    /// the node's death when this is the first op to see it down — `admit`
    /// short-circuits every later op on a crashed node to the same fate — so the
    /// transition gets its own `crash` tick for the timeline.
    #[allow(clippy::too_many_arguments)]
    fn op(
        &self,
        node: usize,
        bucket: usize,
        verb: &str,
        key: &str,
        seq: u64,
        fate: OpFate,
        delay: std::time::Duration,
        blackholed: bool,
    ) {
        let mut inner = self.inner.lock().expect("viz lock");
        Self::push(
            &mut inner,
            "op",
            serde_json::json!({
                "node": node,
                "bucket": bucket,
                "verb": verb,
                "key": key,
                "seq": seq,
                "fate": match fate {
                    OpFate::Ok => "ok",
                    OpFate::Fail => "fail",
                    OpFate::Crash => "crash",
                },
                "delay_ns": u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX),
                "blackholed": blackholed,
            }),
        );
        if matches!(fate, OpFate::Crash) && inner.down.insert(node) {
            Self::push(
                &mut inner,
                "crash",
                serde_json::json!({ "node": node, "reason": "scheduled" }),
            );
        }
    }

    /// A node came back: the next `OpFate::Crash` on it is a new death.
    fn restart(&self, node: usize, phase: &'static str) {
        let mut inner = self.inner.lock().expect("viz lock");
        inner.down.remove(&node);
        Self::push(
            &mut inner,
            "restart",
            serde_json::json!({ "node": node, "phase": phase }),
        );
    }

    /// The only source of bucket state: one event per bucket whose contents
    /// moved since the last snapshot. `full` replaces the reader's map outright
    /// (the empty baseline); otherwise the event carries a delta. Values are
    /// `"<len>:<sha8>"` — the length alone cannot tell the byte-identity story
    /// that same-length corruption and cross-bucket convergence both turn on.
    fn world(&self, dumps: &[BTreeMap<String, Vec<u8>>], full: bool) {
        let mut inner = self.inner.lock().expect("viz lock");
        if inner.world.len() < dumps.len() {
            inner.world.resize(dumps.len(), BTreeMap::new());
        }
        for (b, dump) in dumps.iter().enumerate() {
            let now: BTreeMap<String, String> = dump
                .iter()
                .map(|(key, bytes)| {
                    let sha = sha256_hex(bytes);
                    (
                        key.clone(),
                        format!("{}:{}", bytes.len(), sha.get(..8).unwrap_or(&sha)),
                    )
                })
                .collect();
            let mut put = serde_json::Map::new();
            for (key, value) in &now {
                if inner.world[b].get(key) != Some(value) {
                    put.insert(key.clone(), serde_json::Value::from(value.clone()));
                }
            }
            let del: Vec<String> = inner.world[b]
                .keys()
                .filter(|key| !now.contains_key(*key))
                .cloned()
                .collect();
            if !full && put.is_empty() && del.is_empty() {
                continue;
            }
            Self::push(
                &mut inner,
                "world",
                serde_json::json!({
                    "b": b,
                    "full": full,
                    "put": serde_json::Value::Object(put),
                    "del": del,
                }),
            );
            inner.world[b] = now;
        }
    }

    /// `(events_emitted, truncated)` for the `summary` line.
    fn stats(&self) -> (u64, bool) {
        let inner = self.inner.lock().expect("viz lock");
        (inner.next, inner.truncated)
    }

    fn flush(&self) {
        let inner = self.inner.lock().expect("viz lock");
        let mut out = inner.lines.join("\n");
        out.push('\n');
        if let Err(e) = std::fs::write(&self.path, out) {
            eprintln!("vopr: could not write --trace-jsonl {}: {e}", self.path);
        }
    }
}

/// Advance virtual time and label the jump for the visualizer. One call per
/// reason the driver has to move the clock.
fn advance_clock(clock: &SimClock, secs: u64, why: &'static str, viz: &Option<Arc<Trace>>) {
    clock.advance(std::time::Duration::from_secs(secs));
    if let Some(viz) = viz {
        viz.emit(
            "clock",
            serde_json::json!({ "by_secs": secs, "to": clock.now_rfc3339(), "why": why }),
        );
    }
}

/// Snapshot the raw buckets into a `world` event. Dumps nothing when the flag
/// is off, so the default path is untouched.
fn viz_world(viz: &Option<Arc<Trace>>, buckets: &[Arc<SimStorage>], full: bool) {
    if let Some(viz) = viz {
        let dumps: Vec<BTreeMap<String, Vec<u8>>> =
            buckets.iter().map(|bucket| bucket.dump()).collect();
        viz.world(&dumps, full);
    }
}

impl FaultPlan {
    fn new(
        seed: u64,
        nodes: usize,
        fail_percent: u64,
        forced: Option<(u64, OpFate)>,
        brk: Break,
        viz: Option<Arc<Trace>>,
    ) -> Arc<Self> {
        Arc::new(FaultPlan {
            op_seq: AtomicU64::new(0),
            fail_percent,
            crash_at: Mutex::new(BTreeMap::new()),
            crashed: (0..nodes).map(|_| AtomicBool::new(false)).collect(),
            healing: AtomicBool::new(false),
            forced,
            forced_fired: AtomicBool::new(false),
            heal_seq: AtomicU64::new(u64::MAX),
            rng_stream: Mutex::new(Rng::new(seed ^ 0xFA_11_7E_57)),
            trace: Mutex::new(TraceHasher {
                hash: 0xcbf29ce484222325,
                events: 0,
                log: std::env::var_os("VOPR_TRACE").map(|_| Vec::new()),
            }),
            obs: Observer::default(),
            brk,
            viz,
        })
    }

    fn heal(&self) {
        self.heal_seq
            .store(self.op_seq.load(Ordering::SeqCst), Ordering::SeqCst);
        self.healing.store(true, Ordering::SeqCst);
    }

    /// How many ops ran before the heal phase — the sweepable range.
    fn chaos_ops(&self) -> u64 {
        self.heal_seq
            .load(Ordering::SeqCst)
            .min(self.op_seq.load(Ordering::SeqCst))
    }

    fn schedule_crash_soon(&self, node: usize, within_ops: u64) {
        let at = self.op_seq.load(Ordering::SeqCst)
            + 1
            + self.rng_stream.lock().expect("rng lock").below(within_ops);
        self.crash_at
            .lock()
            .expect("crash lock")
            .insert(at | ((node as u64) << 56), ());
        // The crash map's guard is already dropped: no two locks held at once.
        if let Some(viz) = &self.viz {
            viz.emit(
                "crash_sched",
                serde_json::json!({ "node": node, "at_seq": at, "within": within_ops }),
            );
        }
    }

    fn restart(&self, node: usize) {
        self.crashed[node].store(false, Ordering::SeqCst);
        let mut pending = self.crash_at.lock().expect("crash lock");
        pending.retain(|key, _| (key >> 56) as usize != node);
    }

    fn node_crashed(&self, node: usize) -> bool {
        self.crashed[node].load(Ordering::SeqCst)
    }

    /// Decide this op's fate, unique virtual latency, and op-sequence number.
    ///
    /// The `seq` in the tuple is the value already hashed below; it is returned
    /// only so `gate` can label the visualizer's event without a second read of
    /// `op_seq`. Nothing else in this function may change: what it feeds
    /// `TraceHasher::record`, in what order, and how much entropy it draws are
    /// what every pinned seed in the corpus means.
    fn admit(
        &self,
        node: usize,
        bucket: usize,
        op: &str,
        key: &str,
    ) -> (OpFate, std::time::Duration, u64) {
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
            return (OpFate::Crash, delay, seq);
        }
        if self.healing.load(Ordering::SeqCst) {
            return (OpFate::Ok, delay, seq);
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
            return (OpFate::Crash, delay, seq);
        }
        let fail = {
            let mut rng = self.rng_stream.lock().expect("rng lock");
            rng.chance(self.fail_percent)
        };
        // Depth-1 sweep: this one op's fate is overridden. The draw above still
        // happened — `chance` consumes entropy even at 0% — so every op before
        // this one is bit-identical to the sweep's clean baseline, which is what
        // makes "op k" name the same op across all of a seed's swept runs.
        // Deliberately below the crashed/healing/crash_at arms: a dead node
        // stays dead, and the heal phase stays fault-free so the convergence
        // oracles still mean something.
        if let Some((at, fate)) = self.forced {
            if seq == at {
                self.forced_fired.store(true, Ordering::SeqCst);
                if matches!(fate, OpFate::Crash) {
                    self.crashed[node].store(true, Ordering::SeqCst);
                }
                return (fate, delay, seq);
            }
        }
        if fail {
            (OpFate::Fail, delay, seq)
        } else {
            (OpFate::Ok, delay, seq)
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
        let (fate, delay, seq) = self.plan.admit(self.node, self.bucket, op, key);
        // Recorded here rather than in `admit`: this is where the fate, the
        // latency and the blackhole decision below all coexist, synchronously,
        // before the first await. Pure observation — no rng, no op-sequence
        // number, no `TraceHasher::record`.
        if let Some(viz) = &self.plan.viz {
            let blackholed = matches!(fate, OpFate::Ok) && self.bucket == 1 && self.fanout_break();
            viz.op(
                self.node,
                self.bucket,
                op,
                key,
                seq,
                fate,
                delay,
                blackholed,
            );
        }
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
            if let Some(viz) = &self.plan.viz {
                viz.emit(
                    "drop",
                    serde_json::json!({ "node": self.node, "bucket": self.bucket, "key": key }),
                );
            }
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
    /// Gated like every other op, and for the same reason: this is the
    /// global-index currency probe, so an ungated one would be a storage read
    /// on the fleet's hottest write path that no schedule can ever fail. It
    /// replaced a `list_all` that was gated; leaving it open would have
    /// quietly narrowed the fault space exactly where it was last needed.
    async fn head_etag(&self, key: &str) -> Result<Option<String>> {
        self.gate("head_etag", key).await?;
        self.inner.head_etag(key).await
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

/// One acknowledged upload. Recorded per ack, never per filename: a
/// partitioned fleet can ack two *different* byte-sets under one name (two
/// nodes writing to two buckets), and an overwrite-on-ack map would keep only
/// the last — then demand exactly those bytes on every bucket, red-flagging a
/// merge that legitimately kept the other side.
struct Ack {
    /// `pinned.index` at the moment `publish_record` returned Ok — where these
    /// bytes were authored. CONSERVATION and FREEZE_JUSTIFIED both need it: a
    /// conflict loser survives under `_quarantine/` on the bucket that lost.
    bucket: usize,
    body: Vec<u8>,
    /// Monotonic under the ledger mutex — the total order the harness observed.
    /// Diagnostics only: `last_ack_deleted` still owns resurrection ordering.
    seq: u64,
}

#[derive(Default)]
struct Ledger {
    /// EVERY acknowledged upload, in ack order: (pkg, filename) -> acks.
    acked: BTreeMap<(String, String), Vec<Ack>>,
    /// Packages for which a PRIVATE publish acked. Origin exclusivity is
    /// monotone — private is terminal — so the fleet may never afterwards
    /// settle on a mirror claim for one of these (ORIGIN_TERMINALITY).
    private_claimed: std::collections::BTreeSet<String>,
    /// Filenames an acknowledged (204) delete removed.
    deleted: std::collections::BTreeSet<(String, String)>,
    /// Deletes that were AUTHORIZED per filename: issued and observed to have
    /// tombstoned. A superset of `deleted` — a delete that crashed between its
    /// tombstone and its 204 destroyed just as legitimately, and the two freeze
    /// oracles below have to know that, because `freeze_side` can only preserve
    /// a body the delete left standing. Counted rather than a set: a filename
    /// can be deleted, republished and deleted again, and a later refusal (404
    /// on a filename already gone) must not withdraw an earlier destruction.
    authorized_deletes: BTreeMap<(String, String), u32>,
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
    next_ack_seq: u64,
}

impl Ledger {
    /// Whether an unrefused delete was issued for this filename — the workload
    /// fact the freeze oracles need: bytes a delete destroyed are not bytes a
    /// racing `freeze_side` failed to preserve.
    fn authorized_delete(&self, pkg: &str, fname: &str) -> bool {
        self.authorized_deletes
            .get(&(pkg.to_string(), fname.to_string()))
            .is_some_and(|count| *count > 0)
    }

    fn record_ack(&mut self, pkg: &str, fname: &str, bucket: usize, body: Vec<u8>) {
        self.next_ack_seq += 1;
        let seq = self.next_ack_seq;
        self.acked
            .entry((pkg.to_string(), fname.to_string()))
            .or_default()
            .push(Ack { bucket, body, seq });
    }
}

/// The distinct byte-sets acknowledged under one filename. One is the ordinary
/// case; two is a cross-bucket byte conflict the merge is entitled to resolve
/// either way — but never to satisfy with bytes nobody acked, and never to
/// leave the fleet split over.
fn acked_bodies(acks: &[Ack]) -> std::collections::BTreeSet<&[u8]> {
    acks.iter().map(|a| a.body.as_slice()).collect()
}

/// Every `_quarantine/<pkg>/<file>@<sha12>` body the fleet preserved for one
/// filename. This is the *evidence* an authorized merge left behind when it
/// stopped serving a byte-set: `QuarantineLoser`, `Supersede` and `Freeze` all
/// copy the losing body here before touching it, and the operator is alarmed.
/// Nothing in the product ever deletes from `_quarantine/`.
fn quarantined_bodies<'a>(
    dumps: &'a [BTreeMap<String, Vec<u8>>],
    pkg: &str,
    fname: &str,
) -> std::collections::BTreeSet<&'a [u8]> {
    let prefix = format!("_quarantine/{pkg}/{fname}@");
    dumps
        .iter()
        .flat_map(|dump| dump.iter())
        .filter(|(key, _)| key.starts_with(&prefix))
        .map(|(_, bytes)| bytes.as_slice())
        .collect()
}

/// Per-bucket record shape, for a failure message somebody has to triage: what
/// each bucket holds under one filename, and what the fleet quarantined.
fn marker_census(dumps: &[BTreeMap<String, Vec<u8>>], pkg: &str, fname: &str) -> String {
    let akey = format!("packages/{pkg}/{fname}");
    let per_bucket: Vec<String> = dumps
        .iter()
        .enumerate()
        .map(|(idx, dump)| {
            let mut shape: Vec<&str> = Vec::new();
            if dump.contains_key(&akey) {
                shape.push("body");
            }
            for suffix in RECORD_SUFFIXES {
                if dump.contains_key(&format!("{akey}{suffix}")) {
                    shape.push(suffix);
                }
            }
            format!("b{idx}[{}]", shape.join(" "))
        })
        .collect();
    let quarantine: Vec<String> = quarantined_bodies(dumps, pkg, fname)
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();
    format!("{} quarantine={quarantine:?}", per_bucket.join(" "))
}

/// Whether the renderer deliberately leaves this record out of the package
/// view (`worker::load_file_metadata`, mirrored by `verify::suppressed_mirror`):
/// a mirror record under a private claim, or a quarantined mirror body a
/// private upload has not superseded. Invisible-by-design, not a lost record —
/// the dependency-confusion boundary, and the one thing VISIBILITY must not
/// mistake for a dropped index entry.
fn renderer_omits(dump: &BTreeMap<String, Vec<u8>>, pkg: &str, fname: &str) -> bool {
    let akey = format!("packages/{pkg}/{fname}");
    let origin = dump
        .get(&format!("{akey}.meta.json"))
        .and_then(|bytes| serde_json::from_slice::<pypiron::sidecar::Sidecar>(bytes).ok())
        .and_then(|sc| sc.origin);
    let quarantined = dump.contains_key(&format!("{akey}.mirror-quarantined"));
    let claim = dump
        .get(&format!("packages/{pkg}/.origin"))
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
        .and_then(|doc| {
            doc.get("origin")
                .and_then(|o| o.as_str())
                .map(str::to_string)
        });
    (claim.as_deref() == Some("private") && origin.as_deref() == Some("mirror"))
        || (quarantined && origin.as_deref() != Some("private"))
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
    /// The fleet a profile describes. It takes the whole `Profile` rather than
    /// five of its fields: every fault dimension the plan needs already lives
    /// there together, and the loose-field form had grown past the point where
    /// a caller could get the order right by reading it.
    fn new(seed: u64, profile: &Profile, clock: Arc<SimClock>, viz: Option<Arc<Trace>>) -> Fleet {
        let plan = FaultPlan::new(
            seed,
            profile.nodes,
            profile.fail_percent,
            profile.forced,
            profile.brk,
            viz,
        );
        let buckets: Vec<Arc<SimStorage>> = (0..profile.buckets)
            .map(|_| SimStorage::new(clock.clone()))
            .collect();
        let nodes = (0..profile.nodes)
            .map(|node| Node {
                state: build_node_state(&buckets, node, &plan),
                tasks: Vec::new(),
            })
            .collect();
        let tick_lock = (0..profile.buckets)
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
        if let Some(viz) = &self.plan.viz {
            viz.emit(
                "crash",
                serde_json::json!({ "node": node, "reason": "driver" }),
            );
        }
        // The plan already (or will) park the node's ops; abort everything it
        // had in flight — in-memory state dies with the process.
        for task in self.nodes[node].tasks.drain(..) {
            task.abort();
        }
    }

    fn restart_node(&mut self, node: usize, phase: &'static str) {
        self.crash_node(node);
        self.plan.restart(node);
        // Cold caches: a fresh AppState over the same buckets.
        let plan = self.plan.clone();
        self.nodes[node].state = build_node_state(&self.buckets, node, &plan);
        if let Some(viz) = &self.plan.viz {
            viz.restart(node, phase);
        }
    }
}

// ---------------------------------------------------------------------------
// The partitioned fleet: writers that do not all pin bucket 0.
//
// Every writer in this harness used to pin the *selected* bucket, which
// `BucketSet::new` fixes at index 0 and nothing here ever switches. So every
// byte on buckets 1..N arrived as a COPY of bucket 0's — two buckets could
// never disagree about the bytes under one live filename, and
// `replicate::decide`'s merge algebra was dead code in simulation: only `Copy`
// and `Noop` ever fired. dev/DESIGN.md says outright that correctness must not
// depend on every node selecting the same bucket, and calls the private
// byte-conflict the partition case. This is that case.
//
// A partitioned seed gives each node a HOME bucket and authors its uploads and
// deletes there. The home is a per-seed property, not a per-write coin flip:
// `BucketSet::switch` is health-driven and sticky, a pin is captured once per
// operation and never torn (design §3), so a per-write flip would manufacture
// an interleaving the product cannot produce — and any bug found there would be
// unactionable. Ticks, sweeps, reconciles and audits keep the *selected* pin:
// `state.global_names` and `state.inventory` describe the selected bucket and
// carry no bucket key, so pointing a rebuild at a peer through this seam would
// produce harness-invented stale-index failures, not findings.
// ---------------------------------------------------------------------------

/// Share of publishes that arrive as a mirror record on a partitioned seed.
/// Mirror uploads are the other half of the gap: `Supersede`, the
/// `QuarantinedMirror` record state and every `.mirror-quarantined` path need a
/// package claimed `mirror` on one bucket and `private` on another, which is
/// what two concurrent first-writes racing the pre-artifact `.origin` fan-out
/// produce. Kept a minority — a mirror-dominated corpus starves the private
/// conflict arms, and `delete_record` refuses mirror eviction in multi-bucket
/// mode, so those deletes 409 and never enter the ledger.
const MIRROR_PERCENT: u64 = 12;

/// A seed's partition plan: which bucket each node authors truth on.
#[derive(Clone)]
struct Partition {
    /// `homes[node]` — the bucket that node's publishes and deletes write to.
    /// All zeros on an aligned seed, which is byte-identical to the pre-
    /// partition harness: `write_pin` short-circuits to `state.pin()`.
    homes: Vec<usize>,
    /// Percent of publishes drawn as mirror fills; 0 on an aligned seed.
    mirror_percent: u64,
    split: bool,
}

/// Draw the plan. Pure in the seed and from a DEDICATED rng — never the chaos
/// stream — so arming partitioning cannot shift which op a chaos draw picks on
/// an aligned seed, and `restart_node` can re-read a node's home without
/// consuming entropy (`--seed N` must reproduce exactly).
fn partition_for(seed: u64, nodes: usize, buckets: usize, percent: u64) -> Partition {
    let mut homes = vec![0usize; nodes];
    if percent == 0 || buckets < 2 {
        // A partition needs two buckets to be partitioned across.
        return Partition {
            homes,
            mirror_percent: 0,
            split: false,
        };
    }
    let mut rng = Rng::new(seed ^ 0x9A17_1710_D1FF_0000);
    if !rng.chance(percent) {
        return Partition {
            homes,
            mirror_percent: 0,
            split: false,
        };
    }
    // Node 0 stays home to bucket 0, so the fleet always has a bucket-0 writer
    // and a split seed is a partition rather than a wholesale relocation.
    for home in homes.iter_mut().skip(1) {
        *home = rng.below(buckets as u64) as usize;
    }
    Partition {
        homes,
        mirror_percent: MIRROR_PERCENT,
        split: true,
    }
}

/// The storage context a node's write authors truth through: its home bucket's
/// handle, at that bucket's own cache generation.
///
/// Built here rather than through `BucketSet::switch` (crate-private, and a
/// switch would also move this node's tick/audit selection — see the module
/// note above). Every field of `Pinned` is public precisely so a harness can
/// say "this write landed on a bucket this node did not select"; `publish_record`
/// and `delete_record` do all their I/O through `pinned.storage` and address
/// every other bucket from `pinned.index`, so both are correct for any pin.
fn write_pin(state: &AppState, home: usize) -> Arc<Pinned> {
    let selected = state.pin();
    if home == selected.index {
        return selected;
    }
    Arc::new(Pinned {
        storage: state.buckets.handles()[home].storage.clone(),
        // A distinct generation per bucket. `generation` is the ONLY key the
        // index and presign caches namespace on (src/cache.rs), so two pins
        // over two buckets at generation 0 would share one cache — which is
        // exactly why `switch` bumps it.
        generation: home as u64,
        index: home,
    })
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
    let quarantine = format!("_quarantine/{pkg}/{fname}@");
    let selected_keys = buckets[selected].keys();
    buckets
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != selected)
        .filter(|(i, peer)| {
            let keys = peer.keys();
            let has_record = keys.contains(&akey) && keys.contains(&mkey);
            let owed = format!("_repl/{i}/{pkg}/{fname}!");
            // A partitioned fan-out can find the peer holding a CONFLICTING
            // record, in which case `replicate_record` runs the merge instead
            // of a copy: the peer legitimately ends up frozen, tombstoned, or
            // holding the superseding side, and is owed nothing. Storage
            // evidence of that decision exempts — the peer carrying no record,
            // no note and no merge marker is still a broken promise.
            let merged = keys.iter().any(|k| {
                k.starts_with(&quarantine)
                    || *k == format!("{akey}.frozen")
                    || *k == format!("{akey}.tombstone")
                    || *k == format!("{akey}.mirror-quarantined")
            });
            !has_record && !merged && !selected_keys.iter().any(|k| k.starts_with(&owed))
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

#[allow(clippy::too_many_arguments)]
async fn op_publish(
    state: Arc<AppState>,
    ledger: Arc<Mutex<Ledger>>,
    clock: Arc<SimClock>,
    buckets: Vec<Arc<SimStorage>>,
    pkg: String,
    file: u8,
    variant: u8,
    home: usize,
    is_mirror: bool,
    viz: Option<Arc<Trace>>,
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
    let sha8 = sha256.get(..8).unwrap_or_default().to_string();
    let req = pypiron::PublishRequest {
        pkg: pkg.clone(),
        filename: fname.clone(),
        md5: pypiron::hash::md5_hex(&body),
        sha256,
        size: body.len() as u64,
        version: format!("{}.0", file + 1),
        requires_python: None,
        is_mirror,
        upload_time: clock.now_rfc3339(),
        yanked: pypiron::sidecar::Yanked::Flag(false),
        wheel_metadata: None,
        is_wheel: false,
        provenance: None,
        body: pypiron::PublishBody::Bytes(body.clone()),
    };
    let pinned = write_pin(&state, home);
    if pypiron::publish_record(&state, &pinned, req).await.is_ok() {
        // Before the ledger lock: the visualizer's lock is never held across
        // another of the run's locks.
        if let Some(viz) = &viz {
            viz.emit(
                "ack",
                serde_json::json!({
                    "kind": "publish",
                    "pkg": pkg,
                    "file": fname,
                    "bucket": pinned.index,
                    "sha8": sha8,
                    "variant": variant.to_string(),
                    "mirror": is_mirror,
                    "ok": true,
                }),
            );
        }
        let mut failures = ack_totality_failures(&buckets, pinned.index, &pkg, &fname);
        let mut ledger = ledger.lock().expect("ledger lock");
        ledger.ack_totality.append(&mut failures);
        ledger.record_ack(&pkg, &fname, pinned.index, body);
        if !is_mirror {
            // Private is terminal in the origin lattice; the fleet may never
            // settle back on `mirror` for this name (ORIGIN_TERMINALITY).
            ledger.private_claimed.insert(pkg.clone());
        }
        ledger.last_ack_deleted.insert((pkg, fname), false);
    }
}

async fn op_delete(
    state: Arc<AppState>,
    ledger: Arc<Mutex<Ledger>>,
    buckets: Vec<Arc<SimStorage>>,
    pkg: String,
    file: u8,
    home: usize,
    viz: Option<Arc<Trace>>,
) {
    let fname = filename(&pkg, file);
    {
        // Counted BEFORE the call and withdrawn only on a refusal: a delete
        // that dies mid-flight (crash-only faults abort the task, so nothing
        // after the await runs) still tombstoned and destroyed, and that is
        // exactly the case `authorized_deletes` has to cover.
        let mut ledger = ledger.lock().expect("ledger lock");
        ledger.delete_attempts += 1;
        *ledger
            .authorized_deletes
            .entry((pkg.clone(), fname.clone()))
            .or_default() += 1;
    }
    let pinned = write_pin(&state, home);
    let outcome = pypiron::delete_record(&state, &pinned, &pkg, &fname).await;
    if let Some(viz) = &viz {
        viz.emit(
            "ack",
            serde_json::json!({
                "kind": "delete",
                "pkg": pkg,
                "file": fname,
                "bucket": pinned.index,
                "sha8": serde_json::Value::Null,
                "variant": serde_json::Value::Null,
                "mirror": serde_json::Value::Null,
                "ok": outcome.is_ok(),
            }),
        );
    }
    match outcome {
        Ok(_) => {
            let mut ledger = ledger.lock().expect("ledger lock");
            ledger.deleted.insert((pkg.clone(), fname.clone()));
            ledger.last_ack_deleted.insert((pkg, fname), true);
        }
        // Withdrawn only when the delete provably destroyed nothing, and the
        // evidence for that is storage, not the status code. `delete_record`
        // writes the tombstone BEFORE it removes the body, so every error
        // return from there on — starting with "artifact delete failed", the
        // one an injected outage produces — leaves the filename permanently
        // barred. The fleet then propagates that tombstone and `tombstone_side`
        // destroys the body on every peer, which is exactly the destruction
        // this counter exists to record. Reading any `Err` as a refusal
        // credited the workload with "nothing was destroyed" over a delete that
        // had already passed its point of no return, and CONSERVATION then held
        // the freeze racing it to a totality the delete had made impossible.
        //
        // Raw `SimStorage`, never the traced view: an extra gated op here would
        // consume a sequence number and run a different schedule than the one
        // being judged.
        Err(_) => {
            let tombstoned = buckets[pinned.index]
                .keys()
                .contains(&format!("packages/{pkg}/{fname}.tombstone"));
            if !tombstoned {
                if let Some(count) = ledger
                    .lock()
                    .expect("ledger lock")
                    .authorized_deletes
                    .get_mut(&(pkg, fname))
                {
                    *count -= 1;
                }
            }
        }
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
    /// Storage ops the chaos phase issued — the range `--sweep-faults` walks,
    /// since `admit` forces nothing once the heal phase starts.
    chaos_ops: u64,
    /// Whether a `--force-fault` override actually fired. It cannot when the
    /// issuing node was already down, and a run that forced nothing is a run
    /// that verified nothing new.
    forced_fired: bool,
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

/// The visualizer's `meta` line: everything about this world that is fixed
/// before the first op. Emitted from `run_seed` because `partition_for`'s homes
/// exist nowhere else.
///
/// `scenario`/`title`/`narration`/`caveats` are the scenario pack's job
/// (dev/scripts/viz): the emitter ships the two producer-level caveats it can
/// prove and invents no narration. `node_regions` is all-null on purpose — the
/// VOPR has no region concept and a reader must not draw one.
fn emit_meta(viz: &Arc<Trace>, seed: u64, profile: &Profile, part: &Partition) {
    let homes: Vec<serde_json::Value> = part
        .homes
        .iter()
        .map(|home| {
            if profile.partition_percent == 0 {
                serde_json::Value::Null
            } else {
                serde_json::Value::from(*home)
            }
        })
        .collect();
    viz.emit(
        "meta",
        serde_json::json!({
            "kind": "trace",
            "producer": "vopr",
            "scenario": "",
            "title": "",
            "narration": "",
            "cmd": viz.cmd,
            "commit": viz.commit,
            "seed": seed,
            "nodes": profile.nodes,
            "buckets": profile.buckets,
            "packages": profile.packages,
            "files": profile.files,
            "ops": profile.ops,
            "fail_percent": profile.fail_percent,
            "partition_percent": profile.partition_percent,
            "partitioned": part.split,
            "brk": profile.brk.flag().unwrap_or("none"),
            "bucket_names": (0..profile.buckets)
                .map(|idx| format!("bucket-{idx}"))
                .collect::<Vec<_>>(),
            "node_regions": vec![serde_json::Value::Null; profile.nodes],
            "homes": homes,
            "caveats": [
                "presign_get does not pass through the fault gate, so that op never appears.",
                "A batch delete records only its first key in the `op` event; the `world` \
                 delta shows every removal.",
            ],
        }),
    );
}

/// Could a storage op in this run have returned an availability error?
///
/// Two oracles hold a crash-only world to a stricter contract — an ack-totality
/// miss is a hard violation, and any audit view repair is one. Both used to read
/// "crash-only" off `fail_percent == 0`, which stopped being the same question
/// the moment `--sweep-faults` arrived: a swept run has `fail_percent == 0` and
/// still injects one failure when the forced fate is `Fail`, so the percent
/// alone would report the sweep's own injected failure as a bug on every
/// forced-`Fail` run.
///
/// A forced `Crash` relaxes nothing. Crashes are exactly what the crash-only
/// contract contemplates, so the crash lane keeps the strict gate — which is
/// what makes it the more valuable half of the sweep.
fn availability_faults(fail_percent: u64, forced: Option<(u64, OpFate)>) -> bool {
    fail_percent > 0 || matches!(forced, Some((_, OpFate::Fail)))
}

async fn run_seed(seed: u64, profile: Profile, rerun: bool, viz: Option<Arc<Trace>>) -> RunOutcome {
    let Profile {
        nodes,
        buckets,
        packages,
        files,
        ops,
        fail_percent,
        weights,
        brk,
        partition_percent,
        staleness_secs,
        depth1,
        forced,
    } = profile;
    let availability_faults = availability_faults(fail_percent, forced);
    let plan = partition_for(seed, nodes, buckets, partition_percent);
    if plan.split {
        REACH.partitioned.store(true, Ordering::Relaxed);
    }
    let weight_total: u64 = weights.iter().map(|w| u64::from(*w)).sum();
    let start =
        time::OffsetDateTime::parse(SIM_START, &time::format_description::well_known::Rfc3339)
            .expect("valid sim start");
    let clock = SimClock::new(start);
    let _guard = clock.install_global();
    if let Some(viz) = &viz {
        viz.attach(&clock);
        emit_meta(viz, seed, &profile, &plan);
    }
    let mut fleet = Fleet::new(seed, &profile, clock.clone(), viz.clone());
    let mut rng = Rng::new(seed);
    // The empty baseline every later `world` delta is applied on top of.
    viz_world(&viz, &fleet.buckets, true);
    if let Some(viz) = &viz {
        viz.emit("phase", serde_json::json!({ "name": "chaos" }));
    }

    // ---- Chaos phase: seed-driven interleaving of ops, faults, and time.
    for step in 0..ops {
        let node = rng.below(nodes as u64) as usize;
        if fleet.plan.node_crashed(node) && !rng.chance(30) {
            // A crashed node mostly stays down this step; sometimes restarts.
            continue;
        }
        if fleet.plan.node_crashed(node) {
            fleet.restart_node(node, "chaos");
            continue;
        }
        let state = fleet.nodes[node].state.clone();
        let ledger = fleet.ledger.clone();
        // Exactly one `pick_op` call, as before — the draw is the schedule.
        // A step the two `continue`s above skipped ran no op and emits nothing,
        // so `step.n` has gaps where the fleet was down. Only `i` is gapless.
        let class = pick_op(&weights, weight_total, &mut rng);
        if let Some(viz) = &viz {
            viz.emit(
                "step",
                serde_json::json!({
                    "n": step,
                    "op": OP_LABELS.get(class).copied().unwrap_or("?"),
                    "node": node,
                }),
            );
        }
        match class {
            0 => {
                let pkg = PACKAGE_NAMES[rng.below(packages as u64) as usize].to_string();
                let file = rng.below(u64::from(files)) as u8;
                let variant = rng.below(2) as u8;
                // Short-circuits on an aligned seed, so the entropy budget of a
                // publish is unchanged there and every pinned seed still means
                // what its comment says.
                let is_mirror = plan.mirror_percent > 0 && rng.chance(plan.mirror_percent);
                let clock = fleet.clock.clone();
                let buckets = fleet.buckets.clone();
                let home = plan.homes[node];
                let seen = viz.clone();
                fleet.spawn_on(
                    node,
                    op_publish(
                        state, ledger, clock, buckets, pkg, file, variant, home, is_mirror, seen,
                    ),
                );
            }
            1 => {
                let pkg = PACKAGE_NAMES[rng.below(packages as u64) as usize].to_string();
                let file = rng.below(u64::from(files)) as u8;
                let home = plan.homes[node];
                let buckets = fleet.buckets.clone();
                fleet.spawn_on(
                    node,
                    op_delete(state, ledger, buckets, pkg, file, home, viz.clone()),
                );
            }
            2 => {
                // The *selected* bucket is still 0 on every node — a
                // partitioned node diverges only its writes, never its
                // rebuilds — so one lease still serializes every tick, and the
                // `PkgRace`/`GlobalRace` EXPECTED_ZERO excuses still hold.
                //
                // Note what that excuse does NOT say: production does not
                // serialize rebuilds. This lock is strictly stronger than the
                // sloppy lease (`src/lease.rs`: TTL + heartbeat, no fencing),
                // and it also excludes the leader's own audit task, which
                // `run_worker` spawns off the tick loop and never mutexes
                // against it. That gap is why class 3 reads zero here and is
                // still a real production state; dev/TESTING.md carries the
                // measurement.
                let lease = fleet.tick_lock[0].clone();
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
                advance_clock(&clock, 90, "jump", &viz);
            }
            6 => {
                // Depth-1: the forced op is the run's only fault, so the driver
                // schedules none of its own. A forced `Crash` still kills a node
                // and the loop head still restarts it, so the crash dynamics the
                // oracles need are intact — they are just attributable now.
                if !depth1 {
                    fleet.plan.schedule_crash_soon(node, 12);
                    // Give the doomed node's in-flight work a chance to hit the
                    // crash point before the driver aborts it.
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    if fleet.plan.node_crashed(node) {
                        fleet.crash_node(node);
                    }
                }
            }
            _ => {
                advance_clock(&clock, 1, "nudge", &viz);
            }
        }
        // Let the paused runtime interleave whatever is in flight.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        viz_world(&viz, &fleet.buckets, false);
    }
    // The most divergent moment there is: the partition still in force and the
    // faults still on. Raw dumps, so the sample cannot move the schedule.
    sample_verdicts(
        &fleet.buckets.iter().map(|b| b.dump()).collect::<Vec<_>>(),
        viz.as_ref(),
    );

    // ---- Heal phase: stop faults, restart everyone, then drive the fleet
    // the way production does — worker ticks, note sweeps, warm drains, the
    // pairwise tree diff, and the leader audit — until storage reaches a
    // fixpoint with no protocol debris. The audit matters: the event-protocol
    // model proves concurrent unleased rebuilds can strand a stale view until
    // the audit's fingerprint diff repairs it, so the system-level claim the
    // VOPR checks is "ticks + sweeps + reconcile + audit converge".
    let mut violations_pre: Vec<String> = Vec::new();
    if let Some(viz) = &viz {
        viz.emit("phase", serde_json::json!({ "name": "heal" }));
    }
    fleet.plan.heal();
    for node in 0..nodes {
        fleet.restart_node(node, "heal");
    }
    // t0 for the STALENESS clock: the storm is over. Faults are off, every node
    // has restarted (which aborted its in-flight tasks), and no further client
    // op will be issued — so this is exactly "after the last write and after
    // faults clear". Everything the fleet does from here is convergence, and it
    // is on a deadline.
    //
    // The debt is read here and never again. It is what the fleet owes at the
    // moment it is handed the problem, so the allowance it buys is a constant of
    // the run: no amount of thrashing during the heal phase can extend it.
    // Raw `dump()`, never a `FaultView` — no op-sequence number, no rng draw,
    // no trace event, so measuring cannot move the schedule it measures.
    let storm_end = clock.now_utc();
    let debt = replication_debt(
        &fleet
            .buckets
            .iter()
            .map(|bucket| bucket.dump())
            .collect::<Vec<_>>(),
    );
    let deadline = staleness_deadline(staleness_secs, debt);
    // A fleet that is already agreed and owes nothing converged in zero seconds;
    // sampling before the heal-start jump is what lets it say so.
    let mut agreed_at: Option<u64> = (debt == 0).then_some(0);
    advance_clock(&clock, 120, "heal-start", &viz); // all intents stale
    viz_world(&viz, &fleet.buckets, false);
    let mut quiesced = false;
    let mut audit_view_repairs: u64 = 0;
    let mut repairs_by_class: [u64; 3] = [0; 3]; // [ordering, premature, race]
    let mut last_fingerprint: Option<Vec<BTreeMap<String, Vec<u8>>>> = None;
    // STALENESS is a comparison against a deadline, so a run with nothing to
    // converge would "pass" it having measured nothing; it counts as executed
    // only when the heal phase had real work — debt at t0, or debris that
    // appeared during a drain pass. `debt` already covers the first, including
    // the cross-bucket disagreements the old marker-only sample could not see.
    let mut saw_work = debt > 0;
    for round in 0..HEAL_ROUNDS {
        sample_verdicts(
            &fleet.buckets.iter().map(|b| b.dump()).collect::<Vec<_>>(),
            viz.as_ref(),
        );
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
            // The real count, not a flag: the `pass` event publishes it and a
            // reader watching debris shrink pass over pass needs the number.
            // Same raw `dump()` read either way.
            let pass_dumps: Vec<BTreeMap<String, Vec<u8>>> =
                fleet.buckets.iter().map(|bucket| bucket.dump()).collect();
            let markers_left: usize = pass_dumps.iter().map(marker_count).sum();
            saw_work |= markers_left > 0;
            // Read the staleness clock against the fleet's CURRENT agreement,
            // not its first. Guarded on `markers_left == 0` so the projection
            // only runs when it can possibly say yes — outstanding debris IS
            // debt, so the answer is already no, and this loop runs up to 20
            // times a round.
            note_agreement(
                &mut agreed_at,
                markers_left == 0 && fleet_agreed(&pass_dumps),
                || elapsed_secs(&clock, storm_end),
            );
            Reach::peak(&REACH.peak_drains, pass + 1);
            if let Some(viz) = &viz {
                viz.emit(
                    "pass",
                    serde_json::json!({
                        "round": round,
                        "n": pass,
                        "markers_left": markers_left,
                    }),
                );
            }
            viz_world(&viz, &fleet.buckets, false);
            if markers_left == 0 {
                break;
            }
            advance_clock(&clock, 90, "drain", &viz);
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
        if let Some(viz) = &viz {
            viz.emit(
                "round",
                serde_json::json!({ "n": round, "leader": leader_idx }),
            );
        }
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
                // Sampled INSIDE the leader drain, not just at the round
                // boundary. The tree diff's copies land here, and a clock that
                // only reads at the boundary charges the fleet the whole
                // remaining round for work it had already finished — which is a
                // deadline failing on the harness's sampling rate rather than on
                // the product.
                if agreed_at.is_none() {
                    let mid: Vec<BTreeMap<String, Vec<u8>>> =
                        fleet.buckets.iter().map(|bucket| bucket.dump()).collect();
                    if fleet_agreed(&mid) {
                        agreed_at = Some(elapsed_secs(&clock, storm_end));
                    }
                }
                advance_clock(&clock, 90, "leader-drain", &viz);
            }
        }
        if brk != Break::None && round == 0 && !brk.plants_after_audit() {
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
        // Same reason as the leader-drain sample above: an audit that converged
        // the fleet did so here, and charging it the post-audit jump on top
        // measures the driver, not the product.
        if agreed_at.is_none() {
            let audited: Vec<BTreeMap<String, Vec<u8>>> =
                fleet.buckets.iter().map(|bucket| bucket.dump()).collect();
            if fleet_agreed(&audited) {
                agreed_at = Some(elapsed_secs(&clock, storm_end));
            }
        }
        advance_clock(&clock, 90, "post-audit", &viz);
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
                if let Some(viz) = &viz {
                    viz.emit(
                        "repair",
                        serde_json::json!({
                            "round": round,
                            "bucket": f.bucket,
                            "subject": f.subject,
                            "class": f.class,
                            "detail": f.detail,
                        }),
                    );
                }
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
            if !availability_faults {
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
        // The audits are behind us and the round's damage report is filed, so a
        // break landing here is one the fleet has to notice on its OWN next
        // cycle — see `Break::plants_after_audit`.
        if brk.plants_after_audit() && round == 0 {
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
        // Fixpoint: no markers and no storage change over a full round.
        let snapshot: Vec<BTreeMap<String, Vec<u8>>> =
            fleet.buckets.iter().map(|bucket| bucket.dump()).collect();
        let markers_left = snapshot.iter().any(|dump| marker_count(dump) > 0);
        saw_work |= markers_left;
        // The round's reconcile and audits are convergence work too, so the
        // fleet can arrive here having been behind at the last drain pass.
        note_agreement(
            &mut agreed_at,
            !markers_left && fleet_agreed(&snapshot),
            || elapsed_secs(&clock, storm_end),
        );
        Reach::peak(&REACH.peak_rounds, round + 1);
        if let Some(viz) = &viz {
            viz.world(&snapshot, false);
        }
        if !markers_left && last_fingerprint.as_ref() == Some(&snapshot) {
            quiesced = true;
            break;
        }
        last_fingerprint = Some(snapshot);
    }
    if saw_work {
        REACH.hit(R::Staleness);
    }
    REACH.record_staleness(Staleness {
        seed,
        secs: agreed_at,
        deadline,
        debt,
    });
    if std::env::var_os("VOPR_STALENESS_CSV").is_some() {
        println!(
            "STALECSV,{seed},{},{},0",
            agreed_at.map(|s| s as i64).unwrap_or(-1),
            debt
        );
    }

    // ---- Invariants. The ledger stays unlocked until the last await is behind
    // us: `verify_storage` is async, and a std guard may not cross an await.
    let mut violations = violations_pre;
    if let Some(viz) = &viz {
        viz.emit("phase", serde_json::json!({ "name": "oracle" }));
    }
    // STALENESS: bounded agreement, in simulated minutes. See the header block
    // over `STALENESS_HEALTHY_SECS` for the deadline and why it has the shape it
    // has. Two failures, because they are two different defects: agreeing late,
    // and never settling down at all.
    let spent = elapsed_secs(&clock, storm_end);
    let allowance = format!(
        "{staleness_secs}s healthy + {} cycle(s) x {STALENESS_CYCLE_SECS}s for the {debt} \
         record(s) of replication debt it held when the storm ended",
        cycles_for(debt)
    );
    match agreed_at {
        Some(secs) if secs > deadline => violations.push(format!(
            "STALENESS: the fleet took {secs}s of simulated time to agree after the last write, \
             past its {deadline}s deadline ({allowance})"
        )),
        Some(_) => {}
        None => violations.push(format!(
            "STALENESS: the fleet never agreed — {spent}s of simulated time after the last write \
             and {} record(s) still outstanding, against a {deadline}s deadline ({allowance})",
            replication_debt(
                &fleet
                    .buckets
                    .iter()
                    .map(|bucket| bucket.dump())
                    .collect::<Vec<_>>()
            ),
        )),
    }
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
            "STALENESS: the fleet never stopped working — no fixpoint in {HEAL_ROUNDS} heal \
             rounds x {DRAIN_PASSES} drain passes ({spent}s of simulated time); leftover \
             markers: {leftovers:?}"
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
    count_merge_evidence(&dumps);
    // The exact world every oracle below reads — `apply_break`'s damage included.
    if let Some(viz) = &viz {
        viz.world(&dumps, false);
    }

    // VIEWS == TRUTH, byte-strict: the product's own oracle (`pypiron verify`)
    // re-renders every view from that bucket's truth and diffs the bytes. Run
    // against the raw `SimStorage` — never the traced `FaultView` — so it
    // consumes no op-sequence numbers and cannot shift the fault schedule.
    for (idx, bucket) in fleet.buckets.iter().enumerate() {
        if dumps[idx].keys().any(|k| k.starts_with("packages/")) {
            REACH.hit(R::Verify); // an empty bucket re-renders nothing to diff
        }
        // `deep: false` on purpose. VERIFY's claim here is views == truth, and
        // SELF_CONSISTENCY below already re-hashes every body — running the
        // product's `--deep` pass too would make two oracles hold one claim and
        // cost the whole corpus on every seed. The blackbox suite drives
        // `verify-index --deep` instead.
        match pypiron::verify::verify_storage(bucket.as_ref(), false).await {
            Ok(report) => violations.extend(report.divergences.into_iter().map(|d| {
                format!(
                    "VERIFY: bucket {idx} diverged from its own truth: {} {} — {}",
                    d.kind, d.package, d.detail
                )
            })),
            Err(e) => violations.push(format!("VERIFY: bucket {idx} could not be verified: {e:?}")),
        }
    }

    // UNTYPED_DISAPPEARANCE: an artifact body may not leave a bucket unless the
    // FILENAME is durably closed (`.tombstone`/`.frozen`, the two markers
    // `publish_record` refuses over) or the BYTES are durably kept (a
    // `_quarantine/` copy of exactly them). Nothing else authorizes it — least
    // of all `.mirror-quarantined`, the one fence that deliberately leaves the
    // canonical key writable, so it can stand over bytes it never adjudicated.
    //
    // Every other oracle here reads the FINAL world, so the corruption they
    // catch has to have been made permanent before they can see it. Freeing an
    // immutable filename is already broken at the instant it happens: a rebuild
    // that read those bytes publishes a sidecar over the empty key, the next
    // upload wins the create with different bytes, and the bucket serves body B
    // under body A's published sha256 forever. Getting SELF_CONSISTENCY to see
    // that took two MORE faults on the compensation path — the size verify had
    // to fault so it never overwrote the fabricated sidecar, and the backfill's
    // retracting delete had to fault too. Depth 3, and 115b9ca cost ~16M seeds
    // of lottery. Watched continuously it is depth 1, which is what makes it
    // reachable by `--sweep-faults`.
    //
    // Recorded by `SimStorage` because that is where the predicate already
    // lives and where every writer is visible. The `Observer` effect log is
    // not: `EffectKind::TruthWrite` collapses put and delete into one kind, and
    // the warm-bucket audits build their state over the raw bucket, so every
    // body `tombstone::complete_interrupted_deletes` drops is invisible to it.
    for (idx, bucket) in fleet.buckets.iter().enumerate() {
        let (weighed, freed) = bucket.body_removals();
        // One execution per removal weighed, fenced or not: the question is
        // asked at every body delete, and these are the ones answered wrong.
        REACH.hits(R::UntypedDisappearance, weighed);
        for key in freed {
            violations.push(format!(
                "UNTYPED_DISAPPEARANCE: bucket {idx} freed the immutable filename {key} — the \
                 body was removed with no .tombstone and no .frozen closing the name, and no \
                 _quarantine/ copy of those exact bytes keeping them, so nothing authorized the \
                 removal and nothing records that they ever stood there. The name is re-creatable \
                 by any writer now, and a sidecar already naming the old bytes will publish them \
                 over whatever lands next"
            ));
        }
    }

    let ledger = fleet.ledger.lock().expect("ledger lock");

    // Convergence: identical truth + views across buckets.
    if buckets > 1 {
        let project = agreement_projection;
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
        for ((pkg, fname), acks) in &ledger.acked {
            let akey = format!("packages/{pkg}/{fname}");
            let tomb = dump.contains_key(&format!("{akey}.tombstone"));
            let frozen = dump.contains_key(&format!("{akey}.frozen"));
            // A mirror->private demotion is an authorized removal too: the fence
            // replicates, the body it suppresses moves to `_quarantine/` on
            // whichever bucket held it, and the canonical key ends empty
            // everywhere (dev/DESIGN.md). CONSERVATION is what keeps this
            // exemption honest — it counts `_quarantine/` copies fleet-wide, so
            // a demotion that dropped bytes instead of moving them still reds
            // (`--break demote-lossy`).
            let demoted = dump.contains_key(&format!("{akey}.mirror-quarantined"));
            if ledger.deleted.contains(&(pkg.clone(), fname.clone())) || tomb || frozen || demoted {
                continue; // authorized removal, conflict freeze, or demotion
            }
            REACH.hit(R::Durability);
            // One acked byte-set is the ordinary case and stays byte-strict.
            // Two is a cross-bucket byte conflict: the merge is entitled to
            // keep EITHER side (`conflict_winner` orders by the server-stamped
            // receive time, which the harness cannot reproduce — a clock jump
            // can land between the stamp and the ack), so the claim narrows to
            // "the bytes standing here are bytes somebody acked". It does NOT
            // narrow further than that: the fleet may not settle on two of them
            // at once (checked once, below) and it may not lose either (that is
            // CONSERVATION's clause, which a freeze no longer escapes).
            let bodies = acked_bodies(acks);
            match dump.get(&akey) {
                None => violations.push(format!(
                    "DURABILITY: acked {akey} missing on bucket {bucket_idx}"
                )),
                // Bytes nobody acked stand here. Legal only when the merge
                // superseded the acked bytes and PRESERVED them: a
                // `_quarantine/` copy is the authorized-removal evidence, and
                // the operator was alarmed. Without it, an acknowledged upload
                // was silently replaced.
                Some(stored)
                    if !bodies.contains(stored.as_slice())
                        && !bodies
                            .iter()
                            .all(|b| quarantined_bodies(&dumps, pkg, fname).contains(b)) =>
                {
                    violations.push(format!(
                        "DURABILITY: acked {akey} on bucket {bucket_idx} serves bytes no ack \
                         carried, and the acked bytes are not preserved under _quarantine/ \
                         either ({} acked byte-set(s), acks (seq,bucket) {:?})",
                        bodies.len(),
                        acks.iter().map(|a| (a.seq, a.bucket)).collect::<Vec<_>>(),
                    ));
                }
                Some(_) => {
                    if !dump.contains_key(&format!("{akey}.meta.json")) {
                        violations.push(format!(
                            "DURABILITY: acked {akey} lost its sidecar on bucket {bucket_idx}"
                        ));
                    }
                    // A mirror record under a private claim is invisible by
                    // design — that IS the dependency-confusion boundary — so
                    // the renderer's own omission rules exempt, and nothing
                    // else does.
                    if !renderer_omits(dump, pkg, fname) {
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
        }

        // Self-consistency: a bucket may not serve bytes its own sidecar
        // contradicts. Every other oracle here reads sidecars and compares
        // them — across buckets (CONVERGENCE), against the ledger (DURABILITY),
        // or re-rendered into views (`pypiron verify`) — and none re-hashes a
        // body. So a body swapped under a sidecar that still names the old sha
        // is invisible to all of them: the two buckets' sidecars stay
        // byte-identical, the pair never enters the diverged-key set, and the
        // merge's `decide` reads them as agreed forever. That is exactly how
        // the crossed-body class survived four fixes aimed at it.
        //
        // Scoped to the bucket's own published truth, so it needs no ledger and
        // holds in a single-bucket fleet too. A `.frozen` filename is exempt:
        // an adjudicated byte conflict deliberately preserves both bodies and
        // suppresses the name, and CONSERVATION owns that case.
        for (key, body) in dump {
            let Some(sidecar) = dump.get(&format!("{key}.meta.json")) else {
                continue; // sidecars, markers and origins name no bytes
            };
            if !key.starts_with("packages/") || dump.contains_key(&format!("{key}.frozen")) {
                continue;
            }
            let Ok(sidecar) = serde_json::from_slice::<pypiron::sidecar::Sidecar>(sidecar) else {
                continue; // an unparseable sidecar is VERIFY's business
            };
            REACH.hit(R::SelfConsistency);
            let stored = pypiron::hash::sha256_hex(body);
            if stored != sidecar.sha256 {
                violations.push(format!(
                    "SELF_CONSISTENCY: bucket {bucket_idx} serves {} under {key} while its own \
                     sidecar publishes sha256 {} — a client checking the download it was handed \
                     against the index it read would reject it, and no other oracle here \
                     re-hashes a body",
                    String::from_utf8_lossy(body),
                    sidecar.sha256,
                ));
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
    }

    // Conservation: acked bytes must exist somewhere unless an authorized
    // delete removed them. The tombstone is the point of no return, so storage
    // evidence (a tombstone on any bucket) exempts — an interrupted delete that
    // crashed after its tombstone but before its 204 has still legitimately
    // destroyed. A freeze does NOT exempt on its own: `.frozen` used to buy a
    // blanket pass, and a freeze is precisely where both byte-sets most need to
    // survive. `freeze_side` orders marker -> quarantine -> tombstone -> drop,
    // so the bytes are preserved before the destructive move and a crash
    // anywhere leaves either the body or its `_quarantine/` copy. So a frozen
    // filename owes the fleet EVERY byte-set it acked, findable somewhere.
    // The scan is fleet-wide and runs once: each bucket preserves the body IT
    // personally lost, so the sets legitimately differ per bucket.
    //
    // That argument covers the tombstone `freeze_side` writes — not a delete's.
    // A delete and a merge freeze can race on one filename, and a delete that
    // gets there first drops the body under its OWN tombstone while the freeze
    // is between its marker and its quarantine read: `freeze_side`'s
    // `get_bytes` then 404s and there is nothing left to preserve. The bytes
    // were destroyed by the authorized delete, not lost by the freeze, so a
    // tombstone still exempts when the workload authorized a delete for that
    // filename. Nothing at quiescence distinguishes the two tombstones, so
    // the harness's own record of what it asked for is the evidence.
    for ((pkg, fname), acks) in &ledger.acked {
        let akey = format!("packages/{pkg}/{fname}");
        let frozen_anywhere = dumps
            .iter()
            .any(|d| d.contains_key(&format!("{akey}.frozen")));
        // A demotion is the other authorized removal that keeps its evidence: it
        // moves the loser to `_quarantine/` before it drops the record, and
        // writes no tombstone, so nothing exempts it here — which is what keeps
        // DURABILITY's `.mirror-quarantined` exemption honest.
        let demoted_anywhere = dumps
            .iter()
            .any(|d| d.contains_key(&format!("{akey}.mirror-quarantined")));
        let deleted = ledger.deleted.contains(&(pkg.clone(), fname.clone()))
            || ((!frozen_anywhere || ledger.authorized_delete(pkg, fname))
                && dumps
                    .iter()
                    .any(|d| d.contains_key(&format!("{akey}.tombstone"))));
        if deleted {
            continue;
        }
        REACH.hit(R::Conservation);
        for body in acked_bodies(acks) {
            if !dumps
                .iter()
                .any(|d| d.values().any(|stored| stored == body))
            {
                violations.push(format!(
                    "CONSERVATION: acked bytes {:?} of {akey} vanished from every bucket{} \
                     | acks (seq,bucket) {:?} | markers: {}",
                    String::from_utf8_lossy(body),
                    if frozen_anywhere {
                        " — a freeze quarantines both bodies before it drops either, so a \
                         frozen filename may not lose one"
                    } else if demoted_anywhere {
                        " — a demotion moves the loser to `_quarantine/` before it drops the \
                         record, so a demoted filename may not lose it"
                    } else {
                        ""
                    },
                    acks.iter().map(|a| (a.seq, a.bucket)).collect::<Vec<_>>(),
                    marker_census(&dumps, pkg, fname),
                ));
            }
        }
    }

    // Durability, the fleet-wide half: a byte conflict may be resolved either
    // way, never left split. Two buckets standing on two different acked
    // byte-sets under one live filename is permanent divergence — the state the
    // merge exists to remove — and it is worth naming here rather than leaving
    // to CONVERGENCE, which reports it as one key among a diff.
    for ((pkg, fname), acks) in &ledger.acked {
        if acked_bodies(acks).len() < 2 {
            continue;
        }
        let akey = format!("packages/{pkg}/{fname}");
        let kept: std::collections::BTreeSet<&Vec<u8>> = dumps
            .iter()
            .filter(|d| {
                !d.contains_key(&format!("{akey}.tombstone"))
                    && !d.contains_key(&format!("{akey}.frozen"))
            })
            .filter_map(|d| d.get(&akey))
            .collect();
        if kept.len() > 1 {
            violations.push(format!(
                "DURABILITY: {akey} was acked with different bytes on different buckets and the \
                 fleet settled on {} of them at once — a byte conflict is resolved to one \
                 survivor or frozen, never left split",
                kept.len()
            ));
        }
    }

    // FREEZE JUSTIFIED. A `.frozen` marker buys a blanket DURABILITY exemption
    // — the file may be absent everywhere — so a freeze that fires without a
    // real conflict is a self-granted licence to lose data, and nothing checked
    // that it was deserved. `decide` reaches `Verdict::Freeze` only from two
    // live records whose sidecars name DIFFERENT sha256 under one immutable
    // filename, so the claim to check is exactly that: two byte-sets really
    // existed under this name.
    //
    // Attested in one currency, sha256, from every source that can carry the
    // fact: what the harness saw acked, every `_quarantine/` copy the fleet
    // preserved, and every body the buckets actually committed under the
    // canonical key. The third is not redundant — it is the only one that
    // cannot be erased, and the other two measurably do get erased. A publish
    // can crash after `store_artifact_verified` succeeds and before its 200,
    // leaving real conflicting bytes with no ack recorded. An authorized delete
    // racing `freeze_side` between its `.frozen` marker and its `get_bytes`
    // destroys the body before the quarantine copy is taken — and when the
    // delete wins that race on BOTH sides, a correct fleet-wide freeze is left
    // with no body-shaped evidence anywhere at all. `SimStorage` sees every
    // write, so it just remembers the digests it committed.
    //
    // This is not a loosening. The product cannot manufacture a digest it never
    // wrote, and `--break freeze-unjustified` — a bare `.frozen` planted over a
    // filename that only ever held ONE byte-set — still has nothing to show;
    // its kill proof runs single-bucket, where a byte conflict cannot occur at
    // all. What went away with it is the old "excused if the freeze preserved
    // something" clause, which was a proxy for this question and answered it
    // wrong in both directions.
    //
    // A `.frozen` is fleet state, and so is one term of the attestation — see
    // the `peers` branch below.
    for (pkg, fname) in record_names(&dumps) {
        let akey = format!("packages/{pkg}/{fname}");
        if !dumps
            .iter()
            .any(|d| d.contains_key(&format!("{akey}.frozen")))
        {
            continue;
        }
        REACH.hit(R::FreezeJustified);
        let mut attested: std::collections::BTreeSet<String> =
            quarantined_bodies(&dumps, &pkg, &fname)
                .into_iter()
                .map(sha256_hex)
                .collect();
        if let Some(acks) = ledger.acked.get(&(pkg.clone(), fname.clone())) {
            attested.extend(acked_bodies(acks).into_iter().map(sha256_hex));
        }
        // The fence-refused bodies another writer lost this filename to. Debris
        // only where `publish_record` deletes it, and that is single-bucket
        // alone: with peers "a fenced multi-bucket loser stays occupied and
        // inert" (src/publish.rs), the writer that loses to it is a replication
        // copy leg, and that leg reads it back and freezes both sides on the
        // difference (`replicate::freeze_copy_race`). Those are the freezes the
        // attestation could not see: the refused publish never wrote a sidecar,
        // so no ack names its bytes and `freeze_side` quarantines the body each
        // bucket kept rather than the one it lost.
        let peers = fleet.buckets.len() > 1;
        for bucket in &fleet.buckets {
            attested.extend(bucket.committed_digests(&akey));
            if peers {
                attested.extend(bucket.contested_digests(&akey));
            }
        }
        if attested.len() < 2 {
            violations.push(format!(
                "FREEZE_UNJUSTIFIED: {akey} is frozen fleet-wide but only {} byte-set(s) are \
                 attested for it (acks + every _quarantine/ copy + every body a bucket ever \
                 committed under the key) — a freeze suppresses the filename and exempts it \
                 from DURABILITY, so it must be a real byte conflict | attested {:?} | \
                 markers: {}",
                attested.len(),
                attested
                    .iter()
                    .map(|sha| &sha[..sha.len().min(12)])
                    .collect::<Vec<_>>(),
                marker_census(&dumps, &pkg, &fname),
            ));
        }
    }

    // ORIGIN TERMINALITY. Origin exclusivity is the dependency-confusion
    // defense and its lattice is monotone: mirror may be demoted to private,
    // private is terminal. CONVERGENCE only asks that the buckets AGREE — it
    // would happily pass a fleet that agreed on `mirror` for a name a private
    // upload had already claimed. So: once a private publish acks for a
    // package, no bucket may end claiming `mirror` for it, and no live,
    // unquarantined artifact of it may still carry a mirror sidecar.
    for pkg in &ledger.private_claimed {
        for (bucket_idx, dump) in dumps.iter().enumerate() {
            REACH.hit(R::OriginTerminality);
            let claim = dump
                .get(&format!("packages/{pkg}/.origin"))
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
                .and_then(|doc| {
                    doc.get("origin")
                        .and_then(|o| o.as_str())
                        .map(str::to_string)
                });
            if claim.as_deref() == Some("mirror") {
                violations.push(format!(
                    "ORIGIN_TERMINALITY: bucket {bucket_idx} claims packages/{pkg}/.origin = \
                     mirror after a private upload acked for it — private is terminal, and a \
                     name that falls back to mirror is a dependency-confusion window"
                ));
            }
            for (_, fname) in record_names(std::slice::from_ref(dump))
                .into_iter()
                .filter(|(p, _)| p == pkg)
            {
                let akey = format!("packages/{pkg}/{fname}");
                let inert = ["tombstone", "frozen", "mirror-quarantined"]
                    .iter()
                    .any(|suffix| dump.contains_key(&format!("{akey}.{suffix}")));
                if inert || !dump.contains_key(&akey) {
                    continue;
                }
                let origin = dump
                    .get(&format!("{akey}.meta.json"))
                    .and_then(|bytes| {
                        serde_json::from_slice::<pypiron::sidecar::Sidecar>(bytes).ok()
                    })
                    .and_then(|sc| sc.origin);
                if origin.as_deref() == Some("mirror") {
                    violations.push(format!(
                        "ORIGIN_TERMINALITY: bucket {bucket_idx} still serves {akey} as live \
                         mirror truth under a privately-claimed package — it must be superseded \
                         or quarantined, never left renderable"
                    ));
                }
            }
        }
    }

    // Crash-only: the protocol must fan out or leave a note on every schedule,
    // so a totality miss is a hard violation. Under injected storage failures a
    // note write can itself fail, so there it is a reported statistic — the same
    // split the audit-repair taxonomy uses.
    if !availability_faults {
        violations.extend(ledger.ack_totality.iter().cloned());
    }

    let (trace_hash, trace_events) = fleet.plan.trace_hash();
    RunOutcome {
        chaos_ops: fleet.plan.chaos_ops(),
        forced_fired: fleet.plan.forced_fired.load(Ordering::SeqCst),
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
    for ((pkg, fname), acks) in &ledger.acked {
        mix(&mut hash, pkg.as_bytes());
        mix(&mut hash, fname.as_bytes());
        for ack in acks {
            mix(&mut hash, &ack.bucket.to_le_bytes());
            mix(&mut hash, &ack.body);
        }
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
    /// rerunning it. Passing a workload flag alongside it is rejected — see
    /// `ROTATE_OVERRIDES`.
    rotate: bool,
    /// Deliberate defect to inject (`--break`), for mutation-testing the
    /// oracles. `Break::None` in every ordinary run.
    brk: Break,
    /// Fail the run when an oracle recorded zero executions over the sample, or
    /// executed on under `REACH_FLOOR_PERCENT` of its seeds (see
    /// `EXPECTED_ZERO`). Off by default — a small sample legitimately misses
    /// oracles, and a gate that cries wolf gets ignored. Wire it only where the
    /// sample is big enough to mean something.
    require_reach: bool,
    /// Share of seeds (0-100) whose fleet is partitioned: nodes home to
    /// different buckets and a minority of uploads arrive as mirror fills, so
    /// two buckets can commit different bytes under one filename and
    /// `replicate::decide`'s merge algebra actually runs. Zero by default —
    /// arming it perturbs the schedule, and the pinned regression seeds, the
    /// `--break` kill proofs and the measured baselines were all mined on the
    /// aligned one. Needs >1 bucket to mean anything.
    partition_percent: u64,
    /// `--trace-jsonl <path>`: record ONE seed's timeline as JSONL for the run
    /// visualizer (dev/scripts/viz). Pure observation — see [`Trace`] — and off
    /// by default, so it is not part of any world: it stays out of
    /// `reproduce_command` and out of `ROTATE_OVERRIDES`. One seed only, and
    /// only its FIRST execution: the determinism recheck reruns the same seed
    /// and would otherwise overwrite the file with a world that may legitimately
    /// differ (`--break rerun`).
    trace_jsonl: Option<String>,
    /// The chaos loop's op-class mix (`--weights`), out of whatever they sum to.
    /// `DEFAULT_OP_WEIGHTS` unless asked for, so every existing command means
    /// exactly what it always did. It exists mostly so a `--shrink` result that
    /// found a class the failure never needed can be pasted back — see
    /// [`shrink_search`].
    weights: [u16; OP_CLASSES],
    /// `--shrink`: on the first failing seed, search for the smallest profile
    /// that still fails THE SAME WAY and print it as a ready-to-paste command
    /// (see the `--shrink` section). Off by default and inert until a seed
    /// fails, so it cannot move any schedule.
    shrink: bool,
    /// `--staleness-secs`: the healthy-operation half of the STALENESS
    /// deadline, before the per-debt allowance. Pure oracle parameter — it
    /// moves no schedule — but it moves the verdict, so a non-default value is
    /// part of the reproduce command.
    staleness_secs: u64,
    /// `--sweep-faults`: exhaustive fault depth 1. Hold the schedule fixed and
    /// force every one of its chaos-phase storage ops, one at a time, to fail
    /// and then to crash the node that issued it — every other op clean. Random
    /// injection is a 3%-per-op coin flip compounded over the whole schedule, so
    /// the single-fault position that actually breaks something is a lottery
    /// (one recent bug cost ~60M soak seeds). This buys the whole depth-1 space
    /// of one schedule for a couple of seconds.
    sweep_faults: bool,
    /// `--force-fault <op>:<fail|crash>`: one run of what the sweep does, so a
    /// violation it reports has a repro line that is a single simulation.
    forced: Option<(u64, OpFate)>,
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
    /// Share of seeds that run a partitioned fleet (see `partition_for`). Zero
    /// unless asked for, so every pinned regression seed, every `--break` kill
    /// proof and every measured baseline keeps the exact schedule it was mined
    /// under; `--rotate` draws it, and the nightly's multi-bucket rows pass it.
    partition_percent: u64,
    /// Healthy-operation base of the STALENESS deadline (`--staleness-secs`).
    staleness_secs: u64,
    /// Depth-1 mode (`--sweep-faults`/`--force-fault`): the only fault in this
    /// run is `forced`, so random failures are off (`fail_percent` is zeroed)
    /// and the driver schedules no crashes of its own. Without that, a
    /// violation the sweep reports is "some crash schedule plus one forced
    /// op", which is not a depth-1 counterexample and not what the repro line
    /// would claim.
    depth1: bool,
    /// The single op-sequence number whose fate this run forces, and to what.
    /// `None` under `depth1` is the sweep's clean baseline.
    forced: Option<(u64, OpFate)>,
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
        // Absent unless a fault is forced, so every existing line reads exactly
        // as it always did.
        let forced = match self.forced {
            Some((at, fate)) => format!(" forced={at}:{}", fate.spelling()),
            None => String::new(),
        };
        format!(
            "nodes={} buckets={} packages={} files={} ops={} fail-percent={} partition={} \
             weights=[{}]{forced}",
            self.nodes,
            self.buckets,
            self.packages,
            self.files,
            self.ops,
            self.fail_percent,
            self.partition_percent,
            mix.join(", ")
        )
    }
}

/// The profile this seed runs, with depth-1 mode applied last: a run that
/// forces one fault admits no others, whatever the flags or the rotation drew.
/// Zeroing `fail_percent` here rather than at the flag is what lets
/// `--rotate --force-fault k:crash` reproduce a swept run exactly — `--rotate`
/// rejects `--fail-percent`, so the repro line has no other way to say it.
fn profile_for(seed: u64, args: &Args) -> Profile {
    let mut profile = workload_for(seed, args);
    if args.sweep_faults || args.forced.is_some() {
        profile.depth1 = true;
        profile.fail_percent = 0;
        profile.forced = args.forced;
    }
    profile
}

fn workload_for(seed: u64, args: &Args) -> Profile {
    if !args.rotate {
        return Profile {
            nodes: args.nodes,
            buckets: args.buckets,
            packages: args.packages,
            files: args.files,
            ops: args.ops,
            fail_percent: args.fail_percent,
            weights: args.weights,
            brk: args.brk,
            partition_percent: args.partition_percent,
            staleness_secs: args.staleness_secs,
            depth1: false,
            forced: None,
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
        // NOT drawn from the seed. Partitioning is a chaos dimension like
        // `--break`, not a workload shape: arming it perturbs every schedule,
        // and the rotating row is the one every pinned soak baseline was
        // measured on. `--rotate --partition N` is the partitioned soak.
        partition_percent: args.partition_percent,
        staleness_secs: args.staleness_secs,
        depth1: false,
        forced: None,
    }
}

/// The one command that re-runs exactly this seed. A rotating profile is a
/// pure function of the seed, so `--seed N --rotate` is complete on its own —
/// including the entity counts and the op-weight vector, which have no useful
/// flag form. A fixed profile has to carry its flags. Either way an armed
/// `--break` is part of the world: leave it off and the line reruns a
/// *different*, defect-free simulation, comes back green, and the failure it
/// was printed under gets filed as flaky.
fn reproduce_command(seed: u64, rotate: bool, profile: &Profile) -> String {
    let brk = match profile.brk.flag() {
        Some(spelling) => format!(" --break {spelling}"),
        None => String::new(),
    };
    // `--partition` is part of the world too: leave it off and the line reruns
    // an aligned fleet, which is a different simulation.
    let partition = match profile.partition_percent {
        0 => String::new(),
        percent => format!(" --partition {percent}"),
    };
    // A relaxed or tightened staleness base changes the VERDICT without
    // changing the world, which is the easier way to lose a repro: the run
    // comes back green under the default deadline and gets filed as flaky.
    let staleness = if profile.staleness_secs == STALENESS_HEALTHY_SECS {
        String::new()
    } else {
        format!(" --staleness-secs {}", profile.staleness_secs)
    };
    // A swept run is one op of a schedule nothing else reproduces: the flag
    // carries both the position and the fate, and it implies the depth-1 world
    // (no random failures, no scheduled crashes) that the sweep ran it in.
    //
    // The sweep's own BASELINE run is that same depth-1 world with nothing
    // forced, and no flag spells it — so the line points back at
    // `--sweep-faults`, which runs and re-reports that exact baseline before it
    // forces anything. Omitting it is not cosmetic: without a depth-1 marker
    // `profile_for` leaves `depth1` false on the rerun, the driver's own
    // scheduled crashes come back, and the schedule diverges (measured on seed
    // 7384 at `--nodes 2 --buckets 1 --ops 80`: 709 storage-op interleavings
    // against 604). A red baseline — the one case this branch exists to
    // report — would hand the operator a command for a different world, which
    // comes back green and gets filed as flaky.
    let depth1 = match (profile.forced, profile.depth1) {
        (Some((at, fate)), _) => format!(" --force-fault {at}:{}", fate.spelling()),
        (None, true) => " --sweep-faults".to_string(),
        (None, false) => String::new(),
    };
    if rotate {
        return format!(
            "cargo run --release --example vopr -- --seed {seed} \
             --rotate{partition}{staleness}{depth1}{brk}"
        );
    }
    // The op mix is implicit at `DEFAULT_OP_WEIGHTS` and drawn from the seed
    // under `--rotate`, so it appears only where it is load-bearing: a
    // `--shrink` result that zeroed a class, which is a different world from
    // the default mix and has to say so.
    let weights = if profile.weights == DEFAULT_OP_WEIGHTS {
        String::new()
    } else {
        format!(
            " --weights {}",
            profile.weights.map(|weight| weight.to_string()).join(",")
        )
    };
    format!(
        "cargo run --release --example vopr -- --seed {seed} --nodes {} --buckets {} \
         --packages {} --files {} --ops {} \
         --fail-percent {}{partition}{staleness}{weights}{depth1}{brk}",
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

/// Flags whose value `--rotate` draws from the seed instead. Passing both is a
/// contradiction, not a refinement, and it used to be a silent one: these
/// parsed fine, `profile_for` never read them under rotation, and
/// `--rotate --ops 200` ran 120 ops while the operator believed they had
/// widened coverage. A simulator that reports a workload it did not run is
/// worse than one that refuses to start, so `parse_args_from` rejects the pair.
/// Everything else stays legal under rotation — `--seed/--seeds/--start-seed/
/// --recheck-every/--forever/--max-secs/--break/--require-reach/--partition/
/// --shrink` are the real levers there (`--partition` is a chaos dimension like
/// `--break`, not a workload shape rotation derives; `--shrink` reads the
/// resolved profile and never supplies one), and `--seed N --rotate` must keep
/// reproducing on its own. `--weights` is the op mix, which rotation swarms.
const ROTATE_OVERRIDES: [&str; 7] = [
    "--nodes",
    "--buckets",
    "--packages",
    "--files",
    "--ops",
    "--fail-percent",
    "--weights",
];

/// Flags `--sweep-faults` cannot honour. The sweep is its own loop and owns the
/// exit before `main`'s seed loop starts, so nothing here ever reaches a run:
/// no seed is re-executed, no reach gate is evaluated, no shrink is attempted.
/// Accepting them silently is the `ROTATE_OVERRIDES` defect in a second place —
/// a CI lane copied off the seed loop (`simulation.yml` passes `--require-reach`)
/// would be a gate that cannot fail, and `--shrink` would report nothing to
/// shrink on a sweep that never populated the failing-seed list.
///
/// `--fail-percent` is here only above zero: the sweep zeroes it by design
/// (a swept op is the run's ONLY fault), so a nonzero rate is a depth-1+n run
/// the operator would not get — while `--fail-percent 0` is exactly what the
/// sweep's own baseline reproduce line carries and has to keep parsing.
const SWEEP_IGNORES: [&str; 4] = [
    "--recheck-every",
    "--require-reach",
    "--shrink",
    "--fail-percent",
];

fn parse_args() -> Args {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from(argv: impl Iterator<Item = String>) -> Args {
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
        partition_percent: 0,
        trace_jsonl: None,
        weights: DEFAULT_OP_WEIGHTS,
        shrink: false,
        staleness_secs: STALENESS_HEALTHY_SECS,
        sweep_faults: false,
        forced: None,
    };
    let mut it = argv;
    // Collected as they are seen, checked after the whole line parses, so the
    // rejection does not depend on `--rotate` coming first.
    let mut overridden: Vec<&'static str> = Vec::new();
    let mut swept_away: Vec<&'static str> = Vec::new();
    while let Some(flag) = it.next() {
        if let Some(name) = ROTATE_OVERRIDES.iter().find(|known| **known == flag) {
            overridden.push(name);
        }
        if let Some(name) = SWEEP_IGNORES.iter().find(|known| **known == flag) {
            swept_away.push(name);
        }
        if flag == "--break" {
            let name = it
                .next()
                .unwrap_or_else(|| panic!("missing value for --break"));
            args.brk = Break::parse(&name);
            continue;
        }
        // The string-valued flags are handled before `grab`, which parses u64.
        if flag == "--trace-jsonl" {
            args.trace_jsonl = Some(
                it.next()
                    .unwrap_or_else(|| panic!("missing value for --trace-jsonl")),
            );
            continue;
        }
        // `<op-sequence>:<fate>`, one string rather than a pair of flags: the
        // two halves are meaningless apart, and a repro line that can drop one
        // of them reruns a different world.
        if flag == "--force-fault" {
            let raw = it
                .next()
                .unwrap_or_else(|| panic!("missing value for --force-fault"));
            let (at, fate) = raw
                .split_once(':')
                .unwrap_or_else(|| panic!("--force-fault takes <op>:<fail|crash>, got {raw}"));
            let at = at
                .parse::<u64>()
                .unwrap_or_else(|e| panic!("bad op sequence in --force-fault: {e}"));
            let fate = match fate {
                "fail" => OpFate::Fail,
                "crash" => OpFate::Crash,
                other => panic!("--force-fault fate is fail or crash, got {other}"),
            };
            args.forced = Some((at, fate));
            continue;
        }
        if flag == "--weights" {
            let raw = it
                .next()
                .unwrap_or_else(|| panic!("missing value for --weights"));
            let mix: Vec<u16> = raw
                .split(',')
                .map(|part| {
                    part.trim()
                        .parse::<u16>()
                        .unwrap_or_else(|e| panic!("bad value for --weights: {e}"))
                })
                .collect();
            assert_eq!(
                mix.len(),
                OP_CLASSES,
                "--weights takes {OP_CLASSES} comma-separated weights, one per op class: {}",
                OP_LABELS.join(",")
            );
            // `pick_op` draws `rng.below(total)`: an all-zero mix divides by zero.
            assert!(
                mix.iter().any(|weight| *weight > 0),
                "--weights must leave at least one op class above zero"
            );
            args.weights.copy_from_slice(&mix);
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
            "--partition" => {
                let n = grab();
                assert!(n <= 100, "--partition is a percentage of seeds (0..=100)");
                args.partition_percent = n;
            }
            "--forever" => args.forever = true,
            "--max-secs" => args.max_secs = Some(grab()),
            "--rotate" => args.rotate = true,
            "--require-reach" => args.require_reach = true,
            "--shrink" => args.shrink = true,
            "--sweep-faults" => args.sweep_faults = true,
            "--staleness-secs" => args.staleness_secs = grab(),
            "--verbose" => {}
            other => panic!("unknown flag {other} (see examples/vopr.rs)"),
        }
    }
    assert!(
        !args.rotate || overridden.is_empty(),
        "--rotate derives the whole workload from the seed, so {} would be discarded — \
         drop them, or drop --rotate. Legal with --rotate: --seed --seeds --start-seed \
         --recheck-every --forever --max-secs --break --require-reach --partition --shrink \
         --staleness-secs --sweep-faults --force-fault.",
        overridden.join(" ")
    );
    // The sweep already forces the fault at every position; a hand-picked one
    // beside it would silently win or silently lose, and either way the operator
    // does not get the run they asked for.
    assert!(
        !(args.sweep_faults && args.forced.is_some()),
        "--sweep-faults forces a fault at every op in turn: --force-fault is what you pass \
         to rerun ONE of its results, not something to combine with it."
    );
    // `--fail-percent 0` says exactly what the sweep already does, and the
    // baseline's own reproduce line carries it, so it is not a discarded ask.
    swept_away.retain(|flag| *flag != "--fail-percent" || args.fail_percent > 0);
    assert!(
        !args.sweep_faults || swept_away.is_empty(),
        "--sweep-faults would discard {} — it runs its own loop and exits before the seed \
         loop those belong to. Drop them, or drop --sweep-faults. Legal with it: --seed \
         --seeds --start-seed --forever --max-secs --rotate --break --partition \
         --staleness-secs and the workload flags.",
        swept_away.join(" ")
    );
    // `--break rerun` plants its defect in the determinism recheck, which only
    // the seed loop runs — under a sweep it is an armed break that cannot go
    // red. Refuse the pair rather than print a 0% kill rate for it.
    assert!(
        !(args.sweep_faults && args.brk == Break::Rerun),
        "--break rerun is killed by the determinism recheck, which --sweep-faults does not \
         run: the pair is a gate that cannot fail. Run it on the seed loop instead."
    );
    // One file is one seed's timeline. A multi-seed run would interleave worlds
    // into one stream, and an unbounded one would never write the file at all.
    assert!(
        args.trace_jsonl.is_none()
            || (args.seeds == 1 && !args.forever && args.max_secs.is_none() && !args.sweep_faults),
        "--trace-jsonl records ONE seed: pass --seed N (or --seeds 1), and not \
         --forever/--max-secs/--sweep-faults. To trace a swept run, rerun it with the \
         --force-fault line the sweep printed."
    );
    args
}

fn dump_trace(outcome: &RunOutcome) {
    if let (Some(path), Some(log)) = (std::env::var_os("VOPR_TRACE_FILE"), &outcome.trace_log) {
        let _ = std::fs::write(path, log.join("\n"));
    }
}

fn run_once(seed: u64, profile: &Profile, rerun: bool, viz: Option<Arc<Trace>>) -> RunOutcome {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("build paused runtime");
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(run_seed(seed, *profile, rerun, viz)))
}

/// One violating swept run, printed the way a failing seed is: what broke, and
/// the single command that reruns exactly it.
fn report_sweep(seed: u64, rotate: bool, profile: &Profile, violations: &[String]) {
    match profile.forced {
        Some((at, fate)) => eprintln!(
            "vopr: SWEEP seed {seed} op {at} fate {} FAILED ({} violations):",
            fate.spelling(),
            violations.len()
        ),
        // The clean baseline is red on its own, so nothing the sweep goes on to
        // find would be attributable to a forced fault.
        None => eprintln!(
            "vopr: SWEEP seed {seed} baseline FAILED with no fault forced ({} violations) — \
             fix this schedule before reading anything into the sweep:",
            violations.len()
        ),
    }
    for violation in violations {
        eprintln!("  {violation}");
    }
    eprintln!("reproduce: {}", reproduce_command(seed, rotate, profile));
    eprintln!("profile: {}", profile.describe());
}

/// `--sweep-faults`: exhaustive fault depth 1 over a fixed schedule.
///
/// Run the schedule once clean to learn how many storage ops its chaos phase
/// issues, then run it once more per (op, fate) pair, forcing that one op to
/// that fate and leaving every other op alone. Every oracle runs on every such
/// run. Random injection reaches the same states only by lottery — a 3% coin
/// flip per op, compounded — and the two most recent depth-1 bugs cost ~16M and
/// ~60M soak seeds to hit. Here the whole space is a couple of seconds.
///
/// Stops at the first counterexample, like every non-census run: one is enough,
/// and the rest of the sweep only delays the report.
fn sweep_faults(args: &Args, started: std::time::Instant) -> i32 {
    let mut total_runs: u64 = 0;
    let mut total_swept: u64 = 0;
    let mut seeds_done: u64 = 0;
    for offset in 0.. {
        // `--forever`/`--max-secs` mean here what they mean in the seed loop, so
        // a depth-1 sweep can be a timeboxed lane. Silently ignoring them was
        // the same defect `ROTATE_OVERRIDES` exists to prevent: a run that
        // reports a budget it did not honour.
        if let Some(budget) = args.max_secs {
            if started.elapsed().as_secs() >= budget {
                break;
            }
        } else if !args.forever && offset >= args.seeds {
            break;
        }
        let seed = args.start_seed + offset;
        let mut profile = profile_for(seed, args);
        profile.forced = None;
        let seed_started = std::time::Instant::now();
        let baseline = run_once(seed, &profile, false, None);
        total_runs += 1;
        if !baseline.violations.is_empty() {
            report_sweep(seed, args.rotate, &profile, &baseline.violations);
            return 2;
        }
        let ops = baseline.chaos_ops;
        let mut fired: u64 = 0;
        for fate in SWEPT_FATES {
            for at in 0..ops {
                profile.forced = Some((at, fate));
                let outcome = run_once(seed, &profile, false, None);
                total_runs += 1;
                if outcome.forced_fired {
                    fired += 1;
                }
                if !outcome.violations.is_empty() {
                    report_sweep(seed, args.rotate, &profile, &outcome.violations);
                    return 2;
                }
            }
        }
        total_swept += ops;
        seeds_done += 1;
        // A sweep whose faults mostly did not fire covered mostly nothing: the
        // op's node was already down at that point. Print the ratio rather than
        // the run count alone, so a schedule that cannot be perturbed says so.
        println!(
            "vopr: sweep seed {seed} — {ops} chaos ops x {} fates, {} runs, {fired}/{} forced \
             faults fired, no violations, {:?}",
            SWEPT_FATES.len(),
            ops * SWEPT_FATES.len() as u64 + 1,
            ops * SWEPT_FATES.len() as u64,
            seed_started.elapsed()
        );
    }
    println!(
        "vopr: {seeds_done} seeds swept at fault depth 1, {total_swept} ops swept, \
         {total_runs} runs, 0 violations, in {:?} — every single-fault position held",
        started.elapsed()
    );
    0
}

/// The 24 reach counters, as a relaxed-atomic snapshot. Diffed around one
/// `run_once` this is that run's per-oracle execution count — the meter itself
/// is process-wide and cumulative, and `--trace-jsonl` traces one seed's first
/// execution, which is exactly the interval between two of these.
fn reach_snapshot() -> Vec<u64> {
    REACH
        .slots
        .iter()
        .map(|slot| slot.load(Ordering::Relaxed))
        .collect()
}

/// One `oracle` line per reach slot, plus one `violation` line per raw violation
/// string. `executed` is THIS RUN's count — the terminal's reach table is
/// process-cumulative and also counts the determinism rerun, so its totals can
/// be larger; `summary.oracle_scope` says so on the page. `verdict` is
/// `violated` when a violation string carries this oracle's prefix (see
/// `REACH_VIOLATION_PREFIX`), `not-executed` when the oracle never ran on this
/// world, `held` otherwise.
fn emit_oracles(viz: &Arc<Trace>, before: &[u64], after: &[u64], violations: &[String]) {
    for (slot, (label, unit)) in REACH_METER.iter().enumerate() {
        let reached = if slot == R::Determinism as usize {
            // DETERMINISM is the one oracle the traced run cannot bracket: the
            // rerun that executes it happens after `run_once` returns, so the
            // before/after pair always differs by 0 and the page called a check
            // the terminal had just printed "not executed". It fires at most
            // once per seed, so reading its slot live at trace-close time is
            // exactly this seed's count.
            REACH.slots[slot].load(Ordering::Relaxed)
        } else {
            after.get(slot).copied().unwrap_or_default()
        };
        let executed = reached.saturating_sub(before.get(slot).copied().unwrap_or_default());
        let prefix = REACH_VIOLATION_PREFIX[slot];
        let detail = if prefix.is_empty() {
            None
        } else {
            let needle = format!("{prefix}:");
            violations.iter().find(|v| v.starts_with(&needle))
        };
        let verdict = match (detail.is_some(), executed) {
            (true, _) => "violated",
            (false, 0) => "not-executed",
            (false, _) => "held",
        };
        viz.emit(
            "oracle",
            serde_json::json!({
                "name": label,
                "unit": unit,
                "executed": executed,
                "verdict": verdict,
                "detail": detail,
            }),
        );
    }
    for (n, text) in violations.iter().enumerate() {
        viz.emit("violation", serde_json::json!({ "n": n, "text": text }));
    }
}

/// Close the trace: the `oracle` lines, the `summary` line, then the file.
/// Called at every point `main` can leave a traced seed, because `main` exits
/// through `std::process::exit` and no `Drop` runs — which is also why the
/// oracles are emitted from here and not beside `run_once`: the determinism
/// recheck that executes one of them runs in between.
///
/// `reach` is the before/after counter pair bracketing the ONE traced execution.
/// `seed_exit_code` is what this seed's own verdict implies, and is not the
/// process exit code: `--require-reach` is a whole-run gate that cannot be known
/// from one seed, so a run can exit 4 with a seed that reported 0 here.
fn finish_trace(
    viz: Option<&Arc<Trace>>,
    outcome: &RunOutcome,
    reach: Option<(&[u64], &[u64])>,
    elapsed: std::time::Duration,
    seed_exit_code: i32,
) {
    let Some(viz) = viz else { return };
    if let Some((before, after)) = reach {
        emit_oracles(viz, before, after, &outcome.violations);
    }
    let (events_emitted, truncated) = viz.stats();
    viz.emit(
        "summary",
        serde_json::json!({
            "violations": outcome.violations.len(),
            "acked": outcome.acked,
            "trace_hash": format!("{:#x}", outcome.trace_hash),
            "trace_events": outcome.trace_events,
            "state_hash": format!("{:#x}", outcome.state_hash),
            "audit_view_repairs": outcome.audit_view_repairs,
            "repairs_by_class": outcome.repairs_by_class,
            "ack_totality": outcome.ack_totality,
            // Counting this line: the number of JSONL lines in the file.
            "events_emitted": events_emitted + 1,
            "truncated": truncated,
            "elapsed_ms": u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            "seed_exit_code": seed_exit_code,
            "oracle_scope": "Each oracle's `executed` counts this one traced run of this seed. \
                             The terminal's reach table is process-cumulative and also counts \
                             the determinism rerun, so its totals can be larger.",
        }),
    );
    viz.flush();
}

// ---------------------------------------------------------------------------
// `--shrink`: the smallest configuration that still fails the same way.
//
// Detection is not the bottleneck any more — the soak fleet explores millions
// of seeds unattended — reading the failure is. The same defect that is
// unreadable at 200 ops is obvious at 4, and until now that reduction was a
// human editing flags by hand, one rerun at a time.
//
// The search is greedy descent over the profile dimensions the harness already
// has flags for, repeated to a fixpoint. That constraint is the design: a
// shrunk configuration is a command anyone can paste, no shrink-only replay
// format exists, and nothing new lives inside the simulated world — every probe
// is `run_once` on the SAME seed, so the whole search stays a pure function of
// (seed, profile) like everything else here.
//
// What it does NOT do is reduce the op SEQUENCE. Skipping an individual op
// mid-run cannot be expressed as a profile, and the fault stream is drawn per
// storage op from one shared rng (`FaultPlan::admit`), so removing an op shifts
// every fault after it — a skip-mask reducer would need its own world dimension
// and its own replay flag to reproduce what it found. The op MIX is the cheap
// 90%: zeroing a whole class is a dimension that already exists, costs one
// probe per class, and answers the question a reader actually asks — "which
// operations does this bug need?" — which op-count truncation alone cannot,
// since it only ever removes the tail.
// ---------------------------------------------------------------------------

/// The kind each violation names itself with: the token before its first colon.
/// This is the whole notion of "the same failure" the search keeps fixed.
/// Exit status alone is not enough — a smaller world happily fails for an
/// unrelated reason, and a minimum reported under the wrong bug sends the
/// reader after a defect that was never there.
fn violation_kinds(violations: &[String]) -> BTreeSet<String> {
    violations
        .iter()
        .map(|text| text.split(':').next().unwrap_or(text).trim().to_string())
        .collect()
}

type AxisGet = fn(&Profile) -> u64;
type AxisSet = fn(&mut Profile, u64);

/// The dimensions `--shrink` descends, and the floor each may not cross.
///
/// `--partition` is deliberately absent: for one seed it is a threshold on a
/// single draw from a dedicated stream (`partition_for`), so every percent above
/// that draw yields the identical world and every percent below yields the
/// aligned one. It is binary here, and the search tries it as such rather than
/// reporting whichever arbitrary percent happens to sit on the boundary.
const SHRINK_AXES: [(AxisGet, AxisSet, u64); 6] = [
    (|p| p.ops, |p, v| p.ops = v, 1),
    (|p| p.buckets as u64, |p, v| p.buckets = v as usize, 1),
    (|p| p.nodes as u64, |p, v| p.nodes = v as usize, 1),
    (|p| p.packages as u64, |p, v| p.packages = v as usize, 1),
    (|p| u64::from(p.files), |p, v| p.files = v as u8, 1),
    (|p| p.fail_percent, |p, v| p.fail_percent = v, 0),
];

/// One probe: does this profile still fail the same way on this seed?
fn same_failure(seed: u64, profile: &Profile, target: &BTreeSet<String>) -> bool {
    violation_kinds(&run_once(seed, profile, false, None).violations) == *target
}

/// Greedy descent to a fixpoint: the smallest profile that still failed the
/// same way, and how many simulations it took.
fn shrink_search(seed: u64, base: &Profile, target: &BTreeSet<String>) -> (Profile, u64) {
    let mut best = *base;
    let mut probes = 0u64;
    loop {
        let mut shrank = false;
        // Smallest-first, so the first value that still fails IS that axis's
        // minimum given the rest of the profile. Failure is not monotone in any
        // of these — a bigger world is not a superset of a smaller one, it is a
        // different schedule — so a binary search would report a floor that is
        // only the first one it happened to land on. The scan is cheap where it
        // matters: every probe it runs is smaller than the value in hand.
        for (get, set, floor) in SHRINK_AXES {
            for value in floor..get(&best) {
                let mut trial = best;
                set(&mut trial, value);
                probes += 1;
                if same_failure(seed, &trial, target) {
                    best = trial;
                    shrank = true;
                    break;
                }
            }
        }
        if best.partition_percent > 0 {
            let mut trial = best;
            trial.partition_percent = 0;
            probes += 1;
            if same_failure(seed, &trial, target) {
                best = trial;
                shrank = true;
            }
        }
        for class in 0..OP_CLASSES {
            let mut trial = best;
            trial.weights[class] = 0;
            // `pick_op` draws `rng.below(total)`: an all-zero mix divides by zero.
            if best.weights[class] == 0 || trial.weights.iter().all(|weight| *weight == 0) {
                continue;
            }
            probes += 1;
            if same_failure(seed, &trial, target) {
                best = trial;
                shrank = true;
            }
        }
        if !shrank {
            return (best, probes);
        }
    }
}

/// Run the search under a failing seed and print the result as the one command
/// that reproduces it. Always the explicit fixed-profile form, never
/// `--rotate`: a shrunk mix is not a mix rotation would draw, and the two are
/// mutually exclusive on the command line anyway.
fn shrink_report(seed: u64, base: &Profile, violations: &[String]) {
    let target = violation_kinds(violations);
    let kinds: Vec<&str> = target.iter().map(String::as_str).collect();
    // The target has to be reproducible before it can be searched for. The
    // determinism recheck can adopt the RERUN's violations, and those are not
    // what a plain rerun of this profile produces — searching for them would
    // reject every candidate and report the base profile as its own minimum.
    if !same_failure(seed, base, &target) {
        eprintln!(
            "vopr: --shrink: a plain rerun of this profile does not reproduce {kinds:?} \
             (the failure came from the determinism recheck, which --shrink cannot search) \
             — nothing shrunk."
        );
        return;
    }
    let started = std::time::Instant::now();
    let (best, probes) = shrink_search(seed, base, &target);
    let mix: Vec<&str> = OP_LABELS
        .iter()
        .zip(best.weights)
        .filter(|(_, weight)| *weight > 0)
        .map(|(label, _)| *label)
        .collect();
    eprintln!(
        "vopr: --shrink ran {probes} simulations in {:?} — smallest configuration still failing \
         {kinds:?}:\nshrunk: {}\nprofile: {}\nops the failure needs: {}",
        started.elapsed(),
        reproduce_command(seed, false, &best),
        best.describe(),
        mix.join(" ")
    );
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
    // Built before the first run: the git subprocess and the argv read cannot
    // reach the simulated world.
    let viz = args
        .trace_jsonl
        .as_ref()
        .map(|path| Trace::new(path.clone()));
    let started = std::time::Instant::now();
    // Its own mode: hundreds of runs of ONE schedule, so the seed loop below —
    // and the process-wide reach and merge meters it reports — would describe a
    // sample nobody drew. Like `--shrink`, it owns the exit.
    if args.sweep_faults {
        std::process::exit(sweep_faults(&args, started));
    }
    let keep_going = args.forever || args.max_secs.is_some();
    // A `--break` run is a MEASUREMENT, not a search: it finishes its whole draw
    // so the kill rate it prints at the end is a number this run produced.
    // Everything else stops at its first counterexample — one is enough, and the
    // rest of the draw would only delay the report.
    let census = keep_going || args.brk != Break::None;
    let mut total_events: u64 = 0;
    let mut total_acked: usize = 0;
    let mut total_ack_totality: u64 = 0;
    let mut total_audit_repairs: u64 = 0;
    let mut total_repairs_by_class: [u64; 3] = [0; 3]; // [ordering, premature, race]
    let mut failed_seeds: Vec<u64> = Vec::new();
    let mut determinism_violations: Vec<u64> = Vec::new();
    let mut explored: u64 = 0;
    let mut rechecked: u64 = 0;
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
        // Bracketing `run_once` is the whole per-oracle instrumentation: two
        // relaxed reads of the 23 counters, no touch points inside the run.
        // The pair is per-RUN on purpose — it excludes the determinism rerun
        // below, which the terminal's cumulative meter includes. The lines
        // themselves are written by `finish_trace`, after that rerun.
        let reach_before = viz.as_ref().map(|_| reach_snapshot());
        let mut outcome = run_once(seed, &profile, false, viz.clone());
        let reach_after = viz.as_ref().map(|_| reach_snapshot());
        let reach = || reach_before.as_deref().zip(reach_after.as_deref());
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
            rechecked += 1;
            // `None`: the rerun is the same seed re-executed and must neither
            // append to nor truncate the first execution's file.
            let again = run_once(seed, &profile, true, None);
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
                    reproduce_command(seed, args.rotate, &profile),
                    profile.describe()
                );
                if !census {
                    // We exit on the next line, so this string reaches nothing
                    // but the trace — and without it the page would render
                    // "DETERMINISM held" on the run that caught it red.
                    let red = format!(
                        "DETERMINISM: seed {seed} re-executed to a different op trace ({:#x} vs \
                         {:#x})",
                        outcome.trace_hash, again.trace_hash
                    );
                    outcome.violations.push(red);
                    finish_trace(viz.as_ref(), &outcome, reach(), started.elapsed(), 3);
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
                    reproduce_command(seed, args.rotate, &profile),
                    profile.describe()
                );
                if !census {
                    let red = format!(
                        "DETERMINISM: seed {seed} re-executed to the same op trace but a \
                         different final world ({:#x} vs {:#x})",
                        outcome.state_hash, again.state_hash
                    );
                    outcome.violations.push(red);
                    finish_trace(viz.as_ref(), &outcome, reach(), started.elapsed(), 3);
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
        // Every hit this seed could produce is in, rerun included: close it so
        // the meter knows how many seeds each oracle actually ran on.
        REACH.end_of_seed();
        MERGE.end_of_seed();
        // `--trace-jsonl` asserts a single bounded seed, so this runs exactly
        // once and every path that leaves the loop early has already flushed.
        finish_trace(
            viz.as_ref(),
            &outcome,
            reach(),
            started.elapsed(),
            if outcome.violations.is_empty() { 0 } else { 2 },
        );
        if !outcome.violations.is_empty() {
            eprintln!(
                "vopr: seed {seed} FAILED ({} violations):",
                outcome.violations.len()
            );
            for violation in &outcome.violations {
                eprintln!("  {violation}");
            }
            eprintln!(
                "reproduce: {}",
                reproduce_command(seed, args.rotate, &profile)
            );
            eprintln!("profile: {}", profile.describe());
            // The search runs hundreds of extra simulations through the same
            // process-wide reach and merge meters, so it stops the run: a soak
            // that kept going would report a coverage table those probes wrote.
            // A shrink is a diagnosis session, not a sample.
            if args.shrink {
                shrink_report(seed, &profile, &outcome.violations);
                std::process::exit(2);
            }
            failed_seeds.push(seed);
            if !census {
                std::process::exit(2);
            }
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
        format!(
            "rotating(nodes 2-3, buckets 1-3, packages 1-6, files 1-4, ops 80-200, \
             swarmed op mix, fault+crash-only, partition {}%)",
            args.partition_percent
        )
    } else {
        // The RESOLVED profile, not the flags: `--force-fault` zeroes the
        // failure rate (its one fault is the run's only fault), and a summary
        // that echoed `--fail-percent 3` back would report a workload this run
        // did not run. A fixed profile is the same for every seed, so the first
        // one describes them all.
        let resolved = profile_for(args.start_seed, &args);
        format!(
            "nodes={} buckets={} packages={} files={} ops/run={} fail-percent={}",
            resolved.nodes,
            resolved.buckets,
            resolved.packages,
            resolved.files,
            resolved.ops,
            resolved.fail_percent
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
    if let Some(flag) = args.brk.flag() {
        // The kill rate as a re-measured artifact. dev/TESTING.md used to carry
        // it by hand, and a hand-carried number drifts silently in exactly one
        // direction: an oracle quietly stops catching its own break and the doc
        // still says it does. Print it on every `--break` run, so the CI kill
        // proof's own log is the record and a drop is visible the day it lands
        // instead of the day someone re-measures.
        //
        // A determinism divergence is a kill too — it is the whole of what
        // `--break rerun` plants — and it is booked in its own list, so count
        // the union rather than one column of it.
        let killed = (failed_seeds.len()
            + determinism_violations
                .iter()
                .filter(|seed| !failed_seeds.contains(seed))
                .count()) as u64;
        println!(
            "vopr: --break {flag} killed {killed}/{explored} seeds ({}%) — measured by this run; \
             a draw is a gate only while it stays several times the seeds-to-first-kill this \
             implies (dev/TESTING.md)",
            killed * 100 / explored.max(1),
        );
    }
    let unreached = report_reach(explored, rechecked, args.brk);
    report_merge(explored);
    if args.shrink && failed_seeds.is_empty() {
        eprintln!("vopr: --shrink had nothing to shrink — no seed in this run violated an oracle.");
    }
    if !determinism_violations.is_empty() {
        std::process::exit(3);
    }
    if !failed_seeds.is_empty() {
        std::process::exit(2);
    }
    if args.require_reach && !unreached.is_empty() {
        eprintln!(
            "vopr: --require-reach FAILED — {} oracle(s) never executed over {explored} seeds, \
             or executed on under {REACH_FLOOR_PERCENT}% of them: {unreached:?}. An oracle that \
             verified nothing is a defect report, not a pass — and a run total hides that just \
             as well as a zero does: either the workload cannot reach it (widen it), or it is \
             unreachable and belongs in EXPECTED_ZERO with a reason.",
            unreached.len()
        );
        std::process::exit(4);
    }
}

// ---------------------------------------------------------------------------
// The gate's own logic. `--break` proves an oracle can go red and the reach
// meter proves it ran; these prove the meter's verdict is not itself a rubber
// stamp — the failure this file keeps re-learning is a green gate nobody has
// watched go red.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    /// One seed's executions, then the boundary the driver closes it with.
    fn seed(reach: &Reach, executions: &[R]) {
        for slot in executions {
            reach.hit(*slot);
        }
        reach.end_of_seed();
    }

    fn seeds_reached(reach: &Reach, slot: R) -> u64 {
        reach.seed_hits[slot as usize].load(Ordering::Relaxed)
    }

    #[test]
    fn the_meter_counts_seeds_that_executed_not_executions() {
        let reach = Reach::new();
        seed(&reach, &[R::Durability, R::Durability, R::Durability]);
        seed(&reach, &[R::Durability]);
        seed(&reach, &[R::Tombstone]); // durability verified nothing here
        assert_eq!(
            reach.slots[R::Durability as usize].load(Ordering::Relaxed),
            4,
            "the run total must keep counting every execution"
        );
        assert_eq!(
            seeds_reached(&reach, R::Durability),
            2,
            "four executions on two seeds is two seeds, however they clumped"
        );
        assert_eq!(seeds_reached(&reach, R::Tombstone), 1);
        assert_eq!(seeds_reached(&reach, R::Staleness), 0);
    }

    #[test]
    fn a_run_total_no_longer_covers_for_seeds_that_verified_nothing() {
        // The nightly multi-bucket-crash-only row as it shipped: 4,010
        // DURABILITY executions over 8,698 seeds reads healthy, and 4 seeds in
        // 5 evaluated an empty ledger (75fd6b2).
        let (note, failed) = reach_verdict(4010, 1754, 8698, None, false, false);
        assert!(failed, "20% per-seed reach passed the gate: {note:?}");
        assert!(note.contains("STARVED"), "{note}");
        // Same row with the corpus its oracles need: silent.
        let (note, failed) = reach_verdict(134_530, 13_069, 13_122, None, false, false);
        assert!(!failed && note.is_empty(), "{note}");
    }

    #[test]
    fn the_floor_is_a_share_of_seeds_not_a_count() {
        // Exactly at the floor is reached, one seed short of it is not.
        assert!(!reach_verdict(100, 25, 100, None, false, false).1);
        assert!(reach_verdict(100, 24, 100, None, false, false).1);
        // The kill-proof step runs six seeds (ci.yml); an absolute floor would
        // red every small sample that is doing nothing wrong.
        assert!(!reach_verdict(2, 2, 6, None, false, false).1);
    }

    #[test]
    fn the_floor_is_silent_wherever_a_standing_excuse_already_is() {
        let excuse = expected_zero(R::Convergence as usize, false, false);
        assert!(
            excuse.is_some(),
            "single-bucket topology excuse went missing"
        );
        assert!(!reach_verdict(0, 0, 1000, excuse, false, false).1);
        assert!(
            !reach_verdict(9, 3, 1000, excuse, false, false).1,
            "an excused oracle must not be gated on how often it runs"
        );
        assert!(
            !reach_verdict(9, 3, 1000, None, true, false).1,
            "a deliberate defect's workload is nobody's coverage regression"
        );
    }

    /// A conflict oracle is allowed to read thin — conflicts are a tail event —
    /// but not to read nothing. Waiving the floor without keeping the zero gate
    /// is how an oracle nobody has watched execute survives a review.
    #[test]
    fn a_tail_event_waives_the_floor_but_not_the_zero() {
        let (note, failed) = reach_verdict(44_243, 7_214, 36_571, None, false, true);
        assert!(!failed, "19% of seeds is the honest reading for a conflict");
        assert!(note.contains("tail event"), "{note}");
        assert!(
            reach_verdict(0, 0, 36_571, None, false, true).1,
            "a partitioned run that produced no conflict at all is a defect"
        );
    }

    /// The debt the deadline is bought with has to be *work*, not noise: a
    /// marker is work a breadcrumb points at, a key two buckets disagree on is
    /// work only the tree diff will find, and an agreed fleet owes nothing.
    /// The debt the deadline is bought with has to be *work*: a marker is work a
    /// breadcrumb points at, a key two buckets disagree on is work only the tree
    /// diff will find, and an agreed fleet owes nothing. Both are one record.
    #[test]
    fn debt_counts_breadcrumbed_work_and_the_lost_copies_alike() {
        let dump = |pairs: &[(&str, &str)]| -> BTreeMap<String, Vec<u8>> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                .collect()
        };
        let agreed = dump(&[("packages/p/a.whl", "bytes")]);
        assert_eq!(replication_debt(&[agreed.clone(), agreed.clone()]), 0);
        assert!(fleet_agreed(&[agreed.clone(), agreed.clone()]));

        let mut owing = agreed.clone();
        owing.insert("_repl/1/p/a.whl!7".into(), b"note".to_vec());
        owing.insert("_dirty/p".into(), b"marker".to_vec());
        assert_eq!(
            replication_debt(&[owing, agreed.clone()]),
            2,
            "a queued note and an undrained marker are two records of work"
        );

        assert_eq!(
            replication_debt(&[agreed.clone(), dump(&[])]),
            1,
            "a copy one bucket never got is debt nothing on the fast path can see"
        );
        assert!(!fleet_agreed(&[agreed, dump(&[])]));
    }

    /// The deadline is affine in the work and no faster. A bound that grew
    /// superlinearly would excuse the fleet whose per-record cost rises with its
    /// own backlog — which is the livelock this oracle exists to catch.
    #[test]
    fn the_deadline_grows_one_cycle_per_page_of_debt() {
        assert_eq!(
            staleness_deadline(300, 0),
            300,
            "a fleet owing nothing gets the healthy promise and nothing else"
        );

        // One record and one page cost the same cycle: waiting for a periodic
        // sweep does not get cheaper because the queue behind it is short.
        assert_eq!(staleness_deadline(300, 1), 300 + STALENESS_CYCLE_SECS);
        assert_eq!(
            staleness_deadline(300, STALENESS_CYCLE_RECORDS),
            300 + STALENESS_CYCLE_SECS
        );
        assert_eq!(
            staleness_deadline(300, STALENESS_CYCLE_RECORDS + 1),
            300 + 2 * STALENESS_CYCLE_SECS,
            "a page and one more is two cycles"
        );

        // Linear, checked as linear: doubling the pages doubles the allowance
        // over the base, so the owner's 20,000 queued records buy 20 cycles —
        // half an hour of forgiveness — and not 400.
        let ten = staleness_deadline(300, 10 * STALENESS_CYCLE_RECORDS);
        let twenty = staleness_deadline(300, 20 * STALENESS_CYCLE_RECORDS);
        assert_eq!(twenty - 300, 2 * (ten - 300));
        assert_eq!(twenty, 300 + 20 * STALENESS_CYCLE_SECS);
    }

    /// The margin the run prints is a SHARE of each seed's own deadline. Ranking
    /// by absolute seconds would report whichever seed was owed the most
    /// allowance and used it comfortably, and never the seed about to breach.
    #[test]
    fn the_tightest_margin_is_the_largest_share_not_the_longest_wait() {
        let debt = 0;
        let comfortable = Staleness {
            seed: 1,
            secs: Some(600),
            deadline: 1200,
            debt,
        };
        let tight = Staleness {
            seed: 2,
            secs: Some(280),
            deadline: 300,
            debt,
        };
        assert!(tight.spent_pct() > comfortable.spent_pct());
        let never = Staleness {
            seed: 3,
            secs: None,
            deadline: 300,
            debt,
        };
        assert!(
            never.spent_pct() > tight.spent_pct(),
            "a fleet that never agreed is past every deadline there is"
        );
    }

    #[test]
    fn a_zero_still_fails_on_its_own() {
        let (note, failed) = reach_verdict(0, 0, 50_000, None, false, false);
        assert!(failed && note.contains("NEVER EXECUTED"), "{note}");
    }

    fn a_profile(brk: Break) -> Profile {
        Profile {
            nodes: 3,
            buckets: 3,
            packages: 6,
            files: 4,
            ops: 200,
            fail_percent: 3,
            weights: DEFAULT_OP_WEIGHTS,
            brk,
            partition_percent: 0,
            staleness_secs: STALENESS_HEALTHY_SECS,
            depth1: false,
            forced: None,
        }
    }

    /// The line printed under a failing seed has to rerun the world that
    /// failed. Drop the armed break and it reruns a defect-free simulation,
    /// comes back green, and whoever is triaging a red kill-proof step files
    /// the dead oracle as flaky.
    #[test]
    fn the_reproduce_line_carries_the_armed_break() {
        let armed = a_profile(Break::View);
        assert_eq!(
            reproduce_command(60_000_000, false, &armed),
            "cargo run --release --example vopr -- --seed 60000000 --nodes 3 --buckets 3 \
             --packages 6 --files 4 --ops 200 --fail-percent 3 --break view"
        );
        // Under --rotate the seed is the whole profile — but not the break.
        assert_eq!(
            reproduce_command(21_000_000, true, &armed),
            "cargo run --release --example vopr -- --seed 21000000 --rotate --break view"
        );
    }

    /// ...and not one token more when nothing is armed: the pinned seeds in
    /// ci.yml are pasted from this line.
    #[test]
    fn an_ordinary_failure_gets_an_ordinary_command() {
        let clean = a_profile(Break::None);
        assert_eq!(
            reproduce_command(7384, false, &clean),
            "cargo run --release --example vopr -- --seed 7384 --nodes 3 --buckets 3 \
             --packages 6 --files 4 --ops 200 --fail-percent 3"
        );
        assert_eq!(
            reproduce_command(7384, true, &clean),
            "cargo run --release --example vopr -- --seed 7384 --rotate"
        );
    }

    fn argv(line: &[&str]) -> std::vec::IntoIter<String> {
        line.iter()
            .map(|word| (*word).to_string())
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// `--rotate` draws the workload from the seed, so a workload flag beside
    /// it was parsed and then thrown away: `--rotate --ops 200` ran 120 ops and
    /// said nothing. Flags on both sides of `--rotate` here — the verdict is
    /// order-independent.
    #[test]
    #[should_panic(expected = "--ops --packages would be discarded")]
    fn rotate_refuses_the_workload_flags_it_would_discard() {
        parse_args_from(argv(&[
            "--ops",
            "200",
            "--rotate",
            "--packages",
            "6",
            "--seeds",
            "30",
        ]));
    }

    /// The levers that still mean something under rotation. Rejecting these
    /// would break the nightly rotating row, `make vopr-soak` and the soak
    /// fleet's unit file.
    #[test]
    fn rotation_keeps_its_real_levers() {
        let args = parse_args_from(argv(&[
            "--rotate",
            "--seed",
            "21000000",
            "--start-seed",
            "7",
            "--seeds",
            "30",
            "--recheck-every",
            "500",
            "--forever",
            "--max-secs",
            "60",
            "--break",
            "view",
            "--require-reach",
            "--partition",
            "50",
            "--verbose",
        ]));
        assert!(args.rotate && args.forever && args.require_reach);
        assert_eq!((args.start_seed, args.seeds), (7, 30));
        assert_eq!((args.recheck_every, args.max_secs), (500, Some(60)));
        assert!(args.brk == Break::View);
        assert_eq!(args.partition_percent, 50);
    }

    /// `--shrink` reads the profile a failing seed resolved to and never
    /// supplies one, so it is a lever under rotation like `--break` — and a
    /// rotating soak is exactly where an unreadable failure comes from.
    #[test]
    fn shrinking_is_legal_under_rotation() {
        let args = parse_args_from(argv(&["--rotate", "--seed", "21000000", "--shrink"]));
        assert!(args.rotate && args.shrink);
        assert_eq!(args.weights, DEFAULT_OP_WEIGHTS);
    }

    /// The op mix IS a workload shape, so `--rotate` draws it and a flag beside
    /// it would be discarded — the same silent contradiction `--ops` used to be.
    #[test]
    #[should_panic(expected = "--weights would be discarded")]
    fn rotation_refuses_a_hand_written_op_mix() {
        parse_args_from(argv(&["--rotate", "--weights", "40,10,25,7,4,5,5,4"]));
    }

    /// A mix `--shrink` zeroed down to one class has to parse back, and an
    /// all-zero mix has to be refused: `pick_op` draws `rng.below(total)`.
    #[test]
    fn an_op_mix_round_trips_through_its_flag() {
        let args = parse_args_from(argv(&["--weights", "40,0,25,0,0,0,0,0"]));
        assert_eq!(args.weights, [40, 0, 25, 0, 0, 0, 0, 0]);
    }

    #[test]
    #[should_panic(expected = "at least one op class above zero")]
    fn an_op_mix_of_all_zeros_is_refused() {
        parse_args_from(argv(&["--weights", "0,0,0,0,0,0,0,0"]));
    }

    /// The sweep prints this line under a violation; pasted back it has to be
    /// the same single simulation — the forced op, its fate, and the depth-1
    /// world (no random failures) the sweep ran it in.
    #[test]
    fn a_forced_fault_round_trips_through_its_flag() {
        let args = parse_args_from(argv(&[
            "--seed",
            "7384",
            "--nodes",
            "2",
            "--buckets",
            "1",
            "--ops",
            "80",
            "--fail-percent",
            "3",
            "--force-fault",
            "137:crash",
        ]));
        assert!(matches!(args.forced, Some((137, OpFate::Crash))));
        let profile = profile_for(7384, &args);
        assert!(profile.depth1, "a forced fault is the run's only fault");
        assert_eq!(
            profile.fail_percent, 0,
            "--fail-percent 3 beside a forced fault would make the run depth 1+n"
        );
        let line = reproduce_command(7384, false, &profile);
        assert!(line.ends_with("--force-fault 137:crash"), "{line}");
        assert!(line.contains("--fail-percent 0"), "{line}");
    }

    /// Under `--rotate` the workload comes from the seed and `--fail-percent` is
    /// refused, so the forced fault is the only thing the line can carry — and
    /// it has to imply the zeroed failure rate on its own, or the rerun is a
    /// different world.
    #[test]
    fn a_forced_fault_reproduces_under_rotation_too() {
        let args = parse_args_from(argv(&[
            "--rotate",
            "--seed",
            "13792606396100784374",
            "--force-fault",
            "12:fail",
        ]));
        let profile = profile_for(13_792_606_396_100_784_374, &args);
        assert_eq!(profile.fail_percent, 0);
        assert!(
            reproduce_command(13_792_606_396_100_784_374, true, &profile)
                .ends_with("--rotate --force-fault 12:fail")
        );
    }

    #[test]
    #[should_panic(expected = "--force-fault is what you pass")]
    fn a_sweep_refuses_a_hand_picked_fault_beside_it() {
        parse_args_from(argv(&["--sweep-faults", "--force-fault", "3:fail"]));
    }

    /// The sweep's BASELINE is the depth-1 world with nothing forced. Nothing
    /// else spells that, so the line has to name the sweep — and pasted back it
    /// has to resolve to the same world, or a red baseline reruns green.
    #[test]
    fn the_sweep_baseline_reproduces_as_a_sweep() {
        let args = parse_args_from(argv(&[
            "--seed",
            "7384",
            "--nodes",
            "2",
            "--buckets",
            "1",
            "--ops",
            "80",
            "--sweep-faults",
        ]));
        let mut profile = profile_for(7384, &args);
        profile.forced = None; // what `sweep_faults` runs the baseline with
        let line = reproduce_command(7384, false, &profile);
        assert!(line.ends_with("--sweep-faults"), "{line}");
        // And it round-trips: the pasted line resolves to the same depth-1
        // world, which is the whole point of printing it.
        let pasted = parse_args_from(
            line.split_whitespace()
                .skip_while(|word| *word != "--seed")
                .map(str::to_string),
        );
        let reparsed = profile_for(7384, &pasted);
        assert!(reparsed.depth1 && reparsed.forced.is_none() && reparsed.fail_percent == 0);
        // Under rotation the workload comes from the seed, so the marker is all
        // the line can carry.
        let mut rotating = a_profile(Break::None);
        rotating.depth1 = true;
        rotating.forced = None;
        assert!(
            reproduce_command(7384, true, &rotating).ends_with("--rotate --sweep-faults"),
            "{}",
            reproduce_command(7384, true, &rotating)
        );
        // An ordinary run is untouched.
        assert!(!reproduce_command(7384, false, &a_profile(Break::None)).contains("--sweep"));
    }

    /// Flags the sweep's own loop cannot honour are refused, not swallowed. All
    /// four used to parse fine and do nothing: a `--require-reach` lane copied
    /// off `simulation.yml` onto a sweep exited 0 without evaluating the gate.
    /// Flags on both sides of `--sweep-faults` — the verdict is order-independent.
    #[test]
    #[should_panic(
        expected = "would discard --recheck-every --require-reach --shrink --fail-percent"
    )]
    fn a_sweep_refuses_the_flags_its_own_loop_cannot_honour() {
        parse_args_from(argv(&[
            "--recheck-every",
            "1",
            "--sweep-faults",
            "--require-reach",
            "--shrink",
            "--fail-percent",
            "3",
        ]));
    }

    /// `--fail-percent 0` says what the sweep already does, and the sweep's own
    /// baseline reproduce line carries it — refusing that would make the line it
    /// prints unpasteable.
    #[test]
    fn a_sweep_accepts_the_zero_failure_rate_its_own_repro_line_carries() {
        let args = parse_args_from(argv(&["--sweep-faults", "--fail-percent", "0"]));
        assert_eq!(profile_for(1, &args).fail_percent, 0);
    }

    #[test]
    #[should_panic(expected = "a gate that cannot fail")]
    fn a_sweep_refuses_the_one_break_it_cannot_kill() {
        parse_args_from(argv(&["--sweep-faults", "--break", "rerun"]));
    }

    /// The trap the sweep is built on. `fail_percent == 0` used to mean
    /// "crash-only", and two oracles tighten on it; a swept run is at zero
    /// percent and still injects one failure. Key them on the percent and every
    /// forced-`Fail` run reports the sweep's own fault as a bug — the whole tool
    /// becomes noise. The crash lane must NOT relax: that is the strict half.
    #[test]
    fn a_forced_failure_relaxes_the_crash_only_oracles_and_a_forced_crash_does_not() {
        assert!(!availability_faults(0, None), "crash-only, as before");
        assert!(availability_faults(3, None), "injected failures, as before");
        assert!(
            availability_faults(0, Some((7, OpFate::Fail))),
            "a swept Fail IS an injected storage failure, whatever the percent says"
        );
        assert!(
            !availability_faults(0, Some((7, OpFate::Crash))),
            "a swept Crash is crash-only: the strict contract stands"
        );
    }

    #[test]
    #[should_panic(expected = "--weights takes 8 comma-separated weights")]
    fn a_short_op_mix_is_refused() {
        parse_args_from(argv(&["--weights", "40,10,25"]));
    }

    /// A failure is "the same failure" when it names the same kinds. The kind is
    /// the token before the first colon: the rest of the string carries bucket
    /// numbers, filenames and whole dumps, which differ on every configuration
    /// the search tries and would make every candidate look like a new bug.
    #[test]
    fn a_failure_is_identified_by_the_kinds_it_names() {
        let violations = vec![
            "DURABILITY: acked vopr-alpha/1.0 lost on bucket 1".to_string(),
            "DURABILITY: acked vopr-beta/2.0 lost on bucket 0".to_string(),
            "CONVERGENCE: bucket 0 vs bucket 1 differ on simple/index.json".to_string(),
        ];
        let kinds = violation_kinds(&violations);
        assert_eq!(
            kinds.iter().map(String::as_str).collect::<Vec<_>>(),
            ["CONVERGENCE", "DURABILITY"]
        );
        // Same kinds, different detail — still the same failure.
        assert_eq!(
            kinds,
            violation_kinds(&[
                "CONVERGENCE: bucket 0 vs bucket 1 differ on simple/vopr-zeta/index.html"
                    .to_string(),
                "DURABILITY: acked vopr-zeta/4.0 lost on bucket 2".to_string(),
            ])
        );
        // A different kind is a different bug, however tempting the minimum.
        assert_ne!(kinds, violation_kinds(&["DURABILITY: acked".to_string()]));
    }

    /// A shrunk mix is not a mix `--rotate` would draw, so the result prints as
    /// an explicit command — and `--weights` appears ONLY there. Every pinned
    /// seed in ci.yml is pasted from this line and none of them carries one.
    #[test]
    fn a_shrunk_op_mix_is_the_only_thing_that_prints_weights() {
        let mut shrunk = a_profile(Break::None);
        shrunk.weights = [40, 0, 25, 0, 0, 0, 0, 4];
        assert_eq!(
            reproduce_command(7384, false, &shrunk),
            "cargo run --release --example vopr -- --seed 7384 --nodes 3 --buckets 3 \
             --packages 6 --files 4 --ops 200 --fail-percent 3 --weights 40,0,25,0,0,0,0,4"
        );
        assert!(!reproduce_command(7384, false, &a_profile(Break::None)).contains("--weights"));
    }

    /// `--trace-jsonl` is a recorder, not a workload lever: it takes a path (so
    /// it cannot go through the u64 `grab`), stays out of `reproduce_command`,
    /// and is legal beside `--rotate`.
    #[test]
    fn the_trace_recorder_parses_beside_rotate() {
        let args = parse_args_from(argv(&[
            "--rotate",
            "--seed",
            "21000000",
            "--trace-jsonl",
            "/tmp/vopr.jsonl",
        ]));
        assert!(args.rotate);
        assert_eq!(args.trace_jsonl.as_deref(), Some("/tmp/vopr.jsonl"));
        // ...and it is not part of the world the reproduce line rebuilds.
        assert!(!reproduce_command(21_000_000, true, &a_profile(Break::None)).contains("--trace"));
    }

    /// One file is one seed's timeline. A 25-seed default would interleave 25
    /// worlds into one stream and the reader would render a world that never
    /// existed.
    #[test]
    #[should_panic(expected = "--trace-jsonl records ONE seed")]
    fn the_trace_recorder_refuses_a_multi_seed_run() {
        parse_args_from(argv(&["--trace-jsonl", "/tmp/vopr.jsonl"]));
    }

    /// A partitioned world is not reproducible from the seed — `--partition` is
    /// a chaos dimension, like `--break`. Drop it from the line and the rerun is
    /// an aligned fleet: a different simulation that comes back green.
    #[test]
    fn the_reproduce_line_carries_the_partition() {
        let mut partitioned = a_profile(Break::None);
        partitioned.partition_percent = 100;
        assert!(reproduce_command(42, false, &partitioned).ends_with("--partition 100"));
        assert_eq!(
            reproduce_command(42, true, &partitioned),
            "cargo run --release --example vopr -- --seed 42 --rotate --partition 100"
        );
        // ...and not one token more on the aligned default.
        assert!(!reproduce_command(42, true, &a_profile(Break::None)).contains("--partition"));
    }

    /// ...and without `--rotate` those same flags are the whole profile, which
    /// is what the four pinned nightly rows and every `reproduce:` line depend
    /// on. The weights are asserted too, and are not incidental: a non-rotating
    /// run must use `DEFAULT_OP_WEIGHTS`, the mix that reproduces the
    /// pre-swarm `rng.below(100)` arms exactly. That equality is the only
    /// reason the pinned CI regression seeds survived the workload widening
    /// byte-for-byte, so a change here silently retires their coverage.
    #[test]
    fn without_rotation_the_workload_flags_still_win() {
        let args = parse_args_from(argv(&[
            "--nodes",
            "3",
            "--buckets",
            "2",
            "--packages",
            "6",
            "--files",
            "2",
            "--ops",
            "160",
            "--fail-percent",
            "0",
        ]));
        let profile = profile_for(1, &args);
        assert_eq!(
            profile.describe(),
            "nodes=3 buckets=2 packages=6 files=2 ops=160 fail-percent=0 partition=0 \
             weights=[publish 40, delete 10, tick 25, sweep 7, reconcile 4, jump 5, crash 5, nudge 4]"
        );
    }

    /// One table, so a break added to the parser cannot go missing from the
    /// reproduce line.
    #[test]
    fn every_break_spells_itself_back() {
        for (spelling, brk) in BREAKS {
            assert!(
                Break::parse(spelling) == brk,
                "--break {spelling} misparsed"
            );
            assert_eq!(brk.flag(), Some(spelling));
            assert!(reproduce_command(1, false, &a_profile(brk))
                .ends_with(&format!("--break {spelling}")));
        }
        assert_eq!(Break::None.flag(), None);
    }

    /// `supports_leases` is a promise about four methods, not one, and each has
    /// a defaulted trait impl that errors. So a forwarder that claims the
    /// promise and then inherits one default compiles clean, passes every unit
    /// test, and only speaks up as a fleet that cannot write its global index —
    /// which is how `head_etag` reached master with the sim answering "no
    /// conditional writes on this backend" for a backend it had just said had
    /// them. Every violation the smoke reported was downstream of that one
    /// unforwarded method; none of them named it.
    #[tokio::test]
    async fn the_fault_view_forwards_every_conditional_write_it_claims() {
        let start =
            time::OffsetDateTime::parse(SIM_START, &time::format_description::well_known::Rfc3339)
                .expect("valid sim start");
        let view = FaultView {
            inner: SimStorage::new(SimClock::new(start)),
            node: 0,
            bucket: 0,
            plan: FaultPlan::new(0, 1, 0, None, Break::None, None),
        };
        assert!(view.supports_leases(), "the claim under test");

        let key = "simple/index.json";
        assert_eq!(
            view.head_etag(key).await.expect("head_etag forwards"),
            None,
            "absent is None, not an error"
        );
        let first = view
            .put_if_none_match(key, b"{}".to_vec())
            .await
            .expect("put_if_none_match forwards")
            .expect("created");
        assert_eq!(
            view.head_etag(key).await.expect("head_etag forwards"),
            Some(first.clone()),
            "the token a HEAD reports is the one the write handed back — the \
             whole point of the method, and what a listed ETag cannot promise"
        );
        assert_eq!(
            view.get_with_etag(key)
                .await
                .expect("get_with_etag forwards"),
            Some((b"{}".to_vec(), first.clone())),
        );
        assert!(
            view.put_if_match(key, &first, b"[]".to_vec())
                .await
                .expect("put_if_match forwards")
                .is_some(),
            "a CAS on the freshly-read token wins"
        );
    }
}
