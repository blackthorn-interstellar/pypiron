//! Machine-checked convergence proof for pypiron's single-bucket event
//! protocol, driven by the `stateright` model checker.
//!
//! # What is modeled
//!
//! The `_dirty/` intent/commit marker protocol from dev/DESIGN.md ("Write path:
//! dirty markers, not a queue" and "Ordering invariant"). Writers mutate truth
//! (artifacts, tombstones) bracketed by unique create-only markers; worker nodes
//! rebuild the materialized package view from a truth *listing*, update the
//! global name index, and only then delete exactly the markers they observed
//! (rebuild-before-delete). One package `p0`, up to two files `f0`/`f1`.
//!
//! Three writers:
//!   * an upload of (p0, f0):  intent, artifact, sidecar, commit, ACK;
//!   * a delete of that acked (p0, f0):  intent, rebuild-excluding (snapshot the
//!     truth listing, then write the view minus f0), tombstone, remove artifact,
//!     commit, ACK — the tombstone is the point of no return;
//!   * an upload of (p0, f1), which keeps the global-index arm live (the package
//!     survives a delete of f0 as long as f1 lands).
//!
//! A writer can crash between any two steps and never resumes; its markers stay.
//!
//! Each worker tick is T1 snapshot `_dirty/` + select work, T2 snapshot the
//! package truth listing, T3 write the view from that snapshot (remove it when
//! empty), T4 update the global index under a compare-and-set, T5 delete exactly
//! the marker keys captured in T1. A worker can crash at any step, dropping its
//! snapshots but leaving markers untouched.
//!
//! # What is bound to real code (the conformance anchor)
//!
//! The worker's T1 selection is NOT reimplemented: it calls the real
//! `pypiron::worker::consumable_dirty_work` over real `_dirty/<pkg>!<nonce>
//! .intent|.commit` `FileEntry` keys built from the model's logical clock, and
//! consumes exactly the keys that function returns. Marker keys are parsed back
//! with the real `pypiron::markers::parse_marker`. If the pairing / grace / stale
//! rules in `src/worker.rs` change, this model's transitions change with them —
//! that is the proof-cannot-silently-rot binding rung 2 asks for.
//!
//! # What is abstracted
//!
//! * A view is a *membership set* of filenames, not rendered HTML/JSON; the
//!   global index is a single membership bit (does p0 have >=1 live file).
//! * Sidecars are a write *step* (a crash point) but carry no truth bit — no
//!   invariant reads sidecar presence, so it is elided from state.
//! * Logical time is a u32 count of grace-periods; `AdvanceTime` bumps it by one
//!   grace so an unpaired intent crosses the staleness threshold. A logical
//!   count `c` maps to `UNIX_EPOCH + BASE + c*GRACE` seconds when building the
//!   RFC3339 timestamps the real function reads.
//! * `healed` / `recovered` are latched booleans, not unbounded counters (state
//!   must stay finite); they saturate the "counter > 0" conditions rung 2 wants.
//!
//! # Why serialized rebuilds, and the concurrent-rebuild finding
//!
//! In `src/worker.rs` the tick is gated behind `is_leader` (the bucket lease),
//! and the single-bucket package-view write is a plain list-then-write with NO
//! compare-and-set on the view (only the *global* index uses `If-Match`). So in
//! normal operation package rebuilds are serialized to one leader. The
//! CI-checked models honor that: at most one worker rebuilds at a time (the
//! second worker models failover — it takes over when the first crashes
//! mid-tick). Under that serialization the protocol converges, and the
//! worker-vs-delete-writer transient ("view briefly leads truth") is still
//! reachable because writers are never lease-gated.
//!
//! WITHOUT the lease — two workers rebuilding the same package with a staggered
//! list/write — a stale in-flight rebuild can clobber a newer correct one and
//! the view permanently disagrees with truth. That is the `sloppy leader`
//! window the design leans on the periodic audit to heal; the audit is out of
//! scope for this event-protocol model. The test
//! `concurrent_rebuild_without_lease_diverges` reproduces and regression-guards
//! that violation so the finding is documented, not lost.
//!
//! # Why quiescent-always, not stateright's `eventually`
//!
//! Convergence here is "every quiescent reachable state has views == truth."
//! Encoding it as an `always` property over the quiescent predicate is exact and
//! cheap under BFS; stateright's experimental `eventually` (a liveness modality)
//! would need fairness assumptions the marker protocol does not state, and would
//! be weaker evidence. The `reaches_quiescence` `sometimes` property proves the
//! quiescent-always properties are non-vacuous.

use std::collections::{BTreeMap, BTreeSet};

use pypiron::markers::parse_marker;
use pypiron::storage::FileEntry;
use pypiron::worker::consumable_dirty_work;
use stateright::{Checker, Model, Property};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

// Files, writer nonces, and the logical-clock mapping.
const F0: u8 = 0;
const F1: u8 = 1;
const FILES: [u8; 2] = [F0, F1];
const NONCE_UP0: u8 = 0;
const NONCE_DEL: u8 = 1;
const NONCE_UP1: u8 = 2;
/// A comfortably-positive base so every logical timestamp is a valid RFC3339
/// instant; the absolute value is irrelevant, only differences matter.
const BASE_UNIX: i64 = 1_700_000_000;
/// One grace period, in seconds. `AdvanceTime` moves the clock exactly this far,
/// so any unpaired intent written a tick earlier becomes stale in one advance.
const GRACE_SECS: i64 = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Kind {
    Intent,
    Commit,
}

/// A marker's identity is its key `_dirty/p0!<nonce>.<suffix>` — the storage
/// timestamp (`born`, the value tracked next to it) is metadata, not part of the
/// key, so `(nonce, kind)` is unique and create-only exactly like the real keys.
type MarkerKey = (u8, Kind);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum UploadPhase {
    NotStarted,
    IntentPut,
    ArtifactPut,
    SidecarPut,
    CommitPut,
    Acked,
    Crashed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum DeletePhase {
    NotStarted,
    IntentPut,
    /// 2a done: the bool is whether the truth snapshot saw f1 (so the
    /// rebuild-excluding view written at 2b will list f1).
    Snapshotted(bool),
    ViewWritten,
    Tombstoned,
    ArtifactDeleted,
    CommitPut,
    AckedDelete,
    Crashed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum WorkerPhase {
    T2,
    T3,
    T4,
    T5,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum WorkerState {
    Idle,
    Working {
        phase: WorkerPhase,
        /// Exact marker keys returned by `consumable_dirty_work` at T1.
        keys: BTreeSet<MarkerKey>,
        /// T1 work contained stale unpaired intents (a crashed writer healed on
        /// consume). Carried so T5 can latch `healed`.
        stale: bool,
        /// T2 truth snapshot {f: artifact and not tombstoned}; None until T2.
        snapshot: Option<BTreeSet<u8>>,
        /// Global membership bit as read at T1, for the T4 compare-and-set.
        observed_global: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    // Truth.
    artifact: BTreeSet<u8>,  // files whose artifact body is present
    tombstone: BTreeSet<u8>, // files with a tombstone (grows monotonically)
    // Views.
    view_present: bool,
    view: BTreeSet<u8>, // package-view membership (meaningful when view_present)
    global: bool,       // p0 present in the global name index
    // Markers: key -> born (logical clock when written; commits use 0, unread).
    markers: BTreeMap<MarkerKey, u8>,
    // Writers.
    up0: UploadPhase,
    del0: DeletePhase,
    up1: UploadPhase,
    // Workers.
    workers: Vec<WorkerState>,
    worker_crashed: Vec<bool>, // each worker may spend one crash
    // Logical time.
    now: u8,
    advances: u8,
    // Latched observations (bounded; prove the sometimes-properties non-vacuous).
    healed: bool,
    recovered: bool,
    /// Markers left behind by a worker that crashed after writing the view but
    /// before deleting them; a later tick consuming one of these sets
    /// `recovered`.
    orphaned: BTreeSet<MarkerKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Action {
    /// 0 = upload f0, 1 = delete f0, 2 = upload f1.
    WriterStep(usize),
    WorkerStep(usize),
    CrashWriter(usize),
    CrashWorker(usize),
    AdvanceTime,
}

struct EventModel {
    num_workers: usize,
    enable_f1: bool,
    advance_limit: u8,
    /// When true, workers may rebuild concurrently (no lease). Only the
    /// divergence-demonstration test sets this; the convergence models leave it
    /// false so rebuilds serialize the way the real leader lease serializes them.
    allow_concurrent_rebuild: bool,
}

// ---- pure helpers over state -------------------------------------------------

fn now_instant(now: u8) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(BASE_UNIX + (now as i64) * GRACE_SECS)
        .expect("logical clock is always a valid instant")
}

/// Build the `_dirty/` listing the real selector consumes, from model markers.
fn dirty_entries(s: &State) -> Vec<FileEntry> {
    s.markers
        .iter()
        .map(|(&(nonce, kind), &born)| {
            let suffix = match kind {
                Kind::Intent => ".intent",
                Kind::Commit => ".commit",
            };
            let last_modified = now_instant(born)
                .format(&Rfc3339)
                .expect("RFC3339 formatting is infallible for these instants");
            FileEntry {
                key: format!("_dirty/p0!{nonce}{suffix}"),
                size: 0,
                last_modified: Some(last_modified),
            }
        })
        .collect()
}

/// Parse a real marker key back to its model identity via the real parser.
fn parse_key(key: &str) -> MarkerKey {
    let entry = FileEntry {
        key: key.to_string(),
        size: 0,
        last_modified: None,
    };
    let (_pkg, marker) = parse_marker(&entry).expect("model markers always parse");
    let nonce = marker
        .nonce
        .as_deref()
        .expect("model markers carry a nonce")
        .parse::<u8>()
        .expect("model nonces are small integers");
    let kind = if marker.is_commit {
        Kind::Commit
    } else {
        Kind::Intent
    };
    (nonce, kind)
}

/// The p0 work the real selector deems consumable now, if any.
fn consumable_p0(s: &State) -> Option<(BTreeSet<MarkerKey>, bool)> {
    let entries = dirty_entries(s);
    let work = consumable_dirty_work(&entries, now_instant(s.now), Duration::seconds(GRACE_SECS));
    // One package, so at most one entry; canonicalize keys into a set.
    work.into_iter().next().map(|dw| {
        let keys: BTreeSet<MarkerKey> = dw.keys.iter().map(|k| parse_key(k)).collect();
        (keys, dw.stale_intents > 0)
    })
}

fn live_set(s: &State) -> BTreeSet<u8> {
    FILES
        .iter()
        .copied()
        .filter(|f| s.artifact.contains(f) && !s.tombstone.contains(f))
        .collect()
}

fn upload_acked(s: &State, f: u8) -> bool {
    match f {
        F0 => s.up0 == UploadPhase::Acked,
        F1 => s.up1 == UploadPhase::Acked,
        _ => false,
    }
}

fn upload_holds_uncommitted_intent(p: UploadPhase) -> bool {
    matches!(
        p,
        UploadPhase::IntentPut | UploadPhase::ArtifactPut | UploadPhase::SidecarPut
    )
}

fn delete_holds_uncommitted_intent(p: DeletePhase) -> bool {
    matches!(
        p,
        DeletePhase::IntentPut
            | DeletePhase::Snapshotted(_)
            | DeletePhase::ViewWritten
            | DeletePhase::Tombstoned
            | DeletePhase::ArtifactDeleted
    )
}

/// A writer is mid-protocol (started, not yet acked or crashed).
fn upload_mid(p: UploadPhase) -> bool {
    matches!(
        p,
        UploadPhase::IntentPut
            | UploadPhase::ArtifactPut
            | UploadPhase::SidecarPut
            | UploadPhase::CommitPut
    )
}

fn delete_mid(p: DeletePhase) -> bool {
    matches!(
        p,
        DeletePhase::IntentPut
            | DeletePhase::Snapshotted(_)
            | DeletePhase::ViewWritten
            | DeletePhase::Tombstoned
            | DeletePhase::ArtifactDeleted
            | DeletePhase::CommitPut
    )
}

/// No further protocol activity is pending: no writer mid-protocol, no markers
/// left, every worker idle. Crashed writers that left markers are NOT quiescent
/// (markers non-empty) until a tick heals and consumes them.
fn quiescent(s: &State) -> bool {
    !upload_mid(s.up0)
        && !delete_mid(s.del0)
        && !upload_mid(s.up1)
        && s.markers.is_empty()
        && s.workers.iter().all(|w| *w == WorkerState::Idle)
}

fn any_worker_working(s: &State) -> bool {
    s.workers.iter().any(|w| *w != WorkerState::Idle)
}

// ---- properties (non-capturing fn pointers; config comes from the model) -----

fn prop_durable(_m: &EventModel, s: &State) -> bool {
    FILES.iter().all(|&f| {
        if upload_acked(s, f) && !s.tombstone.contains(&f) {
            s.artifact.contains(&f)
        } else {
            true
        }
    })
}

fn prop_quiescent_views_equal_truth(_m: &EventModel, s: &State) -> bool {
    if !quiescent(s) {
        return true;
    }
    let live = live_set(s);
    let expect_present = !live.is_empty();
    s.view_present == expect_present && s.view == live && s.global == expect_present
}

fn prop_tombstone_never_resurrects(_m: &EventModel, s: &State) -> bool {
    if !quiescent(s) {
        return true;
    }
    !s.tombstone
        .iter()
        .any(|f| s.view_present && s.view.contains(f))
}

fn prop_acked_then_visible(_m: &EventModel, s: &State) -> bool {
    FILES
        .iter()
        .any(|&f| upload_acked(s, f) && s.view_present && s.view.contains(&f))
}

fn prop_stale_intent_healed(_m: &EventModel, s: &State) -> bool {
    s.healed
}

fn prop_delete_completes(_m: &EventModel, s: &State) -> bool {
    s.del0 == DeletePhase::AckedDelete
}

fn prop_worker_crash_recovers(_m: &EventModel, s: &State) -> bool {
    s.recovered
}

fn prop_view_leads_truth(_m: &EventModel, s: &State) -> bool {
    FILES
        .iter()
        .any(|&f| s.view_present && s.view.contains(&f) && !s.artifact.contains(&f))
}

fn prop_reaches_quiescence(_m: &EventModel, s: &State) -> bool {
    quiescent(s)
}

// ---- the model ---------------------------------------------------------------

impl Model for EventModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            artifact: BTreeSet::new(),
            tombstone: BTreeSet::new(),
            view_present: false,
            view: BTreeSet::new(),
            global: false,
            markers: BTreeMap::new(),
            up0: UploadPhase::NotStarted,
            del0: DeletePhase::NotStarted,
            up1: UploadPhase::NotStarted,
            workers: vec![WorkerState::Idle; self.num_workers],
            worker_crashed: vec![false; self.num_workers],
            now: 0,
            advances: 0,
            healed: false,
            recovered: false,
            orphaned: BTreeSet::new(),
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Writers. `upload_mid` or NotStarted already excludes Acked/Crashed,
        // i.e. exactly the phases with a next step.
        if upload_mid(s.up0) || s.up0 == UploadPhase::NotStarted {
            actions.push(Action::WriterStep(0));
        }
        if self.delete_step_enabled(s) {
            actions.push(Action::WriterStep(1));
        }
        if self.enable_f1 && (upload_mid(s.up1) || s.up1 == UploadPhase::NotStarted) {
            actions.push(Action::WriterStep(2));
        }

        // Writer crashes (one terminal crash per writer, only mid-protocol).
        if upload_mid(s.up0) {
            actions.push(Action::CrashWriter(0));
        }
        if delete_mid(s.del0) {
            actions.push(Action::CrashWriter(1));
        }
        if self.enable_f1 && upload_mid(s.up1) {
            actions.push(Action::CrashWriter(2));
        }

        // Workers.
        let work_available = consumable_p0(s).is_some();
        for w in 0..self.num_workers {
            match &s.workers[w] {
                WorkerState::Idle => {
                    let lease_ok = self.allow_concurrent_rebuild || !any_worker_working(s);
                    if work_available && lease_ok {
                        actions.push(Action::WorkerStep(w));
                    }
                }
                WorkerState::Working { .. } => {
                    actions.push(Action::WorkerStep(w));
                    if !s.worker_crashed[w] {
                        actions.push(Action::CrashWorker(w));
                    }
                }
            }
        }

        // Time.
        if self.advance_time_enabled(s) {
            actions.push(Action::AdvanceTime);
        }
    }

    fn next_state(&self, s: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut ns = s.clone();
        let ok = match action {
            Action::WriterStep(0) => self.step_upload(&mut ns, F0, NONCE_UP0),
            Action::WriterStep(1) => self.step_delete(&mut ns),
            Action::WriterStep(2) => self.step_upload(&mut ns, F1, NONCE_UP1),
            Action::WriterStep(_) => false,
            Action::WorkerStep(w) => self.step_worker(&mut ns, w),
            Action::CrashWriter(i) => Self::crash_writer(&mut ns, i),
            Action::CrashWorker(w) => Self::crash_worker(&mut ns, w),
            Action::AdvanceTime => {
                if !self.advance_time_enabled(s) {
                    false
                } else {
                    ns.now += 1;
                    ns.advances += 1;
                    true
                }
            }
        };
        if !ok {
            return None;
        }
        // Tombstones never shrink: the point-of-no-return must be monotonic.
        debug_assert!(
            ns.tombstone.is_superset(&s.tombstone),
            "tombstone set shrank: {:?} -> {:?}",
            s.tombstone,
            ns.tombstone
        );
        Some(ns)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("acked_upload_is_durable", prop_durable),
            Property::<Self>::always(
                "quiescent_views_equal_truth",
                prop_quiescent_views_equal_truth,
            ),
            Property::<Self>::always(
                "tombstone_never_resurrects",
                prop_tombstone_never_resurrects,
            ),
            Property::<Self>::sometimes("acked_then_visible", prop_acked_then_visible),
            Property::<Self>::sometimes("stale_intent_healed", prop_stale_intent_healed),
            Property::<Self>::sometimes("delete_completes", prop_delete_completes),
            Property::<Self>::sometimes(
                "worker_crash_mid_tick_recovers",
                prop_worker_crash_recovers,
            ),
            Property::<Self>::sometimes("view_briefly_leads_truth", prop_view_leads_truth),
            // Non-vacuity guard for the three quiescent-always properties.
            Property::<Self>::sometimes("reaches_quiescence", prop_reaches_quiescence),
        ]
    }
}

impl EventModel {
    fn delete_step_enabled(&self, s: &State) -> bool {
        match s.del0 {
            DeletePhase::NotStarted => s.up0 == UploadPhase::Acked,
            DeletePhase::IntentPut
            | DeletePhase::Snapshotted(_)
            | DeletePhase::ViewWritten
            | DeletePhase::Tombstoned
            | DeletePhase::ArtifactDeleted
            | DeletePhase::CommitPut => true,
            DeletePhase::AckedDelete | DeletePhase::Crashed => false,
        }
    }

    fn advance_time_enabled(&self, s: &State) -> bool {
        // Grace > writer execution time: the clock only crosses the staleness
        // threshold once every in-flight (non-crashed) writer has settled, so a
        // stale unpaired intent provably belongs to a crashed writer. The
        // audit — out of scope here — covers a writer that genuinely exceeds
        // grace.
        // A disabled f1 writer stays NotStarted, so its "holds intent" is always
        // false — no need to special-case `enable_f1` here.
        s.advances < self.advance_limit
            && !upload_holds_uncommitted_intent(s.up0)
            && !delete_holds_uncommitted_intent(s.del0)
            && !upload_holds_uncommitted_intent(s.up1)
    }

    fn step_upload(&self, s: &mut State, file: u8, nonce: u8) -> bool {
        let phase = if file == F0 { s.up0 } else { s.up1 };
        let next = match phase {
            UploadPhase::NotStarted => {
                s.markers.insert((nonce, Kind::Intent), s.now);
                UploadPhase::IntentPut
            }
            UploadPhase::IntentPut => {
                s.artifact.insert(file);
                UploadPhase::ArtifactPut
            }
            UploadPhase::ArtifactPut => UploadPhase::SidecarPut, // sidecar: crash point only
            UploadPhase::SidecarPut => {
                s.markers.insert((nonce, Kind::Commit), 0);
                UploadPhase::CommitPut
            }
            UploadPhase::CommitPut => UploadPhase::Acked,
            UploadPhase::Acked | UploadPhase::Crashed => return false,
        };
        if file == F0 {
            s.up0 = next;
        } else {
            s.up1 = next;
        }
        true
    }

    fn step_delete(&self, s: &mut State) -> bool {
        s.del0 = match s.del0 {
            DeletePhase::NotStarted => {
                if s.up0 != UploadPhase::Acked {
                    return false;
                }
                s.markers.insert((NONCE_DEL, Kind::Intent), s.now);
                DeletePhase::IntentPut
            }
            DeletePhase::IntentPut => {
                // 2a: snapshot the truth listing; the rebuild-excluding view
                // (written at 2b) is {live} minus the deleted f0.
                let snap_f1 = s.artifact.contains(&F1) && !s.tombstone.contains(&F1);
                DeletePhase::Snapshotted(snap_f1)
            }
            DeletePhase::Snapshotted(snap_f1) => {
                // 2b: write the package view from the 2a snapshot, minus f0.
                if snap_f1 {
                    s.view_present = true;
                    s.view = BTreeSet::from([F1]);
                } else {
                    s.view_present = false;
                    s.view = BTreeSet::new();
                }
                DeletePhase::ViewWritten
            }
            DeletePhase::ViewWritten => {
                s.tombstone.insert(F0);
                DeletePhase::Tombstoned
            }
            DeletePhase::Tombstoned => {
                s.artifact.remove(&F0);
                DeletePhase::ArtifactDeleted
            }
            DeletePhase::ArtifactDeleted => {
                s.markers.insert((NONCE_DEL, Kind::Commit), 0);
                DeletePhase::CommitPut
            }
            DeletePhase::CommitPut => DeletePhase::AckedDelete,
            DeletePhase::AckedDelete | DeletePhase::Crashed => return false,
        };
        true
    }

    fn step_worker(&self, s: &mut State, w: usize) -> bool {
        match s.workers[w].clone() {
            WorkerState::Idle => {
                if !self.allow_concurrent_rebuild && any_worker_working(s) {
                    return false; // lease held by another worker
                }
                let Some((keys, stale)) = consumable_p0(s) else {
                    return false;
                };
                s.workers[w] = WorkerState::Working {
                    phase: WorkerPhase::T2,
                    keys,
                    stale,
                    snapshot: None,
                    observed_global: s.global,
                };
            }
            WorkerState::Working {
                phase,
                keys,
                stale,
                snapshot,
                observed_global,
            } => match phase {
                WorkerPhase::T2 => {
                    s.workers[w] = WorkerState::Working {
                        phase: WorkerPhase::T3,
                        keys,
                        stale,
                        snapshot: Some(live_set(s)),
                        observed_global,
                    };
                }
                WorkerPhase::T3 => {
                    let snap = snapshot.clone().expect("snapshot taken at T2");
                    if snap.is_empty() {
                        s.view_present = false;
                        s.view = BTreeSet::new();
                    } else {
                        s.view_present = true;
                        s.view = snap;
                    }
                    s.workers[w] = WorkerState::Working {
                        phase: WorkerPhase::T4,
                        keys,
                        stale,
                        snapshot,
                        observed_global,
                    };
                }
                WorkerPhase::T4 => {
                    let live = !snapshot.as_ref().expect("snapshot taken at T2").is_empty();
                    // Global index is written under a compare-and-set (the real
                    // If-Match). Serialized leaders never contend, so the reload
                    // branch is dead here; it is exercised by the concurrent
                    // divergence test.
                    if s.global == observed_global {
                        s.global = live;
                        s.workers[w] = WorkerState::Working {
                            phase: WorkerPhase::T5,
                            keys,
                            stale,
                            snapshot,
                            observed_global,
                        };
                    } else {
                        s.workers[w] = WorkerState::Working {
                            phase: WorkerPhase::T4,
                            keys,
                            stale,
                            snapshot,
                            observed_global: s.global,
                        };
                    }
                }
                WorkerPhase::T5 => {
                    // Delete exactly the observed keys; markers written after T1
                    // have different keys and survive.
                    for key in &keys {
                        s.markers.remove(key);
                    }
                    if stale {
                        s.healed = true;
                    }
                    let recovered: Vec<MarkerKey> = keys
                        .iter()
                        .filter(|k| s.orphaned.contains(*k))
                        .copied()
                        .collect();
                    if !recovered.is_empty() {
                        s.recovered = true;
                        for k in recovered {
                            s.orphaned.remove(&k);
                        }
                    }
                    s.workers[w] = WorkerState::Idle;
                }
            },
        }
        true
    }

    fn crash_writer(s: &mut State, i: usize) -> bool {
        match i {
            0 if upload_mid(s.up0) => {
                s.up0 = UploadPhase::Crashed;
                true
            }
            1 if delete_mid(s.del0) => {
                s.del0 = DeletePhase::Crashed;
                true
            }
            2 if upload_mid(s.up1) => {
                s.up1 = UploadPhase::Crashed;
                true
            }
            _ => false,
        }
    }

    fn crash_worker(s: &mut State, w: usize) -> bool {
        if s.worker_crashed[w] {
            return false;
        }
        let WorkerState::Working { phase, keys, .. } = s.workers[w].clone() else {
            return false;
        };
        // A crash after the view write (T4/T5) but before the marker delete
        // leaves those markers for a later tick — that later consume is the
        // "recovers" event.
        if matches!(phase, WorkerPhase::T4 | WorkerPhase::T5) {
            s.orphaned.extend(keys);
        }
        s.workers[w] = WorkerState::Idle;
        s.worker_crashed[w] = true;
        true
    }
}

// ---- checked instances -------------------------------------------------------

fn threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Small, fully exhaustive (no depth bound): one worker, upload + delete of f0.
/// This is the exact-convergence core — every reachable state is visited.
#[test]
fn model_event_protocol_small_exhaustive() {
    let model = EventModel {
        num_workers: 1,
        enable_f1: false,
        advance_limit: 2,
        allow_concurrent_rebuild: false,
    };
    let checker = model.checker().threads(threads()).spawn_bfs().join();
    eprintln!(
        "[small_exhaustive] unique states = {}, max depth = {}",
        checker.unique_state_count(),
        checker.max_depth()
    );
    checker.assert_properties();
}

/// CI instance: two workers (the second models failover), all three writers,
/// two time advances. Kept exhaustive so every sometimes-property is reachable;
/// tune `target_max_depth` here if the state space ever outgrows the CI budget.
#[test]
fn model_event_protocol() {
    let model = EventModel {
        num_workers: 2,
        enable_f1: true,
        advance_limit: 2,
        allow_concurrent_rebuild: false,
    };
    let checker = model.checker().threads(threads()).spawn_bfs().join();
    eprintln!(
        "[event_protocol] unique states = {}, max depth = {}",
        checker.unique_state_count(),
        checker.max_depth()
    );
    checker.assert_properties();
}

/// Deep nightly instance: three time advances, larger interleaving budget.
#[test]
#[ignore = "deep model check; runs in the nightly job, not on every change"]
fn model_event_protocol_deep() {
    let model = EventModel {
        num_workers: 2,
        enable_f1: true,
        advance_limit: 3,
        allow_concurrent_rebuild: false,
    };
    let checker = model.checker().threads(threads()).spawn_bfs().join();
    eprintln!(
        "[deep] unique states = {}, max depth = {}",
        checker.unique_state_count(),
        checker.max_depth()
    );
    checker.assert_properties();
}

/// Documents (and regression-guards) the finding: WITHOUT the leader lease
/// serializing package rebuilds, two workers with a staggered list/write can
/// leave a package view permanently disagreeing with truth — a resurrected,
/// tombstoned file. The periodic audit heals this in production; the event
/// protocol alone does not. We assert the violation EXISTS; if serialization is
/// ever modeled here (or a view compare-and-set is added to the real rebuild),
/// this test flags that the known gap changed.
#[test]
fn concurrent_rebuild_without_lease_diverges() {
    let model = EventModel {
        num_workers: 2,
        enable_f1: false,
        advance_limit: 0,
        allow_concurrent_rebuild: true,
    };
    let checker = model.checker().threads(threads()).spawn_bfs().join();
    let discovery = checker.discovery("quiescent_views_equal_truth");
    assert!(
        discovery.is_some(),
        "expected concurrent staggered rebuilds to diverge; did the model gain \
         lease serialization or the real rebuild gain a view compare-and-set?"
    );
    let actions = discovery.expect("checked above").into_actions();
    eprintln!(
        "[concurrent_divergence] counterexample ({} steps): {:?}",
        actions.len(),
        actions
    );
}
