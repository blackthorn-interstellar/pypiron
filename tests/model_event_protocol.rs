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
//! compare-and-set on the view (only the *global* index uses `If-Match`). The
//! CI-checked models model the *common* case that gate produces: at most one
//! worker rebuilds at a time (the second worker models failover — it takes over
//! when the first crashes mid-tick). Under that serialization the protocol
//! converges, and the worker-vs-delete-writer transient ("view briefly leads
//! truth") is still reachable because writers are never lease-gated.
//!
//! **The gate is not a serializer, and nothing here should be read as claiming
//! it is.** `src/lease.rs` is a TTL + heartbeat with no fencing token by
//! design; `is_leader()` is a point-in-time read that
//! `rebuild_package_indexes` never revalidates, so a rebuild outliving the TTL
//! runs beside its successor's. Two further rebuild paths take no lease at all:
//! the leader's audit task, which `run_worker` spawns off the tick loop and
//! never mutexes against the tick, and `delete_record`, which rebuilds the
//! package view straight from any node's request handler. Concurrent rebuilds
//! of one package are a production state, not a hypothetical.
//!
//! `allow_concurrent_rebuild` is therefore the configuration production RUNS,
//! not one it forbids — it is the second CI-checked config precisely because
//! the first cannot reach it. Two workers rebuilding the same package with a
//! staggered list/write let a stale in-flight rebuild clobber a newer correct
//! one, and the view permanently disagrees with truth. That is the `sloppy
//! leader` window dev/DESIGN.md budgets for by name and leans on the periodic
//! audit to heal; the audit is out of scope for this event-protocol model. The
//! test `concurrent_rebuild_without_lease_diverges` reproduces and
//! regression-guards that violation so the finding is documented, not lost —
//! and it is the exhaustive coverage the VOPR's class-3 classifier arm rests
//! on, since the VOPR's `tick_lock` cannot stage the interleaving.
//!
//! # Why quiescent-always, not stateright's `eventually`
//!
//! Convergence here is "every quiescent reachable state has views == truth."
//! Encoding it as an `always` property over the quiescent predicate is exact and
//! cheap under BFS; stateright's experimental `eventually` (a liveness modality)
//! would need fairness assumptions the marker protocol does not state, and would
//! be weaker evidence. The three `reaches_quiescence*` `sometimes` properties
//! prove the quiescent-always properties are non-vacuous — each pinned to
//! content (a settled view, a completed delete, a healed intent), because a
//! bare `quiescent(s)` witness is satisfied by the initial state and guards
//! nothing.

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

/// Non-vacuity guard for the three quiescent-`always` properties — and it has
/// to be pinned to *content*. Bare `quiescent(s)` is satisfied by the INITIAL
/// state: every phase starts `NotStarted` (neither `upload_mid` nor
/// `delete_mid`), no markers exist, and every worker is `Idle`. So the witness
/// fired at depth 0 and proved nothing about the protocol ever draining after
/// work happened. Requiring a settled view means a writer ran, a worker
/// rebuilt, and the markers were consumed.
fn prop_reaches_quiescence(_m: &EventModel, s: &State) -> bool {
    quiescent(s) && !s.view.is_empty()
}

/// Quiescence reached with the delete run to completion: the other end of the
/// protocol, where truth went back to empty and the views were removed rather
/// than written.
fn prop_reaches_quiescence_after_delete(_m: &EventModel, s: &State) -> bool {
    quiescent(s) && s.tombstone.contains(&F0)
}

/// Quiescence reached after the worker healed a crashed writer's stale intent —
/// the marker path that only runs once the clock crosses the grace.
fn prop_reaches_quiescence_after_heal(_m: &EventModel, s: &State) -> bool {
    quiescent(s) && s.healed
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
            // Non-vacuity guards for the three quiescent-always properties.
            // Three of them, each pinning quiescence to a different piece of
            // content, because one bare `quiescent(s)` was true at depth 0 and
            // so guarded nothing.
            Property::<Self>::sometimes("reaches_quiescence", prop_reaches_quiescence),
            Property::<Self>::sometimes(
                "reaches_quiescence_after_delete",
                prop_reaches_quiescence_after_delete,
            ),
            Property::<Self>::sometimes(
                "reaches_quiescence_after_heal",
                prop_reaches_quiescence_after_heal,
            ),
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

/// Documents (and regression-guards) the finding: with two rebuilds of one
/// package in flight — which the sloppy lease permits, the leader's own audit
/// task does unconditionally, and `delete_record` does from any node — a
/// staggered list/write can leave a package view permanently disagreeing with
/// truth: a resurrected, tombstoned file. The periodic audit heals this in
/// production; the event protocol alone does not. We assert the violation
/// EXISTS; if serialization is ever modeled here (or a view compare-and-set is
/// added to the real rebuild), this test flags that the known gap changed.
///
/// This is the sole exhaustive coverage of the state the VOPR classifies as
/// class 3 (CONCURRENT-RACE): `examples/vopr.rs` serializes every rebuild
/// behind a fleet-wide `tick_lock`, so it cannot stage the interleaving and
/// its `--break race` can only plant the history. See dev/TESTING.md.
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

// ---- visualizer state-graph dump --------------------------------------------
//
// Writes this model's state graph as one JSON object for the run visualizer
// (`dev/scripts/viz/player.html`). Output goes wherever `PYPIRON_VIZ_GRAPH`
// points; with the variable unset the test returns immediately.
//
// The env gate is load-bearing, not decorative: `.github/workflows/
// simulation.yml`'s `model-deep` job runs this test target with `-- --ignored`,
// so `#[ignore]` alone would run this in the nightly.
//
// It draws a REDUCED config. The configs CI checks are 1,497,544 states (merge
// gate, `model_event_protocol`) and 3,354,376 (nightly, `..._deep`); at the
// ~1.5 KB/node this dump produces those are gigabytes of JSON and cannot be
// graphed. They are reported as counts, and the reduction is stated in the
// output so the picture cannot overclaim.
mod viz_graph {
    use std::collections::{HashMap, VecDeque};

    use serde_json::{json, Value};
    use stateright::{Checker, Expectation, HasDiscoveries, Model, Property};

    use super::{consumable_p0, live_set, quiescent, EventModel, State, WorkerState};

    /// States checked exhaustively by `model_event_protocol` (the merge gate).
    const CI_STATES: u64 = 1_497_544;
    /// States checked exhaustively by `model_event_protocol_deep` (the nightly).
    const CI_STATES_NIGHTLY: u64 = 3_354_376;
    /// Above this many nodes the per-node `raw` debug rendering is dropped; the
    /// drawn config is far below it, the constant is the guard for anyone who
    /// widens the config later.
    const RAW_LIMIT: usize = 2_000;

    /// The config drawn in full: one worker, one filename, no clock advance.
    /// Deliberately the smallest real `EventModel` — see `caveats` in the output.
    fn drawn_model() -> EventModel {
        EventModel {
            num_workers: 1,
            enable_f1: false,
            advance_limit: 0,
            allow_concurrent_rebuild: false,
        }
    }

    /// The config `concurrent_rebuild_without_lease_diverges` checks. Its
    /// counterexample is exported as a path, and it is NOT the drawn config.
    fn diverging_model() -> EventModel {
        EventModel {
            num_workers: 2,
            enable_f1: false,
            advance_limit: 0,
            allow_concurrent_rebuild: true,
        }
    }

    fn config_json(m: &EventModel) -> Value {
        json!({
            "num_workers": m.num_workers,
            "enable_f1": m.enable_f1,
            "advance_limit": m.advance_limit,
            "allow_concurrent_rebuild": m.allow_concurrent_rebuild,
        })
    }

    fn expectation_str(e: &Expectation) -> &'static str {
        match e {
            Expectation::Always => "always",
            Expectation::Eventually => "eventually",
            Expectation::Sometimes => "sometimes",
        }
    }

    /// Thousands separators, for the human-facing strings only.
    fn commas(n: u64) -> String {
        let digits = n.to_string();
        let mut out = String::with_capacity(digits.len() + digits.len() / 3);
        for (i, c) in digits.chars().enumerate() {
            if i > 0 && (digits.len() - i).is_multiple_of(3) {
                out.push(',');
            }
            out.push(c);
        }
        out
    }

    fn marker_list<'a>(keys: impl Iterator<Item = &'a super::MarkerKey>) -> Vec<Value> {
        keys.map(|&(nonce, kind)| json!({ "nonce": nonce, "kind": format!("{kind:?}") }))
            .collect()
    }

    /// Views agree with truth right now, ignoring quiescence. This is the
    /// unconditional core of `prop_quiescent_views_equal_truth`; the property
    /// itself is vacuously true mid-flight, which is useless as a per-node badge.
    fn views_match_truth(s: &State) -> bool {
        let live = live_set(s);
        let expect_present = !live.is_empty();
        s.view_present == expect_present && s.view == live && s.global == expect_present
    }

    /// Hand-written projection of `State` for the player. Every node also carries
    /// `raw` (`{state:#?}`) precisely because this is a hand projection: if a
    /// field is added to `State` or `WorkerState` and this misses it, the raw
    /// rendering still carries it instead of the picture silently going stale.
    fn project(s: &State) -> Value {
        let markers: Vec<Value> = s
            .markers
            .iter()
            .map(|(&(nonce, kind), &born)| {
                json!({ "nonce": nonce, "kind": format!("{kind:?}"), "born": born })
            })
            .collect();
        let workers: Vec<Value> = s
            .workers
            .iter()
            .zip(&s.worker_crashed)
            .map(|(w, &crash_spent)| match w {
                WorkerState::Idle => json!({
                    "state": "idle",
                    "phase": Value::Null,
                    "keys": [],
                    "stale": false,
                    "snapshot": Value::Null,
                    "observed_global": Value::Null,
                    "crash_spent": crash_spent,
                }),
                WorkerState::Working {
                    phase,
                    keys,
                    stale,
                    snapshot,
                    observed_global,
                } => json!({
                    "state": "working",
                    "phase": format!("{phase:?}"),
                    "keys": marker_list(keys.iter()),
                    "stale": stale,
                    "snapshot": snapshot.as_ref().map(|v| v.iter().copied().collect::<Vec<u8>>()),
                    "observed_global": observed_global,
                    "crash_spent": crash_spent,
                }),
            })
            .collect();
        json!({
            "truth": {
                "artifact": s.artifact.iter().copied().collect::<Vec<u8>>(),
                "tombstone": s.tombstone.iter().copied().collect::<Vec<u8>>(),
            },
            "views": {
                "view_present": s.view_present,
                "view": s.view.iter().copied().collect::<Vec<u8>>(),
                "global": s.global,
            },
            "markers": markers,
            "writers": {
                "up0": format!("{:?}", s.up0),
                "del0": format!("{:?}", s.del0),
                "up1": format!("{:?}", s.up1),
            },
            "workers": workers,
            "clock": { "now": s.now, "advances": s.advances },
            "latches": {
                "healed": s.healed,
                "recovered": s.recovered,
                "orphaned": marker_list(s.orphaned.iter()),
            },
        })
    }

    /// Own breadth-first enumeration over the `Model` trait only. `next_steps`
    /// hands back `(action, state)` pairs, so every edge is observed — including
    /// the joins where independent interleavings reconverge, which a stateright
    /// `StateRecorder` visitor would miss (it only sees a spanning tree).
    /// Single-threaded, so `depth` is a true shortest-path depth.
    struct Dump<'m> {
        model: &'m EventModel,
        props: &'m [Property<EventModel>],
        ids: HashMap<State, usize>,
        nodes: Vec<Value>,
        edges: Vec<Value>,
        tree_edges: usize,
        depth: usize,
    }

    impl<'m> Dump<'m> {
        fn new(model: &'m EventModel, props: &'m [Property<EventModel>]) -> Self {
            Self {
                model,
                props,
                ids: HashMap::new(),
                nodes: Vec::new(),
                edges: Vec::new(),
                tree_edges: 0,
                depth: 0,
            }
        }

        /// Returns `(id, first_seen)`.
        fn intern(&mut self, s: &State, depth: usize) -> (usize, bool) {
            if let Some(&id) = self.ids.get(s) {
                return (id, false);
            }
            let id = self.nodes.len();
            let model = self.model;
            let truthy: Vec<&'static str> = self
                .props
                .iter()
                .filter(|p| (p.condition)(model, s))
                .map(|p| p.name)
                .collect();
            self.ids.insert(s.clone(), id);
            self.nodes.push(json!({
                "id": id,
                "depth": depth,
                "state": project(s),
                "raw": format!("{s:#?}"),
                "props": truthy,
                "flags": {
                    "quiescent": quiescent(s),
                    "views_match_truth": views_match_truth(s),
                    "work_available": consumable_p0(s).is_some(),
                },
            }));
            self.depth = self.depth.max(depth);
            (id, true)
        }

        fn run(&mut self) {
            let model = self.model;
            let mut queue: VecDeque<(State, usize, usize)> = VecDeque::new();
            for init in model.init_states() {
                let (id, fresh) = self.intern(&init, 0);
                if fresh {
                    queue.push_back((init, id, 0));
                }
            }
            while let Some((state, from, depth)) = queue.pop_front() {
                for (action, next) in model.next_steps(&state) {
                    let (to, fresh) = self.intern(&next, depth + 1);
                    self.edges.push(json!({
                        "from": from,
                        "to": to,
                        "action": model.format_action(&action),
                        "verdict": Value::Null,
                        "tree": fresh,
                    }));
                    if fresh {
                        self.tree_edges += 1;
                        queue.push_back((next, to, depth + 1));
                    }
                }
            }
        }
    }

    fn short_commit() -> String {
        std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string())
    }

    #[test]
    #[ignore = "visualizer: writes a state-graph JSON when PYPIRON_VIZ_GRAPH=<path>"]
    fn dump_state_graph() {
        let Ok(out) = std::env::var("PYPIRON_VIZ_GRAPH") else {
            return;
        };

        // 1. Enumerate the drawn config ourselves.
        let model = drawn_model();
        let props = model.properties();
        let mut dump = Dump::new(&model, &props);
        dump.run();

        // 2. The same config through the real checker: it supplies the named
        //    paths, and its unique-state count is what proves the picture is the
        //    space the checker verified.
        let checker = drawn_model().checker().threads(1).spawn_bfs().join();
        assert_eq!(
            dump.nodes.len(),
            checker.unique_state_count(),
            "the dumped graph is not the state space the checker verifies"
        );
        let discoveries = checker.discoveries();

        // Which properties the drawn config actually settles. `always` with a
        // discovery = violated; `sometimes` without one = never reached here.
        let prop_json: Vec<Value> = props
            .iter()
            .map(|p| {
                let found = discoveries.contains_key(p.name);
                let status = match &p.expectation {
                    Expectation::Sometimes if found => "reached",
                    Expectation::Sometimes => "not-reached",
                    // An `always`/`eventually` discovery is a counterexample.
                    _ if found => "violated",
                    _ => "held",
                };
                json!({ "name": p.name, "kind": expectation_str(&p.expectation), "status": status })
            })
            .collect();

        // 3. Named paths that walk THIS graph.
        let mut found: Vec<_> = discoveries.into_iter().collect();
        found.sort_unstable_by_key(|(name, _)| *name);
        let mut paths: Vec<Value> = found
            .into_iter()
            .map(|(name, path)| {
                let kind = props
                    .iter()
                    .find(|p| p.name == name)
                    .map_or("discovery", |p| match p.expectation {
                        Expectation::Sometimes => "discovery",
                        _ => "counterexample",
                    });
                let steps: Vec<Value> = path
                    .into_vec()
                    .into_iter()
                    .map(|(s, a)| {
                        json!({
                            "id": dump.ids.get(&s).copied(),
                            "action": a.map(|a| model.format_action(&a)),
                            "verdict": Value::Null,
                        })
                    })
                    .collect();
                json!({ "name": name, "kind": kind, "in_graph": true, "steps": steps })
            })
            .collect();

        // 4. The best story this model has, and it belongs to a DIFFERENT config:
        //    without the leader lease, two staggered rebuilds diverge. Its states
        //    are not nodes of the drawn graph, so every step's `id` is null and
        //    the step carries its own projection instead. `in_graph:false` and the
        //    caveat below say so.
        let diverging = diverging_model()
            .checker()
            .threads(1)
            .finish_when(HasDiscoveries::AnyFailures)
            .spawn_bfs()
            .join();
        let cx_len = if let Some(path) = diverging.discovery("quiescent_views_equal_truth") {
            let steps = path.into_vec();
            let last_raw = steps.last().map(|(s, _)| format!("{s:#?}"));
            let n = steps.len();
            let steps: Vec<Value> = steps
                .into_iter()
                .map(|(s, a)| {
                    json!({
                        "id": Value::Null,
                        "action": a.map(|a| diverging.model().format_action(&a)),
                        "verdict": Value::Null,
                        "state": project(&s),
                    })
                })
                .collect();
            paths.push(json!({
                "name": "concurrent_rebuild_without_lease_diverges",
                "kind": "counterexample",
                "in_graph": false,
                "config": config_json(diverging.model()),
                "note": "A counterexample from a different, larger config (2 workers, \
                         no leader lease). It is not a walk through the drawn graph, so \
                         each step carries its own state instead of a node id.",
                "violates": "quiescent_views_equal_truth",
                "final_raw": last_raw,
                "steps": steps,
            }));
            n
        } else {
            0
        };

        // 5. Drop `raw` if a future widening pushes the graph past the budget.
        if dump.nodes.len() > RAW_LIMIT {
            for node in &mut dump.nodes {
                if let Some(obj) = node.as_object_mut() {
                    obj.remove("raw");
                }
            }
        }

        let nodes = dump.nodes.len();
        let edges = dump.edges.len();
        let joins = edges - dump.tree_edges;
        let tree_edges = dump.tree_edges;
        let depth = dump.depth;
        let path_count = paths.len();
        let ci = commas(CI_STATES);
        let ci_nightly = commas(CI_STATES_NIGHTLY);
        let doc = json!({
            "kind": "graph",
            "model": "event-protocol",
            "config": config_json(&model),
            "generated_by": format!(
                "PYPIRON_VIZ_GRAPH={out} cargo test --release --test model_event_protocol \
                 -- --ignored --nocapture dump_state_graph"
            ),
            "commit": short_commit(),
            "title": "The _dirty/ marker protocol, drawn in full at a reduced size",
            "narration": format!(
                "Every interleaving of one upload, one delete and one worker tick over the \
                 _dirty/ intent-and-commit markers — {nodes} states, {edges} transitions, \
                 {joins} of them joins where independent orders reconverge — beside the \
                 {ci}-state configuration CI checks on every merge."
            ),
            "counts": {
                "nodes": nodes,
                "edges": edges,
                "tree_edges": tree_edges,
                "join_edges": joins,
                "depth": depth,
                "checker_unique_states": checker.unique_state_count(),
                "ci_states": CI_STATES,
                "ci_states_nightly": CI_STATES_NIGHTLY,
                "ci_depth_note": "depth is this dump's own single-threaded BFS shortest-path \
                                  depth; checker.max_depth() is not the diameter, and the \
                                  CI configs run .threads(available_parallelism()) so their \
                                  reported depths are machine-dependent",
                "counterexample_steps": cx_len,
            },
            "reduction": {
                "drawn": config_json(&model),
                "ci_merge_gate": config_json(&EventModel {
                    num_workers: 2,
                    enable_f1: true,
                    advance_limit: 2,
                    allow_concurrent_rebuild: false,
                }),
                "ci_nightly": config_json(&EventModel {
                    num_workers: 2,
                    enable_f1: true,
                    advance_limit: 3,
                    allow_concurrent_rebuild: false,
                }),
                "drops": [
                    "the stale_intent_healed reachability probe (it needs advance_limit >= 1, \
                     so no marker here ever crosses the grace threshold)",
                    "the f1 upload writer, and with it the global-index arm that keeps the \
                     package alive across a delete of f0",
                    "the second worker, which is what models leader failover mid-tick",
                ],
            },
            "props": prop_json,
            "nodes": dump.nodes,
            "edges": dump.edges,
            "paths": paths,
            "caveats": [
                format!(
                    "Drawn: a reduced config (1 worker, one filename, no clock advance) \
                     rendered in full at {nodes} states. The configs CI checks exhaustively \
                     are {ci} and {ci_nightly} states — too large to draw (gigabytes of \
                     JSON, millions of edges), so they are reported as counts."
                ),
                "The reduction drops the stale_intent_healed reachability probe, the f1 \
                 upload writer, and the second (failover) worker. Every property's real \
                 status in the drawn config is in props[].status.".to_string(),
                "The concurrent_rebuild_without_lease_diverges path is a counterexample from \
                 a different config (2 workers, no leader lease) and is not a walk through \
                 this graph: in_graph is false and its step ids are null.".to_string(),
                "Depth comes from this dump's single-threaded BFS, not from \
                 checker.max_depth(); the CI configs' published depths are machine-dependent \
                 because they run one BFS thread per core.".to_string(),
            ],
        });

        let bytes = serde_json::to_vec(&doc).expect("graph dump serializes");
        eprintln!(
            "[viz_graph] {nodes} nodes, {edges} edges ({tree_edges} tree / {joins} join), \
             depth {depth}, {path_count} paths (counterexample {cx_len} steps), {} bytes -> {out}",
            bytes.len(),
        );
        std::fs::write(&out, &bytes).expect("graph dump is writable");
    }
}
