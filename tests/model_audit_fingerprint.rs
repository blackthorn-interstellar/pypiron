//! Machine-checked proof that the periodic audit never certifies a package
//! view it may have clobbered, driven by the `stateright` model checker.
//!
//! # What is modeled
//!
//! The audit's fingerprint skip gate racing the marker-driven tick on one
//! leader (`run_worker` spawns `audit` on its own task and runs `tick` inline,
//! with nothing mutexing the two). One package `p0`, three writers:
//!
//!   * upload f0 and upload f1: land the artifact, then the commit marker, ACK;
//!   * delete f0 (once its upload acked): tombstone + remove the artifact, then
//!     the commit marker, ACK.
//!
//! A writer may crash between its truth write and its marker (a peer that died
//! without committing — the case the audit exists to heal).
//!
//! The tick is list markers → list truth → write the view from that listing →
//! delete exactly the markers it listed. The audit pass is the per-package path
//! of `audit_shard`: walk (truth and view listings, joined) → skip when the
//! stored fingerprint matches → else rebuild (list truth, unconditional view
//! PUT) → post-rebuild listings (joined) → certify → rewrite the fingerprint
//! shard → re-arm a dirty marker for an uncertified package (the audit's tail).
//!
//! # The property
//!
//! At quiescence (no writer between truth and marker, no markers, tick and
//! audit idle), the view one more audit leaves behind equals a fresh render of
//! truth. The skip gate trusts a stored fingerprint forever, so this is exactly
//! "a fingerprint the audit stored never vouches for a stale view".
//!
//! # Two audits
//!
//! * [`Variant::Old`]: the audit before 4b56403 — fingerprint whatever the
//!   post-rebuild listing shows, no certification, no re-arm. The checker must
//!   find a counterexample (`old_audit_blesses_a_stale_view`, the kill-proof):
//!   the audit lists truth, an upload lands, the tick renders and consumes it,
//!   the audit's stale PUT lands last, and fp(new truth, stale view) then
//!   matches every later walk.
//! * [`Variant::New`]: the shipped audit. Certification is NOT reimplemented:
//!   the model builds real `ObjectMeta` listings and calls the real
//!   `pypiron::worker::audit_certificate` and `pypiron::worker::fingerprint`,
//!   so a change to the certificate rule changes this model's transitions.
//!
//! # What is abstracted
//!
//! * The view pair (`index.html` + `index.json`) is written and removed as one
//!   object; both keys carry the same bytes. ETags are content-derived (S3
//!   single-part semantics), the adversarial choice: equal bytes list equal.
//! * Truth is a membership set; each truth write is a key never written before,
//!   so truth listings never repeat (the code's own no-ABA assumption).
//! * The tick's marker selection is a plain "every marker present" — commit
//!   markers only. The intent/commit pairing is tests/model_event_protocol.rs's
//!   subject, not this model's.
//! * Transparency is off (or its checkpoint lands at once): a certified
//!   fingerprint is stored directly rather than via `pending_fps`.
//! * The global name index is not modeled; the audit's dead re-proof is
//!   covered by in-crate tests in src/worker.rs.
//! * `put_if_changed` (read, compare, PUT) is one write: with content ETags,
//!   skipping an identical write and performing it are indistinguishable.

use std::collections::BTreeSet;

use pypiron::app::{PACKAGES_PREFIX, SIMPLE_PREFIX};
use pypiron::storage::ObjectMeta;
use pypiron::worker::{audit_certificate, fingerprint};
use stateright::{Checker, Model, Property};

const PKG: &str = "p0";
const F0: u8 = 0;
const F1: u8 = 1;
const UP0: usize = 0;
const UP1: usize = 1;
const DEL0: usize = 2;
/// Marker ids: a writer's commit marker is its writer index; the audit's
/// re-arm in pass `n` is `REARM_BASE + n`, so every marker key is unique.
const REARM_BASE: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Variant {
    /// Pre-4b56403: store the post-rebuild listing's fingerprint, always.
    Old,
    /// Shipped: `audit_certificate`, and re-arm the package when it refuses.
    New,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Writer {
    Idle,
    TruthLanded,
    Acked,
    Crashed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Truth {
    artifacts: BTreeSet<u8>,
    tombstones: BTreeSet<u8>,
}

/// The package view pair: `None` = removed (a dead package), `Some(files)` =
/// the files it lists.
type View = Option<BTreeSet<u8>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Tick {
    Idle,
    Listed(BTreeSet<u8>),
    Snapped(BTreeSet<u8>, View),
    Written(BTreeSet<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Audit {
    Idle,
    /// `audit_shard`'s truth and view listings (`try_join`: either lands first).
    Walk {
        truth: Option<Truth>,
        views: Option<View>,
    },
    /// `rebuild_package_indexes` lists truth.
    Rebuild {
        walk_truth: Truth,
    },
    /// ... and PUTs its render, unconditionally.
    Write {
        walk_truth: Truth,
        rendered: View,
    },
    /// `certify_audit_rebuild`'s (or, Old, `package_fingerprint`'s) listings.
    Relist {
        walk_truth: Truth,
        rendered: View,
        truth: Option<Truth>,
        views: Option<View>,
    },
    /// `certify_audit_rebuild` reads back the rendered keys, then decides.
    ReadBack {
        walk_truth: Truth,
        rendered: View,
        truth: Truth,
        views: View,
    },
    /// The fingerprint shard rewrite: `Some` stores, `None` drops the entry.
    Store {
        fp: Option<String>,
    },
    /// The tail's `mark_dirty` for an uncertified ("dead, no fingerprint")
    /// package.
    ReArm,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    truth: Truth,
    view: View,
    markers: BTreeSet<u8>,
    /// `_state/fp-p.json`'s entry for p0.
    stored: Option<String>,
    writers: [Writer; 3],
    ticks: Vec<Tick>,
    audit: Audit,
    passes: u8,
    // Latched observations (prove the sometimes-properties non-vacuous).
    /// A walk skipped a live package on its stored fingerprint.
    skipped: bool,
    /// The audit PUT a stale render over a view that matched truth.
    clobbered: bool,
    /// `audit_certificate` refused.
    refused: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Action {
    Writer(usize),
    CrashWriter(usize),
    Tick(usize),
    AuditListTruth,
    AuditListViews,
    AuditStep,
    AuditReArmFails,
}

struct AuditModel {
    variant: Variant,
    audit_passes: u8,
    /// More than one tick models dual leadership (the lease has no fencing
    /// token): ticks that rebuild the same package concurrently.
    ticks: usize,
    enable_delete: bool,
    writer_crash: bool,
}

// ---- the real listings the real functions read --------------------------------

fn render(t: &Truth) -> View {
    let live: BTreeSet<u8> = t.artifacts.difference(&t.tombstones).copied().collect();
    (!live.is_empty()).then_some(live)
}

fn artifact_key(f: u8) -> String {
    format!("{PACKAGES_PREFIX}{PKG}/{PKG}-{f}.0-py3-none-any.whl")
}

fn truth_meta(t: &Truth) -> Vec<ObjectMeta> {
    let mut out: Vec<ObjectMeta> = t
        .artifacts
        .iter()
        .map(|&f| ObjectMeta {
            key: artifact_key(f),
            size: 100,
            etag: format!("artifact-{f}"),
        })
        .chain(t.tombstones.iter().map(|&f| ObjectMeta {
            key: format!("{}.tombstone", artifact_key(f)),
            size: 10,
            etag: format!("tombstone-{f}"),
        }))
        .collect();
    out.sort_by(|a, b| a.key.cmp(&b.key));
    out
}

fn view_keys() -> [String; 2] {
    [
        format!("{SIMPLE_PREFIX}{PKG}/index.html"),
        format!("{SIMPLE_PREFIX}{PKG}/index.json"),
    ]
}

fn view_bytes(files: &BTreeSet<u8>) -> Vec<u8> {
    format!("{files:?}").into_bytes()
}

/// The view listing: both keys, content-derived ETags.
fn view_meta(v: &View) -> Vec<ObjectMeta> {
    let Some(files) = v else {
        return Vec::new();
    };
    let bytes = view_bytes(files);
    view_keys()
        .into_iter()
        .map(|key| ObjectMeta {
            key,
            size: bytes.len() as u64,
            etag: String::from_utf8_lossy(&bytes).into_owned(),
        })
        .collect()
}

/// What `rebuild_package_indexes` hands back as `views`: the rendered pair, or
/// nothing for a dead package (its views are removed).
fn rendered_pairs(v: &View) -> Vec<(String, Vec<u8>)> {
    let Some(files) = v else {
        return Vec::new();
    };
    view_keys()
        .into_iter()
        .map(|key| (key, view_bytes(files)))
        .collect()
}

fn fp(truth: &Truth, view: &View) -> String {
    let t = truth_meta(truth);
    let v = view_meta(view);
    fingerprint(&t.iter().collect::<Vec<_>>(), &v.iter().collect::<Vec<_>>())
}

fn truth_fp(truth: &Truth) -> String {
    let t = truth_meta(truth);
    fingerprint(&t.iter().collect::<Vec<_>>(), &[])
}

// ---- predicates ----------------------------------------------------------------

/// The package is in neither listing, so the walk never sees it.
fn absent(truth: &Truth, view: &View) -> bool {
    *truth == Truth::default() && view.is_none()
}

fn quiescent(s: &State) -> bool {
    !s.writers.contains(&Writer::TruthLanded)
        && s.markers.is_empty()
        && s.ticks.iter().all(|t| *t == Tick::Idle)
        && s.audit == Audit::Idle
}

/// The view one more uncontended audit pass leaves: the skip gate keeps the
/// view when the stored fingerprint matches the walk, else the rebuild renders
/// truth.
fn after_one_audit(s: &State) -> View {
    if absent(&s.truth, &s.view) || s.stored.as_deref() == Some(fp(&s.truth, &s.view).as_str()) {
        s.view.clone()
    } else {
        render(&s.truth)
    }
}

// ---- transitions ---------------------------------------------------------------

impl AuditModel {
    fn writer_enabled(&self, w: usize) -> bool {
        w != DEL0 || self.enable_delete
    }

    fn step_writer(s: &mut State, w: usize) -> bool {
        match s.writers[w] {
            Writer::Idle => {
                match w {
                    UP0 => {
                        s.truth.artifacts.insert(F0);
                    }
                    UP1 => {
                        s.truth.artifacts.insert(F1);
                    }
                    _ => {
                        if s.writers[UP0] != Writer::Acked {
                            return false;
                        }
                        s.truth.artifacts.remove(&F0);
                        s.truth.tombstones.insert(F0);
                    }
                }
                s.writers[w] = Writer::TruthLanded;
            }
            Writer::TruthLanded => {
                s.markers.insert(w as u8);
                s.writers[w] = Writer::Acked;
            }
            Writer::Acked | Writer::Crashed => return false,
        }
        true
    }

    fn step_tick(s: &mut State, i: usize) -> bool {
        s.ticks[i] = match s.ticks[i].clone() {
            Tick::Idle if s.markers.is_empty() => return false,
            Tick::Idle => Tick::Listed(s.markers.clone()),
            Tick::Listed(keys) => Tick::Snapped(keys, render(&s.truth)),
            Tick::Snapped(keys, view) => {
                s.view = view;
                Tick::Written(keys)
            }
            Tick::Written(keys) => {
                for key in &keys {
                    s.markers.remove(key);
                }
                Tick::Idle
            }
        };
        true
    }

    /// The listing half of a joined walk/relist: `views == true` lists views.
    fn audit_list(s: &mut State, views: bool) -> bool {
        match &mut s.audit {
            Audit::Walk { truth: t, views: v }
            | Audit::Relist {
                truth: t, views: v, ..
            } => {
                if views && v.is_none() {
                    *v = Some(s.view.clone());
                } else if !views && t.is_none() {
                    *t = Some(s.truth.clone());
                } else {
                    return false;
                }
                true
            }
            _ => false,
        }
    }

    fn step_audit(&self, s: &mut State) -> bool {
        s.audit = match s.audit.clone() {
            Audit::Idle => {
                if s.passes >= self.audit_passes {
                    return false;
                }
                s.passes += 1;
                Audit::Walk {
                    truth: None,
                    views: None,
                }
            }
            Audit::Walk {
                truth: Some(truth),
                views: Some(views),
            } => {
                if absent(&truth, &views) {
                    // Not in the walk: the shard rewrite drops its entry.
                    s.stored = None;
                    Audit::Idle
                } else if s.stored.as_deref() == Some(fp(&truth, &views).as_str()) {
                    s.skipped |= views.is_some();
                    Audit::Idle
                } else {
                    Audit::Rebuild { walk_truth: truth }
                }
            }
            Audit::Rebuild { walk_truth } => Audit::Write {
                walk_truth,
                rendered: render(&s.truth),
            },
            Audit::Write {
                walk_truth,
                rendered,
            } => {
                let fresh = render(&s.truth);
                s.clobbered |= s.view == fresh && rendered != fresh;
                s.view = rendered.clone();
                Audit::Relist {
                    walk_truth,
                    rendered,
                    truth: None,
                    views: None,
                }
            }
            Audit::Relist {
                walk_truth,
                rendered,
                truth: Some(truth),
                views: Some(views),
            } => match self.variant {
                Variant::Old => Audit::Store {
                    fp: Some(fp(&truth, &views)),
                },
                Variant::New => Audit::ReadBack {
                    walk_truth,
                    rendered,
                    truth,
                    views,
                },
            },
            Audit::ReadBack {
                walk_truth,
                rendered,
                truth,
                views,
            } => {
                // The real decision, over the listings and read-back this pass
                // actually observed.
                let pairs = rendered_pairs(&rendered);
                let current = s.view.as_ref().map(view_bytes);
                let read_back: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|_| current.clone()).collect();
                let cert = audit_certificate(
                    PKG,
                    &truth_fp(&walk_truth),
                    &truth_meta(&truth),
                    &view_meta(&views),
                    &pairs,
                    &read_back,
                );
                s.refused |= cert.is_none();
                Audit::Store { fp: cert }
            }
            Audit::Store { fp } => {
                let rearm = fp.is_none();
                s.stored = fp;
                if rearm {
                    Audit::ReArm
                } else {
                    Audit::Idle
                }
            }
            Audit::ReArm => {
                s.markers.insert(REARM_BASE + s.passes);
                Audit::Idle
            }
            // A joined listing still outstanding: only `audit_list` advances it.
            Audit::Walk { .. } | Audit::Relist { .. } => return false,
        };
        true
    }
}

impl Model for AuditModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            truth: Truth::default(),
            view: None,
            markers: BTreeSet::new(),
            stored: None,
            writers: [Writer::Idle; 3],
            ticks: vec![Tick::Idle; self.ticks],
            audit: Audit::Idle,
            passes: 0,
            skipped: false,
            clobbered: false,
            refused: false,
        }]
    }

    fn actions(&self, _s: &State, actions: &mut Vec<Action>) {
        for w in 0..3 {
            if self.writer_enabled(w) {
                actions.push(Action::Writer(w));
                if self.writer_crash {
                    actions.push(Action::CrashWriter(w));
                }
            }
        }
        for t in 0..self.ticks {
            actions.push(Action::Tick(t));
        }
        actions.extend([
            Action::AuditListTruth,
            Action::AuditListViews,
            Action::AuditStep,
            Action::AuditReArmFails,
        ]);
    }

    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        let moved = match action {
            Action::Writer(w) => Self::step_writer(&mut n, w),
            Action::CrashWriter(w) if n.writers[w] == Writer::TruthLanded => {
                n.writers[w] = Writer::Crashed;
                true
            }
            Action::CrashWriter(_) => false,
            Action::Tick(t) => Self::step_tick(&mut n, t),
            Action::AuditListTruth => Self::audit_list(&mut n, false),
            Action::AuditListViews => Self::audit_list(&mut n, true),
            Action::AuditStep => self.step_audit(&mut n),
            // `mark_dirty` failing is logged and left to the next audit.
            Action::AuditReArmFails if n.audit == Audit::ReArm => {
                n.audit = Audit::Idle;
                true
            }
            Action::AuditReArmFails => false,
        };
        moved.then_some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("quiescent_view_settles", |_, s: &State| {
                !quiescent(s) || after_one_audit(s) == render(&s.truth)
            }),
            // Non-vacuity: a certified fingerprint is stored and honoured,
            // the audit's stale PUT really does land over the tick's fresh
            // view, and (New) the certificate really is refused.
            Property::sometimes("certified_view_is_skipped", |_, s: &State| s.skipped),
            Property::sometimes("audit_clobbers_a_fresh_view", |_, s: &State| s.clobbered),
            Property::sometimes(
                "audit_refuses_a_certificate",
                |m: &AuditModel, s: &State| m.variant == Variant::New && s.refused,
            ),
        ]
    }
}

// ---- checked instances -----------------------------------------------------------

fn threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn model(variant: Variant, audit_passes: u8, ticks: usize) -> AuditModel {
    AuditModel {
        variant,
        audit_passes,
        ticks,
        enable_delete: true,
        writer_crash: true,
    }
}

/// The shipped audit: every interleaving of three writers (crashes included),
/// one tick and three audit passes keeps the property, and every
/// sometimes-property is reached.
#[test]
fn model_audit_fingerprint() {
    let checker = model(Variant::New, 3, 1)
        .checker()
        .threads(threads())
        .spawn_bfs()
        .join();
    eprintln!(
        "[audit_fingerprint] unique states = {}, max depth = {}",
        checker.unique_state_count(),
        checker.max_depth()
    );
    checker.assert_properties();
}

/// Kill-proof: the pre-certification audit stores fp(new truth, stale view)
/// and the skip gate then keeps the stale view forever. If this ever stops
/// finding a counterexample, the model can no longer see the bug the shipped
/// audit fixes, and `model_audit_fingerprint` proves nothing.
#[test]
fn old_audit_blesses_a_stale_view() {
    let checker = model(Variant::Old, 1, 1)
        .checker()
        .threads(threads())
        .spawn_bfs()
        .join();
    let discovery = checker.discovery("quiescent_view_settles");
    assert!(
        discovery.is_some(),
        "the uncertified audit no longer violates quiescent_view_settles; \
         the model lost sight of the stale-view race"
    );
    let path = discovery.expect("checked above");
    eprintln!(
        "[old_audit] counterexample ({} steps): {:?}\n  final: {:#?}",
        path.clone().into_actions().len(),
        path.clone().into_actions(),
        path.last_state()
    );
}

/// Documents (and regression-guards) a residual hole the shipped certificate
/// leaves open: with two ticks rebuilding concurrently — a dual-leader window,
/// which the fencing-token-free lease permits — the audit can still bless a
/// stale view. Two stale ticks snapshot truth before a delete; the first lands
/// its stale view, the audit's relist lists it, the first tick's next pass
/// lands the fresh view, the read-back sees that fresh view and certifies, and
/// the second stale tick re-lands the stale bytes last. ETags are content
/// digests (S3, GCS), so the certified fingerprint — truth plus the relist's
/// view ETags — matches the stale view at every later walk, and the audit the
/// design names as the dual-leader backstop skips it. The listing's ETag and
/// the read-back's bytes are taken at different instants and nothing binds
/// them; a per-write ETag backend (Azure) would not match. If this stops
/// finding a counterexample, the hole closed: fold the two-tick config into
/// `model_audit_fingerprint` and drop the private/ROADMAP.md entry.
#[test]
fn dual_leader_ticks_aba_the_certificate() {
    let checker = model(Variant::New, 1, 2)
        .checker()
        .threads(threads())
        .spawn_bfs()
        .join();
    let discovery = checker.discovery("quiescent_view_settles");
    assert!(
        discovery.is_some(),
        "two concurrent ticks no longer defeat the certificate; did the audit \
         gain a binding between its relist ETags and its read-back bytes?"
    );
    let actions = discovery.expect("checked above").into_actions();
    eprintln!(
        "[dual_leader_aba] counterexample ({} steps): {:?}",
        actions.len(),
        actions
    );
}
