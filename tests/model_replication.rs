//! Stateright model of the multi-bucket replication merge (second model):
//! two buckets, private/mirror writers, yanks, deletes,
//! partition-shaped double publishes, and the merge protocol that reconciles
//! them.
//!
//! What is BOUND to real code (the conformance guarantee):
//!   - Every merge transition calls the real `pypiron::replicate::decide` on
//!     real `Record`s built from the abstract bucket state. The model cannot
//!     encode a different precedence algebra than production.
//!   - The model's verdict *executor* (`apply_verdict`) mirrors the storage
//!     effects of the real `pypiron::replicate::execute`. That mirror is kept
//!     honest by `conformance_execute_matches_model` below, which enumerates
//!     abstract two-bucket worlds, runs the REAL `execute` against two real
//!     in-memory buckets, and asserts the resulting bucket state equals the
//!     model executor's prediction — byte for byte at the abstraction.
//!
//! What is ABSTRACTED:
//!   - Views/indexes (`simple/`) and `_dirty` markers: covered by the event-
//!     protocol model (tests/model_event_protocol.rs). `execute` schedules
//!     rebuilds via dirty markers; this model checks truth convergence only.
//!   - `.metadata` / `.provenance` companions (presence-only in production
//!     records; none of the merge precedence depends on them).
//!   - Byte contents are small ids mapped to real distinct byte strings.
//!
//! Convergence is expressed as an `always` property over merge-fixpoint states
//! (no writer mid-flight, no merge/audit action changes anything) instead of
//! stateright's experimental `eventually` — "once everything drains, buckets
//! agree" is exactly the claim, and it dodges the checker's documented
//! liveness/cycle caveat.
//!
//! An honestly-documented consequence this model checks rather than hides:
//! a private upload acknowledged on one bucket CAN be destroyed by a
//! concurrent authorized delete of the same filename on the other bucket —
//! tombstone ≻ everything, and deletes drop bodies without quarantine. The
//! byte-durability property is therefore scoped to files never deleted.

use std::collections::BTreeSet;
use std::sync::Arc;

use stateright::{Checker, Model, Property};

use pypiron::replicate::{decide, Origin as ROrigin, Record, Side, Verdict};
use pypiron::sidecar::{
    frozen_key, metadata_key, mirror_quarantined_key, provenance_key, sidecar_key, tombstone_key,
    Sidecar, Yanked,
};

const PKG: &str = "p0";
const FILES: [&str; 2] = ["p0-1.0-py3-none-any.whl", "p0-2.0-py3-none-any.whl"];

/// Abstract byte contents. Distinct ids are distinct real byte strings, so
/// sha256 identities are faithful in the conformance materialization.
fn real_bytes(byte: u8) -> Vec<u8> {
    vec![b'B', byte]
}

fn real_sha(byte: u8) -> String {
    use sha2::digest::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(real_bytes(byte));
    format!("{:x}", hasher.finalize())
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum MOrigin {
    Private,
    Mirror,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct AbsSidecar {
    sha_of: u8,
    origin: MOrigin,
    yanked: bool,
    yank_epoch: u8,
    /// Server-stamped receive time (ms). `None` on mirror/backfilled sidecars.
    epoch_ms: Option<u16>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
struct FileRec {
    artifact: Option<u8>,
    sidecar: Option<AbsSidecar>,
    tombstoned: bool,
    frozen: bool,
    mirror_q: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
struct BucketAbs {
    pkg_origin: Option<MOrigin>,
    files: [FileRec; 2],
    /// Preserved conflict losers: (file index, byte id).
    quarantine: BTreeSet<(u8, u8)>,
}

/// One writer's little program counter. Writers never resume after Crash.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum WriterPc {
    Ready,
    /// Private upload: artifact written, fence check + sidecar pending.
    ArtifactWritten,
    /// Mirror fill: sidecar written (mirror writes sidecar first).
    SidecarWritten,
    /// Delete: tombstone written, body drop pending.
    TombstoneWritten,
    Done,
    Crashed,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum WriterKind {
    /// Private upload of (file, byte) with a server receive stamp, on `bucket`.
    Private { file: u8, byte: u8, epoch: u16 },
    /// Mirror cache fill of (file, byte) on `bucket`.
    Mirror { file: u8, byte: u8 },
    /// Admin delete of `file` on `bucket` (enabled once some upload acked).
    Delete { file: u8 },
    /// Admin yank flip of `file` on `bucket` (one shot).
    Yank { file: u8 },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Writer {
    bucket: u8,
    kind: WriterKind,
    pc: WriterPc,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct World {
    buckets: [BucketAbs; 2],
    writers: Vec<Writer>,
    /// Private uploads acknowledged to a client: (file, byte).
    acked: BTreeSet<(u8, u8)>,
    /// Files an admin delete ever started on (tombstone is the commit point).
    delete_started: BTreeSet<u8>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Act {
    WriterStep(usize),
    CrashWriter(usize),
    /// Run one merge pass (fan-out, note sweep, and tree diff all funnel
    /// through the same read → decide → execute path) for one file.
    Merge(u8),
    /// The bucket-local audit backfills a sidecar for an orphan artifact.
    AuditBackfill {
        bucket: u8,
        file: u8,
    },
    /// converge_package's "late mirror" repair: a mirror-sidecar live body
    /// under a private package claim is preserved and marked inert.
    LateMirrorQuarantine {
        bucket: u8,
        file: u8,
    },
}

struct Fleet {
    writers: Vec<Writer>,
    /// Which `sometimes` reachability probes this configuration promises.
    expect_freeze: bool,
    expect_quarantine_loser: bool,
    expect_supersede: bool,
    expect_yank_propagation: bool,
    expect_delete_propagation: bool,
}

// ---------------------------------------------------------------------------
// Abstract state -> real protocol types (the decide() binding).
// ---------------------------------------------------------------------------

fn to_sidecar(a: &AbsSidecar) -> Sidecar {
    Sidecar {
        sha256: real_sha(a.sha_of),
        size: real_bytes(a.sha_of).len() as u64,
        version: "1.0".to_string(),
        upload_time: "2026-01-01T00:00:00Z".to_string(),
        requires_python: None,
        yanked: Yanked::Flag(a.yanked),
        origin: Some(
            match a.origin {
                MOrigin::Private => pypiron::origin::PRIVATE,
                MOrigin::Mirror => pypiron::origin::MIRROR,
            }
            .to_string(),
        ),
        upload_epoch_ms: a.epoch_ms.map(u64::from),
        yank_epoch: u64::from(a.yank_epoch),
    }
}

fn to_record(bucket: &BucketAbs, file: u8) -> Record {
    let rec = &bucket.files[file as usize];
    // The package-origin fallback only matters for a live artifact whose
    // sidecar lacks a typed origin (mirrors record_from_names).
    let needs_fallback = rec.artifact.is_some() && rec.sidecar.is_none();
    Record {
        sidecar: rec.sidecar.as_ref().map(to_sidecar),
        has_artifact: rec.artifact.is_some(),
        has_metadata: false,
        has_provenance: false,
        tombstoned: rec.tombstoned,
        frozen: rec.frozen,
        mirror_quarantined: rec.mirror_q,
        pkg_origin: if needs_fallback {
            bucket.pkg_origin.map(|o| match o {
                MOrigin::Private => ROrigin::Private,
                MOrigin::Mirror => ROrigin::Mirror,
            })
        } else {
            None
        },
    }
}

// ---------------------------------------------------------------------------
// The model executor: mirrors replicate::execute's storage effects. Kept
// honest by conformance_execute_matches_model below.
// ---------------------------------------------------------------------------

/// freeze_side: `.frozen` first, quarantine any live body, tombstone as the
/// permanent fence, then drop the record.
fn freeze_side_abs(bucket: &mut BucketAbs, file: u8) {
    let rec = &mut bucket.files[file as usize];
    rec.frozen = true;
    if let Some(byte) = rec.artifact {
        bucket.quarantine.insert((file, byte));
    }
    let rec = &mut bucket.files[file as usize];
    rec.tombstoned = true;
    rec.artifact = None;
    rec.sidecar = None;
}

/// tombstone_side: tombstone before dropping the body. Deletes destroy — no
/// quarantine copy (an authorized delete is not data loss).
fn tombstone_side_abs(bucket: &mut BucketAbs, file: u8) {
    let rec = &mut bucket.files[file as usize];
    rec.tombstoned = true;
    rec.artifact = None;
    rec.sidecar = None;
}

/// supersede_record: drive the destination to the winner's private record,
/// preserving any byte-divergent destination body in quarantine and clearing
/// an obsolete mirror-quarantine marker.
fn supersede_abs(dst: &mut BucketAbs, file: u8, winner_artifact: u8, winner_sidecar: AbsSidecar) {
    dst.pkg_origin = Some(MOrigin::Private);
    let rec = dst.files[file as usize];
    if rec.frozen {
        dst.quarantine.insert((file, winner_artifact));
        freeze_side_abs(dst, file);
        return;
    }
    if rec.tombstoned {
        tombstone_side_abs(dst, file);
        return;
    }
    if let Some(current) = rec.artifact {
        if current != winner_artifact {
            dst.quarantine.insert((file, current));
        }
    }
    let rec = &mut dst.files[file as usize];
    rec.artifact = Some(winner_artifact);
    rec.sidecar = Some(winner_sidecar);
    rec.mirror_q = false;
}

/// Mirror of `replicate::execute` for one verdict at the abstraction. Both
/// records were just computed from `w`, so the CAS re-read arms of the real
/// executor see exactly the decide-time state (the merge pass is atomic here;
/// racing writers interleave BETWEEN merge passes).
fn apply_verdict(w: &mut World, file: u8, verdict: &Verdict) {
    let [a, b] = &mut w.buckets;
    let side_bucket = |side: &Side| -> usize {
        match side {
            Side::A => 0,
            Side::B => 1,
        }
    };
    match verdict {
        Verdict::Noop => {}
        Verdict::Copy(side) => {
            let (src, dst) = match side {
                Side::A => (&*a, &mut *b),
                Side::B => (&*b, &mut *a),
            };
            let rec = src.files[file as usize];
            let (Some(byte), Some(sc)) = (rec.artifact, rec.sidecar) else {
                unreachable!("Copy verdict from a non-live record");
            };
            dst.pkg_origin = Some(MOrigin::Private);
            let drec = &mut dst.files[file as usize];
            drec.artifact = Some(byte);
            drec.sidecar = Some(sc);
        }
        Verdict::AdoptSidecar(_) => {
            // The real executor re-reads and recomputes origin-then-yank
            // precedence under CAS (adopt_sidecar_cas). Atomically applied,
            // that is same_bytes' choice on the current sidecars.
            let ra = a.files[file as usize];
            let rb = b.files[file as usize];
            let (Some(sa), Some(sb)) = (ra.sidecar, rb.sidecar) else {
                return;
            };
            match (sa.origin, sb.origin) {
                (MOrigin::Private, MOrigin::Mirror) => {
                    b.files[file as usize].sidecar = Some(sa);
                }
                (MOrigin::Mirror, MOrigin::Private) => {
                    a.files[file as usize].sidecar = Some(sb);
                }
                (MOrigin::Mirror, MOrigin::Mirror) => {}
                (MOrigin::Private, MOrigin::Private) => {
                    match pypiron::replicate::yank_merge(&to_sidecar(&sa), &to_sidecar(&sb)) {
                        pypiron::replicate::MergeChoice::A => {
                            b.files[file as usize].sidecar = Some(sa);
                        }
                        pypiron::replicate::MergeChoice::B => {
                            a.files[file as usize].sidecar = Some(sb);
                        }
                        pypiron::replicate::MergeChoice::Equal => {}
                    }
                }
            }
        }
        Verdict::Supersede(side) | Verdict::QuarantineLoser(side) => {
            let (src, dst) = match side {
                Side::A => (&*a, &mut *b),
                Side::B => (&*b, &mut *a),
            };
            let rec = src.files[file as usize];
            let (Some(byte), Some(sc)) = (rec.artifact, rec.sidecar) else {
                unreachable!("supersede from a non-live record");
            };
            supersede_abs(dst, file, byte, sc);
        }
        Verdict::Freeze | Verdict::FinishFreeze => {
            freeze_side_abs(a, file);
            freeze_side_abs(b, file);
        }
        Verdict::PropagateFreeze(side) => {
            let target = 1 - side_bucket(side);
            freeze_side_abs(&mut w.buckets[target], file);
        }
        Verdict::Tombstone => {
            let (ra, rb) = (a.files[file as usize], b.files[file as usize]);
            if ra.frozen || rb.frozen {
                freeze_side_abs(a, file);
                freeze_side_abs(b, file);
            } else {
                tombstone_side_abs(a, file);
                tombstone_side_abs(b, file);
            }
        }
    }
}

fn merge_once(w: &World, file: u8) -> Option<World> {
    let ra = to_record(&w.buckets[0], file);
    let rb = to_record(&w.buckets[1], file);
    let verdict = decide(&ra, &rb);
    let mut next = w.clone();
    apply_verdict(&mut next, file, &verdict);
    (next != *w).then_some(next)
}

fn backfill_once(w: &World, bucket: u8, file: u8) -> Option<World> {
    let babs = &w.buckets[bucket as usize];
    let rec = babs.files[file as usize];
    // rebuild_package backfills a sidecar for a bare artifact by hashing it;
    // the typed origin comes from the package-level claim, and legacy fields
    // (upload_epoch_ms) stay absent.
    let origin = babs.pkg_origin?;
    if rec.artifact.is_none() || rec.sidecar.is_some() || rec.tombstoned || rec.frozen {
        return None;
    }
    let mut next = w.clone();
    next.buckets[bucket as usize].files[file as usize].sidecar = Some(AbsSidecar {
        sha_of: rec.artifact.expect("checked above"),
        origin,
        yanked: false,
        yank_epoch: 0,
        epoch_ms: None,
    });
    Some(next)
}

fn late_mirror_quarantine_once(w: &World, bucket: u8, file: u8) -> Option<World> {
    let babs = &w.buckets[bucket as usize];
    let rec = babs.files[file as usize];
    if babs.pkg_origin != Some(MOrigin::Private) {
        return None;
    }
    let (Some(byte), Some(sc)) = (rec.artifact, rec.sidecar) else {
        return None;
    };
    if sc.origin != MOrigin::Mirror || rec.mirror_q || rec.tombstoned || rec.frozen {
        return None;
    }
    // quarantine_mirror_record: preserve the body under its content hash and
    // mark the canonical record inert; the artifact key stays occupied.
    let mut next = w.clone();
    next.buckets[bucket as usize]
        .quarantine
        .insert((file, byte));
    next.buckets[bucket as usize].files[file as usize].mirror_q = true;
    Some(next)
}

fn writer_step(w: &World, idx: usize) -> Option<World> {
    let writer = w.writers[idx];
    let bucket = writer.bucket as usize;
    let mut next = w.clone();
    let advance = |next: &mut World, pc: WriterPc| next.writers[idx].pc = pc;
    match (writer.kind, writer.pc) {
        // --- Private upload: claim origin+intent, artifact, fence check,
        // sidecar, ack. (Markers are the event-protocol model's concern.)
        (WriterKind::Private { file, byte, .. }, WriterPc::Ready) => {
            let babs = &w.buckets[bucket];
            // Origin exclusivity: a mirror-owned name rejects private uploads.
            if babs.pkg_origin == Some(MOrigin::Mirror) {
                advance(&mut next, WriterPc::Done);
                return Some(next);
            }
            let rec = babs.files[file as usize];
            // Immutability + filename fences observed before the body lands.
            if rec.artifact.is_some() || rec.tombstoned || rec.frozen {
                advance(&mut next, WriterPc::Done);
                return Some(next);
            }
            next.buckets[bucket].pkg_origin = Some(MOrigin::Private);
            next.buckets[bucket].files[file as usize].artifact = Some(byte);
            advance(&mut next, WriterPc::ArtifactWritten);
            Some(next)
        }
        (WriterKind::Private { file, byte, epoch }, WriterPc::ArtifactWritten) => {
            let rec = w.buckets[bucket].files[file as usize];
            // Post-create tombstone/frozen fence: a hit aborts the upload and
            // (multi-bucket) leaves the body occupied and suppressed.
            if rec.tombstoned || rec.frozen {
                advance(&mut next, WriterPc::Done);
                return Some(next);
            }
            next.buckets[bucket].files[file as usize].sidecar = Some(AbsSidecar {
                sha_of: byte,
                origin: MOrigin::Private,
                yanked: false,
                yank_epoch: 0,
                epoch_ms: Some(epoch),
            });
            next.acked.insert((file, byte));
            advance(&mut next, WriterPc::Done);
            Some(next)
        }
        // --- Mirror fill: claim, create-only sidecar FIRST, then artifact.
        (WriterKind::Mirror { file, byte }, WriterPc::Ready) => {
            let babs = &w.buckets[bucket];
            if babs.pkg_origin == Some(MOrigin::Private) {
                advance(&mut next, WriterPc::Done);
                return Some(next);
            }
            let rec = babs.files[file as usize];
            if rec.sidecar.is_some() || rec.artifact.is_some() || rec.tombstoned || rec.frozen {
                advance(&mut next, WriterPc::Done);
                return Some(next);
            }
            next.buckets[bucket].pkg_origin = Some(MOrigin::Mirror);
            next.buckets[bucket].files[file as usize].sidecar = Some(AbsSidecar {
                sha_of: byte,
                origin: MOrigin::Mirror,
                yanked: false,
                yank_epoch: 0,
                epoch_ms: None,
            });
            advance(&mut next, WriterPc::SidecarWritten);
            Some(next)
        }
        (WriterKind::Mirror { file, byte }, WriterPc::SidecarWritten) => {
            let rec = w.buckets[bucket].files[file as usize];
            if rec.tombstoned || rec.frozen || rec.artifact.is_some() {
                advance(&mut next, WriterPc::Done);
                return Some(next);
            }
            // The post-publish claim re-check leaves a typed mirror loser in
            // place if demotion won; the late-mirror repair quarantines it.
            next.buckets[bucket].files[file as usize].artifact = Some(byte);
            advance(&mut next, WriterPc::Done);
            Some(next)
        }
        // --- Delete: tombstone is the commit point, then the body drops.
        (WriterKind::Delete { file }, WriterPc::Ready) => {
            let rec = w.buckets[bucket].files[file as usize];
            let is_live_private = rec.artifact.is_some()
                && rec.sidecar.is_some_and(|sc| sc.origin == MOrigin::Private)
                && !rec.tombstoned
                && !rec.frozen;
            if !is_live_private {
                return None; // not yet enabled; wait for the record to land
            }
            next.buckets[bucket].files[file as usize].tombstoned = true;
            next.delete_started.insert(file);
            advance(&mut next, WriterPc::TombstoneWritten);
            Some(next)
        }
        (WriterKind::Delete { file }, WriterPc::TombstoneWritten) => {
            let rec = &mut next.buckets[bucket].files[file as usize];
            rec.artifact = None;
            rec.sidecar = None;
            advance(&mut next, WriterPc::Done);
            Some(next)
        }
        // --- Yank: one-shot epoch bump on a live private sidecar.
        (WriterKind::Yank { file }, WriterPc::Ready) => {
            let rec = w.buckets[bucket].files[file as usize];
            let sc = rec.sidecar?;
            if sc.origin != MOrigin::Private || rec.tombstoned || rec.frozen {
                return None;
            }
            let slot = &mut next.buckets[bucket].files[file as usize];
            slot.sidecar = Some(AbsSidecar {
                yanked: !sc.yanked,
                yank_epoch: sc.yank_epoch + 1,
                ..sc
            });
            advance(&mut next, WriterPc::Done);
            Some(next)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Quiescence + convergence predicates for the properties.
// ---------------------------------------------------------------------------

fn writers_settled(w: &World) -> bool {
    (0..w.writers.len()).all(|idx| {
        matches!(w.writers[idx].pc, WriterPc::Done | WriterPc::Crashed)
            // A writer whose step is currently disabled (e.g. a delete of a
            // record that never landed) counts as settled: if a later merge
            // re-enables it, that state simply isn't a fixpoint state.
            || writer_step(w, idx).is_none()
    })
}

/// No merge, backfill, or repair action changes anything.
fn merge_fixpoint(w: &World) -> bool {
    for file in 0..FILES.len() as u8 {
        if merge_once(w, file).is_some() {
            return false;
        }
        for bucket in 0..2u8 {
            if backfill_once(w, bucket, file).is_some() {
                return false;
            }
            if late_mirror_quarantine_once(w, bucket, file).is_some() {
                return false;
            }
        }
    }
    true
}

fn quiescent(w: &World) -> bool {
    writers_settled(w) && merge_fixpoint(w)
}

/// The private-world projection two quiescent buckets must agree on. Mirror
/// caches are deliberately bucket-local and excluded.
fn private_projection(bucket: &BucketAbs, file: u8) -> (bool, bool, Option<(u8, AbsSidecar)>) {
    let rec = bucket.files[file as usize];
    // Mirror Record::state()'s precedence: tombstone/freeze markers suppress
    // a record even while its canonical body remains occupied (an interrupted
    // delete leaves exactly that shape). A mirror-quarantine marker under a
    // *private* sidecar is stale and falls through, like the real resolver.
    let live_private = match (rec.artifact, rec.sidecar) {
        (Some(byte), Some(sc))
            if sc.origin == MOrigin::Private && !rec.tombstoned && !rec.frozen =>
        {
            Some((byte, sc))
        }
        _ => None,
    };
    (rec.tombstoned, rec.frozen, live_private)
}

fn converged(w: &World) -> bool {
    (0..FILES.len() as u8).all(|file| {
        private_projection(&w.buckets[0], file) == private_projection(&w.buckets[1], file)
    })
}

fn acked_bytes_survive(w: &World) -> bool {
    w.acked.iter().all(|&(file, byte)| {
        if w.delete_started.contains(&file) {
            return true; // an authorized delete may destroy the record
        }
        w.buckets.iter().any(|bucket| {
            bucket.files[file as usize].artifact == Some(byte)
                || bucket.quarantine.contains(&(file, byte))
        })
    })
}

// ---------------------------------------------------------------------------
// The stateright model.
// ---------------------------------------------------------------------------

impl Model for Fleet {
    type State = World;
    type Action = Act;

    fn init_states(&self) -> Vec<World> {
        vec![World {
            buckets: [BucketAbs::default(), BucketAbs::default()],
            writers: self.writers.clone(),
            acked: BTreeSet::new(),
            delete_started: BTreeSet::new(),
        }]
    }

    fn actions(&self, state: &World, actions: &mut Vec<Act>) {
        for (idx, writer) in state.writers.iter().enumerate() {
            if !matches!(writer.pc, WriterPc::Done | WriterPc::Crashed) {
                actions.push(Act::WriterStep(idx));
                // Mid-protocol crash: at most one per writer (Ready writers
                // crashing is uninteresting — nothing was written yet).
                if !matches!(writer.pc, WriterPc::Ready) {
                    actions.push(Act::CrashWriter(idx));
                }
            }
        }
        for file in 0..FILES.len() as u8 {
            actions.push(Act::Merge(file));
            for bucket in 0..2u8 {
                actions.push(Act::AuditBackfill { bucket, file });
                actions.push(Act::LateMirrorQuarantine { bucket, file });
            }
        }
    }

    fn next_state(&self, state: &World, action: Act) -> Option<World> {
        match action {
            Act::WriterStep(idx) => writer_step(state, idx),
            Act::CrashWriter(idx) => {
                let mut next = state.clone();
                next.writers[idx].pc = WriterPc::Crashed;
                Some(next)
            }
            Act::Merge(file) => merge_once(state, file),
            Act::AuditBackfill { bucket, file } => backfill_once(state, bucket, file),
            Act::LateMirrorQuarantine { bucket, file } => {
                late_mirror_quarantine_once(state, bucket, file)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::<Self>::always("quiescent_buckets_converge", |_, w| {
                !quiescent(w) || converged(w)
            }),
            Property::<Self>::always("acked_bytes_survive_or_deleted", |_, w| {
                acked_bytes_survive(w)
            }),
            Property::<Self>::always("deleted_files_settle_dead", |_, w| {
                // Once every writer settled and the merge reached fixpoint, a
                // deleted file is tombstoned everywhere with no body left —
                // even when the deleter crashed between its tombstone write
                // and the body drop (the merge finishes the job).
                !quiescent(w)
                    || w.delete_started.iter().all(|&file| {
                        w.buckets.iter().all(|bucket| {
                            let rec = bucket.files[file as usize];
                            rec.tombstoned && rec.artifact.is_none()
                        })
                    })
            }),
            Property::<Self>::sometimes("upload_replicates", |_, w| {
                w.acked.iter().any(|&(file, byte)| {
                    w.buckets
                        .iter()
                        .all(|b| b.files[file as usize].artifact == Some(byte))
                })
            }),
        ];
        if self.expect_freeze {
            properties.push(Property::<Self>::sometimes("freeze_reachable", |_, w| {
                w.buckets.iter().any(|b| b.files.iter().any(|r| r.frozen))
            }));
        }
        if self.expect_quarantine_loser {
            properties.push(Property::<Self>::sometimes(
                "conflict_loser_quarantined_both_live_preserved",
                |_, w| {
                    // Some state holds both conflicting bytes: one canonical
                    // everywhere, the loser preserved in quarantine.
                    w.acked.len() == 2
                        && converged(w)
                        && acked_bytes_survive(w)
                        && w.buckets.iter().any(|b| !b.quarantine.is_empty())
                        && quiescent(w)
                },
            ));
        }
        if self.expect_supersede {
            properties.push(Property::<Self>::sometimes(
                "private_supersedes_mirror",
                |_, w| {
                    // A mirror body was preserved in quarantine while private
                    // truth serves canonically on the same file.
                    (0..FILES.len() as u8).any(|file| {
                        w.buckets.iter().any(|b| {
                            b.quarantine.iter().any(|&(qf, _)| qf == file)
                                && b.files[file as usize]
                                    .sidecar
                                    .is_some_and(|sc| sc.origin == MOrigin::Private)
                        })
                    })
                },
            ));
        }
        if self.expect_yank_propagation {
            properties.push(Property::<Self>::sometimes(
                "yank_propagates_to_peer",
                |_, w| {
                    (0..FILES.len() as u8).any(|file| {
                        w.buckets.iter().all(|b| {
                            b.files[file as usize]
                                .sidecar
                                .is_some_and(|sc| sc.yanked && sc.yank_epoch > 0)
                        })
                    })
                },
            ));
        }
        if self.expect_delete_propagation {
            properties.push(Property::<Self>::sometimes(
                "delete_propagates_to_peer",
                |_, w| {
                    !w.delete_started.is_empty()
                        && quiescent(w)
                        && w.delete_started.iter().all(|&file| {
                            w.buckets.iter().all(|b| {
                                let rec = b.files[file as usize];
                                rec.tombstoned && rec.artifact.is_none()
                            })
                        })
                },
            ));
        }
        properties
    }
}

fn check(name: &str, fleet: Fleet) {
    let start = std::time::Instant::now();
    let checker = fleet.checker().spawn_bfs().join();
    eprintln!(
        "model_replication/{name}: {} unique states, depth {}, {:?}",
        checker.unique_state_count(),
        checker.max_depth(),
        start.elapsed(),
    );
    checker.assert_properties();
}

fn private_writer(bucket: u8, file: u8, byte: u8, epoch: u16) -> Writer {
    Writer {
        bucket,
        kind: WriterKind::Private { file, byte, epoch },
        pc: WriterPc::Ready,
    }
}

/// Partition-shaped double publish: two private uploads of the same filename
/// with different bytes land on different buckets (the serialization point
/// moved). Receive stamps are >2s apart, so first-uploaded-wins orders them:
/// the loser is quarantined, never deleted, and the fleet converges.
#[test]
fn partition_conflict_first_uploaded_wins() {
    check(
        "first_uploaded_wins",
        Fleet {
            writers: vec![private_writer(0, 0, 0, 0), private_writer(1, 0, 1, 5000)],
            expect_freeze: false,
            expect_quarantine_loser: true,
            expect_supersede: false,
            expect_yank_propagation: false,
            expect_delete_propagation: false,
        },
    );
}

/// Same double publish, but the receive stamps sit inside the 2 s clock-skew
/// guard: the tiebreak is untrustworthy, so the conflict degrades to
/// quarantine-both + freeze behind the permanent filename fence.
#[test]
fn partition_conflict_within_skew_freezes() {
    check(
        "skew_freeze",
        Fleet {
            writers: vec![private_writer(0, 0, 0, 0), private_writer(1, 0, 1, 1000)],
            expect_freeze: true,
            expect_quarantine_loser: false,
            expect_supersede: false,
            expect_yank_propagation: false,
            expect_delete_propagation: false,
        },
    );
}

/// Dependency-confusion boundary: a mirror cache fill races a private upload
/// of the same name on the other bucket. Private must win everywhere; the
/// mirror body is preserved but inert.
#[test]
fn private_beats_mirror_across_buckets() {
    check(
        "private_beats_mirror",
        Fleet {
            writers: vec![
                Writer {
                    bucket: 0,
                    kind: WriterKind::Mirror { file: 0, byte: 0 },
                    pc: WriterPc::Ready,
                },
                private_writer(1, 0, 1, 5000),
            ],
            expect_freeze: false,
            expect_quarantine_loser: false,
            expect_supersede: true,
            expect_yank_propagation: false,
            expect_delete_propagation: false,
        },
    );
}

/// Byte-identical double publish (both buckets already hold the same bytes),
/// then a yank on one bucket and a delete on the other file's record: the
/// yank propagates by epoch, the delete propagates by tombstone precedence.
#[test]
fn yank_and_delete_propagate() {
    check(
        "yank_delete",
        Fleet {
            writers: vec![
                private_writer(0, 0, 0, 0),
                private_writer(1, 0, 0, 5000),
                Writer {
                    bucket: 0,
                    kind: WriterKind::Yank { file: 0 },
                    pc: WriterPc::Ready,
                },
                private_writer(0, 1, 1, 0),
                Writer {
                    bucket: 1,
                    kind: WriterKind::Delete { file: 1 },
                    pc: WriterPc::Ready,
                },
            ],
            expect_freeze: false,
            expect_quarantine_loser: false,
            expect_supersede: false,
            expect_yank_propagation: true,
            expect_delete_propagation: true,
        },
    );
}

/// Nightly-depth configuration: byte conflict, mirror-vs-private, yank, and
/// delete all interleaved in one fleet. Too large for the merge gate; the
/// nightly simulation workflow runs it with `--ignored`.
#[test]
#[ignore = "nightly: large state space (run with --ignored)"]
fn model_replication_deep() {
    check(
        "deep",
        Fleet {
            writers: vec![
                private_writer(0, 0, 0, 0),
                private_writer(1, 0, 1, 5000),
                Writer {
                    bucket: 1,
                    kind: WriterKind::Yank { file: 0 },
                    pc: WriterPc::Ready,
                },
                Writer {
                    bucket: 0,
                    kind: WriterKind::Mirror { file: 1, byte: 0 },
                    pc: WriterPc::Ready,
                },
                private_writer(1, 1, 1, 0),
                Writer {
                    bucket: 1,
                    kind: WriterKind::Delete { file: 1 },
                    pc: WriterPc::Ready,
                },
            ],
            expect_freeze: false,
            expect_quarantine_loser: true,
            expect_supersede: true,
            expect_yank_propagation: false,
            expect_delete_propagation: true,
        },
    );
}

// ---------------------------------------------------------------------------
// Conformance: the model executor vs the real replicate::execute, over an
// enumerated vocabulary of two-bucket worlds, on real in-memory buckets.
// ---------------------------------------------------------------------------

mod conformance {
    use super::*;
    use pypiron::sim::{multi_bucket_state, SimClock, SimStorage};
    use pypiron::storage::Storage;

    /// The per-side record vocabulary. Internally consistent states only —
    /// the same shapes production can persist.
    fn vocabulary() -> Vec<FileRec> {
        let sc = |sha_of: u8, origin: MOrigin, yanked: bool, yank_epoch: u8, epoch_ms| AbsSidecar {
            sha_of,
            origin,
            yanked,
            yank_epoch,
            epoch_ms,
        };
        vec![
            // Absent.
            FileRec::default(),
            // Live private, distinct receive stamps (win / skew / missing).
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Private, false, 0, Some(0))),
                ..FileRec::default()
            },
            FileRec {
                artifact: Some(1),
                sidecar: Some(sc(1, MOrigin::Private, false, 0, Some(5000))),
                ..FileRec::default()
            },
            FileRec {
                artifact: Some(1),
                sidecar: Some(sc(1, MOrigin::Private, false, 0, Some(1000))),
                ..FileRec::default()
            },
            FileRec {
                artifact: Some(1),
                sidecar: Some(sc(1, MOrigin::Private, false, 0, None)),
                ..FileRec::default()
            },
            // Live private, yanked at a higher epoch.
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Private, true, 2, Some(0))),
                ..FileRec::default()
            },
            // Live mirror (two different cached bodies).
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Mirror, false, 0, None)),
                ..FileRec::default()
            },
            FileRec {
                artifact: Some(1),
                sidecar: Some(sc(1, MOrigin::Mirror, false, 0, None)),
                ..FileRec::default()
            },
            // Orphan: bare artifact, no sidecar (crashed writer debris).
            FileRec {
                artifact: Some(0),
                sidecar: None,
                ..FileRec::default()
            },
            // Settled delete.
            FileRec {
                tombstoned: true,
                ..FileRec::default()
            },
            // Interrupted delete: tombstone written, body not yet dropped.
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Private, false, 0, Some(0))),
                tombstoned: true,
                ..FileRec::default()
            },
            // Interrupted delete, later crash point: the artifact is gone but
            // the sidecar was orphaned beside the tombstone. The merge must
            // finish the cleanup, never settle on the debris.
            FileRec {
                artifact: None,
                sidecar: Some(sc(0, MOrigin::Private, false, 0, Some(0))),
                tombstoned: true,
                ..FileRec::default()
            },
            // Settled freeze (freeze always carries its tombstone fence).
            FileRec {
                tombstoned: true,
                frozen: true,
                ..FileRec::default()
            },
            // Interrupted freeze: marker landed, canonical body retained.
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Private, false, 0, Some(0))),
                tombstoned: true,
                frozen: true,
                ..FileRec::default()
            },
            // Quarantined mirror loser under a private claim.
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Mirror, false, 0, None)),
                mirror_q: true,
                ..FileRec::default()
            },
        ]
    }

    /// A record's package claim must be consistent with its contents.
    fn implied_pkg_origin(rec: &FileRec) -> Option<MOrigin> {
        match rec.sidecar {
            Some(sc) if rec.mirror_q => {
                debug_assert_eq!(sc.origin, MOrigin::Mirror);
                Some(MOrigin::Private) // quarantined mirror implies demotion
            }
            Some(sc) => Some(sc.origin),
            None if rec.artifact.is_some() => Some(MOrigin::Private), // orphan under a claim
            None => None,
        }
    }

    async fn materialize(bucket: &BucketAbs, storage: &SimStorage) {
        if let Some(origin) = bucket.pkg_origin {
            // Legacy plaintext claims are valid input by contract.
            let body = match origin {
                MOrigin::Private => pypiron::origin::PRIVATE,
                MOrigin::Mirror => pypiron::origin::MIRROR,
            };
            storage.insert(&pypiron::origin::origin_key(PKG), body.as_bytes().to_vec());
        }
        for (idx, rec) in bucket.files.iter().enumerate() {
            let filename = FILES[idx];
            let akey = format!("packages/{PKG}/{filename}");
            if let Some(byte) = rec.artifact {
                storage.insert(&akey, real_bytes(byte));
            }
            if let Some(sc) = rec.sidecar {
                storage.insert(
                    &sidecar_key(&akey),
                    serde_json::to_vec(&to_sidecar(&sc)).expect("sidecar serializes"),
                );
            }
            if rec.tombstoned {
                storage.insert(
                    &tombstone_key(&akey),
                    format!("{{\"filename\":\"{filename}\"}}").into_bytes(),
                );
            }
            if rec.frozen {
                storage.insert(
                    &frozen_key(&akey),
                    format!("{{\"filename\":\"{filename}\"}}").into_bytes(),
                );
            }
            if rec.mirror_q {
                storage.insert(
                    &mirror_quarantined_key(&akey),
                    format!("{{\"filename\":\"{filename}\"}}").into_bytes(),
                );
            }
        }
        for (file, byte) in &bucket.quarantine {
            let filename = FILES[*file as usize];
            let sha = real_sha(*byte);
            let qkey = format!("_quarantine/{PKG}/{filename}@{}", &sha[..12]);
            storage.insert(&qkey, real_bytes(*byte));
        }
    }

    fn byte_id_of(bytes: &[u8]) -> u8 {
        for byte in 0..4u8 {
            if real_bytes(byte) == bytes {
                return byte;
            }
        }
        panic!("unknown byte content in bucket: {bytes:?}");
    }

    fn abstract_sidecar(bytes: &[u8]) -> AbsSidecar {
        let sc: Sidecar = serde_json::from_slice(bytes).expect("sidecar parses");
        let origin = match sc.origin.as_deref() {
            Some("private") => MOrigin::Private,
            Some("mirror") => MOrigin::Mirror,
            other => panic!("sidecar without a typed origin: {other:?}"),
        };
        let sha_of = (0..4u8)
            .find(|byte| real_sha(*byte) == sc.sha256)
            .expect("sidecar sha maps to a known byte id");
        AbsSidecar {
            sha_of,
            origin,
            yanked: !matches!(sc.yanked.normalized(), Yanked::Flag(false)),
            yank_epoch: u8::try_from(sc.yank_epoch).expect("small epochs"),
            epoch_ms: sc
                .upload_epoch_ms
                .map(|ms| u16::try_from(ms).expect("small stamps")),
        }
    }

    async fn abstract_bucket(storage: &SimStorage) -> BucketAbs {
        let dump = storage.dump();
        let pkg_origin = match pypiron::origin::read_origin(storage, PKG)
            .await
            .expect("read origin claim")
            .as_deref()
        {
            Some(pypiron::origin::PRIVATE) => Some(MOrigin::Private),
            Some(pypiron::origin::MIRROR) => Some(MOrigin::Mirror),
            _ => None,
        };
        let mut out = BucketAbs {
            pkg_origin,
            ..BucketAbs::default()
        };
        for (key, bytes) in &dump {
            if let Some(rest) = key.strip_prefix("_quarantine/") {
                let (_pkg, file_at) = rest.split_once('/').expect("quarantine key shape");
                let (filename, _sha12) = file_at.rsplit_once('@').expect("quarantine key shape");
                let file = FILES
                    .iter()
                    .position(|f| *f == filename)
                    .expect("known filename") as u8;
                out.quarantine.insert((file, byte_id_of(bytes)));
                continue;
            }
            if *key == pypiron::origin::origin_key(PKG) {
                continue; // read through the real parser below
            }
            if key.starts_with("_dirty/") {
                continue; // rebuild scheduling — the event-protocol model's concern
            }
            let Some(rest) = key.strip_prefix(&format!("packages/{PKG}/")) else {
                panic!("unexpected key in bucket: {key}");
            };
            for (idx, filename) in FILES.iter().enumerate() {
                let akey = format!("packages/{PKG}/{filename}");
                let rec = &mut out.files[idx];
                if rest == *filename {
                    rec.artifact = Some(byte_id_of(bytes));
                } else if *key == sidecar_key(&akey) {
                    rec.sidecar = Some(abstract_sidecar(bytes));
                } else if *key == tombstone_key(&akey) {
                    rec.tombstoned = true;
                } else if *key == frozen_key(&akey) {
                    rec.frozen = true;
                } else if *key == mirror_quarantined_key(&akey) {
                    rec.mirror_q = true;
                } else if *key == metadata_key(&akey) || *key == provenance_key(&akey) {
                    panic!("companions are outside this model's vocabulary: {key}");
                } else {
                    continue;
                }
                break;
            }
        }
        out
    }

    /// Enumerate the vocabulary cross product; for each consistent pair, run
    /// the model executor and the real `execute` side by side and require the
    /// same end state at the abstraction.
    #[tokio::test]
    async fn conformance_execute_matches_model() {
        let vocab = vocabulary();
        let mut checked = 0usize;
        for rec_a in &vocab {
            for rec_b in &vocab {
                let origin_a = implied_pkg_origin(rec_a);
                let origin_b = implied_pkg_origin(rec_b);
                let world = World {
                    buckets: [
                        BucketAbs {
                            pkg_origin: origin_a,
                            files: [*rec_a, FileRec::default()],
                            quarantine: BTreeSet::new(),
                        },
                        BucketAbs {
                            pkg_origin: origin_b,
                            files: [*rec_b, FileRec::default()],
                            quarantine: BTreeSet::new(),
                        },
                    ],
                    writers: Vec::new(),
                    acked: BTreeSet::new(),
                    delete_started: BTreeSet::new(),
                };

                // Model side: decide + abstract executor.
                let ra = to_record(&world.buckets[0], 0);
                let rb = to_record(&world.buckets[1], 0);
                let verdict = decide(&ra, &rb);
                let mut predicted = world.clone();
                apply_verdict(&mut predicted, 0, &verdict);

                // Real side: materialize, read real records, decide, execute.
                let clock = SimClock::new(
                    time::OffsetDateTime::parse(
                        "2026-01-01T00:00:00Z",
                        &time::format_description::well_known::Rfc3339,
                    )
                    .expect("valid timestamp"),
                );
                let bucket_a = SimStorage::new(clock.clone());
                let bucket_b = SimStorage::new(clock.clone());
                materialize(&world.buckets[0], &bucket_a).await;
                materialize(&world.buckets[1], &bucket_b).await;
                let state = multi_bucket_state(vec![
                    ("a".to_string(), bucket_a.clone() as Arc<dyn Storage>),
                    ("b".to_string(), bucket_b.clone() as Arc<dyn Storage>),
                ]);

                let real_ra = pypiron::replicate::read_record(bucket_a.as_ref(), PKG, FILES[0])
                    .await
                    .expect("read record a");
                let real_rb = pypiron::replicate::read_record(bucket_b.as_ref(), PKG, FILES[0])
                    .await
                    .expect("read record b");
                let real_verdict = decide(&real_ra, &real_rb);
                assert_eq!(
                    real_verdict, verdict,
                    "materialized records decide differently than abstract records\n a={rec_a:?}\n b={rec_b:?}",
                );

                pypiron::replicate::execute(
                    &state,
                    (bucket_a.as_ref(), bucket_b.as_ref()),
                    PKG,
                    FILES[0],
                    (&real_ra, &real_rb),
                    real_verdict,
                )
                .await
                .unwrap_or_else(|e| {
                    panic!("execute failed on\n a={rec_a:?}\n b={rec_b:?}\n verdict={verdict:?}\n error={e:?}")
                });

                let got_a = abstract_bucket(&bucket_a).await;
                let got_b = abstract_bucket(&bucket_b).await;
                assert_eq!(
                    (got_a, got_b),
                    (predicted.buckets[0].clone(), predicted.buckets[1].clone()),
                    "real execute diverged from the model executor\n a={rec_a:?}\n b={rec_b:?}\n verdict={verdict:?}",
                );
                checked += 1;
            }
        }
        eprintln!("conformance_execute_matches_model: {checked} record pairs verified");
        assert!(checked > 100, "vocabulary shrank unexpectedly: {checked}");
    }
}
