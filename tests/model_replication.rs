//! Stateright model of the multi-bucket replication merge (second model):
//! two buckets, private/mirror writers, yanks, deletes,
//! partition-shaped double publishes, and the merge protocol that reconciles
//! them.
//!
//! What is BOUND to real code (the conformance guarantee):
//!   - Every merge transition calls the real `pypiron::replicate::decide` on
//!     real `Record`s built from the abstract bucket state. The model cannot
//!     encode a different precedence algebra than production.
//!   - A merge transition is the whole diff pass, in the real order: the
//!     package-claim reconciliation (`reconcile_split_origin`) runs first and
//!     the records are read from its result, exactly as `converge_package` and
//!     `replicate_record` both do.
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
    /// Provenance of a mirror record: true on a `sync --to` snapshot, false on a
    /// proxy cache and on private records. Both mirror kinds replicate now — the
    /// merge does not arbitrate this bit; it only rides the yank_merge winner.
    snapshot: bool,
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
    /// Baseline liveness: some acked private upload reaches both buckets. Off for
    /// mirror-only fleets (no private upload) and the freeze fleet (nothing lands
    /// live), where a different `sometimes` probe is the reachability check.
    expect_upload_replicates: bool,
    /// A mirror cache filled on one bucket ends up live on BOTH (async cache
    /// replication converges through the same read → decide → execute path).
    expect_mirror_replicates: bool,
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
        snapshot: a.snapshot,
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
    // The tombstone a freeze writes subsumes a demotion fence standing here.
    rec.mirror_q = false;
}

/// tombstone_side: tombstone before dropping the body. Deletes destroy — no
/// quarantine copy (an authorized delete is not data loss). A tombstone is the
/// stronger, permanent fence, so it also removes a `.mirror-quarantined` it
/// lands over.
fn tombstone_side_abs(bucket: &mut BucketAbs, file: u8) {
    let rec = &mut bucket.files[file as usize];
    rec.tombstoned = true;
    rec.artifact = None;
    rec.sidecar = None;
    rec.mirror_q = false;
}

/// settle_mirror_quarantine: claim private, re-read, write the fence FIRST,
/// move the losing body to this bucket's own `_quarantine/`, then drop the
/// canonical record. The fence stays — it is the only part of a demoted record
/// that replicates, and it is what keeps `origin release` refusing the name.
///
/// The re-read is why this refuses a filename that already holds live PRIVATE
/// truth: that record IS the demotion's resolution, and the merge reaches this
/// primitive with a listing-era verdict that may predate it. The merge pass is
/// atomic at this abstraction, so the guard never fires here — it is modelled
/// because the audit scan and the merge share this one primitive, and the two
/// drifted once already.
fn settle_mirror_quarantine_abs(bucket: &mut BucketAbs, file: u8) {
    bucket.pkg_origin = Some(MOrigin::Private);
    let rec = bucket.files[file as usize];
    if rec.artifact.is_some() && rec.sidecar.map(|sc| sc.origin) == Some(MOrigin::Private) {
        return;
    }
    if let Some(byte) = bucket.files[file as usize].artifact {
        bucket.quarantine.insert((file, byte));
    }
    let rec = &mut bucket.files[file as usize];
    rec.mirror_q = true;
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

/// clear_spent_demotion_fence: a demotion fence beside a live PRIVATE record
/// is spent — private truth taking the filename IS the demotion's intended
/// resolution — and every renderer already reads past it, so dropping it is
/// view-neutral. `execute` does this on both sides before it applies any
/// verdict, because otherwise the leftover key survives every future diff:
/// `decide` reads both sides as `Live { Private }` and calls them converged.
fn clear_spent_demotion_fence_abs(bucket: &mut BucketAbs, file: u8) {
    let rec = &mut bucket.files[file as usize];
    if rec.mirror_q
        && !rec.tombstoned
        && !rec.frozen
        && rec.artifact.is_some()
        && rec.sidecar.map(|sc| sc.origin) == Some(MOrigin::Private)
    {
        rec.mirror_q = false;
    }
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
    clear_spent_demotion_fence_abs(a, file);
    clear_spent_demotion_fence_abs(b, file);
    match verdict {
        // Converged, or declined pending the orphan side's own backfill: the
        // model applies nothing either way. The two are distinguished by the
        // caller — a deferral leaves the destination owed (see `Convergence`).
        Verdict::Noop | Verdict::Defer => {}
        Verdict::Copy(side) => {
            let (src, dst) = match side {
                Side::A => (&*a, &mut *b),
                Side::B => (&*b, &mut *a),
            };
            let rec = src.files[file as usize];
            let (Some(byte), Some(sc)) = (rec.artifact, rec.sidecar) else {
                unreachable!("Copy verdict from a non-live record");
            };
            // copy_live claims the destination ahead of the bytes: a private
            // copy runs ensure_private_origin (private is terminal, and it DOES
            // demote a mirror claim); a mirror copy — snapshot or proxy cache —
            // runs ensure_mirror_origin, which claims an absent name but never
            // demotes a destination that already holds private truth.
            match sc.origin {
                MOrigin::Private => dst.pkg_origin = Some(MOrigin::Private),
                MOrigin::Mirror => {
                    if dst.pkg_origin.is_none() {
                        dst.pkg_origin = Some(MOrigin::Mirror);
                    }
                }
            }
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
                // Any two same-byte mirror records (snapshot, cache, or mixed)
                // converge their yank state exactly like two private records —
                // yank_merge, fail-closed, the snapshot bit riding the winner.
                (MOrigin::Mirror, MOrigin::Mirror) | (MOrigin::Private, MOrigin::Private) => {
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
        Verdict::SettleMirrorQuarantine => {
            settle_mirror_quarantine_abs(a, file);
            settle_mirror_quarantine_abs(b, file);
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

/// quarantine_mirror_artifacts: under a private package claim every
/// mirror-sidecar record is a demotion loser — settle it through the same
/// primitive the merge runs. Package-wide, like the real scan: it walks every
/// member, and a tombstone or freeze marker does not exempt one.
fn quarantine_mirror_artifacts_abs(bucket: &mut BucketAbs) {
    if bucket.pkg_origin != Some(MOrigin::Private) {
        return;
    }
    for file in 0..FILES.len() as u8 {
        // The real scan walks LISTED artifacts, so a filename with no body is
        // never visited — its sidecar alone cannot trigger the repair.
        let rec = bucket.files[file as usize];
        let (Some(_), Some(sc)) = (rec.artifact, rec.sidecar) else {
            continue;
        };
        // A mirror sidecar under a private claim is a demotion loser whether or
        // not the fence already stands (an interrupted settle keeps the body).
        // A private sidecar under a fence is a supersede landing truth: skipped.
        if sc.origin != MOrigin::Mirror {
            continue;
        }
        settle_mirror_quarantine_abs(bucket, file);
    }
}

/// reconcile_split_origin: every diff path — the whole-package
/// `converge_package` and the single-file `replicate_record` — runs this on the
/// two package claims BEFORE it reads any record. A lone private claim is
/// terminal, so it promotes the peer; without this the fleet can sit forever on
/// a (private, mirror) split that no per-file verdict can resolve.
fn reconcile_split_origin_abs(w: &mut World) {
    // A lone mirror claim is reserved on the unclaimed peer, ahead of any bytes.
    // Mirror never demotes private, so this only ever fills in a missing claim;
    // an empty claim has no record for the copy path to ride along with, so
    // without it a claim whose fan-out lost its peer never converges.
    match (w.buckets[0].pkg_origin, w.buckets[1].pkg_origin) {
        (Some(MOrigin::Mirror), None) => w.buckets[1].pkg_origin = Some(MOrigin::Mirror),
        (None, Some(MOrigin::Mirror)) => w.buckets[0].pkg_origin = Some(MOrigin::Mirror),
        _ => {}
    }
    match (w.buckets[0].pkg_origin, w.buckets[1].pkg_origin) {
        // Demoting the peer's mirror claim strands its mirror bodies; the
        // promoted side quarantines them in the same pass.
        (Some(MOrigin::Private), Some(MOrigin::Mirror)) => {
            w.buckets[1].pkg_origin = Some(MOrigin::Private);
            quarantine_mirror_artifacts_abs(&mut w.buckets[1]);
        }
        (Some(MOrigin::Mirror), Some(MOrigin::Private)) => {
            w.buckets[0].pkg_origin = Some(MOrigin::Private);
            quarantine_mirror_artifacts_abs(&mut w.buckets[0]);
        }
        // An unclaimed peer holds no mirror record to strand (a mirror writer
        // claims the name before it writes anything), so no scan follows.
        (Some(MOrigin::Private), None) => w.buckets[1].pkg_origin = Some(MOrigin::Private),
        (None, Some(MOrigin::Private)) => w.buckets[0].pkg_origin = Some(MOrigin::Private),
        _ => {}
    }
}

/// Returns the merged world and the verdict that produced it, or `None` when the
/// pass changed nothing. The verdict rides along so a caller can report *which*
/// protocol decision a merge applied without a second copy of the rules; the
/// model itself ignores it.
fn merge_once(w: &World, file: u8) -> Option<(World, Verdict)> {
    let mut next = w.clone();
    reconcile_split_origin_abs(&mut next);
    // Records are read after the reconciliation, on the reconciled claims —
    // the origin fallback a bare artifact inherits moves with them.
    let ra = to_record(&next.buckets[0], file);
    let rb = to_record(&next.buckets[1], file);
    let verdict = decide(&ra, &rb);
    apply_verdict(&mut next, file, &verdict);
    (next != *w).then_some((next, verdict))
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
        // A backfilled sidecar cannot prove it was a snapshot: stays a cache.
        snapshot: false,
    });
    Some(next)
}

fn late_mirror_quarantine_once(w: &World, bucket: u8, file: u8) -> Option<World> {
    let babs = &w.buckets[bucket as usize];
    let rec = babs.files[file as usize];
    if babs.pkg_origin != Some(MOrigin::Private) {
        return None;
    }
    let (Some(_), Some(sc)) = (rec.artifact, rec.sidecar) else {
        return None;
    };
    if sc.origin != MOrigin::Mirror || rec.tombstoned || rec.frozen {
        return None;
    }
    // The trigger is per-file (a *live* mirror record under a private claim),
    // but the repair it fires is the package-wide scan.
    let mut next = w.clone();
    quarantine_mirror_artifacts_abs(&mut next.buckets[bucket as usize]);
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
                snapshot: false,
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
                snapshot: false,
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

/// The truth-projection two quiescent buckets must agree on. Now that a mirror
/// cache replicates too, the projection includes the live mirror record — the
/// totality claim covers every replicated state class, not just private.
type Projection = (
    bool,
    bool,
    Option<(u8, AbsSidecar)>,
    Option<(u8, AbsSidecar)>,
);

fn truth_projection(bucket: &BucketAbs, file: u8) -> Projection {
    let rec = bucket.files[file as usize];
    // Mirror Record::state()'s precedence: tombstone/freeze markers suppress
    // a record even while its canonical body remains occupied (an interrupted
    // delete leaves exactly that shape). A mirror-quarantine marker suppresses a
    // mirror body (a demotion loser), and under a *private* sidecar it is stale
    // and falls through, like the real resolver.
    let live = |want_private: bool| match (rec.artifact, rec.sidecar) {
        (Some(byte), Some(sc))
            if (sc.origin == MOrigin::Private) == want_private
                && !rec.tombstoned
                && !rec.frozen
                && (want_private || !rec.mirror_q) =>
        {
            Some((byte, sc))
        }
        _ => None,
    };
    (rec.tombstoned, rec.frozen, live(true), live(false))
}

fn converged(w: &World) -> bool {
    (0..FILES.len() as u8)
        .all(|file| truth_projection(&w.buckets[0], file) == truth_projection(&w.buckets[1], file))
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
            Act::Merge(file) => merge_once(state, file).map(|(next, _verdict)| next),
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
            // Non-vacuity guard for the two properties above, which are both
            // `!quiescent(w) || ...`. If the merge ever stops settling — a
            // decide/execute pair that ping-pongs, or a future repair action
            // added to `merge_fixpoint` that is not quite idempotent — no state
            // is quiescent, both implications go trivially true, and the
            // checker reports green on a livelock while still printing an
            // impressive state count. Unconditional on purpose: every fleet
            // here is supposed to drain.
            Property::<Self>::sometimes("reaches_merge_fixpoint", |_, w| quiescent(w)),
        ];
        if self.expect_upload_replicates {
            properties.push(Property::<Self>::sometimes("upload_replicates", |_, w| {
                w.acked.iter().any(|&(file, byte)| {
                    w.buckets
                        .iter()
                        .all(|b| b.files[file as usize].artifact == Some(byte))
                })
            }));
        }
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
        if self.expect_mirror_replicates {
            properties.push(Property::<Self>::sometimes(
                "mirror_cache_replicates_to_peer",
                |_, w| {
                    // A mirror byte a single bucket filled is now live on BOTH
                    // buckets — the totality claim for the last state class.
                    (0..FILES.len() as u8).any(|file| {
                        w.buckets.iter().all(|b| {
                            let rec = b.files[file as usize];
                            rec.artifact.is_some()
                                && rec.sidecar.is_some_and(|sc| sc.origin == MOrigin::Mirror)
                                && !rec.tombstoned
                                && !rec.frozen
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
///
/// A constructor rather than a literal because the visualizer dump at the bottom
/// of this file draws this exact fleet: sharing it is what makes "the drawn
/// graph is the space the merge gate checks" a fact instead of a promise.
fn conflict_fleet() -> Fleet {
    Fleet {
        writers: vec![private_writer(0, 0, 0, 0), private_writer(1, 0, 1, 5000)],
        expect_freeze: false,
        expect_quarantine_loser: true,
        expect_supersede: false,
        expect_yank_propagation: false,
        expect_delete_propagation: false,
        expect_upload_replicates: true,
        expect_mirror_replicates: false,
    }
}

#[test]
fn partition_conflict_first_uploaded_wins() {
    check("first_uploaded_wins", conflict_fleet());
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
            expect_upload_replicates: true,
            expect_mirror_replicates: false,
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
            expect_upload_replicates: true,
            expect_mirror_replicates: false,
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
            expect_upload_replicates: true,
            expect_mirror_replicates: false,
        },
    );
}

/// The last state class: a proxy-cache fill on ONE bucket must converge to the
/// peer. A single mirror writer lands `cache(0)` on bucket 0; the merge (the
/// async note's drain path, same read → decide → execute) copies it, so both
/// buckets end live-mirror. Under the extended `truth_projection`, the
/// always-converge property now proves it, and `mirror_cache_replicates` proves
/// the interesting state is actually reached.
#[test]
fn mirror_cache_replicates_to_peer() {
    check(
        "mirror_cache_replicates",
        Fleet {
            writers: vec![Writer {
                bucket: 0,
                kind: WriterKind::Mirror { file: 0, byte: 0 },
                pc: WriterPc::Ready,
            }],
            expect_freeze: false,
            expect_quarantine_loser: false,
            expect_supersede: false,
            expect_yank_propagation: false,
            expect_delete_propagation: false,
            expect_upload_replicates: false,
            expect_mirror_replicates: true,
        },
    );
}

/// Two proxy caches of one immutable filename that disagree on bytes — the
/// upstream-compromise signal. It used to be a silent per-bucket Noop; now the
/// merge freezes both, exactly like two divergent snapshots, and the buckets
/// still converge (both frozen).
#[test]
fn mirror_cache_byte_conflict_freezes() {
    check(
        "mirror_cache_conflict",
        Fleet {
            writers: vec![
                Writer {
                    bucket: 0,
                    kind: WriterKind::Mirror { file: 0, byte: 0 },
                    pc: WriterPc::Ready,
                },
                Writer {
                    bucket: 1,
                    kind: WriterKind::Mirror { file: 0, byte: 1 },
                    pc: WriterPc::Ready,
                },
            ],
            expect_freeze: true,
            expect_quarantine_loser: false,
            expect_supersede: false,
            expect_yank_propagation: false,
            expect_delete_propagation: false,
            expect_upload_replicates: false,
            expect_mirror_replicates: false,
        },
    );
}

/// The widest configuration: byte conflict, mirror-vs-private, yank, and
/// delete all interleaved in one fleet — the only one that mixes writer kinds
/// rather than exercising them apart, so it is where a cross-class merge bug
/// actually shows up.
///
/// It ran nightly-only until it was measured: 61,230 states, 265 ms in release
/// and 3.4 s for this whole suite in debug. That is merge-gate money, and the
/// nightly is the wrong place for it — this config sat RED for two days behind
/// a job that could not report failure (the `| tee` swallowing cargo's exit
/// status, fixed in `.github/workflows/simulation.yml`). A check every
/// contributor runs beats a check nobody reads.
#[test]
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
            expect_upload_replicates: true,
            expect_mirror_replicates: false,
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

    /// Every arm of the real `Verdict`. The conformance pass asserts it
    /// produces all of them, so an arm whose executor effects nobody compares
    /// is a failing test rather than a quiet hole.
    const ALL_VERDICTS: [&str; 11] = [
        "AdoptSidecar",
        "Copy",
        "Defer",
        "FinishFreeze",
        "Freeze",
        "Noop",
        "PropagateFreeze",
        "QuarantineLoser",
        "SettleMirrorQuarantine",
        "Supersede",
        "Tombstone",
    ];

    /// Exhaustive on purpose — no wildcard arm. A new `Verdict` variant breaks
    /// this build, which is the point: it forces a decision about whether the
    /// vocabulary reaches it.
    fn verdict_name(verdict: &Verdict) -> &'static str {
        match verdict {
            Verdict::Noop => "Noop",
            Verdict::Defer => "Defer",
            Verdict::Copy(_) => "Copy",
            Verdict::AdoptSidecar(_) => "AdoptSidecar",
            Verdict::Supersede(_) => "Supersede",
            Verdict::QuarantineLoser(_) => "QuarantineLoser",
            Verdict::Freeze => "Freeze",
            Verdict::Tombstone => "Tombstone",
            Verdict::PropagateFreeze(_) => "PropagateFreeze",
            Verdict::FinishFreeze => "FinishFreeze",
            Verdict::SettleMirrorQuarantine => "SettleMirrorQuarantine",
        }
    }

    /// The per-side record vocabulary. Internally consistent states only —
    /// the same shapes production can persist.
    fn vocabulary() -> Vec<FileRec> {
        let sc = |sha_of: u8, origin: MOrigin, yanked: bool, yank_epoch: u8, epoch_ms| AbsSidecar {
            sha_of,
            origin,
            yanked,
            yank_epoch,
            epoch_ms,
            // `snap` below builds the snapshot-provenance variants.
            snapshot: false,
        };
        let snap = |sha_of: u8, yanked: bool, yank_epoch: u8| AbsSidecar {
            sha_of,
            origin: MOrigin::Mirror,
            yanked,
            yank_epoch,
            epoch_ms: None,
            snapshot: true,
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
            // Live mirror caches (two different cached bodies): both replicate now.
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
            // Live mirror SNAPSHOTS (snapshot=true): two byte variants and a
            // yanked one, so the conformance pass exercises the snapshot Copy,
            // the two-snapshot divergent Freeze, and the mirror yank merge.
            FileRec {
                artifact: Some(0),
                sidecar: Some(snap(0, false, 0)),
                ..FileRec::default()
            },
            FileRec {
                artifact: Some(1),
                sidecar: Some(snap(1, false, 0)),
                ..FileRec::default()
            },
            FileRec {
                artifact: Some(0),
                sidecar: Some(snap(0, true, 2)),
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
            // Settled freeze (a freeze that ran to completion carries its
            // tombstone fence).
            FileRec {
                tombstoned: true,
                frozen: true,
                ..FileRec::default()
            },
            // Frozen, NOT yet tombstoned — the crash window inside
            // `freeze_side`, which writes the frozen marker, then quarantines,
            // then tombstones (src/replicate.rs). `decide` short-circuits on
            // tombstones before it ever reads the freeze markers, so while
            // every other frozen entry here also sets `tombstoned` this is the
            // only shape that reaches `PropagateFreeze`/`FinishFreeze` — the
            // two verdicts that exist for exactly this interrupted state.
            FileRec {
                frozen: true,
                ..FileRec::default()
            },
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Private, false, 0, Some(0))),
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
            // Interrupted demotion: the fence landed, the mirror body it
            // suppresses has not moved yet.
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Mirror, false, 0, None)),
                mirror_q: true,
                ..FileRec::default()
            },
            // Settled demotion: the fence alone, canonical key empty. This is
            // the state every bucket converges to, and the one a peer that
            // never heard of the demotion has to be driven into.
            FileRec {
                mirror_q: true,
                ..FileRec::default()
            },
            // A tombstone landing over a demoted filename: the fence is
            // subsumed debris the delete path has not cleared yet.
            FileRec {
                tombstoned: true,
                mirror_q: true,
                ..FileRec::default()
            },
            // A SPENT fence: private truth took the filename — the demotion's
            // intended resolution — and the marker beside it is inert debris
            // every renderer already reads past.
            FileRec {
                artifact: Some(0),
                sidecar: Some(sc(0, MOrigin::Private, false, 0, Some(0))),
                mirror_q: true,
                ..FileRec::default()
            },
        ]
    }

    /// A record's package claim must be consistent with its contents.
    fn implied_pkg_origin(rec: &FileRec) -> Option<MOrigin> {
        match rec.sidecar {
            // A fence implies the claim already went private, whichever way
            // the record under it resolved.
            Some(_) if rec.mirror_q => Some(MOrigin::Private),
            Some(sc) => Some(sc.origin),
            None if rec.artifact.is_some() => Some(MOrigin::Private), // orphan under a claim
            // A bare fence is a settled demotion: private is terminal.
            None if rec.mirror_q => Some(MOrigin::Private),
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
            snapshot: sc.snapshot,
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
        let mut produced: BTreeSet<&'static str> = BTreeSet::new();
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

                let _: pypiron::replicate::Convergence = pypiron::replicate::execute(
                    &state,
                    (bucket_a.as_ref(), bucket_b.as_ref()),
                    PKG,
                    FILES[0],
                    (&real_ra, &real_rb),
                    real_verdict,
                    pypiron::replicate::ArtifactSource::Bucket,
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
                produced.insert(verdict_name(&verdict));
            }
        }
        eprintln!(
            "conformance_execute_matches_model: {checked} record pairs verified, \
             verdicts produced: {produced:?}"
        );
        // A pair count is not coverage: 18 entries make 324 pairs, so an
        // 11-entry vocabulary still cleared the old `checked > 100` bar while
        // silently dropping whole verdicts. What has to hold is that every arm
        // of the real `Verdict` had its executor effects compared against the
        // model's at least once. `verdict_name` matches exhaustively with no
        // wildcard, so a new arm fails to compile here rather than slipping
        // through unexercised.
        let missing: Vec<_> = ALL_VERDICTS
            .iter()
            .filter(|name| !produced.contains(**name))
            .collect();
        assert!(
            missing.is_empty(),
            "vocabulary never produces {missing:?} — those verdicts' executor \
             effects are unverified; add a record pair that reaches them",
        );
    }

    /// The artifact transport (stream vs server-side copy, with or without an
    /// injected copy fault) is a generator dimension the merge never sees:
    /// `decide` runs on records that carry no transport, and `execute` must land
    /// the same bucket state whichever way the bytes moved. This asserts that
    /// invariance over every `Copy` verdict in the vocabulary, exercising the
    /// real boot matrix (`build_copy_matrix`) and the per-op ladder against the
    /// sim's byte-move copy and its three faults.
    #[tokio::test]
    async fn convergence_is_transport_invariant() {
        use pypiron::buckets::TOPOLOGY_STAMP_KEY;
        use pypiron::replicate::{execute, read_record, ArtifactSource};
        use pypiron::sim::CopyFault;

        #[derive(Clone, Copy, Debug)]
        enum Transport {
            Stream,
            Copy,
            CopyFaulted(CopyFault),
        }
        let transports = [
            Transport::Stream,
            Transport::Copy,
            Transport::CopyFaulted(CopyFault::Denied),
            Transport::CopyFaulted(CopyFault::Timeout),
            Transport::CopyFaulted(CopyFault::Phantom),
        ];

        // Run one Copy-verdict scenario under one transport; return the two
        // buckets at the abstraction.
        async fn run(world: &World, transport: Transport, tag: &str) -> (BucketAbs, BucketAbs) {
            let clock = SimClock::new(
                time::OffsetDateTime::parse(
                    "2026-01-01T00:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )
                .expect("valid timestamp"),
            );
            let copy = !matches!(transport, Transport::Stream);
            let (bucket_a, bucket_b) = if copy {
                (
                    SimStorage::new_copy_source(clock.clone(), &format!("{tag}-a")),
                    SimStorage::new_copy_source(clock.clone(), &format!("{tag}-b")),
                )
            } else {
                (
                    SimStorage::new(clock.clone()),
                    SimStorage::new(clock.clone()),
                )
            };
            materialize(&world.buckets[0], &bucket_a).await;
            materialize(&world.buckets[1], &bucket_b).await;
            let state = multi_bucket_state(vec![
                ("a".to_string(), bucket_a.clone() as Arc<dyn Storage>),
                ("b".to_string(), bucket_b.clone() as Arc<dyn Storage>),
            ]);
            if copy {
                // The boot probe copies each bucket's topology stamp; seed it,
                // build+install the matrix on a clean (unfaulted) fleet, then
                // clear the stamp so the abstraction never sees it.
                bucket_a.insert(TOPOLOGY_STAMP_KEY, b"stamp".to_vec());
                bucket_b.insert(TOPOLOGY_STAMP_KEY, b"stamp".to_vec());
                let matrix =
                    pypiron::buckets::build_copy_matrix(state.buckets.handles(), &[0, 1]).await;
                assert_eq!(
                    matrix.copyable_pairs(),
                    2,
                    "both sim pairs should verify copyable at boot"
                );
                state.buckets.install_copy_matrix(matrix);
                bucket_a
                    .delete_keys(std::slice::from_ref(&TOPOLOGY_STAMP_KEY.to_string()))
                    .await
                    .unwrap();
                bucket_b
                    .delete_keys(std::slice::from_ref(&TOPOLOGY_STAMP_KEY.to_string()))
                    .await
                    .unwrap();
                // Inject the per-op fault *after* the matrix is built, so the
                // ladder attempts the copy (matrix says it can) and then falls
                // back to streaming when it fails.
                if let Transport::CopyFaulted(fault) = transport {
                    bucket_a.set_copy_fault(Some(fault));
                    bucket_b.set_copy_fault(Some(fault));
                }
            }
            let ra = read_record(bucket_a.as_ref(), PKG, FILES[0])
                .await
                .expect("read record a");
            let rb = read_record(bucket_b.as_ref(), PKG, FILES[0])
                .await
                .expect("read record b");
            let verdict = decide(&ra, &rb);
            let _: pypiron::replicate::Convergence = execute(
                &state,
                (bucket_a.as_ref(), bucket_b.as_ref()),
                PKG,
                FILES[0],
                (&ra, &rb),
                verdict,
                ArtifactSource::Bucket,
            )
            .await
            .expect("execute");
            (
                abstract_bucket(&bucket_a).await,
                abstract_bucket(&bucket_b).await,
            )
        }

        let vocab = vocabulary();
        let mut copy_scenarios = 0usize;
        for (ai, rec_a) in vocab.iter().enumerate() {
            for (bi, rec_b) in vocab.iter().enumerate() {
                let world = World {
                    buckets: [
                        BucketAbs {
                            pkg_origin: implied_pkg_origin(rec_a),
                            files: [*rec_a, FileRec::default()],
                            quarantine: BTreeSet::new(),
                        },
                        BucketAbs {
                            pkg_origin: implied_pkg_origin(rec_b),
                            files: [*rec_b, FileRec::default()],
                            quarantine: BTreeSet::new(),
                        },
                    ],
                    writers: Vec::new(),
                    acked: BTreeSet::new(),
                    delete_started: BTreeSet::new(),
                };
                // Only the copy verdict routes through the artifact transport.
                let ra = to_record(&world.buckets[0], 0);
                let rb = to_record(&world.buckets[1], 0);
                if !matches!(decide(&ra, &rb), Verdict::Copy(_)) {
                    continue;
                }
                let baseline = run(&world, Transport::Stream, &format!("s-{ai}-{bi}")).await;
                for (ti, transport) in transports.iter().enumerate().skip(1) {
                    let got = run(&world, *transport, &format!("c{ti}-{ai}-{bi}")).await;
                    assert_eq!(
                        got, baseline,
                        "transport {transport:?} diverged from streaming\n a={rec_a:?}\n b={rec_b:?}",
                    );
                }
                copy_scenarios += 1;
            }
        }
        eprintln!(
            "convergence_is_transport_invariant: {copy_scenarios} copy scenarios × {} transports",
            transports.len()
        );
        assert!(
            copy_scenarios > 10,
            "expected many copy scenarios, got {copy_scenarios}"
        );
    }

    /// The copy transport's artifact leg is an unconditional `CopyObject` — no
    /// create-if-absent, and nothing below it that could stop mirror bytes
    /// landing on a destination a private publish claimed *after* the
    /// listing-era read this verdict was computed from. Two destination reads
    /// stand in the way and both used to be discarded: the origin claim
    /// (`ensure_mirror_origin` yields to private rather than failing) and the
    /// sidecar (`install_or_verify_mirror_sidecar` reports the yield as
    /// `Ok(false)`, the same value it returns for a dedup). Drive each signal
    /// through the real executor over a real boot copy matrix and require a
    /// loud failure with the destination's private truth untouched.
    #[tokio::test]
    async fn a_copy_never_lands_mirror_bytes_on_a_destination_gone_private() {
        use pypiron::buckets::TOPOLOGY_STAMP_KEY;
        use pypiron::origin::{origin_key, MIRROR, PRIVATE};
        use pypiron::replicate::{execute, read_record, ArtifactSource};

        // Seed a live mirror record on the source and an empty destination,
        // decide the copy against that pair, then let `race` privatize the
        // destination the way a concurrent upload would — after the verdict,
        // before the transport runs. Returns the destination and the error.
        async fn run(tag: &str, race: impl FnOnce(&SimStorage)) -> (Arc<SimStorage>, String) {
            let clock = SimClock::new(
                time::OffsetDateTime::parse(
                    "2026-01-01T00:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )
                .expect("valid timestamp"),
            );
            let src = SimStorage::new_copy_source(clock.clone(), &format!("{tag}-src"));
            let dst = SimStorage::new_copy_source(clock.clone(), &format!("{tag}-dst"));
            materialize(
                &BucketAbs {
                    pkg_origin: Some(MOrigin::Mirror),
                    files: [
                        FileRec {
                            artifact: Some(0),
                            sidecar: Some(AbsSidecar {
                                sha_of: 0,
                                origin: MOrigin::Mirror,
                                yanked: false,
                                yank_epoch: 0,
                                epoch_ms: None,
                                snapshot: false,
                            }),
                            ..FileRec::default()
                        },
                        FileRec::default(),
                    ],
                    quarantine: BTreeSet::new(),
                },
                &src,
            )
            .await;
            let state = multi_bucket_state(vec![
                ("src".to_string(), src.clone() as Arc<dyn Storage>),
                ("dst".to_string(), dst.clone() as Arc<dyn Storage>),
            ]);
            // The boot probe copies each bucket's topology stamp; seed it, build
            // the matrix, then clear it so only the record remains.
            src.insert(TOPOLOGY_STAMP_KEY, b"stamp".to_vec());
            dst.insert(TOPOLOGY_STAMP_KEY, b"stamp".to_vec());
            let matrix =
                pypiron::buckets::build_copy_matrix(state.buckets.handles(), &[0, 1]).await;
            assert_eq!(
                matrix.copyable_pairs(),
                2,
                "the server-side copy transport must be the one under test"
            );
            state.buckets.install_copy_matrix(matrix);
            for bucket in [&src, &dst] {
                bucket
                    .delete_keys(std::slice::from_ref(&TOPOLOGY_STAMP_KEY.to_string()))
                    .await
                    .expect("clear the boot stamp");
            }

            let ra = read_record(src.as_ref(), PKG, FILES[0])
                .await
                .expect("read source record");
            let rb = read_record(dst.as_ref(), PKG, FILES[0])
                .await
                .expect("read destination record");
            let verdict = decide(&ra, &rb);
            assert!(
                matches!(verdict, Verdict::Copy(Side::A)),
                "an empty destination must decide as a copy, got {verdict:?}"
            );

            race(&dst);

            let err = execute(
                &state,
                (src.as_ref(), dst.as_ref()),
                PKG,
                FILES[0],
                (&ra, &rb),
                verdict,
                ArtifactSource::Bucket,
            )
            .await
            .expect_err("the copy must fail loudly, not overwrite private truth");
            (dst, err.to_string())
        }

        let akey = format!("packages/{PKG}/{}", FILES[0]);

        // (a) The racing upload claimed the package. Nothing of the mirror
        // record may land — not even the origin-claim-first sidecar.
        let (dst, err) = run("gone-private-claim", |dst| {
            dst.insert(&origin_key(PKG), PRIVATE.as_bytes().to_vec());
        })
        .await;
        assert!(
            err.contains("claims 'p0' private"),
            "unexpected error: {err}"
        );
        assert!(
            !dst.head_exists(&akey).await.expect("head artifact"),
            "the copy landed mirror bytes under a private claim"
        );
        assert!(
            !dst.head_exists(&sidecar_key(&akey))
                .await
                .expect("head sidecar"),
            "the copy published a mirror sidecar under a private claim"
        );

        // (b) The upload's sidecar landed ahead of its claim upgrade, so the
        // origin read passes and the sidecar is the only signal left.
        let private = AbsSidecar {
            sha_of: 1,
            origin: MOrigin::Private,
            yanked: false,
            yank_epoch: 0,
            epoch_ms: Some(0),
            snapshot: false,
        };
        let (dst, err) = run("gone-private-sidecar", |dst| {
            dst.insert(&origin_key(PKG), MIRROR.as_bytes().to_vec());
            dst.insert(&akey, real_bytes(1));
            dst.insert(
                &sidecar_key(&akey),
                serde_json::to_vec(&to_sidecar(&private)).expect("sidecar serializes"),
            );
        })
        .await;
        assert!(
            err.contains("holds private truth"),
            "unexpected error: {err}"
        );
        assert_eq!(
            dst.get_bytes(&akey).await.expect("read artifact"),
            real_bytes(1),
            "the copy replaced the destination's private body"
        );
        let stored: Sidecar = serde_json::from_slice(
            &dst.get_bytes(&sidecar_key(&akey))
                .await
                .expect("read sidecar"),
        )
        .expect("sidecar parses");
        assert_eq!(stored.origin.as_deref(), Some(PRIVATE));
        assert_eq!(stored.sha256, real_sha(1));
    }
}

// ---------------------------------------------------------------------------
// Visualizer export (dev/scripts/viz): this model's reachable state graph as one
// JSON document — nodes, edges, and a few named paths.
//
// Enumeration is the `Model` trait itself (`init_states` + `next_steps`), so no
// part of the protocol is restated here. The checker cannot hand over the graph:
// its `generated` map is a private child->parent spanning tree keyed by an opaque
// fingerprint, and a `StateRecorder`/`PathRecorder` visitor sees only that tree —
// in this fleet most edges are joins into already-discovered states, and those
// joins are precisely the exhaustiveness argument. So the edges are re-derived.
//
// The BFS below is single-threaded, which makes `counts.depth` a real
// shortest-path depth. `Checker::max_depth()` is a different, thread-count
// dependent number, is not the graph diameter, and is deliberately not published.
// ---------------------------------------------------------------------------

mod viz {
    use super::*;
    use std::collections::{BTreeMap, HashMap, VecDeque};

    use serde_json::{json, Value};

    /// The discovering edge into a node, for walking a shortest path back to init.
    struct TreeEdge {
        from: usize,
        action: String,
        verdict: Option<String>,
    }

    struct GraphDump<'f> {
        fleet: &'f Fleet,
        props: Vec<Property<Fleet>>,
        ids: HashMap<World, usize>,
        depth: Vec<usize>,
        parent: Vec<Option<TreeEdge>>,
        /// No enabled transition leaves this state.
        terminal: Vec<bool>,
        nodes: Vec<Value>,
        edges: Vec<Value>,
        tree_edges: usize,
        quiescent_nodes: usize,
        converged_nodes: usize,
        action_counts: BTreeMap<String, usize>,
    }

    impl<'f> GraphDump<'f> {
        fn new(fleet: &'f Fleet) -> Self {
            Self {
                fleet,
                props: fleet.properties(),
                ids: HashMap::new(),
                depth: Vec::new(),
                parent: Vec::new(),
                terminal: Vec::new(),
                nodes: Vec::new(),
                edges: Vec::new(),
                tree_edges: 0,
                quiescent_nodes: 0,
                converged_nodes: 0,
                action_counts: BTreeMap::new(),
            }
        }

        /// Returns `(id, first_seen)`.
        fn intern(&mut self, w: &World, d: usize) -> (usize, bool) {
            if let Some(&id) = self.ids.get(w) {
                return (id, false);
            }
            let id = self.nodes.len();
            let fleet = self.fleet;
            let holds: Vec<&str> = self
                .props
                .iter()
                .filter(|p| (p.condition)(fleet, w))
                .map(|p| p.name)
                .collect();
            let is_quiescent = quiescent(w);
            let is_converged = converged(w);
            let node = json!({
                "id": id,
                "depth": d,
                "state": render_world(w),
                // The anti-rot guard: the projection above is hand-maintained, so
                // a new `World`/`FileRec` field can silently vanish from it. It
                // cannot vanish from here.
                "raw": format!("{w:#?}"),
                "props": holds,
                "flags": { "quiescent": is_quiescent, "converged": is_converged },
            });
            self.ids.insert(w.clone(), id);
            self.depth.push(d);
            self.parent.push(None);
            self.terminal.push(false);
            self.quiescent_nodes += usize::from(is_quiescent);
            self.converged_nodes += usize::from(is_converged);
            self.nodes.push(node);
            (id, true)
        }

        fn explore(&mut self) {
            let fleet = self.fleet;
            let mut queue: VecDeque<(World, usize)> = VecDeque::new();
            for init in fleet.init_states() {
                let (id, fresh) = self.intern(&init, 0);
                if fresh {
                    queue.push_back((init, id));
                }
            }
            while let Some((state, from)) = queue.pop_front() {
                let d = self.depth[from];
                let steps = fleet.next_steps(&state);
                if steps.is_empty() {
                    self.terminal[from] = true;
                }
                for (action, next) in steps {
                    let label = format!("{action:?}");
                    // The verdict the merge actually applied. It comes out of the
                    // same `merge_once` that produced `next` — never a second
                    // copy of the precedence rules.
                    let verdict = merge_verdict(&state, action);
                    let (to, fresh) = self.intern(&next, d + 1);
                    *self.action_counts.entry(label.clone()).or_default() += 1;
                    if fresh {
                        self.tree_edges += 1;
                        self.parent[to] = Some(TreeEdge {
                            from,
                            action: label.clone(),
                            verdict: verdict.clone(),
                        });
                        queue.push_back((next, to));
                    }
                    self.edges.push(json!({
                        "from": from,
                        "to": to,
                        "action": label,
                        "verdict": verdict,
                        "tree": fresh,
                    }));
                }
            }
        }

        fn deepest_terminal(&self) -> Option<usize> {
            (0..self.nodes.len())
                .filter(|&id| self.terminal[id])
                .max_by_key(|&id| self.depth[id])
        }

        /// A shortest path from init to `id`. Each step carries the state and the
        /// action leaving it; the last step's action is null.
        fn path_to(&self, id: usize) -> Vec<Value> {
            let mut chain: Vec<(usize, Option<&TreeEdge>)> = Vec::new();
            let mut cursor = Some(id);
            while let Some(node) = cursor {
                let edge = self.parent[node].as_ref();
                chain.push((node, edge));
                cursor = edge.map(|e| e.from);
            }
            chain.reverse();
            (0..chain.len())
                .map(|i| {
                    let leaving = chain.get(i + 1).and_then(|&(_, edge)| edge);
                    json!({
                        "id": chain[i].0,
                        "action": leaving.map(|e| e.action.clone()),
                        "verdict": leaving.and_then(|e| e.verdict.clone()),
                    })
                })
                .collect()
        }

        fn depth_histogram(&self) -> BTreeMap<usize, usize> {
            let mut histogram = BTreeMap::new();
            for &d in &self.depth {
                *histogram.entry(d).or_default() += 1;
            }
            histogram
        }
    }

    /// The `Verdict` a `Merge` transition applied, `None` for every other action.
    fn merge_verdict(state: &World, action: Act) -> Option<String> {
        match action {
            Act::Merge(file) => merge_once(state, file).map(|(_, v)| format!("{v:?}")),
            _ => None,
        }
    }

    fn render_sidecar(sc: &AbsSidecar) -> Value {
        json!({
            "sha_of": sc.sha_of,
            "origin": format!("{:?}", sc.origin),
            "yanked": sc.yanked,
            "yank_epoch": sc.yank_epoch,
            "epoch_ms": sc.epoch_ms,
            "snapshot": sc.snapshot,
        })
    }

    fn render_bucket(bucket: &BucketAbs) -> Value {
        use pypiron::replicate::RecordState;
        let files: Vec<Value> = (0..FILES.len() as u8)
            .map(|file| {
                let rec = bucket.files[file as usize];
                // The real resolver's own classifier for this cell, so the cell a
                // reader sees cannot disagree with what the merge reads.
                let resolved = match to_record(bucket, file).state() {
                    RecordState::Live { sha, origin } => {
                        format!("Live({origin:?},{})", sha.get(..8).unwrap_or(sha.as_str()))
                    }
                    other => format!("{other:?}"),
                };
                json!({
                    "file": FILES[file as usize],
                    "artifact": rec.artifact,
                    "sidecar": rec.sidecar.as_ref().map(render_sidecar),
                    "tombstoned": rec.tombstoned,
                    "frozen": rec.frozen,
                    "mirror_quarantined": rec.mirror_q,
                    "resolved": resolved,
                })
            })
            .collect();
        json!({
            "pkg_origin": bucket.pkg_origin.map(|o| format!("{o:?}")),
            "files": files,
            "quarantine": bucket.quarantine.iter().map(|&(f, b)| json!([f, b]))
                .collect::<Vec<_>>(),
        })
    }

    fn render_world(w: &World) -> Value {
        json!({
            "buckets": [render_bucket(&w.buckets[0]), render_bucket(&w.buckets[1])],
            "writers": w.writers.iter().map(|wr| json!({
                "bucket": wr.bucket,
                "kind": format!("{:?}", wr.kind),
                "pc": format!("{:?}", wr.pc),
            })).collect::<Vec<_>>(),
            "acked": w.acked.iter().map(|&(f, b)| json!([f, b])).collect::<Vec<_>>(),
            "delete_started": w.delete_started.iter().copied().collect::<Vec<u8>>(),
        })
    }

    /// A checker `Path` mapped through the dump's own id table.
    fn discovery_steps(ids: &HashMap<World, usize>, path: Vec<(World, Option<Act>)>) -> Vec<Value> {
        path.into_iter()
            .map(|(state, action)| {
                let verdict = action.and_then(|a| merge_verdict(&state, a));
                let id = ids
                    .get(&state)
                    .copied()
                    .expect("a discovery state is in the enumerated space");
                json!({
                    "id": id,
                    "action": action.map(|a| format!("{a:?}")),
                    "verdict": verdict,
                })
            })
            .collect()
    }

    fn git_short_head() -> String {
        std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|out| out.status.success())
            .and_then(|out| String::from_utf8(out.stdout).ok())
            .map(|sha| sha.trim().to_string())
            .filter(|sha| !sha.is_empty())
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Writes the state graph of `conflict_fleet()` to `$PYPIRON_VIZ_GRAPH`.
    ///
    /// The env guard is load-bearing, not decoration: the nightly `model-deep`
    /// job runs `cargo test --release ... -- --ignored`, so `#[ignore]` alone
    /// would run this in CI. Without the variable the test does nothing at all.
    #[test]
    #[ignore = "visualizer: writes a state-graph JSON when PYPIRON_VIZ_GRAPH=<path>"]
    fn dump_state_graph() {
        let Ok(out) = std::env::var("PYPIRON_VIZ_GRAPH") else {
            return;
        };
        let start = std::time::Instant::now();

        let fleet = conflict_fleet();
        let mut dump = GraphDump::new(&fleet);
        dump.explore();

        // The drawn graph must be the space the checker verifies, or the picture
        // is fiction. Same fleet, independently enumerated, counts compared.
        let checker = conflict_fleet().checker().spawn_bfs().join();
        checker.assert_properties();
        assert_eq!(
            dump.nodes.len(),
            checker.unique_state_count(),
            "the dumped graph must be exactly the state space the checker verified",
        );

        // Backs caveat 1 below. A fleet that grows a mirror writer must not keep
        // publishing the claim that this action never fires.
        assert!(
            !dump
                .action_counts
                .keys()
                .any(|a| a.starts_with("LateMirrorQuarantine")),
            "the LateMirrorQuarantine caveat is stale: this fleet now takes that action",
        );

        let node_count = dump.nodes.len();
        let edge_count = dump.edges.len();
        let joins = edge_count - dump.tree_edges;
        let depth = dump.depth.iter().copied().max().unwrap_or(0);

        let mut paths: Vec<Value> = Vec::new();
        let mut discoveries: Vec<_> = checker.discoveries().into_iter().collect();
        discoveries.sort_by_key(|(name, _)| *name);
        for (name, path) in discoveries {
            paths.push(json!({
                "name": name,
                "kind": "discovery",
                "note": "the shortest interleaving the checker found for this reachability probe",
                "steps": discovery_steps(&dump.ids, path.into_vec()),
            }));
        }
        if let Some(id) = dump.deepest_terminal() {
            paths.push(json!({
                "name": "deepest_settled_world",
                "kind": "terminal",
                "note": "shortest interleaving from init to the deepest state with no enabled transition",
                "steps": dump.path_to(id),
            }));
        }

        let caveats = vec![
            "`LateMirrorQuarantine` is an enabled action in every state but fires on 0 \
             transitions here — this fleet has no mirror writer, so an all-actions legend \
             would mislead."
                .to_string(),
            "Only `Merge` edges carry a verdict: it is the real `pypiron::replicate::decide` \
             result the transition applied. Every other action has none."
                .to_string(),
            format!(
                "Depth {depth} is a shortest-path depth from this dump's own single-threaded \
                 BFS. `Checker::max_depth()` reports a different, thread-count dependent \
                 number that is not the graph diameter, and is not published here."
            ),
            format!(
                "All {node_count} states are drawn: the count is asserted equal to \
                 `Checker::unique_state_count()` for the same fleet. This is the \
                 configuration `partition_conflict_first_uploaded_wins` checks on every \
                 merge-gate run — nothing is reduced."
            ),
            "This model abstracts views/indexes and `_dirty/` markers (the event-protocol \
             model owns those) and reduces file bodies to two distinct byte strings."
                .to_string(),
            "`model_replication_deep` — the largest configuration in this file, run nightly \
             with `--ignored` — is far too large to draw and is not shown."
                .to_string(),
        ];

        let doc = json!({
            "kind": "graph",
            "model": "replication",
            "config": {
                "test": "partition_conflict_first_uploaded_wins",
                "buckets": 2,
                "files": FILES,
                "writers": fleet.writers.iter().map(|wr| json!({
                    "bucket": wr.bucket,
                    "kind": format!("{:?}", wr.kind),
                    "pc": format!("{:?}", wr.pc),
                })).collect::<Vec<_>>(),
                "expect_freeze": fleet.expect_freeze,
                "expect_quarantine_loser": fleet.expect_quarantine_loser,
                "expect_supersede": fleet.expect_supersede,
                "expect_yank_propagation": fleet.expect_yank_propagation,
                "expect_delete_propagation": fleet.expect_delete_propagation,
                "expect_upload_replicates": fleet.expect_upload_replicates,
                "expect_mirror_replicates": fleet.expect_mirror_replicates,
            },
            "generated_by": format!(
                "PYPIRON_VIZ_GRAPH={out} cargo test --release --test model_replication \
                 -- --ignored dump_state_graph"
            ),
            "commit": git_short_head(),
            "title": "Two writers race one filename across two regions",
            "narration": format!(
                "Every interleaving of two private uploads of the same immutable filename \
                 into different regions — {node_count} states, {edge_count} transitions, \
                 {joins} of them joins where independent orders reconverge on the same world."
            ),
            "caveats": caveats,
            "counts": {
                "nodes": node_count,
                "edges": edge_count,
                "tree_edges": dump.tree_edges,
                "joins": joins,
                "depth": depth,
                "depth_histogram": dump.depth_histogram(),
                "quiescent_nodes": dump.quiescent_nodes,
                "converged_nodes": dump.converged_nodes,
                "terminal_nodes": dump.terminal.iter().filter(|&&t| t).count(),
                "action_counts": dump.action_counts,
                "ci_states": checker.unique_state_count(),
                "ci_depth_note": "depth is a shortest-path depth from this dump's own \
                                  single-threaded BFS; Checker::max_depth() is thread-count \
                                  dependent and is not the graph diameter",
            },
            "props": dump.props.iter().map(|p| json!({
                "name": p.name,
                "kind": format!("{:?}", p.expectation).to_lowercase(),
            })).collect::<Vec<_>>(),
            "nodes": dump.nodes,
            "edges": dump.edges,
            "paths": paths,
        });

        let bytes = serde_json::to_vec(&doc).expect("the graph document serializes");
        let size = bytes.len();
        std::fs::write(&out, bytes).expect("write the graph document");
        eprintln!(
            "dump_state_graph: {node_count} nodes, {edge_count} edges ({} tree, {joins} joins), \
             depth {depth}, {} paths, {size} bytes in {:?} -> {out}",
            dump.tree_edges,
            paths.len(),
            start.elapsed(),
        );
    }
}
