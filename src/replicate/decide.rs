//! Merge algebra — pure and symmetric.
//! Precedence: tombstone ≻ origin (private ≻ mirror) ≻ union ≻ freeze.
//!
//! Split out of [`super`][crate::replicate] as the no-I/O decision core: every
//! input is durable bucket state, so the whole module is unit-testable without
//! storage. The I/O executors that apply a [`Verdict`] stay in `replicate`.

use crate::origin::{OriginState, MIRROR, PRIVATE};
use crate::sidecar::{Sidecar, Yanked};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Upload timestamps closer than this are not trustworthy enough to order a
/// cross-partition byte conflict. Preserve both sides behind a freeze instead.
const CONFLICT_SKEW_MS: u64 = 2_000;

/// A live record's origin: the two [`OriginState`] variants a *claimed* package
/// can hold. `Unclaimed` is unrepresentable here — a record is only ever read for
/// a package that already holds a private or mirror claim — so the narrowing from
/// the canonical claim type is explicit ([`TryFrom<OriginState>`]) rather than a
/// second enum with its own parser.
///
/// The lowercase serde form (`"private"`/`"mirror"`) is the same string persisted
/// as the `pypiron-origin` field of a `.project-status.json` sidecar and exchanged
/// over sync, so this one type both drives the merge and tags a status event.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    Private,
    Mirror,
}

impl Origin {
    pub fn parse(s: &str) -> Option<Origin> {
        OriginState::parse(s)
            .ok()
            .and_then(|state| state.try_into().ok())
    }
}

impl TryFrom<OriginState> for Origin {
    type Error = OriginState;

    /// Narrow a claim state to a record origin. `Unclaimed` has no record-origin
    /// meaning and is returned as the `Err` so callers fold it into "no origin".
    fn try_from(state: OriginState) -> Result<Self, Self::Error> {
        match state {
            OriginState::Private => Ok(Origin::Private),
            OriginState::Mirror => Ok(Origin::Mirror),
            OriginState::Unclaimed => Err(state),
        }
    }
}

/// One bucket's view of a single filename in a package: which of its objects
/// exist, and (if readable) the sidecar that carries sha256/origin/yank state.
#[derive(Clone, Debug)]
pub struct Record {
    pub sidecar: Option<Sidecar>,
    pub has_artifact: bool,
    pub has_metadata: bool,
    pub has_provenance: bool,
    pub tombstoned: bool,
    pub frozen: bool,
    pub mirror_quarantined: bool,
    /// Package-level origin, used only as a fallback when a live artifact's
    /// sidecar omits its own `origin` (a legacy/backfilled record).
    pub pkg_origin: Option<Origin>,
}

/// The normalized state a [`Record`] resolves to for the merge.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RecordState {
    Tombstoned,
    Frozen,
    /// Canonical mirror bytes deliberately retained behind a quarantine
    /// marker. They are absent for ordinary union, but a private peer may
    /// supersede them directly per artifact.
    QuarantinedMirror,
    Live {
        sha: String,
        origin: Origin,
    },
    /// An artifact with no readable/typed sidecar — never replicated as-is (that
    /// would fabricate truth, §4); the bucket's own audit backfills a sidecar
    /// first, promoting it to `Live` on a later pass.
    Orphan,
    Absent,
}

impl Record {
    pub fn origin(&self) -> Option<Origin> {
        self.sidecar
            .as_ref()
            .and_then(|s| s.origin.as_deref())
            .and_then(Origin::parse)
            .or(self.pkg_origin)
    }

    pub fn state(&self) -> RecordState {
        if self.tombstoned {
            return RecordState::Tombstoned;
        }
        if self.frozen {
            return RecordState::Frozen;
        }
        if self.mirror_quarantined {
            match self
                .sidecar
                .as_ref()
                .and_then(|sidecar| sidecar.origin.as_deref())
            {
                Some(PRIVATE) => {}
                Some(MIRROR) | None => return RecordState::QuarantinedMirror,
                Some(_) => {}
            }
        }
        if !self.has_artifact {
            return RecordState::Absent;
        }
        match (self.sidecar.as_ref(), self.origin()) {
            (Some(sc), Some(origin)) => RecordState::Live {
                sha: sc.sha256.clone(),
                origin,
            },
            _ => RecordState::Orphan,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    A,
    B,
}

/// The two-sided decision for one filename. Symmetric: `decide(a, b)` and
/// `decide(b, a)` name the same physical outcome (with the side swapped), so a
/// bidirectional diff cannot double-apply.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Verdict {
    Noop,
    /// The `Side`'s live private record is copied to the other (absent) side.
    Copy(Side),
    /// Same bytes; the `Side`'s sidecar wins the yank/origin merge — overwrite
    /// the other side's sidecar (and make it private if the winner is).
    AdoptSidecar(Side),
    /// The `Side` is private, the other is a *different-byte* mirror: private
    /// wins — quarantine the mirror body and copy the private record over it.
    Supersede(Side),
    /// Different private bytes were ordered by their server-stamped receive
    /// times. The `Side` is the older winner; preserve and replace the loser.
    QuarantineLoser(Side),
    /// Both sides committed different bytes under one filename: freeze both.
    Freeze,
    /// At least one side is tombstoned and the sides disagree: delete the file
    /// and tombstone both (tombstone ≻ everything).
    Tombstone,
    /// The `Side` carries a freeze marker the other lacks: propagate the freeze.
    PropagateFreeze(Side),
    /// Both markers exist but at least one retained canonical body still needs
    /// its idempotent quarantine copy verified after an interrupted freeze.
    FinishFreeze,
}

/// The core merge decision. No I/O, no clocks; every
/// input is bucket state. Unit-tested exhaustively below.
/// A tombstoned record with nothing left to clean: no body, no sidecar, no
/// companions. Only this settles a delete — a sidecar or companion orphaned
/// beside its tombstone (a delete that crashed between removing the artifact
/// and removing its companions) must keep re-firing until dropped, or the
/// debris survives every future diff.
fn settled_delete(record: &Record) -> bool {
    !record.has_artifact
        && record.sidecar.is_none()
        && !record.has_metadata
        && !record.has_provenance
}

pub fn decide(a: &Record, b: &Record) -> Verdict {
    // Tombstone ≻ everything. Converged (both tombstoned, nothing left to
    // clean) is a no-op so a settled delete never re-fires each diff.
    if a.tombstoned || b.tombstoned {
        if a.tombstoned
            && b.tombstoned
            && a.frozen == b.frozen
            && settled_delete(a)
            && settled_delete(b)
        {
            return Verdict::Noop;
        }
        return Verdict::Tombstone;
    }
    // Freeze markers propagate. Both markers are settled only once both live
    // bodies are gone; a failed delete must be retried on the next pass.
    match (a.frozen, b.frozen) {
        (true, true) if a.has_artifact || b.has_artifact => return Verdict::FinishFreeze,
        (true, true) => return Verdict::Noop,
        (true, false) => return Verdict::PropagateFreeze(Side::A),
        (false, true) => return Verdict::PropagateFreeze(Side::B),
        (false, false) => {}
    }

    use RecordState::*;
    match (a.state(), b.state()) {
        // Wait for the local audit to backfill a sidecar before comparing —
        // never fabricate cross-bucket truth from a bare artifact (§4).
        (Orphan, _) | (_, Orphan) => Verdict::Noop,
        (Absent, Absent) => Verdict::Noop,
        (Live { origin, .. }, Absent) => match origin {
            Origin::Private => Verdict::Copy(Side::A),
            Origin::Mirror => Verdict::Noop, // mirror never replicates
        },
        (Absent, Live { origin, .. }) => match origin {
            Origin::Private => Verdict::Copy(Side::B),
            Origin::Mirror => Verdict::Noop,
        },
        (
            Live {
                origin: Origin::Private,
                ..
            },
            QuarantinedMirror,
        ) => Verdict::Supersede(Side::A),
        (
            QuarantinedMirror,
            Live {
                origin: Origin::Private,
                ..
            },
        ) => Verdict::Supersede(Side::B),
        (QuarantinedMirror, _) | (_, QuarantinedMirror) => Verdict::Noop,
        (
            Live {
                sha: sa,
                origin: oa,
            },
            Live {
                sha: sb,
                origin: ob,
            },
        ) => {
            if sa == sb {
                same_bytes(a, b, oa, ob)
            } else {
                match (oa, ob) {
                    (Origin::Private, Origin::Private) => conflict_winner(a, b)
                        .map(Verdict::QuarantineLoser)
                        .unwrap_or(Verdict::Freeze),
                    (Origin::Private, Origin::Mirror) => Verdict::Supersede(Side::A),
                    (Origin::Mirror, Origin::Private) => Verdict::Supersede(Side::B),
                    // Two mirror caches of the same name: each bucket manages its
                    // own upstream cache; nothing to reconcile.
                    (Origin::Mirror, Origin::Mirror) => Verdict::Noop,
                }
            }
        }
        // Tombstoned/Frozen states are resolved before this match; this arm only
        // exists to keep the match total.
        (Tombstoned | Frozen, _) | (_, Tombstoned | Frozen) => Verdict::Noop,
    }
}

pub(crate) fn conflict_winner(a: &Record, b: &Record) -> Option<Side> {
    let ta = a.sidecar.as_ref()?.upload_epoch_ms?;
    let tb = b.sidecar.as_ref()?.upload_epoch_ms?;
    if ta.abs_diff(tb) <= CONFLICT_SKEW_MS {
        return None;
    }
    Some(if ta < tb { Side::A } else { Side::B })
}

/// Both sides hold the same bytes. Origin precedence (private ≻ mirror) first,
/// then the yank merge (§6.5). Adopt the winner's sidecar wholesale.
pub fn same_bytes(a: &Record, b: &Record, oa: Origin, ob: Origin) -> Verdict {
    match (oa, ob) {
        (Origin::Private, Origin::Mirror) => return Verdict::AdoptSidecar(Side::A),
        (Origin::Mirror, Origin::Private) => return Verdict::AdoptSidecar(Side::B),
        // Mirror caches are deliberately bucket-local. That includes their
        // yank metadata and companions: none of it is private truth.
        (Origin::Mirror, Origin::Mirror) => return Verdict::Noop,
        _ => {}
    }
    let (sca, scb) = match (a.sidecar.as_ref(), b.sidecar.as_ref()) {
        (Some(sca), Some(scb)) => (sca, scb),
        _ => return Verdict::Noop,
    };
    match yank_merge(sca, scb) {
        MergeChoice::A => Verdict::AdoptSidecar(Side::A),
        MergeChoice::B => Verdict::AdoptSidecar(Side::B),
        MergeChoice::Equal => Verdict::Noop,
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MergeChoice {
    A,
    B,
    Equal,
}

pub fn is_yanked(sc: &Sidecar) -> bool {
    !matches!(sc.yanked.normalized(), Yanked::Flag(false))
}

/// Yank merge: max epoch wins; on an equal epoch a
/// conflicting state resolves to yanked (fail-closed); a residual tie (both
/// yanked, different reasons) breaks on the lexicographically smaller sidecar
/// sha256. Never a wall clock — two buckets have two clocks.
pub fn yank_merge(a: &Sidecar, b: &Sidecar) -> MergeChoice {
    if a.yank_epoch > b.yank_epoch {
        return MergeChoice::A;
    }
    if b.yank_epoch > a.yank_epoch {
        return MergeChoice::B;
    }
    let (ay, by) = (is_yanked(a), is_yanked(b));
    if ay != by {
        return if ay { MergeChoice::A } else { MergeChoice::B };
    }
    // A residual same-epoch tie includes differing yank reasons and the
    // write-time metadata from two byte-identical partition uploads. Exact
    // equality is already converged; otherwise the sidecar digest gives every
    // pair order the same winner. Comparing serialized bytes directly is not
    // equivalent to comparing their digests.
    match (serde_json::to_vec(a), serde_json::to_vec(b)) {
        (Ok(ja), Ok(jb)) if ja == jb => MergeChoice::Equal,
        (Ok(ja), Ok(jb)) if Sha256::digest(&ja) <= Sha256::digest(&jb) => MergeChoice::A,
        (Ok(_), Ok(_)) => MergeChoice::B,
        _ => MergeChoice::Equal,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn sc(sha: &str, origin: &str, yanked: Yanked, epoch: u64) -> Sidecar {
        Sidecar {
            sha256: sha.to_string(),
            size: 1,
            version: "1.0".to_string(),
            upload_time: "t".to_string(),
            requires_python: None,
            yanked,
            origin: Some(origin.to_string()),
            yank_epoch: epoch,
            upload_epoch_ms: None,
        }
    }

    pub(crate) fn live(sha: &str, origin: &str) -> Record {
        Record {
            sidecar: Some(sc(sha, origin, Yanked::Flag(false), 0)),
            has_artifact: true,
            has_metadata: false,
            has_provenance: false,
            tombstoned: false,
            frozen: false,
            mirror_quarantined: false,
            pkg_origin: None,
        }
    }

    fn live_at(sha: &str, upload_epoch_ms: u64) -> Record {
        let mut record = live(sha, PRIVATE);
        record
            .sidecar
            .as_mut()
            .expect("test record has a sidecar")
            .upload_epoch_ms = Some(upload_epoch_ms);
        record
    }

    fn absent() -> Record {
        Record {
            sidecar: None,
            has_artifact: false,
            has_metadata: false,
            has_provenance: false,
            tombstoned: false,
            frozen: false,
            mirror_quarantined: false,
            pkg_origin: None,
        }
    }

    #[test]
    fn private_copies_to_an_empty_peer() {
        assert_eq!(
            decide(&live("x", PRIVATE), &absent()),
            Verdict::Copy(Side::A)
        );
        assert_eq!(
            decide(&absent(), &live("x", PRIVATE)),
            Verdict::Copy(Side::B)
        );
    }

    #[test]
    fn mirror_never_replicates() {
        assert_eq!(decide(&live("x", MIRROR), &absent()), Verdict::Noop);
        assert_eq!(decide(&absent(), &live("x", MIRROR)), Verdict::Noop);
        // Two mirror caches of the same name, different bytes: nothing to do.
        assert_eq!(
            decide(&live("x", MIRROR), &live("y", MIRROR)),
            Verdict::Noop
        );
        let mut yanked = live("x", MIRROR);
        yanked.sidecar = Some(sc("x", MIRROR, Yanked::Flag(true), 2));
        assert_eq!(decide(&yanked, &live("x", MIRROR)), Verdict::Noop);
    }

    #[test]
    fn identical_bytes_are_a_noop() {
        assert_eq!(
            decide(&live("x", PRIVATE), &live("x", PRIVATE)),
            Verdict::Noop
        );
    }

    #[test]
    fn private_byte_conflict_uses_the_older_upload_epoch() {
        assert_eq!(
            decide(&live_at("x", 1_000), &live_at("y", 4_000)),
            Verdict::QuarantineLoser(Side::A)
        );
        assert_eq!(
            decide(&live_at("y", 4_000), &live_at("x", 1_000)),
            Verdict::QuarantineLoser(Side::B)
        );
    }

    #[test]
    fn private_byte_conflict_without_a_trustworthy_epoch_freezes() {
        // Legacy timestamps and clocks inside the skew window are ambiguous.
        assert_eq!(
            decide(&live("x", PRIVATE), &live("y", PRIVATE)),
            Verdict::Freeze
        );
        assert_eq!(
            decide(&live_at("x", 1_000), &live("y", PRIVATE)),
            Verdict::Freeze
        );
        assert_eq!(
            decide(&live_at("x", 1_000), &live_at("y", 3_000)),
            Verdict::Freeze
        );
    }

    #[test]
    fn origin_precedence_private_beats_mirror() {
        // Different bytes, one private one mirror: private wins, no freeze.
        assert_eq!(
            decide(&live("x", PRIVATE), &live("y", MIRROR)),
            Verdict::Supersede(Side::A)
        );
        assert_eq!(
            decide(&live("y", MIRROR), &live("x", PRIVATE)),
            Verdict::Supersede(Side::B)
        );
        // Same bytes, private vs mirror: adopt the private sidecar (demote peer).
        assert_eq!(
            decide(&live("x", PRIVATE), &live("x", MIRROR)),
            Verdict::AdoptSidecar(Side::A)
        );
    }

    #[test]
    fn tombstone_wins_over_a_live_peer() {
        let mut t = absent();
        t.tombstoned = true;
        assert_eq!(decide(&t, &live("x", PRIVATE)), Verdict::Tombstone);
        assert_eq!(decide(&live("x", PRIVATE), &t), Verdict::Tombstone);
        // Both tombstoned and bodyless is converged.
        let mut t2 = absent();
        t2.tombstoned = true;
        assert_eq!(decide(&t, &t2), Verdict::Noop);
        let mut frozen_tombstone = t2.clone();
        frozen_tombstone.frozen = true;
        assert_eq!(decide(&t, &frozen_tombstone), Verdict::Tombstone);
    }

    #[test]
    fn freeze_marker_propagates_then_settles() {
        let mut f = absent();
        f.frozen = true;
        assert_eq!(
            decide(&f, &live("x", PRIVATE)),
            Verdict::PropagateFreeze(Side::A)
        );
        assert_eq!(
            decide(&live("x", PRIVATE), &f),
            Verdict::PropagateFreeze(Side::B)
        );
        let mut f2 = absent();
        f2.frozen = true;
        assert_eq!(decide(&f, &f2), Verdict::Noop);

        let mut dirty_a = live("x", PRIVATE);
        dirty_a.frozen = true;
        let mut dirty_b = live("y", PRIVATE);
        dirty_b.frozen = true;
        assert_eq!(decide(&dirty_a, &dirty_b), Verdict::FinishFreeze);
    }

    #[test]
    fn yank_merge_takes_the_higher_epoch() {
        let mut a = live("x", PRIVATE);
        a.sidecar = Some(sc("x", PRIVATE, Yanked::Reason("bad".into()), 2));
        let mut b = live("x", PRIVATE);
        b.sidecar = Some(sc("x", PRIVATE, Yanked::Flag(false), 1));
        assert_eq!(decide(&a, &b), Verdict::AdoptSidecar(Side::A));
        assert_eq!(decide(&b, &a), Verdict::AdoptSidecar(Side::B));
    }

    #[test]
    fn yank_merge_equal_epoch_yanked_wins() {
        // Same epoch, conflicting state: yanked wins, fail-closed.
        let yanked = sc("x", PRIVATE, Yanked::Flag(true), 5);
        let clear = sc("x", PRIVATE, Yanked::Flag(false), 5);
        assert_eq!(yank_merge(&yanked, &clear), MergeChoice::A);
        assert_eq!(yank_merge(&clear, &yanked), MergeChoice::B);
        // Identical → Equal.
        assert_eq!(yank_merge(&clear, &clear.clone()), MergeChoice::Equal);
    }

    #[test]
    fn yank_reason_tie_uses_sidecar_sha256_and_is_symmetric() {
        let a = sc("x", PRIVATE, Yanked::Reason("alpha".into()), 5);
        let b = sc("x", PRIVATE, Yanked::Reason("beta".into()), 5);
        let a_digest = Sha256::digest(serde_json::to_vec(&a).unwrap());
        let b_digest = Sha256::digest(serde_json::to_vec(&b).unwrap());
        let expected = if a_digest <= b_digest {
            MergeChoice::A
        } else {
            MergeChoice::B
        };
        assert_eq!(yank_merge(&a, &b), expected);
        assert_eq!(
            yank_merge(&b, &a),
            match expected {
                MergeChoice::A => MergeChoice::B,
                MergeChoice::B => MergeChoice::A,
                MergeChoice::Equal => unreachable!(),
            }
        );
    }

    #[test]
    fn byte_identical_partition_uploads_converge_residual_sidecar_metadata() {
        let a = sc("x", PRIVATE, Yanked::Flag(false), 0);
        let mut b = a.clone();
        b.upload_time = "later".into();
        let ab = yank_merge(&a, &b);
        let ba = yank_merge(&b, &a);
        assert!(matches!(ab, MergeChoice::A | MergeChoice::B));
        assert_eq!(
            (ab, ba),
            match ab {
                MergeChoice::A => (MergeChoice::A, MergeChoice::B),
                MergeChoice::B => (MergeChoice::B, MergeChoice::A),
                MergeChoice::Equal => unreachable!(),
            }
        );
    }

    #[test]
    fn orphan_artifact_defers() {
        // Artifact present, no sidecar: wait for the local backfill.
        let orphan = Record {
            sidecar: None,
            has_artifact: true,
            has_metadata: false,
            has_provenance: false,
            tombstoned: false,
            frozen: false,
            mirror_quarantined: false,
            pkg_origin: None,
        };
        assert_eq!(decide(&orphan, &live("x", PRIVATE)), Verdict::Noop);
        assert_eq!(decide(&orphan, &absent()), Verdict::Noop);
    }
}
