//! Merge algebra — pure and symmetric.
//! Precedence: tombstone ≻ origin (private ≻ mirror) ≻ union ≻ freeze.
//!
//! Split out of [`super`][crate::replicate] as the no-I/O decision core: every
//! input is durable bucket state, so the whole module is unit-testable without
//! storage. The I/O executors that apply a [`Verdict`] stay in `replicate`.
//!
//! Every replicated document merges here and nowhere else: a filename's record
//! ([`decide`]), its sidecar ([`sidecar_merge`]), the package claim
//! ([`claim_join`]) and the project status ([`status_merge`]). They share one
//! origin precedence ([`Origin::rank`]) and one tiebreak ([`ranked_merge`]),
//! and the law tests below hold each ranked merge to a total order — the
//! property that makes N buckets converge on one answer whatever order the
//! pairs sync in.

use crate::origin::{OriginState, MIRROR, PRIVATE};
use crate::sidecar::{Sidecar, Yanked};
use crate::status::{ProjectStatusDoc, VersionedProjectStatus};
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

    /// The one origin precedence every merge ranks by: private ≻ mirror.
    /// Private is the operator's own truth; mirror is a copy of upstream.
    pub fn rank(self) -> u8 {
        match self {
            Origin::Mirror => 1,
            Origin::Private => 2,
        }
    }
}

/// The package claim both buckets converge on: the higher-ranked one, an
/// absent claim lowest. A lone claim is a fan-out a partition cut short, and
/// private is terminal, so the join only ever raises a side.
pub fn claim_join(a: Option<Origin>, b: Option<Origin>) -> Option<Origin> {
    if b.map_or(0, Origin::rank) > a.map_or(0, Origin::rank) {
        b
    } else {
        a
    }
}

/// A mirror record standing under a private package claim. A proxy fill can
/// commit after the claim turned private, so this is a demotion loser to
/// quarantine — never truth to replicate, whatever [`decide`] would say about
/// the record on its own.
pub(crate) fn is_late_mirror(record: &Record, pkg_origin: Option<Origin>) -> bool {
    pkg_origin == Some(Origin::Private)
        && matches!(
            record.state(),
            RecordState::Live {
                origin: Origin::Mirror,
                ..
            }
        )
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
    /// A mirror→private demotion: the `.mirror-quarantined` fence stands over
    /// this filename. Once settled the canonical key is empty and the losing
    /// body sits in this bucket's own `_quarantine/`; until then a body and its
    /// mirror sidecar may still be standing. Absent for ordinary union, but a
    /// private peer may supersede the name directly per artifact.
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
    /// The two sides agree. Nothing is owed to either bucket.
    ///
    /// This is a **durability claim**, not a shrug — the strongest thing this
    /// enum says. `execute` maps it to [`Convergence::Converged`][c], which is
    /// what the pre-ack fan-out reads as "the peer holds it", so it returns a
    /// `200` and writes no `_repl/` note. Answer `Noop` over a pair that does
    /// not agree and the ack claims a durability the fleet does not have
    /// (dev/DESIGN.md's totality principle).
    ///
    /// Both bugs this cost were the same mistake: `(Orphan, _)` (`fd14f01`) and
    /// `(QuarantinedMirror, _)` (`4bb9cb8`), each a pair where one bucket held a
    /// record or a fence and the other had never heard of it. Both read as
    /// harmless no-ops and both acked uploads no peer held. If a new arm cannot
    /// state what the two sides *agree on*, it wants [`Verdict::Defer`] or a
    /// verdict that acts — never this.
    ///
    /// [c]: crate::replicate::Convergence::Converged
    Noop,
    /// The merge *declined to act this pass* — it is not a convergence. Only
    /// state the other bucket's own audit can produce (a sidecar for a bare
    /// artifact, §4) would let a decision be made, so acting now would have to
    /// fabricate truth. Distinct from [`Verdict::Noop`] because a caller that
    /// reads a deferral as "converged" acks a durability the fleet does not
    /// have: it is the pre-ack fan-out's cue to leave a `_repl/` note, and the
    /// marker sweep's cue to retain the note it is draining.
    Defer,
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
    /// At least one bucket demoted this filename mirror→private, and the pair
    /// is not settled: a peer has never heard of the demotion, or a canonical
    /// body is still standing under the fence somewhere. Drive both sides to
    /// the settled state — claim private, fence created, losing body moved to
    /// that bucket's own `_quarantine/`, canonical key empty.
    ///
    /// Symmetric and side-free on purpose: the settle is idempotent and both
    /// buckets run it, so there is no winner to name. It is emphatically not a
    /// [`Verdict::Noop`] — the sides do not agree — and not a
    /// [`Verdict::Defer`] either: nothing is being waited on, the merge can
    /// decide and act now.
    SettleMirrorQuarantine,
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
    // clean) is a no-op so a settled delete never re-fires each diff. A
    // surviving `.mirror-quarantined` is debris here exactly as an orphaned
    // sidecar is: a tombstone bars the filename permanently, which strictly
    // subsumes the demotion fence, so the delete path removes it — and until it
    // is gone the two buckets are one key apart.
    if a.tombstoned || b.tombstoned {
        if a.tombstoned
            && b.tombstoned
            && a.frozen == b.frozen
            && !a.mirror_quarantined
            && !b.mirror_quarantined
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
        // never fabricate cross-bucket truth from a bare artifact (§4). A
        // deferral, never a convergence: the bucket holding the bare artifact
        // holds no record, so whoever asked is still owed one.
        (Orphan, _) | (_, Orphan) => Verdict::Defer,
        (Absent, Absent) => Verdict::Noop,
        // Every live record copies to an absent peer: private truth, a `sync --to`
        // snapshot, and a proxy-cache fill alike. The snapshot/cache distinction is
        // write-time provenance (it picks the *propagation mechanism* — pre-ack
        // fan-out vs an async `_repl/` note — not *whether* a mirror record
        // replicates). The merge no longer branches on it.
        (Live { .. }, Absent) => Verdict::Copy(Side::A),
        (Absent, Live { .. }) => Verdict::Copy(Side::B),
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
        // A demotion settles when both buckets carry the fence and neither
        // still holds a canonical record under it. Anything else — a peer that
        // never heard of the demotion, or a body still standing behind the
        // fence — is an unconverged suppression, not an agreement.
        (QuarantinedMirror, QuarantinedMirror) if settled_delete(a) && settled_delete(b) => {
            Verdict::Noop
        }
        (QuarantinedMirror, _) | (_, QuarantinedMirror) => Verdict::SettleMirrorQuarantine,
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
                    // Two mirror bodies disagree on bytes under one immutable
                    // PyPI filename — an upstream-compromise signal regardless of
                    // whether either side is a snapshot or a lazy cache (both are
                    // sha-verified against upstream before commit, so this is not
                    // transit corruption). Mirror sidecars carry no upload-epoch,
                    // so there is no trustworthy order: fail closed and freeze
                    // both. A supply-chain product must never auto-pick between
                    // two byte-sets of one filename.
                    (Origin::Mirror, Origin::Mirror) => Verdict::Freeze,
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

/// Both sides hold the same bytes: the [`sidecar_merge`] winner's sidecar is
/// adopted wholesale.
pub fn same_bytes(a: &Record, b: &Record, oa: Origin, ob: Origin) -> Verdict {
    let (sca, scb) = match (a.sidecar.as_ref(), b.sidecar.as_ref()) {
        (Some(sca), Some(scb)) => (sca, scb),
        _ => return Verdict::Noop,
    };
    match sidecar_merge(sca, Some(oa), scb, Some(ob)) {
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

/// The one tiebreak every replicated document merges by. The greater key wins;
/// an equal key over identical bytes is already converged; an equal key over
/// different bytes goes to the lexicographically smaller sha256 of the bytes,
/// so every pair order and every bucket picks the same winner. Never a wall
/// clock and never a bucket identity — two buckets have two of each.
/// (Comparing the bytes directly would also be a total order, but not the one
/// stored records were already merged under.)
pub fn ranked_merge<K: Ord>(ka: K, a: &[u8], kb: K, b: &[u8]) -> MergeChoice {
    match ka.cmp(&kb) {
        std::cmp::Ordering::Greater => MergeChoice::A,
        std::cmp::Ordering::Less => MergeChoice::B,
        std::cmp::Ordering::Equal if a == b => MergeChoice::Equal,
        std::cmp::Ordering::Equal if Sha256::digest(a) <= Sha256::digest(b) => MergeChoice::A,
        std::cmp::Ordering::Equal => MergeChoice::B,
    }
}

pub fn is_yanked(sc: &Sidecar) -> bool {
    !matches!(sc.yanked.normalized(), Yanked::Flag(false))
}

/// Same-bytes sidecar merge (§6.5), ranked by: origin (private ≻ mirror; an
/// untyped legacy sidecar ranks as mirror, so it never outranks private truth
/// and never wins on provenance alone); then the yank epoch; then yanked over
/// not, so a split never un-yanks (fail-closed); then the upload-time revision,
/// so a timestamp repair never outranks a yank. A residual tie — differing yank
/// reasons, or the write-time metadata of two byte-identical partition
/// uploads — falls to [`ranked_merge`]'s digest.
pub fn sidecar_merge(
    a: &Sidecar,
    oa: Option<Origin>,
    b: &Sidecar,
    ob: Option<Origin>,
) -> MergeChoice {
    let key = |sc: &Sidecar, origin: Option<Origin>| {
        (
            origin.unwrap_or(Origin::Mirror).rank(),
            sc.yank_epoch,
            is_yanked(sc),
            sc.upload_time_revision,
        )
    };
    ranked_merge(
        key(a, oa),
        &serde_json::to_vec(a).unwrap_or_default(),
        key(b, ob),
        &serde_json::to_vec(b).unwrap_or_default(),
    )
}

/// Project-status merge, without clocks. Absence is `active` at epoch zero, so
/// a stored epoch-zero `active` event and no event agree. Otherwise ranked by:
/// operator-authored over relayed — a mirror-tagged event is a proxy's copy of
/// upstream and ranks below anything an operator wrote, tagged private or
/// untagged (a claim absent at write time); then the greater epoch; then the
/// more restrictive status (fail-closed); then private over untagged.
///
/// Untagged ranks *with* private, not with mirror, unlike an untyped sidecar:
/// an untagged status is still an operator's write, and a peer's private event
/// must not outrank a later one by provenance alone. It ranks above mirror
/// rather than beside it by epoch because both cannot hold: private ≻ mirror
/// regardless of epoch, so ordering untagged against each by epoch alone was a
/// cycle (untagged@2 ≻ private@1 ≻ mirror@3 ≻ untagged@2) whose outcome
/// depended on which pair synced first.
pub fn status_merge(
    a: Option<&VersionedProjectStatus>,
    b: Option<&VersionedProjectStatus>,
) -> MergeChoice {
    let is_default =
        |s: &VersionedProjectStatus| s.epoch == 0 && s.doc == ProjectStatusDoc::default();
    let key = |s: &VersionedProjectStatus| {
        (
            s.origin != Some(Origin::Mirror),
            s.epoch,
            s.doc.status.restriction(),
            s.origin == Some(Origin::Private),
        )
    };
    match (a, b) {
        (None, None) => MergeChoice::Equal,
        (Some(s), None) | (None, Some(s)) if is_default(s) => MergeChoice::Equal,
        (Some(_), None) => MergeChoice::A,
        (None, Some(_)) => MergeChoice::B,
        (Some(a), Some(b)) => ranked_merge(key(a), &a.encode(), key(b), &b.encode()),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The yank half of [`sidecar_merge`]: two sidecars of one origin.
    fn yank_merge(a: &Sidecar, b: &Sidecar) -> MergeChoice {
        sidecar_merge(a, None, b, None)
    }

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
            upload_time_revision: 0,
            upload_epoch_ms: None,
            snapshot: false,
            store_checksum: None,
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

    /// A mirror **snapshot** record: `sync --to` content, `snapshot=true`. Both
    /// this and a proxy cache (`live(_, MIRROR)`, `snapshot=false`) replicate as
    /// truth now; the bit only records provenance.
    pub(crate) fn snapshot(sha: &str) -> Record {
        let mut record = live(sha, MIRROR);
        record
            .sidecar
            .as_mut()
            .expect("test record has a sidecar")
            .snapshot = true;
        record
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
    fn mirror_cache_replicates() {
        // A proxy-cache fill (`snapshot=false`) now replicates as truth like a
        // snapshot: it copies to an empty peer instead of staying bucket-local.
        assert_eq!(
            decide(&live("x", MIRROR), &absent()),
            Verdict::Copy(Side::A)
        );
        assert_eq!(
            decide(&absent(), &live("x", MIRROR)),
            Verdict::Copy(Side::B)
        );
        // Two mirror caches of the same name, DIFFERENT bytes under one immutable
        // filename: fail closed — an upstream-compromise signal (was a silent
        // Noop when caches were bucket-local).
        assert_eq!(
            decide(&live("x", MIRROR), &live("y", MIRROR)),
            Verdict::Freeze
        );
        // Same bytes, one cache yanked at a higher epoch: the yank converges.
        let mut yanked = live("x", MIRROR);
        yanked.sidecar = Some(sc("x", MIRROR, Yanked::Flag(true), 2));
        assert_eq!(
            decide(&yanked, &live("x", MIRROR)),
            Verdict::AdoptSidecar(Side::A)
        );
    }

    #[test]
    fn mirror_snapshot_copies_to_an_empty_peer() {
        // A `sync --to` snapshot (snapshot=true) copies to an empty peer, exactly
        // like a cache — the merge does not distinguish them.
        assert_eq!(decide(&snapshot("x"), &absent()), Verdict::Copy(Side::A));
        assert_eq!(decide(&absent(), &snapshot("x")), Verdict::Copy(Side::B));
    }

    #[test]
    fn same_bytes_never_un_yanks_across_provenance() {
        // The fail-open trap: a snapshot (unyanked, epoch 0) meets a cache
        // (yanked, epoch 0) of the same bytes. A "snapshot wins" pick would adopt
        // the snapshot's sidecar verbatim and silently un-yank. `yank_merge`
        // instead keeps the equal-epoch yanked side, so the withdrawal survives
        // regardless of which side is the operator snapshot.
        let mut snap = snapshot("x"); // unyanked, epoch 0, snapshot=true
        let mut cache = live("x", MIRROR); // epoch 0, snapshot=false
        cache.sidecar = Some(sc("x", MIRROR, Yanked::Flag(true), 0));
        assert_eq!(decide(&snap, &cache), Verdict::AdoptSidecar(Side::B));
        assert_eq!(decide(&cache, &snap), Verdict::AdoptSidecar(Side::A));
        // And the symmetric arrangement: snapshot yanked, cache clear.
        snap.sidecar = Some(sc("x", MIRROR, Yanked::Flag(true), 0));
        snap.sidecar.as_mut().expect("snapshot sidecar").snapshot = true;
        cache.sidecar = Some(sc("x", MIRROR, Yanked::Flag(false), 0));
        assert_eq!(decide(&snap, &cache), Verdict::AdoptSidecar(Side::A));
    }

    #[test]
    fn two_snapshots_same_bytes_converge_yank_state() {
        // The higher yank epoch wins, exactly like private — this is what makes a
        // mirror yank converge across buckets (gap 6).
        let mut yanked = snapshot("x");
        yanked.sidecar = Some(sc("x", MIRROR, Yanked::Flag(true), 2));
        yanked.sidecar.as_mut().expect("snapshot sidecar").snapshot = true;
        assert_eq!(
            decide(&yanked, &snapshot("x")),
            Verdict::AdoptSidecar(Side::A)
        );
        assert_eq!(
            decide(&snapshot("x"), &yanked),
            Verdict::AdoptSidecar(Side::B)
        );
    }

    #[test]
    fn two_mirror_bodies_diverging_bytes_freeze() {
        // Two byte-sets under one immutable name, no trustworthy order (mirror
        // sidecars carry no upload-epoch): fail closed, freeze both. Holds for
        // any mix of snapshot and cache — the merge does not distinguish them.
        assert_eq!(decide(&snapshot("x"), &snapshot("y")), Verdict::Freeze);
        assert_eq!(
            decide(&live("x", MIRROR), &live("y", MIRROR)),
            Verdict::Freeze
        );
        assert_eq!(decide(&snapshot("x"), &live("y", MIRROR)), Verdict::Freeze);
    }

    #[test]
    fn snapshot_paired_with_cache_converges() {
        // The previously-Noop mixed pair now converges coherently. Same bytes
        // differing only in the provenance bit: a deterministic, symmetric adopt
        // (never a Noop-forever divergent fixpoint). The two calls pick opposite
        // sides — the same physical outcome with the side swapped.
        let ab = decide(&snapshot("x"), &live("x", MIRROR));
        let ba = decide(&live("x", MIRROR), &snapshot("x"));
        assert!(matches!(ab, Verdict::AdoptSidecar(_)), "{ab:?}");
        assert_eq!(
            ba,
            match ab {
                Verdict::AdoptSidecar(Side::A) => Verdict::AdoptSidecar(Side::B),
                Verdict::AdoptSidecar(Side::B) => Verdict::AdoptSidecar(Side::A),
                other => panic!("expected an AdoptSidecar, got {other:?}"),
            }
        );
        // Different bytes freeze (covered by two_mirror_bodies_diverging_bytes_freeze);
        // an absent peer just gets a copy from whichever side is live.
        assert_eq!(decide(&snapshot("x"), &absent()), Verdict::Copy(Side::A));
        assert_eq!(
            decide(&absent(), &live("x", MIRROR)),
            Verdict::Copy(Side::B)
        );
    }

    #[test]
    fn private_still_beats_a_mirror_snapshot() {
        // The snapshot bit never lifts mirror above private truth.
        assert_eq!(
            decide(&live("x", PRIVATE), &snapshot("y")),
            Verdict::Supersede(Side::A)
        );
        assert_eq!(
            decide(&live("x", PRIVATE), &snapshot("x")),
            Verdict::AdoptSidecar(Side::A)
        );
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
    fn upload_time_repairs_never_outrank_a_yank() {
        // Partition: bucket A takes two timestamp repairs, bucket B one yank.
        let mut repaired = sc("x", PRIVATE, Yanked::Flag(false), 0);
        repaired.upload_time = "2020-01-01T00:00:00Z".into();
        repaired.upload_time_revision = 2;
        let yanked = sc("x", PRIVATE, Yanked::Flag(true), 1);
        assert_eq!(yank_merge(&repaired, &yanked), MergeChoice::B);
        assert_eq!(yank_merge(&yanked, &repaired), MergeChoice::A);
        // Between equal yank histories, the later repair wins.
        let mut older = repaired.clone();
        older.upload_time_revision = 1;
        assert_eq!(yank_merge(&repaired, &older), MergeChoice::A);
        assert_eq!(yank_merge(&older, &repaired), MergeChoice::B);
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

    /// A settled mirror→private demotion: the fence stands alone, the canonical
    /// key is empty, and the losing body sits in this bucket's `_quarantine/`.
    fn demoted() -> Record {
        Record {
            mirror_quarantined: true,
            ..absent()
        }
    }

    /// A demotion caught mid-settle: the fence is written but the mirror body
    /// and its sidecar are still standing under it.
    fn demoting(sha: &str) -> Record {
        Record {
            mirror_quarantined: true,
            ..live(sha, MIRROR)
        }
    }

    #[test]
    fn a_demotion_replicates_its_fence_to_a_peer_that_never_heard_of_it() {
        // The whole partitioned-lane failure: one bucket demoted a mirror
        // record and the peer holds nothing at all. This used to be `Noop` —
        // "the two sides agree" over a pair one key apart forever.
        assert_eq!(
            decide(&demoted(), &absent()),
            Verdict::SettleMirrorQuarantine
        );
        assert_eq!(
            decide(&absent(), &demoted()),
            Verdict::SettleMirrorQuarantine
        );
    }

    #[test]
    fn a_demotion_settles_a_peer_still_serving_the_withdrawn_mirror() {
        // The fail-open shape: the peer never saw the demotion and is still
        // serving the artifact the operator withdrew.
        assert_eq!(
            decide(&demoted(), &live("x", MIRROR)),
            Verdict::SettleMirrorQuarantine
        );
        assert_eq!(
            decide(&live("x", MIRROR), &demoted()),
            Verdict::SettleMirrorQuarantine
        );
        assert_eq!(
            decide(&demoted(), &snapshot("x")),
            Verdict::SettleMirrorQuarantine
        );
    }

    #[test]
    fn an_unsettled_demotion_keeps_firing_until_the_body_is_gone() {
        // Both fenced, but a canonical body still stands behind the fence: not
        // converged. Two buckets may even have demoted two different bodies —
        // the fence carries no hash, so they still settle to the same state.
        assert_eq!(
            decide(&demoting("x"), &demoting("x")),
            Verdict::SettleMirrorQuarantine
        );
        assert_eq!(
            decide(&demoting("x"), &demoting("y")),
            Verdict::SettleMirrorQuarantine
        );
        assert_eq!(
            decide(&demoted(), &demoting("x")),
            Verdict::SettleMirrorQuarantine
        );
    }

    #[test]
    fn a_settled_demotion_is_converged() {
        // Fence on both, canonical key empty on both: nothing owed, and the
        // verdict must not re-fire the executor on every diff.
        assert_eq!(decide(&demoted(), &demoted()), Verdict::Noop);
    }

    #[test]
    fn private_truth_still_supersedes_a_demotion_fence() {
        // Handing the filename to private truth is the entire point of a
        // demotion; the fence never blocks it.
        assert_eq!(
            decide(&live("x", PRIVATE), &demoted()),
            Verdict::Supersede(Side::A)
        );
        assert_eq!(
            decide(&demoted(), &live("x", PRIVATE)),
            Verdict::Supersede(Side::B)
        );
    }

    #[test]
    fn a_tombstone_subsumes_a_surviving_demotion_fence() {
        // Tombstone ≻ everything, and it is the stronger fence — so a marker
        // left standing beside one is debris, exactly like an orphaned sidecar.
        let mut settled = absent();
        settled.tombstoned = true;
        let mut with_marker = settled.clone();
        with_marker.mirror_quarantined = true;
        assert_eq!(decide(&settled, &with_marker), Verdict::Tombstone);
        assert_eq!(decide(&with_marker, &settled), Verdict::Tombstone);
        assert_eq!(
            decide(&with_marker, &with_marker.clone()),
            Verdict::Tombstone
        );
        // And the ordinary settled delete is still a no-op.
        assert_eq!(decide(&settled, &settled.clone()), Verdict::Noop);
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
        // A deferral, not a `Noop`: the orphan side holds no record, so the
        // pair is not converged and the caller still owes it one.
        assert_eq!(decide(&orphan, &live("x", PRIVATE)), Verdict::Defer);
        assert_eq!(decide(&live("x", PRIVATE), &orphan), Verdict::Defer);
        assert_eq!(decide(&orphan, &absent()), Verdict::Defer);
    }

    // ---------------------------------------------------------------------
    // Merge laws. Exhaustive over small domains: every ranking compares only
    // through `Ord` and byte equality, so a few values per field reach every
    // branch a larger domain would.
    // ---------------------------------------------------------------------

    fn mirrored(choice: MergeChoice) -> MergeChoice {
        match choice {
            MergeChoice::A => MergeChoice::B,
            MergeChoice::B => MergeChoice::A,
            MergeChoice::Equal => MergeChoice::Equal,
        }
    }

    /// `merge` must be a total order: reflexive, antisymmetric under a swap,
    /// and transitive. Transitivity is the convergence property — without it
    /// three buckets can hold a cycle and the fleet's answer depends on which
    /// pair synced first.
    fn assert_total_order<T: std::fmt::Debug>(items: &[T], merge: impl Fn(&T, &T) -> MergeChoice) {
        let at_least = |a: &T, b: &T| merge(a, b) != MergeChoice::B;
        for a in items {
            assert_eq!(merge(a, a), MergeChoice::Equal, "not reflexive: {a:?}");
            for b in items {
                assert_eq!(
                    merge(b, a),
                    mirrored(merge(a, b)),
                    "asymmetric: {a:?} vs {b:?}"
                );
                if !at_least(a, b) {
                    continue;
                }
                for c in items {
                    assert!(
                        !at_least(b, c) || at_least(a, c),
                        "cycle: {a:?} >= {b:?} >= {c:?} but not {a:?} >= {c:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn sidecar_merge_is_a_total_order() {
        let mut items = Vec::new();
        for origin in [None, Some(Origin::Private), Some(Origin::Mirror)] {
            for yank_epoch in 0..3 {
                for yanked in [
                    Yanked::Flag(false),
                    Yanked::Flag(true),
                    Yanked::Reason("r".into()),
                ] {
                    for upload_time_revision in 0..2 {
                        let mut sidecar = sc("x", PRIVATE, yanked.clone(), yank_epoch);
                        sidecar.origin = origin.map(|o| match o {
                            Origin::Private => PRIVATE.to_string(),
                            Origin::Mirror => MIRROR.to_string(),
                        });
                        sidecar.upload_time_revision = upload_time_revision;
                        items.push((sidecar, origin));
                    }
                }
            }
        }
        assert_total_order(&items, |(a, oa), (b, ob)| sidecar_merge(a, *oa, b, *ob));
    }

    #[test]
    fn status_merge_is_a_total_order() {
        use crate::status::ProjectStatus;
        let mut items = Vec::new();
        for origin in [None, Some(Origin::Private), Some(Origin::Mirror)] {
            for epoch in 0..3 {
                for (status, reason) in [
                    (ProjectStatus::Active, None),
                    (ProjectStatus::Deprecated, None),
                    (ProjectStatus::Quarantined, Some("r1")),
                    (ProjectStatus::Quarantined, Some("r2")),
                ] {
                    items.push(VersionedProjectStatus {
                        doc: ProjectStatusDoc {
                            status,
                            reason: reason.map(str::to_string),
                        },
                        epoch,
                        origin,
                        etag: String::new(),
                    });
                }
            }
        }
        assert_total_order(&items, |a, b| status_merge(Some(a), Some(b)));
        // Absence is the epoch-zero active default: equal to it, below the rest.
        for item in &items {
            let expected = if item.epoch == 0 && item.doc == ProjectStatusDoc::default() {
                MergeChoice::Equal
            } else {
                MergeChoice::A
            };
            assert_eq!(status_merge(Some(item), None), expected, "{item:?}");
            assert_eq!(
                status_merge(None, Some(item)),
                mirrored(expected),
                "{item:?}"
            );
        }
    }

    #[test]
    fn claim_join_is_a_join() {
        let claims = [None, Some(Origin::Mirror), Some(Origin::Private)];
        for a in claims {
            assert_eq!(claim_join(a, a), a);
            for b in claims {
                assert_eq!(claim_join(a, b), claim_join(b, a));
                for c in claims {
                    assert_eq!(
                        claim_join(claim_join(a, b), c),
                        claim_join(a, claim_join(b, c))
                    );
                }
            }
        }
        assert_eq!(
            claim_join(Some(Origin::Mirror), Some(Origin::Private)),
            Some(Origin::Private)
        );
        assert_eq!(claim_join(None, Some(Origin::Mirror)), Some(Origin::Mirror));
    }

    fn swap(side: Side) -> Side {
        match side {
            Side::A => Side::B,
            Side::B => Side::A,
        }
    }

    fn mirrored_verdict(verdict: Verdict) -> Verdict {
        match verdict {
            Verdict::Copy(side) => Verdict::Copy(swap(side)),
            Verdict::AdoptSidecar(side) => Verdict::AdoptSidecar(swap(side)),
            Verdict::Supersede(side) => Verdict::Supersede(swap(side)),
            Verdict::QuarantineLoser(side) => Verdict::QuarantineLoser(swap(side)),
            Verdict::PropagateFreeze(side) => Verdict::PropagateFreeze(swap(side)),
            other @ (Verdict::Noop
            | Verdict::Defer
            | Verdict::Freeze
            | Verdict::Tombstone
            | Verdict::FinishFreeze
            | Verdict::SettleMirrorQuarantine) => other,
        }
    }

    /// `decide(a, b)` and `decide(b, a)` name the same physical outcome, so a
    /// bidirectional diff can never double-apply or fight itself.
    #[test]
    fn decide_is_symmetric() {
        let mut sidecars = vec![None];
        for (sha, origin, epoch_ms) in [
            ("x", PRIVATE, Some(0)),
            ("x", MIRROR, None),
            ("y", PRIVATE, Some(10_000)),
            ("y", PRIVATE, Some(1_000)),
            ("y", MIRROR, None),
        ] {
            let mut sidecar = sc(sha, origin, Yanked::Flag(false), 0);
            sidecar.upload_epoch_ms = epoch_ms;
            sidecars.push(Some(sidecar));
        }
        let mut yanked = sc("x", PRIVATE, Yanked::Flag(true), 1);
        yanked.upload_epoch_ms = Some(0);
        sidecars.push(Some(yanked));

        let mut records = Vec::new();
        for sidecar in &sidecars {
            for bits in 0..16u8 {
                records.push(Record {
                    sidecar: sidecar.clone(),
                    has_artifact: bits & 1 != 0,
                    has_metadata: false,
                    has_provenance: false,
                    tombstoned: bits & 2 != 0,
                    frozen: bits & 4 != 0,
                    mirror_quarantined: bits & 8 != 0,
                    pkg_origin: None,
                });
            }
        }
        for a in &records {
            for b in &records {
                assert_eq!(
                    decide(b, a),
                    mirrored_verdict(decide(a, b)),
                    "asymmetric verdict for {a:?} vs {b:?}"
                );
            }
        }
    }
}
