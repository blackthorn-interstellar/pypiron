//! Multi-bucket replication and reconciliation.
//!
//! All durable package truth replicates — a private file's record, a `sync --to`
//! mirror snapshot, and a proxy-cache fill alike (artifact, sidecar,
//! `.metadata`/`.provenance` companions, origin claim, tombstone). A cache is no
//! longer treated as bucket-local re-derivable state: it converges too, just
//! asynchronously (a post-serve `_repl/` note, never pre-ack fan-out — the fill
//! path gains zero latency). Three tiers keep the buckets converged, fastest
//! first, each backstopping the one above:
//!
//! 1. **Synchronous fan-out** ([`fanout_sync`]): before an upload/delete/yank
//!    is acked, push the changed record to every other healthy bucket. A
//!    healthy fleet acks with N copies and no note written.
//! 2. **`_repl/` todo notes**: a fan-out that fails, times out, or is skipped
//!    (destination ineligible) drops a note in the bucket that took the write;
//!    nodes sweep them each tick ([`sweep_all_markers`]).
//! 3. **Full diff** ([`reconcile`]): a pairwise tree diff as the backstop for
//!    lost markers — the same copy path, with the merge rules armed.
//!
//! Every function here takes explicit `(source, destination)` storage handles:
//! this is the one sanctioned two-handle operation (§3 invariant 2). The merge
//! decision ([`decide`]) is a pure, symmetric function over durable record
//! state. Upload timestamps are only used for the rare private/private byte
//! conflict; the executor applies the decision with both handles.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context as _, Result};
use futures::StreamExt as _;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;
use tracing::{error, warn};

use crate::app::{AppState, PACKAGES_PREFIX};
use crate::buckets::Pinned;
use crate::hash::sha256_hex;
use crate::markers;
#[cfg(test)]
use crate::origin::read_origin;
use crate::origin::{
    claim_origin, read_origin_observation, ClaimRequest, OriginState, MIRROR, PRIVATE,
};
use crate::sidecar::{
    frozen_key, metadata_key, mirror_quarantined_key, provenance_key, sidecar_key, superseding_key,
    tombstone_key, Sidecar, FROZEN_SUFFIX, MIRROR_QUARANTINED_SUFFIX, SIDECAR_SUFFIX,
    TOMBSTONE_SUFFIX,
};
use crate::status::{self, StatusConvergence};
use crate::storage::{
    bounded_artifact_write, check_copy_checksum, create_artifact_verified, is_not_found,
    store_artifact_verified, verify_stored_size, ArtifactBody, ChecksumCheck, CopyOrigin,
    CopyOutcome, Existing, Storage,
};
use crate::tombstone;
use crate::upload::{TempPath, UploadSpool};
#[cfg(test)]
use crate::worker;

mod decide;
pub use decide::*;

/// Todo-marker prefix: `_repl/<dest-tag>/<pkg>/<file>!<nonce>`, an empty
/// object in the bucket that took the write (the `_dirty/` idiom pointed at a
/// second bucket). `<dest-tag>` is the destination bucket's stable
/// [`crate::counters::bucket_tag`], never its list position, so a topology
/// reorder/removal cannot orphan or misroute the note. O(1) at commit, consumed
/// and deleted on a successful push.
pub(crate) const REPL_PREFIX: &str = "_repl/";
/// Frozen bodies land here, content-hash-suffixed, so both sides of a byte
/// conflict are preserved as moves (never deletes): `_quarantine/<pkg>/<file>@<sha12>`.
pub(crate) const QUARANTINE_PREFIX: &str = "_quarantine/";
/// Bound on the origin-CAS retry loop in [`ensure_private_origin`]; the same
/// rationale as [`origin`]'s own claim loop — a pathological storm fails closed.
const ORIGIN_ATTEMPTS: usize = 8;
/// Page size for the paged `_repl/` sweep and the reconcile package scan: one
/// S3 LIST page. Bounds resident memory so neither the failure backlog nor the
/// full package tree is ever held in one Vec.
const REPL_SWEEP_PAGE: usize = 1_000;
const RECONCILE_SCAN_PAGE: usize = 1_000;

/// Read buffer for the streaming source read. One of these is resident per
/// copy in flight, and 16 copies run per destination.
const STREAM_CHUNK: usize = 1024 * 1024;
/// Above this, a copy stages the source artifact to a temp file instead of
/// holding it. A resident body multiplies by exactly the fan-out the sweep
/// runs — 16 concurrent copies per destination, destinations in parallel — so
/// an artifact-sized buffer is the same OOM the upload path removed by
/// spooling (a 900 MB wheel × 16 is 14 GB).
///
/// It is the multipart threshold itself, not a smaller number: at or below it
/// the destination write reads the whole file back into a `Vec` to issue one
/// conditional PUT, so staging any earlier buys a disk round-trip and bounds
/// nothing. Only past it does the write stream in parts, which is the point
/// where the temp file starts paying for itself. The two constants must move
/// together — hence the reference rather than a copy of the number.
const STAGE_TO_DISK_ABOVE: u64 = crate::storage::MULTIPART_THRESHOLD;

/// Package-level marker members share the ordinary durable fan-out queue. They
/// cannot collide with distribution filenames: valid artifacts never begin
/// with `.`.
pub const ORIGIN_MARKER: &str = ".origin";
pub const PROJECT_STATUS_MARKER: &str = ".project-status.json";

fn require_replication_unfenced(state: &AppState) -> Result<()> {
    if state.mutations_fenced() {
        bail!("bucket topology mismatch; replication writes are fenced");
    }
    Ok(())
}

/// Background replication may touch startup-validated idle buckets, but never
/// a bucket known unhealthy or one that recovered and still awaits topology
/// validation. Health probes and topology verification are the only operations
/// allowed to cross that gate.
fn bucket_eligible(state: &AppState, index: usize) -> bool {
    state
        .bucket_health
        .as_ref()
        .is_none_or(|health| health.bucket_eligible(index).unwrap_or(false))
}

fn artifact_key(pkg: &str, filename: &str) -> String {
    format!("{PACKAGES_PREFIX}{pkg}/{filename}")
}

// ---------------------------------------------------------------------------
// Reading a record from bucket state.
// ---------------------------------------------------------------------------

/// The package-level origin claim, as an [`Origin`] (fallback for legacy
/// sidecars). Missing/unclaimed is `None`; storage failures and malformed claim
/// values propagate. Treating either as "no origin" would silently strand
/// private truth instead of retaining its replication marker.
async fn read_pkg_origin(storage: &dyn Storage, pkg: &str) -> Result<Option<Origin>> {
    let Some(observed) = read_origin_observation(storage, pkg).await? else {
        return Ok(None);
    };
    // `Unclaimed` narrows to `None`; a claimed state to its record origin.
    Ok(observed.state.try_into().ok())
}

/// Build a [`Record`] from a package listing already in hand (the diff path):
/// object existence comes from `names`, and the sidecar is read only when
/// present. `pkg_origin` is read once per package by the caller.
async fn record_from_names(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    names: &HashSet<String>,
    pkg_origin: Option<Origin>,
) -> Result<Record> {
    let has_artifact = names.contains(filename);
    let has_metadata = names.contains(&format!("{filename}{}", crate::sidecar::METADATA_SUFFIX));
    let has_provenance =
        names.contains(&format!("{filename}{}", crate::sidecar::PROVENANCE_SUFFIX));
    let tombstoned = names.contains(&format!("{filename}{TOMBSTONE_SUFFIX}"));
    let frozen = names.contains(&format!("{filename}{FROZEN_SUFFIX}"));
    let mirror_quarantined = names.contains(&format!("{filename}{MIRROR_QUARANTINED_SUFFIX}"));
    let sidecar = if names.contains(&format!("{filename}{SIDECAR_SUFFIX}")) {
        let akey = artifact_key(pkg, filename);
        match storage.get_bytes(&sidecar_key(&akey)).await {
            Ok(bytes) => Some(
                serde_json::from_slice::<Sidecar>(&bytes)
                    .with_context(|| format!("parse sidecar for {akey}"))?,
            ),
            Err(e) => return Err(e),
        }
    } else {
        None
    };
    if let Some(raw) = sidecar.as_ref().and_then(|s| s.origin.as_deref()) {
        if Origin::parse(raw).is_none() {
            bail!("sidecar for {pkg}/{filename} holds an unexpected origin '{raw}'");
        }
    }
    // The package-origin fallback matters only for a live artifact whose sidecar
    // lacks a typed origin; otherwise carrying it would wrongly type a tombstone.
    let needs_fallback =
        has_artifact && sidecar.as_ref().and_then(|s| s.origin.as_deref()).is_none();
    Ok(Record {
        sidecar,
        has_artifact,
        has_metadata,
        has_provenance,
        tombstoned,
        frozen,
        mirror_quarantined,
        pkg_origin: needs_fallback.then_some(pkg_origin).flatten(),
    })
}

/// Build a [`Record`] with its own listing + origin read (the single-file eager
/// / marker-sweep path).
pub async fn read_record(storage: &dyn Storage, pkg: &str, filename: &str) -> Result<Record> {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let entries = storage.list_dir_entries(&prefix).await?;
    let names: HashSet<String> = entries
        .iter()
        .filter_map(|e| e.key.strip_prefix(&prefix).map(str::to_string))
        .collect();
    let pkg_origin = read_pkg_origin(storage, pkg).await?;
    record_from_names(storage, pkg, filename, &names, pkg_origin).await
}

// ---------------------------------------------------------------------------
// Executor — applies a Verdict with both handles.
// ---------------------------------------------------------------------------

/// Whether one merge pass left the two buckets agreed, or declined to act.
///
/// `Ok(())` used to mean both, and every caller had to remember that
/// [`Verdict::Defer`] hid inside the success. It did not: a pre-ack fan-out read
/// the deferral as convergence and acked an upload the peer did not hold, with
/// no `_repl/` note owing it (dev/DESIGN.md's totality principle). The
/// distinction lives in the type now, and `#[must_use]` keeps it from being
/// dropped on the floor again.
#[must_use]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Convergence {
    /// Both buckets agree: nothing is owed, a repair note may be consumed.
    Converged,
    /// The merge declined this pass ([`Verdict::Defer`]). The destination is
    /// still owed the record — leave (or write) its `_repl/` note.
    Deferred,
}

/// Apply the merge decision for one filename. `a`/`b` are the two storage
/// handles the records were read against; the executor writes to whichever the
/// verdict names and marks that bucket dirty so its own leader rebuilds it.
pub async fn execute(
    state: &AppState,
    stores: (&dyn Storage, &dyn Storage),
    pkg: &str,
    filename: &str,
    recs: (&Record, &Record),
    verdict: Verdict,
    source: ArtifactSource<'_>,
) -> Result<Convergence> {
    require_replication_unfenced(state)?;
    let (a, b) = stores;
    let (ra, rb) = recs;
    // Bracket every truth-mutating verdict in the same intent/commit marker
    // pair the write path uses, on both buckets. A bare post-mutation
    // mark_dirty is not crash-safe: an executor that dies between mutating a
    // bucket and signaling it leaves changed truth with no rebuild signal and
    // (when the copy itself completed) no repair note either — the view then
    // stays stale until that bucket's next audit. The intent lands before the
    // first mutation, so a crash anywhere leaves a marker that goes stale and
    // heals; the commit pairs it on success for an immediate rebuild.
    // Only the sides a verdict can mutate get the bracket — the pre-ack
    // fan-out runs the Copy path on every multi-bucket upload, and a pair of
    // no-op markers on the untouched source would be pure ack-latency.
    let (bracket_a, bracket_b) = match &verdict {
        Verdict::Noop | Verdict::Defer => (false, false),
        Verdict::Copy(side) | Verdict::Supersede(side) | Verdict::QuarantineLoser(side) => {
            match side {
                Side::A => (false, true), // the named side is the source
                Side::B => (true, false),
            }
        }
        Verdict::PropagateFreeze(side) => match side {
            Side::A => (false, true), // the named side already carries the marker
            Side::B => (true, false),
        },
        Verdict::AdoptSidecar(_)
        | Verdict::Freeze
        | Verdict::FinishFreeze
        | Verdict::Tombstone
        | Verdict::SettleMirrorQuarantine => (true, true),
    };
    let intent_a = if bracket_a {
        Some(markers::mark_intent(a, pkg).await?)
    } else {
        None
    };
    let intent_b = if bracket_b {
        match markers::mark_intent(b, pkg).await {
            Ok(nonce) => Some(nonce),
            Err(error) => {
                // Pair the first intent before propagating: a mere storage
                // error on the second bucket must not leave a fresh unpaired
                // intent deferring the package for the whole grace period.
                if let Some(nonce) = &intent_a {
                    let _ = markers::mark_commit(a, pkg, nonce).await;
                }
                return Err(error);
            }
        }
    } else {
        None
    };
    let pick = |side: Side| -> (&dyn Storage, &dyn Storage, &Record) {
        match side {
            Side::A => (a, b, ra),
            Side::B => (b, a, rb),
        }
    };
    let result = async {
        // Independent of the verdict, on both sides: a demotion fence standing
        // beside a live PRIVATE record is spent, and it is the one key two
        // buckets can otherwise never agree on.
        clear_spent_demotion_fence(a, pkg, filename, ra).await?;
        clear_spent_demotion_fence(b, pkg, filename, rb).await?;
        match verdict {
        Verdict::Noop => {
            let (dirty_a, dirty_b) =
                repair_same_sha_companions((a, b), pkg, filename, (ra, rb)).await?;
            if dirty_a {
                markers::mark_dirty(a, pkg).await?;
            }
            if dirty_b {
                markers::mark_dirty(b, pkg).await?;
            }
        }
        // No I/O: only the orphan side's own audit can produce the sidecar this
        // decision needs, and acting without one would fabricate truth (§4).
        Verdict::Defer => {}
        Verdict::Copy(side) => {
            let (src, dst, rec) = pick(side);
            if copy_live(state, src, dst, pkg, filename, rec, source).await? {
                markers::mark_dirty(dst, pkg).await?;
            }
        }
        Verdict::AdoptSidecar(_) => {
            // The verdict was computed from a listing-era read. Re-read both
            // sidecars, origins, and etags and recompute the complete precedence
            // order. Origin beats yank: a high-epoch mirror sidecar must never
            // overwrite private truth after a claim demotion.
            let (adopted_a, adopted_b) = adopt_sidecar_cas(a, b, pkg, filename).await?;
            let (dirty_a, dirty_b) =
                repair_same_sha_companions((a, b), pkg, filename, (ra, rb)).await?;
            if adopted_a || dirty_a {
                markers::mark_dirty(a, pkg).await?;
            }
            if adopted_b || dirty_b {
                markers::mark_dirty(b, pkg).await?;
            }
        }
        Verdict::Supersede(side) => {
            let (src, dst, record) = pick(side);
            supersede_record(state, src, dst, pkg, filename, record, source).await?;
        }
        Verdict::QuarantineLoser(winner) => {
            let (src, dst, record) = pick(winner);
            let loser = match winner {
                Side::A => rb,
                Side::B => ra,
            };
            error!(
                package = %pkg,
                filename = %filename,
                winner = ?winner,
                winner_sha = record.sidecar.as_ref().map(|s| s.sha256.as_str()).unwrap_or("?"),
                loser_sha = loser.sidecar.as_ref().map(|s| s.sha256.as_str()).unwrap_or("?"),
                "byte conflict: first-uploaded kept, loser quarantined; operator review required"
            );
            supersede_record(state, src, dst, pkg, filename, record, source).await?;
            state
                .metrics
                .replication_conflict_quarantines
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Verdict::Freeze => {
            error!(
                package = %pkg,
                filename = %filename,
                sha_a = ra.sidecar.as_ref().map(|s| s.sha256.as_str()).unwrap_or("?"),
                sha_b = rb.sidecar.as_ref().map(|s| s.sha256.as_str()).unwrap_or("?"),
                "byte conflict: same filename, different bytes on two buckets — frozen on both, quarantined, suppressed from indexes; resolve by republishing a new version"
            );
            freeze_side(a, pkg, filename).await?;
            freeze_side(b, pkg, filename).await?;
            markers::mark_dirty(a, pkg).await?;
            markers::mark_dirty(b, pkg).await?;
            state
                .metrics
                .replication_freezes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Verdict::PropagateFreeze(frozen_side) => {
            // Freeze the side that lacks the marker.
            let target: &dyn Storage = match frozen_side {
                Side::A => b,
                Side::B => a,
            };
            freeze_side(target, pkg, filename).await?;
            markers::mark_dirty(target, pkg).await?;
        }
        Verdict::FinishFreeze => {
            freeze_side(a, pkg, filename).await?;
            freeze_side(b, pkg, filename).await?;
            markers::mark_dirty(a, pkg).await?;
            markers::mark_dirty(b, pkg).await?;
        }
        // A mirror→private demotion one side has not heard of, or has not
        // finished. Symmetric and idempotent: both buckets end fenced with an
        // empty canonical key, each holding in its own `_quarantine/` whatever
        // body it personally lost.
        Verdict::SettleMirrorQuarantine => {
            if settle_mirror_quarantine(a, pkg, filename).await? {
                markers::mark_dirty(a, pkg).await?;
            }
            if settle_mirror_quarantine(b, pkg, filename).await? {
                markers::mark_dirty(b, pkg).await?;
            }
        }
        Verdict::Tombstone => {
            // A freeze carries a tombstone solely as its permanent upload
            // fence. Preserve and propagate the richer diagnostic marker when
            // tombstone precedence selects this branch; otherwise the next
            // pairwise hop would degrade a fleet-wide freeze into a bare delete.
            let (ca, cb) = if ra.frozen || rb.frozen {
                freeze_side(a, pkg, filename).await?;
                freeze_side(b, pkg, filename).await?;
                (true, true)
            } else {
                (
                    tombstone_side(a, pkg, filename).await?,
                    tombstone_side(b, pkg, filename).await?,
                )
            };
            if ca {
                markers::mark_dirty(a, pkg).await?;
            }
            if cb {
                markers::mark_dirty(b, pkg).await?;
            }
        }
        }
        Ok(if verdict == Verdict::Defer {
            Convergence::Deferred
        } else {
            Convergence::Converged
        })
    }
    .await;
    // Pair the intents even on error: the mutation may be partial, and an
    // immediate rebuild-from-truth converges the views either way. A commit
    // that fails to write leaves its intent to go stale and heal — the signal
    // is never lost, only delayed by the grace period.
    if let Some(nonce) = intent_a {
        let _ = markers::mark_commit(a, pkg, &nonce).await;
    }
    if let Some(nonce) = intent_b {
        let _ = markers::mark_commit(b, pkg, &nonce).await;
    }
    result
}

/// Conditionally converge an immutable artifact's sidecar. Returns which side
/// changed. A lost CAS re-reads both origins and sidecars and recomputes the
/// full origin-then-yank precedence; no stale or blind overwrite is allowed.
async fn adopt_sidecar_cas(
    a: &dyn Storage,
    b: &dyn Storage,
    pkg: &str,
    filename: &str,
) -> Result<(bool, bool)> {
    let key = sidecar_key(&artifact_key(pkg, filename));
    for _ in 0..ORIGIN_ATTEMPTS {
        let ((left, right), (left_claim, right_claim)) = futures::future::try_join(
            futures::future::try_join(a.get_with_etag(&key), b.get_with_etag(&key)),
            futures::future::try_join(read_pkg_origin(a, pkg), read_pkg_origin(b, pkg)),
        )
        .await?;
        let (Some((left_bytes, left_etag)), Some((right_bytes, right_etag))) = (left, right) else {
            bail!("sidecar changed while reconciling {pkg}/{filename}");
        };
        let left: Sidecar = serde_json::from_slice(&left_bytes)
            .with_context(|| format!("parse sidecar {key} on left bucket"))?;
        let right: Sidecar = serde_json::from_slice(&right_bytes)
            .with_context(|| format!("parse sidecar {key} on right bucket"))?;
        if left.sha256 != right.sha256 {
            bail!("artifact conflict appeared while reconciling {pkg}/{filename}");
        }
        let sidecar_origin = |sidecar: &Sidecar, fallback: Option<Origin>| -> Result<Origin> {
            match sidecar.origin.as_deref() {
                Some(raw) => Origin::parse(raw)
                    .ok_or_else(|| anyhow!("sidecar {key} holds an unexpected origin '{raw}'")),
                None => fallback.ok_or_else(|| anyhow!("sidecar {key} has no typed origin")),
            }
        };
        let left_origin = sidecar_origin(&left, left_claim)?;
        let right_origin = sidecar_origin(&right, right_claim)?;
        let choice = match (left_origin, right_origin) {
            (Origin::Private, Origin::Mirror) => MergeChoice::A,
            (Origin::Mirror, Origin::Private) => MergeChoice::B,
            // Any two mirror records of the same bytes converge their yank state
            // (§6.5) — snapshot, cache, or a mixed pair. yank_merge is fail-closed
            // (a yanked side is never un-yanked by the peer's provenance); the
            // snapshot bit rides the winner's sidecar, matching `same_bytes`.
            (Origin::Mirror, Origin::Mirror) | (Origin::Private, Origin::Private) => {
                yank_merge(&left, &right)
            }
        };
        match choice {
            MergeChoice::Equal => return Ok((false, false)),
            MergeChoice::A => {
                if b.put_if_match(&key, &right_etag, left_bytes)
                    .await?
                    .is_some()
                {
                    return Ok((false, true));
                }
            }
            MergeChoice::B => {
                if a.put_if_match(&key, &left_etag, right_bytes)
                    .await?
                    .is_some()
                {
                    return Ok((true, false));
                }
            }
        }
    }
    bail!("sidecar changed repeatedly while reconciling {pkg}/{filename}")
}

fn json() -> Option<&'static str> {
    Some("application/json")
}

/// Fill missing companions for two records that already agree on artifact bytes.
/// Private/private and mirror/mirror both union (a replicated cache's companions
/// converge too); private/mirror only flows from the private side. Returns which
/// bucket's index needs rebuilding.
async fn repair_same_sha_companions(
    stores: (&dyn Storage, &dyn Storage),
    pkg: &str,
    filename: &str,
    recs: (&Record, &Record),
) -> Result<(bool, bool)> {
    let (a, b) = stores;
    let (ra, rb) = recs;
    let (oa, ob) = match (ra.state(), rb.state()) {
        (
            RecordState::Live {
                sha: sha_a,
                origin: origin_a,
                ..
            },
            RecordState::Live {
                sha: sha_b,
                origin: origin_b,
                ..
            },
        ) if sha_a == sha_b => (origin_a, origin_b),
        _ => return Ok((false, false)),
    };
    match (oa, ob) {
        (Origin::Private, Origin::Private) => {
            let dirty_b = copy_missing_companions(a, b, pkg, filename, ra, rb).await?;
            let dirty_a = copy_missing_companions(b, a, pkg, filename, rb, ra).await?;
            Ok((dirty_a, dirty_b))
        }
        (Origin::Private, Origin::Mirror) => Ok((
            false,
            copy_missing_companions(a, b, pkg, filename, ra, rb).await?,
        )),
        (Origin::Mirror, Origin::Private) => Ok((
            copy_missing_companions(b, a, pkg, filename, rb, ra).await?,
            false,
        )),
        (Origin::Mirror, Origin::Mirror) => {
            let dirty_b = copy_missing_companions(a, b, pkg, filename, ra, rb).await?;
            let dirty_a = copy_missing_companions(b, a, pkg, filename, rb, ra).await?;
            Ok((dirty_a, dirty_b))
        }
    }
}

async fn copy_missing_companions(
    src: &dyn Storage,
    dst: &dyn Storage,
    pkg: &str,
    filename: &str,
    src_record: &Record,
    dst_record: &Record,
) -> Result<bool> {
    let akey = artifact_key(pkg, filename);
    let metadata = metadata_key(&akey);
    let provenance = provenance_key(&akey);
    let mut changed = false;
    changed |= copy_missing_object(
        src,
        dst,
        &metadata,
        "text/plain; charset=utf-8",
        src_record.has_metadata,
        dst_record.has_metadata,
    )
    .await?;
    changed |= copy_missing_object(
        src,
        dst,
        &provenance,
        "application/json",
        src_record.has_provenance,
        dst_record.has_provenance,
    )
    .await?;
    Ok(changed)
}

/// Copy a listing-proven missing object. Losing the conditional create is
/// harmless only if the winner wrote identical bytes; otherwise retain the
/// replication marker and make the discrepancy loud.
async fn copy_missing_object(
    src: &dyn Storage,
    dst: &dyn Storage,
    key: &str,
    content_type: &str,
    source_has: bool,
    destination_has: bool,
) -> Result<bool> {
    if !source_has || destination_has {
        return Ok(false);
    }
    let bytes = src
        .get_bytes(key)
        .await
        .with_context(|| format!("read source companion {key}"))?;
    put_if_absent_or_verify(dst, key, bytes, Some(content_type)).await
}

/// Publish a small companion (sidecar-adjacent `.metadata`/`.provenance`, a
/// quarantine marker) under an immutable key. The bytes are their own identity,
/// so a create is verified (D1), a match dedups, and a wrong-sha body — stale
/// crash debris, e.g. a zero-byte companion a failed write left behind — is
/// repaired in place rather than blocking every future sweep on a `bail!`.
async fn put_if_absent_or_verify(
    storage: &dyn Storage,
    key: &str,
    bytes: Vec<u8>,
    content_type: Option<&str>,
) -> Result<bool> {
    let sha = sha256_hex(&bytes);
    let size = bytes.len() as u64;
    store_artifact_verified(
        storage,
        key,
        ArtifactBody::Bytes(bytes),
        size,
        content_type,
        Existing::Repair(&sha),
    )
    .await
}

struct VerifiedSource {
    artifact: StagedArtifact,
    metadata: Option<Vec<u8>>,
    provenance: Option<Vec<u8>>,
}

/// A running sha256 over a body that must not outgrow the length its own
/// sidecar attests. The cap is what makes the streaming read *bounded* rather
/// than merely incremental: without it a source whose body no longer matches
/// its sidecar — the exact case this read exists to catch — still gets to
/// decide how much this node accumulates before the hash disagrees.
struct HashingSink {
    hasher: Sha256,
    read: u64,
    limit: u64,
}

impl HashingSink {
    fn new(limit: u64) -> Self {
        Self {
            hasher: Sha256::new(),
            read: 0,
            limit,
        }
    }

    /// For a local file this node already owns and is not accumulating: there
    /// is nothing for a cap to protect, and the sha256 below is the check.
    fn unbounded() -> Self {
        Self::new(u64::MAX)
    }

    fn accept(&mut self, chunk: &[u8]) -> Result<()> {
        self.read += chunk.len() as u64;
        if self.read > self.limit {
            bail!(
                "source body is longer than the {} bytes its sidecar attests; abandoning the read",
                self.limit
            );
        }
        self.hasher.update(chunk);
        Ok(())
    }

    fn finish(self) -> (String, u64) {
        (format!("{:x}", self.hasher.finalize()), self.read)
    }
}

/// The source artifact, sha-verified against the source sidecar and held where
/// a destination write can take it without a second full-body read.
enum StagedArtifact {
    /// Small enough that the destination's own write would buffer it anyway.
    Resident(Vec<u8>),
    /// A large body as a local file: the upload spool the publish fan-out
    /// already holds (nothing was copied — it is hashed where it lies), or a
    /// temp file this copy streamed the source into. `_temp` owns the deletion;
    /// a spool the caller owns has none.
    File {
        path: PathBuf,
        size: u64,
        _temp: Option<TempPath>,
    },
}

impl StagedArtifact {
    /// How the destination write takes these bytes. `Spool` lands through
    /// [`Storage::put_file_if_absent`] — a hardlink on disk, multipart-then-
    /// publish on the object stores — so a large body never becomes a `Vec`.
    fn body(&self) -> ArtifactBody<'_> {
        match self {
            // Cloned, not moved: `artifact_leg` may need these bytes again for
            // the conditional repair below. Bounded by [`STAGE_TO_DISK_ABOVE`].
            StagedArtifact::Resident(bytes) => ArtifactBody::Bytes(bytes.clone()),
            StagedArtifact::File { path, .. } => ArtifactBody::Spool(path),
        }
    }

    fn size(&self) -> u64 {
        match self {
            StagedArtifact::Resident(bytes) => bytes.len() as u64,
            StagedArtifact::File { size, .. } => *size,
        }
    }

    /// The whole body, resident. Only the two legs that *replace* an existing
    /// object need it: a conditional replace carries no streaming form —
    /// `put_multipart` takes no precondition on any backend — and the etag
    /// condition is what keeps a racing publisher's body from being clobbered,
    /// so it is not tradeable for the buffer. Both legs run only when the
    /// destination already holds a body under an immutable key (crash debris,
    /// or a conflict being quarantined), never on the copy that lands one.
    async fn read_all(&self) -> Result<Vec<u8>> {
        match self {
            StagedArtifact::Resident(bytes) => Ok(bytes.clone()),
            StagedArtifact::File { path, .. } => tokio::fs::read(path)
                .await
                .with_context(|| format!("read staged artifact {}", path.display())),
        }
    }
}

/// sha256 a local file without holding it in memory, returning its length too.
async fn hash_file(path: &Path) -> Result<(String, u64)> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let mut sink = HashingSink::unbounded();
    let mut buf = vec![0u8; STREAM_CHUNK];
    loop {
        let n = file
            .read(&mut buf)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        if n == 0 {
            break;
        }
        sink.accept(&buf[..n])?;
    }
    Ok(sink.finish())
}

/// Stream the source artifact out of the bucket, hashing every chunk as it
/// arrives, and land it wherever the destination write can take it from.
/// Nothing here ever holds more than [`STAGE_TO_DISK_ABOVE`] of the body.
async fn stage_from_bucket(
    state: &AppState,
    src: &dyn Storage,
    akey: &str,
    expected_size: u64,
) -> Result<(String, StagedArtifact)> {
    let mut body = src
        .serve_artifact(akey, None)
        .await
        .with_context(|| format!("read source artifact {akey}"))?
        .into_body()
        .into_data_stream();
    if expected_size <= STAGE_TO_DISK_ABOVE {
        // Capped at the staging threshold, not at the attested size: a body
        // that outgrew its own sidecar is the torn record the caller repairs
        // from the real hash, and truncating the read here would deny it that
        // hash. RAM is still bounded — by the threshold this branch was chosen
        // under. Past it, the file branch below caps at the attested size and
        // the copy fails loudly instead: at that scale a torn record is worth
        // a `_repl/` note and a later pass, not an unbounded read.
        let mut sink = HashingSink::new(STAGE_TO_DISK_ABOVE);
        let mut bytes = Vec::with_capacity(expected_size as usize);
        while let Some(chunk) = body.next().await {
            let chunk = chunk.with_context(|| format!("read source artifact {akey}"))?;
            sink.accept(&chunk)?;
            bytes.extend_from_slice(&chunk);
        }
        return Ok((sink.finish().0, StagedArtifact::Resident(bytes)));
    }
    // The upload spool, reused: it opens O_EXCL 0600, hashes as it writes, and
    // deletes itself on drop — every property a copy of somebody's private
    // bytes through this node's filesystem needs, already written once.
    tracing::debug!(
        key = %akey,
        size = expected_size,
        "staging the replication source to a spool file"
    );
    let mut spool = UploadSpool::new(&state.spool_dir)
        .await
        .context("open a spool for the replication copy")?;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.with_context(|| format!("read source artifact {akey}"))?;
        spool.write_chunk(&chunk).await?;
        if spool.size() > expected_size {
            bail!(
                "source artifact {akey} is longer than the {expected_size} bytes its sidecar \
                 attests; abandoning the copy"
            );
        }
    }
    let finished = spool.finish().await?;
    let path = finished.path.path().to_path_buf();
    Ok((
        finished.sha256,
        StagedArtifact::File {
            path,
            size: finished.size,
            _temp: Some(finished.path),
        },
    ))
}

/// Where a copy reads the artifact bytes it verifies against the source
/// sidecar. `Bucket` is the universal source — the sweep, reconcile, and every
/// peer of an already-committed record read the artifact back from the source
/// bucket. A live upload additionally still holds its just-written, verified
/// spool, so its pre-ack fan-out reads that local file once per peer instead of
/// GETting the same bytes back out of the source bucket (~one full artifact GET
/// saved per upload). The sha256 check against the sidecar is byte-identical
/// either way: the spool never bypasses verification.
#[derive(Clone, Copy)]
pub enum ArtifactSource<'a> {
    Bucket,
    Spool(&'a std::path::Path),
}

impl ArtifactSource<'_> {
    /// Produce the source bytes as a staged artifact, plus their sha256. The
    /// bucket source streams; the spool is already a local file, so it is
    /// hashed where it lies and handed on by path — no copy, and the fan-out's
    /// peers no longer each read a whole artifact into memory.
    async fn stage(
        &self,
        state: &AppState,
        src: &dyn Storage,
        akey: &str,
        expected_size: u64,
    ) -> Result<(String, StagedArtifact)> {
        match self {
            ArtifactSource::Bucket => stage_from_bucket(state, src, akey, expected_size).await,
            ArtifactSource::Spool(path) => {
                let (sha, size) = hash_file(path)
                    .await
                    .with_context(|| format!("hash upload spool {} for fan-out", path.display()))?;
                Ok((
                    sha,
                    StagedArtifact::File {
                        path: path.to_path_buf(),
                        size,
                        _temp: None,
                    },
                ))
            }
        }
    }
}

async fn verify_source_record(
    state: &AppState,
    src: &dyn Storage,
    pkg: &str,
    filename: &str,
    record: &Record,
    source: ArtifactSource<'_>,
) -> Result<VerifiedSource> {
    let akey = artifact_key(pkg, filename);
    let artifact = verify_source_artifact(state, src, pkg, filename, record, source).await?;
    let Companions {
        metadata,
        provenance,
    } = read_source_companions(src, &akey, record).await?;
    Ok(VerifiedSource {
        artifact,
        metadata,
        provenance,
    })
}

/// The source's listed companions (metadata, provenance). Small and re-authored
/// on the destination whichever artifact transport is used, so they are read
/// separately from the (large) artifact bytes.
struct Companions {
    metadata: Option<Vec<u8>>,
    provenance: Option<Vec<u8>>,
}

async fn read_source_companions(
    src: &dyn Storage,
    akey: &str,
    record: &Record,
) -> Result<Companions> {
    let metadata = read_listed_companion(src, &metadata_key(akey), record.has_metadata).await?;
    let provenance =
        read_listed_companion(src, &provenance_key(akey), record.has_provenance).await?;
    Ok(Companions {
        metadata,
        provenance,
    })
}

/// Read the source artifact and verify it against the source sidecar's
/// sha256 — the check the server-side copy transport skips (bytes never touch
/// this node) and the stream transport relies on. A torn record (sidecar names
/// bytes the body no longer holds) is repaired for a rebuild here.
///
/// The bytes are hashed as they stream and staged, never assembled: this is the
/// one place in the fleet where every cross-provider, cross-endpoint,
/// cross-credential and disk pair moves an artifact, and it used to hold each
/// one whole, 16 at a time per destination.
async fn verify_source_artifact(
    state: &AppState,
    src: &dyn Storage,
    pkg: &str,
    filename: &str,
    record: &Record,
    source: ArtifactSource<'_>,
) -> Result<StagedArtifact> {
    let sidecar = record
        .sidecar
        .as_ref()
        .ok_or_else(|| anyhow!("copy verdict with no source sidecar"))?;
    let akey = artifact_key(pkg, filename);
    let (got, artifact) = source.stage(state, src, &akey, sidecar.size).await?;
    if got != sidecar.sha256 {
        // A spool whose bytes disagree with the sidecar built from the same
        // upload is a caller bug, not crash debris: fail loudly rather than run
        // the bucket source's torn-record repair against a local file.
        if let ArtifactSource::Spool(path) = source {
            bail!(
                "upload spool {} hashes to {got} but its sidecar names {} for {akey}",
                path.display(),
                sidecar.sha256
            );
        }
        // A live body whose sidecar names different bytes is a torn record:
        // the immutable name was freed (a failed publish deletes its own
        // unacked debris) and re-created while a sidecar fabricated for the
        // dead bytes survived. The body is the only truth those bytes have —
        // drop the stale sidecar and signal a rebuild so the tick path
        // refabricates it from the body; the next reconcile pass copies the
        // healed record. Re-read before deleting: a real writer may have
        // already replaced the sidecar since this record was listed.
        let sckey = sidecar_key(&akey);
        let still_stale = match src.get_bytes(&sckey).await {
            Ok(bytes) => serde_json::from_slice::<Sidecar>(&bytes)
                .map(|current| current.sha256 != got)
                .unwrap_or(false),
            Err(_) => false,
        };
        if still_stale {
            src.delete_keys(std::slice::from_ref(&sckey))
                .await
                .with_context(|| format!("drop stale sidecar {sckey}"))?;
            markers::mark_dirty(src, pkg)
                .await
                .with_context(|| format!("mark {pkg} dirty after dropping stale sidecar"))?;
        }
        bail!(
            "source artifact sha mismatch for {akey}: sidecar {}, bytes {got}; stale sidecar dropped for rebuild",
            sidecar.sha256
        );
    }
    Ok(artifact)
}

async fn read_listed_companion(
    storage: &dyn Storage,
    key: &str,
    listed: bool,
) -> Result<Option<Vec<u8>>> {
    if !listed {
        return Ok(None);
    }
    storage
        .get_bytes(key)
        .await
        .map(Some)
        .with_context(|| format!("read source companion {key}"))
}

/// Install private sidecar truth under CAS. A body-less sidecar is inert and
/// may be replaced; a mirror sidecar also yields to private truth. Once the
/// destination body already matches the incoming sha, a different-sha sidecar
/// is necessarily stale crash debris and may be repaired. A different-sha
/// private sidecar backed by a different live body remains a hard conflict.
async fn install_or_verify_sidecar(
    dst: &dyn Storage,
    key: &str,
    sidecar: &Sidecar,
) -> Result<bool> {
    if sidecar.origin.as_deref() != Some(PRIVATE) {
        bail!("replication source sidecar at {key} is not private truth");
    }
    let bytes = serde_json::to_vec(sidecar)?;
    let artifact = key
        .strip_suffix(SIDECAR_SUFFIX)
        .ok_or_else(|| anyhow!("sidecar key has no {SIDECAR_SUFFIX} suffix: {key}"))?;
    for _ in 0..ORIGIN_ATTEMPTS {
        let Some((current_bytes, etag)) = dst.get_with_etag(key).await? else {
            if dst.put_if_absent(key, bytes.clone(), json()).await? {
                return Ok(true);
            }
            continue;
        };
        if current_bytes == bytes {
            return Ok(false);
        }
        let current: Sidecar = serde_json::from_slice(&current_bytes)
            .with_context(|| format!("parse destination sidecar {key}"))?;
        if let Some(raw) = current.origin.as_deref() {
            if Origin::parse(raw).is_none() {
                bail!("sidecar at {key} holds an unexpected origin '{raw}'");
            }
        }
        let body = match dst.get_bytes(artifact).await {
            Ok(body) => Some(body),
            Err(error) if is_not_found(&error) => None,
            Err(error) => return Err(error),
        };
        let body_matches = body
            .as_deref()
            .is_some_and(|body| sha256_hex(body) == sidecar.sha256);
        let replace = if current.sha256 != sidecar.sha256 {
            body.is_none() || body_matches || current.origin.as_deref() == Some(MIRROR)
        } else {
            match current.origin.as_deref() {
                Some(MIRROR) | None => true,
                Some(PRIVATE) => yank_merge(sidecar, &current) == MergeChoice::A,
                Some(_) => false,
            }
        };
        if !replace {
            if current.sha256 != sidecar.sha256 {
                bail!(
                    "destination sidecar at {key} names sha {} backed by different private bytes; expected {}",
                    current.sha256,
                    sidecar.sha256
                );
            }
            return Ok(false);
        }
        if dst.put_if_match(key, &etag, bytes.clone()).await?.is_some() {
            return Ok(true);
        }
    }
    bail!("conditional sidecar replacement retries exhausted for {key}")
}

/// Copy a live private record into `dst`:
/// origin claim first, then sidecar, companions, and the artifact **last** —
/// each verified against the source sidecar's sha256, never an etag or raw
/// presence. The source artifact and listed companions are read before the
/// first destination mutation, so a bad source can never publish a sidecar.
/// Returns whether destination truth changed and therefore needs an index
/// rebuild.
async fn copy_live(
    state: &AppState,
    src: &dyn Storage,
    dst: &dyn Storage,
    pkg: &str,
    filename: &str,
    record: &Record,
    source: ArtifactSource<'_>,
) -> Result<bool> {
    let sc = record
        .sidecar
        .as_ref()
        .ok_or_else(|| anyhow!("copy verdict with no source sidecar"))?;
    let akey = artifact_key(pkg, filename);
    // Companions are small and re-authored on the destination whichever way the
    // artifact bytes move; read them up front. The (large) artifact bytes are
    // read only on the stream path — the server-side copy transport never pulls
    // them through this node.
    let companions = read_source_companions(src, &akey, record).await?;
    // When the boot matrix says this destination can pull this source
    // server-side, that is the artifact transport; on any miss the ladder falls
    // through to streaming. Only that transport publishes the sidecar as its
    // divergence gate — its copy verb has no create-if-absent, so a pre-check is
    // the only gate it can have. Every streamed record lands its bytes first.
    let copy_origin = copy_transport(state, src, dst);
    // Stream transport reads + sha-verifies the source bytes *before* the first
    // destination mutation, so a bad source never publishes a sidecar. The copy
    // transport cannot pre-verify (the bytes never touch this node); it trusts
    // the provider's byte-exact copy and the post-copy size check. Nothing
    // downstream re-checks that trust — the reconcile sha-diff compares the two
    // *sidecars*, so a body contradicting its own sidecar is invisible to it.
    // What guards this leg is the two pre-copy bails below: the destination's
    // origin claim and its sidecar are each read for private truth, and either
    // stops the copy before `CopyObject` (which has no create-if-absent) can put
    // mirror bytes under a private claim. The residual race is the milliseconds
    // between those reads and the copy verb, against the listing-era window this
    // replaced; closing it outright needs a destination-conditional copy, which
    // no provider's copy verb offers.
    let mut streamed = match &copy_origin {
        None => Some(verify_source_artifact(state, src, pkg, filename, record, source).await?),
        Some(_) => None,
    };
    require_replication_unfenced(state)?;

    // A `sync --to` snapshot (mirror origin, replicate=true) replicates as
    // truth on the mirror-safe path: it claims MIRROR create-if-absent (never
    // demoting a private claim) and installs a mirror sidecar (never overwriting
    // private truth). Private records take the private path unchanged.
    let is_mirror = matches!(record.origin(), Some(Origin::Mirror));
    // Origin claim first, ahead of the artifact — shrinks the dependency-
    // confusion window (§4): the name is claimed before its bytes land.
    if is_mirror {
        // A destination that went private since the listing-era read owns this
        // name outright — private is terminal and outranks mirror everywhere.
        // `ensure_mirror_origin` yields to that claim instead of failing, so the
        // yield has to be read here: continuing would replicate a public name
        // onto a bucket where it is somebody's private package (§4).
        if ensure_mirror_origin(dst, pkg).await? == Origin::Private {
            bail!(
                "destination bucket {} claims '{pkg}' private; refusing to replicate mirror record {filename} from {}",
                bucket_name(dst),
                bucket_name(src)
            );
        }
    } else {
        ensure_private_origin(dst, pkg).await?;
    }
    // Artifact leg first whenever this node already holds the sha-verified
    // bytes — private truth and a `sync --to` snapshot alike, since the stream
    // transport verifies both before the first destination mutation. The
    // destination's immutable create is a strictly stronger gate than its
    // sidecar: nothing can land a body under a key we already own, so the
    // sidecar installed after it can only ever describe bytes this bucket holds.
    // Owning it is not forever, though — a demotion settle may empty the key
    // between the two legs — so [`copy_truth`] re-checks before it publishes.
    //
    // Published the other way round, the sidecar stands over a key a concurrent
    // writer can still take — a publish on that bucket during a partition wins
    // the create — and a leg that then dies (crash, cancelled `select!` leg)
    // leaves the bucket asserting sha A over body B. `decide` compares sidecar
    // shas, so both buckets then read as agreed and the merge returns `Noop`
    // forever; nothing re-hashes a stored body, so no sweep, note, full diff,
    // audit, or `verify-chain` ever looks again and that bucket serves bytes
    // that do not match their own published sha256.
    //
    // The cost is a one-op window where the destination holds a bare artifact
    // its own backfill may fabricate a sidecar for (§4). Those are the source's
    // verified bytes under the claim made above, so the fabrication names the
    // same sha and the same-sha sidecar merge settles the metadata — the same
    // window the upload path has always had between its artifact and sidecar.
    if let Some(staged) = streamed.take() {
        let landed = staged.size();
        let changed =
            match artifact_leg(state, src, dst, pkg, filename, sc, &akey, staged, false).await? {
                // The filename is fenced on both buckets and its bodies preserved.
                // Publishing a sidecar over a frozen record would re-list it.
                ArtifactLeg::Frozen => return Ok(false),
                ArtifactLeg::Landed { changed } => changed,
            };
        return copy_truth(dst, &akey, sc, companions, changed, is_mirror, landed).await;
    }

    // Sidecar first for the server-side copy transport, whose copy verb has no
    // create-if-absent: a pre-check is the only gate it can have. A destination
    // that already holds a different-sha private body is caught here and bails
    // before any artifact write. This is the only leg that publishes its gate
    // ahead of the bytes it names, and only because it cannot do otherwise.
    let skey = sidecar_key(&akey);
    let mut changed = if is_mirror {
        let installed = install_or_verify_mirror_sidecar(dst, &skey, sc).await?;
        // `Ok(false)` is "the destination sidecar stands as it is", and that
        // covers two very different states: our own bytes already there, or the
        // yield to private truth (the installer never overwrites it). The upload
        // path is free to read both as "carry on"; this leg is not, because the
        // artifact leg below is an unconditional `CopyObject` that would land
        // mirror bytes under that private sidecar. Distinguish them here rather
        // than in the shared installer, whose `Ok(false)` the upload path
        // (`publish::publish_record`) relies on meaning "yield, continue".
        if !installed && sidecar_holds_private(dst, &skey).await? {
            bail!(
                "destination bucket {} holds private truth for {pkg}/{filename}; refusing to copy the mirror record from {}",
                bucket_name(dst),
                bucket_name(src)
            );
        }
        installed
    } else {
        install_or_verify_sidecar(dst, &skey, sc).await?
    };
    if let Some(bytes) = companions.metadata {
        changed |= put_if_absent_or_verify(
            dst,
            &metadata_key(&akey),
            bytes,
            Some("text/plain; charset=utf-8"),
        )
        .await?;
    }
    if let Some(bytes) = companions.provenance {
        changed |=
            put_if_absent_or_verify(dst, &provenance_key(&akey), bytes, Some("application/json"))
                .await?;
    }

    // Artifact leg, transport ladder. First the server-side copy when eligible:
    // it lands the bytes provider-side (zero through this node) and verifies the
    // stored size. On oversize (`NotCopyable`) or any failure, fall through to
    // streaming — the caller writes a repair note only if streaming also fails.
    if let Some(src_o) = &copy_origin {
        match server_side_copy_artifact(state, dst, &akey, sc, src_o).await {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(error) => {
                warn!(package = %pkg, filename = %filename, error = ?error, "server-side copy failed; streaming the artifact instead");
            }
        }
    }

    // Stream the artifact: only a missed server-side copy reaches here, and that
    // transport never pre-verified the bytes, so read and verify them now.
    let artifact = verify_source_artifact(state, src, pkg, filename, record, source).await?;
    Ok(
        match artifact_leg(state, src, dst, pkg, filename, sc, &akey, artifact, true).await? {
            ArtifactLeg::Frozen => false,
            ArtifactLeg::Landed { changed: landed } => changed || landed,
        },
    )
}

/// Publish the destination's truth for a record whose bytes are already down:
/// the sidecar that names them, then the companions.
///
/// `landed` is the size of the body the artifact leg left under `akey`, and it
/// is re-checked here. The leg's claim — "nothing can land a body under a key we
/// already own" — holds only for as long as we still own it, and one pass in the
/// system is entitled to empty a live canonical key: a demotion settle, whose
/// fence deliberately does not bar the writers racing it. A settle landing
/// between the two legs leaves this sidecar standing over an empty key, the next
/// create takes that key with different bytes, and the bucket publishes sha A
/// over body B permanently — `decide` compares sidecar shas, so both buckets
/// read as agreed and nothing re-hashes a stored body. Traced on vopr seed
/// 62000150551, where bucket 0 ended up serving one byte-set under the other's
/// sha256 and every later merge returned `Noop`.
///
/// One HEAD, not a re-read of the body: this is the same shape as
/// [`sidecar_still_names`] and it narrows rather than closes — a settle can
/// still land in the op between this check and the create. It cannot be closed
/// from here, because `delete_keys` is unconditional; what it takes away is the
/// wide window, the one an unrelated pass walks into. Refusing leaves the
/// caller's `_repl/` note to bring the next `decide` back to the record.
async fn copy_truth(
    dst: &dyn Storage,
    akey: &str,
    sc: &Sidecar,
    companions: Companions,
    mut changed: bool,
    is_mirror: bool,
    landed: u64,
) -> Result<bool> {
    match dst.stored_size(akey).await? {
        Some(size) if size == landed => {}
        Some(size) => bail!(
            "destination artifact at {akey} holds {size} bytes, not the {landed} this copy landed"
        ),
        None => {
            bail!("destination artifact at {akey} was removed before its sidecar was published")
        }
    }
    changed |= if is_mirror {
        install_or_verify_mirror_sidecar(dst, &sidecar_key(akey), sc).await?
    } else {
        install_or_verify_sidecar(dst, &sidecar_key(akey), sc).await?
    };
    if let Some(bytes) = companions.metadata {
        changed |= put_if_absent_or_verify(
            dst,
            &metadata_key(akey),
            bytes,
            Some("text/plain; charset=utf-8"),
        )
        .await?;
    }
    if let Some(bytes) = companions.provenance {
        changed |=
            put_if_absent_or_verify(dst, &provenance_key(akey), bytes, Some("application/json"))
                .await?;
    }
    Ok(changed)
}

/// Whether the destination's own sidecar still names `sha` — the divergence
/// gate [`install_or_verify_sidecar`] established, re-read at the moment a
/// destructive write to the artifact key is about to trust it. An absent or
/// different-sha sidecar means this bucket's truth no longer describes the
/// bytes we are holding, so the body under that key is not ours to replace.
async fn sidecar_still_names(dst: &dyn Storage, key: &str, sha: &str) -> Result<bool> {
    match dst.get_bytes(key).await {
        Ok(bytes) => {
            let current: Sidecar = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse destination sidecar {key}"))?;
            Ok(current.sha256 == sha)
        }
        Err(error) if is_not_found(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Whether the sidecar at `key` is private truth. Read only when a mirror
/// install declined to write, to tell "already our bytes" from "yielded to a
/// private claim" — [`install_or_verify_mirror_sidecar`] collapses both into
/// `Ok(false)` and its other caller needs it to keep doing so.
async fn sidecar_holds_private(dst: &dyn Storage, key: &str) -> Result<bool> {
    match dst.get_bytes(key).await {
        Ok(bytes) => {
            let current: Sidecar = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse destination sidecar {key}"))?;
            Ok(current.origin.as_deref() == Some(PRIVATE))
        }
        Err(error) if is_not_found(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Name a bucket for an operator-facing error. Only backends that can be a
/// server-side copy peer carry a name; disk and the test fakes report `local`.
fn bucket_name(storage: &dyn Storage) -> String {
    storage
        .copy_origin()
        .map_or_else(|| "local".to_string(), |origin| origin.location)
}

/// How the destination's artifact key ended up after [`artifact_leg`].
#[derive(Debug, PartialEq, Eq)]
enum ArtifactLeg {
    /// The key holds the bytes the source sidecar names. `changed` is whether
    /// this leg wrote them (a dedup against identical bytes changes nothing).
    Landed { changed: bool },
    /// A competing body could not be reconciled, so the filename is frozen on
    /// both buckets and both bodies are preserved. No truth may be published
    /// over it.
    Frozen,
}

/// Land the (already verified) artifact bytes on the destination: repair a
/// stale-debris body in place, then publish the artifact under a conditional
/// create, freezing a raced competing body. Shared by the private stream path
/// (which runs it *before* publishing the sidecar) and by the mirror path and
/// the copy transport's fallback (which run it after their sidecar gate).
#[allow(clippy::too_many_arguments)]
async fn artifact_leg(
    state: &AppState,
    src: &dyn Storage,
    dst: &dyn Storage,
    pkg: &str,
    filename: &str,
    sc: &Sidecar,
    akey: &str,
    artifact: StagedArtifact,
    // Whether this leg has already published the destination's sidecar: the
    // gated paths have, the private stream path has not, and that decides what
    // a raced competing body means.
    sidecar_published: bool,
) -> Result<ArtifactLeg> {
    let mut changed = false;
    // A destination body already under this immutable key with the wrong sha is
    // stale crash debris (e.g. a zero-byte object a 200-acked-but-failed write
    // left behind, D2); repair it in place, then the conditional create dedups.
    //
    // "Debris" holds only while the destination sidecar names OUR sha — a gate
    // some earlier leg installed ahead of its bytes (the copy transport's
    // pre-check, the mirror path, or a pre-artifact-first leg). A local publish
    // on this bucket invalidates that gate inside the window: it wins the
    // immutable create and then overwrites the sidecar, leaving a live body its
    // own truth correctly describes. So re-read the gate. A described body is a
    // byte conflict, not debris, and falls through to the conditional create's
    // freeze arm below — the loser is quarantined, never overwritten (§6.3).
    // Between the two states nothing on this bucket can tell an in-flight
    // publisher's body from debris, so preserve the bytes before replacing
    // them, and replace conditionally on the etag we read: an immutable key is
    // never a HEAD-then-PUT.
    if let Some((current, etag)) = dst
        .get_with_etag(akey)
        .await
        .with_context(|| format!("verify destination artifact {akey}"))?
    {
        if sha256_hex(&current) != sc.sha256
            && sidecar_still_names(dst, &sidecar_key(akey), &sc.sha256).await?
        {
            quarantine_bytes(dst, pkg, filename, &current).await?;
            let len = artifact.size();
            // The one leg that must hold the body: `put_if_match` has no
            // streaming form (no backend takes a precondition on a multipart
            // upload) and the etag condition is what stops this write from
            // clobbering a publisher that took the key in the window above.
            // Off the copy path — the destination already holds a body — so the
            // buffer is the rare repair's, not every copy's.
            let bytes = artifact.read_all().await?;
            if bounded_artifact_write(akey, len, dst.put_if_match(akey, &etag, bytes))
                .await?
                .is_some()
            {
                verify_stored_size(dst, akey, len).await?;
                changed = true;
            }
        }
    }
    // Losing this conditional create must never be reported as a copy: verify
    // the winner. Same bytes converged; different bytes freeze immediately, so
    // no sidecar this copy publishes can describe the competing body — the
    // private path has not written one yet, and the gated paths already have.
    // The create is verified (D1) and bounded (D3) by the shared primitive.
    let len = artifact.size();
    if create_artifact_verified(
        dst,
        akey,
        artifact.body(),
        len,
        Some("application/octet-stream"),
    )
    .await?
    {
        state.metrics.record_replicated(len);
        return Ok(ArtifactLeg::Landed { changed: true });
    }
    let raced = dst
        .get_bytes(akey)
        .await
        .with_context(|| format!("verify raced destination artifact {akey}"))?;
    let raced_sha = sha256_hex(&raced);
    if raced_sha == sc.sha256 {
        return Ok(ArtifactLeg::Landed { changed });
    }
    // A raced body the destination's own sidecar describes means that bucket
    // holds live private truth for this filename with different bytes. Ordering
    // it is the merge's call, not this leg's — `upload-epoch-ms` resolves it
    // first-uploaded-wins and only a skew-tied or epoch-less pair degrades to a
    // freeze — and a leg that has published nothing yet can still leave it
    // alone. Refuse exactly as the sidecar gate does, so the caller's `_repl/`
    // note brings the next `decide` to it. Once the sidecar *is* published this
    // leg owns the record and must freeze on the spot instead, so nothing it
    // wrote ever describes the competing body (§6.3).
    if !sidecar_published && sidecar_still_names(dst, &sidecar_key(akey), &raced_sha).await? {
        bail!(
            "destination artifact at {akey} holds different private bytes named by its own sidecar ({raced_sha}); expected {}",
            sc.sha256
        );
    }
    freeze_copy_race(state, src, dst, pkg, filename, &sc.sha256, &raced_sha).await?;
    Ok(ArtifactLeg::Frozen)
}

/// The source's [`CopyOrigin`] when the boot matrix has verified that `dst` can
/// pull it server-side; `None` (stream) otherwise. Stateless — consulted per
/// copy op, so a boot-downgraded cell never attempts a copy.
fn copy_transport(state: &AppState, src: &dyn Storage, dst: &dyn Storage) -> Option<CopyOrigin> {
    let src_o = src.copy_origin()?;
    let dst_o = dst.copy_origin()?;
    state
        .buckets
        .copy_matrix()
        .allows(&dst_o, &src_o)
        .then_some(src_o)
}

/// Attempt the server-side artifact copy. `Ok(true)` = copied and verified;
/// `Ok(false)` = the backend declined (e.g. oversize) so the caller streams;
/// `Err` = a copy was attempted and failed, so the caller streams (which
/// re-reads and sha-verifies the source) and then leaves its repair note. The
/// destination sidecar (installed before this leg) already gates divergence, so
/// overwriting adjudicated truth here is safe — the copy verb has no
/// create-if-absent on S3 by design.
///
/// The copy transport never pulls the bytes through this node, so it cannot
/// sha-verify them. It confirms the destination with (1) the size HEAD it has
/// always done and (2) — when both the source sidecar and the provider's copy
/// response carry an algorithm-matched content checksum — a comparison against
/// the checksum captured when the bytes were first SHA-256-verified. A checksum
/// contradiction is a corrupt copy or a same-size-wrong source; it fails the
/// copy so the caller streams and the sha check catches it, instead of serving
/// bytes that silently disagree with their own sidecar. When either checksum is
/// missing or the algorithms differ, the size-only check stands (no regression).
async fn server_side_copy_artifact(
    state: &AppState,
    dst: &dyn Storage,
    akey: &str,
    sc: &Sidecar,
    src_o: &CopyOrigin,
) -> Result<bool> {
    match dst.server_side_copy(src_o, akey, akey, sc.size).await? {
        CopyOutcome::NotCopyable => Ok(false),
        CopyOutcome::Copied { checksum } => {
            verify_stored_size(dst, akey, sc.size).await?;
            if matches!(
                check_copy_checksum(sc.store_checksum.as_ref(), checksum.as_ref()),
                ChecksumCheck::Mismatch
            ) {
                bail!(
                    "server-side copy of {akey} landed a body whose content checksum \
                     contradicts the source sidecar ({:?} != copy {:?}); refusing to trust it",
                    sc.store_checksum,
                    checksum
                );
            }
            state.metrics.record_server_side_copy(sc.size);
            Ok(true)
        }
    }
}

/// Replace one destination record with verified private truth. This is shared
/// by mirror supersede and the timestamp-ordered private/private conflict path.
/// The losing body is preserved before its conditional replacement; sidecar
/// and companions then converge, and an absent artifact is published last.
async fn supersede_record(
    state: &AppState,
    src: &dyn Storage,
    dst: &dyn Storage,
    pkg: &str,
    filename: &str,
    src_record: &Record,
    source: ArtifactSource<'_>,
) -> Result<()> {
    require_replication_unfenced(state)?;
    let sidecar = src_record
        .sidecar
        .as_ref()
        .ok_or_else(|| anyhow!("supersede verdict with no source sidecar"))?;
    if sidecar.origin.as_deref() != Some(PRIVATE) {
        bail!("supersede source for {pkg}/{filename} is not private truth");
    }
    let verified = verify_source_record(state, src, pkg, filename, src_record, source).await?;
    ensure_private_origin(dst, pkg).await?;
    let akey = artifact_key(pkg, filename);

    // A marker that raced the merge read has precedence. Preserve the incoming
    // private evidence, but never resurrect it through the fence.
    if dst.head_exists(&frozen_key(&akey)).await? {
        quarantine_bytes(dst, pkg, filename, &verified.artifact.read_all().await?).await?;
        freeze_side(dst, pkg, filename).await?;
        markers::mark_dirty(dst, pkg).await?;
        return Ok(());
    }
    if dst.head_exists(&tombstone_key(&akey)).await? {
        if tombstone_side(dst, pkg, filename).await? {
            markers::mark_dirty(dst, pkg).await?;
        }
        return Ok(());
    }

    let replace_companions = match dst.get_bytes(&sidecar_key(&akey)).await {
        Ok(bytes) => {
            let current: Sidecar = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse destination sidecar for {akey}"))?;
            match current.origin.as_deref() {
                Some(PRIVATE) => current.sha256 != sidecar.sha256,
                Some(MIRROR) | None => true,
                Some(raw) => bail!("destination sidecar for {akey} has invalid origin '{raw}'"),
            }
        }
        Err(error) if is_not_found(&error) => false,
        Err(error) => return Err(error),
    };

    // Artifact leg first, for the reason [`copy_live`] spells out: the
    // destination's immutable create is a strictly stronger gate than its
    // sidecar. Demotion's ordinary shape reaches here with the artifact key
    // EMPTY — `quarantine_mirror_artifacts` has already moved the mirror body
    // aside, which is what makes the destination read `QuarantinedMirror` — so
    // a sidecar published first would stand over a key a concurrent publish can
    // still take, and it would also publish the *superseded* bytes as current:
    // on a demotion those are the ones the operator withdrew, which is the
    // fail-OPEN direction.
    //
    // That ordering is right and it is not free: between the body write and the
    // sidecar below, this bucket serves bytes its own published sha256
    // contradicts, and a death in the window used to make that permanent —
    // nothing re-hashes a stored body, both buckets' sidecars stay identical,
    // and `decide` reads them as agreed forever. So declare the intent first,
    // the same shape as the `_dirty/` bracket around this whole verdict: the
    // marker carries the sidecar being installed, and whatever remains after a
    // crash is a *recognizable* torn record that
    // [`finish_interrupted_supersedes`] can complete from the marker alone.
    // Cheap enough to be unconditional — a supersede is a merge-conflict or
    // demotion path, never the upload hot path — and unconditional is what
    // makes it a fence rather than a guess about which leg will tear.
    let superseding = superseding_key(&akey);
    dst.put_bytes(&superseding, serde_json::to_vec(sidecar)?, json())
        .await
        .with_context(|| format!("declare supersede intent at {superseding}"))?;

    let mut artifact_present = false;
    if let Some((current, etag)) = dst.get_with_etag(&akey).await? {
        if sha256_hex(&current) == sidecar.sha256 {
            artifact_present = true;
        } else {
            quarantine_bytes(dst, pkg, filename, &current).await?;
            let len = verified.artifact.size();
            // Buffered for the same reason [`artifact_leg`]'s repair is: a
            // conditional replace has no streaming form, and the etag is what
            // makes replacing a live body safe. The demotion's ordinary shape
            // does not come here — it finds the canonical key empty and takes
            // the streaming create below.
            let bytes = verified.artifact.read_all().await?;
            if bounded_artifact_write(&akey, len, dst.put_if_match(&akey, &etag, bytes))
                .await?
                .is_none()
            {
                bail!("destination artifact changed during supersede for {akey}");
            }
            verify_stored_size(dst, &akey, len).await?;
            state.metrics.record_replicated(len);
            artifact_present = true;
        }
    }

    if !artifact_present {
        let len = verified.artifact.size();
        if create_artifact_verified(
            dst,
            &akey,
            verified.artifact.body(),
            len,
            Some("application/octet-stream"),
        )
        .await?
        {
            state.metrics.record_replicated(len);
        } else {
            let raced = dst
                .get_bytes(&akey)
                .await
                .with_context(|| format!("verify raced destination artifact {akey}"))?;
            let raced_sha = sha256_hex(&raced);
            // Nothing this call published can describe the competing body: the
            // sidecar goes in below, and the freeze fences the filename first.
            if raced_sha != sidecar.sha256 {
                freeze_copy_race(state, src, dst, pkg, filename, &sidecar.sha256, &raced_sha)
                    .await?;
                // The freeze adjudicated the filename and moved both bodies
                // aside: there is no record left for the heal to finish, and a
                // marker outliving it would re-publish a sidecar over the fence.
                dst.delete_keys(&[superseding]).await?;
                return Ok(());
            }
        }
    }

    install_or_verify_sidecar(dst, &sidecar_key(&akey), sidecar).await?;
    // Body and sidecar agree as of this instant: the window is shut. Everything
    // below is companions and marker precedence, none of which can tear the
    // record's own attestation.
    dst.delete_keys(&[superseding]).await?;
    replace_companion(
        dst,
        &metadata_key(&akey),
        verified.metadata,
        Some("text/plain; charset=utf-8"),
        replace_companions,
    )
    .await?;
    replace_companion(
        dst,
        &provenance_key(&akey),
        verified.provenance,
        Some("application/json"),
        replace_companions,
    )
    .await?;

    // A delete/freeze can race the publish. Reassert precedence after the
    // complete record lands, then clear obsolete mirror quarantine state only
    // when private truth remains live.
    if dst.head_exists(&frozen_key(&akey)).await? {
        quarantine_bytes(dst, pkg, filename, &verified.artifact.read_all().await?).await?;
        freeze_side(dst, pkg, filename).await?;
    } else if dst.head_exists(&tombstone_key(&akey)).await? {
        tombstone_side(dst, pkg, filename).await?;
    } else {
        dst.delete_keys(&[mirror_quarantined_key(&akey)]).await?;
    }
    markers::mark_dirty(dst, pkg).await?;
    Ok(())
}

async fn replace_companion(
    storage: &dyn Storage,
    key: &str,
    bytes: Option<Vec<u8>>,
    content_type: Option<&str>,
    replace: bool,
) -> Result<bool> {
    let Some(bytes) = bytes else {
        if replace && storage.head_exists(key).await? {
            storage.delete_keys(&[key.to_string()]).await?;
            return Ok(true);
        }
        return Ok(false);
    };
    if !replace {
        return put_if_absent_or_verify(storage, key, bytes, content_type).await;
    }
    for _ in 0..ORIGIN_ATTEMPTS {
        match storage.get_with_etag(key).await? {
            None => {
                if storage
                    .put_if_absent(key, bytes.clone(), content_type)
                    .await?
                {
                    return Ok(true);
                }
            }
            Some((current, _)) if current == bytes => return Ok(false),
            Some((_, etag)) => {
                if storage
                    .put_if_match(key, &etag, bytes.clone())
                    .await?
                    .is_some()
                {
                    return Ok(true);
                }
            }
        }
    }
    bail!("conditional companion replacement retries exhausted for {key}")
}

/// Drive `pkg`'s origin claim on `dst` to `private`:
/// create-if-absent, CAS the `unclaimed` sentinel, or demote a `mirror` claim —
/// private is terminal, so a claim already private is a no-op. The one demotion
/// primitive; never a delete (which could re-open the name to a proxy fill).
async fn ensure_private_origin(dst: &dyn Storage, pkg: &str) -> Result<()> {
    for _ in 0..ORIGIN_ATTEMPTS {
        match read_origin_observation(dst, pkg).await? {
            None => {
                // Absent: create-if-absent. A racer may beat us to a real claim.
                if claim_origin(dst, pkg, PRIVATE).await?.owner == PRIVATE {
                    return Ok(());
                }
            }
            Some(observed) if observed.state == OriginState::Private => return Ok(()),
            Some(observed) if observed.state == OriginState::Mirror => {
                if crate::origin::demote_observed_mirror(dst, pkg, &observed)
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
            }
            Some(observed) if observed.state == OriginState::Unclaimed => {
                let request = ClaimRequest::new(PRIVATE, Some(&observed));
                if claim_origin(dst, pkg, request).await?.owner == PRIVATE {
                    return Ok(());
                }
            }
            Some(observed) => {
                bail!(
                    "origin claim for '{pkg}' holds unsupported state '{}'",
                    observed.state.as_str()
                )
            }
        }
    }
    bail!("could not make '{pkg}' private on the destination after {ORIGIN_ATTEMPTS} attempts")
}

/// Drive `pkg`'s origin claim on `dst` to at least `mirror`:
/// create-if-absent, or CAS the `unclaimed` sentinel. A claim already `private`
/// is left untouched — private is terminal and outranks mirror everywhere, so a
/// replicated snapshot NEVER demotes it (the mirror analogue of
/// [`ensure_private_origin`], which does demote). A claim already `mirror` is a
/// no-op. Any real claim a racer installed (private or mirror) is accepted, and
/// returned: the caller cannot assume `mirror` the way it can assume `private`
/// after [`ensure_private_origin`], because this call yields to a racer.
async fn ensure_mirror_origin(dst: &dyn Storage, pkg: &str) -> Result<Origin> {
    for _ in 0..ORIGIN_ATTEMPTS {
        let observed = read_origin_observation(dst, pkg).await?;
        match observed.as_ref().map(|observed| observed.state) {
            Some(OriginState::Mirror) => return Ok(Origin::Mirror),
            // Private outranks a replicated mirror snapshot; leave it terminal.
            Some(OriginState::Private) => return Ok(Origin::Private),
            // Absent: create-if-absent. Unclaimed: CAS the sentinel. Either way a
            // racer may beat us to a real claim; private (which we must not
            // demote) or mirror is fine.
            None | Some(OriginState::Unclaimed) => {
                let request = ClaimRequest::new(
                    MIRROR,
                    observed
                        .as_ref()
                        .filter(|observed| observed.state == OriginState::Unclaimed),
                );
                match claim_origin(dst, pkg, request).await?.owner.as_str() {
                    MIRROR => return Ok(Origin::Mirror),
                    PRIVATE => return Ok(Origin::Private),
                    _ => {}
                }
            }
        }
    }
    bail!("could not make '{pkg}' mirror on the destination after {ORIGIN_ATTEMPTS} attempts")
}

/// Install a mirror record's sidecar under CAS. Any mirror sidecar is accepted —
/// a `sync --to` snapshot and a proxy-cache fill alike (unlike
/// [`install_or_verify_sidecar`], which is private-only) — because both replicate
/// now; the snapshot bit is provenance the copy carries verbatim, not a gate.
/// Private truth on the destination is NEVER overwritten — private outranks
/// mirror everywhere. Same-sha crash debris is repaired in place; a different-sha
/// mirror body is a split-brain resolved by the freeze path, not here. Returns
/// whether the destination sidecar changed.
///
/// Shared with the `sync --to` write path (`publish::publish_record`), which
/// faces the identical question once it has won an artifact's conditional
/// create: a sidecar already in the slot names bytes this bucket does not have.
pub(crate) async fn install_or_verify_mirror_sidecar(
    dst: &dyn Storage,
    key: &str,
    sidecar: &Sidecar,
) -> Result<bool> {
    if sidecar.origin.as_deref() != Some(MIRROR) {
        bail!("mirror replication source sidecar at {key} is not mirror truth");
    }
    let bytes = serde_json::to_vec(sidecar)?;
    let artifact = key
        .strip_suffix(SIDECAR_SUFFIX)
        .ok_or_else(|| anyhow!("sidecar key has no {SIDECAR_SUFFIX} suffix: {key}"))?;
    for _ in 0..ORIGIN_ATTEMPTS {
        let Some((current_bytes, etag)) = dst.get_with_etag(key).await? else {
            if dst.put_if_absent(key, bytes.clone(), json()).await? {
                return Ok(true);
            }
            continue;
        };
        if current_bytes == bytes {
            return Ok(false);
        }
        let current: Sidecar = serde_json::from_slice(&current_bytes)
            .with_context(|| format!("parse destination sidecar {key}"))?;
        if let Some(raw) = current.origin.as_deref() {
            if Origin::parse(raw).is_none() {
                bail!("sidecar at {key} holds an unexpected origin '{raw}'");
            }
        }
        // Private truth outranks a replicated mirror snapshot; leave it in place.
        if current.origin.as_deref() == Some(PRIVATE) {
            return Ok(false);
        }
        let replace = if current.sha256 != sidecar.sha256 {
            // A different-sha mirror sidecar is replaceable only as stale crash
            // debris: the destination body must be absent or already our bytes.
            let body = match dst.get_bytes(artifact).await {
                Ok(body) => Some(body),
                Err(error) if is_not_found(&error) => None,
                Err(error) => return Err(error),
            };
            body.is_none()
                || body
                    .as_deref()
                    .is_some_and(|body| sha256_hex(body) == sidecar.sha256)
        } else {
            // Same bytes: the higher yank epoch wins (§6.5).
            yank_merge(sidecar, &current) == MergeChoice::A
        };
        if !replace {
            if current.sha256 != sidecar.sha256 {
                bail!(
                    "destination mirror sidecar at {key} names sha {} backed by different bytes; expected {}",
                    current.sha256,
                    sidecar.sha256
                );
            }
            return Ok(false);
        }
        if dst.put_if_match(key, &etag, bytes.clone()).await?.is_some() {
            return Ok(true);
        }
    }
    bail!("conditional mirror sidecar replacement retries exhausted for {key}")
}

/// Preserve an artifact body at `_quarantine/<pkg>/<file>@<actual-sha12>` before
/// its caller removes the live record. The key is derived from the bytes, not a
/// possibly-corrupt sidecar. Losing the create race is success only after the
/// existing quarantine object is verified byte-for-byte.
async fn quarantine(storage: &dyn Storage, pkg: &str, filename: &str) -> Result<Option<String>> {
    let akey = artifact_key(pkg, filename);
    let bytes = match storage.get_bytes(&akey).await {
        Ok(bytes) => bytes,
        Err(e) if is_not_found(&e) => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(Some(
        quarantine_bytes(storage, pkg, filename, &bytes).await?,
    ))
}

/// Where a preserved body lives: `_quarantine/<pkg>/<file>@<sha12>`. The hash
/// is IN the key, so the two byte-sets of a conflicted filename each get their
/// own object instead of one overwriting the other.
///
/// Shared with `sim`, which asks the inverse question — given a body about to
/// be deleted, is there a copy of exactly these bytes standing? — and must ask
/// it against the same layout the product writes.
pub(crate) fn quarantine_key(pkg: &str, filename: &str, sha: &str) -> String {
    format!(
        "{QUARANTINE_PREFIX}{pkg}/{filename}@{}",
        &sha[..sha.len().min(12)]
    )
}

async fn quarantine_bytes(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    bytes: &[u8],
) -> Result<String> {
    let sha = sha256_hex(bytes);
    let qkey = quarantine_key(pkg, filename, &sha);
    if !storage
        .put_if_absent(&qkey, bytes.to_vec(), Some("application/octet-stream"))
        .await?
    {
        let existing = storage
            .get_bytes(&qkey)
            .await
            .with_context(|| format!("verify existing quarantine object {qkey}"))?;
        if existing != bytes {
            bail!("quarantine key collision at {qkey}");
        }
    }
    Ok(sha)
}

/// Drop a demotion fence that private truth has already resolved.
///
/// Handing the filename to private truth IS the intended resolution of a
/// demotion, so the fence never blocks a private upload — and once one lands,
/// the fence is spent. `Record::state()`, the index renderer
/// (`worker::load_file_metadata`), the file route (`serve::artifact_visible`)
/// and `verify`'s oracle all already read straight past a fence with a private
/// sidecar beside it, so this changes nothing renderable: no dirty marker, no
/// intent bracket. What it changes is convergence. `supersede_record` clears
/// the fence on the destination it writes, and nothing clears it on the bucket
/// that took the private upload directly over its own fenced (empty) key, nor
/// on the *source* side of a supersede — and `decide` reads both sides as
/// `Live { Private }` and calls them converged, so the leftover key survives
/// every future diff. Measured: 174 of 176 partitioned-lane failures.
///
/// `record` is listing-era, so it decides only whether this is worth a read.
/// The delete itself is authorized by a fresh one, through the same primitive
/// [`settle_mirror_quarantine`] uses, because the two race over the same two
/// keys: a settle that started before the private upload landed moves that body
/// to `_quarantine/` and drops the canonical key, leaving the fence as the ONLY
/// record that the emptiness was authorized. Clearing it on the stale reading
/// then leaves the filename empty with nothing standing for it — no tombstone,
/// no `.frozen`, and no fence on any bucket the settle's fan-out had not yet
/// reached — which is exactly the state `origin release` hands back to a proxy
/// to re-fetch the artifact just suppressed. The simulator sees it as acked
/// bytes missing fleet-wide with no authorized removal.
async fn clear_spent_demotion_fence(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    record: &Record,
) -> Result<()> {
    if !record.mirror_quarantined
        || !matches!(
            record.state(),
            RecordState::Live {
                origin: Origin::Private,
                ..
            }
        )
    {
        return Ok(());
    }
    if !demotion_resolved_by_private_truth(storage, pkg, filename).await? {
        return Ok(());
    }
    storage
        .delete_keys(&[mirror_quarantined_key(&artifact_key(pkg, filename))])
        .await
}

/// Write the mirror→private demotion fence. It names the filename and NOTHING
/// else: two partitioned buckets can demote two different bodies of one
/// immutable filename, and a marker carrying its local sha would then differ
/// per bucket and never converge. The hash lives in the `_quarantine/` key,
/// which is where the bytes are. Every consumer tests for existence.
async fn write_mirror_quarantine_marker(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
) -> Result<bool> {
    #[derive(serde::Serialize)]
    struct Marker<'a> {
        filename: &'a str,
    }
    put_if_absent_or_verify(
        storage,
        &mirror_quarantined_key(&artifact_key(pkg, filename)),
        serde_json::to_vec(&Marker { filename })?,
        json(),
    )
    .await
}

/// Settle a mirror→private demotion for one filename on one bucket. The fence
/// is truth and replicates; the body it suppresses is evidence and stays here
/// (dev/DESIGN.md). So this is a *move*, in the same order [`freeze_side`]
/// uses: claim private, fence FIRST (a crash then leaves a recognizable,
/// already-inert demotion rather than a bare delete), verified `_quarantine/`
/// copy, then drop the canonical record — artifact, sidecar and companions —
/// leaving the fence standing. A *move*, so the drop is guarded: it removes
/// only the body the `_quarantine/` copy holds, and abandons the whole settle
/// when a racing publish put something else there.
///
/// The fence is what keeps the name closed: `origin release` refuses while any
/// key besides `.origin` remains under `packages/<pkg>/`, so dropping the
/// marker with the body would open the one state that authorizes a proxy to
/// re-fetch the artifact just suppressed.
///
/// Idempotent, and returns whether anything changed so a settled side is not
/// re-marked dirty.
async fn settle_mirror_quarantine(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
) -> Result<bool> {
    ensure_private_origin(storage, pkg).await?;
    if demotion_resolved_by_private_truth(storage, pkg, filename).await? {
        return Ok(false);
    }
    let marker_created = write_mirror_quarantine_marker(storage, pkg, filename).await?;
    let preserved = quarantine(storage, pkg, filename).await?;
    let akey = artifact_key(pkg, filename);
    let sidecar_left = storage.head_exists(&sidecar_key(&akey)).await?;
    // Drop only the body this settle personally preserved.
    //
    // `freeze_side` runs the same marker-then-preserve-then-drop order and is
    // safe doing it blind, because its marker is an UPLOAD fence: `.frozen` and
    // `.tombstone` both make `publish_record` refuse, so the body it copied to
    // `_quarantine/` is still the body it deletes. This marker is the one fence
    // in the system that deliberately does not bar an upload — handing the
    // filename to private truth IS the demotion's intended resolution — so the
    // canonical key stays writable for the whole settle. A blind
    // `delete_keys` then destroys whatever a racing publish created there,
    // bytes no `_quarantine/` copy holds and no other pass can recover.
    // Measured twice, on the two arms below: a private upload acked on the same
    // bucket whose body a sibling settle, holding a read from three ops earlier,
    // had already deleted (vopr seed 40000042940).
    //
    // A re-read cannot make that safe, and this used to try: the key can be
    // empty at the read above and occupied by the time the delete lands, and
    // `delete_keys` is unconditional, so no reading of the key authorizes
    // removing it. Only the `_quarantine/` copy does. So the body key is in the
    // delete list only when this settle holds a verified copy of exactly the
    // bytes standing there — the move completing — and is left out entirely
    // otherwise. Measured: a private upload acked 200 on vopr seed 86001009016,
    // created under a key a sibling settle had just read as empty and then
    // deleted blind; the bytes were in no `_quarantine/` copy, under no
    // tombstone and no `.frozen`, so nothing ever looked for them again.
    //
    // Different bytes standing there abandons the whole settle instead, which
    // is safe and terminal: the private record reads `Live { Private }` under a
    // spent fence, which `decide` resolves and `clear_spent_demotion_fence`
    // finishes.
    let body_is_ours = match storage.get_bytes(&akey).await {
        Ok(standing) => {
            if preserved.as_deref() != Some(sha256_hex(&standing).as_str()) {
                return Ok(marker_created || preserved.is_some());
            }
            true
        }
        Err(e) if is_not_found(&e) => false,
        Err(e) => return Err(e),
    };
    // The sidecar and companions go either way. A mirror sidecar with no body
    // under it is a torn record, and settling it is the only thing that clears
    // the verdict — skipping the drop would leave `decide` re-issuing it
    // forever. The worst a publish racing this same gap loses is its sidecar,
    // leaving a bare artifact its own backfill re-derives at the same sha: the
    // window the upload path has always had between its bytes and its truth.
    //
    // The fence itself is deliberately NOT in this list — it is the whole point
    // of the demotion, and the only part of the record that replicates.
    //
    // The body still leads when it is in the list: `delete_keys` is one key at a
    // time on every backend, and a crash after the sidecar would leave a BARE
    // mirror body under a private package claim, which `load_file_metadata`
    // backfills into a private sidecar — laundering upstream bytes into private
    // truth (§4). Body first leaves a bare sidecar instead, which serves nothing.
    let keys = if body_is_ours {
        record_object_keys(&akey).to_vec()
    } else {
        let [_body, sidecar, metadata, provenance] = record_object_keys(&akey);
        vec![sidecar, metadata, provenance]
    };
    storage.delete_keys(&keys).await?;
    Ok(marker_created || preserved.is_some() || sidecar_left)
}

/// Is private truth standing under the canonical key *right now*?
///
/// [`Verdict::SettleMirrorQuarantine`] is decided from a listing-era read, and
/// a private upload can land under the fence between that listing and this
/// executor — handing the filename to private truth IS the demotion's intended
/// resolution, so `decide` answers `Supersede` the moment it sees one and the
/// audit's own scan skips a private sidecar under a fence. The merge executor
/// did not re-check, and a stale settle suppressed the record that resolved the
/// demotion: it moved acknowledged private bytes to `_quarantine/` and dropped
/// the canonical key.
///
/// That alone is data loss the fence still excuses. The unexcused half is what
/// the simulator caught: the settle re-establishes a fence that a concurrent
/// pass, holding the same listing-era `Live { Private }` record, is entitled to
/// call spent and delete ([`clear_spent_demotion_fence`]). The two land in
/// either order and the filename ends with no body and nothing authorizing its
/// absence — the state `origin release` will hand back to a proxy to re-fetch
/// clean. Re-reading here is what keeps the audit and the merge from drifting,
/// which was always the point of sharing one primitive — so
/// [`clear_spent_demotion_fence`] reads through it too. Both sides of that race
/// now answer the same question against the same two keys at the moment they
/// act, which is the only reason either is entitled to move.
///
/// A private sidecar with no body is a torn record, not private truth: the
/// settle's own fence-then-drop is the right cleanup for it.
async fn demotion_resolved_by_private_truth(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
) -> Result<bool> {
    let akey = artifact_key(pkg, filename);
    let sidecar: Sidecar = match storage.get_bytes(&sidecar_key(&akey)).await {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).with_context(|| format!("parse sidecar for {akey}"))?
        }
        Err(e) if is_not_found(&e) => return Ok(false),
        Err(e) => return Err(e),
    };
    if sidecar.origin.as_deref() != Some(PRIVATE) {
        return Ok(false);
    }
    storage.head_exists(&akey).await
}

/// Freeze one bucket's copy of a filename. The richer
/// `.frozen` marker lands first, so even a crash during quarantine is still
/// recognizably a freeze rather than an ordinary delete. Publishers check both
/// marker kinds. The tombstone follows as the permanent filename-reuse fence,
/// then the live record is dropped.
async fn write_frozen_marker(storage: &dyn Storage, pkg: &str, filename: &str) -> Result<bool> {
    let akey = artifact_key(pkg, filename);
    #[derive(serde::Serialize)]
    struct Frozen<'a> {
        filename: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        sha256: Option<&'a str>,
    }
    let body = serde_json::to_vec(&Frozen {
        filename,
        sha256: None,
    })?;
    storage
        .put_if_absent(&frozen_key(&akey), body, json())
        .await
}

async fn freeze_side(storage: &dyn Storage, pkg: &str, filename: &str) -> Result<bool> {
    let akey = artifact_key(pkg, filename);
    let marker_created = write_frozen_marker(storage, pkg, filename).await?;
    quarantine(storage, pkg, filename).await?;
    tombstone::write(storage, &akey, filename).await?;
    // Both durable fences and the quarantine copy precede the destructive
    // move. A crash leaves either visible markers plus evidence or a complete
    // settled freeze; a later pass finishes any retained canonical body.
    drop_record_objects(storage, pkg, filename).await?;
    Ok(marker_created)
}

async fn freeze_copy_race(
    state: &AppState,
    src: &dyn Storage,
    dst: &dyn Storage,
    pkg: &str,
    filename: &str,
    source_sha: &str,
    destination_sha: &str,
) -> Result<()> {
    error!(
        package = %pkg,
        filename = %filename,
        sha_a = %source_sha,
        sha_b = %destination_sha,
        "byte conflict raced replication publish — frozen on both buckets"
    );
    // The caller's bracket covers only the destination; this rare race arm
    // also mutates the SOURCE, so it carries its own intent/commit pair —
    // a crash mid-freeze must not leave changed source truth with no rebuild
    // signal (the same reasoning as execute's bracketing).
    let src_intent = markers::mark_intent(src, pkg).await?;
    let result = async {
        freeze_side(src, pkg, filename).await?;
        freeze_side(dst, pkg, filename).await?;
        markers::mark_dirty(dst, pkg).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let _ = markers::mark_commit(src, pkg, &src_intent).await;
    result?;
    state
        .metrics
        .replication_freezes
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

/// Tombstone one bucket's copy of a filename (delete propagation, §6.4): write
/// the tombstone (create-if-absent, checked) before removing the body. Returns
/// whether anything changed, so a side that was already settled is not re-marked.
async fn tombstone_side(storage: &dyn Storage, pkg: &str, filename: &str) -> Result<bool> {
    let akey = artifact_key(pkg, filename);
    let already = storage.head_exists(&tombstone_key(&akey)).await?;
    let had_body = storage.head_exists(&akey).await?;
    if already && !had_body {
        // The body is gone, but a delete that crashed between its artifact
        // removal and its companion removals leaves a sidecar/companion
        // orphaned beside the tombstone — and a delete landing over a demoted
        // filename leaves its now-subsumed `.mirror-quarantined` fence. Finish
        // the job — otherwise `decide` re-fires Tombstone on every diff and
        // this early return would starve the cleanup forever. Report change
        // only when debris existed.
        let (sidecar_left, metadata_left, provenance_left, demotion_fence_left) =
            futures::future::try_join4(
                storage.head_exists(&sidecar_key(&akey)),
                storage.head_exists(&metadata_key(&akey)),
                storage.head_exists(&provenance_key(&akey)),
                storage.head_exists(&mirror_quarantined_key(&akey)),
            )
            .await?;
        if !sidecar_left && !metadata_left && !provenance_left && !demotion_fence_left {
            return Ok(false);
        }
        storage
            .delete_keys(&[
                sidecar_key(&akey),
                metadata_key(&akey),
                provenance_key(&akey),
                mirror_quarantined_key(&akey),
            ])
            .await?;
        return Ok(true);
    }
    tombstone::write(storage, &akey, filename).await?;
    drop_record_objects(storage, pkg, filename).await?;
    Ok(true)
}

/// The canonical record objects for one filename: the body and everything that
/// describes it. Never the durable fences (`.tombstone`, `.frozen`,
/// `.mirror-quarantined`) — those outlive the record by design.
fn record_object_keys(akey: &str) -> [String; 4] {
    [
        akey.to_string(),
        sidecar_key(akey),
        metadata_key(akey),
        provenance_key(akey),
    ]
}

/// Remove a filename's artifact + sidecar + companions after its durable
/// tombstone/freeze record is in place — and the mirror-quarantine fence with
/// them. A tombstone bars the filename permanently, which strictly subsumes a
/// demotion fence that deliberately does not, so leaving the marker behind
/// would strand this bucket one key apart from a peer that never demoted.
/// (`.frozen` is kept instead: it is the richer diagnostic, and it replicates.)
/// Errors propagate so the marker remains and the next sweep retries cleanup.
async fn drop_record_objects(storage: &dyn Storage, pkg: &str, filename: &str) -> Result<()> {
    let akey = artifact_key(pkg, filename);
    let mut keys = record_object_keys(&akey).to_vec();
    keys.push(mirror_quarantined_key(&akey));
    // A supersede intent over a filename that just became tombstoned or frozen
    // has nothing left to finish; the same one-key-apart argument applies.
    keys.push(superseding_key(&akey));
    storage.delete_keys(&keys).await
}

/// Audit repair for an impossible within-bucket state: a package claim is
/// private, yet one or more live artifacts still carry mirror sidecars. Each is
/// a demotion loser — settle it through the one shared primitive the merge's
/// [`Verdict::SettleMirrorQuarantine`] also runs, so audit and merge cannot
/// drift. The caller rebuilds the package index after a non-zero count.
///
/// A record already fenced is not skipped: an interrupted settle leaves the
/// fence over a body that still needs moving, and the settle is idempotent. A
/// *private* sidecar under the fence is left alone — that is a supersede
/// landing private truth, which is the demotion's intended resolution. The
/// selection below is only the cheap half of that: the primitive itself now
/// re-reads ([`demotion_resolved_by_private_truth`]), because the merge reaches
/// it with a listing-era verdict this scan's fresh read cannot stand in for.
pub async fn quarantine_mirror_artifacts(storage: &dyn Storage, pkg: &str) -> Result<usize> {
    let Some(claim) = read_origin_observation(storage, pkg).await? else {
        return Ok(0);
    };
    if claim.state != OriginState::Private {
        return Ok(0);
    }
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let entries = storage.list_dir_entries(&prefix).await?;
    let mut quarantined = 0;
    for entry in entries {
        let Some(filename) = entry.key.strip_prefix(&prefix) else {
            continue;
        };
        if !crate::sidecar::is_artifact(filename) {
            continue;
        }
        let sidecar_bytes = match storage.get_bytes(&sidecar_key(&entry.key)).await {
            Ok(bytes) => bytes,
            Err(e) if is_not_found(&e) => continue,
            Err(e) => return Err(e),
        };
        let sidecar: Sidecar = serde_json::from_slice(&sidecar_bytes)
            .with_context(|| format!("parse sidecar for {}", entry.key))?;
        match sidecar.origin.as_deref() {
            Some(MIRROR) => {}
            Some(PRIVATE) | None => continue,
            Some(raw) => {
                bail!("sidecar for {pkg}/{filename} holds an unexpected origin '{raw}'")
            }
        }
        if settle_mirror_quarantine(storage, pkg, filename).await? {
            quarantined += 1;
        }
    }
    Ok(quarantined)
}

/// Finish a [`supersede_record`] that died between writing a body and
/// publishing the sidecar that names it. `filenames` is the set the caller's
/// package listing already flagged with a `.superseding` marker — the rare set,
/// so a fleet that never crashed mid-supersede does no work here at all. That
/// gating is the whole design: the repair itself has to re-hash a body, which
/// is affordable for a handful of flagged records and is not affordable as a
/// blanket sweep over a 770k-package mirror.
///
/// Called from the index rebuild rather than the periodic audit, and the
/// simulator's repair classifier is what settles that: the executor already
/// brackets every supersede in the `_dirty/` intent pair, so the crashed
/// package is guaranteed a rebuild, and healing anywhere later means the views
/// stay pointed at the stale digest until the audit happens to come round.
///
/// One shot per marker, whatever it finds. The marker's only job is to say "the
/// body under this key may be ahead of its sidecar"; once this has read the
/// body, that question is answered for good and re-asking it on every audit
/// would turn an O(1) listing check into an O(bytes) one.
///
/// Returns the number of records whose sidecar this call published — the
/// caller's signal to rebuild the package's views over the repaired truth.
pub async fn finish_interrupted_supersedes(
    storage: &dyn Storage,
    pkg: &str,
    filenames: &[String],
) -> Result<usize> {
    let mut finished = 0;
    for filename in filenames {
        let akey = artifact_key(pkg, filename);
        let marker = superseding_key(&akey);
        let intended: Sidecar = match storage.get_bytes(&marker).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parse supersede intent at {marker}"))?,
            // Another node finished it between the listing and now.
            Err(e) if is_not_found(&e) => continue,
            Err(e) => return Err(e),
        };
        // A tombstone or freeze landed over the filename after the intent: the
        // record is adjudicated gone and publishing its sidecar would resurrect
        // suppressed truth. `drop_record_objects` normally takes the marker with
        // it; this covers the interleaving where the intent outlived it. Checked
        // before the body read, which is the expensive part.
        let (tombstoned, frozen) = futures::future::try_join(
            storage.head_exists(&tombstone_key(&akey)),
            storage.head_exists(&frozen_key(&akey)),
        )
        .await?;
        if !tombstoned && !frozen {
            let body = match storage.get_bytes(&akey).await {
                Ok(body) => Some(body),
                Err(e) if is_not_found(&e) => None,
                Err(e) => return Err(e),
            };
            // The body is what the interrupted supersede meant to install, so
            // the sidecar naming it is the completion — and only that. A body
            // still holding the *old* bytes means the crash landed before the
            // replacement and the record already describes itself correctly; a
            // body that is neither was written by something this intent knows
            // nothing about. Both are left exactly as found: this finishes a
            // torn write, it never adjudicates one.
            if body.is_some_and(|body| sha256_hex(&body) == intended.sha256)
                && install_or_verify_sidecar(storage, &sidecar_key(&akey), &intended).await?
            {
                finished += 1;
            }
        }
        storage.delete_keys(&[marker]).await?;
    }
    Ok(finished)
}

// ---------------------------------------------------------------------------
// Tier 1 — synchronous fan-out (pre-ack).
// ---------------------------------------------------------------------------

/// Stream a just-committed private record from the selected bucket to every
/// other bucket *before* the client ack. Healthy secondaries are copied
/// concurrently — each via the same
/// [`replicate_record`] copy protocol as the sweep and full diff (origin claim,
/// then the sha256-verified artifact, then the sidecar that names it) — under
/// one shared grace deadline measured from the selected write's completion.
///
/// A secondary that fails, exceeds the grace deadline, becomes topology-
/// ineligible mid-copy, is already ineligible (so no copy is attempted), or
/// whose merge *defers* ([`Convergence::Deferred`] — it holds a bare artifact
/// only its own audit can resolve) gets a durable `_repl/<dest>/…` note in the
/// selected bucket before this returns. Notes are the failure path only: a
/// healthy fleet acks with every bucket holding the record and no note written.
/// A single-bucket node does no I/O.
///
/// `Err` means the opposite of a failed copy: a gap this call could not write
/// down. The note IS the durability guarantee behind an ack — it is the fleet's
/// only record that a peer is owed this file — so a note that will not land
/// leaves the caller acking a state the ACK_TOTALITY oracle forbids: a peer
/// holding neither the record, nor a marker explaining its absence, nor a note
/// owing it. Nothing downstream re-derives it (the `_dirty/` markers drive
/// index rebuilds, not replication), so the caller must refuse the ack instead.
/// Every copy failure above is still an `Ok`: those are recorded.
pub async fn fanout_sync(
    state: &AppState,
    pinned: &Pinned,
    pkg: &str,
    filename: &str,
    spool: Option<&std::path::Path>,
) -> Result<()> {
    if !state.buckets.is_multi() {
        return Ok(());
    }
    let src = pinned.storage.as_ref();
    let src_index = pinned.index;
    // The just-committed upload still holds its verified spool; every peer reads
    // it locally instead of GETting the artifact back from the source bucket.
    // Yank/delete/marker fan-outs carry no spool and read the bucket as before.
    let source = spool.map_or(ArtifactSource::Bucket, ArtifactSource::Spool);
    // A fenced topology or an ineligible selected bucket cannot safely copy;
    // every peer then gets a note so the eventual heal drains it. Health probes
    // and topology verification are the only writers past this gate.
    let can_copy = require_replication_unfenced(state).is_ok() && bucket_eligible(state, src_index);
    let deadline = tokio::time::Instant::now() + state.fanout_grace;

    let jobs = state
        .buckets
        .handles()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != src_index)
        .map(|(idx, handle)| {
            let attempt = can_copy && bucket_eligible(state, idx);
            async move {
                if !attempt {
                    return (idx, false);
                }
                let result = tokio::select! {
                    result = replicate_record(state, src, handle.storage.as_ref(), pkg, filename, source) => result,
                    _ = tokio::time::sleep_until(deadline) => {
                        Err(anyhow!("fan-out to {} exceeded the grace deadline", handle.name))
                    }
                    _ = wait_until_pair_ineligible(state, src_index, idx) => {
                        Err(anyhow!("source or destination became topology-ineligible"))
                    }
                };
                match result {
                    Ok(Convergence::Converged) => (idx, true),
                    // The merge declined (the peer holds a bare artifact of its
                    // own only its audit can resolve). Nothing was copied, so
                    // this is a failed fan-out for totality purposes: note it.
                    Ok(Convergence::Deferred) => {
                        warn!(dest=%handle.name, package=%pkg, filename=%filename, "synchronous fan-out deferred by the merge; leaving a repair note");
                        (idx, false)
                    }
                    Err(e) => {
                        warn!(dest=%handle.name, package=%pkg, filename=%filename, error=?e, "synchronous fan-out failed; leaving a repair note");
                        (idx, false)
                    }
                }
            }
        });
    // Start every destination before awaiting any of them, so a blackholed
    // middle bucket cannot delay a healthy later one (both are still bounded by
    // the shared deadline). Then write one durable note per bucket that did not
    // converge, before the caller acks.
    let handles = state.buckets.handles();
    let mut unrecorded = 0usize;
    for (idx, converged) in futures::future::join_all(jobs).await {
        if converged {
            continue;
        }
        let dest_tag = crate::counters::bucket_tag(&handles[idx].name);
        // One retry: the note is a single small PUT and a transient 5xx must
        // not be what decides a publish. A second failure is a bucket that is
        // not taking writes, and no amount of looping in front of the client
        // fixes that.
        let mut wrote = write_marker(src, &dest_tag, pkg, filename).await;
        if let Err(error) = &wrote {
            warn!(dest=%handles[idx].name, package=%pkg, filename=%filename, error=?error, "replication repair note failed; retrying once before the ack");
            wrote = write_marker(src, &dest_tag, pkg, filename).await;
        }
        if let Err(error) = wrote {
            error!(dest=%handles[idx].name, package=%pkg, filename=%filename, error=?error, "could not write replication repair note; refusing to ack the publish");
            unrecorded += 1;
        }
    }
    if unrecorded > 0 {
        // A count, not the names: this string reaches the client in the 503 body,
        // and the peer buckets' configured URIs (and `@region` labels) are fleet
        // topology no uploader should learn from a transient outage. Every
        // failing bucket is named in the `error!` line just above.
        bail!(
            "replication gap for {pkg}/{filename} could not be recorded on {} of {} other bucket(s)",
            unrecorded,
            handles.len().saturating_sub(1)
        );
    }
    Ok(())
}

/// Async proxy-cache replication (deliberately NOT tier 1: no pre-ack fan-out).
/// A proxy fill is served off the write bucket the instant it commits, so its
/// peer copies must never sit on the request's critical path. Instead, once the
/// fill has committed the artifact + sidecar, the serve handler spawns this
/// **after** the response, and it drops one durable `_repl/<peer>/…` note per
/// peer in the write bucket. The ordinary marker sweep then drains each note over
/// the same copy path as any other repair note (read both records → `decide` →
/// `Copy` → `copy_live`, which now installs a mirror-cache sidecar). A fill is
/// one-time per file, so one small PUT per peer is acceptable.
///
/// Best-effort by design: the note is a latency optimization, not the durability
/// guarantee. If this task is lost — a crash between the commit and the spawn, a
/// shutdown mid-write — the periodic reconcile full diff heals it, because
/// `decide` now copies a mirror cache to an absent peer. The note is written only
/// after the fill's artifact + sidecar are durable, so a sweep never chases a
/// half-written record.
pub fn spawn_proxy_fill_notes(
    state: Arc<AppState>,
    src_index: usize,
    pkg: String,
    filename: String,
) {
    if !state.buckets.is_multi() {
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = note_proxy_fill(&state, src_index, &pkg, &filename).await {
            warn!(package=%pkg, filename=%filename, error=?error, "proxy fill: could not note peer replication; reconcile will heal");
        }
    });
}

async fn note_proxy_fill(
    state: &AppState,
    src_index: usize,
    pkg: &str,
    filename: &str,
) -> Result<()> {
    // A fenced topology, or a write bucket that just left eligibility, cannot be
    // a trustworthy copy source; drop the notes and let the reconcile diff heal
    // once the fleet settles. (Health probes/topology verification are the only
    // writers allowed past this gate.)
    if require_replication_unfenced(state).is_err() || !bucket_eligible(state, src_index) {
        return Ok(());
    }
    let handles = state.buckets.handles();
    let Some(src) = handles.get(src_index) else {
        return Ok(());
    };
    let mut failures = Vec::new();
    for (idx, dst) in handles.iter().enumerate() {
        if idx == src_index {
            continue;
        }
        // Note every peer, even a currently-ineligible one: the marker is the
        // durable record that `idx` still owes this fill, drained on its heal —
        // exactly the failure-path role notes play for a synchronous fan-out.
        let dest_tag = crate::counters::bucket_tag(&dst.name);
        if let Err(error) = write_marker(src.storage.as_ref(), &dest_tag, pkg, filename).await {
            failures.push(format!("peer {idx}: {error:#}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("proxy-fill notes failed: {}", failures.join("; "))
    }
}

/// The outcome of [`reconcile_split_origin`]: each side's origin after promoting
/// a lone private claim, and whether that side's mirror artifacts were scanned
/// (so a caller caching the member listing refreshes the scanned side).
struct SplitOriginReconciled {
    a_origin: Option<Origin>,
    b_origin: Option<Origin>,
    scanned_a: bool,
    scanned_b: bool,
}

/// Reconcile a cross-bucket origin split before a package is diffed. When
/// exactly one side holds the private claim, promote the peer's claim to private
/// and quarantine any mirror bodies stranded on it (marking that side dirty so
/// its own leader rebuilds). A lone mirror claim is reserved on the peer. A
/// pairing that already agrees is left untouched.
async fn reconcile_split_origin(
    a: &dyn Storage,
    b: &dyn Storage,
    pkg: &str,
    mut a_origin: Option<Origin>,
    mut b_origin: Option<Origin>,
) -> Result<SplitOriginReconciled> {
    let (mut scanned_a, mut scanned_b) = (false, false);
    // A claim is reserved fleet-wide ahead of its bytes — `publish::store_upload`
    // fans `.origin` out to every bucket before the artifact lands — so a claim
    // on one side and none on the other is a fan-out a partition cut short, not
    // a steady state. Private is repaired by the arms below; mirror is repaired
    // here, and nowhere else: an *empty* claim has no record for the copy path's
    // own `ensure_mirror_origin` to ride along with, so without this the
    // reservation stays bucket-local forever. `.origin` lives under `packages/`,
    // which `src/layout.rs` classes truth-replicated, and truth that never
    // converges is a bug, not a trade-off (dev/DESIGN.md, the totality
    // principle). Until it converges the two nodes disagree about who owns the
    // name: one answers a private upload with 403 mirror-owned while its peer
    // accepts the same upload, and a bucket loss silently unclaims the name.
    // The reserve never demotes private, so it returns `mirror` or a racer's
    // `private`; the latter falls straight into the split arms below.
    if a_origin == Some(Origin::Mirror) && b_origin.is_none() {
        b_origin = Some(ensure_mirror_origin(b, pkg).await?);
    } else if b_origin == Some(Origin::Mirror) && a_origin.is_none() {
        a_origin = Some(ensure_mirror_origin(a, pkg).await?);
    }
    match (a_origin, b_origin) {
        (Some(Origin::Private), Some(Origin::Mirror)) => {
            ensure_private_origin(b, pkg).await?;
            if quarantine_mirror_artifacts(b, pkg).await? > 0 {
                markers::mark_dirty(b, pkg).await?;
            }
            scanned_b = true;
            b_origin = Some(Origin::Private);
        }
        (Some(Origin::Mirror), Some(Origin::Private)) => {
            ensure_private_origin(a, pkg).await?;
            if quarantine_mirror_artifacts(a, pkg).await? > 0 {
                markers::mark_dirty(a, pkg).await?;
            }
            scanned_a = true;
            a_origin = Some(Origin::Private);
        }
        (Some(Origin::Private), None) => {
            ensure_private_origin(b, pkg).await?;
            b_origin = Some(Origin::Private);
        }
        (None, Some(Origin::Private)) => {
            ensure_private_origin(a, pkg).await?;
            a_origin = Some(Origin::Private);
        }
        _ => {}
    }
    Ok(SplitOriginReconciled {
        a_origin,
        b_origin,
        scanned_a,
        scanned_b,
    })
}

/// Replicate one record from `src` into `dst` (tiers 1 and 2). Reads both
/// sides, decides, and applies — the same merge that reconcile runs, so an
/// ordered byte conflict quarantines the loser and an ambiguous one freezes.
async fn replicate_record(
    state: &AppState,
    src: &dyn Storage,
    dst: &dyn Storage,
    pkg: &str,
    filename: &str,
    source: ArtifactSource<'_>,
) -> Result<Convergence> {
    require_replication_unfenced(state)?;
    let src_origin = read_pkg_origin(src, pkg).await?;
    let dst_origin = read_pkg_origin(dst, pkg).await?;
    let SplitOriginReconciled {
        a_origin: src_origin,
        b_origin: dst_origin,
        ..
    } = reconcile_split_origin(src, dst, pkg, src_origin, dst_origin).await?;

    if filename == ORIGIN_MARKER {
        // The claim itself is the whole record, and `reconcile_split_origin`
        // above already reserved it on the peer — private or mirror alike —
        // ahead of any bytes. Nothing left to copy.
        return Ok(Convergence::Converged);
    }
    if filename == PROJECT_STATUS_MARKER {
        if src_origin == Some(Origin::Private) || dst_origin == Some(Origin::Private) {
            reconcile_project_status(src, dst, pkg).await?;
        }
        return Ok(Convergence::Converged);
    }
    let a = read_record(src, pkg, filename).await?;
    let b = read_record(dst, pkg, filename).await?;
    let verdict = decide(&a, &b);
    execute(state, (src, dst), pkg, filename, (&a, &b), verdict, source).await
}

async fn normalize_mirror_status_under_private_claim(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<bool> {
    let Some(claim) = read_origin_observation(storage, pkg).await? else {
        return Ok(false);
    };
    if claim.state != OriginState::Private {
        return Ok(false);
    }
    let Some(initial_status) = status::read_status_versioned(storage, pkg).await? else {
        return Ok(false);
    };
    if initial_status.origin != Some(Origin::Mirror) {
        return Ok(false);
    }

    // Join the same base package-intent protocol as request writers. The exact
    // claim re-read below keeps a concurrent owner transition fail-closed.
    let nonce = markers::mark_intent(storage, pkg).await?;
    let result: Result<bool> = async {
        if read_origin_observation(storage, pkg).await?.as_ref() != Some(&claim) {
            return Ok(false);
        }
        for _ in 0..ORIGIN_ATTEMPTS {
            let current = status::read_status_versioned(storage, pkg).await?;
            let Some(current) = current else {
                return Ok(false);
            };
            if current.origin != Some(Origin::Mirror) {
                return Ok(false);
            }
            if status::put_status_if_version(
                storage,
                pkg,
                Some(&current.etag),
                &status::ProjectStatusDoc::default(),
                0,
                Some(Origin::Private),
            )
            .await?
            {
                return Ok(true);
            }
        }
        bail!("could not normalize late mirror status for private package '{pkg}'")
    }
    .await;
    let committed = markers::mark_commit(storage, pkg, &nonce).await;
    match (result, committed) {
        (Ok(changed), Ok(())) => Ok(changed),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

async fn reconcile_project_status(a: &dyn Storage, b: &dyn Storage, pkg: &str) -> Result<()> {
    // Normalization owns an intent/commit pair, so its commit is already the
    // dirty event. Emitting another marker would only force a duplicate rebuild.
    normalize_mirror_status_under_private_claim(a, pkg).await?;
    normalize_mirror_status_under_private_claim(b, pkg).await?;
    match status::reconcile_status_pair(a, b, pkg).await? {
        StatusConvergence::InSync => {}
        StatusConvergence::UpdatedLeft => {
            markers::mark_dirty(a, pkg).await?;
        }
        StatusConvergence::UpdatedRight => {
            markers::mark_dirty(b, pkg).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tier 2 — _repl/ todo markers.
// ---------------------------------------------------------------------------

async fn write_marker(
    storage: &dyn Storage,
    dest_tag: &str,
    pkg: &str,
    filename: &str,
) -> Result<String> {
    let key = format!(
        "{REPL_PREFIX}{dest_tag}/{pkg}/{filename}!{}",
        markers::marker_nonce()
    );
    storage.put_bytes(&key, Vec::new(), None).await?;
    Ok(key)
}

struct ReplMarker {
    /// The stable [`crate::counters::bucket_tag`] of the destination bucket this
    /// note is owed to — never its position in the list, so a topology reorder or
    /// removal can neither orphan the note nor point it at the wrong bucket.
    dest_tag: String,
    pkg: String,
    filename: String,
    key: String,
}

/// Parse `_repl/<dest-tag>/<pkg>/<file>!<nonce>`. The tag is
/// [`crate::counters::bucket_tag`] output (charset `[A-Za-z0-9._-]`, never `/`),
/// package names and filenames carry no `!`, and the nonce carries no `/`, so the
/// split is unambiguous. Backfill sentinels (`_repl/<tag>/_backfill!<nonce>`) have
/// only one path segment after the tag and so return `None` here — they are gate
/// markers, not repair notes.
fn parse_repl_marker(key: &str) -> Option<ReplMarker> {
    let rest = key.strip_prefix(REPL_PREFIX)?;
    let (dest_tag, rest) = rest.split_once('/')?;
    let (pkg, file_nonce) = rest.split_once('/')?;
    let (filename, _nonce) = file_nonce.rsplit_once('!')?;
    Some(ReplMarker {
        dest_tag: dest_tag.to_string(),
        pkg: pkg.to_string(),
        filename: filename.to_string(),
        key: key.to_string(),
    })
}

/// Resolve a destination [`crate::counters::bucket_tag`] back to its current
/// position in the configured list. `None` means no configured bucket carries that
/// tag any more — the destination was removed, so a note addressed to it can never
/// be delivered.
fn dest_index_for_tag(handles: &[crate::buckets::BucketHandle], dest_tag: &str) -> Option<usize> {
    handles
        .iter()
        .position(|handle| crate::counters::bucket_tag(&handle.name) == dest_tag)
}

/// Sweep markers from every configured source bucket on the fast worker tick.
/// Backlog is aggregated by destination across all sources, so a straggler on
/// an old selection is visible and drains without waiting for the full-diff
/// cadence. Every source is attempted even if another is unreachable; errors
/// are returned together after the reachable work completes.
pub async fn sweep_all_markers(state: &AppState) -> Result<()> {
    if !state.buckets.is_multi() {
        return Ok(());
    }
    require_replication_unfenced(state)?;
    let mut failures = Vec::new();
    let mut total: HashMap<usize, u64> = HashMap::new();
    let mut sweeps = futures::stream::FuturesUnordered::new();
    for idx in (0..state.buckets.len()).filter(|idx| bucket_eligible(state, *idx)) {
        sweeps.push(async move { (idx, sweep_bucket_markers(state, idx).await) });
    }
    while let Some((idx, result)) = sweeps.next().await {
        match result {
            Ok(backlog) => {
                for (dest, count) in backlog {
                    *total.entry(dest).or_default() += count;
                }
            }
            Err(e) => failures.push(format!("source bucket {idx}: {e:#}")),
        }
        // Publish after every completed source. A blackholed source remains in
        // flight, but cannot hide work already observed on a healthy source.
        publish_marker_backlog(state, &total);
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("marker sweeps failed: {}", failures.join("; "))
    }
}

/// Whether a bucket holds any undrained `_repl/` *repair note* — a real fan-out
/// marker (`_repl/<dest>/<pkg>/<file>!<nonce>`) that may be a record's sole copy.
/// Backfill sentinels (`_repl/<dest>/_backfill!<nonce>`) share the prefix but are
/// empty gate markers, not repair notes: they fence a freshly-added bucket's
/// region reads, never a topology reshape, so they are deliberately excluded —
/// otherwise seeding one on `buckets migrate` would wedge every later migrate
/// until a server ran a reconcile. `buckets migrate` uses this to refuse
/// shrinking/reordering while a genuine repair note is still stranded. Bounded:
/// pages `_repl/` and returns on the first key that [`parse_repl_marker`] accepts;
/// the sentinel count is O(buckets²), so at most a page or two are skipped.
pub async fn has_undrained_repl_notes(storage: &dyn Storage) -> Result<bool> {
    let mut after: Option<String> = None;
    loop {
        let page = storage
            .list_page(REPL_PREFIX, after.as_deref(), REPL_SWEEP_PAGE)
            .await?;
        if page.iter().any(|obj| parse_repl_marker(&obj.key).is_some()) {
            return Ok(true);
        }
        if page.len() < REPL_SWEEP_PAGE {
            return Ok(false);
        }
        after = page.last().map(|obj| obj.key.clone());
    }
}

/// Whether a bucket holds no artifact under `packages/` — a bounded single-key
/// LIST. `buckets migrate` uses it when no prior topology stamp exists (the
/// single-bucket → multi-bucket expansion, where single mode never stamps a
/// member list to diff against): a bucket that already holds corpus is the
/// established source and keeps serving region reads, while an empty one is
/// fenced with a backfill sentinel until a clean reconcile proves it caught up.
pub async fn bucket_is_corpus_empty(storage: &dyn Storage) -> Result<bool> {
    Ok(storage
        .list_page(crate::app::PACKAGES_PREFIX, None, 1)
        .await?
        .is_empty())
}

/// Whether this bucket holds any undrained `_repl/<dest-tag>/` note — a repair
/// still owed *to* the bucket whose stable [`crate::counters::bucket_tag`] is
/// `dest_tag`. The read-affinity worker checks every other bucket with this before
/// it lets reads return to that bucket (its region bucket): an outstanding note
/// means the destination is missing an acked file, so reads stay on the write
/// bucket until it drains. Keying on the tag (not the list position) means the
/// fence follows its bucket across any topology reorder or removal. Same bounded
/// single-key LIST as [`has_undrained_repl_notes`].
pub async fn has_undrained_repl_notes_for(storage: &dyn Storage, dest_tag: &str) -> Result<bool> {
    let prefix = format!("{REPL_PREFIX}{dest_tag}/");
    Ok(!storage.list_page(&prefix, None, 1).await?.is_empty())
}

/// Whether region bucket `region` is owed no undrained note by any peer — no real
/// repair marker and no backfill sentinel under any peer's `_repl/<region-tag>/`.
/// The destination is addressed by its stable [`crate::counters::bucket_tag`], so
/// the fence a peer holds still points at this bucket after any topology reorder.
/// Conservative: an unreachable peer or any error reports `false`, so reads never
/// return to a bucket that might still be missing corpus. Startup uses it before
/// seeding a region read pin; the worker's own caught-up check applies the same
/// gate to *return* reads after a recovery.
pub async fn region_owed_no_notes(handles: &[crate::buckets::BucketHandle], region: usize) -> bool {
    let Some(region_handle) = handles.get(region) else {
        return true;
    };
    let region_tag = crate::counters::bucket_tag(&region_handle.name);
    for (index, handle) in handles.iter().enumerate() {
        if index == region {
            continue;
        }
        match has_undrained_repl_notes_for(handle.storage.as_ref(), &region_tag).await {
            Ok(false) => {}
            Ok(true) => return false,
            Err(error) => {
                warn!(bucket=%handle.name, target=region, error=?error, "could not confirm region bucket caught up at startup; reads follow the write pin");
                return false;
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Backfill sentinel — a freshly-added bucket owes reads nothing until converged.
// ---------------------------------------------------------------------------

/// Filename component of the backfill sentinel a freshly-added bucket seeds under
/// `_repl/<dest>/`. It names no package, so [`parse_repl_marker`] returns `None`
/// and the marker sweep never tries to "deliver" it — only a clean full reconcile
/// pass clears it. It rides the very `_repl/<dest>/` prefix that
/// [`has_undrained_repl_notes_for`] (read affinity) and [`has_undrained_repl_notes`]
/// (`buckets migrate`) already check, so a new region bucket serves no region
/// reads until the corpus has converged onto it — no per-file read-through.
const BACKFILL_SENTINEL: &str = "_backfill!";

fn backfill_sentinel_prefix(dest_tag: &str) -> String {
    format!("{REPL_PREFIX}{dest_tag}/{BACKFILL_SENTINEL}")
}

/// Seed the backfill sentinel for a newly-added bucket on a surviving peer, keyed
/// by the new bucket's stable [`crate::counters::bucket_tag`]. O(1): one empty
/// create, never a corpus walk. `buckets migrate` calls this once per bucket the
/// new topology adds, on every other reachable bucket, so the read-affinity gate
/// holds whichever peer a node happens to consult. Keying on the tag rather than
/// the list position is what lets the fence survive a later reorder or removal
/// without orphaning the sentinel (or false-fencing a bucket that lands on a
/// recycled position).
pub async fn seed_backfill_sentinel(storage: &dyn Storage, dest_tag: &str) -> Result<()> {
    let key = format!(
        "{}{}",
        backfill_sentinel_prefix(dest_tag),
        markers::marker_nonce()
    );
    storage.put_bytes(&key, Vec::new(), None).await
}

/// Remove every backfill sentinel across the fleet. Called only after a fully
/// clean reconcile pass — which proves every reachable bucket now holds the whole
/// corpus — so a freshly-added bucket may finally serve region reads. Bounded:
/// one narrow LIST plus at most one delete per (bucket, dest).
async fn drain_backfill_sentinels(state: &AppState) -> Result<()> {
    let handles = state.buckets.handles();
    let mut failures = Vec::new();
    for handle in handles {
        // Only currently-configured tags are drained; a removed bucket's sentinel
        // lingers as harmless dead bytes (never queried again — its tag names no
        // handle, so no gate ever LISTs it), not worth a self-cleaning sweep.
        for dest in handles {
            let prefix = backfill_sentinel_prefix(&crate::counters::bucket_tag(&dest.name));
            match handle
                .storage
                .list_page(&prefix, None, REPL_SWEEP_PAGE)
                .await
            {
                Ok(page) if page.is_empty() => {}
                Ok(page) => {
                    let keys: Vec<String> = page.into_iter().map(|obj| obj.key).collect();
                    if let Err(e) = handle.storage.delete_keys(&keys).await {
                        failures.push(format!("{}: {e:#}", handle.name));
                    }
                }
                Err(e) => failures.push(format!("{}: {e:#}", handle.name)),
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("backfill sentinel drain failed: {}", failures.join("; "))
    }
}

// ---------------------------------------------------------------------------
// Migrate removal gate — a bucket holding the fleet's only copies never drops.
// ---------------------------------------------------------------------------

/// Page size for the migrate removal diff: one S3 LIST page, the same bound as
/// the reconcile scan so no whole `packages/` tree is ever resident.
const REMOVAL_DIFF_PAGE: usize = 1_000;

/// One bucket's `packages/` keys streamed in ascending order, one bounded page
/// resident at a time. Forward-only: [`Self::contains`] advances past every key
/// strictly less than its target, so feeding it the removed bucket's own
/// ascending artifact keys walks each survivor's listing exactly once overall —
/// the removal diff is a linear multi-way merge (one LIST per page per bucket),
/// never a HEAD per artifact.
struct PagedKeys<'a> {
    storage: &'a dyn Storage,
    after: Option<String>,
    buf: std::collections::VecDeque<String>,
    done: bool,
}

impl<'a> PagedKeys<'a> {
    fn new(storage: &'a dyn Storage) -> Self {
        Self {
            storage,
            after: None,
            buf: std::collections::VecDeque::new(),
            done: false,
        }
    }

    async fn fill(&mut self) -> Result<()> {
        if self.buf.is_empty() && !self.done {
            let page = self
                .storage
                .list_page(PACKAGES_PREFIX, self.after.as_deref(), REMOVAL_DIFF_PAGE)
                .await?;
            if page.len() < REMOVAL_DIFF_PAGE {
                self.done = true;
            }
            self.after = page.last().map(|obj| obj.key.clone());
            for obj in page {
                self.buf.push_back(obj.key);
            }
        }
        Ok(())
    }

    /// Advance past every buffered key `< target`, then report whether `target`
    /// is present on this bucket. Targets must arrive in ascending order.
    async fn contains(&mut self, target: &str) -> Result<bool> {
        loop {
            self.fill().await?;
            match self.buf.front() {
                None => return Ok(false),
                Some(front) if front.as_str() < target => {
                    self.buf.pop_front();
                }
                Some(front) => return Ok(front.as_str() == target),
            }
        }
    }
}

/// Up to `sample_cap` `packages/` artifact keys that live on `removed` but on no
/// surviving bucket. An empty result means every artifact `removed` holds is safe
/// elsewhere, so the bucket can be dropped without losing content. Short-circuits
/// once the sample fills, so a badly-diverged bucket is rejected cheaply. A
/// survivor that errors mid-diff propagates: the caller must treat an
/// unverifiable survivor as a refusal, never a silent drop.
pub async fn artifacts_unique_to_removed(
    removed: &dyn Storage,
    survivors: &[Arc<dyn Storage>],
    sample_cap: usize,
) -> Result<Vec<String>> {
    let mut survivor_keys: Vec<PagedKeys> = survivors
        .iter()
        .map(|s| PagedKeys::new(s.as_ref()))
        .collect();
    let mut samples = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = removed
            .list_page(PACKAGES_PREFIX, after.as_deref(), REMOVAL_DIFF_PAGE)
            .await
            .context("list the removed bucket's packages/ tree")?;
        if page.is_empty() {
            break;
        }
        let full = page.len() >= REMOVAL_DIFF_PAGE;
        after = page.last().map(|obj| obj.key.clone());
        for obj in &page {
            let filename = obj.key.rsplit('/').next().unwrap_or("");
            if !crate::sidecar::is_artifact(filename) {
                continue;
            }
            let mut present = false;
            for survivor in &mut survivor_keys {
                if survivor
                    .contains(&obj.key)
                    .await
                    .context("check a surviving bucket for the removed bucket's artifact")?
                {
                    present = true;
                    break;
                }
            }
            if !present {
                samples.push(obj.key.clone());
                if samples.len() >= sample_cap {
                    return Ok(samples);
                }
            }
        }
        if !full {
            break;
        }
    }
    Ok(samples)
}

fn publish_marker_backlog(state: &AppState, total: &HashMap<usize, u64>) {
    for (idx, handle) in state.buckets.handles().iter().enumerate() {
        state
            .metrics
            .set_marker_backlog(&handle.name, total.get(&idx).copied().unwrap_or(0));
    }
}

/// Resolve once the bucket leaves the eligible set (health failure or a
/// recovery still awaiting topology validation). Shared with the worker's job
/// loop so both paths abandon a stalled bucket on the same predicate.
pub(crate) async fn wait_until_bucket_ineligible(state: &AppState, index: usize) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if !bucket_eligible(state, index) {
            return;
        }
    }
}

async fn wait_until_pair_ineligible(state: &AppState, src: usize, dst: usize) {
    tokio::select! {
        _ = wait_until_bucket_ineligible(state, src) => {}
        _ = wait_until_bucket_ineligible(state, dst) => {}
    }
}

/// Drain one bucket's `_repl/` tree, returning the number of markers that
/// remained undelivered per destination index.
async fn sweep_bucket_markers(state: &AppState, src_index: usize) -> Result<HashMap<usize, u64>> {
    let handles = state.buckets.handles();
    let src = handles[src_index].storage.clone();
    let mut remaining: HashMap<usize, u64> = HashMap::new();
    let mut after: Option<String> = None;
    // Page the `_repl/` tree so an arbitrarily large failure backlog is never
    // materialized in one Vec (v1 review finding). Each bounded page is fully
    // delivered before the next is listed; a page shorter than the cap is the
    // tail. Consumed markers are deleted, but the key cursor advances by the
    // page's last key, so retained (failed) markers are simply revisited next
    // sweep rather than re-listed this pass.
    loop {
        let page = tokio::select! {
            result = src.list_page(REPL_PREFIX, after.as_deref(), REPL_SWEEP_PAGE) => result?,
            _ = wait_until_bucket_ineligible(state, src_index) => {
                bail!("source bucket {src_index} became ineligible during marker listing")
            }
        };
        if page.is_empty() {
            break;
        }
        let page_len = page.len();
        after = page.last().map(|meta| meta.key.clone());

        // BTreeMap: destinations drain in index order, deterministically.
        let mut by_destination: std::collections::BTreeMap<usize, Vec<ReplMarker>> =
            std::collections::BTreeMap::new();
        for meta in &page {
            let Some(marker) = parse_repl_marker(&meta.key) else {
                continue;
            };
            let Some(dest_index) = dest_index_for_tag(handles, &marker.dest_tag) else {
                // Destination tag no longer names any configured bucket — the
                // marker cannot be delivered; drop it rather than retry forever.
                let _ = src.delete_keys(&[marker.key]).await;
                continue;
            };
            by_destination.entry(dest_index).or_default().push(marker);
        }
        // Each destination owns its own 16-wide lane. A large blackholed-B prefix
        // cannot consume every slot and prevent a later healthy-C marker starting.
        let outcomes = futures::stream::iter(by_destination)
            .map(|(dest_index, markers)| {
                let src = src.clone();
                async move {
                    if !bucket_eligible(state, dest_index) {
                        return (dest_index, markers.len() as u64);
                    }
                    let dst = &handles[dest_index];
                    let results = futures::stream::iter(markers)
                        .map(|marker| {
                            let src = src.clone();
                            async move {
                                let result = tokio::select! {
                                    result = replicate_record(
                                        state,
                                        src.as_ref(),
                                        dst.storage.as_ref(),
                                        &marker.pkg,
                                        &marker.filename,
                                        ArtifactSource::Bucket,
                                    ) => result,
                                    _ = wait_until_pair_ineligible(state, src_index, dest_index) => {
                                        Err(anyhow!("source or destination became topology-ineligible"))
                                    }
                                };
                                match result {
                                    // The merge declined: the destination holds
                                    // a bare artifact only its own audit can
                                    // resolve into a record. It is still owed
                                    // this one, so the note must survive — and
                                    // the rebuild that backfills the sidecar is
                                    // exactly what unblocks the next retry.
                                    Ok(Convergence::Deferred) => {
                                        if let Err(e) =
                                            markers::mark_dirty(dst.storage.as_ref(), &marker.pkg)
                                                .await
                                        {
                                            warn!(dest=%dst.name, package=%marker.pkg, filename=%marker.filename, error=?e, "replication deferred and the destination rebuild signal failed; marker retained");
                                        }
                                        true
                                    }
                                    Ok(Convergence::Converged) => {
                                        // The note's existence proves a fan-out went wrong
                                        // mid-flight, and the destination's "rebuild your
                                        // view" dirty marker may have been the write that
                                        // failed. The record may since have converged (this
                                        // retry decides Noop), so re-signal unconditionally:
                                        // one idempotent rebuild is cheap, a warm bucket
                                        // holding live truth with no view is not. The note
                                        // is only consumed once the signal is durable.
                                        if let Err(e) =
                                            markers::mark_dirty(dst.storage.as_ref(), &marker.pkg)
                                                .await
                                        {
                                            warn!(dest=%dst.name, package=%marker.pkg, filename=%marker.filename, error=?e, "replication succeeded but destination rebuild signal failed; marker retained");
                                            return true;
                                        }
                                        if let Err(e) = src.delete_keys(&[marker.key]).await {
                                            warn!(dest=%dst.name, package=%marker.pkg, filename=%marker.filename, error=?e, "replication succeeded but marker could not be consumed");
                                            return true;
                                        }
                                        false
                                    }
                                    Err(e) => {
                                        warn!(dest=%dst.name, package=%marker.pkg, filename=%marker.filename, error=?e, "replication marker retry failed; retained");
                                        true
                                    }
                                }
                            }
                        })
                        .buffer_unordered(16)
                        .collect::<Vec<_>>()
                        .await;
                    (dest_index, results.into_iter().filter(|pending| *pending).count() as u64)
                }
            })
            .buffer_unordered(handles.len().max(1))
            .collect::<Vec<_>>()
            .await;
        for (dest, count) in outcomes {
            *remaining.entry(dest).or_default() += count;
        }
        if page_len < REPL_SWEEP_PAGE {
            break;
        }
    }
    Ok(remaining)
}

// ---------------------------------------------------------------------------
// Tier 3 — full diff backstop (reconcile cadence + audit-on-boot).
// ---------------------------------------------------------------------------

/// The reconcile job (design §4 tier 3, §6): pairwise-diff the pinned bucket
/// against each other bucket through the merge rules. Marker delivery has its
/// own faster worker task; coupling it here would let one dead source delay a
/// healthy peer's lost-marker repair. A no-op on a single-bucket node.
pub async fn reconcile(state: &AppState, pinned: &Pinned) -> Result<()> {
    if !state.buckets.is_multi() {
        return Ok(());
    }
    require_replication_unfenced(state)?;
    if !bucket_eligible(state, pinned.index) {
        bail!(
            "selected bucket {} is not eligible for reconciliation",
            pinned.index
        );
    }
    let started = Instant::now();
    // Pairwise diff, both directions folded into one symmetric pass. Run the
    // selected bucket's star twice, with a barrier after every peer: pass one
    // gathers/finalizes the hub; pass two disseminates that settled state. Both
    // passes must be sequential. Concurrent peers can read the hub before a
    // later peer freezes it and leave the early peer live until tomorrow.
    let handles = state.buckets.handles();
    // A pass is "clean" only if every peer was diffed and every diff succeeded.
    // A skipped (ineligible) peer or a failed diff leaves a bucket unproven, so
    // the backfill sentinels must not drain: a freshly-added bucket might still
    // be missing that peer's unique content.
    let mut clean = true;
    for _ in 0..2 {
        for (index, handle) in handles.iter().enumerate() {
            if index == pinned.index {
                continue;
            }
            if !bucket_eligible(state, index) {
                clean = false;
                continue;
            }
            let result = tokio::select! {
                result = diff_pair(
                    state,
                    handles[pinned.index].storage.as_ref(),
                    handle.storage.as_ref(),
                ) => result,
                _ = wait_until_pair_ineligible(state, pinned.index, index) => {
                    Err(anyhow!("reconcile pair became topology-ineligible"))
                }
            };
            if let Err(e) = result {
                error!(left=%handles[pinned.index].name, right=%handles[index].name, error=?e, "reconcile: pairwise diff failed");
                clean = false;
            }
        }
    }
    // A fully clean pass proves every reachable bucket now holds the whole
    // corpus (two passes converge the pinned star both directions), so any
    // freshly-added bucket has caught up: drop its backfill sentinel and let
    // read affinity return region reads to it.
    if clean {
        if let Err(e) = drain_backfill_sentinels(state).await {
            warn!(error=?e, "reconcile: fleet converged but backfill sentinel drain failed; retrying next pass");
        }
    }
    state
        .metrics
        .set_reconcile_diff_duration(started.elapsed().as_secs_f64());
    Ok(())
}

/// A bounded, in-order scan of the distinct package names under `packages/` on
/// one bucket. Pages the flat listing (one S3 page resident at a time) and
/// yields each package name once, in ascending order — never materializing the
/// whole tree (v1 review finding). Two of these are merged by the reconcile diff.
struct PackageScan<'a> {
    storage: &'a dyn Storage,
    after: Option<String>,
    buf: std::collections::VecDeque<String>,
    /// Last name emitted, to dedup a package whose file keys straddle a page
    /// boundary (all of a package's keys are contiguous in sorted order).
    last_emitted: Option<String>,
    done: bool,
}

impl<'a> PackageScan<'a> {
    fn new(storage: &'a dyn Storage) -> Self {
        Self {
            storage,
            after: None,
            buf: std::collections::VecDeque::new(),
            last_emitted: None,
            done: false,
        }
    }

    /// Pull pages until at least one new package name is buffered or the tree is
    /// exhausted. A page that adds no name (a package larger than one page)
    /// simply advances the cursor and fetches the next.
    async fn fill(&mut self) -> Result<()> {
        while self.buf.is_empty() && !self.done {
            let page = self
                .storage
                .list_page(PACKAGES_PREFIX, self.after.as_deref(), RECONCILE_SCAN_PAGE)
                .await?;
            if page.is_empty() {
                self.done = true;
                break;
            }
            let full = page.len() >= RECONCILE_SCAN_PAGE;
            self.after = page.last().map(|obj| obj.key.clone());
            for obj in &page {
                if let Some((name, _)) = obj
                    .key
                    .strip_prefix(PACKAGES_PREFIX)
                    .and_then(|rest| rest.split_once('/'))
                {
                    if self.last_emitted.as_deref() != Some(name) {
                        self.buf.push_back(name.to_string());
                        self.last_emitted = Some(name.to_string());
                    }
                }
            }
            if !full {
                self.done = true;
            }
        }
        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<&str>> {
        self.fill().await?;
        Ok(self.buf.front().map(String::as_str))
    }

    fn advance(&mut self) {
        self.buf.pop_front();
    }
}

/// The distinct base filenames a package's member set implies — every artifact,
/// plus the base of every tombstone and freeze marker (so a deleted/frozen file
/// with no body is still reconciled).
fn candidate_filenames(names: &HashSet<String>) -> HashSet<String> {
    let mut out = HashSet::new();
    for name in names {
        if let Some(base) = name.strip_suffix(TOMBSTONE_SUFFIX) {
            out.insert(base.to_string());
        } else if let Some(base) = name.strip_suffix(FROZEN_SUFFIX) {
            out.insert(base.to_string());
        } else if let Some(base) = name.strip_suffix(MIRROR_QUARANTINED_SUFFIX) {
            // A settled demotion is a bare fence — no body, no sidecar. Without
            // this it is not a diff candidate at all and the fence, which is
            // truth and must replicate, can never reach a peer.
            out.insert(base.to_string());
        } else if crate::sidecar::is_artifact(name) {
            out.insert(name.clone());
        }
    }
    out
}

async fn package_member_names(storage: &dyn Storage, pkg: &str) -> Result<HashSet<String>> {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    Ok(storage
        .list_dir_entries(&prefix)
        .await?
        .into_iter()
        .filter_map(|entry| entry.key.strip_prefix(&prefix).map(str::to_string))
        .collect())
}

/// Diff two buckets' private truth and converge them through the merge rules.
/// Both `packages/` trees are walked page by page and merged by package name, so
/// neither full listing is ever resident (v1 review finding); every package on
/// either side is converged exactly once, in sorted order.
async fn diff_pair(state: &AppState, a: &dyn Storage, b: &dyn Storage) -> Result<()> {
    let mut scan_a = PackageScan::new(a);
    let mut scan_b = PackageScan::new(b);
    let mut failures = Vec::new();
    loop {
        let next_a = scan_a.peek().await?.map(str::to_string);
        let next_b = scan_b.peek().await?.map(str::to_string);
        let pkg = match (next_a, next_b) {
            (None, None) => break,
            (Some(a_pkg), None) => {
                scan_a.advance();
                a_pkg
            }
            (None, Some(b_pkg)) => {
                scan_b.advance();
                b_pkg
            }
            (Some(a_pkg), Some(b_pkg)) => match a_pkg.cmp(&b_pkg) {
                std::cmp::Ordering::Less => {
                    scan_a.advance();
                    a_pkg
                }
                std::cmp::Ordering::Greater => {
                    scan_b.advance();
                    b_pkg
                }
                std::cmp::Ordering::Equal => {
                    scan_a.advance();
                    scan_b.advance();
                    a_pkg
                }
            },
        };
        if let Err(error) = converge_package(state, a, b, &pkg).await {
            error!(package=%pkg, error=?error, "reconcile: package diff failed; continuing");
            failures.push(format!("{pkg}: {error:#}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("package diffs failed: {}", failures.join("; "))
    }
}

/// Converge one package across the two buckets through the merge rules. Called
/// once per package by [`diff_pair`]; re-lists fresh member names because the
/// paged scan that discovered the package may predate a writer.
async fn converge_package(
    state: &AppState,
    a: &dyn Storage,
    b: &dyn Storage,
    pkg: &str,
) -> Result<()> {
    require_replication_unfenced(state)?;
    // The flat scan that discovered the package may predate a writer;
    // use fresh member listings for the per-package convergence pass.
    let mut a_names = package_member_names(a, pkg).await?;
    let mut b_names = package_member_names(b, pkg).await?;

    let a_origin = read_pkg_origin(a, pkg).await?;
    let b_origin = read_pkg_origin(b, pkg).await?;
    let SplitOriginReconciled {
        a_origin,
        b_origin,
        scanned_a,
        scanned_b,
    } = reconcile_split_origin(a, b, pkg, a_origin, b_origin).await?;
    if scanned_a {
        a_names = package_member_names(a, pkg).await?;
    }
    if scanned_b {
        b_names = package_member_names(b, pkg).await?;
    }

    if a_origin == Some(Origin::Private) || b_origin == Some(Origin::Private) {
        reconcile_project_status(a, b, pkg).await?;
    }

    let mut converged = false;
    for _ in 0..3 {
        let mut filenames: Vec<String> = candidate_filenames(&a_names)
            .union(&candidate_filenames(&b_names))
            .cloned()
            .collect();
        // Deterministic verdict order (the set iterates in RandomState order);
        // each verdict is independent, but seeded-simulator replays and log
        // forensics both want one canonical order.
        filenames.sort();
        let mut retry_after_late_mirror = false;
        for filename in filenames {
            let ra = record_from_names(a, pkg, &filename, &a_names, a_origin).await?;
            let rb = record_from_names(b, pkg, &filename, &b_names, b_origin).await?;
            // A proxy/mirror writer can cross the final claim read and
            // finish after package demotion. Package-private +
            // artifact-mirror is therefore an invalid late record,
            // including when the filename is absent from the true
            // private source. Quarantine it here: a mirror body under a
            // private claim is a demotion loser, never truth to replicate,
            // whatever `decide` would do for two mirror-claimed peers.
            let late_a = a_origin == Some(Origin::Private)
                && matches!(
                    ra.state(),
                    RecordState::Live {
                        origin: Origin::Mirror,
                        ..
                    }
                );
            let late_b = b_origin == Some(Origin::Private)
                && matches!(
                    rb.state(),
                    RecordState::Live {
                        origin: Origin::Mirror,
                        ..
                    }
                );
            if late_a || late_b {
                if late_a {
                    if quarantine_mirror_artifacts(a, pkg).await? > 0 {
                        markers::mark_dirty(a, pkg).await?;
                    }
                    a_names = package_member_names(a, pkg).await?;
                }
                if late_b {
                    if quarantine_mirror_artifacts(b, pkg).await? > 0 {
                        markers::mark_dirty(b, pkg).await?;
                    }
                    b_names = package_member_names(b, pkg).await?;
                }
                retry_after_late_mirror = true;
                break;
            }
            let verdict = decide(&ra, &rb);
            // Tier 3 holds no ack open and has no note to consume, so a
            // deferral needs no repair record of its own: the next cadence
            // re-diffs, by which time the orphan side's own audit has
            // backfilled the sidecar the decision was waiting on.
            let _: Convergence = execute(
                state,
                (a, b),
                pkg,
                &filename,
                (&ra, &rb),
                verdict,
                ArtifactSource::Bucket,
            )
            .await?;
        }
        if retry_after_late_mirror {
            continue;
        }
        converged = true;
        break;
    }
    if !converged {
        bail!("package '{pkg}' kept exposing mirror records under a private claim");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::decide::tests::{live, sc};
    use super::*;
    use crate::buckets::{BucketHandle, BucketSet};
    use crate::sidecar::Yanked;
    use crate::storage::test_support::InMemStorage;
    use crate::storage::{FileEntry, ObjectMeta};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// Another writer landing `bytes` at `key` exactly once, at the moment the
    /// code under test is most exposed: by default when it tries to CREATE the
    /// key (so its create loses), or — with [`racing_a_read`] — the instant it
    /// has READ the key, so whatever it decided from those bytes is already
    /// stale by the time it acts, or — with [`racing_a_delete`] — inside the
    /// DELETE itself, the gap no re-read can close because `delete_keys` is
    /// unconditional.
    struct RacingWriterStorage {
        inner: InMemStorage,
        key: String,
        bytes: Vec<u8>,
        required_prior_key: Option<String>,
        on_read: bool,
        on_delete: bool,
        raced: AtomicBool,
    }

    impl RacingWriterStorage {
        fn new(key: String, bytes: Vec<u8>) -> Self {
            Self {
                inner: InMemStorage::default(),
                key,
                bytes,
                required_prior_key: None,
                on_read: false,
                on_delete: false,
                raced: AtomicBool::new(false),
            }
        }

        fn requiring_prior_key(mut self, key: String) -> Self {
            self.required_prior_key = Some(key);
            self
        }

        fn racing_a_read(mut self) -> Self {
            self.on_read = true;
            self
        }

        fn racing_a_delete(mut self) -> Self {
            self.on_delete = true;
            self
        }
    }

    #[async_trait::async_trait]
    impl Storage for RacingWriterStorage {
        async fn head_exists(&self, key: &str) -> Result<bool> {
            self.inner.head_exists(key).await
        }

        async fn serve_artifact(
            &self,
            key: &str,
            range: Option<&str>,
        ) -> Result<axum::response::Response<axum::body::Body>> {
            self.inner.serve_artifact(key, range).await
        }

        async fn presign_get(
            &self,
            key: &str,
            expires: std::time::Duration,
        ) -> Result<Option<String>> {
            self.inner.presign_get(key, expires).await
        }

        async fn put_bytes(
            &self,
            key: &str,
            bytes: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<()> {
            self.inner.put_bytes(key, bytes, content_type).await
        }

        async fn put_if_absent(
            &self,
            key: &str,
            bytes: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<bool> {
            if !self.on_read
                && !self.on_delete
                && key == self.key
                && !self.raced.swap(true, Ordering::SeqCst)
            {
                if let Some(required) = &self.required_prior_key {
                    if !self.inner.head_exists(required).await? {
                        bail!("{required} must exist before creating {key}");
                    }
                }
                self.inner.insert(key, self.bytes.clone());
                return Ok(false);
            }
            self.inner.put_if_absent(key, bytes, content_type).await
        }

        async fn put_file_if_absent(
            &self,
            key: &str,
            path: &std::path::Path,
            content_type: Option<&str>,
        ) -> Result<bool> {
            self.inner.put_file_if_absent(key, path, content_type).await
        }

        async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
            let read = self.inner.get_bytes(key).await?;
            if self.on_read && key == self.key && !self.raced.swap(true, Ordering::SeqCst) {
                self.inner.insert(key, self.bytes.clone());
            }
            Ok(read)
        }

        async fn list_dir_entries(&self, prefix: &str) -> Result<Vec<FileEntry>> {
            self.inner.list_dir_entries(prefix).await
        }

        async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
            self.inner.list_all(prefix).await
        }

        async fn delete_keys(&self, keys: &[String]) -> Result<()> {
            if self.on_delete && !self.raced.swap(true, Ordering::SeqCst) {
                self.inner.insert(&self.key, self.bytes.clone());
            }
            self.inner.delete_keys(keys).await
        }

        fn supports_leases(&self) -> bool {
            true
        }

        async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.inner.get_with_etag(key).await
        }

        async fn head_etag(&self, key: &str) -> Result<Option<String>> {
            self.inner.head_etag(key).await
        }

        async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
            self.inner.put_if_none_match(key, bytes).await
        }

        async fn put_if_match(
            &self,
            key: &str,
            etag: &str,
            bytes: Vec<u8>,
        ) -> Result<Option<String>> {
            self.inner.put_if_match(key, etag, bytes).await
        }
    }

    fn test_state(storage: Arc<dyn Storage>) -> AppState {
        AppState::headless(storage)
    }

    fn two_bucket_state(a: Arc<dyn Storage>, b: Arc<dyn Storage>) -> AppState {
        let mut state = AppState::headless(a.clone());
        state.buckets = Arc::new(BucketSet::new(vec![
            BucketHandle {
                storage: a,
                name: "a".to_string(),
            },
            BucketHandle {
                storage: b,
                name: "b".to_string(),
            },
        ]));
        state
    }

    fn three_bucket_state(
        a: Arc<dyn Storage>,
        b: Arc<dyn Storage>,
        c: Arc<dyn Storage>,
    ) -> AppState {
        let mut state = AppState::headless(a.clone());
        state.buckets = Arc::new(BucketSet::new(vec![
            BucketHandle {
                storage: a,
                name: "a".to_string(),
            },
            BucketHandle {
                storage: b,
                name: "b".to_string(),
            },
            BucketHandle {
                storage: c,
                name: "c".to_string(),
            },
        ]));
        state
    }

    fn seed_live(storage: &InMemStorage, pkg: &str, filename: &str, bytes: &[u8], origin: &str) {
        let key = artifact_key(pkg, filename);
        storage.insert(&key, bytes.to_vec());
        storage.insert(
            &sidecar_key(&key),
            serde_json::to_vec(&sc(&sha256_hex(bytes), origin, Yanked::Flag(false), 0)).unwrap(),
        );
        // Legacy claim bodies remain valid input during the nonce migration.
        storage.insert(&crate::origin::origin_key(pkg), origin.as_bytes().to_vec());
    }

    fn seed_live_at(
        storage: &InMemStorage,
        pkg: &str,
        filename: &str,
        bytes: &[u8],
        upload_epoch_ms: u64,
    ) {
        let key = artifact_key(pkg, filename);
        storage.insert(&key, bytes.to_vec());
        let mut sidecar = sc(&sha256_hex(bytes), PRIVATE, Yanked::Flag(false), 0);
        sidecar.upload_epoch_ms = Some(upload_epoch_ms);
        storage.insert(&sidecar_key(&key), serde_json::to_vec(&sidecar).unwrap());
        storage.insert(&crate::origin::origin_key(pkg), b"private".to_vec());
    }

    /// A settled demotion converges the fence to a peer that never held the
    /// loser, and nothing but the fence crosses the wire: the suppressed body
    /// stays on the bucket that resolved it, and the canonical key ends empty
    /// on both.
    #[tokio::test]
    async fn a_settled_demotion_replicates_its_fence_and_not_its_body() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(a.as_ref(), "pkg", filename, b"mirror bytes", MIRROR);
        a.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        assert_eq!(
            quarantine_mirror_artifacts(a.as_ref(), "pkg")
                .await
                .unwrap(),
            1
        );
        let state = two_bucket_state(a.clone(), b.clone());

        diff_pair(&state, a.as_ref(), b.as_ref()).await.unwrap();

        let key = artifact_key("pkg", filename);
        for bucket in [a.as_ref(), b.as_ref()] {
            assert!(bucket
                .head_exists(&mirror_quarantined_key(&key))
                .await
                .unwrap());
            assert!(!bucket.head_exists(&key).await.unwrap());
            assert!(!bucket.head_exists(&sidecar_key(&key)).await.unwrap());
        }
        // Evidence is bucket-local: only the side that resolved it holds bytes.
        assert_eq!(a.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 1);
        assert!(b.list_all(QUARANTINE_PREFIX).await.unwrap().is_empty());
        // Settled: a second pass is a no-op, not a re-fire.
        let ra = read_record(a.as_ref(), "pkg", filename).await.unwrap();
        let rb = read_record(b.as_ref(), "pkg", filename).await.unwrap();
        assert_eq!(decide(&ra, &rb), Verdict::Noop);
    }

    /// A demotion settle drops only the body it personally preserved.
    ///
    /// `freeze_side` runs the identical marker-preserve-drop order and is safe
    /// dropping blind, because `.frozen` and `.tombstone` are UPLOAD fences:
    /// `publish_record` refuses over them, so the body a freeze copied aside is
    /// still the body it deletes. `.mirror-quarantined` deliberately is not —
    /// handing the filename to private truth IS the demotion's intended
    /// resolution — so the canonical key stays writable for the whole settle.
    /// The double lands private bytes the instant the settle has read the
    /// mirror body it is copying aside, which is what a racing publish does; a
    /// blind `delete_keys` then destroys bytes no `_quarantine/` copy holds.
    #[tokio::test]
    async fn a_demotion_settle_drops_only_the_body_it_preserved() {
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let storage = Arc::new(
            RacingWriterStorage::new(key.clone(), b"private truth".to_vec()).racing_a_read(),
        );
        seed_live(&storage.inner, "pkg", filename, b"mirror bytes", MIRROR);
        storage
            .inner
            .insert(&crate::origin::origin_key("pkg"), b"private".to_vec());

        settle_mirror_quarantine(storage.as_ref(), "pkg", filename)
            .await
            .unwrap();

        assert_eq!(
            storage.inner.get_bytes(&key).await.unwrap(),
            b"private truth",
            "the settle destroyed a body it never copied to _quarantine/"
        );
        // The body it did read is preserved, and the fence stands: the settle
        // is abandoned, not half-applied.
        assert_eq!(
            storage
                .inner
                .list_all(QUARANTINE_PREFIX)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(storage
            .inner
            .head_exists(&mirror_quarantined_key(&key))
            .await
            .unwrap());
    }

    /// The settle preserved NOTHING — the canonical key read empty, which is
    /// the torn mirror record it is the right cleanup for — and a publish then
    /// created that key while the delete was landing. Nothing preserved is not
    /// the same as nothing to destroy, and no re-read can tell the difference:
    /// `delete_keys` is unconditional, so the only thing that authorizes
    /// removing a body is holding a `_quarantine/` copy of it. Traced on vopr
    /// seed 86001009016 (`--nodes 3 --buckets 2 --packages 1 --files 1 --ops 26
    /// --fail-percent 1 --partition 100`), where a `twine upload` got its 200
    /// and the bytes then existed nowhere — no tombstone, no `.frozen`, no
    /// `_quarantine/` copy — so no sweep, reconcile, audit or `verify-chain`
    /// ever looked for them again.
    #[tokio::test]
    async fn a_demotion_settle_never_drops_a_body_it_never_preserved() {
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let storage = Arc::new(
            RacingWriterStorage::new(key.clone(), b"acked private upload".to_vec())
                .racing_a_delete(),
        );
        // A torn mirror record: sidecar, no body. The settle preserves nothing.
        storage.inner.insert(
            &sidecar_key(&key),
            serde_json::to_vec(&sc(
                &sha256_hex(b"mirror bytes"),
                MIRROR,
                Yanked::Flag(false),
                0,
            ))
            .unwrap(),
        );
        storage
            .inner
            .insert(&crate::origin::origin_key("pkg"), b"private".to_vec());

        settle_mirror_quarantine(storage.as_ref(), "pkg", filename)
            .await
            .unwrap();

        assert_eq!(
            storage.inner.get_bytes(&key).await.unwrap(),
            b"acked private upload",
            "the settle destroyed acked bytes it held no _quarantine/ copy of"
        );
        assert!(storage
            .inner
            .list_all(QUARANTINE_PREFIX)
            .await
            .unwrap()
            .is_empty());
        // The torn sidecar it *was* the right cleanup for still goes, or the
        // verdict re-fires forever; the fence stays, as it always does.
        assert!(!storage.inner.head_exists(&sidecar_key(&key)).await.unwrap());
        assert!(storage
            .inner
            .head_exists(&mirror_quarantined_key(&key))
            .await
            .unwrap());
    }

    /// Private truth taking a demoted filename is the demotion's intended
    /// resolution, so the fence beside it is spent. Only `supersede_record`
    /// used to clear it, and only on the side it wrote — a bucket that took the
    /// private upload directly over its own fenced key kept the marker forever,
    /// and `decide` read both sides as live private and called them converged.
    #[tokio::test]
    async fn a_spent_demotion_fence_beside_private_truth_is_dropped() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(a.as_ref(), "pkg", filename, b"private bytes", PRIVATE);
        seed_live(b.as_ref(), "pkg", filename, b"private bytes", PRIVATE);
        let key = artifact_key("pkg", filename);
        // The private publish landed over a key this bucket had fenced; nothing
        // on the publish path clears the fence.
        a.insert(
            &mirror_quarantined_key(&key),
            br#"{"filename":"pkg-1.whl"}"#.to_vec(),
        );
        let state = two_bucket_state(a.clone(), b.clone());

        diff_pair(&state, a.as_ref(), b.as_ref()).await.unwrap();

        assert!(!a.head_exists(&mirror_quarantined_key(&key)).await.unwrap());
        assert!(!b.head_exists(&mirror_quarantined_key(&key)).await.unwrap());
        // The record itself is untouched — the fence was inert, not a fence.
        assert_eq!(a.get_bytes(&key).await.unwrap(), b"private bytes");
        assert_eq!(b.get_bytes(&key).await.unwrap(), b"private bytes");
        assert!(a.list_all(QUARANTINE_PREFIX).await.unwrap().is_empty());
    }

    /// The spent-fence clear reads listing-era records, and a settle racing it
    /// on the same bucket empties the canonical key under them. Clearing on the
    /// stale reading leaves acked bytes in `_quarantine/` with NOTHING marking
    /// their removal as authorized — no tombstone, no `.frozen`, and no fence on
    /// any bucket the settle's fan-out had not reached yet. Traced at three
    /// buckets on vopr seed 65000024708, where both pair merges cleared bucket
    /// 0's fence after that bucket's own settle had already dropped the body,
    /// and the fan-out's fence write to bucket 2 then crashed: end state, an
    /// acknowledged upload missing on all three buckets.
    #[tokio::test]
    async fn a_stale_spent_fence_clear_never_unauthorizes_a_settled_demotion() {
        let storage = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        seed_live(storage.as_ref(), "pkg", filename, b"private bytes", PRIVATE);
        storage.insert(
            &mirror_quarantined_key(&key),
            br#"{"filename":"pkg-1.whl"}"#.to_vec(),
        );

        // What the merge carries: private truth under a fence it may call spent.
        let stale = read_record(storage.as_ref(), "pkg", filename)
            .await
            .unwrap();
        assert!(stale.mirror_quarantined);

        // ...and what a settle that started before that upload landed does to
        // the same bucket in the meantime: preserve the body, drop the record,
        // leave the fence standing as the authorization.
        assert!(quarantine(storage.as_ref(), "pkg", filename)
            .await
            .unwrap()
            .is_some());
        storage
            .delete_keys(&record_object_keys(&key))
            .await
            .unwrap();

        clear_spent_demotion_fence(storage.as_ref(), "pkg", filename, &stale)
            .await
            .unwrap();

        assert!(
            storage
                .head_exists(&mirror_quarantined_key(&key))
                .await
                .unwrap(),
            "a stale clear erased the only record authorizing an empty canonical key"
        );
    }

    /// A tombstone is the stronger, permanent fence, so it subsumes a demotion
    /// fence it lands over — and the pair is not settled until it is gone.
    #[tokio::test]
    async fn a_tombstone_removes_a_demotion_fence_it_lands_over() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        for bucket in [a.as_ref(), b.as_ref()] {
            bucket.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
            bucket.insert(
                &mirror_quarantined_key(&key),
                br#"{"filename":"pkg-1.whl"}"#.to_vec(),
            );
        }
        tombstone::write(a.as_ref(), &key, filename).await.unwrap();
        let state = two_bucket_state(a.clone(), b.clone());

        diff_pair(&state, a.as_ref(), b.as_ref()).await.unwrap();

        for bucket in [a.as_ref(), b.as_ref()] {
            assert!(bucket.head_exists(&tombstone_key(&key)).await.unwrap());
            assert!(!bucket
                .head_exists(&mirror_quarantined_key(&key))
                .await
                .unwrap());
        }
        let ra = read_record(a.as_ref(), "pkg", filename).await.unwrap();
        let rb = read_record(b.as_ref(), "pkg", filename).await.unwrap();
        assert_eq!(decide(&ra, &rb), Verdict::Noop);
    }

    #[tokio::test]
    async fn private_conflict_quarantines_loser_and_is_stable_on_second_pass() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live_at(a.as_ref(), "pkg", filename, b"first", 1_000);
        seed_live_at(b.as_ref(), "pkg", filename, b"second", 4_000);
        let state = two_bucket_state(a.clone(), b.clone());

        let ra = read_record(a.as_ref(), "pkg", filename).await.unwrap();
        let rb = read_record(b.as_ref(), "pkg", filename).await.unwrap();
        assert_eq!(decide(&ra, &rb), Verdict::QuarantineLoser(Side::A));
        diff_pair(&state, a.as_ref(), b.as_ref()).await.unwrap();

        assert_eq!(
            b.get_bytes(&artifact_key("pkg", filename)).await.unwrap(),
            b"first"
        );
        let loser_sha = sha256_hex(b"second");
        let loser_key = format!("{QUARANTINE_PREFIX}pkg/{filename}@{}", &loser_sha[..12]);
        assert_eq!(b.get_bytes(&loser_key).await.unwrap(), b"second");
        assert_eq!(
            state
                .metrics
                .replication_conflict_quarantines
                .load(Ordering::Relaxed),
            1
        );

        let settled_a = read_record(a.as_ref(), "pkg", filename).await.unwrap();
        let settled_b = read_record(b.as_ref(), "pkg", filename).await.unwrap();
        assert_eq!(decide(&settled_a, &settled_b), Verdict::Noop);
        let before_a = a.list_all("").await.unwrap();
        let before_b = b.list_all("").await.unwrap();
        diff_pair(&state, a.as_ref(), b.as_ref()).await.unwrap();
        assert_eq!(a.list_all("").await.unwrap(), before_a);
        assert_eq!(b.list_all("").await.unwrap(), before_b);
    }

    #[tokio::test]
    async fn private_origin_beats_a_higher_epoch_mirror_during_cas() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        seed_live(a.as_ref(), "pkg", filename, b"same", PRIVATE);
        seed_live(b.as_ref(), "pkg", filename, b"same", MIRROR);
        b.insert(
            &sidecar_key(&key),
            serde_json::to_vec(&sc(
                &sha256_hex(b"same"),
                MIRROR,
                Yanked::Reason("mirror says withdrawn".into()),
                99,
            ))
            .unwrap(),
        );
        let state = test_state(a.clone());
        let left = read_record(a.as_ref(), "pkg", filename).await.unwrap();
        let right = read_record(b.as_ref(), "pkg", filename).await.unwrap();
        let verdict = decide(&left, &right);
        assert_eq!(verdict, Verdict::AdoptSidecar(Side::A));

        let _: Convergence = execute(
            &state,
            (a.as_ref(), b.as_ref()),
            "pkg",
            filename,
            (&left, &right),
            verdict,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        let adopted: Sidecar =
            serde_json::from_slice(&b.get_bytes(&sidecar_key(&key)).await.unwrap()).unwrap();
        assert_eq!(adopted.origin.as_deref(), Some(PRIVATE));
        assert_eq!(adopted.yank_epoch, 0);
        assert_eq!(adopted.yanked, Yanked::Flag(false));
    }

    #[tokio::test]
    async fn private_copy_overwrites_a_bodyless_mirror_sidecar() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        seed_live(a.as_ref(), "pkg", filename, b"private", PRIVATE);
        b.insert(&crate::origin::origin_key("pkg"), b"mirror".to_vec());
        b.insert(
            &sidecar_key(&key),
            serde_json::to_vec(&sc(
                &sha256_hex(b"orphaned mirror"),
                MIRROR,
                Yanked::Flag(false),
                0,
            ))
            .unwrap(),
        );
        let state = two_bucket_state(a.clone(), b.clone());

        let _: Convergence = replicate_record(
            &state,
            a.as_ref(),
            b.as_ref(),
            "pkg",
            filename,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        assert_eq!(b.get_bytes(&key).await.unwrap(), b"private");
        let copied: Sidecar =
            serde_json::from_slice(&b.get_bytes(&sidecar_key(&key)).await.unwrap()).unwrap();
        assert_eq!(copied.origin.as_deref(), Some(PRIVATE));
        assert_eq!(copied.sha256, sha256_hex(b"private"));
    }

    #[tokio::test]
    async fn malformed_claim_and_sidecar_fail_the_record_read() {
        let failed_claim_read = InMemStorage::default();
        failed_claim_read.fail_next_get();
        assert!(read_record(&failed_claim_read, "pkg", "pkg-1.whl")
            .await
            .unwrap_err()
            .to_string()
            .contains("injected storage failure"));

        let bad_claim = InMemStorage::default();
        bad_claim.insert(&crate::origin::origin_key("pkg"), b"bogus".to_vec());
        assert!(read_record(&bad_claim, "pkg", "pkg-1.whl")
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid origin claim state"));

        let bad_sidecar = InMemStorage::default();
        let key = artifact_key("pkg", "pkg-1.whl");
        bad_sidecar.insert(&key, b"bytes".to_vec());
        bad_sidecar.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        bad_sidecar.insert(&sidecar_key(&key), b"not-json".to_vec());
        assert!(read_record(&bad_sidecar, "pkg", "pkg-1.whl")
            .await
            .unwrap_err()
            .to_string()
            .contains("parse sidecar"));
    }

    #[tokio::test]
    async fn corrupt_early_package_does_not_starve_later_full_diff_work() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        a.insert(&crate::origin::origin_key("aaa-corrupt"), b"bogus".to_vec());
        seed_live(
            b.as_ref(),
            "zzz-healthy",
            "zzz_healthy-1.whl",
            b"healthy",
            PRIVATE,
        );
        let state = two_bucket_state(a.clone(), b.clone());

        let error = diff_pair(&state, a.as_ref(), b.as_ref()).await.unwrap_err();
        assert!(error.to_string().contains("aaa-corrupt"));
        assert!(a
            .head_exists(&artifact_key("zzz-healthy", "zzz_healthy-1.whl"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn source_sha_is_verified_before_destination_sidecar_publish() {
        let src = InMemStorage::default();
        let dst = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        src.insert(&artifact_key("pkg", filename), b"wrong bytes".to_vec());
        let record = live(&sha256_hex(b"expected bytes"), PRIVATE);
        let state = test_state(dst.clone());

        let err = copy_live(
            &state,
            &src,
            dst.as_ref(),
            "pkg",
            filename,
            &record,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("source artifact sha mismatch"));
        // The bytes are hashed as they stream and checked before the first
        // destination mutation, so an unverifiable source leaves the
        // destination untouched — there is no half-landed body to delete.
        assert!(
            !dst.head_exists(&artifact_key("pkg", filename))
                .await
                .unwrap(),
            "a source that does not hash to its own sidecar must never put bytes on a peer",
        );
        assert!(!dst
            .head_exists(&sidecar_key(&artifact_key("pkg", filename)))
            .await
            .unwrap());
        assert!(!dst
            .head_exists(&crate::origin::origin_key("pkg"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn torn_source_record_drops_stale_sidecar_for_rebuild() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        // A torn record: a failed publish freed the immutable name after a
        // sidecar was fabricated for its bytes, and a later upload re-created
        // the body — live bytes now disagree with the surviving sidecar.
        seed_live(src.as_ref(), "pkg", filename, b"dead bytes", PRIVATE);
        let akey = artifact_key("pkg", filename);
        src.insert(&akey, b"live bytes".to_vec());
        let state = test_state(src.clone());

        let err = replicate_record(
            &state,
            src.as_ref(),
            dst.as_ref(),
            "pkg",
            filename,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("source artifact sha mismatch"));
        // The stale sidecar is dropped and the package marked dirty, so the
        // tick path refabricates the sidecar from the live body and the next
        // reconcile pass copies the healed record.
        assert!(!src.head_exists(&sidecar_key(&akey)).await.unwrap());
        assert!(!src.list_page("_dirty/", None, 1).await.unwrap().is_empty());
        // The live body itself is untouched.
        assert_eq!(src.get_bytes(&akey).await.unwrap(), b"live bytes");
    }

    #[tokio::test]
    async fn same_sha_private_records_repair_missing_companions() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let bytes = b"same wheel bytes";
        seed_live(src.as_ref(), "pkg", filename, bytes, PRIVATE);
        seed_live(dst.as_ref(), "pkg", filename, bytes, PRIVATE);
        let metadata = metadata_key(&artifact_key("pkg", filename));
        src.insert(&metadata, b"Metadata-Version: 2.4\n".to_vec());
        let state = test_state(src.clone());

        let _: Convergence = replicate_record(
            &state,
            src.as_ref(),
            dst.as_ref(),
            "pkg",
            filename,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();
        assert_eq!(
            dst.get_bytes(&metadata).await.unwrap(),
            b"Metadata-Version: 2.4\n"
        );

        let mut source_record = live(&sha256_hex(bytes), PRIVATE);
        source_record.has_provenance = true;
        let destination_record = live(&sha256_hex(bytes), PRIVATE);
        let err = repair_same_sha_companions(
            (src.as_ref(), dst.as_ref()),
            "pkg",
            filename,
            (&source_record, &destination_record),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("read source companion"));
    }

    #[tokio::test]
    async fn artifact_create_race_with_different_bytes_freezes_both_sides() {
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let source_bytes = b"source wheel";
        let raced_bytes = b"different destination wheel";
        let src = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", filename, source_bytes, PRIVATE);
        let dst = Arc::new(RacingWriterStorage::new(key.clone(), raced_bytes.to_vec()));
        let state = test_state(src.clone());
        let record = live(&sha256_hex(source_bytes), PRIVATE);

        assert!(!copy_live(
            &state,
            src.as_ref(),
            dst.as_ref(),
            "pkg",
            filename,
            &record,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap());
        assert!(!src.head_exists(&key).await.unwrap());
        assert!(!dst.head_exists(&key).await.unwrap());
        assert!(src.head_exists(&frozen_key(&key)).await.unwrap());
        assert!(dst.head_exists(&frozen_key(&key)).await.unwrap());
        assert!(src.head_exists(&tombstone_key(&key)).await.unwrap());
        assert!(dst.head_exists(&tombstone_key(&key)).await.unwrap());

        let src_quarantine = src.list_all(QUARANTINE_PREFIX).await.unwrap();
        let dst_quarantine = dst.list_all(QUARANTINE_PREFIX).await.unwrap();
        assert_eq!(src_quarantine.len(), 1);
        assert_eq!(dst_quarantine.len(), 1);
        assert!(src_quarantine[0]
            .key
            .ends_with(&sha256_hex(source_bytes)[..12]));
        assert!(dst_quarantine[0]
            .key
            .ends_with(&sha256_hex(raced_bytes)[..12]));
    }

    #[tokio::test]
    async fn artifact_create_race_with_same_bytes_verifies_the_pair() {
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let bytes = b"identical wheel";
        let src = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", filename, bytes, PRIVATE);
        let dst = Arc::new(RacingWriterStorage::new(key.clone(), bytes.to_vec()));
        let state = test_state(src.clone());
        let record = live(&sha256_hex(bytes), PRIVATE);

        assert!(copy_live(
            &state,
            src.as_ref(),
            dst.as_ref(),
            "pkg",
            filename,
            &record,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap());
        assert_eq!(dst.get_bytes(&key).await.unwrap(), bytes);
        let sidecar: Sidecar =
            serde_json::from_slice(&dst.get_bytes(&sidecar_key(&key)).await.unwrap()).unwrap();
        assert_eq!(sidecar.sha256, sha256_hex(bytes));
        assert!(!dst.head_exists(&frozen_key(&key)).await.unwrap());
    }

    /// The artifact leg publishes bytes first so the sidecar after it can only
    /// describe bytes this bucket holds — but that claim expires. A demotion
    /// settle is entitled to empty a live canonical key, and its fence
    /// deliberately does not bar the writers racing it. Landing between the two
    /// legs, it leaves the sidecar standing over nothing, the next create takes
    /// the free key with different bytes, and the bucket publishes sha A over
    /// body B forever: `decide` compares sidecar shas, so both buckets read as
    /// agreed and nothing re-hashes a stored body. `pip` then fails its hash
    /// check against the index's own sha256 — from one region only. Traced on
    /// vopr seed 62000150551 (`--nodes 3 --buckets 2 --packages 6 --files 2
    /// --ops 80 --fail-percent 3 --partition 100`).
    #[tokio::test]
    async fn a_copy_never_publishes_truth_over_a_body_the_bucket_lost() {
        let dst = Arc::new(InMemStorage::default());
        let bytes = b"private wheel";
        let key = artifact_key("pkg", "pkg-1.whl");
        let sidecar = sc(&sha256_hex(bytes), PRIVATE, Yanked::Flag(false), 0);
        let companions = Companions {
            metadata: None,
            provenance: None,
        };

        // The artifact leg landed these bytes; a settle on this bucket dropped
        // the record before the sidecar leg reached it.
        let err = copy_truth(
            dst.as_ref(),
            &key,
            &sidecar,
            companions,
            false,
            false,
            bytes.len() as u64,
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("removed before its sidecar"),
            "unexpected error: {err}"
        );
        assert!(
            !dst.head_exists(&sidecar_key(&key)).await.unwrap(),
            "the copy published a sha256 for bytes this bucket does not hold"
        );
    }

    #[tokio::test]
    async fn destination_body_its_own_sidecar_describes_is_a_conflict_not_debris() {
        // The state a local publish leaves behind after it wins the immutable
        // create in the window the replication gate opened: the destination's
        // own sidecar now names the local bytes, not ours. Overwriting that as
        // "stale crash debris" destroys acked bytes and leaves a sidecar over a
        // body it does not describe. It is a byte conflict: freeze, preserve.
        // A gated leg (`sidecar_published`) has published truth of its own here,
        // so it must settle the record rather than hand it back.
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let source_bytes = b"source wheel";
        let local_bytes = b"locally published wheel";
        let src = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", filename, source_bytes, PRIVATE);
        let dst = Arc::new(InMemStorage::default());
        seed_live(dst.as_ref(), "pkg", filename, local_bytes, PRIVATE);
        let state = test_state(src.clone());
        let sidecar = sc(&sha256_hex(source_bytes), PRIVATE, Yanked::Flag(false), 0);

        assert_eq!(
            artifact_leg(
                &state,
                src.as_ref(),
                dst.as_ref(),
                "pkg",
                filename,
                &sidecar,
                &key,
                StagedArtifact::Resident(source_bytes.to_vec()),
                true,
            )
            .await
            .unwrap(),
            ArtifactLeg::Frozen
        );

        let quarantined = dst.list_all(QUARANTINE_PREFIX).await.unwrap();
        assert_eq!(quarantined.len(), 1);
        assert_eq!(
            dst.get_bytes(&quarantined[0].key).await.unwrap(),
            local_bytes
        );
        assert!(dst.head_exists(&frozen_key(&key)).await.unwrap());
        assert!(src.head_exists(&frozen_key(&key)).await.unwrap());
    }

    #[tokio::test]
    async fn an_ungated_leg_hands_a_live_competing_record_back_to_the_merge() {
        // Same destination state, reached by a leg that has published nothing
        // of its own — the private stream path, which takes the artifact key
        // first. Ordering two live private bodies is `decide`'s job: it has both
        // `upload-epoch-ms` values and resolves first-uploaded-wins, degrading
        // to a freeze only inside the skew. So refuse, exactly as the sidecar
        // gate does, and let the caller's repair note bring the merge to it.
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let source_bytes = b"source wheel";
        let local_bytes = b"locally published wheel";
        let src = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", filename, source_bytes, PRIVATE);
        let dst = Arc::new(InMemStorage::default());
        seed_live(dst.as_ref(), "pkg", filename, local_bytes, PRIVATE);
        let state = test_state(src.clone());
        let sidecar = sc(&sha256_hex(source_bytes), PRIVATE, Yanked::Flag(false), 0);

        let err = artifact_leg(
            &state,
            src.as_ref(),
            dst.as_ref(),
            "pkg",
            filename,
            &sidecar,
            &key,
            StagedArtifact::Resident(source_bytes.to_vec()),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("different private bytes"),
            "unexpected error: {err}"
        );
        assert_eq!(dst.get_bytes(&key).await.unwrap(), local_bytes);
        assert!(!dst.head_exists(&frozen_key(&key)).await.unwrap());
        assert!(dst.list_all(QUARANTINE_PREFIX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn zero_byte_debris_under_our_own_sidecar_is_still_repaired() {
        // D2: a 200-acked-but-failed write leaves a body no sidecar describes.
        // The gate's own sidecar still names our sha, so this really is debris
        // — repair it, or heal bails on it forever.
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let source_bytes = b"source wheel";
        let src = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", filename, source_bytes, PRIVATE);
        let dst = Arc::new(InMemStorage::default());
        seed_live(dst.as_ref(), "pkg", filename, source_bytes, PRIVATE);
        dst.insert(&key, Vec::new());
        let state = test_state(src.clone());
        let sidecar = sc(&sha256_hex(source_bytes), PRIVATE, Yanked::Flag(false), 0);

        assert_eq!(
            artifact_leg(
                &state,
                src.as_ref(),
                dst.as_ref(),
                "pkg",
                filename,
                &sidecar,
                &key,
                StagedArtifact::Resident(source_bytes.to_vec()),
                true,
            )
            .await
            .unwrap(),
            ArtifactLeg::Landed { changed: true }
        );

        assert_eq!(dst.get_bytes(&key).await.unwrap(), source_bytes);
        assert!(!dst.head_exists(&frozen_key(&key)).await.unwrap());
    }

    #[tokio::test]
    async fn the_copy_takes_the_destination_key_before_publishing_truth_about_it() {
        // A destination sidecar is that bucket's assertion that the filename is
        // truth with those bytes. Published ahead of the artifact it stands over
        // a key any concurrent writer can still take: a publish on that bucket
        // during a partition wins the immutable create, and a leg that then dies
        // leaves sidecar(A) over body(B) — which `decide` cannot see, because it
        // compares sidecar shas and nothing ever re-hashes a stored body. The
        // create is the stronger gate, so it goes first: nothing can land a body
        // under a key we already own.
        let bytes = b"replicated wheel bytes";
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let src = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", filename, bytes, PRIVATE);
        let dst = Arc::new(InMemStorage::default());
        let state = test_state(src.clone());

        assert!(copy_live(
            &state,
            src.as_ref(),
            dst.as_ref(),
            "pkg",
            filename,
            &live(&sha256_hex(bytes), PRIVATE),
            ArtifactSource::Bucket,
        )
        .await
        .unwrap());

        let log = dst.write_log();
        let artifact_at = log
            .iter()
            .position(|written| written == &key)
            .expect("the copy landed the artifact");
        let sidecar_at = log
            .iter()
            .position(|written| written == &sidecar_key(&key))
            .expect("the copy installed the sidecar");
        assert!(
            artifact_at < sidecar_at,
            "the destination sidecar was published before the bytes it names: {log:?}"
        );
        assert_eq!(dst.get_bytes(&key).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn spool_source_copies_without_reading_the_source_bucket() {
        // The upload fan-out fast path: the artifact bytes come from the local
        // verified spool, so the source bucket is never asked for the body.
        // Prove it by leaving the source bucket's artifact absent — a bucket
        // read would fail; the spool read must succeed and land the bytes on
        // dst, sha-verified exactly as the bucket source is.
        let bytes = b"spooled wheel bytes";
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let state = test_state(src.clone());
        let record = live(&sha256_hex(bytes), PRIVATE);

        let spool =
            std::env::temp_dir().join(format!("pypiron-spool-ok-{}.bin", std::process::id()));
        tokio::fs::write(&spool, bytes).await.unwrap();

        assert!(copy_live(
            &state,
            src.as_ref(),
            dst.as_ref(),
            "pkg",
            filename,
            &record,
            ArtifactSource::Spool(&spool),
        )
        .await
        .unwrap());

        assert_eq!(dst.get_bytes(&key).await.unwrap(), bytes);
        let sidecar: Sidecar =
            serde_json::from_slice(&dst.get_bytes(&sidecar_key(&key)).await.unwrap()).unwrap();
        assert_eq!(sidecar.sha256, sha256_hex(bytes));
        // The source bucket body was never seeded: the copy read the spool.
        assert!(!src.head_exists(&key).await.unwrap());
        let _ = tokio::fs::remove_file(&spool).await;
    }

    #[tokio::test]
    async fn spool_source_that_disagrees_with_its_sidecar_fails_loudly() {
        // A spool whose bytes do not hash to the sidecar's sha is a caller bug,
        // not crash debris: bail before writing anything, never run the bucket
        // source's torn-record repair against a local file.
        let filename = "pkg-1.whl";
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let state = test_state(src.clone());
        let record = live(&sha256_hex(b"correct wheel"), PRIVATE);

        let spool =
            std::env::temp_dir().join(format!("pypiron-spool-bad-{}.bin", std::process::id()));
        tokio::fs::write(&spool, b"tampered wheel").await.unwrap();

        let err = copy_live(
            &state,
            src.as_ref(),
            dst.as_ref(),
            "pkg",
            filename,
            &record,
            ArtifactSource::Spool(&spool),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("upload spool"));
        assert!(!dst
            .head_exists(&artifact_key("pkg", filename))
            .await
            .unwrap());
        let _ = tokio::fs::remove_file(&spool).await;
    }

    #[tokio::test]
    async fn existing_freeze_markers_finish_live_body_cleanup() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        seed_live(a.as_ref(), "pkg", filename, b"a", PRIVATE);
        seed_live(b.as_ref(), "pkg", filename, b"b", PRIVATE);
        a.insert(&frozen_key(&key), b"{}".to_vec());
        b.insert(&frozen_key(&key), b"{}".to_vec());
        let state = test_state(a.clone());

        let _: Convergence = replicate_record(
            &state,
            a.as_ref(),
            b.as_ref(),
            "pkg",
            filename,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();
        assert!(!a.head_exists(&key).await.unwrap());
        assert!(!b.head_exists(&key).await.unwrap());
        assert_eq!(a.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 1);
        assert_eq!(b.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 1);

        let settled_a = read_record(a.as_ref(), "pkg", filename).await.unwrap();
        let settled_b = read_record(b.as_ref(), "pkg", filename).await.unwrap();
        assert_eq!(decide(&settled_a, &settled_b), Verdict::Noop);
        let before_a = a.list_all("").await.unwrap();
        let before_b = b.list_all("").await.unwrap();
        let _: Convergence = execute(
            &state,
            (a.as_ref(), b.as_ref()),
            "pkg",
            filename,
            (&settled_a, &settled_b),
            Verdict::Noop,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();
        assert_eq!(a.list_all("").await.unwrap(), before_a);
        assert_eq!(b.list_all("").await.unwrap(), before_b);
    }

    #[tokio::test]
    async fn freeze_writes_the_diagnostic_marker_before_the_tombstone() {
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let storage = RacingWriterStorage::new(tombstone_key(&key), b"raced fence".to_vec())
            .requiring_prior_key(frozen_key(&key));
        storage.inner.insert(&key, b"conflicting bytes".to_vec());

        freeze_side(&storage, "pkg", filename).await.unwrap();

        assert!(storage.head_exists(&tombstone_key(&key)).await.unwrap());
        assert!(storage.head_exists(&frozen_key(&key)).await.unwrap());
        assert!(!storage.head_exists(&key).await.unwrap());
    }

    #[tokio::test]
    async fn freeze_writes_the_diagnostic_marker_before_quarantining_bytes() {
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let bytes = b"conflicting bytes";
        let sha = sha256_hex(bytes);
        let qkey = format!("{QUARANTINE_PREFIX}pkg/{filename}@{}", &sha[..12]);
        let storage =
            RacingWriterStorage::new(qkey, bytes.to_vec()).requiring_prior_key(frozen_key(&key));
        storage.inner.insert(&key, bytes.to_vec());

        freeze_side(&storage, "pkg", filename).await.unwrap();

        assert!(storage.head_exists(&tombstone_key(&key)).await.unwrap());
        assert!(storage.head_exists(&frozen_key(&key)).await.unwrap());
        assert!(!storage.head_exists(&key).await.unwrap());
    }

    #[tokio::test]
    async fn freeze_failure_after_first_marker_remains_a_recoverable_freeze() {
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let bytes = b"conflicting bytes";
        let sha = sha256_hex(bytes);
        let qkey = format!("{QUARANTINE_PREFIX}pkg/{filename}@{}", &sha[..12]);
        let storage = RacingWriterStorage::new(qkey.clone(), b"collision".to_vec())
            .requiring_prior_key(frozen_key(&key));
        storage.inner.insert(&key, bytes.to_vec());

        assert!(freeze_side(&storage, "pkg", filename).await.is_err());
        assert!(storage.head_exists(&frozen_key(&key)).await.unwrap());
        assert!(storage.head_exists(&key).await.unwrap());
        assert!(!storage.head_exists(&tombstone_key(&key)).await.unwrap());

        storage.inner.delete_keys(&[qkey]).await.unwrap();
        freeze_side(&storage.inner, "pkg", filename).await.unwrap();
        assert!(!storage.inner.head_exists(&key).await.unwrap());
        assert!(storage
            .inner
            .head_exists(&tombstone_key(&key))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn quarantine_verifies_an_existing_actual_hash_key() {
        let storage = InMemStorage::default();
        let filename = "pkg-1.whl";
        let bytes = b"artifact bytes";
        storage.insert(&artifact_key("pkg", filename), bytes.to_vec());
        let sha = sha256_hex(bytes);
        let qkey = format!("{QUARANTINE_PREFIX}pkg/{filename}@{}", &sha[..12]);
        storage.insert(&qkey, b"not the artifact".to_vec());

        let err = quarantine(&storage, "pkg", filename).await.unwrap_err();
        assert!(err.to_string().contains("quarantine key collision"));
        assert!(storage
            .head_exists(&artifact_key("pkg", filename))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn audit_helper_quarantines_mirror_sidecar_under_private_claim() {
        let storage = InMemStorage::default();
        let filename = "pkg-1.whl";
        seed_live(&storage, "pkg", filename, b"mirror bytes", MIRROR);
        storage.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());

        assert_eq!(
            quarantine_mirror_artifacts(&storage, "pkg").await.unwrap(),
            1
        );
        // The loser LEAVES `packages/`: the fence replicates, the body it
        // suppresses moves to this bucket's own `_quarantine/`, and the
        // canonical key ends empty.
        let key = artifact_key("pkg", filename);
        assert!(!storage.head_exists(&key).await.unwrap());
        assert!(!storage.head_exists(&sidecar_key(&key)).await.unwrap());
        assert!(storage
            .head_exists(&mirror_quarantined_key(&key))
            .await
            .unwrap());
        // The fence names the filename and no hash — two buckets may have
        // demoted two bodies, so a per-bucket sha would never converge.
        assert_eq!(
            storage
                .get_bytes(&mirror_quarantined_key(&key))
                .await
                .unwrap(),
            format!(r#"{{"filename":"{filename}"}}"#).into_bytes()
        );
        assert_eq!(storage.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 1);
        let (rendered, _) = worker::list_artifacts(&storage, "pkg").await.unwrap();
        assert!(rendered.is_empty());
        // Idempotent: a second pass has nothing left to settle.
        assert_eq!(
            quarantine_mirror_artifacts(&storage, "pkg").await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn private_supersede_demotes_per_artifact_and_clears_mirror_quarantine() {
        let private = Arc::new(InMemStorage::default());
        let late_mirror = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(private.as_ref(), "pkg", filename, b"private bytes", PRIVATE);
        seed_live(
            late_mirror.as_ref(),
            "pkg",
            filename,
            b"late mirror bytes",
            MIRROR,
        );
        let mirror_marker = mirror_quarantined_key(&artifact_key("pkg", filename));
        late_mirror.insert(&mirror_marker, b"{}".to_vec());
        assert!(late_mirror.head_exists(&mirror_marker).await.unwrap());
        let state = two_bucket_state(private.clone(), late_mirror.clone());

        let _: Convergence = replicate_record(
            &state,
            private.as_ref(),
            late_mirror.as_ref(),
            "pkg",
            filename,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        assert_eq!(
            late_mirror
                .get_bytes(&artifact_key("pkg", filename))
                .await
                .unwrap(),
            b"private bytes"
        );
        let sidecar: Sidecar = serde_json::from_slice(
            &late_mirror
                .get_bytes(&sidecar_key(&artifact_key("pkg", filename)))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(sidecar.origin.as_deref(), Some(PRIVATE));
        assert_eq!(
            read_origin(late_mirror.as_ref(), "pkg")
                .await
                .unwrap()
                .as_deref(),
            Some(PRIVATE)
        );
        let mirror_sha = sha256_hex(b"late mirror bytes");
        let quarantine = format!("{QUARANTINE_PREFIX}pkg/{filename}@{}", &mirror_sha[..12]);
        assert_eq!(
            late_mirror.get_bytes(&quarantine).await.unwrap(),
            b"late mirror bytes"
        );
        assert!(!late_mirror.head_exists(&mirror_marker).await.unwrap());
    }

    #[tokio::test]
    async fn supersede_takes_the_destination_key_before_publishing_truth_about_it() {
        // Demotion's ordinary shape, and the sibling of
        // `the_copy_takes_the_destination_key_before_publishing_truth_about_it`:
        // `quarantine_mirror_artifacts` has already moved the mirror body under
        // `_quarantine/`, which is exactly what makes this destination read
        // `QuarantinedMirror`, so the artifact key is EMPTY when the private
        // record supersedes it. A sidecar published first stands over a key a
        // concurrent publish can still take; if this leg then dies the bucket
        // asserts sha A over body B permanently, and no oracle re-hashes a body
        // to notice.
        let bytes = b"private bytes";
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let private = Arc::new(InMemStorage::default());
        seed_live(private.as_ref(), "pkg", filename, bytes, PRIVATE);

        let dst = Arc::new(InMemStorage::default());
        dst.insert(
            &sidecar_key(&key),
            serde_json::to_vec(&sc(
                &sha256_hex(b"quarantined mirror bytes"),
                MIRROR,
                Yanked::Flag(false),
                0,
            ))
            .unwrap(),
        );
        dst.insert(&mirror_quarantined_key(&key), b"{}".to_vec());
        dst.insert(
            &crate::origin::origin_key("pkg"),
            MIRROR.as_bytes().to_vec(),
        );
        let seeded = dst.write_log().len();
        let state = two_bucket_state(private.clone(), dst.clone());

        let source = read_record(private.as_ref(), "pkg", filename)
            .await
            .unwrap();
        let destination = read_record(dst.as_ref(), "pkg", filename).await.unwrap();
        let verdict = decide(&source, &destination);
        assert_eq!(verdict, Verdict::Supersede(Side::A));

        let _: Convergence = execute(
            &state,
            (private.as_ref(), dst.as_ref()),
            "pkg",
            filename,
            (&source, &destination),
            verdict,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        // Only what the supersede itself wrote; the seeded record is not it.
        let log = dst.write_log().split_off(seeded);
        let artifact_at = log
            .iter()
            .position(|written| written == &key)
            .expect("the supersede landed the artifact");
        let sidecar_at = log
            .iter()
            .position(|written| written == &sidecar_key(&key))
            .expect("the supersede installed the sidecar");
        assert!(
            artifact_at < sidecar_at,
            "the destination sidecar was published before the bytes it names: {log:?}"
        );
        assert_eq!(dst.get_bytes(&key).await.unwrap(), bytes);
        // The ordering above is right and it is not free: between those two
        // writes the bucket serves bytes its own published sha contradicts. The
        // `.superseding` fence has to be declared before the body, or a death in
        // that window is unrecoverable — see the two tests below.
        let fence_at = log
            .iter()
            .position(|written| written == &superseding_key(&key))
            .expect("the supersede declared its intent");
        assert!(
            fence_at < artifact_at,
            "the body was written before the fence that makes a torn write recoverable: {log:?}"
        );
        assert!(
            !dst.head_exists(&superseding_key(&key)).await.unwrap(),
            "a completed supersede left its intent fence behind"
        );
    }

    /// The window `supersede_record` cannot make atomic: the body is replaced,
    /// then the process dies before the sidecar naming it is published. Nothing
    /// re-hashes a stored body in the normal course and both buckets' sidecars
    /// stay byte-identical, so `decide` reads the record as agreed forever —
    /// this is permanent, silent corruption unless the crashed writer left
    /// something behind that says so. Seed the exact wreckage and require the
    /// ordinary index rebuild (the `_dirty/` marker path the executor's intent
    /// bracket guarantees will visit this package) to finish the operation.
    ///
    /// Found by the simulator's SELF_CONSISTENCY oracle at
    /// `vopr --seed 18300018803 --nodes 3 --buckets 2 --packages 1 --files 1
    /// --ops 40 --fail-percent 3 --partition 100`.
    #[tokio::test]
    async fn a_rebuild_finishes_a_supersede_that_died_before_publishing_its_sidecar() {
        let superseded = b"the mirror bytes being replaced";
        let winner = b"the private bytes that won";
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        let intended = sc(&sha256_hex(winner), PRIVATE, Yanked::Flag(false), 0);

        let torn = Arc::new(InMemStorage::default());
        // The body is already the winner's; the published sidecar still names
        // the bytes it replaced. A client checking its download against the
        // index it read would reject the file.
        torn.insert(&key, winner.to_vec());
        torn.insert(
            &sidecar_key(&key),
            serde_json::to_vec(&sc(
                &sha256_hex(superseded),
                PRIVATE,
                Yanked::Flag(false),
                0,
            ))
            .unwrap(),
        );
        torn.insert(
            &superseding_key(&key),
            serde_json::to_vec(&intended).unwrap(),
        );
        torn.insert(
            &crate::origin::origin_key("pkg"),
            PRIVATE.as_bytes().to_vec(),
        );

        let state = test_state(torn.clone());
        worker::rebuild_package(&state, torn.as_ref(), "pkg")
            .await
            .unwrap();

        let published: Sidecar =
            serde_json::from_slice(&torn.get_bytes(&sidecar_key(&key)).await.unwrap()).unwrap();
        assert_eq!(
            published.sha256,
            sha256_hex(winner),
            "the rebuild left the bucket publishing a sha256 its own body contradicts"
        );
        assert_eq!(torn.get_bytes(&key).await.unwrap(), winner);
        assert!(
            !torn.head_exists(&superseding_key(&key)).await.unwrap(),
            "the intent fence outlived the repair, so every later rebuild re-hashes the body"
        );
        // And the view the rebuild rendered names the healed digest, not the
        // stale one — the repair has to land before anything derives from truth.
        let index = String::from_utf8(
            torn.get_bytes("simple/pkg/index.json")
                .await
                .unwrap_or_default(),
        )
        .unwrap();
        assert!(
            index.contains(&sha256_hex(winner)),
            "the package view still advertises the superseded digest: {index}"
        );
    }

    /// The repair finishes a torn write; it never adjudicates one. A crash on
    /// the *other* side of the window leaves the body still holding the bytes
    /// its own sidecar names — a self-consistent record that simply lost a race
    /// — and republishing the intent over it would install a sidecar for bytes
    /// this bucket does not have.
    #[tokio::test]
    async fn the_repair_leaves_a_supersede_that_died_before_touching_the_body() {
        let standing = b"the bytes this bucket actually holds";
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);

        let storage = Arc::new(InMemStorage::default());
        seed_live(storage.as_ref(), "pkg", filename, standing, PRIVATE);
        storage.insert(
            &superseding_key(&key),
            serde_json::to_vec(&sc(
                &sha256_hex(b"bytes the supersede never got to write"),
                PRIVATE,
                Yanked::Flag(false),
                0,
            ))
            .unwrap(),
        );

        let state = test_state(storage.clone());
        worker::rebuild_package(&state, storage.as_ref(), "pkg")
            .await
            .unwrap();

        let published: Sidecar =
            serde_json::from_slice(&storage.get_bytes(&sidecar_key(&key)).await.unwrap()).unwrap();
        assert_eq!(
            published.sha256,
            sha256_hex(standing),
            "the repair published a sidecar for bytes this bucket never held"
        );
        assert!(
            !storage.head_exists(&superseding_key(&key)).await.unwrap(),
            "an answered intent fence must not survive to be re-answered"
        );
    }

    #[tokio::test]
    async fn supersede_does_not_resurrect_a_tombstone_that_races_the_decision() {
        let private = Arc::new(InMemStorage::default());
        let mirror = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        seed_live(private.as_ref(), "pkg", filename, b"private", PRIVATE);
        seed_live(mirror.as_ref(), "pkg", filename, b"mirror", MIRROR);
        let state = two_bucket_state(private.clone(), mirror.clone());
        let source = read_record(private.as_ref(), "pkg", filename)
            .await
            .unwrap();
        let destination = read_record(mirror.as_ref(), "pkg", filename).await.unwrap();
        let verdict = decide(&source, &destination);
        assert_eq!(verdict, Verdict::Supersede(Side::A));

        tombstone::write(mirror.as_ref(), &key, filename)
            .await
            .unwrap();
        let _: Convergence = execute(
            &state,
            (private.as_ref(), mirror.as_ref()),
            "pkg",
            filename,
            (&source, &destination),
            verdict,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        assert!(mirror.head_exists(&tombstone_key(&key)).await.unwrap());
        assert!(!mirror.head_exists(&key).await.unwrap());
        assert!(!mirror.head_exists(&sidecar_key(&key)).await.unwrap());
    }

    #[tokio::test]
    async fn mirror_only_filename_landing_after_demotion_is_quarantined() {
        let private = Arc::new(InMemStorage::default());
        let destination = Arc::new(InMemStorage::default());
        let private_filename = "pkg-2.whl";
        let late_mirror_filename = "pkg-1.whl";
        seed_live(
            private.as_ref(),
            "pkg",
            private_filename,
            b"private bytes",
            PRIVATE,
        );
        seed_live(
            destination.as_ref(),
            "pkg",
            late_mirror_filename,
            b"late public bytes",
            MIRROR,
        );
        // The proxy passed its final mirror-claim check, then package demotion
        // completed before this extra filename's artifact + sidecar landed.
        destination.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        let state = two_bucket_state(private.clone(), destination.clone());

        diff_pair(&state, private.as_ref(), destination.as_ref())
            .await
            .unwrap();

        assert!(!destination
            .head_exists(&artifact_key("pkg", late_mirror_filename))
            .await
            .unwrap());
        assert!(destination
            .head_exists(&mirror_quarantined_key(&artifact_key(
                "pkg",
                late_mirror_filename
            )))
            .await
            .unwrap());
        // The fence replicated to the peer that never held the loser.
        assert!(private
            .head_exists(&mirror_quarantined_key(&artifact_key(
                "pkg",
                late_mirror_filename
            )))
            .await
            .unwrap());
        assert!(!private
            .head_exists(&artifact_key("pkg", late_mirror_filename))
            .await
            .unwrap());
        assert!(destination
            .list_all(QUARANTINE_PREFIX)
            .await
            .unwrap()
            .iter()
            .any(|object| object.key.contains(late_mirror_filename)));
        assert_eq!(
            destination
                .get_bytes(&artifact_key("pkg", private_filename))
                .await
                .unwrap(),
            b"private bytes"
        );
    }

    /// A demotion the operator already resolved by republishing privately, and
    /// a `SettleMirrorQuarantine` verdict still in flight from the listing that
    /// preceded it. The stale settle used to suppress the record that resolved
    /// the demotion — and then a second pass, holding the same listing-era
    /// `Live { Private }` read, called the fence it re-wrote spent and deleted
    /// it. The filename ended with no body and nothing authorizing its absence:
    /// the acknowledged upload simply gone on every bucket, and `origin
    /// release` free to reopen the name for a proxy to re-fetch clean.
    ///
    /// Both passes are replayed here in the order the simulator found them
    /// (`vopr --seed 16200065974 --nodes 3 --buckets 2 --packages 6 --files 2
    /// --ops 160 --fail-percent 3 --partition 100`), against the real executor
    /// with the real stale inputs.
    #[tokio::test]
    async fn a_stale_settle_never_suppresses_the_private_truth_that_resolved_the_demotion() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let key = artifact_key("pkg", filename);
        // A: the private republish landed under a fence still standing from the
        // demotion it resolves. B: that demotion, settled.
        seed_live(a.as_ref(), "pkg", filename, b"acked private bytes", PRIVATE);
        a.insert(&mirror_quarantined_key(&key), b"{}".to_vec());
        b.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        b.insert(&mirror_quarantined_key(&key), b"{}".to_vec());

        let ra = read_record(a.as_ref(), "pkg", filename).await.unwrap();
        let rb = read_record(b.as_ref(), "pkg", filename).await.unwrap();
        // The fence over private truth is spent, so this pair is a supersede —
        // the settle below is only reachable from an older listing.
        assert_eq!(decide(&ra, &rb), Verdict::Supersede(Side::A));

        let state = two_bucket_state(a.clone(), b.clone());
        for verdict in [Verdict::SettleMirrorQuarantine, Verdict::Noop] {
            let _: Convergence = execute(
                &state,
                (a.as_ref(), b.as_ref()),
                "pkg",
                filename,
                (&ra, &rb),
                verdict,
                ArtifactSource::Bucket,
            )
            .await
            .unwrap();
        }

        assert_eq!(
            a.get_bytes(&key).await.unwrap(),
            b"acked private bytes",
            "the settle suppressed the private record that resolved the demotion",
        );
        assert!(
            !a.head_exists(&mirror_quarantined_key(&key)).await.unwrap(),
            "the fence over live private truth is spent and must not survive",
        );
        assert!(
            a.list_all(QUARANTINE_PREFIX).await.unwrap().is_empty(),
            "nothing was demoted, so nothing belongs in _quarantine/",
        );
    }

    #[tokio::test]
    async fn late_mirror_status_under_private_claim_normalizes_to_active() {
        let a = InMemStorage::default();
        let b = InMemStorage::default();
        a.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        b.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        let late = status::ProjectStatusDoc {
            status: status::ProjectStatus::Quarantined,
            reason: Some("late public status".into()),
        };
        status::advance_status(&b, "pkg", &late, Some(Origin::Mirror))
            .await
            .unwrap();

        reconcile_project_status(&a, &b, "pkg").await.unwrap();
        let normalized = status::read_status_versioned(&b, "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(normalized.doc, status::ProjectStatusDoc::default());
        assert_eq!(normalized.epoch, 0);
        assert_eq!(normalized.origin, Some(Origin::Private));
        assert_eq!(
            status::read_status(&b, "pkg").await.unwrap(),
            status::ProjectStatusDoc::default(),
            "the stale mirror status must no longer be user-visible",
        );
        let dirty = b.list_all("_dirty/pkg!").await.unwrap();
        assert_eq!(dirty.len(), 2);
        assert_eq!(
            dirty
                .iter()
                .filter(|object| object.key.ends_with(".commit"))
                .count(),
            1
        );
        assert!(status::read_status_versioned(&a, "pkg")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn fanout_sync_copies_to_healthy_peer_and_writes_no_note() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(a.as_ref(), "pkg", filename, b"wheel", PRIVATE);
        let state = two_bucket_state(a.clone(), b.clone());
        let pinned = state.pin();

        fanout_sync(&state, &pinned, "pkg", filename, None)
            .await
            .expect("every gap this fan-out leaves is recorded");
        assert!(b.head_exists(&artifact_key("pkg", filename)).await.unwrap());
        // The happy path leaves no note: the record is durable on every bucket
        // at ack time.
        assert!(a.list_all(REPL_PREFIX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn fanout_sync_notes_an_ineligible_peer_without_copying() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(a.as_ref(), "pkg", filename, b"wheel", PRIVATE);
        let mut state = two_bucket_state(a.clone(), b.clone());
        let health = Arc::new(
            crate::bucket_health::HealthController::new(
                2,
                crate::bucket_health::HealthPolicy::new(1, std::time::Duration::from_secs(60))
                    .unwrap(),
            )
            .unwrap(),
        );
        // One availability failure crosses B into Unhealthy (leave threshold 1).
        health
            .observe(1, crate::bucket_health::BucketSignal::Timeout)
            .unwrap();
        state.bucket_health = Some(health);
        let pinned = state.pin();

        fanout_sync(&state, &pinned, "pkg", filename, None)
            .await
            .expect("every gap this fan-out leaves is recorded");
        // No attempt against an ineligible bucket, but a durable note is left so
        // the record reaches B when it heals.
        assert!(!b.head_exists(&artifact_key("pkg", filename)).await.unwrap());
        assert!(a
            .list_all(&format!(
                "{REPL_PREFIX}{}/",
                crate::counters::bucket_tag("b")
            ))
            .await
            .unwrap()
            .iter()
            .any(|m| m.key.contains("/pkg/")));
    }

    #[test]
    fn the_hash_sink_hashes_a_chunked_body_exactly_as_one_pass_would() {
        let body = b"the artifact bytes, arriving in pieces";
        let mut sink = HashingSink::new(body.len() as u64);
        for chunk in body.chunks(7) {
            sink.accept(chunk).unwrap();
        }
        assert_eq!(sink.finish(), (sha256_hex(body), body.len() as u64));
    }

    /// The cap is what makes the streaming read *bounded* rather than merely
    /// incremental. Remove it and a source whose body no longer matches its
    /// sidecar — the case this read exists to catch — decides how much this
    /// node accumulates before the hash disagrees.
    #[test]
    fn the_hash_sink_refuses_a_body_longer_than_its_sidecar_attests() {
        let mut sink = HashingSink::new(8);
        sink.accept(b"12345678").unwrap();
        let err = sink.accept(b"9").unwrap_err();
        assert!(
            err.to_string().contains("longer than the 8 bytes"),
            "unexpected error: {err}"
        );
    }

    /// A source bucket that will not take the `_repl/` note. The note is the
    /// only durable record that a peer is owed the file, so a bucket that
    /// refuses it is a bucket where no ack is honest — and the nonce in the key
    /// means the in-memory fake's exact-key failure switch cannot name it.
    struct NoteRefusingStorage {
        inner: Arc<InMemStorage>,
        /// Notes to refuse before letting one through; `usize::MAX` refuses all.
        refusals: std::sync::atomic::AtomicUsize,
    }

    impl NoteRefusingStorage {
        fn new(inner: Arc<InMemStorage>, refusals: usize) -> Arc<Self> {
            Arc::new(Self {
                inner,
                refusals: std::sync::atomic::AtomicUsize::new(refusals),
            })
        }
    }

    #[async_trait::async_trait]
    impl Storage for NoteRefusingStorage {
        async fn put_bytes(
            &self,
            key: &str,
            bytes: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<()> {
            if key.starts_with(REPL_PREFIX)
                && self
                    .refusals
                    .fetch_update(
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                        |n| (n > 0).then_some(n.saturating_sub(1)),
                    )
                    .is_ok()
            {
                bail!("injected note-write failure");
            }
            self.inner.put_bytes(key, bytes, content_type).await
        }
        async fn head_exists(&self, key: &str) -> Result<bool> {
            self.inner.head_exists(key).await
        }
        async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
            self.inner.stored_size(key).await
        }
        async fn serve_artifact(
            &self,
            key: &str,
            range: Option<&str>,
        ) -> Result<axum::response::Response<axum::body::Body>> {
            self.inner.serve_artifact(key, range).await
        }
        async fn presign_get(
            &self,
            key: &str,
            expires: std::time::Duration,
        ) -> Result<Option<String>> {
            self.inner.presign_get(key, expires).await
        }
        async fn put_if_absent(
            &self,
            key: &str,
            bytes: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<bool> {
            self.inner.put_if_absent(key, bytes, content_type).await
        }
        async fn put_file_if_absent(
            &self,
            key: &str,
            path: &std::path::Path,
            content_type: Option<&str>,
        ) -> Result<bool> {
            self.inner.put_file_if_absent(key, path, content_type).await
        }
        async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
            self.inner.get_bytes(key).await
        }
        async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
            self.inner.list_dir_entries(dir_prefix).await
        }
        async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
            self.inner.list_all(prefix).await
        }
        async fn delete_keys(&self, keys: &[String]) -> Result<()> {
            self.inner.delete_keys(keys).await
        }
        async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.inner.get_with_etag(key).await
        }
        async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
            self.inner.put_if_none_match(key, bytes).await
        }
        async fn put_if_match(
            &self,
            key: &str,
            etag: &str,
            bytes: Vec<u8>,
        ) -> Result<Option<String>> {
            self.inner.put_if_match(key, etag, bytes).await
        }
    }

    /// A two-bucket fleet whose peer is fenced out (so a note is owed) over a
    /// source bucket that refuses `refusals` note writes.
    fn note_refusing_fleet(
        a: Arc<InMemStorage>,
        b: Arc<InMemStorage>,
        refusals: usize,
    ) -> AppState {
        let src = NoteRefusingStorage::new(a, refusals);
        let mut state = two_bucket_state(src as Arc<dyn Storage>, b as Arc<dyn Storage>);
        let health = Arc::new(
            crate::bucket_health::HealthController::new(
                2,
                crate::bucket_health::HealthPolicy::new(1, std::time::Duration::from_secs(60))
                    .unwrap(),
            )
            .unwrap(),
        );
        health
            .observe(1, crate::bucket_health::BucketSignal::Timeout)
            .unwrap();
        state.bucket_health = Some(health);
        state
    }

    /// The ack bug: a note write that fails was logged and swallowed, and the
    /// publish answered 200 with the peer holding neither the record, nor a
    /// marker explaining its absence, nor a note owing it — the state
    /// `ACK_TOTALITY` exists to forbid. Nothing downstream re-derives that gap:
    /// `_dirty/` markers drive index rebuilds, not replication.
    #[tokio::test]
    async fn a_note_that_will_not_land_refuses_the_ack() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(a.as_ref(), "pkg", filename, b"wheel", PRIVATE);
        let state = note_refusing_fleet(a.clone(), b.clone(), usize::MAX);
        let pinned = state.pin();

        let err = fanout_sync(&state, &pinned, "pkg", filename, None)
            .await
            .expect_err("an unrecordable replication gap must not ack");
        assert!(
            err.to_string().contains("could not be recorded"),
            "unexpected error: {err}"
        );
        // This string is interpolated into the client-visible 503, so it counts
        // the peers that missed the note instead of naming them: a bucket's
        // configured URI (and `@region` label) is fleet topology no authenticated
        // uploader should learn from a transient outage.
        assert!(
            err.to_string()
                .ends_with("could not be recorded on 1 of 1 other bucket(s)"),
            "the 503 body must count peer buckets, not name them: {err}"
        );
        assert!(a.list_all(REPL_PREFIX).await.unwrap().is_empty());
    }

    /// The retry is not decoration: a single transient failure on one small PUT
    /// must not turn into a 503 on an upload that is otherwise fine.
    #[tokio::test]
    async fn a_note_that_lands_on_the_retry_still_acks() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(a.as_ref(), "pkg", filename, b"wheel", PRIVATE);
        let state = note_refusing_fleet(a.clone(), b.clone(), 1);
        let pinned = state.pin();

        fanout_sync(&state, &pinned, "pkg", filename, None)
            .await
            .expect("the retry recorded the gap");
        assert!(!a.list_all(REPL_PREFIX).await.unwrap().is_empty());
    }

    /// A peer holding a bare artifact of its own — a publish or copy that died
    /// between its bytes and its sidecar — makes the merge defer: only that
    /// bucket's audit can backfill the sidecar, and copying over it would
    /// fabricate cross-bucket truth (§4). The deferral is not a convergence, so
    /// the ack may not fire without a durable note owing the peer the record
    /// (dev/DESIGN.md's totality principle).
    #[tokio::test]
    async fn fanout_sync_notes_a_peer_whose_orphan_artifact_defers_the_merge() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(a.as_ref(), "pkg", filename, b"wheel", PRIVATE);
        // B's own crashed writer: bytes, no sidecar.
        b.insert(&artifact_key("pkg", filename), b"half-written".to_vec());
        b.insert(
            &crate::origin::origin_key("pkg"),
            PRIVATE.as_bytes().to_vec(),
        );
        let state = two_bucket_state(a.clone(), b.clone());
        let pinned = state.pin();

        fanout_sync(&state, &pinned, "pkg", filename, None)
            .await
            .expect("every gap this fan-out leaves is recorded");
        assert!(
            a.list_all(&format!(
                "{REPL_PREFIX}{}/",
                crate::counters::bucket_tag("b")
            ))
            .await
            .unwrap()
            .iter()
            .any(|m| m.key.contains("/pkg/")),
            "a deferred fan-out must leave the peer a repair note before the ack",
        );
    }

    /// The totality principle, asserted as the simulator's `ACK_TOTALITY`
    /// oracle states it: at the moment a publish acks, every peer must hold the
    /// record, or a merge marker explaining its absence, or a `_repl/` note
    /// owing it. A source that resolved a mirror→private demotion is the case
    /// that broke it — `decide` answered `Noop` ("the two sides agree") over a
    /// pair where the peer had never heard of the demotion at all, `execute`
    /// mapped that to `Convergence::Converged`, and the fan-out acked with the
    /// peer holding nothing and owed nothing.
    ///
    /// This is the same defect `fd14f01` fixed for the bare-artifact `(Orphan,
    /// _)` arm above, in a second arm: a `Noop` that was a deferral, not an
    /// agreement. It is pinned here and not only in `decide`'s unit tests
    /// because the algebra is not what was wrong — the *ack* was, and no test
    /// below `fanout_sync` could see it.
    #[tokio::test]
    async fn fanout_sync_never_acks_a_demotion_the_peer_never_heard_of() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        // A resolved the demotion: fence standing, canonical key empty, the
        // losing body in its own `_quarantine/`. B has never heard of it.
        seed_live(a.as_ref(), "pkg", filename, b"mirror bytes", MIRROR);
        a.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        assert_eq!(
            quarantine_mirror_artifacts(a.as_ref(), "pkg")
                .await
                .unwrap(),
            1
        );
        let state = two_bucket_state(a.clone(), b.clone());
        let pinned = state.pin();

        fanout_sync(&state, &pinned, "pkg", filename, None)
            .await
            .expect("every gap this fan-out leaves is recorded");

        let key = artifact_key("pkg", filename);
        let owed = a
            .list_all(&format!(
                "{REPL_PREFIX}{}/",
                crate::counters::bucket_tag("b")
            ))
            .await
            .unwrap()
            .iter()
            .any(|m| m.key.contains("/pkg/"));
        let holds_record =
            b.head_exists(&key).await.unwrap() && b.head_exists(&sidecar_key(&key)).await.unwrap();
        let fenced = b.head_exists(&mirror_quarantined_key(&key)).await.unwrap();
        assert!(
            holds_record || fenced || owed,
            "ACK_TOTALITY: the ack fired with the peer holding neither the \
             record, nor a marker explaining its absence, nor a note owing it",
        );
        // And the resolution the fan-out is supposed to reach: the fence is
        // truth and replicates, so no note is owed and no body crosses.
        assert!(fenced && !owed);
        assert!(b.list_all(QUARANTINE_PREFIX).await.unwrap().is_empty());
    }

    /// The same deferral on the note-draining path: the marker sweep may only
    /// consume a note the copy actually delivered. Consuming one the merge
    /// merely declined drops the fleet's last record that the peer is owed.
    #[tokio::test]
    async fn marker_sweep_retains_a_note_the_merge_deferred() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(b.as_ref(), "pkg", filename, b"straggler", PRIVATE);
        a.insert(&artifact_key("pkg", filename), b"half-written".to_vec());
        a.insert(
            &crate::origin::origin_key("pkg"),
            PRIVATE.as_bytes().to_vec(),
        );
        let state = two_bucket_state(a.clone(), b.clone());
        write_marker(b.as_ref(), "a", "pkg", filename)
            .await
            .unwrap();

        sweep_all_markers(&state).await.unwrap();
        assert!(
            !b.list_all(REPL_PREFIX).await.unwrap().is_empty(),
            "a note the merge deferred must survive the sweep that could not deliver it",
        );
    }

    #[tokio::test]
    async fn all_bucket_sweep_drains_a_non_selected_source() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(b.as_ref(), "pkg", filename, b"straggler", PRIVATE);
        let state = two_bucket_state(a.clone(), b.clone());
        write_marker(b.as_ref(), "a", "pkg", filename)
            .await
            .unwrap();

        sweep_all_markers(&state).await.unwrap();
        assert!(a.head_exists(&artifact_key("pkg", filename)).await.unwrap());
        assert!(b.list_all(REPL_PREFIX).await.unwrap().is_empty());
    }

    fn repl_handle(storage: Arc<InMemStorage>, name: &str) -> BucketHandle {
        BucketHandle {
            storage: storage as Arc<dyn Storage>,
            name: name.to_string(),
        }
    }

    /// F25: a backfill sentinel is keyed by the new bucket's stable
    /// [`crate::counters::bucket_tag`], so the read-affinity fence follows that
    /// bucket across a topology reorder. The position-keyed predecessor seeded
    /// `_repl/<pos>/_backfill!` and orphaned the fence the moment a reorder shifted
    /// the bucket's index — fail-open, the dangerous direction: an un-caught-up
    /// bucket would silently start serving stale/incomplete reads. Revert the key
    /// change (position-keyed seed + gate) and the reordered assertion below flips
    /// to "caught up" and this test fails.
    #[tokio::test]
    async fn a_backfill_sentinel_follows_its_bucket_across_a_reorder() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let c = Arc::new(InMemStorage::default());
        // `c` is freshly added and empty; migrate fences it by seeding its
        // sentinel — keyed by c's tag — on every reachable peer.
        let c_tag = crate::counters::bucket_tag("c");
        seed_backfill_sentinel(a.as_ref(), &c_tag).await.unwrap();
        seed_backfill_sentinel(b.as_ref(), &c_tag).await.unwrap();

        // Original topology [a, b, c]: c (index 2) is un-caught-up and fenced.
        let original = vec![
            repl_handle(a.clone(), "a"),
            repl_handle(b.clone(), "b"),
            repl_handle(c.clone(), "c"),
        ];
        assert!(
            !region_owed_no_notes(&original, 2).await,
            "c is un-caught-up and must be fenced in its original position",
        );

        // Operator reorders the topology to [c, a, b]: c is now index 0. No data
        // moved; only positions did. The sentinel, keyed by the stable tag `c`,
        // still fences c. A position-keyed sentinel (`_repl/2/`) would now be
        // looked up under `_repl/0/`, find nothing, and wrongly report c caught up.
        let reordered = vec![
            repl_handle(c.clone(), "c"),
            repl_handle(a.clone(), "a"),
            repl_handle(b.clone(), "b"),
        ];
        assert!(
            !region_owed_no_notes(&reordered, 0).await,
            "a name-keyed sentinel follows its bucket across a reorder; c stays fenced",
        );
    }

    /// F25, the second failure mode: a bucket that lands where a since-departed
    /// bucket used to sit must not inherit that bucket's stale sentinel. Name
    /// keying closes it — the fresh bucket is looked up under its own tag, which is
    /// clean. The position-keyed predecessor would have found the recycled
    /// `_repl/<pos>/` marker and false-fenced a fully-converged bucket.
    #[tokio::test]
    async fn a_recycled_position_does_not_inherit_a_departed_buckets_sentinel() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let fresh = Arc::new(InMemStorage::default());
        // A since-removed bucket left its sentinel behind on the peers, keyed by
        // ITS tag.
        let gone_tag = crate::counters::bucket_tag("gone");
        seed_backfill_sentinel(a.as_ref(), &gone_tag).await.unwrap();
        seed_backfill_sentinel(b.as_ref(), &gone_tag).await.unwrap();

        // `fresh` is fully converged and carries no sentinel of its own; it now
        // occupies index 2 — the slot the removed bucket used to hold.
        let handles = vec![
            repl_handle(a.clone(), "a"),
            repl_handle(b.clone(), "b"),
            repl_handle(fresh.clone(), "fresh"),
        ];
        assert!(
            region_owed_no_notes(&handles, 2).await,
            "a name-keyed gate never inherits a departed bucket's sentinel",
        );
    }

    /// The repair-note half of the `_repl/` family is keyed the same way: a note
    /// names its destination by tag, so the sweep resolves it to that bucket's
    /// CURRENT position after a reorder and drops it (never misroutes it onto a
    /// survivor) once the destination is gone.
    #[test]
    fn a_repair_note_routes_by_destination_tag_not_position() {
        let key = format!(
            "{REPL_PREFIX}{}/pkg/pkg-1.whl!nonce",
            crate::counters::bucket_tag("c")
        );
        let marker = parse_repl_marker(&key).expect("a repair note key parses");
        assert_eq!(marker.dest_tag, crate::counters::bucket_tag("c"));

        let fresh = |name: &str| repl_handle(Arc::new(InMemStorage::default()), name);
        let original = vec![fresh("a"), fresh("b"), fresh("c")];
        assert_eq!(dest_index_for_tag(&original, &marker.dest_tag), Some(2));

        let reordered = vec![fresh("c"), fresh("a"), fresh("b")];
        assert_eq!(dest_index_for_tag(&reordered, &marker.dest_tag), Some(0));

        // Destination removed entirely: undeliverable, resolves to no bucket.
        let without_c = vec![fresh("a"), fresh("b")];
        assert_eq!(dest_index_for_tag(&without_c, &marker.dest_tag), None);
    }

    #[tokio::test]
    async fn marker_sweep_waits_for_recovered_topology_validation() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let from_a = "from_a-1.whl";
        let from_b = "from_b-1.whl";
        seed_live(a.as_ref(), "from-a", from_a, b"from a", PRIVATE);
        seed_live(b.as_ref(), "from-b", from_b, b"from b", PRIVATE);
        let mut state = two_bucket_state(a.clone(), b.clone());
        let health = Arc::new(
            crate::bucket_health::HealthController::new(
                2,
                crate::bucket_health::HealthPolicy::new(1, std::time::Duration::from_secs(60))
                    .unwrap(),
            )
            .unwrap(),
        );
        health
            .observe(1, crate::bucket_health::BucketSignal::Success)
            .unwrap();
        state.bucket_health = Some(health.clone());
        write_marker(a.as_ref(), "b", "from-a", from_a)
            .await
            .unwrap();
        write_marker(b.as_ref(), "a", "from-b", from_b)
            .await
            .unwrap();

        // Every background path with an index-aware entry point skips B. The
        // B-only private record would copy to A if the full diff touched it.
        reconcile(&state, &state.pin()).await.unwrap();
        assert!(
            !a.head_exists(&artifact_key("from-b", from_b))
                .await
                .unwrap(),
            "pending topology validation must gate full-diff reads"
        );

        sweep_all_markers(&state).await.unwrap();
        assert!(
            !b.head_exists(&artifact_key("from-a", from_a))
                .await
                .unwrap(),
            "pending topology validation must gate destination writes"
        );
        assert!(
            !a.head_exists(&artifact_key("from-b", from_b))
                .await
                .unwrap(),
            "pending topology validation must gate source reads"
        );
        assert_eq!(a.list_all(REPL_PREFIX).await.unwrap().len(), 1);
        assert_eq!(b.list_all(REPL_PREFIX).await.unwrap().len(), 1);

        health.topology_revalidated(1).unwrap();
        sweep_all_markers(&state).await.unwrap();
        assert!(b
            .head_exists(&artifact_key("from-a", from_a))
            .await
            .unwrap());
        assert!(a
            .head_exists(&artifact_key("from-b", from_b))
            .await
            .unwrap());
        assert!(a.list_all(REPL_PREFIX).await.unwrap().is_empty());
        assert!(b.list_all(REPL_PREFIX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn three_bucket_full_diff_converges_peer_writes_in_one_sweep() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let c = Arc::new(InMemStorage::default());
        seed_live(b.as_ref(), "from-b", "from_b-1.whl", b"b", PRIVATE);
        seed_live(c.as_ref(), "from-c", "from_c-1.whl", b"c", PRIVATE);
        let state = three_bucket_state(a.clone(), b.clone(), c.clone());
        let pinned = state.pin();

        reconcile(&state, &pinned).await.unwrap();

        for storage in [&a, &b, &c] {
            assert!(storage
                .head_exists(&artifact_key("from-b", "from_b-1.whl"))
                .await
                .unwrap());
            assert!(storage
                .head_exists(&artifact_key("from-c", "from_c-1.whl"))
                .await
                .unwrap());
        }
    }

    #[tokio::test]
    async fn three_bucket_full_diff_propagates_a_peer_conflict_in_one_sweep() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let c = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(b.as_ref(), "pkg", filename, b"from b", PRIVATE);
        seed_live(c.as_ref(), "pkg", filename, b"from c", PRIVATE);
        let state = three_bucket_state(a.clone(), b.clone(), c.clone());
        let pinned = state.pin();

        reconcile(&state, &pinned).await.unwrap();

        for storage in [&a, &b, &c] {
            assert!(storage
                .head_exists(&frozen_key(&artifact_key("pkg", filename)))
                .await
                .unwrap());
            assert!(!storage
                .head_exists(&artifact_key("pkg", filename))
                .await
                .unwrap());
            assert!(!storage
                .list_all(QUARANTINE_PREFIX)
                .await
                .unwrap()
                .is_empty());
        }
    }

    #[tokio::test]
    async fn ensure_private_origin_creates_demotes_and_is_idempotent() {
        // Absent → creates a private claim.
        let s = InMemStorage::default();
        ensure_private_origin(&s, "fresh").await.unwrap();
        assert_eq!(
            read_origin(&s, "fresh").await.unwrap().as_deref(),
            Some(PRIVATE)
        );

        // Mirror → private is a direct CAS in v2.
        claim_origin(&s, "was-mirror", MIRROR).await.unwrap();
        ensure_private_origin(&s, "was-mirror").await.unwrap();
        assert_eq!(
            read_origin(&s, "was-mirror").await.unwrap().as_deref(),
            Some(PRIVATE)
        );

        // Already private → idempotent no-op.
        ensure_private_origin(&s, "fresh").await.unwrap();
        assert_eq!(
            read_origin(&s, "fresh").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
    }

    #[tokio::test]
    async fn ensure_mirror_origin_claims_but_never_demotes_private() {
        let s = InMemStorage::default();
        // Absent → creates a mirror claim (a snapshot replicating into a fresh
        // bucket).
        ensure_mirror_origin(&s, "fresh").await.unwrap();
        assert_eq!(
            read_origin(&s, "fresh").await.unwrap().as_deref(),
            Some(MIRROR)
        );
        // Already mirror → idempotent.
        ensure_mirror_origin(&s, "fresh").await.unwrap();
        assert_eq!(
            read_origin(&s, "fresh").await.unwrap().as_deref(),
            Some(MIRROR)
        );
        // Private → left terminal; a snapshot NEVER demotes private truth.
        claim_origin(&s, "owned", PRIVATE).await.unwrap();
        ensure_mirror_origin(&s, "owned").await.unwrap();
        assert_eq!(
            read_origin(&s, "owned").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
    }

    #[tokio::test]
    async fn install_mirror_sidecar_accepts_snapshot_and_cache_but_yields_to_private() {
        let akey = artifact_key("pkg", "pkg-1.0-py3-none-any.whl");
        let key = sidecar_key(&akey);
        let mut snapshot = decide::tests::sc("abc", MIRROR, crate::sidecar::Yanked::Flag(false), 0);
        snapshot.snapshot = true;
        let cache = decide::tests::sc("abc", MIRROR, crate::sidecar::Yanked::Flag(false), 0);

        // Both a snapshot and a proxy-cache sidecar install on this path now —
        // both are mirror truth that replicates.
        for source in [&snapshot, &cache] {
            let s = InMemStorage::default();
            assert!(install_or_verify_mirror_sidecar(&s, &key, source)
                .await
                .unwrap());
            // Idempotent second pass.
            assert!(!install_or_verify_mirror_sidecar(&s, &key, source)
                .await
                .unwrap());
        }

        // A private-origin source is still rejected: this path is mirror-only.
        let s = InMemStorage::default();
        let private_src = decide::tests::sc("abc", PRIVATE, crate::sidecar::Yanked::Flag(false), 0);
        assert!(install_or_verify_mirror_sidecar(&s, &key, &private_src)
            .await
            .is_err());

        // Private truth on the destination is never overwritten by a mirror record.
        let s2 = InMemStorage::default();
        let private = decide::tests::sc("abc", PRIVATE, crate::sidecar::Yanked::Flag(false), 0);
        s2.insert(&key, serde_json::to_vec(&private).unwrap());
        assert!(!install_or_verify_mirror_sidecar(&s2, &key, &snapshot)
            .await
            .unwrap());
        let stored: Sidecar = serde_json::from_slice(&s2.get_bytes(&key).await.unwrap()).unwrap();
        assert_eq!(stored.origin.as_deref(), Some(PRIVATE));
    }

    #[tokio::test]
    async fn full_diff_replicates_an_origin_only_private_claim_over_unclaimed() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        a.insert(&crate::origin::origin_key("reserved"), b"private".to_vec());
        b.insert(
            &crate::origin::origin_key("reserved"),
            b"unclaimed".to_vec(),
        );
        let state = two_bucket_state(a.clone(), b.clone());

        diff_pair(&state, a.as_ref(), b.as_ref()).await.unwrap();

        assert_eq!(
            read_origin(b.as_ref(), "reserved")
                .await
                .unwrap()
                .as_deref(),
            Some(PRIVATE)
        );
    }

    #[tokio::test]
    async fn full_diff_replicates_an_origin_only_mirror_claim_over_unclaimed() {
        // A mirror claim whose pre-artifact fan-out never reached the peer. The
        // package holds nothing but the claim, so there is no record for the
        // copy path's own `ensure_mirror_origin` to ride along with — the diff's
        // split reconciliation is the only repair path there is. Without it the
        // pair never converges (VOPR seed 42, four ops and no injected faults:
        // `packages/vopr-alpha/.origin: b0=Some("mirror") b1=None`).
        for claimed_side in [0, 1] {
            for peer_claim in [None, Some(&b"unclaimed"[..])] {
                let a = Arc::new(InMemStorage::default());
                let b = Arc::new(InMemStorage::default());
                let key = crate::origin::origin_key("reserved");
                let (claimant, peer): (&Arc<InMemStorage>, &Arc<InMemStorage>) =
                    if claimed_side == 0 {
                        (&a, &b)
                    } else {
                        (&b, &a)
                    };
                claimant.insert(&key, b"mirror".to_vec());
                if let Some(sentinel) = peer_claim {
                    peer.insert(&key, sentinel.to_vec());
                }
                let state = two_bucket_state(a.clone(), b.clone());

                diff_pair(&state, a.as_ref(), b.as_ref()).await.unwrap();

                assert_eq!(
                    read_origin(peer.as_ref(), "reserved").await.unwrap(),
                    Some(MIRROR.to_string()),
                    "claimed_side={claimed_side} peer_claim={peer_claim:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn origin_marker_fanout_reserves_a_mirror_snapshot_claim_on_the_peer() {
        // A `sync --to` snapshot's first-write claim fans out its `.origin`
        // marker before any bytes land. The peer must reserve MIRROR the same
        // way a private claim reserves PRIVATE — otherwise the pre-artifact
        // dependency-confusion window stays open on every peer during fan-out.
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        a.insert(&crate::origin::origin_key("pkg"), b"mirror".to_vec());
        let state = two_bucket_state(a.clone(), b.clone());

        let _: Convergence = replicate_record(
            &state,
            a.as_ref(),
            b.as_ref(),
            "pkg",
            ORIGIN_MARKER,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        assert_eq!(
            read_origin(b.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(MIRROR)
        );
    }

    #[tokio::test]
    async fn origin_marker_fanout_never_demotes_private_truth_to_mirror() {
        // A mirror snapshot claim reaching a peer that already holds private
        // truth leaves it terminal — private outranks mirror everywhere.
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        a.insert(&crate::origin::origin_key("pkg"), b"mirror".to_vec());
        b.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        let state = two_bucket_state(a.clone(), b.clone());

        let _: Convergence = replicate_record(
            &state,
            a.as_ref(),
            b.as_ref(),
            "pkg",
            ORIGIN_MARKER,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        assert_eq!(
            read_origin(b.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
        // The reconcile split promotes the mirror source to private truth.
        assert_eq!(
            read_origin(a.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
    }

    #[tokio::test]
    async fn private_project_status_replicates_and_marks_destination_dirty() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        for storage in [&a, &b] {
            storage.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        }
        let quarantined = status::ProjectStatusDoc {
            status: status::ProjectStatus::Quarantined,
            reason: Some("investigating".into()),
        };
        status::advance_status(a.as_ref(), "pkg", &quarantined, Some(Origin::Private))
            .await
            .unwrap();
        let state = two_bucket_state(a.clone(), b.clone());

        let _: Convergence = replicate_record(
            &state,
            a.as_ref(),
            b.as_ref(),
            "pkg",
            PROJECT_STATUS_MARKER,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        assert_eq!(
            status::read_status(b.as_ref(), "pkg").await.unwrap(),
            quarantined
        );
        assert!(!b
            .list_all(crate::app::DIRTY_PREFIX)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn mirror_project_status_stays_bucket_local() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        a.insert(&crate::origin::origin_key("pkg"), b"mirror".to_vec());
        let archived = status::ProjectStatusDoc {
            status: status::ProjectStatus::Archived,
            reason: None,
        };
        status::advance_status(a.as_ref(), "pkg", &archived, Some(Origin::Mirror))
            .await
            .unwrap();
        let state = two_bucket_state(a.clone(), b.clone());

        let _: Convergence = replicate_record(
            &state,
            a.as_ref(),
            b.as_ref(),
            "pkg",
            PROJECT_STATUS_MARKER,
            ArtifactSource::Bucket,
        )
        .await
        .unwrap();

        assert_eq!(
            status::read_status(b.as_ref(), "pkg").await.unwrap(),
            status::ProjectStatusDoc::default()
        );
        assert!(b
            .get_with_etag(&status::status_key("pkg"))
            .await
            .unwrap()
            .is_none());
    }

    fn art(pkg: &str, ver: &str) -> String {
        format!("packages/{pkg}/{pkg}-{ver}-py3-none-any.whl")
    }

    #[tokio::test]
    async fn removal_diff_flags_only_sole_copy_artifacts() {
        let removed = Arc::new(InMemStorage::default());
        let survivor = Arc::new(InMemStorage::default());
        // Shared artifact lives on both buckets.
        removed.insert(&art("pkga", "1.0"), b"a".to_vec());
        survivor.insert(&art("pkga", "1.0"), b"a".to_vec());
        // This one lives only on the bucket being removed.
        removed.insert(&art("pkgb", "2.0"), b"b".to_vec());
        // Non-artifact keys (sidecar, origin claim) must be ignored by the diff.
        removed.insert("packages/pkgb/.origin", b"{}".to_vec());
        removed.insert(&format!("{}.meta.json", art("pkgb", "2.0")), b"{}".to_vec());

        let survivors: Vec<Arc<dyn Storage>> = vec![survivor.clone()];
        let unique = artifacts_unique_to_removed(removed.as_ref(), &survivors, 5)
            .await
            .unwrap();
        assert_eq!(unique, vec![art("pkgb", "2.0")]);

        // Once the survivor also holds it, nothing is sole-copy.
        survivor.insert(&art("pkgb", "2.0"), b"b".to_vec());
        let unique = artifacts_unique_to_removed(removed.as_ref(), &survivors, 5)
            .await
            .unwrap();
        assert!(unique.is_empty());
    }

    #[tokio::test]
    async fn removal_diff_accepts_presence_on_any_survivor() {
        let removed = Arc::new(InMemStorage::default());
        let s1 = Arc::new(InMemStorage::default());
        let s2 = Arc::new(InMemStorage::default());
        removed.insert(&art("pkga", "1.0"), b"a".to_vec());
        removed.insert(&art("pkgb", "1.0"), b"b".to_vec());
        // Each artifact survives on a different peer; neither is sole-copy.
        s1.insert(&art("pkga", "1.0"), b"a".to_vec());
        s2.insert(&art("pkgb", "1.0"), b"b".to_vec());

        let survivors: Vec<Arc<dyn Storage>> = vec![s1, s2];
        let unique = artifacts_unique_to_removed(removed.as_ref(), &survivors, 5)
            .await
            .unwrap();
        assert!(unique.is_empty());
    }

    #[tokio::test]
    async fn removal_diff_short_circuits_at_the_sample_cap() {
        let removed = Arc::new(InMemStorage::default());
        let survivor = Arc::new(InMemStorage::default());
        for i in 0..10 {
            removed.insert(&art(&format!("pkg{i:02}"), "1.0"), vec![i as u8]);
        }
        let survivors: Vec<Arc<dyn Storage>> = vec![survivor];
        let unique = artifacts_unique_to_removed(removed.as_ref(), &survivors, 3)
            .await
            .unwrap();
        assert_eq!(unique.len(), 3, "capped, not the full ten");
    }

    #[tokio::test]
    async fn removal_diff_propagates_survivor_error() {
        let removed = Arc::new(InMemStorage::default());
        removed.insert(&art("pkga", "1.0"), b"a".to_vec());
        // A survivor whose listing always fails must surface as an error, so the
        // caller refuses the drop rather than silently treating it as absent.
        let survivor = Arc::new(FailingList);
        let survivors: Vec<Arc<dyn Storage>> = vec![survivor];
        assert!(artifacts_unique_to_removed(removed.as_ref(), &survivors, 5)
            .await
            .is_err());
    }

    struct FailingList;

    #[async_trait::async_trait]
    impl Storage for FailingList {
        async fn head_exists(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }
        async fn serve_artifact(
            &self,
            _key: &str,
            _range: Option<&str>,
        ) -> Result<axum::response::Response<axum::body::Body>> {
            bail!("unused")
        }
        async fn presign_get(
            &self,
            _key: &str,
            _expires: std::time::Duration,
        ) -> Result<Option<String>> {
            Ok(None)
        }
        async fn put_bytes(
            &self,
            _key: &str,
            _bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }
        async fn put_if_absent(
            &self,
            _key: &str,
            _bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bool> {
            Ok(true)
        }
        async fn put_file_if_absent(
            &self,
            _key: &str,
            _path: &std::path::Path,
            _content_type: Option<&str>,
        ) -> Result<bool> {
            Ok(true)
        }
        async fn get_bytes(&self, _key: &str) -> Result<Vec<u8>> {
            bail!("unused")
        }
        async fn list_dir_entries(&self, _dir_prefix: &str) -> Result<Vec<FileEntry>> {
            Ok(Vec::new())
        }
        async fn list_all(&self, _prefix: &str) -> Result<Vec<ObjectMeta>> {
            bail!("injected list failure")
        }
        async fn delete_keys(&self, _keys: &[String]) -> Result<()> {
            Ok(())
        }
    }
}
