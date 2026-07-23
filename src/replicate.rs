//! Multi-bucket replication and reconciliation.
//!
//! Only **private truth** replicates — a private file's sidecar, artifact,
//! `.metadata`/`.provenance` companions, its package origin claim, and its
//! tombstone. Mirror content never does: it is re-derivable per bucket from
//! upstream (§4). Three tiers keep the buckets converged, fastest first, each
//! backstopping the one above:
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
use std::time::Instant;

use anyhow::{anyhow, bail, Context as _, Result};
use futures::StreamExt as _;
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
    frozen_key, metadata_key, mirror_quarantined_key, provenance_key, sidecar_key, tombstone_key,
    Sidecar, FROZEN_SUFFIX, MIRROR_QUARANTINED_SUFFIX, SIDECAR_SUFFIX, TOMBSTONE_SUFFIX,
};
use crate::status::{self, StatusConvergence};
use crate::storage::{
    bounded_artifact_write, create_artifact_verified, is_not_found, store_artifact_verified,
    verify_stored_size, ArtifactBody, Existing, Storage,
};
use crate::tombstone;
#[cfg(test)]
use crate::worker;

mod decide;
pub use decide::*;

/// Todo-marker prefix: `_repl/<dest-index>/<pkg>/<file>!<nonce>`, an empty
/// object in the bucket that took the write (the `_dirty/` idiom pointed at a
/// second bucket). O(1) at commit, consumed and deleted on a successful push.
const REPL_PREFIX: &str = "_repl/";
/// Frozen bodies land here, content-hash-suffixed, so both sides of a byte
/// conflict are preserved as moves (never deletes): `_quarantine/<pkg>/<file>@<sha12>`.
const QUARANTINE_PREFIX: &str = "_quarantine/";
/// Bound on the origin-CAS retry loop in [`ensure_private_origin`]; the same
/// rationale as [`origin`]'s own claim loop — a pathological storm fails closed.
const ORIGIN_ATTEMPTS: usize = 8;
/// Page size for the paged `_repl/` sweep and the reconcile package scan: one
/// S3 LIST page. Bounds resident memory so neither the failure backlog nor the
/// full package tree is ever held in one Vec.
const REPL_SWEEP_PAGE: usize = 1_000;
const RECONCILE_SCAN_PAGE: usize = 1_000;

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
) -> Result<()> {
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
        Verdict::Noop => (false, false),
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
        Verdict::AdoptSidecar(_) | Verdict::Freeze | Verdict::FinishFreeze | Verdict::Tombstone => {
            (true, true)
        }
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
        Verdict::Copy(side) => {
            let (src, dst, rec) = pick(side);
            if copy_live(state, src, dst, pkg, filename, rec).await? {
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
            supersede_record(state, src, dst, pkg, filename, record).await?;
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
            supersede_record(state, src, dst, pkg, filename, record).await?;
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
        Ok(())
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
            (Origin::Mirror, Origin::Mirror) => MergeChoice::Equal,
            (Origin::Private, Origin::Private) => yank_merge(&left, &right),
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

/// Fill missing companions for two records that already agree on private
/// artifact bytes. Private/private is a union; private/mirror only flows from
/// the private side. Mirror/mirror remains bucket-local. Returns which bucket's
/// index needs rebuilding.
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
            },
            RecordState::Live {
                sha: sha_b,
                origin: origin_b,
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
        (Origin::Mirror, Origin::Mirror) => Ok((false, false)),
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
    artifact: Vec<u8>,
    metadata: Option<Vec<u8>>,
    provenance: Option<Vec<u8>>,
}

async fn verify_source_record(
    src: &dyn Storage,
    pkg: &str,
    filename: &str,
    record: &Record,
) -> Result<VerifiedSource> {
    let sidecar = record
        .sidecar
        .as_ref()
        .ok_or_else(|| anyhow!("copy verdict with no source sidecar"))?;
    let akey = artifact_key(pkg, filename);
    let artifact = src
        .get_bytes(&akey)
        .await
        .with_context(|| format!("read source artifact {akey}"))?;
    let got = sha256_hex(&artifact);
    if got != sidecar.sha256 {
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
    let metadata = read_listed_companion(src, &metadata_key(&akey), record.has_metadata).await?;
    let provenance =
        read_listed_companion(src, &provenance_key(&akey), record.has_provenance).await?;
    Ok(VerifiedSource {
        artifact,
        metadata,
        provenance,
    })
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
) -> Result<bool> {
    let sc = record
        .sidecar
        .as_ref()
        .ok_or_else(|| anyhow!("copy verdict with no source sidecar"))?;
    let akey = artifact_key(pkg, filename);
    let verified = verify_source_record(src, pkg, filename, record).await?;
    require_replication_unfenced(state)?;

    // A destination body already sitting under this immutable key with the
    // wrong sha is stale crash debris — e.g. a zero-byte object a
    // 200-acked-but-failed write left behind (D2). Heal could never converge
    // while we bailed on it, so repair it in place with the sha-verified source
    // bytes; the conditional create below then dedups. A body that first
    // *appears during* this copy is still caught as a race at that create and
    // frozen, never silently overwritten.
    let mut changed = false;
    if dst.head_exists(&akey).await? {
        let current = dst
            .get_bytes(&akey)
            .await
            .with_context(|| format!("verify destination artifact {akey}"))?;
        if sha256_hex(&current) != sc.sha256 {
            store_artifact_verified(
                dst,
                &akey,
                ArtifactBody::Bytes(verified.artifact.clone()),
                verified.artifact.len() as u64,
                Some("application/octet-stream"),
                Existing::Repair(&sc.sha256),
            )
            .await?;
            changed = true;
        }
    }
    // Origin claim first, ahead of the artifact — shrinks the dependency-
    // confusion window (§4): the name is private before its bytes land.
    ensure_private_origin(dst, pkg).await?;
    // Sidecar first: an orphan sidecar is inert, but an orphan artifact would be
    // fabricated into truth by the destination's backfill (§4).
    changed |= install_or_verify_sidecar(dst, &sidecar_key(&akey), sc).await?;
    if let Some(bytes) = verified.metadata {
        changed |= put_if_absent_or_verify(
            dst,
            &metadata_key(&akey),
            bytes,
            Some("text/plain; charset=utf-8"),
        )
        .await?;
    }
    if let Some(bytes) = verified.provenance {
        changed |=
            put_if_absent_or_verify(dst, &provenance_key(&akey), bytes, Some("application/json"))
                .await?;
    }
    // Artifact last. Losing this conditional create must never be reported as
    // a copy: verify the winner. Same bytes converged; different bytes freeze
    // immediately so the source sidecar we installed cannot describe the
    // competing body even briefly after this operation returns. The create is
    // verified (D1) and bounded (D3) by the shared primitive.
    let len = verified.artifact.len() as u64;
    if create_artifact_verified(
        dst,
        &akey,
        verified.artifact,
        len,
        Some("application/octet-stream"),
    )
    .await?
    {
        state.metrics.record_replicated(len);
        return Ok(true);
    }
    let raced = dst
        .get_bytes(&akey)
        .await
        .with_context(|| format!("verify raced destination artifact {akey}"))?;
    let raced_sha = sha256_hex(&raced);
    if raced_sha == sc.sha256 {
        return Ok(changed);
    }
    freeze_copy_race(state, src, dst, pkg, filename, &sc.sha256, &raced_sha).await?;
    Ok(false)
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
) -> Result<()> {
    require_replication_unfenced(state)?;
    let sidecar = src_record
        .sidecar
        .as_ref()
        .ok_or_else(|| anyhow!("supersede verdict with no source sidecar"))?;
    if sidecar.origin.as_deref() != Some(PRIVATE) {
        bail!("supersede source for {pkg}/{filename} is not private truth");
    }
    let verified = verify_source_record(src, pkg, filename, src_record).await?;
    ensure_private_origin(dst, pkg).await?;
    let akey = artifact_key(pkg, filename);

    // A marker that raced the merge read has precedence. Preserve the incoming
    // private evidence, but never resurrect it through the fence.
    if dst.head_exists(&frozen_key(&akey)).await? {
        quarantine_bytes(dst, pkg, filename, &verified.artifact).await?;
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

    let mut artifact_present = false;
    if let Some((current, etag)) = dst.get_with_etag(&akey).await? {
        if sha256_hex(&current) == sidecar.sha256 {
            artifact_present = true;
        } else {
            quarantine_bytes(dst, pkg, filename, &current).await?;
            let len = verified.artifact.len() as u64;
            if bounded_artifact_write(
                &akey,
                len,
                dst.put_if_match(&akey, &etag, verified.artifact.clone()),
            )
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

    install_or_verify_sidecar(dst, &sidecar_key(&akey), sidecar).await?;
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

    if !artifact_present {
        let len = verified.artifact.len() as u64;
        if create_artifact_verified(
            dst,
            &akey,
            verified.artifact.clone(),
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
            if raced_sha != sidecar.sha256 {
                freeze_copy_race(state, src, dst, pkg, filename, &sidecar.sha256, &raced_sha)
                    .await?;
                return Ok(());
            }
        }
    }

    // A delete/freeze can race the publish. Reassert precedence after the
    // complete record lands, then clear obsolete mirror quarantine state only
    // when private truth remains live.
    if dst.head_exists(&frozen_key(&akey)).await? {
        quarantine_bytes(dst, pkg, filename, &verified.artifact).await?;
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

async fn quarantine_bytes(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    bytes: &[u8],
) -> Result<String> {
    let sha = sha256_hex(bytes);
    let short = &sha[..sha.len().min(12)];
    let qkey = format!("{QUARANTINE_PREFIX}{pkg}/{filename}@{short}");
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

/// Preserve a mirror loser and mark its canonical record inert without
/// deleting the artifact key. Keeping the old body in place avoids the
/// delete/recreate ABA that an ETag cannot distinguish when the bytes match.
async fn quarantine_mirror_record(
    storage: &dyn Storage,
    pkg: &str,
    filename: &str,
    bytes: &[u8],
) -> Result<bool> {
    let sha256 = quarantine_bytes(storage, pkg, filename, bytes).await?;
    #[derive(serde::Serialize)]
    struct Marker<'a> {
        filename: &'a str,
        sha256: &'a str,
    }
    let key = mirror_quarantined_key(&artifact_key(pkg, filename));
    put_if_absent_or_verify(
        storage,
        &key,
        serde_json::to_vec(&Marker {
            filename,
            sha256: &sha256,
        })?,
        json(),
    )
    .await
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
        // orphaned beside the tombstone. Finish the job — otherwise `decide`
        // re-fires Tombstone on every diff and this early return would starve
        // the cleanup forever. Report change only when debris existed.
        let (sidecar_left, metadata_left, provenance_left) = futures::future::try_join3(
            storage.head_exists(&sidecar_key(&akey)),
            storage.head_exists(&metadata_key(&akey)),
            storage.head_exists(&provenance_key(&akey)),
        )
        .await?;
        if !sidecar_left && !metadata_left && !provenance_left {
            return Ok(false);
        }
        storage
            .delete_keys(&[
                sidecar_key(&akey),
                metadata_key(&akey),
                provenance_key(&akey),
            ])
            .await?;
        return Ok(true);
    }
    tombstone::write(storage, &akey, filename).await?;
    drop_record_objects(storage, pkg, filename).await?;
    Ok(true)
}

/// Remove a filename's artifact + sidecar + companions after its durable
/// tombstone/freeze/quarantine record is in place. Errors propagate so the
/// marker remains and the next sweep retries cleanup.
async fn drop_record_objects(storage: &dyn Storage, pkg: &str, filename: &str) -> Result<()> {
    let akey = artifact_key(pkg, filename);
    storage
        .delete_keys(&[
            akey.clone(),
            sidecar_key(&akey),
            metadata_key(&akey),
            provenance_key(&akey),
        ])
        .await
}

/// Audit repair for an impossible within-bucket state: a package claim is
/// private, yet one or more live artifacts still carry mirror sidecars. Preserve
/// each body under its actual content hash, then remove the live record without
/// tombstoning it. The caller rebuilds the package index after a non-zero count.
pub async fn quarantine_mirror_artifacts(storage: &dyn Storage, pkg: &str) -> Result<usize> {
    let Some(claim) = read_origin_observation(storage, pkg).await? else {
        return Ok(0);
    };
    if claim.state != OriginState::Private {
        return Ok(0);
    }
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let entries = storage.list_dir_entries(&prefix).await?;
    let names: HashSet<String> = entries.iter().map(|entry| entry.key.clone()).collect();
    let mut quarantined = 0;
    for entry in entries {
        let Some(filename) = entry.key.strip_prefix(&prefix) else {
            continue;
        };
        if !crate::sidecar::is_artifact(filename) {
            continue;
        }
        if names.contains(&mirror_quarantined_key(&entry.key)) {
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
        let bytes = match storage.get_bytes(&entry.key).await {
            Ok(bytes) => bytes,
            Err(e) if is_not_found(&e) => continue,
            Err(e) => return Err(e),
        };
        if quarantine_mirror_record(storage, pkg, filename, &bytes).await? {
            quarantined += 1;
        }
    }
    Ok(quarantined)
}

// ---------------------------------------------------------------------------
// Tier 1 — synchronous fan-out (pre-ack).
// ---------------------------------------------------------------------------

/// Stream a just-committed private record from the selected bucket to every
/// other bucket *before* the client ack. Healthy secondaries are copied
/// concurrently — each via the same
/// [`replicate_record`] copy protocol as the sweep and full diff (origin claim,
/// sidecar, companions, then the sha256-verified artifact last) — under one
/// shared grace deadline measured from the selected write's completion.
///
/// A secondary that fails, exceeds the grace deadline, becomes topology-
/// ineligible mid-copy, or is already ineligible (so no copy is attempted) gets
/// a durable `_repl/<dest>/…` note in the selected bucket before this returns.
/// Notes are the failure path only: a healthy fleet acks with every bucket
/// holding the record and no note written. A single-bucket node does no I/O.
pub async fn fanout_sync(state: &AppState, pinned: &Pinned, pkg: &str, filename: &str) {
    if !state.buckets.is_multi() {
        return;
    }
    let src = pinned.storage.as_ref();
    let src_index = pinned.index;
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
                    result = replicate_record(state, src, handle.storage.as_ref(), pkg, filename) => result,
                    _ = tokio::time::sleep_until(deadline) => {
                        Err(anyhow!("fan-out to {} exceeded the grace deadline", handle.name))
                    }
                    _ = wait_until_pair_ineligible(state, src_index, idx) => {
                        Err(anyhow!("source or destination became topology-ineligible"))
                    }
                };
                match result {
                    Ok(()) => (idx, true),
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
    for (idx, converged) in futures::future::join_all(jobs).await {
        if converged {
            continue;
        }
        if let Err(e) = write_marker(src, idx, pkg, filename).await {
            error!(dest=idx, package=%pkg, filename=%filename, error=?e, "could not write replication repair note before ack");
        }
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
/// its own leader rebuilds). Any other pairing (already agreeing, or neither
/// private) is left untouched.
async fn reconcile_split_origin(
    a: &dyn Storage,
    b: &dyn Storage,
    pkg: &str,
    mut a_origin: Option<Origin>,
    mut b_origin: Option<Origin>,
) -> Result<SplitOriginReconciled> {
    let (mut scanned_a, mut scanned_b) = (false, false);
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
) -> Result<()> {
    require_replication_unfenced(state)?;
    let src_origin = read_pkg_origin(src, pkg).await?;
    let dst_origin = read_pkg_origin(dst, pkg).await?;
    let SplitOriginReconciled {
        a_origin: src_origin,
        b_origin: dst_origin,
        ..
    } = reconcile_split_origin(src, dst, pkg, src_origin, dst_origin).await?;

    if filename == ORIGIN_MARKER {
        return Ok(());
    }
    if filename == PROJECT_STATUS_MARKER {
        if src_origin == Some(Origin::Private) || dst_origin == Some(Origin::Private) {
            reconcile_project_status(src, dst, pkg).await?;
        }
        return Ok(());
    }
    let a = read_record(src, pkg, filename).await?;
    let b = read_record(dst, pkg, filename).await?;
    let verdict = decide(&a, &b);
    execute(state, (src, dst), pkg, filename, (&a, &b), verdict).await
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
    dest: usize,
    pkg: &str,
    filename: &str,
) -> Result<String> {
    let key = format!(
        "{REPL_PREFIX}{dest}/{pkg}/{filename}!{}",
        markers::marker_nonce()
    );
    storage.put_bytes(&key, Vec::new(), None).await?;
    Ok(key)
}

struct ReplMarker {
    dest: usize,
    pkg: String,
    filename: String,
    key: String,
}

/// Parse `_repl/<dest>/<pkg>/<file>!<nonce>`. Package names and filenames carry
/// no `!`, and the nonce carries no `/`, so the split is unambiguous.
fn parse_repl_marker(key: &str) -> Option<ReplMarker> {
    let rest = key.strip_prefix(REPL_PREFIX)?;
    let (dest, rest) = rest.split_once('/')?;
    let dest = dest.parse::<usize>().ok()?;
    let (pkg, file_nonce) = rest.split_once('/')?;
    let (filename, _nonce) = file_nonce.rsplit_once('!')?;
    Some(ReplMarker {
        dest,
        pkg: pkg.to_string(),
        filename: filename.to_string(),
        key: key.to_string(),
    })
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

/// Whether a bucket holds any undrained `_repl/` repair note. A bounded
/// existence check (one paged LIST capped at a single key), not a count: used by
/// `buckets migrate` to refuse shrinking/reordering the topology while a repair
/// note — potentially the sole copy of a record not yet replicated — is still
/// stranded.
pub async fn has_undrained_repl_notes(storage: &dyn Storage) -> Result<bool> {
    Ok(!storage.list_page(REPL_PREFIX, None, 1).await?.is_empty())
}

/// Whether this bucket holds any undrained `_repl/<dest>/` note — a repair still
/// owed *to* bucket `dest`. The read-affinity worker checks every other bucket
/// with this before it lets reads return to `dest` (its region bucket): an
/// outstanding note means `dest` is missing an acked file, so reads stay on the
/// write bucket until it drains. Same bounded
/// single-key LIST as [`has_undrained_repl_notes`].
pub async fn has_undrained_repl_notes_for(storage: &dyn Storage, dest: usize) -> Result<bool> {
    let prefix = format!("{REPL_PREFIX}{dest}/");
    Ok(!storage.list_page(&prefix, None, 1).await?.is_empty())
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
            if handles.get(marker.dest).is_none() {
                // Destination no longer configured — the marker cannot be
                // delivered; drop it rather than retry forever.
                let _ = src.delete_keys(&[marker.key]).await;
                continue;
            }
            by_destination.entry(marker.dest).or_default().push(marker);
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
                                    ) => result,
                                    _ = wait_until_pair_ineligible(state, src_index, dest_index) => {
                                        Err(anyhow!("source or destination became topology-ineligible"))
                                    }
                                };
                                match result {
                                    Ok(()) => {
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
    for _ in 0..2 {
        for (index, handle) in handles.iter().enumerate() {
            if index == pinned.index || !bucket_eligible(state, index) {
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
            }
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
            // private source. Quarantine it here; ordinary `decide`
            // deliberately treats mirror-only cache entries as local.
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
            execute(state, (a, b), pkg, &filename, (&ra, &rb), verdict).await?;
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

    struct RaceOnCreateStorage {
        inner: InMemStorage,
        key: String,
        bytes: Vec<u8>,
        required_prior_key: Option<String>,
        raced: AtomicBool,
    }

    impl RaceOnCreateStorage {
        fn new(key: String, bytes: Vec<u8>) -> Self {
            Self {
                inner: InMemStorage::default(),
                key,
                bytes,
                required_prior_key: None,
                raced: AtomicBool::new(false),
            }
        }

        fn requiring_prior_key(mut self, key: String) -> Self {
            self.required_prior_key = Some(key);
            self
        }
    }

    #[async_trait::async_trait]
    impl Storage for RaceOnCreateStorage {
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
            if key == self.key && !self.raced.swap(true, Ordering::SeqCst) {
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
            self.inner.get_bytes(key).await
        }

        async fn list_dir_entries(&self, prefix: &str) -> Result<Vec<FileEntry>> {
            self.inner.list_dir_entries(prefix).await
        }

        async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
            self.inner.list_all(prefix).await
        }

        async fn delete_keys(&self, keys: &[String]) -> Result<()> {
            self.inner.delete_keys(keys).await
        }

        fn supports_leases(&self) -> bool {
            true
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

        execute(
            &state,
            (a.as_ref(), b.as_ref()),
            "pkg",
            filename,
            (&left, &right),
            verdict,
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

        replicate_record(&state, a.as_ref(), b.as_ref(), "pkg", filename)
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

        let err = copy_live(&state, &src, dst.as_ref(), "pkg", filename, &record)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("source artifact sha mismatch"));
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

        let err = replicate_record(&state, src.as_ref(), dst.as_ref(), "pkg", filename)
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

        replicate_record(&state, src.as_ref(), dst.as_ref(), "pkg", filename)
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
        let dst = Arc::new(RaceOnCreateStorage::new(key.clone(), raced_bytes.to_vec()));
        let state = test_state(src.clone());
        let record = live(&sha256_hex(source_bytes), PRIVATE);

        assert!(
            !copy_live(&state, src.as_ref(), dst.as_ref(), "pkg", filename, &record,)
                .await
                .unwrap()
        );
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
        let dst = Arc::new(RaceOnCreateStorage::new(key.clone(), bytes.to_vec()));
        let state = test_state(src.clone());
        let record = live(&sha256_hex(bytes), PRIVATE);

        assert!(
            copy_live(&state, src.as_ref(), dst.as_ref(), "pkg", filename, &record,)
                .await
                .unwrap()
        );
        assert_eq!(dst.get_bytes(&key).await.unwrap(), bytes);
        let sidecar: Sidecar =
            serde_json::from_slice(&dst.get_bytes(&sidecar_key(&key)).await.unwrap()).unwrap();
        assert_eq!(sidecar.sha256, sha256_hex(bytes));
        assert!(!dst.head_exists(&frozen_key(&key)).await.unwrap());
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

        replicate_record(&state, a.as_ref(), b.as_ref(), "pkg", filename)
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
        execute(
            &state,
            (a.as_ref(), b.as_ref()),
            "pkg",
            filename,
            (&settled_a, &settled_b),
            Verdict::Noop,
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
        let storage = RaceOnCreateStorage::new(tombstone_key(&key), b"raced fence".to_vec())
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
            RaceOnCreateStorage::new(qkey, bytes.to_vec()).requiring_prior_key(frozen_key(&key));
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
        let storage = RaceOnCreateStorage::new(qkey.clone(), b"collision".to_vec())
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
        assert!(storage
            .head_exists(&artifact_key("pkg", filename))
            .await
            .unwrap());
        assert!(storage
            .head_exists(&mirror_quarantined_key(&artifact_key("pkg", filename)))
            .await
            .unwrap());
        assert_eq!(storage.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 1);
        let (rendered, _) = worker::list_artifacts(&storage, "pkg").await.unwrap();
        assert!(rendered.is_empty());
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

        replicate_record(
            &state,
            private.as_ref(),
            late_mirror.as_ref(),
            "pkg",
            filename,
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
        execute(
            &state,
            (private.as_ref(), mirror.as_ref()),
            "pkg",
            filename,
            (&source, &destination),
            verdict,
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

        assert!(destination
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

        fanout_sync(&state, &pinned, "pkg", filename).await;
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

        fanout_sync(&state, &pinned, "pkg", filename).await;
        // No attempt against an ineligible bucket, but a durable note is left so
        // the record reaches B when it heals.
        assert!(!b.head_exists(&artifact_key("pkg", filename)).await.unwrap());
        assert!(a
            .list_all(&format!("{REPL_PREFIX}1/"))
            .await
            .unwrap()
            .iter()
            .any(|m| m.key.contains("/pkg/")));
    }

    #[tokio::test]
    async fn all_bucket_sweep_drains_a_non_selected_source() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(b.as_ref(), "pkg", filename, b"straggler", PRIVATE);
        let state = two_bucket_state(a.clone(), b.clone());
        write_marker(b.as_ref(), 0, "pkg", filename).await.unwrap();

        sweep_all_markers(&state).await.unwrap();
        assert!(a.head_exists(&artifact_key("pkg", filename)).await.unwrap());
        assert!(b.list_all(REPL_PREFIX).await.unwrap().is_empty());
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
        write_marker(a.as_ref(), 1, "from-a", from_a).await.unwrap();
        write_marker(b.as_ref(), 0, "from-b", from_b).await.unwrap();

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

        replicate_record(&state, a.as_ref(), b.as_ref(), "pkg", PROJECT_STATUS_MARKER)
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

        replicate_record(&state, a.as_ref(), b.as_ref(), "pkg", PROJECT_STATUS_MARKER)
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
}
