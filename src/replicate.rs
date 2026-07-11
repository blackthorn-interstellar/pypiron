//! Multi-bucket replication and reconciliation (dev/MULTIBUCKET.md §4, §6).
//!
//! Only **private truth** replicates — a private file's sidecar, artifact,
//! `.metadata`/`.provenance` companions, its package origin claim, and its
//! tombstone. Mirror content never does: it is re-derivable per bucket from
//! upstream (§4). Three tiers keep the buckets converged, fastest first, each
//! backstopping the one above:
//!
//! 1. **Eager fan-out** ([`spawn_eager_with_markers`]): after an
//!    upload/delete/yank commits, durably queue markers and push the changed
//!    record to every other bucket.
//! 2. **`_repl/` todo markers**: any failed eager push drops a marker in the
//!    bucket that took the write; nodes sweep them each tick
//!    ([`sweep_all_markers`]).
//! 3. **Full diff** ([`reconcile`]): a pairwise tree diff as the backstop for
//!    lost markers — the same copy path, with the merge rules armed.
//!
//! Every function here takes explicit `(source, destination)` storage handles:
//! this is the one sanctioned two-handle operation (§3 invariant 2). The merge
//! decision ([`decide`]) is a pure, symmetric, clock-free function so any two
//! buckets reconciled by any node in any order reach the same result; the
//! executor applies it with both handles.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context as _, Result};
use futures::StreamExt as _;
use sha2::{Digest, Sha256};
use tracing::{error, warn};

use crate::buckets::Pinned;
#[cfg(test)]
use crate::origin::read_origin;
use crate::origin::{
    begin_private_promotion, claim_origin, finish_private_promotion, read_origin_observation,
    ClaimRequest, OriginState, MIRROR, PRIVATE,
};
use crate::sidecar::{
    frozen_key, metadata_key, mirror_quarantined_key, provenance_key, sidecar_key, tombstone_key,
    Sidecar, Yanked, FROZEN_SUFFIX, MIRROR_QUARANTINED_SUFFIX, SIDECAR_SUFFIX, TOMBSTONE_SUFFIX,
};
use crate::status::{self, StatusConvergence};
use crate::storage::{is_not_found, ObjectMeta, Storage};
use crate::tombstone;
use crate::worker;
use crate::{AppState, PACKAGES_PREFIX};

/// Todo-marker prefix: `_repl/<dest-index>/<pkg>/<file>!<nonce>`, an empty
/// object in the bucket that took the write (the `_dirty/` idiom pointed at a
/// second bucket). O(1) at commit, consumed and deleted on a successful push.
const REPL_PREFIX: &str = "_repl/";
/// Completed package-demotion stages live here until promotion succeeds. A
/// manifest is the commit marker: without it, partial stage objects are inert.
const REPL_STAGING_PREFIX: &str = "_staging/repl/";
const PROMOTION_LOCK_NAME: &str = ".promotion-lock";
const MIN_PROMOTION_LOCK_GRACE: std::time::Duration = std::time::Duration::from_millis(30);
/// Frozen bodies land here, content-hash-suffixed, so both sides of a byte
/// conflict are preserved as moves (never deletes): `_quarantine/<pkg>/<file>@<sha12>`.
const QUARANTINE_PREFIX: &str = "_quarantine/";
/// Bound on the origin-CAS retry loop in [`ensure_private_origin`]; the same
/// rationale as [`origin`]'s own claim loop — a pathological storm fails closed.
const ORIGIN_ATTEMPTS: usize = 8;

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

struct ReplicationIntents {
    left: String,
    right: String,
    current: tokio::sync::Mutex<(String, String)>,
    heartbeat_seq: std::sync::atomic::AtomicU64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum PromotionLockBody {
    Free {
        nonce: String,
    },
    Held {
        holder: String,
        manifest: String,
        nonce: String,
    },
}

struct PromotionLease {
    key: String,
    holder: String,
    manifest: String,
    etag: tokio::sync::Mutex<String>,
}

impl PromotionLease {
    fn held_body(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&PromotionLockBody::Held {
            holder: self.holder.clone(),
            manifest: self.manifest.clone(),
            nonce: worker::marker_nonce(),
        })?)
    }

    async fn require(&self, storage: &dyn Storage) -> Result<()> {
        let Some((bytes, _)) = storage.get_with_etag(&self.key).await? else {
            bail!("promotion lock '{}' vanished", self.key)
        };
        let body: PromotionLockBody = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse promotion lock {}", self.key))?;
        if !matches!(
            body,
            PromotionLockBody::Held { holder, manifest, .. }
                if holder == self.holder && manifest == self.manifest
        ) {
            bail!("promotion lock '{}' changed owner", self.key)
        }
        Ok(())
    }

    async fn renew(&self, storage: &dyn Storage) -> Result<()> {
        let mut expected_etag = self.etag.lock().await;
        let Some(next_etag) = storage
            .put_if_match(&self.key, &expected_etag, self.held_body()?)
            .await?
        else {
            bail!("promotion lock '{}' was lost during heartbeat", self.key)
        };
        *expected_etag = next_etag;
        Ok(())
    }

    async fn release(&self, storage: &dyn Storage) -> Result<()> {
        let expected_etag = self.etag.lock().await.clone();
        let free = serde_json::to_vec(&PromotionLockBody::Free {
            nonce: worker::marker_nonce(),
        })?;
        if storage
            .put_if_match(&self.key, &expected_etag, free)
            .await?
            .is_none()
        {
            bail!("promotion lock '{}' changed before release", self.key)
        }
        Ok(())
    }
}

fn promotion_lock_key(pkg: &str) -> String {
    format!("{REPL_STAGING_PREFIX}{pkg}/{PROMOTION_LOCK_NAME}")
}

fn storage_identity(storage: &dyn Storage) -> usize {
    std::ptr::from_ref(storage).cast::<()>() as usize
}

fn intent_grace_std(state: &AppState) -> std::time::Duration {
    std::time::Duration::try_from(state.intent_grace).unwrap_or_default()
}

fn promotion_lock_grace(state: &AppState) -> std::time::Duration {
    intent_grace_std(state).max(MIN_PROMOTION_LOCK_GRACE)
}

async fn acquire_promotion_lock(
    state: &AppState,
    storage: &dyn Storage,
    staged: &StagedPackage,
    holder: &str,
) -> Result<Option<PromotionLease>> {
    let key = promotion_lock_key(&staged.manifest.package);
    let observation_key = (storage_identity(storage), key.clone());
    for _ in 0..ORIGIN_ATTEMPTS {
        let held = || {
            serde_json::to_vec(&PromotionLockBody::Held {
                holder: holder.to_string(),
                manifest: staged.manifest_key.clone(),
                nonce: worker::marker_nonce(),
            })
        };
        let Some((bytes, etag)) = storage.get_with_etag(&key).await? else {
            if let Some(etag) = storage.put_if_none_match(&key, held()?).await? {
                state
                    .promotion_lock_observations
                    .lock()
                    .await
                    .remove(&observation_key);
                return Ok(Some(PromotionLease {
                    key,
                    holder: holder.to_string(),
                    manifest: staged.manifest_key.clone(),
                    etag: tokio::sync::Mutex::new(etag),
                }));
            }
            continue;
        };
        let body: PromotionLockBody = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse promotion lock {key}"))?;
        let replace = match body {
            PromotionLockBody::Free { .. } => true,
            PromotionLockBody::Held {
                holder: current,
                manifest,
                ..
            } if current == holder && manifest == staged.manifest_key => {
                state
                    .promotion_lock_observations
                    .lock()
                    .await
                    .remove(&observation_key);
                return Ok(Some(PromotionLease {
                    key,
                    holder: holder.to_string(),
                    manifest: staged.manifest_key.clone(),
                    etag: tokio::sync::Mutex::new(etag),
                }));
            }
            PromotionLockBody::Held {
                holder: current, ..
            } => {
                let unchanged_for_grace = {
                    let mut observations = state.promotion_lock_observations.lock().await;
                    match observations.get_mut(&observation_key) {
                        Some((observed_etag, first_seen)) if observed_etag == &etag => {
                            first_seen.elapsed() >= promotion_lock_grace(state)
                        }
                        Some(observed) => {
                            *observed = (etag.clone(), std::time::Instant::now());
                            false
                        }
                        None => {
                            observations.insert(
                                observation_key.clone(),
                                (etag.clone(), std::time::Instant::now()),
                            );
                            false
                        }
                    }
                };
                if !unchanged_for_grace
                    || worker::specific_intent_is_live(
                        state,
                        storage,
                        &staged.manifest.package,
                        &current,
                    )
                    .await?
                {
                    return Ok(None);
                }
                true
            }
        };
        if replace {
            if let Some(next_etag) = storage.put_if_match(&key, &etag, held()?).await? {
                state
                    .promotion_lock_observations
                    .lock()
                    .await
                    .remove(&observation_key);
                return Ok(Some(PromotionLease {
                    key,
                    holder: holder.to_string(),
                    manifest: staged.manifest_key.clone(),
                    etag: tokio::sync::Mutex::new(next_etag),
                }));
            }
        }
    }
    bail!(
        "could not acquire promotion lock for '{}'",
        staged.manifest.package
    )
}

async fn run_with_promotion_heartbeat<T>(
    state: &AppState,
    storage: &dyn Storage,
    lease: &PromotionLease,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let period = promotion_lock_grace(state) / 3;
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    tokio::pin!(operation);
    let outcome = loop {
        tokio::select! {
            result = &mut operation => break result,
            _ = heartbeat.tick() => {
                if let Err(error) = lease.renew(storage).await {
                    break Err(error.context("renew staged-promotion lock"));
                }
            }
        }
    };
    let released = lease.release(storage).await;
    match (outcome, released) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error.context("release staged-promotion lock")),
        (Err(error), Err(release)) => Err(error.context(format!(
            "release staged-promotion lock also failed: {release:#}"
        ))),
    }
}

async fn acquire_replication_intents(
    left: &dyn Storage,
    right: &dyn Storage,
    pkg: &str,
) -> Result<ReplicationIntents> {
    let left_nonce = worker::mark_intent(left, pkg).await?;
    match worker::mark_intent(right, pkg).await {
        Ok(right_nonce) => Ok(ReplicationIntents {
            left: left_nonce.clone(),
            right: right_nonce.clone(),
            current: tokio::sync::Mutex::new((left_nonce, right_nonce)),
            heartbeat_seq: std::sync::atomic::AtomicU64::new(0),
        }),
        Err(error) => {
            let _ = worker::clear_intent(left, pkg, &left_nonce).await;
            Err(error)
        }
    }
}

async fn release_replication_intents(
    left: &dyn Storage,
    right: &dyn Storage,
    pkg: &str,
    intents: &ReplicationIntents,
) -> Result<()> {
    let (left_current, right_current) = intents.current.lock().await.clone();
    let (left_result, right_result) = futures::future::join(
        worker::clear_intent(left, pkg, &left_current),
        worker::clear_intent(right, pkg, &right_current),
    )
    .await;
    left_result?;
    right_result
}

async fn commit_replication_intents(
    left: &dyn Storage,
    right: &dyn Storage,
    pkg: &str,
    intents: &ReplicationIntents,
) {
    let (left_current, right_current) = intents.current.lock().await.clone();
    let _ = futures::future::join(
        worker::mark_commit(left, pkg, &left_current),
        worker::mark_commit(right, pkg, &right_current),
    )
    .await;
}

async fn run_with_replication_heartbeats<T>(
    state: &AppState,
    left: &dyn Storage,
    right: &dyn Storage,
    pkg: &str,
    intents: &ReplicationIntents,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let period = (intent_grace_std(state) / 3).max(std::time::Duration::from_millis(10));
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    tokio::pin!(operation);
    loop {
        tokio::select! {
            result = &mut operation => return result,
            _ = heartbeat.tick() => {
                let seq = intents
                    .heartbeat_seq
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let next_left = format!("{}~{seq}", intents.left);
                let next_right = format!("{}~{seq}", intents.right);
                worker::mark_intent_with_nonce(left, pkg, &next_left)
                    .await
                    .context("create left replication heartbeat intent")?;
                if let Err(error) = worker::mark_intent_with_nonce(right, pkg, &next_right).await {
                    let _ = worker::mark_commit(left, pkg, &next_left).await;
                    return Err(error.context("create right replication heartbeat intent"));
                }
                let (old_left, old_right) = {
                    let mut current = intents.current.lock().await;
                    let old = current.clone();
                    *current = (next_left, next_right);
                    old
                };
                futures::future::try_join(
                    worker::mark_commit(left, pkg, &old_left),
                    worker::mark_commit(right, pkg, &old_right),
                )
                .await
                .context("close prior replication heartbeat intents")?;
            }
        }
    }
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

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

// ---------------------------------------------------------------------------
// Merge algebra — pure, symmetric, clock-free (dev/MULTIBUCKET.md §6).
// Precedence: tombstone ≻ origin (private ≻ mirror) ≻ union ≻ freeze.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Origin {
    Private,
    Mirror,
}

impl Origin {
    fn parse(s: &str) -> Option<Origin> {
        match s {
            PRIVATE => Some(Origin::Private),
            MIRROR => Some(Origin::Mirror),
            _ => None,
        }
    }
}

/// One bucket's view of a single filename in a package: which of its objects
/// exist, and (if readable) the sidecar that carries sha256/origin/yank state.
#[derive(Clone, Debug)]
struct Record {
    sidecar: Option<Sidecar>,
    has_artifact: bool,
    has_metadata: bool,
    has_provenance: bool,
    tombstoned: bool,
    frozen: bool,
    mirror_quarantined: bool,
    /// Package-level origin, used only as a fallback when a live artifact's
    /// sidecar omits its own `origin` (a legacy/backfilled record).
    pkg_origin: Option<Origin>,
}

/// The normalized state a [`Record`] resolves to for the merge.
#[derive(Clone, PartialEq, Eq, Debug)]
enum RecordState {
    Tombstoned,
    Frozen,
    /// Canonical mirror bytes deliberately retained behind a quarantine
    /// marker. They are absent for ordinary union, but a private peer may
    /// supersede them through package staging.
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
    fn origin(&self) -> Option<Origin> {
        self.sidecar
            .as_ref()
            .and_then(|s| s.origin.as_deref())
            .and_then(Origin::parse)
            .or(self.pkg_origin)
    }

    fn state(&self) -> RecordState {
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
enum Side {
    A,
    B,
}

/// The two-sided decision for one filename. Symmetric: `decide(a, b)` and
/// `decide(b, a)` name the same physical outcome (with the side swapped), so a
/// bidirectional diff cannot double-apply.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Verdict {
    Noop,
    /// The `Side`'s live private record is copied to the other (absent) side.
    Copy(Side),
    /// Same bytes; the `Side`'s sidecar wins the yank/origin merge — overwrite
    /// the other side's sidecar (and make it private if the winner is).
    AdoptSidecar(Side),
    /// The `Side` is private, the other is a *different-byte* mirror: private
    /// wins — quarantine the mirror body and copy the private record over it.
    Supersede(Side),
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

/// The core merge decision (dev/MULTIBUCKET.md §6). No I/O, no clocks; every
/// input is bucket state. Unit-tested exhaustively below.
fn decide(a: &Record, b: &Record) -> Verdict {
    // Tombstone ≻ everything. Converged (both tombstoned, no live body) is a
    // no-op so a settled delete never re-fires each diff.
    if a.tombstoned || b.tombstoned {
        // Frozen canonical bodies are deliberately retained behind their
        // durable markers. They are evidence, not live truth, so a settled
        // fleet must not loop forever trying to delete them.
        let frozen_settled = a.frozen && b.frozen;
        if a.tombstoned && b.tombstoned && (frozen_settled || (!a.has_artifact && !b.has_artifact))
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
                    (Origin::Private, Origin::Private) => Verdict::Freeze,
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

/// Both sides hold the same bytes. Origin precedence (private ≻ mirror) first,
/// then the yank merge (§6.5). Adopt the winner's sidecar wholesale.
fn same_bytes(a: &Record, b: &Record, oa: Origin, ob: Origin) -> Verdict {
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
enum MergeChoice {
    A,
    B,
    Equal,
}

fn is_yanked(sc: &Sidecar) -> bool {
    !matches!(sc.yanked.normalized(), Yanked::Flag(false))
}

/// Yank merge (dev/MULTIBUCKET.md §6.5): max epoch wins; on an equal epoch a
/// conflicting state resolves to yanked (fail-closed); a residual tie (both
/// yanked, different reasons) breaks on the lexicographically smaller sidecar
/// sha256. Never a wall clock — two buckets have two clocks.
fn yank_merge(a: &Sidecar, b: &Sidecar) -> MergeChoice {
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
    if let Some(owner) = observed.pending_manifest {
        bail!("package '{pkg}' is under staged promotion by '{owner}'");
    }
    if observed.state == OriginState::Unclaimed {
        return Ok(None);
    }
    Origin::parse(observed.state.as_str())
        .map(Some)
        .ok_or_else(|| {
            anyhow!(
                "origin claim for '{pkg}' holds an unexpected value '{}'",
                observed.state.as_str()
            )
        })
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
async fn read_record(storage: &dyn Storage, pkg: &str, filename: &str) -> Result<Record> {
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
async fn execute(
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
    let pick = |side: Side| -> (&dyn Storage, &dyn Storage, &Record) {
        match side {
            Side::A => (a, b, ra),
            Side::B => (b, a, rb),
        }
    };
    match verdict {
        Verdict::Noop => {
            let (dirty_a, dirty_b) =
                repair_same_sha_companions((a, b), pkg, filename, (ra, rb)).await?;
            if dirty_a {
                worker::mark_dirty(a, pkg).await?;
            }
            if dirty_b {
                worker::mark_dirty(b, pkg).await?;
            }
            // A process may crash after durable freeze/tombstone markers but
            // before the outer freeze path queues its index rebuild. Reassert
            // the cheap derived-view event on every later frozen no-op.
            if ra.frozen {
                worker::mark_dirty(a, pkg).await?;
            }
            if rb.frozen {
                worker::mark_dirty(b, pkg).await?;
            }
        }
        Verdict::Copy(side) => {
            let (src, dst, rec) = pick(side);
            if copy_live(state, src, dst, pkg, filename, rec).await? {
                worker::mark_dirty(dst, pkg).await?;
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
                worker::mark_dirty(a, pkg).await?;
            }
            if adopted_b || dirty_b {
                worker::mark_dirty(b, pkg).await?;
            }
        }
        Verdict::Supersede(_) => bail!(
            "private-over-mirror supersede for {pkg}/{filename} reached the per-file executor; package staging must run first"
        ),
        Verdict::Freeze => {
            freeze_side(a, pkg, filename).await?;
            freeze_side(b, pkg, filename).await?;
            worker::mark_dirty(a, pkg).await?;
            worker::mark_dirty(b, pkg).await?;
            state
                .metrics
                .replication_freezes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            error!(
                package = %pkg,
                filename = %filename,
                sha_a = ra.sidecar.as_ref().map(|s| s.sha256.as_str()).unwrap_or("?"),
                sha_b = rb.sidecar.as_ref().map(|s| s.sha256.as_str()).unwrap_or("?"),
                "byte conflict: same filename, different bytes on two buckets — frozen on both, quarantined, suppressed from indexes; resolve by republishing a new version"
            );
        }
        Verdict::PropagateFreeze(frozen_side) => {
            // Freeze the side that lacks the marker.
            let target: &dyn Storage = match frozen_side {
                Side::A => b,
                Side::B => a,
            };
            freeze_side(target, pkg, filename).await?;
            worker::mark_dirty(target, pkg).await?;
        }
        Verdict::FinishFreeze => {
            freeze_side(a, pkg, filename).await?;
            freeze_side(b, pkg, filename).await?;
            worker::mark_dirty(a, pkg).await?;
            worker::mark_dirty(b, pkg).await?;
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
                worker::mark_dirty(a, pkg).await?;
            }
            if cb {
                worker::mark_dirty(b, pkg).await?;
            }
        }
    }
    Ok(())
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

async fn put_if_absent_or_verify(
    storage: &dyn Storage,
    key: &str,
    bytes: Vec<u8>,
    content_type: Option<&str>,
) -> Result<bool> {
    if storage
        .put_if_absent(key, bytes.clone(), content_type)
        .await?
    {
        return Ok(true);
    }
    let current = storage
        .get_bytes(key)
        .await
        .with_context(|| format!("verify concurrently-created object {key}"))?;
    if current != bytes {
        bail!("concurrently-created object differs at {key}");
    }
    Ok(false)
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
        bail!(
            "source artifact sha mismatch for {akey}: sidecar {}, bytes {got}",
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

/// Install the source sidecar without overwriting a concurrent writer. An
/// existing sidecar is safe for this body only when it names the same sha and
/// is private (or legacy-untyped under the now-private package claim).
async fn install_or_verify_sidecar(
    dst: &dyn Storage,
    key: &str,
    sidecar: &Sidecar,
) -> Result<bool> {
    let bytes = serde_json::to_vec(sidecar)?;
    if dst.put_if_absent(key, bytes, json()).await? {
        return Ok(true);
    }
    let current = dst
        .get_bytes(key)
        .await
        .with_context(|| format!("verify concurrently-created sidecar {key}"))?;
    let current: Sidecar =
        serde_json::from_slice(&current).with_context(|| format!("parse sidecar {key}"))?;
    if current.sha256 != sidecar.sha256 {
        bail!(
            "concurrently-created sidecar at {key} names sha {}, expected {}",
            current.sha256,
            sidecar.sha256
        );
    }
    if current.origin.as_deref() == Some(MIRROR) {
        bail!("concurrently-created sidecar at {key} is mirror truth");
    }
    if let Some(raw) = current.origin.as_deref() {
        if Origin::parse(raw).is_none() {
            bail!("sidecar at {key} holds an unexpected origin '{raw}'");
        }
    }
    Ok(false)
}

/// Copy a live private record into `dst` (dev/MULTIBUCKET.md §4 copy protocol):
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

    // A body that appeared after the merge read is safe only when it is the
    // exact same immutable artifact. Do not touch its sidecar otherwise; the
    // marker retry will observe the complete competing record and freeze it.
    if dst.head_exists(&akey).await? {
        let current = dst
            .get_bytes(&akey)
            .await
            .with_context(|| format!("verify destination artifact {akey}"))?;
        let got = sha256_hex(&current);
        if got != sc.sha256 {
            bail!(
                "destination artifact appeared with sha {got} while copying {}",
                sc.sha256
            );
        }
    }
    // Origin claim first, ahead of the artifact — shrinks the dependency-
    // confusion window (§4): the name is private before its bytes land.
    ensure_private_origin(dst, pkg).await?;
    // Sidecar first: an orphan sidecar is inert, but an orphan artifact would be
    // fabricated into truth by the destination's backfill (§4).
    let mut changed = install_or_verify_sidecar(dst, &sidecar_key(&akey), sc).await?;
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
    // competing body even briefly after this operation returns.
    let len = verified.artifact.len() as u64;
    if dst
        .put_if_absent(&akey, verified.artifact, Some("application/octet-stream"))
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

/// Drive `pkg`'s origin claim on `dst` to `private` (dev/MULTIBUCKET.md §6.2):
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
            Some(observed)
                if observed.state == OriginState::Private
                    && observed.pending_manifest.is_none() =>
            {
                return Ok(())
            }
            Some(observed) if observed.pending_manifest.is_some() => {
                bail!("package '{pkg}' is under staged promotion")
            }
            Some(observed) if observed.state == OriginState::Mirror => {
                bail!("package '{pkg}' became mirror-owned; retry through staged private demotion")
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

/// Freeze one bucket's copy of a filename (dev/MULTIBUCKET.md §6.3). The richer
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
    // Keep the canonical record occupied behind `.frozen` + `.tombstone`.
    // Deleting it after the quarantine GET can erase a different body that
    // won a concurrent CAS. A later pass can quarantine any such replacement;
    // serving and index rebuilds treat the markers as the visibility fence.
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
    freeze_side(src, pkg, filename).await?;
    freeze_side(dst, pkg, filename).await?;
    worker::mark_dirty(src, pkg).await?;
    worker::mark_dirty(dst, pkg).await?;
    state
        .metrics
        .replication_freezes
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    error!(
        package = %pkg,
        filename = %filename,
        sha_a = %source_sha,
        sha_b = %destination_sha,
        "byte conflict raced replication publish — frozen on both buckets"
    );
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
        return Ok(false);
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
async fn quarantine_mirror_artifacts_for(
    storage: &dyn Storage,
    pkg: &str,
    pending_owner: Option<&crate::origin::OriginObservation>,
) -> Result<usize> {
    let Some(claim) = read_origin_observation(storage, pkg).await? else {
        return Ok(0);
    };
    let authorized = match pending_owner {
        Some(expected) => &claim == expected,
        None => claim.state == OriginState::Private && claim.pending_manifest.is_none(),
    };
    if !authorized {
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

pub async fn quarantine_mirror_artifacts(storage: &dyn Storage, pkg: &str) -> Result<usize> {
    quarantine_mirror_artifacts_for(storage, pkg, None).await
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct StagedEntry {
    filename: String,
    #[serde(default)]
    kind: StagedEntryKind,
    sha256: String,
    base: String,
    /// Destination artifact version observed before the package claim CAS. A
    /// different version at promotion time is a post-stage writer, never safe
    /// to classify from an older sidecar read.
    #[serde(default)]
    destination_etag: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum StagedEntryKind {
    #[default]
    Live,
    Tombstone,
    Frozen,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum StagedMode {
    #[default]
    Demotion,
    PrivateRepair,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct CapturedMirrorArtifact {
    filename: String,
    etag: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct StagedManifest {
    package: String,
    #[serde(default)]
    mode: StagedMode,
    records: Vec<StagedEntry>,
    /// Exact pre-CAS mirror bodies absent from the private source. This catches
    /// crashed/legacy mirror uploads that have no typed sidecar and therefore
    /// cannot be classified after the package claim becomes private.
    #[serde(default)]
    mirror_leftovers: Vec<CapturedMirrorArtifact>,
    /// Private-side status captured with the staged package. Missing private
    /// status is represented by the active epoch-zero default, so crash resume
    /// never has to consult a now-unreachable source bucket.
    status: StagedStatus,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct StagedStatus {
    doc: status::ProjectStatusDoc,
    epoch: u64,
    /// Exact mirror-local status version observed while staging. Demotion may
    /// replace only this version: after it changes, the status belongs to the
    /// now-private history and a stale manifest must never overwrite it.
    destination_etag: Option<String>,
}

struct StagedPackage {
    manifest: StagedManifest,
    manifest_key: String,
}

fn staged_key(base: &str, member: &str) -> String {
    format!("{base}/{member}")
}

/// Stage every live private record in `src` into `dst`, then write one manifest
/// as the package-level commit marker. Crash shapes are deliberately boring:
///
/// - before the manifest: partial `_staging/repl/` objects are inert;
/// - after the manifest, before claim CAS: a later sweep can safely CAS once;
/// - after CAS, during promotion: every staged record is independently
///   verifiable and idempotent, so the manifest retries the unfinished tail.
async fn stage_private_package(
    src: &dyn Storage,
    dst: &dyn Storage,
    pkg: &str,
) -> Result<StagedPackage> {
    let source_claim = read_origin_observation(src, pkg)
        .await?
        .ok_or_else(|| anyhow!("cannot stage '{pkg}' without a source claim"))?;
    if source_claim.state != OriginState::Private || source_claim.pending_manifest.is_some() {
        bail!("cannot stage '{pkg}' from a non-private source claim");
    }
    let destination_claim = read_origin_observation(dst, pkg)
        .await?
        .ok_or_else(|| anyhow!("cannot stage '{pkg}' into a destination without a live claim"))?;
    if destination_claim.pending_manifest.is_some() {
        bail!("cannot stage '{pkg}' into a destination under staged promotion");
    }
    let mode = match destination_claim.state {
        OriginState::Mirror => StagedMode::Demotion,
        OriginState::Private => StagedMode::PrivateRepair,
        OriginState::Unclaimed => {
            bail!("cannot stage '{pkg}' into a destination without a live origin claim")
        }
    };
    let source_status = status::read_status_versioned(src, pkg).await?;
    let destination_status = status::read_status_versioned(dst, pkg).await?;
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let entries = src.list_dir_entries(&prefix).await?;
    let destination_versions: HashMap<String, String> = dst
        .list_all(&prefix)
        .await?
        .into_iter()
        .map(|object| (object.key, object.etag))
        .collect();
    let names: HashSet<String> = entries
        .iter()
        .filter_map(|entry| entry.key.strip_prefix(&prefix).map(str::to_string))
        .collect();
    let pkg_origin = Some(Origin::Private);
    let mut filenames: Vec<String> = candidate_filenames(&names).into_iter().collect();
    filenames.sort();
    let mut records = Vec::new();
    for filename in filenames {
        let record = record_from_names(src, pkg, &filename, &names, pkg_origin).await?;
        let destination_etag = destination_versions
            .get(&artifact_key(pkg, &filename))
            .cloned();
        let fence_kind = if record.frozen {
            Some(StagedEntryKind::Frozen)
        } else if record.tombstoned {
            Some(StagedEntryKind::Tombstone)
        } else {
            None
        };
        if let Some(kind) = fence_kind {
            let (source_key, label) = match kind {
                StagedEntryKind::Frozen => (frozen_key(&artifact_key(pkg, &filename)), "frozen"),
                StagedEntryKind::Tombstone => {
                    (tombstone_key(&artifact_key(pkg, &filename)), "tombstone")
                }
                StagedEntryKind::Live => bail!("live record misclassified as a staged fence"),
            };
            let fence = src
                .get_bytes(&source_key)
                .await
                .with_context(|| format!("read staged fence {source_key}"))?;
            let fence_sha = sha256_hex(&fence);
            let base = format!("{REPL_STAGING_PREFIX}{pkg}/{filename}@{label}-{fence_sha}");
            put_if_absent_or_verify(dst, &staged_key(&base, "fence"), fence, json()).await?;
            records.push(StagedEntry {
                filename,
                kind,
                sha256: fence_sha,
                base,
                destination_etag,
            });
            continue;
        }
        if !matches!(
            record.state(),
            RecordState::Live {
                origin: Origin::Private,
                ..
            }
        ) {
            continue;
        }
        let sidecar = record
            .sidecar
            .as_ref()
            .ok_or_else(|| anyhow!("private stage source has no sidecar for {pkg}/{filename}"))?;
        let verified = verify_source_record(src, pkg, &filename, &record).await?;
        let sidecar_bytes = serde_json::to_vec(sidecar)?;
        let sidecar_sha = sha256_hex(&sidecar_bytes);
        let base = format!(
            "{REPL_STAGING_PREFIX}{pkg}/{filename}@{}-{}",
            sidecar.sha256, sidecar_sha
        );
        put_if_absent_or_verify(dst, &staged_key(&base, "sidecar"), sidecar_bytes, json()).await?;
        if let Some(bytes) = verified.metadata {
            put_if_absent_or_verify(
                dst,
                &staged_key(&base, "metadata"),
                bytes,
                Some("text/plain; charset=utf-8"),
            )
            .await?;
        }
        if let Some(bytes) = verified.provenance {
            put_if_absent_or_verify(
                dst,
                &staged_key(&base, "provenance"),
                bytes,
                Some("application/json"),
            )
            .await?;
        }
        // Artifact last: a stage without it cannot be named by the manifest.
        put_if_absent_or_verify(
            dst,
            &staged_key(&base, "artifact"),
            verified.artifact,
            Some("application/octet-stream"),
        )
        .await?;
        records.push(StagedEntry {
            filename,
            kind: StagedEntryKind::Live,
            sha256: sidecar.sha256.clone(),
            base,
            destination_etag,
        });
    }
    let (status_doc, status_epoch) = source_status.map_or_else(
        || (status::ProjectStatusDoc::default(), 0),
        |status| (status.doc, status.epoch),
    );
    let manifest = StagedManifest {
        package: pkg.to_string(),
        mode,
        mirror_leftovers: if mode == StagedMode::Demotion {
            let staged_names: HashSet<&str> = records
                .iter()
                .map(|entry| entry.filename.as_str())
                .collect();
            destination_versions
                .into_iter()
                .filter_map(|(key, etag)| {
                    let filename = key.strip_prefix(&prefix)?;
                    (crate::sidecar::is_artifact(filename) && !staged_names.contains(filename))
                        .then(|| CapturedMirrorArtifact {
                            filename: filename.to_string(),
                            etag,
                        })
                })
                .collect()
        } else {
            Vec::new()
        },
        records,
        status: StagedStatus {
            doc: status_doc,
            epoch: status_epoch,
            destination_etag: destination_status.map(|status| status.etag),
        },
    };
    if read_origin_observation(src, pkg).await?.as_ref() != Some(&source_claim)
        || read_origin_observation(dst, pkg).await?.as_ref() != Some(&destination_claim)
    {
        bail!("package '{pkg}' origin changed while staging; inert members retained for retry");
    }
    let body = serde_json::to_vec(&manifest)?;
    let manifest_key = format!(
        "{REPL_STAGING_PREFIX}{pkg}/manifest@{}.json",
        sha256_hex(&body)
    );
    put_if_absent_or_verify(dst, &manifest_key, body, json()).await?;
    Ok(StagedPackage {
        manifest,
        manifest_key,
    })
}

async fn read_optional(storage: &dyn Storage, key: &str) -> Result<Option<Vec<u8>>> {
    match storage.get_bytes(key).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

async fn load_staged_record(
    storage: &dyn Storage,
    entry: &StagedEntry,
) -> Result<(Sidecar, VerifiedSource)> {
    let sidecar_key = staged_key(&entry.base, "sidecar");
    let sidecar_bytes = storage
        .get_bytes(&sidecar_key)
        .await
        .with_context(|| format!("read staged sidecar {sidecar_key}"))?;
    let sidecar: Sidecar = serde_json::from_slice(&sidecar_bytes)
        .with_context(|| format!("parse staged sidecar {sidecar_key}"))?;
    if sidecar.sha256 != entry.sha256 || sidecar.origin.as_deref() != Some(PRIVATE) {
        bail!(
            "staged sidecar does not match manifest for {}",
            entry.filename
        );
    }
    let artifact_key = staged_key(&entry.base, "artifact");
    let artifact = storage
        .get_bytes(&artifact_key)
        .await
        .with_context(|| format!("read staged artifact {artifact_key}"))?;
    let got = sha256_hex(&artifact);
    if got != entry.sha256 {
        bail!(
            "staged artifact sha mismatch for {}: manifest {}, bytes {got}",
            entry.filename,
            entry.sha256
        );
    }
    Ok((
        sidecar,
        VerifiedSource {
            artifact,
            metadata: read_optional(storage, &staged_key(&entry.base, "metadata")).await?,
            provenance: read_optional(storage, &staged_key(&entry.base, "provenance")).await?,
        },
    ))
}

async fn load_staged_fence(storage: &dyn Storage, entry: &StagedEntry) -> Result<Vec<u8>> {
    let key = staged_key(&entry.base, "fence");
    let bytes = storage
        .get_bytes(&key)
        .await
        .with_context(|| format!("read staged fence {key}"))?;
    let got = sha256_hex(&bytes);
    if got != entry.sha256 {
        bail!(
            "staged fence hash mismatch for {}: manifest {}, bytes {got}",
            entry.filename,
            entry.sha256
        );
    }
    Ok(bytes)
}

/// Replace a small object through create/CAS only. Used after the package claim
/// is private, when staged private truth is authoritative over mirror metadata.
async fn put_exact(storage: &dyn Storage, key: &str, bytes: Vec<u8>) -> Result<bool> {
    for _ in 0..ORIGIN_ATTEMPTS {
        match storage.get_with_etag(key).await? {
            None => {
                if storage.put_if_absent(key, bytes.clone(), None).await? {
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
    bail!("conditional replace retries exhausted for {key}")
}

/// Install a staged private sidecar without regressing yank state written by a
/// private racer. Mirror metadata is superseded; private metadata is merged by
/// the same epoch/tie algebra as ordinary reconciliation, under CAS.
async fn install_staged_sidecar(
    storage: &dyn Storage,
    key: &str,
    staged: &Sidecar,
    replace_legacy: bool,
) -> Result<bool> {
    let staged_bytes = serde_json::to_vec(staged)?;
    for _ in 0..ORIGIN_ATTEMPTS {
        match storage.get_with_etag(key).await? {
            None => {
                if storage
                    .put_if_absent(key, staged_bytes.clone(), json())
                    .await?
                {
                    return Ok(true);
                }
            }
            Some((current_bytes, _)) if current_bytes == staged_bytes => return Ok(false),
            Some((current_bytes, etag)) => {
                let current: Sidecar = serde_json::from_slice(&current_bytes)
                    .with_context(|| format!("parse destination sidecar {key}"))?;
                let private = match current.origin.as_deref() {
                    Some(PRIVATE) => true,
                    Some(MIRROR) => false,
                    None => !replace_legacy,
                    Some(raw) => bail!("destination sidecar {key} has invalid origin '{raw}'"),
                };
                if private && current.sha256 != staged.sha256 {
                    bail!(
                        "destination sidecar {key} names sha {}, staged artifact is {}",
                        current.sha256,
                        staged.sha256
                    );
                }
                if private && yank_merge(staged, &current) != MergeChoice::A {
                    return Ok(false);
                }
                if storage
                    .put_if_match(key, &etag, staged_bytes.clone())
                    .await?
                    .is_some()
                {
                    return Ok(true);
                }
            }
        }
    }
    bail!("conditional sidecar merge retries exhausted for {key}")
}

async fn promote_staged_record(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    entry: &StagedEntry,
) -> Result<bool> {
    if entry.kind != StagedEntryKind::Live {
        let fence = load_staged_fence(storage, entry).await?;
        let akey = artifact_key(pkg, &entry.filename);
        match entry.kind {
            StagedEntryKind::Tombstone => {
                put_if_absent_or_verify(storage, &tombstone_key(&akey), fence, json()).await?;
                drop_record_objects(storage, pkg, &entry.filename).await?;
            }
            StagedEntryKind::Frozen => {
                put_if_absent_or_verify(storage, &frozen_key(&akey), fence, json()).await?;
                quarantine(storage, pkg, &entry.filename).await?;
                tombstone::write(storage, &akey, &entry.filename).await?;
            }
            StagedEntryKind::Live => bail!("live staged entry reached fence promotion"),
        }
        return Ok(true);
    }
    let (sidecar, staged) = load_staged_record(storage, entry).await?;
    let VerifiedSource {
        artifact,
        metadata,
        provenance,
    } = staged;
    let akey = artifact_key(pkg, &entry.filename);

    // A manifest can outlive a later delete/freeze marker. Those states outrank
    // a stale staged live record and must never be resurrected on retry.
    if storage.head_exists(&frozen_key(&akey)).await? {
        quarantine_bytes(storage, pkg, &entry.filename, &artifact).await?;
        return freeze_side(storage, pkg, &entry.filename).await;
    }
    if storage.head_exists(&tombstone_key(&akey)).await? {
        return tombstone_side(storage, pkg, &entry.filename).await;
    }

    let current_sidecar = read_optional(storage, &sidecar_key(&akey)).await?;
    let current_origin = match current_sidecar.as_deref() {
        Some(bytes) => {
            let current: Sidecar = serde_json::from_slice(bytes)
                .with_context(|| format!("parse destination sidecar for {akey}"))?;
            match current.origin.as_deref() {
                Some(PRIVATE) => Some(Origin::Private),
                Some(MIRROR) => Some(Origin::Mirror),
                None => None,
                Some(raw) => bail!("destination sidecar for {akey} has invalid origin '{raw}'"),
            }
        }
        None => None,
    };
    let replace_local_metadata = current_origin != Some(Origin::Private);
    let mut changed = false;
    let mut artifact_present = false;
    if let Some((bytes, etag)) = storage.get_with_etag(&akey).await? {
        if sha256_hex(&bytes) == entry.sha256 {
            artifact_present = true;
        } else if current_origin == Some(Origin::Private)
            || entry.destination_etag.as_deref() != Some(etag.as_str())
        {
            // A private writer beat this (possibly stale) manifest. Preserve
            // both committed bodies and suppress the filename. The artifact
            // etag must be the exact pre-CAS mirror version; a sidecar read by
            // itself cannot classify a writer that raced after the claim CAS.
            write_frozen_marker(storage, pkg, &entry.filename).await?;
            quarantine_bytes(storage, pkg, &entry.filename, &artifact).await?;
            freeze_side(storage, pkg, &entry.filename).await?;
            state
                .metrics
                .replication_freezes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            error!(package=%pkg, filename=%entry.filename, "private artifact raced staged package promotion — frozen");
            return Ok(true);
        } else {
            // Preserve the mirror loser, then atomically replace its body. The
            // live artifact key never disappears, so a one-file package is not
            // left empty during demotion.
            quarantine_bytes(storage, pkg, &entry.filename, &bytes).await?;
            if storage
                .put_if_match(&akey, &etag, artifact.clone())
                .await?
                .is_none()
            {
                bail!("mirror artifact changed during staged promotion for {akey}");
            }
            state.metrics.record_replicated(artifact.len() as u64);
            artifact_present = true;
            changed = true;
        }
    }

    // For an absent body, sidecar first and artifact last. Existing package
    // indexes are not nudged until the complete record is in place.
    changed |= install_staged_sidecar(
        storage,
        &sidecar_key(&akey),
        &sidecar,
        replace_local_metadata,
    )
    .await?;
    for (suffix_key, staged_bytes) in [
        (metadata_key(&akey), metadata),
        (provenance_key(&akey), provenance),
    ] {
        match staged_bytes {
            Some(bytes) if replace_local_metadata => {
                changed |= put_exact(storage, &suffix_key, bytes).await?
            }
            Some(bytes) => {
                changed |= put_if_absent_or_verify(storage, &suffix_key, bytes, None).await?
            }
            None if replace_local_metadata => {
                storage.delete_keys(&[suffix_key]).await?;
                changed = true;
            }
            None => {}
        }
    }
    if !artifact_present {
        if storage
            .put_if_absent(&akey, artifact.clone(), Some("application/octet-stream"))
            .await?
        {
            state.metrics.record_replicated(artifact.len() as u64);
            changed = true;
        } else {
            let raced = storage.get_bytes(&akey).await?;
            if sha256_hex(&raced) != entry.sha256 {
                write_frozen_marker(storage, pkg, &entry.filename).await?;
                quarantine_bytes(storage, pkg, &entry.filename, &artifact).await?;
                freeze_side(storage, pkg, &entry.filename).await?;
                worker::mark_dirty(storage, pkg).await?;
                state
                    .metrics
                    .replication_freezes
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!(package=%pkg, filename=%entry.filename, "private artifact raced staged package promotion — frozen");
                return Ok(true);
            }
        }
    }
    // A delete/freeze can start after the pre-mutation fence reads. Recheck
    // after the artifact/sidecar converge so a publisher that crossed that
    // window leaves only marker-fenced evidence, never newly visible truth.
    if storage.head_exists(&frozen_key(&akey)).await? {
        quarantine_bytes(storage, pkg, &entry.filename, &artifact).await?;
        freeze_side(storage, pkg, &entry.filename).await?;
        return Ok(true);
    }
    if storage.head_exists(&tombstone_key(&akey)).await? {
        return tombstone_side(storage, pkg, &entry.filename).await;
    }
    Ok(changed)
}

/// Remove one exact mirror body that existed before the package claim CAS but
/// had no source-private counterpart. The ETag check protects a post-stage
/// writer. Sidecars are intentionally left inert: a stale mirror writer that
/// passed its final claim read before demotion may still finish, and its typed
/// sidecar lets the ordinary late-mirror cleanup identify that body.
async fn quarantine_captured_mirror_leftover(
    storage: &dyn Storage,
    pkg: &str,
    captured: &CapturedMirrorArtifact,
) -> Result<bool> {
    let key = artifact_key(pkg, &captured.filename);
    let Some((bytes, etag)) = storage.get_with_etag(&key).await? else {
        return Ok(false);
    };
    if etag != captured.etag {
        return Ok(false);
    }
    // The manifest captured this exact artifact version while the package was
    // mirror-owned. A sidecar backfill that raced the later claim CAS cannot
    // reclassify those already-captured bytes as private truth.
    quarantine_mirror_record(storage, pkg, &captured.filename, &bytes).await
}

async fn promote_staged_package(
    state: &AppState,
    storage: &dyn Storage,
    staged: &StagedPackage,
    pending: &crate::origin::OriginObservation,
    lease: &PromotionLease,
) -> Result<()> {
    require_replication_unfenced(state)?;
    let pkg = &staged.manifest.package;
    require_promotion_execution_owner(storage, pkg, pending, lease).await?;
    if staged.manifest.mode == StagedMode::Demotion {
        require_promotion_execution_owner(storage, pkg, pending, lease).await?;
        rebase_status_after_demotion(storage, pkg, &staged.manifest.status).await?;
    }
    for entry in &staged.manifest.records {
        require_replication_unfenced(state)?;
        require_promotion_execution_owner(storage, pkg, pending, lease).await?;
        promote_staged_record(state, storage, pkg, entry).await?;
    }
    for leftover in &staged.manifest.mirror_leftovers {
        require_promotion_execution_owner(storage, pkg, pending, lease).await?;
        quarantine_captured_mirror_leftover(storage, pkg, leftover).await?;
    }
    require_promotion_execution_owner(storage, pkg, pending, lease).await?;
    quarantine_mirror_artifacts_for(storage, pkg, Some(pending)).await?;
    // This marker is part of manifest completion, not an optimization. A prior
    // attempt may have applied every idempotent truth mutation and then failed
    // here; every retry must reassert the derived-view/global-inventory event.
    require_promotion_execution_owner(storage, pkg, pending, lease).await?;
    worker::mark_dirty(storage, pkg).await?;
    // Keep reads closed until the materialized package view matches the fully
    // promoted truth. The ordinary dirty marker remains the crash replay and
    // global-membership/inventory backstop.
    require_promotion_execution_owner(storage, pkg, pending, lease).await?;
    worker::rebuild_package_indexes_for_promotion(state, storage, pkg, pending).await?;
    require_promotion_execution_owner(storage, pkg, pending, lease).await?;
    Ok(())
}

async fn require_promotion_execution_owner(
    storage: &dyn Storage,
    pkg: &str,
    expected: &crate::origin::OriginObservation,
    lease: &PromotionLease,
) -> Result<()> {
    lease.require(storage).await?;
    require_promotion_owner(storage, pkg, expected).await
}

async fn require_promotion_owner(
    storage: &dyn Storage,
    pkg: &str,
    expected: &crate::origin::OriginObservation,
) -> Result<()> {
    if read_origin_observation(storage, pkg).await?.as_ref() == Some(expected) {
        Ok(())
    } else {
        bail!("package '{pkg}' promotion owner changed while applying its manifest")
    }
}

/// Acquire the package's CAS-owned promotion barrier, apply one committed
/// manifest, then release the claim back to ordinary private operation. The
/// pending manifest lives in `.origin`, so no separately-listed key can race a
/// writer's final origin fence.
async fn settle_staged_package(
    state: &AppState,
    storage: &dyn Storage,
    staged: &StagedPackage,
) -> Result<bool> {
    settle_staged_package_ignoring(state, storage, staged, None).await
}

async fn settle_staged_package_ignoring(
    state: &AppState,
    storage: &dyn Storage,
    staged: &StagedPackage,
    ignored_intent: Option<&str>,
) -> Result<bool> {
    let pkg = &staged.manifest.package;
    let owns_attempt = ignored_intent.is_none();
    let attempt = match ignored_intent {
        Some(value) => value.to_string(),
        None => worker::mark_intent(storage, pkg).await?,
    };
    // The durable lock chooses exactly one executor. Losers never enter the
    // mutual-defer protocol; a crashed holder is stealable only after both its
    // lock ETag and intent have proved stale for the grace window.
    let lease = match acquire_promotion_lock(state, storage, staged, &attempt).await {
        Ok(Some(lease)) => lease,
        Ok(None) => {
            if owns_attempt {
                worker::clear_intent(storage, pkg, &attempt).await?;
            }
            return Ok(false);
        }
        Err(error) => {
            if owns_attempt {
                let _ = worker::clear_intent(storage, pkg, &attempt).await;
            }
            return Err(error);
        }
    };
    let operation = settle_staged_package_core(state, storage, staged, &attempt, &lease);
    let result = run_with_promotion_heartbeat(state, storage, &lease, operation).await;
    if !owns_attempt {
        return result;
    }
    match result {
        Ok(value) => {
            worker::clear_intent(storage, pkg, &attempt).await?;
            Ok(value)
        }
        Err(error) => {
            // Promotion may have applied a prefix of its idempotent manifest.
            // Pair the attempt intent so the view worker heals that prefix;
            // clearing it here would erase the only replay event.
            let _ = worker::mark_commit(storage, pkg, &attempt).await;
            Err(error)
        }
    }
}

async fn settle_staged_package_core(
    state: &AppState,
    storage: &dyn Storage,
    staged: &StagedPackage,
    ignored_intent: &str,
    lease: &PromotionLease,
) -> Result<bool> {
    require_replication_unfenced(state)?;
    let pkg = &staged.manifest.package;
    let observed = read_origin_observation(storage, pkg).await?;
    match observed
        .as_ref()
        .and_then(|claim| claim.pending_manifest.as_deref())
    {
        Some(owner) if owner != staged.manifest_key => return Ok(false),
        Some(_) => {
            // This manifest already owns pending after a prior crash. Its stale
            // intent recovery below must remain reachable.
        }
        None => {
            wait_for_other_intents(state, storage, pkg, ignored_intent, lease).await?;
        }
    }
    lease.require(storage).await?;
    let Some(pending) = begin_private_promotion(storage, pkg, &staged.manifest_key).await? else {
        return Ok(false);
    };
    require_replication_unfenced(state)?;
    // A writer can have started between the first check and the origin CAS. Its
    // final origin fence now sees pending and refuses truth, but wait for its
    // intent to close before classifying any existing record. If that writer
    // crashed, only this exact pending owner can heal the stale intent: normal
    // rebuilds correctly refuse to cross the barrier.
    require_promotion_execution_owner(storage, pkg, &pending, lease).await?;
    let stale = wait_for_other_intents(state, storage, pkg, ignored_intent, lease).await?;
    if !stale.is_empty() {
        require_promotion_execution_owner(storage, pkg, &pending, lease).await?;
        storage.delete_keys(&stale).await?;
    }
    require_promotion_execution_owner(storage, pkg, &pending, lease).await?;
    if worker::has_unpaired_intent_ignoring(storage, pkg, Some(ignored_intent)).await? {
        bail!("package '{pkg}' acquired a writer during promotion fencing");
    }
    promote_staged_package(state, storage, staged, &pending, lease).await?;
    require_replication_unfenced(state)?;
    require_promotion_execution_owner(storage, pkg, &pending, lease).await?;
    if !finish_private_promotion(storage, pkg, &pending).await? {
        bail!("package '{pkg}' promotion barrier changed before release");
    }
    // Delete only this manifest. Stage members are content-addressed and may be
    // shared by another committed manifest; unreferenced members remain inert.
    lease.require(storage).await?;
    storage
        .delete_keys(std::slice::from_ref(&staged.manifest_key))
        .await?;
    Ok(true)
}

async fn wait_for_other_intents(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    own_intent: &str,
    lease: &PromotionLease,
) -> Result<Vec<String>> {
    // A competing promotion attempt cannot hold this lock and will pair its
    // intent immediately. Real writers get the configured slow-upload grace;
    // after pending is acquired no new request mutation can cross its final
    // claim fence.
    let deadline = tokio::time::Instant::now()
        + intent_grace_std(state).max(std::time::Duration::from_secs(1));
    loop {
        lease.require(storage).await?;
        if let Some(stale) =
            worker::stale_unpaired_intents_ignoring(state, storage, pkg, Some(own_intent)).await?
        {
            return Ok(stale);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("package '{pkg}' still has an active writer; staged promotion deferred");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn stage_demote_and_promote_ignoring(
    state: &AppState,
    private: &dyn Storage,
    mirror: &dyn Storage,
    pkg: &str,
    ignored_intent: Option<&str>,
) -> Result<()> {
    require_replication_unfenced(state)?;
    let staged = stage_private_package(private, mirror, pkg).await?;
    if settle_staged_package_ignoring(state, mirror, &staged, ignored_intent).await? {
        Ok(())
    } else {
        bail!("another committed manifest currently owns package '{pkg}'")
    }
}

/// Replace mirror-world status with the source's private event. Tagged mirror
/// writes are replaceable even when they land after staging; tagged private
/// writes are acknowledged post-demotion history and always survive a stale
/// manifest. Legacy untagged bodies retain the exact captured-ETag rule.
async fn rebase_status_after_demotion(
    destination: &dyn Storage,
    pkg: &str,
    authoritative: &StagedStatus,
) -> Result<()> {
    for _ in 0..ORIGIN_ATTEMPTS {
        let current = status::read_status_versioned(destination, pkg).await?;
        let current_etag = current.as_ref().map(|status| status.etag.as_str());
        match current.as_ref().and_then(|status| status.origin) {
            Some(status::StatusOrigin::Private) => return Ok(()),
            Some(status::StatusOrigin::Mirror) => {}
            None if current_etag == authoritative.destination_etag.as_deref() => {}
            None => return Ok(()),
        }
        if current.is_none()
            && authoritative.epoch == 0
            && authoritative.doc == status::ProjectStatusDoc::default()
        {
            return Ok(());
        }
        if status::put_status_if_version(
            destination,
            pkg,
            current_etag,
            &authoritative.doc,
            authoritative.epoch,
            Some(status::StatusOrigin::Private),
        )
        .await?
        {
            worker::mark_dirty(destination, pkg).await?;
            return Ok(());
        }
    }
    bail!("status demotion retries exhausted for '{pkg}'")
}

/// Repair mirror sidecars that finished after the package claim was already
/// private (the narrow proxy pre-put fence race). Stage the complete private
/// package before replacing/quarantining any local mirror record, exactly like
/// ordinary claim demotion but without another claim transition.
async fn stage_private_over_local_mirror_ignoring(
    state: &AppState,
    private: &dyn Storage,
    destination: &dyn Storage,
    pkg: &str,
    ignored_intent: Option<&str>,
) -> Result<()> {
    require_replication_unfenced(state)?;
    let staged = stage_private_package(private, destination, pkg).await?;
    if settle_staged_package_ignoring(state, destination, &staged, ignored_intent).await? {
        Ok(())
    } else {
        bail!("another committed manifest currently owns package '{pkg}'")
    }
}

async fn resume_staged_packages_in(state: &AppState, storage: &dyn Storage) -> Result<()> {
    let objects = storage.list_all(REPL_STAGING_PREFIX).await?;
    let manifests: Vec<String> = objects
        .into_iter()
        .map(|object| object.key)
        .filter(|key| {
            key.rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with("manifest@"))
        })
        .collect();
    let live_locks: HashSet<String> = manifests
        .iter()
        .filter_map(|manifest| {
            let rest = manifest.strip_prefix(REPL_STAGING_PREFIX)?;
            let (pkg, name) = rest.split_once('/')?;
            name.starts_with("manifest@")
                .then(|| promotion_lock_key(pkg))
        })
        .collect();
    let storage_id = storage_identity(storage);
    state
        .promotion_lock_observations
        .lock()
        .await
        .retain(|(observed_storage, key), _| {
            *observed_storage != storage_id || live_locks.contains(key)
        });
    let mut failures = Vec::new();
    for manifest_key in manifests {
        let result: Result<()> = async {
            require_replication_unfenced(state)?;
            let bytes = storage.get_bytes(&manifest_key).await?;
            let manifest: StagedManifest = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse replication stage manifest {manifest_key}"))?;
            let staged = StagedPackage {
                manifest,
                manifest_key: manifest_key.clone(),
            };
            settle_staged_package(state, storage, &staged).await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            failures.push(format!("{manifest_key}: {error:#}"));
        }
    }
    if !failures.is_empty() {
        bail!("staged manifest recovery failed: {}", failures.join("; "));
    }
    Ok(())
}

pub(crate) async fn has_committed_stage(storage: &dyn Storage, pkg: &str) -> Result<bool> {
    let prefix = format!("{REPL_STAGING_PREFIX}{pkg}/");
    Ok(storage.list_all(&prefix).await?.iter().any(|object| {
        object
            .key
            .strip_prefix(&prefix)
            .is_some_and(|name| name.starts_with("manifest@"))
    }))
}

/// Resume any committed stage for one package. Marker retries use this narrow
/// form so a crash after claim CAS does not wait for the next global sweep.
async fn resume_staged_package(state: &AppState, storage: &dyn Storage, pkg: &str) -> Result<bool> {
    let prefix = format!("{REPL_STAGING_PREFIX}{pkg}/");
    let objects = storage.list_all(&prefix).await?;
    let manifests: Vec<String> = objects
        .into_iter()
        .map(|object| object.key)
        .filter(|key| {
            key.rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with("manifest@"))
        })
        .collect();
    let found = !manifests.is_empty();
    let mut failures = Vec::new();
    for manifest_key in manifests {
        let result: Result<()> = async {
            require_replication_unfenced(state)?;
            let bytes = storage.get_bytes(&manifest_key).await?;
            let manifest: StagedManifest = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse replication stage manifest {manifest_key}"))?;
            if manifest.package != pkg {
                bail!(
                    "replication stage manifest {manifest_key} names package '{}' instead of '{pkg}'",
                    manifest.package
                );
            }
            let staged = StagedPackage {
                manifest,
                manifest_key: manifest_key.clone(),
            };
            settle_staged_package(state, storage, &staged).await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            failures.push(format!("{manifest_key}: {error:#}"));
        }
    }
    if !failures.is_empty() {
        bail!(
            "staged package recovery failed for '{pkg}': {}",
            failures.join("; ")
        );
    }
    Ok(found)
}

async fn resume_pending_staged_package(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
) -> Result<bool> {
    let pending = read_origin_observation(storage, pkg)
        .await?
        .and_then(|value| value.pending_manifest)
        .is_some();
    if pending {
        resume_staged_package(state, storage, pkg).await
    } else {
        Ok(false)
    }
}

pub async fn resume_staged_packages(state: &AppState) -> Result<()> {
    let mut failures = Vec::new();
    let jobs = state
        .buckets
        .handles()
        .iter()
        .enumerate()
        .filter(|(idx, _)| bucket_eligible(state, *idx))
        .map(|(idx, handle)| async move {
            let result = tokio::select! {
                result = resume_staged_packages_in(state, handle.storage.as_ref()) => result,
                _ = wait_until_bucket_ineligible(state, idx) => {
                    Err(anyhow!("bucket became topology-ineligible during stage recovery"))
                }
            };
            (idx, result)
        });
    for (idx, result) in futures::future::join_all(jobs).await {
        if let Err(e) = result {
            failures.push(format!("bucket {idx}: {e:#}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("staged package recovery failed: {}", failures.join("; "))
    }
}

// ---------------------------------------------------------------------------
// Tier 1 — eager fan-out (post-ack).
// ---------------------------------------------------------------------------

/// Durable todo markers queued on the bucket that accepted a mutation. The
/// request path creates these before acknowledging the client, then hands the
/// token to [`spawn_eager_with_markers`], which removes each marker only after
/// that destination has converged.
#[derive(Default)]
pub struct FanoutMarkers {
    by_destination: HashMap<usize, String>,
}

/// Queue one durable marker per other configured bucket. A single-bucket node
/// returns immediately without storage I/O. The caller decides how a queueing
/// failure affects its response; any successfully-written partial set remains
/// visible to the normal all-bucket sweeper.
pub async fn queue_fanout_markers(
    state: &AppState,
    pinned: &Pinned,
    pkg: &str,
    filename: &str,
) -> Result<FanoutMarkers> {
    if !state.buckets.is_multi() {
        return Ok(FanoutMarkers::default());
    }
    require_replication_unfenced(state)?;
    let mut markers = FanoutMarkers::default();
    for (idx, _) in state.buckets.handles().iter().enumerate() {
        if idx == pinned.index {
            continue;
        }
        let key = tokio::select! {
            result = write_marker(pinned.storage.as_ref(), idx, pkg, filename) => result?,
            _ = wait_until_bucket_ineligible(state, pinned.index) => {
                bail!("selected bucket became ineligible while queueing replication markers")
            }
        };
        markers.by_destination.insert(idx, key);
    }
    Ok(markers)
}

/// Kick eager fan-out for a mutation whose durable markers were already queued
/// by [`queue_fanout_markers`]. Successful destinations consume their exact
/// nonce-bearing marker; failures leave it for the sweeper.
pub fn spawn_eager_with_markers(
    state: &Arc<AppState>,
    pinned: &Pinned,
    pkg: String,
    filename: String,
    markers: FanoutMarkers,
) {
    if !state.buckets.is_multi() {
        return;
    }
    let state = state.clone();
    let src = pinned.storage.clone();
    let src_index = pinned.index;
    tokio::spawn(async move {
        eager_fanout(&state, src.as_ref(), src_index, &pkg, &filename, markers).await;
    });
}

async fn eager_fanout(
    state: &AppState,
    src: &dyn Storage,
    src_index: usize,
    pkg: &str,
    filename: &str,
    mut markers: FanoutMarkers,
) {
    if let Err(error) = require_replication_unfenced(state) {
        warn!(package=%pkg, filename=%filename, error=?error, "eager replication fenced; markers retained");
        return;
    }
    if !bucket_eligible(state, src_index) {
        warn!(package=%pkg, filename=%filename, source=src_index, "eager replication source is not topology-eligible; markers retained");
        return;
    }
    let jobs = state
        .buckets
        .handles()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != src_index && bucket_eligible(state, *idx))
        .map(|(idx, handle)| {
            let marker = markers.by_destination.remove(&idx);
            async move {
                let result = tokio::select! {
                    result = replicate_record(state, src, handle.storage.as_ref(), pkg, filename) => result,
                    _ = wait_until_pair_ineligible(state, src_index, idx) => {
                        Err(anyhow!("source or destination became topology-ineligible"))
                    }
                };
                match result {
                    Ok(()) => {
                        if let Some(key) = marker {
                            if let Err(e) = src.delete_keys(&[key]).await {
                                warn!(dest=%handle.name, package=%pkg, filename=%filename, error=?e, "eager replication succeeded but its marker could not be consumed");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(dest=%handle.name, package=%pkg, filename=%filename, error=?e, "eager replication failed; leaving a marker for the sweep");
                        if marker.is_none() {
                            if let Err(e2) = write_marker(src, idx, pkg, filename).await {
                                error!(dest=%handle.name, error=?e2, "could not write replication marker");
                            }
                        }
                    }
                }
            }
        });
    // Start every destination before awaiting any of them. A blackholed middle
    // bucket may retain its marker, but cannot delay a healthy later bucket.
    futures::future::join_all(jobs).await;
}

/// Replicate one record from `src` into `dst` (tiers 1 and 2). Reads both
/// sides, decides, and applies — the same merge that reconcile runs, so a byte
/// conflict at the destination freezes rather than clobbers.
async fn replicate_record(
    state: &AppState,
    src: &dyn Storage,
    dst: &dyn Storage,
    pkg: &str,
    filename: &str,
) -> Result<()> {
    require_replication_unfenced(state)?;
    // A complete stage may survive a crash immediately before or after the
    // package claim CAS. Consume it before looking at per-file state; otherwise
    // private/private claims with an old mirror sidecar would reach the
    // deliberately-disabled per-file supersede path.
    resume_pending_staged_package(state, src, pkg).await?;
    resume_pending_staged_package(state, dst, pkg).await?;
    let intents = acquire_replication_intents(src, dst, pkg).await?;
    let operation = async {
        let mut src_origin = read_pkg_origin(src, pkg).await?;
        let mut dst_origin = read_pkg_origin(dst, pkg).await?;
        match (src_origin, dst_origin) {
            (Some(Origin::Private), Some(Origin::Mirror)) => {
                stage_demote_and_promote_ignoring(state, src, dst, pkg, Some(&intents.right))
                    .await?;
                dst_origin = Some(Origin::Private);
            }
            (Some(Origin::Mirror), Some(Origin::Private)) => {
                stage_demote_and_promote_ignoring(state, dst, src, pkg, Some(&intents.left))
                    .await?;
                src_origin = Some(Origin::Private);
            }
            (Some(Origin::Private), None) => {
                ensure_private_origin(dst, pkg).await?;
                dst_origin = Some(Origin::Private);
            }
            (None, Some(Origin::Private)) => {
                ensure_private_origin(src, pkg).await?;
                src_origin = Some(Origin::Private);
            }
            _ => {}
        }

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
        match verdict {
            Verdict::Supersede(Side::A) => {
                return stage_private_over_local_mirror_ignoring(
                    state,
                    src,
                    dst,
                    pkg,
                    Some(&intents.right),
                )
                .await
            }
            Verdict::Supersede(Side::B) => {
                return stage_private_over_local_mirror_ignoring(
                    state,
                    dst,
                    src,
                    pkg,
                    Some(&intents.left),
                )
                .await
            }
            _ => {}
        }
        execute(state, (src, dst), pkg, filename, (&a, &b), verdict).await
    };
    let result = run_with_replication_heartbeats(state, src, dst, pkg, &intents, operation).await;
    match result {
        Ok(()) => release_replication_intents(src, dst, pkg, &intents).await,
        Err(error) => {
            commit_replication_intents(src, dst, pkg, &intents).await;
            Err(error)
        }
    }
}

async fn normalize_mirror_status_under_private_claim(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<bool> {
    let Some(claim) = read_origin_observation(storage, pkg).await? else {
        return Ok(false);
    };
    if claim.state != OriginState::Private || claim.pending_manifest.is_some() {
        return Ok(false);
    }
    let Some(initial_status) = status::read_status_versioned(storage, pkg).await? else {
        return Ok(false);
    };
    if initial_status.origin != Some(status::StatusOrigin::Mirror) {
        return Ok(false);
    }

    // Join the same package-intent protocol as request writers. If promotion
    // starts first, the exact claim re-read below sees pending and we defer. If
    // this intent starts first, both promotion checks retain the staged barrier
    // until the status CAS is complete.
    let nonce = worker::mark_intent(storage, pkg).await?;
    let result: Result<bool> = async {
        if read_origin_observation(storage, pkg).await?.as_ref() != Some(&claim) {
            return Ok(false);
        }
        for _ in 0..ORIGIN_ATTEMPTS {
            let current = status::read_status_versioned(storage, pkg).await?;
            let Some(current) = current else {
                return Ok(false);
            };
            if current.origin != Some(status::StatusOrigin::Mirror) {
                return Ok(false);
            }
            if status::put_status_if_version(
                storage,
                pkg,
                Some(&current.etag),
                &status::ProjectStatusDoc::default(),
                0,
                Some(status::StatusOrigin::Private),
            )
            .await?
            {
                return Ok(true);
            }
        }
        bail!("could not normalize late mirror status for private package '{pkg}'")
    }
    .await;
    let committed = worker::mark_commit(storage, pkg, &nonce).await;
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
            worker::mark_dirty(a, pkg).await?;
        }
        StatusConvergence::UpdatedRight => {
            worker::mark_dirty(b, pkg).await?;
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
        worker::marker_nonce()
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
fn parse_marker(key: &str) -> Option<ReplMarker> {
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

fn publish_marker_backlog(state: &AppState, total: &HashMap<usize, u64>) {
    for (idx, handle) in state.buckets.handles().iter().enumerate() {
        state
            .metrics
            .set_marker_backlog(&handle.name, total.get(&idx).copied().unwrap_or(0));
    }
}

async fn wait_until_bucket_ineligible(state: &AppState, index: usize) {
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
    let markers = tokio::select! {
        result = src.list_all(REPL_PREFIX) => result?,
        _ = wait_until_bucket_ineligible(state, src_index) => {
            bail!("source bucket {src_index} became ineligible during marker listing")
        }
    };
    let mut by_destination: HashMap<usize, Vec<ReplMarker>> = HashMap::new();
    for meta in markers {
        let Some(marker) = parse_marker(&meta.key) else {
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
    let mut remaining: HashMap<usize, u64> = HashMap::new();
    for (dest, count) in outcomes {
        remaining.insert(dest, count);
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

/// Group a flat `packages/` listing into `pkg -> {member filenames}` (the last
/// path segment under `packages/<pkg>/`).
fn group_by_pkg(objs: &[ObjectMeta]) -> HashMap<String, HashSet<String>> {
    let mut map: HashMap<String, HashSet<String>> = HashMap::new();
    for obj in objs {
        if let Some(rest) = obj.key.strip_prefix(PACKAGES_PREFIX) {
            if let Some((pkg, member)) = rest.split_once('/') {
                map.entry(pkg.to_string())
                    .or_default()
                    .insert(member.to_string());
            }
        }
    }
    map
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
async fn diff_pair(state: &AppState, a: &dyn Storage, b: &dyn Storage) -> Result<()> {
    let (a_objs, b_objs) =
        futures::future::try_join(a.list_all(PACKAGES_PREFIX), b.list_all(PACKAGES_PREFIX)).await?;
    let a_map = group_by_pkg(&a_objs);
    let b_map = group_by_pkg(&b_objs);
    let pkgs: BTreeSet<&String> = a_map.keys().chain(b_map.keys()).collect();
    let mut failures = Vec::new();
    for pkg in pkgs {
        let result: Result<()> = async {
            require_replication_unfenced(state)?;
            // A pending `.origin` claim is a package-wide promotion barrier.
            // Finish any committed stage before the ordinary pairwise algebra
            // looks at the package's transient record set.
            resume_pending_staged_package(state, a, pkg).await?;
            resume_pending_staged_package(state, b, pkg).await?;
            let intents = acquire_replication_intents(a, b, pkg).await?;
            let locked_operation = async {
                // Re-list only after both package intents are durable. The flat
                // scan that discovered the package predates this fence.
                let mut a_names = package_member_names(a, pkg).await?;
                let mut b_names = package_member_names(b, pkg).await?;

                let mut a_origin = read_pkg_origin(a, pkg).await?;
                let mut b_origin = read_pkg_origin(b, pkg).await?;
                match (a_origin, b_origin) {
                    (Some(Origin::Private), Some(Origin::Mirror)) => {
                        stage_demote_and_promote_ignoring(state, a, b, pkg, Some(&intents.right))
                            .await?;
                        b_names = package_member_names(b, pkg).await?;
                        b_origin = Some(Origin::Private);
                    }
                    (Some(Origin::Mirror), Some(Origin::Private)) => {
                        stage_demote_and_promote_ignoring(state, b, a, pkg, Some(&intents.left))
                            .await?;
                        a_names = package_member_names(a, pkg).await?;
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

                if a_origin == Some(Origin::Private) || b_origin == Some(Origin::Private) {
                    reconcile_project_status(a, b, pkg).await?;
                }

                let mut converged = false;
                for _ in 0..3 {
                    let mut filenames = candidate_filenames(&a_names);
                    filenames.extend(candidate_filenames(&b_names));
                    let mut restage = None;
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
                            if late_a && quarantine_mirror_artifacts(a, pkg).await? > 0 {
                                worker::mark_dirty(a, pkg).await?;
                                a_names = package_member_names(a, pkg).await?;
                            }
                            if late_b && quarantine_mirror_artifacts(b, pkg).await? > 0 {
                                worker::mark_dirty(b, pkg).await?;
                                b_names = package_member_names(b, pkg).await?;
                            }
                            restage = Some(if late_a { Side::B } else { Side::A });
                            break;
                        }
                        let verdict = decide(&ra, &rb);
                        match verdict {
                            Verdict::Supersede(side) => {
                                restage = Some(side);
                                break;
                            }
                            _ => {
                                execute(state, (a, b), pkg, &filename, (&ra, &rb), verdict).await?
                            }
                        }
                    }
                    match restage {
                        Some(Side::A) => {
                            stage_private_over_local_mirror_ignoring(
                                state,
                                a,
                                b,
                                pkg,
                                Some(&intents.right),
                            )
                            .await?;
                            b_names = package_member_names(b, pkg).await?;
                        }
                        Some(Side::B) => {
                            stage_private_over_local_mirror_ignoring(
                                state,
                                b,
                                a,
                                pkg,
                                Some(&intents.left),
                            )
                            .await?;
                            a_names = package_member_names(a, pkg).await?;
                        }
                        None => {
                            converged = true;
                            break;
                        }
                    }
                }
                if !converged {
                    bail!("package '{pkg}' kept exposing mirror records under a private claim");
                }
                Ok(())
            };
            let locked =
                run_with_replication_heartbeats(state, a, b, pkg, &intents, locked_operation).await;
            match locked {
                Ok(()) => release_replication_intents(a, b, pkg, &intents).await,
                Err(error) => {
                    commit_replication_intents(a, b, pkg, &intents).await;
                    Err(error)
                }
            }
        }
        .await;
        if let Err(error) = result {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buckets::{BucketHandle, BucketSet};
    use crate::storage::test_support::InMemStorage;
    use crate::storage::{FileEntry, ObjectMeta};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct LockPause {
        acquired: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    struct RaceOnCreateStorage {
        inner: InMemStorage,
        key: String,
        bytes: Vec<u8>,
        required_prior_key: Option<String>,
        intent_before_cas: Option<String>,
        fail_first_put_prefix: Option<String>,
        fail_first_put_suffix: Option<String>,
        raced: AtomicBool,
        cas_raced: AtomicBool,
        put_failed: AtomicBool,
        lock_pause: Option<Arc<LockPause>>,
        lock_paused: AtomicBool,
        lock_read_pause: Option<Arc<LockPause>>,
        lock_read_paused: AtomicBool,
        fresh_dirty_timestamps: bool,
    }

    impl RaceOnCreateStorage {
        fn new(key: String, bytes: Vec<u8>) -> Self {
            Self {
                inner: InMemStorage::default(),
                key,
                bytes,
                required_prior_key: None,
                intent_before_cas: None,
                fail_first_put_prefix: None,
                fail_first_put_suffix: None,
                raced: AtomicBool::new(false),
                cas_raced: AtomicBool::new(false),
                put_failed: AtomicBool::new(false),
                lock_pause: None,
                lock_paused: AtomicBool::new(false),
                lock_read_pause: None,
                lock_read_paused: AtomicBool::new(false),
                fresh_dirty_timestamps: false,
            }
        }

        fn requiring_prior_key(mut self, key: String) -> Self {
            self.required_prior_key = Some(key);
            self
        }

        fn injecting_intent_before_cas(mut self, key: String) -> Self {
            self.intent_before_cas = Some(key);
            self
        }

        fn failing_first_put_under_ending(mut self, prefix: &str, suffix: &str) -> Self {
            self.fail_first_put_prefix = Some(prefix.to_string());
            self.fail_first_put_suffix = Some(suffix.to_string());
            self
        }

        fn pausing_first_promotion_lock(mut self, pause: Arc<LockPause>) -> Self {
            self.lock_pause = Some(pause);
            self
        }

        fn pausing_first_promotion_lock_read(mut self, pause: Arc<LockPause>) -> Self {
            self.lock_read_pause = Some(pause);
            self
        }

        fn with_fresh_dirty_timestamps(mut self) -> Self {
            self.fresh_dirty_timestamps = true;
            self
        }

        fn should_fail_put(&self, key: &str) -> bool {
            self.fail_first_put_prefix
                .as_ref()
                .is_some_and(|prefix| key.starts_with(prefix))
                && self
                    .fail_first_put_suffix
                    .as_ref()
                    .is_none_or(|suffix| key.ends_with(suffix))
                && !self.put_failed.swap(true, Ordering::SeqCst)
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
            if self.should_fail_put(key) {
                bail!("injected put failure under {key}");
            }
            self.inner.put_bytes(key, bytes, content_type).await
        }

        async fn put_if_absent(
            &self,
            key: &str,
            bytes: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<bool> {
            if self.should_fail_put(key) {
                bail!("injected put failure under {key}");
            }
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
            let mut entries = self.inner.list_dir_entries(prefix).await?;
            if self.fresh_dirty_timestamps && prefix == crate::DIRTY_PREFIX {
                let now = time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)?;
                for entry in &mut entries {
                    entry.last_modified = Some(now.clone());
                }
            }
            Ok(entries)
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
            let result = self.inner.get_with_etag(key).await?;
            if result.is_some()
                && key.ends_with(PROMOTION_LOCK_NAME)
                && !self.lock_read_paused.swap(true, Ordering::SeqCst)
            {
                if let Some(pause) = &self.lock_read_pause {
                    pause.acquired.notify_one();
                    pause.release.notified().await;
                }
            }
            Ok(result)
        }

        async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
            let result = self.inner.put_if_none_match(key, bytes).await?;
            if result.is_some()
                && key.ends_with(PROMOTION_LOCK_NAME)
                && !self.lock_paused.swap(true, Ordering::SeqCst)
            {
                if let Some(pause) = &self.lock_pause {
                    pause.acquired.notify_one();
                    pause.release.notified().await;
                }
            }
            Ok(result)
        }

        async fn put_if_match(
            &self,
            key: &str,
            etag: &str,
            bytes: Vec<u8>,
        ) -> Result<Option<String>> {
            if key == self.key && !self.cas_raced.swap(true, Ordering::SeqCst) {
                if let Some(intent) = &self.intent_before_cas {
                    self.inner.insert(intent, Vec::new());
                }
            }
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

    fn sc(sha: &str, origin: &str, yanked: Yanked, epoch: u64) -> Sidecar {
        Sidecar {
            sha256: sha.to_string(),
            size: 1,
            version: "1.0".to_string(),
            upload_time: "t".to_string(),
            requires_python: None,
            yanked,
            origin: Some(origin.to_string()),
            yank_epoch: epoch,
        }
    }

    fn live(sha: &str, origin: &str) -> Record {
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

    async fn begin_staged_claim(storage: &dyn Storage, staged: &StagedPackage) {
        begin_private_promotion(storage, &staged.manifest.package, &staged.manifest_key)
            .await
            .unwrap()
            .unwrap();
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
    fn byte_conflict_freezes() {
        // Same filename, both private, different bytes: the split-brain freeze.
        assert_eq!(
            decide(&live("x", PRIVATE), &live("y", PRIVATE)),
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
        assert!(src.head_exists(&key).await.unwrap());
        assert!(dst.head_exists(&key).await.unwrap());
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
        assert!(a.head_exists(&key).await.unwrap());
        assert!(b.head_exists(&key).await.unwrap());
        assert_eq!(a.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 1);
        assert_eq!(b.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 1);
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
        assert!(storage.head_exists(&key).await.unwrap());
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
        assert!(storage.head_exists(&key).await.unwrap());
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
        assert!(storage.inner.head_exists(&key).await.unwrap());
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
    async fn late_mirror_record_under_private_claim_is_repaired_by_package_staging() {
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
        // The package claim was demoted after the proxy's final mirror check,
        // but its artifact+sidecar finished afterward.
        late_mirror.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
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
            late_mirror.list_all(QUARANTINE_PREFIX).await.unwrap().len(),
            1
        );
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
    async fn partial_stage_without_manifest_is_inert() {
        let storage = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(storage.as_ref(), "pkg", filename, b"mirror", MIRROR);
        storage.insert(
            &format!("{REPL_STAGING_PREFIX}pkg/partial/artifact"),
            b"private".to_vec(),
        );
        let state = test_state(storage.clone());

        resume_staged_packages_in(&state, storage.as_ref())
            .await
            .unwrap();

        assert_eq!(
            read_origin(storage.as_ref(), "pkg")
                .await
                .unwrap()
                .as_deref(),
            Some(MIRROR)
        );
        assert_eq!(
            storage
                .get_bytes(&artifact_key("pkg", filename))
                .await
                .unwrap(),
            b"mirror"
        );
        assert!(storage
            .list_all(QUARANTINE_PREFIX)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn complete_stage_resumes_after_claim_cas_and_promotes_whole_package() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let first = "pkg-1.whl";
        let second = "pkg-2.whl";
        let mirror_only = "pkg-0.whl";
        seed_live(src.as_ref(), "pkg", first, b"private one", PRIVATE);
        seed_live(src.as_ref(), "pkg", second, b"private two", PRIVATE);
        seed_live(dst.as_ref(), "pkg", first, b"old mirror one", MIRROR);
        seed_live(dst.as_ref(), "pkg", mirror_only, b"old mirror zero", MIRROR);
        dst.insert(
            &status::status_key("pkg"),
            br#"{"status":"quarantined","reason":"upstream mirror","pypiron-epoch":50}"#.to_vec(),
        );
        let state = test_state(dst.clone());

        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        assert!(dst.head_exists(&staged.manifest_key).await.unwrap());
        assert_eq!(
            read_origin(dst.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(MIRROR)
        );
        assert_eq!(
            dst.get_bytes(&artifact_key("pkg", first)).await.unwrap(),
            b"old mirror one"
        );

        // Simulate a crash immediately after the one package-claim CAS. The
        // retry has only the durable destination stage; the source is unused.
        begin_staged_claim(dst.as_ref(), &staged).await;
        drop(src);
        assert!(resume_staged_package(&state, dst.as_ref(), "pkg")
            .await
            .unwrap());

        assert_eq!(
            dst.get_bytes(&artifact_key("pkg", first)).await.unwrap(),
            b"private one"
        );
        assert_eq!(
            dst.get_bytes(&artifact_key("pkg", second)).await.unwrap(),
            b"private two"
        );
        assert!(dst
            .head_exists(&artifact_key("pkg", mirror_only))
            .await
            .unwrap());
        assert!(!dst
            .head_exists(&tombstone_key(&artifact_key("pkg", mirror_only)))
            .await
            .unwrap());
        assert_eq!(dst.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 2);
        let index = String::from_utf8(
            dst.get_bytes(&format!("{}pkg/index.json", crate::SIMPLE_PREFIX))
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(index.contains(first));
        assert!(index.contains(second));
        assert!(!index.contains(mirror_only));
        assert!(read_origin_observation(dst.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap()
            .pending_manifest
            .is_none());
        assert!(!dst
            .list_all(REPL_STAGING_PREFIX)
            .await
            .unwrap()
            .iter()
            .any(|object| object.key.contains("/manifest@")));
        assert_eq!(
            status::read_status(dst.as_ref(), "pkg").await.unwrap(),
            status::ProjectStatusDoc::default(),
            "mirror-local status must not launder through private demotion"
        );
        assert_eq!(
            status::read_status_versioned(dst.as_ref(), "pkg")
                .await
                .unwrap()
                .unwrap()
                .epoch,
            0,
            "mirror-local epochs must not inflate private history",
        );
    }

    #[tokio::test]
    async fn concurrent_same_manifest_recovery_has_one_lock_winner() {
        let src = Arc::new(InMemStorage::default());
        let pause = Arc::new(LockPause {
            acquired: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let dst = Arc::new(
            RaceOnCreateStorage::new("never-raced".into(), Vec::new())
                .pausing_first_promotion_lock(pause.clone()),
        );
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(&dst.inner, "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let state = test_state(dst.clone());
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();

        let mut winner = Box::pin(settle_staged_package(&state, dst.as_ref(), &staged));
        tokio::select! {
            () = pause.acquired.notified() => {}
            result = &mut winner => panic!("winner completed before lock pause: {result:?}"),
        }
        assert!(!settle_staged_package(&state, dst.as_ref(), &staged)
            .await
            .unwrap());
        pause.release.notify_one();
        assert!(winner.await.unwrap());

        assert_eq!(
            dst.get_bytes(&artifact_key("pkg", "pkg-1.whl"))
                .await
                .unwrap(),
            b"private"
        );
        assert!(!dst.head_exists(&staged.manifest_key).await.unwrap());
        assert!(!dst
            .list_all(crate::DIRTY_PREFIX)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.key.ends_with(".intent")));
        let lock: PromotionLockBody =
            serde_json::from_slice(&dst.get_bytes(&promotion_lock_key("pkg")).await.unwrap())
                .unwrap();
        assert!(matches!(lock, PromotionLockBody::Free { .. }));
    }

    #[tokio::test]
    async fn slow_lock_guard_crossing_heartbeat_does_not_deadlock_itself() {
        let src = Arc::new(InMemStorage::default());
        let pause = Arc::new(LockPause {
            acquired: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let dst = Arc::new(
            RaceOnCreateStorage::new("never-raced".into(), Vec::new())
                .pausing_first_promotion_lock_read(pause.clone()),
        );
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(&dst.inner, "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let mut state = test_state(dst.clone());
        state.intent_grace = time::Duration::milliseconds(30);
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();

        let release_guard = async {
            pause.acquired.notified().await;
            // Effective lock grace is 30 ms, so at least one heartbeat crosses
            // the deliberately stalled ownership GET.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            pause.release.notify_one();
        };
        let (settled, ()) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(
                settle_staged_package(&state, dst.as_ref(), &staged),
                release_guard
            )
        })
        .await
        .expect("promotion lock guard deadlocked with its heartbeat");
        assert!(settled.unwrap());
    }

    #[tokio::test]
    async fn stale_promotion_lock_requires_one_full_unchanged_etag_grace() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let mut state = test_state(dst.clone());
        state.intent_grace = time::Duration::ZERO;
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        let old_holder = "old-holder";
        dst.insert(
            &format!("{}pkg!{old_holder}.intent", crate::DIRTY_PREFIX),
            Vec::new(),
        );
        dst.put_if_none_match(
            &promotion_lock_key("pkg"),
            serde_json::to_vec(&PromotionLockBody::Held {
                holder: old_holder.into(),
                manifest: staged.manifest_key.clone(),
                nonce: worker::marker_nonce(),
            })
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        let attempt = worker::mark_intent(dst.as_ref(), "pkg").await.unwrap();

        assert!(
            acquire_promotion_lock(&state, dst.as_ref(), &staged, &attempt)
                .await
                .unwrap()
                .is_none()
        );
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        let lease = acquire_promotion_lock(&state, dst.as_ref(), &staged, &attempt)
            .await
            .unwrap()
            .expect("unchanged crashed lock should be stealable after grace");
        lease.release(dst.as_ref()).await.unwrap();
        worker::clear_intent(dst.as_ref(), "pkg", &attempt)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn live_rotated_intent_family_prevents_promotion_lock_takeover() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(
            RaceOnCreateStorage::new("never-raced".into(), Vec::new())
                .with_fresh_dirty_timestamps(),
        );
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(&dst.inner, "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let mut state = test_state(dst.clone());
        state.intent_grace = time::Duration::milliseconds(30);
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        let root = "pair-root";
        for suffix in [".intent", ".commit"] {
            dst.inner.insert(
                &format!("{}pkg!{root}{suffix}", crate::DIRTY_PREFIX),
                Vec::new(),
            );
        }
        dst.inner.insert(
            &format!("{}pkg!{root}~0.intent", crate::DIRTY_PREFIX),
            Vec::new(),
        );
        dst.put_if_none_match(
            &promotion_lock_key("pkg"),
            serde_json::to_vec(&PromotionLockBody::Held {
                holder: root.into(),
                manifest: staged.manifest_key.clone(),
                nonce: worker::marker_nonce(),
            })
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        let attempt = worker::mark_intent(dst.as_ref(), "pkg").await.unwrap();

        assert!(
            acquire_promotion_lock(&state, dst.as_ref(), &staged, &attempt)
                .await
                .unwrap()
                .is_none()
        );
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        assert!(
            acquire_promotion_lock(&state, dst.as_ref(), &staged, &attempt)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn rotating_pair_heartbeat_survives_old_marker_snapshot_deletion() {
        let left = InMemStorage::default();
        let right = InMemStorage::default();
        let mut state = test_state(Arc::new(InMemStorage::default()));
        state.intent_grace = time::Duration::milliseconds(30);
        let intents = acquire_replication_intents(&left, &right, "pkg")
            .await
            .unwrap();
        let old_left = format!("{}pkg!{}.intent", crate::DIRTY_PREFIX, intents.left);
        let old_right = format!("{}pkg!{}.intent", crate::DIRTY_PREFIX, intents.right);
        let release = tokio::sync::Notify::new();
        let operation = async {
            release.notified().await;
            Ok(())
        };
        let mut running = Box::pin(run_with_replication_heartbeats(
            &state, &left, &right, "pkg", &intents, operation,
        ));
        let heartbeat_arrives = async {
            loop {
                if left
                    .list_all(crate::DIRTY_PREFIX)
                    .await
                    .unwrap()
                    .iter()
                    .any(|entry| entry.key.contains('~') && entry.key.ends_with(".intent"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::select! {
            () = heartbeat_arrives => {}
            result = &mut running => panic!("heartbeat operation ended early: {result:?}"),
        }

        // A worker may delete exactly the keys it snapshotted before rotation.
        // The fresh, unique family member must remain as the package fence.
        left.delete_keys(&[old_left]).await.unwrap();
        right.delete_keys(&[old_right]).await.unwrap();
        assert!(worker::has_unpaired_intent_ignoring(&left, "pkg", None)
            .await
            .unwrap());
        assert!(worker::has_unpaired_intent_ignoring(&right, "pkg", None)
            .await
            .unwrap());

        release.notify_one();
        running.await.unwrap();
        release_replication_intents(&left, &right, "pkg", &intents)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn overlapping_manifests_keep_shared_members_for_later_recovery() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"one", PRIVATE);
        seed_live(dst.as_ref(), "pkg", "old-1.whl", b"mirror", MIRROR);
        let state = test_state(dst.clone());

        let first = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        seed_live(src.as_ref(), "pkg", "pkg-2.whl", b"two", PRIVATE);
        let second = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();

        assert!(settle_staged_package(&state, dst.as_ref(), &first)
            .await
            .unwrap());
        drop(src);
        assert!(settle_staged_package(&state, dst.as_ref(), &second)
            .await
            .unwrap());
        assert_eq!(
            dst.get_bytes(&artifact_key("pkg", "pkg-2.whl"))
                .await
                .unwrap(),
            b"two"
        );
    }

    #[tokio::test]
    async fn staged_promotion_heals_a_provably_stale_writer_intent() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let mut state = test_state(dst.clone());
        state.intent_grace = time::Duration::ZERO;
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        // Timestamp zero is provably stale. The exact pending owner may retire
        // it and heal the crash shape instead of waiting forever.
        dst.insert("_dirty/pkg!0-1-0.intent", Vec::new());

        assert!(settle_staged_package(&state, dst.as_ref(), &staged)
            .await
            .unwrap());
        assert_eq!(
            read_origin(dst.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
        assert!(!dst.head_exists("_dirty/pkg!0-1-0.intent").await.unwrap());
    }

    #[tokio::test]
    async fn writer_starting_during_claim_cas_finishes_before_promotion() {
        let src = Arc::new(InMemStorage::default());
        let origin_key = crate::origin::origin_key("pkg");
        let intent_key = format!(
            "{}pkg!{}.intent",
            crate::DIRTY_PREFIX,
            worker::marker_nonce()
        );
        let dst = Arc::new(
            RaceOnCreateStorage::new(origin_key, Vec::new())
                .injecting_intent_before_cas(intent_key.clone()),
        );
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(&dst.inner, "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let mut state = test_state(dst.clone());
        // InMemStorage exposes a fixed old storage timestamp. Keep it inside a
        // deliberately huge grace for the first attempt, then expire it below.
        state.intent_grace = time::Duration::weeks(10_000);
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();

        let commit_key = intent_key.replace(".intent", ".commit");
        let writer_finishes = async {
            loop {
                let pending = read_origin_observation(dst.as_ref(), "pkg")
                    .await
                    .unwrap()
                    .and_then(|claim| claim.pending_manifest)
                    .is_some();
                if pending {
                    dst.inner.insert(&commit_key, Vec::new());
                    break;
                }
                tokio::task::yield_now().await;
            }
        };
        let (settled, ()) = tokio::join!(
            settle_staged_package(&state, dst.as_ref(), &staged),
            writer_finishes
        );
        assert!(settled.unwrap());
        assert_eq!(
            dst.get_bytes(&artifact_key("pkg", "pkg-1.whl"))
                .await
                .unwrap(),
            b"private"
        );
    }

    #[tokio::test]
    async fn stale_promotion_worker_cannot_mutate_after_owner_changes() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let state = test_state(dst.clone());
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        let stale = begin_private_promotion(dst.as_ref(), "pkg", &staged.manifest_key)
            .await
            .unwrap()
            .unwrap();
        let attempt = worker::mark_intent(dst.as_ref(), "pkg").await.unwrap();
        let lease = acquire_promotion_lock(&state, dst.as_ref(), &staged, &attempt)
            .await
            .unwrap()
            .unwrap();
        dst.insert(
            &crate::origin::origin_key("pkg"),
            serde_json::to_vec(&serde_json::json!({
                "origin": PRIVATE,
                "nonce": "0123456789abcdef0123456789abcdef",
                "pending-manifest": "_staging/repl/pkg/manifest@new-owner.json"
            }))
            .unwrap(),
        );

        let error = promote_staged_package(&state, dst.as_ref(), &staged, &stale, &lease)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("promotion owner changed"));
        assert_eq!(
            dst.get_bytes(&artifact_key("pkg", "pkg-1.whl"))
                .await
                .unwrap(),
            b"mirror"
        );
    }

    #[tokio::test]
    async fn promotion_retry_reasserts_dirty_after_truth_already_converged() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(
            RaceOnCreateStorage::new("never-raced".into(), Vec::new())
                .failing_first_put_under_ending(crate::DIRTY_PREFIX, ".commit"),
        );
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(&dst.inner, "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let state = test_state(dst.clone());
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();

        let first = settle_staged_package(&state, dst.as_ref(), &staged).await;
        assert!(first.is_err());
        assert_eq!(
            dst.get_bytes(&artifact_key("pkg", "pkg-1.whl"))
                .await
                .unwrap(),
            b"private"
        );
        assert!(read_origin_observation(dst.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap()
            .pending_manifest
            .is_some());

        assert!(settle_staged_package(&state, dst.as_ref(), &staged)
            .await
            .unwrap());
        assert!(!dst.list_all(crate::DIRTY_PREFIX).await.unwrap().is_empty());
        assert!(read_origin_observation(dst.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap()
            .pending_manifest
            .is_none());
    }

    #[tokio::test]
    async fn topology_fence_blocks_stage_claim_transition() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let state = test_state(dst.clone());
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        state
            .writes_fenced
            .store(true, std::sync::atomic::Ordering::Release);

        assert!(settle_staged_package(&state, dst.as_ref(), &staged)
            .await
            .is_err());
        assert_eq!(
            read_origin(dst.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(MIRROR)
        );
        assert!(dst.head_exists(&staged.manifest_key).await.unwrap());
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
        status::advance_status(&b, "pkg", &late, Some(status::StatusOrigin::Mirror))
            .await
            .unwrap();

        reconcile_project_status(&a, &b, "pkg").await.unwrap();
        let normalized = status::read_status_versioned(&b, "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(normalized.doc, status::ProjectStatusDoc::default());
        assert_eq!(normalized.epoch, 0);
        assert_eq!(normalized.origin, Some(status::StatusOrigin::Private));
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
    async fn demotion_quarantines_orphan_and_legacy_mirror_leftovers() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", "bar-1.whl", b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", "bar-1.whl", b"mirror", MIRROR);

        let orphan = artifact_key("pkg", "orphan-1.whl");
        dst.insert(&orphan, b"orphan mirror body".to_vec());
        let legacy = artifact_key("pkg", "legacy-1.whl");
        dst.insert(&legacy, b"legacy mirror body".to_vec());
        let mut legacy_sidecar = sc(
            &sha256_hex(b"legacy mirror body"),
            MIRROR,
            Yanked::Flag(false),
            0,
        );
        legacy_sidecar.origin = None;
        dst.insert(
            &sidecar_key(&legacy),
            serde_json::to_vec(&legacy_sidecar).unwrap(),
        );
        let state = test_state(dst.clone());

        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        assert_eq!(staged.manifest.mirror_leftovers.len(), 2);
        begin_staged_claim(dst.as_ref(), &staged).await;
        assert!(settle_staged_package(&state, dst.as_ref(), &staged)
            .await
            .unwrap());

        for key in [&orphan, &legacy] {
            assert!(dst.head_exists(key).await.unwrap());
            assert!(dst.head_exists(&mirror_quarantined_key(key)).await.unwrap());
        }
        let quarantined = dst.list_all(QUARANTINE_PREFIX).await.unwrap();
        assert!(quarantined
            .iter()
            .any(|object| object.key.contains("orphan-1.whl@")));
        assert!(quarantined
            .iter()
            .any(|object| object.key.contains("legacy-1.whl@")));
    }

    #[tokio::test]
    async fn demotion_manifest_carries_tombstone_and_freeze_fences() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let deleted = "deleted-1.whl";
        let frozen = "frozen-1.whl";
        seed_live(src.as_ref(), "pkg", "bar-1.whl", b"private", PRIVATE);
        seed_live(src.as_ref(), "pkg", deleted, b"deleted private", PRIVATE);
        tombstone_side(src.as_ref(), "pkg", deleted).await.unwrap();
        seed_live(src.as_ref(), "pkg", frozen, b"frozen private", PRIVATE);
        freeze_side(src.as_ref(), "pkg", frozen).await.unwrap();
        seed_live(dst.as_ref(), "pkg", "bar-1.whl", b"mirror", MIRROR);
        seed_live(dst.as_ref(), "pkg", deleted, b"deleted mirror", MIRROR);
        seed_live(dst.as_ref(), "pkg", frozen, b"frozen mirror", MIRROR);
        let state = test_state(dst.clone());

        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        begin_staged_claim(dst.as_ref(), &staged).await;
        assert!(resume_staged_package(&state, dst.as_ref(), "pkg")
            .await
            .unwrap());

        let deleted_key = artifact_key("pkg", deleted);
        assert!(dst.head_exists(&tombstone_key(&deleted_key)).await.unwrap());
        assert!(!dst.head_exists(&deleted_key).await.unwrap());
        let frozen_key_name = artifact_key("pkg", frozen);
        assert!(dst
            .head_exists(&tombstone_key(&frozen_key_name))
            .await
            .unwrap());
        assert!(dst
            .head_exists(&frozen_key(&frozen_key_name))
            .await
            .unwrap());
        assert!(dst.head_exists(&frozen_key_name).await.unwrap());
    }

    #[tokio::test]
    async fn demotion_preserves_source_epoch_so_later_private_status_wins() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(src.as_ref(), "pkg", filename, b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", filename, b"mirror", MIRROR);
        let quarantined = status::ProjectStatusDoc {
            status: status::ProjectStatus::Quarantined,
            reason: Some("private review".into()),
        };
        assert_eq!(
            status::advance_status(
                src.as_ref(),
                "pkg",
                &quarantined,
                Some(status::StatusOrigin::Private),
            )
            .await
            .unwrap(),
            1
        );
        dst.insert(
            &status::status_key("pkg"),
            br#"{"status":"quarantined","reason":"private review","pypiron-epoch":50}"#.to_vec(),
        );
        let state = test_state(dst.clone());
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        begin_staged_claim(dst.as_ref(), &staged).await;
        assert!(settle_staged_package(&state, dst.as_ref(), &staged)
            .await
            .unwrap());

        let demoted = status::read_status_versioned(dst.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(demoted.doc, quarantined);
        assert_eq!(demoted.epoch, 1);

        let active = status::ProjectStatusDoc::default();
        assert_eq!(
            status::advance_status(
                src.as_ref(),
                "pkg",
                &active,
                Some(status::StatusOrigin::Private),
            )
            .await
            .unwrap(),
            2
        );
        reconcile_project_status(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        let converged = status::read_status_versioned(dst.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(converged.doc, active);
        assert_eq!(converged.epoch, 2);
    }

    #[tokio::test]
    async fn stale_manifest_replay_preserves_later_private_status() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(src.as_ref(), "pkg", filename, b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", filename, b"mirror", MIRROR);
        let staged_status = status::ProjectStatusDoc {
            status: status::ProjectStatus::Quarantined,
            reason: Some("staged".into()),
        };
        status::advance_status(
            src.as_ref(),
            "pkg",
            &staged_status,
            Some(status::StatusOrigin::Private),
        )
        .await
        .unwrap();
        dst.insert(
            &status::status_key("pkg"),
            br#"{"status":"archived","reason":"upstream mirror","pypiron-epoch":50}"#.to_vec(),
        );
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        begin_staged_claim(dst.as_ref(), &staged).await;
        rebase_status_after_demotion(dst.as_ref(), "pkg", &staged.manifest.status)
            .await
            .unwrap();

        let later = status::ProjectStatusDoc {
            status: status::ProjectStatus::Active,
            reason: None,
        };
        assert_eq!(
            status::advance_status(
                dst.as_ref(),
                "pkg",
                &later,
                Some(status::StatusOrigin::Private),
            )
            .await
            .unwrap(),
            2
        );

        // A transient failure deleting the manifest makes recovery invoke the
        // status step again. Its captured mirror etag is stale, so this replay
        // must not replace the acknowledged private event.
        rebase_status_after_demotion(dst.as_ref(), "pkg", &staged.manifest.status)
            .await
            .unwrap();
        let current = status::read_status_versioned(dst.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.doc, later);
        assert_eq!(current.epoch, 2);
    }

    #[tokio::test]
    async fn demotion_replaces_a_mirror_status_written_after_staging() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", "pkg-1.whl", b"mirror", MIRROR);
        let private_status = status::ProjectStatusDoc {
            status: status::ProjectStatus::Quarantined,
            reason: Some("private review".into()),
        };
        status::advance_status(
            src.as_ref(),
            "pkg",
            &private_status,
            Some(status::StatusOrigin::Private),
        )
        .await
        .unwrap();
        let initial_mirror = status::ProjectStatusDoc {
            status: status::ProjectStatus::Archived,
            reason: Some("initial upstream".into()),
        };
        status::advance_status(
            dst.as_ref(),
            "pkg",
            &initial_mirror,
            Some(status::StatusOrigin::Mirror),
        )
        .await
        .unwrap();
        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();

        let late_mirror = status::ProjectStatusDoc {
            status: status::ProjectStatus::Deprecated,
            reason: Some("late upstream".into()),
        };
        status::advance_status(
            dst.as_ref(),
            "pkg",
            &late_mirror,
            Some(status::StatusOrigin::Mirror),
        )
        .await
        .unwrap();
        begin_staged_claim(dst.as_ref(), &staged).await;
        assert!(
            settle_staged_package(&test_state(dst.clone()), dst.as_ref(), &staged)
                .await
                .unwrap()
        );

        let current = status::read_status_versioned(dst.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.doc, private_status);
        assert_eq!(current.epoch, 1);
        assert_eq!(current.origin, Some(status::StatusOrigin::Private));
    }

    #[tokio::test]
    async fn private_repair_manifest_never_rebases_newer_private_status() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        seed_live(src.as_ref(), "pkg", "pkg-1.whl", b"same", PRIVATE);
        seed_live(dst.as_ref(), "pkg", "pkg-1.whl", b"same", MIRROR);
        // The package claim is already private; only the stale per-file mirror
        // classification needs repair.
        dst.insert(&crate::origin::origin_key("pkg"), b"private".to_vec());
        let old = status::ProjectStatusDoc {
            status: status::ProjectStatus::Archived,
            reason: Some("old source".into()),
        };
        status::advance_status(
            src.as_ref(),
            "pkg",
            &old,
            Some(status::StatusOrigin::Private),
        )
        .await
        .unwrap();
        let newer = status::ProjectStatusDoc {
            status: status::ProjectStatus::Quarantined,
            reason: Some("new destination".into()),
        };
        status::advance_status(
            dst.as_ref(),
            "pkg",
            &newer,
            Some(status::StatusOrigin::Private),
        )
        .await
        .unwrap();
        status::advance_status(
            dst.as_ref(),
            "pkg",
            &newer,
            Some(status::StatusOrigin::Private),
        )
        .await
        .unwrap();

        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        assert_eq!(staged.manifest.mode, StagedMode::PrivateRepair);
        assert!(
            settle_staged_package(&test_state(dst.clone()), dst.as_ref(), &staged)
                .await
                .unwrap()
        );
        let current = status::read_status_versioned(dst.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.doc, newer);
        assert_eq!(current.epoch, 2);
    }

    #[tokio::test]
    async fn tombstone_racing_a_complete_stage_prevents_resurrection() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let akey = artifact_key("pkg", filename);
        seed_live(src.as_ref(), "pkg", filename, b"private", PRIVATE);
        seed_live(dst.as_ref(), "pkg", filename, b"mirror", MIRROR);
        let state = test_state(dst.clone());

        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        begin_staged_claim(dst.as_ref(), &staged).await;
        dst.insert(&tombstone_key(&akey), b"{}".to_vec());

        assert!(resume_staged_package(&state, dst.as_ref(), "pkg")
            .await
            .unwrap());
        assert!(!dst.head_exists(&akey).await.unwrap());
        assert!(dst.head_exists(&tombstone_key(&akey)).await.unwrap());
        assert!(!dst
            .list_all(REPL_STAGING_PREFIX)
            .await
            .unwrap()
            .iter()
            .any(|object| object.key.contains("/manifest@")));
    }

    #[tokio::test]
    async fn post_stage_writer_with_stale_mirror_sidecar_freezes_both_bodies() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let akey = artifact_key("pkg", filename);
        seed_live(src.as_ref(), "pkg", filename, b"private-a", PRIVATE);
        seed_live(dst.as_ref(), "pkg", filename, b"mirror", MIRROR);
        let state = test_state(dst.clone());

        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        begin_staged_claim(dst.as_ref(), &staged).await;
        // The body changes after staging but before its writer publishes a new
        // sidecar. Classification by the stale mirror sidecar would overwrite
        // an acknowledged private body; the captured artifact etag catches it.
        dst.insert(&akey, b"private-b".to_vec());

        assert!(resume_staged_package(&state, dst.as_ref(), "pkg")
            .await
            .unwrap());
        assert!(dst.head_exists(&akey).await.unwrap());
        assert!(dst.head_exists(&frozen_key(&akey)).await.unwrap());
        assert_eq!(dst.list_all(QUARANTINE_PREFIX).await.unwrap().len(), 2);
        assert!(!dst
            .list_all(REPL_STAGING_PREFIX)
            .await
            .unwrap()
            .iter()
            .any(|object| object.key.contains("/manifest@")));
    }

    #[tokio::test]
    async fn staged_promotion_does_not_regress_a_private_yank_epoch() {
        let src = Arc::new(InMemStorage::default());
        let dst = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        let akey = artifact_key("pkg", filename);
        seed_live(src.as_ref(), "pkg", filename, b"same", PRIVATE);
        seed_live(dst.as_ref(), "pkg", filename, b"same", MIRROR);
        let state = test_state(dst.clone());

        let staged = stage_private_package(src.as_ref(), dst.as_ref(), "pkg")
            .await
            .unwrap();
        begin_staged_claim(dst.as_ref(), &staged).await;
        dst.insert(
            &sidecar_key(&akey),
            serde_json::to_vec(&sc(
                &sha256_hex(b"same"),
                PRIVATE,
                Yanked::Reason("newer".into()),
                9,
            ))
            .unwrap(),
        );

        assert!(resume_staged_package(&state, dst.as_ref(), "pkg")
            .await
            .unwrap());
        let sidecar: Sidecar =
            serde_json::from_slice(&dst.get_bytes(&sidecar_key(&akey)).await.unwrap()).unwrap();
        assert_eq!(sidecar.yank_epoch, 9);
        assert_eq!(sidecar.yanked, Yanked::Reason("newer".into()));
    }

    #[tokio::test]
    async fn queued_markers_are_durable_then_consumed_by_eager_success() {
        let a = Arc::new(InMemStorage::default());
        let b = Arc::new(InMemStorage::default());
        let filename = "pkg-1.whl";
        seed_live(a.as_ref(), "pkg", filename, b"wheel", PRIVATE);
        let state = two_bucket_state(a.clone(), b.clone());
        let pinned = state.pin();

        let markers = queue_fanout_markers(&state, &pinned, "pkg", filename)
            .await
            .unwrap();
        assert_eq!(a.list_all(REPL_PREFIX).await.unwrap().len(), 1);
        eager_fanout(&state, a.as_ref(), 0, "pkg", filename, markers).await;
        assert!(b.head_exists(&artifact_key("pkg", filename)).await.unwrap());
        assert!(a.list_all(REPL_PREFIX).await.unwrap().is_empty());
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
        let bad_manifest = format!("{REPL_STAGING_PREFIX}blocked/manifest@bad.json");
        b.insert(&bad_manifest, b"not json".to_vec());

        // Every background path with an index-aware entry point skips B. The
        // malformed manifest would fail stage recovery if B were read, and the
        // B-only private record would copy to A if the full diff touched it.
        resume_staged_packages(&state).await.unwrap();
        reconcile(&state, &state.pin()).await.unwrap();
        assert!(
            !a.head_exists(&artifact_key("from-b", from_b))
                .await
                .unwrap(),
            "pending topology validation must gate full-diff reads"
        );
        b.delete_keys(&[bad_manifest]).await.unwrap();

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
            assert!(storage
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
    async fn ensure_private_origin_creates_claims_but_refuses_unstaged_transitions() {
        // Absent → creates a private claim.
        let s = InMemStorage::default();
        ensure_private_origin(&s, "fresh").await.unwrap();
        assert_eq!(
            read_origin(&s, "fresh").await.unwrap().as_deref(),
            Some(PRIVATE)
        );

        // Mirror demotion must carry a committed package stage.
        claim_origin(&s, "was-mirror", MIRROR).await.unwrap();
        let error = ensure_private_origin(&s, "was-mirror").await.unwrap_err();
        assert!(error.to_string().contains("staged private demotion"));
        assert_eq!(
            read_origin(&s, "was-mirror").await.unwrap().as_deref(),
            Some(MIRROR)
        );

        // Already private → idempotent no-op.
        ensure_private_origin(&s, "fresh").await.unwrap();
        assert_eq!(
            read_origin(&s, "fresh").await.unwrap().as_deref(),
            Some(PRIVATE)
        );

        begin_private_promotion(&s, "fresh", "_staging/repl/fresh/manifest@pending.json")
            .await
            .unwrap()
            .unwrap();
        let error = ensure_private_origin(&s, "fresh").await.unwrap_err();
        assert!(error.to_string().contains("under staged promotion"));
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
        status::advance_status(
            a.as_ref(),
            "pkg",
            &quarantined,
            Some(status::StatusOrigin::Private),
        )
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
        assert!(!b.list_all(crate::DIRTY_PREFIX).await.unwrap().is_empty());
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
        status::advance_status(
            a.as_ref(),
            "pkg",
            &archived,
            Some(status::StatusOrigin::Mirror),
        )
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
