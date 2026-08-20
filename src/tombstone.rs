//! Delete tombstones: `<filename>.tombstone` beside where a private artifact
//! lived. A deleted private filename may never be
//! reused (PyPI semantics), and a crashed delete must converge to "gone" rather
//! than resurrect the file. Tombstones are written before the artifact is
//! removed, checked on every write path, filtered out of indexes, and never
//! lifecycle-expired (expiry would resurrect the delete). Mirror deletions are
//! local cache management and are never tombstoned — a cached upstream file
//! must stay re-fillable forever.

use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

use crate::app::{AppState, PACKAGES_PREFIX};
use crate::sidecar::{
    frozen_key, metadata_key, mirror_quarantined_key, provenance_key, sidecar_key, tombstone_key,
};
use crate::storage::Storage;

/// Floor for [`drop_orphan_companions`]' age gate, independent of
/// `--intent-grace-secs`: long enough that no sidecar-first writer can still be
/// mid-flight, short enough that real debris is swept the same day.
const AGE_FLOOR: time::Duration = time::Duration::seconds(300);

/// Minimal tombstone body. The filename is informational — the key already
/// carries it — and there is deliberately no wall clock: a tombstone's meaning
/// is binary existence, and the cross-bucket merge is clock-free.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tombstone {
    pub filename: String,
}

/// Tombstone `artifact_key` before its artifact is removed. A checked
/// create-if-absent: unlike the best-effort sidecar deletes it sits beside, a
/// failed tombstone must abort the delete, or the filename could be silently
/// reused. Idempotent — an already-present tombstone is success.
pub async fn write(storage: &dyn Storage, artifact_key: &str, filename: &str) -> Result<()> {
    let body = serde_json::to_vec(&Tombstone {
        filename: filename.to_string(),
    })?;
    storage
        .put_if_absent(&tombstone_key(artifact_key), body, Some("application/json"))
        .await?;
    Ok(())
}

/// Finish a delete that crashed between writing the tombstone and removing the
/// body. `filenames` are artifact base names the audit already observed sitting
/// beside their own `.tombstone` in the same listing (single- and multi-bucket
/// alike): the tombstone fences the name but the live bytes stayed downloadable
/// by direct URL. Drop the body and its companions, keeping the tombstone — the
/// exact tail of the ordinary delete path. Returns how many bodies were dropped.
/// The tombstone HEAD is re-confirmed so a stale listing can never delete a body
/// that is not actually fenced, and `.frozen` absence is re-confirmed so a frozen
/// conflict (a preserved body behind a tombstone + freeze marker) is never
/// mistaken for a crashed delete and dropped. Write ordering already keeps the
/// two apart; this makes the invariant explicit at delete time.
pub async fn complete_interrupted_deletes(
    storage: &dyn Storage,
    pkg: &str,
    filenames: &[String],
) -> Result<usize> {
    let mut completed = 0;
    for filename in filenames {
        let akey = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        if !storage.head_exists(&tombstone_key(&akey)).await? {
            continue;
        }
        if storage.head_exists(&frozen_key(&akey)).await? {
            continue;
        }
        // The caller flags filenames whose listing shows record objects left
        // beside a bare tombstone — a live body (crash before the artifact
        // delete) or an orphaned sidecar/companion (crash between the artifact
        // delete and the companion deletes). Either way the tombstone
        // authorizes dropping whatever remains.
        storage
            .delete_keys(&[
                akey.clone(),
                sidecar_key(&akey),
                metadata_key(&akey),
                provenance_key(&akey),
            ])
            .await?;
        completed += 1;
    }
    Ok(completed)
}

/// Drop sidecar/companion objects stranded beside an artifact that no longer
/// exists and carries no deliberate marker. Two crash-safe writers can leave
/// this debris: a failed upload whose `store_artifact_verified` rollback deletes
/// its own just-written body (only when a HEAD proved it corrupt — an
/// unverifiable write leaves the bytes standing) *after* the background audit read that body and
/// fabricated a sidecar for it (worker::backfill_sidecar's confirm/retract has a
/// race window and a non-durable retract), and an interrupted sidecar-first
/// replication copy (replicate::copy_live) that never reaches its artifact
/// write. With no artifact to list and no `.tombstone` to trigger
/// [`complete_interrupted_deletes`], such companions are invisible to every
/// future rebuild, and the cross-bucket merge reads them as `Absent`
/// (RecordState maps sidecar-without-artifact to `Absent`) so `decide` returns
/// `Noop` — the debris then survives every diff and diverges the buckets
/// forever. `filenames` are artifact base names the audit already observed with
/// a companion but no anchoring body/marker in the same listing.
///
/// Every writer that touches an artifact publishes its companions and its body
/// as separate objects, and two of them publish the sidecar *first* by design
/// (proxy cache fill, replication's server-side copy), so between the two
/// writes a live upload is indistinguishable from debris by shape alone. Three
/// gates separate them, and all three must pass before anything is deleted:
///
/// - one fresh `packages/<pkg>/` listing re-checks all four anchors (body,
///   `.tombstone`, `.frozen`, `.mirror-quarantined`) — a stale audit listing
///   never authorizes a delete on its own;
/// - no companion younger than [`AGE_FLOOR`] (or `state.intent_grace`, whichever
///   is longer) is ever dropped. Debris is old by definition; no writer sits a
///   whole grace window between its sidecar and its body, so the age gate is
///   what actually closes the window — no re-check can shrink it. The window it
///   has to cover differs by writer: `replicate::copy_live` publishes its
///   sidecar as the divergence gate and then copies or streams the *entire*
///   artifact, so its window is a whole transfer; the proxy fill has already
///   spooled and verified the bytes before it writes anything, so its window is
///   one origin re-read plus the spooled-bytes PUT. The long one sets the bar.
///   An absent or unparseable storage timestamp counts as young;
/// - the package must have no live (unpaired, in-grace) intent marker, since
///   all three writer paths declare intent before touching truth.
///
/// Returns how many bases were cleaned.
pub async fn drop_orphan_companions(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    filenames: &[String],
) -> Result<usize> {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let listed: HashMap<String, Option<time::OffsetDateTime>> = storage
        .list_dir_entries(&prefix)
        .await?
        .into_iter()
        .map(|entry| {
            let modified = entry.last_modified.as_deref().and_then(|raw| {
                time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
                    .ok()
            });
            (entry.key, modified)
        })
        .collect();
    let now = crate::clock::now_utc();
    // The sweep's two gates both key off `state.intent_grace`, so lowering it
    // weakens them together — and `--intent-grace-secs` validates all the way
    // down to 3s, which would let this delete a replication copy's sidecar while
    // the copy is still streaming. The age gate therefore never goes below
    // AGE_FLOOR whatever the operator sets. `has_live_intent` keeps the
    // configured grace: that one must stay tunable to heal crashed writers.
    let age_gate = state.intent_grace.max(AGE_FLOOR);

    let mut candidates: Vec<(String, Vec<String>)> = Vec::new();
    let mut kept_young = 0usize;
    let mut kept_untimed = false;
    for filename in filenames {
        let akey = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        // Any anchor re-observed under a fresh read means the companions still
        // describe real evidence (a live body, a fenced name, or quarantined
        // canonical bytes) — leave them untouched.
        if listed.contains_key(&akey)
            || listed.contains_key(&tombstone_key(&akey))
            || listed.contains_key(&frozen_key(&akey))
            || listed.contains_key(&mirror_quarantined_key(&akey))
        {
            continue;
        }
        let companions: Vec<String> = [
            sidecar_key(&akey),
            metadata_key(&akey),
            provenance_key(&akey),
        ]
        .into_iter()
        .filter(|key| listed.contains_key(key))
        .collect();
        if companions.is_empty() {
            continue;
        }
        let aged = companions.iter().all(|key| {
            let modified = listed.get(key).and_then(|modified| *modified);
            if modified.is_none() {
                kept_untimed = true;
            }
            modified.is_some_and(|written| now - written >= age_gate)
        });
        if !aged {
            kept_young += 1;
            continue;
        }
        candidates.push((akey, companions));
    }
    if kept_young > 0 {
        // Otherwise a backend that stopped reporting `last_modified` would keep
        // every candidate young forever and pile up debris behind a clean audit
        // log — the failure is invisible from the outside.
        debug!(
            package = %pkg,
            kept_young,
            untimed = kept_untimed,
            "audit: orphan companions held back by the age gate"
        );
    }
    if candidates.is_empty() {
        return Ok(0);
    }
    // Paid only when a package really has aged anchor-less companions — and it
    // is not cheap: `has_live_intent` lists the whole `_dirty/` prefix and
    // filters in memory, so it costs O(all packages' markers), not O(this
    // package's).
    if crate::worker::has_live_intent(state, storage, pkg).await? {
        return Ok(0);
    }

    let mut dropped = 0;
    for (akey, companions) in candidates {
        storage.delete_keys(&companions).await?;
        dropped += 1;
        // The gates make this vanishingly unlikely, but a body appearing right
        // here means a writer we judged absent was mid-flight and just lost its
        // companions. Never re-put the deleted bytes over a fresh body (that is
        // the permanent divergence replicate::copy_live avoids); mark the
        // package dirty so the sidecar backfill runs on the next drain instead
        // of waiting out the next sweep.
        if storage.head_exists(&akey).await? {
            error!(package=%pkg, key=%akey, "audit: artifact appeared while dropping its orphaned companions — a live writer may have been clobbered; scheduling a rebuild");
            crate::markers::mark_dirty(storage, pkg).await?;
        }
    }
    Ok(dropped)
}
