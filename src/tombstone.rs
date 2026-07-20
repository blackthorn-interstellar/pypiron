//! Delete tombstones: `<filename>.tombstone` beside where a private artifact
//! lived (dev/MULTIBUCKET.md §6.4). A deleted private filename may never be
//! reused (PyPI semantics), and a crashed delete must converge to "gone" rather
//! than resurrect the file. Tombstones are written before the artifact is
//! removed, checked on every write path, filtered out of indexes, and never
//! lifecycle-expired (expiry would resurrect the delete). Mirror deletions are
//! local cache management and are never tombstoned — a cached upstream file
//! must stay re-fillable forever.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::app::PACKAGES_PREFIX;
use crate::sidecar::{
    frozen_key, metadata_key, mirror_quarantined_key, provenance_key, sidecar_key, tombstone_key,
};
use crate::storage::Storage;

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
/// its own just-written body *after* the background audit read that body and
/// fabricated a sidecar for it (worker::backfill_sidecar's confirm/retract has a
/// race window and a non-durable retract), and an interrupted sidecar-first
/// replication copy (replicate::copy_live) that never reaches its artifact
/// write. With no artifact to list and no `.tombstone` to trigger
/// [`complete_interrupted_deletes`], such companions are invisible to every
/// future rebuild, and the cross-bucket merge reads them as `Absent`
/// (RecordState maps sidecar-without-artifact to `Absent`) so `decide` returns
/// `Noop` — the debris then survives every diff and diverges the buckets
/// forever. `filenames` are artifact base names the audit already observed with
/// a companion but no anchoring body/marker in the same listing. Each base is
/// re-verified against a fresh HEAD before its companions are removed, so a
/// body or marker that landed after the listing (a sidecar-first copy about to
/// write its artifact, or a fresh upload) is never clobbered. Returns how many
/// bases were cleaned.
pub async fn drop_orphan_companions(
    storage: &dyn Storage,
    pkg: &str,
    filenames: &[String],
) -> Result<usize> {
    let mut dropped = 0;
    for filename in filenames {
        let akey = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        // Any anchor re-observed under a fresh read means the companions still
        // describe real evidence (a live body, a fenced name, or quarantined
        // canonical bytes) — leave them untouched.
        if storage.head_exists(&akey).await?
            || storage.head_exists(&tombstone_key(&akey)).await?
            || storage.head_exists(&frozen_key(&akey)).await?
            || storage.head_exists(&mirror_quarantined_key(&akey)).await?
        {
            continue;
        }
        storage
            .delete_keys(&[
                sidecar_key(&akey),
                metadata_key(&akey),
                provenance_key(&akey),
            ])
            .await?;
        dropped += 1;
    }
    Ok(dropped)
}
