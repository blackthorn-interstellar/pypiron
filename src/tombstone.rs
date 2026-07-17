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

use crate::sidecar::{frozen_key, metadata_key, provenance_key, sidecar_key, tombstone_key};
use crate::storage::Storage;
use crate::PACKAGES_PREFIX;

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
