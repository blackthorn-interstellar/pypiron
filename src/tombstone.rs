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

use crate::sidecar::tombstone_key;
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

/// Whether `artifact_key`'s filename has been tombstoned (deleted and barred
/// from reuse). Storage errors propagate — an outage must not read as "free to
/// reuse", which is the filename-immutability / dependency-confusion direction.
pub async fn is_tombstoned(storage: &dyn Storage, artifact_key: &str) -> Result<bool> {
    storage.head_exists(&tombstone_key(artifact_key)).await
}
