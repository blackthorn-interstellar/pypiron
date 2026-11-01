//! Crash-consistency marker vocabulary: the intent/commit/dirty event keys the
//! rebuild worker (src/worker.rs) drains, plus the nonce idiom the replicator
//! reuses for its `_repl/` markers. A writer declares `mark_intent` *before* it
//! touches truth and `mark_commit` *after*; each event is its own create-only
//! key `_dirty/<pkg>!<nonce><suffix>`, so the worker can rebuild and then delete
//! exactly the keys it observed without racing a concurrent writer. See the
//! worker module doc for how the intent/commit pair heals a crashed writer.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use anyhow::Result;
use tracing::warn;

use crate::app::{AppState, DIRTY_PREFIX};
use crate::storage::{FileEntry, Storage};

pub(crate) const INTENT_SUFFIX: &str = ".intent";
pub(crate) const COMMIT_SUFFIX: &str = ".commit";

/// Unique per-event marker id: wall nanos + pid + process-local counter +
/// per-call randomized entropy. The deterministic fields make logs useful;
/// entropy prevents two processes with the same pid and clock from claiming
/// the same correctness-critical marker identity. Shared with the replicator's
/// `_repl/` markers (src/replicate.rs), which reuse the idiom.
pub(crate) fn marker_nonce() -> String {
    if let Some(n) = crate::clock::sim_nonce() {
        return n;
    }
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let pid = std::process::id();
    let entropy = |domain: u64| {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(nanos);
        hasher.write_u32(pid);
        hasher.write_u64(seq);
        hasher.write_u64(domain);
        hasher.finish()
    };
    format!("{nanos}-{pid}-{seq}-{:016x}{:016x}", entropy(0), entropy(1))
}

/// Declare "I am about to change truth for `pkg`". Returns the nonce the
/// writer must commit with. If the writer dies, the intent goes stale and the
/// worker rebuilds anyway after the grace period.
pub async fn mark_intent(storage: &dyn Storage, pkg: &str) -> Result<String> {
    let nonce = marker_nonce();
    mark_intent_with_nonce(storage, pkg, &nonce).await?;
    Ok(nonce)
}

pub(crate) async fn mark_intent_with_nonce(
    storage: &dyn Storage,
    pkg: &str,
    nonce: &str,
) -> Result<()> {
    put_marker(storage, pkg, nonce, INTENT_SUFFIX).await
}

/// Declare "truth changed for `pkg`": rebuild as soon as possible.
pub async fn mark_commit(storage: &dyn Storage, pkg: &str, nonce: &str) -> Result<()> {
    put_marker(storage, pkg, nonce, COMMIT_SUFFIX).await
}

pub(crate) async fn clear_intent(storage: &dyn Storage, pkg: &str, nonce: &str) -> Result<()> {
    storage
        .delete_keys(&[format!("{DIRTY_PREFIX}{pkg}!{nonce}{INTENT_SUFFIX}")])
        .await
}

/// Write an empty event marker `_dirty/<pkg>!<nonce><suffix>`.
async fn put_marker(storage: &dyn Storage, pkg: &str, nonce: &str, suffix: &str) -> Result<()> {
    storage
        .put_bytes(
            &format!("{DIRTY_PREFIX}{pkg}!{nonce}{suffix}"),
            Vec::new(),
            None,
        )
        .await
}

/// Mark a package as needing an index rebuild (an unpaired commit event, for
/// callers whose truth change already happened).
pub async fn mark_dirty(storage: &dyn Storage, pkg: &str) -> Result<()> {
    mark_commit(storage, pkg, &marker_nonce()).await
}

/// One parsed `_dirty/` entry.
pub struct Marker {
    pub key: String,
    pub nonce: Option<String>,
    pub is_commit: bool,
    /// Storage last-modified — staleness comes from the storage clock.
    pub written_at: Option<time::OffsetDateTime>,
}

/// Split a marker key into (package, marker). Legacy `_dirty/<pkg>` keys
/// parse as nonce-less commits.
pub fn parse_marker(entry: &FileEntry) -> Option<(String, Marker)> {
    let rest = entry.key.strip_prefix(DIRTY_PREFIX)?;
    let written_at = entry.last_modified.as_deref().and_then(|ts| {
        time::OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339).ok()
    });
    let Some((pkg, event)) = rest.split_once('!') else {
        return Some((
            rest.to_string(),
            Marker {
                key: entry.key.clone(),
                nonce: None,
                is_commit: true,
                written_at,
            },
        ));
    };
    let (nonce, is_commit) = if let Some(n) = event.strip_suffix(COMMIT_SUFFIX) {
        (n, true)
    } else if let Some(n) = event.strip_suffix(INTENT_SUFFIX) {
        (n, false)
    } else {
        // Unknown suffix: treat as a commit so nothing rots in the prefix.
        (event, true)
    };
    Some((
        pkg.to_string(),
        Marker {
            key: entry.key.clone(),
            nonce: Some(nonce.to_string()),
            is_commit,
            written_at,
        },
    ))
}

/// Commit a truth change, pairing with `intent_nonce` when the intent marker
/// landed (so the worker consumes both), and wake the worker now instead of
/// letting the marker wait out the tick — upload→visible drops from
/// ~tick+rebuild to ~rebuild. Peer nodes still ride the marker/tick path;
/// the nudge is a same-process accelerant only.
pub(crate) async fn commit_marker(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    intent_nonce: Option<String>,
) -> Result<()> {
    match intent_nonce {
        Some(nonce) => mark_commit(storage, pkg, &nonce).await?,
        None => mark_dirty(storage, pkg).await?,
    }
    state.worker_nudge.notify_one();
    Ok(())
}

/// A writer's open intent, closed on every exit. An unpaired intent is the
/// crashed-writer signal: the worker defers the package's rebuilds for the whole
/// intent grace (15 min by default) before healing it. A writer that bails out
/// with an error, or whose request is cancelled, has not crashed — truth is
/// whatever it left — so dropping an uncommitted `Intent` pairs it in the
/// background. Only a dead process leaves one for the stale-intent healer.
pub(crate) struct Intent {
    storage: Arc<dyn Storage>,
    nudge: Arc<tokio::sync::Notify>,
    pkg: String,
    /// `None` once committed, or when a best-effort open could not write it.
    nonce: Option<String>,
}

impl Intent {
    /// Declare the intent, failing if its marker can't be written.
    pub(crate) async fn open(
        state: &AppState,
        storage: &Arc<dyn Storage>,
        pkg: &str,
    ) -> Result<Self> {
        let nonce = mark_intent(storage.as_ref(), pkg).await?;
        Ok(Self::new(state, storage, pkg, Some(nonce)))
    }

    /// Declare the intent if the marker can be written; the commit then falls
    /// back to a bare dirty marker.
    pub(crate) async fn open_best_effort(
        state: &AppState,
        storage: &Arc<dyn Storage>,
        pkg: &str,
    ) -> Self {
        let nonce = mark_intent(storage.as_ref(), pkg).await.ok();
        Self::new(state, storage, pkg, nonce)
    }

    fn new(state: &AppState, storage: &Arc<dyn Storage>, pkg: &str, nonce: Option<String>) -> Self {
        Self {
            storage: Arc::clone(storage),
            nudge: Arc::clone(&state.worker_nudge),
            pkg: pkg.to_string(),
            nonce,
        }
    }

    /// [`commit_marker`] for this intent. A failed write leaves the intent to go
    /// stale and heal the package, as before.
    pub(crate) async fn commit(mut self) -> Result<()> {
        match self.nonce.take() {
            Some(nonce) => mark_commit(self.storage.as_ref(), &self.pkg, &nonce).await?,
            None => mark_dirty(self.storage.as_ref(), &self.pkg).await?,
        }
        self.nudge.notify_one();
        Ok(())
    }
}

impl Drop for Intent {
    fn drop(&mut self) {
        let Some(nonce) = self.nonce.take() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let storage = Arc::clone(&self.storage);
        let nudge = Arc::clone(&self.nudge);
        let pkg = std::mem::take(&mut self.pkg);
        handle.spawn(async move {
            match mark_commit(storage.as_ref(), &pkg, &nonce).await {
                Ok(()) => nudge.notify_one(),
                Err(error) => warn!(
                    package = %pkg,
                    error = ?error,
                    "could not close an abandoned write intent; it heals after the intent grace"
                ),
            }
        });
    }
}

/// Pairs an open intent in the background if the future holding it is dropped
/// before the writer pairs (or clears) it itself. For writers that hold only a
/// borrowed `&dyn Storage` — the rebuild and replication paths, which a health
/// flap, a selection switch, or a disconnecting client can cancel mid-await.
/// Inert when `storage` is not one of this node's buckets: with no owned handle
/// to pair from, the intent heals after the grace, as it did before.
pub(crate) struct PairIntentOnDrop(Option<(Arc<dyn Storage>, String, String)>);

impl PairIntentOnDrop {
    pub(crate) fn arm(state: &AppState, storage: &dyn Storage, pkg: &str, nonce: &str) -> Self {
        Self(
            owned_bucket_storage(state, storage)
                .map(|owned| (owned, pkg.to_string(), nonce.to_string())),
        )
    }

    /// The writer paired (or cleared) the intent itself.
    pub(crate) fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for PairIntentOnDrop {
    fn drop(&mut self) {
        let Some((storage, pkg, nonce)) = self.0.take() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            if let Err(error) = mark_commit(storage.as_ref(), &pkg, &nonce).await {
                warn!(
                    package = %pkg,
                    error = ?error,
                    "could not pair an intent after cancellation; it heals after the intent grace"
                );
            }
        });
    }
}

/// The `Arc` for `storage` when it is one of this node's configured buckets.
/// Pointer identity is enough: every production caller passes
/// `handle.storage.as_ref()` / `pinned.storage.as_ref()` of that Arc.
fn owned_bucket_storage(state: &AppState, storage: &dyn Storage) -> Option<Arc<dyn Storage>> {
    let want = storage as *const dyn Storage as *const ();
    state.buckets.handles().iter().find_map(|handle| {
        let got = handle.storage.as_ref() as *const dyn Storage as *const ();
        (got == want).then(|| handle.storage.clone())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_nonces_carry_randomized_process_entropy() {
        let first = marker_nonce();
        let second = marker_nonce();
        assert_ne!(first, second);
        for nonce in [first, second] {
            let entropy = nonce.rsplit('-').next().unwrap();
            assert_eq!(entropy.len(), 32);
            assert!(entropy.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }
}
