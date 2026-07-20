//! Crash-consistency marker vocabulary: the intent/commit/dirty event keys the
//! rebuild worker (src/worker.rs) drains, plus the nonce idiom the replicator
//! reuses for its `_repl/` markers. A writer declares `mark_intent` *before* it
//! touches truth and `mark_commit` *after*; each event is its own create-only
//! key `_dirty/<pkg>!<nonce><suffix>`, so the worker can rebuild and then delete
//! exactly the keys it observed without racing a concurrent writer. See the
//! worker module doc for how the intent/commit pair heals a crashed writer.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

use anyhow::Result;

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
