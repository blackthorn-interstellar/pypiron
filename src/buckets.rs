//! Multi-bucket selection primitives (design: dev/MULTIBUCKET.md).
//!
//! P0 lays the types with zero behavior change: with one configured bucket a
//! [`BucketSet`] is a thin wrapper that always pins index 0, [`BucketSet::is_multi`]
//! is false, and none of the multi-bucket machinery (topology stamps, switching)
//! runs at all. The wide sweep that makes every request capture [`BucketSet::pin`]
//! at operation entry is P1; the selection health view that drives
//! [`BucketSet::switch`] is P4.

use std::sync::{Arc, PoisonError, RwLock};

use anyhow::{bail, Context as _, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::storage::{is_not_found, Storage};

/// One configured bucket: its storage handle and its identity (the bucket name,
/// used for topology stamping and log lines).
pub struct BucketHandle {
    pub storage: Arc<dyn Storage>,
    pub name: String,
}

/// The immutable storage context an operation runs against: a single handle, the
/// selection generation it was captured under, and the bucket's position in the
/// configured list. Pinned once at operation entry so a selection switch can
/// never tear an in-flight operation (design §3). Cheap to clone — one `Arc`.
#[derive(Clone)]
pub struct Pinned {
    pub storage: Arc<dyn Storage>,
    // Consumed by P1's pin-at-entry wiring and generation-tagged caches; carried
    // but unread in P0.
    #[allow(dead_code)]
    pub generation: u64,
    #[allow(dead_code)]
    pub index: usize,
}

/// The configured buckets in preference order plus the currently-selected one.
///
/// One bucket is the common case and stays completely dormant: [`pin`](Self::pin)
/// returns index 0, [`is_multi`](Self::is_multi) is false, and nothing ever calls
/// [`switch`](Self::switch). Selection state lives behind a single
/// `RwLock<Arc<Pinned>>` so a switch is one atomic swap and a reader never pairs a
/// stale handle with a fresh generation.
pub struct BucketSet {
    handles: Vec<BucketHandle>,
    current: RwLock<Arc<Pinned>>,
}

impl BucketSet {
    /// A single-bucket set — the overwhelming common case. Pinned to index 0.
    pub fn single(storage: Arc<dyn Storage>) -> Self {
        Self::new(vec![BucketHandle {
            storage,
            name: String::new(),
        }])
    }

    /// Build from ordered handles, pinned to the preferred bucket (index 0,
    /// generation 0). Requires at least one handle — `StorageArgs::build_all`
    /// guarantees it.
    pub fn new(handles: Vec<BucketHandle>) -> Self {
        assert!(
            !handles.is_empty(),
            "BucketSet requires at least one configured bucket"
        );
        let pinned = Arc::new(Pinned {
            storage: handles[0].storage.clone(),
            generation: 0,
            index: 0,
        });
        Self {
            handles,
            current: RwLock::new(pinned),
        }
    }

    /// Capture the current storage context. Every operation calls this once at
    /// entry and performs all its I/O against the returned handle; the wide
    /// wiring that enforces that is P1. Recovers a poisoned lock rather than
    /// panicking on a request path.
    pub fn pin(&self) -> Arc<Pinned> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Number of configured buckets.
    #[allow(clippy::len_without_is_empty)] // a BucketSet is never empty (see `new`)
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// More than one bucket configured — the multi-bucket mechanisms are live.
    pub fn is_multi(&self) -> bool {
        self.handles.len() > 1
    }

    /// Select a different bucket, bumping the generation so caches and any
    /// already-pinned contexts from the old selection are recognizably stale.
    /// Nothing calls this yet; P4 drives it from the per-node health view. Kept
    /// crate-visible and exercised by the tests so its contract is locked now.
    #[allow(dead_code)]
    pub(crate) fn switch(&self, index: usize) -> Arc<Pinned> {
        let mut cur = self.current.write().unwrap_or_else(PoisonError::into_inner);
        let next = Arc::new(Pinned {
            storage: self.handles[index].storage.clone(),
            generation: cur.generation + 1,
            index,
        });
        *cur = next.clone();
        next
    }

    /// Fail-closed topology check (design §7), run once at serve startup and only
    /// when more than one bucket is configured. Every *reachable* bucket is
    /// stamped with the configured topology if it has none, or checked against it
    /// if it does; a mismatch refuses startup. Unreachable buckets are skipped
    /// with a warning so a standby node can still boot during the very outage it
    /// exists for. With a single bucket this does no I/O.
    pub async fn verify_topology(&self) -> Result<()> {
        if !self.is_multi() {
            return Ok(());
        }
        // Multi-bucket relies on conditional writes (the topology CAS below,
        // per-bucket leases, origin claims). A backend without them cannot
        // participate safely, so refuse rather than degrade silently.
        for handle in &self.handles {
            if !handle.storage.supports_leases() {
                bail!(
                    "multi-bucket requires a backend with conditional writes; \
                     bucket '{}' does not support them",
                    handle.name
                );
            }
        }

        let names: Vec<String> = self.handles.iter().map(|h| h.name.clone()).collect();
        let expected = topology_hash(&names);
        let stamp = TopologyStamp {
            buckets: names.clone(),
            hash: expected.clone(),
            generation: 0,
        };
        let body = serde_json::to_vec(&stamp).context("serialize topology stamp")?;

        for handle in &self.handles {
            match handle.storage.get_bytes(TOPOLOGY_STAMP_KEY).await {
                Ok(bytes) => check_stamp(&handle.name, &bytes, &expected, &names)?,
                Err(e) if is_not_found(&e) => {
                    // Claim it with create-if-absent so a racing node stamps it
                    // exactly once; if we lose the race, re-read and verify what
                    // the winner wrote (it might be a differently-ordered deploy).
                    let won = handle
                        .storage
                        .put_if_none_match(TOPOLOGY_STAMP_KEY, body.clone())
                        .await
                        .with_context(|| format!("stamp topology on bucket '{}'", handle.name))?;
                    if won.is_none() {
                        let bytes = handle
                            .storage
                            .get_bytes(TOPOLOGY_STAMP_KEY)
                            .await
                            .with_context(|| {
                                format!("re-read topology stamp on bucket '{}'", handle.name)
                            })?;
                        check_stamp(&handle.name, &bytes, &expected, &names)?;
                    }
                }
                Err(e) => warn!(
                    bucket = %handle.name,
                    error = %e,
                    "bucket unreachable during topology check; skipping (it will be verified when it returns)"
                ),
            }
        }
        Ok(())
    }
}

/// Storage key of the topology stamp — the fail-closed record of which buckets a
/// deployment was configured with, in what order (design §7).
pub const TOPOLOGY_STAMP_KEY: &str = "_topology/stamp.json";

/// The stored topology stamp. `hash` is derived from `buckets` alone;
/// `generation` is bumped by an operator re-stamp (`buckets migrate`, a later
/// phase) and does not participate in the hash.
#[derive(Debug, Serialize, Deserialize)]
pub struct TopologyStamp {
    pub buckets: Vec<String>,
    pub hash: String,
    pub generation: u64,
}

/// Deterministic identity of an ordered bucket list: sha256 over the names joined
/// by a NUL byte (which no bucket name can contain), so two deployments agree iff
/// they list the same buckets in the same order.
pub fn topology_hash(names: &[String]) -> String {
    let mut hasher = Sha256::new();
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            hasher.update([0u8]);
        }
        hasher.update(name.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Compare a bucket's existing stamp against the configured topology; a hash
/// mismatch means two deployments disagree about the bucket set or its order —
/// the one misconfiguration §7 checks hard.
fn check_stamp(bucket: &str, bytes: &[u8], expected_hash: &str, expected: &[String]) -> Result<()> {
    let found: TopologyStamp = serde_json::from_slice(bytes)
        .with_context(|| format!("parse topology stamp on bucket '{bucket}'"))?;
    if found.hash != expected_hash {
        bail!(
            "bucket '{bucket}' was stamped for a different bucket topology ({:?}) than this \
             node is configured with ({:?}); refusing to start. If you intend to change the \
             bucket set, re-stamp it deliberately.",
            found.buckets,
            expected
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DiskStorage;

    fn handle(name: &str, root: &str) -> BucketHandle {
        BucketHandle {
            storage: Arc::new(DiskStorage::new(root)),
            name: name.to_string(),
        }
    }

    #[test]
    fn single_is_index_zero_generation_zero() {
        let s: Arc<dyn Storage> = Arc::new(DiskStorage::new("/tmp/pypiron-bucketset-a"));
        let set = BucketSet::single(s.clone());
        assert!(!set.is_multi());
        assert_eq!(set.len(), 1);
        let pinned = set.pin();
        assert_eq!(pinned.index, 0);
        assert_eq!(pinned.generation, 0);
        assert!(Arc::ptr_eq(&pinned.storage, &s));
    }

    #[test]
    fn pin_is_stable() {
        let set = BucketSet::single(Arc::new(DiskStorage::new("/tmp/pypiron-bucketset-b")));
        let a = set.pin();
        let b = set.pin();
        assert_eq!(a.generation, b.generation);
        assert_eq!(a.index, b.index);
        assert!(Arc::ptr_eq(&a.storage, &b.storage));
    }

    #[test]
    fn switch_bumps_generation_and_selects() {
        let set = BucketSet::new(vec![
            handle("east", "/tmp/pypiron-bucketset-east"),
            handle("west", "/tmp/pypiron-bucketset-west"),
        ]);
        assert!(set.is_multi());
        let before = set.pin();
        assert_eq!(before.index, 0);
        assert_eq!(before.generation, 0);

        let after = set.switch(1);
        assert_eq!(after.index, 1);
        assert_eq!(after.generation, 1);
        // A context pinned before the switch keeps its old handle (design §3).
        assert!(!Arc::ptr_eq(&before.storage, &after.storage));
        // New pins observe the new selection.
        assert_eq!(set.pin().index, 1);
        assert_eq!(set.pin().generation, 1);
    }

    #[test]
    fn topology_hash_is_deterministic_and_order_sensitive() {
        let a = vec!["east".to_string(), "west".to_string()];
        let b = vec!["west".to_string(), "east".to_string()];
        assert_eq!(topology_hash(&a), topology_hash(&a));
        assert_ne!(topology_hash(&a), topology_hash(&b));
        // The NUL join is unambiguous: ["ab","c"] must not collide with ["a","bc"].
        let x = vec!["ab".to_string(), "c".to_string()];
        let y = vec!["a".to_string(), "bc".to_string()];
        assert_ne!(topology_hash(&x), topology_hash(&y));
    }

    #[test]
    fn stamp_round_trips_through_json() {
        let names = vec!["iron-east".to_string(), "iron-west".to_string()];
        let stamp = TopologyStamp {
            buckets: names.clone(),
            hash: topology_hash(&names),
            generation: 0,
        };
        let bytes = serde_json::to_vec(&stamp).unwrap();
        check_stamp("iron-east", &bytes, &stamp.hash, &names).unwrap();

        let wrong = topology_hash(&["other".to_string()]);
        let err = check_stamp("iron-east", &bytes, &wrong, &names).unwrap_err();
        assert!(err.to_string().contains("different bucket topology"));
    }
}
