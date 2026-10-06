//! Parsed-sidecar memo for package index rebuilds.
//!
//! A rebuild derives a package's views from truth: list the package, read every
//! artifact's sidecar. The listing is one call; the sidecar reads are one GET
//! per file, so an upload to a 5,000-file package cost 5,000 GETs to render one
//! new line. Sidecars rarely change (a yank or a backfill rewrites one), and
//! the listing already says which did: every entry carries a change detector
//! ([`FileEntry::etag`](crate::storage::FileEntry::etag)). So each rebuild
//! remembers the sidecars it parsed, keyed by that detector, and the next
//! rebuild of the same package re-reads only the ones whose detector moved.
//!
//! - **Exact, not bounded-stale**: a hit requires the listing taken *this*
//!   rebuild to report the same detector the memo was filled under, so a
//!   rewritten sidecar is always re-read. An entry with no detector is never
//!   memoized.
//! - **Per bucket**: one node serves several buckets from one memo, and a
//!   detector is only a change detector *within* its store — two buckets can
//!   list the same etag (or mtime+size) over different bytes. Each memo is
//!   keyed by the storage handle it was filled from, so one bucket's listing
//!   can never validate another bucket's parse.
//! - **Per package, replaced wholesale**: a rebuild takes its package's memo
//!   out and puts back exactly the files it just listed, so deleted files leave
//!   with no separate invalidation. A concurrent rebuild of the same package
//!   simply misses and reads everything, as before.
//! - **Bounded memory**: a ceiling on memoized files; an insert past it clears
//!   everything, like the page cache (src/project_cache.rs). A miss costs only
//!   the reads the memo would have saved.
//! - **RAM only**: nothing is persisted, so upgrades and rollbacks start cold
//!   and there is no format to migrate.
//!
//! The same memo holds the sha256 of each `.metadata` companion, which the
//! index publishes (PEP 658/714). It is derived from the companion's bytes, not
//! stored, so it is memoized the same way: keyed by the companion's detector.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::sidecar::Sidecar;
use crate::storage::Storage;

/// One package's memo. Each entry is keyed by artifact filename and holds the
/// listing change detector its value was read under.
#[derive(Default)]
pub struct PackageSidecars {
    /// (sidecar's detector, parsed sidecar).
    pub sidecars: HashMap<String, (String, Sidecar)>,
    /// (`.metadata` companion's detector, sha256 of its bytes).
    pub metadata: HashMap<String, (String, String)>,
}

impl PackageSidecars {
    pub fn len(&self) -> usize {
        self.sidecars.len() + self.metadata.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Files memoized across all packages before the memo is dropped. A sidecar
/// parses to a few hundred bytes, so this is tens of MB at worst — and it holds
/// the largest real package (~45k files) with room to spare.
const MAX_FILES: usize = 100_000;

#[derive(Default)]
pub struct SidecarCache {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    packages: HashMap<(usize, String), PackageSidecars>,
    files: usize,
}

/// A bucket's identity for the life of the process: the address of its storage
/// handle. Handles are built once at startup and held by the `AppState` that
/// owns this memo, so an address is never reused for another bucket under it.
fn key(storage: &dyn Storage, pkg: &str) -> (usize, String) {
    (
        (storage as *const dyn Storage).cast::<()>() as usize,
        pkg.to_string(),
    )
}

impl SidecarCache {
    /// Remove and return `pkg`'s memo on `storage` (empty when there is none).
    pub fn take(&self, storage: &dyn Storage, pkg: &str) -> PackageSidecars {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match inner.packages.remove(&key(storage, pkg)) {
            Some(memo) => {
                inner.files -= memo.len();
                memo
            }
            None => PackageSidecars::default(),
        }
    }

    /// Store `pkg`'s memo on `storage` as of the rebuild that just listed it.
    pub fn put(&self, storage: &dyn Storage, pkg: &str, memo: PackageSidecars) {
        if memo.is_empty() || memo.len() > MAX_FILES {
            return;
        }
        let key = key(storage, pkg);
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(old) = inner.packages.remove(&key) {
            inner.files -= old.len();
        }
        if inner.files + memo.len() > MAX_FILES {
            inner.packages.clear();
            inner.files = 0;
        }
        inner.files += memo.len();
        inner.packages.insert(key, memo);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::InMemStorage;

    fn memo(n: usize) -> PackageSidecars {
        let sc: Sidecar = serde_json::from_str(
            r#"{"sha256":"ab","size":1,"version":"1","upload-time":"2026-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        PackageSidecars {
            sidecars: (0..n)
                .map(|i| (format!("f{i}"), (format!("e{i}"), sc.clone())))
                .collect(),
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn take_removes_and_put_replaces() {
        let cache = SidecarCache::default();
        let s = InMemStorage::default();
        cache.put(&s, "a", memo(3));
        cache.put(&s, "a", memo(2));
        assert_eq!(cache.take(&s, "a").len(), 2);
        assert!(cache.take(&s, "a").is_empty());
        assert_eq!(cache.inner.lock().unwrap().files, 0);
    }

    #[test]
    fn one_buckets_memo_is_invisible_to_another() {
        // Two buckets can list the same detector over different sidecars.
        let cache = SidecarCache::default();
        let (b0, b1) = (InMemStorage::default(), InMemStorage::default());
        cache.put(&b0, "a", memo(1));
        assert!(cache.take(&b1, "a").is_empty());
        assert_eq!(cache.take(&b0, "a").len(), 1);
    }

    #[test]
    fn overflowing_the_ceiling_drops_everything_but_the_newcomer() {
        let cache = SidecarCache::default();
        let s = InMemStorage::default();
        cache.put(&s, "a", memo(MAX_FILES - 1));
        cache.put(&s, "b", memo(2));
        assert!(cache.take(&s, "a").is_empty());
        assert_eq!(cache.take(&s, "b").len(), 2);
        cache.put(&s, "huge", memo(MAX_FILES + 1));
        assert!(cache.take(&s, "huge").is_empty());
    }
}
