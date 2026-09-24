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
//! - **Per package, replaced wholesale**: a rebuild takes its package's memo
//!   out and puts back exactly the files it just listed, so deleted files leave
//!   with no separate invalidation. A concurrent rebuild of the same package
//!   simply misses and reads everything, as before.
//! - **Bounded memory**: a ceiling on memoized files; an insert past it clears
//!   everything, like the page cache (src/project_cache.rs). A miss costs only
//!   the reads the memo would have saved.
//! - **RAM only**: nothing is persisted, so upgrades and rollbacks start cold
//!   and there is no format to migrate.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::sidecar::Sidecar;

/// Artifact filename → (listing change detector of its sidecar, parsed sidecar).
pub type PackageSidecars = HashMap<String, (String, Sidecar)>;

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
    packages: HashMap<String, PackageSidecars>,
    files: usize,
}

impl SidecarCache {
    /// Remove and return `pkg`'s memo (empty when there is none).
    pub fn take(&self, pkg: &str) -> PackageSidecars {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match inner.packages.remove(pkg) {
            Some(memo) => {
                inner.files -= memo.len();
                memo
            }
            None => PackageSidecars::new(),
        }
    }

    /// Store `pkg`'s memo as of the rebuild that just listed it.
    pub fn put(&self, pkg: &str, memo: PackageSidecars) {
        if memo.is_empty() || memo.len() > MAX_FILES {
            return;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(old) = inner.packages.remove(pkg) {
            inner.files -= old.len();
        }
        if inner.files + memo.len() > MAX_FILES {
            inner.packages.clear();
            inner.files = 0;
        }
        inner.files += memo.len();
        inner.packages.insert(pkg.to_string(), memo);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memo(n: usize) -> PackageSidecars {
        let sc: Sidecar = serde_json::from_str(
            r#"{"sha256":"ab","size":1,"version":"1","upload-time":"2026-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        (0..n)
            .map(|i| (format!("f{i}"), (format!("e{i}"), sc.clone())))
            .collect()
    }

    #[test]
    fn take_removes_and_put_replaces() {
        let cache = SidecarCache::default();
        cache.put("a", memo(3));
        cache.put("a", memo(2));
        assert_eq!(cache.take("a").len(), 2);
        assert!(cache.take("a").is_empty());
        assert_eq!(cache.inner.lock().unwrap().files, 0);
    }

    #[test]
    fn overflowing_the_ceiling_drops_everything_but_the_newcomer() {
        let cache = SidecarCache::default();
        cache.put("a", memo(MAX_FILES - 1));
        cache.put("b", memo(2));
        assert!(cache.take("a").is_empty());
        assert_eq!(cache.take("b").len(), 2);
        cache.put("huge", memo(MAX_FILES + 1));
        assert!(cache.take("huge").is_empty());
    }
}
