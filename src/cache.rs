//! In-memory index cache: bytes + ETag, bounded staleness.
//!
//! Before this cache, every `/simple/` read did a full storage GET plus a
//! SHA-256 of the body — ~27 ms and one S3 round-trip per request, including
//! 304 revalidations. Indexes are tiny, few, and already allowed to lag truth
//! (rebuilds are async by design), so the read path serves them from RAM:
//!
//! - **Hit**: zero storage calls; the ETag was hashed once at fill time, so a
//!   matching `If-None-Match` costs nothing at all.
//! - **Staleness bound**: entries expire after [`INDEX_CACHE_TTL`]. The
//!   process that rebuilds an index invalidates its own cache immediately, so
//!   on a single node reads are fresh the instant the worker writes; the TTL
//!   only bounds staleness from *other* writers (multi-node S3 peers).
//! - **Negative entries**: a missing index (unknown package) is cached too —
//!   otherwise every 404 probe costs a storage round-trip.
//!
//! Expiry deliberately allows a brief thundering herd (concurrent refills of
//! the same key): it is at worst what every request paid before the cache
//! existed, once per TTL. Single-flight machinery would buy nothing but code.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use sha2::{Digest, Sha256};

use crate::storage::{is_not_found, Storage};

/// How stale a cached index may be when another node rebuilt it.
pub const INDEX_CACHE_TTL: Duration = Duration::from_secs(1);

/// Memory ceiling for cached bodies. When an insert pushes past it, expired
/// entries are pruned; if that isn't enough the cache is cleared outright —
/// a once-per-TTL refill storm is the same cost the cache saves a thousand
/// times over, and "bounded and dumb" beats an LRU nobody will ever tune.
pub const INDEX_CACHE_MAX_BYTES: usize = 128 * 1024 * 1024;

/// One cacheable representation: body bytes plus the ETag identifying them.
/// `Bytes` so responses share the buffer refcounted instead of memcpying it —
/// at 4k rps of 100 KB gzip bodies the clone was ~430 MB/s of pure copy.
#[derive(Clone)]
pub struct Variant {
    pub body: bytes::Bytes,
    pub etag: Arc<str>,
}

#[derive(Clone)]
enum Cached {
    Present {
        identity: Variant,
        /// Precompressed at fill time when it actually shrinks the body —
        /// the hot path serves gzip with zero per-request CPU. None for
        /// bodies too small or too incompressible to bother.
        gzip: Option<Variant>,
    },
    Missing,
}

/// Fixed per-entry overhead charged to the byte ceiling. Without it a negative
/// (`Missing`, zero-body) entry weighs nothing, so a flood of probes for
/// distinct unknown names — anonymous on a public read path — never trips the
/// cap and grows the map until OOM. Charging the key+struct+slot footprint
/// makes entry count bound itself through the same ceiling; the proxy listing
/// cache guards the identical hazard with a hard count cap.
const ENTRY_OVERHEAD_BYTES: usize = 256;

impl Cached {
    /// Bytes this entry charges against the cap: its body plus a fixed
    /// per-entry overhead so zero-body `Missing` entries still count.
    fn weight(&self) -> usize {
        let body = match self {
            Cached::Present { identity, gzip } => {
                identity.body.len() + gzip.as_ref().map_or(0, |g| g.body.len())
            }
            Cached::Missing => 0,
        };
        ENTRY_OVERHEAD_BYTES + body
    }

    /// The `(identity, gzip)` pair `get` hands back, or `None` when missing.
    fn into_pair(self) -> Option<(Variant, Option<Variant>)> {
        match self {
            Cached::Present { identity, gzip } => Some((identity, gzip)),
            Cached::Missing => None,
        }
    }
}

/// Below this, gzip headers cost more than they save.
const GZIP_MIN_BYTES: usize = 1024;
/// Keep the variant only if it actually pays: ≤90% of the original.
const GZIP_KEEP_RATIO_PCT: usize = 90;

pub(crate) fn quoted_sha256(bytes: &[u8]) -> Arc<str> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("\"{:x}\"", hasher.finalize()).into()
}

fn maybe_gzip(identity: &[u8]) -> Option<Variant> {
    if identity.len() < GZIP_MIN_BYTES {
        return None;
    }
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;
    let mut enc = GzEncoder::new(Vec::with_capacity(identity.len() / 4), Compression::new(6));
    enc.write_all(identity).ok()?;
    let compressed = enc.finish().ok()?;
    if compressed.len() * 100 > identity.len() * GZIP_KEEP_RATIO_PCT {
        return None;
    }
    let etag = quoted_sha256(&compressed);
    Some(Variant {
        body: bytes::Bytes::from(compressed),
        etag,
    })
}

struct Entry {
    cached: Cached,
    fetched: Instant,
}

#[derive(Default)]
struct Entries {
    map: HashMap<String, Entry>,
    body_bytes: usize,
}

impl Entries {
    fn insert(&mut self, key: String, entry: Entry) {
        self.body_bytes += entry.cached.weight();
        if let Some(old) = self.map.insert(key, entry) {
            self.body_bytes -= old.cached.weight();
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some(old) = self.map.remove(key) {
            self.body_bytes -= old.cached.weight();
        }
    }

    /// Enforce the byte ceiling: drop expired entries first; if the live set
    /// alone still exceeds the ceiling, clear everything (refill is one
    /// storage GET per hot key, once).
    fn enforce_cap(&mut self, max_bytes: usize, ttl: Duration) {
        if self.body_bytes <= max_bytes {
            return;
        }
        let mut freed = 0usize;
        self.map.retain(|_, e| {
            let keep = e.fetched.elapsed() < ttl;
            if !keep {
                freed += e.cached.weight();
            }
            keep
        });
        self.body_bytes -= freed;
        if self.body_bytes > max_bytes {
            self.map.clear();
            self.body_bytes = 0;
        }
    }
}

pub struct IndexCache {
    ttl: Duration,
    max_bytes: usize,
    entries: Mutex<Entries>,
    /// Hit/miss tally for the dashboard's cache-hit rate. A "hit" is any index
    /// served from memory without touching storage — including negatively
    /// cached misses (a known-absent package answered from RAM).
    hits: AtomicU64,
    misses: AtomicU64,
}

impl IndexCache {
    pub fn new(ttl: Duration) -> Self {
        Self::with_capacity(ttl, INDEX_CACHE_MAX_BYTES)
    }

    pub fn with_capacity(ttl: Duration, max_bytes: usize) -> Self {
        Self {
            ttl,
            max_bytes,
            entries: Mutex::new(Entries::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// `(hits, misses)` since boot, for the dashboard's cache-hit rate.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    /// Fetch an index through the cache. `Ok(None)` means "no such index"
    /// (negatively cached). Returns the identity representation plus the
    /// precompressed gzip variant when one exists; ETags are the quoted
    /// SHA-256 of each representation's bytes, computed once per fill.
    pub async fn get(
        &self,
        storage: &dyn Storage,
        key: &str,
    ) -> Result<Option<(Variant, Option<Variant>)>> {
        if let Some(hit) = self.fresh(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(hit.into_pair());
        }
        self.misses.fetch_add(1, Ordering::Relaxed);

        let cached = match storage.get_bytes(key).await {
            Ok(bytes) => {
                let gzip = maybe_gzip(&bytes);
                let etag = quoted_sha256(&bytes);
                Cached::Present {
                    identity: Variant {
                        body: bytes::Bytes::from(bytes),
                        etag,
                    },
                    gzip,
                }
            }
            Err(e) if is_not_found(&e) => Cached::Missing,
            Err(e) => return Err(e),
        };

        {
            let mut entries = self.entries.lock().unwrap();
            entries.insert(
                key.to_string(),
                Entry {
                    cached: cached.clone(),
                    fetched: Instant::now(),
                },
            );
            entries.enforce_cap(self.max_bytes, self.ttl);
        }
        Ok(cached.into_pair())
    }

    /// Drop a key after writing or deleting its index — same-process reads
    /// are fresh immediately, without waiting out the TTL.
    pub fn invalidate(&self, key: &str) {
        self.entries.lock().unwrap().remove(key);
    }

    fn fresh(&self, key: &str) -> Option<Cached> {
        let entries = self.entries.lock().unwrap();
        let entry = entries.map.get(key)?;
        (entry.fetched.elapsed() < self.ttl).then(|| entry.cached.clone())
    }
}

/// Reusing presigned URLs: artifacts are immutable, so the same signed GET
/// URL is valid for every client until it expires. Signing is local HMAC but
/// not free at tens of thousands of rps (SDK credential plumbing per call);
/// serving a 5-minute-old URL signed for an hour costs nothing and leaves
/// every client at least 55 minutes of validity.
pub const PRESIGN_CACHE_TTL: Duration = Duration::from_secs(300);
/// Presigned GET expiry handed to storage. Must comfortably exceed the cache
/// TTL (clients receive expiry minus cache age).
pub const PRESIGN_EXPIRY: Duration = Duration::from_secs(3600);
const PRESIGN_CACHE_MAX_ENTRIES: usize = 65_536;

pub struct PresignCache {
    ttl: Duration,
    entries: Mutex<HashMap<String, (Arc<str>, Instant)>>,
}

impl PresignCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn fresh(&self, key: &str) -> Option<Arc<str>> {
        let entries = self.entries.lock().unwrap();
        let (url, signed) = entries.get(key)?;
        (signed.elapsed() < self.ttl).then(|| url.clone())
    }

    pub fn put(&self, key: &str, url: Arc<str>) {
        let mut entries = self.entries.lock().unwrap();
        entries.insert(key.to_string(), (url, Instant::now()));
        if entries.len() > PRESIGN_CACHE_MAX_ENTRIES {
            let ttl = self.ttl;
            entries.retain(|_, (_, signed)| signed.elapsed() < ttl);
            if entries.len() > PRESIGN_CACHE_MAX_ENTRIES {
                entries.clear();
            }
        }
    }

    /// Deletes must stop handing out the dead URL immediately (same node).
    pub fn invalidate(&self, key: &str) {
        self.entries.lock().unwrap().remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::InMemStorage;

    fn etag_of(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("\"{:x}\"", hasher.finalize())
    }

    #[tokio::test]
    async fn hit_serves_from_memory_without_storage_calls() {
        let storage = InMemStorage::default();
        storage.insert("simple/foo/index.json", b"body-1".to_vec());
        let cache = IndexCache::new(Duration::from_secs(60));

        let (identity, _) = cache
            .get(&storage, "simple/foo/index.json")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(identity.body.as_ref(), b"body-1");
        assert_eq!(&*identity.etag, etag_of(b"body-1"));
        assert_eq!(storage.get_count(), 1);

        // Second read: served from RAM, same etag, no storage traffic.
        let (identity2, _) = cache
            .get(&storage, "simple/foo/index.json")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(identity2.body.as_ref(), b"body-1");
        assert_eq!(identity2.etag, identity.etag);
        assert_eq!(storage.get_count(), 1);
    }

    #[tokio::test]
    async fn expired_entry_refetches() {
        let storage = InMemStorage::default();
        storage.insert("simple/foo/index.json", b"old".to_vec());
        let cache = IndexCache::new(Duration::from_millis(10));

        cache.get(&storage, "simple/foo/index.json").await.unwrap();
        storage.insert("simple/foo/index.json", b"new".to_vec());
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (identity, _) = cache
            .get(&storage, "simple/foo/index.json")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(identity.body.as_ref(), b"new");
        assert_eq!(
            &*identity.etag,
            etag_of(b"new"),
            "etag must track the new body"
        );
        assert_eq!(storage.get_count(), 2);
    }

    #[tokio::test]
    async fn invalidate_beats_ttl() {
        let storage = InMemStorage::default();
        storage.insert("simple/foo/index.json", b"old".to_vec());
        let cache = IndexCache::new(Duration::from_secs(60));

        cache.get(&storage, "simple/foo/index.json").await.unwrap();
        storage.insert("simple/foo/index.json", b"new".to_vec());
        cache.invalidate("simple/foo/index.json");

        let (identity, _) = cache
            .get(&storage, "simple/foo/index.json")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            identity.body.as_ref(),
            b"new",
            "same-process write must be visible immediately"
        );
    }

    #[tokio::test]
    async fn missing_index_is_negatively_cached() {
        let storage = InMemStorage::default();
        let cache = IndexCache::new(Duration::from_secs(60));

        assert!(cache
            .get(&storage, "simple/nope/index.json")
            .await
            .unwrap()
            .is_none());
        assert!(cache
            .get(&storage, "simple/nope/index.json")
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            storage.get_count(),
            1,
            "repeat 404 probes must not hit storage"
        );

        // The package appears (rebuild writes + invalidates): visible at once.
        storage.insert("simple/nope/index.json", b"born".to_vec());
        cache.invalidate("simple/nope/index.json");
        assert!(cache
            .get(&storage, "simple/nope/index.json")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn byte_cap_evicts_instead_of_growing_forever() {
        let storage = InMemStorage::default();
        // 8 keys x 1 KB with a 4 KB ceiling: the cache must stay bounded.
        for i in 0..8 {
            storage.insert(&format!("simple/p{i}/index.json"), vec![b'x'; 1024]);
        }
        let cache = IndexCache::with_capacity(Duration::from_secs(60), 4 * 1024);
        for i in 0..8 {
            assert!(cache
                .get(&storage, &format!("simple/p{i}/index.json"))
                .await
                .unwrap()
                .is_some());
        }
        let bytes = cache.entries.lock().unwrap().body_bytes;
        assert!(
            bytes <= 4 * 1024,
            "cache body bytes {bytes} exceed the 4096-byte ceiling"
        );
        // Still serves correctly after eviction (refill path).
        assert!(cache
            .get(&storage, "simple/p0/index.json")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn negative_entries_are_bounded() {
        // A flood of probes for distinct unknown names (anonymous on a public
        // read path) must not grow the map forever. Missing entries are
        // zero-body, so they only stay bounded if they charge the per-entry
        // overhead against the ceiling.
        let storage = InMemStorage::default();
        let max_bytes = 4 * 1024;
        let cache = IndexCache::with_capacity(Duration::from_secs(60), max_bytes);
        for i in 0..10_000 {
            assert!(cache
                .get(&storage, &format!("simple/missing{i}/index.json"))
                .await
                .unwrap()
                .is_none());
        }
        let len = cache.entries.lock().unwrap().map.len();
        assert!(
            len <= max_bytes / ENTRY_OVERHEAD_BYTES,
            "negative cache grew to {len} entries — unbounded by the ceiling"
        );
    }

    #[tokio::test]
    async fn presign_cache_round_trip_and_expiry() {
        let cache = PresignCache::new(Duration::from_millis(20));
        assert!(cache.fresh("packages/p/a.whl").is_none());
        cache.put("packages/p/a.whl", "https://signed.example/1".into());
        assert_eq!(
            cache.fresh("packages/p/a.whl").as_deref(),
            Some("https://signed.example/1")
        );
        cache.invalidate("packages/p/a.whl");
        assert!(
            cache.fresh("packages/p/a.whl").is_none(),
            "post-delete the URL must be gone immediately"
        );
        cache.put("packages/p/a.whl", "https://signed.example/2".into());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            cache.fresh("packages/p/a.whl").is_none(),
            "expired URLs must not be served"
        );
    }

    #[tokio::test]
    async fn gzip_variant_round_trips_with_distinct_etag() {
        let storage = InMemStorage::default();
        // Highly compressible and above the size floor.
        let body = b"{\"files\": []}".repeat(500);
        storage.insert("simple/foo/index.json", body.clone());
        let cache = IndexCache::new(Duration::from_secs(60));

        let (identity, gzip) = cache
            .get(&storage, "simple/foo/index.json")
            .await
            .unwrap()
            .unwrap();
        let gz = gzip.expect("compressible body must get a gzip variant");
        assert!(gz.body.len() < body.len() / 2, "gzip should pay for itself");
        assert_ne!(
            gz.etag, identity.etag,
            "each representation has its own ETag"
        );

        use std::io::Read;
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(gz.body.as_ref())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(
            decoded, body,
            "gzip variant must decode to the identity body"
        );
    }

    #[tokio::test]
    async fn tiny_and_incompressible_bodies_skip_gzip() {
        let storage = InMemStorage::default();
        storage.insert("simple/tiny/index.json", b"{}".to_vec());
        // Random-ish bytes: hex of hashes, no structure to compress.
        let incompressible: Vec<u8> = (0..200_000u32)
            .flat_map(|i| {
                let mut h = Sha256::new();
                h.update(i.to_le_bytes());
                h.finalize().to_vec()
            })
            .take(100_000)
            .collect();
        storage.insert("simple/rand/index.json", incompressible);
        let cache = IndexCache::new(Duration::from_secs(60));

        let (_, gz_tiny) = cache
            .get(&storage, "simple/tiny/index.json")
            .await
            .unwrap()
            .unwrap();
        assert!(
            gz_tiny.is_none(),
            "sub-1KB bodies must not carry a gzip variant"
        );
        let (_, gz_rand) = cache
            .get(&storage, "simple/rand/index.json")
            .await
            .unwrap()
            .unwrap();
        assert!(
            gz_rand.is_none(),
            "a variant that saves <10% must be dropped, not cached"
        );
    }

    #[tokio::test]
    async fn storage_errors_are_not_cached() {
        let storage = InMemStorage::default();
        storage.fail_next_get();
        let cache = IndexCache::new(Duration::from_secs(60));

        assert!(cache.get(&storage, "simple/foo/index.json").await.is_err());

        // The error must not poison the cache as a negative entry.
        storage.insert("simple/foo/index.json", b"ok".to_vec());
        assert!(cache
            .get(&storage, "simple/foo/index.json")
            .await
            .unwrap()
            .is_some());
    }
}
