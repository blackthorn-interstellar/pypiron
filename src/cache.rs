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
//! On expiry a single task refills while every concurrent request keeps being
//! served the (at most TTL-stale) entry — one storage GET per lapse, not one
//! per request. The refill hashes the fetched bytes and, when they're
//! unchanged, reuses the existing gzip variant + ETag instead of recompressing:
//! under steady load an index rarely changes second-to-second, so the herd's
//! dominant cost — re-gzip + re-SHA of identical bytes, every TTL — disappears.
//! Invalidation stays a hard drop: no stale-while-revalidate shortcut survives
//! it, so a same-process rebuild is visible the instant it lands — bar a fill
//! already in flight, which can re-insert once (see [`IndexCache::invalidate`]).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use crate::storage::{is_not_found, Storage};

/// How stale a cached index may be when another node rebuilt it.
pub const INDEX_CACHE_TTL: Duration = Duration::from_secs(1);

/// Memory ceiling for cached bodies. When an insert pushes past it, expired
/// entries are pruned; if that isn't enough the cache is cleared outright —
/// a once-per-TTL refill storm is the same cost the cache saves a thousand
/// times over, and "bounded and dumb" beats an LRU nobody will ever tune.
pub(crate) const INDEX_CACHE_MAX_BYTES: usize = 128 * 1024 * 1024;

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
        /// bodies too small or too incompressible to bother. The cost isn't
        /// zero everywhere: "fill time" is itself a request, so the one that
        /// misses waits out the compression before anyone gets the variant.
        /// It waits off the cache lock, and for a body past
        /// [`GZIP_OFFLOAD_MIN_BYTES`] off the runtime's request threads too.
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
/// At or above this, hashing + gzipping a fill runs on the blocking pool
/// instead of inline. A full-PyPI root index is tens of MB and takes ~1s to
/// compress; a burn that long on a request-serving thread stalls every other
/// future queued behind it (tokio doesn't preempt a running task), which on a
/// small box is a visible fraction of the whole runtime. Ordinary package
/// indexes are kilobytes and stay inline — a hop to another thread would cost
/// more than the work.
const GZIP_OFFLOAD_MIN_BYTES: usize = 1024 * 1024;
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

/// Build the identity + optional gzip representations of an index body without
/// caching them. The read-through fallback renders a page fetched straight from
/// the write pin so a package key is never
/// populated in the read-pin index cache from anything but the read pin.
pub fn build_variants(bytes: Vec<u8>) -> (Variant, Option<Variant>) {
    let gzip = maybe_gzip(&bytes);
    let etag = quoted_sha256(&bytes);
    let identity = Variant {
        body: bytes::Bytes::from(bytes),
        etag,
    };
    (identity, gzip)
}

/// Build a `Present` entry from freshly fetched `bytes`, reusing the stale
/// entry's representations when the content is unchanged. Hashing the fetched
/// bytes is unavoidable — it *is* the ETag — but a match means the identity
/// buffer and its precomputed gzip variant are byte-for-byte what we already
/// hold, so we clone the refcounted `Bytes`/`Arc` instead of recompressing.
/// That reuse is the whole point of the refill: under steady load the index is
/// unchanged every second, and re-gzip + re-SHA of identical bytes was the
/// dominant self-time. A changed body (or a stale `Missing`) falls through to a
/// full rebuild, exactly as a cold fill would.
fn reuse_or_build(stale: Option<&Cached>, bytes: Vec<u8>) -> Cached {
    let etag = quoted_sha256(&bytes);
    if let Some(Cached::Present { identity, gzip }) = stale {
        if identity.etag == etag {
            return Cached::Present {
                identity: identity.clone(),
                gzip: gzip.clone(),
            };
        }
    }
    let gzip = maybe_gzip(&bytes);
    Cached::Present {
        identity: Variant {
            body: bytes::Bytes::from(bytes),
            etag,
        },
        gzip,
    }
}

struct Entry {
    cached: Cached,
    fetched: Instant,
}

#[derive(Default)]
struct Entries {
    map: HashMap<String, Entry>,
    body_bytes: usize,
    /// The selection generation these entries were built under. A bucket switch
    /// (P4) bumps the pinned generation; the first access carrying the new value
    /// clears everything so a page built from the old bucket can't be served for
    /// the new one. Single-bucket stays generation 0 forever — the reconcile is
    /// one `u64` compare that never clears.
    ///
    /// Read affinity keeps both pins on one generation (src/buckets.rs), so the
    /// key populates cleanly from a single pin: the root-index key is populated
    /// only from the write pin, package index keys only from the read pin (the
    /// write-pin read-through renders uncached via [`build_variants`]). One
    /// populating pin per key means an entry never mixes two buckets' bytes.
    generation: u64,
    /// Keys with a refill in flight. When a TTL-lapsed (but not invalidated)
    /// entry is read, exactly one task claims the key here and refills it; every
    /// other concurrent reader is served the stale entry meanwhile. Bounded by
    /// live request concurrency — each leader clears its key via [`RefillGuard`]
    /// the moment its refill finishes (or fails). Absent keys don't register: a
    /// cold miss has nothing stale to serve, so it just loads (as before), and
    /// leaving it out keeps invalidation a hard drop — a dropped entry is absent,
    /// so the next read reloads with no stale shortcut to gate.
    refilling: HashSet<String>,
}

impl Entries {
    /// Adopt the caller's selection generation, clearing first if it changed.
    fn reconcile_generation(&mut self, generation: u64) {
        if self.generation != generation {
            self.map.clear();
            self.body_bytes = 0;
            self.refilling.clear();
            self.generation = generation;
        }
    }

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

    /// The staleness bound this cache was built with — the one configured knob
    /// every 1s-tier page cache shares (see `--index-cache-ttl-secs`).
    pub fn ttl(&self) -> Duration {
        self.ttl
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
    /// `generation` is the caller's pinned selection generation (design §3): a
    /// change from what the cache last saw clears it so entries never leak
    /// across a bucket switch.
    ///
    /// Under load an expired entry is refilled by a single task while every
    /// other reader is served the stale (≤ TTL old) entry, so a hot key costs
    /// one storage GET per TTL lapse instead of one per request; if the refill
    /// finds the bytes unchanged it reuses the cached gzip + ETag rather than
    /// recompressing. A dropped ([`invalidate`](Self::invalidate)d) entry is
    /// absent, not stale, so its next read reloads with no stale shortcut.
    pub async fn get(
        &self,
        storage: &dyn Storage,
        key: &str,
        generation: u64,
    ) -> Result<Option<(Variant, Option<Variant>)>> {
        // What this request does, decided under one short lock so it never
        // straddles the storage `.await`.
        enum Next {
            /// Answer from RAM — a fresh hit, or a stale entry served while
            /// another task already refills it.
            Serve(Option<(Variant, Option<Variant>)>),
            /// Load from storage. `stale` is the entry to reuse-compare against
            /// (present only for a refill); `claimed` means this task is the
            /// single refiller and must release its claim when done.
            Load {
                stale: Option<Cached>,
                claimed: bool,
            },
        }

        let next = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            entries.reconcile_generation(generation);
            match entries
                .map
                .get(key)
                .map(|e| (e.cached.clone(), e.fetched.elapsed() < self.ttl))
            {
                // Fresh: serve it.
                Some((cached, true)) => Next::Serve(cached.into_pair()),
                // Expired, someone else already refilling: serve the stale copy.
                Some((cached, false)) if entries.refilling.contains(key) => {
                    Next::Serve(cached.into_pair())
                }
                // Expired, no refill in flight: become the single refiller.
                Some((cached, false)) => {
                    entries.refilling.insert(key.to_string());
                    Next::Load {
                        stale: Some(cached),
                        claimed: true,
                    }
                }
                // Cold miss: load, as before — no stale to serve, no claim.
                None => Next::Load {
                    stale: None,
                    claimed: false,
                },
            }
        };

        let (stale, claimed) = match next {
            Next::Serve(pair) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(pair);
            }
            Next::Load { stale, claimed } => (stale, claimed),
        };
        self.misses.fetch_add(1, Ordering::Relaxed);

        // Release the refill claim however this returns — success, storage
        // error, or unwind — so a failed load never strands the key as forever
        // refilling (which would pin every later read to the stale entry). A
        // cold miss claimed nothing, so it arms no guard. Declared before the
        // lock below so it never re-locks a mutex this scope still holds.
        let _guard = claimed.then(|| RefillGuard::new(&self.entries, key));

        let loaded = storage.get_bytes(key).await;

        // Hash and gzip *before* taking the lock. `stale` was already cloned out
        // under the earlier lock, so the build needs nothing the mutex protects,
        // and on a full-PyPI mirror the root index is tens of MB — a second of
        // compression here would block every other index read on the node.
        // Bodies big enough to be that second go to the blocking pool too, so
        // they don't hold a request-serving thread either.
        let cached = match loaded {
            Ok(bytes) if bytes.len() >= GZIP_OFFLOAD_MIN_BYTES => {
                tokio::task::spawn_blocking(move || reuse_or_build(stale.as_ref(), bytes))
                    .await
                    .context("compressing a fetched index")?
            }
            Ok(bytes) => reuse_or_build(stale.as_ref(), bytes),
            Err(e) if is_not_found(&e) => Cached::Missing,
            Err(e) => return Err(e),
        };

        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.reconcile_generation(generation);
        entries.insert(
            key.to_string(),
            Entry {
                cached: cached.clone(),
                fetched: Instant::now(),
            },
        );
        entries.enforce_cap(self.max_bytes, self.ttl);
        drop(entries);
        Ok(cached.into_pair())
    }

    /// Drop a key after writing or deleting its index — same-process reads are
    /// fresh immediately, without waiting out the TTL. A hard drop: the entry is
    /// gone, so the next read reloads it, and no stale-while-revalidate path
    /// keeps serving the old bytes.
    ///
    /// The honest bound: a fill already in flight fetched its bytes *before*
    /// this drop and still inserts them when it finishes, so the dropped
    /// content can reappear once — for as long as that fill has left to run,
    /// which is its remaining storage read plus its hash+gzip (for a multi-MB
    /// index the compression is the larger half). It self-heals rather than
    /// sticking: the re-inserted entry expires one TTL later and the read after
    /// that reloads the current bytes.
    pub fn invalidate(&self, key: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
    }
}

/// Releases a single-flight refill claim on drop, so leadership of a key is
/// always relinquished — on success, on a storage error, or on an unwind. The
/// remove is idempotent, and a freshly refilled entry no longer consults the
/// claim (it reads fresh), so the brief window before this fires is harmless.
struct RefillGuard<'a> {
    entries: &'a Mutex<Entries>,
    key: &'a str,
}

impl<'a> RefillGuard<'a> {
    fn new(entries: &'a Mutex<Entries>, key: &'a str) -> Self {
        Self { entries, key }
    }
}

impl Drop for RefillGuard<'_> {
    fn drop(&mut self) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .refilling
            .remove(self.key);
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

#[derive(Default)]
struct PresignEntries {
    map: HashMap<String, (Arc<str>, Instant)>,
    /// Selection generation these URLs were signed against (design §3); see
    /// [`Entries::generation`]. A switch clears them so a URL into the old
    /// bucket is never handed out for the new one.
    generation: u64,
}

impl PresignEntries {
    fn reconcile_generation(&mut self, generation: u64) {
        if self.generation != generation {
            self.map.clear();
            self.generation = generation;
        }
    }
}

pub struct PresignCache {
    ttl: Duration,
    entries: Mutex<PresignEntries>,
}

impl PresignCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(PresignEntries::default()),
        }
    }

    pub fn fresh(&self, key: &str, generation: u64) -> Option<Arc<str>> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.reconcile_generation(generation);
        let (url, signed) = entries.map.get(key)?;
        (signed.elapsed() < self.ttl).then(|| url.clone())
    }

    pub fn put(&self, key: &str, url: Arc<str>, generation: u64) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.reconcile_generation(generation);
        entries.map.insert(key.to_string(), (url, Instant::now()));
        if entries.map.len() > PRESIGN_CACHE_MAX_ENTRIES {
            let ttl = self.ttl;
            entries.map.retain(|_, (_, signed)| signed.elapsed() < ttl);
            if entries.map.len() > PRESIGN_CACHE_MAX_ENTRIES {
                entries.map.clear();
            }
        }
    }

    /// Deletes must stop handing out the dead URL immediately (same node).
    pub fn invalidate(&self, key: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map
            .remove(key);
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
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(identity.body.as_ref(), b"body-1");
        assert_eq!(&*identity.etag, etag_of(b"body-1"));
        assert_eq!(storage.get_count(), 1);

        // Second read: served from RAM, same etag, no storage traffic.
        let (identity2, _) = cache
            .get(&storage, "simple/foo/index.json", 0)
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

        cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap();
        storage.insert("simple/foo/index.json", b"new".to_vec());
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (identity, _) = cache
            .get(&storage, "simple/foo/index.json", 0)
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

        cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap();
        storage.insert("simple/foo/index.json", b"new".to_vec());
        cache.invalidate("simple/foo/index.json");

        let (identity, _) = cache
            .get(&storage, "simple/foo/index.json", 0)
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
            .get(&storage, "simple/nope/index.json", 0)
            .await
            .unwrap()
            .is_none());
        assert!(cache
            .get(&storage, "simple/nope/index.json", 0)
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
            .get(&storage, "simple/nope/index.json", 0)
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
                .get(&storage, &format!("simple/p{i}/index.json"), 0)
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
            .get(&storage, "simple/p0/index.json", 0)
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
                .get(&storage, &format!("simple/missing{i}/index.json"), 0)
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
        assert!(cache.fresh("packages/p/a.whl", 0).is_none());
        cache.put("packages/p/a.whl", "https://signed.example/1".into(), 0);
        assert_eq!(
            cache.fresh("packages/p/a.whl", 0).as_deref(),
            Some("https://signed.example/1")
        );
        cache.invalidate("packages/p/a.whl");
        assert!(
            cache.fresh("packages/p/a.whl", 0).is_none(),
            "post-delete the URL must be gone immediately"
        );
        cache.put("packages/p/a.whl", "https://signed.example/2".into(), 0);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            cache.fresh("packages/p/a.whl", 0).is_none(),
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
            .get(&storage, "simple/foo/index.json", 0)
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
            .get(&storage, "simple/tiny/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        assert!(
            gz_tiny.is_none(),
            "sub-1KB bodies must not carry a gzip variant"
        );
        let (_, gz_rand) = cache
            .get(&storage, "simple/rand/index.json", 0)
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

        assert!(cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .is_err());

        // The error must not poison the cache as a negative entry.
        storage.insert("simple/foo/index.json", b"ok".to_vec());
        assert!(cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn generation_change_clears_index_cache() {
        // A bucket switch bumps the generation; an entry cached under the old
        // one must not be served for the new bucket (which has different bytes).
        let old = InMemStorage::default();
        old.insert("simple/foo/index.json", b"east".to_vec());
        let new = InMemStorage::default();
        new.insert("simple/foo/index.json", b"west".to_vec());
        let cache = IndexCache::new(Duration::from_secs(60));

        let (a, _) = cache
            .get(&old, "simple/foo/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.body.as_ref(), b"east");
        // Same key, new generation, different bucket: the stale entry is dropped.
        let (b, _) = cache
            .get(&new, "simple/foo/index.json", 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            b.body.as_ref(),
            b"west",
            "old-generation entry must not leak"
        );
    }

    #[tokio::test]
    async fn generation_change_clears_presign_cache() {
        let cache = PresignCache::new(Duration::from_secs(300));
        cache.put("packages/p/a.whl", "https://east/1".into(), 0);
        assert_eq!(
            cache.fresh("packages/p/a.whl", 0).as_deref(),
            Some("https://east/1")
        );
        // A switch to generation 1 must drop URLs signed against the old bucket.
        assert!(
            cache.fresh("packages/p/a.whl", 1).is_none(),
            "presigned URL from the old generation must not survive a switch"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_expiry_refills_once() {
        // A herd of readers hitting a TTL-lapsed entry must trigger exactly one
        // storage re-read: one task refills, the rest are served the stale copy.
        let storage = std::sync::Arc::new(InMemStorage::default());
        storage.insert("simple/foo/index.json", b"payload".repeat(200));
        // Hold the single refiller in flight long enough for the herd to observe
        // its claim and take the stale-serve path.
        storage.set_get_delay(Duration::from_millis(150));
        let cache = std::sync::Arc::new(IndexCache::new(Duration::from_millis(30)));

        // Warm the cache (one load), then let it lapse.
        cache
            .get(storage.as_ref(), "simple/foo/index.json", 0)
            .await
            .unwrap();
        assert_eq!(storage.get_count(), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 16 concurrent readers hit the expired entry at once.
        let mut handles = Vec::new();
        for _ in 0..16 {
            let c = cache.clone();
            let s = storage.clone();
            handles.push(tokio::spawn(async move {
                c.get(s.as_ref(), "simple/foo/index.json", 0)
                    .await
                    .unwrap()
                    .unwrap()
                    .0
                    .body
            }));
        }
        for h in handles {
            assert_eq!(
                h.await.unwrap().as_ref(),
                b"payload".repeat(200).as_slice(),
                "every reader is served the index during the refill"
            );
        }

        // One warm load + one refill is the guarantee. But under a scheduling
        // stall longer than the refill's 150ms in-flight window, a late reader can
        // arrive after the leader already repopulated the entry and open a second
        // refill window — at most one extra storage read. Bound it stall-tolerantly
        // rather than asserting exactly 2: anything past 3 means coalescing broke
        // and readers are re-reading per request (which trends toward ~17 here: the
        // warm load plus one per reader).
        let reads = storage.get_count();
        assert!(
            (2..=3).contains(&reads),
            "single-flight: expected 2-3 storage reads (1 warm + 1-2 refills under a stall), \
             got {reads} — one-per-reader would be ~17"
        );
    }

    #[tokio::test]
    async fn unchanged_refill_reuses_etag_and_gzip() {
        // The whole point of the refill: if the fetched bytes are unchanged, the
        // gzip variant and ETag are reused, not recomputed.
        let storage = InMemStorage::default();
        let body = b"{\"files\": []}".repeat(500); // compressible, above the floor
        storage.insert("simple/foo/index.json", body.clone());
        let cache = IndexCache::new(Duration::from_millis(10));

        let (id1, gz1) = cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        let gz1 = gz1.expect("compressible body must get a gzip variant");
        tokio::time::sleep(Duration::from_millis(20)).await; // lapse the TTL

        let (id2, gz2) = cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        let gz2 = gz2.expect("gzip variant must survive an unchanged refill");
        assert_eq!(storage.get_count(), 2, "the refill re-reads storage once");

        // gzip is deterministic, so equal *bytes* wouldn't distinguish reuse
        // from a recompute — pointer identity does. Reuse clones the refcounted
        // Arc/Bytes; a rebuild would allocate fresh ones.
        assert!(
            Arc::ptr_eq(&id1.etag, &id2.etag),
            "identity ETag must be the same Arc (reused, not rehashed into a new one)"
        );
        assert!(
            Arc::ptr_eq(&gz1.etag, &gz2.etag),
            "gzip ETag must be the same Arc (reused)"
        );
        assert_eq!(
            gz1.body.as_ptr(),
            gz2.body.as_ptr(),
            "gzip buffer must be the same allocation — no recompression"
        );
        assert_eq!(
            id1.body.as_ptr(),
            id2.body.as_ptr(),
            "identity buffer must be the same allocation"
        );
    }

    #[tokio::test]
    async fn above_threshold_fills_behave_identically() {
        // At or above GZIP_OFFLOAD_MIN_BYTES the hash+gzip runs on the blocking
        // pool rather than inline, so a full-PyPI root index can't burn a second
        // of a request-serving thread. The hop must be invisible: same variants
        // on the cold fill, same reuse on an unchanged refill.
        //
        // What this pins is that equivalence at an above-threshold size — it
        // passes whether or not the offload arm exists, and earns its keep by
        // being the only test that runs a fill through it at all.
        let storage = InMemStorage::default();
        let body = b"{\"name\": \"pkg\", \"files\": []}\n".repeat(50_000);
        assert!(
            body.len() >= GZIP_OFFLOAD_MIN_BYTES,
            "test body must cross the offload threshold"
        );
        storage.insert("simple/index.json", body.clone());
        let cache = IndexCache::new(Duration::from_millis(10));

        let (id1, gz1) = cache
            .get(&storage, "simple/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        let gz1 = gz1.expect("a compressible multi-MB index must get a gzip variant");
        assert_eq!(id1.body, body, "identity body must round-trip unchanged");
        assert_eq!(
            id1.etag,
            quoted_sha256(&body),
            "ETag is the SHA-256 of the bytes, offloaded or not"
        );

        tokio::time::sleep(Duration::from_millis(20)).await; // lapse the TTL
        let (id2, gz2) = cache
            .get(&storage, "simple/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        let gz2 = gz2.expect("gzip variant must survive an unchanged refill");
        assert!(
            Arc::ptr_eq(&id1.etag, &id2.etag),
            "an unchanged refill must reuse the ETag across the blocking hop"
        );
        assert_eq!(
            gz1.body.as_ptr(),
            gz2.body.as_ptr(),
            "an unchanged refill must reuse the gzip buffer, offloaded or not"
        );
    }

    #[tokio::test]
    async fn changed_refill_rebuilds() {
        // Contrast to the reuse test: when the bytes change, the refill must
        // rebuild — new ETag, fresh buffers — so reuse is genuinely conditional.
        let storage = InMemStorage::default();
        storage.insert("simple/foo/index.json", b"{\"files\": []}".repeat(500));
        let cache = IndexCache::new(Duration::from_millis(10));

        let (id1, _) = cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        let changed = b"{\"files\": [1]}".repeat(500);
        storage.insert("simple/foo/index.json", changed.clone());
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (id2, _) = cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id2.body.as_ref(), changed, "refill serves the new bytes");
        assert_ne!(id1.etag, id2.etag, "changed content gets a new ETag");
        assert_ne!(
            id1.body.as_ptr(),
            id2.body.as_ptr(),
            "changed content is rebuilt, not reused"
        );
    }

    #[tokio::test]
    async fn invalidate_drops_entry_no_stale_serve() {
        // After invalidation the entry is absent, not stale: the next read
        // reloads with no stale-while-revalidate shortcut, even once the TTL has
        // lapsed (the path that would otherwise stale-serve).
        let storage = InMemStorage::default();
        storage.insert("simple/foo/index.json", b"old".to_vec());
        let cache = IndexCache::new(Duration::from_millis(10));

        cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await; // lapse the TTL
        storage.insert("simple/foo/index.json", b"new".to_vec());
        cache.invalidate("simple/foo/index.json");

        let (id, _) = cache
            .get(&storage, "simple/foo/index.json", 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            id.body.as_ref(),
            b"new",
            "a hard drop reloads fresh; no stale entry may be served after invalidate"
        );
    }
}
