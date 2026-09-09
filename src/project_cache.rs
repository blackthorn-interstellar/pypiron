//! Rendered `/project/<pkg>/` page cache: RAM bytes, bounded staleness.
//!
//! The human project page renders on demand — a full package-prefix storage
//! scan plus a per-file sidecar parse on *every* request (src/pages.rs
//! `render_project`). For a package with thousands of files that is a storage
//! round-trip and well over a second of work per hit, ~100x slower than its
//! cached `/simple/<pkg>/` sibling, and it collapses under concurrency. The page
//! is already allowed to lag truth (the worker rebuilds indexes asynchronously),
//! so the read path serves the rendered HTML from RAM under the same idioms as
//! the index cache (src/cache.rs):
//!
//! - **Hit**: zero storage calls, zero rendering — a refcounted `Bytes` clone.
//! - **Single-flight refill**: when an entry lapses, one reader claims the
//!   re-render while every concurrent reader is served the (≤ TTL stale) page —
//!   so a burst on a hot key costs one render per TTL, not one per request. The
//!   render is the full scan (seconds for a many-thousand-file package), so
//!   coalescing it is the point. A cold key has no stale page to serve, so it
//!   just renders, exactly as the index cache loads a cold key.
//! - **Staleness bound**: entries expire after the TTL; the worker invalidates a
//!   package's entries the instant it rebuilds its indexes, so same-node reads
//!   reflect an upload immediately and the TTL only bounds staleness from other
//!   writers. Invalidation and a generation switch are hard drops — no stale page
//!   survives them, even with a re-render in flight.
//! - **Bounded memory**: a byte ceiling caps the cache; an insert past it prunes
//!   expired entries and, failing that, clears everything — a refill is one scan
//!   per hot key, once per TTL.
//!
//! The render embeds the request's base URL (the install snippet's index URL),
//! but entries key only on `(package, version)`: the page is rendered with a
//! host sentinel and the real host is filled in at serve time (see
//! [`BASE_URL_SENTINEL`]), so a forged Host header can't thrash the cache with
//! distinct keys or leak one host's URL into another's page. Access control is
//! enforced *before* the cache (the handler rejects a non-reader up front),
//! exactly as `/simple/` does, so the cached bytes are identical for every
//! authorized reader and nothing leaks across auth outcomes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;

/// Memory ceiling for cached pages. A rendered page for a many-thousand-file
/// package is a few MB; this holds a healthy working set and, like the index
/// cache, clears wholesale rather than growing unbounded.
pub(crate) const PROJECT_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Fixed per-entry overhead charged to the ceiling so a flood of distinct keys
/// (an attacker cycling version segments) bounds its own entry count through the
/// same cap instead of growing the map until OOM.
const ENTRY_OVERHEAD_BYTES: usize = 256;

/// The field separator in a cache key. A normalized package name and a version
/// are both drawn from a restricted character set that excludes this control
/// byte, so keys are unambiguous and a package's prefix (used for invalidation)
/// can never match across a package boundary.
const KEY_SEP: char = '\u{1f}';

/// The cache key for one rendered page: package and requested version (empty for
/// the latest view). Deliberately host-independent — the page is rendered with
/// [`BASE_URL_SENTINEL`] and the request's real host is filled in at serve time —
/// so a forged Host / X-Forwarded-Host header can neither multiply the key set
/// into an unbounded full-scan flood nor poison another visitor's install snippet.
pub fn key(pkg: &str, version: Option<&str>) -> String {
    format!("{pkg}{KEY_SEP}{}", version.unwrap_or(""))
}

/// Placeholder the project page is rendered with in place of the request's base
/// URL (`scheme://host`). Cached bytes carry this sentinel; `pages::serve_project_page`
/// substitutes the request's real host per serve. The `\u{1}` delimiters can't
/// occur in a rendered host, package name, or version, so it never collides.
pub const BASE_URL_SENTINEL: &str = "\u{1}pypiron-base-url\u{1}";

/// The prefix covering every cached page for a package, across all versions and
/// hosts — what [`ProjectCache::invalidate_package`] drops.
fn package_prefix(pkg: &str) -> String {
    format!("{pkg}{KEY_SEP}")
}

struct Entry {
    body: Bytes,
    fetched: Instant,
}

impl Entry {
    fn weight(&self) -> usize {
        ENTRY_OVERHEAD_BYTES + self.body.len()
    }
}

#[derive(Default)]
struct Entries {
    map: HashMap<String, Entry>,
    body_bytes: usize,
    /// The selection generation these entries were built under; a bucket switch
    /// (design §3) bumps it and the first access carrying the new value clears
    /// everything, so a page built from the old bucket can't serve the new one.
    /// Single-bucket stays generation 0 forever — one `u64` compare that never
    /// clears. Mirrors [`crate::cache::IndexCache`].
    generation: u64,
    /// Active render tokens, bounded by live request concurrency. Cold readers
    /// share a token; expired entries serve stale while one reader renders.
    /// Invalidation removes the token too, so late renders cannot restore a
    /// dropped page. The last aborted reader releases its token on drop.
    refilling: HashMap<String, Arc<()>>,
}

impl Entries {
    fn reconcile_generation(&mut self, generation: u64) {
        if generation > self.generation {
            self.map.clear();
            self.body_bytes = 0;
            self.refilling.clear();
            self.generation = generation;
        }
    }

    fn insert(&mut self, key: String, entry: Entry) {
        self.body_bytes += entry.weight();
        if let Some(old) = self.map.insert(key, entry) {
            self.body_bytes -= old.weight();
        }
    }

    /// Drop every entry whose key starts with `prefix`, keeping the byte tally
    /// exact.
    fn remove_prefix(&mut self, prefix: &str) {
        let mut freed = 0usize;
        self.map.retain(|k, e| {
            let keep = !k.starts_with(prefix);
            if !keep {
                freed += e.weight();
            }
            keep
        });
        self.body_bytes -= freed;
        self.refilling.retain(|key, _| !key.starts_with(prefix));
    }

    /// Enforce the byte ceiling: drop expired entries first; if the live set
    /// alone still exceeds it, clear everything (a refill is one scan per hot
    /// key, once).
    fn enforce_cap(&mut self, max_bytes: usize, ttl: Duration) {
        if self.body_bytes <= max_bytes {
            return;
        }
        let mut freed = 0usize;
        self.map.retain(|_, e| {
            let keep = e.fetched.elapsed() < ttl;
            if !keep {
                freed += e.weight();
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

/// What [`ProjectCache::get`] tells the caller to do. `Fresh`/`Stale` carry a
/// page to serve as-is; `MustRender` means the caller renders and calls
/// [`ProjectCache::put`].
pub enum Lookup<'a> {
    /// A fresh cached page — serve it, render nothing.
    Fresh(Bytes),
    /// A stale page served while another reader re-renders this key, so a burst
    /// past the TTL costs one render, not one per request.
    Stale(Bytes),
    /// No page to serve: the caller must render and `put`. Carries the render
    /// token and releases it on drop, including when rendering aborts.
    MustRender(RenderClaim<'a>),
}

/// Permission to cache a render, revoked by invalidation or a bucket switch.
/// Cold readers share a token; the first successful render fills the cache.
pub struct RenderClaim<'a> {
    entries: &'a Mutex<Entries>,
    /// None for an already-obsolete bucket generation: render for that pinned
    /// request, but do not disturb or fill the current generation's cache.
    claim: Option<(String, Arc<()>)>,
}

impl Drop for RenderClaim<'_> {
    fn drop(&mut self) {
        if let Some((key, token)) = self.claim.take() {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            if entries.refilling.get(&key).is_some_and(|current| {
                Arc::ptr_eq(current, &token) && Arc::strong_count(current) == 2
            }) {
                entries.refilling.remove(&key);
            }
            // Release under the lock so simultaneous aborts cannot both count
            // the other's claim and leave a token with no renderer behind it.
            drop(token);
        }
    }
}

pub struct ProjectCache {
    ttl: Duration,
    max_bytes: usize,
    entries: Mutex<Entries>,
}

impl ProjectCache {
    pub fn new(ttl: Duration) -> Self {
        Self::with_capacity(ttl, PROJECT_CACHE_MAX_BYTES)
    }

    pub fn with_capacity(ttl: Duration, max_bytes: usize) -> Self {
        Self {
            ttl,
            max_bytes,
            entries: Mutex::new(Entries::default()),
        }
    }

    /// Decide what a read of `key` does, under one short lock. A fresh entry is
    /// served; a lapsed one is served stale when another reader is already
    /// re-rendering it, otherwise this caller claims the single re-render and
    /// must `put` the result (or drop the claim on an abort). Cold misses render
    /// concurrently under a shared token. `generation` is the caller's pinned
    /// selection generation (design §3): a change from what the cache last saw
    /// clears it so entries never leak across a bucket switch.
    pub fn get(&self, key: &str, generation: u64) -> Lookup<'_> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.reconcile_generation(generation);
        if generation < entries.generation {
            return Lookup::MustRender(RenderClaim {
                entries: &self.entries,
                claim: None,
            });
        }
        // Resolve the entry to owned data so the map borrow ends before the
        // `refilling` mutation below (which would otherwise alias it).
        let state = entries
            .map
            .get(key)
            .map(|e| (e.body.clone(), e.fetched.elapsed() < self.ttl));
        match state {
            // Fresh: serve it, render nothing.
            Some((body, true)) => Lookup::Fresh(body),
            // Lapsed, someone else already re-rendering: serve the stale page.
            Some((body, false)) if entries.refilling.contains_key(key) => Lookup::Stale(body),
            // Claim both cold and expired renders so invalidation can revoke
            // every fill. Concurrent cold readers share the same token.
            Some((_, false)) | None => {
                let token = entries
                    .refilling
                    .entry(key.to_string())
                    .or_default()
                    .clone();
                Lookup::MustRender(RenderClaim {
                    entries: &self.entries,
                    claim: Some((key.to_string(), token)),
                })
            }
        }
    }

    /// Cache the first completed render whose token is still current. A render
    /// invalidated while it ran may finish its request, but cannot refill RAM.
    pub fn put(&self, body: Bytes, claim: RenderClaim<'_>) {
        let Some((key, token)) = &claim.claim else {
            return;
        };
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if !entries
            .refilling
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, token))
        {
            return;
        }
        entries.refilling.remove(key);
        entries.insert(
            key.clone(),
            Entry {
                body,
                fetched: Instant::now(),
            },
        );
        entries.enforce_cap(self.max_bytes, self.ttl);
    }

    /// Drop every cached page for a package (all versions and hosts) after its
    /// indexes rebuild — same-node reads reflect the change at once, without
    /// waiting out the TTL. Revokes unfinished renders as well as cached pages.
    pub fn invalidate_package(&self, pkg: &str) {
        let prefix = package_prefix(pkg);
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove_prefix(&prefix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    fn claim<'a>(cache: &'a ProjectCache, key: &str, generation: u64) -> RenderClaim<'a> {
        let Lookup::MustRender(claim) = cache.get(key, generation) else {
            panic!("expected a render claim");
        };
        claim
    }

    fn put(cache: &ProjectCache, key: String, body: Bytes, generation: u64) {
        cache.put(body, claim(cache, &key, generation));
    }

    fn expire(cache: &ProjectCache, key: &str) {
        cache
            .entries
            .lock()
            .unwrap()
            .map
            .get_mut(key)
            .unwrap()
            .fetched -= Duration::from_secs(3600);
    }

    /// The page a `Fresh` or `Stale` lookup carries, or `None` for `MustRender`.
    fn served(lookup: Lookup) -> Option<Bytes> {
        match lookup {
            Lookup::Fresh(b) | Lookup::Stale(b) => Some(b),
            Lookup::MustRender(_) => None,
        }
    }

    #[test]
    fn hit_within_ttl_miss_after() {
        let cache = ProjectCache::new(Duration::from_millis(20));
        let k = key("foo", None);
        assert!(
            matches!(cache.get(&k, 0), Lookup::MustRender(_)),
            "cold key must render"
        );
        put(&cache, k.clone(), body("page-1"), 0);
        assert_eq!(
            served(cache.get(&k, 0)).as_deref(),
            Some(b"page-1".as_ref())
        );
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            matches!(cache.get(&k, 0), Lookup::MustRender(_)),
            "expired entry with no refill in flight must re-render"
        );
    }

    #[test]
    fn version_is_a_distinct_key() {
        let cache = ProjectCache::new(Duration::from_secs(60));
        put(&cache, key("foo", None), body("latest"), 0);
        put(&cache, key("foo", Some("1.0")), body("v1"), 0);
        assert_eq!(
            served(cache.get(&key("foo", None), 0)).as_deref(),
            Some(b"latest".as_ref())
        );
        assert_eq!(
            served(cache.get(&key("foo", Some("1.0")), 0)).as_deref(),
            Some(b"v1".as_ref()),
            "a pinned version must not answer for the latest view"
        );
    }

    #[test]
    fn invalidate_package_drops_all_versions() {
        let cache = ProjectCache::new(Duration::from_secs(60));
        put(&cache, key("foo", None), body("latest"), 0);
        put(&cache, key("foo", Some("1.0")), body("v1"), 0);
        put(&cache, key("foobar", None), body("sibling"), 0);
        cache.invalidate_package("foo");
        assert!(
            matches!(cache.get(&key("foo", None), 0), Lookup::MustRender(_)),
            "invalidation must clear the latest view"
        );
        assert!(
            matches!(
                cache.get(&key("foo", Some("1.0")), 0),
                Lookup::MustRender(_)
            ),
            "invalidation must clear pinned versions"
        );
        assert_eq!(
            served(cache.get(&key("foobar", None), 0)).as_deref(),
            Some(b"sibling".as_ref()),
            "a prefix sibling must survive — 'foo' must not match 'foobar'"
        );
    }

    #[test]
    fn byte_cap_bounds_the_map() {
        // 8 x 1 KB pages under a 4 KB ceiling: the cache must stay bounded.
        let cache = ProjectCache::with_capacity(Duration::from_secs(60), 4 * 1024);
        for i in 0..8 {
            put(
                &cache,
                key(&format!("p{i}"), None),
                Bytes::from(vec![b'x'; 1024]),
                0,
            );
        }
        let used = cache.entries.lock().unwrap().body_bytes;
        assert!(
            used <= 4 * 1024,
            "cache body bytes {used} exceed the ceiling"
        );
    }

    #[test]
    fn generation_change_clears_everything() {
        let cache = ProjectCache::new(Duration::from_secs(60));
        let k = key("foo", None);
        put(&cache, k.clone(), body("gen0"), 0);
        assert_eq!(served(cache.get(&k, 0)).as_deref(), Some(b"gen0".as_ref()));
        // A bucket switch bumps the generation: the old entry must not survive.
        assert!(
            matches!(cache.get(&k, 1), Lookup::MustRender(_)),
            "an entry from the old generation must not serve the new one"
        );
    }

    #[test]
    fn single_flight_one_render_others_stale() {
        // The single-flight guarantee: while one reader holds the render claim for
        // a lapsed key, every concurrent reader is served the stale page — exactly
        // one MustRender, the rest Stale. Held deterministically here (the claim
        // sits in a live variable while the herd calls `get`), so the invariant is
        // proven without leaning on thread scheduling.
        let cache = ProjectCache::new(Duration::from_millis(20));
        let k = key("busy", None);
        put(&cache, k.clone(), body("stale"), 0);
        std::thread::sleep(Duration::from_millis(30)); // lapse the TTL

        // First lapsed read claims the single re-render.
        let claim = cache.get(&k, 0);
        assert!(
            matches!(claim, Lookup::MustRender(_)),
            "the first lapsed read must claim the re-render"
        );
        // Every concurrent read while that claim is held is served the stale page.
        for _ in 0..16 {
            assert_eq!(
                served(cache.get(&k, 0)).as_deref(),
                Some(b"stale".as_ref()),
                "readers must be served the stale page while one render is in flight"
            );
        }
        // Releasing the claim without a `put` (an aborted render) frees the slot,
        // so the next lapsed read re-claims rather than serving stale forever.
        drop(claim);
        assert!(
            matches!(cache.get(&k, 0), Lookup::MustRender(_)),
            "after the claim releases, the next read must re-claim the render"
        );
    }

    #[test]
    fn invalidate_beats_an_inflight_render() {
        for warm in [false, true] {
            let cache = ProjectCache::new(Duration::from_secs(60));
            let k = key("racey", None);
            if warm {
                put(&cache, k.clone(), body("stale"), 0);
                expire(&cache, &k);
            }
            let old = claim(&cache, &k, 0);
            cache.invalidate_package("racey");
            put(&cache, k.clone(), body("new"), 0);
            cache.put(body("old"), old);
            assert_eq!(served(cache.get(&k, 0)).as_deref(), Some(b"new".as_ref()));
            assert!(cache.entries.lock().unwrap().refilling.is_empty());
        }
    }

    #[test]
    fn invalidated_render_cannot_refill_an_empty_cache() {
        let cache = ProjectCache::new(Duration::from_secs(60));
        let k = key("racey", None);
        let old = claim(&cache, &k, 0);
        cache.invalidate_package("racey");
        cache.put(body("old"), old);
        assert!(matches!(cache.get(&k, 0), Lookup::MustRender(_)));
    }

    #[test]
    fn old_generation_cannot_replace_or_clear_new_pages() {
        let cache = ProjectCache::new(Duration::from_secs(60));
        let k = key("racey", None);
        let old = claim(&cache, &k, 0);
        put(&cache, k.clone(), body("new bucket"), 1);
        cache.put(body("old bucket"), old);
        let delayed_old_read = claim(&cache, &k, 0);
        cache.put(body("old bucket again"), delayed_old_read);
        assert_eq!(
            served(cache.get(&k, 1)).as_deref(),
            Some(b"new bucket".as_ref())
        );
    }

    #[test]
    fn dropping_an_old_claim_preserves_the_new_refill() {
        let cache = ProjectCache::new(Duration::from_secs(60));
        let k = key("racey", None);
        let old = claim(&cache, &k, 0);
        cache.invalidate_package("racey");
        put(&cache, k.clone(), body("new"), 0);
        expire(&cache, &k);
        let new = claim(&cache, &k, 0);
        drop(old);
        assert!(matches!(cache.get(&k, 0), Lookup::Stale(_)));
        cache.put(body("newer"), new);
        assert_eq!(served(cache.get(&k, 0)).as_deref(), Some(b"newer".as_ref()));
    }

    #[test]
    fn cold_readers_share_a_token_until_one_finishes_or_all_abort() {
        let cache = ProjectCache::new(Duration::from_secs(60));
        let k = key("racey", None);
        let first = claim(&cache, &k, 0);
        let second = claim(&cache, &k, 0);
        drop(first);
        assert_eq!(cache.entries.lock().unwrap().refilling.len(), 1);
        cache.put(body("second"), second);
        assert_eq!(
            served(cache.get(&k, 0)).as_deref(),
            Some(b"second".as_ref())
        );

        cache.invalidate_package("racey");
        let first = claim(&cache, &k, 0);
        let second = claim(&cache, &k, 0);
        drop(first);
        drop(second);
        assert!(cache.entries.lock().unwrap().refilling.is_empty());
    }

    #[test]
    fn package_invalidation_preserves_other_pending_renders() {
        let cache = ProjectCache::new(Duration::from_secs(60));
        let k = key("foobar", None);
        let other = claim(&cache, &k, 0);
        cache.invalidate_package("foo");
        cache.put(body("unaffected"), other);
        assert_eq!(
            served(cache.get(&k, 0)).as_deref(),
            Some(b"unaffected".as_ref())
        );
    }
}
